//! Coinbase Exchange: last trade price streamed over WebSocket.

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{Observation, SourceId};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::time::{MissedTickBehavior, timeout};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

use super::{parse_decimal, parse_rfc3339};
use crate::{
    error::{SourceError, excerpt},
    runtime::{Source, SourceContext},
};

/// Largest inbound message accepted. Ticker messages are under 1 KiB.
const MAX_MESSAGE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct CoinbaseConfig {
    /// `wss://ws-feed.exchange.coinbase.com`.
    pub url: String,
    /// Market and Coinbase product id, such as `BTC-USD` and `BTC-USD`.
    pub products: Vec<(MarketId, String)>,
    /// Ticks arrive on every trade; at most one observation per product is
    /// emitted per interval, carrying the newest price.
    pub sample_interval: Duration,
    /// The heartbeat channel sends a message per product every second, so
    /// a silent connection this long is a dead connection.
    pub idle_timeout: Duration,
}

/// Messages of the `ticker` and `heartbeat` channels, plus the control
/// messages around them. Unknown types are ignored, so new message types
/// on the feed never break the connector.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum FeedMessage {
    Ticker {
        product_id: String,
        price: String,
        time: String,
        sequence: u64,
    },
    Heartbeat,
    Subscriptions,
    Error {
        message: String,
        #[serde(default)]
        reason: Option<String>,
    },
    #[serde(other)]
    Other,
}

/// Streams the ticker channel and samples it.
///
/// A WebSocket instead of polling because BTC-USD trades many times a
/// second: a REST poll every few seconds would report a price that is
/// already old, and polling faster would hit the public rate limit.
pub struct Coinbase {
    config: CoinbaseConfig,
    markets: HashMap<String, MarketId>,
    source: SourceId,
}

impl Coinbase {
    pub const SOURCE: &'static str = "coinbase";

    pub fn new(config: CoinbaseConfig) -> Result<Self, SourceError> {
        let markets = config
            .products
            .iter()
            .map(|(market, product)| (product.clone(), market.clone()))
            .collect();
        Ok(Self {
            config,
            markets,
            source: SourceId::parse(Self::SOURCE).map_err(SourceError::decode)?,
        })
    }

    fn subscribe_message(&self) -> String {
        let products: Vec<&str> = self.markets.keys().map(String::as_str).collect();
        serde_json::json!({
            "type": "subscribe",
            "product_ids": products,
            "channels": ["ticker", "heartbeat"],
        })
        .to_string()
    }
}

#[async_trait]
impl Source for Coinbase {
    fn name(&self) -> &str {
        Self::SOURCE
    }

    async fn run(&self, ctx: &SourceContext) -> Result<(), SourceError> {
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_MESSAGE_BYTES));
        let (mut socket, _) = timeout(
            Duration::from_secs(10),
            connect_async_with_config(self.config.url.as_str(), Some(config), true),
        )
        .await
        .map_err(|_| SourceError::Transport("connect timed out".into()))?
        .map_err(|error| SourceError::Transport(excerpt(&error.to_string())))?;
        socket
            .send(Message::text(self.subscribe_message()))
            .await
            .map_err(|error| SourceError::Transport(excerpt(&error.to_string())))?;
        tracing::info!(products = self.markets.len(), "coinbase feed connected");

        let mut sampler = Sampler::default();
        let mut tick = tokio::time::interval(self.config.sample_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = ctx.shutdown.cancelled() => {
                    let _ = socket.close(None).await;
                    return Ok(());
                }
                _ = tick.tick() => {
                    for (product, price, time) in sampler.drain() {
                        if let Some(market) = self.markets.get(&product) {
                            let observation = Observation::new(market.clone(), self.source.clone(), price, time)
                                .map_err(SourceError::decode)?;
                            ctx.emit(observation).await?;
                        }
                    }
                }
                message = timeout(self.config.idle_timeout, socket.next()) => {
                    let text = match message {
                        Err(_) => return Err(SourceError::Transport("feed went silent".into())),
                        Ok(None) => return Err(SourceError::Transport("feed closed".into())),
                        Ok(Some(Err(error))) => {
                            return Err(SourceError::Transport(excerpt(&error.to_string())));
                        }
                        Ok(Some(Ok(Message::Text(text)))) => text,
                        Ok(Some(Ok(Message::Close(frame)))) => {
                            let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                            return Err(SourceError::upstream(format_args!("feed closed: {reason}")));
                        }
                        // Pings are answered by the library; nothing else is expected.
                        Ok(Some(Ok(_))) => continue,
                    };
                    match parse_message(&text)? {
                        FeedMessage::Ticker { product_id, price, time, sequence } => {
                            sampler.record(product_id, sequence, parse_decimal(&price)?, parse_rfc3339(&time)?);
                        }
                        FeedMessage::Error { message, reason } => {
                            let reason = reason.unwrap_or_default();
                            return Err(SourceError::upstream(format_args!("{message} {reason}")));
                        }
                        FeedMessage::Heartbeat | FeedMessage::Subscriptions | FeedMessage::Other => {}
                    }
                }
            }
        }
    }
}

fn parse_message(text: &str) -> Result<FeedMessage, SourceError> {
    serde_json::from_str(text).map_err(SourceError::decode)
}

/// Newest unsent tick per product.
///
/// Coinbase sequence numbers grow per product. A tick with a sequence not
/// above the last one seen is a duplicate or arrived out of order, and is
/// dropped rather than allowed to overwrite a newer price.
#[derive(Debug, Default)]
struct Sampler {
    last_sequence: HashMap<String, u64>,
    pending: HashMap<String, (Decimal, Timestamp)>,
}

impl Sampler {
    fn record(&mut self, product: String, sequence: u64, price: Decimal, time: Timestamp) {
        let last = self.last_sequence.entry(product.clone()).or_insert(0);
        if sequence <= *last {
            return;
        }
        *last = sequence;
        self.pending.insert(product, (price, time));
    }

    fn drain(&mut self) -> Vec<(String, Decimal, Timestamp)> {
        let mut ticks: Vec<_> = self
            .pending
            .drain()
            .map(|(product, (price, time))| (product, price, time))
            .collect();
        ticks.sort_by(|a, b| a.0.cmp(&b.0));
        ticks
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn recorded() -> Vec<FeedMessage> {
        include_str!("../../tests/fixtures/coinbase_ws.jsonl")
            .lines()
            .map(|line| parse_message(line).unwrap())
            .collect()
    }

    #[test]
    fn recorded_feed_messages_parse() {
        let messages = recorded();
        assert_eq!(messages[0], FeedMessage::Subscriptions);
        let FeedMessage::Ticker {
            product_id,
            price,
            time,
            sequence,
        } = &messages[1]
        else {
            unreachable!("second message is a ticker")
        };
        assert_eq!(
            (product_id.as_str(), price.as_str()),
            ("BTC-USD", "81760.5")
        );
        assert_eq!(*sequence, 137_455_662_084);
        assert_eq!(time, "2026-10-08T22:09:29.665602Z");
        assert!(messages.contains(&FeedMessage::Heartbeat));
    }

    #[test]
    fn sampler_keeps_the_newest_tick_and_drops_replays() {
        let mut sampler = Sampler::default();
        let at = |n| Timestamp::from_unix_nanos(n);
        for message in recorded() {
            if let FeedMessage::Ticker {
                product_id,
                price,
                time,
                sequence,
            } = message
            {
                sampler.record(
                    product_id,
                    sequence,
                    parse_decimal(&price).unwrap(),
                    parse_rfc3339(&time).unwrap(),
                );
            }
        }
        // A replay of an old sequence must not overwrite the newer price.
        sampler.record("BTC-USD".into(), 1, dec!(1), at(1));
        let ticks = sampler.drain();
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].0, "BTC-USD");
        assert_eq!(ticks[0].1, dec!(81760.51));
        assert!(sampler.drain().is_empty());
    }

    #[test]
    fn unknown_message_types_are_ignored() {
        assert_eq!(
            parse_message(r#"{"type":"status","x":1}"#).unwrap(),
            FeedMessage::Other
        );
        assert!(parse_message("not json").is_err());
    }
}
