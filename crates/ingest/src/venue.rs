//! Orderflow's own trades as a price source.
//!
//! The matching engine publishes every trade to the event topic. This
//! module reads those records with a tolerant reader: it only looks at the
//! envelope fields it needs and the trade price, so producer-side additions
//! never break it. Venue prices are configured with the `monitor` role:
//! shown next to the index and alerted on when they drift, never part of
//! it.

use std::collections::HashSet;

use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{Observation, SourceId};
use serde::Deserialize;

use crate::{connectors::parse_decimal, error::SourceError};

/// Record versions this reader understands. Version 2 added the epoch,
/// which a price observation does not need, so both read the same way.
const SUPPORTED_SCHEMAS: std::ops::RangeInclusive<u16> = 1..=2;

#[derive(Debug, Deserialize)]
struct Envelope {
    schema_version: u16,
    market: String,
    occurred_at_ns: u64,
    event_type: String,
    #[serde(default)]
    data: Option<TradeData>,
}

#[derive(Debug, Deserialize)]
struct TradeData {
    #[serde(default)]
    price: Option<String>,
}

/// Turns one event record into an observation if it is a trade on a
/// market of interest. Other event types are skipped (`Ok(None)`).
pub fn trade_observation(
    payload: &[u8],
    source: &SourceId,
    markets: &HashSet<MarketId>,
) -> Result<Option<Observation>, SourceError> {
    let envelope: Envelope = serde_json::from_slice(payload).map_err(SourceError::decode)?;
    if !SUPPORTED_SCHEMAS.contains(&envelope.schema_version) {
        return Err(SourceError::decode(format_args!(
            "unsupported event schema {}",
            envelope.schema_version
        )));
    }
    if envelope.event_type != "trade_executed" {
        return Ok(None);
    }
    let Ok(market) = MarketId::parse(&envelope.market) else {
        return Err(SourceError::decode("event with an invalid market id"));
    };
    if !markets.contains(&market) {
        return Ok(None);
    }
    let price = envelope
        .data
        .and_then(|data| data.price)
        .ok_or_else(|| SourceError::decode("trade without a price"))?;
    let observation = Observation::new(
        market,
        source.clone(),
        parse_decimal(&price)?,
        Timestamp::from_unix_nanos(envelope.occurred_at_ns),
    )
    .map_err(SourceError::decode)?;
    Ok(Some(observation))
}

#[cfg(feature = "kafka")]
pub use consumer::{OrderflowTrades, OrderflowTradesConfig};

#[cfg(feature = "kafka")]
mod consumer {
    use std::collections::HashSet;

    use async_trait::async_trait;
    use orderflow_domain::MarketId;
    use orderflow_marketdata::SourceId;
    use rdkafka::{
        ClientConfig, Message,
        consumer::{Consumer, StreamConsumer},
    };

    use super::trade_observation;
    use crate::{
        error::{SourceError, excerpt},
        runtime::{Source, SourceContext},
    };

    #[derive(Debug, Clone)]
    pub struct OrderflowTradesConfig {
        pub brokers: String,
        pub topic: String,
        pub group_id: String,
        pub markets: Vec<MarketId>,
    }

    /// Kafka consumer of Orderflow's event topic.
    ///
    /// It starts from the latest offset: a price feed wants the current
    /// price, and replaying hours of history after a restart would only
    /// produce observations the index rejects as too old.
    pub struct OrderflowTrades {
        config: OrderflowTradesConfig,
        markets: HashSet<MarketId>,
        source: SourceId,
    }

    impl OrderflowTrades {
        pub const SOURCE: &'static str = "orderflow";

        pub fn new(config: OrderflowTradesConfig) -> Result<Self, SourceError> {
            Ok(Self {
                markets: config.markets.iter().cloned().collect(),
                config,
                source: SourceId::parse(Self::SOURCE).map_err(SourceError::decode)?,
            })
        }
    }

    #[async_trait]
    impl Source for OrderflowTrades {
        fn name(&self) -> &str {
            Self::SOURCE
        }

        async fn run(&self, ctx: &SourceContext) -> Result<(), SourceError> {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.brokers)
                .set("group.id", &self.config.group_id)
                .set("auto.offset.reset", "latest")
                .set("enable.auto.commit", "true")
                .create()
                .map_err(|error| SourceError::Transport(excerpt(&error.to_string())))?;
            consumer
                .subscribe(&[self.config.topic.as_str()])
                .map_err(|error| SourceError::Transport(excerpt(&error.to_string())))?;
            loop {
                let message = tokio::select! {
                    () = ctx.shutdown.cancelled() => return Ok(()),
                    message = consumer.recv() => message,
                };
                let message =
                    message.map_err(|error| SourceError::Transport(excerpt(&error.to_string())))?;
                let Some(payload) = message.payload() else {
                    continue;
                };
                match trade_observation(payload, &self.source, &self.markets) {
                    Ok(Some(observation)) => ctx.emit(observation).await?,
                    Ok(None) => {}
                    // One bad record must not stop the consumer.
                    Err(error) => {
                        ctx.metrics.source_error(Self::SOURCE, error.kind());
                        tracing::warn!(%error, offset = message.offset(), "skipping an event record");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn markets() -> HashSet<MarketId> {
        HashSet::from([MarketId::parse("BTC-USD").unwrap()])
    }

    fn source() -> SourceId {
        SourceId::parse("orderflow").unwrap()
    }

    #[test]
    fn trades_become_observations() {
        let record =
            br#"{"schema_version":2,"event_id":"BTC-USD:17:4","market":"BTC-USD","epoch":17,
            "sequence":4,"occurred_at_ns":1700000000000000000,"event_type":"trade_executed",
            "data":{"trade_id":1,"price":"81650.5","quantity":"0.1","taker_side":"buy",
            "maker_order_id":"a","taker_order_id":"b","maker_account_id":"m","taker_account_id":"t",
            "added_later":true}}"#;
        let observation = trade_observation(record, &source(), &markets())
            .unwrap()
            .unwrap();
        assert_eq!(observation.price(), dec!(81650.5));
        assert_eq!(
            observation.observed_at().unix_nanos(),
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn other_events_and_markets_are_skipped() {
        let accepted = br#"{"schema_version":2,"market":"BTC-USD","occurred_at_ns":1,
            "event_type":"order_accepted","data":{"order_id":"x"}}"#;
        assert_eq!(
            trade_observation(accepted, &source(), &markets()).unwrap(),
            None
        );
        let elsewhere = br#"{"schema_version":2,"market":"ETH-USD","occurred_at_ns":1,
            "event_type":"trade_executed","data":{"price":"1"}}"#;
        assert_eq!(
            trade_observation(elsewhere, &source(), &markets()).unwrap(),
            None
        );
    }

    #[test]
    fn unknown_schemas_are_refused() {
        let future = br#"{"schema_version":3,"market":"BTC-USD","occurred_at_ns":1,
            "event_type":"trade_executed","data":{"price":"1"}}"#;
        assert!(trade_observation(future, &source(), &markets()).is_err());
    }
}
