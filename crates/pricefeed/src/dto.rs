//! Public JSON shapes of the price API, shared by REST and WebSocket.
//!
//! Decimals are strings and times are RFC 3339 with milliseconds, the same
//! conventions as the exchange API. These types are the contract with
//! clients; the domain types can change without breaking it.

use chrono::{DateTime, SecondsFormat};
use orderflow_domain::Timestamp;
use orderflow_marketdata::{Candle, Component, PriceUpdate};
use serde::{Deserialize, Serialize};

pub fn rfc3339(at: Timestamp) -> String {
    let nanos = i64::try_from(at.unix_nanos()).unwrap_or(i64::MAX);
    DateTime::from_timestamp_nanos(nanos).to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Index price of one market.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PriceView {
    pub market: String,
    pub price: String,
    /// `live`, `degraded` or `stale`.
    pub status: String,
    pub as_of: String,
    pub published_at: String,
    pub epoch: u64,
    pub sequence: u64,
    pub sources_used: usize,
    pub spread_bps: String,
}

impl From<&PriceUpdate> for PriceView {
    fn from(update: &PriceUpdate) -> Self {
        Self {
            market: update.market.to_string(),
            price: update.price.to_string(),
            status: update.status.as_str().to_owned(),
            as_of: rfc3339(update.as_of),
            published_at: rfc3339(update.published_at),
            epoch: update.epoch,
            sequence: update.sequence,
            sources_used: update.sources_used(),
            spread_bps: update.spread_bps.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComponentView {
    pub source: String,
    pub kind: String,
    pub price: String,
    pub observed_at: String,
    /// `used`, `outlier`, `stale` or `monitor`.
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deviation_bps: Option<String>,
}

impl From<&Component> for ComponentView {
    fn from(component: &Component) -> Self {
        Self {
            source: component.source.to_string(),
            kind: component.kind.as_str().to_owned(),
            price: component.price.to_string(),
            observed_at: rfc3339(component.observed_at),
            state: component.state.as_str().to_owned(),
            deviation_bps: component.deviation_bps.map(|bps| bps.to_string()),
        }
    }
}

/// An index price with the per-source breakdown behind it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PriceDetailView {
    #[serde(flatten)]
    pub price: PriceView,
    pub components: Vec<ComponentView>,
}

impl From<&PriceUpdate> for PriceDetailView {
    fn from(update: &PriceUpdate) -> Self {
        Self {
            price: PriceView::from(update),
            components: update.components.iter().map(ComponentView::from).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PricesResponse {
    pub prices: Vec<PriceView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandleView {
    pub start: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
}

impl From<&Candle> for CandleView {
    fn from(candle: &Candle) -> Self {
        Self {
            start: rfc3339(candle.start),
            open: candle.open.to_string(),
            high: candle.high.to_string(),
            low: candle.low.to_string(),
            close: candle.close.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CandlesResponse {
    pub market: String,
    pub interval: String,
    pub candles: Vec<CandleView>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricesQuery {
    /// Comma separated market ids; all markets when absent.
    pub markets: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandlesQuery {
    pub interval: Option<String>,
    pub limit: Option<u32>,
    /// RFC 3339; candles starting before this time.
    pub end: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceView {
    pub source: String,
    pub kind: String,
    /// Markets this source currently quotes.
    pub markets: Vec<String>,
    /// Markets where its quote is part of the index right now.
    pub used_in: usize,
    pub outlier_in: usize,
    pub stale_in: usize,
    pub last_observed_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourcesResponse {
    pub sources: Vec<SourceView>,
}

/// Messages a WebSocket client sends.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientMessage {
    Subscribe { markets: Vec<String> },
    Unsubscribe { markets: Vec<String> },
    Ping,
}

/// Messages the server sends over WebSocket.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// First message on every connection.
    Welcome {
        markets: Vec<String>,
        max_subscriptions: usize,
        ping_interval_secs: u64,
    },
    /// Current subscriptions after a subscribe or unsubscribe.
    Subscribed {
        markets: Vec<String>,
    },
    Price(PriceView),
    Error {
        code: String,
        detail: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        markets: Vec<String>,
    },
    Pong,
}

impl ServerMessage {
    pub fn error(code: &str, detail: impl Into<String>, markets: Vec<String>) -> Self {
        Self::Error {
            code: code.to_owned(),
            detail: detail.into(),
            markets,
        }
    }

    pub fn to_json(&self) -> String {
        // Plain strings and integers only: serialization cannot fail.
        serde_json::to_string(self).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_are_rfc3339_with_milliseconds() {
        assert_eq!(
            rfc3339(Timestamp::from_unix_nanos(1_791_497_322_123_456_789)),
            "2026-10-08T22:08:42.123Z"
        );
    }

    #[test]
    fn client_messages_are_strict() {
        let subscribe: ClientMessage =
            serde_json::from_str(r#"{"op":"subscribe","markets":["BTC-USD"]}"#).unwrap();
        assert_eq!(
            subscribe,
            ClientMessage::Subscribe {
                markets: vec!["BTC-USD".into()]
            }
        );
        assert!(
            serde_json::from_str::<ClientMessage>(r#"{"op":"subscribe","markets":[],"x":1}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<ClientMessage>(r#"{"op":"trade"}"#).is_err());
    }

    #[test]
    fn price_messages_are_flat() {
        let message = ServerMessage::Price(PriceView {
            market: "BTC-USD".into(),
            price: "81650.5".into(),
            status: "live".into(),
            as_of: "2026-10-08T22:08:42.123Z".into(),
            published_at: "2026-10-08T22:08:42.250Z".into(),
            epoch: 1,
            sequence: 2,
            sources_used: 3,
            spread_bps: "1.2".into(),
        });
        let json = message.to_json();
        assert!(
            json.starts_with(r#"{"type":"price","market":"BTC-USD","price":"81650.5""#),
            "{json}"
        );
    }
}
