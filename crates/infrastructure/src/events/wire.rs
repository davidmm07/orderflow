//! Versioned JSON schema for events leaving the process.
//!
//! The domain event types may change shape over time. Consumers only see
//! this explicit schema, mapped field by field, so a refactor inside the
//! domain cannot break a downstream service unless this file changes too.
//! Breaking changes bump `SCHEMA_VERSION`.

use orderflow_domain::{DomainEvent, EventPayload, Order, Trade};
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u16 = 1;

/// Envelope written to the event topic, one per domain event.
///
/// Decimals travel as strings, so consumers never need to parse a price as
/// a binary float.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRecord {
    pub schema_version: u16,
    /// `{market}:{sequence}`. Stable across redeliveries, so consumers can
    /// de-duplicate on it.
    pub event_id: String,
    pub market: String,
    pub sequence: u64,
    pub occurred_at_ns: u64,
    #[serde(flatten)]
    pub body: EventBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data", rename_all = "snake_case")]
pub enum EventBody {
    OrderAccepted(OrderRecord),
    TradeExecuted(TradeRecord),
    OrderCancelled(CancelRecord),
}

impl EventBody {
    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::OrderAccepted(_) => "order_accepted",
            Self::TradeExecuted(_) => "trade_executed",
            Self::OrderCancelled(_) => "order_cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRecord {
    pub order_id: String,
    pub account_id: String,
    pub client_order_id: Option<String>,
    pub side: String,
    pub order_type: String,
    pub price: Option<String>,
    pub time_in_force: Option<String>,
    pub post_only: bool,
    pub quantity: String,
    pub self_trade_prevention: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub taker_side: String,
    pub maker_order_id: String,
    pub taker_order_id: String,
    pub maker_account_id: String,
    pub taker_account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRecord {
    pub order_id: String,
    pub account_id: String,
    pub reason: String,
    pub remaining_quantity: String,
}

impl From<&DomainEvent> for EventRecord {
    fn from(event: &DomainEvent) -> Self {
        let body = match &event.payload {
            EventPayload::OrderAccepted(order) => EventBody::OrderAccepted(order_record(order)),
            EventPayload::TradeExecuted(trade) => EventBody::TradeExecuted(trade_record(trade)),
            EventPayload::OrderCancelled {
                order_id,
                account,
                reason,
                remaining,
            } => EventBody::OrderCancelled(CancelRecord {
                order_id: order_id.to_string(),
                account_id: account.to_string(),
                reason: reason.as_str().to_owned(),
                remaining_quantity: remaining.to_string(),
            }),
        };
        Self {
            schema_version: SCHEMA_VERSION,
            event_id: format!("{}:{}", event.market, event.sequence),
            market: event.market.to_string(),
            sequence: event.sequence,
            occurred_at_ns: event.occurred_at.unix_nanos(),
            body,
        }
    }
}

fn order_record(order: &Order) -> OrderRecord {
    let kind = order.kind();
    OrderRecord {
        order_id: order.id().to_string(),
        account_id: order.account().to_string(),
        client_order_id: order.client_order_id().map(|id| id.as_str().to_owned()),
        side: order.side().as_str().to_owned(),
        order_type: kind.as_str().to_owned(),
        price: kind.limit_price().map(|price| price.to_string()),
        time_in_force: kind.time_in_force().map(|tif| tif.as_str().to_owned()),
        post_only: kind.is_post_only(),
        quantity: order.quantity().to_string(),
        self_trade_prevention: order.self_trade_prevention().as_str().to_owned(),
    }
}

fn trade_record(trade: &Trade) -> TradeRecord {
    TradeRecord {
        trade_id: trade.id.value(),
        price: trade.price.to_string(),
        quantity: trade.quantity.to_string(),
        taker_side: trade.taker_side.as_str().to_owned(),
        maker_order_id: trade.maker_order_id.to_string(),
        taker_order_id: trade.taker_order_id.to_string(),
        maker_account_id: trade.maker_account.to_string(),
        taker_account_id: trade.taker_account.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use orderflow_domain::{
        AccountId, CancelReason, Decimal, MarketId, OrderId, Quantity, Timestamp,
    };
    use serde_json::json;

    use super::*;

    #[test]
    fn cancel_event_has_a_stable_json_shape() {
        let event = DomainEvent {
            market: MarketId::parse("BTC-USD").unwrap(),
            sequence: 42,
            occurred_at: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
            payload: EventPayload::OrderCancelled {
                order_id: OrderId::from_uuid(uuid::Uuid::nil()),
                account: AccountId::parse("alice").unwrap(),
                reason: CancelReason::Requested,
                remaining: Quantity::positive(Decimal::new(15, 1)).unwrap(),
            },
        };

        let value = serde_json::to_value(EventRecord::from(&event)).unwrap();
        assert_eq!(
            value,
            json!({
                "schema_version": 1,
                "event_id": "BTC-USD:42",
                "market": "BTC-USD",
                "sequence": 42,
                "occurred_at_ns": 1_700_000_000_000_000_000u64,
                "event_type": "order_cancelled",
                "data": {
                    "order_id": "00000000-0000-0000-0000-000000000000",
                    "account_id": "alice",
                    "reason": "requested",
                    "remaining_quantity": "1.5"
                }
            })
        );

        let back: EventRecord = serde_json::from_value(value).unwrap();
        assert_eq!(back, EventRecord::from(&event));
    }
}
