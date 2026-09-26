//! Facts emitted by the matching engine.

use crate::{
    ids::{AccountId, OrderId, TradeId},
    market::MarketId,
    numeric::{Price, Quantity},
    order::{CancelReason, Order, Side},
    time::Timestamp,
};

/// One execution between a resting (maker) and an incoming (taker) order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub id: TradeId,
    pub market: MarketId,
    /// Always the maker's price: the order that was on the book first set it.
    pub price: Price,
    pub quantity: Quantity,
    pub taker_side: Side,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub maker_account: AccountId,
    pub taker_account: AccountId,
    pub executed_at: Timestamp,
}

/// An immutable fact about a market, in the order it happened.
///
/// `sequence` is contiguous per market, starting at 1. Downstream consumers
/// use it to detect gaps and to discard duplicates after a redelivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainEvent {
    pub market: MarketId,
    pub sequence: u64,
    pub occurred_at: Timestamp,
    pub payload: EventPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventPayload {
    /// Snapshot of the order at the moment it passed validation.
    OrderAccepted(Order),
    TradeExecuted(Trade),
    OrderCancelled {
        order_id: OrderId,
        account: AccountId,
        reason: CancelReason,
        remaining: Quantity,
    },
    /// A stop order reached its trigger and is about to be matched.
    StopTriggered {
        order_id: OrderId,
        account: AccountId,
        stop_price: Price,
        /// Last trade price that fired the stop.
        trigger_price: Price,
    },
}

impl EventPayload {
    /// Stable event type name used as a topic header and schema discriminator.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::OrderAccepted(_) => "order_accepted",
            Self::TradeExecuted(_) => "trade_executed",
            Self::OrderCancelled { .. } => "order_cancelled",
            Self::StopTriggered { .. } => "stop_triggered",
        }
    }
}
