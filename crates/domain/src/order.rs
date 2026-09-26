//! Orders and the vocabulary that describes them.

use std::fmt;

use crate::{
    error::DomainError,
    ids::{AccountId, ClientOrderId, OrderId},
    market::MarketId,
    numeric::{Price, Quantity},
    time::Timestamp,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

/// How long an order may stay on the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    /// Rest on the book until filled or cancelled.
    GoodTilCancelled,
    /// Fill what is possible right away and cancel the remainder.
    ImmediateOrCancel,
    /// Fill the whole quantity right away or do nothing at all.
    FillOrKill,
}

impl TimeInForce {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GoodTilCancelled => "gtc",
            Self::ImmediateOrCancel => "ioc",
            Self::FillOrKill => "fok",
        }
    }
}

/// What happens when an order would trade against the same account.
///
/// Pattern: Strategy. The policy is chosen per order and the engine consults
/// it at the single point where a self trade is detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SelfTradePrevention {
    /// Cancel the rest of the incoming order and keep the resting one.
    #[default]
    CancelNewest,
    /// Cancel the resting order and keep matching the incoming one.
    CancelOldest,
}

impl SelfTradePrevention {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CancelNewest => "cancel_newest",
            Self::CancelOldest => "cancel_oldest",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderKind {
    Limit {
        price: Price,
        time_in_force: TimeInForce,
        post_only: bool,
    },
    /// Executes against the best available prices and never rests.
    Market,
}

impl OrderKind {
    /// Smart constructor that rejects contradictory flag combinations.
    pub fn limit(
        price: Price,
        time_in_force: TimeInForce,
        post_only: bool,
    ) -> Result<Self, DomainError> {
        if post_only && time_in_force != TimeInForce::GoodTilCancelled {
            return Err(DomainError::PostOnlyRequiresGtc);
        }
        Ok(Self::Limit {
            price,
            time_in_force,
            post_only,
        })
    }

    pub const fn limit_price(&self) -> Option<Price> {
        match self {
            Self::Limit { price, .. } => Some(*price),
            Self::Market => None,
        }
    }

    pub const fn time_in_force(&self) -> Option<TimeInForce> {
        match self {
            Self::Limit { time_in_force, .. } => Some(*time_in_force),
            Self::Market => None,
        }
    }

    pub const fn is_post_only(&self) -> bool {
        matches!(
            self,
            Self::Limit {
                post_only: true,
                ..
            }
        )
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Limit { .. } => "limit",
            Self::Market => "market",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderStatus {
    /// Accepted and about to be matched. Only seen in `OrderAccepted` events.
    New,
    /// Resting on the book with nothing filled.
    Open,
    /// Resting on the book with part of the quantity filled.
    PartiallyFilled,
    Filled,
    Cancelled,
}

impl OrderStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Open => "open",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelReason {
    /// The owner asked for it.
    Requested,
    /// Unfilled remainder of an immediate-or-cancel order.
    ImmediateOrCancel,
    /// Not enough liquidity to fill a fill-or-kill order in full.
    FillOrKill,
    /// Unfilled remainder of a market order.
    NoLiquidity,
    /// Removed to stop an account from trading with itself.
    SelfTradePrevention,
}

impl CancelReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::ImmediateOrCancel => "immediate_or_cancel",
            Self::FillOrKill => "fill_or_kill",
            Self::NoLiquidity => "no_liquidity",
            Self::SelfTradePrevention => "self_trade_prevention",
        }
    }
}

/// A validated request to place an order, ready for the matching engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrder {
    pub id: OrderId,
    pub account: AccountId,
    pub market: MarketId,
    pub side: Side,
    pub kind: OrderKind,
    pub quantity: Quantity,
    pub client_order_id: Option<ClientOrderId>,
    pub self_trade_prevention: SelfTradePrevention,
}

/// An order and its execution state.
///
/// Fields are private and only the engine can mutate them, which protects
/// the invariants `filled <= quantity` and "terminal orders never change".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    id: OrderId,
    account: AccountId,
    market: MarketId,
    side: Side,
    kind: OrderKind,
    quantity: Quantity,
    filled: Quantity,
    status: OrderStatus,
    cancel_reason: Option<CancelReason>,
    client_order_id: Option<ClientOrderId>,
    self_trade_prevention: SelfTradePrevention,
    created_at: Timestamp,
    updated_at: Timestamp,
}

impl Order {
    pub(crate) fn accept(new: NewOrder, now: Timestamp) -> Self {
        Self {
            id: new.id,
            account: new.account,
            market: new.market,
            side: new.side,
            kind: new.kind,
            quantity: new.quantity,
            filled: Quantity::ZERO,
            status: OrderStatus::New,
            cancel_reason: None,
            client_order_id: new.client_order_id,
            self_trade_prevention: new.self_trade_prevention,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn id(&self) -> OrderId {
        self.id
    }

    pub fn account(&self) -> &AccountId {
        &self.account
    }

    pub fn market(&self) -> &MarketId {
        &self.market
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn kind(&self) -> OrderKind {
        self.kind
    }

    pub fn limit_price(&self) -> Option<Price> {
        self.kind.limit_price()
    }

    pub fn quantity(&self) -> Quantity {
        self.quantity
    }

    pub fn filled(&self) -> Quantity {
        self.filled
    }

    pub fn remaining(&self) -> Quantity {
        self.quantity.saturating_sub(self.filled)
    }

    pub fn status(&self) -> OrderStatus {
        self.status
    }

    pub fn cancel_reason(&self) -> Option<CancelReason> {
        self.cancel_reason
    }

    pub fn client_order_id(&self) -> Option<&ClientOrderId> {
        self.client_order_id.as_ref()
    }

    pub fn self_trade_prevention(&self) -> SelfTradePrevention {
        self.self_trade_prevention
    }

    pub fn created_at(&self) -> Timestamp {
        self.created_at
    }

    pub fn updated_at(&self) -> Timestamp {
        self.updated_at
    }

    pub(crate) fn fill(&mut self, quantity: Quantity, now: Timestamp) -> Result<(), DomainError> {
        if self.status.is_terminal() {
            return Err(DomainError::InvariantViolation("fill on a terminal order"));
        }
        let filled = self.filled.checked_add(quantity)?;
        if filled > self.quantity {
            return Err(DomainError::InvariantViolation(
                "fill exceeds order quantity",
            ));
        }
        self.filled = filled;
        self.status = if filled == self.quantity {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        self.updated_at = now;
        Ok(())
    }

    /// Marks an order as resting. Partial fills keep their status.
    pub(crate) fn rest(&mut self) {
        if self.status == OrderStatus::New {
            self.status = OrderStatus::Open;
        }
    }

    pub(crate) fn cancel(&mut self, reason: CancelReason, now: Timestamp) {
        self.status = OrderStatus::Cancelled;
        self.cancel_reason = Some(reason);
        self.updated_at = now;
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn post_only_is_limited_to_good_til_cancelled() {
        let price = Price::new(dec!(10)).unwrap();
        assert!(OrderKind::limit(price, TimeInForce::GoodTilCancelled, true).is_ok());
        assert_eq!(
            OrderKind::limit(price, TimeInForce::ImmediateOrCancel, true),
            Err(DomainError::PostOnlyRequiresGtc)
        );
    }
}
