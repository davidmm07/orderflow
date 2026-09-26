//! Inputs and outputs of the use cases.

use orderflow_domain::{
    AccountId, ClientOrderId, MarketId, NewOrder, Order, OrderId, OrderKind, Quantity,
    SelfTradePrevention, Side, Trade,
};

use crate::error::ApplicationError;

/// A fully validated request to place an order.
///
/// Pattern: Command. The request is a value that can be compared, stored
/// next to an idempotency key and replayed, independent of the transport
/// that delivered it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceOrderCommand {
    pub account: AccountId,
    pub market: MarketId,
    pub side: Side,
    pub kind: OrderKind,
    pub quantity: Quantity,
    pub client_order_id: Option<ClientOrderId>,
    pub self_trade_prevention: SelfTradePrevention,
}

impl PlaceOrderCommand {
    pub(crate) fn into_new_order(self, id: OrderId) -> NewOrder {
        NewOrder {
            id,
            account: self.account,
            market: self.market,
            side: self.side,
            kind: self.kind,
            quantity: self.quantity,
            client_order_id: self.client_order_id,
            self_trade_prevention: self.self_trade_prevention,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOrderCommand {
    pub account: AccountId,
    pub market: MarketId,
    pub order_id: OrderId,
}

/// What a client gets back after placing an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderReceipt {
    pub order: Order,
    pub trades: Vec<Trade>,
}

/// Client supplied key that makes order placement safe to retry.
///
/// Keys are scoped to the account, so two clients can never collide on, or
/// read the result of, each other's keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    account: AccountId,
    value: String,
}

impl IdempotencyKey {
    pub const MAX_LEN: usize = 64;

    pub fn new(account: AccountId, raw: &str) -> Result<Self, ApplicationError> {
        let valid = !raw.is_empty()
            && raw.len() <= Self::MAX_LEN
            && raw.bytes().all(|b| b.is_ascii_graphic());
        if !valid {
            return Err(ApplicationError::InvalidIdempotencyKey);
        }
        Ok(Self {
            account,
            value: raw.to_owned(),
        })
    }

    pub fn account(&self) -> &AccountId {
        &self.account
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Answer of `IdempotencyStore::reserve`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reservation {
    /// The key was free and now belongs to this request.
    Reserved,
    /// Another request with the same key and body is still running.
    InFlight,
    /// The same request already completed with this receipt.
    Completed(OrderReceipt),
    /// The key was used for a different request body.
    Mismatch,
}
