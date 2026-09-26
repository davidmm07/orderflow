//! Orderflow domain: the trading model and a deterministic matching engine.
//!
//! This crate is the innermost ring of the clean architecture. It knows
//! nothing about async runtimes, HTTP, Kafka or storage, so every rule here is
//! tested with plain function calls and can be replayed from a command log.
//!
//! SOLID in this crate:
//! - SRP: `OrderBook` stores resting liquidity, `MatchingEngine` applies the
//!   matching rules and `MarketSpec` validates trading parameters. Each type
//!   has one reason to change.
//! - OCP: order behavior is expressed as `OrderKind`, `TimeInForce` and
//!   `SelfTradePrevention` variants. Adding one is a local change, and the
//!   compiler points at every exhaustive `match` that must handle it.
//! - DIP: the engine never reads a clock or generates ids. Callers pass in a
//!   `Timestamp` and an `OrderId`, which keeps the core free of
//!   infrastructure and makes matching deterministic.

// Binary floating point cannot represent most decimal prices exactly, so any
// float arithmetic in the domain is treated as a bug.
#![deny(clippy::float_arithmetic)]

mod book;
mod engine;
mod error;
mod events;
mod ids;
mod market;
mod numeric;
mod order;
#[cfg(test)]
mod property_tests;
mod time;

pub use book::{BookSnapshot, LevelView, OrderBook};
pub use engine::{CancelOutcome, MatchOutcome, MatchingEngine};
pub use error::DomainError;
pub use events::{DomainEvent, EventPayload, Trade};
pub use ids::{AccountId, ClientOrderId, OrderId, TradeId};
pub use market::{MarketId, MarketSpec, MarketSpecBuilder};
pub use numeric::{Price, Quantity};
pub use order::{
    CancelReason, NewOrder, Order, OrderKind, OrderStatus, SelfTradePrevention, Side, TimeInForce,
};
pub use time::Timestamp;

/// Re-exported so that adapters build prices from the same decimal type.
pub use rust_decimal::Decimal;
