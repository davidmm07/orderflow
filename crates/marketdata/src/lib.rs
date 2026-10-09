//! Market data: turns many untrusted price observations into one index
//! price per market, and defines the ports the price pipeline needs.
//!
//! This crate is the core of the market data context. Like the trading
//! domain it performs no I/O. Exchanges, chains, Postgres, Redis and
//! WebSockets are adapters in other crates that implement the ports here.
//!
//! SOLID in this crate:
//! - SRP: `aggregate` decides what the price is, `PriceIndex` remembers the
//!   latest quote of every source, `Aggregator` decides when to publish, and
//!   the sinks decide how updates leave the process.
//! - OCP: a new exchange or chain is one more producer of `Observation`s.
//!   Nothing in this crate changes to support it.
//! - ISP: sources only see an `ObservationSender`, readers only see
//!   `CandleReader`, indexers only see `ChainStore`.
//! - DIP: the pipeline depends on the `PriceStore`, `PriceBus` and
//!   `UpdateSink` traits, never on a database or a broker.

// Prices are decimals end to end; binary floats would round them.
#![deny(clippy::float_arithmetic)]

mod aggregate;
mod backoff;
mod candle;
mod error;
mod index;
mod observation;
pub mod pipeline;
pub mod ports;
#[cfg(test)]
mod property_tests;
pub mod sinks;
mod source;
mod update;

pub use aggregate::{AggregationPolicy, Component, PriceStatus, QuoteState};
pub use backoff::Backoff;
pub use candle::{Candle, CandleInterval, CandleQuery, Revision, fold_minute_candles, rollup};
pub use error::MarketDataError;
pub use index::{MarketFeed, PriceIndex, Rejection};
pub use observation::Observation;
pub use source::{SourceId, SourceKind, SourceRole, SourceSpec};
pub use update::PriceUpdate;
