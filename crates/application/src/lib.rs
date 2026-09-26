//! Orderflow application layer: use cases, ports and the market runtime.
//!
//! This ring orchestrates the domain. It is in charge of ordering,
//! concurrency and idempotency, while the domain holds the trading rules.
//! It depends only on `orderflow-domain` and on abstract ports; the
//! concrete adapters are plugged in by the composition root in the server
//! binary.
//!
//! SOLID in this crate:
//! - SRP: each use case (`PlaceOrder`, `CancelOrder`, `OrderQueries`) is its
//!   own type with its own dependencies.
//! - ISP: ports are small, single purpose traits. The market actor depends
//!   on a `Clock` and an `OrderRepository` and on nothing wider.
//! - DIP: use cases depend on traits defined here. Infrastructure implements
//!   them, so the dependency arrows point inwards.
//! - LSP: every port is exercised in tests with fakes, and every adapter must
//!   honor the same documented contract, for example "publish returns only
//!   after the batch is accepted by the sink".

mod commands;
mod error;
mod market;
mod outbox;
mod ports;
mod registry;
mod use_cases;

pub use commands::{
    CancelOrderCommand, IdempotencyKey, OrderReceipt, PlaceOrderCommand, Reservation,
};
pub use error::ApplicationError;
pub use market::{MarketDeps, MarketHandle, spawn_market};
pub use outbox::{EventDispatcher, Outbox, OutboxReceiver, outbox};
pub use ports::{
    Clock, EventPublisher, IdempotencyStore, Metrics, NoopMetrics, OrderIdGenerator,
    OrderRepository, PublishError, RepositoryError,
};
pub use registry::MarketRegistry;
pub use use_cases::{CancelOrder, OrderQueries, PlaceOrder, Placement};
