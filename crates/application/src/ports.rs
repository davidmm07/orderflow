//! Ports: the traits this layer needs from the outside world.
//!
//! Pattern: Ports and Adapters (Hexagonal). Each trait is a port owned by the
//! application; the infrastructure crate supplies adapters. Swapping the
//! in-memory repository for Postgres, or the log publisher for Kafka, is a
//! change in the composition root only.

use std::time::Duration;

use async_trait::async_trait;
use orderflow_domain::{DomainEvent, MarketId, Order, OrderId, Timestamp};

use crate::commands::{IdempotencyKey, OrderReceipt, PlaceOrderCommand, Reservation};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RepositoryError {
    #[error("storage backend unavailable: {0}")]
    Unavailable(String),
    #[error("storage capacity exhausted")]
    CapacityExhausted,
}

/// Publishing failures, split by whether a retry can help.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublishError {
    /// Broker unreachable, timeout, leader election. Worth retrying.
    #[error("transient publish failure: {0}")]
    Transient(String),
    /// Serialization bug, authorization failure, unknown topic. Retrying
    /// would only repeat the failure.
    #[error("permanent publish failure: {0}")]
    Permanent(String),
}

impl PublishError {
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

/// Read model of orders, kept up to date by the market actors.
///
/// Pattern: Repository. Callers see a collection of orders and never learn
/// how or where they are stored.
#[async_trait]
pub trait OrderRepository: Send + Sync + 'static {
    /// Stores the latest state of every given order, replacing older ones.
    async fn save(&self, orders: &[Order]) -> Result<(), RepositoryError>;

    async fn find(&self, id: OrderId) -> Result<Option<Order>, RepositoryError>;
}

/// Sink for domain events, such as a Kafka topic.
///
/// Contract: `publish` returns `Ok` only once the sink has accepted the whole
/// batch, and events of one market are handed over in sequence order.
/// Delivery is at least once, so consumers de-duplicate on
/// `(market, sequence)`.
#[async_trait]
pub trait EventPublisher: Send + Sync + 'static {
    async fn publish(&self, events: &[DomainEvent]) -> Result<(), PublishError>;
}

/// Stores the outcome of requests that carry an `Idempotency-Key`, so a
/// client retry after a timeout returns the original result instead of
/// placing a second order.
#[async_trait]
pub trait IdempotencyStore: Send + Sync + 'static {
    /// Atomically claims `key` for `command`, or reports what already owns it.
    async fn reserve(
        &self,
        key: &IdempotencyKey,
        command: &PlaceOrderCommand,
    ) -> Result<Reservation, RepositoryError>;

    /// Records the final result for a claimed key.
    async fn complete(
        &self,
        key: &IdempotencyKey,
        receipt: &OrderReceipt,
    ) -> Result<(), RepositoryError>;

    /// Frees a claimed key after a failure, so the client can retry with it.
    async fn release(&self, key: &IdempotencyKey) -> Result<(), RepositoryError>;
}

pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}

pub trait OrderIdGenerator: Send + Sync + 'static {
    fn next_id(&self) -> OrderId;
}

/// Operational counters the runtime reports.
pub trait Metrics: Send + Sync + 'static {
    fn order_processed(&self, market: &MarketId, outcome: &'static str);
    fn trades_executed(&self, market: &MarketId, count: usize);
    fn engine_latency(&self, market: &MarketId, elapsed: Duration);
    fn events_published(&self, count: usize);
    fn events_dropped(&self, count: usize);
}

/// Metrics sink that records nothing.
///
/// Pattern: Null Object. Tests and tools that do not care about metrics pass
/// this instead of an `Option`, so the runtime never branches on "metrics
/// enabled".
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn order_processed(&self, _: &MarketId, _: &'static str) {}
    fn trades_executed(&self, _: &MarketId, _: usize) {}
    fn engine_latency(&self, _: &MarketId, _: Duration) {}
    fn events_published(&self, _: usize) {}
    fn events_dropped(&self, _: usize) {}
}
