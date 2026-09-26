//! Orderflow infrastructure: adapters that implement the application ports.
//!
//! This is the outermost library ring. It is the only place that knows about
//! concrete technologies (Kafka, wall clocks, UUID versions, the Prometheus
//! text format). Nothing in the domain or application crates depends on it;
//! the server binary wires it in.
//!
//! SOLID in this crate:
//! - OCP: new behavior is added by wrapping existing adapters. Retries,
//!   fan-out and new sinks are separate `EventPublisher` implementations
//!   composed at startup.
//! - LSP: every adapter keeps the contract of its port, so the in-memory
//!   repository used in tests and a database-backed one are interchangeable.
//! - DIP: adapters depend on the application's traits, never the reverse.

mod clock;
mod events;
mod ids;
mod memory;
mod metrics;
mod sync;

pub use clock::MonotonicClock;
#[cfg(feature = "kafka")]
pub use events::KafkaEventPublisher;
pub use events::{
    BroadcastPublisher, EventRecord, FanoutPublisher, LoggingPublisher, RetryPolicy,
    RetryingPublisher,
};
pub use ids::UuidV7Generator;
pub use memory::{InMemoryIdempotencyStore, InMemoryOrderRepository};
pub use metrics::PrometheusMetrics;
