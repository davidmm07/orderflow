//! In-memory adapters for development, tests and single node demos.

mod idempotency;
mod orders;

pub use idempotency::InMemoryIdempotencyStore;
pub use orders::InMemoryOrderRepository;
