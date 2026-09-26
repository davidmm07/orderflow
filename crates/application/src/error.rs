//! Application error type.

use orderflow_domain::{DomainError, MarketId, OrderId, OrderStatus};

use crate::ports::RepositoryError;

/// Failures a use case can report to its caller.
///
/// Domain errors are wrapped rather than flattened, so the API layer can
/// still map each rule violation to the field that caused it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApplicationError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("market {0} is not listed")]
    UnknownMarket(MarketId),
    #[error("order {0} was not found")]
    OrderNotFound(OrderId),
    #[error("order {id} is already {status}")]
    OrderNotOpen { id: OrderId, status: OrderStatus },
    #[error("idempotency key must be 1 to 64 visible ASCII characters")]
    InvalidIdempotencyKey,
    #[error("idempotency key was already used with a different request")]
    IdempotencyKeyReused,
    #[error("a request with this idempotency key is still being processed")]
    IdempotencyKeyInFlight,
    #[error("market {0} is at capacity, retry shortly")]
    Overloaded(MarketId),
    #[error("service unavailable: {0}")]
    Unavailable(&'static str),
    #[error(transparent)]
    Storage(#[from] RepositoryError),
}

impl ApplicationError {
    /// Stable, machine readable identifier for logs, metrics and API errors.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Domain(inner) => inner.code(),
            Self::UnknownMarket(_) => "unknown_market",
            Self::OrderNotFound(_) => "order_not_found",
            Self::OrderNotOpen { .. } => "order_not_open",
            Self::InvalidIdempotencyKey => "invalid_idempotency_key",
            Self::IdempotencyKeyReused => "idempotency_key_reused",
            Self::IdempotencyKeyInFlight => "idempotency_key_in_flight",
            Self::Overloaded(_) => "market_overloaded",
            Self::Unavailable(_) => "service_unavailable",
            Self::Storage(_) => "storage_failure",
        }
    }
}
