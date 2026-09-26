//! Request handlers, one module per resource.
//!
//! Handlers stay thin: extract, validate, call one use case, map the
//! result. Any logic beyond that belongs in the application layer.

pub mod markets;
pub mod ops;
pub mod orders;

use orderflow_domain::{MarketId, OrderId};

use crate::error::ApiError;

fn market_id(raw: &str) -> Result<MarketId, ApiError> {
    MarketId::parse(raw).map_err(ApiError::from)
}

fn order_id(raw: &str) -> Result<OrderId, ApiError> {
    OrderId::parse(raw).map_err(ApiError::from)
}
