//! Shared state handed to every handler.

use std::sync::Arc;

use orderflow_application::{CancelOrder, OrderQueries, PlaceOrder};

use crate::{auth::Authenticator, rate_limit::RateLimiter};

/// Renders the metrics exposition. Supplied by the composition root so the
/// API stays unaware of which metrics backend is in use.
pub type MetricsRender = Arc<dyn Fn() -> String + Send + Sync>;

/// Everything handlers need. Cloning is cheap: every field is an `Arc`.
#[derive(Clone)]
pub struct AppState {
    pub place_order: Arc<PlaceOrder>,
    pub cancel_order: Arc<CancelOrder>,
    pub queries: Arc<OrderQueries>,
    pub authenticator: Arc<Authenticator>,
    pub rate_limiter: Arc<RateLimiter>,
    pub metrics: MetricsRender,
}
