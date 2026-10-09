//! Shared state of the HTTP and WebSocket handlers.

use std::sync::{Arc, atomic::AtomicBool};

use orderflow_application::Clock;
use orderflow_marketdata::ports::CandleReader;
use tokio::sync::Semaphore;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{config::WsLimits, hub::PriceHub, metrics::PricefeedMetrics};

#[derive(Clone)]
pub struct AppState {
    pub hub: Arc<PriceHub>,
    /// Candle reads go through the cache tiers to the store.
    pub candles: Arc<dyn CandleReader>,
    pub metrics: Arc<PricefeedMetrics>,
    pub clock: Arc<dyn Clock>,
    pub ws: WsLimits,
    /// One permit per allowed WebSocket connection.
    pub connections: Arc<Semaphore>,
    /// Fires on shutdown so every WebSocket session closes with 1001.
    pub shutdown: CancellationToken,
    /// Running WebSocket sessions, awaited during shutdown.
    pub sessions: TaskTracker,
    /// False until this node can serve live prices.
    pub ready: Arc<AtomicBool>,
}
