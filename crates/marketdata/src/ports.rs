//! Ports: what the market data pipeline needs from the outside world.
//!
//! Pattern: Ports and Adapters. Postgres, Redis, in-memory test doubles and
//! the WebSocket hub all plug in here, and the pipeline never learns which
//! one it is talking to.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use orderflow_domain::{MarketId, Timestamp};
use rust_decimal::Decimal;

use crate::{
    candle::{Candle, CandleQuery},
    source::SourceId,
    update::PriceUpdate,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// Connection refused, timeout, failover. Worth retrying.
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    /// A stored value no longer parses. Retrying will not help.
    #[error("stored data is invalid: {0}")]
    Corrupt(String),
}

/// Receives every published update, in order.
///
/// Implementations must return immediately: the aggregator calls them on
/// its hot path, so anything slow belongs behind a queue.
pub trait UpdateSink: Send + Sync + 'static {
    fn accept(&self, update: &Arc<PriceUpdate>);
}

/// Durable history of published prices.
#[async_trait]
pub trait PriceStore: Send + Sync + 'static {
    /// Appends `updates` and merges `candles` (1 minute buckets) into the
    /// stored ones. Must be idempotent: a retried call stores nothing twice.
    async fn record(
        &self,
        updates: &[Arc<PriceUpdate>],
        candles: &[Candle],
    ) -> Result<(), StoreError>;
}

/// Read side of the candle history.
///
/// Pattern: Decorator. The cache tiers implement this trait and wrap the
/// next tier, so the HTTP handler holds one `CandleReader` and does not
/// know how many caches sit in front of the database.
#[async_trait]
pub trait CandleReader: Send + Sync + 'static {
    /// Candles in the query window, oldest first. Buckets without any
    /// update are absent rather than filled in.
    async fn candles(&self, query: &CandleQuery) -> Result<Arc<[Candle]>, StoreError>;
}

/// Carries updates from the ingest node to every edge node.
#[async_trait]
pub trait PriceBus: Send + Sync + 'static {
    async fn publish(&self, updates: &[Arc<PriceUpdate>]) -> Result<(), StoreError>;
}

/// A block an indexer has fully processed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: String,
    pub timestamp: Timestamp,
}

/// One decoded DEX swap. Token amounts are kept as the raw signed integers
/// from the chain, so no precision is lost to a decimal type that cannot
/// hold every `int256`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapRecord {
    pub pool: String,
    pub source: SourceId,
    pub market: MarketId,
    pub block_number: u64,
    pub block_hash: String,
    pub tx_hash: String,
    pub log_index: u64,
    /// Pool price after the swap, base in quote.
    pub price: Decimal,
    pub amount0: String,
    pub amount1: String,
    pub sqrt_price: String,
    pub block_time: Timestamp,
}

/// Pool state read from an account-based chain, at a slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolStateRecord {
    pub pool: String,
    pub source: SourceId,
    pub market: MarketId,
    pub slot: u64,
    pub price: Decimal,
    pub sqrt_price: String,
    pub observed_at: Timestamp,
}

/// Storage for chain indexers: decoded data plus the checkpoints needed to
/// resume after a restart and to undo blocks a reorg removed.
#[async_trait]
pub trait ChainStore: Send + Sync + 'static {
    /// The most recent checkpoints of `indexer`, newest first.
    async fn checkpoints(&self, indexer: &str, limit: usize) -> Result<Vec<BlockRef>, StoreError>;

    /// Stores `swaps` and records `block` as processed, atomically: after a
    /// crash either both are stored or neither is.
    async fn commit(
        &self,
        indexer: &str,
        swaps: &[SwapRecord],
        block: &BlockRef,
    ) -> Result<(), StoreError>;

    /// Deletes swaps and checkpoints of `indexer` above block `ancestor`
    /// and returns how many swaps were removed.
    async fn rollback(&self, indexer: &str, ancestor: u64) -> Result<u64, StoreError>;

    async fn record_pool_states(
        &self,
        indexer: &str,
        states: &[PoolStateRecord],
    ) -> Result<(), StoreError>;
}

/// Operational counters for the market data pipeline.
///
/// Every method has an empty default, so an adapter implements only what
/// it exports and new events never break existing adapters.
pub trait MarketDataMetrics: Send + Sync + 'static {
    fn observation_accepted(&self, _source: &SourceId) {}
    fn observation_rejected(&self, _source: &SourceId, _reason: &'static str) {}
    fn source_error(&self, _connector: &str, _kind: &'static str) {}
    fn index_published(&self, _update: &PriceUpdate) {}
    fn store_written(&self, _updates: usize, _elapsed: Duration) {}
    fn store_failed(&self) {}
    fn store_dropped(&self, _updates: usize) {}
    fn bus_published(&self, _updates: usize) {}
    fn bus_failed(&self) {}
    fn indexer_progress(&self, _indexer: &str, _head: u64, _cursor: u64) {}
    fn indexer_reorg(&self, _indexer: &str, _depth: u64) {}
    fn indexer_swaps(&self, _indexer: &str, _count: usize) {}
    /// A candle read answered by cache `tier` (`memory` or `redis`), or not.
    fn cache_lookup(&self, _tier: &'static str, _hit: bool) {}
}

/// Pattern: Null Object, for tests and tools that ignore metrics.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl MarketDataMetrics for NoopMetrics {}
