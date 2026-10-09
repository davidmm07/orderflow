//! Storage adapters for market data.
//!
//! | Adapter             | Implements                                   | Role                           |
//! |---------------------|----------------------------------------------|--------------------------------|
//! | `PgStore`           | `PriceStore`, `CandleReader`, `ChainStore`   | Source of truth                |
//! | `RedisBus`          | `PriceBus`                                   | Fanout from ingest to edges    |
//! | `RedisCandleCache`  | `CandleReader` (decorator)                   | Cache shared by all edge nodes |
//! | `MemoryCandleCache` | `CandleReader` (decorator)                   | Per-process cache, coalescing  |
//! | `MemoryStore`       | `PriceStore`, `CandleReader`, `ChainStore`   | Local runs and tests           |
//!
//! A candle read on an edge node goes memory, then Redis, then Postgres.
//! Each tier absorbs the traffic the next one would see: per process, then
//! per cluster, and Postgres only answers once per key and expiry window.

mod cache;
mod memory;
mod postgres;
mod redis;
pub mod wire;

pub use cache::MemoryCandleCache;
pub use memory::MemoryStore;
pub use postgres::{LeaderLease, PgStore};
pub use redis::{
    LATEST_KEY, PRICE_CHANNEL, RedisBus, RedisCandleCache, candle_key, client, follow_prices,
};
