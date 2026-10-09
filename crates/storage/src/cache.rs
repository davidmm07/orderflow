//! First-tier, in-process cache for candle reads.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use moka::future::Cache;
use orderflow_application::Clock;
use orderflow_marketdata::{
    Candle, CandleQuery,
    ports::{CandleReader, MarketDataMetrics, StoreError},
};

/// In-memory cache with request coalescing in front of a `CandleReader`.
///
/// Pattern: Decorator, plus Single Flight. When a chart goes viral, ten
/// thousand clients ask for the same "last 100 one minute candles" within
/// a second. The first miss starts one load; every concurrent request for
/// the same key waits for that load instead of starting its own, so the
/// next tier sees one query per key and expiry window, whatever the number
/// of clients.
///
/// Two expiry classes: a window that includes the current bucket can still
/// change and lives `hot_ttl` (about a second), a window entirely in the
/// past is settled and lives `settled_ttl`.
pub struct MemoryCandleCache {
    inner: Arc<dyn CandleReader>,
    hot: Cache<CandleQuery, Arc<[Candle]>>,
    settled: Cache<CandleQuery, Arc<[Candle]>>,
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MarketDataMetrics>,
}

impl MemoryCandleCache {
    pub fn new(
        inner: Arc<dyn CandleReader>,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn MarketDataMetrics>,
        max_entries: u64,
        hot_ttl: Duration,
        settled_ttl: Duration,
    ) -> Self {
        let cache = |ttl| {
            Cache::builder()
                .max_capacity(max_entries)
                .time_to_live(ttl)
                .build()
        };
        Self {
            inner,
            hot: cache(hot_ttl),
            settled: cache(settled_ttl),
            clock,
            metrics,
        }
    }
}

#[async_trait]
impl CandleReader for MemoryCandleCache {
    async fn candles(&self, query: &CandleQuery) -> Result<Arc<[Candle]>, StoreError> {
        let cache = if query.is_settled(self.clock.now()) {
            &self.settled
        } else {
            &self.hot
        };
        let loaded = AtomicBool::new(false);
        let result = cache
            .try_get_with(query.clone(), async {
                loaded.store(true, Ordering::Relaxed);
                self.inner.candles(query).await
            })
            .await
            .map_err(|error| (*error).clone());
        self.metrics
            .cache_lookup("memory", !loaded.load(Ordering::Relaxed));
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use orderflow_domain::{MarketId, Timestamp};
    use orderflow_marketdata::{CandleInterval, ports::NoopMetrics};

    use super::*;

    /// Counts loads and takes a while, like a database under load.
    #[derive(Default)]
    struct SlowStore {
        loads: AtomicUsize,
        fail: AtomicBool,
    }

    #[async_trait]
    impl CandleReader for SlowStore {
        async fn candles(&self, _: &CandleQuery) -> Result<Arc<[Candle]>, StoreError> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            if self.fail.load(Ordering::SeqCst) {
                return Err(StoreError::Unavailable("down".into()));
            }
            Ok(Arc::from(Vec::new()))
        }
    }

    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            Timestamp::from_unix_nanos(3_600_000_000_000)
        }
    }

    fn query(limit: u32) -> CandleQuery {
        CandleQuery {
            market: MarketId::parse("BTC-USD").unwrap(),
            interval: CandleInterval::OneMinute,
            limit,
            end: None,
        }
    }

    fn cache(store: Arc<SlowStore>) -> Arc<MemoryCandleCache> {
        Arc::new(MemoryCandleCache::new(
            store,
            Arc::new(FixedClock),
            Arc::new(NoopMetrics),
            1_000,
            Duration::from_secs(1),
            Duration::from_secs(60),
        ))
    }

    #[tokio::test]
    async fn a_burst_of_identical_reads_costs_one_load() {
        let store = Arc::new(SlowStore::default());
        let cache = cache(store.clone());
        let readers: Vec<_> = (0..500)
            .map(|_| {
                let cache = cache.clone();
                tokio::spawn(async move { cache.candles(&query(100)).await })
            })
            .collect();
        for reader in readers {
            reader.await.unwrap().unwrap();
        }
        assert_eq!(store.loads.load(Ordering::SeqCst), 1);

        cache.candles(&query(50)).await.unwrap();
        assert_eq!(
            store.loads.load(Ordering::SeqCst),
            2,
            "a different key loads separately"
        );
    }

    #[tokio::test]
    async fn errors_are_returned_and_not_cached() {
        let store = Arc::new(SlowStore::default());
        store.fail.store(true, Ordering::SeqCst);
        let cache = cache(store.clone());
        assert!(cache.candles(&query(10)).await.is_err());
        store.fail.store(false, Ordering::SeqCst);
        assert!(cache.candles(&query(10)).await.is_ok());
        assert_eq!(store.loads.load(Ordering::SeqCst), 2);
    }
}
