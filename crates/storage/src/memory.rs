//! In-memory storage for local runs without Postgres, and for tests.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use async_trait::async_trait;
use orderflow_application::Clock;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{
    Candle, CandleInterval, CandleQuery, PriceUpdate,
    ports::{
        BlockRef, CandleReader, ChainStore, PoolStateRecord, PriceStore, StoreError, SwapRecord,
    },
    rollup,
};

/// Bounds that keep a long local run from growing without limit.
const MAX_UPDATES: usize = 50_000;
const MAX_SWAPS: usize = 50_000;
const MAX_CANDLE_AGE_NANOS: u64 = 7 * 24 * 3_600 * 1_000_000_000;

#[derive(Default)]
struct Chain {
    checkpoints: BTreeMap<(String, u64), BlockRef>,
    swaps: VecDeque<(String, SwapRecord)>,
}

/// Implements every storage port in memory. Same merge rules as the
/// Postgres adapter, so the service behaves the same with either.
pub struct MemoryStore {
    updates: Mutex<VecDeque<Arc<PriceUpdate>>>,
    candles: Mutex<BTreeMap<(MarketId, Timestamp), Candle>>,
    chain: Mutex<Chain>,
    clock: Arc<dyn Clock>,
}

impl MemoryStore {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            updates: Mutex::default(),
            candles: Mutex::default(),
            chain: Mutex::default(),
            clock,
        }
    }

    /// Number of stored price updates.
    pub fn update_count(&self) -> usize {
        lock(&self.updates).len()
    }

    pub fn swap_count(&self) -> usize {
        lock(&self.chain).swaps.len()
    }
}

#[async_trait]
impl PriceStore for MemoryStore {
    async fn record(
        &self,
        updates: &[Arc<PriceUpdate>],
        candles: &[Candle],
    ) -> Result<(), StoreError> {
        {
            let mut stored = lock(&self.updates);
            stored.extend(updates.iter().cloned());
            while stored.len() > MAX_UPDATES {
                stored.pop_front();
            }
        }
        let horizon = self
            .clock
            .now()
            .unix_nanos()
            .saturating_sub(MAX_CANDLE_AGE_NANOS);
        let mut stored = lock(&self.candles);
        for candle in candles {
            stored
                .entry((candle.market.clone(), candle.start))
                .and_modify(|existing| existing.merge(candle))
                .or_insert_with(|| candle.clone());
        }
        stored.retain(|(_, start), _| start.unix_nanos() >= horizon);
        Ok(())
    }
}

#[async_trait]
impl CandleReader for MemoryStore {
    async fn candles(&self, query: &CandleQuery) -> Result<Arc<[Candle]>, StoreError> {
        let (from, to) = query.window(self.clock.now());
        let minutes: Vec<Candle> = lock(&self.candles)
            .range((query.market.clone(), from)..(query.market.clone(), to))
            .map(|(_, candle)| candle.clone())
            .collect();
        Ok(match query.interval {
            CandleInterval::OneMinute => Arc::from(minutes),
            interval => Arc::from(rollup(&minutes, interval)),
        })
    }
}

#[async_trait]
impl ChainStore for MemoryStore {
    async fn checkpoints(&self, indexer: &str, limit: usize) -> Result<Vec<BlockRef>, StoreError> {
        let chain = lock(&self.chain);
        Ok(chain
            .checkpoints
            .range((indexer.to_owned(), 0)..=(indexer.to_owned(), u64::MAX))
            .rev()
            .take(limit)
            .map(|(_, block)| block.clone())
            .collect())
    }

    async fn commit(
        &self,
        indexer: &str,
        swaps: &[SwapRecord],
        block: &BlockRef,
    ) -> Result<(), StoreError> {
        let mut chain = lock(&self.chain);
        for swap in swaps {
            let duplicate = chain.swaps.iter().any(|(owner, stored)| {
                owner == indexer
                    && stored.tx_hash == swap.tx_hash
                    && stored.log_index == swap.log_index
            });
            if !duplicate {
                chain.swaps.push_back((indexer.to_owned(), swap.clone()));
            }
        }
        while chain.swaps.len() > MAX_SWAPS {
            chain.swaps.pop_front();
        }
        chain
            .checkpoints
            .insert((indexer.to_owned(), block.number), block.clone());
        Ok(())
    }

    async fn rollback(&self, indexer: &str, ancestor: u64) -> Result<u64, StoreError> {
        let mut chain = lock(&self.chain);
        chain
            .checkpoints
            .retain(|(owner, number), _| owner != indexer || *number <= ancestor);
        let before = chain.swaps.len();
        chain
            .swaps
            .retain(|(owner, swap)| owner != indexer || swap.block_number <= ancestor);
        Ok((before - chain.swaps.len()) as u64)
    }

    async fn record_pool_states(
        &self,
        _indexer: &str,
        _states: &[PoolStateRecord],
    ) -> Result<(), StoreError> {
        // Pool states are history for analysis; a local run keeps only the
        // live prices they already produced.
        Ok(())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use orderflow_marketdata::{PriceStatus, Revision};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    const MINUTE: u64 = 60_000_000_000;

    struct FixedClock(u64);

    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            Timestamp::from_unix_nanos(self.0)
        }
    }

    fn market() -> MarketId {
        MarketId::parse("BTC-USD").unwrap()
    }

    fn candle(minute: u64, price: Decimal, sequence: u64) -> Candle {
        Candle::opening(
            market(),
            Timestamp::from_unix_nanos(minute * MINUTE),
            price,
            Revision { epoch: 1, sequence },
        )
    }

    fn update(sequence: u64) -> Arc<PriceUpdate> {
        Arc::new(PriceUpdate {
            market: market(),
            epoch: 1,
            sequence,
            price: Decimal::ONE,
            status: PriceStatus::Live,
            as_of: Timestamp::default(),
            published_at: Timestamp::default(),
            spread_bps: Decimal::ZERO,
            components: Vec::new(),
        })
    }

    #[tokio::test]
    async fn candles_merge_and_roll_up_like_postgres() {
        let store = MemoryStore::new(Arc::new(FixedClock(4 * MINUTE)));
        store
            .record(
                &[update(1)],
                &[candle(1, dec!(10), 1), candle(3, dec!(12), 2)],
            )
            .await
            .unwrap();
        // A late write for minute 1 with an older revision widens the range
        // but does not move the close.
        store.record(&[], &[candle(1, dec!(8), 0)]).await.unwrap();

        let query = CandleQuery {
            market: market(),
            interval: CandleInterval::FiveMinutes,
            limit: 2,
            end: None,
        };
        let candles = store.candles(&query).await.unwrap();
        assert_eq!(candles.len(), 1);
        let bar = &candles[0];
        assert_eq!(
            (bar.open, bar.high, bar.low, bar.close),
            (dec!(10), dec!(12), dec!(8), dec!(12))
        );
        assert_eq!(store.update_count(), 1);
    }

    #[tokio::test]
    async fn rollback_removes_only_blocks_above_the_ancestor() {
        let store = MemoryStore::new(Arc::new(FixedClock(0)));
        let block = |number| BlockRef {
            number,
            hash: format!("0x{number}"),
            timestamp: Timestamp::default(),
        };
        let swap = |number: u64| SwapRecord {
            pool: "0xpool".into(),
            source: orderflow_marketdata::SourceId::parse("uniswap_v3:test").unwrap(),
            market: market(),
            block_number: number,
            block_hash: format!("0x{number}"),
            tx_hash: format!("0xtx{number}"),
            log_index: 0,
            price: Decimal::ONE,
            amount0: "1".into(),
            amount1: "-1".into(),
            sqrt_price: "1".into(),
            block_time: Timestamp::default(),
        };
        store.commit("evm", &[swap(10)], &block(10)).await.unwrap();
        store
            .commit("evm", &[swap(20), swap(20)], &block(20))
            .await
            .unwrap();
        assert_eq!(store.swap_count(), 2, "duplicates are ignored");

        assert_eq!(store.rollback("evm", 15).await.unwrap(), 1);
        let checkpoints = store.checkpoints("evm", 10).await.unwrap();
        assert_eq!(
            checkpoints.iter().map(|b| b.number).collect::<Vec<_>>(),
            [10]
        );
    }
}
