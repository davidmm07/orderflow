//! Postgres adapter for the price history, candles and chain indexers.
//!
//! Write path: every call is one round trip per table. Rows go in as
//! parallel arrays expanded with `UNNEST`, so a batch of 500 updates is a
//! single statement with nine parameters instead of 500 statements or a
//! 4,500 parameter `VALUES` list. Statements are static strings, which lets
//! sqlx prepare them once per connection and reuse the plan.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orderflow_application::Clock;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{
    Candle, CandleQuery, PriceUpdate, Revision,
    ports::{
        BlockRef, CandleReader, ChainStore, PoolStateRecord, PriceStore, StoreError, SwapRecord,
    },
};
use rust_decimal::Decimal;
use sqlx::{
    Connection, PgConnection, PgPool, Row,
    postgres::{PgPoolOptions, PgRow},
};

use crate::wire::component_records;

/// Checkpoints kept per indexer; older ones are pruned on commit.
const KEPT_CHECKPOINTS: i64 = 256;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Pooled Postgres access implementing every storage port.
#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
    clock: Arc<dyn Clock>,
}

impl PgStore {
    /// Connects a pool of at most `max_connections`. The database URL holds
    /// a password, so connection errors are reported without it.
    pub async fn connect(
        url: &str,
        max_connections: u32,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(3))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(1_800))
            .connect(url)
            .await
            .map_err(unavailable)?;
        Ok(Self { pool, clock })
    }

    /// Applies pending migrations. Safe to run from several instances:
    /// sqlx serializes them with an advisory lock.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|error| StoreError::Unavailable(error.to_string()))
    }

    /// Cheap liveness probe for readiness checks.
    pub async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(unavailable)
    }
}

#[async_trait]
impl PriceStore for PgStore {
    async fn record(
        &self,
        updates: &[Arc<PriceUpdate>],
        candles: &[Candle],
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;

        if !updates.is_empty() {
            let mut columns = UpdateColumns::with_capacity(updates.len());
            for update in updates {
                columns.push(update)?;
            }
            sqlx::query(
                "INSERT INTO index_prices
                     (market, epoch, sequence, price, status, as_of, published_at, spread_bps, components)
                 SELECT * FROM UNNEST($1::text[], $2::int8[], $3::int8[], $4::numeric[], $5::text[],
                                      $6::timestamptz[], $7::timestamptz[], $8::numeric[], $9::text[]::jsonb[])
                 ON CONFLICT DO NOTHING",
            )
            .bind(columns.market)
            .bind(columns.epoch)
            .bind(columns.sequence)
            .bind(columns.price)
            .bind(columns.status)
            .bind(columns.as_of)
            .bind(columns.published_at)
            .bind(columns.spread_bps)
            .bind(columns.components)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }

        if !candles.is_empty() {
            let mut columns = CandleColumns::with_capacity(candles.len());
            for candle in candles {
                columns.push(candle)?;
            }
            // Merging keeps the first open, widens high and low, and takes
            // the close of whichever write has the newer revision.
            sqlx::query(
                "INSERT INTO candles_1m AS c
                     (market, bucket, open, high, low, close, close_epoch, close_sequence)
                 SELECT * FROM UNNEST($1::text[], $2::timestamptz[], $3::numeric[], $4::numeric[],
                                      $5::numeric[], $6::numeric[], $7::int8[], $8::int8[])
                 ON CONFLICT (market, bucket) DO UPDATE SET
                     high = GREATEST(c.high, EXCLUDED.high),
                     low = LEAST(c.low, EXCLUDED.low),
                     close = CASE WHEN (EXCLUDED.close_epoch, EXCLUDED.close_sequence) > (c.close_epoch, c.close_sequence)
                                  THEN EXCLUDED.close ELSE c.close END,
                     close_epoch = CASE WHEN (EXCLUDED.close_epoch, EXCLUDED.close_sequence) > (c.close_epoch, c.close_sequence)
                                        THEN EXCLUDED.close_epoch ELSE c.close_epoch END,
                     close_sequence = CASE WHEN (EXCLUDED.close_epoch, EXCLUDED.close_sequence) > (c.close_epoch, c.close_sequence)
                                           THEN EXCLUDED.close_sequence ELSE c.close_sequence END",
            )
            .bind(columns.market)
            .bind(columns.bucket)
            .bind(columns.open)
            .bind(columns.high)
            .bind(columns.low)
            .bind(columns.close)
            .bind(columns.epoch)
            .bind(columns.sequence)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }

        tx.commit().await.map_err(unavailable)
    }
}

#[async_trait]
impl CandleReader for PgStore {
    async fn candles(&self, query: &CandleQuery) -> Result<Arc<[Candle]>, StoreError> {
        let (from, to) = query.window(self.clock.now());
        let step = i64::try_from(query.interval.seconds()).map_err(corrupt)?;
        // date_bin with a Unix epoch origin buckets exactly like
        // `CandleInterval::start_of`. The 1 minute rows of each bucket are
        // folded with ordered array_agg: first open, last close.
        let rows = sqlx::query(
            "SELECT date_bin($2 * interval '1 second', bucket, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS start,
                    (array_agg(open ORDER BY bucket))[1] AS open,
                    max(high) AS high,
                    min(low) AS low,
                    (array_agg(close ORDER BY bucket DESC))[1] AS close,
                    (array_agg(close_epoch ORDER BY bucket DESC))[1] AS close_epoch,
                    (array_agg(close_sequence ORDER BY bucket DESC))[1] AS close_sequence
             FROM candles_1m
             WHERE market = $1 AND bucket >= $3 AND bucket < $4
             GROUP BY start
             ORDER BY start",
        )
        .bind(query.market.as_str())
        .bind(step)
        .bind(time(from)?)
        .bind(time(to)?)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        rows.iter()
            .map(|row| candle(&query.market, row))
            .collect::<Result<Vec<_>, _>>()
            .map(Arc::from)
    }
}

#[async_trait]
impl ChainStore for PgStore {
    async fn checkpoints(&self, indexer: &str, limit: usize) -> Result<Vec<BlockRef>, StoreError> {
        let rows = sqlx::query(
            "SELECT block_number, block_hash, block_time FROM indexer_checkpoints
             WHERE indexer = $1 ORDER BY block_number DESC LIMIT $2",
        )
        .bind(indexer)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        rows.iter()
            .map(|row| {
                Ok(BlockRef {
                    number: unsigned(row.try_get("block_number").map_err(corrupt)?)?,
                    hash: row.try_get("block_hash").map_err(corrupt)?,
                    timestamp: timestamp(row.try_get("block_time").map_err(corrupt)?),
                })
            })
            .collect()
    }

    async fn commit(
        &self,
        indexer: &str,
        swaps: &[SwapRecord],
        block: &BlockRef,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        if !swaps.is_empty() {
            let mut columns = SwapColumns::with_capacity(swaps.len());
            for swap in swaps {
                columns.push(indexer, swap)?;
            }
            sqlx::query(
                "INSERT INTO dex_swaps
                     (indexer, tx_hash, log_index, pool, source, market, block_number, block_hash,
                      block_time, price, amount0, amount1, sqrt_price)
                 SELECT * FROM UNNEST($1::text[], $2::text[], $3::int8[], $4::text[], $5::text[], $6::text[],
                                      $7::int8[], $8::text[], $9::timestamptz[], $10::numeric[],
                                      $11::text[]::numeric[], $12::text[]::numeric[], $13::text[]::numeric[])
                 ON CONFLICT DO NOTHING",
            )
            .bind(columns.indexer)
            .bind(columns.tx_hash)
            .bind(columns.log_index)
            .bind(columns.pool)
            .bind(columns.source)
            .bind(columns.market)
            .bind(columns.block_number)
            .bind(columns.block_hash)
            .bind(columns.block_time)
            .bind(columns.price)
            .bind(columns.amount0)
            .bind(columns.amount1)
            .bind(columns.sqrt_price)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        sqlx::query(
            "INSERT INTO indexer_checkpoints (indexer, block_number, block_hash, block_time)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (indexer, block_number)
             DO UPDATE SET block_hash = EXCLUDED.block_hash, block_time = EXCLUDED.block_time",
        )
        .bind(indexer)
        .bind(signed(block.number)?)
        .bind(&block.hash)
        .bind(time(block.timestamp)?)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            "DELETE FROM indexer_checkpoints
             WHERE indexer = $1 AND block_number < (
                 SELECT block_number FROM indexer_checkpoints
                 WHERE indexer = $1 ORDER BY block_number DESC OFFSET $2 LIMIT 1)",
        )
        .bind(indexer)
        .bind(KEPT_CHECKPOINTS - 1)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }

    async fn rollback(&self, indexer: &str, ancestor: u64) -> Result<u64, StoreError> {
        let ancestor = signed(ancestor)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let removed = sqlx::query("DELETE FROM dex_swaps WHERE indexer = $1 AND block_number > $2")
            .bind(indexer)
            .bind(ancestor)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?
            .rows_affected();
        sqlx::query("DELETE FROM indexer_checkpoints WHERE indexer = $1 AND block_number > $2")
            .bind(indexer)
            .bind(ancestor)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(removed)
    }

    async fn record_pool_states(
        &self,
        indexer: &str,
        states: &[PoolStateRecord],
    ) -> Result<(), StoreError> {
        if states.is_empty() {
            return Ok(());
        }
        let mut columns = StateColumns::with_capacity(states.len());
        for state in states {
            columns.push(indexer, state)?;
        }
        sqlx::query(
            "INSERT INTO dex_pool_states (indexer, pool, slot, source, market, price, sqrt_price, observed_at)
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::int8[], $4::text[], $5::text[],
                                  $6::numeric[], $7::text[]::numeric[], $8::timestamptz[])
             ON CONFLICT DO NOTHING",
        )
        .bind(columns.indexer)
        .bind(columns.pool)
        .bind(columns.slot)
        .bind(columns.source)
        .bind(columns.market)
        .bind(columns.price)
        .bind(columns.sqrt_price)
        .bind(columns.observed_at)
        .execute(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(())
    }
}

/// Exclusive right to run ingestion, held as a Postgres session advisory
/// lock.
///
/// Pattern: Leader election with a lease. Several ingest tasks can run
/// during a rolling deploy, but only the lock holder ingests, so the
/// single-writer design of the aggregator holds across processes. If the
/// holder dies, its session ends, Postgres releases the lock and a standby
/// takes over. All writes are idempotent, so the short overlap possible
/// while a dead session is detected cannot corrupt anything.
pub struct LeaderLease {
    connection: PgConnection,
}

impl LeaderLease {
    /// Lock id: the bytes of "orderflow" read as a number.
    pub const KEY: i64 = 0x6f72_6465_7266_6c6f;

    /// Tries to take the lock once. `None` means another instance holds it.
    pub async fn try_acquire(url: &str) -> Result<Option<Self>, StoreError> {
        let mut connection = PgConnection::connect(url).await.map_err(unavailable)?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(Self::KEY)
            .fetch_one(&mut connection)
            .await
            .map_err(unavailable)?;
        Ok(acquired.then_some(Self { connection }))
    }

    /// Resolves once the session holding the lock is gone, which means the
    /// lock is gone too and ingestion must stop.
    pub async fn lost(mut self, check_every: Duration) -> StoreError {
        loop {
            tokio::time::sleep(check_every).await;
            let check = tokio::time::timeout(
                check_every,
                sqlx::query("SELECT 1").execute(&mut self.connection),
            )
            .await;
            match check {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => return unavailable(error),
                Err(_) => return StoreError::Unavailable("leader session check timed out".into()),
            }
        }
    }
}

#[derive(Default)]
struct UpdateColumns {
    market: Vec<String>,
    epoch: Vec<i64>,
    sequence: Vec<i64>,
    price: Vec<Decimal>,
    status: Vec<String>,
    as_of: Vec<DateTime<Utc>>,
    published_at: Vec<DateTime<Utc>>,
    spread_bps: Vec<Decimal>,
    components: Vec<String>,
}

impl UpdateColumns {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            market: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    fn push(&mut self, update: &PriceUpdate) -> Result<(), StoreError> {
        self.market.push(update.market.to_string());
        self.epoch.push(signed(update.epoch)?);
        self.sequence.push(signed(update.sequence)?);
        self.price.push(update.price);
        self.status.push(update.status.as_str().to_owned());
        self.as_of.push(time(update.as_of)?);
        self.published_at.push(time(update.published_at)?);
        self.spread_bps.push(update.spread_bps);
        let components =
            serde_json::to_string(&component_records(&update.components)).map_err(corrupt)?;
        self.components.push(components);
        Ok(())
    }
}

#[derive(Default)]
struct CandleColumns {
    market: Vec<String>,
    bucket: Vec<DateTime<Utc>>,
    open: Vec<Decimal>,
    high: Vec<Decimal>,
    low: Vec<Decimal>,
    close: Vec<Decimal>,
    epoch: Vec<i64>,
    sequence: Vec<i64>,
}

impl CandleColumns {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            market: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    fn push(&mut self, candle: &Candle) -> Result<(), StoreError> {
        self.market.push(candle.market.to_string());
        self.bucket.push(time(candle.start)?);
        self.open.push(candle.open);
        self.high.push(candle.high);
        self.low.push(candle.low);
        self.close.push(candle.close);
        self.epoch.push(signed(candle.revision.epoch)?);
        self.sequence.push(signed(candle.revision.sequence)?);
        Ok(())
    }
}

#[derive(Default)]
struct SwapColumns {
    indexer: Vec<String>,
    tx_hash: Vec<String>,
    log_index: Vec<i64>,
    pool: Vec<String>,
    source: Vec<String>,
    market: Vec<String>,
    block_number: Vec<i64>,
    block_hash: Vec<String>,
    block_time: Vec<DateTime<Utc>>,
    price: Vec<Decimal>,
    amount0: Vec<String>,
    amount1: Vec<String>,
    sqrt_price: Vec<String>,
}

impl SwapColumns {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            tx_hash: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    fn push(&mut self, indexer: &str, swap: &SwapRecord) -> Result<(), StoreError> {
        self.indexer.push(indexer.to_owned());
        self.tx_hash.push(swap.tx_hash.clone());
        self.log_index.push(signed(swap.log_index)?);
        self.pool.push(swap.pool.clone());
        self.source.push(swap.source.to_string());
        self.market.push(swap.market.to_string());
        self.block_number.push(signed(swap.block_number)?);
        self.block_hash.push(swap.block_hash.clone());
        self.block_time.push(time(swap.block_time)?);
        self.price.push(swap.price);
        self.amount0.push(swap.amount0.clone());
        self.amount1.push(swap.amount1.clone());
        self.sqrt_price.push(swap.sqrt_price.clone());
        Ok(())
    }
}

#[derive(Default)]
struct StateColumns {
    indexer: Vec<String>,
    pool: Vec<String>,
    slot: Vec<i64>,
    source: Vec<String>,
    market: Vec<String>,
    price: Vec<Decimal>,
    sqrt_price: Vec<String>,
    observed_at: Vec<DateTime<Utc>>,
}

impl StateColumns {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            pool: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    fn push(&mut self, indexer: &str, state: &PoolStateRecord) -> Result<(), StoreError> {
        self.indexer.push(indexer.to_owned());
        self.pool.push(state.pool.clone());
        self.slot.push(signed(state.slot)?);
        self.source.push(state.source.to_string());
        self.market.push(state.market.to_string());
        self.price.push(state.price);
        self.sqrt_price.push(state.sqrt_price.clone());
        self.observed_at.push(time(state.observed_at)?);
        Ok(())
    }
}

fn candle(market: &MarketId, row: &PgRow) -> Result<Candle, StoreError> {
    Ok(Candle {
        market: market.clone(),
        start: timestamp(row.try_get("start").map_err(corrupt)?),
        open: row.try_get("open").map_err(corrupt)?,
        high: row.try_get("high").map_err(corrupt)?,
        low: row.try_get("low").map_err(corrupt)?,
        close: row.try_get("close").map_err(corrupt)?,
        revision: Revision {
            epoch: unsigned(row.try_get("close_epoch").map_err(corrupt)?)?,
            sequence: unsigned(row.try_get("close_sequence").map_err(corrupt)?)?,
        },
    })
}

fn time(at: Timestamp) -> Result<DateTime<Utc>, StoreError> {
    let nanos = i64::try_from(at.unix_nanos()).map_err(corrupt)?;
    Ok(DateTime::from_timestamp_nanos(nanos))
}

fn timestamp(at: DateTime<Utc>) -> Timestamp {
    let nanos = at.timestamp_nanos_opt().and_then(|n| u64::try_from(n).ok());
    Timestamp::from_unix_nanos(nanos.unwrap_or(0))
}

fn signed(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(corrupt)
}

fn unsigned(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(corrupt)
}

fn unavailable(error: sqlx::Error) -> StoreError {
    StoreError::Unavailable(error.to_string())
}

fn corrupt(error: impl std::fmt::Display) -> StoreError {
    StoreError::Corrupt(error.to_string())
}
