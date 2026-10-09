//! Postgres adapter against a real database.
//!
//! Ignored by default. Run with a throwaway database:
//!
//! ```text
//! PRICEFEED_TEST_DATABASE_URL=postgres://... cargo test -p orderflow-storage -- --ignored
//! ```
//!
//! Tests share one database and stay apart by using their own market and
//! indexer names.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use orderflow_application::Clock;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{
    Candle, CandleInterval, CandleQuery, PriceStatus, PriceUpdate, Revision, SourceId,
    fold_minute_candles,
    ports::{BlockRef, CandleReader, ChainStore, PriceStore, SwapRecord},
};
use orderflow_storage::{LeaderLease, MemoryStore, PgStore};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const MINUTE: u64 = 60_000_000_000;

struct FixedClock(u64);

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_unix_nanos(self.0)
    }
}

fn database_url() -> String {
    std::env::var("PRICEFEED_TEST_DATABASE_URL")
        .expect("set PRICEFEED_TEST_DATABASE_URL to run ignored tests")
}

async fn store(now: u64) -> PgStore {
    let store = PgStore::connect(&database_url(), 4, Arc::new(FixedClock(now)))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    store
}

fn update(
    market: &str,
    sequence: u64,
    price: Decimal,
    minute: u64,
    second: u64,
) -> Arc<PriceUpdate> {
    let at = Timestamp::from_unix_nanos(minute * MINUTE + second * 1_000_000_000);
    Arc::new(PriceUpdate {
        market: MarketId::parse(market).unwrap(),
        epoch: 1,
        sequence,
        price,
        status: PriceStatus::Live,
        as_of: at,
        published_at: at,
        spread_bps: dec!(1.5),
        components: Vec::new(),
    })
}

#[tokio::test]
#[ignore = "needs PRICEFEED_TEST_DATABASE_URL"]
async fn candles_match_the_in_memory_store_for_every_interval() {
    // Now is minute 30 of some hour, so the current 1h bucket is open.
    let now = 1_000 * 60 * MINUTE + 30 * MINUTE;
    let pg = store(now).await;
    let memory = MemoryStore::new(Arc::new(FixedClock(now)));

    let base = now / MINUTE - 20;
    let mut updates = Vec::new();
    for i in 0..60_u64 {
        let price = Decimal::from(100 + (i * 7) % 13);
        updates.push(update("PGA-USD", i + 1, price, base + i / 3, (i % 3) * 20));
    }
    // Two batches, as the writer would send them.
    for batch in updates.chunks(25) {
        let candles = fold_minute_candles(batch);
        pg.record(batch, &candles).await.unwrap();
        memory.record(batch, &candles).await.unwrap();
    }
    // Retrying a batch must change nothing.
    pg.record(&updates[..25], &fold_minute_candles(&updates[..25]))
        .await
        .unwrap();

    for interval in CandleInterval::ALL {
        let query = CandleQuery {
            market: MarketId::parse("PGA-USD").unwrap(),
            interval,
            limit: 30,
            end: None,
        };
        let from_pg = pg.candles(&query).await.unwrap();
        let from_memory = memory.candles(&query).await.unwrap();
        assert!(!from_pg.is_empty(), "{interval:?}");
        assert_eq!(from_pg, from_memory, "{interval:?}");
    }
}

#[tokio::test]
#[ignore = "needs PRICEFEED_TEST_DATABASE_URL"]
async fn a_late_write_never_replaces_a_newer_close() {
    let pg = store(100 * MINUTE).await;
    let market = MarketId::parse("PGB-USD").unwrap();
    let start = Timestamp::from_unix_nanos(90 * MINUTE);
    let newer = Candle::opening(
        market.clone(),
        start,
        dec!(20),
        Revision {
            epoch: 2,
            sequence: 1,
        },
    );
    let older = Candle::opening(
        market.clone(),
        start,
        dec!(5),
        Revision {
            epoch: 1,
            sequence: 900,
        },
    );
    pg.record(&[], &[newer]).await.unwrap();
    pg.record(&[], &[older]).await.unwrap();

    let query = CandleQuery {
        market,
        interval: CandleInterval::OneMinute,
        limit: 1,
        end: Some(Timestamp::from_unix_nanos(91 * MINUTE)),
    };
    let candles = pg.candles(&query).await.unwrap();
    let bar = &candles[0];
    assert_eq!(
        (bar.open, bar.low, bar.close),
        (dec!(20), dec!(5), dec!(20))
    );
    assert_eq!(
        bar.revision,
        Revision {
            epoch: 2,
            sequence: 1
        }
    );
}

#[tokio::test]
#[ignore = "needs PRICEFEED_TEST_DATABASE_URL"]
async fn swaps_are_idempotent_and_reorgs_roll_back() {
    let pg = store(0).await;
    let indexer = "test:pg-reorg";
    let block = |number: u64| BlockRef {
        number,
        hash: format!("0x{number:064x}"),
        timestamp: Timestamp::from_unix_nanos(number * 1_000_000_000),
    };
    let swap = |number: u64, index: u64| SwapRecord {
        pool: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".into(),
        source: SourceId::parse("uniswap_v3:usdc_weth_500").unwrap(),
        market: MarketId::parse("ETH-USDC").unwrap(),
        block_number: number,
        block_hash: format!("0x{number:064x}"),
        tx_hash: format!("0x{:064x}", number * 1_000 + index),
        log_index: index,
        price: dec!(2478.324669500408464656),
        // Larger than any 64 or 128 bit integer: stored exactly.
        amount0: "-115792089237316195423570985008687907853269984665640564039457584007913129639935"
            .into(),
        amount1: "1000000000000000000".into(),
        sqrt_price: "1591477429476286610218534748006820".into(),
        block_time: Timestamp::from_unix_nanos(number * 1_000_000_000),
    };

    pg.rollback(indexer, 0).await.unwrap();
    pg.commit(indexer, &[swap(10, 0), swap(10, 1)], &block(10))
        .await
        .unwrap();
    pg.commit(indexer, &[swap(10, 0)], &block(10))
        .await
        .unwrap();
    pg.commit(indexer, &[swap(20, 0)], &block(20))
        .await
        .unwrap();
    pg.commit(indexer, &[], &block(30)).await.unwrap();

    let checkpoints = pg.checkpoints(indexer, 10).await.unwrap();
    assert_eq!(
        checkpoints.iter().map(|b| b.number).collect::<Vec<_>>(),
        [30, 20, 10]
    );
    assert_eq!(checkpoints[0], block(30));

    assert_eq!(
        pg.rollback(indexer, 15).await.unwrap(),
        1,
        "the swap in block 20"
    );
    let checkpoints = pg.checkpoints(indexer, 10).await.unwrap();
    assert_eq!(
        checkpoints.iter().map(|b| b.number).collect::<Vec<_>>(),
        [10]
    );
    assert_eq!(pg.rollback(indexer, 0).await.unwrap(), 2);
}

#[tokio::test]
#[ignore = "needs PRICEFEED_TEST_DATABASE_URL"]
async fn only_one_instance_holds_the_leader_lease() {
    let url = database_url();
    let first = LeaderLease::try_acquire(&url).await.unwrap();
    assert!(first.is_some());
    assert!(LeaderLease::try_acquire(&url).await.unwrap().is_none());
    drop(first);
    // Closing the session releases the lock; the server notices quickly.
    let mut taken = None;
    for _ in 0..50 {
        taken = LeaderLease::try_acquire(&url).await.unwrap();
        if taken.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        taken.is_some(),
        "a standby takes over once the leader is gone"
    );
}
