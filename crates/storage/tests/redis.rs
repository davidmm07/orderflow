//! Redis adapters against a real Redis or Valkey.
//!
//! Ignored by default. Run with a throwaway instance:
//!
//! ```text
//! PRICEFEED_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo test -p orderflow-storage -- --ignored
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use orderflow_application::Clock;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{
    Candle, CandleInterval, CandleQuery, PriceStatus, PriceUpdate,
    ports::{CandleReader, NoopMetrics, PriceBus, StoreError},
};
use orderflow_storage::{RedisBus, RedisCandleCache, client, follow_prices};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

fn redis_url() -> String {
    std::env::var("PRICEFEED_TEST_REDIS_URL")
        .expect("set PRICEFEED_TEST_REDIS_URL to run ignored tests")
}

struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_unix_nanos(3_600_000_000_000)
    }
}

fn update(market: &str, sequence: u64) -> Arc<PriceUpdate> {
    Arc::new(PriceUpdate {
        market: MarketId::parse(market).unwrap(),
        epoch: 9,
        sequence,
        price: Decimal::from(sequence),
        status: PriceStatus::Live,
        as_of: Timestamp::from_unix_nanos(sequence),
        published_at: Timestamp::from_unix_nanos(sequence),
        spread_bps: Decimal::ZERO,
        components: Vec::new(),
    })
}

#[tokio::test]
#[ignore = "needs PRICEFEED_TEST_REDIS_URL"]
async fn edges_get_the_snapshot_and_then_live_updates() {
    let client = client(&redis_url()).unwrap();
    let bus = RedisBus::connect(&client).await.unwrap();
    // Markets unique to this run, so data left by earlier runs never mixes in.
    let base = format!("R{}", std::process::id() % 100_000);
    let (usd, eur) = (format!("{base}-USD"), format!("{base}-EUR"));
    // Published before the edge starts: must arrive through the snapshot.
    bus.publish(&[update(&usd, 1)]).await.unwrap();

    let received: Arc<Mutex<Vec<(String, u64)>>> = Arc::default();
    let sink = received.clone();
    let prefix = base.clone();
    let shutdown = CancellationToken::new();
    let ready = Arc::new(AtomicBool::new(false));
    let follower = tokio::spawn(follow_prices(
        client.clone(),
        Arc::new(move |update: PriceUpdate| {
            if update.market.as_str().starts_with(&prefix) {
                sink.lock()
                    .unwrap()
                    .push((update.market.to_string(), update.sequence));
            }
        }),
        Arc::new(NoopMetrics),
        ready.clone(),
        shutdown.clone(),
    ));

    let wait_for = |count: usize| {
        let received = received.clone();
        async move {
            for _ in 0..100 {
                if received.lock().unwrap().len() >= count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    wait_for(1).await;
    assert!(
        ready.load(Ordering::SeqCst),
        "ready once the snapshot is in"
    );
    bus.publish(&[update(&usd, 2), update(&eur, 1)])
        .await
        .unwrap();
    wait_for(3).await;
    shutdown.cancel();
    follower.await.unwrap();

    let received = received.lock().unwrap().clone();
    assert_eq!(received[0], (usd.clone(), 1));
    assert!(received.contains(&(usd, 2)));
    assert!(received.contains(&(eur, 1)));
}

#[derive(Default)]
struct CountingReader(AtomicUsize);

#[async_trait]
impl CandleReader for CountingReader {
    async fn candles(&self, query: &CandleQuery) -> Result<Arc<[Candle]>, StoreError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let candle = Candle::opening(
            query.market.clone(),
            Timestamp::from_unix_nanos(0),
            Decimal::ONE,
            Default::default(),
        );
        Ok(Arc::from(vec![candle]))
    }
}

#[tokio::test]
#[ignore = "needs PRICEFEED_TEST_REDIS_URL"]
async fn the_shared_cache_answers_repeat_reads() {
    let client = client(&redis_url()).unwrap();
    let inner = Arc::new(CountingReader::default());
    let make = || {
        RedisCandleCache::connect(
            &client,
            inner.clone(),
            Arc::new(FixedClock),
            Arc::new(NoopMetrics),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
    };
    // Two caches stand for two edge nodes sharing one Redis.
    let (node_a, node_b) = (make().await.unwrap(), make().await.unwrap());
    let query = CandleQuery {
        market: MarketId::parse("RDB-USD").unwrap(),
        interval: CandleInterval::OneMinute,
        limit: std::process::id() % 900 + 1,
        end: None,
    };
    let first = node_a.candles(&query).await.unwrap();
    let second = node_b.candles(&query).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(
        inner.0.load(Ordering::SeqCst),
        1,
        "node b was served by redis"
    );
}
