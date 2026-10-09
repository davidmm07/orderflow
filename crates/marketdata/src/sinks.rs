//! Update sinks that move published prices off the aggregator's hot path.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use orderflow_domain::MarketId;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    backoff::Backoff,
    candle::fold_minute_candles,
    ports::{MarketDataMetrics, PriceBus, PriceStore, UpdateSink},
    update::PriceUpdate,
};

/// Largest number of updates written in one store call.
const MAX_WRITE_BATCH: usize = 512;
/// Attempts per batch before it is dropped, so a long database outage
/// costs history but never memory.
const MAX_WRITE_ATTEMPTS: u32 = 5;

/// Writes every update, and the candles derived from it, to a `PriceStore`.
///
/// Pattern: Queue-based load leveling. `accept` only enqueues; a background
/// task drains the queue in batches. While the database is slow, batches
/// grow instead of calls multiplying, and while it is down the bounded
/// queue fills and new updates are counted as dropped. Live prices keep
/// flowing to subscribers either way: the store keeps a record and is never
/// a hop on the delivery path.
#[derive(Clone)]
pub struct StoreWriter {
    sender: mpsc::Sender<Arc<PriceUpdate>>,
    metrics: Arc<dyn MarketDataMetrics>,
}

impl StoreWriter {
    /// Starts the writer task. It stops once every `StoreWriter` clone is
    /// dropped and the queue is drained.
    pub fn spawn(
        store: Arc<dyn PriceStore>,
        metrics: Arc<dyn MarketDataMetrics>,
        capacity: usize,
    ) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let task = tokio::spawn(write_loop(receiver, store, metrics.clone()));
        (Self { sender, metrics }, task)
    }
}

impl UpdateSink for StoreWriter {
    fn accept(&self, update: &Arc<PriceUpdate>) {
        if self.sender.try_send(update.clone()).is_err() {
            self.metrics.store_dropped(1);
        }
    }
}

async fn write_loop(
    mut receiver: mpsc::Receiver<Arc<PriceUpdate>>,
    store: Arc<dyn PriceStore>,
    metrics: Arc<dyn MarketDataMetrics>,
) {
    let mut batch = Vec::with_capacity(MAX_WRITE_BATCH);
    let mut backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(5));
    while receiver.recv_many(&mut batch, MAX_WRITE_BATCH).await > 0 {
        let candles = fold_minute_candles(&batch);
        backoff.reset();
        loop {
            let started = Instant::now();
            match store.record(&batch, &candles).await {
                Ok(()) => {
                    metrics.store_written(batch.len(), started.elapsed());
                    break;
                }
                Err(error) if backoff.attempts() + 1 >= MAX_WRITE_ATTEMPTS => {
                    tracing::error!(%error, updates = batch.len(), "dropping a batch of price history");
                    metrics.store_failed();
                    metrics.store_dropped(batch.len());
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, "price history write failed, retrying");
                    metrics.store_failed();
                    tokio::time::sleep(backoff.next_delay()).await;
                }
            }
        }
        batch.clear();
    }
    tracing::info!("price history writer stopped");
}

/// Publishes updates to a `PriceBus`, keeping only the newest per market.
///
/// Pattern: Conflation. Subscribers of a price feed want the current price,
/// not every intermediate one. Pending updates live in a map keyed by
/// market, so however long the bus is unreachable, memory stays at one
/// update per market, and on recovery each market's newest price goes out
/// first and only once.
#[derive(Debug, Clone)]
pub struct ConflatingPublisher {
    pending: Arc<Mutex<HashMap<MarketId, Arc<PriceUpdate>>>>,
    wake: mpsc::Sender<()>,
}

impl ConflatingPublisher {
    /// Starts the publishing task. It flushes what is pending and stops
    /// once every `ConflatingPublisher` clone is dropped.
    pub fn spawn(
        bus: Arc<dyn PriceBus>,
        metrics: Arc<dyn MarketDataMetrics>,
    ) -> (Self, JoinHandle<()>) {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        // One slot is enough: a wake-up already queued covers every update
        // that arrives before the task runs.
        let (wake, signal) = mpsc::channel(1);
        let task = tokio::spawn(publish_loop(signal, pending.clone(), bus, metrics));
        (Self { pending, wake }, task)
    }
}

impl UpdateSink for ConflatingPublisher {
    fn accept(&self, update: &Arc<PriceUpdate>) {
        merge_newer(&mut lock(&self.pending), update.clone());
        let _ = self.wake.try_send(());
    }
}

async fn publish_loop(
    mut signal: mpsc::Receiver<()>,
    pending: Arc<Mutex<HashMap<MarketId, Arc<PriceUpdate>>>>,
    bus: Arc<dyn PriceBus>,
    metrics: Arc<dyn MarketDataMetrics>,
) {
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
    let mut open = true;
    while open {
        open = signal.recv().await.is_some();
        loop {
            let mut batch: Vec<Arc<PriceUpdate>> =
                std::mem::take(&mut *lock(&pending)).into_values().collect();
            if batch.is_empty() {
                break;
            }
            batch.sort_by(|a, b| a.market.cmp(&b.market));
            match bus.publish(&batch).await {
                Ok(()) => {
                    metrics.bus_published(batch.len());
                    backoff.reset();
                }
                Err(error) => {
                    tracing::warn!(%error, "price bus publish failed, retrying");
                    metrics.bus_failed();
                    // Put the batch back unless a newer update already
                    // replaced it while the call was in flight.
                    {
                        let mut map = lock(&pending);
                        for update in batch {
                            merge_newer(&mut map, update);
                        }
                    }
                    if !open {
                        // Shutting down: one attempt is all we have time for.
                        break;
                    }
                    tokio::time::sleep(backoff.next_delay()).await;
                }
            }
        }
    }
    tracing::info!("price bus publisher stopped");
}

fn merge_newer(map: &mut HashMap<MarketId, Arc<PriceUpdate>>, update: Arc<PriceUpdate>) {
    match map.get(&update.market) {
        Some(existing) if !update.is_newer_than(existing) => {}
        _ => {
            map.insert(update.market.clone(), update);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;
    use orderflow_domain::Timestamp;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        PriceStatus,
        candle::Candle,
        ports::{NoopMetrics, StoreError},
    };

    fn update(market: &str, sequence: u64) -> Arc<PriceUpdate> {
        Arc::new(PriceUpdate {
            market: MarketId::parse(market).unwrap(),
            epoch: 1,
            sequence,
            price: Decimal::from(sequence),
            status: PriceStatus::Live,
            as_of: Timestamp::from_unix_nanos(sequence),
            published_at: Timestamp::from_unix_nanos(sequence),
            spread_bps: Decimal::ZERO,
            components: Vec::new(),
        })
    }

    /// Fails the first `failures` calls, then records what it receives.
    #[derive(Default)]
    struct Flaky {
        failures: AtomicU32,
        updates: Mutex<Vec<Arc<PriceUpdate>>>,
        candles: Mutex<Vec<Candle>>,
    }

    impl Flaky {
        fn failing(times: u32) -> Self {
            Self {
                failures: AtomicU32::new(times),
                ..Self::default()
            }
        }

        fn fail(&self) -> bool {
            self.failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        }
    }

    #[async_trait]
    impl PriceStore for Flaky {
        async fn record(
            &self,
            updates: &[Arc<PriceUpdate>],
            candles: &[Candle],
        ) -> Result<(), StoreError> {
            if self.fail() {
                return Err(StoreError::Unavailable("down".into()));
            }
            self.updates.lock().unwrap().extend_from_slice(updates);
            self.candles.lock().unwrap().extend_from_slice(candles);
            Ok(())
        }
    }

    #[async_trait]
    impl PriceBus for Flaky {
        async fn publish(&self, updates: &[Arc<PriceUpdate>]) -> Result<(), StoreError> {
            if self.fail() {
                return Err(StoreError::Unavailable("down".into()));
            }
            self.updates.lock().unwrap().extend_from_slice(updates);
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn store_writer_retries_and_writes_candles() {
        let store = Arc::new(Flaky::failing(2));
        let (writer, task) = StoreWriter::spawn(store.clone(), Arc::new(NoopMetrics), 64);
        writer.accept(&update("BTC-USD", 1));
        writer.accept(&update("BTC-USD", 2));
        drop(writer);
        task.await.unwrap();

        assert_eq!(store.updates.lock().unwrap().len(), 2);
        let candles = store.candles.lock().unwrap();
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].close, Decimal::from(2));
    }

    #[tokio::test(start_paused = true)]
    async fn store_writer_gives_up_on_a_batch_after_bounded_attempts() {
        let store = Arc::new(Flaky::failing(u32::MAX));
        let (writer, task) = StoreWriter::spawn(store.clone(), Arc::new(NoopMetrics), 64);
        writer.accept(&update("BTC-USD", 1));
        drop(writer);
        task.await.unwrap();
        assert!(store.updates.lock().unwrap().is_empty());
        assert_eq!(
            store.failures.load(Ordering::SeqCst),
            u32::MAX - MAX_WRITE_ATTEMPTS
        );
    }

    #[tokio::test(start_paused = true)]
    async fn publisher_keeps_only_the_newest_update_per_market() {
        let bus = Arc::new(Flaky::failing(1));
        let (publisher, task) = ConflatingPublisher::spawn(bus.clone(), Arc::new(NoopMetrics));
        for sequence in 1..=5 {
            publisher.accept(&update("BTC-USD", sequence));
        }
        publisher.accept(&update("ETH-USD", 1));
        // An update older than the pending one never replaces it.
        publisher.accept(&update("ETH-USD", 0));
        tokio::time::sleep(Duration::from_secs(10)).await;
        drop(publisher);
        task.await.unwrap();

        let published = bus.updates.lock().unwrap();
        let seen: Vec<(String, u64)> = published
            .iter()
            .map(|u| (u.market.to_string(), u.sequence))
            .collect();
        assert_eq!(seen, [("BTC-USD".into(), 5), ("ETH-USD".into(), 1)]);
    }
}
