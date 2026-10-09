//! The aggregator: one task that owns the price index.

use std::{sync::Arc, time::Duration};

use orderflow_application::Clock;
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::{
    index::PriceIndex,
    observation::Observation,
    ports::{MarketDataMetrics, UpdateSink},
};

/// Largest number of queued observations applied in one go.
const MAX_BATCH: usize = 512;

/// Sending half handed to every source. Bounded: a source that produces
/// faster than the aggregator applies waits, instead of growing a queue.
pub type ObservationSender = mpsc::Sender<Observation>;

/// Applies observations to the index and publishes changed prices on a
/// fixed cadence.
///
/// Pattern: Actor (single writer). Only this task touches `PriceIndex`, so
/// it needs no locks, and every sink sees updates in sequence order.
/// Publishing on a timer rather than per observation caps the update rate
/// per market at one per `interval`, however chatty the sources are: a
/// burst of 50 trades on one exchange becomes one update, not 50.
pub struct Aggregator {
    index: PriceIndex,
    observations: mpsc::Receiver<Observation>,
    sinks: Vec<Arc<dyn UpdateSink>>,
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MarketDataMetrics>,
    interval: Duration,
}

impl Aggregator {
    pub fn new(
        index: PriceIndex,
        sinks: Vec<Arc<dyn UpdateSink>>,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn MarketDataMetrics>,
        interval: Duration,
        capacity: usize,
    ) -> (Self, ObservationSender) {
        let (sender, observations) = mpsc::channel(capacity.max(1));
        let aggregator = Self {
            index,
            observations,
            sinks,
            clock,
            metrics,
            interval,
        };
        (aggregator, sender)
    }

    /// Runs until `shutdown` fires or every sender is gone, then publishes
    /// one last time so the final prices reach the sinks.
    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut batch = Vec::with_capacity(MAX_BATCH);
        loop {
            // Biased: the timer is checked before the queue, so a flood of
            // observations cannot starve publishing.
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                _ = ticker.tick() => self.publish(),
                received = self.observations.recv_many(&mut batch, MAX_BATCH) => {
                    if received == 0 {
                        break;
                    }
                    self.apply(&mut batch);
                }
            }
        }
        self.publish();
        tracing::info!("aggregator stopped");
    }

    fn apply(&mut self, batch: &mut Vec<Observation>) {
        let now = self.clock.now();
        for observation in batch.drain(..) {
            match self.index.observe(&observation, now) {
                Ok(()) => self.metrics.observation_accepted(observation.source()),
                Err(rejection) => {
                    tracing::debug!(
                        market = %observation.market(),
                        source = %observation.source(),
                        reason = rejection.as_str(),
                        "observation rejected"
                    );
                    self.metrics
                        .observation_rejected(observation.source(), rejection.as_str());
                }
            }
        }
    }

    fn publish(&mut self) {
        for update in self.index.evaluate(self.clock.now()) {
            let update = Arc::new(update);
            self.metrics.index_published(&update);
            for sink in &self.sinks {
                sink.accept(&update);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use orderflow_domain::{MarketId, Timestamp};
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        AggregationPolicy, MarketFeed, PriceStatus, PriceUpdate, SourceId, SourceKind, SourceRole,
        SourceSpec, ports::NoopMetrics,
    };

    struct FixedClock(Timestamp);

    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            self.0
        }
    }

    #[derive(Default)]
    struct Collect(Mutex<Vec<Arc<PriceUpdate>>>);

    impl UpdateSink for Collect {
        fn accept(&self, update: &Arc<PriceUpdate>) {
            self.0.lock().unwrap().push(update.clone());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn observations_become_updates_on_the_next_tick() {
        let market = MarketId::parse("SOL-USD").unwrap();
        let kraken = SourceId::parse("kraken").unwrap();
        let feed = MarketFeed {
            market: market.clone(),
            policy: AggregationPolicy::new(1, 100).unwrap(),
            sources: vec![SourceSpec {
                id: kraken.clone(),
                kind: SourceKind::Cex,
                role: SourceRole::Index,
                max_age: Duration::from_secs(60),
            }],
        };
        let now = Timestamp::from_unix_nanos(1_000_000_000_000);
        let sink = Arc::new(Collect::default());
        let (aggregator, sender) = Aggregator::new(
            PriceIndex::new(1, vec![feed]).unwrap(),
            vec![sink.clone()],
            Arc::new(FixedClock(now)),
            Arc::new(NoopMetrics),
            Duration::from_millis(250),
            16,
        );
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(aggregator.run(shutdown.clone()));

        for price in [dec!(150), dec!(151), dec!(152)] {
            let observation = Observation::new(market.clone(), kraken.clone(), price, now).unwrap();
            sender.send(observation).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown.cancel();
        task.await.unwrap();

        let updates = sink.0.lock().unwrap();
        assert_eq!(updates.len(), 1, "three quotes in one interval, one update");
        assert_eq!(updates[0].price, dec!(152));
        assert_eq!(updates[0].status, PriceStatus::Live);
    }
}
