//! How sources run: the `Source` contract and the supervisor around it.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use orderflow_application::Clock;
use orderflow_marketdata::{
    Backoff, Observation, pipeline::ObservationSender, ports::MarketDataMetrics,
};
use tokio_util::sync::CancellationToken;

use crate::error::SourceError;

/// A run that lasted this long counts as healthy, and the next failure
/// starts the backoff from the beginning again.
const HEALTHY_RUN: Duration = Duration::from_secs(60);

/// Everything a source may use while it runs.
#[derive(Clone)]
pub struct SourceContext {
    observations: ObservationSender,
    pub clock: Arc<dyn Clock>,
    pub metrics: Arc<dyn MarketDataMetrics>,
    pub shutdown: CancellationToken,
}

impl SourceContext {
    pub fn new(
        observations: ObservationSender,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn MarketDataMetrics>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            observations,
            clock,
            metrics,
            shutdown,
        }
    }

    /// Hands an observation to the aggregator, waiting if its queue is full.
    pub async fn emit(&self, observation: Observation) -> Result<(), SourceError> {
        self.observations
            .send(observation)
            .await
            .map_err(|_| SourceError::Closed)
    }

    /// Sleeps for `duration`, or less if shutdown starts. Returns `false`
    /// when the source should stop.
    pub async fn pause(&self, duration: Duration) -> bool {
        tokio::select! {
            () = self.shutdown.cancelled() => false,
            () = tokio::time::sleep(duration) => true,
        }
    }
}

/// A long-running producer of observations.
///
/// Pattern: Strategy. The supervisor, the aggregator and the composition
/// root treat every exchange and chain the same way through this trait.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Name used in logs and metrics, such as `kraken` or
    /// `uniswap_v3:ethereum`.
    fn name(&self) -> &str;

    /// Runs until shutdown, returning `Ok`, or until a failure, returning
    /// `Err`. A source does not retry by itself: it returns the error and
    /// lets the supervisor decide when to start it again.
    async fn run(&self, ctx: &SourceContext) -> Result<(), SourceError>;
}

/// Runs `source` until shutdown, restarting it with backoff after errors.
///
/// Pattern: Supervisor ("let it crash"). Retry policy lives in one place
/// instead of in every connector, a `Retry-After` from the upstream is
/// honored, and a source that keeps failing backs off to one attempt per
/// minute without affecting any other source.
pub async fn supervise(source: Arc<dyn Source>, ctx: SourceContext) {
    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
    loop {
        let started = Instant::now();
        let result = tokio::select! {
            () = ctx.shutdown.cancelled() => Ok(()),
            result = source.run(&ctx) => result,
        };
        if ctx.shutdown.is_cancelled() {
            break;
        }
        match result {
            Ok(()) => {
                tracing::info!(source = source.name(), "source finished");
                break;
            }
            Err(SourceError::Closed) => break,
            Err(error) => {
                ctx.metrics.source_error(source.name(), error.kind());
                if started.elapsed() >= HEALTHY_RUN {
                    backoff.reset();
                }
                let delay = error.retry_after().map_or_else(
                    || backoff.next_delay(),
                    |wait| wait.min(Duration::from_secs(300)),
                );
                tracing::warn!(
                    source = source.name(),
                    %error,
                    retry_in_ms = delay.as_millis() as u64,
                    "source failed"
                );
                if !ctx.pause(delay).await {
                    break;
                }
            }
        }
    }
    tracing::info!(source = source.name(), "source stopped");
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use orderflow_domain::Timestamp;
    use orderflow_marketdata::ports::NoopMetrics;
    use tokio::sync::mpsc;

    use super::*;

    struct Clockwork;

    impl Clock for Clockwork {
        fn now(&self) -> Timestamp {
            Timestamp::from_unix_nanos(0)
        }
    }

    /// Fails `failures` times, then waits for shutdown.
    struct Flaky {
        runs: AtomicU32,
        failures: u32,
    }

    #[async_trait]
    impl Source for Flaky {
        fn name(&self) -> &str {
            "flaky"
        }

        async fn run(&self, ctx: &SourceContext) -> Result<(), SourceError> {
            if self.runs.fetch_add(1, Ordering::SeqCst) < self.failures {
                return Err(SourceError::Status {
                    status: 429,
                    retry_after: Some(Duration::from_secs(30)),
                });
            }
            ctx.shutdown.cancelled().await;
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failed_sources_are_restarted_after_the_upstream_delay() {
        let (sender, _receiver) = mpsc::channel(1);
        let shutdown = CancellationToken::new();
        let ctx = SourceContext::new(
            sender,
            Arc::new(Clockwork),
            Arc::new(NoopMetrics),
            shutdown.clone(),
        );
        let source = Arc::new(Flaky {
            runs: AtomicU32::new(0),
            failures: 2,
        });
        let task = tokio::spawn(supervise(source.clone(), ctx));

        tokio::time::sleep(Duration::from_secs(29)).await;
        assert_eq!(
            source.runs.load(Ordering::SeqCst),
            1,
            "waits for Retry-After"
        );
        tokio::time::sleep(Duration::from_secs(40)).await;
        assert_eq!(source.runs.load(Ordering::SeqCst), 3);

        shutdown.cancel();
        task.await.unwrap();
    }
}
