use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use orderflow_application::{EventPublisher, PublishError};
use orderflow_domain::DomainEvent;

/// How often and how patiently to retry transient failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts including the first one.
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(2),
        }
    }
}

impl RetryPolicy {
    /// Upper bound of the wait before retry number `retry` (1 based):
    /// `base * 2^(retry - 1)`, capped at `max_delay`.
    fn ceiling(&self, retry: u32) -> Duration {
        let factor = 2u32.saturating_pow(retry.saturating_sub(1));
        self.base_delay.saturating_mul(factor).min(self.max_delay)
    }
}

/// Adds retries with exponential backoff to any publisher.
///
/// Pattern: Decorator. It implements `EventPublisher` by wrapping another
/// one, so retry behavior is added without touching the Kafka adapter and
/// can be tested with a fake. Only `PublishError::Transient` is retried.
///
/// Waits use "full jitter" (a uniform random delay up to the backoff
/// ceiling), which spreads retries from many instances apart after a
/// shared broker outage instead of having them hit it in lockstep.
pub struct RetryingPublisher<P> {
    inner: P,
    policy: RetryPolicy,
    jitter_state: AtomicU64,
}

impl<P: EventPublisher> RetryingPublisher<P> {
    pub fn new(inner: P, policy: RetryPolicy) -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            // Keeping only the low 64 bits is intended: any seed will do.
            .map_or(0x9E37_79B9_7F4A_7C15, |d| d.as_nanos() as u64);
        Self {
            inner,
            policy,
            jitter_state: AtomicU64::new(seed | 1),
        }
    }

    /// Xorshift64. It is not cryptographic and only needs to spread retry
    /// timing apart.
    fn next_random(&self) -> u64 {
        let mut x = self.jitter_state.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.jitter_state.store(x, Ordering::Relaxed);
        x
    }

    fn delay(&self, retry: u32) -> Duration {
        let ceiling = u64::try_from(self.policy.ceiling(retry).as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(self.next_random() % ceiling.saturating_add(1))
    }
}

#[async_trait]
impl<P: EventPublisher> EventPublisher for RetryingPublisher<P> {
    async fn publish(&self, events: &[DomainEvent]) -> Result<(), PublishError> {
        let mut attempt = 1;
        loop {
            match self.inner.publish(events).await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_transient() && attempt < self.policy.max_attempts => {
                    let delay = self.delay(attempt);
                    tracing::warn!(%error, attempt, ?delay, "publish failed, retrying");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;

    struct Flaky {
        failures_left: AtomicU32,
        calls: AtomicU32,
        error: PublishError,
    }

    impl Flaky {
        fn new(failures: u32, error: PublishError) -> Self {
            Self {
                failures_left: AtomicU32::new(failures),
                calls: AtomicU32::new(0),
                error,
            }
        }
    }

    #[async_trait]
    impl EventPublisher for Flaky {
        async fn publish(&self, _: &[DomainEvent]) -> Result<(), PublishError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let left = self.failures_left.load(Ordering::SeqCst);
            if left == 0 {
                return Ok(());
            }
            self.failures_left.store(left - 1, Ordering::SeqCst);
            Err(self.error.clone())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn transient_errors_are_retried_until_success() {
        let flaky = Flaky::new(3, PublishError::Transient("broker down".into()));
        let publisher = RetryingPublisher::new(flaky, RetryPolicy::default());

        assert_eq!(publisher.publish(&[]).await, Ok(()));
        assert_eq!(publisher.inner.calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_max_attempts() {
        let flaky = Flaky::new(10, PublishError::Transient("broker down".into()));
        let policy = RetryPolicy {
            max_attempts: 3,
            ..RetryPolicy::default()
        };
        let publisher = RetryingPublisher::new(flaky, policy);

        assert!(publisher.publish(&[]).await.is_err());
        assert_eq!(publisher.inner.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_errors_are_not_retried() {
        let flaky = Flaky::new(1, PublishError::Permanent("bad topic".into()));
        let publisher = RetryingPublisher::new(flaky, RetryPolicy::default());

        assert!(publisher.publish(&[]).await.is_err());
        assert_eq!(publisher.inner.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn backoff_ceiling_doubles_and_is_capped() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
        };
        let ceilings: Vec<_> = (1..=5).map(|n| policy.ceiling(n).as_millis()).collect();
        assert_eq!(ceilings, vec![100, 200, 400, 500, 500]);
    }
}
