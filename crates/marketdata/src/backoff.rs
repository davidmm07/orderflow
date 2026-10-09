//! Retry delays for sinks and sources.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Distinguishes instances created in the same nanosecond.
static INSTANCES: AtomicU64 = AtomicU64::new(0);

/// Exponential backoff with "equal jitter": each delay is half the current
/// ceiling plus a random share of the other half.
///
/// Full jitter can return a delay close to zero, which is fine for one
/// client but lets a reconnect loop against a rate limited public API fire
/// again almost at once. Equal jitter always waits at least half the
/// ceiling while still spreading many clients apart.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
    state: u64,
}

impl Backoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        let clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64);
        let instance = INSTANCES.fetch_add(1, Ordering::Relaxed);
        Self {
            base,
            max: max.max(base),
            attempt: 0,
            state: (clock ^ instance.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1,
        }
    }

    /// Delay before the next retry. Grows with every call until `reset`.
    pub fn next_delay(&mut self) -> Duration {
        let factor = 1_u32.checked_shl(self.attempt).unwrap_or(u32::MAX);
        let ceiling = self.base.saturating_mul(factor).min(self.max);
        self.attempt = self.attempt.saturating_add(1).min(31);
        let half = ceiling / 2;
        let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        half + Duration::from_nanos(self.next_random() % spread.saturating_add(1))
    }

    /// Number of delays handed out since the last reset.
    pub fn attempts(&self) -> u32 {
        self.attempt
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// xorshift64*: good enough to spread retries; never use it for secrets.
    fn next_random(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_grow_stay_bounded_and_reset() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(2);
        let mut backoff = Backoff::new(base, max);
        for attempt in 0..40 {
            let ceiling = base.saturating_mul(1 << attempt.min(20)).min(max);
            let delay = backoff.next_delay();
            assert!(
                delay >= ceiling / 2 && delay <= ceiling,
                "{attempt}: {delay:?}"
            );
        }
        backoff.reset();
        assert_eq!(backoff.attempts(), 0);
        assert!(backoff.next_delay() <= base);
    }
}
