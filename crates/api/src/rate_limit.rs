//! Per-account token bucket rate limiting.

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
    time::Duration,
};

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use orderflow_domain::AccountId;
use tokio::time::Instant;

use crate::{auth::Authenticated, error::ApiError, state::AppState};

/// Tokens are tracked in millionths so refills stay exact with integers.
const MICROS_PER_TOKEN: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitConfig {
    /// Sustained requests per second.
    pub per_second: u32,
    /// Requests allowed in a burst on top of the sustained rate.
    pub burst: u32,
}

#[derive(Debug)]
struct Bucket {
    micro_tokens: u64,
    updated: Instant,
}

/// Token bucket limiter keyed by authenticated account.
///
/// It runs after authentication, so only accounts with valid credentials
/// ever get a bucket. That keeps the map bounded by the number of issued
/// keys: unauthenticated traffic cannot grow it.
#[derive(Debug)]
pub struct RateLimiter {
    config: RateLimitConfig,
    buckets: Mutex<HashMap<AccountId, Bucket>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Takes one token, or returns how long until the next one is available.
    pub fn try_acquire(&self, account: &AccountId) -> Result<(), Duration> {
        let now = Instant::now();
        let rate = u64::from(self.config.per_second.max(1));
        let capacity = u64::from(self.config.burst.max(1)) * MICROS_PER_TOKEN;

        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        let bucket = buckets.entry(account.clone()).or_insert(Bucket {
            micro_tokens: capacity,
            updated: now,
        });

        let elapsed_micros =
            u64::try_from(now.duration_since(bucket.updated).as_micros()).unwrap_or(u64::MAX);
        bucket.micro_tokens = bucket
            .micro_tokens
            .saturating_add(elapsed_micros.saturating_mul(rate))
            .min(capacity);
        bucket.updated = now;

        if bucket.micro_tokens >= MICROS_PER_TOKEN {
            bucket.micro_tokens -= MICROS_PER_TOKEN;
            Ok(())
        } else {
            let missing = MICROS_PER_TOKEN - bucket.micro_tokens;
            Err(Duration::from_micros(missing.div_ceil(rate)))
        }
    }
}

pub(crate) async fn enforce(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let account = request
        .extensions()
        .get::<Authenticated>()
        .map(|auth| auth.0.clone())
        .ok_or_else(|| ApiError::internal("rate limiter ran before authentication"))?;
    state
        .rate_limiter
        .try_acquire(&account)
        .map_err(ApiError::rate_limited)?;
    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn allows_a_burst_then_refills_at_the_sustained_rate() {
        let limiter = RateLimiter::new(RateLimitConfig {
            per_second: 2,
            burst: 3,
        });
        let alice = AccountId::parse("alice").unwrap();

        for _ in 0..3 {
            assert!(limiter.try_acquire(&alice).is_ok());
        }
        let wait = limiter.try_acquire(&alice).unwrap_err();
        assert_eq!(wait, Duration::from_millis(500));

        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(limiter.try_acquire(&alice).is_ok());
        assert!(limiter.try_acquire(&alice).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn accounts_do_not_share_buckets() {
        let limiter = RateLimiter::new(RateLimitConfig {
            per_second: 1,
            burst: 1,
        });
        assert!(limiter.try_acquire(&AccountId::parse("a").unwrap()).is_ok());
        assert!(limiter.try_acquire(&AccountId::parse("b").unwrap()).is_ok());
        assert!(
            limiter
                .try_acquire(&AccountId::parse("a").unwrap())
                .is_err()
        );
    }
}
