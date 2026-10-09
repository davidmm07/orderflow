//! Redis (or Valkey, or ElastiCache) as the price bus between nodes and as
//! the shared candle cache.
//!
//! Bus: the ingest node publishes each update on one channel and also
//! writes it to a hash of latest prices. An edge node subscribes first and
//! then reads the hash, so it starts with every market's price and misses
//! nothing in between; anything seen twice is dropped by the sequence
//! check in the hub.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt;
use orderflow_application::Clock;
use orderflow_marketdata::{
    Backoff, Candle, CandleQuery, PriceUpdate,
    ports::{CandleReader, MarketDataMetrics, PriceBus, StoreError},
};
use redis::{AsyncTypedCommands, aio::ConnectionManager};
use tokio_util::sync::CancellationToken;

use crate::wire::{decode_candles, decode_update, encode_candles, encode_update};

/// Channel carrying every published update. The version suffix lets a new
/// record schema be rolled out on a new channel next to the old one.
pub const PRICE_CHANNEL: &str = "orderflow:prices:v1";
/// Hash of the latest update per market, for edge nodes that just started.
pub const LATEST_KEY: &str = "orderflow:prices:latest:v1";
const CANDLE_PREFIX: &str = "orderflow:candles:v1";
/// An idle subscription is checked with a ping this often, so a silently
/// dropped connection is noticed in seconds rather than never.
const PING_EVERY: Duration = Duration::from_secs(15);

/// Opens a client for `url` (`redis://` or `rediss://` for TLS). Does not
/// connect yet.
pub fn client(url: &str) -> Result<redis::Client, StoreError> {
    redis::Client::open(url).map_err(unavailable)
}

/// Publishing side of the bus, used by the ingest node.
#[derive(Clone)]
pub struct RedisBus {
    connection: ConnectionManager,
}

impl RedisBus {
    pub async fn connect(client: &redis::Client) -> Result<Self, StoreError> {
        let connection = client.get_connection_manager().await.map_err(unavailable)?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl PriceBus for RedisBus {
    async fn publish(&self, updates: &[Arc<PriceUpdate>]) -> Result<(), StoreError> {
        let mut pipeline = redis::pipe();
        for update in updates {
            let record = encode_update(update);
            pipeline
                .hset(LATEST_KEY, update.market.as_str(), &record)
                .ignore()
                .publish(PRICE_CHANNEL, &record)
                .ignore();
        }
        let mut connection = self.connection.clone();
        pipeline
            .exec_async(&mut connection)
            .await
            .map_err(unavailable)
    }
}

/// Subscribing side of the bus, used by edge nodes. Delivers the stored
/// snapshot and then every live update to `deliver`, reconnecting with
/// backoff until `shutdown`. `ready` is true while the subscription is
/// live, which is what an edge node's readiness probe reports.
pub async fn follow_prices(
    client: redis::Client,
    deliver: Arc<dyn Fn(PriceUpdate) + Send + Sync>,
    metrics: Arc<dyn MarketDataMetrics>,
    ready: Arc<AtomicBool>,
    shutdown: CancellationToken,
) {
    let mut backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(10));
    while !shutdown.is_cancelled() {
        let result = follow_once(&client, &*deliver, &ready, &shutdown, &mut backoff).await;
        ready.store(false, Ordering::Release);
        match result {
            Ok(()) => break,
            Err(error) => {
                metrics.bus_failed();
                let delay = backoff.next_delay();
                tracing::warn!(%error, retry_in_ms = delay.as_millis() as u64, "price subscription lost");
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
    tracing::info!("price subscription stopped");
}

async fn follow_once(
    client: &redis::Client,
    deliver: &(dyn Fn(PriceUpdate) + Send + Sync),
    ready: &AtomicBool,
    shutdown: &CancellationToken,
    backoff: &mut Backoff,
) -> Result<(), StoreError> {
    let (mut sink, mut stream) = client
        .get_async_pubsub()
        .await
        .map_err(unavailable)?
        .split();
    sink.subscribe(PRICE_CHANNEL).await.map_err(unavailable)?;

    // Snapshot after subscribing, never before: an update published in
    // between then arrives twice instead of not at all.
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(unavailable)?;
    let latest: HashMap<String, String> =
        connection.hgetall(LATEST_KEY).await.map_err(unavailable)?;
    for record in latest.values() {
        match decode_update(record.as_bytes()) {
            Ok(update) => deliver(update),
            Err(error) => tracing::warn!(%error, "skipping a stored price"),
        }
    }
    tracing::info!(markets = latest.len(), "price subscription ready");
    ready.store(true, Ordering::Release);
    backoff.reset();

    let mut ping = tokio::time::interval(PING_EVERY);
    ping.reset();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = ping.tick() => {
                tokio::time::timeout(Duration::from_secs(5), sink.ping::<String>())
                    .await
                    .map_err(|_| StoreError::Unavailable("subscription ping timed out".into()))?
                    .map_err(unavailable)?;
            }
            message = stream.next() => {
                let Some(message) = message else {
                    return Err(StoreError::Unavailable("subscription closed".into()));
                };
                match decode_update(message.get_payload_bytes()) {
                    Ok(update) => deliver(update),
                    // A bad record is skipped; the next one may be fine.
                    Err(error) => tracing::warn!(%error, "skipping a published price"),
                }
            }
        }
    }
}

/// Shared second-tier cache for candle reads.
///
/// Pattern: Decorator (Cache-Aside). Every edge node checks Redis before
/// the database, so a spike spread over many nodes still costs the
/// database one query per key and expiry window. Redis being down is not
/// an error for readers: lookups fall through to the next tier.
pub struct RedisCandleCache {
    connection: ConnectionManager,
    inner: Arc<dyn CandleReader>,
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MarketDataMetrics>,
    hot_ttl: Duration,
    settled_ttl: Duration,
}

impl RedisCandleCache {
    pub async fn connect(
        client: &redis::Client,
        inner: Arc<dyn CandleReader>,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn MarketDataMetrics>,
        hot_ttl: Duration,
        settled_ttl: Duration,
    ) -> Result<Self, StoreError> {
        let connection = client.get_connection_manager().await.map_err(unavailable)?;
        Ok(Self {
            connection,
            inner,
            clock,
            metrics,
            hot_ttl,
            settled_ttl,
        })
    }
}

/// Key of a candle query. Every part is validated or numeric, so a client
/// cannot shape keys.
pub fn candle_key(query: &CandleQuery) -> String {
    let end = query
        .end
        .map_or_else(|| "now".to_owned(), |end| end.unix_nanos().to_string());
    format!(
        "{CANDLE_PREFIX}:{}:{}:{}:{end}",
        query.market,
        query.interval.as_str(),
        query.limit
    )
}

#[async_trait]
impl CandleReader for RedisCandleCache {
    async fn candles(&self, query: &CandleQuery) -> Result<Arc<[Candle]>, StoreError> {
        let key = candle_key(query);
        let mut connection = self.connection.clone();
        match connection.get(&key).await {
            Ok(Some(cached)) => match decode_candles(&query.market, cached.as_bytes()) {
                Ok(candles) => {
                    self.metrics.cache_lookup("redis", true);
                    return Ok(candles);
                }
                Err(error) => tracing::warn!(%error, "ignoring an unreadable cached candle set"),
            },
            Ok(None) => {}
            Err(error) => tracing::debug!(%error, "redis candle cache unavailable"),
        }
        self.metrics.cache_lookup("redis", false);

        let candles = self.inner.candles(query).await?;
        let ttl = if query.is_settled(self.clock.now()) {
            self.settled_ttl
        } else {
            self.hot_ttl
        };
        // Redis expiries are whole seconds; round up so short TTLs still cache.
        let seconds = ttl.as_secs() + u64::from(ttl.subsec_nanos() > 0);
        if let Err(error) = connection
            .set_ex(&key, encode_candles(&candles), seconds.max(1))
            .await
        {
            tracing::debug!(%error, "could not fill the redis candle cache");
        }
        Ok(candles)
    }
}

fn unavailable(error: redis::RedisError) -> StoreError {
    StoreError::Unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use orderflow_domain::{MarketId, Timestamp};
    use orderflow_marketdata::CandleInterval;

    use super::*;

    #[test]
    fn candle_keys_are_stable_and_distinct() {
        let query = CandleQuery {
            market: MarketId::parse("BTC-USD").unwrap(),
            interval: CandleInterval::FiveMinutes,
            limit: 100,
            end: None,
        };
        assert_eq!(
            candle_key(&query),
            "orderflow:candles:v1:BTC-USD:5m:100:now"
        );
        let settled = CandleQuery {
            end: Some(Timestamp::from_unix_nanos(60)),
            ..query
        };
        assert_eq!(
            candle_key(&settled),
            "orderflow:candles:v1:BTC-USD:5m:100:60"
        );
    }
}
