//! Prometheus metrics for the price feed, in the text exposition format.
//!
//! Counters and gauges live in a small registry keyed by metric name and
//! label set. Every label value comes from a bounded set (configured
//! markets and sources, fixed reason codes, route templates), never from
//! client input, so the number of series cannot grow without limit.

use std::{
    collections::BTreeMap,
    fmt::Write,
    sync::{
        Mutex, MutexGuard, PoisonError,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use orderflow_application::Clock;
use orderflow_marketdata::{
    PriceStatus, PriceUpdate, QuoteState, Rejection, SourceId, ports::MarketDataMetrics,
};

use crate::hub::PriceHub;

/// Why a WebSocket connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disconnect {
    /// The client closed the connection.
    Client,
    /// The client could not keep up with sends.
    SlowConsumer,
    /// No message or pong within the idle timeout.
    Idle,
    /// Too many control messages, or other protocol abuse.
    Policy,
    /// The server is shutting down.
    Shutdown,
    /// A transport error.
    Error,
}

impl Disconnect {
    pub const ALL: [Self; 6] = [
        Self::Client,
        Self::SlowConsumer,
        Self::Idle,
        Self::Policy,
        Self::Shutdown,
        Self::Error,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::SlowConsumer => "slow_consumer",
            Self::Idle => "idle",
            Self::Policy => "policy",
            Self::Shutdown => "shutdown",
            Self::Error => "error",
        }
    }
}

struct Family {
    kind: &'static str,
    help: &'static str,
    series: BTreeMap<String, u64>,
}

/// All metrics of one process.
///
/// Counters on the fanout path (one increment per frame sent, so tens of
/// thousands per second) are plain atomics. Taking the registry lock there
/// would make every WebSocket session queue behind one mutex on each
/// price update.
pub struct PricefeedMetrics {
    families: Mutex<BTreeMap<&'static str, Family>>,
    ws_connections: AtomicI64,
    ws_subscriptions: AtomicI64,
    ws_sent: AtomicU64,
    ws_conflated: AtomicU64,
    leader: AtomicI64,
}

impl PricefeedMetrics {
    /// Creates the registry with every known series at zero. A counter that
    /// first appears above zero hides that first increase from `rate()`.
    pub fn new(markets: &[orderflow_domain::MarketId], sources: &[SourceId]) -> Self {
        let metrics = Self {
            families: Mutex::default(),
            ws_connections: AtomicI64::new(0),
            ws_subscriptions: AtomicI64::new(0),
            ws_sent: AtomicU64::new(0),
            ws_conflated: AtomicU64::new(0),
            leader: AtomicI64::new(0),
        };
        for market in markets {
            for status in PriceStatus::ALL {
                metrics.add(
                    INDEX_UPDATES,
                    &[("market", market.as_str()), ("status", status.as_str())],
                    0,
                );
            }
        }
        for source in sources {
            metrics.add(OBSERVATIONS, &[("source", source.as_str())], 0);
            for rejection in Rejection::ALL {
                metrics.add(
                    REJECTED,
                    &[("source", source.as_str()), ("reason", rejection.as_str())],
                    0,
                );
            }
        }
        for reason in Disconnect::ALL {
            metrics.add(WS_DISCONNECTS, &[("reason", reason.as_str())], 0);
        }
        for result in ["accepted", "rejected"] {
            metrics.add(WS_CONNECTIONS_TOTAL, &[("result", result)], 0);
        }
        for tier in ["memory", "redis"] {
            for result in ["hit", "miss"] {
                metrics.add(CACHE, &[("tier", tier), ("result", result)], 0);
            }
        }
        for name in [STORE_DROPPED, STORE_FAILURES, BUS_PUBLISHED, BUS_FAILURES] {
            metrics.add(name, &[], 0);
        }
        metrics
    }

    fn add(&self, name: &'static str, labels: &[(&str, &str)], delta: u64) {
        let Some((kind, help)) = describe(name) else {
            return;
        };
        let mut families = lock(&self.families);
        let family = families.entry(name).or_insert_with(|| Family {
            kind,
            help,
            series: BTreeMap::new(),
        });
        *family.series.entry(label_string(labels)).or_default() += delta;
    }

    fn set(&self, name: &'static str, labels: &[(&str, &str)], value: u64) {
        let Some((kind, help)) = describe(name) else {
            return;
        };
        let mut families = lock(&self.families);
        let family = families.entry(name).or_insert_with(|| Family {
            kind,
            help,
            series: BTreeMap::new(),
        });
        family.series.insert(label_string(labels), value);
    }

    pub fn ws_connection_opened(&self) {
        self.ws_connections.fetch_add(1, Ordering::Relaxed);
        self.add(WS_CONNECTIONS_TOTAL, &[("result", "accepted")], 1);
    }

    pub fn ws_connection_rejected(&self) {
        self.add(WS_CONNECTIONS_TOTAL, &[("result", "rejected")], 1);
    }

    pub fn ws_connection_closed(&self, reason: Disconnect, subscriptions: usize) {
        self.ws_connections.fetch_sub(1, Ordering::Relaxed);
        self.ws_subscriptions
            .fetch_sub(i64::try_from(subscriptions).unwrap_or(0), Ordering::Relaxed);
        self.add(WS_DISCONNECTS, &[("reason", reason.as_str())], 1);
    }

    pub fn ws_subscriptions_changed(&self, delta: i64) {
        self.ws_subscriptions.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn ws_sent(&self, frames: u64) {
        self.ws_sent.fetch_add(frames, Ordering::Relaxed);
    }

    /// Updates a slow client skipped because only the newest is kept.
    pub fn ws_conflated(&self, skipped: u64) {
        self.ws_conflated.fetch_add(skipped, Ordering::Relaxed);
    }

    pub fn http_request(&self, route: &str, status: u16) {
        self.add(
            HTTP_REQUESTS,
            &[("route", route), ("status", &status.to_string())],
            1,
        );
    }

    pub fn set_leader(&self, leader: bool) {
        self.leader.store(i64::from(leader), Ordering::Relaxed);
    }

    /// Renders every metric, plus gauges derived from the hub's prices.
    pub fn render(&self, hub: &PriceHub, clock: &dyn Clock) -> String {
        let mut out = String::with_capacity(16 * 1024);
        {
            let families = lock(&self.families);
            for (name, family) in families.iter() {
                header(&mut out, name, family.kind, family.help);
                for (labels, value) in &family.series {
                    let _ = writeln!(out, "{name}{labels} {value}");
                }
            }
        }
        counter(
            &mut out,
            WS_SENT,
            "Price frames sent to WebSocket clients.",
            self.ws_sent.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            WS_CONFLATED,
            "Price updates slow clients skipped because only the newest is sent.",
            self.ws_conflated.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "pricefeed_ws_connections",
            "Open WebSocket connections.",
            self.ws_connections.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "pricefeed_ws_subscriptions",
            "Market subscriptions across all connections.",
            self.ws_subscriptions.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "pricefeed_leader",
            "1 while this process holds the ingest lease.",
            self.leader.load(Ordering::Relaxed),
        );
        render_index(&mut out, hub, clock);
        out
    }
}

/// Status, age and venue deviation of each market's latest index price.
fn render_index(out: &mut String, hub: &PriceHub, clock: &dyn Clock) {
    let now = clock.now().unix_nanos();
    let snapshot = hub.snapshot();
    header(
        out,
        "pricefeed_index_status",
        "gauge",
        "1 for the current status of each market's index price.",
    );
    for update in &snapshot {
        for status in PriceStatus::ALL {
            let _ = writeln!(
                out,
                "pricefeed_index_status{{market=\"{}\",status=\"{}\"}} {}",
                update.market,
                status.as_str(),
                u8::from(update.status == status)
            );
        }
    }
    header(
        out,
        "pricefeed_index_age_seconds",
        "gauge",
        "Seconds since the newest quote behind each index price.",
    );
    for update in &snapshot {
        let age = now.saturating_sub(update.as_of.unix_nanos()) / 1_000_000;
        let _ = writeln!(
            out,
            "pricefeed_index_age_seconds{{market=\"{}\"}} {}.{:03}",
            update.market,
            age / 1_000,
            age % 1_000
        );
    }
    header(
        out,
        "pricefeed_monitor_deviation_bps",
        "gauge",
        "Distance of monitor sources (such as Orderflow itself) from the index, in basis points.",
    );
    for update in &snapshot {
        for component in update
            .components
            .iter()
            .filter(|c| c.state == QuoteState::Monitor)
        {
            if let Some(bps) = component.deviation_bps {
                let _ = writeln!(
                    out,
                    "pricefeed_monitor_deviation_bps{{market=\"{}\",source=\"{}\"}} {bps}",
                    update.market, component.source
                );
            }
        }
    }
}

impl MarketDataMetrics for PricefeedMetrics {
    fn observation_accepted(&self, source: &SourceId) {
        self.add(OBSERVATIONS, &[("source", source.as_str())], 1);
    }

    fn observation_rejected(&self, source: &SourceId, reason: &'static str) {
        self.add(
            REJECTED,
            &[("source", source.as_str()), ("reason", reason)],
            1,
        );
    }

    fn source_error(&self, connector: &str, kind: &'static str) {
        self.add(
            SOURCE_ERRORS,
            &[("connector", connector), ("kind", kind)],
            1,
        );
    }

    fn index_published(&self, update: &PriceUpdate) {
        self.add(
            INDEX_UPDATES,
            &[
                ("market", update.market.as_str()),
                ("status", update.status.as_str()),
            ],
            1,
        );
    }

    fn store_written(&self, updates: usize, elapsed: Duration) {
        self.add(STORE_WRITTEN, &[], updates as u64);
        self.add(
            STORE_WRITE_MICROS,
            &[],
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
        );
        self.add(STORE_BATCHES, &[], 1);
    }

    fn store_failed(&self) {
        self.add(STORE_FAILURES, &[], 1);
    }

    fn store_dropped(&self, updates: usize) {
        self.add(STORE_DROPPED, &[], updates as u64);
    }

    fn bus_published(&self, updates: usize) {
        self.add(BUS_PUBLISHED, &[], updates as u64);
    }

    fn bus_failed(&self) {
        self.add(BUS_FAILURES, &[], 1);
    }

    fn indexer_progress(&self, indexer: &str, head: u64, cursor: u64) {
        self.set(INDEXER_HEAD, &[("indexer", indexer)], head);
        self.set(INDEXER_CURSOR, &[("indexer", indexer)], cursor);
    }

    fn indexer_reorg(&self, indexer: &str, depth: u64) {
        self.add(INDEXER_REORGS, &[("indexer", indexer)], 1);
        self.set(INDEXER_REORG_DEPTH, &[("indexer", indexer)], depth);
    }

    fn indexer_swaps(&self, indexer: &str, count: usize) {
        self.add(INDEXER_SWAPS, &[("indexer", indexer)], count as u64);
    }

    fn cache_lookup(&self, tier: &'static str, hit: bool) {
        self.add(
            CACHE,
            &[("tier", tier), ("result", if hit { "hit" } else { "miss" })],
            1,
        );
    }
}

const OBSERVATIONS: &str = "pricefeed_observations_accepted_total";
const REJECTED: &str = "pricefeed_observations_rejected_total";
const SOURCE_ERRORS: &str = "pricefeed_source_errors_total";
const INDEX_UPDATES: &str = "pricefeed_index_updates_total";
const STORE_WRITTEN: &str = "pricefeed_store_updates_written_total";
const STORE_BATCHES: &str = "pricefeed_store_batches_total";
const STORE_WRITE_MICROS: &str = "pricefeed_store_write_microseconds_total";
const STORE_FAILURES: &str = "pricefeed_store_failures_total";
const STORE_DROPPED: &str = "pricefeed_store_dropped_total";
const BUS_PUBLISHED: &str = "pricefeed_bus_published_total";
const BUS_FAILURES: &str = "pricefeed_bus_failures_total";
const INDEXER_HEAD: &str = "pricefeed_indexer_head_block";
const INDEXER_CURSOR: &str = "pricefeed_indexer_cursor_block";
const INDEXER_REORGS: &str = "pricefeed_indexer_reorgs_total";
const INDEXER_REORG_DEPTH: &str = "pricefeed_indexer_last_reorg_depth_blocks";
const INDEXER_SWAPS: &str = "pricefeed_indexer_swaps_total";
const CACHE: &str = "pricefeed_cache_requests_total";
const WS_CONNECTIONS_TOTAL: &str = "pricefeed_ws_connections_total";
const WS_DISCONNECTS: &str = "pricefeed_ws_disconnects_total";
const WS_SENT: &str = "pricefeed_ws_messages_sent_total";
const WS_CONFLATED: &str = "pricefeed_ws_conflated_updates_total";
const HTTP_REQUESTS: &str = "pricefeed_http_requests_total";

fn describe(name: &str) -> Option<(&'static str, &'static str)> {
    Some(match name {
        OBSERVATIONS => (
            "counter",
            "Observations accepted into the index, by source.",
        ),
        REJECTED => (
            "counter",
            "Observations rejected by the index, by source and reason.",
        ),
        SOURCE_ERRORS => (
            "counter",
            "Source failures that led to a restart, by connector and kind.",
        ),
        INDEX_UPDATES => ("counter", "Index prices published, by market and status."),
        STORE_WRITTEN => ("counter", "Price updates written to the store."),
        STORE_BATCHES => ("counter", "Store write batches."),
        STORE_WRITE_MICROS => ("counter", "Time spent in store writes, in microseconds."),
        STORE_FAILURES => ("counter", "Failed store writes, including retried ones."),
        STORE_DROPPED => (
            "counter",
            "Price updates never stored because the store was down or the queue full.",
        ),
        BUS_PUBLISHED => ("counter", "Updates published to the price bus."),
        BUS_FAILURES => (
            "counter",
            "Failed price bus operations, publishing or subscribing.",
        ),
        INDEXER_HEAD => ("gauge", "Chain head seen by each indexer."),
        INDEXER_CURSOR => ("gauge", "Last block or slot each indexer has processed."),
        INDEXER_REORGS => ("counter", "Chain reorganizations each indexer rolled back."),
        INDEXER_REORG_DEPTH => (
            "gauge",
            "Blocks rolled back by each indexer's most recent reorg.",
        ),
        INDEXER_SWAPS => ("counter", "DEX swaps indexed."),
        CACHE => ("counter", "Candle reads by cache tier and result."),
        WS_CONNECTIONS_TOTAL => (
            "counter",
            "WebSocket upgrade attempts, accepted or rejected at the limit.",
        ),
        WS_DISCONNECTS => ("counter", "Closed WebSocket connections by reason."),
        HTTP_REQUESTS => ("counter", "HTTP requests by route template and status."),
        _ => return None,
    })
}

fn label_string(labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let body: Vec<String> = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{}\"", escape(value)))
        .collect();
    format!("{{{}}}", body.join(","))
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    header(out, name, "counter", help);
    let _ = writeln!(out, "{name} {value}");
}

fn gauge(out: &mut String, name: &str, help: &str, value: i64) {
    header(out, name, "gauge", help);
    let _ = writeln!(out, "{name} {value}");
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use orderflow_domain::{MarketId, Timestamp};
    use orderflow_marketdata::{Component, SourceKind};
    use rust_decimal_macros::dec;

    use super::*;

    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            Timestamp::from_unix_nanos(10_500_000_000)
        }
    }

    #[test]
    fn known_series_start_at_zero_and_count_up() {
        let market = MarketId::parse("BTC-USD").unwrap();
        let kraken = SourceId::parse("kraken").unwrap();
        let metrics =
            PricefeedMetrics::new(std::slice::from_ref(&market), std::slice::from_ref(&kraken));
        let hub = PriceHub::new([market.clone()]);
        let text = metrics.render(&hub, &FixedClock);
        assert!(
            text.contains("pricefeed_index_updates_total{market=\"BTC-USD\",status=\"live\"} 0")
        );
        assert!(text.contains(
            "pricefeed_observations_rejected_total{source=\"kraken\",reason=\"too_old\"} 0"
        ));
        assert!(text.contains("pricefeed_ws_disconnects_total{reason=\"slow_consumer\"} 0"));

        metrics.observation_accepted(&kraken);
        metrics.cache_lookup("memory", true);
        let text = metrics.render(&hub, &FixedClock);
        assert!(text.contains("pricefeed_observations_accepted_total{source=\"kraken\"} 1"));
        assert!(
            text.contains("pricefeed_cache_requests_total{result=\"hit\",tier=\"memory\"} 1")
                || text
                    .contains("pricefeed_cache_requests_total{tier=\"memory\",result=\"hit\"} 1")
        );
    }

    #[test]
    fn index_gauges_come_from_the_hub() {
        let market = MarketId::parse("BTC-USD").unwrap();
        let metrics = PricefeedMetrics::new(std::slice::from_ref(&market), &[]);
        let hub = PriceHub::new([market.clone()]);
        hub.publish(Arc::new(PriceUpdate {
            market,
            epoch: 1,
            sequence: 1,
            price: dec!(100),
            status: PriceStatus::Degraded,
            as_of: Timestamp::from_unix_nanos(8_000_000_000),
            published_at: Timestamp::from_unix_nanos(8_000_000_000),
            spread_bps: dec!(0),
            components: vec![Component {
                source: SourceId::parse("orderflow").unwrap(),
                kind: SourceKind::Venue,
                price: dec!(101),
                observed_at: Timestamp::default(),
                state: QuoteState::Monitor,
                deviation_bps: Some(dec!(100)),
            }],
        }));
        let text = metrics.render(&hub, &FixedClock);
        assert!(text.contains("pricefeed_index_status{market=\"BTC-USD\",status=\"degraded\"} 1"));
        assert!(text.contains("pricefeed_index_status{market=\"BTC-USD\",status=\"live\"} 0"));
        assert!(text.contains("pricefeed_index_age_seconds{market=\"BTC-USD\"} 2.500"));
        assert!(text.contains(
            "pricefeed_monitor_deviation_bps{market=\"BTC-USD\",source=\"orderflow\"} 100"
        ));
    }
}
