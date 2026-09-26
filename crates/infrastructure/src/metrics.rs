//! Prometheus text exposition of the runtime metrics.

use std::{
    collections::BTreeMap,
    fmt::Write,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use orderflow_application::{Metrics, REJECTED_OUTCOME};
use orderflow_domain::{MarketId, OrderStatus};

use crate::sync::lock;

/// Histogram bucket bounds in nanoseconds, from 1 microsecond to 5 ms.
/// The matching path is expected to sit in the low microseconds.
const LATENCY_BUCKETS_NS: [u64; 10] = [
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000,
];

#[derive(Debug, Default, Clone)]
struct Histogram {
    buckets: [u64; LATENCY_BUCKETS_NS.len()],
    count: u64,
    sum_ns: u64,
}

impl Histogram {
    fn observe(&mut self, nanos: u64) {
        for (bucket, bound) in self.buckets.iter_mut().zip(LATENCY_BUCKETS_NS) {
            if nanos <= bound {
                *bucket += 1;
            }
        }
        self.count += 1;
        self.sum_ns = self.sum_ns.saturating_add(nanos);
    }
}

/// In-process metrics registry rendered in the Prometheus text format.
///
/// Counters keyed by market sit behind short mutexes. Each market actor is
/// a single writer, so contention is limited to the scrape, which clones the
/// maps and formats outside the locks. Label values are market ids, which
/// the domain already restricts to `[A-Z0-9-]`, so no escaping is needed.
#[derive(Debug, Default)]
pub struct PrometheusMetrics {
    orders: Mutex<BTreeMap<(MarketId, &'static str), u64>>,
    trades: Mutex<BTreeMap<MarketId, u64>>,
    latency: Mutex<BTreeMap<MarketId, Histogram>>,
    events_published: AtomicU64,
    events_dropped: AtomicU64,
}

impl PrometheusMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn render(&self) -> String {
        let orders = lock(&self.orders).clone();
        let trades = lock(&self.trades).clone();
        let latency = lock(&self.latency).clone();
        let mut out = String::with_capacity(2048);

        header(
            &mut out,
            "orderflow_orders_total",
            "counter",
            "Orders processed by the matching engine, by resulting status.",
        );
        for ((market, outcome), value) in &orders {
            let _ = writeln!(
                out,
                "orderflow_orders_total{{market=\"{market}\",outcome=\"{outcome}\"}} {value}"
            );
        }

        header(
            &mut out,
            "orderflow_trades_total",
            "counter",
            "Trades executed.",
        );
        for (market, value) in &trades {
            let _ = writeln!(out, "orderflow_trades_total{{market=\"{market}\"}} {value}");
        }

        header(
            &mut out,
            "orderflow_engine_latency_seconds",
            "histogram",
            "Time the market actor spends per order, including the commit.",
        );
        for (market, histogram) in &latency {
            for (bound, count) in LATENCY_BUCKETS_NS.iter().zip(histogram.buckets) {
                let _ = writeln!(
                    out,
                    "orderflow_engine_latency_seconds_bucket{{market=\"{market}\",le=\"{}\"}} {count}",
                    seconds(*bound)
                );
            }
            let _ = writeln!(
                out,
                "orderflow_engine_latency_seconds_bucket{{market=\"{market}\",le=\"+Inf\"}} {}",
                histogram.count
            );
            let _ = writeln!(
                out,
                "orderflow_engine_latency_seconds_sum{{market=\"{market}\"}} {}",
                seconds(histogram.sum_ns)
            );
            let _ = writeln!(
                out,
                "orderflow_engine_latency_seconds_count{{market=\"{market}\"}} {}",
                histogram.count
            );
        }

        header(
            &mut out,
            "orderflow_events_published_total",
            "counter",
            "Domain events accepted by the event sink.",
        );
        let _ = writeln!(
            out,
            "orderflow_events_published_total {}",
            self.events_published.load(Ordering::Relaxed)
        );
        header(
            &mut out,
            "orderflow_events_dropped_total",
            "counter",
            "Domain events lost after the sink kept failing.",
        );
        let _ = writeln!(
            out,
            "orderflow_events_dropped_total {}",
            self.events_dropped.load(Ordering::Relaxed)
        );
        out
    }
}

impl Metrics for PrometheusMetrics {
    /// Creates every series of the market at zero. Prometheus computes
    /// `rate()` from the difference between samples, so a series whose
    /// first sample is already 3 would hide those first 3 trades.
    fn market_listed(&self, market: &MarketId) {
        let outcomes = OrderStatus::ALL
            .iter()
            .filter(|status| **status != OrderStatus::New)
            .map(|status| status.as_str())
            .chain([REJECTED_OUTCOME]);
        let mut orders = lock(&self.orders);
        for outcome in outcomes {
            orders.entry((market.clone(), outcome)).or_default();
        }
        lock(&self.trades).entry(market.clone()).or_default();
        lock(&self.latency).entry(market.clone()).or_default();
    }

    fn order_processed(&self, market: &MarketId, outcome: &'static str) {
        *lock(&self.orders)
            .entry((market.clone(), outcome))
            .or_default() += 1;
    }

    fn trades_executed(&self, market: &MarketId, count: usize) {
        if count > 0 {
            *lock(&self.trades).entry(market.clone()).or_default() += count as u64;
        }
    }

    fn engine_latency(&self, market: &MarketId, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        lock(&self.latency)
            .entry(market.clone())
            .or_default()
            .observe(nanos);
    }

    fn events_published(&self, count: usize) {
        self.events_published
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    fn events_dropped(&self, count: usize) {
        self.events_dropped
            .fetch_add(count as u64, Ordering::Relaxed);
    }
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// Formats nanoseconds as decimal seconds using integer math only.
fn seconds(nanos: u64) -> String {
    let mut text = format!("{}.{:09}", nanos / 1_000_000_000, nanos % 1_000_000_000);
    while text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.push('0');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_are_formatted_without_floats() {
        assert_eq!(seconds(1_000), "0.000001");
        assert_eq!(seconds(1_500_000_000), "1.5");
        assert_eq!(seconds(0), "0.0");
    }

    #[test]
    fn listed_markets_export_zero_series_before_any_order() {
        let metrics = PrometheusMetrics::new();
        metrics.market_listed(&MarketId::parse("SOL-USD").unwrap());
        let text = metrics.render();
        for outcome in [
            "pending",
            "open",
            "partially_filled",
            "filled",
            "cancelled",
            "rejected",
        ] {
            let line =
                format!("orderflow_orders_total{{market=\"SOL-USD\",outcome=\"{outcome}\"}} 0");
            assert!(text.contains(&line), "missing {line}");
        }
        assert!(!text.contains("outcome=\"new\""));
        assert!(text.contains("orderflow_trades_total{market=\"SOL-USD\"} 0"));
        assert!(text.contains("orderflow_engine_latency_seconds_count{market=\"SOL-USD\"} 0"));
    }

    #[test]
    fn render_contains_counters_and_cumulative_buckets() {
        let metrics = PrometheusMetrics::new();
        let market = MarketId::parse("BTC-USD").unwrap();
        metrics.order_processed(&market, "filled");
        metrics.order_processed(&market, "filled");
        metrics.trades_executed(&market, 3);
        metrics.engine_latency(&market, Duration::from_micros(7));
        metrics.events_published(5);

        let text = metrics.render();
        assert!(text.contains("orderflow_orders_total{market=\"BTC-USD\",outcome=\"filled\"} 2"));
        assert!(text.contains("orderflow_trades_total{market=\"BTC-USD\"} 3"));
        assert!(text.contains(
            "orderflow_engine_latency_seconds_bucket{market=\"BTC-USD\",le=\"0.000005\"} 0"
        ));
        assert!(text.contains(
            "orderflow_engine_latency_seconds_bucket{market=\"BTC-USD\",le=\"0.00001\"} 1"
        ));
        assert!(text.contains("orderflow_engine_latency_seconds_count{market=\"BTC-USD\"} 1"));
        assert!(text.contains("orderflow_events_published_total 5"));
    }
}
