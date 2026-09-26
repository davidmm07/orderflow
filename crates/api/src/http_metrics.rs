//! Request metrics for the HTTP layer, in the Prometheus text format.
//!
//! The engine metrics in the infrastructure crate see only orders that reach
//! a market. Requests rejected before that point (bad signatures, rate
//! limits, full queues, timeouts) are visible only here, which is why the
//! delivery layer keeps its own counters.

use std::{
    collections::BTreeMap,
    fmt::Write,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use axum::{
    extract::{MatchedPath, Request, State},
    http::Method,
    middleware::Next,
    response::Response,
};

/// Stamped on every problem response by `ApiError`, so the middleware can
/// count failures by their stable code.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProblemCode(pub &'static str);

/// Request duration bucket bounds in microseconds, from 250 us to 2.5 s.
const DURATION_BUCKETS_US: [u64; 10] = [
    250, 500, 1_000, 2_500, 5_000, 10_000, 50_000, 250_000, 1_000_000, 2_500_000,
];

/// Label used for requests that matched no route. Raw paths are never used
/// as labels: a client could otherwise create unlimited time series by
/// requesting random URLs.
const UNMATCHED: &str = "unmatched";

#[derive(Debug, Default, Clone)]
struct Histogram {
    buckets: [u64; DURATION_BUCKETS_US.len()],
    count: u64,
    sum_us: u64,
}

impl Histogram {
    fn observe(&mut self, micros: u64) {
        for (bucket, bound) in self.buckets.iter_mut().zip(DURATION_BUCKETS_US) {
            if micros <= bound {
                *bucket += 1;
            }
        }
        self.count += 1;
        self.sum_us = self.sum_us.saturating_add(micros);
    }
}

type RequestKey = (&'static str, String, u16);

/// Counters and latency histograms keyed by route template.
///
/// Every label has a bounded set of values: methods are folded into a fixed
/// list, routes are templates such as `/v1/markets/{market}/orders`, and
/// problem codes are compile-time constants.
#[derive(Debug, Default)]
pub struct HttpMetrics {
    requests: Mutex<BTreeMap<RequestKey, u64>>,
    problems: Mutex<BTreeMap<(String, &'static str), u64>>,
    durations: Mutex<BTreeMap<String, Histogram>>,
}

impl HttpMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    fn observe(
        &self,
        method: &'static str,
        route: &str,
        status: u16,
        problem: Option<&'static str>,
        elapsed: Duration,
    ) {
        *lock(&self.requests)
            .entry((method, route.to_owned(), status))
            .or_default() += 1;
        if let Some(code) = problem {
            *lock(&self.problems)
                .entry((route.to_owned(), code))
                .or_default() += 1;
        }
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        lock(&self.durations)
            .entry(route.to_owned())
            .or_default()
            .observe(micros);
    }

    pub fn render(&self) -> String {
        let requests = lock(&self.requests).clone();
        let problems = lock(&self.problems).clone();
        let durations = lock(&self.durations).clone();
        let mut out = String::with_capacity(4096);

        header(
            &mut out,
            "orderflow_http_requests_total",
            "counter",
            "HTTP requests by method, route template and status code.",
        );
        for ((method, route, status), value) in &requests {
            let _ = writeln!(
                out,
                "orderflow_http_requests_total{{method=\"{method}\",route=\"{route}\",status=\"{status}\"}} {value}"
            );
        }

        header(
            &mut out,
            "orderflow_http_problems_total",
            "counter",
            "Problem responses by route template and problem code.",
        );
        for ((route, code), value) in &problems {
            let _ = writeln!(
                out,
                "orderflow_http_problems_total{{route=\"{route}\",code=\"{code}\"}} {value}"
            );
        }

        header(
            &mut out,
            "orderflow_http_request_duration_seconds",
            "histogram",
            "Time from receiving a request to sending its response.",
        );
        for (route, histogram) in &durations {
            for (bound, count) in DURATION_BUCKETS_US.iter().zip(histogram.buckets) {
                let _ = writeln!(
                    out,
                    "orderflow_http_request_duration_seconds_bucket{{route=\"{route}\",le=\"{}\"}} {count}",
                    seconds(*bound)
                );
            }
            let _ = writeln!(
                out,
                "orderflow_http_request_duration_seconds_bucket{{route=\"{route}\",le=\"+Inf\"}} {}",
                histogram.count
            );
            let _ = writeln!(
                out,
                "orderflow_http_request_duration_seconds_sum{{route=\"{route}\"}} {}",
                seconds(histogram.sum_us)
            );
            let _ = writeln!(
                out,
                "orderflow_http_request_duration_seconds_count{{route=\"{route}\"}} {}",
                histogram.count
            );
        }
        out
    }
}

/// Middleware that records every request once its response is ready.
///
/// It sits outside the panic guard and the timeout, so it sees the status
/// the client actually receives, including 500s from panics and 503s from
/// timeouts.
pub(crate) async fn record(
    State(metrics): State<Arc<HttpMetrics>>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = method_label(request.method());
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or(UNMATCHED, MatchedPath::as_str)
        .to_owned();
    let response = next.run(request).await;
    let problem = response
        .extensions()
        .get::<ProblemCode>()
        .map(|code| code.0);
    metrics.observe(
        method,
        &route,
        response.status().as_u16(),
        problem,
        started.elapsed(),
    );
    response
}

/// Folds request methods into a fixed set; custom methods would otherwise
/// be an unbounded label.
fn method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::PATCH => "PATCH",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        _ => "OTHER",
    }
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// Formats microseconds as decimal seconds using integer math only.
fn seconds(micros: u64) -> String {
    let mut text = format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000);
    while text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.push('0');
    }
    text
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_methods_share_one_label() {
        let custom = Method::from_bytes(b"PURGE").unwrap();
        assert_eq!(method_label(&custom), "OTHER");
        assert_eq!(method_label(&Method::DELETE), "DELETE");
    }

    #[test]
    fn render_reports_counters_problems_and_cumulative_buckets() {
        let metrics = HttpMetrics::new();
        let route = "/v1/markets/{market}/orders";
        metrics.observe("POST", route, 201, None, Duration::from_micros(700));
        metrics.observe(
            "POST",
            route,
            401,
            Some("unauthenticated"),
            Duration::from_micros(300),
        );

        let text = metrics.render();
        assert!(text.contains(
            "orderflow_http_requests_total{method=\"POST\",route=\"/v1/markets/{market}/orders\",status=\"401\"} 1"
        ));
        assert!(text.contains(
            "orderflow_http_problems_total{route=\"/v1/markets/{market}/orders\",code=\"unauthenticated\"} 1"
        ));
        assert!(text.contains(
            "orderflow_http_request_duration_seconds_bucket{route=\"/v1/markets/{market}/orders\",le=\"0.0005\"} 1"
        ));
        assert!(text.contains(
            "orderflow_http_request_duration_seconds_bucket{route=\"/v1/markets/{market}/orders\",le=\"0.001\"} 2"
        ));
        assert!(text.contains("orderflow_http_request_duration_seconds_sum{route=\"/v1/markets/{market}/orders\"} 0.001"));
    }

    #[test]
    fn seconds_are_formatted_without_floats() {
        assert_eq!(seconds(250), "0.00025");
        assert_eq!(seconds(2_500_000), "2.5");
        assert_eq!(seconds(0), "0.0");
    }
}
