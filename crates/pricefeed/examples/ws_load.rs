#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! WebSocket fanout load test.
//!
//! Opens many connections to a running price feed, subscribes each one to
//! a set of markets and measures how many price frames arrive and how long
//! after publication (`published_at`) they reach the client.
//!
//! ```text
//! cargo run --release -p orderflow-pricefeed --example ws_load -- \
//!     --url ws://127.0.0.1:8090/v1/ws --connections 5000 --seconds 60
//! ```
//!
//! Every connection is a file descriptor on both sides; raise the limit
//! first (`ulimit -n 65536`) when testing thousands of them.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

/// Latency histogram bucket bounds in milliseconds.
const BOUNDS_MS: [u64; 12] = [1, 2, 5, 10, 20, 50, 100, 250, 500, 1_000, 2_500, 5_000];

#[derive(Default)]
struct Stats {
    connected: AtomicU64,
    failed: AtomicU64,
    frames: AtomicU64,
    buckets: [AtomicU64; BOUNDS_MS.len() + 1],
}

impl Stats {
    fn record(&self, latency_ms: u64) {
        let slot = BOUNDS_MS
            .iter()
            .position(|bound| latency_ms <= *bound)
            .unwrap_or(BOUNDS_MS.len());
        self.buckets[slot].fetch_add(1, Ordering::Relaxed);
        self.frames.fetch_add(1, Ordering::Relaxed);
    }

    fn percentile(&self, quantile: f64) -> String {
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        let total: u64 = counts.iter().sum();
        let target = (total as f64 * quantile).ceil() as u64;
        let mut seen = 0;
        for (index, count) in counts.iter().enumerate() {
            seen += count;
            if seen >= target && total > 0 {
                return BOUNDS_MS
                    .get(index)
                    .map_or_else(|| "> 5000 ms".to_owned(), |bound| format!("<= {bound} ms"));
            }
        }
        "n/a".to_owned()
    }
}

struct Args {
    url: String,
    connections: usize,
    seconds: u64,
    markets: Vec<String>,
}

fn args() -> Args {
    let mut args = Args {
        url: "ws://127.0.0.1:8090/v1/ws".into(),
        connections: 1_000,
        seconds: 30,
        markets: [
            "BTC-USD", "ETH-USD", "SOL-USD", "ETH-USDC", "BTC-USDC", "SOL-USDC",
        ]
        .map(String::from)
        .to_vec(),
    };
    let mut raw = std::env::args().skip(1);
    while let Some(flag) = raw.next() {
        let value = raw.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--url" => args.url = value,
            "--connections" => args.connections = value.parse().expect("--connections is a number"),
            "--seconds" => args.seconds = value.parse().expect("--seconds is a number"),
            "--markets" => args.markets = value.split(',').map(String::from).collect(),
            other => panic!("unknown flag {other}"),
        }
    }
    args
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

/// Milliseconds since the epoch of an RFC 3339 time like `...T22:08:42.123Z`.
fn parse_millis(text: &str) -> Option<u64> {
    let time = chrono::DateTime::parse_from_rfc3339(text).ok()?;
    u64::try_from(time.timestamp_millis()).ok()
}

/// The fields of a server message this tool reads, borrowed from the frame
/// instead of parsed into a full JSON tree.
#[derive(serde::Deserialize)]
struct Frame<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default)]
    market: &'a str,
    #[serde(default)]
    published_at: &'a str,
}

async fn client(url: String, subscribe: String, stats: Arc<Stats>, until: Instant) {
    // Small buffers, like the server's: the default 128 KiB per connection
    // would make the load generator itself the bottleneck.
    let config = WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(0);
    let Ok((mut socket, _)) = connect_async_with_config(url.as_str(), Some(config), true).await
    else {
        stats.failed.fetch_add(1, Ordering::Relaxed);
        return;
    };
    if socket.send(Message::text(subscribe)).await.is_err() {
        stats.failed.fetch_add(1, Ordering::Relaxed);
        return;
    }
    stats.connected.fetch_add(1, Ordering::Relaxed);
    // The first frame per market is the snapshot, which can be seconds old
    // by design; only the live frames after it measure latency.
    let mut snapshots: HashSet<String> = HashSet::new();
    let deadline = tokio::time::sleep_until(until.into());
    tokio::pin!(deadline);
    loop {
        let message = tokio::select! {
            () = &mut deadline => break,
            message = socket.next() => message,
        };
        let Some(Ok(Message::Text(text))) = message else {
            if message.is_none() {
                break;
            }
            continue;
        };
        let Ok(frame) = serde_json::from_str::<Frame<'_>>(text.as_str()) else {
            continue;
        };
        if frame.kind != "price" {
            continue;
        }
        if !snapshots.contains(frame.market) {
            snapshots.insert(frame.market.to_owned());
        } else if let Some(published) = parse_millis(frame.published_at) {
            stats.record(unix_millis().saturating_sub(published));
        }
    }
    let _ = socket.close(None).await;
}

#[tokio::main]
async fn main() {
    let args = args();
    let stats = Arc::new(Stats::default());
    let subscribe = serde_json::json!({ "op": "subscribe", "markets": args.markets }).to_string();
    let started = Instant::now();
    let until = started + Duration::from_secs(args.seconds);

    println!(
        "opening {} connections to {} for {}s, {} markets each",
        args.connections,
        args.url,
        args.seconds,
        args.markets.len()
    );
    let mut tasks = Vec::with_capacity(args.connections);
    for index in 0..args.connections {
        tasks.push(tokio::spawn(client(
            args.url.clone(),
            subscribe.clone(),
            stats.clone(),
            until,
        )));
        // Ramp up instead of a thundering herd of handshakes.
        if index % 200 == 199 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let reporter = {
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut last = 0;
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.tick().await;
            loop {
                tick.tick().await;
                let frames = stats.frames.load(Ordering::Relaxed);
                println!(
                    "  connected={} failed={} frames/s={}",
                    stats.connected.load(Ordering::Relaxed),
                    stats.failed.load(Ordering::Relaxed),
                    (frames - last) / 5
                );
                last = frames;
            }
        })
    };
    for task in tasks {
        let _ = task.await;
    }
    reporter.abort();

    let elapsed = started.elapsed().as_secs_f64();
    let frames = stats.frames.load(Ordering::Relaxed);
    println!("connected:  {}", stats.connected.load(Ordering::Relaxed));
    println!("failed:     {}", stats.failed.load(Ordering::Relaxed));
    println!("frames:     {frames} ({:.0}/s)", frames as f64 / elapsed);
    println!("latency p50 {}", stats.percentile(0.50));
    println!("latency p99 {}", stats.percentile(0.99));
}
