#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! The price feed's REST and WebSocket API, end to end in process.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use orderflow_application::Clock;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{
    Component, PriceStatus, PriceUpdate, QuoteState, SourceId, SourceKind, fold_minute_candles,
    ports::{NoopMetrics, PriceStore},
};
use orderflow_pricefeed::{
    config::WsLimits, http::router, hub::PriceHub, metrics::PricefeedMetrics, state::AppState,
};
use orderflow_storage::{MemoryCandleCache, MemoryStore};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::Value;
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, protocol::frame::coding::CloseCode},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::ServiceExt;

const MINUTE: u64 = 60_000_000_000;
/// "Now" for every test: 10:30 on day one of the epoch.
const NOW: u64 = 630 * MINUTE;

struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_unix_nanos(NOW)
    }
}

fn market(id: &str) -> MarketId {
    MarketId::parse(id).unwrap()
}

fn update(market_id: &str, sequence: u64, price: Decimal) -> Arc<PriceUpdate> {
    let at = Timestamp::from_unix_nanos(NOW - 2 * MINUTE + sequence * 1_000_000_000);
    Arc::new(PriceUpdate {
        market: market(market_id),
        epoch: 7,
        sequence,
        price,
        status: PriceStatus::Live,
        as_of: at,
        published_at: at,
        spread_bps: dec!(4.5),
        components: vec![
            Component {
                source: SourceId::parse("kraken").unwrap(),
                kind: SourceKind::Cex,
                price,
                observed_at: at,
                state: QuoteState::Used,
                deviation_bps: Some(Decimal::ZERO),
            },
            Component {
                source: SourceId::parse("orderflow").unwrap(),
                kind: SourceKind::Venue,
                price: price + Decimal::ONE,
                observed_at: at,
                state: QuoteState::Monitor,
                deviation_bps: Some(dec!(1.2)),
            },
        ],
    })
}

struct Harness {
    state: AppState,
    store: Arc<MemoryStore>,
}

impl Harness {
    fn new(limits: WsLimits) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(FixedClock);
        let markets = vec![market("BTC-USD"), market("ETH-USD")];
        let store = Arc::new(MemoryStore::new(clock.clone()));
        let candles = Arc::new(MemoryCandleCache::new(
            store.clone(),
            clock.clone(),
            Arc::new(NoopMetrics),
            100,
            Duration::from_millis(1),
            Duration::from_millis(1),
        ));
        let state = AppState {
            hub: Arc::new(PriceHub::new(markets.clone())),
            candles,
            metrics: Arc::new(PricefeedMetrics::new(&markets, &[])),
            clock,
            ws: limits,
            connections: Arc::new(Semaphore::new(limits.max_connections)),
            shutdown: CancellationToken::new(),
            sessions: TaskTracker::new(),
            ready: Arc::new(AtomicBool::new(true)),
        };
        Self { state, store }
    }

    fn router(&self) -> Router {
        router(self.state.clone(), Duration::from_secs(5))
    }

    async fn get(&self, uri: &str) -> (StatusCode, axum::http::HeaderMap, Value) {
        let response = self
            .router()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (parts.status, parts.headers, json)
    }

    /// Serves the router on a random local port, returns the WebSocket URL.
    async fn serve(&self) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = self.router();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("ws://{address}/v1/ws")
    }
}

fn limits() -> WsLimits {
    WsLimits {
        max_connections: 10,
        max_subscriptions: 2,
        max_message_bytes: 4_096,
        send_timeout: Duration::from_secs(2),
        ping_interval: Duration::from_secs(20),
        idle_timeout: Duration::from_secs(60),
        max_control_messages: 20,
    }
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn next_json(client: &mut Client) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("a message within 5s")
            .expect("stream open")
            .unwrap();
        if let Message::Text(text) = message {
            return serde_json::from_str(text.as_str()).unwrap();
        }
    }
}

async fn send(client: &mut Client, json: &str) {
    client.send(Message::text(json.to_owned())).await.unwrap();
}

#[tokio::test]
async fn prices_list_filter_and_cache_headers() {
    let harness = Harness::new(limits());
    harness
        .state
        .hub
        .publish(update("BTC-USD", 1, dec!(81650.5)));
    harness
        .state
        .hub
        .publish(update("ETH-USD", 1, dec!(2478.95)));

    let (status, headers, body) = harness.get("/v1/prices").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["cache-control"], "public, max-age=1");
    assert_eq!(body["prices"].as_array().unwrap().len(), 2);
    assert_eq!(body["prices"][0]["market"], "BTC-USD");
    assert_eq!(body["prices"][0]["price"], "81650.5");
    assert_eq!(body["prices"][0]["sources_used"], 1);

    let (_, _, body) = harness.get("/v1/prices?markets=ETH-USD").await;
    assert_eq!(body["prices"].as_array().unwrap().len(), 1);

    let (status, headers, body) = harness.get("/v1/prices?markets=eth").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(headers["content-type"], "application/problem+json");
    assert_eq!(body["errors"][0]["code"], "invalid_market");

    let (status, _, body) = harness.get("/v1/prices?markets=XYZ-USD").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "market_not_found");

    let (status, _, body) = harness.get("/v1/prices?market=BTC-USD").await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unknown query parameters are refused"
    );
    assert_eq!(body["errors"][0]["code"], "invalid_query");
}

#[tokio::test]
async fn price_detail_explains_the_index() {
    let harness = Harness::new(limits());
    harness
        .state
        .hub
        .publish(update("BTC-USD", 3, dec!(81650.5)));

    let (status, _, body) = harness.get("/v1/prices/BTC-USD").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["sequence"], 3);
    assert_eq!(body["components"][0]["source"], "kraken");
    assert_eq!(body["components"][1]["state"], "monitor");
    assert_eq!(body["components"][1]["deviation_bps"], "1.2");

    let (status, headers, body) = harness.get("/v1/prices/ETH-USD").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "price_unavailable");
    assert_eq!(headers["retry-after"], "1");

    let (_, _, sources) = harness.get("/v1/sources").await;
    assert_eq!(sources["sources"][0]["source"], "kraken");
    assert_eq!(sources["sources"][0]["used_in"], 1);
}

#[tokio::test]
async fn candles_are_validated_and_served() {
    let harness = Harness::new(limits());
    let updates: Vec<Arc<PriceUpdate>> = (1..=90)
        .map(|sequence| update("BTC-USD", sequence, Decimal::from(81_000 + sequence)))
        .collect();
    harness
        .store
        .record(&updates, &fold_minute_candles(&updates))
        .await
        .unwrap();

    let (status, headers, body) = harness
        .get("/v1/prices/BTC-USD/candles?interval=1m&limit=5")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["cache-control"], "public, max-age=1");
    let candles = body["candles"].as_array().unwrap();
    assert_eq!(candles.len(), 2, "updates span two minutes");
    assert_eq!(candles[0]["open"], "81001");
    assert_eq!(candles[1]["close"], "81090");

    let (_, headers, body) = harness
        .get("/v1/prices/BTC-USD/candles?interval=5m&limit=2&end=1970-01-01T10:25:00Z")
        .await;
    assert_eq!(
        headers["cache-control"], "public, max-age=60",
        "a closed window can be cached longer"
    );
    assert_eq!(body["interval"], "5m");

    let (status, _, body) = harness
        .get("/v1/prices/BTC-USD/candles?interval=2m&limit=0&end=yesterday")
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let fields: Vec<&str> = body["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["field"].as_str().unwrap())
        .collect();
    assert_eq!(fields, ["interval", "limit", "end"]);
}

#[tokio::test]
async fn health_metrics_and_unknown_routes() {
    let harness = Harness::new(limits());
    let (status, _, _) = harness.get("/health/ready").await;
    assert_eq!(status, StatusCode::OK);
    harness.state.ready.store(false, Ordering::SeqCst);
    let (status, _, body) = harness.get("/health/ready").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "not_ready");

    let (status, _, body) = harness.get("/v2/anything").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "route_not_found");

    let response = harness
        .router()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let text = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.contains("pricefeed_http_requests_total{route=\"/health/ready\",status=\"503\"} 1"),
        "{text}"
    );
    assert!(text.contains("pricefeed_http_requests_total{route=\"unmatched\",status=\"404\"} 1"));
}

#[tokio::test]
async fn websocket_subscribers_get_a_snapshot_then_live_prices() {
    let harness = Harness::new(limits());
    harness
        .state
        .hub
        .publish(update("BTC-USD", 1, dec!(81650.5)));
    let url = harness.serve().await;
    let (mut client, _) = connect_async(url).await.unwrap();

    let welcome = next_json(&mut client).await;
    assert_eq!(welcome["type"], "welcome");
    assert_eq!(
        welcome["markets"],
        serde_json::json!(["BTC-USD", "ETH-USD"])
    );

    send(
        &mut client,
        r#"{"op":"subscribe","markets":["BTC-USD","ETH-USD"]}"#,
    )
    .await;
    assert_eq!(
        next_json(&mut client).await["markets"],
        serde_json::json!(["BTC-USD", "ETH-USD"])
    );
    let snapshot = next_json(&mut client).await;
    assert_eq!(
        (snapshot["type"].as_str(), snapshot["sequence"].as_u64()),
        (Some("price"), Some(1))
    );

    harness
        .state
        .hub
        .publish(update("ETH-USD", 1, dec!(2478.95)));
    harness.state.hub.publish(update("BTC-USD", 2, dec!(81651)));
    let mut live = Vec::new();
    for _ in 0..2 {
        let message = next_json(&mut client).await;
        live.push((
            message["market"].as_str().unwrap().to_owned(),
            message["sequence"].as_u64().unwrap(),
        ));
    }
    live.sort();
    assert_eq!(live, [("BTC-USD".to_owned(), 2), ("ETH-USD".to_owned(), 1)]);

    send(&mut client, r#"{"op":"unsubscribe","markets":["BTC-USD"]}"#).await;
    assert_eq!(
        next_json(&mut client).await["markets"],
        serde_json::json!(["ETH-USD"])
    );
    send(&mut client, r#"{"op":"ping"}"#).await;
    assert_eq!(next_json(&mut client).await["type"], "pong");
}

#[tokio::test]
async fn websocket_requests_are_validated() {
    let harness = Harness::new(limits());
    let url = harness.serve().await;
    let (mut client, _) = connect_async(url).await.unwrap();
    next_json(&mut client).await;

    for (request, code) in [
        ("hello", "invalid_message"),
        (
            r#"{"op":"subscribe","markets":["<script>"]}"#,
            "invalid_market",
        ),
        (
            r#"{"op":"subscribe","markets":["XYZ-USD"]}"#,
            "unknown_market",
        ),
        (
            r#"{"op":"subscribe","markets":["BTC-USD","ETH-USD","SOL-USD"]}"#,
            "too_many_subscriptions",
        ),
    ] {
        send(&mut client, request).await;
        let reply = next_json(&mut client).await;
        assert_eq!(reply["code"], code, "{request}");
        assert!(
            !reply.to_string().contains("<script>"),
            "invalid input is never echoed"
        );
    }
}

#[tokio::test]
async fn a_full_node_turns_new_websockets_away() {
    let harness = Harness::new(WsLimits {
        max_connections: 1,
        ..limits()
    });
    let url = harness.serve().await;
    let (_first, _) = connect_async(url.clone()).await.unwrap();
    let error = connect_async(url).await.unwrap_err();
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("expected an HTTP rejection, got {error:?}");
    };
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn chatty_clients_are_disconnected() {
    let harness = Harness::new(WsLimits {
        max_control_messages: 3,
        ..limits()
    });
    let url = harness.serve().await;
    let (mut client, _) = connect_async(url).await.unwrap();
    next_json(&mut client).await;
    for _ in 0..5 {
        let _ = client.send(Message::text(r#"{"op":"ping"}"#)).await;
    }
    let close = loop {
        match tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .unwrap()
        {
            Some(Ok(Message::Close(frame))) => break frame,
            Some(Ok(_)) => {}
            other => panic!("expected a close frame, got {other:?}"),
        }
    };
    assert_eq!(close.unwrap().code, CloseCode::Policy);
}

#[tokio::test]
async fn shutdown_asks_clients_to_reconnect_elsewhere() {
    let harness = Harness::new(limits());
    let url = harness.serve().await;
    let (mut client, _) = connect_async(url).await.unwrap();
    next_json(&mut client).await;

    harness.state.shutdown.cancel();
    let close = loop {
        match tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .unwrap()
        {
            Some(Ok(Message::Close(frame))) => break frame,
            Some(Ok(_)) => {}
            other => panic!("expected a close frame, got {other:?}"),
        }
    };
    assert_eq!(close.unwrap().code, CloseCode::Away);
    harness.state.sessions.close();
    tokio::time::timeout(Duration::from_secs(5), harness.state.sessions.wait())
        .await
        .expect("sessions finish after the close");
}
