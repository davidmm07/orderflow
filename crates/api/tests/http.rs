#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! End-to-end HTTP tests: real router, real use cases, in-memory adapters.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use http_body_util::BodyExt;
use orderflow_api::{
    API_KEY_HEADER, ApiConfig, AppState, Authenticator, Credential, PROBLEM_JSON, RateLimitConfig,
    RateLimiter, SIGNATURE_HEADER, TIMESTAMP_HEADER, router, sign_request,
};
use orderflow_application::{
    CancelOrder, EventDispatcher, MarketDeps, MarketRegistry, OrderQueries, PlaceOrder, outbox,
    spawn_market,
};
use orderflow_domain::{AccountId, Decimal, MarketId, MarketSpec};
use orderflow_infrastructure::{
    InMemoryIdempotencyStore, InMemoryOrderRepository, LoggingPublisher, MonotonicClock,
    PrometheusMetrics, UuidV7Generator,
};
use serde_json::{Value, json};
use tower::ServiceExt;

const ALICE_KEY: &str = "alice-key";
const ALICE_SECRET: &[u8] = b"alice-secret-alice-secret-alice-secret";
const BOB_KEY: &str = "bob-key";
const BOB_SECRET: &[u8] = b"bob-secret-bob-secret-bob-secret-bob";
const MAX_BODY: usize = 4 * 1024;

fn app_with(rate_limit: RateLimitConfig) -> Router {
    let repository = Arc::new(InMemoryOrderRepository::default());
    let metrics = Arc::new(PrometheusMetrics::new());
    let (outbox, receiver) = outbox(64);
    EventDispatcher::new(receiver, Arc::new(LoggingPublisher), metrics.clone()).spawn();

    let spec = MarketSpec::builder(MarketId::parse("BTC-USD").unwrap())
        .tick_size(Decimal::new(1, 2))
        .lot_size(Decimal::new(1, 3))
        .max_quantity(Decimal::from(100))
        .build()
        .unwrap();
    let deps = MarketDeps {
        repository: repository.clone(),
        clock: Arc::new(MonotonicClock::default()),
        metrics: metrics.clone(),
        outbox,
    };
    let (handle, _task) = spawn_market(spec, deps, 128);
    let registry = Arc::new(MarketRegistry::new([handle]));

    let credential = |account: &str, secret: &[u8]| {
        Credential::new(AccountId::parse(account).unwrap(), secret).unwrap()
    };
    let state = AppState {
        place_order: Arc::new(PlaceOrder::new(
            registry.clone(),
            Arc::new(UuidV7Generator),
            Arc::new(InMemoryIdempotencyStore::new(Duration::from_secs(60), 1000)),
        )),
        cancel_order: Arc::new(CancelOrder::new(registry.clone(), repository.clone())),
        queries: Arc::new(OrderQueries::new(registry, repository)),
        authenticator: Arc::new(Authenticator::new(
            [
                (ALICE_KEY.to_owned(), credential("alice", ALICE_SECRET)),
                (BOB_KEY.to_owned(), credential("bob", BOB_SECRET)),
            ],
            Duration::from_secs(30),
            MAX_BODY,
        )),
        rate_limiter: Arc::new(RateLimiter::new(rate_limit)),
        metrics: Arc::new(move || metrics.render()),
    };
    router(
        state,
        &ApiConfig {
            request_timeout: Duration::from_secs(5),
            max_body_bytes: MAX_BODY,
        },
    )
}

fn app() -> Router {
    app_with(RateLimitConfig {
        per_second: 1000,
        burst: 1000,
    })
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

struct Client {
    key: &'static str,
    secret: &'static [u8],
}

const ALICE: Client = Client {
    key: ALICE_KEY,
    secret: ALICE_SECRET,
};
const BOB: Client = Client {
    key: BOB_KEY,
    secret: BOB_SECRET,
};

impl Client {
    fn request(&self, method: &str, path: &str, body: Option<Value>) -> Request<Body> {
        self.request_at(method, path, body, now())
    }

    fn request_at(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        timestamp: i64,
    ) -> Request<Body> {
        let bytes = body
            .map(|value| serde_json::to_vec(&value).unwrap())
            .unwrap_or_default();
        let signature = sign_request(self.secret, timestamp, method, path, &bytes);
        Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .header(API_KEY_HEADER, self.key)
            .header(TIMESTAMP_HEADER, timestamp.to_string())
            .header(SIGNATURE_HEADER, signature)
            .body(Body::from(bytes))
            .unwrap()
    }
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}

async fn send(app: &Router, request: Request<Body>) -> Reply {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    Reply {
        status,
        headers,
        body,
    }
}

fn get(path: &str) -> Request<Body> {
    Request::builder().uri(path).body(Body::empty()).unwrap()
}

fn limit(side: &str, price: &str, quantity: &str) -> Value {
    json!({ "side": side, "type": "limit", "price": price, "quantity": quantity })
}

const ORDERS: &str = "/v1/markets/BTC-USD/orders";

fn assert_problem(reply: &Reply, status: StatusCode, code: &str) {
    assert_eq!(reply.status, status, "body: {}", reply.body);
    assert_eq!(reply.headers["content-type"], PROBLEM_JSON);
    assert_eq!(reply.body["code"], code);
    assert_eq!(reply.body["status"], status.as_u16());
    assert_eq!(reply.body["type"], format!("urn:orderflow:problem:{code}"));
}

fn error_fields(reply: &Reply) -> Vec<String> {
    reply.body["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["field"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn health_and_market_listing_are_public() {
    let app = app();
    assert_eq!(send(&app, get("/health/live")).await.status, StatusCode::OK);
    let ready = send(&app, get("/health/ready")).await;
    assert_eq!(ready.body["status"], "ready");

    let markets = send(&app, get("/v1/markets")).await;
    assert_eq!(markets.status, StatusCode::OK);
    assert_eq!(markets.body["markets"][0]["id"], "BTC-USD");
    assert_eq!(markets.body["markets"][0]["tick_size"], "0.01");
}

#[tokio::test]
async fn responses_carry_request_id_and_security_headers() {
    let reply = send(&app(), get("/v1/markets")).await;
    assert!(reply.headers.contains_key("x-request-id"));
    assert_eq!(reply.headers["x-content-type-options"], "nosniff");
    assert_eq!(reply.headers["cache-control"], "no-store");
}

#[tokio::test]
async fn private_routes_reject_unsigned_and_forged_requests() {
    let app = app();
    let unsigned = Request::builder()
        .method("POST")
        .uri(ORDERS)
        .body(Body::empty())
        .unwrap();
    assert_problem(
        &send(&app, unsigned).await,
        StatusCode::UNAUTHORIZED,
        "unauthenticated",
    );

    let mut forged = ALICE.request("POST", ORDERS, Some(limit("buy", "100", "1")));
    forged
        .headers_mut()
        .insert(SIGNATURE_HEADER, "00".repeat(32).parse().unwrap());
    assert_problem(
        &send(&app, forged).await,
        StatusCode::UNAUTHORIZED,
        "unauthenticated",
    );

    let stale = ALICE.request_at("POST", ORDERS, Some(limit("buy", "100", "1")), now() - 3600);
    assert_problem(
        &send(&app, stale).await,
        StatusCode::UNAUTHORIZED,
        "unauthenticated",
    );
}

#[tokio::test]
async fn placed_order_can_be_read_back_by_its_owner_only() {
    let app = app();
    let placed = send(
        &app,
        ALICE.request("POST", ORDERS, Some(limit("buy", "100.50", "0.25"))),
    )
    .await;
    assert_eq!(placed.status, StatusCode::CREATED, "{}", placed.body);
    let order = &placed.body["order"];
    assert_eq!(order["status"], "open");
    assert_eq!(order["price"], "100.5");
    assert_eq!(order["time_in_force"], "gtc");
    let id = order["id"].as_str().unwrap();
    let location = format!("{ORDERS}/{id}");
    assert_eq!(placed.headers["location"], location.as_str());

    let fetched = send(&app, ALICE.request("GET", &location, None)).await;
    assert_eq!(fetched.status, StatusCode::OK);
    assert_eq!(fetched.body["id"], id);

    let foreign = send(&app, BOB.request("GET", &location, None)).await;
    assert_problem(&foreign, StatusCode::NOT_FOUND, "order_not_found");
}

#[tokio::test]
async fn crossing_orders_trade_and_update_the_book() {
    let app = app();
    send(
        &app,
        ALICE.request("POST", ORDERS, Some(limit("sell", "101", "2"))),
    )
    .await;
    let taker = send(
        &app,
        BOB.request("POST", ORDERS, Some(limit("buy", "102", "0.5"))),
    )
    .await;

    assert_eq!(taker.status, StatusCode::CREATED);
    assert_eq!(taker.body["order"]["status"], "filled");
    assert_eq!(taker.body["fills"][0]["price"], "101");
    assert_eq!(taker.body["fills"][0]["quantity"], "0.5");

    let book = send(&app, get("/v1/markets/BTC-USD/book?depth=5")).await;
    assert_eq!(book.status, StatusCode::OK);
    assert_eq!(
        book.body["asks"][0],
        json!({ "price": "101", "quantity": "1.5", "orders": 1 })
    );
    assert_eq!(book.body["sequence"], 3);
}

#[tokio::test]
async fn validation_reports_every_invalid_field_at_once() {
    let app = app();
    let body = json!({ "side": "up", "type": "limit", "quantity": "-1", "self_trade_prevention": "maybe" });
    let reply = send(&app, ALICE.request("POST", ORDERS, Some(body))).await;

    assert_problem(
        &reply,
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_failed",
    );
    assert_eq!(
        error_fields(&reply),
        ["side", "quantity", "price", "self_trade_prevention"]
    );
}

#[tokio::test]
async fn market_rules_map_to_field_errors() {
    let reply = send(
        &app(),
        ALICE.request("POST", ORDERS, Some(limit("buy", "100.001", "0.0001"))),
    )
    .await;
    assert_problem(
        &reply,
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_failed",
    );
    let codes: Vec<_> = reply.body["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["code"].as_str().unwrap())
        .collect();
    assert_eq!(codes, ["quantity_below_minimum", "price_not_on_tick"]);
}

#[tokio::test]
async fn malformed_and_unexpected_bodies_are_rejected() {
    let app = app();
    let unknown_field =
        json!({ "side": "buy", "type": "market", "quantity": "1", "leverage": 100 });
    let reply = send(&app, ALICE.request("POST", ORDERS, Some(unknown_field))).await;
    assert_problem(&reply, StatusCode::UNPROCESSABLE_ENTITY, "invalid_body");

    let numeric_price = json!({ "side": "buy", "type": "limit", "price": 100.5, "quantity": "1" });
    let reply = send(&app, ALICE.request("POST", ORDERS, Some(numeric_price))).await;
    assert_problem(&reply, StatusCode::UNPROCESSABLE_ENTITY, "invalid_body");

    let timestamp = now();
    let raw = b"{not json".to_vec();
    let signature = sign_request(ALICE_SECRET, timestamp, "POST", ORDERS, &raw);
    let request = Request::builder()
        .method("POST")
        .uri(ORDERS)
        .header("content-type", "application/json")
        .header(API_KEY_HEADER, ALICE_KEY)
        .header(TIMESTAMP_HEADER, timestamp.to_string())
        .header(SIGNATURE_HEADER, signature)
        .body(Body::from(raw))
        .unwrap();
    assert_problem(
        &send(&app, request).await,
        StatusCode::BAD_REQUEST,
        "malformed_json",
    );
}

#[tokio::test]
async fn oversized_bodies_are_refused_before_authentication() {
    let padding = "x".repeat(MAX_BODY);
    let body =
        json!({ "side": "buy", "type": "market", "quantity": "1", "client_order_id": padding });
    let reply = send(&app(), ALICE.request("POST", ORDERS, Some(body))).await;
    assert_problem(&reply, StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large");
}

#[tokio::test]
async fn unknown_markets_and_routes_are_problems_too() {
    let app = app();
    let reply = send(
        &app,
        ALICE.request(
            "POST",
            "/v1/markets/DOGE-USD/orders",
            Some(limit("buy", "1", "1")),
        ),
    )
    .await;
    assert_problem(&reply, StatusCode::NOT_FOUND, "unknown_market");

    assert_problem(
        &send(&app, get("/v1/markets/btc/book")).await,
        StatusCode::NOT_FOUND,
        "unknown_market",
    );
    assert_problem(
        &send(&app, get("/nope")).await,
        StatusCode::NOT_FOUND,
        "route_not_found",
    );

    let wrong_method = Request::builder()
        .method("PUT")
        .uri("/v1/markets")
        .body(Body::empty())
        .unwrap();
    assert_problem(
        &send(&app, wrong_method).await,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );

    let bad_depth = send(&app, get("/v1/markets/BTC-USD/book?depth=0")).await;
    assert_problem(
        &bad_depth,
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_failed",
    );
}

#[tokio::test]
async fn idempotency_key_replays_the_original_response() {
    let app = app();
    let with_key = |body: Value| {
        let mut request = ALICE.request("POST", ORDERS, Some(body));
        request
            .headers_mut()
            .insert("idempotency-key", "retry-me-1".parse().unwrap());
        request
    };

    let first = send(&app, with_key(limit("buy", "99", "1"))).await;
    let second = send(&app, with_key(limit("buy", "99", "1"))).await;
    assert_eq!(first.status, StatusCode::CREATED);
    assert_eq!(second.status, StatusCode::CREATED);
    assert_eq!(second.headers["idempotent-replayed"], "true");
    assert_eq!(first.body["order"]["id"], second.body["order"]["id"]);

    let reused = send(&app, with_key(limit("buy", "98", "1"))).await;
    assert_problem(
        &reused,
        StatusCode::UNPROCESSABLE_ENTITY,
        "idempotency_key_reused",
    );

    let book = send(&app, get("/v1/markets/BTC-USD/book")).await;
    assert_eq!(book.body["bids"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn cancel_removes_the_order_and_is_not_repeatable() {
    let app = app();
    let placed = send(
        &app,
        ALICE.request("POST", ORDERS, Some(limit("sell", "150", "1"))),
    )
    .await;
    let path = format!("{ORDERS}/{}", placed.body["order"]["id"].as_str().unwrap());

    assert_problem(
        &send(&app, BOB.request("DELETE", &path, None)).await,
        StatusCode::NOT_FOUND,
        "order_not_found",
    );

    let cancelled = send(&app, ALICE.request("DELETE", &path, None)).await;
    assert_eq!(cancelled.status, StatusCode::OK);
    assert_eq!(cancelled.body["status"], "cancelled");
    assert_eq!(cancelled.body["cancel_reason"], "requested");

    let again = send(&app, ALICE.request("DELETE", &path, None)).await;
    assert_problem(&again, StatusCode::CONFLICT, "order_not_open");

    let bad_id = send(
        &app,
        ALICE.request("DELETE", &format!("{ORDERS}/not-a-uuid"), None),
    )
    .await;
    assert_problem(&bad_id, StatusCode::NOT_FOUND, "order_not_found");
}

#[tokio::test]
async fn post_only_orders_that_would_cross_are_rejected() {
    let app = app();
    send(
        &app,
        ALICE.request("POST", ORDERS, Some(limit("sell", "100", "1"))),
    )
    .await;
    let body = json!({ "side": "buy", "type": "limit", "price": "100", "quantity": "1", "post_only": true });
    let reply = send(&app, BOB.request("POST", ORDERS, Some(body))).await;
    assert_problem(&reply, StatusCode::CONFLICT, "post_only_would_cross");
}

#[tokio::test]
async fn rate_limit_answers_429_with_retry_after() {
    let app = app_with(RateLimitConfig {
        per_second: 1,
        burst: 2,
    });
    let path = format!("{ORDERS}/{}", uuid_like());
    for _ in 0..2 {
        let reply = send(&app, ALICE.request("GET", &path, None)).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
    }
    let limited = send(&app, ALICE.request("GET", &path, None)).await;
    assert_problem(&limited, StatusCode::TOO_MANY_REQUESTS, "rate_limited");
    assert_eq!(limited.headers["retry-after"], "1");

    let other_account = send(&app, BOB.request("GET", &path, None)).await;
    assert_eq!(
        other_account.status,
        StatusCode::NOT_FOUND,
        "limits are per account"
    );
}

#[tokio::test]
async fn metrics_expose_engine_counters() {
    let app = app();
    send(
        &app,
        ALICE.request("POST", ORDERS, Some(limit("buy", "90", "1"))),
    )
    .await;
    let reply = app.clone().oneshot(get("/metrics")).await.unwrap();
    assert!(
        reply.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    let text = String::from_utf8(
        reply
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.contains("orderflow_orders_total{market=\"BTC-USD\",outcome=\"open\"} 1"),
        "{text}"
    );
}

#[tokio::test]
async fn stop_orders_wait_until_a_trade_reaches_their_stop_price() {
    let app = app();
    send(
        &app,
        ALICE.request("POST", ORDERS, Some(limit("sell", "100", "1"))),
    )
    .await;
    send(
        &app,
        BOB.request("POST", ORDERS, Some(limit("buy", "100", "1"))),
    )
    .await;

    let stop =
        json!({ "side": "buy", "type": "stop_market", "stop_price": "101", "quantity": "0.5" });
    let placed = send(&app, BOB.request("POST", ORDERS, Some(stop))).await;
    assert_eq!(placed.status, StatusCode::CREATED, "{}", placed.body);
    assert_eq!(placed.body["order"]["status"], "pending");
    assert_eq!(placed.body["order"]["type"], "stop_market");
    assert_eq!(placed.body["order"]["stop_price"], "101");
    let stop_path = format!("{ORDERS}/{}", placed.body["order"]["id"].as_str().unwrap());

    let book = send(&app, get("/v1/markets/BTC-USD/book")).await;
    assert_eq!(book.body["last_price"], "100");
    assert!(
        book.body["bids"].as_array().unwrap().is_empty(),
        "stops stay out of the book"
    );

    let already_passed =
        json!({ "side": "sell", "type": "stop_market", "stop_price": "101", "quantity": "1" });
    let rejected = send(&app, ALICE.request("POST", ORDERS, Some(already_passed))).await;
    assert_problem(
        &rejected,
        StatusCode::CONFLICT,
        "stop_would_trigger_immediately",
    );

    send(
        &app,
        ALICE.request("POST", ORDERS, Some(limit("sell", "101", "2"))),
    )
    .await;
    let trigger = send(
        &app,
        BOB.request("POST", ORDERS, Some(limit("buy", "101", "0.5"))),
    )
    .await;
    assert_eq!(trigger.body["fills"][0]["liquidity"], "taker");

    let fired = send(&app, BOB.request("GET", &stop_path, None)).await;
    assert_eq!(fired.body["status"], "filled");
    assert_eq!(fired.body["filled_quantity"], "0.5");
    let book = send(&app, get("/v1/markets/BTC-USD/book")).await;
    assert_eq!(book.body["asks"][0]["quantity"], "1");
}

#[tokio::test]
async fn stop_fields_are_validated() {
    let app = app();
    let missing_stop =
        json!({ "side": "buy", "type": "stop_limit", "price": "100", "quantity": "1" });
    let reply = send(&app, ALICE.request("POST", ORDERS, Some(missing_stop))).await;
    assert_problem(
        &reply,
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_failed",
    );
    assert_eq!(error_fields(&reply), ["stop_price"]);

    let stop_on_limit = json!({ "side": "buy", "type": "limit", "price": "100", "stop_price": "99", "quantity": "1" });
    let reply = send(&app, ALICE.request("POST", ORDERS, Some(stop_on_limit))).await;
    assert_eq!(error_fields(&reply), ["stop_price"]);

    let pending = json!({ "side": "sell", "type": "stop_limit", "stop_price": "90", "price": "89", "quantity": "1" });
    let placed = send(&app, ALICE.request("POST", ORDERS, Some(pending))).await;
    let path = format!("{ORDERS}/{}", placed.body["order"]["id"].as_str().unwrap());
    let cancelled = send(&app, ALICE.request("DELETE", &path, None)).await;
    assert_eq!(cancelled.body["status"], "cancelled");
}

fn uuid_like() -> &'static str {
    "01890a5d-ac96-774b-bcce-b302099a8057"
}
