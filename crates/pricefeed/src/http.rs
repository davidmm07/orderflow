//! REST routes, health checks and the metrics endpoint.

use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};

use axum::{
    Json, Router,
    extract::{MatchedPath, Path, Query, Request, State, rejection::QueryRejection},
    http::{HeaderName, HeaderValue, Method, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::DateTime;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{CandleInterval, CandleQuery, QuoteState};
use tower_http::{
    catch_panic::CatchPanicLayer,
    cors::{Any, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};

use crate::{
    dto::{
        CandleView, CandlesQuery, CandlesResponse, PriceDetailView, PriceView, PricesQuery,
        PricesResponse, SourceView, SourcesResponse, rfc3339,
    },
    problem::{FieldError, Problem},
    state::AppState,
    ws,
};

const DEFAULT_CANDLES: u32 = 100;
/// Markets accepted in one `?markets=` filter.
const MAX_FILTER_MARKETS: usize = 100;
const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Public market data may be cached by browsers and CDNs, briefly. A CDN in
/// front of the API turns a traffic spike on one URL into one request per
/// second per edge location.
const LIVE_CACHE: HeaderValue = HeaderValue::from_static("public, max-age=1");
const SETTLED_CACHE: HeaderValue = HeaderValue::from_static("public, max-age=60");
const SOURCES_CACHE: HeaderValue = HeaderValue::from_static("public, max-age=5");

pub fn router(state: AppState, request_timeout: Duration) -> Router {
    // The timeout applies to REST only. A WebSocket upgrade answers at once
    // and the connection then lives as long as the client stays.
    let rest = Router::new()
        .route("/v1/prices", get(prices))
        .route("/v1/prices/{market}", get(price))
        .route("/v1/prices/{market}/candles", get(candles))
        .route("/v1/sources", get(sources))
        .route_layer(middleware::from_fn_with_state(
            request_timeout,
            enforce_timeout,
        ));

    Router::new()
        .merge(rest)
        .route("/v1/ws", get(ws::upgrade))
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(state.clone(), record))
        .layer(CatchPanicLayer::custom(|_| {
            Problem::internal().into_response()
        }))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET]),
        )
        .layer(SetResponseHeaderLayer::overriding(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(PropagateRequestIdLayer::new(REQUEST_ID))
        .layer(TraceLayer::new_for_http())
        .layer(SetRequestIdLayer::new(REQUEST_ID, MakeRequestUuid))
        .with_state(state)
}

/// `GET /v1/prices?markets=BTC-USD,ETH-USD`
///
/// Served from memory: no database or Redis call, however many clients ask.
async fn prices(
    State(state): State<AppState>,
    query: Result<Query<PricesQuery>, QueryRejection>,
) -> Result<Response, Problem> {
    let Query(query) = query.map_err(query_problem)?;
    let snapshot = state.hub.snapshot();
    let prices = match query.markets.as_deref() {
        None => snapshot
            .iter()
            .map(|update| PriceView::from(&**update))
            .collect(),
        Some(raw) => {
            let requested: Vec<&str> = raw
                .split(',')
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .collect();
            if requested.len() > MAX_FILTER_MARKETS {
                return Err(Problem::validation(vec![FieldError {
                    field: "markets",
                    code: "too_many",
                    message: format!("at most {MAX_FILTER_MARKETS} markets"),
                }]));
            }
            let mut markets = Vec::with_capacity(requested.len());
            for raw in requested {
                markets.push(listed_market(&state, raw, "markets")?);
            }
            snapshot
                .iter()
                .filter(|update| markets.contains(&update.market))
                .map(|update| PriceView::from(&**update))
                .collect()
        }
    };
    Ok(cached(Json(PricesResponse { prices }), LIVE_CACHE))
}

/// `GET /v1/prices/{market}` with the per-source breakdown.
async fn price(
    State(state): State<AppState>,
    Path(market): Path<String>,
) -> Result<Response, Problem> {
    let market = listed_market(&state, &market, "market")?;
    let update = state.hub.latest(&market).ok_or_else(|| {
        Problem::service_unavailable(
            "price_unavailable",
            format!("no price for {market} has been published yet"),
        )
    })?;
    Ok(cached(Json(PriceDetailView::from(&*update)), LIVE_CACHE))
}

/// `GET /v1/prices/{market}/candles?interval=1m&limit=100&end=2026-10-08T22:00:00Z`
async fn candles(
    State(state): State<AppState>,
    Path(market): Path<String>,
    query: Result<Query<CandlesQuery>, QueryRejection>,
) -> Result<Response, Problem> {
    let Query(params) = query.map_err(query_problem)?;
    let market = listed_market(&state, &market, "market")?;
    let mut errors = Vec::new();
    let interval = match params.interval.as_deref().map(CandleInterval::parse) {
        None => CandleInterval::OneMinute,
        Some(Ok(interval)) => interval,
        Some(Err(error)) => {
            errors.push(FieldError {
                field: "interval",
                code: "invalid_interval",
                message: error.to_string(),
            });
            CandleInterval::OneMinute
        }
    };
    let limit = params.limit.unwrap_or(DEFAULT_CANDLES);
    if !(1..=CandleQuery::MAX_LIMIT).contains(&limit) {
        errors.push(FieldError {
            field: "limit",
            code: "out_of_range",
            message: format!("must be between 1 and {}", CandleQuery::MAX_LIMIT),
        });
    }
    let end = match params.end.as_deref().map(parse_time) {
        None => None,
        Some(Some(end)) => Some(end),
        Some(None) => {
            errors.push(FieldError {
                field: "end",
                code: "invalid_time",
                message: "must be an RFC 3339 time after 1970, such as 2026-10-08T22:00:00Z".into(),
            });
            None
        }
    };
    if !errors.is_empty() {
        return Err(Problem::validation(errors));
    }

    let query = CandleQuery {
        market,
        interval,
        limit,
        end,
    };
    let candles = state.candles.candles(&query).await.map_err(|error| {
        tracing::warn!(%error, "candle read failed");
        Problem::service_unavailable(
            "store_unavailable",
            "candle history is temporarily unavailable",
        )
    })?;
    let policy = if query.is_settled(state.clock.now()) {
        SETTLED_CACHE
    } else {
        LIVE_CACHE
    };
    let body = CandlesResponse {
        market: query.market.to_string(),
        interval: interval.as_str().to_owned(),
        candles: candles.iter().map(CandleView::from).collect(),
    };
    Ok(cached(Json(body), policy))
}

/// `GET /v1/sources`: every source seen in the current prices and how the
/// index is using it.
async fn sources(State(state): State<AppState>) -> Response {
    let mut summary: BTreeMap<String, SourceView> = BTreeMap::new();
    let mut newest: BTreeMap<String, Timestamp> = BTreeMap::new();
    for update in state.hub.snapshot() {
        for component in &update.components {
            let id = component.source.to_string();
            let view = summary.entry(id.clone()).or_insert_with(|| SourceView {
                source: id.clone(),
                kind: component.kind.as_str().to_owned(),
                markets: Vec::new(),
                used_in: 0,
                outlier_in: 0,
                stale_in: 0,
                last_observed_at: String::new(),
            });
            view.markets.push(update.market.to_string());
            match component.state {
                QuoteState::Used => view.used_in += 1,
                QuoteState::Outlier => view.outlier_in += 1,
                QuoteState::Stale => view.stale_in += 1,
                QuoteState::Monitor => {}
            }
            let latest = newest.entry(id).or_default();
            *latest = (*latest).max(component.observed_at);
        }
    }
    let sources = summary
        .into_iter()
        .map(|(id, mut view)| {
            view.last_observed_at = rfc3339(newest.get(&id).copied().unwrap_or_default());
            view
        })
        .collect();
    cached(Json(SourcesResponse { sources }), SOURCES_CACHE)
}

async fn live() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Ready once this node can serve live prices: immediately for ingesting
/// roles, after the first Redis snapshot for edge nodes.
async fn ready(State(state): State<AppState>) -> Response {
    if state.ready.load(Ordering::Acquire) {
        Json(serde_json::json!({ "status": "ready" })).into_response()
    } else {
        Problem::service_unavailable("not_ready", "waiting for the price stream").into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    let body = state.metrics.render(&state.hub, &*state.clock);
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

async fn not_found() -> Problem {
    Problem::not_found("route_not_found", "no route matches this path")
}

/// Counts requests by route template, never by raw path, so random URLs
/// cannot create new series.
async fn record(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_owned();
    let response = next.run(request).await;
    state
        .metrics
        .http_request(&route, response.status().as_u16());
    response
}

async fn enforce_timeout(
    State(timeout): State<Duration>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => {
            Problem::service_unavailable("timeout", "the request took too long").into_response()
        }
    }
}

/// Parses a market id and checks it is served here.
fn listed_market(state: &AppState, raw: &str, field: &'static str) -> Result<MarketId, Problem> {
    let market = MarketId::parse(raw).map_err(|_| {
        Problem::validation(vec![FieldError {
            field,
            code: "invalid_market",
            message: "market ids look like BTC-USD".into(),
        }])
    })?;
    if state.hub.contains(&market) {
        Ok(market)
    } else {
        Err(Problem::not_found(
            "market_not_found",
            format!("{market} is not listed"),
        ))
    }
}

fn parse_time(raw: &str) -> Option<Timestamp> {
    let nanos = DateTime::parse_from_rfc3339(raw)
        .ok()?
        .timestamp_nanos_opt()?;
    u64::try_from(nanos).ok().map(Timestamp::from_unix_nanos)
}

fn query_problem(rejection: QueryRejection) -> Problem {
    Problem::validation(vec![FieldError {
        field: "query",
        code: "invalid_query",
        // The rejection text can quote the query string; keep it short.
        message: rejection.body_text().chars().take(200).collect(),
    }])
}

fn cached(body: impl IntoResponse, policy: HeaderValue) -> Response {
    let mut response = body.into_response();
    response.headers_mut().insert(header::CACHE_CONTROL, policy);
    response
}
