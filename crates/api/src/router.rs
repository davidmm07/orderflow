//! Route table and middleware stack.

use std::{any::Any, time::Duration};

use axum::{
    Router,
    extract::{DefaultBodyLimit, Request as AxumRequest, State},
    http::{HeaderValue, Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};

use crate::{
    auth,
    error::ApiError,
    handlers::{markets, ops, orders},
    http_metrics, rate_limit,
    state::AppState,
};

const ASSETS: &str = "/v1/assets";
const MARKETS: &str = "/v1/markets";
const MARKET: &str = "/v1/markets/{market}";
const BOOK: &str = "/v1/markets/{market}/book";
const ORDERS: &str = "/v1/markets/{market}/orders";
const ORDER: &str = "/v1/markets/{market}/orders/{order_id}";

/// Problem codes that alerts and dashboard panels watch, per route. Their
/// series exist from startup so the first occurrence is visible to `rate()`.
const WATCHED_PROBLEMS: [(&str, &[&str]); 3] = [
    (
        ORDERS,
        &[
            "unauthenticated",
            "rate_limited",
            "market_overloaded",
            "request_timeout",
        ],
    ),
    (
        ORDER,
        &[
            "unauthenticated",
            "rate_limited",
            "market_overloaded",
            "request_timeout",
        ],
    ),
    (BOOK, &["market_overloaded", "request_timeout"]),
];

#[derive(Debug, Clone, Copy)]
pub struct ApiConfig {
    /// Upper bound for a whole request, after which the client gets a 503
    /// `request_timeout` problem.
    pub request_timeout: Duration,
    pub max_body_bytes: usize,
}

/// Builds the complete HTTP application.
///
/// Pattern: Chain of Responsibility. Each tower layer handles one concern
/// and passes the request on. The order matters: the request id is set
/// first so every log line carries it, then come tracing, panic recovery
/// and the timeout. Authentication and rate limiting run last, and only on
/// the private routes.
pub fn router(state: AppState, config: &ApiConfig) -> Router {
    for (route, codes) in WATCHED_PROBLEMS {
        state.http_metrics.expect_problems(route, codes);
    }

    // `route_layer` runs the last added layer first, so requests are
    // authenticated before they are rate limited.
    let private = Router::new()
        .route(ORDERS, axum::routing::post(orders::place))
        .route(ORDER, get(orders::get).delete(orders::cancel))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit::enforce,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_signature,
        ));

    let public = Router::new()
        .route(ASSETS, get(markets::assets))
        .route(MARKETS, get(markets::list))
        .route(MARKET, get(markets::get))
        .route(BOOK, get(markets::book))
        .route("/health/live", get(ops::live))
        .route("/health/ready", get(ops::ready))
        .route("/metrics", get(ops::metrics));

    public
        .merge(private)
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(TraceLayer::new_for_http().make_span_with(request_span))
                .layer(middleware::from_fn_with_state(
                    state.http_metrics.clone(),
                    http_metrics::record,
                ))
                .layer(CatchPanicLayer::custom(panic_response))
                .layer(middleware::from_fn_with_state(
                    config.request_timeout,
                    enforce_timeout,
                ))
                .layer(SetResponseHeaderLayer::overriding(
                    header::X_CONTENT_TYPE_OPTIONS,
                    HeaderValue::from_static("nosniff"),
                ))
                .layer(SetResponseHeaderLayer::overriding(
                    header::CACHE_CONTROL,
                    HeaderValue::from_static("no-store"),
                )),
        )
        .with_state(state)
}

fn request_span<B>(request: &Request<B>) -> tracing::Span {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-");
    // The path is logged without the query string, which may carry values
    // that do not belong in logs.
    tracing::info_span!(
        "http",
        method = %request.method(),
        path = %request.uri().path(),
        request_id,
    )
}

/// Bounds request latency and answers with a problem document, which
/// `tower_http::timeout` cannot do (it returns an empty body).
///
/// A timed out order placement may still complete: placement runs on its
/// own task. Clients retry with the same `Idempotency-Key` to learn the
/// outcome without placing a second order.
async fn enforce_timeout(
    State(timeout): State<Duration>,
    request: AxumRequest,
    next: Next,
) -> Response {
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "request_timeout",
            "Request timed out",
            "The request did not complete in time. Retry with the same Idempotency-Key.",
        )
        .retry_after(Duration::from_secs(1))
        .into_response(),
    }
}

fn panic_response(_: Box<dyn Any + Send + 'static>) -> Response {
    ApiError::internal("handler panicked").into_response()
}

async fn route_not_found() -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "route_not_found",
        "Route not found",
        "No route matches this path.",
    )
}

async fn method_not_allowed() -> ApiError {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "Method not allowed",
        "This route does not support the request method.",
    )
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn slow_handlers_get_a_timeout_problem() {
        let app = Router::new()
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    "too late"
                }),
            )
            .layer(middleware::from_fn_with_state(
                Duration::from_millis(50),
                enforce_timeout,
            ));

        let request = Request::builder().uri("/slow").body(Body::empty()).unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            crate::error::PROBLEM_JSON
        );
    }
}
