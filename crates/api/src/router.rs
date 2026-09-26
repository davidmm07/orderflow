//! Route table and middleware stack.

use std::{any::Any, time::Duration};

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderValue, Request, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    set_header::SetResponseHeaderLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

use crate::{
    auth,
    error::ApiError,
    handlers::{markets, ops, orders},
    rate_limit,
    state::AppState,
};

#[derive(Debug, Clone, Copy)]
pub struct ApiConfig {
    /// Upper bound for a whole request, after which the client gets 503.
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
    // `route_layer` runs the last added layer first, so requests are
    // authenticated before they are rate limited.
    let private = Router::new()
        .route(
            "/v1/markets/{market}/orders",
            axum::routing::post(orders::place),
        )
        .route(
            "/v1/markets/{market}/orders/{order_id}",
            get(orders::get).delete(orders::cancel),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit::enforce,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_signature,
        ));

    let public = Router::new()
        .route("/v1/markets", get(markets::list))
        .route("/v1/markets/{market}/book", get(markets::book))
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
                .layer(CatchPanicLayer::custom(panic_response))
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::SERVICE_UNAVAILABLE,
                    config.request_timeout,
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
