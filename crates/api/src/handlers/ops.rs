//! Operational endpoints for orchestrators and scrapers.

use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
};

use crate::{dto::StatusView, state::AppState};

/// `GET /health/live`: the process is up and serving HTTP.
pub async fn live() -> Json<StatusView> {
    Json(StatusView { status: "ok" })
}

/// `GET /health/ready`: every market engine is running and can take orders.
/// A load balancer should route traffic only while this returns 200.
pub async fn ready(State(state): State<AppState>) -> (StatusCode, Json<StatusView>) {
    if state.queries.is_ready() {
        (StatusCode::OK, Json(StatusView { status: "ready" }))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(StatusView {
                status: "not_ready",
            }),
        )
    }
}

/// `GET /metrics` in the Prometheus text format: engine and runtime metrics
/// followed by HTTP request metrics.
pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        format!("{}{}", (state.metrics)(), state.http_metrics.render()),
    )
}
