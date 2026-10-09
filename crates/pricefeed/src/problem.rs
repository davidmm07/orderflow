//! RFC 9457 problem responses, the same error format as the exchange API.

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// Prefix of the `type` URI, shared with the exchange API. The codes are
/// listed in docs/errors.md.
const TYPE_PREFIX: &str = "urn:orderflow:problem:";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FieldError {
    pub field: &'static str,
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct Problem {
    #[serde(rename = "type")]
    kind: String,
    title: &'static str,
    status: u16,
    detail: String,
    code: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<FieldError>,
}

impl Problem {
    fn new(
        status: StatusCode,
        title: &'static str,
        code: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind: format!("{TYPE_PREFIX}{code}"),
            title,
            status: status.as_u16(),
            detail: detail.into(),
            code,
            errors: Vec::new(),
        }
    }

    pub fn validation(errors: Vec<FieldError>) -> Self {
        Self {
            errors,
            ..Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Validation failed",
                "validation_failed",
                "one or more parameters are invalid",
            )
        }
    }

    pub fn not_found(code: &'static str, detail: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "Not found", code, detail)
    }

    pub fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error",
            "internal_error",
            "the request failed unexpectedly",
        )
    }

    pub fn service_unavailable(code: &'static str, detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Service unavailable",
            code,
            detail,
        )
    }
}

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let retry = status == StatusCode::SERVICE_UNAVAILABLE;
        let mut response = (status, Json(self)).into_response();
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if retry {
            headers.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}
