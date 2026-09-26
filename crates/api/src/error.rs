//! Mapping of every failure to an RFC 9457 problem response.

use std::time::Duration;

use axum::{
    Json,
    extract::rejection::{JsonRejection, PathRejection, QueryRejection},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use orderflow_application::ApplicationError;
use orderflow_domain::DomainError;
use serde::Serialize;

pub const PROBLEM_JSON: &str = "application/problem+json";

/// One invalid input, pointing at the field that caused it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldError {
    pub field: &'static str,
    pub code: &'static str,
    pub message: String,
}

impl FieldError {
    pub fn new(field: &'static str, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            field,
            code,
            message: message.into(),
        }
    }
}

/// An error on its way to becoming an HTTP response.
///
/// The payload is boxed so that `Result<T, ApiError>` stays one pointer
/// wide on the happy path.
#[derive(Debug)]
pub struct ApiError(Box<Problem>);

/// `detail` is what the client sees. `internal` is logged but never sent,
/// so server side failures do not leak implementation details.
#[derive(Debug)]
struct Problem {
    status: StatusCode,
    code: &'static str,
    title: &'static str,
    detail: String,
    errors: Vec<FieldError>,
    retry_after: Option<u64>,
    internal: Option<String>,
}

impl ApiError {
    pub fn new(
        status: StatusCode,
        code: &'static str,
        title: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Self(Box::new(Problem {
            status,
            code,
            title,
            detail: detail.into(),
            errors: Vec::new(),
            retry_after: None,
            internal: None,
        }))
    }

    pub fn validation(errors: Vec<FieldError>) -> Self {
        let mut error = Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            "Request validation failed",
            "One or more fields are invalid. See `errors` for details.",
        );
        error.0.errors = errors;
        error
    }

    pub fn unauthenticated() -> Self {
        // One message for every cause, so a caller cannot tell a wrong key
        // from a wrong signature or an expired timestamp.
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "Authentication failed",
            "The request signature is missing, expired or invalid.",
        )
    }

    pub fn rate_limited(wait: Duration) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Too many requests",
            "The request rate limit for this account was exceeded.",
        )
        .retry_after(wait)
    }

    pub fn payload_too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "Request body too large",
            "The request body exceeds the configured limit.",
        )
    }

    pub fn internal(reason: impl Into<String>) -> Self {
        let mut error = Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Internal server error",
            "An unexpected error occurred.",
        );
        error.0.internal = Some(reason.into());
        error
    }

    fn unavailable(code: &'static str, detail: impl Into<String>, reason: String) -> Self {
        let mut error = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            code,
            "Service unavailable",
            detail,
        )
        .retry_after(Duration::from_secs(1));
        error.0.internal = Some(reason);
        error
    }

    pub(crate) fn retry_after(mut self, wait: Duration) -> Self {
        // Retry-After takes whole seconds; round up so clients never retry early.
        let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
        self.0.retry_after = Some(secs.max(1));
        self
    }

    pub fn status(&self) -> StatusCode {
        self.0.status
    }

    pub fn code(&self) -> &'static str {
        self.0.code
    }

    pub fn field_errors(&self) -> &[FieldError] {
        &self.0.errors
    }
}

#[derive(Serialize)]
struct ProblemBody<'a> {
    #[serde(rename = "type")]
    kind: String,
    title: &'a str,
    status: u16,
    detail: &'a str,
    code: &'a str,
    #[serde(skip_serializing_if = "<[FieldError]>::is_empty")]
    errors: &'a [FieldError],
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let problem = *self.0;
        if problem.status.is_server_error() {
            tracing::error!(
                code = problem.code,
                reason = problem.internal.as_deref().unwrap_or(&problem.detail),
                "request failed"
            );
        }
        let body = ProblemBody {
            kind: format!("urn:orderflow:problem:{}", problem.code),
            title: problem.title,
            status: problem.status.as_u16(),
            detail: &problem.detail,
            code: problem.code,
            errors: &problem.errors,
        };
        let mut response = (problem.status, Json(body)).into_response();
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(PROBLEM_JSON));
        if let Some(secs) = problem.retry_after {
            headers.insert(header::RETRY_AFTER, HeaderValue::from(secs));
        }
        response
    }
}

impl From<ApplicationError> for ApiError {
    fn from(error: ApplicationError) -> Self {
        match error {
            ApplicationError::Domain(inner) => inner.into(),
            ApplicationError::UnknownMarket(market) => Self::new(
                StatusCode::NOT_FOUND,
                "unknown_market",
                "Market not found",
                format!("Market {market} is not listed."),
            ),
            ApplicationError::OrderNotFound(id) => order_not_found(&id.to_string()),
            ApplicationError::OrderNotOpen { id, status } => Self::new(
                StatusCode::CONFLICT,
                "order_not_open",
                "Order is not open",
                format!("Order {id} is already {status}."),
            ),
            ApplicationError::InvalidIdempotencyKey => Self::validation(vec![FieldError::new(
                "Idempotency-Key",
                "invalid_idempotency_key",
                ApplicationError::InvalidIdempotencyKey.to_string(),
            )]),
            ApplicationError::IdempotencyKeyReused => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "idempotency_key_reused",
                "Idempotency key reused",
                "This Idempotency-Key was already used with a different request body.",
            ),
            ApplicationError::IdempotencyKeyInFlight => Self::new(
                StatusCode::CONFLICT,
                "idempotency_key_in_flight",
                "Request still in progress",
                "A request with this Idempotency-Key is still being processed.",
            )
            .retry_after(Duration::from_secs(1)),
            ApplicationError::Overloaded(market) => Self::unavailable(
                "market_overloaded",
                format!("Market {market} is at capacity. Retry shortly."),
                format!("engine queue full for {market}"),
            ),
            ApplicationError::Unavailable(reason) => Self::unavailable(
                "service_unavailable",
                "The service is temporarily unavailable.",
                reason.to_owned(),
            ),
            ApplicationError::Storage(inner) => Self::unavailable(
                "storage_unavailable",
                "The service is temporarily unavailable.",
                inner.to_string(),
            ),
        }
    }
}

impl From<DomainError> for ApiError {
    fn from(error: DomainError) -> Self {
        let field = |name: &'static str, error: &DomainError| {
            Self::validation(vec![FieldError::new(name, error.code(), error.to_string())])
        };
        match &error {
            DomainError::NonPositivePrice | DomainError::PriceNotOnTick { .. } => {
                field("price", &error)
            }
            DomainError::NonPositiveQuantity
            | DomainError::QuantityNotOnLot { .. }
            | DomainError::QuantityBelowMinimum { .. }
            | DomainError::QuantityAboveMaximum { .. } => field("quantity", &error),
            DomainError::PostOnlyRequiresGtc | DomainError::StopOrderPostOnly => {
                field("post_only", &error)
            }
            DomainError::InvalidClientOrderId => field("client_order_id", &error),
            DomainError::PostOnlyWouldCross => Self::new(
                StatusCode::CONFLICT,
                "post_only_would_cross",
                "Order would take liquidity",
                "A post-only order must not match on arrival; it was rejected.",
            ),
            DomainError::StopWouldTriggerImmediately { .. } => Self::new(
                StatusCode::CONFLICT,
                "stop_would_trigger_immediately",
                "Stop would trigger immediately",
                format!("{error}. Choose a stop price the market has not reached yet."),
            ),
            DomainError::InvalidMarketId => Self::new(
                StatusCode::NOT_FOUND,
                "unknown_market",
                "Market not found",
                error.to_string(),
            ),
            DomainError::InvalidOrderId => order_not_found("that id"),
            DomainError::InvalidAssetCode => field("asset", &error),
            DomainError::OrderNotFound(id) => order_not_found(&id.to_string()),
            DomainError::DuplicateOrderId(_)
            | DomainError::InvalidAsset(_)
            | DomainError::DuplicateAsset(_)
            | DomainError::DuplicateMarket(_)
            | DomainError::UnknownAsset(_)
            | DomainError::LotSizeTooPrecise { .. }
            | DomainError::InvalidAccountId
            | DomainError::MarketMismatch { .. }
            | DomainError::InvalidMarketSpec(_)
            | DomainError::ArithmeticOverflow
            | DomainError::InvariantViolation(_) => Self::internal(error.to_string()),
        }
    }
}

fn order_not_found(id: &str) -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "order_not_found",
        "Order not found",
        format!("No order with {id} exists for this account and market."),
    )
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        let (code, title) = match &rejection {
            JsonRejection::JsonDataError(_) => {
                ("invalid_body", "Request body does not match the schema")
            }
            JsonRejection::JsonSyntaxError(_) => {
                ("malformed_json", "Request body is not valid JSON")
            }
            JsonRejection::MissingJsonContentType(_) => (
                "unsupported_media_type",
                "Expected an application/json body",
            ),
            _ => ("unreadable_body", "Request body could not be read"),
        };
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            return Self::payload_too_large();
        }
        Self::new(rejection.status(), code, title, rejection.body_text())
    }
}

impl From<PathRejection> for ApiError {
    fn from(rejection: PathRejection) -> Self {
        Self::new(
            rejection.status(),
            "invalid_path",
            "Invalid path parameter",
            rejection.body_text(),
        )
    }
}

impl From<QueryRejection> for ApiError {
    fn from(rejection: QueryRejection) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_query",
            "Invalid query string",
            rejection.body_text(),
        )
    }
}
