//! Orderflow HTTP API: the delivery mechanism of the clean architecture.
//!
//! This crate translates HTTP into use case calls and use case results back
//! into HTTP. It owns everything transport specific: routes, JSON shapes,
//! request signing, rate limiting and the RFC 9457 problem format. It holds
//! no business rules; those live in the domain and application crates.
//!
//! SOLID in this crate:
//! - SRP: routing, authentication, rate limiting, validation and error
//!   mapping each live in their own module.
//! - OCP: cross cutting concerns are tower layers. Adding one (for example
//!   CORS or compression) does not touch any handler.
//! - DIP: handlers depend on the application's use case types, and the
//!   router receives them through `AppState`, built by the composition root.

mod auth;
mod dto;
mod error;
mod extract;
mod handlers;
mod rate_limit;
mod router;
mod state;
mod validation;

pub use auth::{
    API_KEY_HEADER, Authenticator, Credential, CredentialError, SIGNATURE_HEADER, TIMESTAMP_HEADER,
    sign_request,
};
pub use error::{ApiError, FieldError, PROBLEM_JSON};
pub use rate_limit::{RateLimitConfig, RateLimiter};
pub use router::{ApiConfig, router};
pub use state::{AppState, MetricsRender};
