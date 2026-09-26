//! Extractors whose rejections are problem responses instead of plain text.
//!
//! Pattern: Adapter. Each type wraps an axum extractor and only changes its
//! rejection type, so every error the API emits has the same JSON shape.

use axum::extract::{FromRequest, FromRequestParts};

use crate::error::ApiError;

#[derive(Debug, FromRequest)]
#[from_request(via(axum::Json), rejection(ApiError))]
pub struct ApiJson<T>(pub T);

#[derive(Debug, FromRequestParts)]
#[from_request(via(axum::extract::Path), rejection(ApiError))]
pub struct ApiPath<T>(pub T);

#[derive(Debug, FromRequestParts)]
#[from_request(via(axum::extract::Query), rejection(ApiError))]
pub struct ApiQuery<T>(pub T);
