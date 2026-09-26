//! Public market data.

use axum::{Json, extract::State};

use super::market_id;
use crate::{
    dto::{BookQuery, BookView, MarketView, MarketsResponse},
    error::{ApiError, FieldError},
    extract::{ApiPath, ApiQuery},
    state::AppState,
};

const DEFAULT_DEPTH: u32 = 10;
const MAX_DEPTH: u32 = 100;

/// `GET /v1/markets`
pub async fn list(State(state): State<AppState>) -> Json<MarketsResponse> {
    let markets = state
        .queries
        .markets()
        .iter()
        .map(MarketView::from)
        .collect();
    Json(MarketsResponse { markets })
}

/// `GET /v1/markets/{market}/book?depth=N`
pub async fn book(
    State(state): State<AppState>,
    ApiPath(market): ApiPath<String>,
    ApiQuery(query): ApiQuery<BookQuery>,
) -> Result<Json<BookView>, ApiError> {
    let market = market_id(&market)?;
    let depth = query.depth.unwrap_or(DEFAULT_DEPTH);
    if !(1..=MAX_DEPTH).contains(&depth) {
        return Err(ApiError::validation(vec![FieldError::new(
            "depth",
            "out_of_range",
            format!("must be between 1 and {MAX_DEPTH}"),
        )]));
    }
    let book = state.queries.order_book(&market, depth as usize).await?;
    Ok(Json(BookView::from(&book)))
}
