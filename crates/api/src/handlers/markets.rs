//! Public market data and instrument discovery.

use axum::{Json, extract::State};
use orderflow_domain::AssetCode;

use super::market_id;
use crate::{
    dto::{
        AssetView, AssetsResponse, BookQuery, BookView, MarketView, MarketsQuery, MarketsResponse,
    },
    error::{ApiError, FieldError},
    extract::{ApiPath, ApiQuery},
    state::AppState,
};

const DEFAULT_DEPTH: u32 = 10;
const MAX_DEPTH: u32 = 100;

/// `GET /v1/assets`
pub async fn assets(State(state): State<AppState>) -> Json<AssetsResponse> {
    let assets = state.queries.assets().iter().map(AssetView::from).collect();
    Json(AssetsResponse { assets })
}

/// `GET /v1/markets?base=BTC&quote=USD`
///
/// With many instruments listed, clients narrow the list by asset instead of
/// downloading everything and filtering it themselves.
pub async fn list(
    State(state): State<AppState>,
    ApiQuery(query): ApiQuery<MarketsQuery>,
) -> Result<Json<MarketsResponse>, ApiError> {
    let mut errors = Vec::new();
    let base = asset_filter("base", query.base.as_deref(), &mut errors);
    let quote = asset_filter("quote", query.quote.as_deref(), &mut errors);
    if !errors.is_empty() {
        return Err(ApiError::validation(errors));
    }
    let accepts = |value: &str, filter: &Option<AssetCode>| {
        filter.as_ref().is_none_or(|code| code.as_str() == value)
    };
    let markets = state
        .queries
        .markets()
        .iter()
        .filter(|spec| accepts(spec.id().base(), &base) && accepts(spec.id().quote(), &quote))
        .map(MarketView::from)
        .collect();
    Ok(Json(MarketsResponse { markets }))
}

/// `GET /v1/markets/{market}`
pub async fn get(
    State(state): State<AppState>,
    ApiPath(market): ApiPath<String>,
) -> Result<Json<MarketView>, ApiError> {
    let spec = state.queries.market(&market_id(&market)?)?;
    Ok(Json(MarketView::from(&spec)))
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

fn asset_filter(
    field: &'static str,
    raw: Option<&str>,
    errors: &mut Vec<FieldError>,
) -> Option<AssetCode> {
    match AssetCode::parse(raw?) {
        Ok(code) => Some(code),
        Err(error) => {
            errors.push(FieldError::new(field, error.code(), error.to_string()));
            None
        }
    }
}
