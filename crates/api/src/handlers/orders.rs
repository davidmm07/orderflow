//! Private order endpoints. Every route here sits behind signature checks
//! and rate limiting, applied by the router.

use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use orderflow_application::{ApplicationError, CancelOrderCommand, IdempotencyKey};
use orderflow_domain::AccountId;

use super::{market_id, order_id};
use crate::{
    auth::Authenticated,
    dto::{OrderView, PlaceOrderRequest, PlaceOrderResponse},
    error::ApiError,
    extract::{ApiJson, ApiPath},
    state::AppState,
};

const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
const IDEMPOTENT_REPLAYED: HeaderName = HeaderName::from_static("idempotent-replayed");

/// `POST /v1/markets/{market}/orders`
///
/// Returns 201 with the order and its immediate fills. With an
/// `Idempotency-Key` header, a retry of the same request returns the
/// original response and sets `Idempotent-Replayed: true`.
pub async fn place(
    State(state): State<AppState>,
    Extension(Authenticated(account)): Extension<Authenticated>,
    ApiPath(market): ApiPath<String>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<PlaceOrderRequest>,
) -> Result<Response, ApiError> {
    let market = market_id(&market)?;
    let spec = state.queries.market(&market)?;
    let key = idempotency_key(&headers, &account)?;
    let command = body.into_command(account, &spec)?;

    let placement = state.place_order.execute(command, key).await?;

    let location = format!(
        "/v1/markets/{market}/orders/{}",
        placement.receipt.order.id()
    );
    let mut response = (
        StatusCode::CREATED,
        Json(PlaceOrderResponse::from(&placement.receipt)),
    )
        .into_response();
    let headers = response.headers_mut();
    if let Ok(location) = HeaderValue::from_str(&location) {
        headers.insert(header::LOCATION, location);
    }
    if placement.replayed {
        headers.insert(IDEMPOTENT_REPLAYED, HeaderValue::from_static("true"));
    }
    Ok(response)
}

/// `GET /v1/markets/{market}/orders/{order_id}`
pub async fn get(
    State(state): State<AppState>,
    Extension(Authenticated(account)): Extension<Authenticated>,
    ApiPath((market, id)): ApiPath<(String, String)>,
) -> Result<Json<OrderView>, ApiError> {
    let market = market_id(&market)?;
    let id = order_id(&id)?;
    let order = state.queries.order(&account, &market, id).await?;
    Ok(Json(OrderView::from(&order)))
}

/// `DELETE /v1/markets/{market}/orders/{order_id}`
///
/// Returns 200 with the cancelled order, or 409 if it already finished.
pub async fn cancel(
    State(state): State<AppState>,
    Extension(Authenticated(account)): Extension<Authenticated>,
    ApiPath((market, id)): ApiPath<(String, String)>,
) -> Result<Json<OrderView>, ApiError> {
    let command = CancelOrderCommand {
        account,
        market: market_id(&market)?,
        order_id: order_id(&id)?,
    };
    let order = state.cancel_order.execute(command).await?;
    Ok(Json(OrderView::from(&order)))
}

fn idempotency_key(
    headers: &HeaderMap,
    account: &AccountId,
) -> Result<Option<IdempotencyKey>, ApiError> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY) else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| ApplicationError::InvalidIdempotencyKey)?;
    Ok(Some(IdempotencyKey::new(account.clone(), raw)?))
}
