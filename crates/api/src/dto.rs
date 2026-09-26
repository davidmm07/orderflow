//! JSON shapes of the public API.
//!
//! Pattern: Data Transfer Object. These types are the API contract and are
//! mapped by hand from domain types, so renaming a domain field leaves the
//! wire format unchanged. Decimals are sent as strings because most client
//! languages parse JSON numbers as binary floats, which cannot hold a price
//! such as 0.1 exactly.

use chrono::{DateTime, SecondsFormat, Utc};
use orderflow_application::OrderReceipt;
use orderflow_domain::{
    Asset, BookSnapshot, LevelView, MarketSpec, Order, OrderId, Timestamp, Trade,
};
use serde::{Deserialize, Serialize};

/// Body of `POST /v1/markets/{market}/orders`.
///
/// Every field is optional at the serde level. The validator then reports
/// all missing and invalid fields in one response, where serde would stop
/// at the first problem.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaceOrderRequest {
    pub side: Option<String>,
    #[serde(rename = "type")]
    pub order_type: Option<String>,
    pub price: Option<String>,
    pub quantity: Option<String>,
    pub stop_price: Option<String>,
    pub time_in_force: Option<String>,
    pub post_only: Option<bool>,
    pub client_order_id: Option<String>,
    pub self_trade_prevention: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookQuery {
    pub depth: Option<u32>,
}

/// Filters for `GET /v1/markets`. Both are optional and combine with AND.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketsQuery {
    pub base: Option<String>,
    pub quote: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OrderView {
    pub id: String,
    pub client_order_id: Option<String>,
    pub market: String,
    pub side: &'static str,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub price: Option<String>,
    pub stop_price: Option<String>,
    pub time_in_force: Option<&'static str>,
    pub post_only: bool,
    pub self_trade_prevention: &'static str,
    pub quantity: String,
    pub filled_quantity: String,
    pub remaining_quantity: String,
    pub status: &'static str,
    pub cancel_reason: Option<&'static str>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<&Order> for OrderView {
    fn from(order: &Order) -> Self {
        let kind = order.kind();
        Self {
            id: order.id().to_string(),
            client_order_id: order.client_order_id().map(|id| id.as_str().to_owned()),
            market: order.market().to_string(),
            side: order.side().as_str(),
            order_type: order.type_name(),
            price: kind.limit_price().map(|price| price.to_string()),
            stop_price: order.stop_price().map(|price| price.to_string()),
            time_in_force: kind.time_in_force().map(|tif| tif.as_str()),
            post_only: kind.is_post_only(),
            self_trade_prevention: order.self_trade_prevention().as_str(),
            quantity: order.quantity().to_string(),
            filled_quantity: order.filled().to_string(),
            remaining_quantity: order.remaining().to_string(),
            status: order.status().as_str(),
            cancel_reason: order.cancel_reason().map(|reason| reason.as_str()),
            created_at: rfc3339(order.created_at()),
            updated_at: rfc3339(order.updated_at()),
        }
    }
}

/// One execution from the point of view of the order that was just placed.
#[derive(Debug, Serialize)]
pub struct FillView {
    pub trade_id: u64,
    pub price: String,
    pub quantity: String,
    /// `taker` when the order matched on arrival, `maker` when it rested and
    /// a stop order fired by the same request traded against it.
    pub liquidity: &'static str,
    pub counterparty_order_id: String,
    pub executed_at: String,
}

impl FillView {
    fn new(trade: &Trade, order: OrderId) -> Self {
        let (liquidity, counterparty) = if trade.taker_order_id == order {
            ("taker", trade.maker_order_id)
        } else {
            ("maker", trade.taker_order_id)
        };
        Self {
            trade_id: trade.id.value(),
            price: trade.price.to_string(),
            quantity: trade.quantity.to_string(),
            liquidity,
            counterparty_order_id: counterparty.to_string(),
            executed_at: rfc3339(trade.executed_at),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PlaceOrderResponse {
    pub order: OrderView,
    pub fills: Vec<FillView>,
}

impl From<&OrderReceipt> for PlaceOrderResponse {
    fn from(receipt: &OrderReceipt) -> Self {
        Self {
            order: OrderView::from(&receipt.order),
            fills: receipt
                .trades
                .iter()
                .map(|trade| FillView::new(trade, receipt.order.id()))
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MarketView {
    pub id: String,
    pub base: String,
    pub quote: String,
    pub tick_size: String,
    pub lot_size: String,
    pub min_quantity: String,
    pub max_quantity: String,
}

impl From<&MarketSpec> for MarketView {
    fn from(spec: &MarketSpec) -> Self {
        Self {
            id: spec.id().to_string(),
            base: spec.id().base().to_owned(),
            quote: spec.id().quote().to_owned(),
            tick_size: spec.tick_size().to_string(),
            lot_size: spec.lot_size().to_string(),
            min_quantity: spec.min_quantity().to_string(),
            max_quantity: spec.max_quantity().to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MarketsResponse {
    pub markets: Vec<MarketView>,
}

#[derive(Debug, Serialize)]
pub struct AssetView {
    pub code: String,
    pub name: String,
    pub decimals: u32,
}

impl From<&Asset> for AssetView {
    fn from(asset: &Asset) -> Self {
        Self {
            code: asset.code().to_string(),
            name: asset.name().to_owned(),
            decimals: asset.decimals(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct AssetsResponse {
    pub assets: Vec<AssetView>,
}

#[derive(Debug, Serialize)]
pub struct LevelDto {
    pub price: String,
    pub quantity: String,
    pub orders: usize,
}

impl From<&LevelView> for LevelDto {
    fn from(level: &LevelView) -> Self {
        Self {
            price: level.price.to_string(),
            quantity: level.quantity.to_string(),
            orders: level.order_count,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BookView {
    pub market: String,
    /// Run of the engine; `sequence` counts from 1 again in each epoch.
    pub epoch: u64,
    pub sequence: u64,
    pub last_price: Option<String>,
    pub bids: Vec<LevelDto>,
    pub asks: Vec<LevelDto>,
}

impl From<&BookSnapshot> for BookView {
    fn from(book: &BookSnapshot) -> Self {
        Self {
            market: book.market.to_string(),
            epoch: book.epoch,
            sequence: book.sequence,
            last_price: book.last_price.map(|price| price.to_string()),
            bids: book.bids.iter().map(LevelDto::from).collect(),
            asks: book.asks.iter().map(LevelDto::from).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StatusView {
    pub status: &'static str,
}

fn rfc3339(timestamp: Timestamp) -> String {
    let nanos = i64::try_from(timestamp.unix_nanos()).unwrap_or(i64::MAX);
    DateTime::<Utc>::from_timestamp_nanos(nanos).to_rfc3339_opts(SecondsFormat::Nanos, true)
}
