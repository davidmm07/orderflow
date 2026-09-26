//! Domain error type.

use rust_decimal::Decimal;

use crate::{
    asset::AssetCode,
    ids::OrderId,
    market::MarketId,
    numeric::{Price, Quantity},
};

/// Every way a domain operation can fail.
///
/// Variants carry typed data rather than preformatted strings, so outer
/// layers decide how to present them (HTTP status, field pointer, metric
/// label) without parsing messages.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    #[error("price must be greater than zero")]
    NonPositivePrice,
    #[error("quantity must be greater than zero")]
    NonPositiveQuantity,
    #[error("price {price} is not a multiple of the tick size {tick_size}")]
    PriceNotOnTick { price: Price, tick_size: Decimal },
    #[error("quantity {quantity} is not a multiple of the lot size {lot_size}")]
    QuantityNotOnLot {
        quantity: Quantity,
        lot_size: Decimal,
    },
    #[error("quantity {quantity} is below the market minimum of {minimum}")]
    QuantityBelowMinimum {
        quantity: Quantity,
        minimum: Quantity,
    },
    #[error("quantity {quantity} is above the market maximum of {maximum}")]
    QuantityAboveMaximum {
        quantity: Quantity,
        maximum: Quantity,
    },
    #[error("post-only orders must be good-til-cancelled")]
    PostOnlyRequiresGtc,
    #[error("post-only order would take liquidity from the book")]
    PostOnlyWouldCross,
    #[error("stop orders cannot be post-only")]
    StopOrderPostOnly,
    #[error(
        "stop price {stop_price} would trigger immediately at the last trade price {last_price}"
    )]
    StopWouldTriggerImmediately {
        stop_price: Price,
        last_price: Price,
    },
    #[error("order targets market {actual} but this engine serves {expected}")]
    MarketMismatch {
        expected: MarketId,
        actual: MarketId,
    },
    #[error("order {0} already exists")]
    DuplicateOrderId(OrderId),
    #[error("order {0} was not found")]
    OrderNotFound(OrderId),
    #[error("market id must look like BASE-QUOTE, for example BTC-USD")]
    InvalidMarketId,
    #[error("asset code must be 2 to 10 uppercase letters or digits")]
    InvalidAssetCode,
    #[error("invalid asset: {0}")]
    InvalidAsset(&'static str),
    #[error("asset {0} is listed twice")]
    DuplicateAsset(AssetCode),
    #[error("market {0} is listed twice")]
    DuplicateMarket(MarketId),
    #[error("asset {0} is not listed")]
    UnknownAsset(String),
    #[error(
        "market {market} has lot size {lot_size}, finer than the {decimals} decimals of its base asset"
    )]
    LotSizeTooPrecise {
        market: MarketId,
        lot_size: Decimal,
        decimals: u32,
    },
    #[error("account id must be 1 to 64 characters of letters, digits, '-' or '_'")]
    InvalidAccountId,
    #[error("client order id must be 1 to 36 characters of letters, digits, '-' or '_'")]
    InvalidClientOrderId,
    #[error("order id must be a UUID")]
    InvalidOrderId,
    #[error("invalid market specification: {0}")]
    InvalidMarketSpec(&'static str),
    #[error("arithmetic overflow")]
    ArithmeticOverflow,
    #[error("internal invariant violated: {0}")]
    InvariantViolation(&'static str),
}

impl DomainError {
    /// Stable, machine readable identifier for logs, metrics and API errors.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NonPositivePrice => "non_positive_price",
            Self::NonPositiveQuantity => "non_positive_quantity",
            Self::PriceNotOnTick { .. } => "price_not_on_tick",
            Self::QuantityNotOnLot { .. } => "quantity_not_on_lot",
            Self::QuantityBelowMinimum { .. } => "quantity_below_minimum",
            Self::QuantityAboveMaximum { .. } => "quantity_above_maximum",
            Self::PostOnlyRequiresGtc => "post_only_requires_gtc",
            Self::PostOnlyWouldCross => "post_only_would_cross",
            Self::StopOrderPostOnly => "stop_order_post_only",
            Self::StopWouldTriggerImmediately { .. } => "stop_would_trigger_immediately",
            Self::MarketMismatch { .. } => "market_mismatch",
            Self::DuplicateOrderId(_) => "duplicate_order_id",
            Self::OrderNotFound(_) => "order_not_found",
            Self::InvalidMarketId => "invalid_market_id",
            Self::InvalidAssetCode => "invalid_asset_code",
            Self::InvalidAsset(_) => "invalid_asset",
            Self::DuplicateAsset(_) => "duplicate_asset",
            Self::DuplicateMarket(_) => "duplicate_market",
            Self::UnknownAsset(_) => "unknown_asset",
            Self::LotSizeTooPrecise { .. } => "lot_size_too_precise",
            Self::InvalidAccountId => "invalid_account_id",
            Self::InvalidClientOrderId => "invalid_client_order_id",
            Self::InvalidOrderId => "invalid_order_id",
            Self::InvalidMarketSpec(_) => "invalid_market_spec",
            Self::ArithmeticOverflow => "arithmetic_overflow",
            Self::InvariantViolation(_) => "invariant_violation",
        }
    }
}
