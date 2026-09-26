//! Turns untrusted request DTOs into validated commands.
//!
//! "Parse, don't validate": the output is a `PlaceOrderCommand` made of
//! domain value objects, so code after this point cannot receive a negative
//! price or an unknown side, because such values cannot be represented.

use orderflow_application::PlaceOrderCommand;
use orderflow_domain::{
    AccountId, ClientOrderId, Decimal, DomainError, MarketSpec, OrderKind, Price, Quantity,
    SelfTradePrevention, Side, TimeInForce,
};

use crate::{
    dto::PlaceOrderRequest,
    error::{ApiError, FieldError},
};

/// Longest decimal string accepted. Real prices need far fewer characters;
/// the cap stops pathological inputs before they reach the parser.
const MAX_DECIMAL_CHARS: usize = 40;

const DECIMAL_HINT: &str = "must be a positive decimal string such as \"101.25\"";

/// Collects every field error instead of stopping at the first one.
#[derive(Default)]
struct Collector {
    errors: Vec<FieldError>,
}

struct Invalid {
    code: &'static str,
    message: String,
}

impl Invalid {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn from_domain(error: &DomainError) -> Self {
        Self::new(error.code(), error.to_string())
    }
}

impl Collector {
    fn push(&mut self, field: &'static str, code: &'static str, message: impl Into<String>) {
        self.errors.push(FieldError::new(field, code, message));
    }

    fn required<T>(
        &mut self,
        field: &'static str,
        raw: Option<&str>,
        parse: impl FnOnce(&str) -> Result<T, Invalid>,
    ) -> Option<T> {
        match raw {
            None => {
                self.push(field, "required", "is required");
                None
            }
            Some(raw) => self.optional(field, Some(raw), parse),
        }
    }

    fn optional<T>(
        &mut self,
        field: &'static str,
        raw: Option<&str>,
        parse: impl FnOnce(&str) -> Result<T, Invalid>,
    ) -> Option<T> {
        match parse(raw?) {
            Ok(value) => Some(value),
            Err(invalid) => {
                self.push(field, invalid.code, invalid.message);
                None
            }
        }
    }

    fn forbid(&mut self, field: &'static str, present: bool, message: &str) {
        if present {
            self.push(field, "not_allowed", message);
        }
    }

    /// Keeps `value` only if the market rule accepts it.
    fn rule<T>(
        &mut self,
        field: &'static str,
        value: Option<T>,
        rule: impl FnOnce(&T) -> Result<(), DomainError>,
    ) -> Option<T> {
        let value = value?;
        match rule(&value) {
            Ok(()) => Some(value),
            Err(error) => {
                self.push(field, error.code(), error.to_string());
                None
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OrderType {
    Limit,
    Market,
    StopLimit,
    StopMarket,
}

impl OrderType {
    fn is_stop(self) -> bool {
        matches!(self, Self::StopLimit | Self::StopMarket)
    }

    fn has_limit_price(self) -> bool {
        matches!(self, Self::Limit | Self::StopLimit)
    }
}

impl PlaceOrderRequest {
    /// Validates the request against syntax rules and the market's trading
    /// parameters. The engine re-checks the market rules itself; checking
    /// them here as well lets one response list every problem at once.
    pub(crate) fn into_command(
        self,
        account: AccountId,
        spec: &MarketSpec,
    ) -> Result<PlaceOrderCommand, ApiError> {
        let mut c = Collector::default();

        let side = c.required("side", self.side.as_deref(), parse_side);
        let order_type = c.required("type", self.order_type.as_deref(), parse_order_type);
        let quantity = c.required("quantity", self.quantity.as_deref(), parse_quantity);
        let quantity = c.rule("quantity", quantity, |q| spec.check_quantity(*q));

        let stop_price = match order_type {
            Some(kind) if kind.is_stop() => {
                let stop = c.required("stop_price", self.stop_price.as_deref(), parse_price);
                c.rule("stop_price", stop, |p| spec.check_price(*p))
            }
            Some(_) => {
                c.forbid(
                    "stop_price",
                    self.stop_price.is_some(),
                    "only stop_limit and stop_market orders take a stop price",
                );
                None
            }
            None => None,
        };

        let kind = match order_type {
            Some(kind) if kind.has_limit_price() => {
                let price = c.required("price", self.price.as_deref(), parse_price);
                let price = c.rule("price", price, |p| spec.check_price(*p));
                let time_in_force = c
                    .optional("time_in_force", self.time_in_force.as_deref(), parse_tif)
                    .unwrap_or(TimeInForce::GoodTilCancelled);
                let post_only = if kind.is_stop() {
                    c.forbid(
                        "post_only",
                        self.post_only == Some(true),
                        "stop orders cannot be post-only",
                    );
                    false
                } else {
                    self.post_only.unwrap_or(false)
                };
                price.and_then(
                    |price| match OrderKind::limit(price, time_in_force, post_only) {
                        Ok(kind) => Some(kind),
                        Err(error) => {
                            c.push("post_only", error.code(), error.to_string());
                            None
                        }
                    },
                )
            }
            Some(_) => {
                c.forbid("price", self.price.is_some(), "market orders take no price");
                c.forbid(
                    "time_in_force",
                    self.time_in_force.is_some(),
                    "market orders always execute immediately",
                );
                c.forbid(
                    "post_only",
                    self.post_only == Some(true),
                    "market orders always take liquidity",
                );
                Some(OrderKind::Market)
            }
            None => None,
        };

        let client_order_id =
            c.optional("client_order_id", self.client_order_id.as_deref(), |raw| {
                ClientOrderId::parse(raw).map_err(|e| Invalid::from_domain(&e))
            });
        let self_trade_prevention = c
            .optional(
                "self_trade_prevention",
                self.self_trade_prevention.as_deref(),
                parse_stp,
            )
            .unwrap_or_default();

        match (side, kind, quantity) {
            (Some(side), Some(kind), Some(quantity)) if c.errors.is_empty() => {
                Ok(PlaceOrderCommand {
                    account,
                    market: spec.id().clone(),
                    side,
                    kind,
                    quantity,
                    stop_price,
                    client_order_id,
                    self_trade_prevention,
                })
            }
            _ => Err(ApiError::validation(c.errors)),
        }
    }
}

fn parse_side(raw: &str) -> Result<Side, Invalid> {
    match raw {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(Invalid::new("invalid_enum", "must be one of: buy, sell")),
    }
}

fn parse_order_type(raw: &str) -> Result<OrderType, Invalid> {
    match raw {
        "limit" => Ok(OrderType::Limit),
        "market" => Ok(OrderType::Market),
        "stop_limit" => Ok(OrderType::StopLimit),
        "stop_market" => Ok(OrderType::StopMarket),
        _ => Err(Invalid::new(
            "invalid_enum",
            "must be one of: limit, market, stop_limit, stop_market",
        )),
    }
}

fn parse_tif(raw: &str) -> Result<TimeInForce, Invalid> {
    match raw {
        "gtc" => Ok(TimeInForce::GoodTilCancelled),
        "ioc" => Ok(TimeInForce::ImmediateOrCancel),
        "fok" => Ok(TimeInForce::FillOrKill),
        _ => Err(Invalid::new(
            "invalid_enum",
            "must be one of: gtc, ioc, fok",
        )),
    }
}

fn parse_stp(raw: &str) -> Result<SelfTradePrevention, Invalid> {
    match raw {
        "cancel_newest" => Ok(SelfTradePrevention::CancelNewest),
        "cancel_oldest" => Ok(SelfTradePrevention::CancelOldest),
        _ => Err(Invalid::new(
            "invalid_enum",
            "must be one of: cancel_newest, cancel_oldest",
        )),
    }
}

/// Accepts plain decimal notation only: digits and at most one dot. Signs,
/// exponents, whitespace and separators are rejected outright.
fn parse_decimal(raw: &str) -> Result<Decimal, Invalid> {
    let well_formed = !raw.is_empty()
        && raw.len() <= MAX_DECIMAL_CHARS
        && raw.bytes().all(|b| b.is_ascii_digit() || b == b'.');
    if !well_formed {
        return Err(Invalid::new("invalid_decimal", DECIMAL_HINT));
    }
    Decimal::from_str_exact(raw).map_err(|_| Invalid::new("invalid_decimal", DECIMAL_HINT))
}

fn parse_price(raw: &str) -> Result<Price, Invalid> {
    Price::new(parse_decimal(raw)?).map_err(|e| Invalid::from_domain(&e))
}

fn parse_quantity(raw: &str) -> Result<Quantity, Invalid> {
    Quantity::positive(parse_decimal(raw)?).map_err(|e| Invalid::from_domain(&e))
}

#[cfg(test)]
mod tests {
    use orderflow_domain::MarketId;

    use super::*;

    fn spec() -> MarketSpec {
        MarketSpec::builder(MarketId::parse("BTC-USD").unwrap())
            .tick_size(Decimal::new(1, 2))
            .lot_size(Decimal::new(1, 3))
            .max_quantity(Decimal::from(100))
            .build()
            .unwrap()
    }

    fn request() -> PlaceOrderRequest {
        PlaceOrderRequest {
            side: Some("buy".into()),
            order_type: Some("limit".into()),
            price: Some("100.25".into()),
            quantity: Some("0.5".into()),
            stop_price: None,
            time_in_force: None,
            post_only: None,
            client_order_id: None,
            self_trade_prevention: None,
        }
    }

    fn fields(result: Result<PlaceOrderCommand, ApiError>) -> Vec<&'static str> {
        let error = result.unwrap_err();
        assert_eq!(error.code(), "validation_failed");
        error.field_errors().iter().map(|e| e.field).collect()
    }

    fn account() -> AccountId {
        AccountId::parse("alice").unwrap()
    }

    #[test]
    fn valid_limit_order_becomes_a_command_with_defaults() {
        let command = request().into_command(account(), &spec()).unwrap();
        assert_eq!(command.side, Side::Buy);
        assert_eq!(
            command.kind.time_in_force(),
            Some(TimeInForce::GoodTilCancelled)
        );
        assert_eq!(
            command.self_trade_prevention,
            SelfTradePrevention::CancelNewest
        );
    }

    #[test]
    fn market_rules_are_reported_per_field() {
        let mut req = request();
        req.price = Some("100.255".into());
        req.quantity = Some("0.0005".into());
        req.side = Some("up".into());
        assert_eq!(
            fields(req.into_command(account(), &spec())),
            ["side", "quantity", "price"]
        );
    }

    #[test]
    fn stop_orders_need_a_stop_price_and_other_orders_reject_one() {
        let mut stop = request();
        stop.order_type = Some("stop_limit".into());
        assert_eq!(
            fields(stop.into_command(account(), &spec())),
            ["stop_price"]
        );

        let mut limit = request();
        limit.stop_price = Some("99".into());
        assert_eq!(
            fields(limit.into_command(account(), &spec())),
            ["stop_price"]
        );

        let mut stop_market = request();
        stop_market.order_type = Some("stop_market".into());
        stop_market.price = None;
        stop_market.stop_price = Some("99.5".into());
        let command = stop_market.into_command(account(), &spec()).unwrap();
        assert_eq!(command.kind, OrderKind::Market);
        assert!(command.stop_price.is_some());
    }

    #[test]
    fn stop_limit_orders_cannot_be_post_only() {
        let mut req = request();
        req.order_type = Some("stop_limit".into());
        req.stop_price = Some("101".into());
        req.post_only = Some(true);
        assert_eq!(fields(req.into_command(account(), &spec())), ["post_only"]);
    }

    #[test]
    fn rejects_non_plain_decimals() {
        for raw in ["-1", "1e3", " 1", "1,5", "+2", "", "0x10", "1.2.3"] {
            assert!(parse_decimal(raw).is_err(), "{raw:?} should be rejected");
        }
        assert!(parse_decimal(&"9".repeat(MAX_DECIMAL_CHARS + 1)).is_err());
        assert_eq!(parse_decimal("0.10").ok(), Some(Decimal::new(10, 2)));
    }

    #[test]
    fn market_orders_must_not_carry_limit_fields() {
        let mut req = request();
        req.order_type = Some("market".into());
        req.time_in_force = Some("gtc".into());
        assert_eq!(
            fields(req.into_command(account(), &spec())),
            ["price", "time_in_force"]
        );
    }

    #[test]
    fn missing_fields_are_all_reported() {
        let req = PlaceOrderRequest {
            side: None,
            order_type: None,
            price: None,
            quantity: None,
            stop_price: None,
            time_in_force: None,
            post_only: None,
            client_order_id: None,
            self_trade_prevention: None,
        };
        assert_eq!(
            fields(req.into_command(account(), &spec())),
            ["side", "type", "quantity"]
        );
    }
}
