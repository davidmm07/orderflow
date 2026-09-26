//! Market identity and trading parameters.

use std::{fmt, sync::Arc};

use rust_decimal::Decimal;

use crate::{
    error::DomainError,
    numeric::{Price, Quantity},
    order::NewOrder,
};

/// Trading pair symbol such as `BTC-USD`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MarketId(Arc<str>);

impl MarketId {
    /// Parses `BASE-QUOTE` where each side is 2 to 10 uppercase letters or
    /// digits. The error does not echo the input, so hostile strings never
    /// reach log lines through this path.
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        let valid = raw
            .split_once('-')
            .is_some_and(|(base, quote)| is_asset_code(base) && is_asset_code(quote));
        if valid {
            Ok(Self(Arc::from(raw)))
        } else {
            Err(DomainError::InvalidMarketId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn base(&self) -> &str {
        self.0.split_once('-').map_or("", |(base, _)| base)
    }

    pub fn quote(&self) -> &str {
        self.0.split_once('-').map_or("", |(_, quote)| quote)
    }
}

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn is_asset_code(code: &str) -> bool {
    (2..=10).contains(&code.len())
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// Trading parameters for one market.
///
/// Every order is checked against these before it can touch the book, so the
/// engine only ever sees prices on the tick grid and sizes on the lot grid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketSpec {
    id: MarketId,
    tick_size: Decimal,
    lot_size: Decimal,
    min_quantity: Quantity,
    max_quantity: Quantity,
}

impl MarketSpec {
    pub fn builder(id: MarketId) -> MarketSpecBuilder {
        MarketSpecBuilder {
            id,
            tick_size: None,
            lot_size: None,
            min_quantity: None,
            max_quantity: None,
        }
    }

    pub fn id(&self) -> &MarketId {
        &self.id
    }

    pub fn tick_size(&self) -> Decimal {
        self.tick_size
    }

    pub fn lot_size(&self) -> Decimal {
        self.lot_size
    }

    pub fn min_quantity(&self) -> Quantity {
        self.min_quantity
    }

    pub fn max_quantity(&self) -> Quantity {
        self.max_quantity
    }

    /// Checks that a price sits on the tick grid.
    pub fn check_price(&self, price: Price) -> Result<(), DomainError> {
        if is_multiple_of(price.value(), self.tick_size) {
            Ok(())
        } else {
            Err(DomainError::PriceNotOnTick {
                price,
                tick_size: self.tick_size,
            })
        }
    }

    /// Checks that a size is inside the market limits and on the lot grid.
    pub fn check_quantity(&self, quantity: Quantity) -> Result<(), DomainError> {
        if quantity < self.min_quantity {
            return Err(DomainError::QuantityBelowMinimum {
                quantity,
                minimum: self.min_quantity,
            });
        }
        if quantity > self.max_quantity {
            return Err(DomainError::QuantityAboveMaximum {
                quantity,
                maximum: self.max_quantity,
            });
        }
        if !is_multiple_of(quantity.value(), self.lot_size) {
            return Err(DomainError::QuantityNotOnLot {
                quantity,
                lot_size: self.lot_size,
            });
        }
        Ok(())
    }

    /// Validates a whole order against this market.
    ///
    /// The API layer calls `check_price` and `check_quantity` too so it can
    /// report every field error in one response. The engine calls this method
    /// again anyway, because the domain must not trust its callers.
    pub fn validate(&self, order: &NewOrder) -> Result<(), DomainError> {
        if order.market != self.id {
            return Err(DomainError::MarketMismatch {
                expected: self.id.clone(),
                actual: order.market.clone(),
            });
        }
        if let Some(price) = order.kind.limit_price() {
            self.check_price(price)?;
        }
        if let Some(stop_price) = order.stop_price {
            self.check_price(stop_price)?;
        }
        self.check_quantity(order.quantity)
    }
}

fn is_multiple_of(value: Decimal, step: Decimal) -> bool {
    value.checked_rem(step).is_some_and(|rest| rest.is_zero())
}

/// Assembles a `MarketSpec` from loose configuration values.
///
/// Pattern: Builder. Parameters arrive one by one from a config file, and
/// `build` validates them as a set, so a half-configured `MarketSpec` can
/// never exist.
#[derive(Debug, Clone)]
pub struct MarketSpecBuilder {
    id: MarketId,
    tick_size: Option<Decimal>,
    lot_size: Option<Decimal>,
    min_quantity: Option<Decimal>,
    max_quantity: Option<Decimal>,
}

impl MarketSpecBuilder {
    pub fn tick_size(mut self, value: Decimal) -> Self {
        self.tick_size = Some(value);
        self
    }

    pub fn lot_size(mut self, value: Decimal) -> Self {
        self.lot_size = Some(value);
        self
    }

    pub fn min_quantity(mut self, value: Decimal) -> Self {
        self.min_quantity = Some(value);
        self
    }

    pub fn max_quantity(mut self, value: Decimal) -> Self {
        self.max_quantity = Some(value);
        self
    }

    pub fn build(self) -> Result<MarketSpec, DomainError> {
        let tick_size = positive(self.tick_size, "tick_size must be set and positive")?;
        let lot_size = positive(self.lot_size, "lot_size must be set and positive")?;
        // The smallest tradable size defaults to a single lot.
        let min_quantity = Quantity::positive(self.min_quantity.unwrap_or(lot_size))
            .map_err(|_| DomainError::InvalidMarketSpec("min_quantity must be positive"))?;
        let max_quantity = Quantity::positive(positive(
            self.max_quantity,
            "max_quantity must be set and positive",
        )?)
        .map_err(|_| DomainError::InvalidMarketSpec("max_quantity must be positive"))?;

        if max_quantity < min_quantity {
            return Err(DomainError::InvalidMarketSpec(
                "max_quantity must not be below min_quantity",
            ));
        }
        if !is_multiple_of(min_quantity.value(), lot_size) {
            return Err(DomainError::InvalidMarketSpec(
                "min_quantity must be a multiple of lot_size",
            ));
        }

        Ok(MarketSpec {
            id: self.id,
            tick_size: tick_size.normalize(),
            lot_size: lot_size.normalize(),
            min_quantity,
            max_quantity,
        })
    }
}

fn positive(value: Option<Decimal>, message: &'static str) -> Result<Decimal, DomainError> {
    match value {
        Some(v) if v > Decimal::ZERO => Ok(v),
        _ => Err(DomainError::InvalidMarketSpec(message)),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn btc_usd() -> MarketSpec {
        MarketSpec::builder(MarketId::parse("BTC-USD").unwrap())
            .tick_size(dec!(0.01))
            .lot_size(dec!(0.001))
            .min_quantity(dec!(0.001))
            .max_quantity(dec!(100))
            .build()
            .unwrap()
    }

    #[test]
    fn market_ids_follow_base_dash_quote() {
        let id = MarketId::parse("BTC-USD").unwrap();
        assert_eq!((id.base(), id.quote()), ("BTC", "USD"));
        for bad in [
            "btc-usd",
            "BTCUSD",
            "BTC-",
            "-USD",
            "B-USD",
            "BTC-USD-X",
            "BTC_USD",
        ] {
            assert_eq!(
                MarketId::parse(bad),
                Err(DomainError::InvalidMarketId),
                "{bad}"
            );
        }
    }

    #[test]
    fn prices_must_sit_on_the_tick_grid() {
        let spec = btc_usd();
        assert!(spec.check_price(Price::new(dec!(100.25)).unwrap()).is_ok());
        assert!(matches!(
            spec.check_price(Price::new(dec!(100.255)).unwrap()),
            Err(DomainError::PriceNotOnTick { .. })
        ));
    }

    #[test]
    fn quantities_are_bounded_and_on_the_lot_grid() {
        let spec = btc_usd();
        let q = |v| Quantity::positive(v).unwrap();
        assert!(spec.check_quantity(q(dec!(1.5))).is_ok());
        assert!(matches!(
            spec.check_quantity(q(dec!(0.0001))),
            Err(DomainError::QuantityBelowMinimum { .. })
        ));
        assert!(matches!(
            spec.check_quantity(q(dec!(101))),
            Err(DomainError::QuantityAboveMaximum { .. })
        ));
        assert!(matches!(
            spec.check_quantity(q(dec!(1.0005))),
            Err(DomainError::QuantityNotOnLot { .. })
        ));
    }

    #[test]
    fn builder_rejects_inconsistent_parameters() {
        let id = MarketId::parse("ETH-USD").unwrap();
        let missing_tick = MarketSpec::builder(id.clone())
            .lot_size(dec!(1))
            .max_quantity(dec!(10))
            .build();
        assert!(missing_tick.is_err());

        let min_above_max = MarketSpec::builder(id.clone())
            .tick_size(dec!(1))
            .lot_size(dec!(1))
            .min_quantity(dec!(20))
            .max_quantity(dec!(10))
            .build();
        assert!(min_above_max.is_err());

        let min_off_lot = MarketSpec::builder(id)
            .tick_size(dec!(1))
            .lot_size(dec!(1))
            .min_quantity(dec!(1.5))
            .max_quantity(dec!(10))
            .build();
        assert!(min_off_lot.is_err());
    }
}
