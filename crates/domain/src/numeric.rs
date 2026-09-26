//! Exact decimal value objects for prices and quantities.

use std::fmt;

use rust_decimal::Decimal;

use crate::error::DomainError;

/// A strictly positive price, expressed in units of the quote asset.
///
/// Pattern: Value Object (Newtype). Wrapping `Decimal` stops a quantity from
/// being passed where a price is expected, and the only constructor enforces
/// the invariant, so every `Price` in the program is known to be valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(Decimal);

impl Price {
    /// Builds a price, rejecting zero and negative values.
    pub fn new(value: Decimal) -> Result<Self, DomainError> {
        if value <= Decimal::ZERO {
            return Err(DomainError::NonPositivePrice);
        }
        // Normalizing strips trailing zeros, so `100.0` and `100` land on the
        // same price level and print the same way.
        Ok(Self(value.normalize()))
    }

    pub fn value(self) -> Decimal {
        self.0
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A non-negative amount of the base asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Quantity(Decimal);

impl Quantity {
    pub const ZERO: Self = Self(Decimal::ZERO);

    /// Builds a quantity that must be greater than zero, as order sizes are.
    pub fn positive(value: Decimal) -> Result<Self, DomainError> {
        if value <= Decimal::ZERO {
            return Err(DomainError::NonPositiveQuantity);
        }
        Ok(Self(value.normalize()))
    }

    pub fn value(self) -> Decimal {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0.is_zero()
    }

    /// Adds two quantities and reports overflow instead of panicking.
    pub fn checked_add(self, other: Self) -> Result<Self, DomainError> {
        self.0
            .checked_add(other.0)
            .map(|sum| Self(sum.normalize()))
            .ok_or(DomainError::ArithmeticOverflow)
    }

    /// Subtracts `other`, flooring at zero. Both operands are non-negative, so
    /// the difference cannot overflow.
    pub fn saturating_sub(self, other: Self) -> Self {
        if other.0 >= self.0 {
            Self::ZERO
        } else {
            Self((self.0 - other.0).normalize())
        }
    }
}

impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn price_rejects_zero_and_negative_values() {
        assert_eq!(Price::new(dec!(0)), Err(DomainError::NonPositivePrice));
        assert_eq!(Price::new(dec!(-1)), Err(DomainError::NonPositivePrice));
    }

    #[test]
    fn equal_prices_with_different_scale_are_the_same_level() {
        let a = Price::new(dec!(100.00)).unwrap();
        let b = Price::new(dec!(100)).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.to_string(), "100");
    }

    #[test]
    fn quantity_subtraction_floors_at_zero() {
        let one = Quantity::positive(dec!(1)).unwrap();
        let two = Quantity::positive(dec!(2)).unwrap();
        assert_eq!(one.saturating_sub(two), Quantity::ZERO);
        assert_eq!(two.saturating_sub(one), one);
    }

    #[test]
    fn quantity_addition_reports_overflow() {
        let max = Quantity::positive(Decimal::MAX).unwrap();
        assert_eq!(max.checked_add(max), Err(DomainError::ArithmeticOverflow));
    }
}
