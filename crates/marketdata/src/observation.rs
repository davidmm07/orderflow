//! A single price report from a single source.

use orderflow_domain::{MarketId, Timestamp};
use rust_decimal::Decimal;

use crate::{error::MarketDataError, source::SourceId};

/// One price reported by one source for one market at one time.
///
/// Sources are untrusted. The constructor rejects prices that are not
/// positive, and `PriceIndex` rejects quotes from the future, quotes that
/// arrive out of order and quotes from sources the market does not list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    market: MarketId,
    source: SourceId,
    price: Decimal,
    observed_at: Timestamp,
}

impl Observation {
    pub fn new(
        market: MarketId,
        source: SourceId,
        price: Decimal,
        observed_at: Timestamp,
    ) -> Result<Self, MarketDataError> {
        if price <= Decimal::ZERO {
            return Err(MarketDataError::NonPositivePrice);
        }
        Ok(Self {
            market,
            source,
            price: price.normalize(),
            observed_at,
        })
    }

    pub fn market(&self) -> &MarketId {
        &self.market
    }

    pub fn source(&self) -> &SourceId {
        &self.source
    }

    pub fn price(&self) -> Decimal {
        self.price
    }

    /// When the source says the price was valid: a trade time, a block
    /// time or, for sources that report no time, when it was received.
    pub fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
}
