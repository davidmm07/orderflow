//! Who reports prices, and how much the index trusts them.

use std::{fmt, sync::Arc, time::Duration};

use crate::error::MarketDataError;

/// Identifier of one price source, such as `kraken` or
/// `uniswap_v3:usdc_weth_500`.
///
/// Ids end up in metric labels, database rows and API responses, so the
/// alphabet is restricted and the length is capped.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(Arc<str>);

impl SourceId {
    pub fn parse(raw: &str) -> Result<Self, MarketDataError> {
        let valid = (1..=64).contains(&raw.len())
            && raw.bytes().all(|b| {
                b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || matches!(b, b'_' | b'-' | b'.' | b':')
            });
        if valid {
            Ok(Self(Arc::from(raw)))
        } else {
            Err(MarketDataError::InvalidSourceId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What kind of backend a source is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceKind {
    /// Publishes prices it computed from many venues, such as CoinGecko.
    Aggregator,
    /// A centralized exchange order book.
    Cex,
    /// An on-chain pool read by one of our own indexers.
    Dex,
    /// Orderflow's own matching engine.
    Venue,
}

impl SourceKind {
    pub const ALL: [Self; 4] = [Self::Aggregator, Self::Cex, Self::Dex, Self::Venue];

    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == raw)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Aggregator => "aggregator",
            Self::Cex => "cex",
            Self::Dex => "dex",
            Self::Venue => "venue",
        }
    }
}

/// Whether a source's quotes move the index or are only watched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceRole {
    /// Contributes to the index price.
    Index,
    /// Recorded and compared with the index, never part of it. Orderflow's
    /// own trades use this role: a thin internal book must not be able to
    /// move the reference price the venue itself is measured against.
    Monitor,
}

impl SourceRole {
    pub fn parse(raw: &str) -> Option<Self> {
        [Self::Index, Self::Monitor]
            .into_iter()
            .find(|role| role.as_str() == raw)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Monitor => "monitor",
        }
    }
}

/// One source as configured for one market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    pub id: SourceId,
    pub kind: SourceKind,
    pub role: SourceRole,
    /// Quotes older than this no longer count. Each source has its own
    /// value: an aggregator refreshing every minute would otherwise look
    /// dead next to an exchange streaming every trade.
    pub max_age: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_ids_accept_a_restricted_alphabet() {
        assert!(SourceId::parse("uniswap_v3:usdc_weth_500").is_ok());
        assert!(SourceId::parse("orca:sol-usdc.4").is_ok());
        for bad in ["", "Kraken", "kraken pro", "x\n", &"a".repeat(65)] {
            assert_eq!(SourceId::parse(bad), Err(MarketDataError::InvalidSourceId));
        }
    }
}
