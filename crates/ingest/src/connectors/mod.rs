//! Off-chain sources: exchange and aggregator APIs.

mod coinbase;
mod coingecko;
mod kraken;

pub use coinbase::{Coinbase, CoinbaseConfig};
pub use coingecko::{CoinGecko, CoinGeckoConfig, CoinGeckoQuote};
pub use kraken::{Kraken, KrakenConfig};

use orderflow_domain::Timestamp;
use rust_decimal::Decimal;

use crate::error::SourceError;

/// Parses a decimal as written in JSON, either plain (`2478.95`) or in
/// scientific notation (`1.2e-7`), without going through a binary float.
pub(crate) fn parse_decimal(raw: &str) -> Result<Decimal, SourceError> {
    let raw = raw.trim().trim_matches('"');
    let parsed = if raw.contains(['e', 'E']) {
        Decimal::from_scientific(raw)
    } else {
        Decimal::from_str_exact(raw)
    };
    parsed.map_err(|_| SourceError::decode(format_args!("not a decimal: {raw}")))
}

/// Parses an RFC 3339 time such as `2026-10-08T22:09:29.665602Z`.
pub(crate) fn parse_rfc3339(raw: &str) -> Result<Timestamp, SourceError> {
    let time = chrono::DateTime::parse_from_rfc3339(raw)
        .map_err(|_| SourceError::decode(format_args!("not an RFC 3339 time: {raw}")))?;
    let nanos = time
        .timestamp_nanos_opt()
        .and_then(|nanos| u64::try_from(nanos).ok())
        .ok_or_else(|| SourceError::decode("time out of range"))?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn decimals_parse_exactly_in_both_notations() {
        assert_eq!(parse_decimal("2478.95").unwrap(), dec!(2478.95));
        assert_eq!(parse_decimal("\"81760.51\"").unwrap(), dec!(81760.51));
        assert_eq!(parse_decimal("1.25e-7").unwrap(), dec!(0.000000125));
        assert!(parse_decimal("NaN").is_err());
    }

    #[test]
    fn rfc3339_times_keep_sub_second_precision() {
        let at = parse_rfc3339("2026-10-08T22:09:29.665602Z").unwrap();
        assert_eq!(at.unix_nanos() % 1_000_000_000, 665_602_000);
    }
}
