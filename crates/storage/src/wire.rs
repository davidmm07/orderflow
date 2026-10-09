//! JSON records for prices and candles stored outside the process: on the
//! Redis bus, in the Redis cache and in the `components` column.
//!
//! Decimals travel as strings and times as Unix nanoseconds, so a record
//! read back is exactly the value written. `schema_version` lets an edge
//! node running older code refuse records it does not understand during a
//! rolling deploy instead of misreading them.

use std::sync::Arc;

use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{
    Candle, Component, PriceStatus, PriceUpdate, QuoteState, Revision, SourceId, SourceKind,
    ports::StoreError,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct PriceUpdateRecord {
    schema_version: u16,
    market: String,
    epoch: u64,
    sequence: u64,
    price: String,
    status: String,
    as_of_ns: u64,
    published_at_ns: u64,
    spread_bps: String,
    components: Vec<ComponentRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ComponentRecord {
    source: String,
    kind: String,
    price: String,
    observed_at_ns: u64,
    state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deviation_bps: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CandleRecord {
    start_ns: u64,
    open: String,
    high: String,
    low: String,
    close: String,
    epoch: u64,
    sequence: u64,
}

pub fn encode_update(update: &PriceUpdate) -> String {
    let record = PriceUpdateRecord {
        schema_version: SCHEMA_VERSION,
        market: update.market.to_string(),
        epoch: update.epoch,
        sequence: update.sequence,
        price: update.price.to_string(),
        status: update.status.as_str().to_owned(),
        as_of_ns: update.as_of.unix_nanos(),
        published_at_ns: update.published_at.unix_nanos(),
        spread_bps: update.spread_bps.to_string(),
        components: component_records(&update.components),
    };
    // Serializing plain strings and integers cannot fail.
    serde_json::to_string(&record).unwrap_or_default()
}

pub fn decode_update(bytes: &[u8]) -> Result<PriceUpdate, StoreError> {
    let record: PriceUpdateRecord = serde_json::from_slice(bytes).map_err(corrupt)?;
    if record.schema_version != SCHEMA_VERSION {
        return Err(StoreError::Corrupt(format!(
            "price record schema {} is not supported",
            record.schema_version
        )));
    }
    Ok(PriceUpdate {
        market: MarketId::parse(&record.market).map_err(corrupt)?,
        epoch: record.epoch,
        sequence: record.sequence,
        price: decimal(&record.price)?,
        status: PriceStatus::parse(&record.status).ok_or_else(|| corrupt("status"))?,
        as_of: Timestamp::from_unix_nanos(record.as_of_ns),
        published_at: Timestamp::from_unix_nanos(record.published_at_ns),
        spread_bps: decimal(&record.spread_bps)?,
        components: components(record.components)?,
    })
}

pub(crate) fn component_records(components: &[Component]) -> Vec<ComponentRecord> {
    components
        .iter()
        .map(|component| ComponentRecord {
            source: component.source.to_string(),
            kind: component.kind.as_str().to_owned(),
            price: component.price.to_string(),
            observed_at_ns: component.observed_at.unix_nanos(),
            state: component.state.as_str().to_owned(),
            deviation_bps: component.deviation_bps.map(|bps| bps.to_string()),
        })
        .collect()
}

fn components(records: Vec<ComponentRecord>) -> Result<Vec<Component>, StoreError> {
    records
        .into_iter()
        .map(|record| {
            Ok(Component {
                source: SourceId::parse(&record.source).map_err(corrupt)?,
                kind: SourceKind::parse(&record.kind).ok_or_else(|| corrupt("source kind"))?,
                price: decimal(&record.price)?,
                observed_at: Timestamp::from_unix_nanos(record.observed_at_ns),
                state: QuoteState::parse(&record.state).ok_or_else(|| corrupt("quote state"))?,
                deviation_bps: record.deviation_bps.as_deref().map(decimal).transpose()?,
            })
        })
        .collect()
}

pub fn encode_candles(candles: &[Candle]) -> String {
    let records: Vec<CandleRecord> = candles
        .iter()
        .map(|candle| CandleRecord {
            start_ns: candle.start.unix_nanos(),
            open: candle.open.to_string(),
            high: candle.high.to_string(),
            low: candle.low.to_string(),
            close: candle.close.to_string(),
            epoch: candle.revision.epoch,
            sequence: candle.revision.sequence,
        })
        .collect();
    serde_json::to_string(&records).unwrap_or_default()
}

pub fn decode_candles(market: &MarketId, bytes: &[u8]) -> Result<Arc<[Candle]>, StoreError> {
    let records: Vec<CandleRecord> = serde_json::from_slice(bytes).map_err(corrupt)?;
    records
        .into_iter()
        .map(|record| {
            Ok(Candle {
                market: market.clone(),
                start: Timestamp::from_unix_nanos(record.start_ns),
                open: decimal(&record.open)?,
                high: decimal(&record.high)?,
                low: decimal(&record.low)?,
                close: decimal(&record.close)?,
                revision: Revision {
                    epoch: record.epoch,
                    sequence: record.sequence,
                },
            })
        })
        .collect()
}

fn decimal(raw: &str) -> Result<Decimal, StoreError> {
    Decimal::from_str_exact(raw).map_err(corrupt)
}

fn corrupt(error: impl std::fmt::Display) -> StoreError {
    StoreError::Corrupt(error.to_string())
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn sample_update() -> PriceUpdate {
        PriceUpdate {
            market: MarketId::parse("ETH-USDC").unwrap(),
            epoch: 1_791_497_000_000,
            sequence: 42,
            price: dec!(2478.324669500408464656),
            status: PriceStatus::Live,
            as_of: Timestamp::from_unix_nanos(1_791_497_322_123_456_789),
            published_at: Timestamp::from_unix_nanos(1_791_497_322_500_000_000),
            spread_bps: dec!(18.05),
            components: vec![Component {
                source: SourceId::parse("uniswap_v3:usdc_weth_500").unwrap(),
                kind: SourceKind::Dex,
                price: dec!(2478.324669500408464656),
                observed_at: Timestamp::from_unix_nanos(1_791_497_322_000_000_000),
                state: QuoteState::Used,
                deviation_bps: Some(dec!(0)),
            }],
        }
    }

    #[test]
    fn updates_round_trip_exactly() {
        let update = sample_update();
        assert_eq!(
            decode_update(encode_update(&update).as_bytes()).unwrap(),
            update
        );
    }

    #[test]
    fn unknown_schema_versions_are_refused() {
        let text =
            encode_update(&sample_update()).replace("\"schema_version\":1", "\"schema_version\":9");
        assert!(matches!(
            decode_update(text.as_bytes()),
            Err(StoreError::Corrupt(_))
        ));
    }

    #[test]
    fn candles_round_trip_exactly() {
        let market = MarketId::parse("BTC-USD").unwrap();
        let candle = Candle::opening(
            market.clone(),
            Timestamp::from_unix_nanos(60_000_000_000),
            dec!(81651.20865616859),
            Revision {
                epoch: 1,
                sequence: 2,
            },
        );
        let decoded = decode_candles(
            &market,
            encode_candles(std::slice::from_ref(&candle)).as_bytes(),
        )
        .unwrap();
        assert_eq!(decoded.as_ref(), [candle]);
    }
}
