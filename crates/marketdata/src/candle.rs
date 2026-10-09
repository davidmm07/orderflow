//! OHLC candles of the index price.
//!
//! Only 1 minute candles are stored. Longer intervals are rolled up from
//! them when read, so there is a single write path and every interval
//! agrees with every other one by construction.

use std::{collections::BTreeMap, sync::Arc};

use orderflow_domain::{MarketId, Timestamp};
use rust_decimal::Decimal;

use crate::{aggregate::PriceStatus, error::MarketDataError, update::PriceUpdate};

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Position of an update in the price stream. When two writes for the same
/// candle race, the close from the later revision wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Revision {
    pub epoch: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CandleInterval {
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    FourHours,
    OneDay,
}

impl CandleInterval {
    pub const ALL: [Self; 6] = [
        Self::OneMinute,
        Self::FiveMinutes,
        Self::FifteenMinutes,
        Self::OneHour,
        Self::FourHours,
        Self::OneDay,
    ];

    pub fn parse(raw: &str) -> Result<Self, MarketDataError> {
        Self::ALL
            .into_iter()
            .find(|interval| interval.as_str() == raw)
            .ok_or(MarketDataError::InvalidInterval)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::FourHours => "4h",
            Self::OneDay => "1d",
        }
    }

    pub const fn seconds(self) -> u64 {
        match self {
            Self::OneMinute => 60,
            Self::FiveMinutes => 300,
            Self::FifteenMinutes => 900,
            Self::OneHour => 3_600,
            Self::FourHours => 14_400,
            Self::OneDay => 86_400,
        }
    }

    const fn nanos(self) -> u64 {
        self.seconds() * NANOS_PER_SECOND
    }

    /// Start of the bucket containing `at`. Buckets are aligned to the Unix
    /// epoch, which for these intervals is the same as Postgres `date_bin`
    /// with its usual `2000-01-01` origin.
    pub const fn start_of(self, at: Timestamp) -> Timestamp {
        let step = self.nanos();
        Timestamp::from_unix_nanos(at.unix_nanos() / step * step)
    }
}

/// One OHLC bar of a market's index price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candle {
    pub market: MarketId,
    pub start: Timestamp,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    /// Revision of the update that set `close`.
    pub revision: Revision,
}

impl Candle {
    pub fn opening(market: MarketId, start: Timestamp, price: Decimal, revision: Revision) -> Self {
        Self {
            market,
            start,
            open: price,
            high: price,
            low: price,
            close: price,
            revision,
        }
    }

    /// Folds another candle of the same bucket into this one. The open is
    /// kept, so the first write of a bucket decides it; the close follows
    /// the newest revision whatever the arrival order.
    pub fn merge(&mut self, other: &Self) {
        self.high = self.high.max(other.high);
        self.low = self.low.min(other.low);
        if other.revision > self.revision {
            self.close = other.close;
            self.revision = other.revision;
        }
    }
}

/// Builds the 1 minute candles touched by `updates`, given in publication
/// order. `Stale` updates only repeat an old price and are skipped.
pub fn fold_minute_candles(updates: &[Arc<PriceUpdate>]) -> Vec<Candle> {
    let mut candles: BTreeMap<(MarketId, Timestamp), Candle> = BTreeMap::new();
    for update in updates {
        if update.status == PriceStatus::Stale {
            continue;
        }
        let start = CandleInterval::OneMinute.start_of(update.published_at);
        let next = Candle::opening(
            update.market.clone(),
            start,
            update.price,
            update.revision(),
        );
        candles
            .entry((update.market.clone(), start))
            .and_modify(|candle| candle.merge(&next))
            .or_insert(next);
    }
    candles.into_values().collect()
}

/// Rolls 1 minute candles of one market, sorted by start, up into
/// `interval` buckets.
pub fn rollup(minutes: &[Candle], interval: CandleInterval) -> Vec<Candle> {
    let mut rolled: Vec<Candle> = Vec::new();
    for minute in minutes {
        let start = interval.start_of(minute.start);
        match rolled.last_mut() {
            Some(last) if last.start == start => {
                last.high = last.high.max(minute.high);
                last.low = last.low.min(minute.low);
                last.close = minute.close;
                last.revision = minute.revision;
            }
            _ => rolled.push(Candle {
                start,
                ..minute.clone()
            }),
        }
    }
    rolled
}

/// A request for the most recent candles of a market.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CandleQuery {
    pub market: MarketId,
    pub interval: CandleInterval,
    /// Number of buckets, between 1 and `MAX_LIMIT`.
    pub limit: u32,
    /// Exclusive upper bound for candle starts. `None` means "up to now",
    /// including the bucket still in progress.
    pub end: Option<Timestamp>,
}

impl CandleQuery {
    pub const MAX_LIMIT: u32 = 1_000;

    /// Range `[from, to)` of bucket starts this query covers at `now`.
    pub fn window(&self, now: Timestamp) -> (Timestamp, Timestamp) {
        let step = self.interval.nanos();
        let to = self.end.map_or_else(
            || {
                self.interval
                    .start_of(now)
                    .unix_nanos()
                    .saturating_add(step)
            },
            Timestamp::unix_nanos,
        );
        let last_bucket = self
            .interval
            .start_of(Timestamp::from_unix_nanos(to.saturating_sub(1)))
            .unix_nanos();
        let span = step.saturating_mul(u64::from(self.limit.max(1)) - 1);
        (
            Timestamp::from_unix_nanos(last_bucket.saturating_sub(span)),
            Timestamp::from_unix_nanos(to),
        )
    }

    /// True when every bucket in the window has closed at `now`, so the
    /// result can no longer change and may be cached for longer.
    pub fn is_settled(&self, now: Timestamp) -> bool {
        self.end
            .is_some_and(|end| end <= CandleInterval::OneMinute.start_of(now))
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::aggregate::PriceStatus;

    fn minutes(n: u64) -> Timestamp {
        Timestamp::from_unix_nanos(n * 60 * NANOS_PER_SECOND)
    }

    fn market() -> MarketId {
        MarketId::parse("ETH-USD").unwrap()
    }

    fn update(sequence: u64, price: Decimal, published_at: Timestamp) -> Arc<PriceUpdate> {
        Arc::new(PriceUpdate {
            market: market(),
            epoch: 1,
            sequence,
            price,
            status: PriceStatus::Live,
            as_of: published_at,
            published_at,
            spread_bps: Decimal::ZERO,
            components: Vec::new(),
        })
    }

    #[test]
    fn intervals_parse_and_align() {
        assert_eq!(
            CandleInterval::parse("15m"),
            Ok(CandleInterval::FifteenMinutes)
        );
        assert_eq!(
            CandleInterval::parse("2m"),
            Err(MarketDataError::InvalidInterval)
        );
        let at = Timestamp::from_unix_nanos(minutes(61).unix_nanos() + 5);
        assert_eq!(CandleInterval::OneHour.start_of(at), minutes(60));
        assert_eq!(CandleInterval::FiveMinutes.start_of(at), minutes(60));
    }

    #[test]
    fn updates_fold_into_minute_candles() {
        let second = |s: u64| Timestamp::from_unix_nanos(s * NANOS_PER_SECOND);
        let candles = fold_minute_candles(&[
            update(1, dec!(10), second(60)),
            update(2, dec!(12), second(70)),
            update(3, dec!(9), second(80)),
            update(4, dec!(11), second(119)),
            update(5, dec!(20), second(120)),
        ]);
        assert_eq!(candles.len(), 2);
        let first = &candles[0];
        assert_eq!(
            (first.open, first.high, first.low, first.close),
            (dec!(10), dec!(12), dec!(9), dec!(11))
        );
        assert_eq!(candles[1].start, minutes(2));
    }

    #[test]
    fn merge_keeps_the_open_and_the_newest_close() {
        let revision = |sequence| Revision { epoch: 1, sequence };
        let mut stored = Candle::opening(market(), minutes(1), dec!(10), revision(5));
        let older = Candle::opening(market(), minutes(1), dec!(7), revision(3));
        stored.merge(&older);
        assert_eq!(
            (stored.open, stored.low, stored.close),
            (dec!(10), dec!(7), dec!(10))
        );
        let newer = Candle::opening(market(), minutes(1), dec!(13), revision(9));
        stored.merge(&newer);
        assert_eq!((stored.high, stored.close), (dec!(13), dec!(13)));
    }

    #[test]
    fn minutes_roll_up_into_longer_buckets() {
        let candle = |minute, open, high, low, close| Candle {
            market: market(),
            start: minutes(minute),
            open,
            high,
            low,
            close,
            revision: Revision::default(),
        };
        let rolled = rollup(
            &[
                candle(0, dec!(1), dec!(5), dec!(1), dec!(4)),
                candle(3, dec!(4), dec!(6), dec!(2), dec!(3)),
                candle(5, dec!(3), dec!(3), dec!(1), dec!(2)),
            ],
            CandleInterval::FiveMinutes,
        );
        assert_eq!(rolled.len(), 2);
        assert_eq!(
            (
                rolled[0].open,
                rolled[0].high,
                rolled[0].low,
                rolled[0].close
            ),
            (dec!(1), dec!(6), dec!(1), dec!(3))
        );
        assert_eq!(rolled[1].start, minutes(5));
    }

    #[test]
    fn window_covers_limit_buckets_up_to_the_current_one() {
        let query = CandleQuery {
            market: market(),
            interval: CandleInterval::OneMinute,
            limit: 3,
            end: None,
        };
        let now = Timestamp::from_unix_nanos(minutes(10).unix_nanos() + 1);
        assert_eq!(query.window(now), (minutes(8), minutes(11)));
        assert!(!query.is_settled(now));

        let closed = CandleQuery {
            end: Some(minutes(9)),
            ..query
        };
        assert_eq!(closed.window(now), (minutes(6), minutes(9)));
        assert!(closed.is_settled(now));
    }
}
