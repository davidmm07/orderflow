//! The index price rule: the median of fresh quotes, with outliers removed.
//!
//! The median is used rather than a mean because one broken or manipulated
//! source can drag a mean anywhere, while it moves a median by at most one
//! position. Outlier rejection then drops quotes too far from that first
//! median, so a source quoting a stale or wrong price is visible in the
//! breakdown but has no effect on the result.

use orderflow_domain::Timestamp;
use rust_decimal::Decimal;

use crate::{
    error::MarketDataError,
    source::{SourceId, SourceKind, SourceRole, SourceSpec},
};

const BPS_PER_UNIT: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);

/// Rules that turn fresh quotes into one index price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregationPolicy {
    quorum: usize,
    max_deviation_bps: u32,
}

impl AggregationPolicy {
    /// `quorum` is how many fresh, agreeing index sources a `Live` price
    /// needs. Quotes further than `max_deviation_bps` from the median are
    /// outliers.
    pub fn new(quorum: usize, max_deviation_bps: u32) -> Result<Self, MarketDataError> {
        if quorum == 0 {
            return Err(MarketDataError::InvalidQuorum);
        }
        if !(1..=10_000).contains(&max_deviation_bps) {
            return Err(MarketDataError::InvalidDeviation);
        }
        Ok(Self {
            quorum,
            max_deviation_bps,
        })
    }

    pub const fn quorum(&self) -> usize {
        self.quorum
    }

    pub const fn max_deviation_bps(&self) -> u32 {
        self.max_deviation_bps
    }
}

/// How much a published price can be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PriceStatus {
    /// At least `quorum` fresh index sources agree.
    Live,
    /// A price exists, but fewer than `quorum` sources back it or the
    /// sources disagree. Good enough to display; automated consumers should
    /// wait for `Live`.
    Degraded,
    /// No fresh index source is left. The price is the last one published.
    Stale,
}

impl PriceStatus {
    pub const ALL: [Self; 3] = [Self::Live, Self::Degraded, Self::Stale];

    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|status| status.as_str() == raw)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Degraded => "degraded",
            Self::Stale => "stale",
        }
    }
}

/// What the aggregation did with one source's quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuoteState {
    Used,
    /// Fresh, but too far from the median.
    Outlier,
    /// Older than the source's `max_age`.
    Stale,
    /// From a `Monitor` source, shown for comparison only.
    Monitor,
}

impl QuoteState {
    pub const ALL: [Self; 4] = [Self::Used, Self::Outlier, Self::Stale, Self::Monitor];

    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == raw)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Used => "used",
            Self::Outlier => "outlier",
            Self::Stale => "stale",
            Self::Monitor => "monitor",
        }
    }
}

/// One source's quote and how it was treated, kept with every published
/// price so any value can be explained after the fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Component {
    pub source: SourceId,
    pub kind: SourceKind,
    pub price: Decimal,
    pub observed_at: Timestamp,
    pub state: QuoteState,
    /// Distance from the index price in basis points, negative below it.
    pub deviation_bps: Option<Decimal>,
}

/// The latest quote of one source, as input to `aggregate`.
pub(crate) struct Quote<'a> {
    pub spec: &'a SourceSpec,
    pub price: Decimal,
    pub observed_at: Timestamp,
}

pub(crate) struct Aggregate {
    /// `None` when no fresh index source is left.
    pub price: Option<Decimal>,
    pub status: PriceStatus,
    pub as_of: Option<Timestamp>,
    pub spread_bps: Decimal,
    pub components: Vec<Component>,
}

pub(crate) fn aggregate(
    policy: &AggregationPolicy,
    now: Timestamp,
    quotes: &[Quote<'_>],
) -> Aggregate {
    let mut states: Vec<QuoteState> = quotes
        .iter()
        // Freshness comes first for every role: a venue whose last trade is
        // old is as stale as an exchange that stopped quoting, and must not
        // keep reporting a deviation from that old price.
        .map(|quote| match quote.spec.role {
            _ if !is_fresh(quote, now) => QuoteState::Stale,
            SourceRole::Monitor => QuoteState::Monitor,
            SourceRole::Index => QuoteState::Used,
        })
        .collect();

    let Some(first_median) = median(&prices_in(quotes, &states, QuoteState::Used)) else {
        return Aggregate {
            price: None,
            status: PriceStatus::Stale,
            as_of: None,
            spread_bps: Decimal::ZERO,
            components: components(quotes, &states, None),
        };
    };

    let max_deviation = Decimal::from(policy.max_deviation_bps);
    let mut agreeing = 0;
    for (quote, state) in quotes.iter().zip(states.iter_mut()) {
        if *state != QuoteState::Used {
            continue;
        }
        let close = deviation_bps(quote.price, first_median)
            .is_some_and(|deviation| deviation.abs() <= max_deviation);
        if close {
            agreeing += 1;
        } else {
            *state = QuoteState::Outlier;
        }
    }
    // Every quote is far from the median, as with two sources quoting very
    // different prices. There is no way to tell which one is right, so all
    // of them count and the result is flagged instead of guessed.
    let disagreement = agreeing == 0;
    if disagreement {
        for state in &mut states {
            if *state == QuoteState::Outlier {
                *state = QuoteState::Used;
            }
        }
    }

    let used = prices_in(quotes, &states, QuoteState::Used);
    let price = median(&used).unwrap_or(first_median).normalize();
    let status = if !disagreement && used.len() >= policy.quorum {
        PriceStatus::Live
    } else {
        PriceStatus::Degraded
    };
    let as_of = quotes
        .iter()
        .zip(&states)
        .filter(|(_, state)| **state == QuoteState::Used)
        .map(|(quote, _)| quote.observed_at)
        .max();

    Aggregate {
        price: Some(price),
        status,
        as_of,
        spread_bps: spread_bps(&used, price),
        components: components(quotes, &states, Some(price)),
    }
}

fn is_fresh(quote: &Quote<'_>, now: Timestamp) -> bool {
    let age = now
        .unix_nanos()
        .saturating_sub(quote.observed_at.unix_nanos());
    u128::from(age) <= quote.spec.max_age.as_nanos()
}

fn prices_in(quotes: &[Quote<'_>], states: &[QuoteState], wanted: QuoteState) -> Vec<Decimal> {
    quotes
        .iter()
        .zip(states)
        .filter(|(_, state)| **state == wanted)
        .map(|(quote, _)| quote.price)
        .collect()
}

/// Median of `values`; the mean of the two middle values for an even count.
pub(crate) fn median(values: &[Decimal]) -> Option<Decimal> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => None,
        len if len % 2 == 1 => sorted.get(middle).copied(),
        _ => {
            let (low, high) = (*sorted.get(middle - 1)?, *sorted.get(middle)?);
            // `low + (high - low) / 2` cannot overflow, unlike `(low + high) / 2`.
            Some(low + (high - low) / Decimal::TWO)
        }
    }
}

/// Signed distance of `price` from `reference` in basis points, or `None`
/// if the ratio does not fit in a decimal.
pub(crate) fn deviation_bps(price: Decimal, reference: Decimal) -> Option<Decimal> {
    (price - reference)
        .checked_div(reference)?
        .checked_mul(BPS_PER_UNIT)
}

fn spread_bps(used: &[Decimal], price: Decimal) -> Decimal {
    let (Some(low), Some(high)) = (used.iter().min(), used.iter().max()) else {
        return Decimal::ZERO;
    };
    (*high - *low)
        .checked_div(price)
        .and_then(|ratio| ratio.checked_mul(BPS_PER_UNIT))
        .map_or(Decimal::MAX, |bps| bps.round_dp(2))
}

fn components(
    quotes: &[Quote<'_>],
    states: &[QuoteState],
    price: Option<Decimal>,
) -> Vec<Component> {
    quotes
        .iter()
        .zip(states)
        .map(|(quote, state)| Component {
            source: quote.spec.id.clone(),
            kind: quote.spec.kind,
            price: quote.price,
            observed_at: quote.observed_at,
            state: *state,
            deviation_bps: price
                .and_then(|price| deviation_bps(quote.price, price))
                .map(|bps| bps.round_dp(2)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rust_decimal_macros::dec;

    use super::*;

    const SECOND: u64 = 1_000_000_000;

    fn spec(id: &str, role: SourceRole) -> SourceSpec {
        SourceSpec {
            id: SourceId::parse(id).unwrap(),
            kind: SourceKind::Cex,
            role,
            max_age: Duration::from_secs(60),
        }
    }

    fn at(seconds: u64) -> Timestamp {
        Timestamp::from_unix_nanos(seconds * SECOND)
    }

    fn run(
        policy: AggregationPolicy,
        now: u64,
        input: &[(&SourceSpec, Decimal, u64)],
    ) -> Aggregate {
        let quotes: Vec<Quote<'_>> = input
            .iter()
            .map(|(spec, price, seen)| Quote {
                spec,
                price: *price,
                observed_at: at(*seen),
            })
            .collect();
        aggregate(&policy, at(now), &quotes)
    }

    fn states(result: &Aggregate) -> Vec<QuoteState> {
        result.components.iter().map(|c| c.state).collect()
    }

    #[test]
    fn median_handles_odd_and_even_counts() {
        assert_eq!(median(&[]), None);
        assert_eq!(median(&[dec!(3), dec!(1), dec!(2)]), Some(dec!(2)));
        assert_eq!(
            median(&[dec!(4), dec!(1), dec!(3), dec!(2)]),
            Some(dec!(2.5))
        );
    }

    #[test]
    fn agreeing_sources_produce_a_live_median() {
        let (a, b, c) = (
            spec("a", SourceRole::Index),
            spec("b", SourceRole::Index),
            spec("c", SourceRole::Index),
        );
        let policy = AggregationPolicy::new(2, 100).unwrap();
        let result = run(
            policy,
            100,
            &[
                (&a, dec!(100.0), 99),
                (&b, dec!(100.4), 98),
                (&c, dec!(99.9), 97),
            ],
        );
        assert_eq!(result.price, Some(dec!(100)));
        assert_eq!(result.status, PriceStatus::Live);
        assert_eq!(result.as_of, Some(at(99)));
        assert_eq!(result.spread_bps, dec!(50));
        assert_eq!(result.components[1].deviation_bps, Some(dec!(40)));
    }

    #[test]
    fn a_wild_quote_is_an_outlier_and_does_not_move_the_price() {
        let (a, b, c) = (
            spec("a", SourceRole::Index),
            spec("b", SourceRole::Index),
            spec("c", SourceRole::Index),
        );
        let policy = AggregationPolicy::new(2, 100).unwrap();
        let result = run(
            policy,
            100,
            &[
                (&a, dec!(100), 100),
                (&b, dec!(101), 100),
                (&c, dec!(150), 100),
            ],
        );
        assert_eq!(result.price, Some(dec!(100.5)));
        assert_eq!(result.status, PriceStatus::Live);
        assert_eq!(
            states(&result),
            [QuoteState::Used, QuoteState::Used, QuoteState::Outlier]
        );
    }

    #[test]
    fn missing_quorum_degrades_the_price() {
        let (a, b) = (spec("a", SourceRole::Index), spec("b", SourceRole::Index));
        let policy = AggregationPolicy::new(2, 100).unwrap();
        let result = run(policy, 100, &[(&a, dec!(100), 99), (&b, dec!(100), 10)]);
        assert_eq!(result.price, Some(dec!(100)));
        assert_eq!(result.status, PriceStatus::Degraded);
        assert_eq!(states(&result), [QuoteState::Used, QuoteState::Stale]);
    }

    #[test]
    fn two_disagreeing_sources_are_both_kept_and_flagged() {
        let (a, b) = (spec("a", SourceRole::Index), spec("b", SourceRole::Index));
        let policy = AggregationPolicy::new(2, 100).unwrap();
        let result = run(policy, 100, &[(&a, dec!(100), 100), (&b, dec!(110), 100)]);
        assert_eq!(result.price, Some(dec!(105)));
        assert_eq!(result.status, PriceStatus::Degraded);
        assert_eq!(states(&result), [QuoteState::Used, QuoteState::Used]);
    }

    #[test]
    fn monitor_sources_are_reported_but_never_used() {
        let (a, venue) = (
            spec("a", SourceRole::Index),
            spec("venue", SourceRole::Monitor),
        );
        let policy = AggregationPolicy::new(1, 100).unwrap();
        let result = run(
            policy,
            100,
            &[(&a, dec!(100), 100), (&venue, dec!(90), 100)],
        );
        assert_eq!(result.price, Some(dec!(100)));
        assert_eq!(result.status, PriceStatus::Live);
        assert_eq!(result.components[1].state, QuoteState::Monitor);
        assert_eq!(result.components[1].deviation_bps, Some(dec!(-1000)));

        // Once the venue's last trade is older than its max age, it is
        // stale like any other source.
        let later = run(
            policy,
            200,
            &[(&a, dec!(100), 200), (&venue, dec!(90), 100)],
        );
        assert_eq!(later.components[1].state, QuoteState::Stale);
    }

    #[test]
    fn no_fresh_index_quote_means_no_price() {
        let a = spec("a", SourceRole::Index);
        let policy = AggregationPolicy::new(1, 100).unwrap();
        let result = run(policy, 1_000, &[(&a, dec!(100), 1)]);
        assert_eq!(result.price, None);
        assert_eq!(result.status, PriceStatus::Stale);
    }

    #[test]
    fn policy_rejects_nonsense() {
        assert_eq!(
            AggregationPolicy::new(0, 100),
            Err(MarketDataError::InvalidQuorum)
        );
        assert_eq!(
            AggregationPolicy::new(1, 0),
            Err(MarketDataError::InvalidDeviation)
        );
    }
}
