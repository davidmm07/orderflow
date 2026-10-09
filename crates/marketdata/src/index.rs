//! The latest quote of every source, per market, and when a new index price
//! is worth publishing.

use std::{collections::BTreeMap, time::Duration};

use orderflow_domain::{MarketId, Timestamp};
use rust_decimal::Decimal;

use crate::{
    aggregate::{AggregationPolicy, PriceStatus, Quote, QuoteState, aggregate},
    error::MarketDataError,
    observation::Observation,
    source::{SourceId, SourceRole, SourceSpec},
    update::PriceUpdate,
};

/// Largest accepted difference between a source's clock and ours. Quotes
/// stamped further in the future are rejected, otherwise one source with a
/// broken clock would stay "fresh" forever.
const MAX_FUTURE_SKEW: Duration = Duration::from_secs(5);

/// A price that does not move is republished at most this often while
/// fresher quotes keep confirming it. Without it `as_of` would freeze on a
/// quiet market, and clients could not tell a steady price from a dead one.
const REFRESH_AFTER: Duration = Duration::from_secs(5);

/// Configuration of one market: its aggregation rule and its sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketFeed {
    pub market: MarketId,
    pub policy: AggregationPolicy,
    pub sources: Vec<SourceSpec>,
}

/// Why an observation was not taken into account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rejection {
    UnknownMarket,
    /// The source is not configured for this market.
    UnknownSource,
    FromTheFuture,
    /// Older than the quote already held for the same source.
    OutOfOrder,
    /// Already older than the source's `max_age` on arrival.
    TooOld,
}

impl Rejection {
    pub const ALL: [Self; 5] = [
        Self::UnknownMarket,
        Self::UnknownSource,
        Self::FromTheFuture,
        Self::OutOfOrder,
        Self::TooOld,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownMarket => "unknown_market",
            Self::UnknownSource => "unknown_source",
            Self::FromTheFuture => "from_the_future",
            Self::OutOfOrder => "out_of_order",
            Self::TooOld => "too_old",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LatestQuote {
    price: Decimal,
    observed_at: Timestamp,
}

/// What subscribers last saw. A new update is published only when one of
/// these fields changes; a quote that repeats the same price publishes
/// nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Published {
    price: Decimal,
    status: PriceStatus,
    used: Vec<SourceId>,
}

#[derive(Debug)]
struct MarketState {
    policy: AggregationPolicy,
    /// Sorted by id, so components always come out in the same order.
    sources: Vec<SourceSpec>,
    /// Parallel to `sources`.
    quotes: Vec<Option<LatestQuote>>,
    sequence: u64,
    last: Option<(Published, Timestamp)>,
    /// When the last update went out.
    published_at: Timestamp,
}

/// Index prices of every configured market.
///
/// Pattern: State machine per market, driven by two inputs. `observe`
/// records a quote and never publishes; `evaluate` turns the current quotes
/// into updates. Splitting them lets the caller decide the publishing
/// cadence, and keeps both functions free of I/O and clocks: time is passed
/// in, so tests replay any scenario exactly.
#[derive(Debug)]
pub struct PriceIndex {
    epoch: u64,
    markets: BTreeMap<MarketId, MarketState>,
}

impl PriceIndex {
    pub fn new(epoch: u64, feeds: Vec<MarketFeed>) -> Result<Self, MarketDataError> {
        let mut markets = BTreeMap::new();
        for feed in feeds {
            let mut sources = feed.sources;
            sources.sort_by(|a, b| a.id.cmp(&b.id));
            if let Some(pair) = sources.windows(2).find(|pair| pair[0].id == pair[1].id) {
                return Err(MarketDataError::DuplicateSource {
                    market: feed.market.to_string(),
                    source_id: pair[0].id.to_string(),
                });
            }
            if !sources.iter().any(|spec| spec.role == SourceRole::Index) {
                return Err(MarketDataError::NoIndexSources(feed.market.to_string()));
            }
            let state = MarketState {
                policy: feed.policy,
                quotes: vec![None; sources.len()],
                sources,
                sequence: 0,
                last: None,
                published_at: Timestamp::default(),
            };
            if markets.insert(feed.market.clone(), state).is_some() {
                return Err(MarketDataError::DuplicateMarket(feed.market.to_string()));
            }
        }
        Ok(Self { epoch, markets })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn markets(&self) -> impl Iterator<Item = &MarketId> {
        self.markets.keys()
    }

    /// Records `observation` as its source's latest quote.
    pub fn observe(&mut self, observation: &Observation, now: Timestamp) -> Result<(), Rejection> {
        let state = self
            .markets
            .get_mut(observation.market())
            .ok_or(Rejection::UnknownMarket)?;
        let slot = state
            .sources
            .binary_search_by(|spec| spec.id.cmp(observation.source()))
            .map_err(|_| Rejection::UnknownSource)?;
        let (Some(spec), Some(latest)) = (state.sources.get(slot), state.quotes.get_mut(slot))
        else {
            return Err(Rejection::UnknownSource);
        };

        let observed = observation.observed_at().unix_nanos();
        let skew = u64::try_from(MAX_FUTURE_SKEW.as_nanos()).unwrap_or(u64::MAX);
        if observed > now.unix_nanos().saturating_add(skew) {
            return Err(Rejection::FromTheFuture);
        }
        if u128::from(now.unix_nanos().saturating_sub(observed)) > spec.max_age.as_nanos() {
            return Err(Rejection::TooOld);
        }
        if latest.is_some_and(|quote| quote.observed_at > observation.observed_at()) {
            return Err(Rejection::OutOfOrder);
        }
        *latest = Some(LatestQuote {
            price: observation.price(),
            observed_at: observation.observed_at(),
        });
        Ok(())
    }

    /// Computes every market's index and returns the ones that changed
    /// since they were last published, in market order.
    ///
    /// A market whose sources all went quiet is republished once as
    /// `Stale` with its last price, so subscribers learn that the number on
    /// their screen is no longer backed by fresh data.
    pub fn evaluate(&mut self, now: Timestamp) -> Vec<PriceUpdate> {
        let mut updates = Vec::new();
        for (market, state) in &mut self.markets {
            let quotes: Vec<Quote<'_>> = state
                .sources
                .iter()
                .zip(&state.quotes)
                .filter_map(|(spec, quote)| {
                    quote.map(|quote| Quote {
                        spec,
                        price: quote.price,
                        observed_at: quote.observed_at,
                    })
                })
                .collect();
            let result = aggregate(&state.policy, now, &quotes);

            let (next, as_of) = match (result.price, &state.last) {
                (Some(price), _) => {
                    let used = result
                        .components
                        .iter()
                        .filter(|component| component.state == QuoteState::Used)
                        .map(|component| component.source.clone())
                        .collect();
                    let published = Published {
                        price,
                        status: result.status,
                        used,
                    };
                    (published, result.as_of.unwrap_or(now))
                }
                (None, Some((last, as_of))) => {
                    let published = Published {
                        price: last.price,
                        status: PriceStatus::Stale,
                        used: Vec::new(),
                    };
                    (published, *as_of)
                }
                (None, None) => continue,
            };
            let unchanged = state.last.as_ref().is_some_and(|(last, _)| *last == next);
            let refresh_due = state
                .last
                .as_ref()
                .is_some_and(|(_, last_as_of)| as_of > *last_as_of)
                && u128::from(
                    now.unix_nanos()
                        .saturating_sub(state.published_at.unix_nanos()),
                ) >= REFRESH_AFTER.as_nanos();
            if unchanged && !refresh_due {
                continue;
            }

            state.sequence += 1;
            updates.push(PriceUpdate {
                market: market.clone(),
                epoch: self.epoch,
                sequence: state.sequence,
                price: next.price,
                status: next.status,
                as_of,
                published_at: now,
                spread_bps: result.spread_bps,
                components: result.components,
            });
            state.last = Some((next, as_of));
            state.published_at = now;
        }
        updates
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::source::SourceKind;

    const SECOND: u64 = 1_000_000_000;

    fn at(seconds: u64) -> Timestamp {
        Timestamp::from_unix_nanos(seconds * SECOND)
    }

    fn market() -> MarketId {
        MarketId::parse("BTC-USD").unwrap()
    }

    fn source(id: &str) -> SourceId {
        SourceId::parse(id).unwrap()
    }

    fn spec(id: &str, role: SourceRole) -> SourceSpec {
        SourceSpec {
            id: source(id),
            kind: SourceKind::Cex,
            role,
            max_age: Duration::from_secs(30),
        }
    }

    fn index(quorum: usize) -> PriceIndex {
        let feed = MarketFeed {
            market: market(),
            policy: AggregationPolicy::new(quorum, 100).unwrap(),
            sources: vec![
                spec("kraken", SourceRole::Index),
                spec("coinbase", SourceRole::Index),
                spec("orderflow", SourceRole::Monitor),
            ],
        };
        PriceIndex::new(7, vec![feed]).unwrap()
    }

    fn quote(id: &str, price: Decimal, seconds: u64) -> Observation {
        Observation::new(market(), source(id), price, at(seconds)).unwrap()
    }

    #[test]
    fn observations_are_validated_before_they_count() {
        let mut index = index(1);
        let now = at(100);
        let other = Observation::new(
            MarketId::parse("ETH-USD").unwrap(),
            source("kraken"),
            dec!(1),
            now,
        )
        .unwrap();
        assert_eq!(index.observe(&other, now), Err(Rejection::UnknownMarket));
        assert_eq!(
            index.observe(&quote("binance", dec!(1), 100), now),
            Err(Rejection::UnknownSource)
        );
        assert_eq!(
            index.observe(&quote("kraken", dec!(1), 106), now),
            Err(Rejection::FromTheFuture)
        );
        assert_eq!(
            index.observe(&quote("kraken", dec!(1), 69), now),
            Err(Rejection::TooOld)
        );
        assert_eq!(index.observe(&quote("kraken", dec!(1), 99), now), Ok(()));
        assert_eq!(
            index.observe(&quote("kraken", dec!(1), 98), now),
            Err(Rejection::OutOfOrder)
        );
    }

    #[test]
    fn updates_are_published_only_when_something_changes() {
        let mut index = index(2);
        assert!(index.evaluate(at(100)).is_empty(), "no quotes, no price");

        index
            .observe(&quote("kraken", dec!(100), 100), at(100))
            .unwrap();
        let first = index.evaluate(at(100));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].status, PriceStatus::Degraded);
        assert_eq!((first[0].epoch, first[0].sequence), (7, 1));

        index
            .observe(&quote("coinbase", dec!(100), 101), at(101))
            .unwrap();
        let second = index.evaluate(at(101));
        assert_eq!(second[0].status, PriceStatus::Live);
        assert_eq!(second[0].sequence, 2);

        // Same prices again: newer quotes, nothing new to tell subscribers.
        index
            .observe(&quote("kraken", dec!(100), 102), at(102))
            .unwrap();
        assert!(index.evaluate(at(102)).is_empty());

        // The venue's own price never moves the index.
        index
            .observe(&quote("orderflow", dec!(90), 102), at(102))
            .unwrap();
        assert!(index.evaluate(at(102)).is_empty());

        // A steady price is still refreshed now and then, so `as_of` keeps
        // telling how fresh the backing quotes are.
        index
            .observe(&quote("kraken", dec!(100), 106), at(106))
            .unwrap();
        let refreshed = index.evaluate(at(106));
        assert_eq!(refreshed.len(), 1);
        assert_eq!((refreshed[0].sequence, refreshed[0].as_of), (3, at(106)));
        assert!(index.evaluate(at(107)).is_empty());
    }

    #[test]
    fn a_market_whose_sources_go_quiet_turns_stale_once() {
        let mut index = index(1);
        index
            .observe(&quote("kraken", dec!(100), 100), at(100))
            .unwrap();
        assert_eq!(index.evaluate(at(100)).len(), 1);

        let stale = index.evaluate(at(200));
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].status, PriceStatus::Stale);
        assert_eq!(stale[0].price, dec!(100));
        assert_eq!(stale[0].as_of, at(100));
        assert!(index.evaluate(at(300)).is_empty());

        index
            .observe(&quote("kraken", dec!(101), 300), at(300))
            .unwrap();
        let live = index.evaluate(at(300));
        assert_eq!(live[0].status, PriceStatus::Live);
        assert_eq!(live[0].sequence, 3);
    }

    #[test]
    fn configuration_mistakes_are_rejected() {
        let feed = |sources| MarketFeed {
            market: market(),
            policy: AggregationPolicy::new(1, 100).unwrap(),
            sources,
        };
        let twice = vec![
            spec("kraken", SourceRole::Index),
            spec("kraken", SourceRole::Index),
        ];
        assert!(matches!(
            PriceIndex::new(1, vec![feed(twice)]),
            Err(MarketDataError::DuplicateSource { .. })
        ));
        let monitors_only = vec![spec("orderflow", SourceRole::Monitor)];
        assert!(matches!(
            PriceIndex::new(1, vec![feed(monitors_only)]),
            Err(MarketDataError::NoIndexSources(_))
        ));
        let ok = vec![spec("kraken", SourceRole::Index)];
        assert!(matches!(
            PriceIndex::new(1, vec![feed(ok.clone()), feed(ok)]),
            Err(MarketDataError::DuplicateMarket(_))
        ));
    }
}
