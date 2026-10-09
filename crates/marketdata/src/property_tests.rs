//! Properties of the index rule that must hold for any input.

use std::time::Duration;

use orderflow_domain::Timestamp;
use proptest::prelude::*;
use rust_decimal::Decimal;

use crate::{
    aggregate::{AggregationPolicy, PriceStatus, Quote, QuoteState, aggregate},
    source::{SourceId, SourceKind, SourceRole, SourceSpec},
};

const NOW: Timestamp = Timestamp::from_unix_nanos(1_000_000_000_000);

fn specs(count: usize) -> Vec<SourceSpec> {
    (0..count)
        .map(|i| SourceSpec {
            id: SourceId::parse(&format!("source-{i}")).unwrap_or_else(|_| unreachable!()),
            kind: SourceKind::Cex,
            role: SourceRole::Index,
            max_age: Duration::from_secs(60),
        })
        .collect()
}

fn quotes<'a>(specs: &'a [SourceSpec], prices: &[Decimal]) -> Vec<Quote<'a>> {
    specs
        .iter()
        .zip(prices)
        .map(|(spec, price)| Quote {
            spec,
            price: *price,
            observed_at: NOW,
        })
        .collect()
}

/// Prices in cents between 0.01 and 1,000,000.00.
fn price() -> impl Strategy<Value = Decimal> {
    (1_i64..=100_000_000).prop_map(|cents| Decimal::new(cents, 2))
}

proptest! {
    #[test]
    fn index_stays_within_the_quotes_it_used(prices in prop::collection::vec(price(), 1..12)) {
        let specs = specs(prices.len());
        let policy = AggregationPolicy::new(1, 10_000).unwrap_or_else(|_| unreachable!());
        let result = aggregate(&policy, NOW, &quotes(&specs, &prices));
        let used: Vec<Decimal> = result
            .components
            .iter()
            .filter(|c| c.state == QuoteState::Used)
            .map(|c| c.price)
            .collect();
        let price = result.price.unwrap_or_default();
        prop_assert!(!used.is_empty());
        prop_assert!(used.iter().min().is_some_and(|low| *low <= price));
        prop_assert!(used.iter().max().is_some_and(|high| price <= *high));
    }

    #[test]
    fn index_ignores_quote_order(mut prices in prop::collection::vec(price(), 1..12), seed in any::<u64>()) {
        let specs = specs(prices.len());
        let policy = AggregationPolicy::new(2, 150).unwrap_or_else(|_| unreachable!());
        let before = aggregate(&policy, NOW, &quotes(&specs, &prices));
        let len = prices.len();
        prices.rotate_left(usize::try_from(seed % len as u64).unwrap_or(0));
        prices.reverse();
        let after = aggregate(&policy, NOW, &quotes(&specs, &prices));
        prop_assert_eq!(before.price, after.price);
        prop_assert_eq!(before.status, after.status);
    }

    #[test]
    fn one_manipulated_source_cannot_move_a_live_index(
        honest in prop::collection::vec(9_950_i64..=10_050, 3..8),
        factor in prop_oneof![Just(2_i64), Just(10), Just(1000)],
        below in any::<bool>(),
    ) {
        // Agreeing quotes within 0.5% of 100.00; one source quotes far away.
        let mut prices: Vec<Decimal> = honest.iter().map(|cents| Decimal::new(*cents, 2)).collect();
        let wild = if below {
            Decimal::new(10_000, 2) / Decimal::from(factor)
        } else {
            Decimal::new(10_000, 2) * Decimal::from(factor)
        };
        prices.push(wild);
        let specs = specs(prices.len());
        let policy = AggregationPolicy::new(3, 200).unwrap_or_else(|_| unreachable!());
        let result = aggregate(&policy, NOW, &quotes(&specs, &prices));

        prop_assert_eq!(result.status, PriceStatus::Live);
        let price = result.price.unwrap_or_default();
        prop_assert!(price >= Decimal::new(9_950, 2) && price <= Decimal::new(10_050, 2));
        prop_assert_eq!(result.components.last().map(|c| c.state), Some(QuoteState::Outlier));
    }
}
