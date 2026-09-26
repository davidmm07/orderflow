//! Property-based tests: random order flow must never break the invariants.

use proptest::prelude::*;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::{
    AccountId, DomainError, MarketId, MarketSpec, MatchOutcome, MatchingEngine, NewOrder, OrderId,
    OrderKind, OrderStatus, Price, Quantity, SelfTradePrevention, Side, TimeInForce, Timestamp,
};

#[derive(Debug, Clone)]
enum Op {
    Submit {
        account: u8,
        buy: bool,
        market: bool,
        price_ticks: u32,
        lots: u32,
        tif: u8,
        post_only: bool,
        cancel_oldest: bool,
        stop_ticks: Option<u32>,
    },
    Cancel {
        account: u8,
        pick: usize,
    },
}

fn op() -> impl Strategy<Value = Op> {
    let submit = (
        0u8..3,
        any::<bool>(),
        prop::bool::weighted(0.15),
        95u32..=105,
        1u32..=20,
        0u8..3,
        prop::bool::weighted(0.1),
        any::<bool>(),
        prop::option::weighted(0.2, 95u32..=105),
    )
        .prop_map(
            |(
                account,
                buy,
                market,
                price_ticks,
                lots,
                tif,
                post_only,
                cancel_oldest,
                stop_ticks,
            )| {
                Op::Submit {
                    account,
                    buy,
                    market,
                    price_ticks,
                    lots,
                    tif,
                    post_only,
                    cancel_oldest,
                    stop_ticks,
                }
            },
        );
    let cancel = (0u8..3, any::<usize>()).prop_map(|(account, pick)| Op::Cancel { account, pick });
    prop_oneof![4 => submit, 1 => cancel]
}

fn engine() -> MatchingEngine {
    let spec = MarketSpec::builder(MarketId::parse("BTC-USD").unwrap())
        .tick_size(Decimal::ONE)
        .lot_size(Decimal::ONE)
        .max_quantity(Decimal::from(1000))
        .build()
        .unwrap();
    MatchingEngine::new(spec)
}

fn account(n: u8) -> AccountId {
    AccountId::parse(&format!("acct{n}")).unwrap()
}

#[derive(Debug, PartialEq)]
enum Step {
    Submitted(Result<MatchOutcome, DomainError>),
    Cancelled(Result<crate::CancelOutcome, DomainError>),
    Skipped,
}

/// Replays `ops` on a fresh engine, checking invariants after every step.
fn run(ops: &[Op]) -> Vec<Step> {
    let mut engine = engine();
    let mut ids = Vec::new();
    let mut last_sequence = 0;
    let mut steps = Vec::with_capacity(ops.len());

    for (i, op) in ops.iter().enumerate() {
        let now = Timestamp::from_unix_nanos(i as u64 + 1);
        let step = match op {
            Op::Submit {
                account: acct,
                buy,
                market,
                price_ticks,
                lots,
                tif,
                post_only,
                cancel_oldest,
                stop_ticks,
            } => {
                let price = Price::new(Decimal::from(*price_ticks)).unwrap();
                let tif = [
                    TimeInForce::GoodTilCancelled,
                    TimeInForce::ImmediateOrCancel,
                    TimeInForce::FillOrKill,
                ][usize::from(*tif)];
                let kind = if *market {
                    OrderKind::Market
                } else {
                    match OrderKind::limit(price, tif, *post_only) {
                        Ok(kind) => kind,
                        Err(_) => {
                            steps.push(Step::Skipped);
                            continue;
                        }
                    }
                };
                let id = OrderId::from_uuid(Uuid::from_u128(i as u128 + 1));
                ids.push(id);
                let order = NewOrder {
                    id,
                    account: account(*acct),
                    market: engine.spec().id().clone(),
                    side: if *buy { Side::Buy } else { Side::Sell },
                    kind,
                    quantity: Quantity::positive(Decimal::from(*lots)).unwrap(),
                    stop_price: stop_ticks.map(|ticks| Price::new(Decimal::from(ticks)).unwrap()),
                    client_order_id: None,
                    self_trade_prevention: if *cancel_oldest {
                        SelfTradePrevention::CancelOldest
                    } else {
                        SelfTradePrevention::CancelNewest
                    },
                };
                let result = engine.submit(order, now);
                if let Ok(outcome) = &result {
                    check_outcome(&engine, outcome, &mut last_sequence);
                }
                Step::Submitted(result)
            }
            Op::Cancel {
                account: acct,
                pick,
            } => {
                if ids.is_empty() {
                    Step::Skipped
                } else {
                    let id = ids[pick % ids.len()];
                    let result = engine.cancel(id, &account(*acct), now);
                    if let Ok(outcome) = &result {
                        assert_eq!(outcome.event.sequence, last_sequence + 1);
                        last_sequence = outcome.event.sequence;
                        assert!(engine.book().get(id).is_none());
                        assert!(engine.pending_stop(id).is_none());
                    }
                    Step::Cancelled(result)
                }
            }
        };
        engine.book().assert_consistent();
        engine.stops_assert_consistent();
        steps.push(step);
    }
    steps
}

fn check_outcome(engine: &MatchingEngine, outcome: &MatchOutcome, last_sequence: &mut u64) {
    let order = &outcome.order;

    for event in &outcome.events {
        assert_eq!(event.sequence, *last_sequence + 1, "sequence gap");
        *last_sequence = event.sequence;
    }

    let mut traded = Quantity::ZERO;
    for trade in &outcome.trades {
        assert!(!trade.quantity.is_zero());
        assert_ne!(
            trade.maker_account, trade.taker_account,
            "self trade executed"
        );
        let as_taker = trade.taker_order_id == order.id();
        assert!(
            as_taker || trade.maker_order_id == order.id(),
            "foreign trade reported"
        );
        if let Some(limit) = order.limit_price() {
            match (as_taker, order.side()) {
                (true, Side::Buy) => assert!(trade.price <= limit),
                (true, Side::Sell) => assert!(trade.price >= limit),
                (false, _) => assert_eq!(trade.price, limit, "makers trade at their own price"),
            }
        }
        traded = traded.checked_add(trade.quantity).unwrap();
    }
    assert_eq!(
        traded,
        order.filled(),
        "fills must equal reported trade volume"
    );

    let mut ids = std::collections::HashSet::new();
    for changed in outcome.changed_orders() {
        assert!(ids.insert(changed.id()), "an order is reported twice");
        let resting = engine.book().get(changed.id()).is_some();
        let pending = engine.pending_stop(changed.id()).is_some();
        let status = changed.status();
        assert_eq!(
            resting,
            matches!(status, OrderStatus::Open | OrderStatus::PartiallyFilled)
        );
        assert_eq!(pending, status == OrderStatus::Pending);
        if changed.kind() == OrderKind::Market {
            assert!(!resting, "market orders never rest");
        }
    }

    match order.kind() {
        OrderKind::Limit {
            time_in_force: TimeInForce::FillOrKill,
            ..
        } if order.stop_price().is_none() => {
            assert!(order.filled().is_zero() || order.status() == OrderStatus::Filled)
        }
        OrderKind::Limit {
            post_only: true, ..
        } => assert!(
            outcome
                .trades
                .iter()
                .all(|t| t.taker_order_id != order.id()),
            "post-only orders never take liquidity"
        ),
        _ => {}
    }
}

proptest! {
    // 256 cases by default; set PROPTEST_CASES for longer runs.
    #![proptest_config(ProptestConfig::default())]

    #[test]
    fn random_order_flow_preserves_book_invariants(ops in prop::collection::vec(op(), 1..200)) {
        run(&ops);
    }

    #[test]
    fn matching_is_deterministic(ops in prop::collection::vec(op(), 1..100)) {
        prop_assert_eq!(run(&ops), run(&ops));
    }
}
