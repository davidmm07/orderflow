//! Throughput of the matching hot path, without any async or IO around it.
//!
//! Run with `cargo bench -p orderflow-domain`.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use orderflow_domain::{
    AccountId, Decimal, MarketId, MarketSpec, MatchingEngine, NewOrder, OrderId, OrderKind, Price,
    Quantity, SelfTradePrevention, Side, TimeInForce, Timestamp,
};
use uuid::Uuid;

fn spec() -> MarketSpec {
    MarketSpec::builder(MarketId::parse("BTC-USD").unwrap_or_else(|_| unreachable!()))
        .tick_size(Decimal::ONE)
        .lot_size(Decimal::ONE)
        .max_quantity(Decimal::from(1_000_000))
        .build()
        .unwrap_or_else(|_| unreachable!())
}

fn order(n: u128, side: Side, price: u32, quantity: u32, account: &AccountId) -> NewOrder {
    NewOrder {
        id: OrderId::from_uuid(Uuid::from_u128(n)),
        account: account.clone(),
        market: MarketId::parse("BTC-USD").unwrap_or_else(|_| unreachable!()),
        side,
        kind: OrderKind::limit(
            Price::new(Decimal::from(price)).unwrap_or_else(|_| unreachable!()),
            TimeInForce::GoodTilCancelled,
            false,
        )
        .unwrap_or_else(|_| unreachable!()),
        quantity: Quantity::positive(Decimal::from(quantity)).unwrap_or_else(|_| unreachable!()),
        stop_price: None,
        client_order_id: None,
        self_trade_prevention: SelfTradePrevention::CancelNewest,
    }
}

/// A book with `levels` ask levels of `per_level` orders each.
fn seeded_engine(levels: u32, per_level: u32) -> MatchingEngine {
    let maker = AccountId::parse("maker").unwrap_or_else(|_| unreachable!());
    let mut engine = MatchingEngine::new(spec(), 1);
    let mut n = 0;
    for level in 0..levels {
        for _ in 0..per_level {
            n += 1;
            let _ = engine.submit(
                order(n, Side::Sell, 1_000 + level, 10, &maker),
                Timestamp::from_unix_nanos(n as u64),
            );
        }
    }
    engine
}

fn benchmarks(c: &mut Criterion) {
    let taker = AccountId::parse("taker").unwrap_or_else(|_| unreachable!());

    c.bench_function("rest_non_crossing_limit", |b| {
        b.iter_batched(
            || seeded_engine(50, 4),
            |mut engine| {
                let outcome = engine.submit(
                    order(1_000_000, Side::Buy, 900, 10, &taker),
                    Timestamp::from_unix_nanos(1),
                );
                // Returning the engine moves its drop outside the timed
                // section, so only the submit itself is measured.
                (engine, black_box(outcome))
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("sweep_ten_levels", |b| {
        b.iter_batched(
            || seeded_engine(50, 4),
            |mut engine| {
                let outcome = engine.submit(
                    order(1_000_000, Side::Buy, 1_010, 400, &taker),
                    Timestamp::from_unix_nanos(1),
                );
                (engine, black_box(outcome))
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
