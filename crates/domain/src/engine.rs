//! Price-time priority matching for a single market.

use std::collections::HashSet;

use crate::{
    book::{BookSnapshot, OrderBook, crosses},
    error::DomainError,
    events::{DomainEvent, EventPayload, Trade},
    ids::{AccountId, OrderId, TradeId},
    market::MarketSpec,
    numeric::Price,
    order::{CancelReason, NewOrder, Order, OrderKind, SelfTradePrevention, TimeInForce},
    stops::{StopBook, stop_reached},
    time::Timestamp,
};

/// Result of submitting one order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchOutcome {
    /// Final state of the submitted order.
    pub order: Order,
    /// Every trade the submitted order took part in, as taker or, after a
    /// stop cascade, as maker.
    pub trades: Vec<Trade>,
    /// Final state of every other order this submission changed: filled or
    /// cancelled makers and stop orders that fired. One entry per order.
    pub updates: Vec<Order>,
    /// Everything that happened, in order, ready to publish.
    pub events: Vec<DomainEvent>,
}

impl MatchOutcome {
    /// The submitted order followed by every other order whose state changed.
    pub fn changed_orders(&self) -> impl Iterator<Item = &Order> {
        std::iter::once(&self.order).chain(self.updates.iter())
    }
}

/// Result of cancelling one order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOutcome {
    pub order: Order,
    pub event: DomainEvent,
}

/// Mutable scratch space for one submission, including any stop cascade.
#[derive(Default)]
struct Journal {
    events: Vec<DomainEvent>,
    trades: Vec<Trade>,
    /// Snapshots of changed orders, oldest first. An order may appear more
    /// than once; the last snapshot wins.
    touched: Vec<Order>,
}

/// Deterministic matching engine for one market.
///
/// The engine is a plain state machine: `submit` and `cancel` take `&mut
/// self`, never block and never allocate ids or read clocks. It is designed
/// to be owned by exactly one task (the single writer principle), so the hot
/// path needs no locks at all.
#[derive(Debug)]
pub struct MatchingEngine {
    spec: MarketSpec,
    book: OrderBook,
    stops: StopBook,
    last_price: Option<Price>,
    sequence: u64,
    last_trade_id: u64,
}

impl MatchingEngine {
    pub fn new(spec: MarketSpec) -> Self {
        Self {
            spec,
            book: OrderBook::default(),
            stops: StopBook::default(),
            last_price: None,
            sequence: 0,
            last_trade_id: 0,
        }
    }

    pub fn spec(&self) -> &MarketSpec {
        &self.spec
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    /// Sequence number of the last emitted event, 0 before the first one.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Price of the most recent trade, which is what stop orders watch.
    pub fn last_price(&self) -> Option<Price> {
        self.last_price
    }

    /// Number of stop orders waiting for their trigger.
    pub fn pending_stops(&self) -> usize {
        self.stops.len()
    }

    /// A pending stop order, if `id` is one.
    pub fn pending_stop(&self, id: OrderId) -> Option<&Order> {
        self.stops.get(id)
    }

    /// Checks the stop book invariants. Used by the property tests.
    #[cfg(test)]
    pub(crate) fn stops_assert_consistent(&self) {
        self.stops.assert_consistent(self.last_price);
    }

    pub fn snapshot(&self, depth: usize) -> BookSnapshot {
        let (bids, asks) = self.book.depth(depth);
        BookSnapshot {
            market: self.spec.id().clone(),
            sequence: self.sequence,
            last_price: self.last_price,
            bids,
            asks,
        }
    }

    /// Validates, matches and, if appropriate, rests an incoming order.
    ///
    /// Rejections (`Err`) leave the engine untouched and emit no events.
    /// Accepted orders always produce `OrderAccepted` first, then one
    /// `TradeExecuted` per fill, then `OrderCancelled` for any remainder that
    /// is not allowed to rest. A stop order only produces `OrderAccepted`
    /// and waits until a later trade reaches its stop price.
    ///
    /// Trades can fire stop orders, whose own trades can fire more. That
    /// cascade runs to completion inside this call, so the engine never
    /// leaves a reached stop waiting.
    pub fn submit(&mut self, new: NewOrder, now: Timestamp) -> Result<MatchOutcome, DomainError> {
        self.spec.validate(&new)?;
        if self.book.get(new.id).is_some() || self.stops.get(new.id).is_some() {
            return Err(DomainError::DuplicateOrderId(new.id));
        }
        match new.stop_price {
            Some(_) if new.kind.is_post_only() => return Err(DomainError::StopOrderPostOnly),
            Some(stop_price) => {
                if let Some(last_price) = self.last_price
                    && stop_reached(new.side, stop_price, last_price)
                {
                    return Err(DomainError::StopWouldTriggerImmediately {
                        stop_price,
                        last_price,
                    });
                }
            }
            None => {
                if let OrderKind::Limit {
                    price,
                    post_only: true,
                    ..
                } = new.kind
                    && self.book.would_cross(new.side, price)
                {
                    return Err(DomainError::PostOnlyWouldCross);
                }
            }
        }

        let mut order = Order::accept(new, now);
        let mut journal = Journal::default();
        let accepted = self.emit(now, EventPayload::OrderAccepted(order.clone()));
        journal.events.push(accepted);

        if order.stop_price().is_some() {
            self.stops.insert(order.clone())?;
        } else {
            self.execute(&mut order, now, &mut journal)?;
            self.run_stop_cascade(now, &mut journal)?;
        }
        Ok(journal.finish(order))
    }

    /// Cancels a resting or pending order on behalf of its owner.
    ///
    /// An order that exists but belongs to someone else is reported as not
    /// found, so the endpoint cannot be used to probe other accounts' ids.
    pub fn cancel(
        &mut self,
        order_id: OrderId,
        account: &AccountId,
        now: Timestamp,
    ) -> Result<CancelOutcome, DomainError> {
        let owned = |order: &Order| order.account() == account;
        let removed = if self.book.get(order_id).is_some_and(owned) {
            self.book.remove(order_id)
        } else if self.stops.get(order_id).is_some_and(owned) {
            self.stops.remove(order_id)
        } else {
            None
        };
        let mut order = removed.ok_or(DomainError::OrderNotFound(order_id))?;
        order.cancel(CancelReason::Requested, now);
        let event = self.cancelled_event(&order, now);
        Ok(CancelOutcome { order, event })
    }

    /// Matches an active order against the book and settles its remainder.
    /// Used for new orders and for stop orders that just fired.
    fn execute(
        &mut self,
        taker: &mut Order,
        now: Timestamp,
        journal: &mut Journal,
    ) -> Result<(), DomainError> {
        let limit = taker.limit_price();

        if taker.kind().time_in_force() == Some(TimeInForce::FillOrKill)
            && self.book.fillable_quantity(taker, limit)? < taker.quantity()
        {
            self.cancel_taker(taker, CancelReason::FillOrKill, now, journal);
            return Ok(());
        }

        let maker_side = taker.side().opposite();
        let mut self_trade_stop = false;

        while !taker.remaining().is_zero() {
            let Some(best) = self.book.best_order(maker_side) else {
                break;
            };
            let maker_id = best.id();
            let maker_remaining = best.remaining();
            let same_account = best.account() == taker.account();
            let maker_price = best.limit_price().ok_or(DomainError::InvariantViolation(
                "resting order without price",
            ))?;

            if !crosses(taker.side(), limit, maker_price) {
                break;
            }

            if same_account {
                match taker.self_trade_prevention() {
                    SelfTradePrevention::CancelNewest => {
                        self_trade_stop = true;
                        break;
                    }
                    SelfTradePrevention::CancelOldest => {
                        let mut stale = self
                            .book
                            .remove(maker_id)
                            .ok_or(DomainError::InvariantViolation("best order vanished"))?;
                        stale.cancel(CancelReason::SelfTradePrevention, now);
                        let event = self.cancelled_event(&stale, now);
                        journal.events.push(event);
                        journal.touched.push(stale);
                        continue;
                    }
                }
            }

            let quantity = taker.remaining().min(maker_remaining);
            let maker = self.book.fill_best(maker_side, quantity, now)?;
            taker.fill(quantity, now)?;
            self.last_price = Some(maker_price);

            let trade = Trade {
                id: self.next_trade_id(),
                market: self.spec.id().clone(),
                price: maker_price,
                quantity,
                taker_side: taker.side(),
                maker_order_id: maker.id(),
                taker_order_id: taker.id(),
                maker_account: maker.account().clone(),
                taker_account: taker.account().clone(),
                executed_at: now,
            };
            let event = self.emit(now, EventPayload::TradeExecuted(trade.clone()));
            journal.events.push(event);
            journal.trades.push(trade);
            journal.touched.push(maker);
        }

        if !taker.remaining().is_zero() {
            match remainder_policy(taker, self_trade_stop) {
                None => {
                    taker.rest();
                    self.book.insert(taker.clone())?;
                }
                Some(reason) => self.cancel_taker(taker, reason, now, journal),
            }
        }
        Ok(())
    }

    /// Fires every stop order the last trade price has reached, one at a
    /// time, until none is left. Each fired stop may trade and move the
    /// price, so the check is repeated after every execution. The loop ends
    /// because every stop can fire at most once.
    fn run_stop_cascade(
        &mut self,
        now: Timestamp,
        journal: &mut Journal,
    ) -> Result<(), DomainError> {
        while let Some(trigger_price) = self.last_price {
            let Some(mut stop) = self.stops.pop_reached(trigger_price) else {
                break;
            };
            let stop_price = stop
                .stop_price()
                .ok_or(DomainError::InvariantViolation("stop without stop price"))?;
            stop.trigger(now);
            let event = self.emit(
                now,
                EventPayload::StopTriggered {
                    order_id: stop.id(),
                    account: stop.account().clone(),
                    stop_price,
                    trigger_price,
                },
            );
            journal.events.push(event);
            self.execute(&mut stop, now, journal)?;
            journal.touched.push(stop);
        }
        Ok(())
    }

    fn cancel_taker(
        &mut self,
        taker: &mut Order,
        reason: CancelReason,
        now: Timestamp,
        journal: &mut Journal,
    ) {
        taker.cancel(reason, now);
        let event = self.cancelled_event(taker, now);
        journal.events.push(event);
    }

    fn cancelled_event(&mut self, order: &Order, now: Timestamp) -> DomainEvent {
        self.emit(
            now,
            EventPayload::OrderCancelled {
                order_id: order.id(),
                account: order.account().clone(),
                reason: order.cancel_reason().unwrap_or(CancelReason::Requested),
                remaining: order.remaining(),
            },
        )
    }

    fn emit(&mut self, now: Timestamp, payload: EventPayload) -> DomainEvent {
        // A u64 lasts centuries at a billion events per second, so plain
        // increments are safe and keep the sequence gap free.
        self.sequence += 1;
        DomainEvent {
            market: self.spec.id().clone(),
            sequence: self.sequence,
            occurred_at: now,
            payload,
        }
    }

    fn next_trade_id(&mut self) -> TradeId {
        self.last_trade_id += 1;
        TradeId::new(self.last_trade_id)
    }
}

impl Journal {
    /// Builds the outcome for `order`, keeping only the latest snapshot of
    /// every changed order. The submitted order itself can be touched again
    /// later in the cascade, as the maker for a fired stop, so its latest
    /// snapshot may come from `touched` too.
    fn finish(self, order: Order) -> MatchOutcome {
        let id = order.id();
        let mut seen = HashSet::new();
        let mut latest: Vec<Order> = self
            .touched
            .into_iter()
            .rev()
            .filter(|touched| seen.insert(touched.id()))
            .collect();
        latest.reverse();

        let order = match latest.iter().position(|touched| touched.id() == id) {
            Some(index) => latest.remove(index),
            None => order,
        };
        let trades = self
            .trades
            .into_iter()
            .filter(|trade| trade.taker_order_id == id || trade.maker_order_id == id)
            .collect();
        MatchOutcome {
            order,
            trades,
            updates: latest,
            events: self.events,
        }
    }
}

/// Decides what to do with quantity left after matching: `None` rests it on
/// the book, `Some(reason)` cancels it.
fn remainder_policy(taker: &Order, self_trade_stop: bool) -> Option<CancelReason> {
    if self_trade_stop {
        return Some(CancelReason::SelfTradePrevention);
    }
    match taker.kind() {
        OrderKind::Limit { time_in_force, .. } => match time_in_force {
            TimeInForce::GoodTilCancelled => None,
            TimeInForce::ImmediateOrCancel => Some(CancelReason::ImmediateOrCancel),
            // The pre-check guarantees a full fill, so this arm only runs if
            // that guarantee is broken. Cancelling is the safe fallback.
            TimeInForce::FillOrKill => Some(CancelReason::FillOrKill),
        },
        OrderKind::Market => Some(CancelReason::NoLiquidity),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::*;
    use crate::{
        ids::ClientOrderId,
        market::MarketId,
        numeric::{Price, Quantity},
        order::{OrderStatus, Side},
    };

    struct Harness {
        engine: MatchingEngine,
        next_id: u128,
        clock: u64,
    }

    impl Harness {
        fn new() -> Self {
            let spec = MarketSpec::builder(MarketId::parse("BTC-USD").unwrap())
                .tick_size(dec!(0.5))
                .lot_size(dec!(0.1))
                .max_quantity(dec!(1000))
                .build()
                .unwrap();
            Self {
                engine: MatchingEngine::new(spec),
                next_id: 0,
                clock: 0,
            }
        }

        fn order(
            &mut self,
            account: &str,
            side: Side,
            kind: OrderKind,
            quantity: Decimal,
        ) -> NewOrder {
            self.next_id += 1;
            NewOrder {
                id: OrderId::from_uuid(Uuid::from_u128(self.next_id)),
                account: AccountId::parse(account).unwrap(),
                market: self.engine.spec().id().clone(),
                side,
                kind,
                quantity: Quantity::positive(quantity).unwrap(),
                stop_price: None,
                client_order_id: None,
                self_trade_prevention: SelfTradePrevention::CancelNewest,
            }
        }

        fn submit(&mut self, order: NewOrder) -> Result<MatchOutcome, DomainError> {
            self.clock += 1;
            let outcome = self
                .engine
                .submit(order, Timestamp::from_unix_nanos(self.clock));
            self.engine.book().assert_consistent();
            outcome
        }

        fn limit(
            &mut self,
            account: &str,
            side: Side,
            price: Decimal,
            quantity: Decimal,
        ) -> MatchOutcome {
            let order = self.order(account, side, gtc(price), quantity);
            self.submit(order).unwrap()
        }
    }

    fn gtc(price: Decimal) -> OrderKind {
        limit_with(price, TimeInForce::GoodTilCancelled)
    }

    fn limit_with(price: Decimal, time_in_force: TimeInForce) -> OrderKind {
        OrderKind::limit(Price::new(price).unwrap(), time_in_force, false).unwrap()
    }

    fn qty(value: Decimal) -> Quantity {
        Quantity::positive(value).unwrap()
    }

    #[test]
    fn non_crossing_limit_order_rests_on_the_book() {
        let mut h = Harness::new();
        let outcome = h.limit("alice", Side::Buy, dec!(100), dec!(1));

        assert_eq!(outcome.order.status(), OrderStatus::Open);
        assert!(outcome.trades.is_empty());
        assert_eq!(
            h.engine.book().best_price(Side::Buy),
            Some(Price::new(dec!(100)).unwrap())
        );
        assert_eq!(outcome.events.len(), 1);
        assert_eq!(outcome.events[0].payload.kind(), "order_accepted");
    }

    #[test]
    fn crossing_order_trades_at_the_maker_price() {
        let mut h = Harness::new();
        h.limit("alice", Side::Sell, dec!(100), dec!(1));
        let outcome = h.limit("bob", Side::Buy, dec!(105), dec!(1));

        assert_eq!(outcome.order.status(), OrderStatus::Filled);
        assert_eq!(outcome.trades.len(), 1);
        assert_eq!(outcome.trades[0].price, Price::new(dec!(100)).unwrap());
        assert_eq!(outcome.updates[0].status(), OrderStatus::Filled);
        assert!(h.engine.book().is_empty());
    }

    #[test]
    fn better_prices_fill_first_then_older_orders() {
        let mut h = Harness::new();
        let late_at_100 = h.limit("m1", Side::Sell, dec!(100), dec!(1));
        let at_99 = h.limit("m2", Side::Sell, dec!(99), dec!(1));
        let later_at_100 = h.limit("m3", Side::Sell, dec!(100), dec!(1));

        let outcome = h.limit("taker", Side::Buy, dec!(100), dec!(2.5));

        let makers: Vec<_> = outcome.trades.iter().map(|t| t.maker_order_id).collect();
        assert_eq!(
            makers,
            vec![
                at_99.order.id(),
                late_at_100.order.id(),
                later_at_100.order.id()
            ]
        );
        assert_eq!(outcome.trades[2].quantity, qty(dec!(0.5)));
        assert_eq!(outcome.order.status(), OrderStatus::Filled);
        let resting = h.engine.book().get(later_at_100.order.id()).unwrap();
        assert_eq!(resting.status(), OrderStatus::PartiallyFilled);
        assert_eq!(resting.remaining(), qty(dec!(0.5)));
    }

    #[test]
    fn partially_filled_limit_order_rests_its_remainder() {
        let mut h = Harness::new();
        h.limit("alice", Side::Sell, dec!(100), dec!(1));
        let outcome = h.limit("bob", Side::Buy, dec!(100), dec!(3));

        assert_eq!(outcome.order.status(), OrderStatus::PartiallyFilled);
        assert_eq!(outcome.order.remaining(), qty(dec!(2)));
        assert_eq!(
            h.engine.book().best_price(Side::Buy),
            Some(Price::new(dec!(100)).unwrap())
        );
    }

    #[test]
    fn immediate_or_cancel_never_rests() {
        let mut h = Harness::new();
        h.limit("alice", Side::Sell, dec!(100), dec!(1));
        let order = h.order(
            "bob",
            Side::Buy,
            limit_with(dec!(100), TimeInForce::ImmediateOrCancel),
            dec!(2),
        );
        let outcome = h.submit(order).unwrap();

        assert_eq!(outcome.order.status(), OrderStatus::Cancelled);
        assert_eq!(
            outcome.order.cancel_reason(),
            Some(CancelReason::ImmediateOrCancel)
        );
        assert_eq!(outcome.order.filled(), qty(dec!(1)));
        assert!(h.engine.book().is_empty());
        let kinds: Vec<_> = outcome.events.iter().map(|e| e.payload.kind()).collect();
        assert_eq!(
            kinds,
            ["order_accepted", "trade_executed", "order_cancelled"]
        );
    }

    #[test]
    fn fill_or_kill_without_enough_liquidity_leaves_the_book_untouched() {
        let mut h = Harness::new();
        let maker = h.limit("alice", Side::Sell, dec!(100), dec!(1));
        let order = h.order(
            "bob",
            Side::Buy,
            limit_with(dec!(100), TimeInForce::FillOrKill),
            dec!(2),
        );
        let outcome = h.submit(order).unwrap();

        assert!(outcome.trades.is_empty());
        assert_eq!(
            outcome.order.cancel_reason(),
            Some(CancelReason::FillOrKill)
        );
        assert_eq!(
            h.engine.book().get(maker.order.id()).unwrap().remaining(),
            qty(dec!(1))
        );
    }

    #[test]
    fn fill_or_kill_with_enough_liquidity_fills_completely() {
        let mut h = Harness::new();
        h.limit("alice", Side::Sell, dec!(100), dec!(1));
        h.limit("carol", Side::Sell, dec!(100.5), dec!(1));
        let order = h.order(
            "bob",
            Side::Buy,
            limit_with(dec!(101), TimeInForce::FillOrKill),
            dec!(2),
        );
        let outcome = h.submit(order).unwrap();

        assert_eq!(outcome.order.status(), OrderStatus::Filled);
        assert_eq!(outcome.trades.len(), 2);
    }

    #[test]
    fn market_order_sweeps_levels_and_cancels_the_rest() {
        let mut h = Harness::new();
        h.limit("alice", Side::Buy, dec!(99), dec!(1));
        h.limit("carol", Side::Buy, dec!(98), dec!(1));
        let order = h.order("bob", Side::Sell, OrderKind::Market, dec!(3));
        let outcome = h.submit(order).unwrap();

        let prices: Vec<_> = outcome.trades.iter().map(|t| t.price.value()).collect();
        assert_eq!(prices, vec![dec!(99), dec!(98)]);
        assert_eq!(
            outcome.order.cancel_reason(),
            Some(CancelReason::NoLiquidity)
        );
        assert_eq!(outcome.order.filled(), qty(dec!(2)));
        assert!(h.engine.book().is_empty());
    }

    #[test]
    fn post_only_order_is_rejected_when_it_would_take_liquidity() {
        let mut h = Harness::new();
        h.limit("alice", Side::Sell, dec!(100), dec!(1));
        let sequence_before = h.engine.sequence();
        let kind = OrderKind::limit(
            Price::new(dec!(100)).unwrap(),
            TimeInForce::GoodTilCancelled,
            true,
        )
        .unwrap();
        let order = h.order("bob", Side::Buy, kind, dec!(1));

        assert_eq!(h.submit(order), Err(DomainError::PostOnlyWouldCross));
        assert_eq!(
            h.engine.sequence(),
            sequence_before,
            "rejections emit nothing"
        );
    }

    #[test]
    fn self_trade_cancel_newest_keeps_the_resting_order() {
        let mut h = Harness::new();
        let resting = h.limit("alice", Side::Sell, dec!(100), dec!(1));
        let outcome = h.limit("alice", Side::Buy, dec!(100), dec!(1));

        assert!(outcome.trades.is_empty());
        assert_eq!(
            outcome.order.cancel_reason(),
            Some(CancelReason::SelfTradePrevention)
        );
        assert!(h.engine.book().get(resting.order.id()).is_some());
    }

    #[test]
    fn self_trade_cancel_oldest_removes_the_resting_order_and_keeps_matching() {
        let mut h = Harness::new();
        let own = h.limit("alice", Side::Sell, dec!(100), dec!(1));
        let other = h.limit("carol", Side::Sell, dec!(100), dec!(1));
        let mut order = h.order("alice", Side::Buy, gtc(dec!(100)), dec!(1));
        order.self_trade_prevention = SelfTradePrevention::CancelOldest;
        let outcome = h.submit(order).unwrap();

        assert_eq!(outcome.trades.len(), 1);
        assert_eq!(outcome.trades[0].maker_order_id, other.order.id());
        assert_eq!(outcome.updates[0].id(), own.order.id());
        assert_eq!(
            outcome.updates[0].cancel_reason(),
            Some(CancelReason::SelfTradePrevention)
        );
        assert!(h.engine.book().is_empty());
    }

    #[test]
    fn cancel_only_works_for_the_owner() {
        let mut h = Harness::new();
        let resting = h.limit("alice", Side::Buy, dec!(100), dec!(1));
        let id = resting.order.id();
        let mallory = AccountId::parse("mallory").unwrap();
        let alice = AccountId::parse("alice").unwrap();
        let now = Timestamp::from_unix_nanos(99);

        assert_eq!(
            h.engine.cancel(id, &mallory, now),
            Err(DomainError::OrderNotFound(id))
        );
        let outcome = h.engine.cancel(id, &alice, now).unwrap();
        assert_eq!(outcome.order.status(), OrderStatus::Cancelled);
        assert!(h.engine.book().is_empty());
        assert_eq!(
            h.engine.cancel(id, &alice, now),
            Err(DomainError::OrderNotFound(id))
        );
    }

    #[test]
    fn invalid_orders_are_rejected_without_side_effects() {
        let mut h = Harness::new();
        let off_tick = h.order("alice", Side::Buy, gtc(dec!(100.25)), dec!(1));
        assert!(matches!(
            h.submit(off_tick),
            Err(DomainError::PriceNotOnTick { .. })
        ));

        let mut wrong_market = h.order("alice", Side::Buy, gtc(dec!(100)), dec!(1));
        wrong_market.market = MarketId::parse("ETH-USD").unwrap();
        assert!(matches!(
            h.submit(wrong_market),
            Err(DomainError::MarketMismatch { .. })
        ));
        assert_eq!(h.engine.sequence(), 0);
    }

    #[test]
    fn duplicate_order_ids_are_rejected() {
        let mut h = Harness::new();
        let order = h.order("alice", Side::Buy, gtc(dec!(100)), dec!(1));
        h.submit(order.clone()).unwrap();
        assert_eq!(
            h.submit(order.clone()),
            Err(DomainError::DuplicateOrderId(order.id))
        );
    }

    #[test]
    fn events_are_numbered_without_gaps() {
        let mut h = Harness::new();
        h.limit("alice", Side::Sell, dec!(100), dec!(1));
        h.limit("carol", Side::Sell, dec!(101), dec!(1));
        let outcome = h.limit("bob", Side::Buy, dec!(101), dec!(2));

        let sequences: Vec<_> = outcome.events.iter().map(|e| e.sequence).collect();
        assert_eq!(sequences, vec![3, 4, 5]);
        assert_eq!(h.engine.sequence(), 5);
    }

    #[test]
    fn snapshot_aggregates_levels_best_first() {
        let mut h = Harness::new();
        h.limit("a", Side::Buy, dec!(99), dec!(1));
        h.limit("b", Side::Buy, dec!(99), dec!(2));
        h.limit("c", Side::Buy, dec!(98), dec!(1));
        h.limit("d", Side::Sell, dec!(101), dec!(1.5));

        let snapshot = h.engine.snapshot(1);
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.bids[0].price, Price::new(dec!(99)).unwrap());
        assert_eq!(snapshot.bids[0].quantity, qty(dec!(3)));
        assert_eq!(snapshot.bids[0].order_count, 2);
        assert_eq!(snapshot.asks[0].quantity, qty(dec!(1.5)));
        assert_eq!(snapshot.sequence, 4);
    }

    fn stop(
        h: &mut Harness,
        account: &str,
        side: Side,
        kind: OrderKind,
        stop: Decimal,
        qty: Decimal,
    ) -> NewOrder {
        let mut order = h.order(account, side, kind, qty);
        order.stop_price = Some(Price::new(stop).unwrap());
        order
    }

    /// Trades 1 lot at `price` between two throwaway accounts, which sets the
    /// last trade price that stop orders watch.
    fn trade_at(h: &mut Harness, price: Decimal) {
        h.limit("seed-seller", Side::Sell, price, dec!(1));
        h.limit("seed-buyer", Side::Buy, price, dec!(1));
        assert_eq!(h.engine.last_price(), Some(Price::new(price).unwrap()));
    }

    fn kinds(outcome: &MatchOutcome) -> Vec<&'static str> {
        outcome.events.iter().map(|e| e.payload.kind()).collect()
    }

    #[test]
    fn stop_order_waits_outside_the_book_until_a_trade_reaches_it() {
        let mut h = Harness::new();
        trade_at(&mut h, dec!(100));
        let order = stop(
            &mut h,
            "carol",
            Side::Buy,
            OrderKind::Market,
            dec!(105),
            dec!(1),
        );
        let pending = h.submit(order).unwrap();

        assert_eq!(pending.order.status(), OrderStatus::Pending);
        assert_eq!(kinds(&pending), ["order_accepted"]);
        assert_eq!(h.engine.pending_stops(), 1);
        assert!(
            h.engine.book().is_empty(),
            "stops are not visible in the book"
        );

        let ask = h.limit("alice", Side::Sell, dec!(105), dec!(2));
        let trigger = h.limit("bob", Side::Buy, dec!(105), dec!(1));

        assert_eq!(
            kinds(&trigger),
            [
                "order_accepted",
                "trade_executed",
                "stop_triggered",
                "trade_executed"
            ]
        );
        let fired = trigger
            .updates
            .iter()
            .find(|o| o.id() == pending.order.id())
            .unwrap();
        assert_eq!(fired.status(), OrderStatus::Filled);
        let maker = trigger
            .updates
            .iter()
            .find(|o| o.id() == ask.order.id())
            .unwrap();
        assert_eq!(maker.status(), OrderStatus::Filled);
        assert_eq!(h.engine.pending_stops(), 0);
    }

    #[test]
    fn stop_that_would_fire_immediately_is_rejected() {
        let mut h = Harness::new();
        let before_any_trade = stop(
            &mut h,
            "carol",
            Side::Buy,
            OrderKind::Market,
            dec!(50),
            dec!(1),
        );
        assert!(
            h.submit(before_any_trade).is_ok(),
            "without a last price any stop waits"
        );

        let mut h = Harness::new();
        trade_at(&mut h, dec!(100));
        let buy_at_last = stop(
            &mut h,
            "carol",
            Side::Buy,
            OrderKind::Market,
            dec!(100),
            dec!(1),
        );
        assert!(matches!(
            h.submit(buy_at_last),
            Err(DomainError::StopWouldTriggerImmediately { .. })
        ));
        let sell_above_last = stop(
            &mut h,
            "carol",
            Side::Sell,
            OrderKind::Market,
            dec!(101),
            dec!(1),
        );
        assert!(matches!(
            h.submit(sell_above_last),
            Err(DomainError::StopWouldTriggerImmediately { .. })
        ));
        let valid = stop(
            &mut h,
            "carol",
            Side::Sell,
            OrderKind::Market,
            dec!(99),
            dec!(1),
        );
        assert!(h.submit(valid).is_ok());
    }

    #[test]
    fn sell_stop_limit_fires_on_a_falling_price_and_rests_its_remainder() {
        let mut h = Harness::new();
        trade_at(&mut h, dec!(100));
        let order = stop(
            &mut h,
            "carol",
            Side::Sell,
            gtc(dec!(94)),
            dec!(95),
            dec!(3),
        );
        let pending = h.submit(order).unwrap();

        h.limit("frank", Side::Buy, dec!(94.5), dec!(1));
        h.limit("dave", Side::Buy, dec!(95), dec!(1));
        let trigger = h.limit("erin", Side::Sell, dec!(95), dec!(1));

        let fired = trigger
            .updates
            .iter()
            .find(|o| o.id() == pending.order.id())
            .unwrap();
        assert_eq!(fired.status(), OrderStatus::PartiallyFilled);
        assert_eq!(fired.remaining(), qty(dec!(2)));
        assert_eq!(
            h.engine.book().best_price(Side::Sell),
            Some(Price::new(dec!(94)).unwrap())
        );
        assert_eq!(h.engine.last_price(), Some(Price::new(dec!(94.5)).unwrap()));
    }

    #[test]
    fn stop_cascade_fires_stops_in_price_order() {
        let mut h = Harness::new();
        trade_at(&mut h, dec!(100));
        let first = stop(
            &mut h,
            "s1",
            Side::Buy,
            OrderKind::Market,
            dec!(102),
            dec!(1),
        );
        let first = h.submit(first).unwrap().order.id();
        let second = stop(
            &mut h,
            "s2",
            Side::Buy,
            OrderKind::Market,
            dec!(103),
            dec!(1),
        );
        let second = h.submit(second).unwrap().order.id();
        for price in [dec!(102), dec!(103), dec!(104)] {
            h.limit("maker", Side::Sell, price, dec!(1));
        }

        let trigger = h.limit("bob", Side::Buy, dec!(102), dec!(1));

        let fired: Vec<_> = trigger
            .events
            .iter()
            .filter_map(|e| match &e.payload {
                EventPayload::StopTriggered { order_id, .. } => Some(*order_id),
                _ => None,
            })
            .collect();
        assert_eq!(fired, vec![first, second]);
        let prices: Vec<_> = trigger
            .events
            .iter()
            .filter_map(|e| match &e.payload {
                EventPayload::TradeExecuted(trade) => Some(trade.price.value()),
                _ => None,
            })
            .collect();
        assert_eq!(prices, vec![dec!(102), dec!(103), dec!(104)]);
        assert_eq!(h.engine.pending_stops(), 0);
    }

    #[test]
    fn submitted_order_reports_fills_it_received_as_maker_during_a_cascade() {
        let mut h = Harness::new();
        trade_at(&mut h, dec!(100));
        let order = stop(
            &mut h,
            "carol",
            Side::Buy,
            OrderKind::Market,
            dec!(101),
            dec!(1),
        );
        h.submit(order).unwrap();
        h.limit("dave", Side::Buy, dec!(101), dec!(1));

        let outcome = h.limit("bob", Side::Sell, dec!(101), dec!(2));

        assert_eq!(outcome.order.status(), OrderStatus::Filled);
        assert_eq!(outcome.trades.len(), 2, "one fill as taker, one as maker");
        assert!(
            outcome
                .trades
                .iter()
                .any(|t| t.maker_order_id == outcome.order.id())
        );
        assert!(outcome.updates.iter().all(|o| o.id() != outcome.order.id()));
    }

    #[test]
    fn pending_stop_can_be_cancelled_by_its_owner_only() {
        let mut h = Harness::new();
        let order = stop(
            &mut h,
            "carol",
            Side::Sell,
            OrderKind::Market,
            dec!(90),
            dec!(1),
        );
        let id = h.submit(order).unwrap().order.id();
        let now = Timestamp::from_unix_nanos(99);

        let mallory = AccountId::parse("mallory").unwrap();
        assert_eq!(
            h.engine.cancel(id, &mallory, now),
            Err(DomainError::OrderNotFound(id))
        );
        let carol = AccountId::parse("carol").unwrap();
        let cancelled = h.engine.cancel(id, &carol, now).unwrap();
        assert_eq!(cancelled.order.status(), OrderStatus::Cancelled);
        assert_eq!(h.engine.pending_stops(), 0);
    }

    #[test]
    fn stop_orders_cannot_be_post_only() {
        let mut h = Harness::new();
        let kind = OrderKind::limit(
            Price::new(dec!(100)).unwrap(),
            TimeInForce::GoodTilCancelled,
            true,
        )
        .unwrap();
        let order = stop(&mut h, "carol", Side::Buy, kind, dec!(101), dec!(1));
        assert_eq!(h.submit(order), Err(DomainError::StopOrderPostOnly));
    }

    #[test]
    fn client_order_id_is_carried_through() {
        let mut h = Harness::new();
        let mut order = h.order("alice", Side::Buy, gtc(dec!(100)), dec!(1));
        order.client_order_id = Some(ClientOrderId::parse("my-order-1").unwrap());
        let outcome = h.submit(order).unwrap();
        assert_eq!(
            outcome.order.client_order_id().map(ClientOrderId::as_str),
            Some("my-order-1")
        );
    }
}
