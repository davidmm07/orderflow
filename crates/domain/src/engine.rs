//! Price-time priority matching for a single market.

use crate::{
    book::{BookSnapshot, OrderBook, crosses},
    error::DomainError,
    events::{DomainEvent, EventPayload, Trade},
    ids::{AccountId, OrderId, TradeId},
    market::MarketSpec,
    order::{CancelReason, NewOrder, Order, OrderKind, SelfTradePrevention, TimeInForce},
    time::Timestamp,
};

/// Result of submitting one order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchOutcome {
    /// Final state of the incoming order.
    pub order: Order,
    /// Executions in the order they happened.
    pub trades: Vec<Trade>,
    /// New state of every resting order this submission touched.
    pub makers: Vec<Order>,
    /// Everything that happened, ready to publish.
    pub events: Vec<DomainEvent>,
}

impl MatchOutcome {
    /// The taker followed by every maker whose state changed.
    pub fn changed_orders(&self) -> impl Iterator<Item = &Order> {
        std::iter::once(&self.order).chain(self.makers.iter())
    }
}

/// Result of cancelling one order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOutcome {
    pub order: Order,
    pub event: DomainEvent,
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
    sequence: u64,
    last_trade_id: u64,
}

impl MatchingEngine {
    pub fn new(spec: MarketSpec) -> Self {
        Self {
            spec,
            book: OrderBook::default(),
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

    pub fn snapshot(&self, depth: usize) -> BookSnapshot {
        let (bids, asks) = self.book.depth(depth);
        BookSnapshot {
            market: self.spec.id().clone(),
            sequence: self.sequence,
            bids,
            asks,
        }
    }

    /// Validates, matches and, if appropriate, rests an incoming order.
    ///
    /// Rejections (`Err`) leave the engine untouched and emit no events.
    /// Accepted orders always produce `OrderAccepted` first, then one
    /// `TradeExecuted` per fill, then `OrderCancelled` for any remainder that
    /// is not allowed to rest.
    pub fn submit(&mut self, new: NewOrder, now: Timestamp) -> Result<MatchOutcome, DomainError> {
        self.spec.validate(&new)?;
        if self.book.get(new.id).is_some() {
            return Err(DomainError::DuplicateOrderId(new.id));
        }
        if let OrderKind::Limit {
            price,
            post_only: true,
            ..
        } = new.kind
            && self.book.would_cross(new.side, price)
        {
            return Err(DomainError::PostOnlyWouldCross);
        }

        let mut taker = Order::accept(new, now);
        let mut events = vec![self.emit(now, EventPayload::OrderAccepted(taker.clone()))];
        let mut trades = Vec::new();
        let mut makers = Vec::new();
        let limit = taker.limit_price();

        if taker.kind().time_in_force() == Some(TimeInForce::FillOrKill)
            && self.book.fillable_quantity(&taker, limit)? < taker.quantity()
        {
            self.cancel_taker(&mut taker, CancelReason::FillOrKill, now, &mut events);
            return Ok(MatchOutcome {
                order: taker,
                trades,
                makers,
                events,
            });
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
                        events.push(self.cancelled_event(&stale, now));
                        makers.push(stale);
                        continue;
                    }
                }
            }

            let quantity = taker.remaining().min(maker_remaining);
            let maker = self.book.fill_best(maker_side, quantity, now)?;
            taker.fill(quantity, now)?;

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
            events.push(self.emit(now, EventPayload::TradeExecuted(trade.clone())));
            trades.push(trade);
            makers.push(maker);
        }

        if !taker.remaining().is_zero() {
            match remainder_policy(&taker, self_trade_stop) {
                None => {
                    taker.rest();
                    self.book.insert(taker.clone())?;
                }
                Some(reason) => self.cancel_taker(&mut taker, reason, now, &mut events),
            }
        }

        Ok(MatchOutcome {
            order: taker,
            trades,
            makers,
            events,
        })
    }

    /// Cancels a resting order on behalf of its owner.
    ///
    /// An order that exists but belongs to someone else is reported as not
    /// found, so the endpoint cannot be used to probe other accounts' ids.
    pub fn cancel(
        &mut self,
        order_id: OrderId,
        account: &AccountId,
        now: Timestamp,
    ) -> Result<CancelOutcome, DomainError> {
        let owned = self
            .book
            .get(order_id)
            .is_some_and(|order| order.account() == account);
        if !owned {
            return Err(DomainError::OrderNotFound(order_id));
        }
        let mut order = self
            .book
            .remove(order_id)
            .ok_or(DomainError::OrderNotFound(order_id))?;
        order.cancel(CancelReason::Requested, now);
        let event = self.cancelled_event(&order, now);
        Ok(CancelOutcome { order, event })
    }

    fn cancel_taker(
        &mut self,
        taker: &mut Order,
        reason: CancelReason,
        now: Timestamp,
        events: &mut Vec<DomainEvent>,
    ) {
        taker.cancel(reason, now);
        events.push(self.cancelled_event(taker, now));
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
        assert_eq!(outcome.makers[0].status(), OrderStatus::Filled);
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
        assert_eq!(outcome.makers[0].id(), own.order.id());
        assert_eq!(
            outcome.makers[0].cancel_reason(),
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
