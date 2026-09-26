//! Resting liquidity for one market.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::{
    error::DomainError,
    ids::OrderId,
    market::MarketId,
    numeric::{Price, Quantity},
    order::{Order, SelfTradePrevention, Side},
    time::Timestamp,
};

/// All resting orders at one price, oldest first.
#[derive(Debug, Default)]
struct PriceLevel {
    queue: VecDeque<OrderId>,
    total: Quantity,
}

/// Aggregated view of one price level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelView {
    pub price: Price,
    pub quantity: Quantity,
    pub order_count: usize,
}

/// Point-in-time view of the top of the book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    pub market: MarketId,
    /// Sequence of the last event applied before the snapshot was taken.
    pub sequence: u64,
    /// Price of the most recent trade, `None` before the first one.
    pub last_price: Option<Price>,
    /// Best (highest) bid first.
    pub bids: Vec<LevelView>,
    /// Best (lowest) ask first.
    pub asks: Vec<LevelView>,
}

/// Price-time priority order book.
///
/// Levels live in a `BTreeMap` so the best price is an O(log n) lookup and
/// depth snapshots come out sorted. Orders live in a `HashMap` keyed by id so
/// cancels find their order in O(1). Each level keeps a running total, which
/// makes depth snapshots independent of the number of orders per level.
#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<Price, PriceLevel>,
    asks: BTreeMap<Price, PriceLevel>,
    orders: HashMap<OrderId, Order>,
}

impl OrderBook {
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn get(&self, id: OrderId) -> Option<&Order> {
        self.orders.get(&id)
    }

    /// Best resting price on `side`: highest bid or lowest ask.
    pub fn best_price(&self, side: Side) -> Option<Price> {
        match side {
            Side::Buy => self.bids.keys().next_back().copied(),
            Side::Sell => self.asks.keys().next().copied(),
        }
    }

    /// Aggregated levels, best first, at most `depth` per side.
    pub fn depth(&self, depth: usize) -> (Vec<LevelView>, Vec<LevelView>) {
        let view = |(price, level): (&Price, &PriceLevel)| LevelView {
            price: *price,
            quantity: level.total,
            order_count: level.queue.len(),
        };
        let bids = self.bids.iter().rev().take(depth).map(view).collect();
        let asks = self.asks.iter().take(depth).map(view).collect();
        (bids, asks)
    }

    /// Oldest order at the best price on `side`.
    pub(crate) fn best_order(&self, side: Side) -> Option<&Order> {
        let price = self.best_price(side)?;
        let id = self.levels(side).get(&price)?.queue.front()?;
        self.orders.get(id)
    }

    /// Whether an incoming order at `limit` would trade immediately.
    pub(crate) fn would_cross(&self, taker_side: Side, limit: Price) -> bool {
        self.best_price(taker_side.opposite())
            .is_some_and(|best| crosses(taker_side, Some(limit), best))
    }

    pub(crate) fn insert(&mut self, order: Order) -> Result<(), DomainError> {
        let price = order
            .limit_price()
            .ok_or(DomainError::InvariantViolation("market orders cannot rest"))?;
        if order.status().is_terminal() {
            return Err(DomainError::InvariantViolation(
                "terminal orders cannot rest",
            ));
        }
        let level = match order.side() {
            Side::Buy => self.bids.entry(price).or_default(),
            Side::Sell => self.asks.entry(price).or_default(),
        };
        level.total = level.total.checked_add(order.remaining())?;
        level.queue.push_back(order.id());
        self.orders.insert(order.id(), order);
        Ok(())
    }

    /// Fills the oldest order at the best price on `side` and returns its new
    /// state. A fully filled order leaves the book, and so does its level
    /// once empty.
    pub(crate) fn fill_best(
        &mut self,
        side: Side,
        quantity: Quantity,
        now: Timestamp,
    ) -> Result<Order, DomainError> {
        let price = self
            .best_price(side)
            .ok_or(DomainError::InvariantViolation("fill on an empty side"))?;
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = levels
            .get_mut(&price)
            .ok_or(DomainError::InvariantViolation("best level missing"))?;
        let id = *level
            .queue
            .front()
            .ok_or(DomainError::InvariantViolation("empty price level"))?;
        let order = self
            .orders
            .get_mut(&id)
            .ok_or(DomainError::InvariantViolation("queued order missing"))?;

        order.fill(quantity, now)?;
        level.total = level.total.saturating_sub(quantity);
        let snapshot = order.clone();

        if snapshot.remaining().is_zero() {
            level.queue.pop_front();
            self.orders.remove(&id);
            if level.queue.is_empty() {
                levels.remove(&price);
            }
        }
        Ok(snapshot)
    }

    /// Takes an order off the book.
    ///
    /// Removing from the middle of a level is O(n) in that level's size. Real
    /// books stay shallow per level and cancels are rarer than matches, so a
    /// `VecDeque` beats an intrusive linked list on cache behavior here.
    pub(crate) fn remove(&mut self, id: OrderId) -> Option<Order> {
        let order = self.orders.remove(&id)?;
        let levels = match order.side() {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if let Some(price) = order.limit_price()
            && let Some(level) = levels.get_mut(&price)
        {
            level.queue.retain(|queued| *queued != id);
            level.total = level.total.saturating_sub(order.remaining());
            if level.queue.is_empty() {
                levels.remove(&price);
            }
        }
        Some(order)
    }

    /// Quantity an incoming order could fill right now, honoring its limit
    /// price and self-trade policy. Used to decide fill-or-kill orders before
    /// touching the book, so a rejected order leaves no trace.
    pub(crate) fn fillable_quantity(
        &self,
        taker: &Order,
        limit: Option<Price>,
    ) -> Result<Quantity, DomainError> {
        let maker_side = taker.side().opposite();
        let levels: Box<dyn Iterator<Item = (&Price, &PriceLevel)>> = match maker_side {
            Side::Buy => Box::new(self.bids.iter().rev()),
            Side::Sell => Box::new(self.asks.iter()),
        };

        let mut total = Quantity::ZERO;
        for (price, level) in levels {
            if !crosses(taker.side(), limit, *price) {
                break;
            }
            for maker in level.queue.iter().filter_map(|id| self.orders.get(id)) {
                if maker.account() == taker.account() {
                    match taker.self_trade_prevention() {
                        SelfTradePrevention::CancelNewest => return Ok(total),
                        SelfTradePrevention::CancelOldest => continue,
                    }
                }
                total = total.checked_add(maker.remaining())?;
                if total >= taker.quantity() {
                    return Ok(total);
                }
            }
        }
        Ok(total)
    }

    fn levels(&self, side: Side) -> &BTreeMap<Price, PriceLevel> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    /// Checks every structural invariant. Used by the property tests.
    #[cfg(test)]
    pub(crate) fn assert_consistent(&self) {
        let mut queued = 0;
        for (side, levels) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            for (price, level) in levels {
                assert!(!level.queue.is_empty(), "empty level left at {price}");
                let mut sum = Quantity::ZERO;
                for id in &level.queue {
                    let order = self.orders.get(id).expect("queued id must be stored");
                    assert_eq!(order.side(), side);
                    assert_eq!(order.limit_price(), Some(*price));
                    assert!(!order.status().is_terminal());
                    assert!(!order.remaining().is_zero());
                    sum = sum.checked_add(order.remaining()).unwrap();
                }
                assert_eq!(sum, level.total, "level total drifted at {price}");
                queued += level.queue.len();
            }
        }
        assert_eq!(queued, self.orders.len(), "orders missing from queues");
        if let (Some(bid), Some(ask)) = (self.best_price(Side::Buy), self.best_price(Side::Sell)) {
            assert!(bid < ask, "book is crossed: bid {bid} >= ask {ask}");
        }
    }
}

/// Whether a taker with an optional limit can trade against `maker_price`.
/// Market orders (`None`) cross any price.
pub(crate) fn crosses(taker_side: Side, limit: Option<Price>, maker_price: Price) -> bool {
    match (taker_side, limit) {
        (_, None) => true,
        (Side::Buy, Some(limit)) => maker_price <= limit,
        (Side::Sell, Some(limit)) => maker_price >= limit,
    }
}
