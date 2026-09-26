//! Stop orders waiting for their trigger price.

use std::collections::{BTreeMap, HashMap};

use crate::{
    error::DomainError,
    ids::OrderId,
    numeric::Price,
    order::{Order, OrderStatus, Side},
};

/// Whether the last trade price has reached a stop.
///
/// A buy stop fires when the price rises to or above its stop price; a sell
/// stop fires when the price falls to or below it.
pub(crate) fn stop_reached(side: Side, stop_price: Price, last_price: Price) -> bool {
    match side {
        Side::Buy => last_price >= stop_price,
        Side::Sell => last_price <= stop_price,
    }
}

/// Stop orders that have not fired yet, kept apart from the visible book.
///
/// Each side is a `BTreeMap` keyed by `(stop price, arrival sequence)`, so
/// the next stop to fire is always at one end of the map: the lowest buy
/// stop as prices rise, the highest sell stop as prices fall. Ties fire in
/// arrival order, which keeps triggering deterministic.
#[derive(Debug, Default)]
pub(crate) struct StopBook {
    buys: BTreeMap<(Price, u64), OrderId>,
    sells: BTreeMap<(Price, u64), OrderId>,
    orders: HashMap<OrderId, (Order, u64)>,
    next_arrival: u64,
}

impl StopBook {
    pub(crate) fn len(&self) -> usize {
        self.orders.len()
    }

    pub(crate) fn get(&self, id: OrderId) -> Option<&Order> {
        self.orders.get(&id).map(|(order, _)| order)
    }

    pub(crate) fn insert(&mut self, order: Order) -> Result<(), DomainError> {
        let stop_price = order.stop_price().ok_or(DomainError::InvariantViolation(
            "stop book needs a stop price",
        ))?;
        if order.status() != OrderStatus::Pending {
            return Err(DomainError::InvariantViolation(
                "only pending stops wait for a trigger",
            ));
        }
        self.next_arrival += 1;
        let key = (stop_price, self.next_arrival);
        match order.side() {
            Side::Buy => self.buys.insert(key, order.id()),
            Side::Sell => self.sells.insert(key, order.id()),
        };
        self.orders.insert(order.id(), (order, self.next_arrival));
        Ok(())
    }

    pub(crate) fn remove(&mut self, id: OrderId) -> Option<Order> {
        let (order, arrival) = self.orders.remove(&id)?;
        if let Some(stop_price) = order.stop_price() {
            let key = (stop_price, arrival);
            match order.side() {
                Side::Buy => self.buys.remove(&key),
                Side::Sell => self.sells.remove(&key),
            };
        }
        Some(order)
    }

    /// Removes and returns the next stop that `last_price` has reached, if
    /// any. Buy stops are checked before sell stops.
    pub(crate) fn pop_reached(&mut self, last_price: Price) -> Option<Order> {
        let buy = self
            .buys
            .first_key_value()
            .filter(|((stop, _), _)| stop_reached(Side::Buy, *stop, last_price))
            .map(|(_, id)| *id);
        let sell = self
            .sells
            .last_key_value()
            .filter(|((stop, _), _)| stop_reached(Side::Sell, *stop, last_price))
            .map(|(_, id)| *id);
        self.remove(buy.or(sell)?)
    }

    /// Checks every structural invariant. Used by the property tests.
    #[cfg(test)]
    pub(crate) fn assert_consistent(&self, last_price: Option<Price>) {
        assert_eq!(self.buys.len() + self.sells.len(), self.orders.len());
        for (side, index) in [(Side::Buy, &self.buys), (Side::Sell, &self.sells)] {
            for ((stop, arrival), id) in index {
                let (order, stored_arrival) =
                    self.orders.get(id).expect("indexed stop must be stored");
                assert_eq!(order.side(), side);
                assert_eq!(order.stop_price(), Some(*stop));
                assert_eq!(arrival, stored_arrival);
                assert_eq!(order.status(), OrderStatus::Pending);
                if let Some(last) = last_price {
                    assert!(
                        !stop_reached(side, *stop, last),
                        "stop at {stop} should have fired at {last}"
                    );
                }
            }
        }
    }
}
