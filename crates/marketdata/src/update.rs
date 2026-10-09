//! A published index price.

use orderflow_domain::{MarketId, Timestamp};
use rust_decimal::Decimal;

use crate::{
    aggregate::{Component, PriceStatus, QuoteState},
    candle::Revision,
};

/// One index price as published to subscribers, the bus and the store.
///
/// Updates are ordered by `(epoch, sequence)`. A subscriber that receives
/// updates from several paths (a snapshot plus a live stream, or a Redis
/// reconnect) keeps whichever is newer and drops the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceUpdate {
    pub market: MarketId,
    /// Run of the publisher: its start time in milliseconds. A restart
    /// starts a higher epoch, so its sequence numbers never collide with
    /// the previous run's.
    pub epoch: u64,
    /// Counts from 1 within an epoch, per market.
    pub sequence: u64,
    pub price: Decimal,
    pub status: PriceStatus,
    /// Time of the newest quote behind the price.
    pub as_of: Timestamp,
    pub published_at: Timestamp,
    /// Distance between the highest and lowest quote used, in basis points.
    pub spread_bps: Decimal,
    /// Every source of the market and how its quote was treated.
    pub components: Vec<Component>,
}

impl PriceUpdate {
    pub fn revision(&self) -> Revision {
        Revision {
            epoch: self.epoch,
            sequence: self.sequence,
        }
    }

    pub fn is_newer_than(&self, other: &Self) -> bool {
        self.revision() > other.revision()
    }

    pub fn sources_used(&self) -> usize {
        self.components
            .iter()
            .filter(|component| component.state == QuoteState::Used)
            .count()
    }
}
