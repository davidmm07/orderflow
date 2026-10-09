//! The in-process fanout point for live prices.

use std::{collections::HashMap, sync::Arc};

use axum::extract::ws::Utf8Bytes;
use orderflow_domain::MarketId;
use orderflow_marketdata::{PriceUpdate, ports::UpdateSink};
use tokio::sync::watch;

use crate::dto::{PriceView, ServerMessage};

/// A price update together with its WebSocket frame, encoded once.
#[derive(Debug)]
pub struct PriceFrame {
    pub update: Arc<PriceUpdate>,
    /// The `{"type":"price",...}` message, shared by every subscriber.
    pub text: Utf8Bytes,
}

pub type PriceReceiver = watch::Receiver<Option<Arc<PriceFrame>>>;

/// Latest price of every market, with change notification.
///
/// Pattern: Observer, built on one `watch` channel per market. A watch
/// channel holds a single value: a subscriber that falls behind skips
/// straight to the newest price instead of working through a backlog, so a
/// slow client costs no memory and never delays anyone else. That is the
/// right semantics for prices (only the current one matters) and is what
/// makes fanout to thousands of connections cheap: a publish is one store
/// and one wake-up, whatever the number of subscribers.
///
/// Each update is serialized to JSON once, here, and every connection
/// sends the same reference counted bytes.
pub struct PriceHub {
    channels: HashMap<MarketId, watch::Sender<Option<Arc<PriceFrame>>>>,
}

impl PriceHub {
    pub fn new(markets: impl IntoIterator<Item = MarketId>) -> Self {
        let channels = markets
            .into_iter()
            .map(|market| (market, watch::Sender::new(None)))
            .collect();
        Self { channels }
    }

    /// Stores `update` if it is newer than the current price of its market.
    ///
    /// Updates can arrive twice or out of order: an edge node subscribes to
    /// the live stream before it reads the Redis snapshot, so the two
    /// overlap, and a reconnect replays the snapshot. The
    /// `(epoch, sequence)` check makes every path safe.
    pub fn publish(&self, update: Arc<PriceUpdate>) -> bool {
        let Some(channel) = self.channels.get(&update.market) else {
            return false;
        };
        let stale = channel
            .borrow()
            .as_ref()
            .is_some_and(|current| !update.is_newer_than(&current.update));
        if stale {
            return false;
        }
        let text = Utf8Bytes::from(ServerMessage::Price(PriceView::from(&*update)).to_json());
        let frame = Arc::new(PriceFrame { update, text });
        channel.send_if_modified(|current| {
            // Checked again under the lock, in case of a concurrent publish.
            if current
                .as_ref()
                .is_some_and(|existing| !frame.update.is_newer_than(&existing.update))
            {
                return false;
            }
            *current = Some(frame);
            true
        })
    }

    pub fn subscribe(&self, market: &MarketId) -> Option<PriceReceiver> {
        self.channels.get(market).map(watch::Sender::subscribe)
    }

    pub fn latest(&self, market: &MarketId) -> Option<Arc<PriceUpdate>> {
        self.channels
            .get(market)?
            .borrow()
            .as_ref()
            .map(|frame| frame.update.clone())
    }

    /// Latest price of every market that has one, in market order.
    pub fn snapshot(&self) -> Vec<Arc<PriceUpdate>> {
        let mut updates: Vec<Arc<PriceUpdate>> = self
            .channels
            .values()
            .filter_map(|channel| channel.borrow().as_ref().map(|frame| frame.update.clone()))
            .collect();
        updates.sort_by(|a, b| a.market.cmp(&b.market));
        updates
    }

    pub fn markets(&self) -> Vec<MarketId> {
        let mut markets: Vec<MarketId> = self.channels.keys().cloned().collect();
        markets.sort();
        markets
    }

    pub fn contains(&self, market: &MarketId) -> bool {
        self.channels.contains_key(market)
    }
}

impl UpdateSink for PriceHub {
    fn accept(&self, update: &Arc<PriceUpdate>) {
        self.publish(update.clone());
    }
}

#[cfg(test)]
mod tests {
    use orderflow_domain::Timestamp;
    use orderflow_marketdata::PriceStatus;
    use rust_decimal::Decimal;

    use super::*;

    fn update(epoch: u64, sequence: u64) -> Arc<PriceUpdate> {
        Arc::new(PriceUpdate {
            market: MarketId::parse("BTC-USD").unwrap(),
            epoch,
            sequence,
            price: Decimal::from(sequence),
            status: PriceStatus::Live,
            as_of: Timestamp::default(),
            published_at: Timestamp::default(),
            spread_bps: Decimal::ZERO,
            components: Vec::new(),
        })
    }

    #[test]
    fn only_newer_updates_replace_the_price() {
        let market = MarketId::parse("BTC-USD").unwrap();
        let hub = PriceHub::new([market.clone()]);
        assert!(hub.publish(update(1, 5)));
        assert!(!hub.publish(update(1, 4)), "older sequence");
        assert!(!hub.publish(update(1, 5)), "duplicate");
        assert!(
            hub.publish(update(2, 1)),
            "a later epoch wins with any sequence"
        );
        assert_eq!(hub.latest(&market).unwrap().epoch, 2);
        assert!(!hub.publish(Arc::new(PriceUpdate {
            market: MarketId::parse("ETH-USD").unwrap(),
            ..(*update(3, 1)).clone()
        })));
    }

    #[tokio::test]
    async fn a_slow_subscriber_sees_only_the_newest_price() {
        let market = MarketId::parse("BTC-USD").unwrap();
        let hub = PriceHub::new([market.clone()]);
        let mut receiver = hub.subscribe(&market).unwrap();
        for sequence in 1..=100 {
            hub.publish(update(1, sequence));
        }
        receiver.changed().await.unwrap();
        let frame = receiver.borrow_and_update().clone().unwrap();
        assert_eq!(frame.update.sequence, 100);
        assert!(frame.text.as_str().contains(r#""sequence":100"#));
        assert!(!receiver.has_changed().unwrap());
    }
}
