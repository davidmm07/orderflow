use std::{collections::HashMap, sync::Mutex, time::Duration};

use async_trait::async_trait;
use orderflow_application::{
    IdempotencyKey, IdempotencyStore, OrderReceipt, PlaceOrderCommand, RepositoryError, Reservation,
};
use tokio::time::Instant;

use crate::sync::lock;

struct Entry {
    command: PlaceOrderCommand,
    receipt: Option<OrderReceipt>,
    expires_at: Instant,
}

/// Idempotency records with a time to live and a hard size cap.
///
/// The cap matters for security: keys are client chosen, and without a
/// bound a single account could exhaust memory by sending unique keys.
/// Expired entries are purged lazily when the cap is reached, which keeps
/// the common path O(1).
pub struct InMemoryIdempotencyStore {
    ttl: Duration,
    max_entries: usize,
    entries: Mutex<HashMap<IdempotencyKey, Entry>>,
}

impl InMemoryIdempotencyStore {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            max_entries: max_entries.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl IdempotencyStore for InMemoryIdempotencyStore {
    async fn reserve(
        &self,
        key: &IdempotencyKey,
        command: &PlaceOrderCommand,
    ) -> Result<Reservation, RepositoryError> {
        let now = Instant::now();
        let mut entries = lock(&self.entries);

        if let Some(entry) = entries.get(key)
            && entry.expires_at > now
        {
            return Ok(if entry.command != *command {
                Reservation::Mismatch
            } else if let Some(receipt) = &entry.receipt {
                Reservation::Completed(receipt.clone())
            } else {
                Reservation::InFlight
            });
        }

        if entries.len() >= self.max_entries {
            entries.retain(|_, entry| entry.expires_at > now);
            if entries.len() >= self.max_entries {
                return Err(RepositoryError::CapacityExhausted);
            }
        }

        entries.insert(
            key.clone(),
            Entry {
                command: command.clone(),
                receipt: None,
                expires_at: now + self.ttl,
            },
        );
        Ok(Reservation::Reserved)
    }

    async fn complete(
        &self,
        key: &IdempotencyKey,
        receipt: &OrderReceipt,
    ) -> Result<(), RepositoryError> {
        if let Some(entry) = lock(&self.entries).get_mut(key) {
            entry.receipt = Some(receipt.clone());
            entry.expires_at = Instant::now() + self.ttl;
        }
        Ok(())
    }

    async fn release(&self, key: &IdempotencyKey) -> Result<(), RepositoryError> {
        lock(&self.entries).remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use orderflow_domain::{
        AccountId, Decimal, MarketId, OrderKind, Quantity, SelfTradePrevention, Side,
    };

    use super::*;

    fn command(quantity: u32) -> PlaceOrderCommand {
        PlaceOrderCommand {
            account: AccountId::parse("alice").unwrap(),
            market: MarketId::parse("BTC-USD").unwrap(),
            side: Side::Buy,
            kind: OrderKind::Market,
            quantity: Quantity::positive(Decimal::from(quantity)).unwrap(),
            stop_price: None,
            client_order_id: None,
            self_trade_prevention: SelfTradePrevention::CancelNewest,
        }
    }

    fn key(raw: &str) -> IdempotencyKey {
        IdempotencyKey::new(AccountId::parse("alice").unwrap(), raw).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn keys_expire_after_the_ttl() {
        let store = InMemoryIdempotencyStore::new(Duration::from_secs(60), 10);
        assert_eq!(
            store.reserve(&key("a"), &command(1)).await,
            Ok(Reservation::Reserved)
        );
        assert_eq!(
            store.reserve(&key("a"), &command(1)).await,
            Ok(Reservation::InFlight)
        );
        assert_eq!(
            store.reserve(&key("a"), &command(2)).await,
            Ok(Reservation::Mismatch)
        );

        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(
            store.reserve(&key("a"), &command(2)).await,
            Ok(Reservation::Reserved)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_is_enforced_after_purging_expired_keys() {
        let store = InMemoryIdempotencyStore::new(Duration::from_secs(60), 2);
        store.reserve(&key("a"), &command(1)).await.unwrap();
        store.reserve(&key("b"), &command(1)).await.unwrap();
        assert_eq!(
            store.reserve(&key("c"), &command(1)).await,
            Err(RepositoryError::CapacityExhausted)
        );

        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(
            store.reserve(&key("c"), &command(1)).await,
            Ok(Reservation::Reserved)
        );
    }

    #[tokio::test]
    async fn released_keys_can_be_claimed_again() {
        let store = InMemoryIdempotencyStore::new(Duration::from_secs(60), 10);
        store.reserve(&key("a"), &command(1)).await.unwrap();
        store.release(&key("a")).await.unwrap();
        assert_eq!(
            store.reserve(&key("a"), &command(2)).await,
            Ok(Reservation::Reserved)
        );
    }
}
