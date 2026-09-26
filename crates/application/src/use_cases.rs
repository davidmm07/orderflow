//! The operations the system offers, one type per use case.

use std::sync::Arc;

use orderflow_domain::{
    AccountId, BookSnapshot, DomainError, MarketId, MarketSpec, Order, OrderId,
};

use crate::{
    commands::{CancelOrderCommand, IdempotencyKey, OrderReceipt, PlaceOrderCommand, Reservation},
    error::ApplicationError,
    market::MarketHandle,
    ports::{IdempotencyStore, OrderIdGenerator, OrderRepository},
    registry::MarketRegistry,
};

/// Result of `PlaceOrder::execute`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    pub receipt: OrderReceipt,
    /// True when the receipt was served from the idempotency store.
    pub replayed: bool,
}

/// Places an order, at most once per idempotency key.
#[derive(Clone)]
pub struct PlaceOrder {
    registry: Arc<MarketRegistry>,
    ids: Arc<dyn OrderIdGenerator>,
    idempotency: Arc<dyn IdempotencyStore>,
}

impl PlaceOrder {
    pub fn new(
        registry: Arc<MarketRegistry>,
        ids: Arc<dyn OrderIdGenerator>,
        idempotency: Arc<dyn IdempotencyStore>,
    ) -> Self {
        Self {
            registry,
            ids,
            idempotency,
        }
    }

    pub async fn execute(
        &self,
        command: PlaceOrderCommand,
        key: Option<IdempotencyKey>,
    ) -> Result<Placement, ApplicationError> {
        let market = self.registry.get(&command.market)?.clone();
        let this = self.clone();
        // Axum drops a handler future when the client disconnects. Running
        // the reserve, submit and complete steps on their own task makes the
        // sequence cancellation safe: it always finishes, so a key can never
        // be left claimed for an order that was actually placed.
        tokio::spawn(async move { this.run(&market, command, key).await })
            .await
            .map_err(|_| ApplicationError::Unavailable("order placement task failed"))?
    }

    async fn run(
        &self,
        market: &MarketHandle,
        command: PlaceOrderCommand,
        key: Option<IdempotencyKey>,
    ) -> Result<Placement, ApplicationError> {
        let Some(key) = key else {
            return self.submit(market, command).await.map(fresh);
        };

        match self.idempotency.reserve(&key, &command).await? {
            Reservation::Reserved => {}
            Reservation::Completed(receipt) => {
                return Ok(Placement {
                    receipt,
                    replayed: true,
                });
            }
            Reservation::InFlight => return Err(ApplicationError::IdempotencyKeyInFlight),
            Reservation::Mismatch => return Err(ApplicationError::IdempotencyKeyReused),
        }

        match self.submit(market, command).await {
            Ok(receipt) => {
                if let Err(error) = self.idempotency.complete(&key, &receipt).await {
                    // The order is live; failing the request now would make
                    // the client retry into a duplicate. Report success and
                    // flag the degraded store.
                    tracing::warn!(%error, "could not record idempotent result");
                }
                Ok(fresh(receipt))
            }
            Err(error) => {
                if let Err(release_error) = self.idempotency.release(&key).await {
                    tracing::warn!(error = %release_error, "could not release idempotency key");
                }
                Err(error)
            }
        }
    }

    async fn submit(
        &self,
        market: &MarketHandle,
        command: PlaceOrderCommand,
    ) -> Result<OrderReceipt, ApplicationError> {
        let order = command.into_new_order(self.ids.next_id());
        let outcome = market.place(order).await?;
        Ok(OrderReceipt {
            order: outcome.order,
            trades: outcome.trades,
        })
    }
}

fn fresh(receipt: OrderReceipt) -> Placement {
    Placement {
        receipt,
        replayed: false,
    }
}

/// Cancels a resting order on behalf of its owner.
pub struct CancelOrder {
    registry: Arc<MarketRegistry>,
    repository: Arc<dyn OrderRepository>,
}

impl CancelOrder {
    pub fn new(registry: Arc<MarketRegistry>, repository: Arc<dyn OrderRepository>) -> Self {
        Self {
            registry,
            repository,
        }
    }

    pub async fn execute(&self, command: CancelOrderCommand) -> Result<Order, ApplicationError> {
        let market = self.registry.get(&command.market)?;
        match market
            .cancel(command.order_id, command.account.clone())
            .await
        {
            Err(ApplicationError::Domain(DomainError::OrderNotFound(id))) => {
                // The engine only knows resting orders. The read model tells
                // "already filled or cancelled" apart from "never existed",
                // but only for the owner, so ids cannot be probed.
                match self.repository.find(id).await? {
                    Some(order)
                        if order.account() == &command.account
                            && order.market() == &command.market =>
                    {
                        Err(ApplicationError::OrderNotOpen {
                            id,
                            status: order.status(),
                        })
                    }
                    _ => Err(ApplicationError::OrderNotFound(id)),
                }
            }
            other => other,
        }
    }
}

/// Read side: markets, books and order status.
///
/// Queries never go through the matching path except for book snapshots,
/// which must reflect the engine exactly. Order lookups hit the read model,
/// so heavy polling cannot slow down matching.
pub struct OrderQueries {
    registry: Arc<MarketRegistry>,
    repository: Arc<dyn OrderRepository>,
}

impl OrderQueries {
    pub fn new(registry: Arc<MarketRegistry>, repository: Arc<dyn OrderRepository>) -> Self {
        Self {
            registry,
            repository,
        }
    }

    pub fn markets(&self) -> Vec<MarketSpec> {
        self.registry.specs().cloned().collect()
    }

    pub fn market(&self, id: &MarketId) -> Result<MarketSpec, ApplicationError> {
        self.registry.get(id).map(|handle| handle.spec().clone())
    }

    pub async fn order_book(
        &self,
        market: &MarketId,
        depth: usize,
    ) -> Result<BookSnapshot, ApplicationError> {
        self.registry.get(market)?.snapshot(depth).await
    }

    /// Returns an order only to its owner and only under its own market.
    pub async fn order(
        &self,
        account: &AccountId,
        market: &MarketId,
        id: OrderId,
    ) -> Result<Order, ApplicationError> {
        self.registry.get(market)?;
        match self.repository.find(id).await? {
            Some(order) if order.account() == account && order.market() == market => Ok(order),
            _ => Err(ApplicationError::OrderNotFound(id)),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.registry.all_running()
    }
}
