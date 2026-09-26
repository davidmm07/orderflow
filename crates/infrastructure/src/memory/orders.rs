use std::{collections::HashMap, sync::RwLock};

use async_trait::async_trait;
use orderflow_application::{OrderRepository, RepositoryError};
use orderflow_domain::{Order, OrderId};

use crate::sync::{read, write};

/// Order read model held in a `HashMap`.
///
/// A `RwLock` fits the access pattern: many API readers polling order
/// status, one writer per market actor. No guard is ever held across an
/// `.await`.
#[derive(Debug, Default)]
pub struct InMemoryOrderRepository {
    orders: RwLock<HashMap<OrderId, Order>>,
}

#[async_trait]
impl OrderRepository for InMemoryOrderRepository {
    async fn save(&self, orders: &[Order]) -> Result<(), RepositoryError> {
        let mut map = write(&self.orders);
        for order in orders {
            map.insert(order.id(), order.clone());
        }
        Ok(())
    }

    async fn find(&self, id: OrderId) -> Result<Option<Order>, RepositoryError> {
        Ok(read(&self.orders).get(&id).cloned())
    }
}
