//! Order id generation.

use orderflow_application::OrderIdGenerator;
use orderflow_domain::OrderId;
use uuid::Uuid;

/// Generates UUIDv7 order ids.
///
/// Version 7 ids start with a millisecond timestamp, so they sort roughly by
/// creation time. That keeps B-tree indexes append-mostly once orders are
/// persisted, unlike random v4 ids that scatter writes across the index.
#[derive(Debug, Default, Clone, Copy)]
pub struct UuidV7Generator;

impl OrderIdGenerator for UuidV7Generator {
    fn next_id(&self) -> OrderId {
        OrderId::from_uuid(Uuid::now_v7())
    }
}
