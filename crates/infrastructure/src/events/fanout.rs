use std::sync::Arc;

use async_trait::async_trait;
use orderflow_application::{EventPublisher, PublishError};
use orderflow_domain::DomainEvent;

/// Publishes the same batch to several sinks.
///
/// Pattern: Composite. A group of publishers is itself a publisher, so the
/// dispatcher does not know or care how many sinks exist. Every sink is
/// attempted even if an earlier one fails, and the first error is reported.
pub struct FanoutPublisher {
    sinks: Vec<Arc<dyn EventPublisher>>,
}

impl FanoutPublisher {
    pub fn new(sinks: Vec<Arc<dyn EventPublisher>>) -> Self {
        Self { sinks }
    }
}

#[async_trait]
impl EventPublisher for FanoutPublisher {
    async fn publish(&self, events: &[DomainEvent]) -> Result<(), PublishError> {
        let mut first_error = None;
        for sink in &self.sinks {
            if let Err(error) = sink.publish(events).await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
