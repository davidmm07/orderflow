use async_trait::async_trait;
use orderflow_application::{EventPublisher, PublishError};
use orderflow_domain::DomainEvent;

/// Writes every event to the structured log.
///
/// The default sink for local runs: no broker needed, and `RUST_LOG`
/// controls the volume.
#[derive(Debug, Default, Clone, Copy)]
pub struct LoggingPublisher;

#[async_trait]
impl EventPublisher for LoggingPublisher {
    async fn publish(&self, events: &[DomainEvent]) -> Result<(), PublishError> {
        for event in events {
            tracing::info!(
                target: "orderflow::events",
                market = %event.market,
                sequence = event.sequence,
                event_type = event.payload.kind(),
                "domain event"
            );
        }
        Ok(())
    }
}
