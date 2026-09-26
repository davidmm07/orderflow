use async_trait::async_trait;
use orderflow_application::{EventPublisher, PublishError};
use orderflow_domain::DomainEvent;
use tokio::sync::broadcast;

/// Fans events out to in-process subscribers.
///
/// Pattern: Observer (publish and subscribe). Subscribers call `subscribe`
/// and receive every event published afterwards. The channel is bounded. A
/// subscriber that falls behind gets `RecvError::Lagged` and skips ahead,
/// and the publisher never waits for it; a live feed prefers fresh data.
#[derive(Debug, Clone)]
pub struct BroadcastPublisher {
    sender: broadcast::Sender<DomainEvent>,
}

impl BroadcastPublisher {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DomainEvent> {
        self.sender.subscribe()
    }
}

#[async_trait]
impl EventPublisher for BroadcastPublisher {
    async fn publish(&self, events: &[DomainEvent]) -> Result<(), PublishError> {
        for event in events {
            // An error only means nobody is subscribed right now.
            let _ = self.sender.send(event.clone());
        }
        Ok(())
    }
}
