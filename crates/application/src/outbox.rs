//! Hand-off of domain events from the market actors to the event sink.

use std::sync::Arc;

use orderflow_domain::DomainEvent;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    error::ApplicationError,
    ports::{EventPublisher, Metrics},
};

/// Largest number of queued batches merged into one publish call.
const MAX_BATCHES_PER_PUBLISH: usize = 64;

/// Sending half of the outbox, cloned into every market actor.
///
/// Pattern: Outbox. Actors never talk to Kafka directly. They append their
/// events here and move on, so broker latency stays off the matching path.
/// The queue is bounded: if the sink falls behind, actors wait, their
/// request queues fill up, and the API starts shedding load instead of the
/// process running out of memory.
#[derive(Debug, Clone)]
pub struct Outbox {
    sender: mpsc::Sender<Vec<DomainEvent>>,
}

#[derive(Debug)]
pub struct OutboxReceiver(mpsc::Receiver<Vec<DomainEvent>>);

/// Creates a bounded outbox holding up to `capacity` event batches.
pub fn outbox(capacity: usize) -> (Outbox, OutboxReceiver) {
    let (sender, receiver) = mpsc::channel(capacity.max(1));
    (Outbox { sender }, OutboxReceiver(receiver))
}

impl Outbox {
    pub async fn send(&self, events: Vec<DomainEvent>) -> Result<(), ApplicationError> {
        if events.is_empty() {
            return Ok(());
        }
        self.sender
            .send(events)
            .await
            .map_err(|_| ApplicationError::Unavailable("event outbox is closed"))
    }
}

/// Drains the outbox into an `EventPublisher`.
///
/// The channel is FIFO and each actor sends its batches in sequence order,
/// so per-market ordering survives the hand-off even though markets
/// interleave.
pub struct EventDispatcher {
    receiver: OutboxReceiver,
    publisher: Arc<dyn EventPublisher>,
    metrics: Arc<dyn Metrics>,
}

impl EventDispatcher {
    pub fn new(
        receiver: OutboxReceiver,
        publisher: Arc<dyn EventPublisher>,
        metrics: Arc<dyn Metrics>,
    ) -> Self {
        Self {
            receiver,
            publisher,
            metrics,
        }
    }

    /// Runs until every `Outbox` handle is dropped and the queue is empty.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(mut self) {
        let mut batches = Vec::with_capacity(MAX_BATCHES_PER_PUBLISH);
        loop {
            // `recv_many` waits for at least one batch, then takes whatever
            // else is already queued, which amortizes broker round trips
            // under load without adding latency when idle.
            let received = self
                .receiver
                .0
                .recv_many(&mut batches, MAX_BATCHES_PER_PUBLISH)
                .await;
            if received == 0 {
                break;
            }
            let events: Vec<DomainEvent> = batches.drain(..).flatten().collect();
            match self.publisher.publish(&events).await {
                Ok(()) => self.metrics.events_published(events.len()),
                Err(error) => {
                    // Retries already happened inside the publisher. The
                    // lost batch is logged at error level with the sequence
                    // range needed to replay it from the engine journal.
                    self.metrics.events_dropped(events.len());
                    tracing::error!(
                        %error,
                        count = events.len(),
                        first_sequence = events.first().map(|e| e.sequence),
                        last_sequence = events.last().map(|e| e.sequence),
                        "dropping event batch after publish failed"
                    );
                }
            }
        }
        tracing::info!("event dispatcher drained and stopped");
    }
}
