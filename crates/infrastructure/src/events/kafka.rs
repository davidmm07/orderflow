//! Kafka adapter, compiled only with the `kafka` feature.

use std::time::Duration;

use async_trait::async_trait;
use orderflow_application::{EventPublisher, PublishError};
use orderflow_domain::DomainEvent;
use rdkafka::{
    ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
};

use super::wire::{EventRecord, SCHEMA_VERSION};

/// Publishes domain events to a Kafka topic as JSON `EventRecord`s.
///
/// Records are keyed by market id. Kafka only orders messages within a
/// partition, and the key pins every event of a market to one partition,
/// so consumers see each market's sequence in order.
///
/// The producer runs with `enable.idempotence=true` and `acks=all`: the
/// broker drops duplicates caused by internal producer retries and only
/// acknowledges once the write is replicated.
pub struct KafkaEventPublisher {
    producer: FutureProducer,
    topic: String,
}

impl KafkaEventPublisher {
    pub fn new(brokers: &str, topic: impl Into<String>) -> Result<Self, PublishError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "lz4")
            .set("linger.ms", "5")
            .set("delivery.timeout.ms", "30000")
            .create()
            .map_err(|error| PublishError::Permanent(error.to_string()))?;
        Ok(Self {
            producer,
            topic: topic.into(),
        })
    }

    /// Blocks until queued messages are delivered, used during shutdown.
    pub fn flush(&self, timeout: Duration) -> Result<(), PublishError> {
        self.producer.flush(timeout).map_err(classify)
    }
}

#[async_trait]
impl EventPublisher for KafkaEventPublisher {
    async fn publish(&self, events: &[DomainEvent]) -> Result<(), PublishError> {
        let schema_version = SCHEMA_VERSION.to_string();
        let mut deliveries = Vec::with_capacity(events.len());

        // Enqueue the whole batch first, then await the acknowledgements, so
        // the batch costs one pipelined round trip instead of one per event.
        for event in events {
            let record = EventRecord::from(event);
            let payload = serde_json::to_vec(&record)
                .map_err(|error| PublishError::Permanent(error.to_string()))?;
            let headers = OwnedHeaders::new()
                .insert(Header {
                    key: "event_type",
                    value: Some(record.body.event_type()),
                })
                .insert(Header {
                    key: "schema_version",
                    value: Some(schema_version.as_str()),
                })
                .insert(Header {
                    key: "content-type",
                    value: Some("application/json"),
                });
            let message = FutureRecord::to(&self.topic)
                .key(event.market.as_str())
                .payload(&payload)
                .headers(headers);
            let delivery = self
                .producer
                .send_result(message)
                .map_err(|(error, _)| classify(error))?;
            deliveries.push(delivery);
        }

        for delivery in deliveries {
            match delivery.await {
                Ok(Ok(_)) => {}
                Ok(Err((error, _))) => return Err(classify(error)),
                Err(_) => return Err(PublishError::Transient("delivery was cancelled".into())),
            }
        }
        Ok(())
    }
}

/// Splits Kafka errors into retryable and fatal ones.
fn classify(error: KafkaError) -> PublishError {
    let fatal = matches!(
        error.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::TopicAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed
                | RDKafkaErrorCode::UnknownTopicOrPartition
                | RDKafkaErrorCode::MessageSizeTooLarge
                | RDKafkaErrorCode::InvalidMessage
        )
    );
    if fatal {
        PublishError::Permanent(error.to_string())
    } else {
        PublishError::Transient(error.to_string())
    }
}
