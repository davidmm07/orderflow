//! `EventPublisher` adapters and the event wire format.

mod broadcast;
mod fanout;
#[cfg(feature = "kafka")]
mod kafka;
mod logging;
mod retry;
mod wire;

pub use broadcast::BroadcastPublisher;
pub use fanout::FanoutPublisher;
#[cfg(feature = "kafka")]
pub use kafka::KafkaEventPublisher;
pub use logging::LoggingPublisher;
pub use retry::{RetryPolicy, RetryingPublisher};
pub use wire::EventRecord;
