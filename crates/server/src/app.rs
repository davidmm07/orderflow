//! Composition root: the one place that knows every concrete type.

use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use orderflow_api::{ApiConfig, AppState, Authenticator, Credential, RateLimiter};
use orderflow_application::{
    CancelOrder, EventDispatcher, EventPublisher, MarketDeps, MarketRegistry, OrderQueries,
    OrderRepository, PlaceOrder, outbox, spawn_market,
};
use orderflow_domain::MarketSpec;
#[cfg(feature = "kafka")]
use orderflow_infrastructure::{
    FanoutPublisher, KafkaEventPublisher, RetryPolicy, RetryingPublisher,
};
use orderflow_infrastructure::{
    InMemoryIdempotencyStore, InMemoryOrderRepository, LoggingPublisher, MonotonicClock,
    PrometheusMetrics, UuidV7Generator,
};
use tokio::task::JoinHandle;

use crate::config::{EventSink, Settings};

/// A wired application plus the background tasks it started.
pub struct Application {
    pub router: Router,
    pub engines: Vec<JoinHandle<()>>,
    pub dispatcher: JoinHandle<()>,
}

/// Builds the object graph from settings.
///
/// Pattern: Composition Root (with Factory functions). Dependency injection
/// happens here and nowhere else: inner layers receive trait objects and
/// never construct adapters themselves. Replacing the in-memory repository
/// with a database is a change to this function only.
pub fn build(settings: &Settings, markets: Vec<MarketSpec>) -> anyhow::Result<Application> {
    let metrics = Arc::new(PrometheusMetrics::new());
    let repository: Arc<dyn OrderRepository> = Arc::new(InMemoryOrderRepository::default());

    let (outbox, outbox_receiver) = outbox(settings.outbox_capacity);
    let dispatcher = EventDispatcher::new(
        outbox_receiver,
        event_publisher(&settings.event_sink)?,
        metrics.clone(),
    )
    .spawn();

    let deps = MarketDeps {
        repository: repository.clone(),
        clock: Arc::new(MonotonicClock::default()),
        metrics: metrics.clone(),
        outbox,
    };
    let (handles, engines): (Vec<_>, Vec<_>) = markets
        .into_iter()
        .map(|spec| spawn_market(spec, deps.clone(), settings.engine_queue_capacity))
        .unzip();
    // From here on only the market actors hold the outbox, so it closes,
    // and the dispatcher exits, once the last actor has drained.
    drop(deps);

    let registry = Arc::new(MarketRegistry::new(handles));
    let credentials = settings
        .credentials
        .iter()
        .map(|c| {
            Credential::new(c.account.clone(), c.secret.expose().as_bytes())
                .map(|credential| (c.key_id.clone(), credential))
                .with_context(|| format!("credential {}", c.key_id))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let state = AppState {
        place_order: Arc::new(PlaceOrder::new(
            registry.clone(),
            Arc::new(UuidV7Generator),
            Arc::new(InMemoryIdempotencyStore::new(
                settings.idempotency_ttl,
                settings.idempotency_max_entries,
            )),
        )),
        cancel_order: Arc::new(CancelOrder::new(registry.clone(), repository.clone())),
        queries: Arc::new(OrderQueries::new(registry, repository)),
        authenticator: Arc::new(Authenticator::new(
            credentials,
            settings.signature_tolerance,
            settings.max_body_bytes,
        )),
        rate_limiter: Arc::new(RateLimiter::new(settings.rate_limit)),
        metrics: Arc::new(move || metrics.render()),
    };
    let router = orderflow_api::router(
        state,
        &ApiConfig {
            request_timeout: settings.request_timeout,
            max_body_bytes: settings.max_body_bytes,
        },
    );

    Ok(Application {
        router,
        engines,
        dispatcher,
    })
}

/// Factory for the configured event sink.
fn event_publisher(sink: &EventSink) -> anyhow::Result<Arc<dyn EventPublisher>> {
    match sink {
        EventSink::Log => Ok(Arc::new(LoggingPublisher)),
        #[cfg(feature = "kafka")]
        EventSink::Kafka { brokers, topic } => {
            let kafka = KafkaEventPublisher::new(brokers, topic.clone())
                .context("creating the kafka producer")?;
            // Retries wrap Kafka only, so a slow broker never replays the
            // log sink. The log copy helps when tracing an order by hand.
            Ok(Arc::new(FanoutPublisher::new(vec![
                Arc::new(RetryingPublisher::new(kafka, RetryPolicy::default())),
                Arc::new(LoggingPublisher),
            ])))
        }
        #[cfg(not(feature = "kafka"))]
        EventSink::Kafka { .. } => anyhow::bail!("this binary was built without the kafka feature"),
    }
}
