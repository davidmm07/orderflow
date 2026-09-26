#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Use case tests against hand written fakes, with no infrastructure crate.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use orderflow_application::{
    ApplicationError, CancelOrder, CancelOrderCommand, Clock, EventDispatcher, EventPublisher,
    IdempotencyKey, IdempotencyStore, MarketDeps, MarketRegistry, NoopMetrics, OrderIdGenerator,
    OrderQueries, OrderReceipt, OrderRepository, PlaceOrder, PlaceOrderCommand, PublishError,
    RepositoryError, Reservation, outbox, spawn_market,
};
use orderflow_domain::{
    AccountId, Decimal, DomainError, DomainEvent, MarketId, MarketSpec, Order, OrderId, OrderKind,
    OrderStatus, Price, Quantity, SelfTradePrevention, Side, TimeInForce, Timestamp,
};

use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Default)]
struct FakeRepository(Mutex<HashMap<OrderId, Order>>);

#[async_trait]
impl OrderRepository for FakeRepository {
    async fn save(&self, orders: &[Order]) -> Result<(), RepositoryError> {
        let mut map = self.0.lock().unwrap();
        for order in orders {
            map.insert(order.id(), order.clone());
        }
        Ok(())
    }

    async fn find(&self, id: OrderId) -> Result<Option<Order>, RepositoryError> {
        Ok(self.0.lock().unwrap().get(&id).cloned())
    }
}

#[derive(Default)]
struct RecordingPublisher(Mutex<Vec<DomainEvent>>);

#[async_trait]
impl EventPublisher for RecordingPublisher {
    async fn publish(&self, events: &[DomainEvent]) -> Result<(), PublishError> {
        self.0.lock().unwrap().extend_from_slice(events);
        Ok(())
    }
}

#[derive(Default)]
struct FakeIdempotency(Mutex<HashMap<IdempotencyKey, (PlaceOrderCommand, Option<OrderReceipt>)>>);

#[async_trait]
impl IdempotencyStore for FakeIdempotency {
    async fn reserve(
        &self,
        key: &IdempotencyKey,
        command: &PlaceOrderCommand,
    ) -> Result<Reservation, RepositoryError> {
        let mut map = self.0.lock().unwrap();
        Ok(match map.get(key) {
            None => {
                map.insert(key.clone(), (command.clone(), None));
                Reservation::Reserved
            }
            Some((stored, _)) if stored != command => Reservation::Mismatch,
            Some((_, None)) => Reservation::InFlight,
            Some((_, Some(receipt))) => Reservation::Completed(receipt.clone()),
        })
    }

    async fn complete(
        &self,
        key: &IdempotencyKey,
        receipt: &OrderReceipt,
    ) -> Result<(), RepositoryError> {
        if let Some(entry) = self.0.lock().unwrap().get_mut(key) {
            entry.1 = Some(receipt.clone());
        }
        Ok(())
    }

    async fn release(&self, key: &IdempotencyKey) -> Result<(), RepositoryError> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

struct TickingClock(AtomicU64);

impl Clock for TickingClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_unix_nanos(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

struct SequentialIds(AtomicU64);

impl OrderIdGenerator for SequentialIds {
    fn next_id(&self) -> OrderId {
        let n = self.0.fetch_add(1, Ordering::Relaxed);
        OrderId::from_uuid(Uuid::from_u128(u128::from(n)))
    }
}

struct World {
    place: PlaceOrder,
    cancel: CancelOrder,
    queries: OrderQueries,
    publisher: Arc<RecordingPublisher>,
    tasks: Vec<JoinHandle<()>>,
}

impl World {
    fn start() -> Self {
        let repository = Arc::new(FakeRepository::default());
        let publisher = Arc::new(RecordingPublisher::default());
        let (outbox, receiver) = outbox(16);
        let dispatcher =
            EventDispatcher::new(receiver, publisher.clone(), Arc::new(NoopMetrics)).spawn();
        let spec = MarketSpec::builder(market())
            .tick_size(Decimal::from(1))
            .lot_size(Decimal::from(1))
            .max_quantity(Decimal::from(100))
            .build()
            .unwrap();
        let deps = MarketDeps {
            repository: repository.clone(),
            clock: Arc::new(TickingClock(AtomicU64::new(1))),
            metrics: Arc::new(NoopMetrics),
            outbox,
        };
        let (handle, engine) = spawn_market(spec, deps, 64);
        let registry = Arc::new(MarketRegistry::new([handle]));
        Self {
            place: PlaceOrder::new(
                registry.clone(),
                Arc::new(SequentialIds(AtomicU64::new(1))),
                Arc::new(FakeIdempotency::default()),
            ),
            cancel: CancelOrder::new(registry.clone(), repository.clone()),
            queries: OrderQueries::new(registry, repository),
            publisher,
            tasks: vec![engine, dispatcher],
        }
    }

    /// Drops every handle and waits for the actors to drain.
    async fn shutdown(self) -> Vec<DomainEvent> {
        let publisher = self.publisher.clone();
        let tasks = self.tasks;
        drop((self.place, self.cancel, self.queries));
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("task did not stop")
                .unwrap();
        }
        publisher.0.lock().unwrap().clone()
    }
}

fn market() -> MarketId {
    MarketId::parse("BTC-USD").unwrap()
}

fn account(name: &str) -> AccountId {
    AccountId::parse(name).unwrap()
}

fn limit(account_name: &str, side: Side, price: u32, quantity: u32) -> PlaceOrderCommand {
    PlaceOrderCommand {
        account: account(account_name),
        market: market(),
        side,
        kind: OrderKind::limit(
            Price::new(price.into()).unwrap(),
            TimeInForce::GoodTilCancelled,
            false,
        )
        .unwrap(),
        quantity: Quantity::positive(quantity.into()).unwrap(),
        stop_price: None,
        client_order_id: None,
        self_trade_prevention: SelfTradePrevention::CancelNewest,
    }
}

#[tokio::test]
async fn placing_crossing_orders_trades_and_publishes_events_in_order() {
    let world = World::start();
    world
        .place
        .execute(limit("alice", Side::Sell, 100, 2), None)
        .await
        .unwrap();
    let placed = world
        .place
        .execute(limit("bob", Side::Buy, 100, 1), None)
        .await
        .unwrap();

    assert_eq!(placed.receipt.order.status(), OrderStatus::Filled);
    assert_eq!(placed.receipt.trades.len(), 1);
    assert!(!placed.replayed);

    let book = world.queries.order_book(&market(), 10).await.unwrap();
    assert_eq!(
        book.asks[0].quantity,
        Quantity::positive(Decimal::from(1)).unwrap()
    );

    let events = world.shutdown().await;
    let sequences: Vec<_> = events.iter().map(|e| e.sequence).collect();
    assert_eq!(
        sequences,
        vec![1, 2, 3],
        "dispatcher must keep sequence order"
    );
}

#[tokio::test]
async fn idempotent_retry_returns_the_original_receipt() {
    let world = World::start();
    let key = IdempotencyKey::new(account("alice"), "req-1").unwrap();
    let command = limit("alice", Side::Buy, 100, 1);

    let first = world
        .place
        .execute(command.clone(), Some(key.clone()))
        .await
        .unwrap();
    let second = world
        .place
        .execute(command, Some(key.clone()))
        .await
        .unwrap();

    assert!(second.replayed);
    assert_eq!(first.receipt, second.receipt);

    let different = limit("alice", Side::Buy, 101, 1);
    assert_eq!(
        world.place.execute(different, Some(key)).await,
        Err(ApplicationError::IdempotencyKeyReused)
    );
    let book = world.queries.order_book(&market(), 10).await.unwrap();
    assert_eq!(book.bids.len(), 1, "only one order may rest");
    world.shutdown().await;
}

#[tokio::test]
async fn failed_placement_releases_the_idempotency_key() {
    let world = World::start();
    let key = IdempotencyKey::new(account("alice"), "req-2").unwrap();
    let mut too_big = limit("alice", Side::Buy, 100, 1);
    too_big.quantity = Quantity::positive(Decimal::from(1000)).unwrap();

    let error = world
        .place
        .execute(too_big, Some(key.clone()))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ApplicationError::Domain(DomainError::QuantityAboveMaximum { .. })
    ));

    let retry = world
        .place
        .execute(limit("alice", Side::Buy, 100, 1), Some(key))
        .await;
    assert!(retry.is_ok(), "released key must be reusable");
    world.shutdown().await;
}

#[tokio::test]
async fn cancel_distinguishes_finished_orders_from_unknown_ones() {
    let world = World::start();
    let resting = world
        .place
        .execute(limit("alice", Side::Buy, 100, 1), None)
        .await
        .unwrap()
        .receipt
        .order;
    let command = |who: &str| CancelOrderCommand {
        account: account(who),
        market: market(),
        order_id: resting.id(),
    };

    assert_eq!(
        world.cancel.execute(command("mallory")).await,
        Err(ApplicationError::OrderNotFound(resting.id()))
    );
    let cancelled = world.cancel.execute(command("alice")).await.unwrap();
    assert_eq!(cancelled.status(), OrderStatus::Cancelled);
    assert_eq!(
        world.cancel.execute(command("alice")).await,
        Err(ApplicationError::OrderNotOpen {
            id: resting.id(),
            status: OrderStatus::Cancelled
        })
    );
    world.shutdown().await;
}

#[tokio::test]
async fn orders_are_only_visible_to_their_owner() {
    let world = World::start();
    let order = world
        .place
        .execute(limit("alice", Side::Buy, 100, 1), None)
        .await
        .unwrap()
        .receipt
        .order;

    let found = world
        .queries
        .order(&account("alice"), &market(), order.id())
        .await
        .unwrap();
    assert_eq!(found.id(), order.id());
    assert_eq!(
        world
            .queries
            .order(&account("mallory"), &market(), order.id())
            .await,
        Err(ApplicationError::OrderNotFound(order.id()))
    );
    world.shutdown().await;
}

#[tokio::test]
async fn unknown_markets_are_reported() {
    let world = World::start();
    let mut command = limit("alice", Side::Buy, 100, 1);
    command.market = MarketId::parse("DOGE-USD").unwrap();
    assert_eq!(
        world.place.execute(command, None).await,
        Err(ApplicationError::UnknownMarket(
            MarketId::parse("DOGE-USD").unwrap()
        ))
    );
    assert!(world.queries.is_ready());
    world.shutdown().await;
}

/// Repository that parks the market actor until the test releases it.
#[derive(Default)]
struct GatedRepository {
    closed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl OrderRepository for GatedRepository {
    async fn save(&self, _: &[Order]) -> Result<(), RepositoryError> {
        if self.closed.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }

    async fn find(&self, _: OrderId) -> Result<Option<Order>, RepositoryError> {
        Ok(None)
    }
}

#[tokio::test]
async fn full_engine_queue_sheds_load_instead_of_queueing_forever() {
    let repository = Arc::new(GatedRepository::default());
    repository.closed.store(true, Ordering::SeqCst);
    let (outbox, receiver) = outbox(16);
    let _dispatcher = EventDispatcher::new(
        receiver,
        Arc::new(RecordingPublisher::default()),
        Arc::new(NoopMetrics),
    )
    .spawn();
    let spec = MarketSpec::builder(market())
        .tick_size(Decimal::from(1))
        .lot_size(Decimal::from(1))
        .max_quantity(Decimal::from(100))
        .build()
        .unwrap();
    let deps = MarketDeps {
        repository: repository.clone(),
        clock: Arc::new(TickingClock(AtomicU64::new(1))),
        metrics: Arc::new(NoopMetrics),
        outbox,
    };
    let (handle, _engine) = spawn_market(spec, deps, 1);
    let place = PlaceOrder::new(
        Arc::new(MarketRegistry::new([handle])),
        Arc::new(SequentialIds(AtomicU64::new(1))),
        Arc::new(FakeIdempotency::default()),
    );

    // First order occupies the actor, second one fills the queue of one.
    let busy = tokio::spawn({
        let place = place.clone();
        async move { place.execute(limit("alice", Side::Buy, 90, 1), None).await }
    });
    repository.entered.notified().await;
    let queued = tokio::spawn({
        let place = place.clone();
        async move { place.execute(limit("alice", Side::Buy, 91, 1), None).await }
    });
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    let shed = place.execute(limit("alice", Side::Buy, 92, 1), None).await;
    assert_eq!(shed, Err(ApplicationError::Overloaded(market())));

    repository.closed.store(false, Ordering::SeqCst);
    repository.release.notify_one();
    assert!(busy.await.unwrap().is_ok());
    assert!(queued.await.unwrap().is_ok());
}
