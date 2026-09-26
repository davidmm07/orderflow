//! The task that owns one market's matching engine.

use std::{sync::Arc, time::Instant};

use orderflow_domain::{
    AccountId, BookSnapshot, EventPayload, MarketSpec, MatchOutcome, MatchingEngine, NewOrder,
    Order, OrderId,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    error::ApplicationError,
    outbox::Outbox,
    ports::{Clock, Metrics, OrderRepository},
};

/// Everything a market actor needs from the outside.
#[derive(Clone)]
pub struct MarketDeps {
    pub repository: Arc<dyn OrderRepository>,
    pub clock: Arc<dyn Clock>,
    pub metrics: Arc<dyn Metrics>,
    pub outbox: Outbox,
}

type Reply<T> = oneshot::Sender<Result<T, ApplicationError>>;

/// Messages understood by the market actor.
///
/// Pattern: Command. Each request carries its own reply channel, which lets
/// callers await a result while the actor processes requests strictly one
/// at a time.
enum Request {
    Place {
        order: NewOrder,
        reply: Reply<MatchOutcome>,
    },
    Cancel {
        order_id: OrderId,
        account: AccountId,
        reply: Reply<Order>,
    },
    Snapshot {
        depth: usize,
        reply: Reply<BookSnapshot>,
    },
}

/// Cheap, cloneable address of a running market actor.
///
/// Pattern: Actor. The engine is owned by a single task and only reachable
/// through this handle, so there is exactly one writer per book and no lock
/// on the matching path. Throughput scales out across markets, which is the
/// natural sharding key of an exchange.
#[derive(Debug, Clone)]
pub struct MarketHandle {
    spec: Arc<MarketSpec>,
    sender: mpsc::Sender<Request>,
}

impl MarketHandle {
    pub fn spec(&self) -> &MarketSpec {
        &self.spec
    }

    /// False once the actor has stopped, used by readiness checks.
    pub fn is_running(&self) -> bool {
        !self.sender.is_closed()
    }

    pub(crate) async fn place(&self, order: NewOrder) -> Result<MatchOutcome, ApplicationError> {
        self.call(|reply| Request::Place { order, reply }).await
    }

    pub(crate) async fn cancel(
        &self,
        order_id: OrderId,
        account: AccountId,
    ) -> Result<Order, ApplicationError> {
        self.call(|reply| Request::Cancel {
            order_id,
            account,
            reply,
        })
        .await
    }

    pub(crate) async fn snapshot(&self, depth: usize) -> Result<BookSnapshot, ApplicationError> {
        self.call(|reply| Request::Snapshot { depth, reply }).await
    }

    async fn call<T>(
        &self,
        request: impl FnOnce(Reply<T>) -> Request,
    ) -> Result<T, ApplicationError> {
        let (reply, response) = oneshot::channel();
        // `try_send` sheds load when the queue is full instead of letting
        // requests pile up. For a trading client, an immediate 503 is more
        // useful than an answer that arrives after prices have moved.
        self.sender
            .try_send(request(reply))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    ApplicationError::Overloaded(self.spec.id().clone())
                }
                mpsc::error::TrySendError::Closed(_) => {
                    ApplicationError::Unavailable("market engine is not running")
                }
            })?;
        response
            .await
            .map_err(|_| ApplicationError::Unavailable("market engine stopped"))?
    }
}

/// Starts the actor for `spec` and returns its handle and task.
///
/// The task exits once every handle is dropped and its queue is drained,
/// which is how graceful shutdown flushes in-flight orders.
pub fn spawn_market(
    spec: MarketSpec,
    deps: MarketDeps,
    queue_capacity: usize,
) -> (MarketHandle, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(queue_capacity.max(1));
    let handle = MarketHandle {
        spec: Arc::new(spec.clone()),
        sender,
    };
    let actor = MarketActor {
        engine: MatchingEngine::new(spec),
        deps,
        receiver,
    };
    (handle, tokio::spawn(actor.run()))
}

struct MarketActor {
    engine: MatchingEngine,
    deps: MarketDeps,
    receiver: mpsc::Receiver<Request>,
}

impl MarketActor {
    async fn run(mut self) {
        let market = self.engine.spec().id().clone();
        tracing::info!(%market, "market engine started");
        while let Some(request) = self.receiver.recv().await {
            // A failed `send` below means the caller stopped waiting. The
            // work is already committed, so dropping the reply is correct.
            match request {
                Request::Place { order, reply } => {
                    let _ = reply.send(self.place(order).await);
                }
                Request::Cancel {
                    order_id,
                    account,
                    reply,
                } => {
                    let _ = reply.send(self.cancel(order_id, &account).await);
                }
                Request::Snapshot { depth, reply } => {
                    let _ = reply.send(Ok(self.engine.snapshot(depth)));
                }
            }
        }
        tracing::info!(%market, sequence = self.engine.sequence(), "market engine stopped");
    }

    async fn place(&mut self, order: NewOrder) -> Result<MatchOutcome, ApplicationError> {
        let started = Instant::now();
        let market = self.engine.spec().id().clone();
        let now = self.deps.clock.now();

        let mut outcome = match self.engine.submit(order, now) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.deps.metrics.order_processed(&market, "rejected");
                return Err(error.into());
            }
        };

        let changed: Vec<Order> = outcome.changed_orders().cloned().collect();
        // Counted from events so trades of fired stop orders are included.
        let trade_count = outcome
            .events
            .iter()
            .filter(|event| matches!(event.payload, EventPayload::TradeExecuted(_)))
            .count();
        let events = std::mem::take(&mut outcome.events);
        self.commit(events, &changed).await?;

        self.deps
            .metrics
            .order_processed(&market, outcome.order.status().as_str());
        self.deps.metrics.trades_executed(&market, trade_count);
        self.deps.metrics.engine_latency(&market, started.elapsed());
        Ok(outcome)
    }

    async fn cancel(
        &mut self,
        order_id: OrderId,
        account: &AccountId,
    ) -> Result<Order, ApplicationError> {
        let now = self.deps.clock.now();
        let outcome = self.engine.cancel(order_id, account, now)?;
        self.commit(vec![outcome.event], std::slice::from_ref(&outcome.order))
            .await?;
        Ok(outcome.order)
    }

    /// Publishes events, then updates the read model.
    ///
    /// Events go first because the event stream is the source of truth and
    /// the repository is a projection that can be rebuilt from it. A failed
    /// projection write is logged but not reported to the caller: the match
    /// already happened, and an error would invite a duplicate retry.
    async fn commit(
        &self,
        events: Vec<orderflow_domain::DomainEvent>,
        orders: &[Order],
    ) -> Result<(), ApplicationError> {
        self.deps.outbox.send(events).await?;
        if let Err(error) = self.deps.repository.save(orders).await {
            tracing::error!(%error, market = %self.engine.spec().id(), "order read model update failed");
        }
        Ok(())
    }
}
