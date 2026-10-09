//! Composition root: the one place that knows every concrete type.
//!
//! ```text
//!  sources --> Aggregator --+--> PriceHub --> WebSocket / REST   (this node)
//!  (ingest role)            +--> StoreWriter --> Postgres
//!                           +--> ConflatingPublisher --> Redis --> edge nodes' PriceHub
//! ```

use std::{
    collections::BTreeSet,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use anyhow::Context;
use axum::Router;
use orderflow_application::Clock;
use orderflow_infrastructure::MonotonicClock;
use orderflow_ingest::{
    HttpClient, Source, SourceContext,
    chain::{HttpRpc, evm::UniswapV3Indexer, solana::WhirlpoolIndexer},
    connectors::{CoinGecko, Coinbase, Kraken},
    install_crypto_provider, supervise,
};
use orderflow_marketdata::{
    MarketFeed, PriceIndex, SourceId,
    pipeline::Aggregator,
    ports::{CandleReader, ChainStore, PriceStore, UpdateSink},
    sinks::{ConflatingPublisher, StoreWriter},
};
use orderflow_storage::{
    LeaderLease, MemoryCandleCache, MemoryStore, PgStore, RedisBus, RedisCandleCache, follow_prices,
};
use tokio::{sync::Semaphore, task::JoinHandle};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    config::{Secret, Settings},
    feeds::Feeds,
    http,
    hub::PriceHub,
    metrics::PricefeedMetrics,
    state::AppState,
};

/// How often the ingest leader checks that its lease session is alive.
const LEASE_CHECK: Duration = Duration::from_secs(5);
/// How often a standby tries to take over the lease.
const STANDBY_RETRY: Duration = Duration::from_secs(5);
/// Updates queued for the store before new ones are dropped.
const STORE_QUEUE: usize = 8_192;
/// Observations queued for the aggregator before sources wait.
const OBSERVATION_QUEUE: usize = 4_096;

/// A wired application plus the background tasks it started.
pub struct Application {
    pub router: Router,
    /// Cancelled on shutdown: stops ingestion and closes WebSockets.
    pub shutdown: CancellationToken,
    pub sessions: TaskTracker,
    pub tasks: Vec<JoinHandle<()>>,
}

/// Builds the object graph for the configured role.
///
/// Pattern: Composition Root. Every adapter is chosen here: Postgres or
/// memory for storage, Redis or nothing for the bus and the shared cache,
/// and which sources run. The layers below receive trait objects.
pub async fn build(settings: &Settings, feeds: Feeds) -> anyhow::Result<Application> {
    install_crypto_provider();
    let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::default());
    let markets = feeds.market_ids();
    let metrics = Arc::new(PricefeedMetrics::new(&markets, &source_ids(&feeds.markets)));
    let hub = Arc::new(PriceHub::new(markets));
    let shutdown = CancellationToken::new();
    let ready = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();

    let (price_store, candle_store, chain_store) = storage(settings, &clock).await?;
    let redis = settings
        .redis_url
        .as_ref()
        .map(|url| orderflow_storage::client(url.expose()))
        .transpose()
        .context("PRICEFEED_REDIS_URL")?;

    // Candle reads: process memory, then Redis shared by all nodes, then
    // the store.
    let mut reader = candle_store;
    if let Some(client) = &redis {
        reader = Arc::new(
            RedisCandleCache::connect(
                client,
                reader,
                clock.clone(),
                metrics.clone(),
                settings.cache.hot_ttl,
                settings.cache.settled_ttl,
            )
            .await
            .context("connecting to redis")?,
        );
    }
    let candles: Arc<dyn CandleReader> = Arc::new(MemoryCandleCache::new(
        reader,
        clock.clone(),
        metrics.clone(),
        settings.cache.max_entries,
        settings.cache.hot_ttl,
        settings.cache.settled_ttl,
    ));

    if settings.role.ingests() {
        let (writer, writer_task) = StoreWriter::spawn(price_store, metrics.clone(), STORE_QUEUE);
        tasks.push(writer_task);
        let mut sinks: Vec<Arc<dyn UpdateSink>> = vec![hub.clone(), Arc::new(writer)];
        if let Some(client) = &redis {
            let bus = RedisBus::connect(client)
                .await
                .context("connecting to redis")?;
            let (publisher, publisher_task) =
                ConflatingPublisher::spawn(Arc::new(bus), metrics.clone());
            sinks.push(Arc::new(publisher));
            tasks.push(publisher_task);
        }
        let ingestion = Ingestion {
            markets: feeds.markets.clone(),
            sources: sources(settings, &feeds, &chain_store)?,
            sinks,
            clock: clock.clone(),
            metrics: metrics.clone(),
            interval: settings.publish_interval,
        };
        // An ingesting node serves what it computes, from the first moment.
        ready.store(true, std::sync::atomic::Ordering::Release);
        tasks.push(tokio::spawn(lead(
            settings.database_url.clone(),
            ingestion,
            shutdown.clone(),
            metrics.clone(),
        )));
    } else {
        let client = redis.context("edge nodes need PRICEFEED_REDIS_URL")?;
        let target = hub.clone();
        tasks.push(tokio::spawn(follow_prices(
            client,
            Arc::new(move |update| {
                target.publish(Arc::new(update));
            }),
            metrics.clone(),
            ready.clone(),
            shutdown.clone(),
        )));
    }

    let sessions = TaskTracker::new();
    let state = AppState {
        hub,
        candles,
        metrics,
        clock,
        ws: settings.ws,
        connections: Arc::new(Semaphore::new(settings.ws.max_connections)),
        shutdown: shutdown.clone(),
        sessions: sessions.clone(),
        ready,
    };
    Ok(Application {
        router: http::router(state, settings.request_timeout),
        shutdown,
        sessions,
        tasks,
    })
}

type Stores = (
    Arc<dyn PriceStore>,
    Arc<dyn CandleReader>,
    Arc<dyn ChainStore>,
);

async fn storage(settings: &Settings, clock: &Arc<dyn Clock>) -> anyhow::Result<Stores> {
    let Some(url) = &settings.database_url else {
        tracing::warn!(
            "PRICEFEED_DATABASE_URL is not set: history lives in memory and is lost on restart"
        );
        let memory = Arc::new(MemoryStore::new(clock.clone()));
        return Ok((memory.clone(), memory.clone(), memory));
    };
    let store = PgStore::connect(
        url.expose(),
        settings.database_max_connections,
        clock.clone(),
    )
    .await
    .context("connecting to postgres")?;
    if settings.role.ingests() {
        store.migrate().await.context("migrating the database")?;
    }
    let store = Arc::new(store);
    Ok((store.clone(), store.clone(), store))
}

/// Every source id in the feeds, for zero-initialized metrics.
fn source_ids(markets: &[MarketFeed]) -> Vec<SourceId> {
    let ids: BTreeSet<SourceId> = markets
        .iter()
        .flat_map(|feed| feed.sources.iter().map(|spec| spec.id.clone()))
        .collect();
    ids.into_iter().collect()
}

/// Factory for the configured and enabled sources.
fn sources(
    settings: &Settings,
    feeds: &Feeds,
    chain: &Arc<dyn ChainStore>,
) -> anyhow::Result<Vec<Arc<dyn Source>>> {
    let http = HttpClient::new(Duration::from_secs(10))?;
    // eth_getLogs over a few hundred blocks can take a while on public nodes.
    let rpc_http = HttpClient::new(Duration::from_secs(30))?;
    let mut sources: Vec<Arc<dyn Source>> = Vec::new();

    if let Some(config) = feeds.coingecko.clone()
        && settings.source_enabled("coingecko")
    {
        let config = orderflow_ingest::connectors::CoinGeckoConfig {
            api_key: settings
                .coingecko_api_key
                .as_ref()
                .map(|key| key.expose().to_owned()),
            ..config
        };
        sources.push(Arc::new(CoinGecko::new(http.clone(), config)?));
    }
    if let Some(config) = feeds.kraken.clone()
        && settings.source_enabled("kraken")
    {
        sources.push(Arc::new(Kraken::new(http.clone(), config)?));
    }
    if let Some(config) = feeds.coinbase.clone()
        && settings.source_enabled("coinbase")
    {
        sources.push(Arc::new(Coinbase::new(config)?));
    }
    if let Some(config) = feeds.uniswap.clone()
        && settings.source_enabled("uniswap_v3")
    {
        let rpc = Arc::new(HttpRpc::new(
            rpc_http.clone(),
            settings.evm_rpc_url.expose(),
        ));
        sources.push(Arc::new(UniswapV3Indexer::new(config, rpc, chain.clone())));
    }
    if let Some(config) = feeds.orca.clone()
        && settings.source_enabled("orca")
    {
        let rpc = Arc::new(HttpRpc::new(rpc_http, settings.solana_rpc_url.expose()));
        sources.push(Arc::new(WhirlpoolIndexer::new(config, rpc, chain.clone())?));
    }
    #[cfg(feature = "kafka")]
    if let (Some(markets), Some(brokers)) =
        (feeds.orderflow.clone(), settings.kafka_brokers.clone())
        && settings.source_enabled("orderflow")
    {
        use orderflow_ingest::venue::{OrderflowTrades, OrderflowTradesConfig};
        sources.push(Arc::new(OrderflowTrades::new(OrderflowTradesConfig {
            brokers,
            topic: settings.kafka_topic.clone(),
            group_id: "orderflow-pricefeed".into(),
            markets,
        })?));
    }
    let names: Vec<&str> = sources.iter().map(|source| source.name()).collect();
    tracing::info!(sources = ?names, "sources configured");
    Ok(sources)
}

/// Everything one leadership term needs to ingest.
struct Ingestion {
    markets: Vec<MarketFeed>,
    sources: Vec<Arc<dyn Source>>,
    sinks: Vec<Arc<dyn UpdateSink>>,
    clock: Arc<dyn Clock>,
    metrics: Arc<PricefeedMetrics>,
    interval: Duration,
}

impl Ingestion {
    /// Runs every source and the aggregator until `term` ends.
    ///
    /// Each term publishes under its own epoch, its start time in
    /// milliseconds, so a new leader's sequence numbers never collide with
    /// the previous leader's.
    async fn run(&self, term: CancellationToken) {
        let epoch = self.clock.now().unix_nanos() / 1_000_000;
        let index = match PriceIndex::new(epoch, self.markets.clone()) {
            Ok(index) => index,
            Err(error) => {
                tracing::error!(%error, "invalid market configuration");
                return;
            }
        };
        let (aggregator, observations) = Aggregator::new(
            index,
            self.sinks.clone(),
            self.clock.clone(),
            self.metrics.clone(),
            self.interval,
            OBSERVATION_QUEUE,
        );
        let aggregator = tokio::spawn(aggregator.run(term.clone()));
        let workers: Vec<JoinHandle<()>> = self
            .sources
            .iter()
            .map(|source| {
                let ctx = SourceContext::new(
                    observations.clone(),
                    self.clock.clone(),
                    self.metrics.clone(),
                    term.clone(),
                );
                tokio::spawn(supervise(source.clone(), ctx))
            })
            .collect();
        drop(observations);
        tracing::info!(epoch, sources = workers.len(), "ingestion started");

        term.cancelled().await;
        for worker in workers {
            let _ = worker.await;
        }
        let _ = aggregator.await;
        tracing::info!(epoch, "ingestion stopped");
    }
}

/// Ingests while holding the leader lease; waits as a standby otherwise.
async fn lead(
    database_url: Option<Secret>,
    ingestion: Ingestion,
    shutdown: CancellationToken,
    metrics: Arc<PricefeedMetrics>,
) {
    let Some(url) = database_url else {
        // No shared database, so no other instance to coordinate with.
        metrics.set_leader(true);
        ingestion.run(shutdown.clone()).await;
        metrics.set_leader(false);
        return;
    };
    while !shutdown.is_cancelled() {
        match LeaderLease::try_acquire(url.expose()).await {
            Ok(Some(lease)) => {
                tracing::info!("holding the ingest lease");
                metrics.set_leader(true);
                let term = shutdown.child_token();
                let run = ingestion.run(term.clone());
                tokio::pin!(run);
                tokio::select! {
                    () = &mut run => {}
                    error = lease.lost(LEASE_CHECK) => {
                        tracing::warn!(%error, "lost the ingest lease, stopping ingestion");
                        term.cancel();
                        run.await;
                    }
                }
                metrics.set_leader(false);
            }
            Ok(None) => tracing::debug!("another instance is ingesting, standing by"),
            Err(error) => tracing::warn!(%error, "cannot reach the database for the ingest lease"),
        }
        tokio::select! {
            () = shutdown.cancelled() => {}
            () = tokio::time::sleep(STANDBY_RETRY) => {}
        }
    }
}
