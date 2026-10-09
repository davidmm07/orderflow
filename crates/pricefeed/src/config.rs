//! Typed configuration read from `PRICEFEED_*` environment variables.
//!
//! Like the exchange server, every problem is collected before failing, and
//! values that can carry credentials (database and Redis URLs, RPC URLs
//! with provider keys, API keys) are wrapped in `Secret` so they never
//! reach a log line or a `Debug` print.

use std::{
    fmt::{self, Display},
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

#[derive(Debug, thiserror::Error)]
#[error("invalid configuration:\n  - {}", .0.join("\n  - "))]
pub struct ConfigError(pub Vec<String>);

/// A value that never prints.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

/// What this process does. One binary, three deployment shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Ingest and serve in one process: local development and small setups.
    All,
    /// Run the sources and the aggregator, write to Postgres, publish to
    /// Redis. Deploy one active instance; extra ones wait as standbys.
    Ingest,
    /// Serve REST and WebSocket clients from Redis and Postgres. Stateless,
    /// scaled horizontally behind a load balancer.
    Edge,
}

impl Role {
    pub const fn ingests(self) -> bool {
        matches!(self, Self::All | Self::Ingest)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Ingest => "ingest",
            Self::Edge => "edge",
        }
    }
}

/// Limits that keep one WebSocket client from costing more than its share.
#[derive(Debug, Clone, Copy)]
pub struct WsLimits {
    pub max_connections: usize,
    pub max_subscriptions: usize,
    pub max_message_bytes: usize,
    /// A client that cannot take a frame within this time is disconnected.
    pub send_timeout: Duration,
    pub ping_interval: Duration,
    /// A client silent for this long (no pong, no message) is gone.
    pub idle_timeout: Duration,
    /// Control messages a client may send per `ping_interval`.
    pub max_control_messages: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct CacheSettings {
    pub hot_ttl: Duration,
    pub settled_ttl: Duration,
    pub max_entries: u64,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub bind_addr: SocketAddr,
    pub role: Role,
    pub log_format: LogFormat,
    pub feeds_file: PathBuf,
    pub database_url: Option<Secret>,
    pub database_max_connections: u32,
    pub redis_url: Option<Secret>,
    pub evm_rpc_url: Secret,
    pub solana_rpc_url: Secret,
    pub coingecko_api_key: Option<Secret>,
    /// Connectors to run, or `None` for every configured one.
    pub sources: Option<Vec<String>>,
    pub kafka_brokers: Option<String>,
    pub kafka_topic: String,
    pub publish_interval: Duration,
    pub request_timeout: Duration,
    pub ws: WsLimits,
    pub cache: CacheSettings,
    pub shutdown_grace: Duration,
}

/// Connector names accepted by `PRICEFEED_SOURCES`.
pub const CONNECTORS: [&str; 6] = [
    "coingecko",
    "kraken",
    "coinbase",
    "uniswap_v3",
    "orca",
    "orderflow",
];

impl Settings {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let mut env = Env {
            lookup,
            problems: Vec::new(),
        };

        let bind_addr = env.parsed(
            "PRICEFEED_BIND_ADDR",
            SocketAddr::from(([127, 0, 0, 1], 8090)),
        );
        let role = match env.text("PRICEFEED_ROLE", "all").as_str() {
            "all" => Role::All,
            "ingest" => Role::Ingest,
            "edge" => Role::Edge,
            other => {
                env.problem(format!(
                    "PRICEFEED_ROLE: expected all, ingest or edge, got {other:?}"
                ));
                Role::All
            }
        };
        let log_format = match env.text("PRICEFEED_LOG_FORMAT", "json").as_str() {
            "json" => LogFormat::Json,
            "pretty" => LogFormat::Pretty,
            other => {
                env.problem(format!(
                    "PRICEFEED_LOG_FORMAT: expected json or pretty, got {other:?}"
                ));
                LogFormat::Json
            }
        };
        let feeds_file = PathBuf::from(env.text("PRICEFEED_FEEDS_FILE", "config/marketdata.json"));
        let database_url = env.raw("PRICEFEED_DATABASE_URL").map(Secret);
        let database_max_connections =
            env.positive("PRICEFEED_DATABASE_MAX_CONNECTIONS", 10) as u32;
        let redis_url = env.raw("PRICEFEED_REDIS_URL").map(Secret);
        // Public endpoints by default. Provider URLs with keys belong in .env.
        let evm_rpc_url = Secret(env.text(
            "PRICEFEED_EVM_RPC_URL",
            "https://ethereum-rpc.publicnode.com",
        ));
        let solana_rpc_url = Secret(env.text(
            "PRICEFEED_SOLANA_RPC_URL",
            "https://api.mainnet-beta.solana.com",
        ));
        let coingecko_api_key = env.raw("PRICEFEED_COINGECKO_API_KEY").map(Secret);
        let sources = env.raw("PRICEFEED_SOURCES").map(|raw| {
            raw.split(',')
                .map(|name| name.trim().to_owned())
                .filter(|name| !name.is_empty() && name != "none")
                .collect::<Vec<_>>()
        });
        if let Some(names) = &sources {
            for name in names {
                if !CONNECTORS.contains(&name.as_str()) {
                    env.problem(format!(
                        "PRICEFEED_SOURCES: unknown source {name:?}, expected some of {}",
                        CONNECTORS.join(", ")
                    ));
                }
            }
        }
        let kafka_brokers = env.raw("PRICEFEED_KAFKA_BROKERS");
        if kafka_brokers.is_some() && !cfg!(feature = "kafka") {
            env.problem("PRICEFEED_KAFKA_BROKERS needs a build with `--features kafka`".into());
        }
        let kafka_topic = env.text("PRICEFEED_KAFKA_TOPIC", "orderflow.events.v1");
        let publish_interval =
            Duration::from_millis(env.positive("PRICEFEED_PUBLISH_INTERVAL_MS", 250));
        let request_timeout =
            Duration::from_millis(env.positive("PRICEFEED_REQUEST_TIMEOUT_MS", 5_000));
        let ws = WsLimits {
            max_connections: env.positive("PRICEFEED_WS_MAX_CONNECTIONS", 10_000) as usize,
            max_subscriptions: env.positive("PRICEFEED_WS_MAX_SUBSCRIPTIONS", 50) as usize,
            max_message_bytes: env.positive("PRICEFEED_WS_MAX_MESSAGE_BYTES", 4_096) as usize,
            send_timeout: Duration::from_millis(
                env.positive("PRICEFEED_WS_SEND_TIMEOUT_MS", 5_000),
            ),
            ping_interval: Duration::from_secs(env.positive("PRICEFEED_WS_PING_SECS", 20)),
            idle_timeout: Duration::from_secs(env.positive("PRICEFEED_WS_IDLE_SECS", 60)),
            max_control_messages: env.positive("PRICEFEED_WS_MAX_CONTROL_MESSAGES", 20) as u32,
        };
        if ws.idle_timeout <= ws.ping_interval {
            env.problem("PRICEFEED_WS_IDLE_SECS must be longer than PRICEFEED_WS_PING_SECS".into());
        }
        let cache = CacheSettings {
            hot_ttl: Duration::from_millis(env.positive("PRICEFEED_CACHE_HOT_TTL_MS", 1_000)),
            settled_ttl: Duration::from_secs(env.positive("PRICEFEED_CACHE_SETTLED_TTL_SECS", 60)),
            max_entries: env.positive("PRICEFEED_CACHE_MAX_ENTRIES", 10_000),
        };
        let shutdown_grace = Duration::from_secs(env.positive("PRICEFEED_SHUTDOWN_GRACE_SECS", 10));

        match role {
            Role::Edge if redis_url.is_none() || database_url.is_none() => env.problem(
                "PRICEFEED_ROLE=edge needs PRICEFEED_REDIS_URL for live prices and PRICEFEED_DATABASE_URL for candles"
                    .into(),
            ),
            Role::Ingest if redis_url.is_none() => {
                env.problem("PRICEFEED_ROLE=ingest needs PRICEFEED_REDIS_URL to reach the edge nodes".into());
            }
            _ => {}
        }

        if !env.problems.is_empty() {
            return Err(ConfigError(env.problems));
        }
        Ok(Self {
            bind_addr,
            role,
            log_format,
            feeds_file,
            database_url,
            database_max_connections,
            redis_url,
            evm_rpc_url,
            solana_rpc_url,
            coingecko_api_key,
            sources,
            kafka_brokers,
            kafka_topic,
            publish_interval,
            request_timeout,
            ws,
            cache,
            shutdown_grace,
        })
    }

    /// Whether connector `name` should run.
    pub fn source_enabled(&self, name: &str) -> bool {
        self.sources
            .as_ref()
            .is_none_or(|enabled| enabled.iter().any(|candidate| candidate == name))
    }
}

struct Env<F> {
    lookup: F,
    problems: Vec<String>,
}

impl<F: Fn(&str) -> Option<String>> Env<F> {
    fn problem(&mut self, message: String) {
        self.problems.push(message);
    }

    fn raw(&self, key: &str) -> Option<String> {
        (self.lookup)(key)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    }

    fn text(&self, key: &str, default: &str) -> String {
        self.raw(key).unwrap_or_else(|| default.to_owned())
    }

    fn parsed<T>(&mut self, key: &str, default: T) -> T
    where
        T: FromStr,
        T::Err: Display,
    {
        match self.raw(key) {
            None => default,
            Some(raw) => raw.parse().unwrap_or_else(|error| {
                self.problem(format!("{key}: {error}"));
                default
            }),
        }
    }

    fn positive(&mut self, key: &str, default: u32) -> u64 {
        let value: u32 = self.parsed(key, default);
        if value == 0 {
            self.problem(format!("{key} must be greater than zero"));
        }
        u64::from(value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn settings(vars: &[(&str, &str)]) -> Result<Settings, ConfigError> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        Settings::from_lookup(|key| map.get(key).cloned())
    }

    #[test]
    fn defaults_run_everything_in_one_process_on_localhost() {
        let settings = settings(&[]).unwrap();
        assert_eq!(settings.role, Role::All);
        assert_eq!(settings.bind_addr.to_string(), "127.0.0.1:8090");
        assert!(settings.database_url.is_none() && settings.redis_url.is_none());
        assert!(settings.source_enabled("kraken"));
    }

    #[test]
    fn split_roles_need_their_backends() {
        let error = settings(&[("PRICEFEED_ROLE", "edge")]).unwrap_err();
        assert!(error.to_string().contains("PRICEFEED_REDIS_URL"));
        let error = settings(&[("PRICEFEED_ROLE", "ingest")]).unwrap_err();
        assert!(error.to_string().contains("PRICEFEED_REDIS_URL"));
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let error = settings(&[
            ("PRICEFEED_ROLE", "leader"),
            ("PRICEFEED_SOURCES", "kraken,binance"),
            ("PRICEFEED_WS_PING_SECS", "90"),
            ("PRICEFEED_WS_MAX_CONNECTIONS", "0"),
        ])
        .unwrap_err();
        assert_eq!(error.0.len(), 4, "{error}");
    }

    #[test]
    fn source_selection_and_secrets() {
        let settings = settings(&[
            ("PRICEFEED_SOURCES", "kraken, orca"),
            ("PRICEFEED_EVM_RPC_URL", "https://eth.example/v2/secret-key"),
            (
                "PRICEFEED_DATABASE_URL",
                "postgres://user:hunter2@db/pricefeed",
            ),
        ])
        .unwrap();
        assert!(settings.source_enabled("orca") && !settings.source_enabled("coinbase"));
        let printed = format!("{settings:?}");
        assert!(!printed.contains("secret-key") && !printed.contains("hunter2"));
    }
}
