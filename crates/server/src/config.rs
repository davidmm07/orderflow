//! Typed configuration read from environment variables.
//!
//! Every problem is collected before failing, so a misconfigured deployment
//! reports all of its mistakes in one go instead of one per restart.

use std::{
    fmt::{self, Display},
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use orderflow_api::RateLimitConfig;
use orderflow_domain::{AccountId, Decimal, MarketId, MarketSpec};
use serde::Deserialize;

/// Shortest accepted API secret, mirrored from the API crate.
const MIN_SECRET_LEN: usize = 32;

#[derive(Debug, thiserror::Error)]
#[error("invalid configuration:\n  - {}", .0.join("\n  - "))]
pub struct ConfigError(pub Vec<String>);

/// A secret value that never prints.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[derive(Debug, Clone)]
pub struct ApiCredential {
    pub key_id: String,
    pub account: AccountId,
    pub secret: Secret,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventSink {
    Log,
    Kafka { brokers: String, topic: String },
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub bind_addr: SocketAddr,
    pub log_format: LogFormat,
    pub markets_file: PathBuf,
    pub credentials: Vec<ApiCredential>,
    pub request_timeout: Duration,
    pub max_body_bytes: usize,
    pub signature_tolerance: Duration,
    pub rate_limit: RateLimitConfig,
    pub engine_queue_capacity: usize,
    pub outbox_capacity: usize,
    pub idempotency_ttl: Duration,
    pub idempotency_max_entries: usize,
    pub event_sink: EventSink,
    pub shutdown_grace: Duration,
}

impl Settings {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Reads settings through `lookup`, which lets tests pass a map instead
    /// of mutating the process environment (`set_var` is `unsafe` in the
    /// 2024 edition because it races with other threads).
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let mut env = Env {
            lookup,
            problems: Vec::new(),
        };

        // Localhost by default: exposing the port is an explicit decision.
        let bind_addr = env.parsed(
            "ORDERFLOW_BIND_ADDR",
            SocketAddr::from(([127, 0, 0, 1], 8080)),
        );
        let log_format = match env.text("ORDERFLOW_LOG_FORMAT", "json").as_str() {
            "json" => LogFormat::Json,
            "pretty" => LogFormat::Pretty,
            other => {
                env.problem(format!(
                    "ORDERFLOW_LOG_FORMAT: expected json or pretty, got {other:?}"
                ));
                LogFormat::Json
            }
        };
        let markets_file = PathBuf::from(env.text("ORDERFLOW_MARKETS_FILE", "config/markets.json"));
        let credentials = env.credentials("ORDERFLOW_API_CREDENTIALS");
        let request_timeout =
            Duration::from_millis(env.positive("ORDERFLOW_REQUEST_TIMEOUT_MS", 5_000));
        let max_body_bytes = env.positive("ORDERFLOW_MAX_BODY_BYTES", 16 * 1024) as usize;
        let signature_tolerance =
            Duration::from_secs(env.positive("ORDERFLOW_SIGNATURE_TOLERANCE_SECS", 30));
        let rate_limit = RateLimitConfig {
            per_second: env.positive("ORDERFLOW_RATE_LIMIT_PER_SEC", 50) as u32,
            burst: env.positive("ORDERFLOW_RATE_LIMIT_BURST", 100) as u32,
        };
        let engine_queue_capacity = env.positive("ORDERFLOW_ENGINE_QUEUE_CAPACITY", 4_096) as usize;
        let outbox_capacity = env.positive("ORDERFLOW_OUTBOX_CAPACITY", 8_192) as usize;
        let idempotency_ttl =
            Duration::from_secs(env.positive("ORDERFLOW_IDEMPOTENCY_TTL_SECS", 86_400));
        let idempotency_max_entries =
            env.positive("ORDERFLOW_IDEMPOTENCY_MAX_ENTRIES", 1_000_000) as usize;
        let shutdown_grace = Duration::from_secs(env.positive("ORDERFLOW_SHUTDOWN_GRACE_SECS", 10));

        let event_sink = match env.text("ORDERFLOW_EVENT_SINK", "log").as_str() {
            "log" => EventSink::Log,
            "kafka" => {
                if !cfg!(feature = "kafka") {
                    env.problem(
                        "ORDERFLOW_EVENT_SINK=kafka needs a build with `--features kafka`".into(),
                    );
                }
                let brokers = env.required("ORDERFLOW_KAFKA_BROKERS").unwrap_or_default();
                let topic = env.text("ORDERFLOW_KAFKA_TOPIC", "orderflow.events.v1");
                EventSink::Kafka { brokers, topic }
            }
            other => {
                env.problem(format!(
                    "ORDERFLOW_EVENT_SINK: expected log or kafka, got {other:?}"
                ));
                EventSink::Log
            }
        };

        if !env.problems.is_empty() {
            return Err(ConfigError(env.problems));
        }
        Ok(Self {
            bind_addr,
            log_format,
            markets_file,
            credentials,
            request_timeout,
            max_body_bytes,
            signature_tolerance,
            rate_limit,
            engine_queue_capacity,
            outbox_capacity,
            idempotency_ttl,
            idempotency_max_entries,
            event_sink,
            shutdown_grace,
        })
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

    fn required(&mut self, key: &str) -> Option<String> {
        let value = self.raw(key);
        if value.is_none() {
            self.problem(format!("{key} is required"));
        }
        value
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

    /// Parses `key_id:account_id:secret` triples separated by commas.
    /// Messages name the offending entry by position, never by content, so
    /// a typo cannot leak a secret into the logs.
    fn credentials(&mut self, key: &str) -> Vec<ApiCredential> {
        let Some(raw) = self.required(key) else {
            return Vec::new();
        };
        let mut credentials: Vec<ApiCredential> = Vec::new();
        for (index, entry) in raw.split(',').map(str::trim).enumerate() {
            let position = index + 1;
            let mut parts = entry.splitn(3, ':');
            let (Some(key_id), Some(account), Some(secret)) =
                (parts.next(), parts.next(), parts.next())
            else {
                self.problem(format!(
                    "{key}: entry {position} is not key_id:account_id:secret"
                ));
                continue;
            };
            let key_id_ok = !key_id.is_empty()
                && key_id.len() <= 64
                && key_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
            if !key_id_ok {
                self.problem(format!("{key}: entry {position} has an invalid key id"));
                continue;
            }
            let Ok(account) = AccountId::parse(account) else {
                self.problem(format!("{key}: entry {position} has an invalid account id"));
                continue;
            };
            if secret.len() < MIN_SECRET_LEN {
                self.problem(format!(
                    "{key}: entry {position} has a secret shorter than {MIN_SECRET_LEN} characters"
                ));
                continue;
            }
            if credentials.iter().any(|c| c.key_id == key_id) {
                self.problem(format!("{key}: entry {position} repeats key id {key_id}"));
                continue;
            }
            credentials.push(ApiCredential {
                key_id: key_id.to_owned(),
                account,
                secret: Secret(secret.to_owned()),
            });
        }
        credentials
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketsFile {
    markets: Vec<MarketEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketEntry {
    id: String,
    tick_size: String,
    lot_size: String,
    min_quantity: Option<String>,
    max_quantity: String,
}

/// Loads and validates the market list. Market parameters are not secret,
/// so they live in a versioned file rather than in the environment.
pub fn load_markets(path: &Path) -> Result<Vec<MarketSpec>, ConfigError> {
    let fail = |message: String| ConfigError(vec![message]);
    let text = fs::read_to_string(path)
        .map_err(|error| fail(format!("cannot read {}: {error}", path.display())))?;
    let file: MarketsFile = serde_json::from_str(&text)
        .map_err(|error| fail(format!("{}: {error}", path.display())))?;

    let mut problems = Vec::new();
    let mut specs: Vec<MarketSpec> = Vec::new();
    for entry in file.markets {
        match market_spec(&entry) {
            Ok(spec) if specs.iter().any(|s| s.id() == spec.id()) => {
                problems.push(format!("market {} is listed twice", spec.id()));
            }
            Ok(spec) => specs.push(spec),
            Err(message) => problems.push(format!("market {:?}: {message}", entry.id)),
        }
    }
    if specs.is_empty() && problems.is_empty() {
        problems.push(format!("{} lists no markets", path.display()));
    }
    if problems.is_empty() {
        Ok(specs)
    } else {
        Err(ConfigError(problems))
    }
}

fn market_spec(entry: &MarketEntry) -> Result<MarketSpec, String> {
    let decimal = |field: &str, raw: &str| {
        Decimal::from_str_exact(raw).map_err(|_| format!("{field} {raw:?} is not a decimal"))
    };
    let id = MarketId::parse(&entry.id).map_err(|error| error.to_string())?;
    let mut builder = MarketSpec::builder(id)
        .tick_size(decimal("tick_size", &entry.tick_size)?)
        .lot_size(decimal("lot_size", &entry.lot_size)?)
        .max_quantity(decimal("max_quantity", &entry.max_quantity)?);
    if let Some(min) = &entry.min_quantity {
        builder = builder.min_quantity(decimal("min_quantity", min)?);
    }
    builder.build().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const SECRET: &str = "0123456789abcdef0123456789abcdef";

    fn settings(vars: &[(&str, &str)]) -> Result<Settings, ConfigError> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        Settings::from_lookup(|key| map.get(key).cloned())
    }

    #[test]
    fn minimal_configuration_uses_safe_defaults() {
        let credentials = format!("key-1:alice:{SECRET}");
        let settings = settings(&[("ORDERFLOW_API_CREDENTIALS", &credentials)]).unwrap();
        assert_eq!(settings.bind_addr.to_string(), "127.0.0.1:8080");
        assert_eq!(settings.event_sink, EventSink::Log);
        assert_eq!(settings.credentials.len(), 1);
        assert_eq!(settings.credentials[0].account.as_str(), "alice");
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let error = settings(&[
            ("ORDERFLOW_BIND_ADDR", "not-an-address"),
            ("ORDERFLOW_RATE_LIMIT_PER_SEC", "0"),
            ("ORDERFLOW_EVENT_SINK", "carrier-pigeon"),
        ])
        .unwrap_err();
        assert_eq!(error.0.len(), 4, "{error}");
    }

    #[test]
    fn credential_errors_never_echo_the_secret() {
        let error = settings(&[(
            "ORDERFLOW_API_CREDENTIALS",
            "key-1:alice:tooshort-secret-value,key 2:bob:x",
        )])
        .unwrap_err();
        let text = error.to_string();
        assert!(text.contains("entry 1 has a secret shorter"));
        assert!(text.contains("entry 2 has an invalid key id"));
        assert!(!text.contains("tooshort-secret-value"));
    }

    #[test]
    fn secrets_are_redacted_in_debug_output() {
        let credentials = format!("key-1:alice:{SECRET}");
        let settings = settings(&[("ORDERFLOW_API_CREDENTIALS", &credentials)]).unwrap();
        assert!(!format!("{settings:?}").contains(SECRET));
    }

    #[test]
    fn duplicate_key_ids_are_rejected() {
        let credentials = format!("k:alice:{SECRET},k:bob:{SECRET}");
        let error = settings(&[("ORDERFLOW_API_CREDENTIALS", &credentials)]).unwrap_err();
        assert!(error.to_string().contains("repeats key id"));
    }

    #[test]
    fn bundled_market_file_is_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/markets.json");
        let markets = load_markets(&path).unwrap();
        assert!(markets.iter().any(|m| m.id().as_str() == "BTC-USD"));
    }
}
