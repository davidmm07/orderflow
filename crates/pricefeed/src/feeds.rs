//! Loads `config/marketdata.json`: the markets, their sources and the
//! pools the indexers read.
//!
//! Feeds are data, like the exchange's instrument catalog. Adding a market,
//! a pool or a CEX pair is an edit to the file and a restart. Every entry
//! is validated and every mistake reported in one pass.

use std::{collections::BTreeMap, fs, path::Path, str::FromStr, time::Duration};

use alloy_primitives::Address;
use orderflow_domain::MarketId;
use orderflow_ingest::{
    chain::{
        evm::{EvmIndexerConfig, EvmPool},
        solana::{SolanaIndexerConfig, Whirlpool},
    },
    connectors::{CoinGeckoConfig, CoinGeckoQuote, CoinbaseConfig, KrakenConfig},
};
use orderflow_marketdata::{
    AggregationPolicy, MarketFeed, PriceIndex, SourceId, SourceKind, SourceRole, SourceSpec,
};
use serde::Deserialize;

use crate::config::ConfigError;

/// Largest token decimals accepted; real tokens use at most 18 or 24.
const MAX_DECIMALS: u8 = 36;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedsFile {
    aggregation: AggregationEntry,
    markets: Vec<MarketEntry>,
    sources: SourcesEntry,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregationEntry {
    quorum: usize,
    max_deviation_bps: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketEntry {
    market: String,
    quorum: Option<usize>,
    max_deviation_bps: Option<u32>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SourcesEntry {
    coingecko: Option<CoinGeckoEntry>,
    kraken: Option<KrakenEntry>,
    coinbase: Option<CoinbaseEntry>,
    uniswap_v3: Option<UniswapEntry>,
    orca: Option<OrcaEntry>,
    orderflow: Option<OrderflowEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoinGeckoEntry {
    base_url: String,
    poll_interval_secs: u64,
    max_age_secs: u64,
    /// Market to `coin_id/currency`.
    quotes: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KrakenEntry {
    base_url: String,
    poll_interval_secs: u64,
    max_age_secs: u64,
    pairs: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoinbaseEntry {
    url: String,
    sample_interval_ms: u64,
    idle_timeout_secs: u64,
    max_age_secs: u64,
    products: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UniswapEntry {
    chain: String,
    confirmations: u64,
    max_block_range: u64,
    backfill_blocks: u64,
    poll_interval_secs: u64,
    max_age_secs: u64,
    pools: Vec<UniswapPoolEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UniswapPoolEntry {
    id: String,
    address: String,
    market: String,
    base: String,
    token0_decimals: u8,
    token1_decimals: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrcaEntry {
    poll_interval_secs: u64,
    max_age_secs: u64,
    pools: Vec<OrcaPoolEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrcaPoolEntry {
    id: String,
    address: String,
    market: String,
    base: String,
    mint_a: String,
    mint_b: String,
    decimals_a: u8,
    decimals_b: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderflowEntry {
    role: String,
    max_age_secs: u64,
    markets: Vec<String>,
}

/// The validated feed configuration.
#[derive(Debug, Clone)]
pub struct Feeds {
    pub markets: Vec<MarketFeed>,
    pub coingecko: Option<CoinGeckoConfig>,
    pub kraken: Option<KrakenConfig>,
    pub coinbase: Option<CoinbaseConfig>,
    pub uniswap: Option<EvmIndexerConfig>,
    pub orca: Option<SolanaIndexerConfig>,
    pub orderflow: Option<Vec<MarketId>>,
}

impl Feeds {
    pub fn market_ids(&self) -> Vec<MarketId> {
        self.markets
            .iter()
            .map(|feed| feed.market.clone())
            .collect()
    }
}

pub fn load(path: &Path) -> Result<Feeds, ConfigError> {
    let text = fs::read_to_string(path)
        .map_err(|error| ConfigError(vec![format!("cannot read {}: {error}", path.display())]))?;
    parse(&text, &path.display().to_string())
}

pub fn parse(text: &str, origin: &str) -> Result<Feeds, ConfigError> {
    let file: FeedsFile = serde_json::from_str(text)
        .map_err(|error| ConfigError(vec![format!("{origin}: {error}")]))?;
    let mut builder = Builder::default();

    let default_policy = builder.policy(
        "aggregation",
        file.aggregation.quorum,
        file.aggregation.max_deviation_bps,
    );
    for entry in &file.markets {
        let Some(market) = builder.market(&entry.market, "markets") else {
            continue;
        };
        let policy = builder.policy(
            &entry.market,
            entry.quorum.unwrap_or(file.aggregation.quorum),
            entry
                .max_deviation_bps
                .unwrap_or(file.aggregation.max_deviation_bps),
        );
        if builder.sources.insert(market.clone(), Vec::new()).is_some() {
            builder.problem(format!("market {market} is listed twice"));
        }
        if let Some(policy) = policy.or(default_policy) {
            builder.policies.insert(market, policy);
        }
    }

    let sources = file.sources;
    let coingecko = sources.coingecko.map(|entry| builder.coingecko(entry));
    let kraken = sources.kraken.map(|entry| builder.kraken(entry));
    let coinbase = sources.coinbase.map(|entry| builder.coinbase(entry));
    let uniswap = sources.uniswap_v3.map(|entry| builder.uniswap(entry));
    let orca = sources.orca.map(|entry| builder.orca(entry));
    let orderflow = sources.orderflow.map(|entry| builder.orderflow(entry));

    let markets: Vec<MarketFeed> = builder
        .sources
        .iter()
        .filter_map(|(market, specs)| {
            builder.policies.get(market).map(|policy| MarketFeed {
                market: market.clone(),
                policy: *policy,
                sources: specs.clone(),
            })
        })
        .collect();
    if markets.is_empty() && builder.problems.is_empty() {
        builder.problem(format!("{origin} lists no markets"));
    }
    // The index constructor knows the remaining rules (an index source per
    // market, no duplicates); run it now so mistakes surface at startup.
    if let Err(error) = PriceIndex::new(0, markets.clone()) {
        builder.problem(error.to_string());
    }
    if !builder.problems.is_empty() {
        return Err(ConfigError(builder.problems));
    }
    Ok(Feeds {
        markets,
        coingecko,
        kraken,
        coinbase,
        uniswap,
        orca,
        orderflow,
    })
}

#[derive(Default)]
struct Builder {
    sources: BTreeMap<MarketId, Vec<SourceSpec>>,
    policies: BTreeMap<MarketId, AggregationPolicy>,
    problems: Vec<String>,
}

impl Builder {
    fn problem(&mut self, message: String) {
        self.problems.push(message);
    }

    fn policy(
        &mut self,
        label: &str,
        quorum: usize,
        max_deviation_bps: u32,
    ) -> Option<AggregationPolicy> {
        AggregationPolicy::new(quorum, max_deviation_bps)
            .map_err(|error| self.problem(format!("{label}: {error}")))
            .ok()
    }

    fn market(&mut self, raw: &str, origin: &str) -> Option<MarketId> {
        MarketId::parse(raw)
            .map_err(|_| self.problem(format!("{origin}: {raw:?} is not a market id like BTC-USD")))
            .ok()
    }

    fn source_id(&mut self, raw: &str) -> Option<SourceId> {
        SourceId::parse(raw)
            .map_err(|error| self.problem(format!("source {raw:?}: {error}")))
            .ok()
    }

    /// Registers `source` for `market`, which must be a listed market.
    fn attach(&mut self, connector: &str, market: &str, spec: SourceSpec) -> Option<MarketId> {
        let market = self.market(market, connector)?;
        match self.sources.get_mut(&market) {
            Some(specs) => {
                specs.push(spec);
                Some(market)
            }
            None => {
                self.problem(format!(
                    "{connector} quotes {market}, which is not in markets"
                ));
                None
            }
        }
    }

    fn spec(
        &mut self,
        id: &str,
        kind: SourceKind,
        role: SourceRole,
        max_age_secs: u64,
    ) -> Option<SourceSpec> {
        if max_age_secs == 0 {
            self.problem(format!("{id}: max_age_secs must be greater than zero"));
        }
        Some(SourceSpec {
            id: self.source_id(id)?,
            kind,
            role,
            max_age: Duration::from_secs(max_age_secs),
        })
    }

    fn interval(&mut self, label: &str, value: Duration) -> Duration {
        if value.is_zero() {
            self.problem(format!("{label} must be greater than zero"));
        }
        value
    }

    fn coingecko(&mut self, entry: CoinGeckoEntry) -> CoinGeckoConfig {
        let mut quotes = Vec::new();
        for (market, quote) in &entry.quotes {
            let Some((coin_id, currency)) = quote.split_once('/') else {
                self.problem(format!(
                    "coingecko {market}: expected coin_id/currency, got {quote:?}"
                ));
                continue;
            };
            let Some(spec) = self.spec(
                "coingecko",
                SourceKind::Aggregator,
                SourceRole::Index,
                entry.max_age_secs,
            ) else {
                continue;
            };
            if let Some(market) = self.attach("coingecko", market, spec) {
                quotes.push(CoinGeckoQuote {
                    market,
                    coin_id: coin_id.to_owned(),
                    vs_currency: currency.to_owned(),
                });
            }
        }
        CoinGeckoConfig {
            base_url: entry.base_url,
            api_key: None,
            poll_interval: self.interval(
                "coingecko poll_interval_secs",
                Duration::from_secs(entry.poll_interval_secs),
            ),
            quotes,
        }
    }

    fn kraken(&mut self, entry: KrakenEntry) -> KrakenConfig {
        let mut pairs = Vec::new();
        for (market, pair) in &entry.pairs {
            let Some(spec) = self.spec(
                "kraken",
                SourceKind::Cex,
                SourceRole::Index,
                entry.max_age_secs,
            ) else {
                continue;
            };
            if let Some(market) = self.attach("kraken", market, spec) {
                pairs.push((market, pair.clone()));
            }
        }
        KrakenConfig {
            base_url: entry.base_url,
            poll_interval: self.interval(
                "kraken poll_interval_secs",
                Duration::from_secs(entry.poll_interval_secs),
            ),
            pairs,
        }
    }

    fn coinbase(&mut self, entry: CoinbaseEntry) -> CoinbaseConfig {
        let mut products = Vec::new();
        for (market, product) in &entry.products {
            let Some(spec) = self.spec(
                "coinbase",
                SourceKind::Cex,
                SourceRole::Index,
                entry.max_age_secs,
            ) else {
                continue;
            };
            if let Some(market) = self.attach("coinbase", market, spec) {
                products.push((market, product.clone()));
            }
        }
        CoinbaseConfig {
            url: entry.url,
            products,
            sample_interval: self.interval(
                "coinbase sample_interval_ms",
                Duration::from_millis(entry.sample_interval_ms),
            ),
            idle_timeout: self.interval(
                "coinbase idle_timeout_secs",
                Duration::from_secs(entry.idle_timeout_secs),
            ),
        }
    }

    fn uniswap(&mut self, entry: UniswapEntry) -> EvmIndexerConfig {
        let mut pools = Vec::new();
        for pool in &entry.pools {
            let label = format!("uniswap_v3 pool {}", pool.id);
            let Ok(address) = Address::from_str(&pool.address) else {
                self.problem(format!("{label}: {:?} is not an address", pool.address));
                continue;
            };
            let base_is_token0 = match pool.base.as_str() {
                "token0" => true,
                "token1" => false,
                other => {
                    self.problem(format!(
                        "{label}: base must be token0 or token1, got {other:?}"
                    ));
                    continue;
                }
            };
            if pool.token0_decimals > MAX_DECIMALS || pool.token1_decimals > MAX_DECIMALS {
                self.problem(format!("{label}: decimals above {MAX_DECIMALS}"));
                continue;
            }
            let id = format!("uniswap_v3:{}", pool.id);
            let Some(spec) = self.spec(&id, SourceKind::Dex, SourceRole::Index, entry.max_age_secs)
            else {
                continue;
            };
            let source = spec.id.clone();
            if let Some(market) = self.attach("uniswap_v3", &pool.market, spec) {
                pools.push(EvmPool {
                    source,
                    address,
                    market,
                    base_is_token0,
                    decimals0: pool.token0_decimals,
                    decimals1: pool.token1_decimals,
                });
            }
        }
        if entry.max_block_range == 0 {
            self.problem("uniswap_v3 max_block_range must be greater than zero".into());
        }
        EvmIndexerConfig {
            name: format!("uniswap_v3:{}", entry.chain),
            confirmations: entry.confirmations,
            max_block_range: entry.max_block_range,
            backfill_blocks: entry.backfill_blocks,
            poll_interval: self.interval(
                "uniswap_v3 poll_interval_secs",
                Duration::from_secs(entry.poll_interval_secs),
            ),
            pools,
        }
    }

    fn orca(&mut self, entry: OrcaEntry) -> SolanaIndexerConfig {
        let mut pools = Vec::new();
        for pool in &entry.pools {
            let base_is_a = match pool.base.as_str() {
                "token_a" => true,
                "token_b" => false,
                other => {
                    self.problem(format!(
                        "orca pool {}: base must be token_a or token_b, got {other:?}",
                        pool.id
                    ));
                    continue;
                }
            };
            if pool.decimals_a > MAX_DECIMALS || pool.decimals_b > MAX_DECIMALS {
                self.problem(format!(
                    "orca pool {}: decimals above {MAX_DECIMALS}",
                    pool.id
                ));
                continue;
            }
            let id = format!("orca:{}", pool.id);
            let Some(spec) = self.spec(&id, SourceKind::Dex, SourceRole::Index, entry.max_age_secs)
            else {
                continue;
            };
            let source = spec.id.clone();
            if let Some(market) = self.attach("orca", &pool.market, spec) {
                pools.push(Whirlpool {
                    source,
                    address: pool.address.clone(),
                    market,
                    mint_a: pool.mint_a.clone(),
                    mint_b: pool.mint_b.clone(),
                    decimals_a: pool.decimals_a,
                    decimals_b: pool.decimals_b,
                    base_is_a,
                });
            }
        }
        SolanaIndexerConfig {
            name: "orca:solana".into(),
            poll_interval: self.interval(
                "orca poll_interval_secs",
                Duration::from_secs(entry.poll_interval_secs),
            ),
            pools,
        }
    }

    fn orderflow(&mut self, entry: OrderflowEntry) -> Vec<MarketId> {
        let Some(role) = SourceRole::parse(&entry.role) else {
            self.problem(format!(
                "orderflow role must be index or monitor, got {:?}",
                entry.role
            ));
            return Vec::new();
        };
        let mut markets = Vec::new();
        for market in &entry.markets {
            let Some(spec) = self.spec("orderflow", SourceKind::Venue, role, entry.max_age_secs)
            else {
                continue;
            };
            if let Some(market) = self.attach("orderflow", market, spec) {
                markets.push(market);
            }
        }
        markets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_feeds_are_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/marketdata.json");
        let feeds = load(&path).unwrap();
        assert!(feeds.markets.len() >= 15, "found {}", feeds.markets.len());
        let eth_usdc = feeds
            .markets
            .iter()
            .find(|feed| feed.market.as_str() == "ETH-USDC")
            .unwrap();
        let ids: Vec<&str> = eth_usdc
            .sources
            .iter()
            .map(|spec| spec.id.as_str())
            .collect();
        assert!(
            ids.contains(&"uniswap_v3:usdc_weth_500") && ids.contains(&"kraken"),
            "{ids:?}"
        );
        assert_eq!(eth_usdc.policy.max_deviation_bps(), 100);
        assert_eq!(feeds.uniswap.as_ref().unwrap().pools.len(), 4);
        assert_eq!(feeds.orca.as_ref().unwrap().pools.len(), 2);
    }

    #[test]
    fn every_mistake_is_reported() {
        let text = r#"{
            "aggregation": {"quorum": 0, "max_deviation_bps": 200},
            "markets": [{"market": "BTC-USD"}, {"market": "btc"}],
            "sources": {
                "kraken": {"base_url": "x", "poll_interval_secs": 5, "max_age_secs": 60,
                           "pairs": {"BTC-USD": "XXBTZUSD", "ETH-USD": "XETHZUSD"}},
                "uniswap_v3": {"chain": "ethereum", "confirmations": 3, "max_block_range": 500,
                    "backfill_blocks": 0, "poll_interval_secs": 12, "max_age_secs": 120,
                    "pools": [{"id": "p", "address": "0x12", "market": "BTC-USD", "base": "token0",
                               "token0_decimals": 8, "token1_decimals": 6}]}
            }
        }"#;
        let problems = parse(text, "test").unwrap_err().0;
        let joined = problems.join("\n");
        assert!(joined.contains("aggregation: quorum"), "{joined}");
        assert!(joined.contains("\"btc\" is not a market id"), "{joined}");
        assert!(
            joined.contains("kraken quotes ETH-USD, which is not in markets"),
            "{joined}"
        );
        assert!(joined.contains("is not an address"), "{joined}");
    }

    #[test]
    fn unknown_fields_are_refused() {
        let text = r#"{"aggregation": {"quorum": 1, "max_deviation_bps": 1, "typo": 1}, "markets": [], "sources": {}}"#;
        assert!(parse(text, "test").is_err());
    }
}
