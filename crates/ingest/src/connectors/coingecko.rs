//! CoinGecko: an aggregator that publishes prices it computed from many
//! exchanges. Useful as a sanity anchor, slow to update (about a minute).

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{Observation, SourceId};
use serde_json::value::RawValue;

use super::parse_decimal;
use crate::{
    error::SourceError,
    http::HttpClient,
    runtime::{Source, SourceContext},
};

/// One market priced from one CoinGecko coin, such as `ETH-EUR` from
/// coin `ethereum` in currency `eur`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinGeckoQuote {
    pub market: MarketId,
    pub coin_id: String,
    pub vs_currency: String,
}

#[derive(Clone)]
pub struct CoinGeckoConfig {
    /// `https://api.coingecko.com/api/v3`, or the pro host for paid keys.
    pub base_url: String,
    /// Optional key from the environment. Never logged.
    pub api_key: Option<String>,
    pub poll_interval: Duration,
    pub quotes: Vec<CoinGeckoQuote>,
}

impl std::fmt::Debug for CoinGeckoConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoinGeckoConfig")
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("poll_interval", &self.poll_interval)
            .field("quotes", &self.quotes)
            .finish()
    }
}

/// Polls `/simple/price` for every configured coin in one request, which
/// keeps well inside the free tier's rate limit.
pub struct CoinGecko {
    http: HttpClient,
    url: String,
    api_key: Option<(&'static str, String)>,
    poll_interval: Duration,
    quotes: Vec<CoinGeckoQuote>,
    source: SourceId,
}

impl CoinGecko {
    pub const SOURCE: &'static str = "coingecko";

    pub fn new(http: HttpClient, config: CoinGeckoConfig) -> Result<Self, SourceError> {
        let mut ids: Vec<&str> = Vec::new();
        let mut currencies: Vec<&str> = Vec::new();
        for quote in &config.quotes {
            // Both values go into the URL, so only plain ids are allowed.
            let id_ok = !quote.coin_id.is_empty()
                && quote
                    .coin_id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
            let currency_ok = !quote.vs_currency.is_empty()
                && quote.vs_currency.bytes().all(|b| b.is_ascii_lowercase());
            if !id_ok || !currency_ok {
                return Err(SourceError::decode(format_args!(
                    "invalid coingecko mapping for {}",
                    quote.market
                )));
            }
            if !ids.contains(&quote.coin_id.as_str()) {
                ids.push(&quote.coin_id);
            }
            if !currencies.contains(&quote.vs_currency.as_str()) {
                currencies.push(&quote.vs_currency);
            }
        }
        let url = format!(
            "{}/simple/price?ids={}&vs_currencies={}&include_last_updated_at=true&precision=full",
            config.base_url.trim_end_matches('/'),
            ids.join(","),
            currencies.join(",")
        );
        // Demo and paid keys travel in different headers.
        let header = if config.base_url.contains("pro-api") {
            "x-cg-pro-api-key"
        } else {
            "x-cg-demo-api-key"
        };
        Ok(Self {
            http,
            url,
            api_key: config.api_key.map(|key| (header, key)),
            poll_interval: config.poll_interval,
            quotes: config.quotes,
            source: SourceId::parse(Self::SOURCE).map_err(SourceError::decode)?,
        })
    }
}

#[async_trait]
impl Source for CoinGecko {
    fn name(&self) -> &str {
        Self::SOURCE
    }

    async fn run(&self, ctx: &SourceContext) -> Result<(), SourceError> {
        loop {
            let headers: Vec<(&'static str, &str)> = self
                .api_key
                .iter()
                .map(|(name, key)| (*name, key.as_str()))
                .collect();
            let body: HashMap<String, HashMap<String, Box<RawValue>>> =
                self.http.get_json(&self.url, &headers).await?;
            for observation in parse_prices(&body, &self.quotes, &self.source)? {
                ctx.emit(observation).await?;
            }
            if !ctx.pause(self.poll_interval).await {
                return Ok(());
            }
        }
    }
}

/// Turns a `/simple/price` body into observations. Numbers are read from
/// the raw JSON text, so `81651.20865616859` arrives exactly as sent rather
/// than rounded through an `f64`.
pub(crate) fn parse_prices(
    body: &HashMap<String, HashMap<String, Box<RawValue>>>,
    quotes: &[CoinGeckoQuote],
    source: &SourceId,
) -> Result<Vec<Observation>, SourceError> {
    let mut observations = Vec::with_capacity(quotes.len());
    for quote in quotes {
        let Some(coin) = body.get(&quote.coin_id) else {
            tracing::debug!(coin = quote.coin_id, "coingecko omitted a coin");
            continue;
        };
        let (Some(price), Some(updated)) =
            (coin.get(&quote.vs_currency), coin.get("last_updated_at"))
        else {
            continue;
        };
        let seconds: u64 = updated
            .get()
            .parse()
            .map_err(|_| SourceError::decode("last_updated_at is not an integer"))?;
        let observed_at = Timestamp::from_unix_nanos(seconds.saturating_mul(1_000_000_000));
        let observation = Observation::new(
            quote.market.clone(),
            source.clone(),
            parse_decimal(price.get())?,
            observed_at,
        )
        .map_err(SourceError::decode)?;
        observations.push(observation);
    }
    if observations.is_empty() && !quotes.is_empty() {
        return Err(SourceError::decode("no configured coin in the response"));
    }
    Ok(observations)
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn quote(market: &str, coin: &str, currency: &str) -> CoinGeckoQuote {
        CoinGeckoQuote {
            market: MarketId::parse(market).unwrap(),
            coin_id: coin.into(),
            vs_currency: currency.into(),
        }
    }

    #[test]
    fn recorded_response_parses_into_exact_observations() {
        let body = serde_json::from_str(include_str!(
            "../../tests/fixtures/coingecko_simple_price.json"
        ))
        .unwrap();
        let source = SourceId::parse("coingecko").unwrap();
        let quotes = [
            quote("BTC-USD", "bitcoin", "usd"),
            quote("ETH-EUR", "ethereum", "eur"),
            quote("DOGE-USD", "dogecoin", "usd"),
        ];
        let observations = parse_prices(&body, &quotes, &source).unwrap();
        assert_eq!(observations.len(), 2, "dogecoin was not in the response");
        assert_eq!(observations[0].price(), dec!(81651.20865616859));
        assert_eq!(
            observations[0].observed_at().unix_nanos(),
            1_791_497_322_000_000_000
        );
        assert_eq!(observations[1].market().as_str(), "ETH-EUR");
        assert_eq!(observations[1].price(), dec!(2209.7731319843647));
    }

    #[test]
    fn mappings_cannot_inject_into_the_url() {
        let config = CoinGeckoConfig {
            base_url: "https://api.coingecko.com/api/v3".into(),
            api_key: None,
            poll_interval: Duration::from_secs(30),
            quotes: vec![quote("BTC-USD", "bitcoin&x=1", "usd")],
        };
        let http = HttpClient::new(Duration::from_secs(5)).unwrap();
        assert!(CoinGecko::new(http, config).is_err());
    }

    #[test]
    fn debug_output_redacts_the_api_key() {
        let config = CoinGeckoConfig {
            base_url: "https://api.coingecko.com/api/v3".into(),
            api_key: Some("CG-secret-value".into()),
            poll_interval: Duration::from_secs(30),
            quotes: Vec::new(),
        };
        assert!(!format!("{config:?}").contains("CG-secret-value"));
    }
}
