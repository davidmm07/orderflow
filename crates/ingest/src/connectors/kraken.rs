//! Kraken: last trade price from the public ticker endpoint.

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{Observation, SourceId};
use serde::Deserialize;

use super::parse_decimal;
use crate::{
    error::SourceError,
    http::HttpClient,
    runtime::{Source, SourceContext},
};

#[derive(Debug, Clone)]
pub struct KrakenConfig {
    /// `https://api.kraken.com/0/public`.
    pub base_url: String,
    pub poll_interval: Duration,
    /// Market and Kraken pair name, such as `BTC-USD` and `XXBTZUSD`. Kraken
    /// answers with its canonical pair names, so the canonical name is the
    /// one configured.
    pub pairs: Vec<(MarketId, String)>,
}

#[derive(Debug, Deserialize)]
struct TickerResponse {
    error: Vec<String>,
    #[serde(default)]
    result: HashMap<String, Ticker>,
}

#[derive(Debug, Deserialize)]
struct Ticker {
    /// Last trade: `[price, lot volume]`.
    c: Vec<String>,
}

/// Polls the ticker of every configured pair in one request.
pub struct Kraken {
    http: HttpClient,
    url: String,
    poll_interval: Duration,
    pairs: Vec<(MarketId, String)>,
    source: SourceId,
}

impl Kraken {
    pub const SOURCE: &'static str = "kraken";

    pub fn new(http: HttpClient, config: KrakenConfig) -> Result<Self, SourceError> {
        if config
            .pairs
            .iter()
            .any(|(_, pair)| pair.is_empty() || !pair.bytes().all(|b| b.is_ascii_alphanumeric()))
        {
            return Err(SourceError::decode("kraken pair names are alphanumeric"));
        }
        let names: Vec<&str> = config.pairs.iter().map(|(_, pair)| pair.as_str()).collect();
        Ok(Self {
            http,
            url: format!(
                "{}/Ticker?pair={}",
                config.base_url.trim_end_matches('/'),
                names.join(",")
            ),
            poll_interval: config.poll_interval,
            pairs: config.pairs,
            source: SourceId::parse(Self::SOURCE).map_err(SourceError::decode)?,
        })
    }
}

#[async_trait]
impl Source for Kraken {
    fn name(&self) -> &str {
        Self::SOURCE
    }

    async fn run(&self, ctx: &SourceContext) -> Result<(), SourceError> {
        loop {
            let response: TickerResponse = self.http.get_json(&self.url, &[]).await?;
            // The ticker carries no timestamp: the price is "last trade as of
            // now", so the receive time is the observation time.
            let now = ctx.clock.now();
            for observation in parse_ticker(response, &self.pairs, &self.source, now)? {
                ctx.emit(observation).await?;
            }
            if !ctx.pause(self.poll_interval).await {
                return Ok(());
            }
        }
    }
}

fn parse_ticker(
    response: TickerResponse,
    pairs: &[(MarketId, String)],
    source: &SourceId,
    now: Timestamp,
) -> Result<Vec<Observation>, SourceError> {
    if let Some(error) = response.error.first() {
        return Err(SourceError::upstream(error));
    }
    let mut observations = Vec::with_capacity(pairs.len());
    for (market, pair) in pairs {
        let Some(price) = response
            .result
            .get(pair)
            .and_then(|ticker| ticker.c.first())
        else {
            tracing::debug!(pair, "kraken omitted a pair");
            continue;
        };
        let observation =
            Observation::new(market.clone(), source.clone(), parse_decimal(price)?, now)
                .map_err(SourceError::decode)?;
        observations.push(observation);
    }
    Ok(observations)
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn recorded_ticker_parses_into_observations() {
        let response: TickerResponse =
            serde_json::from_str(include_str!("../../tests/fixtures/kraken_ticker.json")).unwrap();
        let pairs = vec![
            (MarketId::parse("BTC-USD").unwrap(), "XXBTZUSD".to_owned()),
            (MarketId::parse("ETH-USDC").unwrap(), "ETHUSDC".to_owned()),
        ];
        let source = SourceId::parse("kraken").unwrap();
        let now = Timestamp::from_unix_nanos(42);
        let observations = parse_ticker(response, &pairs, &source, now).unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].price(), dec!(81711.4));
        assert_eq!(observations[1].price(), dec!(2479.29));
        assert_eq!(observations[1].observed_at(), now);
    }

    #[test]
    fn upstream_errors_fail_the_poll() {
        let response: TickerResponse =
            serde_json::from_str(r#"{"error":["EQuery:Unknown asset pair"]}"#).unwrap();
        let error = parse_ticker(
            response,
            &[],
            &SourceId::parse("kraken").unwrap(),
            Timestamp::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), "upstream");
    }
}
