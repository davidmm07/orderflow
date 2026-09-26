//! Loads the instrument catalog: every asset and market the exchange lists.
//!
//! Instruments are data. Listing a new market is an edit to
//! `config/instruments.json` and a restart; no code changes. Market
//! parameters are not secret, so they live in a versioned file that goes
//! through code review, rather than in the environment.

use std::{fs, path::Path};

use orderflow_domain::{
    Asset, AssetCode, Decimal, DomainError, InstrumentCatalog, MarketId, MarketSpec,
};
use serde::Deserialize;

use crate::config::ConfigError;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentsFile {
    assets: Vec<AssetEntry>,
    markets: Vec<MarketEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssetEntry {
    code: String,
    name: String,
    decimals: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketEntry {
    base: String,
    quote: String,
    tick_size: String,
    lot_size: String,
    min_quantity: Option<String>,
    max_quantity: String,
}

/// Reads and validates the catalog at `path`.
pub fn load(path: &Path) -> Result<InstrumentCatalog, ConfigError> {
    let text = fs::read_to_string(path)
        .map_err(|error| ConfigError(vec![format!("cannot read {}: {error}", path.display())]))?;
    parse(&text, &path.display().to_string())
}

/// Builds the catalog from JSON text. Every invalid entry is reported, not
/// only the first, so a bad listing is fixed in one pass.
pub fn parse(text: &str, source: &str) -> Result<InstrumentCatalog, ConfigError> {
    let file: InstrumentsFile = serde_json::from_str(text)
        .map_err(|error| ConfigError(vec![format!("{source}: {error}")]))?;

    let mut catalog = InstrumentCatalog::new();
    let mut problems = Vec::new();

    for entry in &file.assets {
        let added = AssetCode::parse(&entry.code)
            .and_then(|code| Asset::new(code, &entry.name, entry.decimals))
            .and_then(|asset| catalog.add_asset(asset));
        if let Err(error) = added {
            problems.push(format!("asset {:?}: {error}", entry.code));
        }
    }
    for entry in &file.markets {
        let label = format!("{}-{}", entry.base, entry.quote);
        let added = market_spec(entry).and_then(|spec| {
            catalog
                .add_market(spec)
                .map_err(|error: DomainError| error.to_string())
        });
        if let Err(message) = added {
            problems.push(format!("market {label:?}: {message}"));
        }
    }
    if catalog.market_count() == 0 && problems.is_empty() {
        problems.push(format!("{source} lists no markets"));
    }

    if problems.is_empty() {
        Ok(catalog)
    } else {
        Err(ConfigError(problems))
    }
}

fn market_spec(entry: &MarketEntry) -> Result<MarketSpec, String> {
    let decimal = |field: &str, raw: &str| {
        Decimal::from_str_exact(raw).map_err(|_| format!("{field} {raw:?} is not a decimal"))
    };
    let asset = |raw: &str| AssetCode::parse(raw).map_err(|error| error.to_string());
    let id = MarketId::from_assets(&asset(&entry.base)?, &asset(&entry.quote)?)
        .map_err(|error| error.to_string())?;
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
    use super::*;

    fn problems(text: &str) -> Vec<String> {
        parse(text, "test").unwrap_err().0
    }

    #[test]
    fn bundled_catalog_is_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/instruments.json");
        let catalog = load(&path).unwrap();
        assert!(
            catalog.market_count() >= 15,
            "found {}",
            catalog.market_count()
        );
        for id in [
            "BTC-USD", "ETH-USD", "SOL-USD", "ETH-BTC", "BTC-EUR", "USDT-USD",
        ] {
            assert!(
                catalog.market(&MarketId::parse(id).unwrap()).is_some(),
                "{id} missing"
            );
        }
    }

    #[test]
    fn every_bad_entry_is_reported() {
        let text = r#"{
            "assets": [
                { "code": "BTC", "name": "Bitcoin", "decimals": 8 },
                { "code": "USD", "name": "US Dollar", "decimals": 2 },
                { "code": "usd", "name": "lowercase", "decimals": 2 }
            ],
            "markets": [
                { "base": "BTC", "quote": "USD", "tick_size": "0.01", "lot_size": "0.0001", "max_quantity": "10" },
                { "base": "DOGE", "quote": "USD", "tick_size": "0.0001", "lot_size": "1", "max_quantity": "10" },
                { "base": "BTC", "quote": "USD", "tick_size": "0.01", "lot_size": "0.0001", "max_quantity": "10" },
                { "base": "BTC", "quote": "BTC", "tick_size": "0.01", "lot_size": "0.0001", "max_quantity": "10" },
                { "base": "BTC", "quote": "USD", "tick_size": "abc", "lot_size": "0.0001", "max_quantity": "10" }
            ]
        }"#;
        let found = problems(text);
        assert_eq!(found.len(), 5, "{found:#?}");
        assert!(found.iter().any(|p| p.contains("asset \"usd\"")));
        assert!(found.iter().any(|p| p.contains("asset DOGE is not listed")));
        assert!(found.iter().any(|p| p.contains("listed twice")));
        assert!(found.iter().any(|p| p.contains("different assets")));
        assert!(found.iter().any(|p| p.contains("is not a decimal")));
    }

    #[test]
    fn lot_size_must_fit_the_base_asset_precision() {
        let text = r#"{
            "assets": [
                { "code": "ADA", "name": "Cardano", "decimals": 6 },
                { "code": "USD", "name": "US Dollar", "decimals": 2 }
            ],
            "markets": [
                { "base": "ADA", "quote": "USD", "tick_size": "0.0001", "lot_size": "0.0000001", "min_quantity": "1", "max_quantity": "10" }
            ]
        }"#;
        assert!(problems(text)[0].contains("finer than the 6 decimals"));
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let text = r#"{ "assets": [], "markets": [], "extra": true }"#;
        assert!(problems(text)[0].contains("unknown field"));
    }
}
