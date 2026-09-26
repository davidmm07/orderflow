//! The assets and markets the exchange lists.

use std::collections::BTreeMap;

use crate::{
    asset::{Asset, AssetCode},
    error::DomainError,
    market::{MarketId, MarketSpec},
};

/// Every listed asset and market, validated as a whole.
///
/// Pattern: Registry. Instruments are data: listing a market means adding
/// an entry to the configuration file, with no code change. This type
/// enforces the rules no single entry can check on its own. Market ids are
/// unique, every market trades two listed assets, and a lot size never
/// asks for more decimal places than the base asset has.
#[derive(Debug, Clone, Default)]
pub struct InstrumentCatalog {
    assets: BTreeMap<AssetCode, Asset>,
    markets: BTreeMap<MarketId, MarketSpec>,
}

impl InstrumentCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_asset(&mut self, asset: Asset) -> Result<(), DomainError> {
        if self.assets.contains_key(asset.code()) {
            return Err(DomainError::DuplicateAsset(asset.code().clone()));
        }
        self.assets.insert(asset.code().clone(), asset);
        Ok(())
    }

    pub fn add_market(&mut self, spec: MarketSpec) -> Result<(), DomainError> {
        let id = spec.id();
        if self.markets.contains_key(id) {
            return Err(DomainError::DuplicateMarket(id.clone()));
        }
        let base = self.listed(id.base())?;
        self.listed(id.quote())?;
        if spec.lot_size().scale() > base.decimals() {
            return Err(DomainError::LotSizeTooPrecise {
                market: id.clone(),
                lot_size: spec.lot_size(),
                decimals: base.decimals(),
            });
        }
        self.markets.insert(id.clone(), spec);
        Ok(())
    }

    pub fn asset(&self, code: &AssetCode) -> Option<&Asset> {
        self.assets.get(code)
    }

    pub fn market(&self, id: &MarketId) -> Option<&MarketSpec> {
        self.markets.get(id)
    }

    /// Assets sorted by code.
    pub fn assets(&self) -> impl Iterator<Item = &Asset> {
        self.assets.values()
    }

    /// Markets sorted by id.
    pub fn markets(&self) -> impl Iterator<Item = &MarketSpec> {
        self.markets.values()
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub fn into_parts(self) -> (Vec<Asset>, Vec<MarketSpec>) {
        (
            self.assets.into_values().collect(),
            self.markets.into_values().collect(),
        )
    }

    fn listed(&self, code: &str) -> Result<&Asset, DomainError> {
        AssetCode::parse(code)
            .ok()
            .and_then(|code| self.assets.get(&code))
            .ok_or_else(|| DomainError::UnknownAsset(code.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn asset(code: &str, decimals: u32) -> Asset {
        Asset::new(AssetCode::parse(code).unwrap(), code, decimals).unwrap()
    }

    fn market(id: &str, lot: rust_decimal::Decimal) -> MarketSpec {
        MarketSpec::builder(MarketId::parse(id).unwrap())
            .tick_size(dec!(0.01))
            .lot_size(lot)
            .max_quantity(dec!(1000))
            .build()
            .unwrap()
    }

    fn catalog() -> InstrumentCatalog {
        let mut catalog = InstrumentCatalog::new();
        catalog.add_asset(asset("BTC", 8)).unwrap();
        catalog.add_asset(asset("USD", 2)).unwrap();
        catalog
    }

    #[test]
    fn markets_must_trade_listed_assets() {
        let mut catalog = catalog();
        assert!(catalog.add_market(market("BTC-USD", dec!(0.0001))).is_ok());
        assert_eq!(
            catalog.add_market(market("ETH-USD", dec!(0.01))),
            Err(DomainError::UnknownAsset("ETH".into()))
        );
        assert_eq!(catalog.market_count(), 1);
    }

    #[test]
    fn duplicates_are_rejected() {
        let mut catalog = catalog();
        assert!(matches!(
            catalog.add_asset(asset("BTC", 8)),
            Err(DomainError::DuplicateAsset(_))
        ));
        catalog.add_market(market("BTC-USD", dec!(0.0001))).unwrap();
        assert!(matches!(
            catalog.add_market(market("BTC-USD", dec!(0.0001))),
            Err(DomainError::DuplicateMarket(_))
        ));
    }

    #[test]
    fn lot_size_cannot_exceed_base_precision() {
        let mut catalog = catalog();
        assert!(matches!(
            catalog.add_market(market("BTC-USD", dec!(0.000000001))),
            Err(DomainError::LotSizeTooPrecise { decimals: 8, .. })
        ));
    }

    #[test]
    fn market_ids_are_built_from_two_different_assets() {
        let btc = AssetCode::parse("BTC").unwrap();
        let usd = AssetCode::parse("USD").unwrap();
        assert_eq!(
            MarketId::from_assets(&btc, &usd).unwrap().as_str(),
            "BTC-USD"
        );
        assert!(MarketId::from_assets(&btc, &btc).is_err());
    }
}
