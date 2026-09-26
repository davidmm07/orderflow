//! Lookup of running markets.

use std::collections::BTreeMap;

use orderflow_domain::{MarketId, MarketSpec};

use crate::{error::ApplicationError, market::MarketHandle};

/// Maps each listed market to the actor that owns its book.
///
/// Pattern: Registry. Built once at startup and shared read-only, so lookups
/// need no locking. A `BTreeMap` keeps market listings in a stable order.
#[derive(Debug, Default)]
pub struct MarketRegistry {
    markets: BTreeMap<MarketId, MarketHandle>,
}

impl MarketRegistry {
    pub fn new(handles: impl IntoIterator<Item = MarketHandle>) -> Self {
        let markets = handles
            .into_iter()
            .map(|handle| (handle.spec().id().clone(), handle))
            .collect();
        Self { markets }
    }

    pub fn get(&self, id: &MarketId) -> Result<&MarketHandle, ApplicationError> {
        self.markets
            .get(id)
            .ok_or_else(|| ApplicationError::UnknownMarket(id.clone()))
    }

    pub fn specs(&self) -> impl Iterator<Item = &MarketSpec> {
        self.markets.values().map(MarketHandle::spec)
    }

    /// True when every market actor is alive.
    pub fn all_running(&self) -> bool {
        !self.markets.is_empty() && self.markets.values().all(MarketHandle::is_running)
    }
}
