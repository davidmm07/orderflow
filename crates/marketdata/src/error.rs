//! Errors raised while building market data values.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MarketDataError {
    #[error("source ids are 1 to 64 characters of a-z, 0-9, '_', '-', '.' and ':'")]
    InvalidSourceId,
    #[error("prices must be greater than zero")]
    NonPositivePrice,
    #[error("unknown candle interval, expected one of 1m, 5m, 15m, 1h, 4h, 1d")]
    InvalidInterval,
    #[error("quorum must be at least 1")]
    InvalidQuorum,
    #[error("max deviation must be between 1 and 10000 basis points")]
    InvalidDeviation,
    #[error("market {0} is configured twice")]
    DuplicateMarket(String),
    #[error("source {source_id} is listed twice for market {market}")]
    DuplicateSource { market: String, source_id: String },
    #[error("market {0} has no index sources")]
    NoIndexSources(String),
}
