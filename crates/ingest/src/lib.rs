//! Market data sources: where observations come from.
//!
//! Every source turns an outside system into `Observation`s and nothing
//! else. Off-chain connectors poll or stream exchange and aggregator APIs;
//! chain indexers read DEX pools straight from Ethereum and Solana nodes, so
//! on-chain prices do not depend on a third party's API at all.
//!
//! | Source            | Kind       | Transport                       |
//! |-------------------|------------|---------------------------------|
//! | CoinGecko         | aggregator | REST poll                       |
//! | Kraken            | CEX        | REST poll                       |
//! | Coinbase Exchange | CEX        | WebSocket stream                |
//! | Uniswap v3        | DEX        | EVM JSON-RPC log indexer        |
//! | Orca Whirlpools   | DEX        | Solana JSON-RPC account indexer |
//! | Orderflow         | venue      | Kafka consumer (`kafka` feature)|
//!
//! Patterns: every source implements `Source` (Strategy), and `supervise`
//! restarts a failed source with backoff (Supervisor), so one broken
//! upstream never takes the others down.

pub mod chain;
pub mod connectors;
mod error;
mod http;
mod runtime;
pub mod venue;

pub use error::SourceError;
pub use http::{HttpClient, install_crypto_provider};
pub use runtime::{Source, SourceContext, supervise};
