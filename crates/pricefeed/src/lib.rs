//! Price feed service: one reliable backend for market data.
//!
//! Third-party APIs (CoinGecko, Kraken, Coinbase), in-house chain indexers
//! (Uniswap v3 on Ethereum, Orca on Solana) and Orderflow's own trades feed
//! one aggregator, which publishes an index price per market. Clients read
//! it over REST or subscribe over WebSocket; history lives in Postgres
//! behind two cache tiers.
//!
//! The binary runs in one of three roles (see `config::Role`): `all` for
//! local use, or `ingest` plus any number of `edge` nodes connected through
//! Redis for production. That split is what lets WebSocket fanout scale
//! out without running the sources more than once.

pub mod app;
pub mod config;
pub mod dto;
pub mod feeds;
pub mod http;
pub mod hub;
pub mod metrics;
pub mod problem;
pub mod state;
pub mod telemetry;
pub mod ws;
