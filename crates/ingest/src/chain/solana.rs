//! Orca Whirlpool indexer for Solana.
//!
//! Solana programs keep state in accounts, so this indexer reads the pool
//! accounts themselves: one `getMultipleAccounts` call returns every
//! configured pool at a single slot, and the price comes from the pool's
//! `sqrt_price` field. That is one cheap call per poll, which suits public
//! RPC endpoints. Indexing every swap transaction instead would take a
//! Geyser plugin or a paid streaming provider.
//!
//! Consistency: requests carry `minContextSlot` with the last slot seen.
//! Public endpoints sit behind load balancers, and without it a lagging
//! node could answer with older state than the previous poll.

use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy_primitives::U256;
use async_trait::async_trait;
use base64::Engine as _;
use orderflow_domain::MarketId;
use orderflow_marketdata::{
    Observation, SourceId,
    ports::{ChainStore, PoolStateRecord},
};
use serde::Deserialize;
use serde_json::json;

use super::{
    math::price_from_sqrt,
    rpc::{RpcTransport, call},
};
use crate::{
    error::SourceError,
    runtime::{Source, SourceContext},
};

/// Program that owns every Whirlpool account.
pub const WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

/// Anchor account discriminator: the first 8 bytes of
/// `sha256("account:Whirlpool")`.
const DISCRIMINATOR: [u8; 8] = [0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09];

// Field offsets in the Whirlpool account (Anchor layout, little endian).
const SQRT_PRICE: std::ops::Range<usize> = 65..81;
const TICK: std::ops::Range<usize> = 81..85;
const MINT_A: std::ops::Range<usize> = 101..133;
const MINT_B: std::ops::Range<usize> = 181..213;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Whirlpool {
    /// Such as `orca:sol_usdc_4`.
    pub source: SourceId,
    /// Base58 address of the pool account.
    pub address: String,
    pub market: MarketId,
    /// Base58 mints the pool must hold, checked on every read.
    pub mint_a: String,
    pub mint_b: String,
    pub decimals_a: u8,
    pub decimals_b: u8,
    /// Whether the market's base asset is token A.
    pub base_is_a: bool,
}

#[derive(Debug, Clone)]
pub struct SolanaIndexerConfig {
    /// Such as `orca:solana`.
    pub name: String,
    pub poll_interval: Duration,
    pub pools: Vec<Whirlpool>,
}

#[derive(Debug, Deserialize)]
struct AccountsResponse {
    context: Context,
    value: Vec<Option<Account>>,
}

#[derive(Debug, Deserialize)]
struct Context {
    slot: u64,
}

#[derive(Debug, Deserialize)]
struct Account {
    /// `[base64 data, "base64"]`.
    data: (String, String),
    owner: String,
}

/// Fields of a Whirlpool account the indexer uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PoolState {
    sqrt_price: u128,
    tick: i32,
    mint_a: [u8; 32],
    mint_b: [u8; 32],
}

/// A configured pool with its mints decoded once at startup.
struct Target {
    pool: Whirlpool,
    mint_a: [u8; 32],
    mint_b: [u8; 32],
}

pub struct WhirlpoolIndexer {
    name: String,
    poll_interval: Duration,
    targets: Vec<Target>,
    rpc: Arc<dyn RpcTransport>,
    store: Arc<dyn ChainStore>,
}

impl WhirlpoolIndexer {
    pub fn new(
        config: SolanaIndexerConfig,
        rpc: Arc<dyn RpcTransport>,
        store: Arc<dyn ChainStore>,
    ) -> Result<Self, SourceError> {
        let targets = config
            .pools
            .into_iter()
            .map(|pool| {
                pubkey(&pool.address)?;
                Ok(Target {
                    mint_a: pubkey(&pool.mint_a)?,
                    mint_b: pubkey(&pool.mint_b)?,
                    pool,
                })
            })
            .collect::<Result<Vec<_>, SourceError>>()?;
        Ok(Self {
            name: config.name,
            poll_interval: config.poll_interval,
            targets,
            rpc,
            store,
        })
    }

    async fn read(&self, min_slot: u64) -> Result<AccountsResponse, SourceError> {
        let addresses: Vec<&str> = self
            .targets
            .iter()
            .map(|t| t.pool.address.as_str())
            .collect();
        let params = json!([addresses, {
            "encoding": "base64",
            "commitment": "confirmed",
            "minContextSlot": min_slot,
        }]);
        call(&*self.rpc, "getMultipleAccounts", params).await
    }
}

#[async_trait]
impl Source for WhirlpoolIndexer {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, ctx: &SourceContext) -> Result<(), SourceError> {
        let mut min_slot = 0;
        let mut last_seen: HashMap<usize, u128> = HashMap::new();
        loop {
            let response = self.read(min_slot).await?;
            let slot = response.context.slot;
            if response.value.len() != self.targets.len() {
                return Err(SourceError::decode(
                    "account count does not match the request",
                ));
            }
            min_slot = min_slot.max(slot);
            // The response has no block time. It reflects the chain at a
            // confirmed slot a moment ago, so receive time is close enough.
            let now = ctx.clock.now();

            let mut changed = Vec::new();
            for (index, (target, account)) in self.targets.iter().zip(response.value).enumerate() {
                let account = account.ok_or_else(|| {
                    SourceError::Chain(format!(
                        "pool account of {} does not exist",
                        target.pool.source
                    ))
                })?;
                let state = decode_account(&account)?;
                if (state.mint_a, state.mint_b) != (target.mint_a, target.mint_b) {
                    return Err(SourceError::Chain(format!(
                        "pool account of {} holds other mints than configured",
                        target.pool.source
                    )));
                }
                let pool = &target.pool;
                let price = price_from_sqrt(
                    U256::from(state.sqrt_price),
                    64,
                    pool.base_is_a,
                    pool.decimals_a,
                    pool.decimals_b,
                )
                .ok_or_else(|| {
                    SourceError::decode(format_args!("price of {} out of range", pool.source))
                })?;
                let observation =
                    Observation::new(pool.market.clone(), pool.source.clone(), price, now)
                        .map_err(SourceError::decode)?;
                ctx.emit(observation).await?;

                if last_seen.insert(index, state.sqrt_price) != Some(state.sqrt_price) {
                    changed.push(PoolStateRecord {
                        pool: pool.address.clone(),
                        source: pool.source.clone(),
                        market: pool.market.clone(),
                        slot,
                        price,
                        sqrt_price: state.sqrt_price.to_string(),
                        observed_at: now,
                    });
                }
                tracing::trace!(pool = %pool.source, slot, tick = state.tick, %price, "whirlpool read");
            }
            if !changed.is_empty() {
                self.store.record_pool_states(&self.name, &changed).await?;
            }
            ctx.metrics.indexer_progress(&self.name, slot, slot);
            if !ctx.pause(self.poll_interval).await {
                return Ok(());
            }
        }
    }
}

fn pubkey(address: &str) -> Result<[u8; 32], SourceError> {
    bs58::decode(address)
        .into_vec()
        .ok()
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .ok_or_else(|| SourceError::decode(format_args!("not a Solana address: {address}")))
}

fn decode_account(account: &Account) -> Result<PoolState, SourceError> {
    if account.owner != WHIRLPOOL_PROGRAM {
        return Err(SourceError::Chain(
            "account is not owned by the Whirlpool program".into(),
        ));
    }
    if account.data.1 != "base64" {
        return Err(SourceError::decode("account data is not base64"));
    }
    let data = base64::engine::general_purpose::STANDARD
        .decode(&account.data.0)
        .map_err(SourceError::decode)?;
    if data.get(..8) != Some(&DISCRIMINATOR[..]) {
        return Err(SourceError::Chain("account is not a Whirlpool".into()));
    }
    let field = |range: std::ops::Range<usize>| {
        data.get(range)
            .ok_or_else(|| SourceError::decode("Whirlpool account too short"))
    };
    let array32 = |range| -> Result<[u8; 32], SourceError> {
        <[u8; 32]>::try_from(field(range)?).map_err(SourceError::decode)
    };
    Ok(PoolState {
        sqrt_price: u128::from_le_bytes(
            <[u8; 16]>::try_from(field(SQRT_PRICE)?).map_err(SourceError::decode)?,
        ),
        tick: i32::from_le_bytes(<[u8; 4]>::try_from(field(TICK)?).map_err(SourceError::decode)?),
        mint_a: array32(MINT_A)?,
        mint_b: array32(MINT_B)?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use orderflow_application::Clock;
    use orderflow_domain::Timestamp;
    use orderflow_marketdata::ports::{BlockRef, NoopMetrics, StoreError, SwapRecord};
    use rust_decimal_macros::dec;
    use serde_json::Value;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    const FIXTURE: &str = include_str!("../../tests/fixtures/solana_getMultipleAccounts.json");
    const SOL: &str = "So11111111111111111111111111111111111111112";
    const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    fn whirlpool(id: &str, address: &str) -> Whirlpool {
        Whirlpool {
            source: SourceId::parse(id).unwrap(),
            address: address.into(),
            market: MarketId::parse("SOL-USDC").unwrap(),
            mint_a: SOL.into(),
            mint_b: USDC.into(),
            decimals_a: 9,
            decimals_b: 6,
            base_is_a: true,
        }
    }

    /// Serves the recorded response and remembers each `minContextSlot`.
    struct RecordedNode {
        min_slots: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl RpcTransport for RecordedNode {
        async fn call(&self, _: &str, params: Value) -> Result<Value, SourceError> {
            let min_slot = params[1]["minContextSlot"].as_u64().unwrap();
            self.min_slots.lock().unwrap().push(min_slot);
            let body: Value = serde_json::from_str(FIXTURE).unwrap();
            Ok(body["result"].clone())
        }
    }

    #[derive(Default)]
    struct States(Mutex<Vec<PoolStateRecord>>);

    #[async_trait]
    impl ChainStore for States {
        async fn checkpoints(&self, _: &str, _: usize) -> Result<Vec<BlockRef>, StoreError> {
            Ok(Vec::new())
        }
        async fn commit(&self, _: &str, _: &[SwapRecord], _: &BlockRef) -> Result<(), StoreError> {
            Ok(())
        }
        async fn rollback(&self, _: &str, _: u64) -> Result<u64, StoreError> {
            Ok(0)
        }
        async fn record_pool_states(
            &self,
            _: &str,
            states: &[PoolStateRecord],
        ) -> Result<(), StoreError> {
            self.0.lock().unwrap().extend_from_slice(states);
            Ok(())
        }
    }

    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            Timestamp::from_unix_nanos(7)
        }
    }

    #[test]
    fn recorded_accounts_decode_as_sol_usdc_whirlpools() {
        let body: Value = serde_json::from_str(FIXTURE).unwrap();
        let response: AccountsResponse = serde_json::from_value(body["result"].clone()).unwrap();
        let first = decode_account(response.value[0].as_ref().unwrap()).unwrap();
        assert_eq!(first.mint_a, pubkey(SOL).unwrap());
        assert_eq!(first.mint_b, pubkey(USDC).unwrap());
        let price = price_from_sqrt(U256::from(first.sqrt_price), 64, true, 9, 6).unwrap();
        assert_eq!(price, dec!(110.491834446448041214));
    }

    #[test]
    fn foreign_accounts_are_refused() {
        let account = Account {
            data: (String::new(), "base64".into()),
            owner: "11111111111111111111111111111111".into(),
        };
        assert_eq!(decode_account(&account).unwrap_err().kind(), "chain");
    }

    #[tokio::test(start_paused = true)]
    async fn polls_at_or_after_the_last_slot_and_stores_changes_once() {
        let node = Arc::new(RecordedNode {
            min_slots: Mutex::default(),
        });
        let store = Arc::new(States::default());
        let config = SolanaIndexerConfig {
            name: "orca:solana".into(),
            poll_interval: Duration::from_secs(2),
            pools: vec![
                whirlpool(
                    "orca:sol_usdc_4",
                    "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE",
                ),
                whirlpool(
                    "orca:sol_usdc_1",
                    "83v8iPyZihDEjDdY8RdZddyZNyUtXngz69Lgo9Kt5d6d",
                ),
            ],
        };
        let indexer = WhirlpoolIndexer::new(config, node.clone(), store.clone()).unwrap();
        let (sender, mut receiver) = mpsc::channel(16);
        let shutdown = CancellationToken::new();
        let ctx = SourceContext::new(
            sender,
            Arc::new(FixedClock),
            Arc::new(NoopMetrics),
            shutdown.clone(),
        );
        let task = tokio::spawn(async move { indexer.run(&ctx).await });

        let mut observations = Vec::new();
        while observations.len() < 4 {
            observations.push(receiver.recv().await.unwrap());
        }
        shutdown.cancel();
        task.await.unwrap().unwrap();

        assert_eq!(observations[0].price(), dec!(110.491834446448041214));
        assert_eq!(observations[1].source().as_str(), "orca:sol_usdc_1");
        let slots = node.min_slots.lock().unwrap().clone();
        assert_eq!(slots[..2], [0, 454_674_045]);
        assert_eq!(
            store.0.lock().unwrap().len(),
            2,
            "unchanged pools are not stored again"
        );
    }
}
