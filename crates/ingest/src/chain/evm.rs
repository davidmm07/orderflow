//! Uniswap v3 swap indexer for Ethereum and other EVM chains.
//!
//! The indexer follows the chain `confirmations` blocks behind the head,
//! reads `Swap` logs of the configured pools with `eth_getLogs`, stores
//! every swap with the block it was processed in, and publishes each
//! pool's price as of the newest processed block.
//!
//! Reorgs: every processed range ends in a checkpoint `(number, hash)`.
//! Before each step the newest checkpoint is compared with the chain. If
//! its hash changed, older checkpoints are checked until one still
//! matches; everything stored above it is deleted and indexing resumes
//! from there. Waiting for confirmations makes this rare, the rollback
//! makes it harmless.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::{SolCall, SolEvent, sol};
use async_trait::async_trait;
use orderflow_domain::{MarketId, Timestamp};
use orderflow_marketdata::{
    Observation, SourceId,
    ports::{BlockRef, ChainStore, SwapRecord},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use serde_json::json;

use super::{
    math::price_from_sqrt,
    rpc::{RpcTransport, call},
};
use crate::{
    error::SourceError,
    runtime::{Source, SourceContext},
};

sol! {
    event Swap(
        address indexed sender,
        address indexed recipient,
        int256 amount0,
        int256 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick
    );

    function slot0() external view returns (
        uint160 sqrtPriceX96,
        int24 tick,
        uint16 observationIndex,
        uint16 observationCardinality,
        uint16 observationCardinalityNext,
        uint8 feeProtocol,
        bool unlocked
    );
}

/// Checkpoints kept for reorg recovery. Ethereum finalizes blocks after
/// two epochs (64 blocks), so a reorg deeper than this many batches would
/// mean a finality failure, which needs a human anyway.
const CHECKPOINT_DEPTH: usize = 128;

/// One Uniswap v3 pool priced as one market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvmPool {
    /// Such as `uniswap_v3:usdc_weth_500`.
    pub source: SourceId,
    pub address: Address,
    pub market: MarketId,
    /// Whether the market's base asset is the pool's token0.
    pub base_is_token0: bool,
    pub decimals0: u8,
    pub decimals1: u8,
}

#[derive(Debug, Clone)]
pub struct EvmIndexerConfig {
    /// Such as `uniswap_v3:ethereum`; also the checkpoint key in storage.
    pub name: String,
    /// Blocks to stay behind the head.
    pub confirmations: u64,
    /// Largest `eth_getLogs` range. Halved automatically when a node
    /// rejects a range as too large.
    pub max_block_range: u64,
    /// Blocks to index before the first start, when there is no checkpoint.
    pub backfill_blocks: u64,
    pub poll_interval: Duration,
    pub pools: Vec<EvmPool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
    #[serde(deserialize_with = "quantity")]
    block_number: u64,
    block_hash: B256,
    transaction_hash: B256,
    #[serde(deserialize_with = "quantity")]
    log_index: u64,
    #[serde(default)]
    removed: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RpcBlock {
    #[serde(deserialize_with = "quantity")]
    number: u64,
    hash: B256,
    #[serde(deserialize_with = "quantity")]
    timestamp: u64,
}

impl RpcBlock {
    fn reference(&self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.hash.to_string(),
            timestamp: seconds(self.timestamp),
        }
    }
}

/// Reads a JSON-RPC quantity such as `"0x18f069f"`.
fn quantity<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let text = String::deserialize(deserializer)?;
    let digits = text
        .strip_prefix("0x")
        .ok_or_else(|| serde::de::Error::custom("quantity without 0x prefix"))?;
    u64::from_str_radix(digits, 16).map_err(serde::de::Error::custom)
}

fn seconds(value: u64) -> Timestamp {
    Timestamp::from_unix_nanos(value.saturating_mul(1_000_000_000))
}

fn hex(number: u64) -> String {
    format!("{number:#x}")
}

/// Where indexing stands: recent checkpoints, newest first, and the last
/// known price of each pool.
struct Progress {
    checkpoints: VecDeque<BlockRef>,
    prices: HashMap<Address, Decimal>,
}

pub struct UniswapV3Indexer {
    config: EvmIndexerConfig,
    rpc: Arc<dyn RpcTransport>,
    store: Arc<dyn ChainStore>,
}

impl UniswapV3Indexer {
    pub fn new(
        config: EvmIndexerConfig,
        rpc: Arc<dyn RpcTransport>,
        store: Arc<dyn ChainStore>,
    ) -> Self {
        Self { config, rpc, store }
    }

    async fn head(&self) -> Result<u64, SourceError> {
        let text: String = call(&*self.rpc, "eth_blockNumber", json!([])).await?;
        let digits = text.strip_prefix("0x").unwrap_or(&text);
        u64::from_str_radix(digits, 16).map_err(SourceError::decode)
    }

    async fn block(&self, number: u64) -> Result<RpcBlock, SourceError> {
        let block: Option<RpcBlock> = call(
            &*self.rpc,
            "eth_getBlockByNumber",
            json!([hex(number), false]),
        )
        .await?;
        // A node behind a load balancer may not have the block yet.
        block.ok_or_else(|| SourceError::Chain(format!("node does not have block {number}")))
    }

    /// Loads checkpoints, or creates the first one `backfill_blocks` behind
    /// the confirmed head.
    async fn resume(&self) -> Result<Progress, SourceError> {
        let mut checkpoints = self
            .store
            .checkpoints(&self.config.name, CHECKPOINT_DEPTH)
            .await?;
        if checkpoints.is_empty() {
            let head = self.head().await?;
            let start =
                head.saturating_sub(self.config.confirmations + self.config.backfill_blocks);
            let block = self.block(start).await?.reference();
            self.store.commit(&self.config.name, &[], &block).await?;
            tracing::info!(
                indexer = self.config.name,
                block = start,
                "starting a new index"
            );
            checkpoints.push(block);
        }
        Ok(Progress {
            checkpoints: checkpoints.into(),
            prices: HashMap::new(),
        })
    }

    /// Detects a reorg below the newest checkpoint and rolls storage back to
    /// the newest checkpoint that is still on the canonical chain.
    async fn unwind_reorg(
        &self,
        progress: &mut Progress,
        ctx: &SourceContext,
    ) -> Result<(), SourceError> {
        let Some(tip) = progress.checkpoints.front().cloned() else {
            return Err(SourceError::Chain("no checkpoint to resume from".into()));
        };
        if self.block(tip.number).await?.hash.to_string() == tip.hash {
            return Ok(());
        }
        progress.checkpoints.pop_front();
        while let Some(candidate) = progress.checkpoints.front().cloned() {
            if self.block(candidate.number).await?.hash.to_string() == candidate.hash {
                let removed = self
                    .store
                    .rollback(&self.config.name, candidate.number)
                    .await?;
                ctx.metrics
                    .indexer_reorg(&self.config.name, tip.number - candidate.number);
                tracing::warn!(
                    indexer = self.config.name,
                    from = tip.number,
                    to = candidate.number,
                    removed_swaps = removed,
                    "chain reorganized, rolled back"
                );
                // Prices may come from swaps that no longer exist.
                progress.prices.clear();
                return Ok(());
            }
            progress.checkpoints.pop_front();
        }
        Err(SourceError::Chain(format!(
            "reorg below the oldest of {CHECKPOINT_DEPTH} checkpoints, manual recovery needed"
        )))
    }

    /// Indexes blocks `from..=to` and commits them with a checkpoint.
    async fn index_range(
        &self,
        progress: &mut Progress,
        from: u64,
        to: u64,
    ) -> Result<(), SourceError> {
        let addresses: Vec<String> = self
            .config
            .pools
            .iter()
            .map(|pool| pool.address.to_string())
            .collect();
        let filter = json!([{
            "fromBlock": hex(from),
            "toBlock": hex(to),
            "address": addresses,
            "topics": [[Swap::SIGNATURE_HASH.to_string()]],
        }]);
        let logs: Vec<RpcLog> = call(&*self.rpc, "eth_getLogs", filter).await?;

        // Headers of every block with a swap, to check each log still sits
        // on the canonical chain and to learn its time.
        let end = self.block(to).await?;
        let mut headers: HashMap<u64, RpcBlock> = HashMap::from([(to, end.clone())]);
        let numbers: BTreeSet<u64> = logs.iter().map(|log| log.block_number).collect();
        for number in numbers {
            if let std::collections::hash_map::Entry::Vacant(slot) = headers.entry(number) {
                slot.insert(self.block(number).await?);
            }
        }

        let mut swaps = Vec::with_capacity(logs.len());
        let mut latest: Vec<(u64, u64, Address, Decimal)> = Vec::new();
        for log in &logs {
            let Some(pool) = self
                .config
                .pools
                .iter()
                .find(|pool| pool.address == log.address)
            else {
                continue;
            };
            let canonical = headers
                .get(&log.block_number)
                .filter(|header| header.hash == log.block_hash && !log.removed);
            let Some(header) = canonical else {
                // The chain moved while we read it. Retry the whole range.
                return Err(SourceError::Chain(format!(
                    "log in block {} is no longer canonical",
                    log.block_number
                )));
            };
            let swap = decode_swap(pool, log, seconds(header.timestamp))?;
            latest.push((swap.block_number, swap.log_index, pool.address, swap.price));
            swaps.push(swap);
        }
        swaps.sort_by_key(|swap| (swap.block_number, swap.log_index));
        latest.sort_by_key(|(block, index, _, _)| (*block, *index));

        let checkpoint = end.reference();
        self.store
            .commit(&self.config.name, &swaps, &checkpoint)
            .await?;
        for (_, _, address, price) in latest {
            progress.prices.insert(address, price);
        }
        progress.checkpoints.push_front(checkpoint);
        progress.checkpoints.truncate(CHECKPOINT_DEPTH);
        if !swaps.is_empty() {
            tracing::debug!(
                indexer = self.config.name,
                from,
                to,
                swaps = swaps.len(),
                "indexed"
            );
        }
        Ok(())
    }

    /// Publishes every pool's price as of the newest checkpoint.
    ///
    /// A v3 pool's price only changes through swaps, so the price after the
    /// last swap is the price at any later block. Pools without a swap since
    /// the start or a rollback are read with `slot0` at that block.
    async fn emit_prices(
        &self,
        progress: &mut Progress,
        ctx: &SourceContext,
    ) -> Result<(), SourceError> {
        let Some(tip) = progress.checkpoints.front().cloned() else {
            return Ok(());
        };
        for pool in &self.config.pools {
            let price = match progress.prices.get(&pool.address) {
                Some(price) => *price,
                None => {
                    let price = self.slot0_price(pool, tip.number).await?;
                    progress.prices.insert(pool.address, price);
                    price
                }
            };
            let observation = Observation::new(
                pool.market.clone(),
                pool.source.clone(),
                price,
                tip.timestamp,
            )
            .map_err(SourceError::decode)?;
            ctx.emit(observation).await?;
        }
        Ok(())
    }

    async fn slot0_price(&self, pool: &EvmPool, block: u64) -> Result<Decimal, SourceError> {
        let data = Bytes::from(slot0Call {}.abi_encode());
        let params =
            json!([{"to": pool.address.to_string(), "data": data.to_string()}, hex(block)]);
        let result: Bytes = call(&*self.rpc, "eth_call", params).await?;
        let slot0 = slot0Call::abi_decode_returns(&result).map_err(SourceError::decode)?;
        price(pool, U256::from(slot0.sqrtPriceX96))
    }
}

#[async_trait]
impl Source for UniswapV3Indexer {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn run(&self, ctx: &SourceContext) -> Result<(), SourceError> {
        let mut progress = self.resume().await?;
        let mut range = self.config.max_block_range.max(1);
        loop {
            let head = self.head().await?;
            let safe = head.saturating_sub(self.config.confirmations);
            self.unwind_reorg(&mut progress, ctx).await?;
            let cursor = progress.checkpoints.front().map_or(0, |block| block.number);
            ctx.metrics
                .indexer_progress(&self.config.name, head, cursor);

            if cursor >= safe {
                if !ctx.pause(self.config.poll_interval).await {
                    return Ok(());
                }
                continue;
            }
            let to = safe.min(cursor + range);
            match self.index_range(&mut progress, cursor + 1, to).await {
                Ok(()) => {}
                Err(SourceError::Rpc { message, .. }) if range > 1 && is_range_limit(&message) => {
                    range = (range / 2).max(1);
                    tracing::info!(
                        indexer = self.config.name,
                        range,
                        "node limits log ranges, narrowing"
                    );
                    continue;
                }
                Err(error) => return Err(error),
            }
            // Catching up: keep going without waiting, and without
            // publishing prices of blocks that are already old.
            if to < safe {
                if ctx.shutdown.is_cancelled() {
                    return Ok(());
                }
                continue;
            }
            self.emit_prices(&mut progress, ctx).await?;
            if !ctx.pause(self.config.poll_interval).await {
                return Ok(());
            }
        }
    }
}

/// Providers word "range too large" differently. Rate limit errors share
/// some of the words, and narrowing the range would not help them.
fn is_range_limit(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    let range = ["range", "results", "too large", "response size"]
        .iter()
        .any(|word| message.contains(word));
    range && !message.contains("rate")
}

fn price(pool: &EvmPool, sqrt_price: U256) -> Result<Decimal, SourceError> {
    price_from_sqrt(
        sqrt_price,
        96,
        pool.base_is_token0,
        pool.decimals0,
        pool.decimals1,
    )
    .ok_or_else(|| SourceError::decode(format_args!("price of {} out of range", pool.source)))
}

fn decode_swap(
    pool: &EvmPool,
    log: &RpcLog,
    block_time: Timestamp,
) -> Result<SwapRecord, SourceError> {
    let swap =
        Swap::decode_raw_log(log.topics.iter().copied(), &log.data).map_err(SourceError::decode)?;
    let sqrt_price = U256::from(swap.sqrtPriceX96);
    Ok(SwapRecord {
        pool: pool.address.to_string().to_ascii_lowercase(),
        source: pool.source.clone(),
        market: pool.market.clone(),
        block_number: log.block_number,
        block_hash: log.block_hash.to_string(),
        tx_hash: log.transaction_hash.to_string(),
        log_index: log.log_index,
        price: price(pool, sqrt_price)?,
        amount0: swap.amount0.to_string(),
        amount1: swap.amount1.to_string(),
        sqrt_price: sqrt_price.to_string(),
        block_time,
    })
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Mutex};

    use orderflow_application::Clock;
    use orderflow_marketdata::ports::{NoopMetrics, PoolStateRecord, StoreError};
    use rust_decimal_macros::dec;
    use serde_json::Value;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    const FIXTURE_LOGS: &str = include_str!("../../tests/fixtures/eth_getLogs.json");
    const FIXTURE_BLOCKS: &str = include_str!("../../tests/fixtures/eth_getBlockByNumber.json");
    const FIXTURE_SLOT0: &str = include_str!("../../tests/fixtures/eth_call_slot0.json");
    /// Range of the recorded `eth_getLogs` call.
    const FROM: u64 = 26_150_529;
    const TO: u64 = 26_150_559;

    fn pool(
        id: &str,
        address: &str,
        base_is_token0: bool,
        decimals0: u8,
        decimals1: u8,
        market: &str,
    ) -> EvmPool {
        EvmPool {
            source: SourceId::parse(id).unwrap(),
            address: Address::from_str(address).unwrap(),
            market: MarketId::parse(market).unwrap(),
            base_is_token0,
            decimals0,
            decimals1,
        }
    }

    fn pools() -> Vec<EvmPool> {
        vec![
            pool(
                "uniswap_v3:usdc_weth_500",
                "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
                false,
                6,
                18,
                "ETH-USDC",
            ),
            pool(
                "uniswap_v3:usdc_weth_3000",
                "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
                false,
                6,
                18,
                "ETH-USDC",
            ),
            pool(
                "uniswap_v3:wbtc_usdc_3000",
                "0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35",
                true,
                8,
                6,
                "BTC-USDC",
            ),
        ]
    }

    /// A node that answers from recorded data. Blocks that were not
    /// recorded get a synthetic hash, which a test can override to fake a
    /// reorg.
    struct FakeNode {
        head: u64,
        blocks: HashMap<u64, Value>,
        overrides: Mutex<HashMap<u64, String>>,
        logs: Vec<Value>,
        slot0: Value,
    }

    impl FakeNode {
        fn recorded(head: u64) -> Self {
            let blocks: HashMap<String, Value> = serde_json::from_str(FIXTURE_BLOCKS).unwrap();
            let logs: Value = serde_json::from_str(FIXTURE_LOGS).unwrap();
            Self {
                head,
                blocks: blocks
                    .into_iter()
                    .map(|(number, body)| {
                        let number = u64::from_str_radix(&number[2..], 16).unwrap();
                        (number, body["result"].clone())
                    })
                    .collect(),
                overrides: Mutex::default(),
                logs: logs["result"].as_array().unwrap().clone(),
                slot0: serde_json::from_str(FIXTURE_SLOT0).unwrap(),
            }
        }

        fn synthetic_hash(number: u64) -> String {
            format!("0x{number:064x}")
        }
    }

    fn param_u64(value: &Value) -> u64 {
        u64::from_str_radix(&value.as_str().unwrap()[2..], 16).unwrap()
    }

    #[async_trait]
    impl RpcTransport for FakeNode {
        async fn call(&self, method: &str, params: Value) -> Result<Value, SourceError> {
            match method {
                "eth_blockNumber" => Ok(json!(hex(self.head))),
                "eth_getBlockByNumber" => {
                    let number = param_u64(&params[0]);
                    if let Some(hash) = self.overrides.lock().unwrap().get(&number) {
                        return Ok(
                            json!({"number": hex(number), "hash": hash, "timestamp": "0x1"}),
                        );
                    }
                    Ok(self.blocks.get(&number).cloned().unwrap_or_else(|| {
                        json!({"number": hex(number), "hash": Self::synthetic_hash(number), "timestamp": "0x1"})
                    }))
                }
                "eth_getLogs" => {
                    let (from, to) = (
                        param_u64(&params[0]["fromBlock"]),
                        param_u64(&params[0]["toBlock"]),
                    );
                    let logs: Vec<Value> = self
                        .logs
                        .iter()
                        .filter(|log| (from..=to).contains(&param_u64(&log["blockNumber"])))
                        .cloned()
                        .collect();
                    Ok(Value::Array(logs))
                }
                "eth_call" => {
                    let to = params[0]["to"].as_str().unwrap().to_ascii_lowercase();
                    Ok(self.slot0[to.as_str()]["result"].clone())
                }
                other => Err(SourceError::rpc(
                    -32601,
                    &format!("method {other} not found"),
                )),
            }
        }
    }

    #[derive(Default)]
    struct MemoryChain {
        checkpoints: Mutex<Vec<BlockRef>>,
        swaps: Mutex<Vec<SwapRecord>>,
        rollbacks: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl ChainStore for MemoryChain {
        async fn checkpoints(&self, _: &str, limit: usize) -> Result<Vec<BlockRef>, StoreError> {
            let mut all = self.checkpoints.lock().unwrap().clone();
            all.sort_by_key(|block| std::cmp::Reverse(block.number));
            all.truncate(limit);
            Ok(all)
        }

        async fn commit(
            &self,
            _: &str,
            swaps: &[SwapRecord],
            block: &BlockRef,
        ) -> Result<(), StoreError> {
            self.swaps.lock().unwrap().extend_from_slice(swaps);
            self.checkpoints.lock().unwrap().push(block.clone());
            Ok(())
        }

        async fn rollback(&self, _: &str, ancestor: u64) -> Result<u64, StoreError> {
            self.rollbacks.lock().unwrap().push(ancestor);
            self.checkpoints
                .lock()
                .unwrap()
                .retain(|block| block.number <= ancestor);
            let mut swaps = self.swaps.lock().unwrap();
            let before = swaps.len();
            swaps.retain(|swap| swap.block_number <= ancestor);
            Ok((before - swaps.len()) as u64)
        }

        async fn record_pool_states(
            &self,
            _: &str,
            _: &[PoolStateRecord],
        ) -> Result<(), StoreError> {
            Ok(())
        }
    }

    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            Timestamp::from_unix_nanos(0)
        }
    }

    fn config() -> EvmIndexerConfig {
        EvmIndexerConfig {
            name: "uniswap_v3:ethereum".into(),
            confirmations: 3,
            max_block_range: 500,
            backfill_blocks: 0,
            poll_interval: Duration::from_secs(12),
            pools: pools(),
        }
    }

    fn checkpoint(number: u64, hash: String) -> BlockRef {
        BlockRef {
            number,
            hash,
            timestamp: Timestamp::default(),
        }
    }

    /// Runs the indexer until it has emitted `count` observations.
    async fn run_until(node: FakeNode, store: Arc<MemoryChain>, count: usize) -> Vec<Observation> {
        let (sender, mut receiver) = mpsc::channel(64);
        let shutdown = CancellationToken::new();
        let ctx = SourceContext::new(
            sender,
            Arc::new(FixedClock),
            Arc::new(NoopMetrics),
            shutdown.clone(),
        );
        let indexer = UniswapV3Indexer::new(config(), Arc::new(node), store);
        let task = tokio::spawn(async move { indexer.run(&ctx).await });
        let mut observations = Vec::new();
        while observations.len() < count {
            observations.push(receiver.recv().await.unwrap());
        }
        shutdown.cancel();
        task.await.unwrap().unwrap();
        observations
    }

    #[test]
    fn recorded_swap_logs_decode_to_pool_prices() {
        let logs: Value = serde_json::from_str(FIXTURE_LOGS).unwrap();
        let logs: Vec<RpcLog> = serde_json::from_value(logs["result"].clone()).unwrap();
        let pools = pools();
        let swaps: Vec<SwapRecord> = logs
            .iter()
            .map(|log| decode_swap(&pools[0], log, Timestamp::default()).unwrap())
            .collect();
        assert_eq!(swaps.len(), 20);
        let last = swaps.last().unwrap();
        assert_eq!((last.block_number, last.log_index), (TO, 2039));
        assert_eq!(last.price, dec!(2478.324669500408464656));
        assert_eq!(last.pool, "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640");
        assert!(last.amount0.parse::<i128>().is_ok() && last.amount1.parse::<i128>().is_ok());
    }

    #[tokio::test]
    async fn indexes_recorded_blocks_and_prices_every_pool() {
        let store = Arc::new(MemoryChain::default());
        store
            .checkpoints
            .lock()
            .unwrap()
            .push(checkpoint(FROM - 1, FakeNode::synthetic_hash(FROM - 1)));

        let observations = run_until(FakeNode::recorded(TO + 3), store.clone(), 3).await;

        assert_eq!(store.swaps.lock().unwrap().len(), 20);
        let tip = store.checkpoints.lock().unwrap().last().cloned().unwrap();
        assert_eq!(tip.number, TO);
        let prices: Vec<(String, Decimal)> = observations
            .iter()
            .map(|o| (o.source().to_string(), o.price()))
            .collect();
        assert_eq!(
            prices,
            [
                (
                    "uniswap_v3:usdc_weth_500".to_owned(),
                    dec!(2478.324669500408464656)
                ),
                (
                    "uniswap_v3:usdc_weth_3000".to_owned(),
                    dec!(2473.91579168835633987)
                ),
                (
                    "uniswap_v3:wbtc_usdc_3000".to_owned(),
                    dec!(81500.064982892932354632)
                ),
            ]
        );
        assert!(
            observations
                .iter()
                .all(|o| o.observed_at() == tip.timestamp)
        );
    }

    #[tokio::test]
    async fn a_reorg_rolls_back_to_the_newest_canonical_checkpoint() {
        let store = Arc::new(MemoryChain::default());
        {
            let mut checkpoints = store.checkpoints.lock().unwrap();
            checkpoints.push(checkpoint(100, FakeNode::synthetic_hash(100)));
            checkpoints.push(checkpoint(105, FakeNode::synthetic_hash(105)));
            // Block 108 as we saw it before the reorg.
            checkpoints.push(checkpoint(108, "0xdead".into()));
        }
        let node = FakeNode::recorded(115);

        let observations = run_until(node, store.clone(), 3).await;

        assert_eq!(*store.rollbacks.lock().unwrap(), [105]);
        let tip = store
            .checkpoints
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.number)
            .max();
        assert_eq!(tip, Some(112), "re-indexed from 106 up to head - 3");
        assert_eq!(observations.len(), 3);
    }

    #[test]
    fn range_errors_are_told_apart_from_rate_limits() {
        assert!(is_range_limit("query returned more than 10000 results"));
        assert!(is_range_limit("eth_getLogs block range too large"));
        assert!(!is_range_limit("rate limit exceeded"));
        assert!(!is_range_limit("execution reverted"));
    }
}
