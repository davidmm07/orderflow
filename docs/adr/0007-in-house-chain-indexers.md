# ADR 0007: In-house chain indexers for DEX prices

- Status: accepted
- Date: 2026-10-08

## Context

DEX prices are part of the index (ADR 0006). They could come from a third
party (an aggregator API, a subgraph), but then the "independent" on-chain
source shares the failure modes of every other API. Reading the chain
ourselves gives a source that only depends on a node, and nodes are
interchangeable: a public endpoint, a provider like Alchemy or Helius, or
our own.

The two chains need different approaches:

- On Ethereum, Uniswap v3 emits a `Swap` log with the pool price after
  every swap, and logs for a whole block range come back from one call.
  Blocks near the head can be reorganized.
- Solana programs keep pool state in accounts, and the Orca Whirlpool
  account holds the current `sqrt_price`. Fetching every swap would mean
  one `getTransaction` per signature, too expensive on public endpoints.

## Decision

Two indexers behind one JSON-RPC transport trait, so both are tested
against recorded mainnet responses with no network.

**EVM (Uniswap v3), `crates/ingest/src/chain/evm.rs`**

- Follow the chain `confirmations` blocks (3) behind the head and fetch
  `Swap` logs of all configured pools with one `eth_getLogs` per range.
  Decoding uses `alloy-sol-types`, so the event layout comes from its
  Solidity signature instead of hand-written offsets.
- Check every log's block hash against the block header, then store the
  swaps and a checkpoint `(number, hash)` in one transaction. Re-indexing
  a range is a no-op thanks to the `(indexer, tx_hash, log_index)` key.
- Before each step compare the newest checkpoint with the chain. On a
  mismatch walk back through older checkpoints to the newest one still
  canonical, delete everything above it and continue from there.
- A v3 pool's price only changes through swaps, so the price after the
  last swap is the price at any later block. Pools without a recent swap
  are read with `slot0` at the checkpoint block.
- Narrow the range automatically when a node rejects it as too large.
- Raw on-chain integers (amounts, `sqrtPriceX96`) are stored as
  `NUMERIC(78,0)`, and prices are computed with 512 bit integer math, so
  no value passes through a float or a type too small for `uint256`.

**Solana (Orca Whirlpools), `crates/ingest/src/chain/solana.rs`**

- One `getMultipleAccounts` call per poll reads every configured pool at a
  single `confirmed` slot. The account owner, the Anchor discriminator and
  both token mints are checked before the price is trusted.
- Every request carries `minContextSlot` with the last slot seen. Public
  endpoints sit behind load balancers, and without it a lagging node
  could answer with older state than the previous poll.
- Pool states are stored when they change.

Both indexers report head and cursor heights, swaps and reorgs as metrics,
with alerts on lag (`IndexerLagging`) and rollbacks (`ChainReorgRolledBack`).

## Alternatives considered

- **Subgraphs or indexing services (The Graph, Substreams, Goldsky).**
  Less code, but another dependency with its own outages and pricing, and
  the reason for this source is independence.
- **Full node client libraries (`alloy-provider`, `solana-client`).** They
  pull in hundreds of crates for the four RPC methods used here, and hide
  the transport we want to replay in tests.
- **Finalized blocks only.** No reorg handling needed, but Ethereum
  finality is about 13 minutes behind the head, too old for a live price.
- **Solana swap indexing via Geyser or a streaming provider.** The right
  tool for per-trade history; it needs paid infrastructure or our own
  validator. Account state is enough for a price.

## Consequences

- On-chain prices depend on any RPC node, never on a price API.
- Reorgs deeper than the retained checkpoints (128) stop the indexer with
  an error instead of guessing. Post-merge Ethereum finalizes long before
  that depth, so it would mean something is badly wrong.
- Prices published before a reorg are not retracted; the next update
  corrects them, and the stored swap history is rolled back.
- Solana observations carry the receive time, because account reads have
  no block time. At `confirmed` commitment that is within a second or two.
- WBTC is treated as BTC and USDC pools price `-USDC` markets only. A
  depeg shows up as an outlier against the CEX sources of the same market.
