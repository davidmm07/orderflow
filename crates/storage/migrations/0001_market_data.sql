-- Market data schema.
--
-- Prices are NUMERIC so no value is ever rounded through a binary float.
-- Times are timestamptz (stored as UTC). Raw on-chain integers (token
-- amounts, square root prices) are NUMERIC(78, 0), wide enough for any
-- uint256, so the indexer stores exactly what the chain emitted.

-- Every published index price, with the per-source breakdown that
-- explains it. Append-only.
CREATE TABLE index_prices (
    market       text        NOT NULL,
    epoch        bigint      NOT NULL,
    sequence     bigint      NOT NULL,
    price        numeric     NOT NULL CHECK (price > 0),
    status       text        NOT NULL CHECK (status IN ('live', 'degraded', 'stale')),
    as_of        timestamptz NOT NULL,
    published_at timestamptz NOT NULL,
    spread_bps   numeric     NOT NULL,
    components   jsonb       NOT NULL,
    PRIMARY KEY (market, epoch, sequence)
);

-- Rows arrive in time order, so a BRIN index answers time range scans for
-- a tiny fraction of a B-tree's size and write cost.
CREATE INDEX index_prices_published_at ON index_prices USING brin (published_at);

-- One minute OHLC bars. Longer intervals are rolled up at read time. The
-- close carries the revision of the update that set it, so concurrent or
-- retried writes always converge on the newest close.
CREATE TABLE candles_1m (
    market         text        NOT NULL,
    bucket         timestamptz NOT NULL,
    open           numeric     NOT NULL,
    high           numeric     NOT NULL,
    low            numeric     NOT NULL,
    close          numeric     NOT NULL,
    close_epoch    bigint      NOT NULL,
    close_sequence bigint      NOT NULL,
    PRIMARY KEY (market, bucket)
);

-- Swaps decoded by the EVM indexers. The primary key makes re-indexing a
-- range after a restart a no-op instead of a duplicate.
CREATE TABLE dex_swaps (
    indexer      text           NOT NULL,
    tx_hash      text           NOT NULL,
    log_index    bigint         NOT NULL,
    pool         text           NOT NULL,
    source       text           NOT NULL,
    market       text           NOT NULL,
    block_number bigint         NOT NULL,
    block_hash   text           NOT NULL,
    block_time   timestamptz    NOT NULL,
    price        numeric        NOT NULL,
    amount0      numeric(78, 0) NOT NULL,
    amount1      numeric(78, 0) NOT NULL,
    sqrt_price   numeric(78, 0) NOT NULL,
    PRIMARY KEY (indexer, tx_hash, log_index)
);

-- Reorg rollbacks delete by block number.
CREATE INDEX dex_swaps_indexer_block ON dex_swaps (indexer, block_number);
-- "Recent swaps of a pool" reads.
CREATE INDEX dex_swaps_pool_time ON dex_swaps (pool, block_time DESC);

-- Blocks an indexer has fully processed. The newest one is where indexing
-- resumes; older ones are the fallback points when a reorg removes it.
CREATE TABLE indexer_checkpoints (
    indexer      text        NOT NULL,
    block_number bigint      NOT NULL,
    block_hash   text        NOT NULL,
    block_time   timestamptz NOT NULL,
    PRIMARY KEY (indexer, block_number)
);

-- Pool states read from account-based chains, stored when they change.
CREATE TABLE dex_pool_states (
    indexer     text           NOT NULL,
    pool        text           NOT NULL,
    slot        bigint         NOT NULL,
    source      text           NOT NULL,
    market      text           NOT NULL,
    price       numeric        NOT NULL,
    sqrt_price  numeric(39, 0) NOT NULL,
    observed_at timestamptz    NOT NULL,
    PRIMARY KEY (indexer, pool, slot)
);
