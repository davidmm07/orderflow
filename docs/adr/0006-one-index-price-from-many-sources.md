# ADR 0006: One index price from many sources

- Status: accepted
- Date: 2026-10-08

## Context

The price feed answers one question for every market: what is the price
right now? Clients use the answer for charts, for alerts and, later, for
the exchange itself (mark prices, price bands, stop triggers). No single
upstream is good enough on its own:

- Exchanges go down, rate limit, or print a wild trade on a thin book.
- Aggregators such as CoinGecko are dependable but update about once a minute.
- On-chain pools are the ground truth of DeFi prices but only change when
  someone swaps, and a pool can be manipulated within a block.
- Orderflow's own trades are the price clients see on this venue, which is
  exactly why they must not define the reference price for it.

The service ingests all of them and must publish one number per market
that stays correct when sources disagree, says so when they go quiet, and
can be explained afterwards.

## Decision

Each market has a set of sources, each with a kind, a role and a maximum
age (`config/marketdata.json`). The aggregator keeps the latest quote of
every source and, on a fixed cadence, computes:

1. **Freshness.** Quotes older than their source's `max_age` are `stale`
   and ignored. Each source has its own limit: a minute is old for an
   exchange stream and normal for CoinGecko.
2. **Median** of the fresh quotes from `index` sources. One broken source
   moves a median by at most one position; it can move a mean anywhere.
3. **Outlier rejection.** Quotes further than `max_deviation_bps` from that
   median are `outlier`s, and the price is the median of the rest. If every
   quote is far from the median (two sources that disagree), all of them
   are kept and the result is flagged rather than guessed.
4. **Status.** `live` when at least `quorum` sources agree, `degraded` when
   a price exists with fewer or disagreeing sources, `stale` when no fresh
   source is left, in which case the last price is republished once with
   that status.

`monitor` sources (Orderflow's own trades) are shown next to the index with
their deviation in basis points, and alerted on, but never counted.

Every published update carries `(epoch, sequence)`, the per-source
breakdown (`components`) and the spread between the quotes used. It is
published only when the price, the status or the set of sources used
changes, and at most once per `publish_interval` (250 ms): bursts of
quotes collapse into one update.

All arithmetic is `rust_decimal`, as in the matching engine. The crate
denies float arithmetic.

## Alternatives considered

- **Volume-weighted average.** Fairer when volumes are comparable, but
  volume figures differ in meaning between venues and a manipulated venue
  can inflate its own volume. Median needs no trust in reported volume.
- **Primary venue with fallbacks.** Simple, but the primary's bad print
  goes straight to clients, and failover is a cliff instead of a gradient.
- **Publishing every quote.** Pushes the aggregation problem onto every
  client and multiplies fanout traffic by the number of sources.

## Consequences

- One bad or manipulated source cannot move a live index. A property test
  checks it for any agreeing cluster and any wild quote
  (`crates/marketdata/src/property_tests.rs`).
- `degraded` and `stale` make data quality visible to clients instead of
  hiding it behind a number. Automated consumers should act only on `live`.
- A market needs at least `quorum` independent sources to be `live`, which
  costs integration work per market. The DEX indexers are one way to get
  sources nobody can switch off.
- Updates are change-driven: a price that does not move is not
  republished, and `as_of` tells how fresh the backing quotes are.
