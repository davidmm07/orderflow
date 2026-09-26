# ADR 0004: Instruments are validated data

- Status: accepted
- Date: 2026-09-26

## Context

The exchange started with three markets. It needs to list more over time,
and the cost of listing one should stay flat: a configuration change that
goes through review, with no code change and no new failure modes.

Listing mistakes are expensive once trading starts. A tick size that is
too coarse, a lot size finer than the asset can hold, or a market that
references an asset nobody defined all have to be caught before an engine
starts.

## Decision

- Assets and markets live in one versioned file,
  `config/instruments.json`. Assets declare a code, a name and the number
  of decimals they support. Markets reference two assets and declare their
  tick size, lot size and quantity limits.
- `InstrumentCatalog` in the domain crate validates the file as a whole:
  asset codes are well formed and unique, market ids are unique, every
  market trades two different listed assets, and no lot size has more
  decimal places than its base asset.
- The loader reports every problem in the file at once and the server
  refuses to start on any of them.
- Each market still runs in its own actor (ADR 0002). Listing a market
  adds one Tokio task and one bounded queue; nothing else grows with the
  number of markets except the registry map, which is O(log n) to search.
- Clients discover instruments through `GET /v1/assets`,
  `GET /v1/markets?base=&quote=` and `GET /v1/markets/{market}` instead of
  hard coding them.
- Kafka records stay keyed by market id, so new markets spread over the
  existing partitions without a schema change.

## Consequences

- Listing an instrument is a pull request that edits one JSON file. The
  server test `bundled_catalog_is_valid` fails the build if the file breaks
  any rule.
- A restart is needed to list or delist. The engines hold state in memory
  today, so a restart also clears books; with the journal from ADR 0003 in
  place it would not.
- Paths for further growth, in order of need:
  1. Listing without restarts: swap the read-only registry for one behind
     an atomic pointer, spawn the new actor, then publish the new registry.
     The rest of the code already reaches markets only through the
     registry.
  2. More markets than one node can hold: assign markets to nodes with
     consistent hashing on the market id and route requests by that id.
     The market is already the shard key in the URL and in Kafka.
  3. Per-market states such as `halted` or `post_only`, added as a field
     on the market entry and checked by the engine before matching.
