# ADR 0008: Price fanout over WebSocket and caching for traffic spikes

- Status: accepted
- Date: 2026-10-08

## Context

Market data traffic is the opposite of order traffic: a handful of
updates per second, read by thousands of clients, with spikes when prices
move and everyone opens a chart at once. Three things must hold:

1. Sources run once. Ten copies of the service must not mean ten times the
   API calls, rate limits and RPC load, or ten different index prices.
2. Delivery scales out. Adding WebSocket capacity is adding a node.
3. Spikes never reach the database. History reads are cacheable; live
   prices should not touch storage at all.

## Decision

**Two roles, one binary.** `PRICEFEED_ROLE=ingest` runs the sources and the
aggregator, writes Postgres and publishes to Redis (Valkey in compose,
ElastiCache in AWS). `PRICEFEED_ROLE=edge` serves REST and WebSocket from
memory, fed by a Redis subscription. `all` does both for local use.

**One ingest leader.** Ingest instances compete for a Postgres session
advisory lock. The holder ingests; the others stand by and take over when
its session ends, which takes about a second after a crash. Every write is
idempotent and every term publishes under a new epoch, so the short
overlap possible during failover cannot corrupt history or reorder prices.

**Redis pub/sub plus a snapshot.** The ingest node publishes each update on
one channel and also writes it to a hash of latest prices. An edge node
subscribes first and reads the hash second, so it starts with every price
and misses nothing in between. Duplicates are dropped by `(epoch,
sequence)`. Pub/sub is at most once; that is fine because only the newest
price matters and the next update supersedes a lost one.

**Conflating fanout on each edge.** `PriceHub` holds one `tokio::watch`
channel per market. A watch holds a single value: a slow client skips to
the newest price instead of building a backlog, so memory per client is
bounded and one slow client never delays others. Each update is
serialized to JSON once and every session sends the same reference
counted bytes. Sessions are bounded on every axis: connections per node
(503 at the limit, so the load balancer tries another node), markets per
connection, message size, control messages per heartbeat, send timeout
(slow consumers are dropped), and an idle timeout backed by pings. On
shutdown every session closes with code 1001 and clients reconnect to
another node.

**Caching tiers for history.** Candle reads go through decorators of one
`CandleReader` trait:

| Tier | Scope | Expiry | Absorbs |
|---|---|---|---|
| CloudFront (AWS) | per edge location | `Cache-Control`: 1 s live, 60 s closed windows | identical URLs from the internet |
| `MemoryCandleCache` (moka) | per process | same | concurrent identical reads, coalesced into one load |
| `RedisCandleCache` | all edge nodes | same | the same read on different nodes |
| Postgres | source of truth | none | one query per key and expiry window |

The in-process tier uses single flight: concurrent misses for the same
key wait for one load instead of starting their own (tested with 500
concurrent readers and one load). A Redis outage degrades reads to the
next tier instead of failing them. Live prices bypass all of this: they
are served from the hub's memory.

**Postgres write path.** Updates and candles are written in batches by a
background writer, with `UNNEST` arrays so a batch is one statement per
table. Only 1 minute candles are stored; longer intervals are rolled up in
the query with `date_bin`. The candle close carries the revision of the
update that set it, so retried or concurrent writes converge. The store is
never on the delivery path: if Postgres is down, prices keep flowing and
the gap is counted in `pricefeed_store_dropped_total`.

## Measured

`make load-test` against one release-build edge node and one ingest node,
Valkey and Postgres, all on one laptop (WSL2, 16 vCPU) with the load
generator on the same machine, live sources, 6 markets per client:

| Connections | Failed | Frames/s delivered | p50 latency | p99 latency | Edge memory |
|---|---|---|---|---|---|
| 1 | 0 | 1 | <= 1 ms | <= 50 ms | not measured |
| 10,000 | 0 | about 10,600 | <= 50 ms | <= 250 ms | 126 MiB |
| 20,000 | 0 | about 24,400 | <= 100 ms | <= 250 ms | 249 MiB |

Latency is measured from the ingest node's `published_at` to the client,
so it includes the Redis hop. The first run reached 1.4 GiB at 10,000
connections: the WebSocket library reserves 128 KiB of read buffer per
connection by default. Sizing buffers for this protocol (4 KiB in, flush
per frame, 64 KiB cap on unsent data) brought it to about 12 KiB per
connection.

## Alternatives considered

- **Kafka between ingest and edges.** Durable and replayable, but edges
  only need the latest price, and consumer group rebalancing would delay
  new edge nodes. Kafka stays the channel for exchange events.
- **Server-Sent Events.** Simpler, but one-directional: subscriptions would
  need a separate request per change.
- **Broadcast channels instead of watch.** Deliver every update, which
  forces a choice between unbounded memory and dropping clients that lag.
  For prices, conflation is the correct semantics.
- **Every node ingests.** No coordination needed, but multiplies upstream
  load and lets nodes publish slightly different prices.

## Consequences

- Edge nodes are stateless and scale on CPU; the AWS Terraform sets that
  up (ECS service autoscaling behind an ALB, CloudFront in front).
- The ingest leader is a single point of computation, made highly
  available by the standby rather than by sharding. One node computes the
  index for hundreds of markets, so sharding is not needed at this scale.
- Clients must handle reconnects and duplicates. The TypeScript SDK does
  both, and the protocol (snapshot on subscribe, `(epoch, sequence)`)
  makes it simple.
