# ADR 0002: One single-writer actor per market

- Status: accepted
- Date: 2026-09-25

## Context

Every order and every cancel mutates an order book. If request handlers
shared the book they would need a lock, and a lock around the book
serializes all writers anyway while adding contention, priority inversion
and the risk of holding it across an `.await`.

## Decision

Each market's `MatchingEngine` is owned by exactly one Tokio task, the
market actor. Handlers reach it through a `MarketHandle`, which sends a
command with a oneshot reply channel over a bounded `mpsc` queue.

- The engine is plain `&mut self` code, so the hot path takes no locks.
- Commands are applied in queue order, which gives each market a total
  order. Event sequence numbers have no gaps and match processing order.
- Markets are independent, which makes them the natural shard key, across
  tasks today and across nodes later.
- Handlers use `try_send`. When a queue is full the API answers
  `503 market_overloaded` with `Retry-After` right away, instead of queueing
  work that would finish after the market has moved.
- The actor waits on the bounded outbox. A slow event sink therefore slows
  the actor, fills its queue and triggers load shedding, and memory stays
  bounded.

## Consequences

- A busy market uses one core. Exchanges accept this trade because
  sequencing within a market is a requirement.
- Book snapshots (`GET /book`) go through the actor, so each snapshot is
  consistent with the sequence number it reports.
- Order lookups use the read model, so polling does not slow matching.
