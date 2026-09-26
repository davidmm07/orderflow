# ADR 0003: Event delivery guarantees and the durability roadmap

- Status: accepted
- Date: 2026-09-25

## Context

Downstream services (ledger, market data, risk) consume the event stream.
They need to know what is guaranteed about ordering, duplicates and loss.

## Decision

- Events are published at least once. The retry decorator can resend a
  batch that the broker partly accepted, so consumers must be idempotent.
- Every event carries `event_id = "{market}:{sequence}"`. Sequence numbers
  are contiguous per market, so a consumer can de-duplicate and detect gaps
  by keeping one integer per market.
- Kafka records are keyed by market id. Kafka orders messages within a
  partition, so every consumer sees each market's events in sequence order.
- The producer runs with `enable.idempotence=true` and `acks=all`.
- The wire format is an explicit, versioned JSON schema (`schema_version`),
  mapped by hand from the domain types. Decimals are strings. Additive
  changes keep the version: `stop_triggered` events and the optional
  `stop_price` field were added under version 1, so consumers must skip
  event types and fields they do not know.
- Transient errors are retried with exponential backoff and full jitter.
  Permanent errors, such as authorization failures or an unknown topic,
  are reported without retrying.

## Durability in this version

State lives in memory, so resting orders are lost if the process crashes.
This version leaves durability out of scope. The design is ready for the
usual approach:

1. Input journaling. Each command is appended to a replicated log before
   the actor applies it. The engine is deterministic given the command, the
   timestamp and the order id, all of which are recorded, so replaying the
   journal rebuilds the same book and the same event sequence.
2. Book snapshots every N sequence numbers to bound replay time.
3. Rebuilding the read model from the event topic, since the repository is
   a projection of it.

The property test `matching_is_deterministic` protects step 1. It fails if
the engine ever stops being a pure function of its inputs.
