# ADR 0005: Redpanda as the local Kafka broker

- Status: accepted
- Date: 2026-09-26

## Context

The service publishes every state change of every market to a Kafka topic
(ADR 0003). Developers and CI need a real broker to exercise that path end
to end: keys, partitions, delivery acknowledgements, idempotent producing.
The log sink (`ORDERFLOW_EVENT_SINK=log`) is enough for most local work,
but it cannot show partitioning or how a consumer reads the stream.

Options for the local stack:

1. Apache Kafka in a single container (KRaft mode, JVM).
2. Redpanda, a streaming platform that implements the Kafka protocol.
3. No broker at all, only the log sink.

## Decision

Use Redpanda in `docker-compose.yml`, in `dev-container` mode, as the
local and CI broker.

- It speaks the Kafka protocol, so the service uses the standard Kafka
  client (librdkafka through the `rdkafka` crate) with no Redpanda-specific
  code. The adapter is `crates/infrastructure/src/events/kafka.rs`.
- One container, no JVM, no separate controller or ZooKeeper, ready in a
  few seconds and a few hundred megabytes of memory. That keeps `make up`
  fast on a laptop and in CI.
- It ships `rpk`, a command line client used by `make consume` and
  `make topics` to read and inspect the topic.

Production does not have to run Redpanda. `ORDERFLOW_KAFKA_BROKERS` points
at whichever Kafka-compatible cluster the company operates: Apache Kafka,
Confluent, Amazon MSK or Redpanda.

## Consequences

- The event path is testable locally with the same client, the same
  producer settings (`enable.idempotence=true`, `acks=all`) and the same
  record keys as in production.
- `dev-container` mode trades durability for speed (for example it does
  not fsync every write). It is for development only, never for data that
  matters.
- Brokers differ in details such as retention defaults, quotas and some
  admin APIs. Staging should run the same broker product as production.
