# Orderflow

[![ci](https://github.com/davidmm07/orderflow/actions/workflows/ci.yml/badge.svg)](https://github.com/davidmm07/orderflow/actions/workflows/ci.yml)

Orderflow is a price-time priority matching engine with a trading API,
written in Rust. It is a reference for the kind of service that sits at the
core of an exchange, where correctness, security and operability matter
more than feature count.

If you are new to the codebase, follow [KT_README.MD](KT_README.MD) first.

## Features

Matching
- Limit and market orders with `gtc`, `ioc` and `fok` time in force.
- Post-only orders and self-trade prevention (`cancel_newest`,
  `cancel_oldest`).
- Exact decimal arithmetic. The domain crate denies float arithmetic.

API
- Versioned REST under `/v1` with HMAC-SHA256 signed requests.
- Per-account rate limiting and `Idempotency-Key` support.
- Strict validation that reports every invalid field in one response.
- RFC 9457 problem documents for every failure.

Events
- Every state change becomes a domain event with a gap-free sequence
  number per market.
- Events go to Kafka under a versioned JSON schema, keyed by market so
  each market keeps its order.

Operations
- Prometheus metrics, liveness and readiness probes, JSON logs with
  request ids.
- Load shedding, backpressure, retries with jittered backoff.
- Graceful shutdown that drains every queued order before exit.

## Quick start

You need `rustup`, `openssl` and `curl`. The toolchain pinned in
`rust-toolchain.toml` installs itself on the first `cargo` call.

```bash
make env
```

```bash
make run
```

In a second terminal:

```bash
scripts/signed-request.sh POST /v1/markets/BTC-USD/orders '{"side":"sell","type":"limit","price":"64000.50","quantity":"0.5"}'
```

```bash
curl -s localhost:8080/v1/markets/BTC-USD/book
```

To run the container image against Kafka (Redpanda):

```bash
make up
```

```bash
make consume
```

`make help` lists every target.

## Architecture

### Clean architecture over a layered one

The full reasoning is in
[ADR 0001](docs/adr/0001-clean-architecture-over-layered.md).

A layered architecture points dependencies down towards persistence:
presentation calls business logic, which calls data access. Applied here,
the matching engine would depend on how orders are stored and how events
are published. In an exchange the matching rules are the part that has to
last, while transports and brokers get replaced, so that direction is
wrong for this system.

Clean architecture points every dependency inwards, towards the domain.
The reasons it was chosen:

1. The rules outlive the plumbing. The API could move from REST to gRPC or
   FIX, and events could move off Kafka, without touching the domain crate.
2. Correctness is cheap to test. The engine is plain Rust with no IO, so
   property tests run thousands of random order flows against it in
   milliseconds, without containers or mocks.
3. Replay is possible. The engine receives time and ids as inputs and never
   reads them itself, so replaying the same command journal always rebuilds
   the same book.
4. The compiler enforces the boundaries. Each ring is its own crate, and
   `orderflow-domain` does not list tokio, axum or rdkafka as dependencies,
   so using them there fails the build.
5. A port can have several adapters. Events can go to the log, to Kafka or
   to both, and storage is in memory today with room for a database.

The cost is more types and explicit mapping at each boundary. A CRUD
service would not need that. The core of an exchange does.

### Dependency rule

```
                +-------------------------------+
                |        orderflow-server        |  composition root, config,
                |  (the only crate that sees     |  lifecycle, telemetry
                |   every concrete type)         |
                +-------------------------------+
                     |                     |
                     v                     v
     +---------------------+     +-------------------------+
     |    orderflow-api    |     | orderflow-infrastructure|
     | HTTP, auth, limits, |     | stores, clock, ids,     |
     | validation, errors  |     | metrics, Kafka, retries |
     +---------------------+     +-------------------------+
                     |                     |
                     v                     v
                +-------------------------------+
                |     orderflow-application      |  use cases, ports,
                |                                |  market actors, outbox
                +-------------------------------+
                               |
                               v
                +-------------------------------+
                |       orderflow-domain         |  order book, matching,
                |    (no async, no IO, no deps   |  value objects, events
                |     beyond decimal and uuid)   |
                +-------------------------------+
```

### Life of an order

```
client
  | POST /v1/markets/BTC-USD/orders  (signed)
  v
router: request id -> trace span -> panic guard -> timeout
  v
auth: buffer body (capped) -> verify HMAC in constant time -> account
  v
rate limit: token bucket for that account
  v
handler: parse JSON -> validate every field -> PlaceOrderCommand
  v
PlaceOrder use case (runs on its own task so a disconnect cannot cut it short)
  | idempotency reserve
  v
MarketHandle --try_send--> [bounded queue] --> market actor (single writer)
                                                   | MatchingEngine::submit
                                                   | events -> outbox (bounded)
                                                   | orders -> read model
  <------------------ reply (oneshot) -------------+
  | idempotency complete
  v
201 Created + Location + fills

outbox --> dispatcher (batches) --> retry decorator --> Kafka (key = market)
```

### Crates

| Crate | Ring | Responsibility |
|---|---|---|
| [`orderflow-domain`](crates/domain) | Entities | Value objects, order book, matching engine, domain events and errors |
| [`orderflow-application`](crates/application) | Use cases | `PlaceOrder`, `CancelOrder`, `OrderQueries`, ports, market actors, outbox |
| [`orderflow-infrastructure`](crates/infrastructure) | Adapters | In-memory stores, monotonic clock, UUIDv7 ids, Prometheus metrics, event sinks, Kafka |
| [`orderflow-api`](crates/api) | Delivery | Routes, DTOs, validation, HMAC auth, rate limiting, problem responses |
| [`orderflow-server`](crates/server) | Composition root | Env config, wiring, telemetry, graceful shutdown |

## SOLID in practice

The `lib.rs` of each crate lists the principles that crate applies, and the
doc comment of each type says which one it follows.

| Principle | Where | How |
|---|---|---|
| Single responsibility | `OrderBook`, `MatchingEngine`, `MarketSpec` | Resting liquidity, matching rules and trading parameters live in three types, each with one reason to change |
| Open/closed | `RetryingPublisher`, `FanoutPublisher`, tower layers | New behavior wraps existing code through decorators and layers |
| Liskov substitution | Every port | The in-memory repository, the test fakes and a future database adapter all follow the same documented contract |
| Interface segregation | `ports.rs` | `Clock`, `OrderIdGenerator`, `OrderRepository`, `EventPublisher`, `IdempotencyStore` and `Metrics` are separate, small traits |
| Dependency inversion | Application and server crates | Use cases depend on traits they own; only the composition root knows concrete types |

## Design patterns

| Pattern | Where | Why here |
|---|---|---|
| Ports and Adapters (Hexagonal) | [`ports.rs`](crates/application/src/ports.rs), infrastructure crate | Keeps technology choices out of the core |
| Actor / single writer | [`market.rs`](crates/application/src/market.rs) | One task owns each book, so there are no locks and every market has a total order |
| Command | [`commands.rs`](crates/application/src/commands.rs), actor `Request` | Requests are values that can be compared for idempotency, queued and replayed |
| Outbox | [`outbox.rs`](crates/application/src/outbox.rs) | Keeps broker latency off the matching path; bounded for backpressure |
| Repository | [`OrderRepository`](crates/application/src/ports.rs), [`orders.rs`](crates/infrastructure/src/memory/orders.rs) | Read model behind a collection-like interface |
| Decorator | [`retry.rs`](crates/infrastructure/src/events/retry.rs) | Adds retries with backoff and jitter to any publisher |
| Composite | [`fanout.rs`](crates/infrastructure/src/events/fanout.rs) | Several sinks behave as one |
| Observer (pub/sub) | [`broadcast.rs`](crates/infrastructure/src/events/broadcast.rs) | In-process subscribers to the event stream |
| Strategy | [`SelfTradePrevention`](crates/domain/src/order.rs) | Self-trade policy chosen per order and applied at one point |
| Builder | [`MarketSpecBuilder`](crates/domain/src/market.rs) | Validates a whole market configuration at once |
| Value Object / Newtype | [`numeric.rs`](crates/domain/src/numeric.rs), [`ids.rs`](crates/domain/src/ids.rs) | Invalid prices and ids cannot be represented |
| Null Object | [`NoopMetrics`](crates/application/src/ports.rs) | Removes `Option<Metrics>` checks from the runtime |
| Registry | [`registry.rs`](crates/application/src/registry.rs) | Read-only map from market to its actor |
| Adapter | [`extract.rs`](crates/api/src/extract.rs) | Axum extractors that reject with problem documents |
| Chain of Responsibility | [`router.rs`](crates/api/src/router.rs) | Middleware stack with one concern per layer |
| Data Transfer Object | [`dto.rs`](crates/api/src/dto.rs), [`wire.rs`](crates/infrastructure/src/events/wire.rs) | Public contracts kept separate from the domain model |
| Composition Root / Factory | [`app.rs`](crates/server/src/app.rs) | The single place where dependencies are injected |

## API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/v1/markets` | public | Markets with their tick, lot and size limits |
| GET | `/v1/markets/{market}/book?depth=N` | public | Aggregated book, best price first, with its sequence number |
| POST | `/v1/markets/{market}/orders` | signed | Place an order; supports `Idempotency-Key` |
| GET | `/v1/markets/{market}/orders/{id}` | signed | Read one of your orders |
| DELETE | `/v1/markets/{market}/orders/{id}` | signed | Cancel one of your resting orders |
| GET | `/health/live`, `/health/ready` | public | Probes |
| GET | `/metrics` | public | Prometheus text format |

The full contract is in [docs/openapi.yaml](docs/openapi.yaml) and the
error codes are in [docs/errors.md](docs/errors.md).

Some API design choices:

- Order resources nest under their market. The market is the shard key,
  so the path tells the server which actor owns the order.
- Decimals are strings on the wire. Most client languages read JSON
  numbers as binary floats, which cannot hold `0.1` exactly.
- Placement returns `201` with a `Location` header and the immediate fills
  in the body, so a client learns its execution in one round trip.
- Orders that belong to another account return `404`. A `403` would
  confirm that the id exists.

### Request signing

```
signature = hex(HMAC_SHA256(secret, timestamp + METHOD + path_and_query + body))
headers:    x-orderflow-key, x-orderflow-timestamp (unix seconds), x-orderflow-signature
```

The body is part of the signed string, so an intermediary cannot change an
order, and the timestamp window (30 seconds by default) limits replays.
`sign_request` in [`auth.rs`](crates/api/src/auth.rs) is the reference
implementation, and both [`examples/sign.rs`](crates/api/examples/sign.rs)
and [`scripts/signed-request.sh`](scripts/signed-request.sh) call it.

## Data validation

Validation happens in four layers:

1. Serde runs with `deny_unknown_fields` on every request type, so a typo
   such as `"prise"` is rejected instead of ignored.
2. The request validator in [`validation.rs`](crates/api/src/validation.rs)
   parses each field into domain types and collects every problem, so one
   response lists all of them. Decimals must be plain (`"101.25"`; `"1e2"`,
   `"-1"` and `" 1"` are rejected) and at most 40 characters long.
3. Value objects such as `Price`, `Quantity`, `MarketId` and `AccountId`
   can only be built through constructors that enforce their invariants.
4. `MatchingEngine::submit` checks tick size, lot size and limits again,
   because the domain does not rely on its callers having done it.

## Error handling

- Each ring has its own error type: `DomainError`, `ApplicationError` and
  `ApiError`. Each one wraps the inner one, and every variant has a stable
  `code()` used in logs, metrics and responses.
- The HTTP mapping is a `match` with no catch-all arm, so a new error
  variant fails to compile until someone assigns it a status.
- 5xx responses carry a generic `detail`. The real cause goes to the log
  together with the request id.
- Workspace lints deny `unwrap`, `expect`, `panic!`, `todo!` and
  `unimplemented!` outside tests. `CatchPanicLayer` still turns an
  unexpected panic into a 500 problem instead of a dropped connection.
- Placement runs on its own task and stores its result under the
  idempotency key, so a client can retry after a timeout, a 503 or a
  disconnect without creating a second order.

## Security

- Requests are signed with HMAC and verified in constant time, inside a
  replay window. Unknown keys go through the same HMAC work against a decoy
  secret, so response timing does not reveal which key ids exist.
- Secrets must be at least 32 bytes. Their `Debug` output is redacted, and
  helper tools read them from the environment instead of the command line.
- `.env` files are git-ignored. The repository only tracks
  [`.env.example`](.env.example), whose secret values are empty, and
  `make env` generates a local secret.
- Body size, decimal length, id length, the idempotency store and every
  queue have limits. Rate limit buckets are only created for authenticated
  accounts, so anonymous traffic cannot grow them.
- Identifiers are restricted to an allow list of characters and are safe
  to log or use as Kafka keys without escaping.
- `unsafe` code is forbidden in every crate. CI runs `cargo deny` for
  advisories, licenses and sources ([deny.toml](deny.toml)).
- The server binds to `127.0.0.1` unless configured otherwise. The
  container is distroless, runs as non-root and has a read-only filesystem.

## Reliability and performance

- Each market has a single writer
  ([ADR 0002](docs/adr/0002-single-writer-market-actors.md)), so the
  matching path takes no locks.
- A full engine queue answers `503 market_overloaded` right away instead
  of queueing work that would finish too late.
- The outbox is bounded. A slow broker slows the engine down instead of
  filling memory.
- Events are delivered at least once, with ids for de-duplication and
  per-market ordering
  ([ADR 0003](docs/adr/0003-event-delivery-and-durability.md)).
- On SIGTERM the server stops accepting requests, each engine drains its
  queue, the outbox closes and the dispatcher flushes the remaining events
  before the process exits.

Matching benchmark (`make bench`, Criterion, release build, AMD Ryzen 7
5700U under WSL2; results vary by machine):

| Scenario | Median |
|---|---|
| Rest a non-crossing limit order on a 200 order book | about 1.2 us |
| Sweep 10 price levels (40 fills) with one order | about 19 us |

## Observability

- Logs are JSON lines in production (`ORDERFLOW_LOG_FORMAT=json`). Each
  request runs in a span with its method, path and `x-request-id`.
- `/metrics` exposes `orderflow_orders_total{market,outcome}`,
  `orderflow_trades_total{market}`,
  `orderflow_engine_latency_seconds{market}` (histogram with buckets from
  1 us to 5 ms), `orderflow_events_published_total` and
  `orderflow_events_dropped_total`.
- `/health/live` reports that the process is up, and `/health/ready`
  reports that every market engine is running.

## Testing

`make test` runs 82 tests, all offline:

| Suite | Count | What it covers |
|---|---|---|
| Domain unit tests | 28 | Matching rules, value objects, market specs |
| Domain property tests | 2 x 256 cases | Random order flow keeps every book invariant; matching is deterministic |
| Application tests | 7 | Use cases with hand written fakes, idempotency, load shedding |
| Infrastructure tests | 11 | TTLs and caps with paused time, retry policy, wire schema, metrics |
| API unit and HTTP tests | 13 + 15 | Signing, validation, rate limits, timeouts, and the full router end to end |
| Server tests | 6 | Config parsing, secret redaction, bundled market file |

`make ci` runs the same checks as the pipeline: formatting, clippy with
warnings as errors, and the tests.

## Configuration

All settings come from environment variables. [.env.example](.env.example)
lists them with their defaults. The main ones:

| Variable | Default | Meaning |
|---|---|---|
| `ORDERFLOW_API_CREDENTIALS` | required | `key_id:account_id:secret` entries, comma separated |
| `ORDERFLOW_BIND_ADDR` | `127.0.0.1:8080` | Listen address |
| `ORDERFLOW_MARKETS_FILE` | `config/markets.json` | Market definitions |
| `ORDERFLOW_EVENT_SINK` | `log` | `log` or `kafka` (build with `--features kafka`) |
| `ORDERFLOW_KAFKA_BROKERS` | none | Required when the sink is `kafka` |
| `ORDERFLOW_RATE_LIMIT_PER_SEC` / `_BURST` | `50` / `100` | Per-account token bucket |
| `ORDERFLOW_ENGINE_QUEUE_CAPACITY` | `4096` | Per-market queue size before load shedding |
| `ORDERFLOW_LOG_FORMAT` | `json` | `json` or `pretty` |

The server reports every invalid setting at startup in a single message.
Errors about credentials refer to the entry by position and never print
its content.

## Project layout

```
crates/
  domain/           matching engine and model (plus benches/)
  application/      use cases, ports, actors, outbox
  infrastructure/   adapters, feature "kafka" for rdkafka
  api/              HTTP layer (plus examples/sign.rs)
  server/           the `orderflow` binary
config/markets.json market definitions
docs/adr/           architecture decision records
docs/openapi.yaml   API contract
docs/errors.md      error catalog
scripts/            env bootstrap and signed request helper
```

## Commit convention

Commits follow [Conventional Commits](https://www.conventionalcommits.org):
`type(scope): description`, with a body of at most two short lines.

## Known limitations

- State is kept in memory. The planned next step is input journaling with
  snapshots, which the deterministic engine supports (see ADR 0003).
- It runs on a single node. Markets are already the shard key, but
  spreading them across nodes needs a routing layer.
- There is no streaming market data feed yet. `BroadcastPublisher` is the
  starting point for a WebSocket feed.
- Prices use `Decimal`. Integer ticks would make book comparisons cheaper,
  and the tick size needed for the conversion is already known per market.
- Replay protection relies on the timestamp window. A short-lived nonce
  cache would close the remaining 30 seconds.

## License

[MIT](LICENSE)
