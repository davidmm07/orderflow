# Orderflow

[![ci](https://github.com/davidmm07/orderflow/actions/workflows/ci.yml/badge.svg)](https://github.com/davidmm07/orderflow/actions/workflows/ci.yml)

Orderflow is a price-time priority matching engine with a trading API,
written in Rust. It is a reference for the kind of service that sits at the
core of an exchange, where correctness, security and operability matter
more than feature count.

If you are new to the codebase, follow [KT_README.MD](KT_README.MD) first.

## Features

Matching
- Limit, market, stop-limit and stop-market orders, with `gtc`, `ioc` and
  `fok` time in force.
- Stop orders wait outside the book and fire when the last trade price
  reaches them. A fired stop can move the price and fire further stops;
  the whole cascade settles within the request that caused it.
- Post-only orders and self-trade prevention (`cancel_newest`,
  `cancel_oldest`).
- Exact decimal arithmetic. The domain crate denies float arithmetic.

Instruments
- 16 markets over 14 assets out of the box: USD, EUR and stablecoin
  quotes plus an ETH-BTC cross.
- Listing another instrument is an edit to `config/instruments.json`. The
  catalog is validated as a whole at startup.
- Clients discover what is listed through `/v1/assets` and
  `/v1/markets?base=&quote=`.

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

`make env` creates two traders, A and B, because an account never trades
with itself. In a second terminal:

```bash
scripts/signed-request.sh POST /v1/markets/BTC-USD/orders '{"side":"sell","type":"limit","price":"64000.50","quantity":"0.5"}'
```

```bash
ORDERFLOW_CREDENTIAL=2 scripts/signed-request.sh POST /v1/markets/BTC-USD/orders '{"side":"buy","type":"market","quantity":"0.2"}'
```

```bash
curl -s localhost:8080/v1/markets/BTC-USD/book
```

To run every scenario flow against the running server (see
[Scenario flows](#scenario-flows-postman)):

```bash
make postman-env
```

```bash
make flows
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
| Registry | [`registry.rs`](crates/application/src/registry.rs), [`catalog.rs`](crates/domain/src/catalog.rs) | Read-only map from market to its actor; validated list of instruments |
| Adapter | [`extract.rs`](crates/api/src/extract.rs) | Axum extractors that reject with problem documents |
| Chain of Responsibility | [`router.rs`](crates/api/src/router.rs) | Middleware stack with one concern per layer |
| Data Transfer Object | [`dto.rs`](crates/api/src/dto.rs), [`wire.rs`](crates/infrastructure/src/events/wire.rs) | Public contracts kept separate from the domain model |
| Composition Root / Factory | [`app.rs`](crates/server/src/app.rs) | The single place where dependencies are injected |

## API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/v1/assets` | public | Listed assets with their precision |
| GET | `/v1/markets?base=&quote=` | public | Markets with their tick, lot and size limits, optionally filtered by asset |
| GET | `/v1/markets/{market}` | public | One market |
| GET | `/v1/markets/{market}/book?depth=N` | public | Aggregated book, best price first, with its sequence number and last trade price |
| POST | `/v1/markets/{market}/orders` | signed | Place an order; supports `Idempotency-Key` |
| GET | `/v1/markets/{market}/orders/{id}` | signed | Read one of your orders |
| DELETE | `/v1/markets/{market}/orders/{id}` | signed | Cancel one of your resting or pending orders |
| GET | `/health/live`, `/health/ready` | public | Probes |
| GET | `/metrics` | public | Prometheus text format |

The full contract is in [docs/openapi.yaml](docs/openapi.yaml) and the
error codes are in [docs/errors.md](docs/errors.md).

### Order types

| `type` | Required fields | Behavior |
|---|---|---|
| `limit` | `price` | Matches up to `price`; the rest follows `time_in_force` (`gtc` rests, `ioc` and `fok` never do) |
| `market` | none beyond `side` and `quantity` | Matches at the best prices; any rest is cancelled |
| `stop_limit` | `stop_price`, `price` | Waits as `pending` until a trade reaches `stop_price`, then acts as a limit order |
| `stop_market` | `stop_price` | Waits as `pending` until a trade reaches `stop_price`, then acts as a market order |

A buy stop fires when a trade prints at or above its stop price, and a sell
stop at or below. A stop the market has already reached is rejected with
`409 stop_would_trigger_immediately`. Pending stops can be cancelled and
never show in the public book. Fills in a placement response carry
`liquidity`: `taker` for matches on arrival, `maker` when a stop fired by
the same request traded against the order.

### Instruments

The catalog in [`config/instruments.json`](config/instruments.json) has two
lists: assets (code, name, decimals) and markets (base, quote, tick size,
lot size, quantity limits). To list a new instrument:

1. Add the asset to `assets` if it is not there yet.
2. Add the market to `markets`, referencing the base and quote codes.
3. Run `cargo test -p orderflow-server`; `bundled_catalog_is_valid` loads
   the file with the same rules as the server.
4. Deploy. The server starts one engine per market.

The loader rejects unknown assets, duplicates, a market whose base and
quote are the same, and lot sizes with more decimals than the base asset
supports, and it lists every problem in one message.
[ADR 0004](docs/adr/0004-instrument-catalog-as-data.md) covers how the
design scales further: listing without a restart and sharding markets
across nodes.

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

`make test` runs 103 tests, all offline:

| Suite | Count | What it covers |
|---|---|---|
| Domain unit tests | 41 | Matching rules, stop orders and cascades, value objects, the instrument catalog |
| Domain property tests | 2 x 256 cases | Random order flow, stops included, keeps every book invariant; matching is deterministic |
| Application tests | 7 | Use cases with hand written fakes, idempotency, load shedding |
| Infrastructure tests | 11 | TTLs and caps with paused time, retry policy, wire schema, metrics |
| API unit and HTTP tests | 15 + 18 | Signing, validation, stop orders, discovery, rate limits, timeouts, the full router end to end |
| Server tests | 9 | Config parsing, secret redaction, instrument catalog loading |

Set `PROPTEST_CASES` for longer property runs; 20,000 cases take about two
seconds in release mode.

`make ci` runs the same checks as the pipeline: formatting, clippy with
warnings as errors, and the tests. CI also starts the server and runs the
scenario flows below.

## Scenario flows (Postman)

[`postman/orderflow.postman_collection.json`](postman/orderflow.postman_collection.json)
holds eight scenario flows with 77 requests and 226 assertions. Each flow
is a folder that runs top to bottom, checks every response and passes ids
to the next request. A collection pre-request script signs private
requests, so nobody computes signatures by hand.

| Flow | Market | Covers |
|---|---|---|
| 01 Discovery and health | all | Probes, assets, market filters, one market, book, metrics |
| 02 Limit order lifecycle | LTC-USD | Place, read, owner-only access, cancel, cancel twice |
| 03 Matching and partial fills | ETH-USD | Partial fill at the maker price, market order, last price |
| 04 Time in force | SOL-USD | IOC, FOK in both outcomes, market order with no liquidity |
| 05 Post-only and self-trade prevention | AVAX-USD | Post-only rejection, `cancel_newest`, `cancel_oldest` |
| 06 Stop orders | LINK-USD | Pending stops, a passed stop rejected, buy and sell stops firing, cancel |
| 07 Idempotent retries | BTC-USD | Replay with the same key, key reuse, malformed key |
| 08 Authentication and validation errors | BTC-USD | 401, 404, 405 and 422 problem documents |

`make postman-env` writes `postman/local.postman_environment.json` from
`.env`. That file holds secrets and is git-ignored; the tracked
`postman/orderflow.postman_environment.json` is an empty template for
manual setup. Then either import both files into Postman and use the
Collection Runner, or run `make flows` (all flows) or
`make flows FLOW="06 Stop orders"` (one flow). Each flow uses its own
market and cleans up after itself, so the flows can run again and again
on the same server.

## Configuration

All settings come from environment variables. [.env.example](.env.example)
lists them with their defaults. The main ones:

| Variable | Default | Meaning |
|---|---|---|
| `ORDERFLOW_API_CREDENTIALS` | required | `key_id:account_id:secret` entries, comma separated |
| `ORDERFLOW_BIND_ADDR` | `127.0.0.1:8080` | Listen address |
| `ORDERFLOW_INSTRUMENTS_FILE` | `config/instruments.json` | Assets and markets to list |
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
config/instruments.json  assets and markets
docs/adr/           architecture decision records
docs/openapi.yaml   API contract
docs/errors.md      error catalog
postman/            scenario flows and an environment template
scripts/            env bootstrap, Postman env writer, signed request helper
```

## Commit convention

Commits follow [Conventional Commits](https://www.conventionalcommits.org):
`type(scope): description`, with a body of at most two short lines.

## Known limitations

- State is kept in memory. The planned next step is input journaling with
  snapshots, which the deterministic engine supports (see ADR 0003).
- It runs on a single node. Markets are already the shard key, but
  spreading them across nodes needs a routing layer.
- Listing or delisting an instrument needs a restart (ADR 0004 describes
  the path to listing without one).
- There is no streaming market data feed yet. `BroadcastPublisher` is the
  starting point for a WebSocket feed.
- Prices use `Decimal`. Integer ticks would make book comparisons cheaper,
  and the tick size needed for the conversion is already known per market.
- Replay protection relies on the timestamp window. A short-lived nonce
  cache would close the remaining 30 seconds.

## License

[MIT](LICENSE)
