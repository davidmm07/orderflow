# Orderflow: Knowledge Transfer

This guide walks a new developer through the project step by step. Do the
steps in order. Each one ends with a checkpoint; if you cannot meet it,
fix that before moving on, because later steps depend on it.

- [README.md](README.md) describes what the system does and why it is
  designed the way it is.
- This file covers how to work on it: setting up, running, reading the
  code, changing it and shipping.

Plan one working day for steps 1 to 11 and a second day for the guided
change in step 12.

---

## Step 0. Overview

Orderflow is a matching engine for spot markets such as `BTC-USD`, with an
HTTP API in front and an event stream behind.

```
clients --HTTP (signed)--> orderflow --events--> Kafka topic --> ledger, market data, risk...
```

Keep three design rules in mind while you read the code:

1. Clean architecture. The code is split into five crates arranged in
   rings, and dependencies point inwards to the domain. The domain has no
   async code, no IO and no framework.
2. One writer per market. Each market's book is owned by a single task (an
   actor). The matching path takes no locks, and every change to a market
   gets a sequence number with no gaps.
3. Every queue, body and cache has a limit. When a limit is reached the
   client receives a problem response with a stable `code`, and operators
   get a log line and a metric.

---

## Step 1. Set up your machine

| Tool | Version | Used for |
|---|---|---|
| rustup | any recent | Installs the pinned toolchain automatically |
| Rust | 1.98.1, pinned in `rust-toolchain.toml` | Build and tests |
| C toolchain (`gcc`, `make`) | any | Only for `--features kafka`, which compiles librdkafka from source |
| openssl, curl | any | `make env` and the request helper script |
| Docker with Compose v2 | any recent | Only for the Kafka, Prometheus and Grafana stack (steps 8 and 11) |

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

```bash
git clone https://github.com/davidmm07/orderflow.git
```

```bash
cd orderflow && cargo --version
```

The first `cargo` call reads `rust-toolchain.toml` and installs 1.98.1 with
clippy and rustfmt, so everyone on the team uses the same compiler.

> Checkpoint: `cargo --version` prints `cargo 1.98.1`.

---

## Step 2. Build and run the tests

```bash
make test
```

After the first build, six suites with 110 tests should pass in well under a
minute:

| Suite | Where | What it checks |
|---|---|---|
| Domain unit tests | `crates/domain/src/**` (`#[cfg(test)]`) | Each matching rule, value object and market check |
| Domain property tests | [`property_tests.rs`](crates/domain/src/property_tests.rs) | Random order flows keep every book invariant, and matching is deterministic |
| Application tests | [`crates/application/tests/use_cases.rs`](crates/application/tests/use_cases.rs) | Use cases, idempotency and load shedding, with fakes in place of infrastructure |
| Infrastructure tests | `crates/infrastructure/src/**` | TTLs, caps, retries and backoff with paused time; wire schema; metrics |
| API tests | [`crates/api/tests/http.rs`](crates/api/tests/http.rs) and unit tests | The real router end to end, with in-memory adapters |
| Server tests | [`config.rs`](crates/server/src/config.rs) | Env parsing and secret redaction |

Then run the same checks as CI:

```bash
make ci
```

> Checkpoint: `make ci` finishes with no errors or warnings.

---

## Step 3. Run the server and place your first order

Create your local configuration. This copies `.env.example` to `.env` and
fills in two traders, `trader-a` and `trader-b`, each with a newly
generated API secret. Two accounts are needed to see trades, because an
account never trades with itself. `.env` is git-ignored. Do not commit it,
and do not paste its contents into tickets or chats.

```bash
make env
```

Start the server with the log event sink and readable logs:

```bash
make run
```

In a second terminal, call the public endpoints:

```bash
curl -s localhost:8080/health/ready
```

```bash
curl -s localhost:8080/v1/markets
```

Place a resting sell order. The helper script signs the request with the
first key in `.env` (trader A). The first call compiles a small signing
tool, which takes a few seconds.

```bash
scripts/signed-request.sh POST /v1/markets/BTC-USD/orders '{"side":"sell","type":"limit","price":"64000.50","quantity":"0.5","client_order_id":"my-first-order"}'
```

The response is `201` with a `Location` header, `"status":"open"` and an
empty `fills` array. Now look at the book:

```bash
curl -s 'localhost:8080/v1/markets/BTC-USD/book?depth=5'
```

Send a market buy with the same key:

```bash
scripts/signed-request.sh POST /v1/markets/BTC-USD/orders '{"side":"buy","type":"market","quantity":"0.2"}'
```

It comes back `cancelled` with `cancel_reason: self_trade_prevention`
because both orders belong to trader A, and an account may not trade with
itself. Send the same buy as trader B, the second entry in `.env`:

```bash
ORDERFLOW_CREDENTIAL=2 scripts/signed-request.sh POST /v1/markets/BTC-USD/orders '{"side":"buy","type":"market","quantity":"0.2"}'
```

This time the order is `filled` at `64000.5`, and `fills` shows the trade.

Finally, send an invalid order and read the error:

```bash
scripts/signed-request.sh POST /v1/markets/BTC-USD/orders '{"side":"up","type":"limit","price":"-1","quantity":"abc"}'
```

A single `422` response lists every invalid field. The codes are described
in [docs/errors.md](docs/errors.md).

> Checkpoint: you have seen a `201`, a self-trade cancel, a trade between
> two accounts, a `422` with several field errors, and your order in the
> book.

---

## Step 4. Run the scenario flows

`postman/collections/Orderflow scenarios/` holds eight scenario flows in the
Postman Collection v3 YAML format, the format Postman v12 uses for Native
Git. Every request is its own `.request.yaml` file with its tests inline;
each folder has a `.resources/definition.yaml`. A flow runs top to bottom,
checks every response and passes ids from one request to the next. It is
the fastest way to see the whole API working, and CI runs the same flows on
every push.

| Flow | Market | What it shows |
|---|---|---|
| 01 Discovery and health | all | Setup preflight, probes, assets, market filters, one market, book, metrics |
| 02 Limit order lifecycle | LTC-USD | Place, read, owner-only access, cancel, cancel twice |
| 03 Matching and partial fills | ETH-USD | Partial fill at the maker price, market order, last price |
| 04 Time in force - IOC, FOK and market | SOL-USD | IOC, FOK in both outcomes, market order with no liquidity |
| 05 Post-only and self-trade prevention | AVAX-USD | Post-only rejection, `cancel_newest`, `cancel_oldest` |
| 06 Stop orders | LINK-USD | Pending stops, a passed stop rejected, buy and sell stops firing, cancel |
| 07 Idempotent retries | BTC-USD | Replay with the same key, key reuse, malformed key |
| 08 Authentication and validation errors | BTC-USD | 401, 404, 405 and 422 problem documents |

First write the `Orderflow local` environment from your `.env`. It goes to
`postman/environments/orderflow-local.environment.yaml`, holds your
secrets, is git-ignored and must not be shared.

```bash
make postman-env
```

### In the Postman desktop app (v12 or later)

1. Open **Files**, click **Open folder** and choose the repository root:
   the folder that contains `postman/` and `Cargo.toml`. Do not choose
   `postman/` itself; Postman would create a second `postman/` inside it.
   Postman records your own cloud workspace in `.postman/resources.yaml`;
   that folder is git-ignored so nobody's workspace id is published.
2. Switch to **Local View**. The `Orderflow scenarios` collection and the
   `Orderflow local` and `Orderflow template` environments appear on their
   own; nothing needs importing.
3. Select the `Orderflow local` environment.
4. With the server from step 3 running, open the collection, click
   **Run**, and run one folder or all of them with the Collection Runner.

The collection script signs every private request, so you never compute a
signature by hand. Edits you make in Local View are saved straight into the
YAML files, so they show up in `git diff` like code.

### From the terminal

The flows run with the Postman CLI, installed on demand through `npx`
(Node 20 or later). Newman, the older runner, cannot read the v3 format.

```bash
make flows
```

To run a single flow:

```bash
make flows FLOW="06 Stop orders"
```

To check the files against the collection schema after editing them:

```bash
make flows-lint
```

Each flow uses its own market and cancels what it leaves on the book, so
the flows can run again on the same server. If a run stops halfway,
restart the server to start from empty books.

### When the flows fail

The first request of flow 01, `Preflight - server build and environment`,
checks the setup. Read its failure message first.

| Symptom | Cause | Fix |
|---|---|---|
| Preflight: `GET /v1/assets must answer 200. An older build answers 404` | The server on port 8080 runs a build from before the flows existed, often a `make up` stack started earlier | Stop it and start the current code: `make run`, or `make down` then `make up` to rebuild the image |
| Preflight: `trader_a_key is empty`, or signed requests stop with `Environment is missing trader_a_key...` | The `Orderflow template` environment is selected, or `make postman-env` was never run | Run `make postman-env` and select `Orderflow local` |
| `make postman-env` says `.env` has 1 trader | `.env` was created before `make env` made two traders | `scripts/bootstrap-env.sh --add-trader`, restart the server, run `make postman-env` again |
| Many `401 unauthenticated` | The environment does not match the server's `.env`, for example after regenerating `.env` | Restart the server and run `make postman-env` again |
| A `postman/postman/` folder appears | The `postman/` folder was opened instead of the repository root | Close it in Postman, delete the nested folder, open the repository root |
| `404 unknown_market` in a flow | The server loads an old instruments file | Check `ORDERFLOW_INSTRUMENTS_FILE` points at `config/instruments.json` |

If `make postman-env` reports a single trader, add one and restart the
server:

```bash
scripts/bootstrap-env.sh --add-trader
```

> Checkpoint: the preflight passes, and the Collection Runner (or
> `make flows`) reports 0 failed requests and 0 failed assertions.

---

## Step 5. Understand the architecture

Read [ADR 0001](docs/adr/0001-clean-architecture-over-layered.md), which
explains why the project uses clean architecture instead of layers. Then
map it to the code:

| Ring | Crate | Can depend on | Cannot depend on |
|---|---|---|---|
| Domain | `orderflow-domain` | `rust_decimal`, `uuid`, `thiserror` | tokio, axum, serde, rdkafka |
| Application | `orderflow-application` | domain, tokio `sync`/`rt` | axum, rdkafka, concrete adapters |
| Infrastructure | `orderflow-infrastructure` | application, domain | api |
| Delivery | `orderflow-api` | application, domain | infrastructure, except as a dev-dependency for tests |
| Composition root | `orderflow-server` | everything | (nothing depends on it) |

The rule is enforced through `Cargo.toml`. A `use axum` in the domain does
not compile. The header comment of each crate's `lib.rs` lists the SOLID
principles that ring applies.

Exercise: find the `Clock` port. Where is it declared, where is it
implemented, and where is the implementation chosen?

<details><summary>Answer</summary>

It is declared in [`crates/application/src/ports.rs`](crates/application/src/ports.rs),
implemented by `MonotonicClock` in
[`crates/infrastructure/src/clock.rs`](crates/infrastructure/src/clock.rs),
and chosen in `build` in [`crates/server/src/app.rs`](crates/server/src/app.rs).
The domain sees none of them; it receives a `Timestamp` argument.

</details>

> Checkpoint: you can explain why the domain crate receives the time as an
> argument instead of reading a clock.

---

## Step 6. Follow one order through the code

Open these files side by side and trace a `POST /v1/markets/BTC-USD/orders`
from the socket to Kafka. This walk-through covers most of what you need to
know about the code.

| # | File | What happens |
|---|---|---|
| 1 | [`api/src/router.rs`](crates/api/src/router.rs) `router` | Every request gets a request id, a tracing span, a panic guard and a timeout. Private routes add auth and rate limiting through `route_layer`. |
| 2 | [`api/src/auth.rs`](crates/api/src/auth.rs) `require_signature` | Reads the body up to the limit, verifies the HMAC in constant time and stores `Authenticated(account)` on the request. |
| 3 | [`api/src/rate_limit.rs`](crates/api/src/rate_limit.rs) `enforce` | Takes a token from the account's bucket or answers 429. |
| 4 | [`api/src/handlers/orders.rs`](crates/api/src/handlers/orders.rs) `place` | Extracts the path, headers and JSON, loads the market spec and reads `Idempotency-Key`. |
| 5 | [`api/src/validation.rs`](crates/api/src/validation.rs) `into_command` | Converts the DTO into a `PlaceOrderCommand` of domain types and collects every field error. |
| 6 | [`application/src/use_cases.rs`](crates/application/src/use_cases.rs) `PlaceOrder::execute` | Runs on its own task for cancellation safety, reserves the idempotency key and assigns an order id. |
| 7 | [`application/src/market.rs`](crates/application/src/market.rs) `MarketHandle::call` | Calls `try_send` on the market's bounded queue. A full queue means `503 market_overloaded`. |
| 8 | same file, `MarketActor::place` | The single writer reads the clock, calls the engine and commits the result. |
| 9 | [`domain/src/engine.rs`](crates/domain/src/engine.rs) `MatchingEngine::submit` | Validation, the post-only check, the fill-or-kill pre-check, the matching loop, the remainder policy and the events. |
| 10 | [`application/src/market.rs`](crates/application/src/market.rs) `commit` | Sends events to the outbox first, as the source of truth, then writes orders to the read model. |
| 11 | [`application/src/outbox.rs`](crates/application/src/outbox.rs) `EventDispatcher::run` | Batches queued events and passes them to the publisher. |
| 12 | [`infrastructure/src/events/retry.rs`](crates/infrastructure/src/events/retry.rs) then [`kafka.rs`](crates/infrastructure/src/events/kafka.rs) | Backoff with jitter around the Kafka producer; records are keyed by market. |
| 13 | back in `orders.rs` | Returns `201 Created` with `Location`, the order and its fills. |

Run `RUST_LOG=debug make run` and send an order. Each log line carries the
span fields `method`, `path` and `request_id`, which you can match against
this table.

> Checkpoint: for each of the 13 rows you can say which task runs it: the
> request task, the placement task, the market actor or the dispatcher.

---

## Step 7. The matching engine

Files: [`book.rs`](crates/domain/src/book.rs),
[`engine.rs`](crates/domain/src/engine.rs),
[`order.rs`](crates/domain/src/order.rs).

Data structures. Bids and asks are two `BTreeMap<Price, PriceLevel>`,
which give the best price in O(log n) and return depth already sorted.
Each level is a FIFO `VecDeque<OrderId>` with a running total. A
`HashMap<OrderId, Order>` holds the orders themselves, so a cancel finds
its order in O(1).

Algorithm (price-time priority):

1. Validate against the `MarketSpec` (tick, lot, minimum, maximum). A
   failure rejects the order with no side effects.
2. Reject a post-only order that would cross before anything else happens.
3. Emit `OrderAccepted`.
4. For fill-or-kill, `fillable_quantity` walks the book without changing
   it. If the order cannot fill completely it is cancelled and the book
   stays as it was.
5. Loop: take the oldest order at the best opposite price while that price
   crosses the taker's limit. Trade at the maker's price and emit
   `TradeExecuted`.
6. Self-trade: `cancel_newest` stops and cancels the rest of the taker;
   `cancel_oldest` cancels the resting order and continues matching.
7. Remainder: `gtc` orders rest on the book, `ioc` and market orders are
   cancelled.

Stop orders (`stop_limit`, `stop_market`) live in a separate `StopBook` in
[`stops.rs`](crates/domain/src/stops.rs) and are not visible in the book.
A buy stop fires when a trade prints at or above its stop price, a sell
stop at or below. After every submission the engine runs
`run_stop_cascade`: it fires every reached stop, lowest buy stop and
highest sell stop first, and matches it like a new order. A fired stop can
trade and move the price, which can fire more stops, so the loop repeats
until none is reached. A stop the market has already passed is rejected
with `stop_would_trigger_immediately`, so no stop can wait forever by
mistake.

`OrderBook::assert_consistent` runs after every step of every property
test and checks the invariants: no empty levels, level totals equal to the
sum of remaining quantities, no terminal order on the book, and a book
that is never crossed (best bid below best ask).

Exercise: add a unit test to the `tests` module in `engine.rs` showing that
an `ioc` buy that crosses nothing is cancelled with reason
`immediate_or_cancel` and leaves the sequence number at 2. Use the
existing `Harness` helper.

```bash
cargo test -p orderflow-domain engine
```

Changes to the engine can change its speed. Measure the matching hot path
before and after with Criterion (release build, a few minutes); the
README lists reference numbers:

```bash
make bench
```

> Checkpoint: your new test passes, and you can explain why a trade uses
> the maker's price.

---

## Step 8. Events and Kafka

Every state change in a market is published as an event, and the rest of
an exchange (ledger, market data, risk, notifications, audit) is built by
reading that stream. The README section "Event streaming" lists those
consumers and what each one does with the events.

Locally the broker is Redpanda: a Kafka-compatible streaming platform that
runs as one container. The service talks plain Kafka through librdkafka
and has no Redpanda-specific code, so production can use any
Kafka-compatible cluster. [ADR 0005](docs/adr/0005-redpanda-as-local-kafka.md)
explains the choice.

Read [ADR 0003](docs/adr/0003-event-delivery-and-durability.md). Stop the
server from step 3, because the stack uses the same port 8080, and start
Redpanda (Kafka compatible) with the production image. Compose reads your
credentials from `.env`.

```bash
make up
```

```bash
scripts/signed-request.sh POST /v1/markets/ETH-USD/orders '{"side":"sell","type":"limit","price":"3200","quantity":"1.5"}'
```

```bash
make consume
```

Things to check in each record:

- The key is the market id. Kafka keeps order within a partition and the
  key sends every event of a market to the same partition, so each market
  stays ordered.
- `event_id` is `{market}:{epoch}:{sequence}`. Delivery is at least once,
  so consumers de-duplicate on it and use the sequence to detect gaps.
  `epoch` is the start time of the service run in milliseconds; sequences
  count from 1 again after every restart, and the epoch tells the runs
  apart.
- `schema_version` is 2. The JSON shape is defined in
  [`wire.rs`](crates/infrastructure/src/events/wire.rs), mapped by hand
  from the domain types and covered by a test. A breaking change needs a
  new version.
- The `event_type`, `schema_version` and `content-type` headers let
  consumers route records without parsing the body.

### Inspect the topic with `make topics`

`make topics` describes the topic: its partitions, and how many records
each one holds. It only reads, so it is safe to run at any time while the
stack is up.

```bash
make topics
```

Example output after a few scenario flows:

```
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          0       1      [0]       0                 0
1          0       1      [0]       0                 0
2          0       1      [0]       0                 2
3          0       1      [0]       0                 5
4          0       1      [0]       0                 8
5          0       1      [0]       0                 32
```

| Column | Meaning |
|---|---|
| `PARTITION` | The partition number; the topic is created with six |
| `LEADER`, `REPLICAS` | The broker that owns the partition; a single broker (`0`) locally |
| `EPOCH` | The broker's leadership epoch for that partition, unrelated to the event `epoch` field |
| `LOG-START-OFFSET` | Offset of the oldest record still stored |
| `HIGH-WATERMARK` | Offset the next record will get; with nothing deleted yet, the number of records in the partition |

To see which markets land on which partition, read the keys of the
records already stored (it stops after 200 records):

```bash
docker compose exec redpanda rpk topic consume orderflow.events.v1 --offset start --num 200 --format '%p %k\n' | sort -u
```

```
2 BTC-USD
3 ETH-USD
4 AVAX-USD
5 LINK-USD
5 LTC-USD
5 SOL-USD
```

Every market appears on exactly one partition, because records are keyed
by market id. That is what keeps each market's events in order. Several
markets can share a partition, and some partitions can stay empty; with 16
markets and 6 partitions both are normal. If a market ever showed up on
two partitions, its events could be read out of order, and that would be a
bug to report.

Use `make consume` to watch new records arrive, and `make topics` to
check where they went and how many there are.

A consumer keeps the last `(epoch, sequence)` it applied for each market
and processes each record in this order:

1. Parse it.
2. If its epoch is higher than the last one, a new run started: accept
   sequence 1 of that epoch as the next event.
3. If the epoch is the same and the sequence is not above the last one,
   it is a duplicate: skip it.
4. If the sequence jumps by more than one, events are missing: stop and
   alert.
5. Apply it, then commit the offset. Committing after applying is what
   makes the consumer safe to crash and restart.

```bash
make down
```

> Checkpoint: you saw your order as an `order_accepted` record and can
> explain why a consumer has to tolerate duplicates.

---

## Step 9. Security model

| Threat | Control | Where |
|---|---|---|
| Forged or altered request | HMAC-SHA256 over timestamp, method, path and body | [`auth.rs`](crates/api/src/auth.rs) |
| Replay of a captured request | 30 second timestamp window | `Authenticator::verify` |
| Timing attacks on the signature | `verify_slice` compares in constant time | same |
| Probing which key ids exist | Unknown keys run the same HMAC against a decoy secret | same |
| Secrets in logs | Hand written `Debug` for `Credential` and `Secret`; config errors refer to entries by position | `auth.rs`, [`config.rs`](crates/server/src/config.rs) |
| Secrets in git | `.env*` is ignored; only `.env.example` with empty values is tracked | [`.gitignore`](.gitignore) |
| Secrets in the process list | Tools read the secret from the environment instead of arguments | [`scripts/signed-request.sh`](scripts/signed-request.sh) |
| Memory exhaustion | Limits on body size, decimal length, ids, the idempotency store and queues | throughout |
| Enumerating other accounts' orders | Foreign orders are reported as 404 | `OrderQueries::order`, `MatchingEngine::cancel` |
| Log or header injection | Ids use an ASCII allow list and errors do not echo raw input | [`ids.rs`](crates/domain/src/ids.rs), [`market.rs`](crates/domain/src/market.rs) |
| Vulnerable dependencies | `cargo deny` checks advisories, licenses and sources in CI | [`deny.toml`](deny.toml) |
| Container attack surface | Distroless image, non-root user, read-only filesystem | [`Dockerfile`](Dockerfile), [`docker-compose.yml`](docker-compose.yml) |

Rules for your own changes: never log a secret, never put one in a URL,
never add a credential to a tracked file, and never return internal error
text in a 5xx `detail`.

> Checkpoint: you can explain why the signature covers the body, and why
> the rate limiter runs after authentication.

---

## Step 10. Validation and errors

Validation has four layers, described under "Data validation" in the
README: strict serde, the request validator that collects every field
error, value objects that cannot hold invalid data, and a second check in
the engine.

Errors follow the same rings: `DomainError`, then `ApplicationError`, then
`ApiError`. The HTTP mapping in [`error.rs`](crates/api/src/error.rs) is an
exhaustive `match`, so a new variant fails to compile until it has a
status code.

To add an error, follow "Adding a new error" at the end of
[docs/errors.md](docs/errors.md). The `assert_problem` helper in the HTTP
tests checks the status, content type, `code` and `type` in one call.

> Checkpoint: you can name the four validation layers and say which one
> produces `invalid_body` and which one produces `price_not_on_tick`.

---

## Step 11. Observability and operations

Logs are JSON in production. Every line of a request includes its
`request_id`, which the client also receives as `x-request-id`. When a user
reports a problem, ask for that header first.

Metrics are served at `/metrics`, from the matching engine
(`orderflow_orders_total`, `orderflow_trades_total`,
`orderflow_engine_latency_seconds`, `orderflow_events_*`) and from the
HTTP layer (`orderflow_http_requests_total`,
`orderflow_http_request_duration_seconds`, `orderflow_http_problems_total`):

```bash
curl -s localhost:8080/metrics | grep '^orderflow_'
```

`make up` also starts Prometheus and Grafana. Try this once to see the
whole loop:

1. Start the stack and generate traffic with the scenario flows:

   ```bash
   make up
   ```

   ```bash
   make flows
   ```

2. Open http://127.0.0.1:3000/d/orderflow. The dashboard refreshes every 5
   seconds; the market filter at the top narrows the engine panels.
3. Open http://127.0.0.1:9090/alerts and see six rules, all inactive.
4. Stop the service with `docker compose stop orderflow`. After about a
   minute `OrderflowDown` goes from pending to firing, and the dashboard's
   "Firing alerts" stat turns red. Start it again with
   `docker compose start orderflow` and the alert resolves.

Alert rules, scrape config and the dashboard are code under `deploy/`.
After changing any of them, run the checks CI runs:

```bash
make monitoring-check
```

To change the dashboard, edit `deploy/grafana/dashboards/orderflow.json`
(Grafana reloads it within 10 seconds); edits made in the Grafana UI are
not saved.

Shutdown sequence on SIGTERM: HTTP stops accepting connections, in-flight
requests finish, each market engine drains its queue, the outbox closes,
the dispatcher flushes, and the process exits.
`ORDERFLOW_SHUTDOWN_GRACE_SECS` sets the upper bound.

Runbook. The first column matches the alert names in
`deploy/prometheus/alerts.yml` where one exists:

| Symptom | Likely cause | First action |
|---|---|---|
| Alert `OrderflowDown` | The process or container is gone, or `/metrics` is unreachable | Check the container state and its last log lines |
| Alert `MatchingLatencyHigh` | A market is busy, or the event sink is slow and backpressure reaches the engine | Compare "Event pipeline" and "Trades by market" on the dashboard before adding capacity |
| Alert `ServerErrorRateHigh` | Timeouts, overload or a bug | Read "Problems by code" on the dashboard; `internal_error` means a bug, look up its request ids in the logs |
| Alert `AuthenticationFailuresHigh` | A client with a broken clock or secret, or someone guessing keys | Find the key id in the logs by request id and contact its owner |
| Alert `LoadShedding`, or `503 market_overloaded` rising | A market's engine queue is full | Check `orderflow_engine_latency_seconds` for that market and the event sink; raise `ORDERFLOW_ENGINE_QUEUE_CAPACITY` only once the cause is known |
| Alert `EventsDropped` | The sink kept failing after all retries | Check broker health and look for "dropping event batch" in the logs; the line includes the first and last sequence to replay |
| Many `401` for one client | Clock skew, wrong secret, or signing the wrong path | Compare the client's timestamp with server time; confirm they sign `path_and_query` exactly as sent |
| `429` for one account | Token bucket exhausted | Expected under abuse; otherwise review `ORDERFLOW_RATE_LIMIT_*` |
| `/health/ready` returns 503 | A market actor stopped | Look for "market engine stopped" in logs outside a shutdown |
| Process exits at startup | Invalid configuration | Read the first lines of output, which list every problem |

> Checkpoint: you can find the p99 matching latency for `BTC-USD` on the
> dashboard, you have watched `OrderflowDown` fire and resolve, and you can
> describe each shutdown stage.

---

## Step 12. Guided first change

Add a minimum notional rule: for each market, `price * quantity` of an
order must be at least `min_notional`. The change touches every ring.

1. Domain. Add an optional `min_notional` to `MarketSpec` and its builder
   in [`market.rs`](crates/domain/src/market.rs). Add
   `DomainError::NotionalBelowMinimum` with a `code()`. Check it in
   `MarketSpec::validate` for limit orders using `checked_mul`, and return
   an error on overflow. Write the unit tests first.
2. Engine. No change is needed because it already calls `validate`. Add a
   test in `engine.rs` confirming that a rejected order emits no events.
3. Application. No change is needed either. Think about why: the use case
   only coordinates, and the rule belongs to the market.
4. API. Map the new variant in [`error.rs`](crates/api/src/error.rs), which
   the compiler will require, to a field error on `quantity`. Optionally
   check it in [`validation.rs`](crates/api/src/validation.rs) as well, so
   it is reported together with the other field errors. Add `min_notional`
   to `MarketView` in [`dto.rs`](crates/api/src/dto.rs).
5. Config. Accept `min_notional` in `MarketEntry` in
   [`instruments.rs`](crates/server/src/instruments.rs) and add it to the
   markets that need it in
   [`config/instruments.json`](config/instruments.json).
6. Tests. Add an HTTP test with `assert_problem`, keep the server test
   `bundled_catalog_is_valid` passing, and extend a Postman flow if clients
   can see the rule.
7. Docs. Add a row to [docs/errors.md](docs/errors.md) and the field to
   [docs/openapi.yaml](docs/openapi.yaml).
8. Ship. Run `make ci`, commit following step 13 and open a pull request.

> Checkpoint: `make ci` passes and a reviewer can follow your commits one
> ring at a time.

---

## Step 13. Team conventions

Commits follow [Conventional Commits](https://www.conventionalcommits.org):

```
type(scope): short description in the imperative

At most two short lines of body, only when the subject needs context.
```

- Types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`, `ci`,
  `chore`. Scopes: `domain`, `application`, `infrastructure`, `api`,
  `server`, `postman`.
- Keep the description brief.

Code rules:

- No `unwrap`, `expect`, `panic!`, `todo!` or `unimplemented!` outside
  tests. The workspace lints deny them, so return an error instead.
- No floats in the domain; `clippy::float_arithmetic` is denied there.
- No `unsafe`; it is forbidden in every crate.
- Comments explain why the code does something, in plain sentences. When
  a type implements a design pattern or a SOLID principle, name it in the
  doc comment the way the existing code does.
- Format with `make fmt` before committing. `make lint` runs the same
  format check and clippy as CI's lint job, and `make ci` adds the tests.

Pull request checklist:

- [ ] `make ci` passes locally.
- [ ] New behavior has tests in the lowest ring that can express it.
- [ ] New errors have a stable `code`, an HTTP mapping and a row in `docs/errors.md`.
- [ ] API changes are reflected in `docs/openapi.yaml`.
- [ ] The diff contains no secrets, `.env` files or key material.
- [ ] Commits follow the convention above.

---

## Command reference

`make help` prints this list. The step that introduces each command explains
it in context.

| Command | What it does | Introduced in |
|---|---|---|
| `make env` | Creates `.env` with two traders and fresh API secrets | Step 3 |
| `make run` | Runs the server locally with the log event sink | Step 3 |
| `make test` | Runs every test in the workspace | Step 2 |
| `make lint` | Checks formatting and runs clippy with warnings as errors | Step 13 |
| `make fmt` | Formats the code | Step 13 |
| `make ci` | Runs lint and tests, the same checks as the CI pipeline | Step 2 |
| `make bench` | Benchmarks the matching engine | Step 7 |
| `make postman-env` | Writes the `Orderflow local` Postman environment from `.env` | Step 4 |
| `make flows` | Runs the Postman scenario flows; `FLOW="06 Stop orders"` runs one | Step 4 |
| `make flows-lint` | Validates the flows against the Postman collection schema | Step 4 |
| `make up` | Builds and starts the service with Redpanda, Prometheus and Grafana | Step 8 |
| `make consume` | Prints events from the topic as they arrive | Step 8 |
| `make topics` | Shows the topic's partitions and how many records each holds | Step 8 |
| `make down` | Stops the stack and removes its volumes | Step 8 |
| `make monitoring-check` | Validates the Prometheus config, tests the alert rules, checks the dashboard | Step 11 |

Scripts outside the Makefile:

| Script | What it does | Introduced in |
|---|---|---|
| `scripts/bootstrap-env.sh --add-trader` | Appends one more trader to an existing `.env` | Step 4 |
| `scripts/signed-request.sh METHOD PATH [BODY]` | Sends one signed request; `ORDERFLOW_CREDENTIAL=2` signs as trader B | Step 3 |

## Glossary

| Term | Meaning |
|---|---|
| Maker / taker | The resting order / the incoming order that trades against it |
| Tick size | Smallest price increment of a market |
| Lot size | Smallest quantity increment of a market |
| GTC / IOC / FOK | Good til cancelled / immediate or cancel / fill or kill |
| Post-only | A limit order that must add liquidity; rejected if it would trade on arrival |
| STP | Self-trade prevention |
| Sequence | Per-market counter on every event, with no gaps |
| Outbox | Bounded queue between the engines and the event sink |
| Read model | The order repository, a projection of the event stream used for queries |
| Load shedding | Refusing work early with a 503 when a queue is full |

## Where to look

| Task | Location |
|---|---|
| Change a matching rule | `crates/domain/src/engine.rs`, with a property test in mind |
| List a new instrument | Add the asset (if new) and the market to `config/instruments.json`, then restart; see ADR 0004 |
| Add a market parameter | `crates/domain/src/market.rs` and `crates/server/src/instruments.rs` |
| Add a scenario flow | Postman app in Local View, which writes the new `.request.yaml` files; then `make flows-lint` and `make flows` |
| Add an endpoint | `crates/api/src/router.rs`, a handler, DTOs, then `docs/openapi.yaml` |
| Add an event sink | Implement `EventPublisher` in the infrastructure crate and select it in `crates/server/src/app.rs` |
| Replace in-memory storage | Implement `OrderRepository` and `IdempotencyStore` and wire them in `app.rs`; nothing else changes |
| Tune capacity | Env variables listed in `.env.example` |
| Add a metric, alert or panel | Metric in `crates/infrastructure/src/metrics.rs` or `crates/api/src/http_metrics.rs`; alert and its test in `deploy/prometheus/`; panel in `deploy/grafana/dashboards/orderflow.json`; then `make monitoring-check` |
| Understand a past decision | `docs/adr/` |

## Onboarding checklist

- [ ] Step 1: toolchain installed, repository cloned
- [ ] Step 2: `make ci` passes
- [ ] Step 3: first order placed, errors read
- [ ] Step 4: every scenario flow passes
- [ ] Step 5: ADR 0001 read, `Clock` exercise done
- [ ] Step 6: one order traced through all 13 rows
- [ ] Step 7: engine test written
- [ ] Step 8: events consumed with `make consume`, partitions checked with `make topics`
- [ ] Step 9: security model explained to your onboarding buddy
- [ ] Step 10: validation and error layers understood
- [ ] Step 11: dashboard explored, an alert watched firing, runbook reviewed
- [ ] Step 12: minimum notional change merged
- [ ] Step 13: conventions understood
