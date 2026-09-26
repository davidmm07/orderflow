# Error catalog

Every failure is an [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457)
problem document served as `application/problem+json`:

```json
{
  "type": "urn:orderflow:problem:validation_failed",
  "title": "Request validation failed",
  "status": 422,
  "detail": "One or more fields are invalid. See `errors` for details.",
  "code": "validation_failed",
  "errors": [
    { "field": "price", "code": "price_not_on_tick", "message": "price 100.001 is not a multiple of the tick size 0.01" }
  ]
}
```

Clients should branch on `code` (stable) and never on `title` or `detail`
(human text that may change). Every response also carries `x-request-id`;
quote it when reporting a problem, because every log line of the request
contains it.

For 5xx responses, `detail` is generic. The real cause is only in the
server log, next to the request id.

## Top level codes

| Status | `code` | When | What the client should do |
|---|---|---|---|
| 400 | `malformed_json` | Body is not valid JSON | Fix the payload |
| 400 | `unreadable_body` | Body ended early or could not be read | Resend |
| 400 | `invalid_path` | A path segment could not be decoded | Fix the URL |
| 401 | `unauthenticated` | Missing, expired or wrong signature, or unknown key | Check key, secret, clock and signing string |
| 404 | `unknown_market` | Market is not listed, or its id is malformed | Use `GET /v1/markets` |
| 404 | `order_not_found` | No such order for this account and market | Do not retry |
| 404 | `route_not_found` | No route matches the path | Fix the URL |
| 405 | `method_not_allowed` | Route exists, method does not | Fix the method |
| 409 | `order_not_open` | Cancel on an order that already filled or was cancelled | Read the order to see its final state |
| 409 | `post_only_would_cross` | A post-only order would have traded on arrival | Reprice or drop `post_only` |
| 409 | `stop_would_trigger_immediately` | The last trade price has already reached the stop price | Read `last_price` from the book and choose a stop beyond it, or send a normal order |
| 409 | `idempotency_key_in_flight` | Same key is still being processed | Retry after `Retry-After` |
| 413 | `payload_too_large` | Body exceeds `ORDERFLOW_MAX_BODY_BYTES` | Send a smaller body |
| 415 | `unsupported_media_type` | Missing `content-type: application/json` | Set the header |
| 422 | `validation_failed` | One or more fields are invalid; see `errors` | Fix every listed field |
| 422 | `invalid_body` | Unknown field, or a field has the wrong JSON type (for example a number instead of a decimal string) | Follow the schema |
| 422 | `invalid_query` | Query string does not match the schema | Fix the query |
| 422 | `idempotency_key_reused` | Key was used before with a different body | Use a new key per distinct request |
| 429 | `rate_limited` | Account exceeded its token bucket | Wait `Retry-After` seconds |
| 500 | `internal_error` | A bug or a broken invariant | Report with `x-request-id` |
| 503 | `market_overloaded` | The market's engine queue is full (load shedding) | Retry after `Retry-After` with the same `Idempotency-Key` |
| 503 | `request_timeout` | The request exceeded `ORDERFLOW_REQUEST_TIMEOUT_MS` | Retry with the same `Idempotency-Key` |
| 503 | `service_unavailable` | Engine or outbox is shutting down | Retry after `Retry-After` |
| 503 | `storage_unavailable` | A storage adapter failed | Retry after `Retry-After` |

## Field level codes (inside `errors`)

| `field` | `code` | Meaning |
|---|---|---|
| any | `required` | The field is missing |
| any | `not_allowed` | The field is not valid for this order type, for example `price` on a market order |
| `side`, `type`, `time_in_force`, `self_trade_prevention` | `invalid_enum` | Value is not one of the listed options |
| `price`, `stop_price`, `quantity` | `invalid_decimal` | Not a plain decimal string such as `"101.25"` (signs, exponents and spaces are rejected) |
| `price`, `stop_price` | `non_positive_price` | Zero |
| `price`, `stop_price` | `price_not_on_tick` | Not a multiple of the market's `tick_size` |
| `quantity` | `non_positive_quantity` | Zero |
| `quantity` | `quantity_not_on_lot` | Not a multiple of the market's `lot_size` |
| `quantity` | `quantity_below_minimum` | Below `min_quantity` |
| `quantity` | `quantity_above_maximum` | Above `max_quantity` |
| `post_only` | `post_only_requires_gtc` | `post_only` combined with `ioc` or `fok` |
| `post_only` | `not_allowed` | `post_only` on a stop order |
| `stop_price` | `required` / `not_allowed` | Missing on a stop order, or present on a limit or market order |
| `client_order_id` | `invalid_client_order_id` | Not 1 to 36 characters of letters, digits, `-` or `_` |
| `Idempotency-Key` | `invalid_idempotency_key` | Not 1 to 64 visible ASCII characters |
| `depth` | `out_of_range` | Book depth outside 1 to 100 |
| `base`, `quote` | `invalid_asset_code` | Market filter that is not 2 to 10 uppercase letters or digits |

## Adding a new error

1. Add the variant to `DomainError` or `ApplicationError` and give it a
   `code()`.
2. Map it in `crates/api/src/error.rs`. The compiler flags the missing arm,
   because both `match` statements are exhaustive.
3. Add a row to this file and, if clients can see it, to
   `docs/openapi.yaml`.
4. Cover it with a test in `crates/api/tests/http.rs` using
   `assert_problem`.
