# @orderflow/prices

Typed client for the Orderflow price feed, and the `orderflow-prices` CLI.

- `PricesClient`: the REST API (`/v1/prices`, `/v1/prices/{market}`,
  candles, sources). Error responses become `PricefeedError` with the
  server's RFC 9457 problem attached (`code`, `status`, `retryable`).
- `PriceStream`: live prices over WebSocket. It reconnects with jittered
  backoff, subscribes again, drops anything not newer than what it has by
  `(epoch, sequence)`, detects dead connections with pings, and can be read
  with `for await`, which conflates per market like the server does.

Prices are decimal strings. Keep them as strings for anything that is
compared or stored; `Number()` is for display only.

## Use

```bash
npm ci
npm run build
```

```ts
import WebSocket from 'ws';
import { PricefeedError, PriceStream, PricesClient, streamUrl } from '@orderflow/prices';

const client = new PricesClient({ baseUrl: 'http://127.0.0.1:8090', timeoutMs: 5_000 });
try {
  const eth = await client.price('ETH-USDC');
  console.log(eth.price, eth.status, eth.components.map((c) => `${c.source}:${c.state}`));
} catch (error) {
  if (error instanceof PricefeedError && error.retryable) {
    // 503: no price published yet, or history temporarily unavailable.
  }
}

const stream = new PriceStream({
  url: streamUrl('http://127.0.0.1:8090'),
  markets: ['BTC-USD', 'SOL-USDC'],
  // Browsers and Node 22+ have a global WebSocket; Node 20 uses `ws`.
  webSocket: (url) => new WebSocket(url),
}).start();
stream.on('state', (state) => console.log('connection', state));
stream.on('error', (error) => console.warn(error.code, error.detail));
for await (const price of stream) {
  console.log(price.market, price.price, price.status);
}
```

## CLI

```bash
node dist/cli.js prices
node dist/cli.js price SOL-USDC
node dist/cli.js candles BTC-USD --interval 5m --limit 12
node dist/cli.js sources
node dist/cli.js watch BTC-USD ETH-USDC
```

`--url` or `PRICEFEED_URL` selects the service (default
`http://127.0.0.1:8090`). From the repository root, `make prices` and
`make prices MARKETS="BTC-USD ETH-USD"` do the same.

## Test

```bash
npm test
```

Builds with TypeScript 7 and runs the tests with `node --test` against a
local HTTP server and a local WebSocket server; no price feed needed.
