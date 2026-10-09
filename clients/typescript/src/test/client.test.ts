import assert from 'node:assert/strict';
import { type IncomingMessage, type ServerResponse, createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { after, before, describe, it } from 'node:test';

import { PricefeedError, PricesClient } from '../index.js';

const price = {
  market: 'BTC-USD',
  price: '81650.5',
  status: 'live',
  as_of: '2026-10-08T22:08:42.123Z',
  published_at: '2026-10-08T22:08:42.250Z',
  epoch: 1791497000000,
  sequence: 42,
  sources_used: 3,
  spread_bps: '7.39',
};

describe('PricesClient', () => {
  const requests: string[] = [];
  let baseUrl = '';
  const server = createServer((request: IncomingMessage, response: ServerResponse) => {
    requests.push(request.url ?? '');
    const url = new URL(request.url ?? '/', 'http://localhost');
    const json = (status: number, body: unknown, type = 'application/json') => {
      response.writeHead(status, { 'content-type': type });
      response.end(JSON.stringify(body));
    };
    if (url.pathname === '/v1/prices') return json(200, { prices: [price] });
    if (url.pathname === '/v1/prices/BTC-USD') return json(200, { ...price, components: [] });
    if (url.pathname === '/v1/prices/BTC-USD/candles') {
      return json(200, { market: 'BTC-USD', interval: url.searchParams.get('interval'), candles: [] });
    }
    if (url.pathname === '/v1/prices/XYZ-USD') {
      return json(
        404,
        { type: 'about:blank', title: 'Not found', status: 404, detail: 'XYZ-USD is not listed', code: 'market_not_found' },
        'application/problem+json',
      );
    }
    response.writeHead(502, { 'content-type': 'text/html' });
    response.end('<html>bad gateway</html>');
  });

  before(async () => {
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });
  after(() => server.close());

  it('reads prices and filters by market', async () => {
    const client = new PricesClient(baseUrl);
    const prices = await client.prices(['BTC-USD', 'ETH-USD']);
    assert.equal(prices[0]?.price, '81650.5');
    assert.ok(requests.includes('/v1/prices?markets=BTC-USD%2CETH-USD'));
  });

  it('builds candle queries', async () => {
    const client = new PricesClient({ baseUrl, timeoutMs: 2_000 });
    const candles = await client.candles('BTC-USD', {
      interval: '5m',
      limit: 10,
      end: new Date('2026-10-08T22:00:00Z'),
    });
    assert.equal(candles.interval, '5m');
    assert.ok(
      requests.includes('/v1/prices/BTC-USD/candles?interval=5m&limit=10&end=2026-10-08T22%3A00%3A00.000Z'),
      requests.join('\n'),
    );
  });

  it('turns problem responses into typed errors', async () => {
    const client = new PricesClient(baseUrl);
    await assert.rejects(client.price('XYZ-USD'), (error: unknown) => {
      assert.ok(error instanceof PricefeedError);
      assert.equal(error.status, 404);
      assert.equal(error.code, 'market_not_found');
      assert.equal(error.retryable, false);
      return true;
    });
  });

  it('describes responses that are not problems', async () => {
    const client = new PricesClient(baseUrl);
    await assert.rejects(client.sources(), (error: unknown) => {
      assert.ok(error instanceof PricefeedError);
      assert.equal(error.code, 'unexpected_response');
      assert.equal(error.status, 502);
      return true;
    });
  });
});
