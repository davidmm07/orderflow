import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { after, before, describe, it } from 'node:test';

import { WebSocket, WebSocketServer } from 'ws';

import { Backoff, type Price, PriceStream } from '../index.js';

function price(market: string, sequence: number, epoch = 1): Price {
  return {
    market,
    price: String(100 + sequence),
    status: 'live',
    as_of: '2026-10-08T22:08:42.123Z',
    published_at: '2026-10-08T22:08:42.250Z',
    epoch,
    sequence,
    sources_used: 3,
    spread_bps: '1',
  };
}

function waitFor(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const started = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      if (condition()) return resolve();
      if (Date.now() - started > timeoutMs) return reject(new Error('timed out'));
      setTimeout(check, 10);
    };
    check();
  });
}

describe('PriceStream', () => {
  let server: WebSocketServer;
  let url = '';
  const subscriptions: string[][] = [];
  const sockets: WebSocket[] = [];

  before(async () => {
    server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
    await new Promise<void>((resolve) => server.once('listening', resolve));
    url = `ws://127.0.0.1:${(server.address() as AddressInfo).port}/v1/ws`;
    server.on('connection', (socket) => {
      sockets.push(socket);
      socket.send(JSON.stringify({ type: 'welcome', markets: ['BTC-USD'], max_subscriptions: 50, ping_interval_secs: 20 }));
      socket.on('message', (data) => {
        const message = JSON.parse(String(data)) as { op: string; markets?: string[] };
        if (message.op === 'subscribe') {
          subscriptions.push(message.markets ?? []);
          socket.send(JSON.stringify({ type: 'subscribed', markets: message.markets }));
          // The current price, as the real server sends on subscribe.
          socket.send(JSON.stringify({ type: 'price', ...price('BTC-USD', 1) }));
        }
        if (message.op === 'ping') socket.send(JSON.stringify({ type: 'pong' }));
      });
    });
  });
  after(() => server.close());

  it('delivers prices in order, reconnects and drops replays', async () => {
    const received: number[] = [];
    const stream = new PriceStream({
      url,
      markets: ['BTC-USD'],
      webSocket: (target) => new WebSocket(target),
      backoff: new Backoff(10, 20),
    });
    stream.on('price', (p) => received.push(p.sequence));
    stream.start();
    await waitFor(() => received.length === 1);

    const first = sockets.at(-1);
    first?.send(JSON.stringify({ type: 'price', ...price('BTC-USD', 3) }));
    first?.send(JSON.stringify({ type: 'price', ...price('BTC-USD', 2) }));
    await waitFor(() => received.length === 2);

    // The server goes away; the client reconnects and subscribes again.
    first?.terminate();
    await waitFor(() => subscriptions.length === 2);
    // The snapshot after reconnecting (sequence 1) is older than what the
    // client has, so it is dropped; a later epoch is accepted.
    sockets.at(-1)?.send(JSON.stringify({ type: 'price', ...price('BTC-USD', 1, 2) }));
    await waitFor(() => received.length === 3);
    stream.close();

    assert.deepEqual(received, [1, 3, 1]);
    assert.equal(stream.latest('BTC-USD')?.epoch, 2);
    assert.equal(stream.state, 'closed');
  });

  it('iterates prices and ends when closed', async () => {
    const stream = new PriceStream({
      url,
      markets: ['BTC-USD'],
      webSocket: (target) => new WebSocket(target),
    }).start();
    const seen: Price[] = [];
    for await (const p of stream) {
      seen.push(p);
      stream.close();
    }
    assert.equal(seen.length, 1);
    assert.equal(seen[0]?.market, 'BTC-USD');
  });
});

describe('Backoff', () => {
  it('waits at least half the ceiling and never more than the cap', () => {
    const low = new Backoff(100, 1_000, () => 0);
    const high = new Backoff(100, 1_000, () => 1);
    assert.deepEqual([low.next(), low.next(), low.next()], [50, 100, 200]);
    assert.deepEqual([high.next(), high.next(), high.next(), high.next(), high.next()], [100, 200, 400, 800, 1_000]);
    high.reset();
    assert.equal(high.next(), 100);
  });
});
