#!/usr/bin/env node
/**
 * orderflow-prices: the price feed from a terminal.
 *
 *   orderflow-prices prices [MARKET...]
 *   orderflow-prices price MARKET
 *   orderflow-prices candles MARKET [--interval 1m] [--limit 20]
 *   orderflow-prices sources
 *   orderflow-prices watch MARKET...
 *
 * The service URL comes from --url or PRICEFEED_URL, default http://127.0.0.1:8090.
 */
import { WebSocket } from 'ws';

import { PricesClient } from './client.js';
import { PricefeedError } from './errors.js';
import { PriceStream, streamUrl } from './stream.js';
import type { CandleInterval, Price } from './types.js';

const USAGE = `usage: orderflow-prices [--url URL] <command>

commands:
  prices [MARKET...]                          index prices
  price MARKET                                one price and the sources behind it
  candles MARKET [--interval 1m] [--limit 20] OHLC candles
  sources                                     how each source feeds the index
  watch MARKET...                             stream live prices (Ctrl+C to stop)
`;

interface Parsed {
  url: string;
  command: string | undefined;
  args: string[];
  flags: Map<string, string>;
}

function parse(argv: string[]): Parsed {
  const flags = new Map<string, string>();
  const args: string[] = [];
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i] ?? '';
    if (arg.startsWith('--')) {
      flags.set(arg.slice(2), argv[i + 1] ?? '');
      i += 1;
    } else {
      args.push(arg);
    }
  }
  const url = flags.get('url') ?? process.env.PRICEFEED_URL ?? 'http://127.0.0.1:8090';
  return { url, command: args[0], args: args.slice(1), flags };
}

function table(rows: string[][]): string {
  const widths = rows[0]?.map((_, column) => Math.max(...rows.map((row) => (row[column] ?? '').length))) ?? [];
  return rows.map((row) => row.map((cell, column) => cell.padEnd(widths[column] ?? 0)).join('  ').trimEnd()).join('\n');
}

const STATUS_COLOR: Record<string, string> = { live: '\x1b[32m', degraded: '\x1b[33m', stale: '\x1b[31m' };

function status(value: string, color: boolean): string {
  return color ? `${STATUS_COLOR[value] ?? ''}${value}\x1b[0m` : value;
}

function priceRows(prices: Price[], color: boolean): string[][] {
  return [
    ['MARKET', 'PRICE', 'STATUS', 'SOURCES', 'SPREAD BPS', 'AS OF'],
    ...prices.map((p) => [p.market, p.price, status(p.status, color), String(p.sources_used), p.spread_bps, p.as_of]),
  ];
}

async function watch(baseUrl: string, markets: string[]): Promise<void> {
  if (markets.length === 0) throw new Error('watch needs at least one market, such as BTC-USD');
  const tty = process.stdout.isTTY === true;
  const stream = new PriceStream({
    url: streamUrl(baseUrl),
    markets,
    webSocket: (url) => new WebSocket(url),
  });
  stream.on('state', (state) => process.stderr.write(`connection: ${state}\n`));
  stream.on('error', (error) => process.stderr.write(`server: ${error.code} ${error.detail} ${error.markets?.join(',') ?? ''}\n`));
  stream.on('price', (price) => {
    if (tty) {
      const latest = markets.map((market) => stream.latest(market)).filter((p): p is Price => p !== undefined);
      process.stdout.write(`\x1b[2J\x1b[H${table(priceRows(latest, true))}\n`);
    } else {
      process.stdout.write(`${price.published_at} ${price.market} ${price.price} ${price.status} seq=${price.sequence}\n`);
    }
  });
  process.once('SIGINT', () => {
    stream.close();
    process.exit(0);
  });
  stream.start();
  await new Promise<never>(() => {});
}

async function main(): Promise<void> {
  const { url, command, args, flags } = parse(process.argv.slice(2));
  const client = new PricesClient(url);
  const color = process.stdout.isTTY === true;
  switch (command) {
    case 'prices':
      console.log(table(priceRows(await client.prices(args), color)));
      return;
    case 'price': {
      const market = args[0];
      if (!market) throw new Error('price needs a market, such as BTC-USD');
      const detail = await client.price(market);
      console.log(table(priceRows([detail], color)));
      console.log();
      console.log(
        table([
          ['SOURCE', 'KIND', 'PRICE', 'STATE', 'DEVIATION BPS', 'OBSERVED AT'],
          ...detail.components.map((c) => [c.source, c.kind, c.price, c.state, c.deviation_bps ?? '', c.observed_at]),
        ]),
      );
      return;
    }
    case 'candles': {
      const market = args[0];
      if (!market) throw new Error('candles needs a market, such as BTC-USD');
      const response = await client.candles(market, {
        interval: (flags.get('interval') ?? '1m') as CandleInterval,
        limit: Number(flags.get('limit') ?? '20'),
      });
      console.log(
        table([
          ['START', 'OPEN', 'HIGH', 'LOW', 'CLOSE'],
          ...response.candles.map((c) => [c.start, c.open, c.high, c.low, c.close]),
        ]),
      );
      return;
    }
    case 'sources':
      console.log(
        table([
          ['SOURCE', 'KIND', 'USED IN', 'OUTLIER IN', 'STALE IN', 'LAST OBSERVED'],
          ...(await client.sources()).map((s) => [
            s.source,
            s.kind,
            String(s.used_in),
            String(s.outlier_in),
            String(s.stale_in),
            s.last_observed_at,
          ]),
        ]),
      );
      return;
    case 'watch':
      await watch(url, args);
      return;
    default:
      process.stdout.write(USAGE);
      process.exitCode = command ? 2 : 0;
  }
}

main().catch((error: unknown) => {
  if (error instanceof PricefeedError) {
    console.error(`${error.status} ${error.code}: ${error.problem.detail}`);
    for (const field of error.problem.errors ?? []) console.error(`  ${field.field}: ${field.message}`);
  } else {
    console.error(error instanceof Error ? error.message : String(error));
  }
  process.exitCode = 1;
});
