import { PricefeedError } from './errors.js';
import type { CandleInterval, CandlesResponse, Price, PriceDetail, Problem, SourceSummary } from './types.js';

export interface ClientOptions {
  /** Such as `http://127.0.0.1:8090`. */
  baseUrl: string;
  /** Per request timeout. Defaults to 10 seconds. */
  timeoutMs?: number;
  /** Injected for tests or custom agents; defaults to the global `fetch`. */
  fetch?: typeof fetch;
}

export interface CandleOptions {
  interval?: CandleInterval;
  /** Number of buckets, 1 to 1000. Defaults to 100. */
  limit?: number;
  /** Only candles that start before this time. */
  end?: Date | string;
}

/** REST client for the price feed. */
export class PricesClient {
  private readonly baseUrl: URL;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

  constructor(options: ClientOptions | string) {
    const settings = typeof options === 'string' ? { baseUrl: options } : options;
    this.baseUrl = new URL(settings.baseUrl);
    this.timeoutMs = settings.timeoutMs ?? 10_000;
    this.fetchImpl = settings.fetch ?? globalThis.fetch;
  }

  /** Index prices of every market, or of `markets` only. */
  async prices(markets?: string[]): Promise<Price[]> {
    const query = markets && markets.length > 0 ? { markets: markets.join(',') } : undefined;
    const body = await this.get<{ prices: Price[] }>('/v1/prices', query);
    return body.prices;
  }

  /** One market's index price with the per-source breakdown. */
  price(market: string): Promise<PriceDetail> {
    return this.get(`/v1/prices/${encodeURIComponent(market)}`);
  }

  candles(market: string, options: CandleOptions = {}): Promise<CandlesResponse> {
    const query: Record<string, string> = {};
    if (options.interval) query.interval = options.interval;
    if (options.limit !== undefined) query.limit = String(options.limit);
    if (options.end !== undefined) {
      query.end = options.end instanceof Date ? options.end.toISOString() : options.end;
    }
    return this.get(`/v1/prices/${encodeURIComponent(market)}/candles`, query);
  }

  async sources(): Promise<SourceSummary[]> {
    const body = await this.get<{ sources: SourceSummary[] }>('/v1/sources');
    return body.sources;
  }

  private async get<T>(path: string, query?: Record<string, string>): Promise<T> {
    const url = new URL(path, this.baseUrl);
    for (const [key, value] of Object.entries(query ?? {})) {
      url.searchParams.set(key, value);
    }
    const response = await this.fetchImpl(url, {
      headers: { accept: 'application/json' },
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (!response.ok) {
      throw new PricefeedError(await problemOf(response));
    }
    return (await response.json()) as T;
  }
}

/** Reads the problem body of an error response, or describes a non-problem one. */
async function problemOf(response: Response): Promise<Problem> {
  const text = await response.text();
  try {
    const body = JSON.parse(text) as Partial<Problem>;
    if (typeof body.code === 'string' && typeof body.detail === 'string') {
      return {
        type: body.type ?? 'about:blank',
        title: body.title ?? response.statusText,
        status: body.status ?? response.status,
        detail: body.detail,
        code: body.code,
        ...(body.errors ? { errors: body.errors } : {}),
      };
    }
  } catch {
    // Not JSON: a proxy or load balancer answered instead of the service.
  }
  return {
    type: 'about:blank',
    title: response.statusText,
    status: response.status,
    detail: text.slice(0, 200) || response.statusText,
    code: 'unexpected_response',
  };
}
