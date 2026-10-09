/**
 * Wire types of the price feed API. They mirror the server's JSON exactly.
 *
 * Prices and other decimals are strings. JavaScript numbers are binary
 * floats and cannot hold most decimal prices exactly, so keep the strings
 * for anything that is compared or stored, and convert with `Number()`
 * only for display.
 */

export type PriceStatus = 'live' | 'degraded' | 'stale';
export type QuoteState = 'used' | 'outlier' | 'stale' | 'monitor';
export type SourceKind = 'aggregator' | 'cex' | 'dex' | 'venue';
export type CandleInterval = '1m' | '5m' | '15m' | '1h' | '4h' | '1d';

/** Index price of one market. */
export interface Price {
  market: string;
  price: string;
  /** `live`: quorum met. `degraded`: displayable, not actionable. `stale`: last known price. */
  status: PriceStatus;
  /** Time of the newest quote behind the price (RFC 3339). */
  as_of: string;
  published_at: string;
  /** Run of the publisher. Updates order by `(epoch, sequence)`. */
  epoch: number;
  sequence: number;
  sources_used: number;
  spread_bps: string;
}

/** One source's quote and how the index treated it. */
export interface Component {
  source: string;
  kind: SourceKind;
  price: string;
  observed_at: string;
  state: QuoteState;
  deviation_bps?: string;
}

export interface PriceDetail extends Price {
  components: Component[];
}

export interface Candle {
  start: string;
  open: string;
  high: string;
  low: string;
  close: string;
}

export interface CandlesResponse {
  market: string;
  interval: CandleInterval;
  candles: Candle[];
}

export interface SourceSummary {
  source: string;
  kind: SourceKind;
  markets: string[];
  used_in: number;
  outlier_in: number;
  stale_in: number;
  last_observed_at: string;
}

export interface FieldError {
  field: string;
  code: string;
  message: string;
}

/** RFC 9457 problem details, the body of every error response. */
export interface Problem {
  type: string;
  title: string;
  status: number;
  detail: string;
  code: string;
  errors?: FieldError[];
}

/** Messages the server sends over WebSocket. */
export type ServerMessage =
  | { type: 'welcome'; markets: string[]; max_subscriptions: number; ping_interval_secs: number }
  | { type: 'subscribed'; markets: string[] }
  | ({ type: 'price' } & Price)
  | { type: 'error'; code: string; detail: string; markets?: string[] }
  | { type: 'pong' };

/** Messages a client sends over WebSocket. */
export type ClientMessage =
  | { op: 'subscribe' | 'unsubscribe'; markets: string[] }
  | { op: 'ping' };

/** Whether `next` comes after `current` in the price stream. */
export function isNewer(next: Pick<Price, 'epoch' | 'sequence'>, current: Pick<Price, 'epoch' | 'sequence'>): boolean {
  return next.epoch > current.epoch || (next.epoch === current.epoch && next.sequence > current.sequence);
}
