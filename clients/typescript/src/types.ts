/**
 * Wire types of the price feed API.
 *
 * Every type here is an alias of a schema in `generated/pricefeed.ts`, which
 * is generated from docs/pricefeed-openapi.yaml by `npm run generate`. To
 * change a type, change the contract and regenerate. CI regenerates the
 * file and fails if it differs from the committed one.
 *
 * Prices and other decimals are strings. JavaScript numbers are binary
 * floats and cannot hold most decimal prices exactly, so keep the strings
 * for anything that is compared or stored, and convert with `Number()`
 * only for display.
 */
import type { components } from './generated/pricefeed.js';

type Schemas = components['schemas'];

/** `live`: quorum met. `degraded`: displayable, not actionable. `stale`: last known price. */
export type PriceStatus = Schemas['PriceStatus'];
export type QuoteState = Schemas['QuoteState'];
export type SourceKind = Schemas['SourceKind'];
export type CandleInterval = Schemas['CandleInterval'];

/** Index price of one market. Updates order by `(epoch, sequence)`. */
export type Price = Schemas['Price'];
/** One source's quote and how the index treated it. */
export type Component = Schemas['Component'];
export type PriceDetail = Schemas['PriceDetail'];
export type Candle = Schemas['Candle'];
export type SourceSummary = Schemas['SourceSummary'];

export type PricesResponse = Schemas['PricesResponse'];
export type CandlesResponse = Schemas['CandlesResponse'];
export type SourcesResponse = Schemas['SourcesResponse'];

export type FieldError = Schemas['FieldError'];
/** RFC 9457 problem details, the body of every error response. */
export type Problem = Schemas['Problem'];

/** Messages the server sends over WebSocket, told apart by `type`. */
export type ServerMessage = Schemas['ServerMessage'];
/** Messages a client sends over WebSocket, told apart by `op`. */
export type ClientMessage = Schemas['ClientMessage'];

/** Whether `next` comes after `current` in the price stream. */
export function isNewer(next: Pick<Price, 'epoch' | 'sequence'>, current: Pick<Price, 'epoch' | 'sequence'>): boolean {
  return next.epoch > current.epoch || (next.epoch === current.epoch && next.sequence > current.sequence);
}
