import { Backoff } from './backoff.js';
import { type ClientMessage, type Price, type ServerMessage, isNewer } from './types.js';

/** The subset of the WebSocket API the stream uses: browsers, Node 22+ and `ws` all provide it. */
export interface WebSocketLike {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: 'open' | 'message' | 'close' | 'error', listener: (event: any) => void): void;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

export type ConnectionState = 'connecting' | 'open' | 'reconnecting' | 'closed';

export interface StreamOptions {
  /** Such as `ws://127.0.0.1:8090/v1/ws`. */
  url: string;
  markets?: string[];
  /** Defaults to the global `WebSocket`; pass `(url) => new WebSocket(url)` from `ws` on Node 20. */
  webSocket?: WebSocketFactory;
  /** How often to send an application ping. Defaults to 15 seconds. */
  heartbeatMs?: number;
  /** Silence after which the connection is considered dead. Defaults to 45 seconds. */
  staleAfterMs?: number;
  backoff?: Backoff;
}

export interface StreamError {
  code: string;
  detail: string;
  markets?: string[];
}

interface Listeners {
  price: Set<(price: Price) => void>;
  state: Set<(state: ConnectionState) => void>;
  error: Set<(error: StreamError) => void>;
}

const OPEN = 1;
/** Close code a server sends while shutting down: reconnect at once. */
const GOING_AWAY = 1001;

/**
 * Live prices over WebSocket, with reconnection.
 *
 * - Reconnects with jittered backoff and subscribes again to every market.
 * - Delivers each market's prices in order: anything not newer than the
 *   last price seen, by `(epoch, sequence)`, is dropped. That makes the
 *   snapshot a reconnect replays harmless.
 * - Detects dead connections with application pings.
 * - `for await (const price of stream)` reads prices with conflation: a
 *   slow consumer gets each market's newest price, never a growing backlog.
 */
export class PriceStream implements AsyncIterable<Price> {
  private readonly options: Required<Omit<StreamOptions, 'markets' | 'backoff'>>;
  private readonly backoff: Backoff;
  private readonly markets = new Set<string>();
  private readonly latestPrices = new Map<string, Price>();
  private readonly listeners: Listeners = { price: new Set(), state: new Set(), error: new Set() };
  private socket: WebSocketLike | undefined;
  private heartbeat: ReturnType<typeof setInterval> | undefined;
  private retry: ReturnType<typeof setTimeout> | undefined;
  private lastSeen = 0;
  private stopped = false;
  private currentState: ConnectionState = 'closed';

  constructor(options: StreamOptions) {
    const factory = options.webSocket ?? defaultWebSocket();
    this.options = {
      url: options.url,
      webSocket: factory,
      heartbeatMs: options.heartbeatMs ?? 15_000,
      staleAfterMs: options.staleAfterMs ?? 45_000,
    };
    this.backoff = options.backoff ?? new Backoff();
    for (const market of options.markets ?? []) this.markets.add(market);
  }

  get state(): ConnectionState {
    return this.currentState;
  }

  /** Last price received for `market`. */
  latest(market: string): Price | undefined {
    return this.latestPrices.get(market);
  }

  on(event: 'price', listener: (price: Price) => void): () => void;
  on(event: 'state', listener: (state: ConnectionState) => void): () => void;
  on(event: 'error', listener: (error: StreamError) => void): () => void;
  on(event: keyof Listeners, listener: (value: any) => void): () => void {
    const set = this.listeners[event] as Set<(value: unknown) => void>;
    set.add(listener);
    return () => set.delete(listener);
  }

  start(): this {
    this.stopped = false;
    this.connect();
    return this;
  }

  subscribe(markets: string[]): void {
    const added = markets.filter((market) => !this.markets.has(market));
    for (const market of added) this.markets.add(market);
    if (added.length > 0) this.send({ op: 'subscribe', markets: added });
  }

  unsubscribe(markets: string[]): void {
    const removed = markets.filter((market) => this.markets.delete(market));
    for (const market of removed) this.latestPrices.delete(market);
    if (removed.length > 0) this.send({ op: 'unsubscribe', markets: removed });
  }

  /** Closes the connection for good. */
  close(): void {
    this.stopped = true;
    clearTimeout(this.retry);
    this.stopHeartbeat();
    this.socket?.close(1000, 'client closing');
    this.socket = undefined;
    this.setState('closed');
  }

  [Symbol.asyncIterator](): AsyncIterator<Price> {
    const pending = new Map<string, Price>();
    let wake: (() => void) | undefined;
    const unsubscribe = this.on('price', (price) => {
      pending.set(price.market, price);
      wake?.();
    });
    const stopState = this.on('state', (state) => {
      if (state === 'closed') wake?.();
    });
    return {
      next: async (): Promise<IteratorResult<Price>> => {
        for (;;) {
          const first = pending.entries().next();
          if (!first.done) {
            const [market, price] = first.value;
            pending.delete(market);
            return { value: price, done: false };
          }
          if (this.stopped) {
            unsubscribe();
            stopState();
            return { value: undefined, done: true };
          }
          await new Promise<void>((resolve) => {
            wake = resolve;
          });
          wake = undefined;
        }
      },
      return: async (): Promise<IteratorResult<Price>> => {
        unsubscribe();
        stopState();
        return { value: undefined, done: true };
      },
    };
  }

  private connect(): void {
    if (this.stopped) return;
    this.setState(this.currentState === 'closed' ? 'connecting' : 'reconnecting');
    let socket: WebSocketLike;
    try {
      socket = this.options.webSocket(this.options.url);
    } catch (error) {
      this.emitError({ code: 'connect_failed', detail: String(error) });
      this.scheduleReconnect();
      return;
    }
    this.socket = socket;
    socket.addEventListener('open', () => {
      this.lastSeen = Date.now();
      this.setState('open');
      if (this.markets.size > 0) this.send({ op: 'subscribe', markets: [...this.markets] });
      this.startHeartbeat();
    });
    socket.addEventListener('message', (event: { data: unknown }) => {
      this.lastSeen = Date.now();
      this.handle(String(event.data));
    });
    socket.addEventListener('close', (event: { code?: number }) => {
      if (this.socket !== socket) return;
      this.stopHeartbeat();
      this.socket = undefined;
      if (this.stopped) return;
      this.scheduleReconnect(event.code === GOING_AWAY ? 0 : undefined);
    });
    socket.addEventListener('error', () => {
      // A close event always follows; reconnection happens there.
    });
  }

  private handle(text: string): void {
    let message: ServerMessage;
    try {
      message = JSON.parse(text) as ServerMessage;
    } catch {
      this.emitError({ code: 'invalid_message', detail: 'the server sent a frame that is not JSON' });
      return;
    }
    switch (message.type) {
      case 'price': {
        const { type: _type, ...price } = message;
        const current = this.latestPrices.get(price.market);
        if (!this.markets.has(price.market) || (current && !isNewer(price, current))) return;
        // A healthy stream: the next disconnect starts its backoff from scratch.
        this.backoff.reset();
        this.latestPrices.set(price.market, price);
        for (const listener of this.listeners.price) listener(price);
        return;
      }
      case 'error':
        this.emitError({
          code: message.code,
          detail: message.detail,
          ...(message.markets ? { markets: message.markets } : {}),
        });
        return;
      case 'welcome':
      case 'subscribed':
      case 'pong':
        return;
    }
  }

  private send(message: ClientMessage): void {
    if (this.socket?.readyState === OPEN) this.socket.send(JSON.stringify(message));
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeat = setInterval(() => {
      if (Date.now() - this.lastSeen > this.options.staleAfterMs) {
        // Half-open connection: close it and let the close handler reconnect.
        this.socket?.close(4000, 'heartbeat timeout');
        return;
      }
      this.send({ op: 'ping' });
    }, this.options.heartbeatMs);
  }

  private stopHeartbeat(): void {
    clearInterval(this.heartbeat);
    this.heartbeat = undefined;
  }

  private scheduleReconnect(delayMs?: number): void {
    if (this.stopped) return;
    this.setState('reconnecting');
    clearTimeout(this.retry);
    this.retry = setTimeout(() => this.connect(), delayMs ?? this.backoff.next());
  }

  private setState(state: ConnectionState): void {
    if (this.currentState === state) return;
    this.currentState = state;
    for (const listener of this.listeners.state) listener(state);
  }

  private emitError(error: StreamError): void {
    for (const listener of this.listeners.error) listener(error);
  }
}

function defaultWebSocket(): WebSocketFactory {
  const Native = (globalThis as { WebSocket?: new (url: string) => WebSocketLike }).WebSocket;
  if (!Native) {
    return () => {
      throw new Error('no global WebSocket: pass options.webSocket, for example from the ws package');
    };
  }
  return (url) => new Native(url);
}

/** WebSocket URL of the price feed at `baseUrl` (`http` becomes `ws`, `https` becomes `wss`). */
export function streamUrl(baseUrl: string): string {
  const url = new URL('/v1/ws', baseUrl);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.toString();
}
