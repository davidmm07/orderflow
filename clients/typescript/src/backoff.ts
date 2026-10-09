/**
 * Exponential backoff with equal jitter, like the server's sources use: each
 * delay is half the current ceiling plus a random share of the other half,
 * so thousands of clients reconnecting after a deploy spread out instead of
 * arriving at once, and none of them retries immediately.
 */
export class Backoff {
  private attempt = 0;

  constructor(
    private readonly baseMs = 250,
    private readonly maxMs = 15_000,
    private readonly random: () => number = Math.random,
  ) {}

  next(): number {
    const ceiling = Math.min(this.maxMs, this.baseMs * 2 ** Math.min(this.attempt, 30));
    this.attempt += 1;
    const half = ceiling / 2;
    return Math.round(half + this.random() * half);
  }

  reset(): void {
    this.attempt = 0;
  }
}
