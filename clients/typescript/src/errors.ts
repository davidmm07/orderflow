import type { Problem } from './types.js';

/** An error response from the price feed, with its problem details. */
export class PricefeedError extends Error {
  readonly problem: Problem;

  constructor(problem: Problem) {
    super(`${problem.status} ${problem.code}: ${problem.detail}`);
    this.name = 'PricefeedError';
    this.problem = problem;
  }

  get status(): number {
    return this.problem.status;
  }

  get code(): string {
    return this.problem.code;
  }

  /** 503 responses are temporary: the price is not published yet, or a store is down. */
  get retryable(): boolean {
    return this.problem.status === 503;
  }
}
