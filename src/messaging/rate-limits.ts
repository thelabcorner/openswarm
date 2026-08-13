/**
 * In-memory rate counters (t-flood-rate).
 *
 * Rolling-window counters keyed by (scope, window). All state is in-memory
 * per process: counters reset on restart, which is acceptable for flood
 * protection — the goal is damping a live burst (a member flooded with 17
 * inbox deliveries in 60s), not durable accounting.
 *
 * Every counter here is policy-overridable via SwarmPolicies (see
 * src/core/types.ts); the defaults below are the documented baselines.
 */

export interface RateLimiterOptions {
  /** Clock for deterministic tests (defaults to Date.now). */
  now?: () => number;
}

/** Default non-urgent mailbox-prompt budget per member per 60s (broker). */
export const DEFAULT_MAX_INBOX_PER_MIN = 5;
/** Default per-sender soft send quota per 60s (broadcast = 1 send). */
export const DEFAULT_SEND_QUOTA_PER_MIN = 30;
/** Default mention fan-out cap per message (extra mentions ignored). */
export const DEFAULT_MENTION_FAN_OUT_CAP = 10;
/** Default cross-swarm force-message quota per sender per 60s. */
export const DEFAULT_FORCE_QUOTA_PER_MIN = 10;
/** Default hive_need caps per member per 5 min (1 shout + 3 whispers). */
export const DEFAULT_NEED_SHOUT_PER_WINDOW = 1;
export const DEFAULT_NEED_WHISPER_PER_WINDOW = 3;
export const DEFAULT_NEED_RATE_WINDOW_MS = 5 * 60_000;
/** Default minimum gap between digest-health flip notices (5 min). */
export const DEFAULT_DIGEST_FLIP_NOTICE_MIN_MS = 5 * 60_000;

/**
 * A rolling-window counter. `hit` records one event and returns the count of
 * events in the trailing `windowMs` (including this one); `count`/`over`
 * inspect without recording. Timestamps are pruned lazily so a bucket never
 * grows unboundedly.
 */
export class RateLimiter {
  private readonly buckets = new Map<string, number[]>();
  private readonly now: () => number;

  constructor(options: RateLimiterOptions = {}) {
    this.now = options.now ?? Date.now;
  }

  /** Record one hit for `key` and return the count within the trailing window. */
  hit(key: string, windowMs: number): number {
    const t = this.now();
    const hits = (this.buckets.get(key) ?? []).filter((h) => t - h < windowMs);
    hits.push(t);
    this.buckets.set(key, hits);
    return hits.length;
  }

  /** Count hits for `key` within the trailing window WITHOUT recording. */
  count(key: string, windowMs: number): number {
    const t = this.now();
    const hits = (this.buckets.get(key) ?? []).filter((h) => t - h < windowMs);
    this.buckets.set(key, hits);
    return hits.length;
  }

  /** True when `key` already has >= `limit` hits in the trailing window. */
  over(key: string, limit: number, windowMs: number): boolean {
    return this.count(key, windowMs) >= limit;
  }

  /** Drop one key's history (or everything when no key is given). */
  reset(key?: string): void {
    if (key !== undefined) this.buckets.delete(key);
    else this.buckets.clear();
  }
}
