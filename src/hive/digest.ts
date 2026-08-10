import type { Belief } from "../core/types.js";

/**
 * Hive H2 anti-entropy digest (features/hive-mind-execution-layer item 1).
 * Pure, deterministic digest computation over a swarm's active beliefs:
 * a cheap hash of each belief's identity + version, so two members with the
 * same belief set derive the same digest and can detect drift (missed gossip /
 * stale local view) without exchanging payloads.
 *
 * Local bookkeeping only this wave: the plugin sweep computes the digest,
 * compares against the last-stored one (bounded blackboard key
 * context/digest/<swarmId>), and surfaces `digestChanged`/`digestStale` health.
 * Cross-member exchange is documented as future work (H2).
 */

/** Cheap FNV-1a 32-bit hash (deterministic, good enough for drift detection). */
export function fnv1a(input: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
}

/**
 * Compute the anti-entropy digest for a belief set. Deterministic regardless
 * of order: sorts contributions by belief id. Each contribution covers
 * identity + version fields that change when the belief is edited,
 * reinforced, or re-tiered (factHash, confidence, tier, reinforceCount,
 * status, updatedAt). Returns the hex digest.
 */
export function computeBeliefDigest(beliefs: Array<Pick<Belief, "id" | "factHash" | "confidence" | "tier" | "reinforceCount" | "status" | "updatedAt">>): string {
  const parts = [...beliefs]
    .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
    .map((b) => `${b.id}|${b.factHash}|${b.confidence}|${b.tier}|${b.reinforceCount}|${b.status}|${b.updatedAt}`);
  return fnv1a(parts.join("\n"));
}

export interface DigestHealth {
  /** Current computed digest (hex). */
  digest: string;
  /** True when the current digest differs from the stored last digest. */
  digestChanged: boolean;
  /** True when we have no stored digest yet (first computation). */
  digestStale: boolean;
  /** Epoch-ms of the last stored digest computation, or undefined if none. */
  lastComputedAt?: number;
}

/**
 * Compare the freshly computed digest against the stored one and produce the
 * health verdict. `storedDigest` is the previously persisted value (undefined
 * when never computed — e.g. first sweep, or the key was evicted).
 */
export function evaluateDigestHealth(
  currentDigest: string,
  storedDigest: string | undefined,
  storedAt: number | undefined,
): DigestHealth {
  return {
    digest: currentDigest,
    digestChanged: storedDigest !== undefined && storedDigest !== currentDigest,
    digestStale: storedDigest === undefined,
    lastComputedAt: storedAt,
  };
}
