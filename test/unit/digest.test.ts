import { describe, expect, test } from "bun:test";
import { computeBeliefDigest, evaluateDigestHealth, fnv1a, type DigestHealth } from "../../src/hive/digest.ts";
import type { Belief } from "../../src/core/types.ts";

/**
 * Wave 5 Hive H2 anti-entropy digest tests:
 *  - computeBeliefDigest: deterministic, order-independent, changes when a
 *    belief's identity/version fields change.
 *  - evaluateDigestHealth: fresh / stale / unknown transitions.
 */
function belief(id: string, over: Partial<Pick<Belief, "factHash" | "confidence" | "tier" | "reinforceCount" | "status" | "updatedAt">> = {}): Pick<Belief, "id" | "factHash" | "confidence" | "tier" | "reinforceCount" | "status" | "updatedAt"> {
  return {
    id,
    factHash: `hash-${id}`,
    confidence: 0.5,
    tier: "whisper",
    reinforceCount: 1,
    status: "active",
    updatedAt: 1000,
    ...over,
  };
}

describe("fnv1a", () => {
  test("is deterministic and distinct for different inputs", () => {
    expect(fnv1a("a")).toBe(fnv1a("a"));
    expect(fnv1a("a")).not.toBe(fnv1a("b"));
    expect(fnv1a("hello")).toMatch(/^[0-9a-f]{8}$/);
  });
});

describe("computeBeliefDigest", () => {
  test("is deterministic for the same belief set", () => {
    const setA = [belief("b1"), belief("b2"), belief("b3")];
    expect(computeBeliefDigest(setA)).toBe(computeBeliefDigest([...setA].reverse()));
  });

  test("changes when a belief's version fields change", () => {
    const base = [belief("b1", { reinforceCount: 1 })];
    const reinforced = [belief("b1", { reinforceCount: 2 })];
    const reTiered = [belief("b1", { tier: "shout" })];
    const edited = [belief("b1", { updatedAt: 2000 })];
    const d = computeBeliefDigest(base);
    expect(computeBeliefDigest(reinforced)).not.toBe(d);
    expect(computeBeliefDigest(reTiered)).not.toBe(d);
    expect(computeBeliefDigest(edited)).not.toBe(d);
  });

  test("changes when the belief set changes (added/removed)", () => {
    const one = [belief("b1")];
    const two = [belief("b1"), belief("b2")];
    expect(computeBeliefDigest(two)).not.toBe(computeBeliefDigest(one));
  });

  test("is order-independent", () => {
    const a = [belief("b1"), belief("b2"), belief("b3")];
    const b = [belief("b3"), belief("b1"), belief("b2")];
    expect(computeBeliefDigest(a)).toBe(computeBeliefDigest(b));
  });
});

describe("evaluateDigestHealth", () => {
  test("unknown when no stored digest (first computation)", () => {
    const h = evaluateDigestHealth("abc", undefined, undefined);
    expect(h.digest).toBe("abc");
    expect(h.digestChanged).toBe(false);
    expect(h.digestStale).toBe(true);
    expect(h.lastComputedAt).toBeUndefined();
  });

  test("fresh when current == stored", () => {
    const h = evaluateDigestHealth("abc", "abc", 5000);
    expect(h.digestStale).toBe(false);
    expect(h.digestChanged).toBe(false);
    expect(h.lastComputedAt).toBe(5000);
  });

  test("stale/changed when current != stored", () => {
    const h = evaluateDigestHealth("new-digest", "old-digest", 5000);
    expect(h.digestChanged).toBe(true);
    expect(h.digestStale).toBe(false);
    expect(h.lastComputedAt).toBe(5000);
  });

  test("DigestHealth shape matches the hive/digest blackboard contract", () => {
    const h: DigestHealth = evaluateDigestHealth("x", undefined, undefined);
    // Desktop's diagnostics read {health, lastSyncAt}; the sweep writer maps
    // digestStale/digestChanged to health: unknown|fresh|stale.
    const health = h.digestStale ? "unknown" : h.digestChanged ? "stale" : "fresh";
    expect(["fresh", "stale", "unknown"]).toContain(health);
  });
});
