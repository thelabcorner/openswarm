import type { Belief } from "../core/types.js";

/**
 * Hive H2 resonance + consolidation helpers (features/hive-mind-execution-layer
 * items 10 & 12 — task_d7f58f, Core-Auditor lane). Pure logic only; the tool
 * layer (plugin.ts) owns store calls + election + output.
 *
 * Resonance (item 10): independent convergence — the SAME belief reached by
 * DIFFERENT authors from DISJOINT evidence chains is stronger than either
 * alone: combined confidence = 1 - (1-c1)*(1-c2). A belief becomes `resonant`
 * when an incoming reinforce comes from a DIFFERENT author with disjoint
 * evidence refs.
 *
 * Consolidation (item 12): ephemeral → durable summarization guidance, pruning
 * of low-confidence/low-reuse beliefs, causal-chain compression notes, and
 * upgrade/expire decisions. One consolidator wins per swarm (CAS/lease — the
 * tool layer enforces it via a blackboard lease key).
 */

/** Merge two evidence-ref JSON arrays, deduped, preserving order. */
export function mergeEvidenceRefs(a: string[] | undefined, b: string[] | undefined): string[] {
  const out: string[] = [];
  for (const ref of [...(a ?? []), ...(b ?? [])]) {
    if (!out.includes(ref)) out.push(ref);
  }
  return out;
}

/** Parse a Belief.evidenceRefs JSON-array string safely ([] on garbage). */
export function parseEvidenceRefs(raw: string | undefined): string[] {
  if (!raw) return [];
  try {
    const p = JSON.parse(raw);
    return Array.isArray(p) ? p.filter((x): x is string => typeof x === "string") : [];
  } catch {
    return [];
  }
}

/**
 * Resonance boost: if the incoming author differs from the belief's author AND
 * the incoming evidence refs are disjoint from the stored ones, combine
 * confidences via 1 - (1-c1)*(1-c2) and return { combined, resonant: true }.
 * Otherwise return the plain updated confidence with resonant: false.
 */
export function computeResonance(input: {
  existing: Belief;
  incomingAuthorId: string;
  incomingEvidence: string[];
  deltaConfidence?: number;
}): { confidence: number; resonant: boolean; mergedEvidence: string[] } {
  const existingRefs = parseEvidenceRefs(input.existing.evidenceRefs);
  const merged = mergeEvidenceRefs(existingRefs, input.incomingEvidence);
  const disjoint = input.incomingEvidence.length > 0
    && !input.incomingEvidence.some((r) => existingRefs.includes(r));
  const differentAuthor = input.incomingAuthorId !== input.existing.authorMemberId;
  if (disjoint && differentAuthor) {
    const c1 = input.existing.confidence;
    const c2 = Math.min(1, Math.max(0, c1 + (input.deltaConfidence ?? 0.1)));
    const combined = 1 - (1 - c1) * (1 - c2);
    return { confidence: Math.min(1, combined), resonant: true, mergedEvidence: merged };
  }
  const plain = Math.min(1, Math.max(0, input.existing.confidence + (input.deltaConfidence ?? 0.1)));
  return { confidence: plain, resonant: false, mergedEvidence: merged };
}

/** Is this belief eligible for consolidation pruning (low confidence AND low
 * reuse)? A belief with reinforceCount < minReinforce and confidence <
 * minConfidence is a weak signal — candidate for pruning. */
export function isPruneCandidate(belief: Belief, minConfidence: number, minReinforce: number): boolean {
  return belief.confidence < minConfidence && belief.reinforceCount < minReinforce;
}

/** Should a belief be upgraded (whisper → shout)? Already-shout stays. */
export function shouldUpgrade(belief: Belief): boolean {
  return belief.tier === "whisper" && belief.reinforceCount >= 2;
}

/**
 * Consolidation decision for one belief: returns the action the consolidator
 * should take. Deterministic and pure:
 *  - "upgrade"  — whisper with enough reinforcements → promote to shout
 *  - "prune"    — weak + rarely-reinforced → forget (candidate for pruning)
 *  - "retain"   — strong and/or reinforced → keep (durable)
 *  - "expire"   — TTL passed → let the sweep expire it (noted, not pruned)
 */
export function consolidationAction(
  belief: Belief,
  opts: { minConfidence?: number; minReinforce?: number; now?: number } = {},
): "upgrade" | "prune" | "retain" | "expire" {
  const minConfidence = opts.minConfidence ?? 0.3;
  const minReinforce = opts.minReinforce ?? 2;
  const now = opts.now ?? Date.now();
  if (belief.expiresAt !== undefined && belief.expiresAt <= now) return "expire";
  if (shouldUpgrade(belief)) return "upgrade";
  if (isPruneCandidate(belief, minConfidence, minReinforce)) return "prune";
  return "retain";
}

/** Compact one-line causal-chain note for a belief (compression guidance,
 * item 12: "compress causal chains into learnings"). Pure; no store access. */
export function causalChainNote(belief: Belief): string {
  const refs = parseEvidenceRefs(belief.evidenceRefs);
  return refs.length
    ? `causal chain: ${belief.factHash} ← ${refs.length} evidence ref(s) (${refs.slice(0, 3).join(", ")}${refs.length > 3 ? ", …" : ""})`
    : `causal chain: ${belief.factHash} ← no recorded evidence`;
}
