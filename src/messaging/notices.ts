import { fence } from "../core/fence.js";

/**
 * P5 Hive H2 notification half — consolidation / pruning / digest-change
 * notices (features/hive-mind-execution-layer item 12).
 *
 * Consolidation logic is Core-Auditor's lane (hive_consolidate); digest health
 * is Scheduler-Auditor's lane (anti-entropy digest in sweep). THIS module owns
 * the NOTICE side: turning those run results into truthful, fenced, exactly-
 * once, low-noise swarm messages.
 *
 * Truthfulness rule (mirrors the hive diagnostics rule): every count rendered
 * comes from the caller's actual result — never fabricated, never padded.
 * Non-trivial only: a run that retained/pruned/upgraded nothing and has no
 * contradictions emits NO notice.
 */

/** Consolidation run result — the agreed contract Core's hive_consolidate
 * returns (confirmed shape). Fields optional so a pre-landing shape degrades
 * cleanly. */
export interface ConsolidationResult {
  /** Opaque run id (e.g. `cons-<ts>-<rand>`) — the exactly-once dedupe key. */
  runId: string;
  /** Winner member name of the consolidation election. */
  coordinator?: string;
  /** Beliefs kept after consolidation. */
  retained?: number;
  /** Beliefs pruned by consolidation (weak + low-reuse, soft+hard). */
  pruned?: number;
  /** Beliefs upgraded whisper→shout by this run. */
  upgraded?: number;
  /** TTL-passed beliefs noted for the expire sweep. */
  expired?: number;
  /** Unresolved contradictions: distinct beliefs that token-overlap strongly
   * but differ in evidence (both kept, flagged). Rendered fenced. May be the
   * array (Core's full shape) or a bare count (Core's current tool output
   * writes `contradictions.length`) — both tolerated so the contract degrades
   * gracefully. */
  contradictions?: Array<{ factHash?: string; text?: string }> | number;
  /** Pruned fact hashes for traceability. */
  prunedFactHashes?: string[];
  /** One-line causal-chain notes. */
  causalChains?: string[];
  /** Ephemeral→durable summarization guidance. */
  guidance?: string;
}

/** True when a consolidation run produced anything worth reporting. */
export function consolidationIsNotable(r: ConsolidationResult): boolean {
  const contradictionCount = typeof r.contradictions === "number" ? r.contradictions : r.contradictions?.length ?? 0;
  return (r.retained ?? 0) > 0
    || (r.pruned ?? 0) > 0
    || (r.upgraded ?? 0) > 0
    || (r.expired ?? 0) > 0
    || contradictionCount > 0
    || (r.guidance?.trim().length ?? 0) > 0
    || (r.causalChains?.length ?? 0) > 0;
}

/**
 * Render the consolidation notice body (fenced, self-contained, truthful).
 * Counts come verbatim from the result — nothing is fabricated. The body is
 * the coordinator-facing detail; a compact one-line broadcast variant is
 * provided separately.
 */
export function renderConsolidationNotice(r: ConsolidationResult): string {
  const parts = [
    "[HIVE CONSOLIDATION]",
    `run ${r.runId}${r.coordinator ? ` (winner: ${fence(r.coordinator)})` : ""}`,
    `retained ${r.retained ?? 0}, pruned ${r.pruned ?? 0}, upgraded ${r.upgraded ?? 0}${(r.expired ?? 0) > 0 ? `, expired ${r.expired}` : ""}`,
  ];
  const contradictionCount = typeof r.contradictions === "number" ? r.contradictions : r.contradictions?.length ?? 0;
  if (contradictionCount > 0) {
    if (Array.isArray(r.contradictions)) {
      const list = r.contradictions.map((c) => `- ${c.factHash ? `${c.factHash}: ` : ""}${fence(c.text ?? "unknown")}`).join("\n");
      parts.push(`unresolved contradictions (${contradictionCount}):\n${list}`);
    } else {
      parts.push(`unresolved contradictions (${contradictionCount})`);
    }
  }
  if (r.causalChains?.length) {
    parts.push(`causal chains (${r.causalChains.length}):\n${r.causalChains.map((c) => `- ${fence(c)}`).join("\n")}`);
  }
  if (r.guidance?.trim()) parts.push(`guidance: ${fence(r.guidance)}`);
  return parts.join("\n");
}

/** Compact one-line broadcast variant (Core's notice preference #2). */
export function renderConsolidationSummary(r: ConsolidationResult): string {
  const contradictionCount = typeof r.contradictions === "number" ? r.contradictions : r.contradictions?.length ?? 0;
  return `consolidation run ${r.runId}: retained ${r.retained ?? 0}, pruned ${r.pruned ?? 0}, upgraded ${r.upgraded ?? 0}${(r.expired ?? 0) > 0 ? `, expired ${r.expired}` : ""}${contradictionCount > 0 ? `, ${contradictionCount} unresolved contradiction(s)` : ""}`;
}

/** Pruning-truth notice: only when non-trivial, counts verbatim from the sweep. */
export function renderPruningNotice(pruned: number): string | undefined {
  if (pruned <= 0) return undefined;
  return `[HIVE PRUNING] ${pruned} stale belief(s) expired/pruned by the sweep (no fabrication — actual count).`;
}

/** Digest-health flip notice: low-noise, names the new state. */
export function renderDigestNotice(health: "healthy" | "degraded"): string {
  return `[HIVE DIGEST] anti-entropy digest health flipped to ${health}.`;
}
