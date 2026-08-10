import type { Belief } from "../core/types.js";
import { needTokens } from "../messaging/need.js";

/**
 * Hive H2 relevance surface (features/hive-mind-execution-layer item 11 —
 * task_d7f58f, Core-Auditor lane). Pure token-based relevance scoring for
 * queries vs beliefs/tasks, with a documented embedding hook for future H2
 * semantic search (no SDK dependency today).
 *
 * Truthfulness: scoring is deterministic and local; the tool layer reports
 * scores as computed, and the embedding hook is a documented no-op extension
 * point (never fabricates semantic similarity).
 */

/** Token-overlap relevance score (0..1) of a belief text+tags against a query.
 * Uses the same tokenizer as need routing (needTokens: lowercase alnum, min
 * length 3) so hive_relevant and hive_need agree on what "matches". */
export function beliefRelevanceScore(belief: Pick<Belief, "text" | "tags">, query: string): number {
  const q = needTokens(query);
  if (q.length === 0) return 0;
  const hay = `${belief.text} ${belief.tags ?? ""}`.toLowerCase();
  let hits = 0;
  for (const t of q) {
    if (hay.includes(t)) hits++;
  }
  // Fraction of query tokens present; cap at 1.
  return Math.min(1, hits / q.length);
}

/** Relevance of a task (title+description) against a query — reuses the same
 * tokenizer so task relevance and belief relevance are comparable. */
export function taskRelevanceScore(task: { title: string; description?: string }, query: string): number {
  const q = needTokens(query);
  if (q.length === 0) return 0;
  const hay = `${task.title} ${task.description ?? ""}`.toLowerCase();
  let hits = 0;
  for (const t of q) {
    if (hay.includes(t)) hits++;
  }
  return Math.min(1, hits / q.length);
}

/**
 * Future extension point: semantic relevance hook (H2 embeddings). Today it is
 * a documented no-op that returns undefined — the caller then falls back to
 * token scoring. When an embedding provider is wired (no SDK dependency
 * required; e.g. a local model endpoint or hash-of-embedding cache), this
 * returns a 0..1 score. Kept separate so the pure token path never changes.
 */
export function semanticRelevanceHook(
  _query: string,
  _text: string,
): number | undefined {
  return undefined; // not implemented in H2 — token scoring only (documented)
}

export interface RelevanceResult {
  factHash: string;
  text: string;
  score: number;
  tier: Belief["tier"];
  tags?: string;
}

/** Rank active beliefs by token relevance to the query (desc), capped at
 * `limit`. Believes with score 0 are excluded (no noise). */
export function rankBeliefsByRelevance(
  beliefs: Belief[],
  query: string,
  limit = 5,
): RelevanceResult[] {
  const scored = beliefs
    .map((b) => ({ b, score: beliefRelevanceScore(b, query) }))
    .filter((s) => s.score > 0)
    .sort((a, b2) => b2.score - a.score || b2.b.reinforceCount - a.b.reinforceCount)
    .slice(0, limit);
  return scored.map((s) => ({
    factHash: s.b.factHash,
    text: s.b.text,
    score: s.score,
    tier: s.b.tier,
    tags: s.b.tags,
  }));
}
