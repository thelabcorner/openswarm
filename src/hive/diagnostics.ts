import type { ArtifactAnnotation } from "../core/types.js";

/**
 * Hive H1 diagnostics (features/hive-mind-execution-layer, Wave 4 —
 * task_ff1d34). Pure rendering helpers for the compact HIVE block shown in
 * swarm_status / swarm_roster.
 *
 * Truthfulness rule (per assignment): never fabricate counts. Sections whose
 * substrate (beliefs/needs/spotlight tables or counters) is unavailable render
 * nothing; the block shows only what the caller could actually read.
 */

/** What a hive-adjacent read surface can return (caller supplies). */
export interface HiveReadInput {
  /** Active artifact annotations (stable substrate — corpse/gold counts). */
  annotations: ArtifactAnnotation[];
  /** Optional active-belief counts by tier (whisper/shout); undefined if the
   * beliefs substrate is unavailable. */
  beliefsByTier?: { whisper: number; shout: number };
  /** Optional count of beliefs in `resonant` status (H2 item 10); undefined
   * when the resonance substrate is unavailable. */
  resonantCount?: number;
  /** Optional active-need count; undefined if the needs substrate is absent. */
  activeNeeds?: number;
  /** Optional active spotlight topics; undefined if none exposed. */
  spotlightTopics?: string[];
  /** Optional consolidation status (H2 item 12): last run summary surfaced by
   * the hive_consolidate tool. `lastRunAt` epoch-ms, counts retained/pruned/
   * upgraded; undefined when consolidation hasn't run / has no surface. */
  consolidation?: { lastRunAt: number; retained: number; pruned: number; upgraded: number };
  /** Optional digest health (H2 anti-entropy, Scheduler's flag): "fresh" |
   * "stale" | "unknown", with the last-sync epoch-ms when known. */
  digest?: { health: "fresh" | "stale" | "unknown"; lastSyncAt?: number };
}

/**
 * Compute the compact hive summary from whatever the caller could read.
 * Returns a multi-line block (no trailing newline) or undefined when there is
 * nothing truthful to show (all sections absent/zero).
 */
export function buildHiveBlock(input: HiveReadInput): string | undefined {
  const lines: string[] = [];

  if (input.beliefsByTier) {
    const total = input.beliefsByTier.whisper + input.beliefsByTier.shout;
    if (total > 0) {
      lines.push(
        `  beliefs: ${total} active (${input.beliefsByTier.shout} shout, ${input.beliefsByTier.whisper} whisper)`,
      );
    }
  }

  if (input.resonantCount !== undefined && input.resonantCount > 0) {
    lines.push(`  resonant: ${input.resonantCount}`);
  }

  if (input.activeNeeds !== undefined && input.activeNeeds > 0) {
    lines.push(`  needs: ${input.activeNeeds} active`);
  }

  if (input.spotlightTopics && input.spotlightTopics.length > 0) {
    lines.push(`  spotlight: ${input.spotlightTopics.slice(0, 3).join(", ")}`);
  }

  // Corpse pile: paths with >= 3 active corpse annotations — collective
  // hesitation signal (H1, item 4). Gold dust: paths with gold annotations.
  const corpseByPath = new Map<string, number>();
  const goldPaths = new Set<string>();
  for (const a of input.annotations) {
    if (a.type === "corpse") corpseByPath.set(a.path, (corpseByPath.get(a.path) ?? 0) + 1);
    else if (a.type === "gold") goldPaths.add(a.path);
  }
  const corpsePiles = [...corpseByPath.entries()].filter(([, n]) => n >= 3).map(([p, n]) => `${p} (${n})`);
  if (corpsePiles.length > 0) {
    lines.push(`  corpse piles: ${corpsePiles.slice(0, 3).join(", ")}`);
  } else if (goldPaths.size > 0) {
    // Gold trails are the positive counterpart; show only when no corpse pile
    // dominates (keep the block compact).
    lines.push(`  gold trails: ${[...goldPaths].slice(0, 3).join(", ")}`);
  }

  if (input.consolidation) {
    const age = Math.max(0, Math.round((Date.now() - input.consolidation.lastRunAt) / 1000));
    lines.push(
      `  consolidation: ${age}s ago (retained ${input.consolidation.retained}, pruned ${input.consolidation.pruned}, upgraded ${input.consolidation.upgraded})`,
    );
  }

  if (input.digest) {
    const sync = input.digest.lastSyncAt
      ? ` (last sync ${Math.max(0, Math.round((Date.now() - input.digest.lastSyncAt) / 1000))}s ago)`
      : "";
    lines.push(`  digest: ${input.digest.health}${sync}`);
  }

  if (lines.length === 0) return undefined;
  return ["HIVE (advisory — counts are live, not fabricated)", ...lines].join("\n");
}

/** One-line hive summary for swarm_roster (compact; undefined when trivial). */
export function buildHiveSummary(input: HiveReadInput): string | undefined {
  const parts: string[] = [];
  if (input.beliefsByTier && input.beliefsByTier.shout + input.beliefsByTier.whisper > 0) {
    parts.push(`${input.beliefsByTier.shout + input.beliefsByTier.whisper} beliefs`);
  }
  if (input.resonantCount !== undefined && input.resonantCount > 0) {
    parts.push(`${input.resonantCount} resonant`);
  }
  if (input.activeNeeds !== undefined && input.activeNeeds > 0) {
    parts.push(`${input.activeNeeds} needs`);
  }
  const corpsePiles = new Map<string, number>();
  for (const a of input.annotations) {
    if (a.type === "corpse") corpsePiles.set(a.path, (corpsePiles.get(a.path) ?? 0) + 1);
  }
  const piles = [...corpsePiles.values()].filter((n) => n >= 3).length;
  if (piles > 0) parts.push(`${piles} corpse pile(s)`);
  if (input.spotlightTopics && input.spotlightTopics.length > 0) {
    parts.push("spotlight active");
  }
  if (input.consolidation) {
    parts.push(`consolidated ${Math.max(0, Math.round((Date.now() - input.consolidation.lastRunAt) / 1000))}s ago`);
  }
  if (input.digest && input.digest.health !== "unknown") {
    parts.push(`digest ${input.digest.health}`);
  }
  if (parts.length === 0) return undefined;
  return `hive: ${parts.join(" · ")}`;
}
