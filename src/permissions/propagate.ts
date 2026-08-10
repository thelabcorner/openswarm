import type { AgentRuntime } from "../runtime/runtime-types.js";
import type { AgentPermissions } from "../runtime/runtime-types.js";
import { clampPropagatedPermissions } from "./clamp.js";

/**
 * Autopermissions propagation executor (Case B, D6-clamped).
 *
 * Reads the coordinator session's permission ruleset via the runtime adapter,
 * clamps it (never widen; webfetch ask unless allowed), and writes the clamped
 * ruleset to every active worker member session via `updateSession({ permission })`.
 *
 * Case A (agent block visible) needs no propagation — the next member
 * `permission.ask` already inherits pull-based. This executor only acts when
 * the SESSION-private permission surface is the visible source (Case B), and
 * only if the runtime supports the write surface (`updateSession`).
 */

export interface PropagationResult {
  /** Whether this sweep performed Case B propagation. */
  propagated: boolean;
  /** Permission mode observed for the coordinator (see PermissionMode). */
  mode: "inherit" | "worktree-scoped" | "unknown";
  /** Member names that received a clamped permission write. */
  updated: string[];
  /** Member names skipped (stopped/failed/coordinator or no write surface). */
  skipped: string[];
  /** Skip reasons keyed by member name (why each was not updated). */
  skipReasons: Record<string, string>;
  /** True when the write surface is unavailable (updateSession missing). */
  noWriteSurface: boolean;
  detail: string;
}

/**
 * Compact per-member counts summary for the `perms:` diagnostics line
 * (Wave 2 UX carry-over #1). E.g. "3/4 updated; 1 skipped: stopped".
 *
 * P3 carry-over (Wave 6 polish): when MULTIPLE distinct skip reasons exist,
 * enumerate per-reason counts instead of collapsed labels —
 * e.g. "2 skipped: stopped (1), update failed (1)". Single-reason output is
 * unchanged ("1 skipped: stopped") to keep the common case compact.
 */
export function permsCountsSummary(result: PropagationResult): string {
  const total = result.updated.length + result.skipped.length;
  if (result.mode !== "worktree-scoped" || !result.propagated) {
    return `${result.updated.length}/${total} updated`;
  }
  if (result.skipped.length === 0) {
    return `${result.updated.length}/${total} updated`;
  }
  // Count each distinct skip reason.
  const byReason = new Map<string, number>();
  for (const reason of Object.values(result.skipReasons)) {
    byReason.set(reason, (byReason.get(reason) ?? 0) + 1);
  }
  const labels = [...byReason.entries()]
    .map(([reason, n]) => (byReason.size === 1 ? reason : `${reason} (${n})`))
    .join(", ");
  return `${result.updated.length}/${total} updated; ${result.skipped.length} skipped: ${labels}`;
}

export interface PropagateInput {
  runtime: AgentRuntime;
  coordinatorSessionId: string;
  memberSessions: Array<{ name: string; sessionId: string; status: string; role?: string }>;
}

/**
 * Propagate the coordinator's clamped session permission to members.
 *
 * Order of operations:
 *  1. Resolve the coordinator's agent permission block (Case A source). If it
 *     resolves, propagation is pull-based — no write needed; report `inherit`.
 *  2. Else read the coordinator session's own permission field (Case B source).
 *     If present, clamp it and write to each active worker member.
 *  3. Else report `unknown` (Case C — emulation cache documented, no write).
 */
export async function propagateAutopermissions(input: PropagateInput): Promise<PropagationResult> {
  const { runtime, coordinatorSessionId, memberSessions } = input;

  // Case A: agent block is the live source — nothing to propagate.
  let agentBlockVisible = false;
  try {
    agentBlockVisible = (await runtime.getSessionPermissions?.(coordinatorSessionId)) !== undefined;
  } catch {
    agentBlockVisible = false;
  }
  if (agentBlockVisible) {
    return {
      propagated: false,
      mode: "inherit",
      updated: [],
      skipped: memberSessions.map((m) => m.name),
      skipReasons: Object.fromEntries(memberSessions.map((m) => [m.name, "pull-based inherit"])),
      noWriteSurface: false,
      detail: "Case A: coordinator agent permission block visible — members inherit pull-based on each permission.ask; no write needed.",
    };
  }

  // Case B: session-private permission surface.
  let coordinatorPerms: AgentPermissions | undefined;
  try {
    coordinatorPerms = (await runtime.getSession(coordinatorSessionId))?.permission;
  } catch {
    coordinatorPerms = undefined;
  }

  if (!coordinatorPerms) {
    return {
      propagated: false,
      mode: "unknown",
      updated: [],
      skipped: memberSessions.map((m) => m.name),
      skipReasons: Object.fromEntries(memberSessions.map((m) => [m.name, "no permission surface"])),
      noWriteSurface: false,
      detail: "Case C: neither agent block nor session permission visible — emulation cache fallback documented; no write performed.",
    };
  }

  // Write surface required for propagation.
  if (!runtime.updateSession) {
    return {
      propagated: false,
      mode: "worktree-scoped",
      updated: [],
      skipped: memberSessions.map((m) => m.name),
      skipReasons: Object.fromEntries(memberSessions.map((m) => [m.name, "no updateSession surface"])),
      noWriteSurface: true,
      detail: "Case B detected but runtime lacks updateSession — propagation skipped (no write surface).",
    };
  }

  const clamped = clampPropagatedPermissions(coordinatorPerms);
  const updated: string[] = [];
  const skipped: string[] = [];
  const skipReasons: Record<string, string> = {};
  for (const m of memberSessions) {
    if (m.role === "coordinator") { skipped.push(m.name); skipReasons[m.name] = "coordinator"; continue; }
    if (["stopped", "stopping", "failed"].includes(m.status)) { skipped.push(m.name); skipReasons[m.name] = m.status; continue; }
    try {
      await runtime.updateSession(m.sessionId, { permission: clamped });
      updated.push(m.name);
    } catch {
      skipped.push(m.name);
      skipReasons[m.name] = "update failed";
    }
  }

  return {
    propagated: updated.length > 0,
    mode: "worktree-scoped",
    updated,
    skipped,
    skipReasons,
    noWriteSurface: false,
    detail: `Case B: clamped coordinator session permission propagated to ${updated.length} member(s) (never widened; webfetch ${clamped.webfetch}).`,
  };
}
