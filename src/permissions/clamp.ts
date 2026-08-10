import type { AgentPermissions } from "../runtime/runtime-types.js";

/**
 * Autopermissions propagation — pure clamp + classification helpers.
 *
 * Implements the D6-clamped Case B propagation path from
 * decisions/autopermissions-propagation-plan and audit/desktop D6:
 *
 *  - Propagation must NEVER widen a member's permission scope beyond the
 *    swarm worktree/temp boundary rules.
 *  - webfetch stays `ask` unless the coordinator's session ruleset explicitly
 *    allows it.
 *  - edit/bash/external_directory are clamped to the coordinator's verdict
 *    where the coordinator allows; anything the coordinator does NOT allow is
 *    `ask` (never promoted to allow).
 *
 * This module is deliberately pure of OpenCode SDK types so it is fully
 * unit-testable.
 */

/** The effective permission modes surfaced by diagnostics (Wave 2). */
export type PermissionMode =
  /** Coordinator's agent block is the live source (Case A — pull-based). */
  | "inherit"
  /** Clamped coordinator session ruleset propagated to members (Case B). */
  | "worktree-scoped"
  /** Plugin option allowAllMemberPermissions (accept-all, static). */
  | "accept-all-static"
  /** Neither source resolvable (Case C — emulation fallback documented). */
  | "unknown";

export interface ClampOptions {
  /** The swarm's worktree (may be empty for legacy swarms). */
  worktree?: string;
  /** OS temp scratch dir (build artifacts etc.). */
  tempDir?: string;
}

/**
 * Clamp a coordinator session permission ruleset for propagation to members.
 *
 * Safety rules (never widen — P-D4):
 *  - edit:      allow only if the coordinator allows edit. edit is the one
 *               worktree-shaped surface, so a coordinator `allow` may propagate.
 *  - bash:      ALWAYS ask. A blanket `bash: allow` authorizes arbitrary
 *               commands (not path-scoped); per-command object rules are not
 *               copied verbatim (paths may be out-of-scope). Path-level bash
 *               enforcement stays in autoAllowSwarmPermission's D6 boundary.
 *  - webfetch:  ALWAYS ask unless the coordinator's ruleset has an explicit
 *               per-rule allow we can verify is safe; we do not propagate a
 *               blanket webfetch allow (it is not path-scoped).
 *  - external_directory: ALWAYS ask. A blanket `allow` is NOT path-scoped; the
 *               D6 worktree/temp boundary lives in autoAllowSwarmPermission,
 *               which consults it per-ask. Propagating a blanket external
 *               allow would grant members arbitrary external-directory access.
 *
 * The net effect: only `edit` may propagate as `allow`; everything else is
 * `ask`. This is strictly narrower than the coordinator — never wider.
 * `ClampOptions` (worktree/tempDir) is accepted for future path-scoped rule
 * derivation but is intentionally unused today (P-D13: kept minimal rather
 * than pretending to enforce a boundary it cannot represent).
 */
export function clampPropagatedPermissions(
  coordinator: AgentPermissions | undefined,
  _opts: ClampOptions = {},
): AgentPermissions {
  const c: AgentPermissions = coordinator ?? { edit: "ask", bash: "ask", webfetch: "ask", external_directory: "ask" };
  // Only `edit` can propagate as allow (it is the worktree-shaped surface);
  // bash/webfetch/external_directory are always ask (never widen, P-D4).
  const edit = c.edit === "allow" ? "allow" : "ask";
  return { edit, bash: "ask", webfetch: "ask", external_directory: "ask" };
}

/**
 * Classify the effective permission mode for a member.
 *
 * Priority: the accept-all plugin option is the widest and most static surface
 * (constructor-time only), so it reports first. Otherwise, if the coordinator's
 * agent permission block resolves (Case A), the member inherits live from it
 * (`inherit`). If only session-private permission is visible (Case B), the
 * clamped ruleset governs (`worktree-scoped`). If neither resolves (Case C),
 * report `unknown` (emulation cache pointer in docs).
 */
export function permissionMode(input: {
  allowAllMemberPermissions: boolean;
  agentBlockVisible: boolean;
  sessionPermissionVisible: boolean;
}): PermissionMode {
  if (input.allowAllMemberPermissions) return "accept-all-static";
  if (input.agentBlockVisible) return "inherit";
  if (input.sessionPermissionVisible) return "worktree-scoped";
  return "unknown";
}

/** Short label for roster/status lines (kept compact for TU1/TU12). */
export function permissionModeLabel(mode: PermissionMode): string {
  switch (mode) {
    case "inherit": return "perms: inherit";
    case "worktree-scoped": return "perms: worktree-scoped";
    case "accept-all-static": return "perms: accept-all-static";
    default: return "perms: unknown";
  }
}
