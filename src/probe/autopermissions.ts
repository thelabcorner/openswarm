import type { AgentRuntime } from "../runtime/runtime-types.js";
import type { AgentPermissions } from "../runtime/opencode-runtime.js";

/**
 * Autopermissions propagation probe (plugin-only, read-only).
 *
 * Distinguishes how the coordinator's autopermissions toggle (Ctrl+Shift+A /
 * OpenCode settings) is stored, per decisions/autopermissions-propagation-plan:
 *
 *   Case A — the toggle mutates the AGENT's permission block visible via
 *            `client.app.agents` / `getSessionPermissions(coordinator)`.
 *            Propagation is already pull-based: the next member `permission.ask`
 *            inherits the coordinator's current ruleset automatically.
 *   Case B — the toggle lives in SESSION-private permission state (the session's
 *            own `permission` field on session.get). The plugin must read that
 *            field and, to propagate, write it to member sessions via
 *            `updateSession({ permission })` — clamped to worktree/temp (D6).
 *   Case C — neither surface is observable from the plugin; propagation would
 *            have to emulate via a coordinator-verdict cache (fallback only).
 *
 * The probe reads BOTH surfaces for the coordinator session and reports which
 * case holds. It performs NO writes and NO permission widening.
 */

export type AutopermissionsProbeResult = {
  case: "A" | "B" | "C" | "mixed";
  coordinatorSessionId: string;
  /** The agent's permission block (Case A source), if resolvable. */
  agentPermissions?: AgentPermissions;
  /** The session's own permission field (Case B source), if exposed. */
  sessionPermissions?: AgentPermissions;
  /** Whether getSessionPermissions (agent block) resolved at all. */
  agentBlockVisible: boolean;
  /** Whether the session payload exposes a permission field. */
  sessionPermissionVisible: boolean;
  detail: string;
};

/**
 * Run the autopermissions probe against a live runtime for the coordinator
 * session. Read-only: calls getSession + getSessionPermissions only.
 */
export async function probeAutopermissions(
  runtime: AgentRuntime,
  coordinatorSessionId: string,
): Promise<AutopermissionsProbeResult> {
  let sessionPermissions: AgentPermissions | undefined;
  let sessionPermissionVisible = false;
  try {
    const session = await runtime.getSession(coordinatorSessionId);
    if (session?.permission) {
      sessionPermissions = session.permission;
      sessionPermissionVisible = true;
    }
  } catch {
    sessionPermissionVisible = false;
  }

  let agentPermissions: AgentPermissions | undefined;
  let agentBlockVisible = false;
  try {
    agentPermissions = await runtime.getSessionPermissions?.(coordinatorSessionId);
    agentBlockVisible = agentPermissions !== undefined;
  } catch {
    agentBlockVisible = false;
  }

  const case_: AutopermissionsProbeResult["case"] =
    agentBlockVisible && sessionPermissionVisible
      ? "mixed"
      : agentBlockVisible
        ? "A"
        : sessionPermissionVisible
          ? "B"
          : "C";

  const detail = [
    `agent permission block visible: ${agentBlockVisible}`,
    `session permission field visible: ${sessionPermissionVisible}`,
    case_ === "A"
      ? "toggle likely lives in the agent config → pull-based propagation already works; next member permission.ask inherits automatically"
      : case_ === "B"
        ? "toggle lives in session-private state → requires the session-permission read/write path (RuntimeSession.permission + updateSession({permission})) with D6 clamp"
        : case_ === "mixed"
          ? "both surfaces visible — cross-check values to find the authoritative one"
          : "neither surface observable plugin-only → Case C emulation (coordinator-verdict cache) is the fallback",
  ].join(" · ");

  return {
    case: case_,
    coordinatorSessionId,
    agentPermissions,
    sessionPermissions,
    agentBlockVisible,
    sessionPermissionVisible,
    detail,
  };
}
