import { describe, expect, test } from "bun:test";
import { clampPropagatedPermissions, permissionMode, permissionModeLabel } from "../../src/permissions/clamp.ts";
import { propagateAutopermissions, permsCountsSummary } from "../../src/permissions/propagate.ts";
import type { AgentRuntime, AgentPermissions, RuntimeSession } from "../../src/runtime/runtime-types.ts";

/**
 * Fake runtime with switchable permission surfaces:
 *  - agentPermissions: what getSessionPermissions returns (Case A source).
 *  - sessionPermission: what getSession().permission returns (Case B source).
 *  - updateSession records writes so tests can assert propagation + clamping.
 */
class FakePermRuntime implements AgentRuntime {
  readonly kind = "fake-perm";
  agentPermissions: AgentPermissions | undefined;
  sessionPermission: AgentPermissions | undefined;
  updateSessionCalls: Array<{ sid: string; permission: AgentPermissions }> = [];
  hasUpdateSession = true;
  failUpdate = false;

  async createSession(): Promise<RuntimeSession> { throw new Error("unused"); }
  async getSession(sid: string): Promise<RuntimeSession | null> {
    return { id: sid, title: "", directory: ".", permission: this.sessionPermission };
  }
  async listChildren(): Promise<RuntimeSession[]> { return []; }
  async prompt(): Promise<any> { throw new Error("unused"); }
  async promptAsync(): Promise<void> {}
  async abort(): Promise<void> {}
  async getStatus(): Promise<any> { return { type: "idle" }; }
  async getMessages(): Promise<any[]> { return []; }
  async updateSession?(sid: string, patch: { permission?: AgentPermissions }): Promise<void> {
    if (this.failUpdate) throw new Error("update failed");
    this.updateSessionCalls.push({ sid, permission: patch.permission! });
  }
  async getSessionPermissions?(sessionID: string): Promise<AgentPermissions | undefined> {
    return this.agentPermissions;
  }
}

const MEMBERS = [
  { name: "worker1", sessionId: "ses-w1", status: "idle", role: "worker" },
  { name: "worker2", sessionId: "ses-w2", status: "idle", role: "worker" },
  { name: "stopped1", sessionId: "ses-st", status: "stopped", role: "worker" },
];

describe("clampPropagatedPermissions (D6, never widen)", () => {
  test("undefined coordinator ruleset -> all ask (nothing widened)", () => {
    expect(clampPropagatedPermissions(undefined)).toEqual({
      edit: "ask", bash: "ask", webfetch: "ask", external_directory: "ask",
    });
  });

  test("only edit propagates allow; bash/webfetch/external_directory stay ask even when coordinator allows (P-D4)", () => {
    const clamped = clampPropagatedPermissions({
      edit: "allow", bash: "allow", webfetch: "allow", external_directory: "allow",
    });
    expect(clamped).toEqual({
      edit: "allow", bash: "ask", webfetch: "ask", external_directory: "ask",
    });
  });

  test("coordinator ask/deny -> member stays ask (never promoted)", () => {
    const clamped = clampPropagatedPermissions({
      edit: "ask", bash: "deny", webfetch: "deny", external_directory: "ask",
    });
    expect(clamped).toEqual({
      edit: "ask", bash: "ask", webfetch: "ask", external_directory: "ask",
    });
  });

  test("webfetch stays ask unless explicitly allowed", () => {
    const clamped = clampPropagatedPermissions({
      edit: "allow", bash: "ask", webfetch: "ask", external_directory: "deny",
    });
    expect(clamped.webfetch).toBe("ask");
    expect(clamped.external_directory).toBe("ask");
  });

  test("per-command bash object is NOT copied verbatim (paths may be out-of-scope)", () => {
    const clamped = clampPropagatedPermissions({
      edit: "allow", bash: { "C:/repo/**": "allow" }, webfetch: "ask", external_directory: "ask",
    });
    // Object rules are not propagated as-is; member bash is ask (D6 keeps
    // path-level enforcement in autoAllowSwarmPermission).
    expect(clamped.bash).toBe("ask");
  });
});

describe("permissionMode / permissionModeLabel (diagnostics)", () => {
  test("accept-all-static wins over everything", () => {
    expect(permissionMode({ allowAllMemberPermissions: true, agentBlockVisible: true, sessionPermissionVisible: true }))
      .toBe("accept-all-static");
  });
  test("inherit when agent block visible (Case A)", () => {
    expect(permissionMode({ allowAllMemberPermissions: false, agentBlockVisible: true, sessionPermissionVisible: false }))
      .toBe("inherit");
  });
  test("worktree-scoped when only session permission visible (Case B)", () => {
    expect(permissionMode({ allowAllMemberPermissions: false, agentBlockVisible: false, sessionPermissionVisible: true }))
      .toBe("worktree-scoped");
  });
  test("unknown when neither visible (Case C)", () => {
    expect(permissionMode({ allowAllMemberPermissions: false, agentBlockVisible: false, sessionPermissionVisible: false }))
      .toBe("unknown");
  });
  test("labels are compact and self-describing", () => {
    expect(permissionModeLabel("inherit")).toBe("perms: inherit");
    expect(permissionModeLabel("worktree-scoped")).toBe("perms: worktree-scoped");
    expect(permissionModeLabel("accept-all-static")).toBe("perms: accept-all-static");
    expect(permissionModeLabel("unknown")).toBe("perms: unknown");
  });
});

describe("propagateAutopermissions (Case B propagation)", () => {
  test("Case A: agent block visible -> no write, mode inherit", async () => {
    const rt = new FakePermRuntime();
    rt.agentPermissions = { edit: "allow", bash: "ask", webfetch: "ask", external_directory: "ask" };
    const result = await propagateAutopermissions({ runtime: rt, coordinatorSessionId: "ses-coord", memberSessions: MEMBERS });
    expect(result.mode).toBe("inherit");
    expect(result.propagated).toBe(false);
    expect(rt.updateSessionCalls.length).toBe(0);
  });

  test("Case B: session permission visible -> clamped write to active members only", async () => {
    const rt = new FakePermRuntime();
    rt.sessionPermission = { edit: "allow", bash: "allow", webfetch: "ask", external_directory: "ask" };
    const result = await propagateAutopermissions({ runtime: rt, coordinatorSessionId: "ses-coord", memberSessions: MEMBERS });
    expect(result.mode).toBe("worktree-scoped");
    expect(result.propagated).toBe(true);
    // Two active workers written; stopped member skipped.
    expect(rt.updateSessionCalls.length).toBe(2);
    const sids = rt.updateSessionCalls.map((c) => c.sid).sort();
    expect(sids).toEqual(["ses-w1", "ses-w2"]);
    // Clamped (P-D4): bash is always ask (never propagated verbatim), webfetch
    // ask preserved, external_directory ask — only edit may propagate allow.
    for (const call of rt.updateSessionCalls) {
      expect(call.permission.webfetch).toBe("ask");
      expect(call.permission.bash).toBe("ask");
      expect(call.permission.external_directory).toBe("ask");
    }
  });

  test("Case B: coordinator allow-everything is clamped (webfetch/external/bash stay ask)", async () => {
    const rt = new FakePermRuntime();
    rt.sessionPermission = { edit: "allow", bash: "allow", webfetch: "allow", external_directory: "allow" };
    await propagateAutopermissions({ runtime: rt, coordinatorSessionId: "ses-coord", memberSessions: MEMBERS });
    for (const call of rt.updateSessionCalls) {
      expect(call.permission.webfetch).toBe("ask");
      expect(call.permission.external_directory).toBe("ask");
      expect(call.permission.bash).toBe("ask");
      expect(call.permission.edit).toBe("allow");
    }
  });

  test("Case B: never widens ask/deny from coordinator", async () => {
    const rt = new FakePermRuntime();
    rt.sessionPermission = { edit: "deny", bash: "ask", webfetch: "deny", external_directory: "deny" };
    await propagateAutopermissions({ runtime: rt, coordinatorSessionId: "ses-coord", memberSessions: MEMBERS });
    for (const call of rt.updateSessionCalls) {
      expect(call.permission.edit).toBe("ask"); // deny -> ask, never allow
      expect(call.permission.webfetch).toBe("ask");
    }
  });

  test("Case C: neither visible -> no write, mode unknown", async () => {
    const rt = new FakePermRuntime();
    const result = await propagateAutopermissions({ runtime: rt, coordinatorSessionId: "ses-coord", memberSessions: MEMBERS });
    expect(result.mode).toBe("unknown");
    expect(result.propagated).toBe(false);
    expect(rt.updateSessionCalls.length).toBe(0);
  });

  test("Case B without updateSession surface -> no write, noWriteSurface true", async () => {
    const rt = new FakePermRuntime();
    rt.sessionPermission = { edit: "allow", bash: "ask", webfetch: "ask", external_directory: "ask" };
    rt.hasUpdateSession = false;
    (rt as any).updateSession = undefined;
    const result = await propagateAutopermissions({ runtime: rt, coordinatorSessionId: "ses-coord", memberSessions: MEMBERS });
    expect(result.noWriteSurface).toBe(true);
    expect(result.propagated).toBe(false);
  });
});

describe("permsCountsSummary (Wave 3 per-member counts)", () => {
  test("Case B with skips -> 'N/M updated; K skipped: <reason>'", () => {
    const result = {
      propagated: true,
      mode: "worktree-scoped" as const,
      updated: ["worker1", "worker2"],
      skipped: ["stopped1"],
      skipReasons: { stopped1: "stopped" },
      noWriteSurface: false,
      detail: "",
    };
    expect(permsCountsSummary(result)).toBe("2/3 updated; 1 skipped: stopped");
  });

  test("Case B all updated -> plain count, no skip clause", () => {
    const result = {
      propagated: true,
      mode: "worktree-scoped" as const,
      updated: ["worker1", "worker2"],
      skipped: [] as string[],
      skipReasons: {},
      noWriteSurface: false,
      detail: "",
    };
    expect(permsCountsSummary(result)).toBe("2/2 updated");
  });

  test("multiple distinct skip reasons are enumerated with counts (P3)", () => {
    const result = {
      propagated: true,
      mode: "worktree-scoped" as const,
      updated: ["worker1"],
      skipped: ["stopped1", "coord"],
      skipReasons: { stopped1: "stopped", coord: "coordinator" },
      noWriteSurface: false,
      detail: "",
    };
    expect(permsCountsSummary(result)).toBe("1/3 updated; 2 skipped: stopped (1), coordinator (1)");
  });

  test("duplicate skip reasons are counted, not listed per member (P3)", () => {
    const result = {
      propagated: true,
      mode: "worktree-scoped" as const,
      updated: ["worker1"],
      skipped: ["stopped1", "stopped2", "failed1"],
      skipReasons: { stopped1: "stopped", stopped2: "stopped", failed1: "failed" },
      noWriteSurface: false,
      detail: "",
    };
    expect(permsCountsSummary(result)).toBe("1/4 updated; 3 skipped: stopped (2), failed (1)");
  });

  test("single skip reason keeps the compact label without a count (unchanged format)", () => {
    const result = {
      propagated: true,
      mode: "worktree-scoped" as const,
      updated: ["worker1"],
      skipped: ["stopped1"],
      skipReasons: { stopped1: "stopped" },
      noWriteSurface: false,
      detail: "",
    };
    expect(permsCountsSummary(result)).toBe("1/2 updated; 1 skipped: stopped");
  });

  test("non-Case-B modes do not append counts", () => {
    const inherit = {
      propagated: false,
      mode: "inherit" as const,
      updated: [] as string[],
      skipped: ["worker1"] as string[],
      skipReasons: { worker1: "pull-based inherit" },
      noWriteSurface: false,
      detail: "",
    };
    expect(permsCountsSummary(inherit)).toBe("0/1 updated");
  });
});
