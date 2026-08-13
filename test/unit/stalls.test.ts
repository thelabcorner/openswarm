import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import { StallDiagnoser } from "../../src/supervisor/stalls.ts";
import type { StallDiagnosis, StallHost } from "../../src/supervisor/stalls.ts";
import { FENCE_MARKER } from "../../src/core/fence.ts";

/**
 * Stall auto-diagnosis + escalation ladder tests (task t-stalls):
 *  - per-reason diagnosis (permission-wall / usage-limit / session-silent /
 *    session-absent / lease-stuck / chat-paused / awaiting-mail / working /
 *    stopped / idle) constructed via store + fake runtime;
 *  - usage-limit detection from a live retry-status AND from a delivery error
 *    message (broker revert path);
 *  - the escalation ladder (nudge -> blocker -> release) with a controllable
 *    clock, plus dedup (blocker fires once per member+reason window);
 *  - swarm_stalls tool: report (any member) + ladder (coordinator-only).
 */

let dirs: string[] = [];
let hooks: Hooks | undefined;
let tool: Record<string, any>;

// ==== mutable fake-runtime state (reset per plugin init) ====
let statusData: Record<string, any> = {};
let messagesData: Record<string, any[]> = {};
let promptAsyncError: string | null = null;
const sessions = new Map<string, any>();

const fakeClient = {
  config: {
    providers: async () => ({
      data: {
        providers: [
          { id: "opencode-go", models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash (2x usage)" } } },
        ],
      },
      error: undefined,
    }),
  },
  session: {
    create: async (opts: any) => {
      const id = `ses-stall-${Math.random().toString(36).slice(2, 8)}`;
      const s = { id, title: opts.body?.title, parentID: undefined, directory: "." };
      sessions.set(id, s);
      return { data: s, error: undefined };
    },
    get: async (opts: any) => {
      const s = sessions.get(opts?.path?.id);
      if (!s) return { data: null, error: undefined };
      return { data: { ...s }, error: undefined };
    },
    children: async () => ({ data: [], error: undefined }),
    messages: async (opts: any) => ({ data: messagesData[opts?.path?.id] ?? [], error: undefined }),
    status: async () => ({ data: statusData, error: undefined }),
    abort: async () => ({ data: undefined, error: undefined }),
    update: async () => ({ data: {}, error: undefined }),
    prompt: async () => ({ data: { info: {} }, error: undefined }),
    promptAsync: async () => {
      if (promptAsyncError) throw new Error(promptAsyncError);
      return { data: undefined, error: undefined };
    },
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-stalls" },
  directory: ".",
  worktree: ".",
  experimental_workspace: { register() {} },
  serverUrl: new URL("http://x"),
  $: {},
};

function ctx(sessionID: string): any {
  return {
    sessionID,
    messageID: "msg-call",
    agent: "build",
    directory: ".",
    worktree: ".",
    abort: new AbortController().signal,
    metadata() {},
    ask: () => {},
  };
}

async function initPlugin(): Promise<void> {
  disposeSwarmRuntime();
  const dir = mkdtempSync(join(tmpdir(), "swarms-stalls-"));
  dirs.push(dir);
  statusData = {};
  messagesData = {};
  promptAsyncError = null;
  sessions.clear();
  hooks = await swarmPlugin(pluginInput, { dataDir: dir });
  tool = hooks.tool ?? {};
}

async function runtime(): Promise<any> {
  const mod = await import("../../src/plugin.ts");
  return mod.swarmRuntime();
}

/** Create a swarm + spawn a worker member; returns ids + the runtime. */
async function makeSwarmWithWorker(name: string, workerName = "worker") {
  const coordSession = `ses-stall-lead-${Math.random().toString(36).slice(2, 8)}`;
  const createRes = await tool.swarm_create.execute({ name }, ctx(coordSession));
  const created = JSON.parse(String(createRes.output ?? createRes));
  const swarmId = created.swarm.id as string;
  const spawnRes = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name: workerName, role: "impl" }] },
    ctx(coordSession),
  );
  const spawned = JSON.parse(String(spawnRes.output ?? spawnRes));
  const workerSessionId = spawned.spawned[0].sessionId as string;
  const rt = await runtime();
  const workerMember = await rt.store.getMemberBySessionId(workerSessionId);
  const coordMember = await rt.store.getMemberBySessionId(coordSession);
  return { rt, swarmId, coordSession, workerSessionId, workerMember, coordMember };
}

/** A fake StallHost recording every action it executes, with a controllable
 * clock (fakeNowRef.v). Backed by the real plugin store + runtime adapter. */
function makeStallHost(rt: any, fakeNowRef: { v: number }) {
  const calls: string[] = [];
  const host: StallHost = {
    store: rt.store,
    runtimeAdapter: rt.runtimeAdapter,
    now: () => fakeNowRef.v,
    nudgeMember: async () => { calls.push("nudge"); },
    notifyPermissionWall: async () => { calls.push("permission-notify"); },
    notifyUsageLimit: async () => { calls.push("usage-notify"); },
    notifyProviderError: async () => { calls.push("provider-error-notify"); },
    notifyCoordinatorBlocker: async () => { calls.push("blocker"); },
    releaseMemberTask: async (m) => {
      calls.push("release");
      if (m.currentTaskId) await rt.store.releaseTask(m.currentTaskId, { countAsRetry: false }).catch(() => undefined);
      await rt.store.updateMemberStatus(m.id, "idle", { currentTaskId: null, lastActiveAt: Date.now() }).catch(() => undefined);
      return true;
    },
    respawnMember: async () => { calls.push("respawn"); return "ses-fresh"; },
  };
  return { host, calls };
}

async function reportFor(swarmId: string, sessionID: string): Promise<string> {
  const res = await tool.swarm_stalls.execute({ swarmId, action: "report" }, ctx(sessionID));
  return String(res.output ?? res);
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe("a. per-reason diagnosis (constructed state via store + fake runtime)", () => {
  test("permission-wall: pending prompt -> reason permission-wall + reply recipe (stalled)", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("st-perm");
    await rt.store.insertPendingPermission({
      id: "perm-stall-1",
      swarmId,
      memberId: workerMember.id,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "rm -rf *",
      title: "danger",
      response: null,
      respondedAt: null,
      createdAt: Date.now() - 60_000,
    });
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("verdict: STALLED");
    expect(out).toContain("permission-wall");
    expect(out).toContain("swarm_permissions(swarmId:");
    expect(out).toContain("permissionId: 'perm-stall-1'");
    expect(out).toContain(FENCE_MARKER); // pattern rendered as untrusted data
    expect(out).toContain("causes: permission-wall");
  });

  test("usage-limit: recorded signal -> reason usage-limit + remedy (stalled)", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("st-usage");
    const recorded = await rt.stalls.recordLimitSignal(workerMember.id, "429 rate limit exceeded on deepseek-v4-flash");
    expect(recorded).toBe(true);
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("verdict: STALLED");
    expect(out).toContain("usage-limit");
    expect(out).toContain("USAGE LIMITS");
    expect(out).toContain("429 rate limit exceeded");
    expect(out).toContain("model usage limits");
  });

  test("non-limit signals are NOT recorded (recordLimitSignal false)", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("st-nolimit");
    const recorded = await rt.stalls.recordLimitSignal(workerMember.id, "connection reset by peer");
    expect(recorded).toBe(false);
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).not.toContain("usage-limit");
    expect(out).not.toContain("USAGE LIMITS");
  });

  test("session-silent: working member with old lastActiveAt and no messages -> nudge rung", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember, coordMember } = await makeSwarmWithWorker("st-silent");
    // updateMemberStatus validates currentTaskId references a real task row.
    await rt.core.createTask({ swarmId, id: "t-silent", title: "silent", createdByMemberId: coordMember.id });
    await rt.store.updateMemberStatus(workerMember.id, "working", {
      currentTaskId: "t-silent",
      lastActiveAt: Date.now() - 6 * 60_000,
    });
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("verdict: STALLED");
    expect(out).toContain("session-silent");
    expect(out).toContain("next: nudge");
    expect(out).toContain("swarm_wake(swarmId:");
  });

  test("session-absent: session vanished from the runtime -> respawn action", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("st-absent");
    sessions.delete(workerSessionId); // simulate a crash/deleted session
    // Even an idle member with no backing session is a respawn candidate.
    await rt.store.updateMemberStatus(workerMember.id, "idle", { lastActiveAt: Date.now() - 60_000 });
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("session-absent");
    expect(out).toContain("next: respawn");
    expect(out).toContain("swarm_revive(swarmId:");
  });

  test("lease-stuck: expired claim lease -> release-task", async () => {
    await initPlugin();
    const { rt, swarmId, coordMember, workerMember } = await makeSwarmWithWorker("st-lease");
    const task = await rt.core.createTask({
      swarmId,
      id: "t-lease",
      title: "lease me",
      createdByMemberId: coordMember.id,
    });
    await rt.store.updateTaskStatus(task.id, "ready");
    // 60s lease anchored at REAL now; the diagnoser's fake clock is 2 min later.
    const claimed = await rt.store.claimTask(task.id, workerMember.id, 60_000);
    expect(claimed).toBe(true);
    await rt.store.updateMemberStatus(workerMember.id, "working", {
      currentTaskId: task.id,
      lastActiveAt: Date.now(),
    });
    const fakeNow = { v: Date.now() + 2 * 60_000 };
    const { host } = makeStallHost(rt, fakeNow);
    const diagnoser = new StallDiagnoser(host);
    const d = await diagnoser.diagnose(swarmId);
    const me = d.members.find((x) => x.memberName === "worker");
    expect(me?.reason).toBe("lease-stuck");
    expect(me?.nextAction).toBe("release-task");
    expect(me?.recipe).toContain("t-lease");
  });

  test("chat-paused: user chatting -> legitimate pause, not a stall", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("st-chat");
    await rt.store.updateMemberHumanChat(workerMember.id, Date.now());
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("chat-paused");
    expect(out).toContain("next: none");
    expect(out).toContain("verdict: HEALTHY");
  });

  test("awaiting-mail: idle member with a queued message", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember, coordMember } = await makeSwarmWithWorker("st-mail");
    await rt.store.insertMessages([
      {
        id: `msg-${Math.random().toString(36).slice(2, 8)}`,
        swarmId,
        fromMemberId: coordMember.id,
        to: { type: "member", memberId: workerMember.id },
        kind: "message",
        priority: "normal",
        body: { text: "hello" },
        deliveryState: "queued",
        attemptCount: 0,
        createdAt: Date.now(),
      },
    ]);
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("awaiting-mail");
    expect(out).toContain("next: none");
    expect(out).toContain("verdict: HEALTHY");
  });

  test("working + recent activity -> healthy working", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember, coordMember } = await makeSwarmWithWorker("st-work");
    await rt.core.createTask({ swarmId, id: "t-work", title: "work", createdByMemberId: coordMember.id });
    await rt.store.updateMemberStatus(workerMember.id, "working", {
      currentTaskId: "t-work",
      lastActiveAt: Date.now(),
    });
    messagesData[workerSessionId] = [
      { info: { id: "m1", role: "assistant", time: { created: Date.now() - 1000 } } },
    ];
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("working");
    expect(out).toContain("verdict: HEALTHY");
  });

  test("stopped member is a tombstone, not a stall", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("st-stop");
    await rt.store.updateMemberStatus(workerMember.id, "stopped");
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("stopped");
    expect(out).toContain("verdict: HEALTHY");
  });

  test("fresh swarm with an idle worker -> healthy, no causes", async () => {
    await initPlugin();
    const { swarmId, workerSessionId } = await makeSwarmWithWorker("st-healthy");
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("verdict: HEALTHY");
    expect(out).toContain("idle");
    expect(out).not.toContain("causes:");
  });
});

describe("b. usage-limit detection (retry-status + delivery error)", () => {
  test("live retry-status with a limit-like message is auto-detected and recorded", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("st-retry");
    statusData[workerSessionId] = { type: "retry", attempt: 1, message: "quota exceeded: free tier limit reached", next: 60 };
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("usage-limit");
    expect(out).toContain("session retry");
    // Auto-recorded into the per-swarm in-memory limit map.
    const limits = await rt.stalls.reportLimits(swarmId);
    expect(limits.length).toBe(1);
    expect(limits[0]).toMatchObject({ memberName: "worker", memberId: workerMember.id });
    expect(limits[0].signal).toContain("quota exceeded");
  });

  test("a delivery error message with a limit signal records the limit (broker revert path)", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember, coordMember } = await makeSwarmWithWorker("st-deliverr");
    // Queue a message WITHOUT auto-wake (direct insert), then deliver through
    // the broker with promptAsync throwing a limit error.
    await rt.store.insertMessages([
      {
        id: `msg-${Math.random().toString(36).slice(2, 8)}`,
        swarmId,
        fromMemberId: coordMember.id,
        to: { type: "member", memberId: workerMember.id },
        kind: "message",
        priority: "normal",
        body: { text: "ping" },
        deliveryState: "queued",
        attemptCount: 0,
        createdAt: Date.now(),
      },
    ]);
    promptAsyncError = "provider 429: rate limit hit for model deepseek-v4-flash";
    await expect(rt.broker.deliverToIdleMember(workerMember.id, workerSessionId)).rejects.toThrow();
    promptAsyncError = null;
    const limits = await rt.stalls.reportLimits(swarmId);
    expect(limits.length).toBe(1);
    expect(limits[0].signal).toContain("429");
    // And the diagnosis surfaces it (usage-limit + the remedy).
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("usage-limit");
    expect(out).toContain("model usage limits");
  });

  test("a delivery error WITHOUT a limit signal records nothing", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember, coordMember } = await makeSwarmWithWorker("st-deliverr2");
    await rt.store.insertMessages([
      {
        id: `msg-${Math.random().toString(36).slice(2, 8)}`,
        swarmId,
        fromMemberId: coordMember.id,
        to: { type: "member", memberId: workerMember.id },
        kind: "message",
        priority: "normal",
        body: { text: "ping" },
        deliveryState: "queued",
        attemptCount: 0,
        createdAt: Date.now(),
      },
    ]);
    promptAsyncError = "network timeout";
    await expect(rt.broker.deliverToIdleMember(workerMember.id, workerSessionId)).rejects.toThrow();
    promptAsyncError = null;
    expect(await rt.stalls.reportLimits(swarmId)).toEqual([]);
  });
});

describe("c. escalation ladder (controllable clock) + dedup", () => {
  /** Build a silent member (status working, task claimed with a LONG lease so
   * it never reads lease-stuck, lastActiveAt anchored at T). */
  async function silentWorker(name: string) {
    const { rt, swarmId, coordMember, workerMember, workerSessionId } = await makeSwarmWithWorker(name);
    const task = await rt.core.createTask({
      swarmId,
      id: `t-${name}`,
      title: name,
      createdByMemberId: coordMember.id,
    });
    await rt.store.updateTaskStatus(task.id, "ready");
    const claimed = await rt.store.claimTask(task.id, workerMember.id, 2 * 60 * 60_000);
    expect(claimed).toBe(true);
    const T = Date.now();
    await rt.store.updateMemberStatus(workerMember.id, "working", { currentTaskId: task.id, lastActiveAt: T });
    return { rt, swarmId, workerMember, workerSessionId, T };
  }

  test("ladder advance: nudge -> blocker -> release across time", async () => {
    await initPlugin();
    const { rt, swarmId, T } = await silentWorker("st-ladder");
    const fakeNow = { v: T };
    const { host, calls } = makeStallHost(rt, fakeNow);
    const diagnoser = new StallDiagnoser(host);

    // Rung 1: 6 min silent -> nudge.
    fakeNow.v = T + 6 * 60_000;
    await diagnoser.executeNext(swarmId);
    expect(calls).toEqual(["nudge"]);

    // Rung 3: 16 min silent -> coordinator blocker.
    fakeNow.v = T + 16 * 60_000;
    await diagnoser.executeNext(swarmId);
    expect(calls).toEqual(["nudge", "blocker"]);

    // Rung 4: 21 min silent -> release-task.
    fakeNow.v = T + 21 * 60_000;
    await diagnoser.executeNext(swarmId);
    expect(calls).toEqual(["nudge", "blocker", "release"]);
  });

  test("escalationState reports the time-derived rung", async () => {
    await initPlugin();
    const { rt, workerMember, T } = await silentWorker("st-rung");
    const fakeNow = { v: T + 16 * 60_000 };
    const { host } = makeStallHost(rt, fakeNow);
    const diagnoser = new StallDiagnoser(host);
    const { rung, diagnosis } = await diagnoser.escalationState(workerMember.id);
    expect(rung).toBe(3);
    expect(diagnosis.reason).toBe("session-silent");
    expect(diagnosis.nextAction).toBe("coordinator-blocker");
  });

  test("dedup: the blocker fires once per member+reason window (repeat sweeps)", async () => {
    await initPlugin();
    const { rt, swarmId, T } = await silentWorker("st-dedup");
    const fakeNow = { v: T + 16 * 60_000 };
    const { host, calls } = makeStallHost(rt, fakeNow);
    const diagnoser = new StallDiagnoser(host);
    await diagnoser.executeNext(swarmId);
    await diagnoser.executeNext(swarmId);
    await diagnoser.executeNext(swarmId);
    expect(calls).toEqual(["blocker"]);
  });

  test("dedup: permission-notify fires once per window (permission-wall member)", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember, T } = await silentWorker("st-pdedup");
    await rt.store.insertPendingPermission({
      id: "perm-pdedup-1",
      swarmId,
      memberId: workerMember.id,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "*",
      response: null,
      respondedAt: null,
      createdAt: T,
    });
    const fakeNow = { v: T + 60_000 };
    const { host, calls } = makeStallHost(rt, fakeNow);
    const diagnoser = new StallDiagnoser(host);
    await diagnoser.executeNext(swarmId);
    await diagnoser.executeNext(swarmId);
    expect(calls).toEqual(["permission-notify"]);
  });

  test("forceAdvance walks the reason's action sequence one rung per call", async () => {
    await initPlugin();
    const { rt, swarmId, workerMember } = await silentWorker("st-force");
    const fakeNow = { v: Date.now() + 6 * 60_000 };
    const { host, calls } = makeStallHost(rt, fakeNow);
    const diagnoser = new StallDiagnoser(host);

    const first: StallDiagnosis = await diagnoser.forceAdvance(swarmId, "worker");
    expect(first.nextAction).toBe("nudge");
    const second = await diagnoser.forceAdvance(swarmId, "worker");
    expect(second.nextAction).toBe("coordinator-blocker");
    const third = await diagnoser.forceAdvance(swarmId, "worker");
    expect(third.nextAction).toBe("release-task");
    expect(calls).toEqual(["nudge", "blocker", "release"]);
    void workerMember;
  });

  test("forceAdvance on a healthy member is a no-op", async () => {
    await initPlugin();
    const { rt, swarmId, workerMember } = await makeSwarmWithWorker("st-force-ok");
    const fakeNow = { v: Date.now() };
    const { host } = makeStallHost(rt, fakeNow);
    const diagnoser = new StallDiagnoser(host);
    const d = await diagnoser.forceAdvance(swarmId, "worker");
    expect(d.nextAction).toBe("none");
    expect(d.reason).toBe("idle");
    void workerMember;
  });
});

describe("d. swarm_stalls tool", () => {
  test("report works for ANY member (read-only surface)", async () => {
    await initPlugin();
    const { swarmId, workerSessionId } = await makeSwarmWithWorker("st-tool-report");
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("STALL REPORT");
    expect(out).toContain("verdict: HEALTHY");
    expect(out).toContain("NEXT:");
  });

  test("ladder as a WORKER is rejected (coordinator-only)", async () => {
    await initPlugin();
    const { swarmId, workerSessionId } = await makeSwarmWithWorker("st-tool-worker");
    const res = await tool.swarm_stalls.execute(
      { swarmId, action: "ladder", member: "worker" },
      ctx(workerSessionId),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("coordinator-only");
  });

  test("ladder as the COORDINATOR advances a silent member one rung (and the next call advances again)", async () => {
    await initPlugin();
    const { rt, swarmId, coordSession, workerMember, coordMember } = await makeSwarmWithWorker("st-tool-ladder");
    await rt.core.createTask({ swarmId, id: "t-ladder-tool", title: "ladder tool", createdByMemberId: coordMember.id });
    const T = Date.now();
    await rt.store.updateMemberStatus(workerMember.id, "working", {
      currentTaskId: "t-ladder-tool",
      lastActiveAt: T - 6 * 60_000,
    });

    const res = await tool.swarm_stalls.execute(
      { swarmId, action: "ladder", member: "worker" },
      ctx(coordSession),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("advanced 'worker' one rung");
    expect(out).toContain("session-silent");
    expect(out).toContain("nudge");

    // Second call advances to the blocker rung.
    const res2 = await tool.swarm_stalls.execute(
      { swarmId, action: "ladder", member: "worker" },
      ctx(coordSession),
    );
    expect(String(res2.output ?? res2)).toContain("coordinator-blocker");
  });

  test("ladder on a non-stalled member reports nothing to advance", async () => {
    await initPlugin();
    const { swarmId, coordSession } = await makeSwarmWithWorker("st-tool-ok");
    const res = await tool.swarm_stalls.execute(
      { swarmId, action: "ladder", member: "worker" },
      ctx(coordSession),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("not stalled");
    expect(out).toContain("nothing to advance");
  });

  test("report surfaces recorded usage limits with the remedy", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("st-tool-limits");
    await rt.stalls.recordLimitSignal(workerMember.id, "billing quota reached");
    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("USAGE LIMITS");
    expect(out).toContain("billing quota reached");
    expect(out).toContain("swarm_revive");
  });

  test("report on an unknown swarm is a clear error", async () => {
    await initPlugin();
    const { workerSessionId } = await makeSwarmWithWorker("st-tool-unknown");
    // resolveSwarmId throws before the tool body (id-or-name resolution).
    await expect(
      tool.swarm_stalls.execute({ swarmId: "swarm-nope", action: "report" }, ctx(workerSessionId)),
    ).rejects.toThrow(/no swarm found/);
  });
});
