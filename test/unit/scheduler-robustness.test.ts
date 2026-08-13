import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, handleOpenCodeEvent, disposeSwarmRuntime } from "../../src/plugin.ts";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { Recovery } from "../../src/supervisor/recovery.ts";
import type { SwarmPluginRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { Permission } from "@opencode-ai/sdk";
import type { AgentRuntime } from "../../src/runtime/runtime-types.ts";
import type { NewSwarm } from "../../src/storage/models.ts";

/**
 * Session-failure robustness tests (t-sched-robustness). The ANVIL incident:
 * every member failed with 'failed: [object Object]' (unstringified error
 * object in the coordinator notice), tasks were released but members stayed
 * taskless-'working' limbo (a READY task sat unclaimed), and a taskless member
 * probing temp dirs flooded the coordinator with [PERMISSION ALLOWED]
 * advisories.
 *
 *  (a) session.error with an OBJECT error payload -> the coordinator notice
 *      contains a readable message, never '[object Object]';
 *  (b) session-error releases never consume the retry budget — repeated
 *      session errors never fail the task, while GENUINE task-level failures
 *      (default releaseTask) still do at maxRetriesPerTask;
 *  (c) after a session error + release the member is IDLE (not working/failed
 *      limbo) and the next scheduler pass assigns the ready task right back;
 *  (d) recovery's respawn of a member with NO task returns it IDLE so the
 *      scheduler engages (a respawned member WITH a task stays working);
 *  (e) advisory flood cap: N asks in the window -> exactly 1 [PERMISSION
 *      ALLOWED] advisory per member; per-swarm cap 3/5min; the window resets.
 */

let dirs: string[] = [];
let hooks: Hooks;
let tool: Record<string, any>;

// ==== mutable fake-runtime state (reset per plugin init) ====
const sessions = new Map<string, any>();
let messagesData: Record<string, any[]> = {};
const promptCalls: Array<{ sessionID: string; text: string }> = [];

const fakeClient = {
  config: {
    providers: async () => ({
      data: {
        providers: [
          { id: "opencode-go", models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash" } } },
        ],
      },
      error: undefined,
    }),
  },
  session: {
    create: async (opts: any) => {
      const id = `ses-sr-${Math.random().toString(36).slice(2, 8)}`;
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
    status: async () => ({ data: {}, error: undefined }),
    abort: async () => ({ data: undefined, error: undefined }),
    update: async () => ({ data: {}, error: undefined }),
    prompt: async () => ({ data: { info: {} }, error: undefined }),
    promptAsync: async (opts: any) => {
      const text = (opts?.body?.parts ?? [])
        .map((p: any) => (typeof p?.text === "string" ? p.text : ""))
        .join("\n");
      promptCalls.push({ sessionID: opts?.path?.id ?? "?", text });
      return { data: undefined, error: undefined };
    },
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-sr" },
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

function permission(input: Pick<Permission, "id" | "type" | "pattern" | "sessionID" | "title">): Permission {
  return {
    ...input,
    messageID: `msg-${input.id}`,
    metadata: {},
    time: { created: Date.now() },
  };
}

function askOutput(): { status: "ask" | "deny" | "allow" } {
  return { status: "ask" };
}

async function initPlugin(allowAllMemberPermissions = false): Promise<void> {
  disposeSwarmRuntime();
  const dir = mkdtempSync(join(tmpdir(), "swarms-sr-"));
  dirs.push(dir);
  sessions.clear();
  messagesData = {};
  promptCalls.length = 0;
  hooks = await swarmPlugin(pluginInput, { dataDir: dir, allowAllMemberPermissions });
  tool = hooks.tool ?? {};
}

async function runtime(): Promise<SwarmPluginRuntime> {
  const mod = await import("../../src/plugin.ts");
  const rt = mod.swarmRuntime();
  if (!rt) throw new Error("no swarm runtime initialized");
  return rt;
}

/** Create a swarm (+ optional policies) and spawn N worker members. */
async function makeSwarmWithWorkers(name: string, count = 1, policies: Record<string, unknown> = {}) {
  const coordSession = `ses-sr-lead-${Math.random().toString(36).slice(2, 8)}`;
  const createRes = await tool.swarm_create.execute({ name, policies }, ctx(coordSession));
  const created = JSON.parse(String(createRes.output ?? createRes));
  const swarmId = created.swarm.id as string;
  const members = Array.from({ length: count }, (_, i) => ({ name: `w${i}`, role: "impl" }));
  const spawnRes = await tool.swarm_spawn.execute({ swarmId, members }, ctx(coordSession));
  const spawned = JSON.parse(String(spawnRes.output ?? spawnRes));
  const workerSessions = spawned.spawned.map((s: any) => s.sessionId as string);
  const rt = await runtime();
  const workerMembers = await Promise.all(workerSessions.map((sid: string) => rt.store.getMemberBySessionId(sid)));
  return { rt, swarmId, coordSession, workerSessions, workerMembers };
}

async function coordinatorOf(swarmId: string): Promise<{ id: string }> {
  const rt = await runtime();
  const members = await rt.store.listMembers(swarmId);
  const coord = members.find((m) => m.role === "coordinator")!;
  return { id: coord.id };
}

/** Insert a ready task claimed by the given member (the in-flight shape). */
async function claimTaskFor(rt: SwarmPluginRuntime, swarmId: string, taskId: string, memberId: string, title: string) {
  const coord = await coordinatorOf(swarmId);
  await rt.store.insertTask({
    id: taskId, swarmId, title, status: "ready", priority: 0,
    createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
  });
  await rt.store.claimTask(taskId, memberId, 60_000);
  await rt.store.updateTaskStatus(taskId, "working");
  await rt.store.updateMemberStatus(memberId, "working", { currentTaskId: taskId, lastActiveAt: Date.now() });
}

/** Coordinator advisory DIGEST lines for a swarm (t-flood-aggregate: advisories
 * are delivered as lines of the debounced coordinator digest instead of
 * mailbox findings). Forces a flush, then reads the coordinator session's
 * promptAsync turns for the marker — counting digest LINES, not turns. */
async function advisoryDigestLines(rt: SwarmPluginRuntime, swarmId: string, coordSession: string, marker: string): Promise<string[]> {
  await rt.notices.flush(swarmId);
  return promptCalls
    .filter((c) => c.sessionID === coordSession)
    .flatMap((c) => c.text.split("\n"))
    .filter((l) => l.includes(marker));
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) { try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

describe("t-sched-robustness (a) — [object Object] notice normalization", () => {
  test("session.error with an OBJECT error payload -> coordinator notice readable, never '[object Object]'", async () => {
    await initPlugin();
    const { rt, swarmId, coordSession, workerSessions, workerMembers } = await makeSwarmWithWorkers("sr-a", 1, { noticeFlushMs: 50 });
    const workerSession = workerSessions[0]!;
    await claimTaskFor(rt, swarmId, "TA-obj", workerMembers[0]!.id, "object error task");

    await handleOpenCodeEvent(rt, {
      type: "session.error",
      properties: { sessionID: workerSession, error: { name: "ProviderError", message: "upstream timeout" } },
    } as never);

    // The coordinator notice is debounced into the notice aggregator's digest
    // (t-flood-aggregate; flush window set to 50ms via policies above).
    await new Promise((r) => setTimeout(r, 300));
    const coordNotices = promptCalls.filter((c) => c.sessionID === coordSession);
    expect(coordNotices.length).toBeGreaterThan(0);
    const notice = coordNotices.map((c) => c.text).join("\n");
    expect(notice).toContain("failed");
    expect(notice).toContain("upstream timeout");
    expect(notice).not.toContain("[object Object]");
    // The task release ran with no retry-budget consumption.
    const task = (await rt.store.listTasks(swarmId)).find((t) => t.id === "TA-obj");
    expect(task?.retryCount).toBe(0);
  });

  test("session.error with a STRING error payload still renders normally", async () => {
    await initPlugin();
    const { rt, swarmId, coordSession, workerSessions } = await makeSwarmWithWorkers("sr-a2", 1, { noticeFlushMs: 50 });
    await handleOpenCodeEvent(rt, {
      type: "session.error",
      properties: { sessionID: workerSessions[0]!, error: "rate limited" },
    } as never);
    await new Promise((r) => setTimeout(r, 300));
    const notice = promptCalls.filter((c) => c.sessionID === coordSession).map((c) => c.text).join("\n");
    expect(notice).toContain("rate limited");
    expect(notice).not.toContain("[object Object]");
    void swarmId;
  });
});

describe("t-sched-robustness (b) — session-error releases do NOT burn the retry budget", () => {
  test("repeated session errors never fail the task; genuine task failures still do", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessions, workerMembers } = await makeSwarmWithWorkers("sr-b", 1, { maxRetriesPerTask: 1 });
    const workerSession = workerSessions[0]!;
    const workerId = workerMembers[0]!.id;
    await rt.store.insertTask({
      id: "TB-sess", swarmId, title: "session bounce", status: "ready", priority: 0,
      createdByMemberId: (await coordinatorOf(swarmId)).id, createdAt: Date.now(), updatedAt: Date.now(),
    });

    // Churn: session.error -> release (countAsRetry:false) -> member idle ->
    // re-claim -> repeat. With maxRetriesPerTask=1 the task must NEVER fail.
    for (let i = 0; i < 4; i++) {
      const m = (await rt.store.getMemberById(workerId))!;
      expect(await rt.store.claimTask("TB-sess", m.id, 60_000)).toBe(true);
      await rt.store.updateTaskStatus("TB-sess", "working");
      await rt.store.updateMemberStatus(m.id, "working", { currentTaskId: "TB-sess", lastActiveAt: Date.now() });
      await rt.supervisor.onOpenCodeEvent({
        type: "session.error",
        properties: { sessionID: workerSession, error: "upstream outage" },
      } as never);
      const after = (await rt.store.listTasks(swarmId)).find((t) => t.id === "TB-sess");
      expect(after?.status).toBe("ready");
      expect(after?.retryCount).toBe(0); // retry budget NEVER moves on session errors
      const mAfter = await rt.store.getMemberById(workerId);
      expect(mAfter?.status).toBe("idle");
      expect(mAfter?.currentTaskId).toBeUndefined();
    }
    const swarm = await rt.store.getSwarm(swarmId);
    const r1 = await rt.scheduler.run(swarm!);
    expect(r1.failedExceededRetries).not.toContain("TB-sess");
    expect((await rt.store.listTasks(swarmId)).find((t) => t.id === "TB-sess")?.status).not.toBe("failed");

    // The scheduler pass above re-assigned the ready task to the (idle) member.
    // Free it again — countAsRetry:false, so still no budget burn — and idle
    // the member before the genuine-failure churn.
    const m1 = (await rt.store.getMemberById(workerId))!;
    if (m1.currentTaskId) {
      await rt.store.releaseTask(m1.currentTaskId, { countAsRetry: false });
      await rt.store.updateMemberStatus(workerId, "idle", { currentTaskId: null, lastActiveAt: Date.now() });
    }

    // GENUINE task-level failures (default releaseTask = counts as retry) still
    // consume budget: 2 releases with maxRetries=1 -> retryCount 2 > 1 -> fail.
    const m2 = (await rt.store.getMemberById(workerId))!;
    expect(await rt.store.claimTask("TB-sess", m2.id, 60_000)).toBe(true);
    await rt.store.updateTaskStatus("TB-sess", "working");
    expect(await rt.store.releaseTask("TB-sess")).toBe(true);
    expect(await rt.store.claimTask("TB-sess", m2.id, 60_000)).toBe(true);
    await rt.store.updateTaskStatus("TB-sess", "working");
    expect(await rt.store.releaseTask("TB-sess")).toBe(true);
    const t2 = (await rt.store.listTasks(swarmId)).find((t) => t.id === "TB-sess");
    expect(t2?.retryCount).toBe(2);
    await rt.scheduler.run(swarm!);
    expect((await rt.store.listTasks(swarmId)).find((t) => t.id === "TB-sess")?.status).toBe("failed");
  });
});

describe("t-sched-robustness (c) — no working-limbo after a session failure", () => {
  test("after session error + release the member is IDLE and the next scheduler pass re-assigns the ready task", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessions, workerMembers } = await makeSwarmWithWorkers("sr-c");
    const workerSession = workerSessions[0]!;
    const workerId = workerMembers[0]!.id;
    await claimTaskFor(rt, swarmId, "TC-1", workerId, "ready task");

    const effects = await rt.supervisor.onOpenCodeEvent({
      type: "session.error",
      properties: { sessionID: workerSession, error: "boom" },
    } as never);
    expect(effects.notifyCoordinator).toBe(true);
    expect(effects.releasedTaskIds).toContain("TC-1");

    // NO working-limbo: the member is IDLE (not 'working' with no task, not
    // 'failed' which is non-resumable).
    const m = await rt.store.getMemberById(workerId);
    expect(m?.status).toBe("idle");
    expect(m?.currentTaskId).toBeUndefined();
    const task = (await rt.store.listTasks(swarmId)).find((t) => t.id === "TC-1");
    expect(task?.status).toBe("ready");
    expect(task?.ownerMemberId).toBeUndefined();
    expect(task?.retryCount).toBe(0);

    // The next scheduler pass assigns the ready task right back to the member.
    const swarm = await rt.store.getSwarm(swarmId);
    const res = await rt.scheduler.run(swarm!);
    expect(res.assigned.some((a) => a.taskId === "TC-1")).toBe(true);
    const t2 = (await rt.store.listTasks(swarmId)).find((t) => t.id === "TC-1");
    expect(t2?.status).toBe("working");
    expect(t2?.ownerMemberId).toBe(workerId);
  });
});

describe("t-sched-robustness (d) — respawned taskless member returns IDLE", () => {
  function newSwarm(name: string): NewSwarm {
    return {
      id: `swarm-sr-${name}`,
      projectId: "test-project",
      name,
      coordinatorSessionId: `ses-coord-${name}`,
      coordinatorMemberId: `mem-coord-${name}`,
      directory: ".",
      status: "active",
      policies: {
        maxMembers: 8,
        maxConcurrentMembers: 5,
        allowMemberSpawn: false,
        maxSpawnDepth: 1,
        coordinatorMode: "normal",
        defaultWorkspace: "worktree",
        messageDelivery: "idle",
        autoWake: true,
        autoReview: false,
        abortChildrenOnSwarmStop: true,
        maxRetriesPerTask: 2,
        retention: "project",
      },
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
  }

  const fakeRuntime: AgentRuntime = {
    kind: "fake",
    createSession: async () => ({ id: "ses-respawned", title: "", directory: "." }),
    getSession: async () => null, // absent — triggers the respawn branch
    listChildren: async () => [],
    prompt: async () => { throw new Error("unused"); },
    promptAsync: async () => {},
    abort: async () => {},
    getStatus: async () => null,
    getMessages: async () => [],
  };

  test("recovery respawn of a member with NO task -> status idle (scheduler-engageable)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "swarms-sr-d1-"));
    const store = new SQLiteStore(join(dir, "store.db"));
    await store.ready();
    try {
      const sid = newSwarm("d1").id;
      await store.insertSwarm(newSwarm("d1"));
      // Member marked working but with NO currentTaskId (its task was released
      // by a session failure before the restart).
      await store.insertMember({
        id: "mem-sr-d1", swarmId: sid, name: "a", role: "worker", sessionId: "ses-gone",
        status: "working", workspaceMode: "worktree", createdAt: Date.now(), updatedAt: Date.now(),
      });
      const recovery = new Recovery(store, fakeRuntime, async () => "ses-respawned");
      const res = await recovery.reconcileSwarm(sid);
      const action = res.actions.find((a) => a.memberId === "mem-sr-d1");
      expect(action?.action).toBe("respawned");
      const m = await store.getMemberById("mem-sr-d1");
      expect(m?.sessionId).toBe("ses-respawned");
      // IDLE, not 'working' — the scheduler must be able to assign it work.
      expect(m?.status).toBe("idle");
      expect(m?.currentTaskId).toBeUndefined();
    } finally {
      await store.close();
      try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  test("recovery respawn of a member WITH a task -> status working (task kept)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "swarms-sr-d2-"));
    const store = new SQLiteStore(join(dir, "store.db"));
    await store.ready();
    try {
      const sid = newSwarm("d2").id;
      await store.insertSwarm(newSwarm("d2"));
      await store.insertMember({
        id: "mem-sr-d2", swarmId: sid, name: "a", role: "worker", sessionId: "ses-gone",
        status: "working", currentTaskId: "T-KEPT", workspaceMode: "worktree",
        createdAt: Date.now(), updatedAt: Date.now(),
      });
      await store.insertTask({
        id: "T-KEPT", swarmId: sid, title: "kept", status: "working", priority: 0,
        createdByMemberId: "mem-coord-sr-d2", createdAt: Date.now(), updatedAt: Date.now(),
      });
      const recovery = new Recovery(store, fakeRuntime, async () => "ses-respawned");
      const res = await recovery.reconcileSwarm(sid);
      expect(res.actions.find((a) => a.memberId === "mem-sr-d2")?.action).toBe("respawned");
      const m = await store.getMemberById("mem-sr-d2");
      expect(m?.status).toBe("working");
      expect(m?.currentTaskId).toBe("T-KEPT");
    } finally {
      await store.close();
      try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });
});

describe("t-sched-robustness (e) — advisory flood cap", () => {
  test("N asks in the window -> exactly 1 advisory per member; per-swarm cap 3; window resets", async () => {
    await initPlugin(true); // allowAllMemberPermissions so high-risk asks auto-allow
    const { rt, swarmId, coordSession, workerSessions, workerMembers } = await makeSwarmWithWorkers("sr-e", 4);
    const [sa, sb, sc, sd] = workerSessions;
    const [ma, mb, mc, md] = workerMembers;

    // N asks from member 'a' in the window -> exactly ONE advisory.
    for (let i = 0; i < 5; i++) {
      const out = askOutput();
      await hooks["permission.ask"]!(
        permission({ id: `pa-${i}`, type: "bash", pattern: "C:/elsewhere/*", sessionID: sa, title: "run" }),
        out,
      );
      expect(out.status).toBe("allow"); // the CAP suppresses the notice, never the allow
    }
    // The allowed advisory lands as ONE [PERMISSION ALLOWED] digest line.
    let advisoryLines = await advisoryDigestLines(rt, swarmId, coordSession, "[PERMISSION ALLOWED]");
    expect(advisoryLines.length).toBe(1);

    // Per-swarm cap: members b and c each get their own 1 (swarm total 3);
    // member d's first ask is SUPPRESSED (swarm cap 3/5min).
    for (const [sid, mid] of [[sb, mb], [sc, mc], [sd, md]] as const) {
      const out = askOutput();
      await hooks["permission.ask"]!(
        permission({ id: `p-${mid!.id}`, type: "bash", pattern: "C:/elsewhere/*", sessionID: sid, title: "run" }),
        out,
      );
      expect(out.status).toBe("allow");
    }
    advisoryLines = await advisoryDigestLines(rt, swarmId, coordSession, "[PERMISSION ALLOWED]");
    expect(advisoryLines.length).toBe(3);

    // Member 'a' again (new id) — still suppressed by the per-member cap.
    const again = askOutput();
    await hooks["permission.ask"]!(
      permission({ id: "pa-again", type: "bash", pattern: "C:/elsewhere/*", sessionID: sa, title: "run" }),
      again,
    );
    expect(again.status).toBe("allow");
    advisoryLines = await advisoryDigestLines(rt, swarmId, coordSession, "[PERMISSION ALLOWED]");
    expect(advisoryLines.length).toBe(3);

    // Window reset: clear the in-memory caps (a new 5-min window) -> member 'a'
    // may notify again, and the swarm counter restarts.
    (rt as any).advisoryMemberLastAt.clear();
    (rt as any).advisorySwarmCount.clear();
    const afterReset = askOutput();
    await hooks["permission.ask"]!(
      permission({ id: "pa-after-reset", type: "bash", pattern: "C:/elsewhere/*", sessionID: sa, title: "run" }),
      afterReset,
    );
    expect(afterReset.status).toBe("allow");
    advisoryLines = await advisoryDigestLines(rt, swarmId, coordSession, "[PERMISSION ALLOWED]");
    expect(advisoryLines.length).toBe(4);
  });

  test("[CHAT FAILURE] advisories are capped the same way (1 per member per window)", async () => {
    await initPlugin();
    const { rt, swarmId, coordSession, workerSessions } = await makeSwarmWithWorkers("sr-e2", 1);
    const workerSession = workerSessions[0]!;
    const member = await rt.store.getMemberBySessionId(workerSession);
    if (!member) throw new Error("worker not found");

    // Two DISTINCT failure classes for the same member, back-to-back: the
    // per-(memberId,subtype) dedup would allow both, but the flood cap allows
    // only ONE advisory per member per window.
    await rt.notifyProviderError(member, {
      memberName: member.name, role: member.role, status: member.status,
      reason: "provider-error", subtype: "auth", stallMs: 0,
      evidence: ["chat failure: auth — invalid bearer"], nextAction: "provider-error-notify", recipe: "check credentials",
    });
    await rt.notifyProviderError(member, {
      memberName: member.name, role: member.role, status: member.status,
      reason: "provider-error", subtype: "context", stallMs: 0,
      evidence: ["chat failure: context — too long"], nextAction: "provider-error-notify", recipe: "compact",
    });
    // Exactly ONE [CHAT FAILURE] digest line (the second is suppressed and
    // counted into '+N suppressed' by the aggregator).
    const chatFailureLines = await advisoryDigestLines(rt, swarmId, coordSession, "[CHAT FAILURE]");
    expect(chatFailureLines.length).toBe(1);
    expect(chatFailureLines[0]).toContain("hit auth");
  });
});
