import { describe, expect, test, afterAll, beforeAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { ChunkDbStore } from "../../src/storage/chunkdb-store.ts";
import { Scheduler } from "../../src/scheduler/scheduler.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { SwarmPluginRuntime } from "../../src/plugin.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";
import type { NewSwarm, NewSwarmMember, NewTask } from "../../src/storage/models.ts";

/**
 * Watchdog / retry-budget regression tests (t-sched-watchdog):
 *  (a) watchdog releases do NOT burn the retry budget — churning a silent
 *      member never fails the task (a stall is not the task's fault);
 *  (b) a GENUINE failure (default releaseTask = counts as retry) still
 *      consumes budget and fails the task at maxRetriesPerTask;
 *  (c) an ABSENT session (getSession → null) is respawned — the task stays
 *      with the member (re-claimed + re-prompted); respawn FAILURE → released
 *      with countAsRetry:false;
 *  (d) policy watchdogSilenceMs override is respected (a tiny value fires the
 *      watchdog quickly where the default would not);
 *  (e) swarm_tasks action 'retry': failed task → ready + retryCount 0 +
 *      re-assignable; a worker cannot retry (coordinator-only); a cancelled
 *      task is also recoverable; optional member reservation;
 *  (f) both store backends (sqlite + chunkdb) implement resetTaskForRetry.
 */

// ==== store-level helpers (shared by the backend-pair tests) ====

function newSwarm(name: string, over: Record<string, unknown> = {}): NewSwarm {
  return {
    id: `swarm-${name}`,
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
      ...over,
    },
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
}

function newMember(swarmId: string, name: string, sessionId: string): NewSwarmMember {
  return {
    id: `mem-${swarmId}-${name}`,
    swarmId,
    name,
    role: "worker",
    sessionId,
    status: "idle",
    workspaceMode: "worktree",
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
}

function newTask(swarmId: string, id: string, over: Partial<NewTask> = {}): NewTask {
  return {
    id,
    swarmId,
    title: id,
    status: "ready",
    priority: 0,
    createdByMemberId: `mem-coord-${swarmId}`,
    createdAt: Date.now(),
    updatedAt: Date.now(),
    ...over,
  };
}

// ==== (f) both store backends: resetTaskForRetry + countAsRetry semantics ====

for (const backend of ["sqlite", "chunkdb"] as const) {
  describe(`store backend: ${backend} — retry-budget primitives`, () => {
    let dir: string;
    let store: SQLiteStore | ChunkDbStore;
    let scheduler: Scheduler;
    let runtime: AgentRuntime;

    beforeAll(async () => {
      dir = mkdtempSync(join(tmpdir(), `swarms-wd-${backend}-`));
      store = backend === "sqlite"
        ? new SQLiteStore(join(dir, "store.db"))
        : new ChunkDbStore(join(dir, "store.chunkdb"));
      await store.ready();
      runtime = {
        kind: "fake",
        createSession: async () => ({ id: "s", title: "", directory: "." }),
        getSession: async () => null,
        listChildren: async () => [],
        prompt: async () => {
          throw new Error("unused");
        },
        promptAsync: async () => {},
        abort: async () => {},
        getStatus: async () => null,
        getMessages: async () => [],
      };
      scheduler = new Scheduler(store, runtime);
    });

    afterAll(async () => {
      await store.close();
      try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
    });

    test("resetTaskForRetry: failed -> ready, retryCount 0, owner/lease/reservation cleared", async () => {
      const sid = `swarm-${backend}-rt1`;
      await store.insertSwarm(newSwarm(`${backend}-rt1`));
      await store.insertMember(newMember(sid, "a", "ses-a"));
      await store.insertTask(newTask(sid, "T1", { status: "failed", retryCount: 3, claimedAt: 1, leaseExpiresAt: 2 }));
      await store.setTaskReservation("T1", "a");
      const ok = await store.resetTaskForRetry("T1");
      expect(ok).toBe(true);
      const t = (await store.listTasks(sid)).find((x) => x.id === "T1");
      expect(t?.status).toBe("ready");
      expect(t?.retryCount).toBe(0);
      expect(t?.ownerMemberId).toBeUndefined();
      expect(t?.claimedAt).toBeUndefined();
      expect(t?.leaseExpiresAt).toBeUndefined();
      expect(t?.reservedFor).toBeUndefined();
      // Id + DAG identity preserved.
      expect(t?.id).toBe("T1");
      // Re-assignable: scheduler claims it for an idle member.
      await store.updateMemberStatus((await store.listMembers(sid))[0]!.id, "idle", { currentTaskId: null });
      const swarm = await store.getSwarm(sid);
      const r = await scheduler.run(swarm!);
      expect(r.failedExceededRetries).not.toContain("T1");
      expect((await store.listTasks(sid)).find((x) => x.id === "T1")?.status).toBe("working");
    });

    test("resetTaskForRetry: cancelled -> ready; ready/working/completed are NOT recoverable", async () => {
      const sid = `swarm-${backend}-rt2`;
      await store.insertSwarm(newSwarm(`${backend}-rt2`));
      await store.insertTask(newTask(sid, "C1", { status: "cancelled", retryCount: 2 }));
      expect(await store.resetTaskForRetry("C1")).toBe(true);
      expect((await store.listTasks(sid)).find((x) => x.id === "C1")?.status).toBe("ready");
      expect((await store.listTasks(sid)).find((x) => x.id === "C1")?.retryCount).toBe(0);
      // Not recoverable states: ready, working, completed (and missing).
      await store.insertTask(newTask(sid, "W1", { status: "working" }));
      await store.insertTask(newTask(sid, "R1", { status: "ready" }));
      await store.insertTask(newTask(sid, "D1", { status: "completed" }));
      expect(await store.resetTaskForRetry("W1")).toBe(false);
      expect(await store.resetTaskForRetry("R1")).toBe(false);
      expect(await store.resetTaskForRetry("D1")).toBe(false);
      expect(await store.resetTaskForRetry("MISSING")).toBe(false);
    });

    test("releaseTask({countAsRetry:false}) never increments retryCount (watchdog churn semantics)", async () => {
      const sid = `swarm-${backend}-rt3`;
      await store.insertSwarm(newSwarm(`${backend}-rt3`, { maxRetriesPerTask: 2 }));
      await store.insertMember(newMember(sid, "a", "ses-a"));
      await store.insertTask(newTask(sid, "CH"));
      // Churn: claim -> working -> watchdog-style release (countAsRetry:false),
      // re-claim, repeat. The retry budget must never move.
      for (let i = 0; i < 5; i++) {
        const m = (await store.listMembers(sid))[0]!;
        expect(await store.claimTask("CH", m.id, 60_000)).toBe(true);
        await store.updateTaskStatus("CH", "working");
        expect(await store.releaseTask("CH", { countAsRetry: false })).toBe(true);
      }
      const t = (await store.listTasks(sid)).find((x) => x.id === "CH");
      expect(t?.retryCount).toBe(0);
      expect(t?.status).toBe("ready");
      // maxRetries=2 with retryCount=0: the scheduler keeps it alive.
      const swarm = await store.getSwarm(sid);
      const r = await scheduler.run(swarm!);
      expect(r.failedExceededRetries).not.toContain("CH");
      expect((await store.listTasks(sid)).find((x) => x.id === "CH")?.status).not.toBe("failed");
    });

    test("default releaseTask (genuine failure) consumes budget and fails at maxRetriesPerTask", async () => {
      const sid = `swarm-${backend}-rt4`;
      await store.insertSwarm(newSwarm(`${backend}-rt4`, { maxRetriesPerTask: 2 }));
      await store.insertMember(newMember(sid, "a", "ses-a"));
      await store.insertTask(newTask(sid, "GEN"));
      // Three genuine releases -> retryCount 3 > maxRetries 2 -> scheduler fails.
      for (let i = 0; i < 3; i++) {
        const m = (await store.listMembers(sid))[0]!;
        expect(await store.claimTask("GEN", m.id, 60_000)).toBe(true);
        await store.updateTaskStatus("GEN", "working");
        expect(await store.releaseTask("GEN")).toBe(true);
      }
      const t = (await store.listTasks(sid)).find((x) => x.id === "GEN");
      expect(t?.retryCount).toBe(3);
      const swarm = await store.getSwarm(sid);
      const r = await scheduler.run(swarm!);
      expect(r.failedExceededRetries).toContain("GEN");
      expect((await store.listTasks(sid)).find((x) => x.id === "GEN")?.status).toBe("failed");
      expect((await store.listTasks(sid)).find((x) => x.id === "GEN")?.ownerMemberId).toBeUndefined();
    });
  });
}

// ==== plugin-level tests (watchdog + swarm_tasks retry) — sqlite backend ====

let dirs: string[] = [];
let hooks: Hooks | undefined;
let tool: Record<string, any>;

// Mutable fake-runtime state (reset per plugin init).
const sessions = new Map<string, any>();
let messagesData: Record<string, any[]> = {};
let failCreate = false;
const promptLog: string[] = [];

const fakeClient = {
  session: {
    create: async (opts: any) => {
      if (failCreate) throw new Error("session create failed");
      const id = `ses-wd-${Math.random().toString(36).slice(2, 8)}`;
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
    promptAsync: async () => {
      promptLog.push("promptAsync");
      return { data: undefined, error: undefined };
    },
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-wd" },
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

beforeAll(async () => {
  const dir = mkdtempSync(join(tmpdir(), "swarms-wd-test-"));
  dirs.push(dir);
  sessions.clear();
  messagesData = {};
  failCreate = false;
  promptLog.length = 0;
  hooks = await swarmPlugin(pluginInput, { dataDir: dir });
  tool = hooks.tool ?? {};
});

afterAll(async () => {
  disposeSwarmRuntime();
  await (hooks as any).dispose?.();
  for (const d of dirs) { try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

async function rt(): Promise<SwarmPluginRuntime> {
  const r = await import("../../src/plugin.ts").then((m) => m.swarmRuntime());
  if (!r) throw new Error("swarmRuntime not initialized");
  return r;
}

async function makeSwarm(tag: string, policies: Record<string, unknown> = {}): Promise<string> {
  const create = await tool.swarm_create.execute(
    { name: `wd-${tag}`, policies },
    ctx(`ses-wd-lead-${tag}`),
  );
  const created = JSON.parse(String(create.output));
  return created.swarm.id as string;
}

async function insertMember(swarmId: string, name: string, sessionId: string) {
  const r = await rt();
  await r.store.insertMember({
    id: `mem-${swarmId}-${name}`,
    swarmId,
    name,
    role: "worker",
    sessionId,
    status: "idle",
    workspaceMode: "worktree",
    createdAt: Date.now(),
    updatedAt: Date.now(),
  });
}

/** Claim a task for a member and put both into the `working` state (the state
 * the watchdog watches). */
async function makeMemberWorkingOnTask(swarmId: string, memberId: string, taskId: string) {
  const r = await rt();
  const swarm = await r.store.getSwarm(swarmId);
  expect(await r.store.claimTask(taskId, memberId, 60_000)).toBe(true);
  await r.store.updateTaskStatus(taskId, "working");
  await r.store.updateMemberStatus(memberId, "working", { currentTaskId: taskId, lastActiveAt: Date.now() - 60_000 });
  void swarm;
}

describe("W-1 — watchdog releases do not burn the retry budget", () => {
  test("churning a silent member (release -> reclaim) never fails the task", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag, { maxRetriesPerTask: 2, watchdogSilenceMs: 10, watchdogMaxStrikes: 2 });
    const r = await rt();
    await insertMember(swarmId, "a", `ses-a-${tag}`);
    sessions.set(`ses-a-${tag}`, { id: `ses-a-${tag}`, title: "a", directory: "." }); // session present (silent, not absent)
    const m = (await r.store.listMembers(swarmId)).find((x) => x.name === "a")!;
    const coord = (await r.store.listMembers(swarmId)).find((x) => x.role === "coordinator")!;
    await r.store.insertTask({
      id: `T-${tag}`, swarmId, title: "churn task", status: "ready", priority: 0,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });

    // Churn: each cycle = working -> watchdog release (strike 1 nudges, strike
    // 2 releases with maxStrikes 2) -> reclaim -> repeat.
    for (let i = 0; i < 4; i++) {
      await makeMemberWorkingOnTask(swarmId, m.id, `T-${tag}`);
      await r.watchdog(swarmId); // strike 1 -> nudge
      await r.watchdog(swarmId); // strike 2 -> release (countAsRetry: false)
      const after = (await r.store.listTasks(swarmId)).find((t) => t.id === `T-${tag}`);
      expect(after?.status).toBe("ready");
      expect(after?.ownerMemberId).toBeUndefined();
      expect(after?.retryCount).toBe(0); // retry budget NEVER moves
      // Member released into interrupted; reset for the next cycle.
      await r.store.updateMemberStatus(m.id, "idle", { currentTaskId: null });
    }

    // After 4 churn cycles with maxRetries=2 the task must STILL be alive.
    const final = (await r.store.listTasks(swarmId)).find((t) => t.id === `T-${tag}`);
    expect(final?.status).toBe("ready");
    expect(final?.retryCount).toBe(0);
    await r.runScheduler(swarmId);
    const afterSched = (await r.store.listTasks(swarmId)).find((t) => t.id === `T-${tag}`);
    expect(afterSched?.status).not.toBe("failed");
  });
});

describe("W-2 — respawn-on-absent instead of release", () => {
  test("absent session -> respawn attempted; task stays with the member (re-claimed + re-prompted)", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag, { watchdogSilenceMs: 10, watchdogMaxStrikes: 2 });
    const r = await rt();
    await insertMember(swarmId, "a", `ses-abs-${tag}`);
    // Session ABSENT: NOT in the sessions map (getSession -> null).
    const m = (await r.store.listMembers(swarmId)).find((x) => x.name === "a")!;
    const coord = (await r.store.listMembers(swarmId)).find((x) => x.role === "coordinator")!;
    await r.store.insertTask({
      id: `TA-${tag}`, swarmId, title: "absent task", status: "ready", priority: 0,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    await makeMemberWorkingOnTask(swarmId, m.id, `TA-${tag}`);

    // Pre-escalation: assert the session is genuinely absent.
    expect(await r.runtimeAdapter.getSession(m.sessionId)).toBeNull();

    const promptsBefore = promptLog.length;
    await r.watchdog(swarmId); // strike 1 -> nudge
    await r.watchdog(swarmId); // strike 2 -> absent -> respawn

    const memberAfter = await r.store.getMemberById(m.id);
    const taskAfter = (await r.store.listTasks(swarmId)).find((t) => t.id === `TA-${tag}`);
    // Respawned: fresh session id bound to the member; the task STAYS claimed.
    expect(memberAfter?.sessionId).not.toBe(m.sessionId);
    expect(memberAfter?.status).toBe("working");
    expect(memberAfter?.currentTaskId).toBe(`TA-${tag}`);
    expect(taskAfter?.ownerMemberId).toBe(m.id);
    expect(taskAfter?.status).toBe("working");
    expect(taskAfter?.retryCount).toBe(0); // release inside respawn was countAsRetry:false
    // Fresh session re-prompted (respawnMember's promptAsync) beyond the nudges.
    expect(promptLog.length).toBeGreaterThan(promptsBefore);
  });

  test("absent session + respawn FAILURE -> released with countAsRetry:false", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag, { watchdogSilenceMs: 10, watchdogMaxStrikes: 2 });
    const r = await rt();
    await insertMember(swarmId, "a", `ses-absf-${tag}`);
    const m = (await r.store.listMembers(swarmId)).find((x) => x.name === "a")!;
    const coord = (await r.store.listMembers(swarmId)).find((x) => x.role === "coordinator")!;
    await r.store.insertTask({
      id: `TF-${tag}`, swarmId, title: "absent fail task", status: "ready", priority: 0,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    await makeMemberWorkingOnTask(swarmId, m.id, `TF-${tag}`);

    failCreate = true; // respawn will throw
    try {
      await r.watchdog(swarmId); // strike 1 -> nudge
      await r.watchdog(swarmId); // strike 2 -> absent -> respawn fails -> release
    } finally {
      failCreate = false;
    }

    const memberAfter = await r.store.getMemberById(m.id);
    const taskAfter = (await r.store.listTasks(swarmId)).find((t) => t.id === `TF-${tag}`);
    expect(memberAfter?.status).toBe("interrupted");
    expect(memberAfter?.currentTaskId).toBeUndefined();
    expect(taskAfter?.status).toBe("ready");
    expect(taskAfter?.ownerMemberId).toBeUndefined();
    expect(taskAfter?.retryCount).toBe(0); // release was countAsRetry:false
  });

  test("a merely-SILENT session (present) is released, not respawned", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag, { watchdogSilenceMs: 10, watchdogMaxStrikes: 2 });
    const r = await rt();
    await insertMember(swarmId, "a", `ses-sil-${tag}`);
    sessions.set(`ses-sil-${tag}`, { id: `ses-sil-${tag}`, title: "a", directory: "." }); // PRESENT
    const m = (await r.store.listMembers(swarmId)).find((x) => x.name === "a")!;
    const coord = (await r.store.listMembers(swarmId)).find((x) => x.role === "coordinator")!;
    await r.store.insertTask({
      id: `TS-${tag}`, swarmId, title: "silent task", status: "ready", priority: 0,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    await makeMemberWorkingOnTask(swarmId, m.id, `TS-${tag}`);
    const oldSession = m.sessionId;

    await r.watchdog(swarmId); // strike 1 -> nudge
    await r.watchdog(swarmId); // strike 2 -> present (silent) -> plain release

    const memberAfter = await r.store.getMemberById(m.id);
    const taskAfter = (await r.store.listTasks(swarmId)).find((t) => t.id === `TS-${tag}`);
    expect(memberAfter?.sessionId).toBe(oldSession); // NOT respawned
    expect(memberAfter?.status).toBe("interrupted");
    expect(taskAfter?.status).toBe("ready");
    expect(taskAfter?.retryCount).toBe(0);
  });
});

describe("W-3 — policy knobs", () => {
  test("watchdogSilenceMs override: tiny value fires the watchdog where the default would not", async () => {
    // Same recent activity (1s-old message): the tiny-window swarm escalates,
    // the default-window swarm does not.
    const recent = Date.now() - 1000;
    const mkMsg = () => [{ info: { id: "m1", role: "assistant" as const, createdAt: recent, parts: [{ type: "text", text: "working" }] } }];

    // A) tiny override (10ms) -> watchdog fires quickly.
    const tagA = Math.random().toString(36).slice(2, 8);
    const swarmA = await makeSwarm(tagA, { watchdogSilenceMs: 10, watchdogMaxStrikes: 2 });
    const rA = await rt();
    await insertMember(swarmA, "a", `ses-tiny-${tagA}`);
    sessions.set(`ses-tiny-${tagA}`, { id: `ses-tiny-${tagA}`, title: "a", directory: "." });
    messagesData[`ses-tiny-${tagA}`] = mkMsg();
    const mA = (await rA.store.listMembers(swarmA)).find((x) => x.name === "a")!;
    const coordA = (await rA.store.listMembers(swarmA)).find((x) => x.role === "coordinator")!;
    await rA.store.insertTask({
      id: `TTA-${tagA}`, swarmId: swarmA, title: "tiny task", status: "ready", priority: 0,
      createdByMemberId: coordA.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    await makeMemberWorkingOnTask(swarmA, mA.id, `TTA-${tagA}`);
    await rA.watchdog(swarmA); // silent under 10ms -> strike 1 (nudge)
    await rA.watchdog(swarmA); // strike 2 -> release
    expect((await rA.store.listTasks(swarmA)).find((t) => t.id === `TTA-${tagA}`)?.status).toBe("ready");

    // B) default (5 min) with the SAME 1s-old activity -> NOT silent, no release.
    const tagB = Math.random().toString(36).slice(2, 8);
    const swarmB = await makeSwarm(tagB);
    const rB = await rt();
    await insertMember(swarmB, "b", `ses-def-${tagB}`);
    sessions.set(`ses-def-${tagB}`, { id: `ses-def-${tagB}`, title: "b", directory: "." });
    messagesData[`ses-def-${tagB}`] = mkMsg();
    const mB = (await rB.store.listMembers(swarmB)).find((x) => x.name === "b")!;
    const coordB = (await rB.store.listMembers(swarmB)).find((x) => x.role === "coordinator")!;
    await rB.store.insertTask({
      id: `TTB-${tagB}`, swarmId: swarmB, title: "default task", status: "ready", priority: 0,
      createdByMemberId: coordB.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    await makeMemberWorkingOnTask(swarmB, mB.id, `TTB-${tagB}`);
    await rB.watchdog(swarmB);
    await rB.watchdog(swarmB);
    expect((await rB.store.listTasks(swarmB)).find((t) => t.id === `TTB-${tagB}`)?.status).toBe("working");
    expect((await rB.store.listTasks(swarmB)).find((t) => t.id === `TTB-${tagB}`)?.ownerMemberId).toBe(mB.id);
  });
});

describe("W-4 — swarm_tasks action 'retry' (failed-task recovery)", () => {
  test("coordinator retries a failed task: ready + retryCount 0 + re-assignable + DAG edges preserved", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag);
    const r = await rt();
    const coord = (await r.store.listMembers(swarmId)).find((x) => x.role === "coordinator")!;
    await insertMember(swarmId, "a", `ses-a-${tag}`);
    sessions.set(`ses-a-${tag}`, { id: `ses-a-${tag}`, title: "a", directory: "." });
    const m = (await r.store.listMembers(swarmId)).find((x) => x.name === "a")!;
    await r.store.insertTask({
      id: `RF-${tag}`, swarmId, title: "failed task", status: "failed", priority: 0, retryCount: 2,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    // DAG edge preserved through the retry.
    await r.store.insertTask({
      id: `RD-${tag}`, swarmId, title: "dependent", status: "blocked", priority: 0,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    await r.store.insertTaskDependency(`RD-${tag}`, `RF-${tag}`);
    // Keep member 'a' BUSY during the retry (owns a different task) so the
    // retry action's internal scheduler pass leaves the task ready + reserved
    // instead of instantly assigning it — the reservation is then demonstrable.
    await r.store.insertTask({
      id: `T2-${tag}`, swarmId, title: "busy task", status: "ready", priority: 0,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    await makeMemberWorkingOnTask(swarmId, m.id, `T2-${tag}`);

    // Coordinator session id for tool context.
    const coordSession = (await r.store.getMemberById(coord.id))!.sessionId;
    const res = await tool.swarm_tasks.execute(
      { swarmId, action: "retry", taskId: `RF-${tag}`, member: "a" },
      ctx(coordSession),
    );
    const out = String(res.output);
    expect(out).toContain(`'${`RF-${tag}`}' reset to ready (fresh retry budget), reserved for 'a'`);

    const t = (await r.store.listTasks(swarmId)).find((x) => x.id === `RF-${tag}`);
    expect(t?.status).toBe("ready");
    expect(t?.retryCount).toBe(0);
    expect(t?.ownerMemberId).toBeUndefined();
    expect(t?.reservedFor).toBe("a");
    // Reservation: scheduler prefers member 'a' for the retried task.
    expect(t?.reservedAt).toBeDefined();
    // DAG edge survives the retry (id + edges preserved).
    const deps = await r.store.listTaskDependencies(swarmId);
    expect(deps.some((d) => d.taskId === `RD-${tag}` && d.dependsOnTaskId === `RF-${tag}`)).toBe(true);

    // Re-assignable: free member 'a', and the scheduler assigns the reserved
    // retried task to it.
    await r.store.releaseTask(`T2-${tag}`, { countAsRetry: false });
    await r.store.updateMemberStatus(m.id, "idle", { currentTaskId: null });
    await r.runScheduler(swarmId);
    const assigned = (await r.store.listTasks(swarmId)).find((x) => x.id === `RF-${tag}`);
    expect(assigned?.status).toBe("working");
    expect(assigned?.ownerMemberId).toBe(m.id);
    // The dependent stays BLOCKED (its prerequisite is working again, not yet
    // terminal) — the DAG recomputes normally; the edge is intact.
    expect((await r.store.listTasks(swarmId)).find((x) => x.id === `RD-${tag}`)?.status).toBe("blocked");
  });

  test("a worker member cannot retry (coordinator-only)", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag);
    const r = await rt();
    const coord = (await r.store.listMembers(swarmId)).find((x) => x.role === "coordinator")!;
    await insertMember(swarmId, "w", `ses-w-${tag}`);
    const w = (await r.store.listMembers(swarmId)).find((x) => x.name === "w")!;
    await r.store.insertTask({
      id: `RW-${tag}`, swarmId, title: "w task", status: "failed", priority: 0, retryCount: 1,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    const res = await tool.swarm_tasks.execute(
      { swarmId, action: "retry", taskId: `RW-${tag}` },
      ctx(w.sessionId),
    );
    expect(String(res.output)).toContain("only the coordinator may retry tasks");
    expect((await r.store.listTasks(swarmId)).find((x) => x.id === `RW-${tag}`)?.status).toBe("failed");
  });

  test("cancelled tasks are also recoverable; a ready task is not", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag);
    const r = await rt();
    const coord = (await r.store.listMembers(swarmId)).find((x) => x.role === "coordinator")!;
    const coordSession = (await r.store.getMemberById(coord.id))!.sessionId;
    await r.store.insertTask({
      id: `RC-${tag}`, swarmId, title: "cancelled task", status: "cancelled", priority: 0, retryCount: 5,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    await r.store.insertTask({
      id: `RX-${tag}`, swarmId, title: "ready task", status: "ready", priority: 0,
      createdByMemberId: coord.id, createdAt: Date.now(), updatedAt: Date.now(),
    });
    const ok = await tool.swarm_tasks.execute({ swarmId, action: "retry", taskId: `RC-${tag}` }, ctx(coordSession));
    expect(String(ok.output)).toContain("reset to ready (fresh retry budget)");
    expect((await r.store.listTasks(swarmId)).find((x) => x.id === `RC-${tag}`)?.status).toBe("ready");
    expect((await r.store.listTasks(swarmId)).find((x) => x.id === `RC-${tag}`)?.retryCount).toBe(0);
    // A READY task is not recoverable — the reset is a no-op.
    const noop = await tool.swarm_tasks.execute({ swarmId, action: "retry", taskId: `RX-${tag}` }, ctx(coordSession));
    expect(String(noop.output)).toContain("not recoverable");
    expect((await r.store.listTasks(swarmId)).find((x) => x.id === `RX-${tag}`)?.status).toBe("ready");
  });
});
