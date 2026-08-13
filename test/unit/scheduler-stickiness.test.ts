import { describe, expect, test } from "bun:test";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Scheduler, DEFAULT_TASK_STICKY_MS } from "../../src/scheduler/scheduler.ts";
import { affinityScore } from "../../src/scheduler/dag.ts";
import { DEFAULT_POLICIES, type SwarmPolicies, type SwarmTask } from "../../src/core/types.ts";

/**
 * Coordinator-reassign STICKINESS tests (scheduler-affinity fix). The bug: the
 * affinity sweep re-grabbed tasks the coordinator had explicitly REASSIGNED —
 * a release wiped the reserved_for/reserved_at marker and the next sweep
 * re-assigned the task by loose name-substring affinity to a different member
 * (e.g. 'browser-contract' -> 'recon-protocol', 'backfill-nonblocking' ->
 * 'timeout-fixer', and search-* tasks ping-ponging by the shared 'search'
 * token). The fix:
 *  - reassign persists reserved_for = new owner + reserved_at = now, and
 *    release PRESERVES/refreshes the marker (sticky window, taskStickyMs);
 *  - within the sticky window the affinity sweep must NOT re-grab the task
 *    (unless the intended owner is unavailable -> fall through + claimWarning);
 *  - affinityScore requires >= 2 significant tokens (no bare substring wins);
 *  - non-resumable members (interrupted/stopped/stopping/failed) never get
 *    tasks by affinity.
 */

const fakeRuntime = {
  createSession: async () => ({ id: "sess-x" }),
  updateSession: async () => undefined,
  promptAsync: async () => undefined,
  prompt: async () => undefined,
  listChildren: async () => [],
  getSession: async () => undefined,
  getSessionPermissions: async () => undefined,
  getSessionTodos: async () => [],
};

async function setup(policies: Partial<SwarmPolicies> = {}) {
  const { SQLiteStore } = await import("../../src/storage/sqlite-store.ts");
  const store = new SQLiteStore(":memory:");
  await store.ready();
  const core = new SwarmCore(store as never, fakeRuntime as never);
  const created = await core.createSwarm({
    name: "sticky", projectId: "sticky", coordinatorSessionId: "sess-c",
    policies: { ...DEFAULT_POLICIES, ...policies },
  });
  const swarmId = created.swarm.id;
  const now = Date.now();
  // 'intended-owner' is the coordinator's reassign target; 'affinity-winner'
  // has a role that scores HIGH on the test tasks — the old sweep would have
  // stolen the task for it.
  await store.insertMember({ id: "m-int", swarmId, name: "intended-owner", sessionId: "sess-int", role: "plain worker", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
  await store.insertMember({ id: "m-aff", swarmId, name: "affinity-winner", sessionId: "sess-aff", role: "backfill nonblocking queue implementer", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
  const swarm = (await store.getSwarm(swarmId))!;
  const scheduler = new Scheduler(store as never, fakeRuntime as never);
  return { store, core, swarmId, swarm, scheduler };
}

async function insertTask(store: any, swarmId: string, id: string, over: Partial<SwarmTask> = {}): Promise<SwarmTask> {
  const now = Date.now();
  const t = {
    id, swarmId, title: id, status: "ready" as const, priority: 0,
    createdByMemberId: "m-int",
    retryCount: 0, createdAt: now, updatedAt: now,
    ...over,
  };
  await store.insertTask(t);
  return t as SwarmTask;
}

describe("coordinator reassign stickiness", () => {
  test("(a) after reassign + release, the sweep leaves the task for the NEW owner despite a competing idle higher-affinity member", async () => {
    const { swarmId, swarm, scheduler, store } = await setup();
    const mInt = await store.getMemberByName(swarmId, "intended-owner");
    const mAff = await store.getMemberByName(swarmId, "affinity-winner");
    // Simulate the REAL reassign flow: claim -> working -> coordinator
    // reassignTask (writes reserved_for + reserved_at) -> kickoff-failure
    // release (countAsRetry:false, the plugin reassign path) + owner freed.
    await insertTask(store, swarmId, "T-RE", { title: "backfill nonblocking queue" });
    await store.claimTask("T-RE", mAff!.id, 0);
    await store.updateTaskStatus("T-RE", "working");
    await store.reassignTask("T-RE", mInt!.id);
    const before = (await store.listTasks(swarmId)).find((t: any) => t.id === "T-RE");
    expect(before!.reservedFor).toBe("intended-owner"); // reassign marker persisted
    await store.releaseTask("T-RE", { countAsRetry: false });
    await store.updateMemberStatus(mInt!.id, "idle", { currentTaskId: null, lastActiveAt: Date.now() });
    // The marker must SURVIVE the release (the old bug wiped it here).
    const released = (await store.listTasks(swarmId)).find((t: any) => t.id === "T-RE");
    expect(released!.reservedFor).toBe("intended-owner");
    expect(released!.reservedAt).toBeGreaterThan(0);

    // affinity-winner is idle AND scores high on the task; the sticky window
    // must keep the task with the intended owner.
    const result = await scheduler.run(swarm, {});
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]!.memberName).toBe("intended-owner");
    const after = (await store.listTasks(swarmId)).find((t: any) => t.id === "T-RE");
    expect(after!.ownerMemberId).toBe(mInt!.id);
    const affAfter = await store.getMemberById(mAff!.id);
    expect(affAfter!.currentTaskId).toBeUndefined(); // NOT stolen
    // Once claimed, the sticky marker is cleared (real owner exists).
    expect(after!.reservedFor).toBeUndefined();
  });

  test("(a2) sticky task falls through to affinity with a claimWarning when the intended owner is unavailable (stopped)", async () => {
    const { swarmId, swarm, scheduler, store } = await setup();
    const mInt = await store.getMemberByName(swarmId, "intended-owner");
    await insertTask(store, swarmId, "T-FT", {
      title: "backfill nonblocking queue",
      reservedFor: "intended-owner", reservedAt: Date.now(),
    });
    // Intended owner stopped (respawn needed) — holding would starve the task
    // for the whole window; fall through to affinity WITH a warning.
    await store.updateMemberStatus(mInt!.id, "stopped", { currentTaskId: null });
    const result = await scheduler.run(swarm, {});
    expect(result.stickyFallthroughs.length).toBe(1);
    expect(result.stickyFallthroughs[0]!.taskId).toBe("T-FT");
    expect(result.stickyFallthroughs[0]!.intendedMemberName).toBe("intended-owner");
    expect(result.stickyFallthroughs[0]!.reason).toContain("stopped");
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]!.memberName).toBe("affinity-winner");
  });

  test("(b) after the sticky window expires, affinity can re-engage", async () => {
    const { swarmId, swarm, scheduler, store } = await setup();
    await insertTask(store, swarmId, "T-EXP", {
      title: "backfill nonblocking queue",
      reservedFor: "intended-owner", reservedAt: Date.now() - DEFAULT_TASK_STICKY_MS - 60_000,
    });
    const result = await scheduler.run(swarm, {});
    expect(result.reservationFallbacks.length).toBe(1);
    expect(result.reservationFallbacks[0]!.intendedMemberName).toBe("intended-owner");
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]!.memberName).toBe("affinity-winner"); // affinity re-engaged
  });

  test("(e) policy taskStickyMs override is respected (small override frees early; default window holds)", async () => {
    // Override: 1s sticky window, marker 5s old -> expired -> affinity wins.
    const short = await setup({ taskStickyMs: 1000 });
    await insertTask(short.store, short.swarmId, "T-OVR", {
      title: "backfill nonblocking queue",
      reservedFor: "intended-owner", reservedAt: Date.now() - 5_000,
    });
    const resultShort = await short.scheduler.run(short.swarm, {});
    expect(resultShort.reservationFallbacks.length).toBe(1);
    expect(resultShort.assigned[0]!.memberName).toBe("affinity-winner");

    // Control: default 10-min window, same 5s-old marker -> still sticky ->
    // goes to the intended owner (who is idle).
    const def = await setup();
    await insertTask(def.store, def.swarmId, "T-CTRL", {
      title: "backfill nonblocking queue",
      reservedFor: "intended-owner", reservedAt: Date.now() - 5_000,
    });
    const resultDef = await def.scheduler.run(def.swarm, {});
    expect(resultDef.reservationFallbacks.length).toBe(0);
    expect(resultDef.assigned.length).toBe(1);
    expect(resultDef.assigned[0]!.memberName).toBe("intended-owner");
  });
});

describe("tighter affinity scoring", () => {
  test("(c) substring-only / single-token matches score 0; real 2-token matches win", () => {
    // 'timeout-fixer' shares only 'fixer' (via substring-ish token) -> 0.
    expect(affinityScore("timeout-fixer", "timeout recovery", "backfill nonblocking fixer")).toBe(0);
    // 'recon-protocol' shares only the loose 'protocol' token -> 0.
    expect(affinityScore("recon-protocol", "protocol research", "browser contract protocol")).toBe(0);
    // The real owners beat them.
    expect(affinityScore("backfill-fixer", "timeout recovery", "backfill nonblocking fixer")).toBeGreaterThan(0);
    expect(affinityScore("browser-contract", "browser contract implementation", "browser contract protocol")).toBeGreaterThan(0);
  });

  test("(c2) scheduler assigns the real token match, not the substring-only member", async () => {
    const { SQLiteStore } = await import("../../src/storage/sqlite-store.ts");
    const store = new SQLiteStore(":memory:");
    await store.ready();
    const core = new SwarmCore(store as never, fakeRuntime as never);
    const created = await core.createSwarm({
      name: "sticky2", projectId: "sticky2", coordinatorSessionId: "sess-c2",
      policies: { ...DEFAULT_POLICIES },
    });
    const swarmId = created.swarm.id;
    const now = Date.now();
    await store.insertMember({ id: "m-tf", swarmId, name: "timeout-fixer", sessionId: "s-tf", role: "timeout recovery", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    await store.insertMember({ id: "m-bf", swarmId, name: "backfill-fixer", sessionId: "s-bf", role: "timeout recovery", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    await insertTask(store, swarmId, "T-SUB", { title: "backfill nonblocking fixer" });
    const swarm = (await store.getSwarm(swarmId))!;
    const scheduler = new Scheduler(store as never, fakeRuntime as never);
    const result = await scheduler.run(swarm, {});
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]!.memberName).toBe("backfill-fixer");
  });
});

describe("non-resumable members", () => {
  test("(d) interrupted/stopped members never receive tasks by affinity", async () => {
    const { SQLiteStore } = await import("../../src/storage/sqlite-store.ts");
    const store = new SQLiteStore(":memory:");
    await store.ready();
    const core = new SwarmCore(store as never, fakeRuntime as never);
    const created = await core.createSwarm({
      name: "sticky3", projectId: "sticky3", coordinatorSessionId: "sess-c3",
      policies: { ...DEFAULT_POLICIES },
    });
    const swarmId = created.swarm.id;
    const now = Date.now();
    const idle = { status: "idle" as const, model: undefined, workspaceMode: "shared-read" as const, createdAt: now, updatedAt: now };
    await store.insertMember({ id: "m-a", swarmId, name: "worker-a", sessionId: "s-a", role: "widget builder", ...idle });
    await store.insertMember({ id: "m-b", swarmId, name: "worker-b", sessionId: "s-b", role: "widget builder", status: "stopped", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    await store.insertMember({ id: "m-c", swarmId, name: "worker-c", sessionId: "s-c", role: "widget builder", status: "interrupted", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    await insertTask(store, swarmId, "T-D", { title: "build the widget" });
    const swarm = (await store.getSwarm(swarmId))!;
    const scheduler = new Scheduler(store as never, fakeRuntime as never);
    const result = await scheduler.run(swarm, {});
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]!.memberName).toBe("worker-a");
    const t = (await store.listTasks(swarmId)).find((x: any) => x.id === "T-D");
    expect(t!.ownerMemberId).toBe("m-a");
    for (const name of ["worker-b", "worker-c"]) {
      const m = await store.getMemberByName(swarmId, name);
      expect(m!.currentTaskId).toBeUndefined();
    }
  });
});

describe("store release marker semantics", () => {
  test("release preserves an existing reassign marker (refreshes reservedAt)", async () => {
    const { swarmId, store } = await setup();
    const mInt = await store.getMemberByName(swarmId, "intended-owner");
    const mAff = await store.getMemberByName(swarmId, "affinity-winner");
    await insertTask(store, swarmId, "T-KEEP", { title: "backfill nonblocking queue" });
    await store.claimTask("T-KEEP", mAff!.id, 0);
    await store.updateTaskStatus("T-KEEP", "working");
    await store.reassignTask("T-KEEP", mInt!.id);
    const ok = await store.releaseTask("T-KEEP", { countAsRetry: false });
    expect(ok).toBe(true);
    const t = (await store.listTasks(swarmId)).find((x: any) => x.id === "T-KEEP");
    expect(t!.reservedFor).toBe("intended-owner"); // NOT wiped by the release
    expect(t!.reservedAt).toBeGreaterThan(0); // refreshed
  });

  test("a normal (countAsRetry) release creates a previous-owner marker (grace window)", async () => {
    const { swarmId, store } = await setup();
    const mAff = await store.getMemberByName(swarmId, "affinity-winner");
    await insertTask(store, swarmId, "T-GRACE", { title: "backfill nonblocking queue" });
    await store.claimTask("T-GRACE", mAff!.id, 0);
    await store.updateTaskStatus("T-GRACE", "working");
    await store.releaseTask("T-GRACE"); // default countAsRetry: true
    const t = (await store.listTasks(swarmId)).find((x: any) => x.id === "T-GRACE");
    expect(t!.reservedFor).toBe("affinity-winner"); // previous owner gets first refusal
    expect(t!.reservedAt).toBeGreaterThan(0);
  });

  test("a rescue (countAsRetry:false) release with NO marker does NOT create one", async () => {
    const { swarmId, store } = await setup();
    const mAff = await store.getMemberByName(swarmId, "affinity-winner");
    await insertTask(store, swarmId, "T-RESCUE", { title: "backfill nonblocking queue" });
    await store.claimTask("T-RESCUE", mAff!.id, 0);
    await store.updateTaskStatus("T-RESCUE", "working");
    await store.releaseTask("T-RESCUE", { countAsRetry: false });
    const t = (await store.listTasks(swarmId)).find((x: any) => x.id === "T-RESCUE");
    expect(t!.reservedFor).toBeUndefined(); // freed for a DIFFERENT member
  });
});
