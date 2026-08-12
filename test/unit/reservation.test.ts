import { describe, expect, test } from "bun:test";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Scheduler, DEFAULT_RESERVATION_TTL_MS } from "../../src/scheduler/scheduler.ts";
import { DEFAULT_POLICIES, type SwarmPolicies, type SwarmTask } from "../../src/core/types.ts";

/**
 * Durable intended-owner reservation tests (S-15 fix). The bug: a task bound
 * to a member at delegation (member.taskId) lost its binding after the
 * delegate's in-memory pass — a task that became ready LATER via DAG
 * dependency resolution was assigned by affinity to whatever member was idle.
 * The fix persists reservedFor on the task row and the scheduler prefers it.
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
    name: "res", projectId: "res", coordinatorSessionId: "sess-c",
    policies: { ...DEFAULT_POLICIES, ...policies },
  });
  const swarmId = created.swarm.id;
  const now = Date.now();
  // 'affinity-winner' has a role that matches "combine haikus" tasks.
  await store.insertMember({ id: "m-int", swarmId, name: "intended", sessionId: "sess-int", role: "plain worker", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
  await store.insertMember({ id: "m-aff", swarmId, name: "affinity-winner", sessionId: "sess-aff", role: "combines haikus", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
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

describe("durable reservation — later-ready DAG tasks", () => {
  test("a ready task reserved for an idle intended owner goes to THEM, not the affinity winner", async () => {
    const { swarmId, swarm, scheduler, store } = await setup();
    await insertTask(store, swarmId, "t-later", {
      title: "combine haikus task",
      reservedFor: "intended", reservedAt: Date.now(),
    });
    const result = await scheduler.run(swarm, {});
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]!.memberName).toBe("intended");
    const task = await store.listTasks(swarmId);
    expect(task[0]!.ownerMemberId).toBe("m-int");
    expect(task[0]!.reservedFor).toBeUndefined(); // cleared on claim
  });

  test("reserved task is NOT given to another member while the intended owner is busy", async () => {
    const { swarmId, swarm, scheduler, store } = await setup();
    // The intended owner is working on another (real) task — not idle.
    await insertTask(store, swarmId, "other", { title: "other task" });
    await store.claimTask("other", "m-int", 0);
    await store.updateMemberStatus("m-int", "working", { currentTaskId: "other" });
    await store.updateTaskStatus("other", "working");
    await insertTask(store, swarmId, "t-held", {
      title: "held task",
      reservedFor: "intended", reservedAt: Date.now(),
    });
    const result = await scheduler.run(swarm, {});
    expect(result.assigned.length).toBe(0);
    expect(result.readyUnassigned).toContain("t-held");
    const task = await store.listTasks(swarmId);
    const held = task.find((t: any) => t.id === "t-held")!;
    expect(held.ownerMemberId).toBeUndefined();
    expect(held.reservedFor).toBe("intended"); // still reserved
  });

  test("reservation expires after TTL and the task frees to affinity with a fallback recorded", async () => {
    const { swarmId, swarm, scheduler, store } = await setup({ reservationTtlMs: 1000 });
    await insertTask(store, swarmId, "other", { title: "other task" });
    await store.claimTask("other", "m-int", 0);
    await store.updateMemberStatus("m-int", "working", { currentTaskId: "other" });
    await store.updateTaskStatus("other", "working");
    await insertTask(store, swarmId, "t-expired", {
      title: "expired reservation task",
      reservedFor: "intended", reservedAt: Date.now() - 5_000,
    });
    const result = await scheduler.run(swarm, {});
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]!.memberName).toBe("affinity-winner");
    expect(result.reservationFallbacks.length).toBe(1);
    expect(result.reservationFallbacks[0]!.taskId).toBe("t-expired");
    expect(result.reservationFallbacks[0]!.intendedMemberName).toBe("intended");
    const task = await store.listTasks(swarmId);
    expect(task[0]!.reservedFor).toBeUndefined(); // cleared after fallback
  });

  test("reserved task whose intended member never spawns frees after TTL", async () => {
    const { swarmId, swarm, scheduler, store } = await setup({ reservationTtlMs: 1000 });
    await insertTask(store, swarmId, "t-nomember", {
      title: "never-spawned owner task",
      reservedFor: "ghost-member", reservedAt: Date.now() - 5_000,
    });
    const result = await scheduler.run(swarm, {});
    expect(result.assigned.length).toBe(1);
    expect(result.reservationFallbacks[0]!.intendedMemberName).toBe("ghost-member");
  });

  test("unreserved tasks still go to the affinity winner (no behavior change)", async () => {
    const { swarmId, swarm, scheduler, store } = await setup();
    await insertTask(store, swarmId, "t-free", { title: "combine haikus task" });
    const result = await scheduler.run(swarm, {});
    expect(result.assigned[0]!.memberName).toBe("affinity-winner");
    expect(result.reservationFallbacks.length).toBe(0);
  });
});

describe("store reservation lifecycle", () => {
  test("claimTask clears the reservation (real owner exists)", async () => {
    const { swarmId, store } = await setup();
    await insertTask(store, swarmId, "t-c", { reservedFor: "intended", reservedAt: Date.now() });
    const ok = await store.claimTask("t-c", "m-int", 0);
    expect(ok).toBe(true);
    const task = await store.listTasks(swarmId);
    expect(task[0]!.reservedFor).toBeUndefined();
    expect(task[0]!.reservedAt).toBeUndefined();
    expect(task[0]!.ownerMemberId).toBe("m-int");
  });

  test("releaseTask clears the reservation (rescue — no re-reserve to failed member)", async () => {
    const { swarmId, store } = await setup();
    await insertTask(store, swarmId, "t-r", { reservedFor: "intended", reservedAt: Date.now() });
    await store.claimTask("t-r", "m-int", 0);
    await store.updateTaskStatus("t-r", "working");
    const ok = await store.releaseTask("t-r", { countAsRetry: false });
    expect(ok).toBe(true);
    const task = await store.listTasks(swarmId);
    expect(task[0]!.reservedFor).toBeUndefined();
    expect(task[0]!.reservedAt).toBeUndefined();
    expect(task[0]!.status).toBe("ready");
    expect(task[0]!.ownerMemberId).toBeUndefined();
  });

  test("reassignTask sets the reservation to the new owner", async () => {
    const { swarmId, store } = await setup();
    await insertTask(store, swarmId, "t-re", { reservedFor: "intended", reservedAt: Date.now() });
    await store.claimTask("t-re", "m-int", 0);
    await store.updateTaskStatus("t-re", "working");
    const prev = await store.reassignTask("t-re", "m-aff");
    expect(prev).toBe("m-int");
    const task = await store.listTasks(swarmId);
    expect(task[0]!.reservedFor).toBe("affinity-winner");
    expect(task[0]!.ownerMemberId).toBe("m-aff");
  });

  test("setTaskReservation roundtrips through toTask", async () => {
    const { swarmId, store } = await setup();
    await insertTask(store, swarmId, "t-rt");
    await store.setTaskReservation("t-rt", "intended");
    let task = await store.listTasks(swarmId);
    expect(task[0]!.reservedFor).toBe("intended");
    expect(task[0]!.reservedAt).toBeGreaterThan(0);
    await store.setTaskReservation("t-rt", null);
    task = await store.listTasks(swarmId);
    expect(task[0]!.reservedFor).toBeUndefined();
  });
});
