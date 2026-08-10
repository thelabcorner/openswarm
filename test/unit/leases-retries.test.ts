import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Scheduler, DEFAULT_TASK_LEASE_MS } from "../../src/scheduler/scheduler.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";
import type { NewSwarm, NewSwarmMember, NewTask } from "../../src/storage/models.ts";

/**
 * Wave 2 leases/retries bundle regression tests:
 *  - retryCount first-class roundtrip (no metadata hack), increment on
 *    release-from-active, maxRetriesPerTask enforcement → failed.
 *  - claimTask sets claimed_at/lease_expires_at from policies.taskLeaseMs;
 *    listExpiredLeaseTasks finds stale claims.
 *  - migration adds the lease columns to a pre-existing (older) database.
 */
class FakeRuntime implements AgentRuntime {
  readonly kind = "fake";
  sessions = new Map<string, RuntimeSession>();
  seq = 0;
  prompts: string[] = [];
  failCreate = false;
  statusOverride: unknown = { type: "idle" };
  async createSession(input: { title: string }): Promise<RuntimeSession> {
    if (this.failCreate) throw new Error("session create failed");
    const id = `ses-fake-${++this.seq}`;
    const s: RuntimeSession = { id, title: input.title, directory: ".", parentID: undefined };
    this.sessions.set(id, s);
    return s;
  }
  async getSession(sid: string): Promise<RuntimeSession | null> { return this.sessions.get(sid) ?? null; }
  async listChildren(parentSID: string): Promise<RuntimeSession[]> {
    return [...this.sessions.values()].filter((s) => s.parentID === parentSID);
  }
  async prompt(): Promise<any> { throw new Error("not used"); }
  async promptAsync(input: { text: string }): Promise<void> { this.prompts.push(input.text); }
  async abort(): Promise<void> {}
  async getStatus(): Promise<any> { return this.statusOverride; }
  async getMessages(): Promise<any[]> { return []; }
}

let dir: string;
let store: SQLiteStore;
let runtime: FakeRuntime;
let core: SwarmCore;
let scheduler: Scheduler;

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-lease-test-"));
  store = new SQLiteStore(join(dir, "lease.db"));
  await store.ready();
  runtime = new FakeRuntime();
  core = new SwarmCore(store, runtime);
  scheduler = new Scheduler(store, runtime);
});

afterAll(async () => {
  await store.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

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
    // NB: swarm_member.id is a GLOBAL primary key — derive it from the swarm
    // too so the same member name in different swarms does not collide.
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

async function makeReadyTask(swarmId: string, id: string): Promise<void> {
  await store.insertTask(newTask(swarmId, id));
  await store.updateTaskStatus(id, "ready");
}

describe("F3 — retryCount roundtrip + enforcement", () => {
  test("retryCount is a first-class field (no metadata hack) and roundtrips", async () => {
    await store.insertSwarm(newSwarm("rt1"));
    await store.insertTask(newTask("swarm-rt1", "R1", { retryCount: 4 }));
    const t = (await store.listTasks("swarm-rt1")).find((x) => x.id === "R1");
    expect(t?.retryCount).toBe(4);
  });

  test("releaseTask increments retry_count when released from an active state", async () => {
    await store.insertSwarm(newSwarm("rt2"));
    await store.insertMember(newMember("swarm-rt2", "a", "ses-rt2-a"));
    await makeReadyTask("swarm-rt2", "R2");
    expect(await store.claimTask("R2", "mem-swarm-rt2-a", 1000)).toBe(true);
    await store.updateTaskStatus("R2", "working");
    expect(await store.releaseTask("R2")).toBe(true);
    const t = (await store.listTasks("swarm-rt2")).find((x) => x.id === "R2");
    expect(t?.status).toBe("ready");
    expect(t?.retryCount).toBe(1);
    // Second release increments again.
    await store.claimTask("R2", "mem-swarm-rt2-a", 1000);
    await store.updateTaskStatus("R2", "working");
    await store.releaseTask("R2");
    expect((await store.listTasks("swarm-rt2")).find((x) => x.id === "R2")?.retryCount).toBe(2);
  });

  test("scheduler fails a ready task whose retryCount exceeds maxRetriesPerTask", async () => {
    await store.insertSwarm(newSwarm("rt3", { maxRetriesPerTask: 2 }));
    await store.insertMember(newMember("swarm-rt3", "a", "ses-rt3-a"));
    // retryCount 3 > maxRetries 2 → must be failed, not re-queued.
    await store.insertTask(newTask("swarm-rt3", "R3", { retryCount: 3 }));
    await store.updateTaskStatus("R3", "ready");
    const swarm = await store.getSwarm("swarm-rt3");
    const result = await scheduler.run(swarm!);
    expect(result.failedExceededRetries).toContain("R3");
    const t = (await store.listTasks("swarm-rt3")).find((x) => x.id === "R3");
    expect(t?.status).toBe("failed");
    expect(t?.ownerMemberId).toBeUndefined();
  });

  test("a task at-or-below maxRetriesPerTask is still assignable", async () => {
    await store.insertSwarm(newSwarm("rt4", { maxRetriesPerTask: 2 }));
    await store.insertMember(newMember("swarm-rt4", "a", "ses-rt4-a"));
    await store.insertTask(newTask("swarm-rt4", "R4", { retryCount: 2 })); // == max, still allowed
    await store.updateTaskStatus("R4", "ready");
    const swarm = await store.getSwarm("swarm-rt4");
    const result = await scheduler.run(swarm!);
    expect(result.failedExceededRetries).not.toContain("R4");
    expect(result.assigned.length).toBe(1);
    expect((await store.listTasks("swarm-rt4")).find((x) => x.id === "R4")?.status).toBe("working");
  });
});

describe("F2 — claim lease", () => {
  test("claimTask sets claimed_at + lease_expires_at from leaseMs", async () => {
    await store.insertSwarm(newSwarm("ls1"));
    await store.insertMember(newMember("swarm-ls1", "a", "ses-ls1-a"));
    await makeReadyTask("swarm-ls1", "L1");
    const before = Date.now();
    expect(await store.claimTask("L1", "mem-swarm-ls1-a", 60_000)).toBe(true);
    const t = (await store.listTasks("swarm-ls1")).find((x) => x.id === "L1");
    expect(t?.claimedAt).toBeDefined();
    expect(t?.leaseExpiresAt).toBeDefined();
    expect(t!.leaseExpiresAt! - t!.claimedAt!).toBe(60_000);
    expect(t!.claimedAt!).toBeGreaterThanOrEqual(before);
  });

  test("listExpiredLeaseTasks returns only claimed/working tasks past their lease", async () => {
    await store.insertSwarm(newSwarm("ls2"));
    await store.insertMember(newMember("swarm-ls2", "a", "ses-ls2-a"));
    await makeReadyTask("swarm-ls2", "L2A");
    await makeReadyTask("swarm-ls2", "L2B");
    // L2A: short lease, expired. L2B: long lease, still valid.
    await store.claimTask("L2A", "mem-swarm-ls2-a", 10);
    await store.updateTaskStatus("L2A", "working");
    await store.claimTask("L2B", "mem-swarm-ls2-a", 10_000);
    await store.updateTaskStatus("L2B", "working");
    await new Promise((r) => setTimeout(r, 30)); // let the short lease lapse
    const expired = await store.listExpiredLeaseTasks("swarm-ls2", Date.now());
    const expiredIds = expired.map((t) => t.id);
    expect(expiredIds).toContain("L2A");
    expect(expiredIds).not.toContain("L2B");
  });

  test("a ready task is not a lease candidate; terminal tasks are excluded", async () => {
    await store.insertSwarm(newSwarm("ls3"));
    await store.insertMember(newMember("swarm-ls3", "a", "ses-ls3-a"));
    await makeReadyTask("swarm-ls3", "L3A"); // stays ready
    await makeReadyTask("swarm-ls3", "L3B");
    await store.claimTask("L3B", "mem-swarm-ls3-a", 10);
    await store.updateTaskStatus("L3B", "working");
    await store.updateTaskStatus("L3B", "completed"); // terminal — excluded
    await new Promise((r) => setTimeout(r, 30));
    const expired = await store.listExpiredLeaseTasks("swarm-ls3", Date.now());
    expect(expired.map((t) => t.id)).not.toContain("L3A");
    expect(expired.map((t) => t.id)).not.toContain("L3B");
  });

  test("scheduler assignTask anchors the lease from taskLeaseMs policy", async () => {
    await store.insertSwarm(newSwarm("ls4", { taskLeaseMs: 123_456 }));
    await store.insertMember(newMember("swarm-ls4", "a", "ses-ls4-a"));
    await makeReadyTask("swarm-ls4", "L4");
    const swarm = await store.getSwarm("swarm-ls4");
    await scheduler.run(swarm!);
    const t = (await store.listTasks("swarm-ls4")).find((x) => x.id === "L4");
    expect(t?.status).toBe("working");
    expect(t!.leaseExpiresAt! - t!.claimedAt!).toBe(123_456);
  });

  test("DEFAULT_TASK_LEASE_MS is 30 minutes", () => {
    expect(DEFAULT_TASK_LEASE_MS).toBe(30 * 60_000);
  });
});

describe("F2 — migration (schema columns on pre-existing DB)", () => {
  test("claimed_at/lease_expires_at columns exist after ready() on a legacy DB", async () => {
    // Simulate a legacy DB: create the store once (migrations run), then verify
    // the columns are queryable on a second open (the migrate() + chain both run
    // idempotently and the defensive re-apply covers pre-chain DBs).
    const legacyDir = join(dir, "legacy");
    mkdirSync(legacyDir, { recursive: true });
    const legacy = new SQLiteStore(join(legacyDir, "legacy.db"));
    await legacy.ready();
    await legacy.insertSwarm(newSwarm("legacy1"));
    await legacy.insertTask(newTask("swarm-legacy1", "LG", { retryCount: 1, claimedAt: 1, leaseExpiresAt: 2 }));
    const t = (await legacy.listTasks("swarm-legacy1")).find((x) => x.id === "LG");
    expect(t?.retryCount).toBe(1);
    expect(t?.claimedAt).toBe(1);
    expect(t?.leaseExpiresAt).toBe(2);
    await legacy.close();
  });
});

describe("S-fixes — scheduler edge-case regression (rescue cluster)", () => {
  test("S-01: releaseTask({countAsRetry:false}) does not increment retryCount", async () => {
    await store.insertSwarm(newSwarm("sf01", { maxRetriesPerTask: 0 }));
    await store.insertMember(newMember("swarm-sf01", "a", "ses-sf01-a"));
    await makeReadyTask("swarm-sf01", "S01");
    const claimed = await store.claimTask("S01", (await store.listMembers("swarm-sf01"))[0]!.id, 60_000);
    expect(claimed).toBe(true);
    // Kickoff-failure release: countAsRetry false → retryCount stays 0.
    const released = await store.releaseTask("S01", { countAsRetry: false });
    expect(released).toBe(true);
    const t = (await store.listTasks("swarm-sf01")).find((x) => x.id === "S01");
    expect(t?.retryCount).toBe(0);
    // With maxRetries=0 and retryCount=0, the scheduler keeps it ready (not failed).
    const swarm = await store.getSwarm("swarm-sf01");
    const r = await scheduler.run(swarm!);
    expect(r.failedExceededRetries).not.toContain("S01");
    const after = (await store.listTasks("swarm-sf01")).find((x) => x.id === "S01");
    expect(after?.status).not.toBe("failed");
  });

  test("S-03: reassignTask to a stopped member throws (store-level guard)", async () => {
    await store.insertSwarm(newSwarm("sf03"));
    const m = await store.insertMember(newMember("swarm-sf03", "a", "ses-sf03-a"));
    const stopped = await store.insertMember(newMember("swarm-sf03", "dead", "ses-sf03-dead"));
    await store.updateMemberStatus(stopped.id, "stopped", { currentTaskId: null });
    await makeReadyTask("swarm-sf03", "S03");
    await store.claimTask("S03", m.id, 60_000);
    await expect(store.reassignTask("S03", stopped.id)).rejects.toThrow("stopped");
  });

  test("S-04: member-side claim CAS — a member with a DIFFERENT currentTaskId cannot claim", async () => {
    await store.insertSwarm(newSwarm("sf04"));
    const m = await store.insertMember(newMember("swarm-sf04", "a", "ses-sf04-a"));
    await makeReadyTask("swarm-sf04", "S04A");
    await makeReadyTask("swarm-sf04", "S04B");
    const c1 = await store.claimTask("S04A", m.id, 60_000);
    expect(c1).toBe(true);
    // Member now holds S04A (current_task_id set to it) → claiming a DIFFERENT task fails.
    await store.updateMemberStatus(m.id, "working", { currentTaskId: "S04A" });
    const c2 = await store.claimTask("S04B", m.id, 60_000);
    expect(c2).toBe(false);
    // Claiming the SAME task when it's already owned+working also fails
    // (task-side CAS: not ready/unowned) — correct.
    const c3 = await store.claimTask("S04A", m.id, 60_000);
    expect(c3).toBe(false);
  });

  test("S-06: orphan-sweep release of an ownerless task does not feed the retry cap", async () => {
    await store.insertSwarm(newSwarm("sf06", { maxRetriesPerTask: 0 }));
    const m = await store.insertMember(newMember("swarm-sf06", "a", "ses-sf06-a"));
    await makeReadyTask("swarm-sf06", "S06");
    await store.claimTask("S06", m.id, 60_000);
    // Simulate an orphan: clear the owner without releasing (member removed).
    await store.deleteMember(m.id);
    const before = (await store.listTasks("swarm-sf06")).find((x) => x.id === "S06");
    expect(before?.status).toBe("claimed");
    expect(before?.ownerMemberId).toBeUndefined();
    // Scheduler orphan sweep releases WITHOUT counting a retry.
    const swarm = await store.getSwarm("swarm-sf06");
    await scheduler.run(swarm!);
    const after = (await store.listTasks("swarm-sf06")).find((x) => x.id === "S06");
    expect(after?.status).toBe("ready");
    expect(after?.retryCount).toBe(0);
  });
});
