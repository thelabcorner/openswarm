import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Scheduler, claimOverlapsTask } from "../../src/scheduler/scheduler.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";
import type { NewSwarm, NewSwarmMember, NewTask } from "../../src/storage/models.ts";

/**
 * Wave 3 WIP Aura (H0) regression tests:
 *  - refreshPathClaim heartbeat: extends expires_at; re-activates a released
 *    claim; undefined for a missing claim.
 *  - claims expire after no heartbeat (listPathClaims excludes stale).
 *  - advisory claim-aware scheduling: Scheduler.run emits claimWarnings for
 *    ready tasks overlapping another member's active claim (no hard skip);
 *    no warning for the holder's own task or non-overlapping lanes.
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
  dir = mkdtempSync(join(tmpdir(), "swarms-aura-test-"));
  store = new SQLiteStore(join(dir, "aura.db"));
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

function newTask(swarmId: string, id: string, title: string): NewTask {
  return {
    id,
    swarmId,
    title,
    status: "ready",
    priority: 0,
    createdByMemberId: `mem-coord-${swarmId}`,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
}

describe("WIP Aura — claim heartbeat (refreshPathClaim)", () => {
  test("refresh extends expires_at and the claim stays active", async () => {
    await store.insertSwarm(newSwarm("aura1"));
    await store.insertMember(newMember("swarm-aura1", "a", "ses-aura1-a"));
    const claim = await store.insertPathClaim({
      id: "aura-1",
      swarmId: "swarm-aura1",
      memberId: "mem-swarm-aura1-a",
      pattern: "src/**",
      mode: "advisory",
      createdAt: Date.now(),
      expiresAt: Date.now() + 10_000,
    });
    const refreshed = await store.refreshPathClaim("aura-1", 60_000);
    expect(refreshed).toBeDefined();
    expect(refreshed!.expiresAt).toBeGreaterThan(claim.expiresAt!);
    expect(refreshed!.expiresAt! - Date.now()).toBeGreaterThan(50_000);
    // Still active.
    const active = await store.listPathClaims("swarm-aura1");
    expect(active.map((c) => c.id)).toContain("aura-1");
  });

  test("refresh re-activates a released claim (owner still working the lane)", async () => {
    await store.insertSwarm(newSwarm("aura2"));
    await store.insertMember(newMember("swarm-aura2", "a", "ses-aura2-a"));
    await store.insertPathClaim({
      id: "aura-2",
      swarmId: "swarm-aura2",
      memberId: "mem-swarm-aura2-a",
      pattern: "packaging",
      mode: "advisory",
      createdAt: Date.now(),
      expiresAt: Date.now() + 10_000,
    });
    expect(await store.releasePathClaim("aura-2")).toBe(true);
    expect((await store.listPathClaims("swarm-aura2")).length).toBe(0);
    const refreshed = await store.refreshPathClaim("aura-2", 30_000);
    expect(refreshed).toBeDefined();
    expect(refreshed!.releasedAt).toBeUndefined();
    // Active again.
    const active = await store.listPathClaims("swarm-aura2");
    expect(active.map((c) => c.id)).toContain("aura-2");
  });

  test("refresh of a missing claim returns undefined", async () => {
    expect(await store.refreshPathClaim("aura-nope", 1000)).toBeUndefined();
  });

  test("claims expire after no heartbeat (listPathClaims excludes stale)", async () => {
    await store.insertSwarm(newSwarm("aura3"));
    await store.insertMember(newMember("swarm-aura3", "a", "ses-aura3-a"));
    await store.insertPathClaim({
      id: "aura-3",
      swarmId: "swarm-aura3",
      memberId: "mem-swarm-aura3-a",
      pattern: "docs/**",
      mode: "advisory",
      createdAt: Date.now() - 5_000,
      expiresAt: Date.now() - 1_000, // already expired
    });
    // Active set (default now) excludes it.
    expect((await store.listPathClaims("swarm-aura3")).length).toBe(0);
    // A heartbeat refreshes it back into the active set.
    await store.refreshPathClaim("aura-3", 30_000);
    expect((await store.listPathClaims("swarm-aura3")).length).toBe(1);
  });
});

describe("WIP Aura — advisory claim-aware scheduling", () => {
  test("scheduler emits a claimWarning when a ready task overlaps another member's active claim (no hard skip)", async () => {
    await store.insertSwarm(newSwarm("aura4"));
    await store.insertMember(newMember("swarm-aura4", "a", "ses-aura4-a"));
    await store.insertMember(newMember("swarm-aura4", "b", "ses-aura4-b"));
    // A holds "nibble wire" claim; B is idle.
    await store.insertPathClaim({
      id: "aura-4",
      swarmId: "swarm-aura4",
      memberId: "mem-swarm-aura4-a",
      pattern: "nibble wire",
      mode: "advisory",
      createdAt: Date.now(),
      expiresAt: Date.now() + 60_000,
    });
    // Ready task about the nibble wire lane → overlap with A's claim.
    await store.insertTask(newTask("swarm-aura4", "T4", "pack the nibble wire v3"));
    await store.updateTaskStatus("T4", "ready");
    const swarm = await store.getSwarm("swarm-aura4");
    const claims = await store.listPathClaims("swarm-aura4");
    const result = await scheduler.run(swarm!, { activeClaims: claims });
    // Advisory: the task is still assigned (B takes it), but a warning is emitted.
    expect(result.assigned.length).toBe(1);
    expect(result.claimWarnings.length).toBe(1);
    expect(result.claimWarnings[0]!.taskId).toBe("T4");
    expect(result.claimWarnings[0]!.pattern).toBe("nibble wire");
    expect(result.claimWarnings[0]!.holderMemberId).toBe("mem-swarm-aura4-a");
  });

  test("no warning when the ready task does NOT overlap any active claim", async () => {
    await store.insertSwarm(newSwarm("aura5"));
    await store.insertMember(newMember("swarm-aura5", "a", "ses-aura5-a"));
    await store.insertMember(newMember("swarm-aura5", "b", "ses-aura5-b"));
    await store.insertPathClaim({
      id: "aura-5",
      swarmId: "swarm-aura5",
      memberId: "mem-swarm-aura5-a",
      pattern: "packaging",
      mode: "advisory",
      createdAt: Date.now(),
      expiresAt: Date.now() + 60_000,
    });
    await store.insertTask(newTask("swarm-aura5", "T5", "write release notes"));
    await store.updateTaskStatus("T5", "ready");
    const swarm = await store.getSwarm("swarm-aura5");
    const claims = await store.listPathClaims("swarm-aura5");
    const result = await scheduler.run(swarm!, { activeClaims: claims });
    expect(result.claimWarnings.length).toBe(0);
  });

  test("a task matching the holder's OWN claim does not warn", async () => {
    await store.insertSwarm(newSwarm("aura6"));
    await store.insertMember(newMember("swarm-aura6", "a", "ses-aura6-a"));
    // A holds the claim AND owns the ready task (owner check skips own claim).
    await store.insertPathClaim({
      id: "aura-6",
      swarmId: "swarm-aura6",
      memberId: "mem-swarm-aura6-a",
      pattern: "backend",
      mode: "advisory",
      createdAt: Date.now(),
      expiresAt: Date.now() + 60_000,
    });
    await store.insertTask(newTask("swarm-aura6", "T6", "build the backend API"));
    await store.claimTask("T6", "mem-swarm-aura6-a", 60_000);
    await store.updateTaskStatus("T6", "working");
    await store.updateMemberStatus("mem-swarm-aura6-a", "working", { currentTaskId: "T6" });
    const swarm = await store.getSwarm("swarm-aura6");
    const claims = await store.listPathClaims("swarm-aura6");
    const result = await scheduler.run(swarm!, { activeClaims: claims });
    expect(result.claimWarnings.length).toBe(0); // holder's own lane — no warning
  });
});

describe("WIP Aura — claimOverlapsTask heuristic", () => {
  test("verbatim pattern and shared tokens both match", () => {
    expect(claimOverlapsTask("nibble wire", { title: "pack the nibble wire", description: "" })).toBe(true);
    expect(claimOverlapsTask("packaging", { title: "run the packaging lane", description: "" })).toBe(true);
    expect(claimOverlapsTask("src/**", { title: "touch files under src", description: "" })).toBe(true);
    expect(claimOverlapsTask("unrelated", { title: "release notes", description: "" })).toBe(false);
  });
});
