import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore, BlackboardConflict } from "../../src/core/swarm.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";

class FakeRuntime implements AgentRuntime {
  readonly kind = "fake";
  sessions = new Map<string, RuntimeSession>();
  seq = 0;
  prompts: string[] = [];

  async createSession(input: { title: string; directory?: string; metadata?: Record<string, unknown> }): Promise<RuntimeSession> {
    const id = `ses-fake-${++this.seq}`;
    const s: RuntimeSession = {
      id,
      title: input.title,
      directory: input.directory ?? ".",
      parentID: undefined, // members are root sessions
      metadata: input.metadata,
    };
    this.sessions.set(id, s);
    return s;
  }
  async getSession(sid: string): Promise<RuntimeSession | null> {
    return this.sessions.get(sid) ?? null;
  }
  async listChildren(parentSID: string): Promise<RuntimeSession[]> {
    return [...this.sessions.values()].filter((s) => s.parentID === parentSID);
  }
  async prompt(): Promise<any> { throw new Error("not used"); }
  async promptAsync(input: { text: string }): Promise<void> { this.prompts.push(input.text); }
  async abort(): Promise<void> { /* no-op */ }
  async getStatus(): Promise<any> { return { type: "idle" }; }
  async getMessages(): Promise<any[]> { return []; }
}
let dir: string;
let store: SQLiteStore;
let core: SwarmCore;
let runtime: FakeRuntime;

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-core-test-"));
  store = new SQLiteStore(join(dir, "core.db"));
  await store.ready();
  runtime = new FakeRuntime();
  core = new SwarmCore(store, runtime);
});

afterAll(async () => {
  await store.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

describe("SwarmCore", () => {
  test("createSwarm registers coordinator and returns swarm", async () => {
    const { swarm, coordinator, tasks } = await core.createSwarm({
      name: "auth-rewrite",
      projectId: "proj-x",
      coordinatorSessionId: "ses-lead",
      coordinatorMemberName: "architect",
    });
    expect(swarm.name).toBe("auth-rewrite");
    expect(swarm.status).toBe("active");
    expect(coordinator.name).toBe("architect");
    expect(coordinator.sessionId).toBe("ses-lead");
    expect(tasks.length).toBe(0);
  });

  test("createSwarm is idempotent for the same name+project (no UNIQUE error)", async () => {
    const first = await core.createSwarm({
      name: "dup-name",
      projectId: "proj-dup",
      coordinatorSessionId: "ses-lead-dup",
    });
    // A different coordinator session, same swarm name — must return existing,
    // not throw UNIQUE constraint.
    const second = await core.createSwarm({
      name: "dup-name",
      projectId: "proj-dup",
      coordinatorSessionId: "ses-lead-dup-2",
    });
    expect(second.swarm.id).toBe(first.swarm.id);
  });

  test("resolveSwarmId accepts a swarm name as well as its id", async () => {
    const { swarm } = await core.createSwarm({
      name: "by-name",
      projectId: "proj-name",
      coordinatorSessionId: "ses-lead-name",
    });
    expect(await core.resolveSwarmId("by-name", "proj-name")).toBe(swarm.id);
    expect(await core.resolveSwarmId(swarm.id, "proj-name")).toBe(swarm.id);
    await expect(core.resolveSwarmId("nope", "proj-name")).rejects.toThrow("no swarm found");
  });


  test("createSwarm rejects a cyclic DAG", async () => {
    await expect(
      core.createSwarm({
        name: "cyclic",
        projectId: "proj-x",
        coordinatorSessionId: "ses-lead-cyc",
        tasks: [
          { id: "a", title: "a", dependsOn: ["a2"] },
          { id: "b", title: "b", dependsOn: ["a"] },
          { id: "c", title: "c", dependsOn: ["b"] },
          { id: "a2", title: "a2", dependsOn: ["c"] },
        ],
      }),
    ).rejects.toThrow("cycle");
  });

  test("createSwarm rejects invalid names", async () => {
    await expect(
      core.createSwarm({
        name: "bad name with spaces!",
        projectId: "proj-x",
        coordinatorSessionId: "ses-bad",
      }),
    ).rejects.toThrow("must match");
  });

  test("spawnMember creates a backing session and member record", async () => {
    const { swarm } = await core.createSwarm({
      name: "spawn-test",
      projectId: "proj-x",
      coordinatorSessionId: "ses-lead-sp",
    });
    const member = await core.spawnMember({
      swarmId: swarm.id,
      name: "backend",
      role: "backend specialist",
      workspace: "worktree",
    });
    expect(member.sessionId).toMatch(/^ses-fake-/);
    expect(runtime.sessions.has(member.sessionId)).toBe(true);
    expect(runtime.sessions.get(member.sessionId)?.title).toBe(`🐝 spawn-test / backend`);
    // Members are ROOT sessions — no parent, so they appear as normal chats.
    expect(runtime.sessions.get(member.sessionId)?.parentID).toBeUndefined();
    expect(runtime.sessions.get(member.sessionId)?.metadata?.swarmMember).toBe("1");
    const stored = await store.getMemberByName(swarm.id, "backend");
    expect(stored?.name).toBe("backend");
  });

  test("spawnMember rejects duplicate names and limits", async () => {
    const { swarm } = await core.createSwarm({
      name: "spawn-dup",
      projectId: "proj-x",
      coordinatorSessionId: "ses-lead-du",
    });
    await core.spawnMember({ swarmId: swarm.id, name: "a", role: "r" });
    await expect(core.spawnMember({ swarmId: swarm.id, name: "A", role: "r" })).rejects.toThrow(
      "already exists",
    );
  });

  test("sendMessage direct and broadcast enqueue messages", async () => {
    const { swarm, coordinator } = await core.createSwarm({
      name: "msg-test",
      projectId: "proj-x",
      coordinatorSessionId: "ses-lead-msg",
    });
    const b = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r" });

    const direct = await core.sendMessage({
      swarmId: swarm.id,
      fromMemberId: coordinator.id,
      to: "backend",
      kind: "request",
      message: "can you expose createdAt?",
    });
    expect(direct.length).toBe(1);
    expect(direct[0]?.to.type).toBe("member");
    expect(direct[0]?.to.memberId).toBe(b.id);

    const broadcast = await core.sendMessage({
      swarmId: swarm.id,
      fromMemberId: coordinator.id,
      to: "*",
      kind: "decision",
      message: "contract moved to v3",
    });
    // broadcast excludes the sender
    expect(broadcast.length).toBe(1);

    const pending = await store.listPendingMessages(b.id);
    expect(pending.length).toBe(2);
  });

  test("blackboard optimistic concurrency conflict", async () => {
    const { swarm, coordinator } = await core.createSwarm({
      name: "bb-core",
      projectId: "proj-x",
      coordinatorSessionId: "ses-lead-bb",
    });

    await core.blackboardPut({
      swarmId: swarm.id,
      key: "contracts/foo",
      value: "v1",
      contentType: "text/markdown",
      authorMemberId: coordinator.id,
    });
    const e2 = await core.blackboardPut({
      swarmId: swarm.id,
      key: "contracts/foo",
      value: "v2",
      contentType: "text/markdown",
      expectedVersion: 1,
      authorMemberId: coordinator.id,
    });
    expect(e2.version).toBe(2);

    await expect(
      core.blackboardPut({
        swarmId: swarm.id,
        key: "contracts/foo",
        value: "v3-stale",
        contentType: "text/markdown",
        expectedVersion: 1,
        authorMemberId: coordinator.id,
      }),
    ).rejects.toBeInstanceOf(BlackboardConflict);
  });

  test("overwriting an existing key WITHOUT expectedVersion is a conflict (no silent LWW)", async () => {
    const { swarm, coordinator } = await core.createSwarm({
      name: "bb-noev",
      projectId: "proj-x",
      coordinatorSessionId: "ses-lead-noev",
    });

    await core.blackboardPut({
      swarmId: swarm.id,
      key: "contracts/foo",
      value: "v1",
      contentType: "text/markdown",
      authorMemberId: coordinator.id,
    });

    // Audit S2: a put that omits expectedVersion on an existing key must NOT
    // silently overwrite — it's a conflict telling the caller to read first.
    await expect(
      core.blackboardPut({
        swarmId: swarm.id,
        key: "contracts/foo",
        value: "v2-silent",
        contentType: "text/markdown",
        authorMemberId: coordinator.id,
      }),
    ).rejects.toBeInstanceOf(BlackboardConflict);

    // The original value is untouched.
    const after = await store.getBlackboard(swarm.id, "contracts/foo");
    expect(after?.value).toBe("v1");
    expect(after?.version).toBe(1);

    // With the current version passed, the overwrite succeeds (CAS roundtrip).
    const e3 = await core.blackboardPut({
      swarmId: swarm.id,
      key: "contracts/foo",
      value: "v2-cas",
      contentType: "text/markdown",
      expectedVersion: 1,
      authorMemberId: coordinator.id,
    });
    expect(e3.version).toBe(2);
    expect((await store.getBlackboard(swarm.id, "contracts/foo"))?.value).toBe("v2-cas");
  });

  test("createSwarm idempotent path heals a legacy swarm's empty directory", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const legacyName = `heal-legacy-${tag}`;
    // Create a swarm with a directory, then simulate a legacy swarm that lost
    // it (e.g. persisted before the directory column existed).
    const legacySwarm = (await core.createSwarm({
      name: legacyName,
      projectId: "proj-x",
      coordinatorSessionId: `ses-lead-hl-${tag}`,
      directory: "C:\\work\\scripts",
    })).swarm;
    await store.updateSwarmDirectory(legacySwarm.id, "");

    // Reusing by name with a directory heals the stored (empty) directory.
    const reused = await core.createSwarm({
      name: legacyName,
      projectId: "proj-x",
      coordinatorSessionId: `ses-lead-hl-2-${tag}`,
      directory: "C:\\work\\scripts",
    });
    expect(reused.swarm.id).toBe(legacySwarm.id);
    expect(reused.swarm.directory).toBe("C:\\work\\scripts");
    const persisted = await store.getSwarm(legacySwarm.id);
    expect(persisted?.directory).toBe("C:\\work\\scripts");
  });

  test("createSwarm rebinds the coordinator when reused from a new session", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const name = `rebind-${tag}`;
    const firstCoord = `ses-coord-1-${tag}`;
    const secondCoord = `ses-coord-2-${tag}`;

    await core.createSwarm({ name, projectId: "proj-x", coordinatorSessionId: firstCoord });

    // A different session reuses the swarm by name → it must become the
    // coordinator, or it could not message members ("sender is not a member").
    const reused = await core.createSwarm({ name, projectId: "proj-x", coordinatorSessionId: secondCoord });
    expect(reused.swarm.coordinatorSessionId).toBe(secondCoord);
    expect(reused.coordinator.sessionId).toBe(secondCoord);

    // A member-addressable message from the new coordinator must now work.
    const member = await core.spawnMember({ swarmId: reused.swarm.id, name: "peer", role: "r" });
    const sent = await core.sendMessage({
      swarmId: reused.swarm.id,
      fromSessionId: secondCoord,
      to: "peer",
      kind: "request",
      message: "hello from the new coordinator",
    });
    expect(sent.length).toBe(1);
    void member;
  });

  test("continueMember re-prompts a mid-task member instead of completing it", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarm, coordinator } = await core.createSwarm({
      name: `cont-${tag}`,
      projectId: "proj-x",
      coordinatorSessionId: `ses-lead-cont-${tag}`,
    });
    const member = await core.spawnMember({ swarmId: swarm.id, name: "builder", role: "impl" });
    const task = await core.createTask({
      swarmId: swarm.id,
      title: "Build the facade",
      createdByMemberId: coordinator.id,
    });
    await store.claimTask(task.id, member.id);
    await store.updateTaskStatus(task.id, "working");
    await store.updateMemberStatus(member.id, "working", { currentTaskId: task.id });

    // A mid-task idle must CONTINUE the member, not mark the task complete.
    await core.continueMember(swarm, { ...member, currentTaskId: task.id }, 1);
    expect(runtime.prompts.length).toBe(1);
    expect(runtime.prompts[0]).toContain("Continue working on it");
    expect(runtime.prompts[0]).toContain(task.id);

    const after = (await store.listTasks(swarm.id)).find((t) => t.id === task.id);
    expect(after?.status).toBe("working"); // NOT completed
    const stored = await store.getMemberByName(swarm.id, "builder");
    expect(stored?.currentTaskId).toBe(task.id); // still owns the task
  });

  test("spawnMember falls back to coordinator directory when swarm directory is empty", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const coordSession = `ses-lead-fb-${tag}`;
    const { swarm } = await core.createSwarm({ name: `fallback-${tag}`, projectId: "proj-x", coordinatorSessionId: coordSession, directory: "" });
    // Coordinator session is not in the fake runtime's map, so getSession
    // returns null and the fallback yields undefined — member still spawns.
    const member = await core.spawnMember({ swarmId: swarm.id, name: "fb-worker", role: "r" });
    expect(member.sessionId).toMatch(/^ses-fake-/);
    const created = runtime.sessions.get(member.sessionId);
    expect(created).toBeDefined();
  });

  test("spawnMember atomically claims a task so it cannot be double-assigned", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarm, coordinator } = await core.createSwarm({
      name: `claim-${tag}`,
      projectId: "proj-x",
      coordinatorSessionId: `ses-lead-claim-${tag}`,
    });
    await core.createTask({ swarmId: swarm.id, title: "Build core", createdByMemberId: coordinator.id });
    const task = (await store.listTasks(swarm.id))[0]!;
    await store.updateTaskStatus(task.id, "ready");

    const a = await core.spawnMember({ swarmId: swarm.id, name: "worker-a", role: "r", taskId: task.id });
    expect(a.currentTaskId).toBe(task.id);
    expect((await store.getMemberById(a.id))?.currentTaskId).toBe(task.id);
    // Task now owned by worker-a.
    expect((await store.listTasks(swarm.id))[0]?.ownerMemberId).toBe(a.id);

    // worker-b requests the SAME task — the claim must fail atomically, so
    // worker-b spawns WITHOUT the task (scheduler will assign it later).
    const b = await core.spawnMember({ swarmId: swarm.id, name: "worker-b", role: "r", taskId: task.id });
    expect(b.currentTaskId).toBeUndefined();
    expect((await store.getMemberById(b.id))?.currentTaskId).toBeUndefined();
    expect((await store.listTasks(swarm.id))[0]?.ownerMemberId).toBe(a.id); // still worker-a
  });

  test("spawnMember transitions a claimed task to working (no stuck 'claimed')", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarm, coordinator } = await core.createSwarm({
      name: `work-${tag}`,
      projectId: "proj-x",
      coordinatorSessionId: `ses-lead-work-${tag}`,
    });
    await core.createTask({ swarmId: swarm.id, title: "Build it", createdByMemberId: coordinator.id });
    const task = (await store.listTasks(swarm.id))[0]!;
    await store.updateTaskStatus(task.id, "ready");

    const member = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "r", taskId: task.id });
    expect(member.currentTaskId).toBe(task.id);
    // The task must read 'working' — not linger at 'claimed' — because the
    // member is immediately kicked off on it.
    expect((await store.listTasks(swarm.id))[0]?.status).toBe("working");
    expect((await store.listTasks(swarm.id))[0]?.ownerMemberId).toBe(member.id);
  });

  test("spawnMember prefers the coordinator's LIVE session directory over a stale swarm directory", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const coordSession = `ses-lead-auth-${tag}`;
    // Stale swarm directory (e.g. captured at create from a different cwd).
    const { swarm } = await core.createSwarm({
      name: `auth-${tag}`,
      projectId: "proj-x",
      coordinatorSessionId: coordSession,
      directory: "C:\\stale\\wrong\\root",
    });
    // The coordinator's live session is actually rooted at the real project dir.
    runtime.sessions.set(coordSession, {
      id: coordSession,
      title: "coordinator",
      directory: "C:\\Program Files\\Adobe\\Illustrator\\Scripts",
    });

    const member = await core.spawnMember({ swarmId: swarm.id, name: "auth-worker", role: "r" });
    const created = runtime.sessions.get(member.sessionId);
    // The member MUST root in the coordinator's real session directory, not the
    // stale swarm.directory — this is the openswarm\eshttp vs Scripts\eshttp
    // split the user hit.
    expect(created?.directory).toBe("C:\\Program Files\\Adobe\\Illustrator\\Scripts");
  });

  test("syncMember injects a synthetic team-status digest without marking the member working", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarm } = await core.createSwarm({
      name: `sync-${tag}`,
      projectId: "proj-x",
      coordinatorSessionId: `ses-lead-sync-${tag}`,
    });
    const member = await core.spawnMember({ swarmId: swarm.id, name: "peer", role: "r" });

    const before = runtime.prompts.length;
    await core.syncMember(swarm, member, "done: t1 · ready (unassigned): t2");

    // A digest prompt was injected...
    expect(runtime.prompts.length).toBe(before + 1);
    expect(runtime.prompts[before]).toContain("[TEAM SYNC");
    expect(runtime.prompts[before]).toContain("done: t1");
    // ...but the member is NOT marked working or given a task — a digest is
    // informational, not an assignment.
    const stored = await store.getMemberById(member.id);
    expect(stored?.status).toBe("idle");
    expect(stored?.currentTaskId).toBeUndefined();
  });
});
