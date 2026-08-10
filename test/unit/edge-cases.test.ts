import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore, BlackboardConflict } from "../../src/core/swarm.ts";
import { Broker } from "../../src/messaging/broker.ts";
import { Recovery } from "../../src/supervisor/recovery.ts";
import { Scheduler } from "../../src/scheduler/scheduler.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";

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
let broker: Broker;

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-edge-test-"));
  store = new SQLiteStore(join(dir, "edge.db"));
  await store.ready();
  runtime = new FakeRuntime();
  core = new SwarmCore(store, runtime);
  broker = new Broker(store, runtime);
});

afterAll(async () => {
  await store.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

async function makeSwarm(policies?: Record<string, unknown>) {
  const tag = Math.random().toString(36).slice(2, 8);
  return core.createSwarm({
    name: `edge-${tag}`,
    projectId: "proj",
    coordinatorSessionId: `ses-lead-edge-${tag}`,
    policies: policies as never,
  });
}

describe("edge cases — concurrency accounting", () => {
  test("the coordinator does not count against maxConcurrentMembers", async () => {
    // maxConcurrentMembers = 2 should allow 2 WORKER members (coordinator excluded).
    const { swarm } = await makeSwarm({ maxConcurrentMembers: 2, maxMembers: 10 });
    const a = await core.spawnMember({ swarmId: swarm.id, name: "a", role: "r" });
    const b = await core.spawnMember({ swarmId: swarm.id, name: "b", role: "r" });
    expect(a.status).not.toBe("failed");
    expect(b.status).not.toBe("failed");
    // A third worker must hit the limit.
    await expect(
      core.spawnMember({ swarmId: swarm.id, name: "c", role: "r" }),
    ).rejects.toThrow("concurrency limit");
  });

  test("stopped members free roster slots (maxMembers counts live workers only)", async () => {
    const { swarm } = await makeSwarm({ maxConcurrentMembers: 10, maxMembers: 2 });
    const a = await core.spawnMember({ swarmId: swarm.id, name: "a", role: "r" });
    const b = await core.spawnMember({ swarmId: swarm.id, name: "b", role: "r" });
    // At maxMembers=2 live workers, a third is rejected...
    await expect(
      core.spawnMember({ swarmId: swarm.id, name: "c", role: "r" }),
    ).rejects.toThrow("member limit");

    // ...but stopping one frees its slot, so a replacement spawns.
    await store.updateMemberStatus(b.id, "stopped", { currentTaskId: null });
    const c = await core.spawnMember({ swarmId: swarm.id, name: "c", role: "r" });
    expect(c.status).not.toBe("failed");
  });

  test("scheduler does NOT assign a ready task to a chatting idle member (D1)", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const chatting = await core.spawnMember({ swarmId: swarm.id, name: "chatty", role: "r" });
    const free = await core.spawnMember({ swarmId: swarm.id, name: "free", role: "r" });
    const task = await core.createTask({ swarmId: swarm.id, title: "t-chat", createdByMemberId: coordinator.id });
    await store.updateTaskStatus(task.id, "ready");

    // The user is talking to `chatty` right now (within the lull window).
    await store.updateMemberHumanChat(chatting.id, Date.now());
    await store.updateMemberStatus(chatting.id, "idle");
    await store.updateMemberStatus(free.id, "idle");

    // Local runtime so this test's prompts don't pollute the shared counter.
    const schedRuntime = new FakeRuntime();
    const scheduler = new Scheduler(store, schedRuntime);
    const result = await scheduler.run(swarm);

    // The task goes to the non-chatting member, never to the chatting one.
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]?.memberName).toBe("free");
    const chattyAfter = await store.getMemberById(chatting.id);
    expect(chattyAfter?.currentTaskId).toBeUndefined();
  });

  test("scheduler assigns to a chatting member once the lull has lapsed", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const member = await core.spawnMember({ swarmId: swarm.id, name: "slowpoke", role: "r" });
    const task = await core.createTask({ swarmId: swarm.id, title: "t-lull", createdByMemberId: coordinator.id });
    await store.updateTaskStatus(task.id, "ready");

    // humanChatAt is older than the 5-min lull -> NOT chatting -> assignable.
    await store.updateMemberHumanChat(member.id, Date.now() - 400_000);
    await store.updateMemberStatus(member.id, "idle");

    const schedRuntime = new FakeRuntime();
    const scheduler = new Scheduler(store, schedRuntime);
    const result = await scheduler.run(swarm);
    expect(result.assigned.length).toBe(1);
    expect(result.assigned[0]?.memberName).toBe("slowpoke");
  });
});


describe("edge cases — transactions", () => {
  test("concurrent transactions serialize without 'nested transaction' error", async () => {
    // Two concurrent broker deliveries on the same store must not collide on
    // BEGIN IMMEDIATE.
    const { swarm, coordinator } = await makeSwarm();
    const m = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r" });
    await core.sendMessage({ swarmId: swarm.id, fromMemberId: coordinator.id, to: "backend", kind: "request", message: "m1" });
    const [d1, d2] = await Promise.all([
      broker.deliverToIdleMember(m.id, m.sessionId),
      broker.deliverToIdleMember(m.id, m.sessionId),
    ]);
    expect(d1 + d2).toBe(1); // exactly one wake wins
    expect(runtime.prompts.length).toBe(1); // exactly one prompt
  });

  test("rollback on transaction error leaves no partial writes", async () => {
    const { swarm, coordinator } = await makeSwarm();
    await expect(
      store.transaction(async (tx) => {
        await tx.insertMember({
          id: "mem-rollback-x",
          swarmId: swarm.id,
          name: "x",
          role: "r",
          sessionId: "ses-rollback-x",
          status: "idle",
          workspaceMode: "worktree",
          createdAt: Date.now(),
          updatedAt: Date.now(),
        });
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
    expect(await store.getMemberById("mem-rollback-x")).toBeUndefined();
    void coordinator;
  });
});

describe("edge cases — member lifecycle", () => {
  test("currentTaskId can be cleared with null but preserved with undefined", async () => {
    const { swarm } = await makeSwarm();
    // The task must exist and be ready for the atomic claim to succeed.
    await store.insertTask({
      id: "TLIFE",
      swarmId: swarm.id,
      title: "t",
      status: "ready",
      priority: 0,
      createdByMemberId: (await store.listMembers(swarm.id)).find((x) => x.role === "coordinator")!.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    const m = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r", taskId: "TLIFE" });
    expect((await store.getMemberById(m.id))?.currentTaskId).toBe("TLIFE");
    // undefined preserves
    await store.updateMemberStatus(m.id, "idle");
    expect((await store.getMemberById(m.id))?.currentTaskId).toBe("TLIFE");
    // null clears
    await store.updateMemberStatus(m.id, "stopped", { currentTaskId: null });
    expect((await store.getMemberById(m.id))?.currentTaskId).toBeUndefined();
  });

  test("spawnMember rolls back reservation when session create fails", async () => {
    const { swarm } = await makeSwarm();
    runtime.failCreate = true;
    try {
      await core.spawnMember({ swarmId: swarm.id, name: "ghost", role: "r" });
      expect.unreachable();
    } catch (e) {
      expect((e as Error).message).toContain("session create failed");
    }
    runtime.failCreate = false;
    const members = await store.listMembers(swarm.id);
    expect(members.some((m) => m.name === "ghost")).toBe(false);
  });

  test("spawnMember persists the real backing session id", async () => {
    const { swarm } = await makeSwarm();
    const m = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "r" });
    const stored = await store.getMemberById(m.id);
    expect(stored?.sessionId).toBe(m.sessionId);
    expect(stored?.sessionId).toMatch(/^ses-fake-/);
    // Without a prompt, a spawned member is idle and ready to receive work.
    expect(stored?.status).toBe("idle");
  });
});

describe("edge cases — messaging", () => {
  test("sendMessage to a stopped member is rejected (no zombie mail)", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const m = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r" });
    await store.updateMemberStatus(m.id, "stopped", { currentTaskId: null });
    await expect(
      core.sendMessage({ swarmId: swarm.id, fromMemberId: coordinator.id, to: "backend", kind: "request", message: "hi" }),
    ).rejects.toThrow("member is stopped");
  });

  test("broadcast skips stopped members", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const m = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r" });
    await store.updateMemberStatus(m.id, "stopped", { currentTaskId: null });
    const msgs = await core.sendMessage({ swarmId: swarm.id, fromMemberId: coordinator.id, to: "*", kind: "decision", message: "hi all" });
    expect(msgs.length).toBe(0); // only stopped member exists besides sender
  });

  test("replyToMessage rejects messages from another swarm", async () => {
    const a = await makeSwarm();
    const b = await makeSwarm();
    const am = await core.spawnMember({ swarmId: a.swarm.id, name: "alice", role: "r" });
    const bm = await core.spawnMember({ swarmId: b.swarm.id, name: "bob", role: "r" });
    const req = await core.sendMessage({ swarmId: a.swarm.id, fromMemberId: am.id, to: "alice", kind: "request", message: "q" });
    await expect(
      core.replyToMessage({ swarmId: b.swarm.id, fromMemberId: bm.id, toMessageId: req[0]!.id, message: "reply" }),
    ).rejects.toThrow("different swarm");
  });

  test("replyToMessage rejects when original sender is stopped", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const m = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r" });
    const req = await core.sendMessage({ swarmId: swarm.id, fromMemberId: coordinator.id, to: "backend", kind: "request", message: "q" });
    await store.updateMemberStatus(coordinator.id, "stopped", { currentTaskId: null });
    await expect(
      core.replyToMessage({ swarmId: swarm.id, fromMemberId: m.id, toMessageId: req[0]!.id, message: "reply" }),
    ).rejects.toThrow("is stopped");
  });

  test("publishBlackboard skips stopped subscribers", async () => {
    const { swarm, frontend, tests } = await makeSwarmWithSubs();
    await store.updateMemberStatus(frontend.id, "stopped", { currentTaskId: null });
    const msgs = await core.publishBlackboard({ swarmId: swarm.id, key: "contracts/foo", entryVersion: 2 });
    expect(msgs.length).toBe(0); // frontend stopped, tests not subscribed to contracts
    void tests;
  });

  async function makeSwarmWithSubs() {
    const s = await makeSwarm();
    const frontend = await core.spawnMember({ swarmId: s.swarm.id, name: "frontend", role: "ui" });
    const tests = await core.spawnMember({ swarmId: s.swarm.id, name: "tests", role: "qa" });
    await core.subscribe({ swarmId: s.swarm.id, memberId: frontend.id, pattern: "contracts/**" });
    await core.subscribe({ swarmId: s.swarm.id, memberId: tests.id, pattern: "findings/**" });
    return { swarm: s.swarm, frontend, tests };
  }

  test("broker claims each message exactly once under concurrent wakes", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const m = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r" });
    await core.sendMessage({ swarmId: swarm.id, fromMemberId: coordinator.id, to: "backend", kind: "request", message: "one" });
    await core.sendMessage({ swarmId: swarm.id, fromMemberId: coordinator.id, to: "backend", kind: "request", message: "two" });
    const [d1, d2] = await Promise.all([
      broker.deliverToIdleMember(m.id, m.sessionId),
      broker.deliverToIdleMember(m.id, m.sessionId),
    ]);
    expect(d1 + d2).toBe(2);
    expect(runtime.prompts.length).toBe(2); // both messages delivered, but each once
    expect((await store.listPendingMessages(m.id)).length).toBe(0);
  });
});

describe("edge cases — blackboard", () => {
  test("CAS is atomic: second concurrent writer with same expectedVersion conflicts", async () => {
    const { swarm, coordinator } = await makeSwarm();
    await core.blackboardPut({ swarmId: swarm.id, key: "contracts/foo", value: "v1", contentType: "text/markdown", authorMemberId: coordinator.id });
    const w1 = core.blackboardPut({ swarmId: swarm.id, key: "contracts/foo", value: "w1", contentType: "text/markdown", expectedVersion: 1, authorMemberId: coordinator.id });
    const w2 = core.blackboardPut({ swarmId: swarm.id, key: "contracts/foo", value: "w2", contentType: "text/markdown", expectedVersion: 1, authorMemberId: coordinator.id });
    const [r1, r2] = await Promise.allSettled([w1, w2]);
    expect(r1.status).toBe("fulfilled");
    expect(r2.status).toBe("rejected");
    if (r2.status === "rejected") expect(r2.reason).toBeInstanceOf(BlackboardConflict);
    const final = await store.getBlackboard(swarm.id, "contracts/foo");
    expect(final?.version).toBe(2);
    expect(final?.value).toBe("w1");
  });

  test("expectedVersion on a missing key is a conflict", async () => {
    const { swarm, coordinator } = await makeSwarm();
    await expect(
      core.blackboardPut({ swarmId: swarm.id, key: "contracts/missing", value: "v", contentType: "text/markdown", expectedVersion: 2, authorMemberId: coordinator.id }),
    ).rejects.toBeInstanceOf(BlackboardConflict);
  });

  test("blackboard FK rejects a non-member author id", async () => {
    const { swarm } = await makeSwarm();
    await expect(
      core.blackboardPut({ swarmId: swarm.id, key: "k", value: "v", contentType: "text/markdown", authorMemberId: "not-a-member-id" }),
    ).rejects.toThrow("FOREIGN KEY");
  });
});

describe("edge cases — recovery", () => {
  test("idle-but-present session is NOT marked interrupted", async () => {
    const { swarm } = await makeSwarm();
    const m = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r" });
    await store.updateMemberStatus(m.id, "working");
    // runtime: session exists but status map has no entry (idle drops out)
    runtime.statusOverride = null;
    const rec = new Recovery(store, runtime);
    const result = await rec.reconcileSwarm(swarm.id);
    const action = result.actions.find((a) => a.memberId === m.id);
    expect(action?.action).not.toBe("interrupted");
    expect((await store.getMemberById(m.id))?.status).toBe("working");
  });

  test("truly absent session is marked interrupted and its task released", async () => {
    const { swarm } = await makeSwarm();
    const coordId = (await store.listMembers(swarm.id)).find((x) => x.role === "coordinator")!.id;
    await store.insertTask({
      id: "TREC",
      swarmId: swarm.id,
      title: "t",
      status: "ready",
      priority: 0,
      createdByMemberId: coordId,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    const m = await core.spawnMember({ swarmId: swarm.id, name: "backend", role: "r", taskId: "TREC" });
    await store.updateMemberStatus(m.id, "working", { currentTaskId: "TREC" });
    runtime.sessions.delete(m.sessionId); // simulate gone
    runtime.statusOverride = null;
    const rec = new Recovery(store, runtime);
    const result = await rec.reconcileSwarm(swarm.id);
    const action = result.actions.find((a) => a.memberId === m.id);
    expect(action?.action).toBe("interrupted");
    expect((await store.getMemberById(m.id))?.status).toBe("interrupted");
    const task = (await store.listTasks(swarm.id)).find((t) => t.id === "TREC");
    expect(task?.status).toBe("ready");
    expect(task?.ownerMemberId).toBeUndefined();
  });

  test("absent active member is re-spawned (self-healing) when a respawn fn is provided", async () => {
    const { swarm } = await makeSwarm();
    const coordId = (await store.listMembers(swarm.id)).find((x) => x.role === "coordinator")!.id;
    await store.insertTask({
      id: "TRSP",
      swarmId: swarm.id,
      title: "t",
      status: "ready",
      priority: 0,
      createdByMemberId: coordId,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    const m = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "r", taskId: "TRSP" });
    const oldSession = m.sessionId;
    runtime.sessions.delete(oldSession); // simulate gone after restart
    runtime.statusOverride = null;

    // Respawn fn re-creates a session (simulate the plugin's respawnMember).
    let respawnedCount = 0;
    const rec = new Recovery(store, runtime, async (member) => {
      respawnedCount++;
      const s = await runtime.createSession({ title: `🐝 / ${member.name}` });
      return s.id;
    });
    const result = await rec.reconcileSwarm(swarm.id);
    const action = result.actions.find((a) => a.memberId === m.id);
    expect(action?.action).toBe("respawned");
    expect(respawnedCount).toBe(1);

    // Member now points at a NEW session and stays working on its task.
    const stored = await store.getMemberById(m.id);
    expect(stored?.sessionId).not.toBe(oldSession);
    expect(stored?.status).toBe("working");
    expect(stored?.currentTaskId).toBe("TRSP");
  });
});

describe("edge cases — task DAG", () => {
  test("explicit-id dependencies work", async () => {
    const { swarm } = await makeSwarm();
    const tasks = (await store.listTasks(swarm.id));
    void tasks;
    const r = await core.createSwarm({
      name: `dag-${Date.now()}`,
      projectId: "proj",
      coordinatorSessionId: `ses-lead-dag-${Date.now()}`,
      tasks: [
        { id: "research", title: "r" },
        { id: "implement", title: "i", dependsOn: ["research"] },
      ],
    });
    const deps = await store.listTaskDependencies(r.swarm.id);
    expect(deps).toContainEqual({ taskId: "implement", dependsOnTaskId: "research" });
  });

  test("auto-id tasks cannot be dependency targets (clear error)", async () => {
    await expect(
      core.createSwarm({
        name: `dagbad-${Date.now()}`,
        projectId: "proj",
        coordinatorSessionId: `ses-lead-dagbad-${Date.now()}`,
        tasks: [
          { title: "research" },
          { title: "implement", dependsOn: ["research"] },
        ],
      }),
    ).rejects.toThrow("must reference an explicit task id");
  });
});
