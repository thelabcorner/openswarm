import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Supervisor } from "../../src/supervisor/supervisor.ts";
import { topicMatches } from "../../src/core/types.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";

class FakeRuntime implements AgentRuntime {
  readonly kind = "fake";
  sessions = new Map<string, RuntimeSession>();
  seq = 0;
  async createSession(input: { title: string }): Promise<RuntimeSession> {
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
  async promptAsync(): Promise<void> {}
  async abort(): Promise<void> {}
  async getStatus(): Promise<any> { return { type: "idle" }; }
  async getMessages(): Promise<any[]> { return []; }
}

let dir: string;
let store: SQLiteStore;
let core: SwarmCore;
let runtime: FakeRuntime;

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-sub-test-"));
  store = new SQLiteStore(join(dir, "sub.db"));
  await store.ready();
  runtime = new FakeRuntime();
  core = new SwarmCore(store, runtime);
});

afterAll(async () => {
  await store.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

describe("topicMatches glob", () => {
  const cases: Array<[string, string, boolean]> = [
    ["contracts/**", "contracts/auth/refresh-v3", true],
    ["contracts/**", "contracts/foo", true],
    ["contracts/**", "decisions/ui/foo", false],
    ["contracts/api/*", "contracts/api/auth", true],
    ["contracts/api/*", "contracts/api/auth/extra", false],
    ["decisions/ui/**", "decisions/ui/foo", true],
    ["contracts/**", "contracts", true], // ** can match zero segments
    ["contracts/**", "contracts/a/b/c/d", true],
    ["findings/*", "findings/refresh-race", true],
    ["findings/*", "findings/a/b", false],
  ];
  for (const [pattern, topic, expected] of cases) {
    test(`"${pattern}" matches "${topic}" = ${expected}`, () => {
      expect(topicMatches(pattern, topic)).toBe(expected);
    });
  }
});

describe("subscriptions + pub/sub routing", () => {
  async function makeSwarm(): Promise<{ swarmId: string; coordinatorId: string; frontendId: string; testsId: string }> {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarm, coordinator } = await core.createSwarm({
      name: `sub-${tag}`,
      projectId: "proj",
      coordinatorSessionId: `ses-lead-sub-${tag}`,
    });
    const frontend = await core.spawnMember({ swarmId: swarm.id, name: "frontend", role: "ui" });
    const tests = await core.spawnMember({ swarmId: swarm.id, name: "tests", role: "qa" });
    return { swarmId: swarm.id, coordinatorId: coordinator.id, frontendId: frontend.id, testsId: tests.id };
  }

  test("frontend subscribes to contracts/**, tests to findings/**", async () => {
    const { swarmId, frontendId, testsId } = await makeSwarm();
    const f = await core.subscribe({ swarmId, memberId: frontendId, pattern: "contracts/**" });
    const t = await core.subscribe({ swarmId, memberId: testsId, pattern: "findings/**" });
    expect(f.pattern).toBe("contracts/**");
    expect(t.pattern).toBe("findings/**");
  });

  test("publish notifies only interested subscribers", async () => {
    const { swarmId, coordinatorId, frontendId, testsId } = await makeSwarm();
    await core.subscribe({ swarmId, memberId: frontendId, pattern: "contracts/**" });
    await core.subscribe({ swarmId, memberId: testsId, pattern: "findings/**" });

    // backend blackboard update: only frontend cares
    const contractMsgs = await core.publishBlackboard({ swarmId, key: "contracts/auth/refresh-v3", entryVersion: 3 });
    expect(contractMsgs.length).toBe(1);
    expect(contractMsgs[0]?.to.memberId).toBe(frontendId);
    expect(contractMsgs[0]?.body.refs?.[0]).toBe("blackboard://contracts/auth/refresh-v3");

    // findings update: only tests cares
    const findingMsgs = await core.publishBlackboard({ swarmId, key: "findings/refresh-race", entryVersion: 1 });
    expect(findingMsgs.length).toBe(1);
    expect(findingMsgs[0]?.to.memberId).toBe(testsId);

    // unrelated key: nobody notified
    const none = await core.publishBlackboard({ swarmId, key: "decisions/ui/foo", entryVersion: 1 });
    expect(none.length).toBe(0);
    void coordinatorId;
  });

  test("publish includes the value so subscribers get content without a get", async () => {
    const { swarmId, frontendId } = await makeSwarm();
    await core.subscribe({ swarmId, memberId: frontendId, pattern: "contracts/**" });
    const msgs = await core.publishBlackboard({
      swarmId,
      key: "contracts/api/v1",
      entryVersion: 2,
      value: "The API surface: get/post/put",
    });
    expect(msgs.length).toBe(1);
    // The notification body carries the content — hive-mind, no extra round trip.
    expect(msgs[0]?.body.text).toContain("The API surface: get/post/put");
  });

  test("unsubscribe removes routing", async () => {
    const { swarmId, frontendId, testsId } = await makeSwarm();
    const f = await core.subscribe({ swarmId, memberId: frontendId, pattern: "contracts/**" });
    await core.unsubscribe(f.id);
    const msgs = await core.publishBlackboard({ swarmId, key: "contracts/foo", entryVersion: 2 });
    // tests is not subscribed to contracts, so no notifications
    expect(msgs.length).toBe(0);
    void testsId;
  });
});

describe("supervisor failure recovery", () => {
  async function makeSwarmWithClaimedTask(): Promise<{ swarmId: string; memberId: string; memberSession: string; taskId: string }> {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarm, coordinator } = await core.createSwarm({
      name: `err-${tag}`,
      projectId: "proj",
      coordinatorSessionId: `ses-lead-err-${tag}`,
    });
    const member = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "r" });
    const task = await core.createTask({ swarmId: swarm.id, title: "t", createdByMemberId: coordinator.id });
    await store.updateTaskStatus(task.id, "ready");
    await store.claimTask(task.id, member.id);
    await store.updateMemberStatus(member.id, "working", { currentTaskId: task.id });
    return { swarmId: swarm.id, memberId: member.id, memberSession: member.sessionId, taskId: task.id };
  }

  test("session.error releases the task (countAsRetry:false) and puts the member IDLE (t-sched-robustness)", async () => {
    const supervisor = new Supervisor(store);
    const { swarmId, memberId, memberSession, taskId } = await makeSwarmWithClaimedTask();

    const effects = await supervisor.onOpenCodeEvent({
      type: "session.error",
      properties: { sessionID: memberSession, error: { name: "ProviderError", message: "upstream timeout" } },
    });
    expect(effects.notifyCoordinator).toBe(true);
    expect(effects.releasedTaskIds).toContain(taskId);

    const member = await store.getMemberById(memberId);
    // A session/provider error is SYSTEMIC — the member is NOT failed (that
    // would make it non-resumable) but IDLE so the scheduler re-assigns it.
    expect(member?.status).toBe("idle");
    expect(member?.currentTaskId).toBeUndefined();
    // The task is released back to ready so another member (or this one) can
    // claim it — WITHOUT consuming the retry budget.
    const task = (await store.listTasks(swarmId)).find((t) => t.id === taskId);
    expect(task?.status).toBe("ready");
    expect(task?.ownerMemberId).toBeUndefined();
    expect(task?.retryCount).toBe(0);
  });

  test("session.error from an ABORT keeps the member interrupted, task claimed, no coordinator panic", async () => {
    const supervisor = new Supervisor(store);
    const { swarmId, memberId, memberSession, taskId } = await makeSwarmWithClaimedTask();

    const effects = await supervisor.onOpenCodeEvent({
      type: "session.error",
      properties: { sessionID: memberSession, error: { name: "AbortedError", message: "The run was aborted" } },
    });
    expect(effects.notifyCoordinator).toBe(false);

    const member = await store.getMemberById(memberId);
    // Not failed — the user manually stopped the chat. The task is retained.
    expect(member?.status).toBe("interrupted");
    expect(member?.currentTaskId).toBe(taskId);
    const task = (await store.listTasks(swarmId)).find((t) => t.id === taskId);
    expect(task?.status).not.toBe("ready"); // NOT released for reassignment
  });

  test("isAbortError classifies abort/cancel/interrupt payloads", async () => {
    const { isAbortError } = await import("../../src/supervisor/supervisor.ts");
    expect(isAbortError({ name: "AbortedError", message: "The run was aborted" })).toBe(true);
    expect(isAbortError("AbortError")).toBe(true);
    expect(isAbortError("User cancelled the run")).toBe(true);
    expect(isAbortError({ message: "interrupted" })).toBe(true);
    expect(isAbortError("boom")).toBe(false);
    expect(isAbortError({ message: "rate limit exceeded" })).toBe(false);
  });

  test("session.deleted releases the member's owned task (D2 regression)", async () => {
    // User deletes the member's chat in the Desktop app: the task must return
    // to ready so the DAG advances, not dead-lock with a stopped owner.
    const supervisor = new Supervisor(store);
    const { swarmId, memberId, memberSession, taskId } = await makeSwarmWithClaimedTask();

    const effects = await supervisor.onOpenCodeEvent({
      type: "session.deleted",
      properties: { sessionID: memberSession },
    });

    const member = await store.getMemberById(memberId);
    expect(member?.status).toBe("stopped");
    expect(member?.currentTaskId).toBeUndefined();
    const task = (await store.listTasks(swarmId)).find((t) => t.id === taskId);
    expect(task?.status).toBe("ready");
    expect(task?.ownerMemberId).toBeUndefined();
    expect(effects.notifyCoordinator).toBe(false);
  });
});
