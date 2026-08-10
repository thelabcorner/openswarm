import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Scheduler } from "../../src/scheduler/scheduler.ts";
import { HumanChatTracker } from "../../src/humanchat/tracker.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";

/**
 * P0 task-lifecycle bundle regression tests (build-p0-task-lifecycle):
 *  - F12: scheduler kickoff prompt is classified as a SELF injection (not a
 *    human message) by the human-chat tracker — kills spurious 👤 chatting.
 *  - F4: capacity counts only task-bearing working members; a working-with-
 *    no-task member does not starve real assignment.
 *  - F4: the scheduler's member-side claim CAS keeps one member from being
 *    handed two different tasks by concurrent passes (F7-adjacent guard).
 *  - F1: pull-claim (claimTask) leaves the task claimable/consistent so a
 *    subsequent full-transition claim works end-to-end.
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
  dir = mkdtempSync(join(tmpdir(), "swarms-sched-bundle-"));
  store = new SQLiteStore(join(dir, "bundle.db"));
  await store.ready();
  runtime = new FakeRuntime();
  core = new SwarmCore(store, runtime);
  scheduler = new Scheduler(store, runtime);
});

afterAll(async () => {
  await store.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

async function makeSwarm(policies?: Record<string, unknown>) {
  const tag = Math.random().toString(36).slice(2, 8);
  return core.createSwarm({
    name: `sched-${tag}`,
    projectId: "proj",
    coordinatorSessionId: `ses-lead-sched-${tag}`,
    policies: policies as never,
  });
}

describe("F12 — kickoff prompt self-injection classification", () => {
  test("buildAssignmentPrompt output matches the tracker's 'You are `' prefix (not misread as human)", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const member = await core.spawnMember({ swarmId: swarm.id, name: "builder", role: "impl" });
    const task = await core.createTask({
      swarmId: swarm.id,
      title: "Build the facade",
      description: "Make it pretty",
      createdByMemberId: coordinator.id,
    });
    await store.updateTaskStatus(task.id, "ready");
    await store.updateTaskStatus(task.id, "working");
    // Attach acceptance criteria via metadata-free direct update path is not
    // available; the prompt still carries the task id + description.
    const prompt = await scheduler.buildAssignmentPrompt(swarm, { ...member, currentTaskId: task.id }, { ...task, status: "working" });

    // The leading shape must be `You are `name`` — HumanChatTracker matches
    // `You are \`` (WITH backtick) to classify plugin injections as self.
    expect(prompt.startsWith("You are `")).toBe(true);
    expect(prompt).toContain("[ASSIGNED TASK");

    // The full tracker classification: this must be a SELF injection, never a
    // human message (a false human classification sets humanChatAt and yields
    // swarm machinery for the 5-minute lull).
    const tracker = new HumanChatTracker(
      { store: undefined as never, now: Date.now },
      { selfInjectionIds: new Set<string>() },
    );
    expect(tracker.isSelfInjection(undefined, prompt)).toBe(true);
    expect(tracker.isSelfInjection(undefined, "You are builder, impl in swarm x")).toBe(false);
  });
});

describe("F4 — working-with-no-task limbo", () => {
  test("capacity counts only task-bearing working members (taskless working member does not starve assignment)", async () => {
    // maxConcurrentMembers = 1: with ONE idle member and ONE ready task, the
    // scheduler must assign despite a separate taskless 'working' member that
    // (before the fix) consumed the only capacity slot.
    const { swarm, coordinator } = await makeSwarm({ maxConcurrentMembers: 1, maxMembers: 10 });
    const idleMember = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "r" });
    // A second member marked 'working' with NO currentTaskId (limbo): must NOT
    // consume capacity.
    await store.insertMember({
      id: "mem-limbo",
      swarmId: swarm.id,
      name: "limbo",
      role: "r",
      sessionId: "ses-limbo",
      status: "working",
      workspaceMode: "worktree",
      createdAt: Date.now(),
      updatedAt: Date.now(),
      lastActiveAt: Date.now() - 60_000,
    });
    await store.insertTask({
      id: "T-ONLY",
      swarmId: swarm.id,
      title: "only task",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });

    const result = await scheduler.run(swarm);
    expect(result.assigned.length).toBe(1); // the ready task WAS assigned
    const t = (await store.listTasks(swarm.id)).find((x) => x.id === "T-ONLY");
    expect(t?.ownerMemberId).toBe(idleMember.id);
    expect(t?.status).toBe("working");
    // The limbo member stays untouched (demotion is the watchdog's job).
    const limbo = await store.getMemberById("mem-limbo");
    expect(limbo?.status).toBe("working");
    expect(limbo?.currentTaskId).toBeUndefined();
  });

  test("a genuinely task-bearing working member DOES consume capacity", async () => {
    const { swarm, coordinator } = await makeSwarm({ maxConcurrentMembers: 1, maxMembers: 10 });
    const idleMember = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "r" });
    // A second member working WITH a task — inserted directly (spawn under
    // maxConcurrentMembers=1 would reject it) — must consume the only slot.
    await store.insertMember({
      id: "mem-busy",
      swarmId: swarm.id,
      name: "busy",
      role: "r",
      sessionId: "ses-busy",
      status: "working",
      workspaceMode: "worktree",
      createdAt: Date.now(),
      updatedAt: Date.now(),
      lastActiveAt: Date.now(),
    });
    await store.insertTask({
      id: "BUSY-T",
      swarmId: swarm.id,
      title: "busy task",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    await store.claimTask("BUSY-T", "mem-busy");
    await store.updateTaskStatus("BUSY-T", "working");
    await store.updateMemberStatus("mem-busy", "working", { currentTaskId: "BUSY-T" });

    await store.insertTask({
      id: "WAIT-T",
      swarmId: swarm.id,
      title: "waiting task",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });

    const result = await scheduler.run(swarm);
    // Capacity 1 fully consumed by the task-bearing member → no assignment,
    // even though an idle member exists.
    expect(result.assigned.length).toBe(0);
    expect(result.readyUnassigned).toContain("WAIT-T");
    expect((await store.listTasks(swarm.id)).find((t) => t.id === "WAIT-T")?.ownerMemberId).toBeUndefined();
    void idleMember;
  });
});

describe("F7-adjacent — member-side claim CAS", () => {
  test("claimTask binds a task to one member; a second claim of a DIFFERENT task while member is working is prevented by the member check in scheduler.assignTask", async () => {
    const { swarm, coordinator } = await makeSwarm({ maxConcurrentMembers: 10, maxMembers: 10 });
    const member = await core.spawnMember({ swarmId: swarm.id, name: "m", role: "r" });
    await store.insertTask({
      id: "A",
      swarmId: swarm.id,
      title: "A",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    await store.insertTask({
      id: "B",
      swarmId: swarm.id,
      title: "B",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    // First claim succeeds.
    expect(await store.claimTask("A", member.id)).toBe(true);
    // Second claim on a different task ALSO succeeds at the store level — the
    // member-side guard lives in the scheduler (it only picks idle members).
    // Assert the scheduler never offers a task to a member that is already
    // working (its idle filter + claim CAS keep it consistent).
    await store.updateMemberStatus(member.id, "working", { currentTaskId: "A" });
    await store.updateTaskStatus("A", "working");

    const result = await scheduler.run(swarm);
    // B stays unassigned: the only member is working (not idle), so the
    // scheduler must NOT hand B to it.
    expect(result.assigned.length).toBe(0);
    expect(result.readyUnassigned).toContain("B");
    expect((await store.listTasks(swarm.id)).find((t) => t.id === "B")?.ownerMemberId).toBeUndefined();
  });
});

describe("R3 — idle-with-task corruption guard", () => {
  test("scheduler does NOT assign a ready task to an idle member whose currentTaskId is set", async () => {
    const { swarm, coordinator } = await makeSwarm({ maxConcurrentMembers: 10, maxMembers: 10 });
    const member = await core.spawnMember({ swarmId: swarm.id, name: "corrupt", role: "r" });
    await store.insertTask({
      id: "OLD",
      swarmId: swarm.id,
      title: "old task",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    // claimTask requires status='ready' — claim, then move to working.
    expect(await store.claimTask("OLD", member.id)).toBe(true);
    await store.updateTaskStatus("OLD", "working");
    // Simulate the corruption: member status flipped to idle (recovery/lost
    // working flag) while currentTaskId is still bound to OLD.
    await store.updateMemberStatus(member.id, "idle", { currentTaskId: "OLD" });

    await store.insertTask({
      id: "NEW",
      swarmId: swarm.id,
      title: "new task",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });

    const result = await scheduler.run(swarm);
    // The idle-with-task member must NOT receive NEW (R3).
    expect(result.assigned.length).toBe(0);
    expect(result.readyUnassigned).toContain("NEW");
    const newTask = (await store.listTasks(swarm.id)).find((t) => t.id === "NEW");
    expect(newTask?.ownerMemberId).toBeUndefined();
    expect(newTask?.status).toBe("ready");
    // Its old binding is untouched.
    const old = (await store.listTasks(swarm.id)).find((t) => t.id === "OLD");
    expect(old?.ownerMemberId).toBe(member.id);
    const m = await store.getMemberById(member.id);
    expect(m?.currentTaskId).toBe("OLD");
  });

  test("an idle member WITHOUT currentTaskId still receives assignments (R3 does not over-block)", async () => {
    const { swarm, coordinator } = await makeSwarm({ maxConcurrentMembers: 10, maxMembers: 10 });
    const member = await core.spawnMember({ swarmId: swarm.id, name: "free", role: "r" });
    await store.insertTask({
      id: "FRESH",
      swarmId: swarm.id,
      title: "fresh task",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });

    const result = await scheduler.run(swarm);
    expect(result.assigned.length).toBe(1);
    const t = (await store.listTasks(swarm.id)).find((x) => x.id === "FRESH");
    expect(t?.ownerMemberId).toBe(member.id);
    expect(t?.status).toBe("working");
  });
});

describe("F1 — pull-claim full transition (store contract)", () => {
  test("a claimed task that gets a full transition ends working + owned (the plugin's swarm_tasks claim path does this)", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const member = await core.spawnMember({ swarmId: swarm.id, name: "puller", role: "r" });
    await store.insertTask({
      id: "PULL",
      swarmId: swarm.id,
      title: "pull me",
      status: "ready",
      priority: 0,
      createdByMemberId: coordinator.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });

    // The plugin's claim handler (F1 fix) does: claimTask → member working +
    // currentTaskId → task working → kickoff prompt → scheduler run.
    expect(await store.claimTask("PULL", member.id)).toBe(true);
    await store.updateMemberStatus(member.id, "working", { currentTaskId: "PULL", lastActiveAt: Date.now() });
    await store.updateTaskStatus("PULL", "working");
    const prompt = await scheduler.buildAssignmentPrompt(swarm, { ...member, currentTaskId: "PULL" }, (await store.listTasks(swarm.id)).find((t) => t.id === "PULL")!);
    expect(prompt).toContain("[ASSIGNED TASK PULL]");

    const t = (await store.listTasks(swarm.id)).find((x) => x.id === "PULL");
    expect(t?.status).toBe("working");
    expect(t?.ownerMemberId).toBe(member.id);
    const m = await store.getMemberById(member.id);
    expect(m?.currentTaskId).toBe("PULL");
    expect(m?.status).toBe("working");
    // No claimed-with-idle-owner row persists after a scheduler pass.
    const after = await scheduler.run(swarm);
    const stale = (await store.listTasks(swarm.id)).filter((x) => x.status === "claimed" && !x.ownerMemberId);
    expect(stale.length).toBe(0);
    expect(after.assigned.length).toBe(0); // nothing else ready + member busy
  });
});
