import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Broker } from "../../src/messaging/broker.ts";
import { Scheduler } from "../../src/scheduler/scheduler.ts";
import { Recovery } from "../../src/supervisor/recovery.ts";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * External guest messaging (t-guest-messaging) — verification tests for the
 * seamless auto-registration of NON-swarm chat sessions:
 *
 *   a. non-member session's first send → guest row auto-created (role guest,
 *      name 'guest-<short>', status idle, workspaceMode shared-read), message
 *      delivered, recipient's inbox renders the guest by name.
 *   b. the same session sends again → NO duplicate guest row (idempotent).
 *   c. guest replies (replyToMessage) → reply delivered to the ORIGINAL sender,
 *      thread intact (responseTo + correlationId).
 *   d. a swarm member messages the guest BY NAME → the guest's session gets an
 *      inbox prompt (delivered through the normal broker path).
 *   e. coordinator-only tools stay blocked for guests (delegate/stop/revive/
 *      delete/retry) — plugin-tool harness.
 *   f. the scheduler NEVER assigns tasks to a guest (guest excluded even when
 *      it is the only idle member).
 *   g. recovery does NOT respawn a guest (absent session + reconcile → guest
 *      untouched).
 *   h. allowExternalGuests=false → clear rejection, no row created.
 *   i. the SAME session is a guest in TWO swarms → two rows, messages isolated
 *      (per-swarm (sessionId, swarmId) resolution).
 *   j. the cross-swarm force path is unaffected: a REGISTERED member of swarm A
 *      force-messaging swarm B is NOT registered as a guest of B, and a
 *      non-force send still gets the cross-swarm hint.
 *   k. removed-member contract (t-remove-grace): a session REMOVED from the
 *      swarm gets the orphan error, NOT a silent guest resurrection.
 *
 * Two harnesses, mirroring cross-swarm.test.ts: a core+Broker block (precise
 * row/delivery semantics, file-local FakeRuntime records prompts) and a
 * plugin-tool block (real swarm_message / swarm_reply / coordinator-only
 * surfaces with resolveSwarmId + fromSessionId).
 */

// ---------------------------------------------------------------------------
// Part 1 — core + broker harness (row-level semantics + broker prompt text)
// ---------------------------------------------------------------------------

class FakeRuntime implements AgentRuntime {
  readonly kind = "fake";
  sessions = new Map<string, RuntimeSession>();
  prompts: Array<{ sessionID: string; text: string }> = [];
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
  async promptAsync(input: { text: string }, sessionID: string): Promise<void> {
    this.prompts.push({ sessionID, text: input.text });
  }
  async abort(): Promise<void> {}
  async getStatus(): Promise<any> { return { type: "idle" }; }
  async getMessages(): Promise<any[]> { return []; }
  async listModels(): Promise<any[]> { return []; }
  async resolveModel(): Promise<any> { return undefined; }
}

let dir: string;
let store: SQLiteStore;
let runtime: FakeRuntime;
let core: SwarmCore;
let broker: Broker;

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-guest-core-"));
  store = new SQLiteStore(join(dir, "guest.db"));
  await store.ready();
  runtime = new FakeRuntime();
  core = new SwarmCore(store, runtime);
  broker = new Broker(store, runtime, { deliveryCooldownMs: 0 });
  core.setWakeDeliverer((memberId, sessionId) => broker.deliverToIdleMember(memberId, sessionId));
});

afterAll(async () => {
  await store.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

/** A swarm with coordinator + one worker ('w1', role worker, status idle). */
async function makeSwarm(tag: string): Promise<{
  id: string;
  name: string;
  coordinatorSession: string;
  worker: { id: string; name: string; sessionId: string };
}> {
  const name = `gs-${tag}-${Math.random().toString(36).slice(2, 6)}`;
  const coordinatorSession = `ses-${tag}-co`;
  const created = await core.createSwarm({ name, projectId: "proj-guest", coordinatorSessionId: coordinatorSession });
  const worker = await core.spawnMember({ swarmId: created.swarm.id, name: "w1", role: "worker", workspace: "shared-read" });
  await store.updateMemberStatus(worker.id, "idle");
  return {
    id: created.swarm.id,
    name: created.swarm.name,
    coordinatorSession,
    worker: { id: worker.id, name: worker.name, sessionId: worker.sessionId },
  };
}

describe("guest messaging (core + broker)", () => {
  test("(a) a non-member session's first send auto-registers a guest and delivers, recipient sees the guest name", async () => {
    const s = await makeSwarm("ga");
    const guestSession = "ses-abcdef12";
    const msgs = await core.sendMessage({
      swarmId: s.id,
      fromSessionId: guestSession,
      to: "w1",
      kind: "message",
      message: "hi from outside",
    });
    expect(msgs.length).toBe(1);

    // Guest row auto-created with the exact guest shape.
    const guest = await store.getMemberBySessionAndSwarm(guestSession, s.id);
    expect(guest).toBeDefined();
    expect(guest!.role).toBe("guest");
    expect(guest!.name).toBe("guest-abcd"); // ses-abcdef12 -> guest-abcd
    expect(guest!.status).toBe("idle");
    expect(guest!.workspaceMode).toBe("shared-read");
    expect(guest!.sessionId).toBe(guestSession);

    // The message row is sent by the guest member id.
    const row = await store.getMessageById(msgs[0]!.id);
    expect(row!.fromMemberId).toBe(guest!.id);

    // The recipient's broker inbox renders the guest BY NAME (not an id).
    const prompt = runtime.prompts.find((p) => p.sessionID === s.worker.sessionId);
    expect(prompt).toBeDefined();
    expect(prompt!.text).toContain("guest-abcd");
    expect(prompt!.text).toContain("hi from outside");
  });

  test("(b) the same session sends again — no duplicate guest row", async () => {
    const s = await makeSwarm("gb");
    const guestSession = "ses-b123456";
    await core.sendMessage({ swarmId: s.id, fromSessionId: guestSession, to: "w1", kind: "message", message: "one" });
    await core.sendMessage({ swarmId: s.id, fromSessionId: guestSession, to: "w1", kind: "message", message: "two" });
    const rows = (await store.listMembers(s.id)).filter((m) => m.sessionId === guestSession);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.role).toBe("guest");
    // Idempotent: the SECOND send reused the SAME member id.
    const m2 = (await store.listMessagesBySwarm(s.id, 50)).find((m) => m.body.text === "two");
    expect(m2!.fromMemberId).toBe(rows[0]!.id);
  });

  test("(c) guest replies — reply delivered to the original sender, thread intact", async () => {
    const s = await makeSwarm("gc");
    const guestSession = "ses-c123456";
    await core.sendMessage({ swarmId: s.id, fromSessionId: guestSession, to: "w1", kind: "message", message: "hello" });
    const guest = await store.getMemberBySessionAndSwarm(guestSession, s.id);
    expect(guest).toBeDefined();

    // The worker messages the guest BY NAME (name-based addressing works).
    const toGuest = await core.sendMessage({
      swarmId: s.id,
      fromMemberId: s.worker.id,
      to: guest!.name,
      kind: "request",
      message: "what is your question?",
    });
    expect(toGuest[0]!.to.memberId).toBe(guest!.id);

    // The guest replies — routed back to the ORIGINAL sender (the worker).
    const reply = await core.replyToMessage({
      swarmId: s.id,
      fromSessionId: guestSession,
      toMessageId: toGuest[0]!.id,
      message: "my question is: why?",
    });
    expect(reply.length).toBe(1);
    expect(reply[0]!.to.memberId).toBe(s.worker.id);
    expect(reply[0]!.responseTo).toBe(toGuest[0]!.id);
    expect(reply[0]!.correlationId).toBe(toGuest[0]!.correlationId);
    const replyRow = await store.getMessageById(reply[0]!.id);
    expect(replyRow!.fromMemberId).toBe(guest!.id);
  });

  test("(d) a swarm member messaging the guest by name delivers an inbox prompt to the guest session", async () => {
    const s = await makeSwarm("gd");
    const guestSession = "ses-d123456";
    await core.sendMessage({ swarmId: s.id, fromSessionId: guestSession, to: "w1", kind: "message", message: "hello" });
    const guest = await store.getMemberBySessionAndSwarm(guestSession, s.id);
    expect(guest).toBeDefined();
    runtime.prompts.length = 0; // only observe the NEW delivery

    const sent = await core.sendMessage({
      swarmId: s.id,
      fromMemberId: s.worker.id,
      to: guest!.name,
      kind: "message",
      message: "question for the guest",
    });
    const row = await store.getMessageById(sent[0]!.id);
    expect(row!.to.memberId).toBe(guest!.id);
    expect(row!.deliveryState).toBe("delivered");

    // The guest's SESSION received the broker inbox prompt (normal delivery).
    const guestPrompt = runtime.prompts.find((p) => p.sessionID === guestSession);
    expect(guestPrompt).toBeDefined();
    expect(guestPrompt!.text).toContain("w1");
    expect(guestPrompt!.text).toContain("question for the guest");
  });

  test("(f) the scheduler never assigns tasks to a guest — even when it is the only idle member", async () => {
    const sched = new Scheduler(store, runtime);
    // Higher-affinity worker: named 'zzz' so it sorts AFTER any guest — without
    // the guest exclusion the default name-order would hand the task to the
    // guest instead.
    const created = await core.createSwarm({ name: `gs-f-${Math.random().toString(36).slice(2, 6)}`, projectId: "proj-guest", coordinatorSessionId: "ses-f-co" });
    const swarm = created.swarm;
    const worker = await core.spawnMember({ swarmId: swarm.id, name: "zzz", role: "worker" });
    await store.updateMemberStatus(worker.id, "idle");
    // Register a guest (idle).
    const guestSession = "ses-f-guest1234";
    await core.sendMessage({ swarmId: swarm.id, fromSessionId: guestSession, to: "zzz", kind: "message", message: "hi" });
    const guest = await store.getMemberBySessionAndSwarm(guestSession, swarm.id);
    expect(guest).toBeDefined();

    const task = await core.createTask({ swarmId: swarm.id, title: "any ready task", createdByMemberId: swarm.coordinatorMemberId });
    await store.updateTaskStatus(task.id, "ready");
    const res = await sched.run(swarm);
    expect(res.assigned.length).toBe(1);
    expect(res.assigned[0]!.memberName).toBe("zzz");
    const after = (await store.listTasks(swarm.id)).find((t) => t.id === task.id)!;
    expect(after.ownerMemberId).toBe(worker.id);

    // Guest ALONE (no worker): a ready task must stay unassigned — the guest
    // is structurally excluded from the idle candidate pool.
    const aloneResult = await core.createSwarm({ name: `gs-f2-${Math.random().toString(36).slice(2, 6)}`, projectId: "proj-guest", coordinatorSessionId: "ses-f2-co" });
    const alone = aloneResult.swarm;
    await core.sendMessage({ swarmId: alone.id, fromSessionId: "ses-f2-guest", to: "coordinator", kind: "message", message: "hi" });
    const guestAlone = await store.getMemberBySessionAndSwarm("ses-f2-guest", alone.id);
    expect(guestAlone).toBeDefined();
    expect(guestAlone!.status).toBe("idle");
    const task2 = await core.createTask({ swarmId: alone.id, title: "unassignable", createdByMemberId: alone.coordinatorMemberId });
    await store.updateTaskStatus(task2.id, "ready");
    const res2 = await sched.run(alone);
    expect(res2.assigned).toHaveLength(0);
    expect(res2.readyUnassigned).toContain(task2.id);
    const after2 = (await store.listTasks(alone.id)).find((t) => t.id === task2.id)!;
    expect(after2.ownerMemberId).toBeUndefined();
  });

  test("(g) recovery does not respawn a guest (absent session + reconcile → guest untouched)", async () => {
    const s = await makeSwarm("gg");
    const guestSession = "ses-g-guest1234";
    await core.sendMessage({ swarmId: s.id, fromSessionId: guestSession, to: "w1", kind: "message", message: "hi" });
    const guest = await store.getMemberBySessionAndSwarm(guestSession, s.id);
    expect(guest).toBeDefined();

    // A runtime where EVERY session is absent (simulated restart) — the guest
    // session was never a runtime child, so it is 'absent' too.
    const absentRuntime = new FakeRuntime();
    const respawned: string[] = [];
    const recovery = new Recovery(store, absentRuntime, async (m) => {
      respawned.push(m.name);
      return "ses-respawned-1";
    });
    await recovery.reconcileSwarm(s.id);

    // The guest was never respawned and its durable state is untouched.
    expect(respawned).not.toContain(guest!.name);
    const after = await store.getMemberById(guest!.id);
    expect(after).toBeDefined();
    expect(after!.status).toBe("idle");
  });

  test("(h) allowExternalGuests=false rejects the send with a clear error and creates no row", async () => {
    const created = await core.createSwarm({
      name: `gs-h-${Math.random().toString(36).slice(2, 6)}`,
      projectId: "proj-guest",
      coordinatorSessionId: "ses-h-co",
      policies: { allowExternalGuests: false },
    });
    const swarm = created.swarm;
    const worker = await core.spawnMember({ swarmId: swarm.id, name: "w1", role: "worker" });
    await store.updateMemberStatus(worker.id, "idle");
    await expect(
      core.sendMessage({ swarmId: swarm.id, fromSessionId: "ses-h-guest", to: "w1", kind: "message", message: "hi" }),
    ).rejects.toThrow("this swarm does not accept messages from non-member sessions (allowExternalGuests=false)");
    const members = await store.listMembers(swarm.id);
    expect(members.some((m) => m.role === "guest")).toBe(false);
    expect(members.some((m) => m.sessionId === "ses-h-guest")).toBe(false);
  });

  test("(i) the same session as guest in TWO swarms → two rows, messages isolated", async () => {
    const a = await makeSwarm("ia");
    const b = await makeSwarm("ib");
    const guestSession = "ses-i123456";
    const mA = await core.sendMessage({ swarmId: a.id, fromSessionId: guestSession, to: "w1", kind: "message", message: "to A" });
    const mB = await core.sendMessage({ swarmId: b.id, fromSessionId: guestSession, to: "w1", kind: "message", message: "to B" });

    const gA = await store.getMemberBySessionAndSwarm(guestSession, a.id);
    const gB = await store.getMemberBySessionAndSwarm(guestSession, b.id);
    expect(gA).toBeDefined();
    expect(gB).toBeDefined();
    expect(gA!.id).not.toBe(gB!.id); // two distinct guest rows
    expect(gA!.name).toBe(gB!.name); // same stable handle in both

    const rowA = await store.getMessageById(mA[0]!.id);
    const rowB = await store.getMessageById(mB[0]!.id);
    expect(rowA!.swarmId).toBe(a.id);
    expect(rowA!.fromMemberId).toBe(gA!.id);
    expect(rowB!.swarmId).toBe(b.id);
    expect(rowB!.fromMemberId).toBe(gB!.id);
  });

  test("(j) the cross-swarm force path is unaffected — a registered member is NOT a guest", async () => {
    const a = await makeSwarm("ja");
    const b = await makeSwarm("jb");

    // A registered member of A force-messages B → NOT registered as a guest.
    const sent = await core.sendMessage({
      swarmId: b.id,
      fromSessionId: a.worker.sessionId,
      to: "w1",
      force: true,
      kind: "message",
      message: "cross-swarm hello",
    });
    const row = await store.getMessageById(sent[0]!.id);
    expect(row!.fromMemberId).toBe(a.worker.id);
    const bMembers = await store.listMembers(b.id);
    expect(bMembers.some((m) => m.sessionId === a.worker.sessionId && m.role === "guest")).toBe(false);
    expect(bMembers.filter((m) => m.role === "guest")).toHaveLength(0);

    // Without force the same member still gets the cross-swarm hint (not a
    // guest registration).
    await expect(
      core.sendMessage({ swarmId: b.id, fromSessionId: a.worker.sessionId, to: "w1", kind: "message", message: "no force" }),
    ).rejects.toThrow("sender is not a member of swarm");
    const after = await store.listMembers(b.id);
    expect(after.filter((m) => m.role === "guest")).toHaveLength(0);
  });

  test("(k) a session REMOVED from the swarm gets the orphan error, NOT a silent guest resurrection", async () => {
    const s = await makeSwarm("gk");
    const guestSession = "ses-k-removed1";
    // Register + then remove the guest via the plugin path would need the
    // plugin; here we simulate the durable evidence: a member.removed event
    // carrying the session id (the exact payload swarm_remove writes).
    const member = await store.insertMember({
      id: `mem-${Math.random().toString(36).slice(2, 8)}`,
      swarmId: s.id,
      name: "ex-worker",
      role: "worker",
      sessionId: guestSession,
      status: "idle",
      workspaceMode: "shared-read",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    await store.deleteMember(member.id);
    await store.insertEvent({
      swarmId: s.id,
      type: "member.removed",
      entityType: "member",
      entityId: member.id,
      payloadJson: JSON.stringify({ name: "ex-worker", sessionId: guestSession }),
      createdAt: Date.now(),
    });
    await expect(
      core.sendMessage({ swarmId: s.id, fromSessionId: guestSession, to: "w1", kind: "message", message: "hi" }),
    ).rejects.toThrow("your session is not registered as a member of any swarm (you may have been removed)");
    // No guest row was created.
    const rows = (await store.listMembers(s.id)).filter((m) => m.sessionId === guestSession);
    expect(rows).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Part 2 — plugin-tool harness (real tool surfaces)
// ---------------------------------------------------------------------------

let hooks: Hooks | undefined;
let tool: Record<string, any>;

function makeClient() {
  return {
    config: {
      providers: async () => ({
        data: {
          providers: [{ id: "opencode-go", models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash" } } }],
        },
        error: undefined,
      }),
    },
    session: {
      create: async (o: any) => ({
        data: {
          id: `ses-${Math.random().toString(36).slice(2, 8)}`,
          title: o?.body?.title,
          parentID: undefined,
          directory: ".",
        },
        error: undefined,
      }),
      get: async (o: any) => ({
        data: { id: o?.path?.id, title: "t", model: undefined, directory: "." },
        error: undefined,
      }),
      children: async () => ({ data: [], error: undefined }),
      messages: async () => ({ data: [], error: undefined }),
      status: async () => ({ data: {}, error: undefined }),
      abort: async () => ({ data: undefined, error: undefined }),
      update: async () => ({ data: {}, error: undefined }),
      prompt: async () => ({ data: { info: {} }, error: undefined }),
      promptAsync: async () => ({ data: undefined, error: undefined }),
    },
  };
}

const pluginInput = (client: unknown): any => ({
  client,
  project: { id: "proj-guest" },
  directory: ".",
  worktree: ".",
  experimental_workspace: { register() {} },
  serverUrl: new URL("http://x"),
  $: {},
});

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

let pluginDirs: string[] = [];

async function initPlugin(): Promise<void> {
  disposeSwarmRuntime();
  const dir = mkdtempSync(join(tmpdir(), "swarms-guest-"));
  pluginDirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient()), { dataDir: dir });
  tool = hooks.tool ?? {};
}

async function createSwarmTool(name: string, sessionID: string, policies?: Record<string, unknown>): Promise<{ id: string; name: string }> {
  const res = await tool.swarm_create.execute({ name, ...(policies ? { policies } : {}) }, ctx(sessionID));
  const json = JSON.parse(String(res.output ?? res));
  return json.swarm;
}

async function spawnTool(swarmId: string, name: string, sessionID: string): Promise<any> {
  const res = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name, role: "worker" }] },
    ctx(sessionID),
  );
  const json = JSON.parse(String(res.output ?? res));
  return json.spawned[0];
}

describe("guest messaging (plugin tools)", () => {
  test("non-swarm session messages a swarm through swarm_message — guest row created, roster renders it", async () => {
    await initPlugin();
    const swarm = await createSwarmTool("guest-e2e", "ses-e2e-co");
    const worker = await spawnTool(swarm.id, "w1", "ses-e2e-co");

    const guestSession = "ses-e2eguest55";
    const res = await tool.swarm_message.execute(
      { swarmId: swarm.id, to: "w1", kind: "message", message: "hello from outside" },
      ctx(guestSession),
    );
    const out = String(res.output ?? res);
    // Delivery verdict is timing-sensitive (may be delivered now or pending)
    // — accept either; the deterministic assertions are on the store rows.
    expect(out).toMatch(/delivered to (1 now|0 now, 1 pending)/);

    const rt = swarmRuntime();
    expect(rt).toBeDefined();
    const guest = await rt!.core.store.getMemberBySessionAndSwarm(guestSession, swarm.id);
    expect(guest).toBeDefined();
    expect(guest!.role).toBe("guest");
    expect(guest!.name).toMatch(/^guest-/);
    expect(guest!.status).toBe("idle");

    // Roster/status render the guest as a normal member (name + role).
    const roster = String((await tool.swarm_roster.execute({ swarmId: swarm.id }, ctx(guestSession))).output ?? "");
    expect(roster).toContain(guest!.name);
    expect(roster).toContain("(guest)");
    const status = String(
      (await tool.swarm_status.execute({ swarmId: swarm.id, detail: "messages" }, ctx(guestSession))).output ?? "",
    );
    expect(status).toContain(guest!.name);
  });

  test("guest replies through swarm_reply — routed to the original sender, thread intact", async () => {
    await initPlugin();
    const swarm = await createSwarmTool("guest-reply", "ses-rep-co");
    await spawnTool(swarm.id, "w1", "ses-rep-co");
    const guestSession = "ses-replyguest12345";
    const rt = swarmRuntime();
    expect(rt).toBeDefined();

    // Register the guest with a first send.
    await tool.swarm_message.execute(
      { swarmId: swarm.id, to: "w1", kind: "message", message: "hi" },
      ctx(guestSession),
    );
    const guest = await rt!.core.store.getMemberBySessionAndSwarm(guestSession, swarm.id);
    expect(guest).toBeDefined();

    // The coordinator messages the guest BY NAME.
    const sent = await tool.swarm_message.execute(
      { swarmId: swarm.id, to: guest!.name, kind: "request", message: "what do you think?" },
      ctx("ses-rep-co"),
    );
    const sentJson = JSON.parse(String(sent.output ?? sent));
    const msgId = sentJson.messages[0].id;

    // The guest replies — the reply lands with the ORIGINAL sender.
    const replied = await tool.swarm_reply.execute(
      { swarmId: swarm.id, toMessageId: msgId, message: "i think it works" },
      ctx(guestSession),
    );
    const repliedJson = JSON.parse(String(replied.output ?? replied));
    const coord = (await rt!.core.store.listMembers(swarm.id)).find((m) => m.role === "coordinator");
    expect(repliedJson.delivered[0].to.memberId).toBe(coord!.id);
    const replyRow = await rt!.core.store.getMessageById(repliedJson.delivered[0].id);
    expect(replyRow!.responseTo).toBe(msgId);
    expect(replyRow!.fromMemberId).toBe(guest!.id);
  });

  test("(e) a guest cannot delegate/stop/revive/delete/retry (coordinator-only tools)", async () => {
    await initPlugin();
    const swarm = await createSwarmTool("guest-guards", "ses-gco");
    await spawnTool(swarm.id, "w1", "ses-gco");
    const guestSession = "ses-guardguest12345";
    // Register the guest.
    await tool.swarm_message.execute(
      { swarmId: swarm.id, to: "w1", kind: "message", message: "hi" },
      ctx(guestSession),
    );

    const delegate = await tool.swarm_delegate.execute(
      { swarmId: swarm.id, name: "x", title: "t", prompt: "p" },
      ctx(guestSession),
    );
    // swarm_delegate's caller gate rejects non-coordinators with the
    // "set up a swarm" wording — the important contract is the coordinator-only
    // rejection, not the exact sentence.
    expect(String(delegate.output ?? delegate)).toMatch(/only the coordinator may/);

    const stop = await tool.swarm_stop.execute({ swarmId: swarm.id, member: "w1" }, ctx(guestSession));
    expect(String(stop.output ?? stop)).toContain("only the coordinator may stop");

    const revive = await tool.swarm_revive.execute(
      { swarmId: swarm.id, action: "revive", strategy: "keep" },
      ctx(guestSession),
    );
    expect(String(revive.output ?? revive)).toContain("only the coordinator may run swarm_revive");

    const del = await tool.swarm_delete.execute({ swarmId: swarm.id, confirm: swarm.name }, ctx(guestSession));
    expect(String(del.output ?? del)).toContain("only the coordinator may delete");

    const retry = await tool.swarm_tasks.execute(
      { swarmId: swarm.id, action: "retry", taskId: "t-does-not-exist" },
      ctx(guestSession),
    );
    expect(String(retry.output ?? retry)).toContain("only the coordinator may retry");

    // Guests never get tasks — not even by manual claim (defense in depth:
    // the scheduler excludes role 'guest' AND the claim tool rejects it).
    const claim = await tool.swarm_tasks.execute(
      { swarmId: swarm.id, action: "claim", taskId: "t-any" },
      ctx(guestSession),
    );
    expect(String(claim.output ?? claim)).toContain("guests never receive tasks");

    // The guest could not delete the swarm — it still exists.
    const rt = swarmRuntime();
    expect(await rt!.core.store.getSwarm(swarm.id)).toBeDefined();
  });
});

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of pluginDirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});
