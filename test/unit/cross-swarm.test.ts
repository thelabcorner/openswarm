import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Broker } from "../../src/messaging/broker.ts";
import { enrichForeignSenderNames } from "../../src/messaging/senders.ts";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * Cross-swarm messaging (force) — verification tests for the `force` feature
 * on swarm_message / swarm_reply:
 *
 *   a. force send into another swarm → row lives in the TARGET swarm (the
 *      recipient's home), recipient's session is prompted, sender resolves.
 *   b. the same send WITHOUT force → "sender is not a member of swarm".
 *   c. broker inbox renders the foreign sender as `name@swarm`.
 *   d. reply routing: a reply lands in the ORIGINAL sender's home swarm
 *      (uniform "row lives where the recipient lives" rule) → symmetric
 *      ping-pong without force on either side.
 *   e. swarm_reply with force when the caller passes the FOREIGN swarm id →
 *      works; without force → "belongs to a different swarm".
 *   f. broadcast to "*" with force → every target-swarm member except the
 *      sender.
 *   g. the self-message guard still blocks a member messaging itself.
 *
 * Two harnesses: the store+core+Broker style (precise row-level control,
 * FakeRuntime records prompt text) and the plugin-tool style (real
 * swarm_message / swarm_reply surfaces with resolveSwarmId + fromSessionId).
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
  dir = mkdtempSync(join(tmpdir(), "swarms-cross-core-"));
  store = new SQLiteStore(join(dir, "cross.db"));
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

/** Two sibling swarms: swarm A ("xs-a-<tag>") with worker alice, swarm B
 * ("xs-b-<tag>") with workers bob + carol. Each swarm has its own
 * coordinator session (a session can belong to only one swarm). */
async function makePair(): Promise<{
  swarmA: { id: string; name: string };
  swarmB: { id: string; name: string };
  alice: { id: string; sessionId: string };
  bob: { id: string; sessionId: string };
  carol: { id: string };
  coordB: { id: string };
}> {
  const tag = Math.random().toString(36).slice(2, 8);
  const a = await core.createSwarm({
    name: `xs-a-${tag}`,
    projectId: "proj-cross",
    coordinatorSessionId: `ses-ca-${tag}`,
  });
  const b = await core.createSwarm({
    name: `xs-b-${tag}`,
    projectId: "proj-cross",
    coordinatorSessionId: `ses-cb-${tag}`,
  });
  const alice = await core.spawnMember({ swarmId: a.swarm.id, name: "alice", role: "worker" });
  const bob = await core.spawnMember({ swarmId: b.swarm.id, name: "bob", role: "worker" });
  const carol = await core.spawnMember({ swarmId: b.swarm.id, name: "carol", role: "worker" });
  const coordB = (await store.listMembers(b.swarm.id)).find((m) => m.role === "coordinator")!;
  return {
    swarmA: { id: a.swarm.id, name: a.swarm.name },
    swarmB: { id: b.swarm.id, name: b.swarm.name },
    alice: { id: alice.id, sessionId: alice.sessionId },
    bob: { id: bob.id, sessionId: bob.sessionId },
    carol: { id: carol.id },
    coordB: { id: coordB.id },
  };
}

describe("cross-swarm messaging (core)", () => {
  test("a. force send: A member messages into swarm B — row lives in B, recipient prompted", async () => {
    const { swarmA, swarmB, alice, bob } = await makePair();
    const before = runtime.prompts.length;
    const msgs = await core.sendMessage({
      swarmId: swarmB.id,
      fromMemberId: alice.id,
      to: "bob",
      kind: "request",
      message: "cross-swarm hello",
      correlationId: "thread-a1",
      force: true,
    });
    expect(msgs.length).toBe(1);
    // The message row lives in the TARGET swarm (the recipient's home).
    expect(msgs[0]!.swarmId).toBe(swarmB.id);
    // The sender resolves globally — the row carries alice's real member id.
    expect(msgs[0]!.fromMemberId).toBe(alice.id);
    expect(msgs[0]!.to.memberId).toBe(bob.id);
    expect(msgs[0]!.correlationId).toBe("thread-a1");
    // Broker delivery: bob's session was prompted and the mail is drained.
    expect(runtime.prompts.length).toBe(before + 1);
    expect(runtime.prompts[before]!.sessionID).toBe(bob.sessionId);
    expect(runtime.prompts[before]!.text).toContain("cross-swarm hello");
    expect((await store.listPendingMessages(bob.id)).length).toBe(0);
    void swarmA;
  });

  test("b. same send WITHOUT force rejects: sender is not a member of the target swarm", async () => {
    const { swarmA, swarmB, alice } = await makePair();
    // member-id resolution is gated to the target swarm roster. Agent-UX: the
    // error names the exact remedy (force: true) instead of dead-ending.
    await expect(
      core.sendMessage({
        swarmId: swarmB.id,
        fromMemberId: alice.id,
        to: "bob",
        kind: "message",
        message: "no force",
      }),
    ).rejects.toThrow(new RegExp(`sender is not a member of swarm '${swarmB.name}'.*force: true`));
    // session-based resolution is gated the same way.
    await expect(
      core.sendMessage({
        swarmId: swarmB.id,
        fromSessionId: alice.sessionId,
        to: "bob",
        kind: "message",
        message: "no force",
      }),
    ).rejects.toThrow(new RegExp(`sender is not a member of swarm '${swarmB.name}'.*force: true`));
    void swarmA;
  });

  test("c. broker inbox shows the foreign sender as name@swarm", async () => {
    const { swarmA, swarmB, alice, bob } = await makePair();
    const before = runtime.prompts.length;
    await core.sendMessage({
      swarmId: swarmB.id,
      fromMemberId: alice.id,
      to: "bob",
      kind: "request",
      message: "from the other side",
      force: true,
    });
    expect(runtime.prompts.length).toBe(before + 1);
    const promptText = runtime.prompts[runtime.prompts.length - 1]!.text;
    // Header + envelope render the foreign sender as `name@swarm` so the
    // recipient always sees the origin.
    expect(promptText).toContain(`[NEW MESSAGE FROM: alice@${swarmA.name}]`);
    // Envelope renders `alice@swarm-a [request]:` (kind label before the colon).
    expect(promptText).toContain(`alice@${swarmA.name} [request]:`);
    expect(promptText).toContain("from the other side");
  });

  test("d. reply ping-pong: each reply lands in the ORIGINAL sender's home swarm (symmetric)", async () => {
    const { swarmA, swarmB, alice, bob } = await makePair();
    const m1 = (await core.sendMessage({
      swarmId: swarmB.id,
      fromMemberId: alice.id,
      to: "bob",
      kind: "request",
      message: "hello bob",
      correlationId: "thread-pong",
      force: true,
    }))[0]!;

    // bob (swarm B) replies WITHOUT force using his OWN swarm id — the reply
    // must land in the original sender's home swarm (A), addressed to alice,
    // pointing responseTo at the original and preserving the correlation id.
    const m2 = (await core.replyToMessage({
      swarmId: swarmB.id,
      fromMemberId: bob.id,
      toMessageId: m1.id,
      message: "pong 1",
      kind: "response",
    }))[0]!;
    expect(m2.swarmId).toBe(swarmA.id);
    expect(m2.to.memberId).toBe(alice.id);
    expect(m2.responseTo).toBe(m1.id);
    expect(m2.correlationId).toBe("thread-pong");

    // alice replies back WITHOUT force using her OWN swarm id — the reply
    // lands in bob's home swarm (B). Symmetric ping-pong between the two.
    const m3 = (await core.replyToMessage({
      swarmId: swarmA.id,
      fromMemberId: alice.id,
      toMessageId: m2.id,
      message: "pong 2",
      kind: "response",
    }))[0]!;
    expect(m3.swarmId).toBe(swarmB.id);
    expect(m3.to.memberId).toBe(bob.id);
    expect(m3.responseTo).toBe(m2.id);
    expect(m3.correlationId).toBe("thread-pong");
  });

  test("e. reply with force when passing the FOREIGN swarm id; without force it rejects", async () => {
    const { swarmA, swarmB, alice, bob } = await makePair();
    const m1 = (await core.sendMessage({
      swarmId: swarmB.id,
      fromMemberId: alice.id,
      to: "bob",
      kind: "request",
      message: "hi",
      force: true,
    }))[0]!;

    // WITHOUT force: the caller's swarmId (the foreign swarm A) does not own
    // the original message (which lives in B) — must reject. Agent-UX: the
    // error names the exact remedy (force: true) instead of dead-ending.
    await expect(
      core.replyToMessage({
        swarmId: swarmA.id,
        fromMemberId: bob.id,
        toMessageId: m1.id,
        message: "nope",
      }),
    ).rejects.toThrow(new RegExp(`message '${m1.id}' belongs to a different swarm.*force: true`));

    // WITH force: the caller may pass the FOREIGN swarm id; the reply still
    // lands in the ORIGINAL sender's home swarm (A).
    const reply = (await core.replyToMessage({
      swarmId: swarmA.id,
      fromMemberId: bob.id,
      toMessageId: m1.id,
      message: "yes via force",
      kind: "response",
      force: true,
    }))[0]!;
    expect(reply.swarmId).toBe(swarmA.id);
    expect(reply.to.memberId).toBe(alice.id);
    expect(reply.responseTo).toBe(m1.id);
  });

  test("f. broadcast to '*' with force reaches every target-swarm member except the sender", async () => {
    const { swarmB, alice, bob, carol, coordB } = await makePair();
    const msgs = await core.sendMessage({
      swarmId: swarmB.id,
      fromMemberId: alice.id,
      to: "*",
      kind: "decision",
      message: "all-hands",
      force: true,
    });
    // Coordinator + bob + carol (the sender is a swarm-A member, so it is not
    // among the recipients — "all B members except the sender").
    expect(msgs.length).toBe(3);
    expect(msgs.map((m) => m.to.memberId).sort()).toEqual([bob.id, carol.id, coordB.id].sort());
    // Every row lives in the target swarm.
    expect(msgs.every((m) => m.swarmId === swarmB.id)).toBe(true);
    expect(msgs.every((m) => m.to.memberId !== alice.id)).toBe(true);
  });

  test("g. self-message guard still blocks a member messaging themselves (same swarm)", async () => {
    const { swarmA, alice } = await makePair();
    await expect(
      core.sendMessage({
        swarmId: swarmA.id,
        fromMemberId: alice.id,
        to: "alice",
        kind: "message",
        message: "to myself",
      }),
    ).rejects.toThrow("cannot send a message to yourself");
  });
});

// ---------------------------------------------------------------------------
// Part 2 — plugin-tool harness (real swarm_message / swarm_reply surfaces)
// ---------------------------------------------------------------------------

let hooks: Hooks | undefined;
let tool: Record<string, any>;

// NOTE on harness design (flake history): the plugin-tools block deliberately
// does NOT record promptAsync deliveries into a module-level array and assert
// on it. Under full-suite parallel load that assertion proved non-deterministic
// (cross-swarm.test.ts P1 flaked: deliveryState was 'delivered' yet the
// recorded prompt was not visible at assert time — a wall-clock/ordering race
// under CPU contention, reproduced by 4 peers, passes in isolation). All
// plugin-level assertions therefore read DETERMINISTIC state: tool output +
// swarm_status store reads (the broker-prompt `name@swarm` rendering itself is
// asserted deterministically in the core block via a file-local FakeRuntime).
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
  project: { id: "proj-cross" },
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
  const dir = mkdtempSync(join(tmpdir(), "swarms-cross-"));
  pluginDirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient()), { dataDir: dir });
  tool = hooks.tool ?? {};
}

async function createSwarmTool(name: string, sessionID: string): Promise<{ id: string; name: string }> {
  const res = await tool.swarm_create.execute({ name }, ctx(sessionID));
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

/**
 * Deterministic cross-swarm rendering check. Reads the message ROW the tools
 * just produced (via the same runtime the tools used — self-consistent even
 * if the plugin singleton is shared with another file's runtime) and resolves
 * the sender's display name with the REAL enrichForeignSenderNames. No
 * rendered-text capture, no cross-file contamination: immune to the
 * parallel-load flake that hit the old prompt/status-text assertions.
 */
async function resolvedSenderDisplay(
  messageId: string,
): Promise<{ fromMemberId: string; display: string | undefined }> {
  const rt = swarmRuntime();
  expect(rt).toBeDefined();
  const msg = await rt!.core.store.getMessageById(messageId);
  expect(msg).toBeDefined();
  const names = new Map<string, string>();
  for (const m of await rt!.core.store.listMembers(msg!.swarmId)) names.set(m.id, m.name);
  await enrichForeignSenderNames(rt!.core.store, [msg!], names);
  return { fromMemberId: msg!.fromMemberId, display: names.get(msg!.fromMemberId) };
}

describe("cross-swarm messaging (plugin tools)", () => {
  test("swarm_message force from a swarm-A member delivers to the B member as name@swarm", async () => {
    await initPlugin();
    const swarmA = await createSwarmTool("cross-a", "ses-ca");
    const swarmB = await createSwarmTool("cross-b", "ses-cb");
    const alice = await spawnTool(swarmA.id, "alice", "ses-ca");
    await spawnTool(swarmB.id, "bob", "ses-cb");

    const res = await tool.swarm_message.execute(
      { swarmId: swarmB.id, to: "bob", kind: "request", message: "hi from across", force: true },
      ctx(alice.sessionId),
    );
    const out = String(res.output ?? res);
    // Delivery verdict is a timing-sensitive async wake (may be delivered now
    // or pending on the next wake under load) — accept either; the message must
    // have reached bob's mailbox either way (deterministic assertions below).
    expect(out).toMatch(/delivered to (1 now|0 now, 1 pending)/);
    expect(out).toContain('"bob"');

    // Deterministic cross-swarm rendering: the message row lives in swarm B
    // and the REAL enrichForeignSenderNames resolves the foreign sender to
    // `alice@<swarm-a>` (store-row assertions — no rendered-text capture, so
    // immune to the parallel-load contamination that flaked the old check).
    const sentJson = JSON.parse(out);
    const { fromMemberId, display } = await resolvedSenderDisplay(sentJson.messages[0].id);
    expect(fromMemberId).toBe(alice.memberId);
    expect(display).toBe(`alice@${swarmA.name}`);
  });

  test("swarm_message without force from a foreign member rejects", async () => {
    await initPlugin();
    const swarmA = await createSwarmTool("cross-no-a", "ses-na");
    const swarmB = await createSwarmTool("cross-no-b", "ses-nb");
    const alice = await spawnTool(swarmA.id, "alice", "ses-na");
    await spawnTool(swarmB.id, "bob", "ses-nb");

    await expect(
      tool.swarm_message.execute(
        { swarmId: swarmB.id, to: "bob", kind: "message", message: "no force" },
        ctx(alice.sessionId),
      ),
    ).rejects.toThrow("sender is not a member of swarm");
  });

  test("swarm_reply force with the FOREIGN swarm id works; without force it rejects", async () => {
    await initPlugin();
    const swarmA = await createSwarmTool("cross-r-a", "ses-ra");
    const swarmB = await createSwarmTool("cross-r-b", "ses-rb");
    const alice = await spawnTool(swarmA.id, "alice", "ses-ra");
    const bob = await spawnTool(swarmB.id, "bob", "ses-rb");

    const sent = await tool.swarm_message.execute(
      { swarmId: swarmB.id, to: "bob", kind: "request", message: "question?", force: true },
      ctx(alice.sessionId),
    );
    const sentJson = JSON.parse(String(sent.output ?? sent));
    const msgId = sentJson.messages[0].id;

    // WITHOUT force: passing the FOREIGN swarm id (swarm A) must reject.
    await expect(
      tool.swarm_reply.execute(
        { swarmId: swarmA.id, toMessageId: msgId, message: "no force" },
        ctx(bob.sessionId),
      ),
    ).rejects.toThrow("belongs to a different swarm");

    // WITH force: works, and the reply lands in the ORIGINAL sender's swarm.
    const replied = await tool.swarm_reply.execute(
      { swarmId: swarmA.id, toMessageId: msgId, message: "yes, with force", force: true },
      ctx(bob.sessionId),
    );
    const repliedJson = JSON.parse(String(replied.output ?? replied));
    expect(repliedJson.delivered[0].to.memberId).toBe(alice.memberId);

    // Deterministic cross-swarm rendering: the reply row lives in swarm A and
    // the REAL enrichForeignSenderNames resolves the foreign reply sender to
    // `bob@<swarm-b>` (store-row assertions — no rendered-text capture).
    const { display } = await resolvedSenderDisplay(repliedJson.delivered[0].id);
    expect(display).toBe(`bob@${swarmB.name}`);
  });

  test("symmetric ping-pong through the tools: replies route home with no force on either side", async () => {
    await initPlugin();
    const swarmA = await createSwarmTool("cross-p-a", "ses-pa");
    const swarmB = await createSwarmTool("cross-p-b", "ses-pb");
    const alice = await spawnTool(swarmA.id, "alice", "ses-pa");
    const bob = await spawnTool(swarmB.id, "bob", "ses-pb");

    const sent = await tool.swarm_message.execute(
      { swarmId: swarmB.id, to: "bob", kind: "request", message: "ping", force: true },
      ctx(alice.sessionId),
    );
    const m1 = JSON.parse(String(sent.output ?? sent)).messages[0];

    // bob replies without force, using his OWN swarm id (B) — lands in A.
    const r1 = await tool.swarm_reply.execute(
      { swarmId: swarmB.id, toMessageId: m1.id, message: "pong 1" },
      ctx(bob.sessionId),
    );
    const r1Json = JSON.parse(String(r1.output ?? r1));
    expect(r1Json.delivered[0].to.memberId).toBe(alice.memberId);

    // alice replies without force, using her OWN swarm id (A) — lands in B.
    const r2 = await tool.swarm_reply.execute(
      { swarmId: swarmA.id, toMessageId: r1Json.delivered[0].id, message: "pong 2" },
      ctx(alice.sessionId),
    );
    const r2Json = JSON.parse(String(r2.output ?? r2));
    expect(r2Json.delivered[0].to.memberId).toBe(bob.memberId);

    // The thread is visible from swarm B's message stream.
    const statusB = String(
      (await tool.swarm_status.execute({ swarmId: swarmB.id, detail: "messages" }, ctx(bob.sessionId))).output ?? "",
    );
    expect(statusB).toContain("pong 2");
  });
});

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of pluginDirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});
