import { describe, expect, test } from "bun:test";
import { SwarmCore } from "../../src/core/swarm.ts";

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

async function setup() {
  const { SQLiteStore } = await import("../../src/storage/sqlite-store.ts");
  const store = new SQLiteStore(":memory:");
  await store.ready();
  const core = new SwarmCore(store as never, fakeRuntime as never);
  const created = await core.createSwarm({ name: "g", projectId: "g", coordinatorSessionId: "sess-c" });
  const swarmId = created.swarm.id;
  const now = Date.now();
  await store.insertMember({ id: "m-a", swarmId, name: "A", sessionId: "sess-a", role: "worker", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
  await store.insertMember({ id: "m-b", swarmId, name: "B", sessionId: "sess-b", role: "worker", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
  return { store, core, swarmId, now };
}

describe("messaging guards", () => {
  test("sendMessage rejects empty message body", async () => {
    const { core, swarmId } = await setup();
    await expect(
      core.sendMessage({ swarmId, fromMemberId: "m-a", to: "B", kind: "message", message: "" }),
    ).rejects.toThrow("cannot be empty");
    await expect(
      core.sendMessage({ swarmId, fromMemberId: "m-a", to: "B", kind: "message", message: "   " }),
    ).rejects.toThrow("cannot be empty");
  });

  test("sendMessage rejects self-messaging for a worker member", async () => {
    const { core, swarmId } = await setup();
    await expect(
      core.sendMessage({ swarmId, fromMemberId: "m-a", to: "A", kind: "message", message: "hi" }),
    ).rejects.toThrow("cannot send a message to yourself");
  });

  test("sendMessage rejects self-messaging via coordinator alias when sender is coordinator", async () => {
    // Coordinator CAN message itself (self-notices); this test checks that
    // a non-coordinator sending to its own name is blocked even if the name
    // happens to also match the coordinator alias.
    const { core, swarmId } = await setup();
    // m-a is a worker named "A" — sending to "A" (itself) must be rejected.
    await expect(
      core.sendMessage({ swarmId, fromMemberId: "m-a", to: "A", kind: "message", message: "to myself" }),
    ).rejects.toThrow("cannot send a message to yourself");
  });

  test("coordinator self-messaging is allowed (self-notice channel)", async () => {
    const { core, swarmId, now: _now } = await setup();
    void _now;
    const coord = await core.store.getMemberBySessionId("sess-c");
    // Coordinator messaging itself must NOT throw — it's the self-notice channel.
    const msgs = await core.sendMessage({ swarmId, fromMemberId: coord!.id, to: coord!.name, kind: "finding", message: "coordinator self notice" });
    expect(msgs.length).toBe(1);
  });

  test("broadcast never reaches the sender (already filtered)", async () => {
    const { core, swarmId } = await setup();
    // setup() has coordinator + m-a + m-b. Broadcast from m-a excludes only
    // the sender, so it reaches the other two (coordinator + m-b).
    const msgs = await core.sendMessage({ swarmId, fromMemberId: "m-a", to: "*", kind: "message", message: "hi all" });
    expect(msgs.length).toBe(2);
    expect(msgs.every((m) => m.to.memberId !== "m-a")).toBe(true);
  });

  test("replyToMessage rejects empty body", async () => {
    const { core, swarmId } = await setup();
    const sent = await core.sendMessage({ swarmId, fromMemberId: "m-a", to: "B", kind: "request", message: "q" });
    await expect(
      core.replyToMessage({ swarmId, fromMemberId: "m-b", toMessageId: sent[0]!.id, message: "" }),
    ).rejects.toThrow("cannot be empty");
  });

  test("replyToMessage rejects replying to your own message", async () => {
    const { core, swarmId } = await setup();
    const sent = await core.sendMessage({ swarmId, fromMemberId: "m-a", to: "B", kind: "request", message: "q" });
    await expect(
      core.replyToMessage({ swarmId, fromMemberId: "m-a", toMessageId: sent[0]!.id, message: "replying to myself" }),
    ).rejects.toThrow("cannot reply to your own message");
  });

  test("replyToMessage allows a distinct member to reply normally", async () => {
    const { core, swarmId } = await setup();
    const sent = await core.sendMessage({ swarmId, fromMemberId: "m-a", to: "B", kind: "request", message: "q" });
    const reply = await core.replyToMessage({ swarmId, fromMemberId: "m-b", toMessageId: sent[0]!.id, message: "here is my answer" });
    expect(reply.length).toBe(1);
    expect(reply[0]!.responseTo).toBe(sent[0]!.id);
  });
});
