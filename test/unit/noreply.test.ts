import { describe, expect, test } from "bun:test";
import { SwarmCore, assertNoreplyAllowed, detectAckReply } from "../../src/core/swarm.ts";
import { formatEnvelope } from "../../src/messaging/formatter.ts";
import { Broker } from "../../src/messaging/broker.ts";
import { DEFAULT_POLICIES } from "../../src/core/types.ts";
import type { SwarmMessage } from "../../src/core/types.ts";

/**
 * Noreply feature tests (fire-and-forget messages — structural guard against
 * ack-only responses, anti-pattern A2). Covers: kind validation, persistence
 * roundtrip, envelope rendering, all-noreply inbox prompt, reply soft guards,
 * ack-detection nudge.
 */

function fakeMessage(overrides: Partial<SwarmMessage> = {}): SwarmMessage {
  const now = Date.now();
  return {
    id: `msg_${Math.random().toString(36).slice(2)}`,
    swarmId: "swarm-test",
    fromMemberId: "m-a",
    to: { type: "member", memberId: "m-b" },
    kind: "finding",
    priority: "normal",
    body: { text: "body", refs: undefined },
    deliveryState: "queued",
    attemptCount: 0,
    createdAt: now,
    ...overrides,
  };
}

describe("assertNoreplyAllowed", () => {
  test("allows noreply on informational kinds", () => {
    for (const kind of ["message", "finding", "decision", "response", "control"] as const) {
      expect(() => assertNoreplyAllowed(kind, true)).not.toThrow();
    }
  });

  test("rejects noreply on action-demanding kinds", () => {
    for (const kind of ["request", "blocker", "handoff", "review"] as const) {
      expect(() => assertNoreplyAllowed(kind, true)).toThrow(/cannot be marked noreply/);
    }
  });

  test("no-op when noreply is unset", () => {
    expect(() => assertNoreplyAllowed("blocker", undefined)).not.toThrow();
    expect(() => assertNoreplyAllowed("blocker", false)).not.toThrow();
  });
});

describe("detectAckReply (ack nudge)", () => {
  test("flags trivially short replies", () => {
    const w = detectAckReply("Please review the schema changes", "ok");
    expect(w).toBeDefined();
    expect(w).toMatch(/ack-only/);
  });

  test("flags echo-like replies", () => {
    const w = detectAckReply("The lease sweep now runs before the scheduler", "the lease sweep now runs before the scheduler");
    expect(w).toBeDefined();
    expect(w).toMatch(/echo/);
  });

  test("returns undefined for substantive replies", () => {
    expect(detectAckReply("Please review the schema changes", "Done — added the FK and a migration test; see deliverable/x")).toBeUndefined();
  });

  test("returns undefined for empty input", () => {
    expect(detectAckReply("", "ok")).toBeUndefined();
    expect(detectAckReply("x", "")).toBeUndefined();
  });
});

describe("sendMessage noreply integration", () => {
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
    const created = await core.createSwarm({ name: "nr", projectId: "nr", coordinatorSessionId: "sess-c" });
    const swarmId = created.swarm.id;
    const now = Date.now();
    await store.insertMember({ id: "m-a", swarmId, name: "A", sessionId: "sess-a", role: "worker", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    await store.insertMember({ id: "m-b", swarmId, name: "B", sessionId: "sess-b", role: "worker", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    return { store, core, swarmId };
  }

  test("noreply flag persists and roundtrips", async () => {
    const { core, swarmId } = await setup();
    const a = await core.store.getMemberById("m-a");
    const msgs = await core.sendMessage({
      swarmId, fromMemberId: a!.id, to: "B", kind: "finding",
      message: "status: all green", noreply: true,
    });
    expect(msgs[0]!.noreply).toBe(true);
    const reread = await core.store.getMessageById(msgs[0]!.id);
    expect(reread?.noreply).toBe(true);
  });

  test("non-noreply messages default to false", async () => {
    const { core, swarmId } = await setup();
    const a = await core.store.getMemberById("m-a");
    const msgs = await core.sendMessage({
      swarmId, fromMemberId: a!.id, to: "B", kind: "message", message: "hi",
    });
    expect(msgs[0]!.noreply).toBe(false);
    const reread = await core.store.getMessageById(msgs[0]!.id);
    expect(reread?.noreply).toBe(false);
  });

  test("rejects noreply request/blocker with a clear error", async () => {
    const { core, swarmId } = await setup();
    const a = await core.store.getMemberById("m-a");
    await expect(
      core.sendMessage({ swarmId, fromMemberId: a!.id, to: "B", kind: "blocker", message: "blocked", noreply: true }),
    ).rejects.toThrow(/cannot be marked noreply/);
    await expect(
      core.sendMessage({ swarmId, fromMemberId: a!.id, to: "B", kind: "request", message: "please", noreply: true }),
    ).rejects.toThrow(/cannot be marked noreply/);
  });

  test("replyToMessage rejects noreply on a handoff reply", async () => {
    const { core, swarmId } = await setup();
    const a = await core.store.getMemberById("m-a");
    const sent = await core.sendMessage({
      swarmId, fromMemberId: a!.id, to: "B", kind: "request", message: "please review",
    });
    await expect(
      core.replyToMessage({
        swarmId, fromMemberId: "m-b", toMessageId: sent[0]!.id,
        kind: "handoff", message: "done", noreply: true,
      }),
    ).rejects.toThrow(/cannot be marked noreply/);
  });
});

describe("system notices auto-noreply", () => {
  test("notifyPruning sends a noreply finding", async () => {
    const { core, swarmId } = await setup2();
    const msgs = await core.notifyPruning({ swarmId, pruned: 3 });
    expect(msgs.length).toBeGreaterThan(0);
    expect(msgs[0]!.noreply).toBe(true);
  });

  test("notifyDigestFlip sends a noreply finding", async () => {
    const { core, swarmId } = await setup2();
    const res = await core.notifyDigestFlip({ swarmId, health: "stale", lastKnownHealth: "fresh" });
    expect(res.notified).toBe(true);
    const coord = await core.store.getMemberByName(swarmId, "coordinator");
    const pending = await core.listMessagesTo(coord!.id);
    const flip = pending.find((m) => m.body.text.includes("digest"));
    expect(flip?.noreply).toBe(true);
  });
});

async function setup2() {
  const { SQLiteStore } = await import("../../src/storage/sqlite-store.ts");
  const store = new SQLiteStore(":memory:");
  await store.ready();
  const core = new SwarmCore(store as never, {
    createSession: async () => ({ id: "sess-x" }),
    updateSession: async () => undefined,
    promptAsync: async () => undefined,
    prompt: async () => undefined,
    listChildren: async () => [],
    getSession: async () => undefined,
    getSessionPermissions: async () => undefined,
    getSessionTodos: async () => [],
  } as never);
  const created = await core.createSwarm({ name: "nr2", projectId: "nr2", coordinatorSessionId: "sess-c2" });
  return { store, core, swarmId: created.swarm.id };
}

describe("envelope + broker prompt", () => {
  test("formatEnvelope renders the noreply marker", () => {
    const env = formatEnvelope(fakeMessage({ noreply: true }), new Map([["m-a", "A"]]));
    expect(env).toContain("[noreply — no response expected");
  });

  test("formatEnvelope omits the marker when not noreply", () => {
    const env = formatEnvelope(fakeMessage(), new Map([["m-a", "A"]]));
    expect(env).not.toContain("noreply");
  });

  async function brokerSetup() {
    const { SQLiteStore } = await import("../../src/storage/sqlite-store.ts");
    const store = new SQLiteStore(":memory:");
    await store.ready();
    const now = Date.now();
    await store.insertSwarm({
      id: "s", projectId: "p", name: "s", coordinatorSessionId: "sess-c",
      coordinatorMemberId: "m-c", status: "active", policies: { ...DEFAULT_POLICIES },
      createdAt: now, updatedAt: now, directory: ".",
    });
    await store.insertMember({ id: "m-a", swarmId: "s", name: "A", sessionId: "sess-a", role: "worker", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    await store.insertMember({ id: "m-c", swarmId: "s", name: "coordinator", sessionId: "sess-c", role: "coordinator", status: "idle", model: undefined, workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    const prompts: string[] = [];
    const broker = new Broker(
      store,
      { async promptAsync(input: { text: string }) { prompts.push(input.text); } } as never,
      { deliveryCooldownMs: 0 },
    );
    return { store, broker, prompts };
  }

  test("all-noreply batch says no response needed", async () => {
    const { broker, prompts, store } = await brokerSetup();
    await store.insertMessages([
      fakeMessage({ swarmId: "s", fromMemberId: "m-a", to: { type: "member", memberId: "m-a" }, noreply: true }),
      fakeMessage({ swarmId: "s", fromMemberId: "m-a", to: { type: "member", memberId: "m-a" }, noreply: true }),
    ]);
    await broker.deliverToIdleMember("m-a", "sess-a");
    expect(prompts.length).toBe(1);
    expect(prompts[0]).toContain("None of these messages expect a reply");
    expect(prompts[0]).toContain("do not respond unless you can act or escalate");
  });

  test("mixed batch keeps the reply protocol line", async () => {
    const { broker, prompts, store } = await brokerSetup();
    await store.insertMessages([
      fakeMessage({ swarmId: "s", fromMemberId: "m-a", to: { type: "member", memberId: "m-a" }, noreply: true }),
      fakeMessage({ swarmId: "s", fromMemberId: "m-a", to: { type: "member", memberId: "m-a" }, noreply: false }),
    ]);
    await broker.deliverToIdleMember("m-a", "sess-a");
    expect(prompts[0]).toContain("Reply to senders with swarm_message");
    expect(prompts[0]).not.toContain("None of these messages expect a reply");
  });
});
