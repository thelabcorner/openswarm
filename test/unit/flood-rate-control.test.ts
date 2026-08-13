import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore } from "../../src/core/swarm.ts";
import { Broker } from "../../src/messaging/broker.ts";
import { NEED_RATE_LIMITED_GUIDANCE } from "../../src/messaging/need.ts";
import { SwarmPluginRuntime } from "../../src/plugin.ts";
import { OpenCodeRuntime } from "../../src/runtime/opencode-runtime.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";
import type { SwarmMessage } from "../../src/core/types.ts";

/**
 * Flood rate controls (task t-flood-rate):
 *   (a) inbox delivery throttle batches bursts (8 msgs -> <=5 prompts within
 *       the window, the rest QUEUED, delivered after the window boundary),
 *   (b) URGENT bypasses the inbox throttle,
 *   (c) per-sender send quota: over-quota warns + broadcasts/mentions are
 *       suppressed for the window while direct sends keep working,
 *   (d) mention fan-out cap (10),
 *   (e) cross-swarm force quota,
 *   (f) hive_need rate cap with 'need rate-limited — retry later' guidance,
 *   (g) digest-health flip damping (flip-flop -> at most one notice per 5 min),
 *   (h) policies override the default numbers.
 *
 * A single mutable clock drives both the core and the broker so windows can
 * be advanced deterministically. The clock starts at the real Date.now so
 * urgent expiresAt comparisons (which use the real clock in markDelivered)
 * stay consistent.
 */

class RecordingRuntime implements AgentRuntime {
  readonly kind = "fake";
  sessions = new Map<string, RuntimeSession>();
  seq = 0;
  /** Recorded promptAsync calls: sessionID + text. */
  calls: Array<{ sessionID: string; text: string }> = [];
  failPrompts = false;

  async createSession(input: { title: string; directory?: string; metadata?: Record<string, unknown>; agent?: string }): Promise<RuntimeSession> {
    const id = `ses-flood-${++this.seq}`;
    const s: RuntimeSession = {
      id,
      title: input.title,
      directory: input.directory ?? ".",
      parentID: undefined,
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
  async promptAsync(input: { text: string; model?: { providerID: string; modelID: string }; agent?: string }, sessionID: string): Promise<void> {
    if (this.failPrompts) throw new Error("injected prompt failure");
    this.calls.push({ sessionID, text: input.text });
  }
  async abort(): Promise<void> {}
  async getStatus(): Promise<any> { return { type: "idle" }; }
  async getMessages(): Promise<any[]> { return []; }
  async listModels(): Promise<any[]> { return []; }
  async resolveModel(): Promise<{ providerID: string; modelID: string } | undefined> { return undefined; }
}

let dir: string;
let store: SQLiteStore;
let runtime: RecordingRuntime;
let nowMs: number;
const clock = () => nowMs;
const advance = (ms: number) => { nowMs += ms; };

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-flood-rate-"));
  store = new SQLiteStore(join(dir, "flood.db"));
  await store.ready();
  runtime = new RecordingRuntime();
  nowMs = Date.now();
});

afterAll(async () => {
  await store.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

async function makeHarness(policies: Record<string, unknown> = {}) {
  const tag = Math.random().toString(36).slice(2, 8);
  const core = new SwarmCore(store, runtime, { now: clock });
  // Broker WITHOUT an explicit maxInboxPerMin so the swarm policy (or the
  // default) governs the inbox budget.
  const broker = new Broker(store, runtime, { deliveryCooldownMs: 0, now: clock });
  core.setWakeDeliverer((memberId, sessionId) => broker.deliverToIdleMember(memberId, sessionId));
  const coordSession = `ses-flood-coord-${tag}`;
  const { swarm, coordinator } = await core.createSwarm({
    name: `flood-${tag}`,
    projectId: "proj",
    coordinatorSessionId: coordSession,
    policies: policies as never,
  });
  return { core, broker, swarm, coordinator, coordSession };
}

function promptsFor(runtime: RecordingRuntime, sessionId: string): number {
  return runtime.calls.filter((c) => c.sessionID === sessionId).length;
}

async function deliveredTo(store: SQLiteStore, memberId: string): Promise<SwarmMessage[]> {
  const all = await store.listMessagesBySwarm((await store.getMemberById(memberId))!.swarmId, 500);
  return all.filter((m) => m.to.type === "member" && m.to.memberId === memberId && m.deliveryState === "delivered");
}

describe("(a) inbox delivery throttle — burst batches, rest queued then delivered", () => {
  test("8 messages in a burst -> <=5 prompts in the window, 3 queued, delivered after the window", async () => {
    const { core, broker, swarm, coordinator } = await makeHarness();
    const worker = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "impl" });

    for (let i = 0; i < 8; i++) {
      await core.sendMessage({
        swarmId: swarm.id,
        fromMemberId: coordinator.id,
        to: "worker",
        kind: "message",
        message: `burst-${i}`,
      });
    }

    // At most 5 prompts within the window (default MAX_INBOX_PER_MIN=5).
    expect(promptsFor(runtime, worker.sessionId)).toBeLessThanOrEqual(5);
    // The remaining mail stayed QUEUED (no prompt fired for it).
    const pending = await store.listPendingMessages(worker.id);
    expect(pending.length).toBe(8 - promptsFor(runtime, worker.sessionId));
    expect(pending.length).toBeGreaterThan(0);

    // Deliver after the window boundary: the F-M7 sweep would call
    // deliverToIdleMember again; here we advance the clock and deliver.
    advance(60_001);
    const delivered = await broker.deliverToIdleMember(worker.id, worker.sessionId);
    expect(delivered).toBe(pending.length);
    expect(await store.listPendingMessages(worker.id)).toHaveLength(0);
    // All 8 messages eventually landed (delivered state).
    expect((await deliveredTo(store, worker.id)).length).toBe(8);
  });

  test("(h) policies override: maxInboxPerMin: 2 caps the burst at 2 prompts", async () => {
    const { core, swarm, coordinator } = await makeHarness({ maxInboxPerMin: 2 });
    const worker = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "impl" });

    for (let i = 0; i < 4; i++) {
      await core.sendMessage({
        swarmId: swarm.id,
        fromMemberId: coordinator.id,
        to: "worker",
        kind: "message",
        message: `b-${i}`,
      });
    }
    expect(promptsFor(runtime, worker.sessionId)).toBeLessThanOrEqual(2);
    expect((await store.listPendingMessages(worker.id)).length).toBeGreaterThanOrEqual(2);
  });
});

describe("(b) URGENT bypasses the inbox throttle", () => {
  test("after the budget is spent, an urgent message is still delivered immediately", async () => {
    const { core, broker, swarm, coordinator } = await makeHarness();
    const worker = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "impl" });

    // Spend the whole 5-prompt budget with normal mail.
    for (let i = 0; i < 5; i++) {
      await core.sendMessage({
        swarmId: swarm.id,
        fromMemberId: coordinator.id,
        to: "worker",
        kind: "message",
        message: `fill-${i}`,
      });
    }
    expect(promptsFor(runtime, worker.sessionId)).toBe(5);

    // URGENT bypasses even with the budget exhausted.
    const urgent = await core.sendMessage({
      swarmId: swarm.id,
      fromMemberId: coordinator.id,
      to: "worker",
      kind: "message",
      message: "urgent-flood",
      priority: "urgent",
    });
    expect(urgent[0]!.deliveryState).toBe("delivered");
    expect(promptsFor(runtime, worker.sessionId)).toBe(6);

    // Non-urgent mail after the budget is spent stays queued (no prompt).
    for (let i = 0; i < 3; i++) {
      await core.sendMessage({
        swarmId: swarm.id,
        fromMemberId: coordinator.id,
        to: "worker",
        kind: "message",
        message: `extra-${i}`,
      });
    }
    expect(promptsFor(runtime, worker.sessionId)).toBe(6);
    expect((await store.listPendingMessages(worker.id)).length).toBe(3);
    void broker;
  });
});

describe("(c) per-sender send quota — warn + broadcast/mention suppression", () => {
  test("over-quota sender: warn finding, broadcasts suppressed, direct sends still work, mentions dropped", async () => {
    const { core, swarm, coordinator } = await makeHarness({ senderSendQuotaPerMin: 3 });
    const alice = await core.spawnMember({ swarmId: swarm.id, name: "alice", role: "a" });
    const bob = await core.spawnMember({ swarmId: swarm.id, name: "bob", role: "b" });
    const carol = await core.spawnMember({ swarmId: swarm.id, name: "carol", role: "c" });

    // 3 sends within quota — all delivered.
    for (let i = 0; i < 3; i++) {
      const msgs = await core.sendMessage({
        swarmId: swarm.id,
        fromMemberId: alice.id,
        to: "bob",
        kind: "message",
        message: `ok-${i}`,
      });
      expect(msgs.length).toBe(1);
    }

    // 4th send (a broadcast) is over quota: suppressed -> no recipients.
    const overBroadcast = await core.sendMessage({
      swarmId: swarm.id,
      fromMemberId: alice.id,
      to: "*",
      kind: "message",
      message: "flood broadcast",
    });
    expect(overBroadcast.length).toBe(0); // broadcast suppressed for the window

    // The sender got exactly ONE noreply flood warning (once per window).
    const aliceMsgs = await store.listMessagesBySwarm(swarm.id, 100);
    const warnings = aliceMsgs.filter(
      (m) => m.to.type === "member" && m.to.memberId === alice.id && m.body.text.includes("you are sending too many messages"),
    );
    expect(warnings.length).toBe(1);
    expect(warnings[0]!.noreply).toBe(true);

    // Direct sends still work while over quota.
    const direct = await core.sendMessage({
      swarmId: swarm.id,
      fromMemberId: alice.id,
      to: "bob",
      kind: "message",
      message: "direct still works",
    });
    expect(direct.length).toBe(1);

    // Mention fan-out is suppressed for the window too: @carol must NOT be
    // auto-notified (only the direct recipient bob).
    const withMention = await core.sendMessage({
      swarmId: swarm.id,
      fromMemberId: alice.id,
      to: "bob",
      kind: "message",
      message: "for bob @carol",
    });
    const mentionedIds = new Set(withMention.map((m) => m.to.memberId));
    expect(withMention.length).toBe(1);
    expect(mentionedIds.has(bob.id)).toBe(true);
    expect(mentionedIds.has(carol.id)).toBe(false);

    // One warning per window: another over-quota broadcast does NOT re-warn.
    await core.sendMessage({ swarmId: swarm.id, fromMemberId: alice.id, to: "*", kind: "message", message: "flood again" });
    const warningsAfter = (await store.listMessagesBySwarm(swarm.id, 200)).filter(
      (m) => m.to.type === "member" && m.to.memberId === alice.id && m.body.text.includes("you are sending too many messages"),
    );
    expect(warningsAfter.length).toBe(1);
    void coordinator;
  });
});

describe("(d) mention fan-out cap (default 10)", () => {
  test("12 mentioned members -> only 10 auto-notified (extra mentions ignored)", async () => {
    const { core, swarm, coordinator } = await makeHarness({ maxMembers: 20, maxConcurrentMembers: 20 });
    // Coordinator + 13 workers: w0 is the direct recipient, w1..w12 mentioned.
    const names: string[] = [];
    for (let i = 0; i < 13; i++) names.push(`w${i}`);
    for (const n of names) {
      await core.spawnMember({ swarmId: swarm.id, name: n, role: "r" });
    }
    const directName = "w0";
    const mentionNames = names.filter((n) => n !== directName); // 12 mentions
    const body = `hi ${directName} ${mentionNames.map((n) => `@${n}`).join(" ")}`;

    const msgs = await core.sendMessage({
      swarmId: swarm.id,
      fromMemberId: coordinator.id,
      to: directName,
      kind: "message",
      message: body,
    });
    // 1 direct recipient + 10 mentioned (cap) = 11; 2 extra mentions ignored.
    expect(msgs.length).toBe(11);
    const recipientIds = new Set(msgs.map((m) => m.to.memberId));
    const mentionedMembers = await store.listMembers(swarm.id);
    const byName = new Map(mentionedMembers.map((m) => [m.name, m.id]));
    expect(recipientIds.has(byName.get(directName)!)).toBe(true);
    // The last two mentioned members were ignored.
    expect(recipientIds.has(byName.get(mentionNames[10]!))).toBe(false);
    expect(recipientIds.has(byName.get(mentionNames[11]!))).toBe(false);
  });

  test("(h) policies override: mentionFanOutCap: 3 caps fan-out at 3", async () => {
    const { core, swarm, coordinator } = await makeHarness({ mentionFanOutCap: 3 });
    for (let i = 0; i < 6; i++) {
      await core.spawnMember({ swarmId: swarm.id, name: `m${i}`, role: "r" });
    }
    const msgs = await core.sendMessage({
      swarmId: swarm.id,
      fromMemberId: coordinator.id,
      to: "m0",
      kind: "message",
      message: "hi @m1 @m2 @m3 @m4 @m5",
    });
    expect(msgs.length).toBe(4); // m0 direct + m1..m3 (cap 3)
  });
});

describe("(e) cross-swarm force quota", () => {
  test("over-quota force sends still deliver direct but warn; force broadcast suppressed", async () => {
    const { core, swarm: target, coordinator: targetCoord } = await makeHarness({ senderForceQuotaPerMin: 1 });
    const tag = Math.random().toString(36).slice(2, 8);
    const home = await core.createSwarm({
      name: `home-${tag}`,
      projectId: "proj",
      coordinatorSessionId: `ses-home-${tag}`,
    });
    const outsider = await core.spawnMember({ swarmId: home.swarm.id, name: "outsider", role: "x" });
    const targetWorker = await core.spawnMember({ swarmId: target.id, name: "target-worker", role: "t" });

    // Force send #1 within quota: delivers.
    const first = await core.sendMessage({
      swarmId: target.id,
      fromMemberId: outsider.id,
      to: "target-worker",
      kind: "finding",
      message: "cross one",
      force: true,
    });
    expect(first.length).toBe(1);

    // Force send #2 over the force quota (1/min): direct still delivers...
    const second = await core.sendMessage({
      swarmId: target.id,
      fromMemberId: outsider.id,
      to: "target-worker",
      kind: "finding",
      message: "cross two",
      force: true,
    });
    expect(second.length).toBe(1);

    // ...but the sender is warned (once per window).
    const targetMsgs = await store.listMessagesBySwarm(target.id, 100);
    const warnings = targetMsgs.filter(
      (m) => m.to.type === "member" && m.to.memberId === outsider.id && m.body.text.includes("you are sending too many messages"),
    );
    expect(warnings.length).toBe(1);

    // Force BROADCAST over quota is suppressed (no recipients).
    const overBroadcast = await core.sendMessage({
      swarmId: target.id,
      fromMemberId: outsider.id,
      to: "*",
      kind: "message",
      message: "cross flood",
      force: true,
    });
    expect(overBroadcast.length).toBe(0);
    void targetCoord;
    void targetWorker;
  });
});

describe("(f) hive_need rate cap", () => {
  test("excess whispers/shouts return 'need rate-limited — retry later' without sending", async () => {
    const { core, swarm, coordinator } = await makeHarness({ needWhisperPerWindow: 2, needShoutPerWindow: 1 });
    const seeker = await core.spawnMember({ swarmId: swarm.id, name: "seeker", role: "curious" });
    const helper = await core.spawnMember({ swarmId: swarm.id, name: "helper", role: "knows things" });
    const countMsgs = () => store.listMessagesBySwarm(swarm.id, 500).then((m) => m.length);

    // 2 whispers within quota (1 finding each to the matching helper).
    for (let i = 0; i < 2; i++) {
      const r = await core.deliverNeed({
        swarmId: swarm.id,
        fromMemberId: seeker.id,
        query: "things",
        need: `need-${i}`,
        tier: "whisper",
      });
      expect(r.recipients.some((x) => x.name === "helper")).toBe(true);
    }
    const afterWhispers = await countMsgs();

    // 3rd whisper is rate-limited: guidance, nothing sent.
    const r3 = await core.deliverNeed({
      swarmId: swarm.id,
      fromMemberId: seeker.id,
      query: "things",
      need: "need-3",
      tier: "whisper",
    });
    expect(r3.guidance).toBe(NEED_RATE_LIMITED_GUIDANCE);
    expect(r3.delivered.length).toBe(0);
    expect(r3.recipients.length).toBe(0);
    expect(await countMsgs()).toBe(afterWhispers); // no messages from the cap

    // Shout cap is separate: 1 shout ok, 2nd rate-limited.
    const shout1 = await core.deliverNeed({
      swarmId: swarm.id,
      fromMemberId: seeker.id,
      query: "things",
      need: "shout-1",
      tier: "shout",
    });
    expect(shout1.recipients.length).toBeGreaterThan(0);
    const afterShout = await countMsgs();
    const shout2 = await core.deliverNeed({
      swarmId: swarm.id,
      fromMemberId: seeker.id,
      query: "things",
      need: "shout-2",
      tier: "shout",
    });
    expect(shout2.guidance).toBe(NEED_RATE_LIMITED_GUIDANCE);
    expect(shout2.delivered.length).toBe(0);
    expect(await countMsgs()).toBe(afterShout); // no messages from the cap

    // A DIFFERENT member is unaffected (cap is per-member).
    const other = await core.spawnMember({ swarmId: swarm.id, name: "other-seeker", role: "curious" });
    const ok = await core.deliverNeed({
      swarmId: swarm.id,
      fromMemberId: other.id,
      query: "things",
      need: "other-whisper",
      tier: "whisper",
    });
    expect(ok.guidance).not.toBe(NEED_RATE_LIMITED_GUIDANCE);
    void coordinator;
  });
});

describe("(g) digest-health flip damping", () => {
  test("flip-flop -> at most one flip notice per 5 min regardless of oscillation", async () => {
    const { core, swarm, coordinator } = await makeHarness();
    const before = runtime.calls.length;

    // Flip 1 (fresh -> stale): notifies.
    const r1 = await core.notifyDigestFlip({ swarmId: swarm.id, health: "stale", lastKnownHealth: "fresh" });
    expect(r1.notified).toBe(true);
    expect(runtime.calls.length).toBe(before + 1); // one finding to the coordinator

    // Flip 2 (stale -> fresh) within the 5-min window: damped, no notice.
    const r2 = await core.notifyDigestFlip({ swarmId: swarm.id, health: "fresh", lastKnownHealth: "stale" });
    expect(r2.notified).toBe(false);
    expect(runtime.calls.length).toBe(before + 1);

    // Flip 3 (fresh -> stale) still within the window: damped.
    const r3 = await core.notifyDigestFlip({ swarmId: swarm.id, health: "stale", lastKnownHealth: "fresh" });
    expect(r3.notified).toBe(false);
    expect(runtime.calls.length).toBe(before + 1);

    // After 5 minutes the next flip notifies again.
    advance(5 * 60_000 + 1);
    const r4 = await core.notifyDigestFlip({ swarmId: swarm.id, health: "fresh", lastKnownHealth: "stale" });
    expect(r4.notified).toBe(true);
    expect(runtime.calls.length).toBe(before + 2);
    void coordinator;
  });

  test("(h) policies override: digestFlipNoticeMinMs: 1_000 shortens the damping window", async () => {
    const { core, swarm, coordinator } = await makeHarness({ digestFlipNoticeMinMs: 1_000 });
    const r1 = await core.notifyDigestFlip({ swarmId: swarm.id, health: "stale", lastKnownHealth: "fresh" });
    expect(r1.notified).toBe(true);
    advance(500);
    const r2 = await core.notifyDigestFlip({ swarmId: swarm.id, health: "fresh", lastKnownHealth: "stale" });
    expect(r2.notified).toBe(false); // still within 1s
    advance(501);
    const r3 = await core.notifyDigestFlip({ swarmId: swarm.id, health: "stale", lastKnownHealth: "fresh" });
    expect(r3.notified).toBe(true); // past the 1s override window
    void coordinator;
  });

  test("no flip -> never notifies", async () => {
    const { core, swarm } = await makeHarness();
    const r = await core.notifyDigestFlip({ swarmId: swarm.id, health: "stale", lastKnownHealth: "stale" });
    expect(r.notified).toBe(false);
  });
});

describe("team-sync digest-flap damping (completed-only deltas)", () => {
  test("completed-only task deltas fire at most once per teamSyncCompletedCooldownMs", async () => {
    // Construct the plugin runtime directly (no singleton) with a recording
    // promptAsync so sync digests can be counted.
    const dir2 = mkdtempSync(join(tmpdir(), "swarms-flood-tsync-"));
    const store2 = new SQLiteStore(join(dir2, "tsync.db"));
    await store2.ready();
    const fakeClient = {
      session: {
        create: async (opts: any) => ({
          data: { id: `ses-ts-${Math.random().toString(36).slice(2, 8)}`, title: opts.body?.title, parentID: undefined, directory: "." },
          error: undefined,
        }),
        get: async () => ({ data: null, error: undefined }),
        children: async () => ({ data: [], error: undefined }),
        messages: async () => ({ data: [], error: undefined }),
        status: async () => ({ data: {}, error: undefined }),
        abort: async () => ({ data: undefined, error: undefined }),
        update: async () => ({ data: {}, error: undefined }),
        prompt: async () => ({ data: { info: {} }, error: undefined }),
        promptAsync: async () => ({ data: undefined, error: undefined }),
      },
    };
    const rt = new SwarmPluginRuntime(
      store2,
      new OpenCodeRuntime(fakeClient as never, ".", "."),
      100_000, // sweep long enough to never interfere
      false,
      { providerID: "opencode", modelID: "deepseek-v4-flash" },
      join(dir2, "emergency.json"),
    );
    const prompts: Array<{ sessionID: string; text: string }> = [];
    (rt as any)["runtime"].promptAsync = async (input: any, sessionID: string) => {
      prompts.push({ sessionID, text: input.text });
    };
    try {
      const tag = Math.random().toString(36).slice(2, 8);
      const { swarm, coordinator } = await rt.core.createSwarm({
        name: `tsync-flood-${tag}`,
        projectId: "proj",
        coordinatorSessionId: `ses-ts-lead-${tag}`,
        policies: { teamSyncCompletedCooldownMs: 50 } as never,
      });
      const worker = await rt.core.spawnMember({ swarmId: swarm.id, name: "backend", role: "backend engineer" });

      // A ready unassigned task makes the worker a digest recipient
      // (affinity: role + task share >= 2 tokens).
      const ready = await rt.core.createTask({ swarmId: swarm.id, title: "backend api engineer work", createdByMemberId: coordinator.id });
      await store2.updateTaskStatus(ready.id, "ready");
      await store2.updateMemberStatus(worker.id, "idle");

      // First sync: fresh fingerprint -> fires.
      await rt.syncSwarm(swarm.id);
      expect(prompts.filter((p) => p.text.includes("[TEAM SYNC"))).toHaveLength(1);

      // Completed-only delta within the 50ms override window -> damped.
      const doneA = await rt.core.createTask({ swarmId: swarm.id, title: "backend cleanup a", createdByMemberId: coordinator.id });
      await store2.updateTaskStatus(doneA.id, "completed");
      await rt.syncSwarm(swarm.id);
      expect(prompts.filter((p) => p.text.includes("[TEAM SYNC"))).toHaveLength(1);

      // After the window a completed-only delta fires again.
      await new Promise((r) => setTimeout(r, 80));
      const doneB = await rt.core.createTask({ swarmId: swarm.id, title: "backend cleanup b", createdByMemberId: coordinator.id });
      await store2.updateTaskStatus(doneB.id, "completed");
      await rt.syncSwarm(swarm.id);
      expect(prompts.filter((p) => p.text.includes("[TEAM SYNC"))).toHaveLength(2);

      // Ready-task deltas stay gated by the normal 45s cooldown even after the
      // completed-only window lapsed (both damped here — nothing to wait 45s for).
      const readyB = await rt.core.createTask({ swarmId: swarm.id, title: "backend api engineer work b", createdByMemberId: coordinator.id });
      await store2.updateTaskStatus(readyB.id, "ready");
      await rt.syncSwarm(swarm.id);
      expect(prompts.filter((p) => p.text.includes("[TEAM SYNC"))).toHaveLength(2);
    } finally {
      await store2.close();
      try { rmSync(dir2, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });
});
