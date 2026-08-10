import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { SwarmCore } from "../../src/core/swarm.ts";
import type { AgentRuntime, RuntimeSession } from "../../src/runtime/runtime-types.ts";

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

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-notice-int-"));
  store = new SQLiteStore(join(dir, "notice.db"));
  await store.ready();
  runtime = new FakeRuntime();
  core = new SwarmCore(store, runtime);
});

afterAll(async () => {
  await store.close();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

async function makeSwarm() {
  const tag = Math.random().toString(36).slice(2, 8);
  const { swarm, coordinator } = await core.createSwarm({
    name: `notice-${tag}`,
    projectId: "proj",
    coordinatorSessionId: `ses-lead-notice-${tag}`,
  });
  return { swarm, coordinator };
}

describe("notifyConsolidation (integration, exactly-once)", () => {
  test("notable run delivers coordinator finding + compact broadcast, tagged with runId", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const m = await core.spawnMember({ swarmId: swarm.id, name: "worker", role: "impl" });
    const result = {
      runId: `cons-${Date.now()}-abc`,
      coordinator: "worker",
      retained: 5,
      pruned: 2,
      upgraded: 1,
      contradictions: [{ factHash: "fh", text: "flag" }],
    };
    const before = runtime.prompts.length;
    const msgs = await core.notifyConsolidation({ swarmId: swarm.id, result });
    expect(msgs.length).toBeGreaterThanOrEqual(2); // coordinator finding + broadcast
    // Coordinator received the detail (fenced counts verbatim).
    const coordMsgs = await store.listPendingMessages(coordinator.id);
    const detail = coordMsgs.find((x) => x.body.text.includes("[HIVE CONSOLIDATION]"));
    expect(detail?.body.text).toContain("retained 5, pruned 2, upgraded 1");
    expect(detail?.correlationId).toBe(`consolidation:${result.runId}`);
    // The worker member received the compact broadcast.
    const workerMsgs = await store.listPendingMessages(m.id);
    expect(workerMsgs.some((x) => x.body.text.includes("retained 5, pruned 2, upgraded 1"))).toBe(true);
    void before;
  });

  test("non-notable run emits nothing", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const before = runtime.prompts.length;
    const msgs = await core.notifyConsolidation({
      swarmId: swarm.id,
      result: { runId: "cons-empty-1", retained: 0, pruned: 0, upgraded: 0 },
    });
    expect(msgs.length).toBe(0);
    expect(runtime.prompts.length).toBe(before);
    void coordinator;
  });

  test("guidance from the result is rendered fenced in the coordinator notice (P3 carry-over)", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const result = {
      runId: `cons-guid-${Date.now()}`,
      coordinator: "worker",
      retained: 3,
      pruned: 1,
      upgraded: 0,
      guidance: "re-plan the migration; gold beliefs were retained",
    };
    await core.notifyConsolidation({ swarmId: swarm.id, result });
    const coordMsgs = await store.listPendingMessages(coordinator.id);
    const detail = coordMsgs.find((x) => x.body.text.includes("[HIVE CONSOLIDATION]"));
    expect(detail?.body.text).toContain("guidance:");
    expect(detail?.body.text).toContain("re-plan the migration");
    expect(detail?.body.text).toContain("[DATA"); // fenced
    expect(detail?.body.text.startsWith("re-plan the migration")).toBe(false);
  });
});

describe("notifyPruning (integration, non-trivial only)", () => {
  test("pruned > 0 delivers one truthful finding; 0 emits nothing", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const zero = await core.notifyPruning({ swarmId: swarm.id, pruned: 0 });
    expect(zero.length).toBe(0);
    const pruned = await core.notifyPruning({ swarmId: swarm.id, pruned: 4 });
    expect(pruned.length).toBe(1);
    const coordMsgs = await store.listPendingMessages(coordinator.id);
    expect(coordMsgs.some((x) => x.body.text.includes("4 stale belief(s)"))).toBe(true);
  });
});

describe("notifyDigestFlip (integration, transition dedupe)", () => {
  test("flip fresh->stale notifies once; same-health repeats do not", async () => {
    const { swarm, coordinator } = await makeSwarm();
    const flip = await core.notifyDigestFlip({ swarmId: swarm.id, health: "stale", lastKnownHealth: "fresh" });
    expect(flip.notified).toBe(true);
    // Same health again with lastKnown=stale: no flip.
    const same = await core.notifyDigestFlip({ swarmId: swarm.id, health: "stale", lastKnownHealth: "stale" });
    expect(same.notified).toBe(false);
    // Flip back healthy.
    const back = await core.notifyDigestFlip({ swarmId: swarm.id, health: "fresh", lastKnownHealth: "stale" });
    expect(back.notified).toBe(true);
    const coordMsgs = await store.listPendingMessages(coordinator.id);
    const digestNotices = coordMsgs.filter((x) => x.body.text.includes("[HIVE DIGEST]"));
    expect(digestNotices.length).toBe(2); // exactly the two flips
    void coordinator;
  });

  test("first observation with no lastKnown does not notify (no false flip)", async () => {
    const { swarm } = await makeSwarm();
    const first = await core.notifyDigestFlip({ swarmId: swarm.id, health: "stale" });
    expect(first.notified).toBe(false);
  });
});
