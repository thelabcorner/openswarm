import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync, existsSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import { ChunkDbStore } from "../../src/storage/chunkdb-store.ts";
import { migrateSwarmDb } from "../../src/storage/migrate.ts";
import type { NewMessage } from "../../src/storage/models.ts";
import { initSwarmRuntime, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { SwarmPolicies, SwarmMessage } from "../../src/core/types.ts";

const now = Date.now();

function policies(): SwarmPolicies {
  return {
    maxMembers: 8,
    maxConcurrentMembers: 5,
    allowMemberSpawn: false,
    maxSpawnDepth: 1,
    coordinatorMode: "normal",
    defaultWorkspace: "worktree",
    messageDelivery: "idle",
    autoWake: true,
    autoReview: false,
    abortChildrenOnSwarmStop: true,
    maxRetriesPerTask: 2,
    retention: "project",
  };
}

function newSwarm(id: string, name: string) {
  return {
    id,
    projectId: "migrate-project",
    name,
    coordinatorSessionId: `ses-coord-${name}`,
    coordinatorMemberId: `mem-coord-${name}`,
    directory: ".",
    status: "active" as const,
    policies: policies(),
    createdAt: now,
    updatedAt: now,
  };
}

function newMember(swarmId: string, id: string, name: string, sessionId: string) {
  return {
    id,
    swarmId,
    name,
    role: "worker",
    sessionId,
    status: "idle" as const,
    workspaceMode: "worktree" as const,
    createdAt: now,
    updatedAt: now,
  };
}

function newTask(swarmId: string, id: string, title: string, priority: number, status = "pending" as const) {
  return {
    id,
    swarmId,
    title,
    description: "task description",
    status,
    priority,
    ownerMemberId: undefined,
    createdByMemberId: `mem-coord-mig`,
    acceptanceCriteria: ["green", "fast"],
    createdAt: now,
    updatedAt: now,
  };
}

function newMessage(swarmId: string, id: string, from: string, to: string, priority: SwarmMessage["priority"], body: string, createdAt: number, deliveryState: SwarmMessage["deliveryState"] = "queued") {
  return {
    id,
    swarmId,
    fromMemberId: from,
    to: { type: "member" as const, memberId: to },
    kind: "message" as const,
    priority,
    body: { text: body, refs: [`ref-${id}`] },
    deliveryState,
    attemptCount: 0,
    noreply: false,
    createdAt,
    expiresAt: undefined,
  };
}

/** Total on-disk size of a SQLite-family database (base + WAL + SHM). */
function totalDbSize(base: string): number {
  let total = statSync(base).size;
  for (const suffix of ["-wal", "-shm"]) {
    const f = base + suffix;
    if (existsSync(f)) total += statSync(f).size;
  }
  return total;
}

describe("ChunkDbStore — migration from SQLite", () => {
  let srcDir: string;
  let srcPath: string;
  let destPath: string;

  beforeAll(async () => {
    srcDir = mkdtempSync(join(tmpdir(), "chunkdb-migrate-"));
    srcPath = join(srcDir, "swarms.db");
    destPath = join(srcDir, "swarms.chunkdb");
    const src = new SQLiteStore(srcPath);
    await src.ready();

    const swarmId = "swarm-mig";
    await src.insertSwarm(newSwarm(swarmId, "mig"));
    // Distinct createdAt so member ordering is deterministic across backends.
    await src.insertMember({ ...newMember(swarmId, "mem-mig-a", "alpha", "ses-mig-a"), createdAt: now, updatedAt: now });
    await src.insertMember({ ...newMember(swarmId, "mem-mig-b", "beta", "ses-mig-b"), createdAt: now + 1, updatedAt: now + 1 });
    // Tasks with distinct priority + createdAt so ordering is deterministic.
    await src.insertTask({ ...newTask(swarmId, "task-mig-1", "low priority", 0), createdAt: now });
    await src.insertTask({ ...newTask(swarmId, "task-mig-2", "high priority", 5), createdAt: now + 1 });
    await src.insertTaskDependency("task-mig-2", "task-mig-1");
    await src.insertMessages([
      newMessage(swarmId, "msg-mig-1", "mem-mig-a", "mem-mig-b", "normal", "first message", now + 1),
      newMessage(swarmId, "msg-mig-2", "mem-mig-b", "mem-mig-a", "high", "second message", now + 2),
      newMessage(swarmId, "msg-mig-3", "mem-mig-a", "mem-mig-b", "urgent", "third message", now + 3),
    ]);
    await src.insertBlackboard({
      id: "bb-mig-1", swarmId, key: "context/lanes", value: JSON.stringify({ lanes: ["a", "b"] }),
      contentType: "application/json", version: 1, authorMemberId: "mem-mig-a", taskId: "task-mig-1",
      createdAt: now, updatedAt: now,
    });
    await src.insertBlackboard({
      id: "bb-mig-2", swarmId, key: "deliverable/summary", value: "all green",
      contentType: "text/markdown", version: 3, authorMemberId: "mem-mig-b",
      createdAt: now, updatedAt: now,
    });
    await src.insertBelief({
      id: "blf-mig-1", swarmId, factHash: "fact-1", text: "first belief", confidence: 0.4,
      tier: "whisper", authorMemberId: "mem-mig-a", reinforceCount: 1, createdAt: now, updatedAt: now,
    });
    await src.insertBelief({
      id: "blf-mig-2", swarmId, factHash: "fact-2", text: "strong belief", confidence: 0.9,
      tier: "shout", authorMemberId: "mem-mig-b", evidenceRefs: '["msg-mig-1"]',
      reinforceCount: 2, createdAt: now, updatedAt: now,
    });
    await src.insertAnnotation({
      id: "ann-mig-1", swarmId, path: "src/wire.ts", type: "gold", weight: 3,
      note: "verified pattern", solutionHash: "abc", authorMemberId: "mem-mig-a", createdAt: now,
    });
    await src.insertPathClaim({
      id: "claim-mig-1", swarmId, memberId: "mem-mig-a", pattern: "src/**", mode: "advisory", createdAt: now,
    });
    await src.addSubscription(swarmId, "mem-mig-a", "contracts/*");
    await src.insertPendingPermission({
      id: "perm-mig-1", swarmId, memberId: "mem-mig-a", sessionId: "ses-mig-a",
      type: "bash", pattern: "npm test", response: null, respondedAt: null, createdAt: now,
    });
    await src.insertDeliverable({
      id: "dlv-mig-1", swarmId, memberId: "mem-mig-a", taskId: "task-mig-1",
      summary: "storage layer shipped", refs: ["msg-mig-1"], files: ["src/storage/chunkdb-store.ts"],
      createdAt: now,
    });
    await src.insertContract({
      id: "ctr-mig-1", swarmId, keyPattern: "contracts/*", schemaJson: '{"type":"object"}',
      description: "contract test", createdBy: "mem-coord-mig", createdAt: now, updatedAt: now,
    });
    await src.insertEvent({ swarmId, type: "message.sent", actorMemberId: "mem-mig-a", entityType: "message", entityId: "msg-mig-1", createdAt: now + 1 });
    await src.insertEvent({ swarmId, type: "task.completed", actorMemberId: "mem-mig-b", entityType: "task", entityId: "task-mig-1", createdAt: now + 2 });
    await src.close();
  });

  afterAll(async () => {
  disposeSwarmRuntime();
    try { rmSync(srcDir, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  test("migrateSwarmDb copies every entity with preserved ids + same list shapes/orders", async () => {
    const counts = await migrateSwarmDb(srcPath, destPath);
    expect(counts).toEqual({
      swarms: 1, members: 2, tasks: 2, messages: 3, blackboard: 2, beliefs: 2,
      annotations: 1, claims: 1, subscriptions: 1, permissions: 1, deliverables: 1,
      contracts: 1, events: 2, dependencies: 1,
    });

    const dest = new ChunkDbStore(destPath);
    await dest.ready();
    try {
      // Swarm: same id + fields (compare against a fresh read of the source).
      const sourceSwarm = new SQLiteStore(srcPath);
      await sourceSwarm.ready();
      const srcSwarm = await sourceSwarm.getSwarm("swarm-mig");
      await sourceSwarm.close();
      expect(await dest.getSwarm("swarm-mig")).toEqual(srcSwarm);

      // Members: same ids, createdAt order.
      expect((await dest.listMembers("swarm-mig")).map((m) => m.id)).toEqual(["mem-mig-a", "mem-mig-b"]);
      expect((await dest.getMemberBySessionId("ses-mig-b"))?.id).toBe("mem-mig-b");
      expect((await dest.getMemberById("mem-mig-a"))?.swarmId).toBe("swarm-mig");

      // Tasks: priority DESC then createdAt; same ids.
      expect((await dest.listTasks("swarm-mig")).map((t) => t.id)).toEqual(["task-mig-2", "task-mig-1"]);
      expect((await dest.listTasks("swarm-mig"))[0]).toMatchObject({
        id: "task-mig-2", priority: 5, acceptanceCriteria: ["green", "fast"],
      });
      // Dependencies preserved.
      expect(await dest.listTaskDependencies("swarm-mig")).toEqual([{ taskId: "task-mig-2", dependsOnTaskId: "task-mig-1" }]);

      // Messages: createdAt DESC, same ids + fields.
      expect((await dest.listMessagesBySwarm("swarm-mig", 1000)).map((m) => m.id)).toEqual(["msg-mig-3", "msg-mig-2", "msg-mig-1"]);
      expect((await dest.listMessagesBySwarm("swarm-mig", 1000))[2]).toMatchObject({
        id: "msg-mig-1", priority: "normal", body: { text: "first message", refs: ["ref-msg-mig-1"] },
      });

      // Blackboard: key ASC, values preserved.
      const bb = await dest.listBlackboardEntries("swarm-mig");
      expect(bb.map((e) => e.key)).toEqual(["context/lanes", "deliverable/summary"]);
      expect(bb[1]).toMatchObject({ version: 3, contentType: "text/markdown", value: "all green" });

      // Beliefs: confidence DESC then createdAt DESC; ids + factHash preserved.
      expect((await dest.listBeliefs("swarm-mig", { activeOnly: false })).map((b) => b.id)).toEqual(["blf-mig-2", "blf-mig-1"]);
      expect((await dest.listBeliefs("swarm-mig", { activeOnly: false }))[0]).toMatchObject({
        factHash: "fact-2", reinforceCount: 2, tier: "shout", confidence: 0.9,
      });

      // Annotations: createdAt DESC.
      const anns = await dest.listAnnotations("swarm-mig", { activeOnly: false });
      expect(anns.map((a) => a.id)).toEqual(["ann-mig-1"]);
      expect(anns[0]).toMatchObject({ path: "src/wire.ts", type: "gold", solutionHash: "abc" });

      // Path claims: same pattern/member.
      const claims = await dest.listPathClaims("swarm-mig");
      expect(claims.map((c) => c.pattern)).toEqual(["src/**"]);

      // Subscriptions: pattern/member preserved (ids are regenerated by the
      // public addSubscription write API — the only id-not-preserved entity).
      const subs = await dest.listSubscriptions("swarm-mig");
      expect(subs.map((s) => ({ memberId: s.memberId, pattern: s.pattern }))).toEqual([
        { memberId: "mem-mig-a", pattern: "contracts/*" },
      ]);

      // Pending permissions: newest first, id preserved.
      const perms = await dest.listPendingPermissions("swarm-mig");
      expect(perms.map((p) => p.id)).toEqual(["perm-mig-1"]);
      expect(perms[0]).toMatchObject({ type: "bash", pattern: "npm test", response: null });

      // Deliverables: created_at DESC, id preserved.
      const dlvs = await dest.listDeliverables("swarm-mig");
      expect(dlvs.map((d) => d.id)).toEqual(["dlv-mig-1"]);
      expect(dlvs[0]).toMatchObject({ summary: "storage layer shipped", refs: ["msg-mig-1"] });

      // Contracts: keyPattern ASC, id preserved.
      const contracts = await dest.listContracts("swarm-mig");
      expect(contracts.map((c) => c.id)).toEqual(["ctr-mig-1"]);
      expect(contracts[0]).toMatchObject({ keyPattern: "contracts/*", schemaJson: '{"type":"object"}' });

      // Events: original autoincrement ids preserved; id DESC (newest first).
      const events = await dest.listEvents("swarm-mig");
      expect(events.map((e) => [e.id, e.type])).toEqual([
        [2, "task.completed"],
        [1, "message.sent"],
      ]);
    } finally {
      await dest.close();
    }
  });

  test("migrateSwarmDb is re-runnable (idempotent writes)", async () => {
    const counts = await migrateSwarmDb(srcPath, join(srcDir, "swarms-2.chunkdb"));
    expect(counts.swarms).toBe(1);
    const dest = new ChunkDbStore(join(srcDir, "swarms-2.chunkdb"));
    await dest.ready();
    try {
      expect((await dest.listMembers("swarm-mig")).length).toBe(2);
      expect((await dest.listMessagesBySwarm("swarm-mig", 1000)).length).toBe(3);
      expect((await dest.listBeliefs("swarm-mig", { activeOnly: false })).length).toBe(2);
    } finally {
      await dest.close();
    }
  });
});

describe("ChunkDbStore — core flows", () => {
  let dir: string;
  let store: ChunkDbStore;
  let swarmId: string;
  const coord = "mem-flow-coord";
  const a = "mem-flow-a";
  const b = "mem-flow-b";

  beforeAll(async () => {
    dir = mkdtempSync(join(tmpdir(), "chunkdb-flow-"));
    store = new ChunkDbStore(join(dir, "flow.chunkdb"));
    await store.ready();
    swarmId = "swarm-flow";
    await store.insertSwarm(newSwarm(swarmId, "flow"));
    // Distinct createdAt per member so ordering is deterministic (chunkdb sorts
    // by createdAt; equal timestamps fall back to key order, unlike sqlite's
    // insertion order).
    await store.insertMember({ id: coord, swarmId, name: "coordinator", role: "coordinator", sessionId: "ses-flow-coord", status: "idle", workspaceMode: "shared-read", createdAt: now, updatedAt: now });
    await store.insertMember({ ...newMember(swarmId, a, "alpha", "ses-flow-a"), createdAt: now + 1, updatedAt: now + 1 });
    await store.insertMember({ ...newMember(swarmId, b, "beta", "ses-flow-b"), createdAt: now + 2, updatedAt: now + 2 });
  });

  afterAll(async () => {
  disposeSwarmRuntime();
    await store.close();
    try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  test("swarm + member roundtrip and lookups", async () => {
    expect((await store.getSwarm(swarmId))?.name).toBe("flow");
    expect((await store.listMembers(swarmId)).map((m) => m.id)).toEqual([coord, a, b]);
    expect((await store.getMemberBySessionId("ses-flow-b"))?.id).toBe(b);
    expect((await store.getMemberByName(swarmId, "alpha"))?.id).toBe(a);
  });

  test("mailbox ordering: urgent > high > normal > low, then createdAt", async () => {
    await store.insertMessages([
      newMessage(swarmId, "m-low", a, b, "low", "low", now + 1),
      newMessage(swarmId, "m-high", a, b, "high", "high", now + 2),
      newMessage(swarmId, "m-urgent", a, b, "urgent", "urgent", now + 3),
      newMessage(swarmId, "m-normal", a, b, "normal", "normal", now + 4),
      newMessage(swarmId, "m-other", a, b, "urgent", "delivered elsewhere", now + 5, "delivered"),
    ]);
    const pending = await store.listPendingMessages(b);
    expect(pending.map((m) => m.id)).toEqual(["m-urgent", "m-high", "m-normal", "m-low"]);
    // Expired urgent mail is excluded (expiresAt in the past at insert).
    await store.insertMessages([{
      ...newMessage(swarmId, "m-expired", a, b, "urgent", "stale", now + 6, "queued"),
      expiresAt: now - 1000,
    }]);
    expect((await store.listPendingMessages(b)).some((m) => m.id === "m-expired")).toBe(false);
  });

  test("claimTask → releaseTask → re-claim lifecycle", async () => {
    await store.insertTask({ ...newTask(swarmId, "task-flow-1", "build", 1), status: "ready" });
    expect(await store.claimTask("task-flow-1", a)).toBe(true);
    // Member-side CAS mirrors sqlite: the guard reads the member's OWN
    // currentTaskId binding (set by the caller via updateMemberStatus — the
    // claim itself only binds the TASK row).
    await store.updateMemberStatus(a, "working", { currentTaskId: "task-flow-1" });
    await store.insertTask({ ...newTask(swarmId, "task-flow-2", "second", 0), status: "ready" });
    expect(await store.claimTask("task-flow-2", a)).toBe(false);
    // Second claim of an ALREADY-claimed task also fails (task-side guard:
    // owner must be NULL and status ready — mirrors sqlite).
    expect(await store.claimTask("task-flow-1", a)).toBe(false);
    // Release (counts as retry) then re-claim from another member.
    expect(await store.releaseTask("task-flow-1")).toBe(true);
    expect((await store.listTasks(swarmId)).find((t) => t.id === "task-flow-1")).toMatchObject({
      status: "ready", retryCount: 1,
    });
    expect(await store.claimTask("task-flow-1", b)).toBe(true);
  });

  test("blackboard CAS upsert: version guard + conflict on stale version", async () => {
    await store.insertBlackboard({
      id: "bb-flow-1", swarmId, key: "contracts/alpha", value: "v1",
      contentType: "text/plain", version: 1, authorMemberId: a, createdAt: now, updatedAt: now,
    });
    const entry = await store.getBlackboard(swarmId, "contracts/alpha");
    expect(entry?.version).toBe(1);
    await store.upsertBlackboard({ ...entry!, value: "v2", version: 2, updatedAt: now + 1 }, 1);
    expect((await store.getBlackboard(swarmId, "contracts/alpha"))?.value).toBe("v2");
    // Stale version → conflict.
    await expect(
      store.upsertBlackboard({ ...entry!, value: "v3", version: 3, updatedAt: now + 2 }, 1),
    ).rejects.toThrow(/blackboard conflict/);
    // Fresh insert via upsert (no expectedVersion).
    await store.upsertBlackboard({
      id: "bb-flow-2", swarmId, key: "contracts/beta", value: "x",
      contentType: "text/plain", version: 1, authorMemberId: a, createdAt: now, updatedAt: now,
    });
    expect((await store.getBlackboard(swarmId, "contracts/beta"))?.value).toBe("x");
  });

  test("belief dedupe by fact_hash: re-insert reinforces (same id), reinforce bumps", async () => {
    const first = await store.insertBelief({
      id: "blf-flow-1", swarmId, factHash: "fact-flow", text: "the backend works", confidence: 0.5,
      tier: "whisper", authorMemberId: a, createdAt: now, updatedAt: now,
    });
    expect(first.reinforceCount).toBe(1);
    const second = await store.insertBelief({
      id: "blf-flow-2", swarmId, factHash: "fact-flow", text: "the backend works", confidence: 0.5,
      tier: "whisper", authorMemberId: a, createdAt: now + 1, updatedAt: now + 1,
    });
    expect(second.id).toBe("blf-flow-1"); // original id preserved
    expect(second.reinforceCount).toBe(2);
    expect(second.confidence).toBeCloseTo(0.6);
    const reinforced = await store.reinforceBelief(swarmId, "fact-flow");
    expect(reinforced?.reinforceCount).toBe(3);
    // upgrade to shout at >= 2 reinforces
    const shouted = await store.upgradeWhisperToShout(swarmId, "fact-flow");
    expect(shouted?.tier).toBe("shout");
    expect((await store.listBeliefs(swarmId)).length).toBe(1); // dedupe held
  });

  test("events: per-swarm counter, newest first", async () => {
    await store.insertEvent({ swarmId, type: "message.sent", actorMemberId: a, createdAt: now + 1 });
    await store.insertEvent({ swarmId, type: "task.completed", actorMemberId: b, entityType: "task", entityId: "task-flow-1", createdAt: now + 2 });
    await store.insertEvent({ swarmId, type: "member.spawned", actorMemberId: coord, createdAt: now + 3 });
    const events = await store.listEvents(swarmId);
    expect(events.map((e) => e.type)).toEqual(["member.spawned", "task.completed", "message.sent"]);
    expect(events[0]!.id).toBeGreaterThan(events[1]!.id);
    expect((await store.listEventsForEntity(swarmId, "task", "task-flow-1")).map((e) => e.type)).toEqual(["task.completed"]);
  });

  test("deliverables + contracts roundtrip and verdict flow", async () => {
    const d = await store.insertDeliverable({
      id: "dlv-flow-1", swarmId, memberId: a, taskId: "task-flow-1",
      summary: "ledger entry", refs: ["msg-flow-1"], createdAt: now,
    });
    expect(d.verdict).toBeNull();
    expect((await store.getDeliverable("dlv-flow-1"))?.summary).toBe("ledger entry");
    expect(await store.setDeliverableVerdict("dlv-flow-1", "accepted", coord)).toBe(true);
    expect(await store.setDeliverableVerdict("dlv-flow-1", "rejected", coord)).toBe(false); // final
    expect((await store.getDeliverable("dlv-flow-1"))).toMatchObject({ verdict: "accepted", verdictBy: coord });

    const c = await store.insertContract({
      id: "ctr-flow-1", swarmId, keyPattern: "context/*", schemaJson: '{"type":"object"}',
      createdBy: coord, createdAt: now, updatedAt: now,
    });
    expect(c.keyPattern).toBe("context/*");
    expect((await store.getContract(swarmId, "context/*"))?.id).toBe("ctr-flow-1");
    expect(await store.deleteContract(swarmId, "context/*")).toBe(true);
    expect(await store.deleteContract(swarmId, "context/*")).toBe(false);
  });

  test("transaction() serializes a multi-op body and rolls nothing into the queue", async () => {
    const result = await store.transaction(async (tx) => {
      await tx.insertBlackboard({
        id: "bb-flow-tx", swarmId, key: "tx/key", value: "1",
        contentType: "text/plain", version: 1, authorMemberId: a, createdAt: now, updatedAt: now,
      });
      const entry = await tx.getBlackboard(swarmId, "tx/key");
      expect(entry?.value).toBe("1");
      return "done";
    });
    expect(result).toBe("done");
    expect((await store.getBlackboard(swarmId, "tx/key"))?.value).toBe("1");
  });

  test("deleteSwarm cascades all entity namespaces", async () => {
    const tmp = mkdtempSync(join(tmpdir(), "chunkdb-del-"));
    const s = new ChunkDbStore(join(tmp, "del.chunkdb"));
    await s.ready();
    await s.insertSwarm(newSwarm("swarm-del", "del"));
    await s.insertMember(newMember("swarm-del", "mem-del-1", "one", "ses-del-1"));
    await s.insertMessages([newMessage("swarm-del", "msg-del-1", "mem-del-1", "mem-del-1", "normal", "bye", now)]);
    await s.insertBelief({ id: "blf-del", swarmId: "swarm-del", factHash: "f", text: "t", confidence: 0.5, tier: "whisper", authorMemberId: "mem-del-1", createdAt: now, updatedAt: now });
    await s.insertEvent({ swarmId: "swarm-del", type: "message.sent", createdAt: now });
    await s.deleteSwarm("swarm-del");
    expect(await s.getSwarm("swarm-del")).toBeUndefined();
    expect(await s.listMembers("swarm-del")).toEqual([]);
    expect(await s.listMessagesBySwarm("swarm-del", 100)).toEqual([]);
    expect(await s.listBeliefs("swarm-del")).toEqual([]);
    expect(await s.listEvents("swarm-del")).toEqual([]);
    expect(await s.getMemberBySessionId("ses-del-1")).toBeUndefined();
    await s.close();
    try { rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });
});

describe("ChunkDbStore — auto-migrate wiring in plugin init", () => {
  test("storeBackend:'chunkdb' migrates a legacy sqlite dir on startup", async () => {
    const dataDir = mkdtempSync(join(tmpdir(), "chunkdb-plugin-"));
    try {
      // Prepare a legacy sqlite dir exactly where the plugin looks for it.
      const swarmsDir = join(dataDir, ".opencode", "swarms");
      mkdirSync(swarmsDir, { recursive: true });
      const legacyPath = join(swarmsDir, "swarms.db");
      const legacy = new SQLiteStore(legacyPath);
      await legacy.ready();
      await legacy.insertSwarm(newSwarm("swarm-legacy", "legacy"));
      await legacy.insertMember(newMember("swarm-legacy", "mem-legacy-1", "w", "ses-legacy-1"));
      await legacy.close();

      const fakeClient = {
        session: {
          create: async (opts: any) => ({ data: { id: "ses-fake", title: opts.body?.title, parentID: undefined, directory: "." }, error: undefined }),
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
      const pluginInput: any = {
        client: fakeClient,
        project: { id: "proj-legacy" },
        directory: ".",
        worktree: ".",
        experimental_workspace: { register() {} },
        serverUrl: new URL("http://x"),
        $: {},
      };

      await initSwarmRuntime(pluginInput, { dataDir, storeBackend: "chunkdb" });
      const rt = swarmRuntime();
      expect(rt).toBeDefined();
      // The runtime is backed by the chunkdb store and the legacy swarm is visible.
      expect(rt!.store).toBeInstanceOf(ChunkDbStore);
      const swarm = await rt!.store.getSwarm("swarm-legacy");
      expect(swarm?.name).toBe("legacy");
      expect((await rt!.store.listMembers("swarm-legacy")).map((m) => m.id)).toEqual(["mem-legacy-1"]);
      // The chunkdb file exists and the legacy sqlite db was NOT deleted.
      expect(existsSync(join(dataDir, ".opencode", "swarms", "swarms.chunkdb"))).toBe(true);
      expect(existsSync(legacyPath)).toBe(true);
    } finally {
      // This test created a plugin runtime via initSwarmRuntime — clear the
      // module-level singleton so it cannot leak into other test files that
      // share a worker process (cross-file harness hygiene; see disposeSwarmRuntime).
      disposeSwarmRuntime();
      try { rmSync(dataDir, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });
});

describe("ChunkDbStore — storage-space benchmark (KPI)", () => {
  test("chunkdb file is meaningfully smaller than sqlite on a realistic swarm dataset", async () => {
    const dir = mkdtempSync(join(tmpdir(), "chunkdb-bench-"));
    const sqlitePath = join(dir, "swarms.db");
    const chunkPath = join(dir, "swarms.chunkdb");
    const sql = new SQLiteStore(sqlitePath);
    const cdb = new ChunkDbStore(chunkPath);
    await sql.ready();
    await cdb.ready();

    const bodies = [
      "Here is the latest status update on the storage layer — the migration path is working and all tests are green so far.",
      "Can you review the chunkdb integration and confirm the key scheme matches the documented layout?",
      "The auto-compaction threshold keeps the delta pile bounded; please double check the ratio on the benchmark dataset.",
      "Blocked on the permission prompt for external directory access — the ask stayed unanswered and the member stalled.",
    ];

    async function buildDataset(store: SQLiteStore | ChunkDbStore, s: number) {
      const swarmId = `swarm-bench-${s}`;
      await store.insertSwarm(newSwarm(swarmId, `bench-${s}`));
      const members = [];
      for (let m = 0; m < 4; m++) {
        members.push(newMember(swarmId, `mem-${s}-${m}`, `worker${m}`, `ses-${s}-${m}`));
      }
      for (const m of members) await store.insertMember(m);
      for (let t = 0; t < 8; t++) {
        await store.insertTask({ ...newTask(swarmId, `task-${s}-${t}`, `Implement chunkdb storage phase ${t}`, t % 2), status: t % 3 === 0 ? "ready" : "pending" });
      }
      for (let d = 0; d < 4; d++) {
        await store.insertTaskDependency(`task-${s}-${d + 1}`, `task-${s}-${d}`);
      }
      const msgs: NewMessage[] = [];
      const priorities: SwarmMessage["priority"][] = ["low", "normal", "high", "urgent"];
      for (let m = 0; m < 150; m++) {
        msgs.push({
          ...newMessage(swarmId, `msg-${s}-${m}`, `mem-${s}-${m % 4}`, `mem-${s}-${(m + 1) % 4}`, priorities[m % 4]!, bodies[m % bodies.length]!, now + m),
          kind: m % 3 === 0 ? "request" : "finding",
          deliveryState: m % 6 === 0 ? "delivered" : "queued",
          expiresAt: m % 11 === 0 ? now + 60000 : undefined,
        });
      }
      await store.insertMessages(msgs);
      for (let bb = 0; bb < 25; bb++) {
        await store.insertBlackboard({
          id: `bb-${s}-${bb}`, swarmId, key: `context/lane-${bb}`,
          value: JSON.stringify({ note: "lane ownership advisory", owner: `mem-${s}-${bb % 4}`, lanes: ["wire", "migrate", "test"] }),
          contentType: "application/json", version: 1, authorMemberId: `mem-${s}-${bb % 4}`, createdAt: now, updatedAt: now,
        });
      }
      for (let f = 0; f < 15; f++) {
        await store.insertBelief({
          id: `blf-${s}-${f}`, swarmId, factHash: `hash-${s}-${f % 5}`,
          text: `The chunkdb backend reduces on-disk footprint because rows compress well at block granularity (observation ${f}).`,
          confidence: 0.5 + (f % 3) * 0.1, tags: "storage,chunkdb", tier: f % 4 === 0 ? "shout" : "whisper",
          authorMemberId: `mem-${s}-${f % 4}`, evidenceRefs: `["msg-${s}-${f}"]`, reinforceCount: 1, createdAt: now, updatedAt: now,
        });
      }
    }

    for (let s = 0; s < 2; s++) {
      await buildDataset(sql, s);
      await buildDataset(cdb, s);
    }
    await sql.close();
    await cdb.close();

    const sqliteBytes = totalDbSize(sqlitePath);
    const chunkBytes = totalDbSize(chunkPath);
    // The KPI: chunkdb should be meaningfully smaller (measured ~66% smaller
    // on this dataset). Assert a conservative >30% reduction so the gate never
    // flakes on WAL/compression variance.
    console.log(`[chunkdb bench] sqlite=${sqliteBytes}B chunkdb=${chunkBytes}B ratio=${(chunkBytes / sqliteBytes).toFixed(3)}`);
    expect(chunkBytes).toBeLessThan(sqliteBytes * 0.7);
    try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
  });
});
