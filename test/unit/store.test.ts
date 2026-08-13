import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import type { NewMessage, NewSwarm, NewSwarmMember, NewTask } from "../../src/storage/models.ts";

let dir: string;
let store: SQLiteStore;
let now = Date.now();

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-test-"));
  store = new SQLiteStore(join(dir, "swarms.db"));
  await store.ready();
});

afterAll(async () => {
  await store.close();
  // Best-effort cleanup; Windows may hold WAL handles momentarily.
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
});

function newSwarm(name: string): NewSwarm {
  return {
    id: `swarm-${name}`,
    projectId: "test-project",
    name,
    coordinatorSessionId: `ses-coord-${name}`,
    coordinatorMemberId: `mem-coord-${name}`,
    directory: ".",
    status: "active",
    policies: {
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
    },
    createdAt: now,
    updatedAt: now,
  };
}

function newMember(swarmId: string, name: string, sessionId: string): NewSwarmMember {
  const safe = swarmId.replace(/^swarm-/, "");
  return {
    id: `mem-${safe}-${name}`,
    swarmId,
    name,
    role: "worker",
    sessionId,
    status: "idle",
    workspaceMode: "worktree",
    createdAt: now,
    updatedAt: now,
  };
}

describe("SQLiteStore", () => {
  test("swarm + member persistence roundtrip", async () => {
    await store.insertSwarm(newSwarm("alpha"));
    const swarm = await store.getSwarm("swarm-alpha");
    expect(swarm?.name).toBe("alpha");
    expect(swarm?.status).toBe("active");
  });

  test("duplicate swarm name in same project fails", async () => {
    await store.insertSwarm(newSwarm("dup"));
    await expect(store.insertSwarm(newSwarm("dup"))).rejects.toThrow();
  });

  test("member lookup by name and session", async () => {
    await store.insertMember(newMember("swarm-alpha", "backend", "ses-backend"));
    const byName = await store.getMemberByName("swarm-alpha", "backend");
    expect(byName?.name).toBe("backend");
    const bySession = await store.getMemberBySessionId("ses-backend");
    expect(bySession?.sessionId).toBe("ses-backend");
  });

  test("duplicate member name in same swarm fails", async () => {
    await store.insertMember(newMember("swarm-alpha", "frontend", "ses-f1"));
    // same name, different session/id => must fail on UNIQUE(swarm_id,name)
    const dup: NewSwarmMember = {
      id: "mem-alpha-frontend-2",
      swarmId: "swarm-alpha",
      name: "frontend",
      role: "worker",
      sessionId: "ses-f2",
      status: "idle",
      workspaceMode: "worktree",
      createdAt: now,
      updatedAt: now,
    };
    await expect(store.insertMember(dup)).rejects.toThrow();
  });  test("atomic claim: second claim of ready task fails", async () => {
    await store.insertSwarm(newSwarm("claim"));
    await store.insertMember(newMember("swarm-claim", "a", "ses-a"));
    await store.insertMember(newMember("swarm-claim", "b", "ses-b"));
    const task: NewTask = {
      id: "task-1",
      swarmId: "swarm-claim",
      title: "t",
      status: "ready",
      priority: 0,
      createdByMemberId: "mem-claim-a",
      createdAt: now,
      updatedAt: now,
    };
    await store.insertTask(task);

    expect(await store.claimTask("task-1", "mem-claim-a")).toBe(true);
    expect(await store.claimTask("task-1", "mem-claim-b")).toBe(false);
  });

  test("claim of non-ready task fails", async () => {
    await store.insertSwarm(newSwarm("claim2"));
    await store.insertMember(newMember("swarm-claim2", "a", "ses-2a"));
    await store.insertTask({
      id: "task-2",
      swarmId: "swarm-claim2",
      title: "t",
      status: "blocked",
      priority: 0,
      createdByMemberId: "mem-claim2-a",
      createdAt: now,
      updatedAt: now,
    });
    expect(await store.claimTask("task-2", "mem-claim2-a")).toBe(false);
  });

  test("message mailbox enqueue + pending listing + scheduled marking", async () => {
    await store.insertSwarm(newSwarm("mail"));
    await store.insertMember(newMember("swarm-mail", "a", "ses-msa"));
    await store.insertMember(newMember("swarm-mail", "b", "ses-msb"));
    const msg: NewMessage = {
      id: "msg-1",
      swarmId: "swarm-mail",
      fromMemberId: "mem-mail-a",
      to: { type: "member", memberId: "mem-mail-b" },
      kind: "request",
      priority: "normal",
      body: { text: "hello b" },
      deliveryState: "queued",
      attemptCount: 0,
      createdAt: now,
    };
    await store.insertMessages([msg]);
    const pending = await store.listPendingMessages("mem-mail-b");
    expect(pending.length).toBe(1);
    expect(pending[0]?.body.text).toBe("hello b");

    await store.markMessagesScheduled("mem-mail-b", ["msg-1"]);
    expect((await store.listPendingMessages("mem-mail-b")).length).toBe(0);

    await store.updateMessageDelivery("msg-1", "delivered");
    await store.revertMessageToQueued("msg-1", "mem-mail-b"); // no-op: it's delivered
  });

  test("expireOverdueMessages transitions overdue urgent mail once (F-M2)", async () => {
    await store.insertSwarm(newSwarm("exp"));
    await store.insertMember(newMember("swarm-exp", "a", "ses-expa"));
    await store.insertMember(newMember("swarm-exp", "b", "ses-expb"));
    const past = Date.now() - 1000;
    const future = Date.now() + 60_000;
    await store.insertMessages([
      { id: "exp-1", swarmId: "swarm-exp", fromMemberId: "mem-exp-a", to: { type: "member", memberId: "mem-exp-b" }, kind: "request", priority: "urgent", body: { text: "overdue" }, deliveryState: "queued", attemptCount: 0, createdAt: past - 1000, expiresAt: past },
      { id: "exp-2", swarmId: "swarm-exp", fromMemberId: "mem-exp-a", to: { type: "member", memberId: "mem-exp-b" }, kind: "request", priority: "urgent", body: { text: "still valid" }, deliveryState: "queued", attemptCount: 0, createdAt: Date.now(), expiresAt: future },
      { id: "exp-3", swarmId: "swarm-exp", fromMemberId: "mem-exp-a", to: { type: "member", memberId: "mem-exp-b" }, kind: "message", priority: "normal", body: { text: "no expiry" }, deliveryState: "queued", attemptCount: 0, createdAt: Date.now() },
    ]);
    const expired = await store.expireOverdueMessages(Date.now());
    // Exactly the overdue urgent row is returned + transitioned, once.
    expect(expired.map((m) => m.id).sort()).toEqual(["exp-1"]);
    expect((await store.getMessageById("exp-1"))?.deliveryState).toBe("expired");
    expect((await store.getMessageById("exp-2"))?.deliveryState).toBe("queued");
    expect((await store.getMessageById("exp-3"))?.deliveryState).toBe("queued");
    // Second sweep returns nothing — notice path is exactly-once per message.
    expect((await store.expireOverdueMessages(Date.now())).length).toBe(0);
  });

  test("revertStaleScheduledForSwarm does not resurrect expired messages (S5/F-M2)", async () => {
    await store.insertSwarm(newSwarm("rsx"));
    await store.insertMember(newMember("swarm-rsx", "a", "ses-rsxa"));
    await store.insertMember(newMember("swarm-rsx", "b", "ses-rsxb"));
    const past = Date.now() - 1000;
    await store.insertMessages([
      // Expired while scheduled (claimed, then expiry passed before delivery commit).
      { id: "rsx-1", swarmId: "swarm-rsx", fromMemberId: "mem-rsx-a", to: { type: "member", memberId: "mem-rsx-b" }, kind: "request", priority: "urgent", body: { text: "dead" }, deliveryState: "scheduled", attemptCount: 0, createdAt: past - 5000, expiresAt: past },
      // Stale scheduled without expiry — must come back to queued.
      { id: "rsx-2", swarmId: "swarm-rsx", fromMemberId: "mem-rsx-a", to: { type: "member", memberId: "mem-rsx-b" }, kind: "request", priority: "normal", body: { text: "revivable" }, deliveryState: "scheduled", attemptCount: 0, createdAt: past - 5000 },
    ]);
    await store.revertStaleScheduledForSwarm("swarm-rsx");
    expect((await store.getMessageById("rsx-1"))?.deliveryState).toBe("expired");
    expect((await store.getMessageById("rsx-2"))?.deliveryState).toBe("queued");
  });

  test("getMessagesByIds re-reads persisted delivery states (F-M1)", async () => {
    await store.insertSwarm(newSwarm("gbi"));
    await store.insertMember(newMember("swarm-gbi", "a", "ses-gbia"));
    await store.insertMember(newMember("swarm-gbi", "b", "ses-gbib"));
    await store.insertMessages([
      { id: "gbi-1", swarmId: "swarm-gbi", fromMemberId: "mem-gbi-a", to: { type: "member", memberId: "mem-gbi-b" }, kind: "request", priority: "normal", body: { text: "one" }, deliveryState: "queued", attemptCount: 0, createdAt: now },
      { id: "gbi-2", swarmId: "swarm-gbi", fromMemberId: "mem-gbi-a", to: { type: "member", memberId: "mem-gbi-b" }, kind: "request", priority: "normal", body: { text: "two" }, deliveryState: "queued", attemptCount: 0, createdAt: now },
    ]);
    await store.updateMessageDelivery("gbi-1", "delivered");
    const fresh = await store.getMessagesByIds(["gbi-1", "gbi-2"]);
    expect(fresh.find((m) => m.id === "gbi-1")?.deliveryState).toBe("delivered");
    expect(fresh.find((m) => m.id === "gbi-2")?.deliveryState).toBe("queued");
    expect((await store.getMessagesByIds([])).length).toBe(0);
  });

  test("revertMessageToQueuedWithError records attempt + last_error; markMessageFailed transitions (F-M5)", async () => {
    await store.insertSwarm(newSwarm("retry"));
    await store.insertMember(newMember("swarm-retry", "a", "ses-retrya"));
    await store.insertMember(newMember("swarm-retry", "b", "ses-retryb"));
    await store.insertMessages([
      { id: "retry-1", swarmId: "swarm-retry", fromMemberId: "mem-retry-a", to: { type: "member", memberId: "mem-retry-b" }, kind: "request", priority: "normal", body: { text: "flaky" }, deliveryState: "scheduled", attemptCount: 0, createdAt: now },
    ]);
    const reverted = await store.revertMessageToQueuedWithError("retry-1", "mem-retry-b", "promptAsync failed: boom");
    expect(reverted?.deliveryState).toBe("queued");
    expect(reverted?.attemptCount).toBe(1);
    expect(reverted?.lastError ?? "").toContain("boom");
    // Plain revert (no error bookkeeping) leaves the row untouched in attempts.
    const failed = await store.markMessageFailed("retry-1");
    expect(failed?.deliveryState).toBe("failed");
    expect(failed?.lastError ?? "").toBeTruthy();
  });

  test("M-3: attemptCount does NOT increment on successful delivery (updateMessageDelivery)", async () => {
    await store.insertSwarm(newSwarm("m3"));
    await store.insertMember(newMember("swarm-m3", "a", "ses-m3a"));
    await store.insertMember(newMember("swarm-m3", "b", "ses-m3b"));
    await store.insertMessages([
      { id: "m3-1", swarmId: "swarm-m3", fromMemberId: "mem-m3-a", to: { type: "member", memberId: "mem-m3-b" }, kind: "request", priority: "normal", body: { text: "ok" }, deliveryState: "scheduled", attemptCount: 0, createdAt: now },
    ]);
    await store.updateMessageDelivery("m3-1", "delivered");
    const fresh = await store.getMessageById("m3-1");
    expect(fresh?.deliveryState).toBe("delivered");
    expect(fresh?.attemptCount).toBe(0); // success must NOT inflate the failure counter
  });

  test("M-6: expireOverdueMessages sweeps scheduled-past-expiry too (in-session, not only startup)", async () => {
    await store.insertSwarm(newSwarm("m6"));
    await store.insertMember(newMember("swarm-m6", "a", "ses-m6a"));
    await store.insertMember(newMember("swarm-m6", "b", "ses-m6b"));
    const past = Date.now() - 1000;
    await store.insertMessages([
      // Claimed by a wake (scheduled) then expiry passed before delivery commit.
      { id: "m6-1", swarmId: "swarm-m6", fromMemberId: "mem-m6-a", to: { type: "member", memberId: "mem-m6-b" }, kind: "request", priority: "urgent", body: { text: "expired-scheduled" }, deliveryState: "scheduled", attemptCount: 0, createdAt: past - 5000, expiresAt: past },
      // Scheduled with future expiry — must NOT be expired.
      { id: "m6-2", swarmId: "swarm-m6", fromMemberId: "mem-m6-a", to: { type: "member", memberId: "mem-m6-b" }, kind: "request", priority: "urgent", body: { text: "still valid" }, deliveryState: "scheduled", attemptCount: 0, createdAt: Date.now(), expiresAt: Date.now() + 60_000 },
    ]);
    const expired = await store.expireOverdueMessages(Date.now());
    expect(expired.map((m) => m.id).sort()).toEqual(["m6-1"]);
    expect((await store.getMessageById("m6-1"))?.deliveryState).toBe("expired");
    expect((await store.getMessageById("m6-2"))?.deliveryState).toBe("scheduled");
  });

  test("M-5: revertStaleScheduledForSwarm returns total transitions (queued-revert + expired-scheduled)", async () => {
    await store.insertSwarm(newSwarm("m5"));
    await store.insertMember(newMember("swarm-m5", "a", "ses-m5a"));
    await store.insertMember(newMember("swarm-m5", "b", "ses-m5b"));
    const past = Date.now() - 1000;
    await store.insertMessages([
      { id: "m5-1", swarmId: "swarm-m5", fromMemberId: "mem-m5-a", to: { type: "member", memberId: "mem-m5-b" }, kind: "request", priority: "urgent", body: { text: "expired" }, deliveryState: "scheduled", attemptCount: 0, createdAt: past - 5000, expiresAt: past },
      { id: "m5-2", swarmId: "swarm-m5", fromMemberId: "mem-m5-a", to: { type: "member", memberId: "mem-m5-b" }, kind: "request", priority: "normal", body: { text: "revivable" }, deliveryState: "scheduled", attemptCount: 0, createdAt: past - 5000 },
    ]);
    const changed = await store.revertStaleScheduledForSwarm("swarm-m5");
    expect(changed).toBe(2); // 1 reverted to queued + 1 expired-scheduled
    expect((await store.getMessageById("m5-1"))?.deliveryState).toBe("expired");
    expect((await store.getMessageById("m5-2"))?.deliveryState).toBe("queued");
  });

  test("listMembersWithPendingMail returns members with queued, unexpired mail only (F-M7)", async () => {
    await store.insertSwarm(newSwarm("sweepm"));
    await store.insertMember(newMember("swarm-sweepm", "a", "ses-sweepma"));
    await store.insertMember(newMember("swarm-sweepm", "b", "ses-sweepmb"));
    await store.insertMember(newMember("swarm-sweepm", "c", "ses-sweepmc"));
    const past = Date.now() - 1000;
    await store.insertMessages([
      { id: "sweepm-1", swarmId: "swarm-sweepm", fromMemberId: "mem-sweepm-a", to: { type: "member", memberId: "mem-sweepm-b" }, kind: "request", priority: "normal", body: { text: "pending" }, deliveryState: "queued", attemptCount: 0, createdAt: now },
      { id: "sweepm-2", swarmId: "swarm-sweepm", fromMemberId: "mem-sweepm-a", to: { type: "member", memberId: "mem-sweepm-c" }, kind: "request", priority: "urgent", body: { text: "expired-so-excluded" }, deliveryState: "queued", attemptCount: 0, createdAt: past - 5000, expiresAt: past },
      { id: "sweepm-3", swarmId: "swarm-sweepm", fromMemberId: "mem-sweepm-a", to: { type: "member", memberId: "mem-sweepm-c" }, kind: "message", priority: "normal", body: { text: "delivered-not-counted" }, deliveryState: "delivered", attemptCount: 1, createdAt: now },
    ]);
    const pending = await store.listMembersWithPendingMail();
    const b = pending.find((p) => p.memberId === "mem-sweepm-b");
    expect(b?.count).toBe(1); // only the queued non-expired message
    expect(pending.some((p) => p.memberId === "mem-sweepm-c")).toBe(false); // expired + delivered excluded
  });

  test("transaction rolls back on error", async () => {
    await store.insertSwarm(newSwarm("txn"));
    try {
      await store.transaction(async (tx) => {
        await tx.insertMember(newMember("swarm-txn", "x", "ses-tx1"));
        throw new Error("boom");
      });
    } catch {
      // expected
    }
    const members = await store.listMembers("swarm-txn");
    expect(members.length).toBe(0);
  });

  test("concurrent claims: exactly one owner", async () => {
    await store.insertSwarm(newSwarm("race"));
    await store.insertMember(newMember("swarm-race", "a", "ses-ra"));
    await store.insertMember(newMember("swarm-race", "b", "ses-rb"));
    await store.insertMember(newMember("swarm-race", "c", "ses-rc"));
    await store.insertTask({
      id: "task-race",
      swarmId: "swarm-race",
      title: "t",
      status: "ready",
      priority: 0,
      createdByMemberId: "mem-race-a",
      createdAt: now,
      updatedAt: now,
    });

    const results = await Promise.all([
      store.claimTask("task-race", "mem-race-a"),
      store.claimTask("task-race", "mem-race-b"),
      store.claimTask("task-race", "mem-race-c"),
    ]);
    expect(results.filter(Boolean).length).toBe(1);
  });

  test("blackboard CAS: conflicting expectedVersion throws", async () => {
    await store.insertSwarm(newSwarm("bb"));
    await store.insertMember(newMember("swarm-bb", "a", "ses-bba"));
    await store.insertMember(newMember("swarm-bb", "b", "ses-bbb"));

    await store.insertBlackboard({
      id: "bb-1",
      swarmId: "swarm-bb",
      key: "contracts/foo",
      value: "v1",
      contentType: "text/markdown",
      version: 1,
      authorMemberId: "mem-bb-a",
      createdAt: now,
      updatedAt: now,
    });

    // A conflicting version-1 write must be detected at the application layer
    // (the store only provides the primitive); the core throws BlackboardConflict.
    const entry = await store.getBlackboard("swarm-bb", "contracts/foo");
    expect(entry?.version).toBe(1);
    expect(entry?.value).toBe("v1");
  });

  test("store-level CAS guard: upsert with stale expectedVersion refuses to overwrite", async () => {
    await store.insertSwarm(newSwarm("bb-cas"));
    await store.insertMember(newMember("swarm-bb-cas", "a", "ses-bbca"));
    const base = {
      id: "bb-cas-1",
      swarmId: "swarm-bb-cas",
      key: "contracts/foo",
      contentType: "text/markdown" as const,
      authorMemberId: "mem-bb-cas-a",
      createdAt: now,
      updatedAt: now,
    };
    await store.insertBlackboard({ ...base, value: "v1", version: 1 });

    // Correct expectedVersion applies the update.
    await store.upsertBlackboard({ ...base, value: "v2", version: 2, updatedAt: now + 1 }, 1);
    expect((await store.getBlackboard("swarm-bb-cas", "contracts/foo"))?.value).toBe("v2");

    // Stale expectedVersion must NOT silently overwrite — it throws.
    await expect(
      store.upsertBlackboard({ ...base, value: "v3-stale", version: 3, updatedAt: now + 2 }, 1),
    ).rejects.toThrow(/conflict/);
    expect((await store.getBlackboard("swarm-bb-cas", "contracts/foo"))?.value).toBe("v2");
  });

  test("blackboard + message search is case-insensitive and wildcard-safe", async () => {
    await store.insertSwarm(newSwarm("search"));
    const mem = newMember("swarm-search", "a", "ses-search-a");
    await store.insertMember(mem);
    await store.insertBlackboard({
      id: "bb-s1",
      swarmId: "swarm-search",
      key: "deliverable/nibble",
      value: "Nibble wire v3 ADOPTED",
      contentType: "text/markdown",
      version: 1,
      authorMemberId: mem.id,
      createdAt: now,
      updatedAt: now,
    });

    // Case-insensitive: probing "nibble" finds "Nibble" / "ADOPTED".
    expect((await store.searchBlackboard("swarm-search", "nibble")).length).toBe(1);
    expect((await store.searchBlackboard("swarm-search", "adopted")).length).toBe(1);
    // Wildcard chars are literal, not SQL wildcards: "a_%" must NOT match "adopted".
    expect((await store.searchBlackboard("swarm-search", "a_%")).length).toBe(0);
    // Case-insensitive message search.
    await store.insertMessages([{
      id: "msg-s1",
      swarmId: "swarm-search",
      fromMemberId: mem.id,
      to: { type: "member", memberId: mem.id },
      kind: "finding",
      priority: "normal",
      body: { text: "Working the Nibble packing lane" },
      deliveryState: "queued",
      attemptCount: 0,
      createdAt: now,
    }]);
    expect((await store.searchMessagesBySwarm("swarm-search", "nibble")).length).toBe(1);
    expect((await store.searchMessagesBySwarm("swarm-search", "NIBBLE")).length).toBe(1);
  });

  test("search treats _ and % literally (ESCAPE clause): literal keys are found", async () => {
    await store.insertSwarm(newSwarm("search-lit"));
    const mem = newMember("swarm-search-lit", "a", "ses-sla");
    await store.insertMember(mem);
    await store.insertBlackboard({
      id: "bb-lit1",
      swarmId: "swarm-search-lit",
      key: "deliverable/a_b",
      value: "underscore value",
      contentType: "text/markdown",
      version: 1,
      authorMemberId: mem.id,
      createdAt: now,
      updatedAt: now,
    });
    await store.insertBlackboard({
      id: "bb-lit2",
      swarmId: "swarm-search-lit",
      key: "deliverable/100%",
      value: "percent value",
      contentType: "text/markdown",
      version: 1,
      authorMemberId: mem.id,
      createdAt: now,
      updatedAt: now,
    });

    // Audit S3: before the ESCAPE '\' clause these returned [] (the escaped
    // backslashes were inert), so literal-_/% probes silently missed real keys.
    expect((await store.searchBlackboard("swarm-search-lit", "a_b")).length).toBe(1);
    expect((await store.searchBlackboard("swarm-search-lit", "a_b"))[0]?.key).toBe("deliverable/a_b");
    expect((await store.searchBlackboard("swarm-search-lit", "100%")).length).toBe(1);
    expect((await store.searchBlackboard("swarm-search-lit", "100%"))[0]?.key).toBe("deliverable/100%");

    // Wildcard characters still don't act as SQL wildcards: searching the
    // literal "a_b" must NOT match "adopted" or everything-with-a (the
    // existing negative case, preserved).
    await store.insertBlackboard({
      id: "bb-lit3",
      swarmId: "swarm-search-lit",
      key: "deliverable/adopted",
      value: "v",
      contentType: "text/markdown",
      version: 1,
      authorMemberId: mem.id,
      createdAt: now,
      updatedAt: now,
    });
    const hits = await store.searchBlackboard("swarm-search-lit", "a_b");
    expect(hits.map((e) => e.key).sort()).toEqual(["deliverable/a_b"]);
  });

  test("mailbox priority is ranked urgent > high > normal > low (not TEXT order)", async () => {
    await store.insertSwarm(newSwarm("prio"));
    const mem = newMember("swarm-prio", "a", "ses-pa");
    await store.insertMember(mem);
    await store.insertMessages([
      { id: "m-low", swarmId: "swarm-prio", fromMemberId: mem.id, to: { type: "member", memberId: mem.id }, kind: "message", priority: "low", body: { text: "low" }, deliveryState: "queued", attemptCount: 0, createdAt: now },
      { id: "m-high", swarmId: "swarm-prio", fromMemberId: mem.id, to: { type: "member", memberId: mem.id }, kind: "message", priority: "high", body: { text: "high" }, deliveryState: "queued", attemptCount: 0, createdAt: now },
      { id: "m-normal", swarmId: "swarm-prio", fromMemberId: mem.id, to: { type: "member", memberId: mem.id }, kind: "message", priority: "normal", body: { text: "normal" }, deliveryState: "queued", attemptCount: 0, createdAt: now },
      { id: "m-urgent", swarmId: "swarm-prio", fromMemberId: mem.id, to: { type: "member", memberId: mem.id }, kind: "message", priority: "urgent", body: { text: "urgent" }, deliveryState: "queued", attemptCount: 0, createdAt: now },
    ]);
    // Audit S4: TEXT DESC ordering put 'high' after 'low' (lexicographic).
    const pending = await store.listPendingMessages(mem.id);
    expect(pending.map((m) => m.id)).toEqual(["m-urgent", "m-high", "m-normal", "m-low"]);
  });

  test("migration: missing column is added to a pre-existing table", async () => {
    // Create a raw DB (as an older plugin version would) WITHOUT the
    // `directory` column, then let ready()/migrate() add it.
    const dir2 = mkdtempSync(join(tmpdir(), "swarms-migrate-"));
    try {
      const old = new SQLiteStore(join(dir2, "old.db"));
      await old.ready();
      // Drop the column by rebuilding the table the way a v1 plugin had it.
      old.transaction(async () => {
        (old as any).db.exec(`
          DROP TABLE swarm;
          CREATE TABLE swarm (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            name TEXT NOT NULL,
            coordinator_session_id TEXT NOT NULL,
            coordinator_member_id TEXT NOT NULL,
            status TEXT NOT NULL,
            policies_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            completed_at INTEGER,
            UNIQUE(project_id, name)
          );
        `);
      });
      await old.close();

      // Reopen: migrate() must add `directory`.
      const reopened = new SQLiteStore(join(dir2, "old.db"));
      await reopened.ready();
      await reopened.insertSwarm(newSwarm("migrated"));
      const swarm = await reopened.getSwarm("swarm-migrated");
      expect(swarm?.directory).toBe(".");
      await reopened.close();
    } finally {
      try { rmSync(dir2, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  test("deleteSwarm cascades members, tasks, and messages", async () => {
    await store.insertSwarm(newSwarm("del"));
    await store.insertMember(newMember("swarm-del", "a", "ses-del-a"));
    await store.insertMember(newMember("swarm-del", "b", "ses-del-b"));
    await store.insertTask({
      id: "task-del",
      swarmId: "swarm-del",
      title: "t",
      status: "pending",
      priority: 0,
      createdByMemberId: "mem-del-a",
      createdAt: now,
      updatedAt: now,
    });
    await store.insertMessages([{
      id: "msg-del",
      swarmId: "swarm-del",
      fromMemberId: "mem-del-a",
      to: { type: "member", memberId: "mem-del-b" },
      kind: "message",
      priority: "normal",
      body: { text: "bye" },
      deliveryState: "queued",
      attemptCount: 0,
      createdAt: now,
    }]);

    await store.deleteSwarm("swarm-del");
    expect(await store.getSwarm("swarm-del")).toBeUndefined();
    expect(await store.listMembers("swarm-del")).toEqual([]);
    expect(await store.listTasks("swarm-del")).toEqual([]);
    expect(await store.getMemberBySessionId("ses-del-a")).toBeUndefined();
  });

  describe("PathClaim write path + TTL (build-pathclaims-schema)", () => {
    async function claimSwarm(name: string): Promise<{ swarmId: string; a: string; b: string }> {
      await store.insertSwarm(newSwarm(name));
      await store.insertMember(newMember(`swarm-${name}`, "a", `ses-pc-${name}-a`));
      await store.insertMember(newMember(`swarm-${name}`, "b", `ses-pc-${name}-b`));
      return { swarmId: `swarm-${name}`, a: `mem-${name}-a`, b: `mem-${name}-b` };
    }

    test("insertPathClaim → listPathClaims → releasePathClaim lifecycle", async () => {
      const { swarmId, a } = await claimSwarm("pc1");
      const claim = await store.insertPathClaim({
        id: "claim-1",
        swarmId,
        memberId: a,
        pattern: "src/**",
        mode: "advisory",
        createdAt: now,
      });
      expect(claim.id).toBe("claim-1");

      const active = await store.listPathClaims(swarmId);
      expect(active.length).toBe(1);
      expect(active[0]?.pattern).toBe("src/**");
      expect(active[0]?.memberId).toBe(a);

      // Release: claim leaves the active set.
      expect(await store.releasePathClaim("claim-1")).toBe(true);
      expect((await store.listPathClaims(swarmId)).length).toBe(0);
      // Double release is a no-op (false).
      expect(await store.releasePathClaim("claim-1")).toBe(false);

      // Delete removes the row entirely.
      const claim2 = await store.insertPathClaim({ id: "claim-1b", swarmId, memberId: a, pattern: "docs/**", mode: "advisory", createdAt: now });
      await store.deletePathClaim(claim2.id);
      expect((await store.listPathClaims(swarmId)).length).toBe(0);
    });

    test("UNIQUE(swarm_id, member_id, pattern) for ACTIVE claims: duplicate claim throws, re-claim after release works", async () => {
      const { swarmId, a } = await claimSwarm("pc2");
      await store.insertPathClaim({ id: "c2-1", swarmId, memberId: a, pattern: "src/**", mode: "advisory", createdAt: now });
      // Same member, same pattern, still active → UNIQUE violation.
      await expect(
        store.insertPathClaim({ id: "c2-2", swarmId, memberId: a, pattern: "src/**", mode: "advisory", createdAt: now }),
      ).rejects.toThrow();
      // Another member may claim the same pattern (per-member uniqueness).
      await store.insertPathClaim({ id: "c2-3", swarmId, memberId: "mem-pc2-b", pattern: "src/**", mode: "advisory", createdAt: now });
      expect((await store.listPathClaims(swarmId)).length).toBe(2);

      // Release then re-claim the same pattern: allowed (released row is out of
      // the uniqueness scope).
      await store.releasePathClaim("c2-1");
      const re = await store.insertPathClaim({ id: "c2-4", swarmId, memberId: a, pattern: "src/**", mode: "advisory", createdAt: now });
      expect(re.id).toBe("c2-4");
    });

    test("TTL: stale claims (expires_at passed) are not counted as active", async () => {
      const { swarmId, a } = await claimSwarm("pc3");
      const past = Date.now() - 1000;
      await store.insertPathClaim({ id: "c3-stale", swarmId, memberId: a, pattern: "stale/**", mode: "advisory", createdAt: now, expiresAt: past });
      const future = Date.now() + 60_000;
      await store.insertPathClaim({ id: "c3-fresh", swarmId, memberId: "mem-pc3-b", pattern: "fresh/**", mode: "advisory", createdAt: now, expiresAt: future });
      const active = await store.listPathClaims(swarmId);
      expect(active.map((c) => c.pattern)).toEqual(["fresh/**"]);
      // Explicit now further in the future: fresh claim also goes stale.
      const later = await store.listPathClaims(swarmId, Date.now() + 120_000);
      expect(later.length).toBe(0);
    });
  });

  test("migration: user_version advances and a legacy DB catches up (directory + expires_at)", async () => {
    const dir3 = mkdtempSync(join(tmpdir(), "swarms-uv-"));
    try {
      // Simulate the ORIGINAL v1 schema: swarm WITHOUT directory, member without
      // human_chat_at, path_claim WITHOUT expires_at.
      const old = new SQLiteStore(join(dir3, "old.db"));
      await old.ready();
      old.transaction(async () => {
        (old as any).db.exec(`
          DROP TABLE swarm;
          CREATE TABLE swarm (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            name TEXT NOT NULL,
            coordinator_session_id TEXT NOT NULL,
            coordinator_member_id TEXT NOT NULL,
            status TEXT NOT NULL,
            policies_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            completed_at INTEGER,
            UNIQUE(project_id, name)
          );
        `);
        (old as any).db.exec(`
          DROP TABLE swarm_path_claim;
          CREATE TABLE swarm_path_claim (
            id TEXT PRIMARY KEY,
            swarm_id TEXT NOT NULL,
            member_id TEXT NOT NULL,
            pattern TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'advisory',
            created_at INTEGER NOT NULL,
            released_at INTEGER
          );
        `);
        // Force user_version back to 0 so the chain re-runs from scratch.
        (old as any).db.exec(`PRAGMA user_version = 0;`);
      });
      await old.close();

      const reopened = new SQLiteStore(join(dir3, "old.db"));
      await reopened.ready();
      // user_version reached the latest migration (12: swarm_member.session_id
      // UNIQUE dropped — multi-own; 11: deliverables+contracts;
      // 10: pending permissions; 9: task reserved_for; 8 = message.noreply;
      // 7 = resonant_at, Hive H2; 6 = beliefs, 5 = annotations, 4 = lease
      // columns, 3 = expires_at).
      const uv = await (reopened as any).db.query(`PRAGMA user_version`).get();
      expect((uv as { user_version: number } | undefined)?.user_version).toBe(12);
      // Columns present + usable.
      await reopened.insertSwarm(newSwarm("uv"));
      const swarm = await reopened.getSwarm("swarm-uv");
      expect(swarm?.directory).toBe(".");
      const { a } = await (async () => {
        await reopened.insertMember(newMember("swarm-uv", "a", "ses-uv-a"));
        return { a: "mem-uv-a" };
      })();
      await reopened.insertPathClaim({ id: "uv-c", swarmId: "swarm-uv", memberId: a, pattern: "src/**", mode: "advisory", createdAt: now, expiresAt: Date.now() + 1000 });
      expect((await reopened.listPathClaims("swarm-uv")).length).toBe(1);
      // Annotation table exists and is usable on the migrated DB.
      await reopened.insertAnnotation({ id: "uv-ann", swarmId: "swarm-uv", path: "src/x.ts", type: "gold", weight: 9, authorMemberId: a, createdAt: now });
      expect((await reopened.listAnnotations("swarm-uv")).length).toBe(1);
      // Belief table exists and is usable on the migrated DB.
      await reopened.insertBelief({ id: "uv-bel", swarmId: "swarm-uv", factHash: "h1", text: "fact", confidence: 0.5, tier: "whisper", authorMemberId: a, createdAt: now, updatedAt: now });
      expect((await reopened.listBeliefs("swarm-uv")).length).toBe(1);
      // v12 (multi-own): the migrated DB accepts a SECOND member with the same
      // session (session_id UNIQUE is gone) — the whole point of the rebuild.
      await reopened.insertMember({ id: "mem-uv-c2", swarmId: "swarm-uv", name: "coordinator2", role: "coordinator", sessionId: "ses-uv-a", status: "idle", workspaceMode: "shared-read", createdAt: now, updatedAt: now });
      const sameSession = await reopened.listMembersBySessionId("ses-uv-a");
      expect(sameSession.length).toBe(2);
      // The swarm-scoped lookup resolves within the (session, swarm) pair —
      // both rows share the session, and the second insert is reachable.
      expect((await reopened.getMemberByName("swarm-uv", "coordinator2"))?.sessionId).toBe("ses-uv-a");
      await reopened.close();
    } finally {
      try { rmSync(dir3, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  test("updateMemberStatus rejects binding a task owned by ANOTHER member (ownership guard)", async () => {
    await store.insertSwarm(newSwarm("own"));
    await store.insertMember(newMember("swarm-own", "a", "ses-own-a"));
    await store.insertMember(newMember("swarm-own", "b", "ses-own-b"));
    const task: NewTask = {
      id: "task-own",
      swarmId: "swarm-own",
      title: "t",
      status: "ready",
      priority: 0,
      createdByMemberId: "mem-own-a",
      createdAt: now,
      updatedAt: now,
    };
    await store.insertTask(task);
    // a claims the task (owner = a).
    expect(await store.claimTask("task-own", "mem-own-a")).toBe(true);

    // b must NOT be bound to a task owned by a.
    await expect(
      store.updateMemberStatus("mem-own-b", "working", { currentTaskId: "task-own" }),
    ).rejects.toThrow(/does not own task/);

    // a can bind (owns it).
    await store.updateMemberStatus("mem-own-a", "working", { currentTaskId: "task-own" });
    expect((await store.getMemberById("mem-own-a"))?.currentTaskId).toBe("task-own");

    // NULL clear and status-only (undefined) writes are unaffected.
    await store.updateMemberStatus("mem-own-b", "idle", { currentTaskId: null });
    await store.updateMemberStatus("mem-own-b", "idle");
  });

  describe("Hive H0 artifact annotations (build-annotations-serialization)", () => {
    async function annSwarm(name: string): Promise<{ swarmId: string; author: string }> {
      await store.insertSwarm(newSwarm(name));
      await store.insertMember(newMember(`swarm-${name}`, "a", `ses-ann-${name}-a`));
      return { swarmId: `swarm-${name}`, author: `mem-${name}-a` };
    }

    test("insertAnnotation → listAnnotations → releaseOrDeleteAnnotation lifecycle", async () => {
      const { swarmId, author } = await annSwarm("ann1");
      const a = await store.insertAnnotation({
        id: "ann-1",
        swarmId,
        path: "src/nibble.ts",
        type: "claim",
        weight: 8,
        note: "packing this lane",
        authorMemberId: author,
        createdAt: now,
      });
      expect(a.type).toBe("claim");
      expect(a.path).toBe("src/nibble.ts");

      const listed = await store.listAnnotations(swarmId);
      expect(listed.length).toBe(1);
      expect(listed[0]?.note).toBe("packing this lane");

      // path filter
      const miss = await store.listAnnotations(swarmId, { path: "src/other.ts" });
      expect(miss.length).toBe(0);

      // delete
      expect(await store.releaseOrDeleteAnnotation("ann-1")).toBe(true);
      expect((await store.listAnnotations(swarmId)).length).toBe(0);
      // delete of a missing row is false
      expect(await store.releaseOrDeleteAnnotation("ann-1")).toBe(false);
    });

    test("UNIQUE(swarm_id, path, type): fresh annotation replaces the previous one", async () => {
      const { swarmId, author } = await annSwarm("ann2");
      await store.insertAnnotation({ id: "ann2-1", swarmId, path: "src/a.ts", type: "gold", weight: 9, solutionHash: "h1", authorMemberId: author, createdAt: now });
      // Same path + type replaces; different type coexists.
      await store.insertAnnotation({ id: "ann2-2", swarmId, path: "src/a.ts", type: "gold", weight: 10, solutionHash: "h2", authorMemberId: author, createdAt: now });
      await store.insertAnnotation({ id: "ann2-3", swarmId, path: "src/a.ts", type: "struggle", weight: 4, note: "still stuck", authorMemberId: author, createdAt: now });
      const listed = await store.listAnnotations(swarmId);
      expect(listed.length).toBe(2);
      const gold = listed.find((x) => x.type === "gold");
      expect(gold?.solutionHash).toBe("h2"); // latest wins
    });

    test("TTL: stale annotations (expires_at passed) are excluded when activeOnly", async () => {
      const { swarmId, author } = await annSwarm("ann3");
      const past = now - 1000;
      await store.insertAnnotation({ id: "ann3-1", swarmId, path: "src/stale.ts", type: "corpse", weight: 2, errorSig: "E1", authorMemberId: author, createdAt: now, expiresAt: past });
      await store.insertAnnotation({ id: "ann3-2", swarmId, path: "src/fresh.ts", type: "gold", weight: 9, authorMemberId: author, createdAt: now, ttl: 60_000 });

      const active = await store.listAnnotations(swarmId);
      expect(active.map((x) => x.path)).toEqual(["src/fresh.ts"]);

      // activeOnly:false includes stale rows
      const all = await store.listAnnotations(swarmId, { activeOnly: false });
      expect(all.length).toBe(2);

      // Explicit future `now` makes the fresh one stale too.
      const later = await store.listAnnotations(swarmId, { now: now + 120_000 });
      expect(later.length).toBe(0);
    });

    test("insertAnnotation derives expiresAt from ttl", async () => {
      const { swarmId, author } = await annSwarm("ann4");
      const a = await store.insertAnnotation({ id: "ann4-1", swarmId, path: "src/ttl.ts", type: "note", weight: 1, authorMemberId: author, createdAt: now, ttl: 30_000 });
      expect(a.expiresAt).toBe(now + 30_000);
    });

    test("errorSig/solutionHash roundtrip through insertAnnotation → listAnnotations (P3 carry-over)", async () => {
      const { swarmId, author } = await annSwarm("ann5");
      // corpse with an error signature
      await store.insertAnnotation({
        id: "ann5-corpse", swarmId, path: "src/dead.ts", type: "corpse", weight: 3,
        errorSig: "TS2345: not assignable", authorMemberId: author, createdAt: now,
      });
      // gold with a solution hash
      await store.insertAnnotation({
        id: "ann5-gold", swarmId, path: "src/solved.ts", type: "gold", weight: 9,
        solutionHash: "sha256:abc123", authorMemberId: author, createdAt: now,
      });

      const listed = await store.listAnnotations(swarmId);
      expect(listed.length).toBe(2);
      const corpse = listed.find((x) => x.type === "corpse");
      expect(corpse?.errorSig).toBe("TS2345: not assignable");
      const gold = listed.find((x) => x.type === "gold");
      expect(gold?.solutionHash).toBe("sha256:abc123");

      // Both fields are queryable via listAnnotations data (probe haystack
      // completeness — Core adds solutionHash to the probe match next).
      const corpseBySig = listed.filter((a) => `${a.path} ${a.type} ${a.note ?? ""} ${a.errorSig ?? ""} ${a.solutionHash ?? ""}`.toLowerCase().includes("ts2345"));
      expect(corpseBySig.length).toBe(1);
      const goldByHash = listed.filter((a) => `${a.path} ${a.type} ${a.note ?? ""} ${a.errorSig ?? ""} ${a.solutionHash ?? ""}`.toLowerCase().includes("sha256:abc123"));
      expect(goldByHash.length).toBe(1);
    });

    test("S1: replace returns the REAL stored row id (no phantom id); releaseOrDelete by it works", async () => {
      const { swarmId, author } = await annSwarm("ann-s1");
      const first = await store.insertAnnotation({ id: "ann-s1-a", swarmId, path: "src/x.ts", type: "gold", weight: 8, authorMemberId: author, createdAt: now });
      const second = await store.insertAnnotation({ id: "ann-s1-b", swarmId, path: "src/x.ts", type: "gold", weight: 9, authorMemberId: author, createdAt: now });
      // The returned id must be the STORED row's id (the original), not the
      // caller's new id — releaseOrDeleteAnnotation(second.id) must delete it.
      const listed = await store.listAnnotations(swarmId, { activeOnly: false });
      expect(listed.length).toBe(1);
      expect(listed[0]?.id).toBe(second.id); // returned id == listed id
      expect(second.id).not.toBe("ann-s1-b"); // phantom id NOT returned
      expect(await store.releaseOrDeleteAnnotation(second.id)).toBe(true);
      expect((await store.listAnnotations(swarmId, { activeOnly: false })).length).toBe(0);
    });

    test("S9: ttl=0 means no expiry (annotation not instantly stale)", async () => {
      const { swarmId, author } = await annSwarm("ann-s9");
      const a = await store.insertAnnotation({ id: "ann-s9-1", swarmId, path: "src/z.ts", type: "note", weight: 1, authorMemberId: author, createdAt: now, ttl: 0 });
      expect(a.expiresAt).toBeUndefined(); // no expiry
      expect((await store.listAnnotations(swarmId, { path: "src/z.ts" })).length).toBe(1); // visible
    });
  });

  describe("Hive H1 beliefs/facts substrate (build-beliefs-schema)", () => {
    async function belSwarm(name: string): Promise<{ swarmId: string; author: string; author2: string }> {
      await store.insertSwarm(newSwarm(name));
      await store.insertMember(newMember(`swarm-${name}`, "a", `ses-bel-${name}-a`));
      await store.insertMember(newMember(`swarm-${name}`, "b", `ses-bel-${name}-b`));
      return { swarmId: `swarm-${name}`, author: `mem-${name}-a`, author2: `mem-${name}-b` };
    }

    test("insertBelief → listBeliefs lifecycle with tier/confidence filters", async () => {
      const { swarmId, author } = await belSwarm("bel1");
      const b = await store.insertBelief({
        id: "bel-1", swarmId, factHash: "f1", text: "nibble wire v3 adopted",
        confidence: 0.6, tags: "nibble,wire", tier: "whisper", evidenceRefs: JSON.stringify(["msg-1"]),
        authorMemberId: author, createdAt: now, updatedAt: now,
      });
      expect(b.tier).toBe("whisper");
      expect(b.reinforceCount).toBe(1);

      const listed = await store.listBeliefs(swarmId);
      expect(listed.length).toBe(1);
      expect(listed[0]?.text).toBe("nibble wire v3 adopted");
      expect(listed[0]?.evidenceRefs).toContain("msg-1");

      // tier + minConfidence filters
      expect((await store.listBeliefs(swarmId, { tier: "shout" })).length).toBe(0);
      expect((await store.listBeliefs(swarmId, { minConfidence: 0.9 })).length).toBe(0);
      // query (case-insensitive, ESCAPE-safe)
      expect((await store.listBeliefs(swarmId, { query: "NIBBLE" })).length).toBe(1);
      expect((await store.listBeliefs(swarmId, { query: "adopted wire" })).length).toBe(0);
    });

    test("fact_hash dedupe: re-insert reinforces instead of duplicating", async () => {
      const { swarmId, author, author2 } = await belSwarm("bel2");
      await store.insertBelief({ id: "bel2-1", swarmId, factHash: "f1", text: "fact x", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });
      const again = await store.insertBelief({ id: "bel2-2", swarmId, factHash: "f1", text: "fact x", confidence: 0.5, tier: "whisper", authorMemberId: author2, createdAt: now + 1, updatedAt: now + 1 });
      const listed = await store.listBeliefs(swarmId);
      expect(listed.length).toBe(1); // deduped
      expect(listed[0]?.reinforceCount).toBe(2); // reinforced
      expect(again?.reinforceCount).toBe(2);
      // reinforce_count + confidence bumped toward 1
      expect(listed[0]?.confidence).toBeGreaterThan(0.5);
    });

    test("reinforceBelief increments count/confidence; missing fact → undefined", async () => {
      const { swarmId, author } = await belSwarm("bel3");
      await store.insertBelief({ id: "bel3-1", swarmId, factHash: "f1", text: "fact", confidence: 0.4, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });
      const r = await store.reinforceBelief(swarmId, "f1", 0.2);
      expect(r?.reinforceCount).toBe(2);
      expect(r?.confidence).toBeCloseTo(0.6, 5);
      expect(await store.reinforceBelief(swarmId, "missing")).toBeUndefined();
      // confidence clamps at 1.0
      await store.reinforceBelief(swarmId, "f1", 0.9);
      const c = await store.listBeliefs(swarmId);
      expect(c[0]?.confidence).toBe(1);
    });

    test("upgradeWhisperToShout requires reinforce_count >= 2", async () => {
      const { swarmId, author } = await belSwarm("bel4");
      await store.insertBelief({ id: "bel4-1", swarmId, factHash: "f1", text: "fact", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });
      // reinforce_count = 1 → not eligible
      expect(await store.upgradeWhisperToShout(swarmId, "f1")).toBeUndefined();
      // reinforce → 2 → eligible
      await store.reinforceBelief(swarmId, "f1");
      const shout = await store.upgradeWhisperToShout(swarmId, "f1");
      expect(shout?.tier).toBe("shout");
      expect((await store.listBeliefs(swarmId, { tier: "shout" })).length).toBe(1);
      // already shout → undefined (no-op)
      expect(await store.upgradeWhisperToShout(swarmId, "f1")).toBeUndefined();
    });

    test("TTL + status exclusion in listBeliefs(activeOnly) and expireBeliefs sweep", async () => {
      const { swarmId, author } = await belSwarm("bel5");
      const past = now - 1000;
      await store.insertBelief({ id: "bel5-stale", swarmId, factHash: "f-stale", text: "stale", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, expiresAt: past, updatedAt: now });
      await store.insertBelief({ id: "bel5-fresh", swarmId, factHash: "f-fresh", text: "fresh", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, ttl: 60_000, updatedAt: now });

      const active = await store.listBeliefs(swarmId);
      expect(active.map((x) => x.factHash)).toEqual(["f-fresh"]);

      // activeOnly:false includes stale (still status 'active' until sweep)
      const all = await store.listBeliefs(swarmId, { activeOnly: false });
      expect(all.length).toBe(2);

      // expireBeliefs marks the past-expiry one 'expired'
      expect(await store.expireBeliefs(Date.now())).toBe(1);
      expect((await store.listBeliefs(swarmId)).length).toBe(1);
      const after = await store.listBeliefs(swarmId, { activeOnly: false });
      const stale = after.find((x) => x.factHash === "f-stale");
      expect(stale?.status).toBe("expired");
    });

    test("S5: re-inserting a soft-pruned belief REVIVES it (status active + refreshed expiry)", async () => {
      const { swarmId, author } = await belSwarm("bel-s5");
      await store.insertBelief({ id: "bel-s5-1", swarmId, factHash: "f1", text: "fact", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, ttl: 60_000, updatedAt: now });
      await store.softPruneBelief(swarmId, "f1", "superseded");
      expect((await store.listBeliefs(swarmId, { status: "superseded" })).length).toBe(1);

      // Re-publish: status must reset to active and the belief become visible
      // again (not silently reinforce a dead row).
      const revived = await store.insertBelief({ id: "bel-s5-2", swarmId, factHash: "f1", text: "fact", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now + 1, ttl: 60_000, updatedAt: now + 1 });
      expect(revived?.status).toBe("active");
      expect(revived?.reinforceCount).toBe(2);
      expect((await store.listBeliefs(swarmId, { query: "fact" })).length).toBe(1); // visible again
    });

    test("S6: explicit status filter overrides the activeOnly default", async () => {
      const { swarmId, author } = await belSwarm("bel-s6");
      await store.insertBelief({ id: "bel-s6-1", swarmId, factHash: "f1", text: "fact", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, expiresAt: now - 1000, updatedAt: now });
      await store.expireBeliefs(Date.now());
      // Without the fix this returned 0 (activeOnly silently contradicted).
      const expired = await store.listBeliefs(swarmId, { status: "expired" });
      expect(expired.length).toBe(1);
      expect(expired[0]?.status).toBe("expired");
    });

    test("S7: insertBelief clamps confidence to 0..1", async () => {
      const { swarmId, author } = await belSwarm("bel-s7");
      await store.insertBelief({ id: "bel-s7-1", swarmId, factHash: "f-neg", text: "neg", confidence: -0.7, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });
      await store.insertBelief({ id: "bel-s7-2", swarmId, factHash: "f-over", text: "over", confidence: 1.7, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });
      const beliefs = await store.listBeliefs(swarmId, { activeOnly: false });
      const neg = beliefs.find((b) => b.factHash === "f-neg");
      const over = beliefs.find((b) => b.factHash === "f-over");
      expect(neg?.confidence).toBe(0);
      expect(over?.confidence).toBe(1);
    });

    test("S9: ttl=0 means no expiry (belief not instantly stale)", async () => {
      const { swarmId, author } = await belSwarm("bel-s9");
      const b = await store.insertBelief({ id: "bel-s9-1", swarmId, factHash: "f1", text: "fact", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, ttl: 0, updatedAt: now });
      expect(b.expiresAt).toBeUndefined();
      expect((await store.listBeliefs(swarmId, { query: "fact" })).length).toBe(1);
    });
  });

  describe("Hive H2 resonance/consolidation/digest (build-resonance-consolidation-schema)", () => {
    async function h2Swarm(name: string): Promise<{ swarmId: string; author: string; author2: string }> {
      await store.insertSwarm(newSwarm(name));
      await store.insertMember(newMember(`swarm-${name}`, "a", `ses-h2-${name}-a`));
      await store.insertMember(newMember(`swarm-${name}`, "b", `ses-h2-${name}-b`));
      return { swarmId: `swarm-${name}`, author: `mem-${name}-a`, author2: `mem-${name}-b` };
    }

    test("markResonant transitions active→resonant; idempotent; listBeliefs({status}) queries it", async () => {
      const { swarmId, author } = await h2Swarm("h2r");
      await store.insertBelief({ id: "h2r-1", swarmId, factHash: "f1", text: "fact", confidence: 0.8, tier: "shout", authorMemberId: author, createdAt: now, updatedAt: now });
      const r = await store.markResonant(swarmId, "f1");
      expect(r?.status).toBe("resonant");
      expect(r?.resonantAt).toBeGreaterThan(0);
      // idempotent: already-resonant returns the row, no error
      const again = await store.markResonant(swarmId, "f1");
      expect(again?.status).toBe("resonant");
      // query by status
      expect((await store.listBeliefs(swarmId, { status: "resonant" })).length).toBe(1);
      expect((await store.listBeliefs(swarmId, { status: "active" })).length).toBe(0);
      // missing → undefined
      expect(await store.markResonant(swarmId, "nope")).toBeUndefined();
    });

    test("beliefEvidenceDisjoint: disjoint evidence sets → true; shared refs → false; empty → false", () => {
      const mk = (refs: string[] | undefined) => ({ evidenceRefs: refs ? JSON.stringify(refs) : undefined } as never);
      expect(store.beliefEvidenceDisjoint(mk(["a", "b"]), mk(["c", "d"]))).toBe(true);
      expect(store.beliefEvidenceDisjoint(mk(["a", "b"]), mk(["b", "c"]))).toBe(false);
      expect(store.beliefEvidenceDisjoint(mk([]), mk(["c"]))).toBe(false); // empty evidence = not provable
      expect(store.beliefEvidenceDisjoint(mk(undefined), mk(undefined))).toBe(false);
    });

    test("listBeliefsForPruning returns low-confidence/low-reuse/old candidates only", async () => {
      const { swarmId, author } = await h2Swarm("h2p");
      const old = now - 100_000;
      await store.insertBelief({ id: "h2p-weak-old", swarmId, factHash: "weak", text: "weak old", confidence: 0.2, tier: "whisper", authorMemberId: author, createdAt: old, updatedAt: old });
      await store.insertBelief({ id: "h2p-strong-new", swarmId, factHash: "strong", text: "strong new", confidence: 0.9, tier: "shout", authorMemberId: author, createdAt: now, updatedAt: now });
      await store.insertBelief({ id: "h2p-weak-new", swarmId, factHash: "weaknew", text: "weak new", confidence: 0.2, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });

      const candidates = await store.listBeliefsForPruning(swarmId, { maxConfidence: 0.3, minReinforce: 2, olderThanMs: 60_000 });
      expect(candidates.map((b) => b.factHash)).toEqual(["weak"]); // only weak+old
    });

    test("softPruneBelief transitions status; hardPruneBeliefs deletes rows", async () => {
      const { swarmId, author } = await h2Swarm("h2pr");
      await store.insertBelief({ id: "h2pr-1", swarmId, factHash: "a", text: "a", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });
      await store.insertBelief({ id: "h2pr-2", swarmId, factHash: "b", text: "b", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });

      const superseded = await store.softPruneBelief(swarmId, "a", "superseded");
      expect(superseded?.status).toBe("superseded");
      expect((await store.listBeliefs(swarmId, { activeOnly: false })).length).toBe(2); // row kept
      expect(await store.softPruneBelief(swarmId, "missing", "expired")).toBeUndefined();

      expect(await store.hardPruneBeliefs(swarmId, ["a", "b", "missing"])).toBe(2);
      expect((await store.listBeliefs(swarmId, { activeOnly: false })).length).toBe(0);
    });

    test("beliefDigest is stable + changes when beliefs change; listBeliefsChangedSince returns deltas", async () => {
      const { swarmId, author } = await h2Swarm("h2d");
      await store.insertBelief({ id: "h2d-1", swarmId, factHash: "a", text: "a", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });
      const d1 = await store.beliefDigest(swarmId);
      expect(d1.count).toBe(1);
      expect(d1.digest).toMatch(/^[0-9a-f]{40}$/); // sha1 hex

      // identical state → identical digest
      const d1b = await store.beliefDigest(swarmId);
      expect(d1b.digest).toBe(d1.digest);

      // change (reinforce bumps updated_at) → digest changes
      await store.reinforceBelief(swarmId, "a");
      const d2 = await store.beliefDigest(swarmId);
      expect(d2.digest).not.toBe(d1.digest);

      // changed-since: the reinforced belief (updated_at > d1 snapshot time)
      const changed = await store.listBeliefsChangedSince(swarmId, now);
      expect(changed.map((b) => b.factHash)).toContain("a");
    });

    test("S3: same-ms reinforce still changes the digest (monotonic fields in tuple)", async () => {
      const { swarmId, author } = await h2Swarm("h2s3");
      // Insert and reinforce with the SAME updated_at (same-ms window): the old
      // id+updated_at digest would not change even though reinforce_count did.
      await store.insertBelief({ id: "h2s3-1", swarmId, factHash: "f1", text: "fact", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now });
      const d1 = await store.beliefDigest(swarmId);
      await store.insertBelief({ id: "h2s3-2", swarmId, factHash: "f1", text: "fact", confidence: 0.5, tier: "whisper", authorMemberId: author, createdAt: now, updatedAt: now }); // same updated_at
      const d2 = await store.beliefDigest(swarmId);
      // reinforce_count 1→2 is in the tuple → digest MUST differ.
      expect(d2.digest).not.toBe(d1.digest);
      expect((await store.listBeliefs(swarmId, { activeOnly: false }))[0]?.reinforceCount).toBe(2);
    });
  });

  test("S2: deleteMember succeeds for a member who authored blackboard/annotation/belief rows", async () => {
    await store.insertSwarm(newSwarm("s2"));
    await store.insertMember(newMember("swarm-s2", "a", "ses-s2-a"));
    const mem = "mem-s2-a";
    // Member authors one row in each FK-bearing table (no ON DELETE action).
    await store.insertBlackboard({ id: "bb-s2", swarmId: "swarm-s2", key: "k", value: "v", contentType: "text/markdown", version: 1, authorMemberId: mem, createdAt: now, updatedAt: now });
    await store.insertAnnotation({ id: "an-s2", swarmId: "swarm-s2", path: "src/x.ts", type: "gold", weight: 5, authorMemberId: mem, createdAt: now });
    await store.insertBelief({ id: "be-s2", swarmId: "swarm-s2", factHash: "f1", text: "t", confidence: 0.5, tier: "whisper", authorMemberId: mem, createdAt: now, updatedAt: now });

    // Before the fix this threw FOREIGN KEY constraint failed (swarm_remove
    // blocked forever for authoring members).
    await store.deleteMember(mem);
    expect(await store.getMemberById(mem)).toBeUndefined();
    // Authored rows are cascaded away, not orphaned.
    expect((await store.searchBlackboard("swarm-s2", "k")).length).toBe(0);
    expect((await store.listAnnotations("swarm-s2", { activeOnly: false })).length).toBe(0);
    expect((await store.listBeliefs("swarm-s2", { activeOnly: false })).length).toBe(0);
  });

  test("S4: updateMessageDelivery cannot resurrect an expired/failed message", async () => {
    await store.insertSwarm(newSwarm("s4"));
    await store.insertMember(newMember("swarm-s4", "a", "ses-s4-a"));
    await store.insertMessages([{
      id: "msg-s4", swarmId: "swarm-s4", fromMemberId: "mem-s4-a", to: { type: "member", memberId: "mem-s4-a" },
      kind: "message", priority: "urgent", body: { text: "x" }, deliveryState: "queued", attemptCount: 0, createdAt: now, expiresAt: now - 1000,
    }]);
    await store.expireMessage("msg-s4");
    expect((await store.getMessageById("msg-s4"))?.deliveryState).toBe("expired");
    // A stale broker call must NOT resurrect expired → delivered.
    await store.updateMessageDelivery("msg-s4", "delivered");
    expect((await store.getMessageById("msg-s4"))?.deliveryState).toBe("expired");
    // Normal queued → delivered still works.
    await store.insertMessages([{
      id: "msg-s4b", swarmId: "swarm-s4", fromMemberId: "mem-s4-a", to: { type: "member", memberId: "mem-s4-a" },
      kind: "message", priority: "normal", body: { text: "y" }, deliveryState: "queued", attemptCount: 0, createdAt: now,
    }]);
    await store.updateMessageDelivery("msg-s4b", "delivered");
    expect((await store.getMessageById("msg-s4b"))?.deliveryState).toBe("delivered");
  });

  test("S8: a raw store write during an open transaction does NOT join/rollback with it", async () => {
    // audit S8: before the fix, raw methods executed directly on the shared
    // connection, so a write issued while transaction() held BEGIN IMMEDIATE
    // would join the open txn and be rolled back (or committed) with it.
    await store.insertSwarm(newSwarm("s8"));
    await store.insertMember(newMember("swarm-s8", "a", "ses-s8-a"));

    // Open a transaction that inserts a member (will be rolled back), and
    // deliberately yield so a concurrent raw write can slip in.
    let releaseTxn: () => void = () => {};
    const gate = new Promise<void>((res) => { releaseTxn = res; });

    const txnP = (async () => {
      try {
        await store.transaction(async (tx) => {
          await tx.insertMember(newMember("swarm-s8", "txn-member", "ses-s8-txn"));
          await gate; // hold the txn open while a raw write is attempted
          throw new Error("rollback me");
        });
      } catch {
        /* expected rollback */
      }
    })();

    // Give the txn a chance to reach the gate, then issue a RAW write on the
    // same store WITHOUT awaiting it: with the S8 fix it serializes behind the
    // txn (runs after rollback, so it survives); without the fix it joins the
    // open txn and is rolled back with it. Releasing the gate lets the txn
    // finish (rollback), after which the queued raw write commits.
    await new Promise((r) => setTimeout(r, 20));
    const rawWriteP = store.insertTask({
      id: "task-s8",
      swarmId: "swarm-s8",
      title: "survivor",
      status: "ready",
      priority: 0,
      createdByMemberId: "mem-s8-a",
      createdAt: now,
      updatedAt: now,
    });
    releaseTxn();
    await txnP;
    await rawWriteP;

    // The raw write must have survived the txn's rollback.
    const tasks = await store.listTasks("swarm-s8");
    expect(tasks.some((t) => t.id === "task-s8")).toBe(true);
    // The txn's own writes must NOT have survived.
    expect(await store.getMemberBySessionId("ses-s8-txn")).toBeUndefined();
  });
});
