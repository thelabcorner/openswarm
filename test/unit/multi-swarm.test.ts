import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import { SQLiteStore } from "../../src/storage/sqlite-store.ts";
import type { NewSwarm, NewSwarmMember } from "../../src/storage/models.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * Multi-swarm ownership per session (task t-multiown-tests): ONE session may
 * own MULTIPLE swarms. Covers createSwarm x2 / swarm_delegate x2 + reuse-by-
 * name, per-swarm memberForContext (spawn/message/status on A and B from the
 * same session), the swarm-scoped coordinator lookup
 * (getMemberBySessionAndSwarm returns a DIFFERENT row per swarm), the v11→v12
 * migration that drops UNIQUE(session_id) on swarm_member, swarm_revive on B
 * resolving B's coordinator, and chunkdb-store parity. Both backends.
 */

let dirs: string[] = [];

function makeClient() {
  return {
    config: {
      providers: async () => ({
        data: {
          providers: [
            { id: "opencode-go", models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash (2x usage)" } } },
            { id: "opencode", models: { "deepseek-v4-flash-free": { name: "DeepSeek V4 Flash Free" } } },
          ],
        },
        error: undefined,
      }),
    },
    session: {
      create: async (o: any) => ({
        data: { id: `ses-${Math.random().toString(36).slice(2, 8)}`, title: o?.body?.title, parentID: undefined, directory: "." },
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
}

const pluginInput = (client: unknown): any => ({
  client,
  project: { id: "proj-multiown" },
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

let hooks: Hooks | undefined;
let tool: Record<string, any>;

async function initPlugin(options: Record<string, unknown> = {}): Promise<void> {
  disposeSwarmRuntime();
  const dir = mkdtempSync(join(tmpdir(), "swarms-multiown-"));
  dirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient()), { dataDir: dir, ...options });
  tool = hooks.tool ?? {};
}

function rt() {
  const r = swarmRuntime();
  if (!r) throw new Error("swarm runtime not initialized");
  return r;
}

/** The live SwarmStore behind the current runtime (both backends implement it). */
function store() {
  return rt().core.store;
}

async function createSwarm(name: string, sessionID = "ses-lead"): Promise<string> {
  const created = await tool.swarm_create.execute({ name }, ctx(sessionID));
  const json = JSON.parse(String(created.output ?? created));
  return json.swarm.id;
}

async function delegate(
  name: string,
  members: Array<{ name: string; role: string }> = [],
): Promise<{ swarmId: string; members: Array<{ memberId: string; sessionId: string }> }> {
  const res = await tool.swarm_delegate.execute({ name, members }, ctx("ses-lead"));
  return JSON.parse(String(res.output ?? res));
}

async function spawn(swarmId: string, name: string): Promise<{ memberId: string; sessionId: string }> {
  const res = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name, role: "worker" }] },
    ctx("ses-lead"),
  );
  const json = JSON.parse(String(res.output ?? res));
  return json.spawned[0];
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe("multi-swarm ownership per session", () => {
  // Every flow test runs against BOTH backends (g is called out explicitly
  // below, but the loop is the exhaustive both-backend coverage).
  for (const backend of ["sqlite", "chunkdb"] as const) {
    describe(`backend=${backend}`, () => {
      test("a: createSwarm twice from the SAME session with DIFFERENT names -> two swarms, session is coordinator of both", async () => {
        await initPlugin({ storeBackend: backend });
        const idA = await createSwarm("own-a");
        const idB = await createSwarm("own-b");
        expect(idA).not.toBe(idB);

        const members = await store().listMembersBySessionId("ses-lead");
        expect(members).toHaveLength(2);
        expect(members.every((m) => m.role === "coordinator")).toBe(true);
        expect(new Set(members.map((m) => m.swarmId))).toEqual(new Set([idA, idB]));

        const swarms = await store().listSwarmsBySession("ses-lead");
        expect(swarms.map((s) => s.id).sort()).toEqual([idA, idB].sort());
        // The authoritative owned-swarm check: both rows name the session as coordinator.
        expect(swarms.every((s) => s.coordinatorSessionId === "ses-lead")).toBe(true);
      });

      test("b: swarm_delegate twice (no swarmId) -> two swarms with members; reuse-by-name reuses, no duplicate", async () => {
        await initPlugin({ storeBackend: backend });
        const d1 = await delegate("del-a", [{ name: "wa", role: "worker" }]);
        const d2 = await delegate("del-b", [{ name: "wb", role: "worker" }]);
        expect(d1.swarmId).toBeDefined();
        expect(d2.swarmId).toBeDefined();
        expect(d1.swarmId).not.toBe(d2.swarmId);
        expect(d1.members).toHaveLength(1);
        expect(d2.members).toHaveLength(1);

        // Reuse-by-name: delegate with swarm A's name again -> SAME swarm, no duplicate.
        const d3 = await delegate("del-a", [{ name: "wa", role: "worker" }]);
        expect(d3.swarmId).toBe(d1.swarmId);
        const membersA = await store().listMembers(d1.swarmId);
        expect(membersA.filter((m) => m.name === "wa")).toHaveLength(1);
        // Still exactly two swarms for the session.
        expect(await store().listSwarmsBySession("ses-lead")).toHaveLength(2);
      });

      test("c: per-swarm memberForContext — spawn/message/status on A and B from the SAME session both work", async () => {
        await initPlugin({ storeBackend: backend });
        const idA = await createSwarm("ctx-a");
        const idB = await createSwarm("ctx-b");

        // Spawn a member in EACH swarm (memberForContext resolves per-swarm).
        const sA = await spawn(idA, "worker-a");
        const sB = await spawn(idB, "worker-b");
        expect(sA.memberId).toBeDefined();
        expect(sB.memberId).toBeDefined();

        // Message members in each swarm.
        const mA = await tool.swarm_message.execute(
          { swarmId: idA, to: "worker-a", message: "hello a", noreply: true },
          ctx("ses-lead"),
        );
        expect(String(mA.output ?? mA)).not.toContain("Error");
        const mB = await tool.swarm_message.execute(
          { swarmId: idB, to: "worker-b", message: "hello b", noreply: true },
          ctx("ses-lead"),
        );
        expect(String(mB.output ?? mB)).not.toContain("Error");

        // swarm_status on each: members AND messages stay per-swarm isolated.
        const stA = String((await tool.swarm_status.execute({ swarmId: idA, detail: "messages" }, ctx("ses-lead"))).output ?? "");
        expect(stA).toContain("hello a");
        expect(stA).not.toContain("hello b");
        const stB = String((await tool.swarm_status.execute({ swarmId: idB, detail: "messages" }, ctx("ses-lead"))).output ?? "");
        expect(stB).toContain("hello b");
        expect(stB).not.toContain("hello a");
        const memA = String((await tool.swarm_status.execute({ swarmId: idA, detail: "members" }, ctx("ses-lead"))).output ?? "");
        expect(memA).toContain("worker-a");
        const memB = String((await tool.swarm_status.execute({ swarmId: idB, detail: "members" }, ctx("ses-lead"))).output ?? "");
        expect(memB).toContain("worker-b");
      });

      test("d: getMemberBySessionAndSwarm returns a DIFFERENT coordinator row per swarm", async () => {
        await initPlugin({ storeBackend: backend });
        const idA = await createSwarm("coord-a");
        const idB = await createSwarm("coord-b");

        const ca = await store().getMemberBySessionAndSwarm("ses-lead", idA);
        const cb = await store().getMemberBySessionAndSwarm("ses-lead", idB);
        expect(ca).toBeDefined();
        expect(cb).toBeDefined();
        expect(ca!.swarmId).toBe(idA);
        expect(cb!.swarmId).toBe(idB);
        expect(ca!.role).toBe("coordinator");
        expect(cb!.role).toBe("coordinator");
        // DIFFERENT rows — this is the multi-own break from getMemberBySessionId's first-match.
        expect(ca!.id).not.toBe(cb!.id);
        // The session-scoped lookup still resolves (first-match) — but per-swarm is authoritative.
        expect(ca!.sessionId).toBe("ses-lead");
        expect(cb!.sessionId).toBe("ses-lead");
      });

      test("f1: swarm_revive health on B from a session owning A AND B shows B's members only", async () => {
        await initPlugin({ storeBackend: backend });
        const idA = await createSwarm("rev-a");
        const idB = await createSwarm("rev-b");
        await spawn(idA, "worker-a");
        await spawn(idB, "worker-b");

        const res = await tool.swarm_revive.execute({ swarmId: idB, action: "health" }, ctx("ses-lead"));
        const out = String(res.output ?? res);
        expect(out).toContain("rev-b");
        expect(out).toContain("worker-b");
        // B's health must not leak A's members.
        expect(out).not.toContain("worker-a");
      });

      test("f2: swarm_revive resolves the per-swarm coordinator — a session that is worker-in-A and coordinator-of-B can revive B but not A", async () => {
        await initPlugin({ storeBackend: backend });
        const idA = await createSwarm("gate-a");
        // The spawned worker's session becomes a second swarm's coordinator:
        // same session, two swarms, different roles in each.
        const { sessionId: workerSes } = await spawn(idA, "worker-a");
        const idB = await createSwarm("gate-b", workerSes);

        // Sanity: the worker session is a member of both swarms now.
        const swarms = await store().listSwarmsBySession(workerSes);
        expect(swarms.map((s) => s.id).sort()).toEqual([idA, idB].sort());

        // The SAME session is a WORKER in A -> mutating revive on A is denied...
        const denied = await tool.swarm_revive.execute(
          { swarmId: idA, action: "revive", strategy: "keep" },
          ctx(workerSes),
        );
        expect(String(denied.output ?? denied)).toContain("only the coordinator");

        // ...and the COORDINATOR of B -> mutating revive on B must succeed
        // (resolved via getMemberBySessionAndSwarm, not first-match).
        const allowed = await tool.swarm_revive.execute(
          { swarmId: idB, action: "retask", strategy: "repoint", tasks: [{ id: "b1", title: "new mission" }] },
          ctx(workerSes),
        );
        const out = String(allowed.output ?? allowed);
        expect(out).toContain("RE-TASKED");
        expect(out).toContain("strategy=repoint");
      });
    });
  }
});

describe("migration v12 (sqlite)", () => {
  test("e: a v11 DB (UNIQUE session_id) catches up; the constraint is GONE — two members with the same session in different swarms insert", async () => {
    const dir2 = mkdtempSync(join(tmpdir(), "swarms-multiown-migrate-"));
    try {
      // Simulate a v11 database: swarm_member carries `session_id UNIQUE`.
      const old = new SQLiteStore(join(dir2, "old.db"));
      await old.ready();
      old.transaction(async () => {
        (old as any).db.exec(`DROP TABLE swarm_member;`);
        (old as any).db.exec(`
          CREATE TABLE swarm_member (
            id TEXT PRIMARY KEY,
            swarm_id TEXT NOT NULL,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            session_id TEXT NOT NULL UNIQUE,
            agent TEXT,
            provider_id TEXT,
            model_id TEXT,
            status TEXT NOT NULL,
            workspace_mode TEXT NOT NULL,
            workspace_path TEXT,
            branch TEXT,
            current_task_id TEXT,
            human_chat_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            last_active_at INTEGER,
            FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
            UNIQUE(swarm_id, name)
          );
        `);
        (old as any).db.exec(`PRAGMA user_version = 11;`);
      });
      await old.close();

      // Reopen: migrate() must advance to v12 and drop the UNIQUE(session_id).
      const reopened = new SQLiteStore(join(dir2, "old.db"));
      await reopened.ready();
      const uv = await (reopened as any).db.query(`PRAGMA user_version`).get();
      expect((uv as { user_version: number }).user_version).toBe(12);

      await reopened.insertSwarm(newSwarm("m1"));
      await reopened.insertSwarm(newSwarm("m2"));
      // SAME session_id across TWO swarms — impossible under v11, required by multi-own.
      await reopened.insertMember(newMember("swarm-m1", "coordinator", "ses-shared"));
      await reopened.insertMember(newMember("swarm-m2", "coordinator", "ses-shared"));
      expect((await reopened.listMembersBySessionId("ses-shared")).length).toBe(2);
      expect((await reopened.listSwarmsBySession("ses-shared")).length).toBe(2);

      // UNIQUE(swarm_id, name) is still enforced.
      await expect(
        reopened.insertMember(newMember("swarm-m1", "coordinator", "ses-other")),
      ).rejects.toThrow();
      await reopened.close();
    } finally {
      try { rmSync(dir2, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });
});

describe("chunkdb parity (g)", () => {
  test("chunkdb backend: one session creates two swarms; listMembersBySessionId returns 2 and per-swarm tools work", async () => {
    await initPlugin({ storeBackend: "chunkdb" });
    const idA = await createSwarm("par-a");
    const idB = await createSwarm("par-b");
    expect(idA).not.toBe(idB);

    expect((await store().listMembersBySessionId("ses-lead"))).toHaveLength(2);
    expect((await store().listSwarmsBySession("ses-lead")).map((s) => s.id).sort()).toEqual([idA, idB].sort());

    // Same-session per-swarm spawn + message on the chunkdb backend.
    await spawn(idA, "worker-a");
    await spawn(idB, "worker-b");
    const mA = await tool.swarm_message.execute(
      { swarmId: idA, to: "worker-a", message: "chunk hello", noreply: true },
      ctx("ses-lead"),
    );
    expect(String(mA.output ?? mA)).not.toContain("Error");
    const stA = String((await tool.swarm_status.execute({ swarmId: idA, detail: "messages" }, ctx("ses-lead"))).output ?? "");
    expect(stA).toContain("chunk hello");
    // Per-swarm coordinator rows differ.
    const ca = await store().getMemberBySessionAndSwarm("ses-lead", idA);
    const cb = await store().getMemberBySessionAndSwarm("ses-lead", idB);
    expect(ca!.id).not.toBe(cb!.id);
  });
});

// ---- store.test.ts-style helpers (migration test) ----
const now = Date.now();

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
