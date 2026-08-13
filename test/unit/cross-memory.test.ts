import { describe, test, expect, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * t-cross-memory — cross-swarm blackboard access + guest memory (plugin-tool
 * harness, mirroring guest-messaging.test.ts).
 *
 * Contracts under test:
 *   (a) a member of swarm A reads swarm B's blackboard — get/search/list
 *   (b) a NEVER-registered session reads B's blackboard — reads leave no
 *       trace (no member row created)
 *   (c) foreign authors render as 'name@swarm'
 *   (d) a member of A puts to B -> guest row auto-created in B (role guest,
 *       name guest-*) + the put lands with CAS
 *   (e) the same session puts to B again -> no duplicate guest row
 *   (f) allowExternalGuests=false -> put rejected with the policy message,
 *       reads still allowed
 *   (g) a REMOVED member of B puts to B -> orphan error, no guest
 *       re-registration (wasRemovedFrom seam)
 *   (h) a guest of B reads + writes B natively (no friction)
 *   (i) a guest of B reads A cross-swarm
 *   (j) CAS conflict still rejected on cross-swarm put
 *   (k) contract-validated keys still validate on guest writes
 */

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
  project: { id: "proj-cmem" },
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
  const dir = mkdtempSync(join(tmpdir(), "swarms-cmem-"));
  pluginDirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient()), { dataDir: dir });
  tool = hooks.tool ?? {};
}

async function createSwarmTool(name: string, sessionID: string, policies?: Record<string, unknown>): Promise<{ id: string; name: string }> {
  const res = await tool.swarm_create.execute({ name, ...(policies ? { policies } : {}) }, ctx(sessionID));
  const json = JSON.parse(String(res.output ?? res));
  return json.swarm;
}

async function put(swarmId: string, key: string, value: string, sessionID: string, expectedVersion?: number): Promise<string> {
  const res = await tool.swarm_memory.execute(
    { swarmId, action: "put", key, value, ...(expectedVersion !== undefined ? { expectedVersion } : {}) },
    ctx(sessionID),
  );
  return String(res.output ?? res);
}

async function get(swarmId: string, key: string, sessionID: string): Promise<string> {
  const res = await tool.swarm_memory.execute({ swarmId, action: "get", key }, ctx(sessionID));
  return String(res.output ?? res);
}

describe("cross-memory: reads are open, writes auto-register guests", () => {
  test("(a) a member of swarm A reads swarm B's blackboard — get/search/list", async () => {
    await initPlugin();
    const A = await createSwarmTool(`cm-a-${Date.now()}`, "ses-cm-a-co");
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co");

    // B's coordinator writes a couple of keys to B.
    await put(B.id, "keys/alpha", "alpha-value", "ses-cm-b-co");
    await put(B.id, "keys/beta", "beta-value", "ses-cm-b-co");

    // A's coordinator reads B's blackboard with zero ceremony.
    const got = JSON.parse(await get(B.id, "keys/alpha", "ses-cm-a-co"));
    expect(got.key).toBe("keys/alpha");
    expect(got.value).toBe("alpha-value");
    expect(got.version).toBe(1);
    expect(got.author).toBe("coordinator"); // in-swarm author renders plain

    const searchRes = await tool.swarm_memory.execute({ swarmId: B.id, action: "search", query: "alpha" }, ctx("ses-cm-a-co"));
    const search = JSON.parse(String(searchRes.output ?? searchRes));
    expect(search.length).toBe(1);
    expect(search[0].key).toBe("keys/alpha");

    const listRes = await tool.swarm_memory.execute({ swarmId: B.id, action: "list" }, ctx("ses-cm-a-co"));
    const list = JSON.parse(String(listRes.output ?? listRes));
    expect(list.map((e: any) => e.key)).toEqual(["keys/alpha", "keys/beta"]);
  });

  test("(b) a never-registered session reads B's blackboard — reads leave no trace", async () => {
    await initPlugin();
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co2");
    await put(B.id, "keys/alpha", "alpha-value", "ses-cm-b-co2");

    const stranger = "ses-cm-stranger-1";
    const got = JSON.parse(await get(B.id, "keys/alpha", stranger));
    expect(got.value).toBe("alpha-value");
    expect(got.version).toBe(1);

    const listRes = await tool.swarm_memory.execute({ swarmId: B.id, action: "list" }, ctx(stranger));
    const list = String(listRes.output ?? listRes);
    expect(list).toContain("keys/alpha");
    const searchRes = await tool.swarm_memory.execute({ swarmId: B.id, action: "search", query: "alpha" }, ctx(stranger));
    const search = String(searchRes.output ?? searchRes);
    expect(search).toContain("keys/alpha");

    // No member row was created by any of those reads.
    const rt = swarmRuntime();
    expect(rt).toBeDefined();
    expect(await rt!.core.store.getMemberBySessionAndSwarm(stranger, B.id)).toBeUndefined();
    expect(await rt!.core.store.listMembersBySessionId(stranger)).toEqual([]);
  });

  test("(c) foreign authors render as 'name@swarm'", async () => {
    await initPlugin();
    const A = await createSwarmTool(`cm-a-${Date.now()}`, "ses-cm-a-co3");
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co3");
    const rt = swarmRuntime();
    expect(rt).toBeDefined();

    // A foreign-author entry: written via the core path with A's coordinator
    // member id as the author (the author is NOT a member of B — exactly the
    // case enrichForeignSenderNames is built for; a plugin-path write always
    // stores an in-roster id, so this direct write is the honest seam test).
    const aCoord = (await rt!.core.store.listMembers(A.id)).find((m) => m.role === "coordinator")!;
    await rt!.core.blackboardPut({
      swarmId: B.id,
      key: "keys/foreign",
      value: "written by A",
      contentType: "text/markdown",
      authorMemberId: aCoord.id,
    });

    // Reading B renders the foreign author as name@homeSwarm.
    const got = JSON.parse(await get(B.id, "keys/foreign", "ses-cm-a-co3"));
    expect(got.author).toBe(`coordinator@${A.name}`);

    const listRes = await tool.swarm_memory.execute({ swarmId: B.id, action: "list" }, ctx("ses-cm-a-co3"));
    const list = JSON.parse(String(listRes.output ?? listRes));
    expect(list.find((e: any) => e.key === "keys/foreign").author).toBe(`coordinator@${A.name}`);

    const searchRes = await tool.swarm_memory.execute({ swarmId: B.id, action: "search", query: "foreign" }, ctx("ses-cm-a-co3"));
    const search = JSON.parse(String(searchRes.output ?? searchRes));
    expect(search[0].author).toBe(`coordinator@${A.name}`);
  });

  test("(d) a member of A puts to B -> guest row auto-created in B + put lands with CAS", async () => {
    await initPlugin();
    const A = await createSwarmTool(`cm-a-${Date.now()}`, "ses-cm-a-co4");
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co4");

    const out = await put(B.id, "x/key", "v1", "ses-cm-a-co4");
    expect(JSON.parse(out).version).toBe(1);

    const rt = swarmRuntime();
    expect(rt).toBeDefined();
    const guest = await rt!.core.store.getMemberBySessionAndSwarm("ses-cm-a-co4", B.id);
    expect(guest).toBeDefined();
    expect(guest!.role).toBe("guest");
    expect(guest!.name).toMatch(/^guest-/);
    expect(guest!.status).toBe("idle");

    // The caller's OWN row in A is untouched (multi-own: rows are per-swarm).
    const aCoord = await rt!.core.store.getMemberBySessionAndSwarm("ses-cm-a-co4", A.id);
    expect(aCoord).toBeDefined();
    expect(aCoord!.role).toBe("coordinator");
    expect(aCoord!.id).not.toBe(guest!.id);

    // The entry carries the guest author id (an in-roster id of B).
    const entry = await rt!.core.store.getBlackboard(B.id, "x/key");
    expect(entry!.authorMemberId).toBe(guest!.id);

    const got = JSON.parse(await get(B.id, "x/key", "ses-cm-b-co4"));
    expect(got.value).toBe("v1");
    expect(got.author).toBe(guest!.name); // in-roster author renders plain
  });

  test("(e) the same session puts to B again -> no duplicate guest row", async () => {
    await initPlugin();
    const A = await createSwarmTool(`cm-a-${Date.now()}`, "ses-cm-a-co5");
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co5");

    await put(B.id, "x/key", "v1", "ses-cm-a-co5");
    await put(B.id, "x/key", "v2", "ses-cm-a-co5", 1);

    const rt = swarmRuntime();
    expect(rt).toBeDefined();
    const rows = await rt!.core.store.listMembersBySessionId("ses-cm-a-co5");
    // One coordinator row in A + exactly ONE guest row in B.
    const inB = rows.filter((m) => m.swarmId === B.id);
    expect(inB.length).toBe(1);
    expect(inB[0]!.role).toBe("guest");
    const entry = await rt!.core.store.getBlackboard(B.id, "x/key");
    expect(entry!.version).toBe(2);
  });

  test("(f) allowExternalGuests=false -> put rejected with the policy message, reads still allowed", async () => {
    await initPlugin();
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co6", { allowExternalGuests: false });
    await put(B.id, "keys/alpha", "alpha-value", "ses-cm-b-co6");

    const stranger = "ses-cm-stranger-6";
    const err = await tool.swarm_memory.execute({ swarmId: B.id, action: "put", key: "x/key", value: "v1" }, ctx(stranger)).then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeDefined();
    expect((err as Error).message).toContain("this swarm does not accept writes from non-member sessions (allowExternalGuests=false)");

    const rt = swarmRuntime();
    expect(rt).toBeDefined();
    expect(await rt!.core.store.getMemberBySessionAndSwarm(stranger, B.id)).toBeUndefined();

    // Reads are STILL open to any session even when the swarm refuses writes.
    const got = JSON.parse(await get(B.id, "keys/alpha", stranger));
    expect(got.value).toBe("alpha-value");
  });

  test("(g) a REMOVED member of B puts to B -> orphan error, no guest re-registration (wasRemovedFrom seam)", async () => {
    await initPlugin();
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co7");
    const victim = "ses-cm-victim-7";

    // B's coordinator authors a survival-check key, and the victim writes
    // their OWN key (auto-registering as a guest of B).
    await put(B.id, "coord/key", "coord-value", "ses-cm-b-co7");
    await put(B.id, "victim/key", "v1", victim);

    const rt = swarmRuntime();
    expect(rt).toBeDefined();
    const guest = await rt!.core.store.getMemberBySessionAndSwarm(victim, B.id);
    expect(guest).toBeDefined();
    expect(guest!.role).toBe("guest");

    // ...then B's coordinator REMOVES them. The removal records a durable
    // member.removed event carrying the sessionId (t-remove-grace).
    const removed = await tool.swarm_remove.execute({ swarmId: B.id, member: guest!.name }, ctx("ses-cm-b-co7"));
    expect(String(removed.output ?? removed)).toContain("removed");
    expect(await rt!.core.store.getMemberBySessionAndSwarm(victim, B.id)).toBeUndefined();

    // A subsequent put must NOT silently resurrect the victim as a guest —
    // it gets the orphan error.
    const err = await tool.swarm_memory.execute({ swarmId: B.id, action: "put", key: "victim/key", value: "v2" }, ctx(victim)).then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeDefined();
    expect((err as Error).message).toContain("may have been removed");
    expect((err as Error).message).toContain("can write to the blackboard");

    // No re-registration happened (the rejected put left no row behind).
    expect(await rt!.core.store.getMemberBySessionAndSwarm(victim, B.id)).toBeUndefined();
    // Edge S2 (pre-existing, store.test.ts): deleteMember CASCADES the removed
    // member's own authored blackboard rows so removal always succeeds — the
    // victim's own entry is gone, while the coordinator-authored entry (and the
    // swarm itself) survives untouched. The lane contracts under test are the
    // orphan error + no guest resurrection, NOT row preservation.
    expect(await rt!.core.store.getBlackboard(B.id, "victim/key")).toBeUndefined();
    const coordEntry = await rt!.core.store.getBlackboard(B.id, "coord/key");
    expect(coordEntry?.value).toBe("coord-value");
  });

  test("(h) a guest of B reads + writes B natively (no friction)", async () => {
    await initPlugin();
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co8");
    const guestSession = "ses-cm-guest-8";

    // First write auto-registers the guest.
    const out1 = await put(B.id, "g/key", "g1", guestSession);
    expect(JSON.parse(out1).version).toBe(1);

    // Reads in their own swarm just work.
    const got = JSON.parse(await get(B.id, "g/key", guestSession));
    expect(got.value).toBe("g1");
    const listRes = await tool.swarm_memory.execute({ swarmId: B.id, action: "list" }, ctx(guestSession));
    const list = String(listRes.output ?? listRes);
    expect(list).toContain("g/key");
    const searchRes = await tool.swarm_memory.execute({ swarmId: B.id, action: "search", query: "g1" }, ctx(guestSession));
    const search = String(searchRes.output ?? searchRes);
    expect(search).toContain("g/key");

    // CAS write continues to work (version advances).
    const out2 = await put(B.id, "g/key", "g2", guestSession, 1);
    expect(JSON.parse(out2).version).toBe(2);
  });

  test("(i) a guest of B reads A cross-swarm", async () => {
    await initPlugin();
    const A = await createSwarmTool(`cm-a-${Date.now()}`, "ses-cm-a-co9");
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co9");
    await put(A.id, "a/key", "a-value", "ses-cm-a-co9");

    // Register a guest of B via a write.
    const guestSession = "ses-cm-guest-9";
    await put(B.id, "g/key", "g1", guestSession);

    // The guest reads A's blackboard cross-swarm — open reads.
    const got = JSON.parse(await get(A.id, "a/key", guestSession));
    expect(got.value).toBe("a-value");
    expect(got.author).toBe("coordinator"); // in-roster author of A renders plain
  });

  test("(j) CAS conflict still rejected on cross-swarm put", async () => {
    await initPlugin();
    const A = await createSwarmTool(`cm-a-${Date.now()}`, "ses-cm-a-co10");
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co10");

    await put(B.id, "x/key", "v1", "ses-cm-a-co10");

    // Missing expectedVersion on an existing key -> conflict notice.
    const conflict = await tool.swarm_memory.execute({ swarmId: B.id, action: "put", key: "x/key", value: "v2-silent" }, ctx("ses-cm-a-co10"));
    const out = String(conflict.output ?? conflict);
    expect(out).toContain("BLACKBOARD CONFLICT");
    expect(out).toContain("current: 1");

    // Wrong expectedVersion -> conflict notice too.
    const wrong = await tool.swarm_memory.execute({ swarmId: B.id, action: "put", key: "x/key", value: "v2-wrong", expectedVersion: 5 }, ctx("ses-cm-a-co10"));
    expect(String(wrong.output ?? wrong)).toContain("BLACKBOARD CONFLICT");

    // Correct CAS roundtrip lands (guest writer, version advances).
    const ok = await put(B.id, "x/key", "v2-cas", "ses-cm-a-co10", 1);
    expect(JSON.parse(ok).version).toBe(2);
  });

  test("(k) contract-validated keys still validate on guest writes", async () => {
    await initPlugin();
    const B = await createSwarmTool(`cm-b-${Date.now()}`, "ses-cm-b-co11");

    // B's coordinator defines a typed contract on an exact key.
    const defined = await tool.swarm_contract.execute(
      { swarmId: B.id, action: "define", key: "contracts/foo", schema: JSON.stringify({ type: "object", properties: { v: { type: "number" } }, required: ["v"] }) },
      ctx("ses-cm-b-co11"),
    );
    expect(String(defined.output ?? defined)).toContain("contract");

    const guestSession = "ses-cm-guest-11";
    // Non-JSON value -> rejected.
    const notJson = await tool.swarm_memory.execute({ swarmId: B.id, action: "put", key: "contracts/foo", value: "not-json" }, ctx(guestSession)).then(
      () => null,
      (e: unknown) => e,
    );
    expect(notJson).toBeDefined();
    expect((notJson as Error).message).toContain("requires a JSON value");

    // Schema-violating JSON -> rejected.
    const wrongType = await tool.swarm_memory.execute({ swarmId: B.id, action: "put", key: "contracts/foo", value: '{"v":"x"}' }, ctx(guestSession)).then(
      () => null,
      (e: unknown) => e,
    );
    expect(wrongType).toBeDefined();
    expect((wrongType as Error).message).toContain("contract violation");

    // Conforming JSON -> lands, and the writer is a registered guest of B.
    const ok = await put(B.id, "contracts/foo", '{"v":42}', guestSession);
    expect(JSON.parse(ok).version).toBe(1);

    const rt = swarmRuntime();
    expect(rt).toBeDefined();
    const guest = await rt!.core.store.getMemberBySessionAndSwarm(guestSession, B.id);
    expect(guest).toBeDefined();
    expect(guest!.role).toBe("guest");
    const entry = await rt!.core.store.getBlackboard(B.id, "contracts/foo");
    expect(entry!.value).toBe('{"v":42}');
  });
});

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of pluginDirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});
