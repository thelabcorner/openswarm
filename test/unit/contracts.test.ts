import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import { validateValueAgainstSchema, summarizeSchema } from "../../src/storage/json-schema.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { Permission } from "@opencode-ai/sdk";

/**
 * Typed blackboard contracts (t-contracts):
 *   - JSON-schema validation on blackboardPut for contracted keys
 *   - swarm_contract tool (define/list/delete)
 *   - blackboard.write changelog events on every successful contracted write
 * Driven through the plugin's registered tool `execute` handlers with a
 * synthetic client + context (same harness as tools.test.ts).
 */
let dir: string;
let hooks: Hooks;
let tool: Record<string, any>;

const fakeClient = {
  session: {
    create: async (opts: any) => {
      const sessionID = `ses-ct-${Math.random().toString(36).slice(2, 8)}`;
      if (opts.body?.parentID !== undefined) {
        throw new Error(`session.create received a parentID (${opts.body.parentID}); members must be root sessions`);
      }
      return {
        data: { id: sessionID, title: opts.body?.title, parentID: undefined, directory: "." },
        error: undefined,
      };
    },
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
  project: { id: "proj-ct" },
  directory: ".",
  worktree: ".",
  experimental_workspace: { register() {} },
  serverUrl: new URL("http://x"),
  $: {},
};

function permission(input: Pick<Permission, "id" | "type" | "pattern" | "sessionID" | "title">): Permission {
  return {
    ...input,
    messageID: `msg-${input.id}`,
    metadata: {},
    time: { created: Date.now() },
  };
}

function askOutput(): { status: "ask" | "deny" | "allow" } {
  return { status: "ask" };
}

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

/** Schema for the object under test across the tool tests. */
const OBJECT_SCHEMA = JSON.stringify({
  type: "object",
  properties: { title: { type: "string" }, version: { type: "integer" } },
  required: ["title", "version"],
});

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-contract-test-"));
  hooks = await swarmPlugin(pluginInput, { dataDir: dir });
  tool = hooks.tool ?? {};
});

afterAll(async () => {
  // Singleton hygiene (cross-file contamination fix): clear the module-level
  // runtime so a sibling test file sharing a bun worker never inherits this
  // file's store+client.
  disposeSwarmRuntime();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

/** Create a swarm + one spawned worker; returns ids/sessions. */
async function makeSwarm(tag: string) {
  const coordSession = `ses-ct-lead-${tag}`;
  const created = JSON.parse(String((await tool.swarm_create.execute({ name: `ct-${tag}` }, ctx(coordSession))).output));
  const swarmId = created.swarm.id;
  const spawned = JSON.parse(String((await tool.swarm_spawn.execute(
    { swarmId, members: [{ name: "writer", role: "contract writer", prompt: "test worker" }] },
    ctx(coordSession),
  )).output));
  const workerSession = spawned.spawned[0].sessionId as string;
  return { swarmId, coordSession, workerSession };
}

const store = () => swarmRuntime()!.core.store;

describe("swarm_contract define (coordinator-only)", () => {
  test("worker is rejected; coordinator defines with schema summary confirmation", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession, workerSession } = await makeSwarm(tag);

    // Worker attempt is rejected and defines nothing.
    const workerOut = String((await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA },
      ctx(workerSession),
    )).output);
    expect(workerOut).toContain("only the coordinator may define contracts");
    expect(await store().getContract(swarmId, "contracts/foo")).toBeUndefined();

    // Coordinator defines successfully, with a rich confirmation.
    const out = String((await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA, description: "release metadata" },
      ctx(coordSession),
    )).output);
    expect(out).toContain("CONTRACT DEFINED");
    expect(out).toContain("contracts/foo");
    expect(out).toContain("object{title:string, version:integer}");
    expect(out).toContain("release metadata");

    const contract = await store().getContract(swarmId, "contracts/foo");
    expect(contract?.keyPattern).toBe("contracts/foo");
    expect(JSON.parse(contract!.schemaJson)).toEqual(JSON.parse(OBJECT_SCHEMA));
    expect(contract?.description).toBe("release metadata");
  });

  test("define requires a parseable JSON-schema object", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession } = await makeSwarm(tag);

    const badJson = String((await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/bad", schema: "{not json" },
      ctx(coordSession),
    )).output);
    expect(badJson).toContain("schema is not valid JSON");

    const notObj = String((await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/bad", schema: "[1,2,3]" },
      ctx(coordSession),
    )).output);
    expect(notObj).toContain("schema must be a JSON object");

    expect(await store().getContract(swarmId, "contracts/bad")).toBeUndefined();
  });
});

describe("blackboardPut validation on contracted keys", () => {
  test("valid JSON satisfying the schema is accepted and records a changelog event", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession } = await makeSwarm(tag);
    await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA },
      ctx(coordSession),
    );

    const put = await tool.swarm_memory.execute(
      { swarmId, action: "put", key: "contracts/foo", value: JSON.stringify({ title: "v1", version: 1 }) },
      ctx(coordSession),
    );
    expect(JSON.parse(String(put.output)).version).toBe(1);

    const entry = await store().getBlackboard(swarmId, "contracts/foo");
    expect(entry?.value).toBe(JSON.stringify({ title: "v1", version: 1 }));

    // Changelog: one blackboard.write event for the insert.
    let events = await store().listEventsForEntity(swarmId, "blackboard", "contracts/foo");
    expect(events.filter((e) => e.type === "blackboard.write")).toHaveLength(1);
    expect(JSON.parse(events[0]!.payloadJson ?? "{}")).toEqual({ version: 1, authorMemberId: expect.any(String) });

    // Update path (CAS intact): second put with expectedVersion records v2.
    const put2 = await tool.swarm_memory.execute(
      { swarmId, action: "put", key: "contracts/foo", value: JSON.stringify({ title: "v2", version: 2 }), expectedVersion: 1 },
      ctx(coordSession),
    );
    expect(JSON.parse(String(put2.output)).version).toBe(2);

    events = await store().listEventsForEntity(swarmId, "blackboard", "contracts/foo");
    const writes = events.filter((e) => e.type === "blackboard.write");
    expect(writes).toHaveLength(2);
    expect(JSON.parse(writes[0]!.payloadJson ?? "{}").version).toBe(2); // newest first
    expect(JSON.parse(writes[1]!.payloadJson ?? "{}").version).toBe(1);
  });

  test("non-JSON value on a contracted key is rejected with a clear error", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession } = await makeSwarm(tag);
    await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA },
      ctx(coordSession),
    );

    await expect(
      tool.swarm_memory.execute(
        { swarmId, action: "put", key: "contracts/foo", value: "not json at all" },
        ctx(coordSession),
      ),
    ).rejects.toThrow("contract contracts/foo requires a JSON value");

    expect(await store().getBlackboard(swarmId, "contracts/foo")).toBeUndefined();
  });

  test("schema violations (missing required / wrong type) are rejected", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession } = await makeSwarm(tag);
    await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA },
      ctx(coordSession),
    );

    // Missing required property.
    await expect(
      tool.swarm_memory.execute(
        { swarmId, action: "put", key: "contracts/foo", value: JSON.stringify({ title: "no version" }) },
        ctx(coordSession),
      ),
    ).rejects.toThrow("contract violation on contracts/foo: $: missing required property 'version'");

    // Wrong type on a property.
    await expect(
      tool.swarm_memory.execute(
        { swarmId, action: "put", key: "contracts/foo", value: JSON.stringify({ title: 42, version: 1 }) },
        ctx(coordSession),
      ),
    ).rejects.toThrow(/contract violation on contracts\/foo: \$\.title: expected type string, got integer/);

    expect(await store().getBlackboard(swarmId, "contracts/foo")).toBeUndefined();
  });

  test("a NON-contracted key is unaffected (no validation, no changelog)", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession } = await makeSwarm(tag);
    await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA },
      ctx(coordSession),
    );

    // Plain text write to an ungoverned key — accepted verbatim.
    const put = await tool.swarm_memory.execute(
      { swarmId, action: "put", key: "free/notes", value: "plain text" },
      ctx(coordSession),
    );
    expect(JSON.parse(String(put.output)).version).toBe(1);
    expect((await store().getBlackboard(swarmId, "free/notes"))?.value).toBe("plain text");
    expect(await store().listEventsForEntity(swarmId, "blackboard", "free/notes")).toHaveLength(0);
  });

  test("CAS semantics are preserved on contracted keys (conflict still blocks)", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession } = await makeSwarm(tag);
    await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA },
      ctx(coordSession),
    );
    await tool.swarm_memory.execute(
      { swarmId, action: "put", key: "contracts/foo", value: JSON.stringify({ title: "a", version: 1 }) },
      ctx(coordSession),
    );
    // Stale expectedVersion → conflict, value untouched.
    const conflict = String((await tool.swarm_memory.execute(
      { swarmId, action: "put", key: "contracts/foo", value: JSON.stringify({ title: "b", version: 9 }), expectedVersion: 7 },
      ctx(coordSession),
    )).output);
    expect(conflict).toContain("BLACKBOARD CONFLICT");
    expect((await store().getBlackboard(swarmId, "contracts/foo"))?.value).toContain('"a"');
    // No changelog entry for the rejected write.
    const writes = (await store().listEventsForEntity(swarmId, "blackboard", "contracts/foo")).filter((e) => e.type === "blackboard.write");
    expect(writes).toHaveLength(1);
  });
});

describe("swarm_contract list", () => {
  test("renders schema summary + current version + changelog tail", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession } = await makeSwarm(tag);
    await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA, description: "release metadata" },
      ctx(coordSession),
    );
    await tool.swarm_memory.execute(
      { swarmId, action: "put", key: "contracts/foo", value: JSON.stringify({ title: "r1", version: 1 }) },
      ctx(coordSession),
    );

    const out = String((await tool.swarm_contract.execute({ swarmId, action: "list" }, ctx(coordSession))).output);
    expect(out).toContain("CONTRACTS");
    expect(out).toContain("contracts/foo");
    expect(out).toContain("object{title:string, version:integer}");
    expect(out).toContain("release metadata");
    expect(out).toContain("current v1");
    expect(out).toContain("changelog: v1 by coordinator");
  });

  test("list is available to any member and reports empty state", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, workerSession } = await makeSwarm(tag);

    const empty = String((await tool.swarm_contract.execute({ swarmId, action: "list" }, ctx(workerSession))).output);
    expect(empty).toContain("no contracts defined");
  });
});

describe("swarm_contract delete (coordinator-only + confirm)", () => {
  test("requires confirm = key; worker rejected; coordinator deletes", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId, coordSession, workerSession } = await makeSwarm(tag);
    await tool.swarm_contract.execute(
      { swarmId, action: "define", key: "contracts/foo", schema: OBJECT_SCHEMA },
      ctx(coordSession),
    );

    // No confirm → refused.
    const noConfirm = String((await tool.swarm_contract.execute(
      { swarmId, action: "delete", key: "contracts/foo" },
      ctx(coordSession),
    )).output);
    expect(noConfirm).toContain("REQUIRES confirm");
    expect(await store().getContract(swarmId, "contracts/foo")).toBeDefined();

    // Wrong confirm → refused.
    const wrongConfirm = String((await tool.swarm_contract.execute(
      { swarmId, action: "delete", key: "contracts/foo", confirm: "contracts/bar" },
      ctx(coordSession),
    )).output);
    expect(wrongConfirm).toContain("REQUIRES confirm");

    // Worker → refused.
    const workerOut = String((await tool.swarm_contract.execute(
      { swarmId, action: "delete", key: "contracts/foo", confirm: "contracts/foo" },
      ctx(workerSession),
    )).output);
    expect(workerOut).toContain("only the coordinator may delete contracts");
    expect(await store().getContract(swarmId, "contracts/foo")).toBeDefined();

    // Coordinator + exact confirm → deleted, and writes are unvalidated again.
    const ok = String((await tool.swarm_contract.execute(
      { swarmId, action: "delete", key: "contracts/foo", confirm: "contracts/foo" },
      ctx(coordSession),
    )).output);
    expect(ok).toContain("CONTRACT DELETED");
    expect(await store().getContract(swarmId, "contracts/foo")).toBeUndefined();

    const put = await tool.swarm_memory.execute(
      { swarmId, action: "put", key: "contracts/foo", value: "plain text now" },
      ctx(coordSession),
    );
    expect(JSON.parse(String(put.output)).version).toBe(1);
  });
});

describe("minimal draft-07 validator (supported keywords)", () => {
  test("type + properties + required", () => {
    const schema = { type: "object", properties: { title: { type: "string" }, version: { type: "integer" } }, required: ["title", "version"] };
    expect(validateValueAgainstSchema(schema, { title: "x", version: 1 })).toEqual([]);
    expect(validateValueAgainstSchema(schema, { title: "x" })).toEqual(["$: missing required property 'version'"]);
    expect(validateValueAgainstSchema(schema, { title: 1, version: 1 })).toEqual(["$.title: expected type string, got integer"]);
    expect(validateValueAgainstSchema(schema, "nope")).toEqual(["$: expected type object, got string"]);
  });

  test("items validates array elements", () => {
    const schema = { type: "array", items: { type: "integer" } };
    expect(validateValueAgainstSchema(schema, [1, 2, 3])).toEqual([]);
    expect(validateValueAgainstSchema(schema, [1, "x"])).toEqual(["$[1]: expected type integer, got string"]);
  });

  test("enum", () => {
    expect(validateValueAgainstSchema({ enum: ["a", "b"] }, "a")).toEqual([]);
    expect(validateValueAgainstSchema({ enum: ["a", "b"] }, "c")).toEqual(["$: value not in enum [\"a\",\"b\"]"]);
  });

  test("anyOf (at least one subschema must match)", () => {
    const schema = { anyOf: [{ type: "string" }, { type: "integer" }] };
    expect(validateValueAgainstSchema(schema, "s")).toEqual([]);
    expect(validateValueAgainstSchema(schema, 3)).toEqual([]);
    expect(validateValueAgainstSchema(schema, true)).toEqual(["$: value matches none of anyOf"]);
  });

  test("unsupported keywords pass through (treated as valid)", () => {
    const schema = { type: "string", minLength: 100, pattern: "^[0-9]+$", format: "email" };
    expect(validateValueAgainstSchema(schema, "x")).toEqual([]);
    expect(validateValueAgainstSchema(schema, 5)).toEqual(["$: expected type string, got integer"]);
  });

  test("summarizeSchema renders compact summaries", () => {
    expect(summarizeSchema(JSON.parse(OBJECT_SCHEMA))).toBe("object{title:string, version:integer}");
    expect(summarizeSchema({ type: "array", items: { type: "string" } })).toBe("array<string>");
    expect(summarizeSchema({ enum: ["a", "b"] })).toBe("enum[a, b]");
    expect(summarizeSchema({ type: "number" })).toBe("number");
  });
});
