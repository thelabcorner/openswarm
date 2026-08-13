import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, disposeSwarmRuntime, handleOpenCodeEvent, swarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * Model-management tests (t-model-mgmt): the `swarm_model` tool (show/set) and
 * the session.updated auto-sync that keeps a member's model in sync when the
 * user changes it in the chat composer.
 *
 *   (a) show renders member + model
 *   (b) set is coordinator-only (workers rejected)
 *   (c) set updates the row + last-used (next spawn without a model inherits
 *       it, source 'last-used'), notifies the member, records a timeline event
 *   (d) set with an invalid model -> clear error, nothing changed
 *   (e) session.updated auto-sync updates the member row + notifies
 *   (f) session.updated with undefined model -> no change
 *   (g) updateMemberModel roundtrips on both backends (sqlite + chunkdb)
 */

let dirs: string[] = [];

function makeClient() {
  const providersPayload = [
    {
      id: "opencode-go",
      models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash (2x usage)" } },
    },
    {
      id: "opencode",
      models: {
        "deepseek-v4-flash-free": { name: "DeepSeek V4 Flash Free" },
        "longcat-2.0-free": { name: "Longcat" },
      },
    },
  ];
  return {
    config: {
      providers: async () => ({ data: { providers: providersPayload }, error: undefined }),
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
  project: { id: "proj-mgmt" },
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

async function initPlugin(options: Record<string, unknown> = {}, client: unknown = makeClient()): Promise<void> {
  disposeSwarmRuntime();
  const dir = mkdtempSync(join(tmpdir(), "swarms-mgmt-"));
  dirs.push(dir);
  hooks = await swarmPlugin(pluginInput(client), { dataDir: dir, ...options });
  tool = hooks.tool ?? {};
}

async function createSwarm(name: string): Promise<string> {
  const created = await tool.swarm_create.execute({ name }, ctx("ses-lead"));
  const json = JSON.parse(String(created.output ?? created));
  return json.swarm.id;
}

async function spawn(swarmId: string, name: string, model?: unknown): Promise<any> {
  const res = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name, role: "worker", ...(model ? { model } : {}) }] },
    ctx("ses-lead"),
  );
  const json = JSON.parse(String(res.output ?? res));
  return json.spawned[0];
}

async function messages(swarmId: string, limit = 100) {
  const rt = swarmRuntime()!;
  return rt.store.listMessagesBySwarm(swarmId, limit);
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe("swarm_model show", () => {
  test("renders member + model (and default for unset members)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("mm-show");
    // worker-b first: plain default. worker-a second: explicit longcat (also
    // becomes last-used — worker-b must NOT inherit it, so it spawns first).
    await spawn(swarmId, "worker-b");
    await spawn(swarmId, "worker-a", { providerID: "opencode", modelID: "longcat-2.0-free" });

    // List all members: worker-a has the set model; worker-b carries its
    // resolved spawn default; the coordinator row is unset -> "model default".
    const all = await tool.swarm_model.execute({ swarmId, action: "show" }, ctx("ses-lead"));
    const allOut = String(all.output ?? all);
    expect(allOut).toContain("worker-a");
    expect(allOut).toContain("opencode/longcat-2.0-free (set)");
    expect(allOut).toContain("worker-b");
    expect(allOut).toContain("opencode-go/deepseek-v4-flash (set)");
    expect(allOut).toContain("model default (unset");

    // Single member lookup.
    const one = await tool.swarm_model.execute({ swarmId, action: "show", member: "worker-a" }, ctx("ses-lead"));
    const oneOut = String(one.output ?? one);
    expect(oneOut).toContain("worker-a");
    expect(oneOut).toContain("opencode/longcat-2.0-free");

    // Unknown member -> clear error.
    const missing = await tool.swarm_model.execute({ swarmId, action: "show", member: "ghost" }, ctx("ses-lead"));
    expect(String(missing.output ?? missing)).toContain("no member 'ghost'");
  });
});

describe("swarm_model set", () => {
  test("is coordinator-only (a worker caller is rejected)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("mm-coordonly");
    const w = await spawn(swarmId, "worker-a");
    const res = await tool.swarm_model.execute(
      { swarmId, action: "set", member: "worker-a", model: { providerID: "opencode", modelID: "longcat-2.0-free" } },
      ctx(w.sessionId),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("coordinator-only");
    // Nothing changed: the member row still carries its spawn default (no set).
    const rt = swarmRuntime()!;
    const member = await rt.store.getMemberByName(swarmId, "worker-a");
    expect(member!.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
  });

  test("valid model updates the row + last-used, notifies the member, records a timeline event", async () => {
    await initPlugin();
    const swarmId = await createSwarm("mm-set");
    await spawn(swarmId, "worker-a");

    const res = await tool.swarm_model.execute(
      { swarmId, action: "set", member: "worker-a", model: { providerID: "opencode", modelID: "longcat-2.0-free" } },
      ctx("ses-lead"),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("member 'worker-a' model set to opencode/longcat-2.0-free");
    expect(out).toContain("last-used updated");

    const rt = swarmRuntime()!;

    // (1) The member row is updated.
    const member = await rt.store.getMemberByName(swarmId, "worker-a");
    expect(member!.model).toEqual({ providerID: "opencode", modelID: "longcat-2.0-free" });

    // (2) last-used: a subsequent spawn WITHOUT a model inherits the new one.
    const next = await spawn(swarmId, "worker-b");
    expect(next.model).toEqual({ providerID: "opencode", modelID: "longcat-2.0-free" });
    expect(next.modelSource).toBe("last-used");

    // (3) The member was notified (noreply finding in its mailbox).
    const msgs = await messages(swarmId);
    const notice = msgs.find((m) => m.kind === "finding" && m.body.text.includes("Your model changed to opencode/longcat-2.0-free"));
    expect(notice).toBeDefined();
    expect(notice!.to.type).toBe("member");

    // (4) A timeline event was recorded.
    const events = await rt.store.listEvents(swarmId);
    const changed = events.find((e) => e.type === "member.model_changed" && e.entityId === member!.id);
    expect(changed).toBeDefined();
    const payload = JSON.parse(changed!.payloadJson ?? "{}") as { model?: { providerID?: string; modelID?: string }; auto?: boolean };
    expect(payload.model).toEqual({ providerID: "opencode", modelID: "longcat-2.0-free" });
    expect(payload.auto).toBeUndefined();
  });

  test("tier label provider and modelID-only refs resolve via the smart resolver", async () => {
    await initPlugin();
    const swarmId = await createSwarm("mm-tier");
    await spawn(swarmId, "worker-a");
    // Tier label 'go' -> opencode-go.
    const tier = await tool.swarm_model.execute(
      { swarmId, action: "set", member: "worker-a", model: { providerID: "go", modelID: "deepseek-v4-flash" } },
      ctx("ses-lead"),
    );
    expect(String(tier.output ?? tier)).toContain("opencode-go/deepseek-v4-flash");

    // modelID-only ref resolves across providers (prefers opencode-go).
    const idOnly = await tool.swarm_model.execute(
      { swarmId, action: "set", member: "worker-a", model: { modelID: "deepseek-v4-flash" } },
      ctx("ses-lead"),
    );
    expect(String(idOnly.output ?? idOnly)).toContain("opencode-go/deepseek-v4-flash");
  });

  test("invalid model -> clear error, nothing changed", async () => {
    await initPlugin();
    const swarmId = await createSwarm("mm-invalid");
    await spawn(swarmId, "worker-a");

    const res = await tool.swarm_model.execute(
      { swarmId, action: "set", member: "worker-a", model: { providerID: "nope", modelID: "nope" } },
      ctx("ses-lead"),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("invalid model 'nope/nope'");
    expect(out).toContain("Usage:");
    expect(out).toContain("swarm_models");

    // Nothing changed: row still carries its spawn default, no last-used flip
    // to the invalid ref, no event, no notice.
    const rt = swarmRuntime()!;
    const member = await rt.store.getMemberByName(swarmId, "worker-a");
    expect(member!.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    const events = await rt.store.listEvents(swarmId);
    expect(events.some((e) => e.type === "member.model_changed")).toBe(false);
    const msgs = await messages(swarmId);
    expect(msgs.some((m) => m.body.text.includes("model changed"))).toBe(false);
    // The next spawn does NOT inherit the invalid ref (last-used untouched —
    // it still holds worker-a's spawn default).
    const next = await spawn(swarmId, "worker-b");
    expect(next.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    expect(next.modelSource).toBe("last-used");
  });
});

describe("session.updated auto-sync", () => {
  test("a session.updated carrying a new model updates the member row and notifies", async () => {
    await initPlugin();
    const swarmId = await createSwarm("mm-auto");
    const w = await spawn(swarmId, "worker-a");
    const rt = swarmRuntime()!;
    // Spawned with the resolved default; the sync then flips it to longcat.
    expect((await rt.store.getMemberByName(swarmId, "worker-a"))!.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });

    await handleOpenCodeEvent(rt!, {
      type: "session.updated",
      properties: {
        info: {
          id: w.sessionId,
          title: "worker-a",
          model: { providerID: "opencode", modelID: "longcat-2.0-free" },
        },
      },
    });

    // Member row synced.
    const member = await rt.store.getMemberByName(swarmId, "worker-a");
    expect(member!.model).toEqual({ providerID: "opencode", modelID: "longcat-2.0-free" });

    // Member notified with the auto-sync wording.
    const msgs = await messages(swarmId);
    const notice = msgs.find((m) => m.body.text.includes("Your model was updated to opencode/longcat-2.0-free (changed in the session)."));
    expect(notice).toBeDefined();

    // Timeline event flagged auto: true.
    const events = await rt.store.listEvents(swarmId);
    const changed = events.find((e) => e.type === "member.model_changed" && e.entityId === member!.id);
    expect(changed).toBeDefined();
    const payload = JSON.parse(changed!.payloadJson ?? "{}") as { auto?: boolean };
    expect(payload.auto).toBe(true);
  });

  test("session.updated with an undefined model -> no change", async () => {
    await initPlugin();
    const swarmId = await createSwarm("mm-undefined");
    const w = await spawn(swarmId, "worker-a");
    const rt = swarmRuntime()!;

    await handleOpenCodeEvent(rt!, {
      type: "session.updated",
      properties: { info: { id: w.sessionId, title: "worker-a" } },
    });
    await handleOpenCodeEvent(rt!, {
      type: "session.updated",
      properties: { info: { id: w.sessionId, title: "worker-a", model: undefined } },
    });

    // Nothing changed: the row keeps its spawn default, no event, no notice.
    const member = await rt.store.getMemberByName(swarmId, "worker-a");
    expect(member!.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    const events = await rt.store.listEvents(swarmId);
    expect(events.some((e) => e.type === "member.model_changed")).toBe(false);
    const msgs = await messages(swarmId);
    expect(msgs.some((m) => m.body.text.includes("model was updated"))).toBe(false);
  });

  test("a non-diff session.updated does not re-notify (dedup)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("mm-dedup");
    const w = await spawn(swarmId, "worker-a");
    const rt = swarmRuntime()!;

    const evt = {
      type: "session.updated",
      properties: {
        info: { id: w.sessionId, title: "worker-a", model: { providerID: "opencode", modelID: "longcat-2.0-free" } },
      },
    };
    await handleOpenCodeEvent(rt!, evt);
    // Same model again — no diff, must not notify or record again.
    await handleOpenCodeEvent(rt!, evt);

    const events = await rt.store.listEvents(swarmId);
    const changed = events.filter((e) => e.type === "member.model_changed");
    expect(changed.length).toBe(1);
    const msgs = await messages(swarmId);
    expect(msgs.filter((m) => m.body.text.includes("model was updated")).length).toBe(1);
  });
});

describe("updateMemberModel store roundtrip (both backends)", () => {
  test("sqlite backend: set + clear roundtrip through the store API", async () => {
    await initPlugin({ storeBackend: "sqlite" });
    const swarmId = await createSwarm("mm-sqlite");
    const w = await spawn(swarmId, "worker-a");
    const rt = swarmRuntime()!;
    const id = (await rt.store.getMemberByName(swarmId, "worker-a"))!.id;

    await rt.store.updateMemberModel(id, { providerID: "opencode", modelID: "longcat-2.0-free" });
    expect((await rt.store.getMemberById(id))!.model).toEqual({ providerID: "opencode", modelID: "longcat-2.0-free" });
    expect(w.sessionId).toBeTruthy();

    await rt.store.updateMemberModel(id, undefined);
    expect((await rt.store.getMemberById(id))!.model).toBeUndefined();
  });

  test("chunkdb backend: set + clear roundtrip through the store API", async () => {
    await initPlugin({ storeBackend: "chunkdb" });
    const swarmId = await createSwarm("mm-chunkdb");
    await spawn(swarmId, "worker-a");
    const rt = swarmRuntime()!;
    const id = (await rt.store.getMemberByName(swarmId, "worker-a"))!.id;

    await rt.store.updateMemberModel(id, { providerID: "opencode", modelID: "longcat-2.0-free" });
    expect((await rt.store.getMemberById(id))!.model).toEqual({ providerID: "opencode", modelID: "longcat-2.0-free" });

    await rt.store.updateMemberModel(id, undefined);
    expect((await rt.store.getMemberById(id))!.model).toBeUndefined();
  });
});
