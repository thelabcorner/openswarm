import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * Model-selection tests: the deterministic priority chain for member spawns
 * (requested > last-used > coordinator session > config default > fallback),
 * the durable last-used tuple, and the AUIX surface (spawn outputs report
 * model + modelSource; swarm_models shows the DEFAULT).
 */

let dirs: string[] = [];

function makeClient(opts: { coordModel?: { providerID: string; modelID: string }; providers?: unknown } = {}) {
  const { coordModel, providers } = opts;
  const providersPayload = providers ?? [
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
      // session.get returns the coordinator session ONLY when coordModel is set.
      get: async (o: any) => {
        if (coordModel && o?.path?.id === "ses-lead") {
          return { data: { id: "ses-lead", title: "lead", model: coordModel }, error: undefined };
        }
        return { data: null, error: undefined };
      },
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
  project: { id: "proj-models" },
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
  const dir = mkdtempSync(join(tmpdir(), "swarms-models-"));
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

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe("model selection priority chain", () => {
  test("no model known anywhere -> config default (opencode-go / deepseek-v4-flash)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("ms-default");
    const m = await spawn(swarmId, "worker-a");
    expect(m.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    expect(m.modelSource).toBe("default");
  });

  test("explicit requested model wins", async () => {
    await initPlugin();
    const swarmId = await createSwarm("ms-req");
    const m = await spawn(swarmId, "worker-a", { providerID: "opencode", modelID: "longcat-2.0-free" });
    expect(m.model).toEqual({ providerID: "opencode", modelID: "longcat-2.0-free" });
    expect(m.modelSource).toBe("requested");
  });

  test("tier label as provider ('go') maps to the real provider id", async () => {
    await initPlugin();
    const swarmId = await createSwarm("ms-tier");
    const m = await spawn(swarmId, "worker-a", { providerID: "go", modelID: "deepseek-v4-flash" });
    expect(m.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    expect(m.modelSource).toBe("requested");
  });

  test("modelID-only request resolves across providers, preferring opencode-go", async () => {
    await initPlugin();
    const swarmId = await createSwarm("ms-idonly");
    const m = await spawn(swarmId, "worker-a", { modelID: "deepseek-v4-flash" });
    expect(m.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    expect(m.modelSource).toBe("requested");
  });

  test("display-name request resolves by model name", async () => {
    await initPlugin();
    const swarmId = await createSwarm("ms-name");
    const m = await spawn(swarmId, "worker-a", { modelID: "Longcat" });
    expect(m.model?.modelID).toBe("longcat-2.0-free");
    expect(m.modelSource).toBe("requested");
  });

  test("unavailable requested model falls back with an explanatory note", async () => {
    await initPlugin();
    const swarmId = await createSwarm("ms-badreq");
    const m = await spawn(swarmId, "worker-a", { providerID: "nope", modelID: "nope" });
    expect(m.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    expect(m.modelSource).toBe("default");
    expect(String(m.modelNote)).toContain("requested model 'nope/nope' is not available");
  });

  test("coordinator session model is used when nothing was ever spawned", async () => {
    await initPlugin({}, makeClient({ coordModel: { providerID: "opencode", modelID: "longcat-2.0-free" } }));
    const swarmId = await createSwarm("ms-coord");
    const m = await spawn(swarmId, "worker-a");
    expect(m.model).toEqual({ providerID: "opencode", modelID: "longcat-2.0-free" });
    expect(m.modelSource).toBe("coordinator");
  });

  test("last-used tuple beats the coordinator model for later spawns", async () => {
    await initPlugin({}, makeClient({ coordModel: { providerID: "opencode", modelID: "longcat-2.0-free" } }));
    const swarmId = await createSwarm("ms-lastused");
    // First spawn EXPLICITLY uses deepseek flash on go — recorded as last-used.
    const first = await spawn(swarmId, "worker-a", { providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    expect(first.modelSource).toBe("requested");
    // Second spawn omits the model -> reuses the last-used tuple, NOT the
    // coordinator's longcat.
    const second = await spawn(swarmId, "worker-b");
    expect(second.model).toEqual({ providerID: "opencode-go", modelID: "deepseek-v4-flash" });
    expect(second.modelSource).toBe("last-used");
  });

  test("last-used tuple is durable (blackboard context/model/last-used)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("ms-durable");
    await spawn(swarmId, "worker-a", { providerID: "opencode", modelID: "longcat-2.0-free" });
    const got = await tool.swarm_memory.execute(
      { swarmId, action: "get", key: "context/model/last-used" },
      ctx("ses-lead"),
    );
    const out = String(got.output ?? got);
    expect(out).toContain("longcat-2.0-free");
  });

  test("config defaultMemberModel override changes the default", async () => {
    await initPlugin({ defaultMemberModel: { providerID: "opencode", modelID: "deepseek-v4-flash-free" } });
    const swarmId = await createSwarm("ms-cfg");
    const m = await spawn(swarmId, "worker-a");
    expect(m.model).toEqual({ providerID: "opencode", modelID: "deepseek-v4-flash-free" });
    expect(m.modelSource).toBe("default");
  });

  test("default unavailable on this machine -> any available model with fallback note", async () => {
    await initPlugin(
      {},
      makeClient({
        providers: [{ id: "lmstudio", models: { "local-1": { name: "Local 1" } } }],
      }),
    );
    const swarmId = await createSwarm("ms-fallback");
    const m = await spawn(swarmId, "worker-a");
    expect(m.model).toEqual({ providerID: "lmstudio", modelID: "local-1" });
    expect(m.modelSource).toBe("fallback");
    expect(String(m.modelNote)).toContain("not available");
  });
});

describe("swarm_models AUIX", () => {
  test("swarm_models surfaces the resolved default up front", async () => {
    await initPlugin();
    const res = await tool.swarm_models.execute({}, ctx("ses-lead"));
    const out = String(res.output ?? res);
    expect(out).toContain("DEFAULT (used when model is omitted");
    expect(out).toContain('providerID: "opencode-go", modelID: "deepseek-v4-flash"');
    expect(out).toContain("GO");
  });
});
