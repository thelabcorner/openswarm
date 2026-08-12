import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, disposeSwarmRuntime } from "../../src/plugin.ts";
import { hasCapability, modelInputPrice, modelsWithCapability, priceLabel, cheapestWithCapability } from "../../src/models/catalog.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { RuntimeModelInfo } from "../../src/runtime/runtime-types.ts";

/**
 * Capability-aware delegation: models carry modalities + cost from the
 * provider config; swarm_models(capability) lists capable models cheapest
 * first; spawning a member with `capability` picks the cheapest capable model.
 */

let dirs: string[] = [];

function makeClient() {
  return {
    config: {
      providers: async () => ({
        data: {
          providers: [
            {
              id: "opencode-go",
              models: {
                // No modalities published -> text-only; cost published.
                "deepseek-v4-flash": {
                  name: "DeepSeek V4 Flash",
                  cost: { input: 0.14, output: 0.28, cache_read: 0.0028 },
                  limit: { context: 128000, output: 16000 },
                },
              },
            },
            {
              id: "opencode",
              models: {
                // Vision + PDF, cheapest of the capable set.
                "mimo-v2.5": {
                  name: "MiMo V2.5",
                  modalities: { input: ["text", "image", "pdf"], output: ["text"] },
                  cost: { input: 0.14, output: 0.28 },
                  limit: { context: 200000, output: 16000 },
                },
                // Vision + PDF, mid price.
                "gpt-5.6-luna": {
                  name: "GPT 5.6 Luna",
                  modalities: { input: ["text", "image", "pdf"], output: ["text"] },
                  cost: { input: 0.2, output: 1.2 },
                  limit: { context: 272000, output: 16000 },
                },
                // Vision only, priciest.
                "grok-4.5": {
                  name: "Grok 4.5",
                  modalities: { input: ["text", "image"], output: ["text"] },
                  cost: { input: 2.0, output: 6.0 },
                },
              },
            },
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
  project: { id: "proj-cap" },
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

async function initPlugin(): Promise<void> {
  disposeSwarmRuntime();
  const dir = mkdtempSync(join(tmpdir(), "swarms-cap-"));
  dirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient()), { dataDir: dir });
  tool = hooks.tool ?? {};
}

async function createSwarm(name: string): Promise<string> {
  const created = await tool.swarm_create.execute({ name }, ctx("ses-lead"));
  return JSON.parse(String(created.output ?? created)).swarm.id;
}

async function spawn(swarmId: string, name: string, extra: Record<string, unknown> = {}): Promise<any> {
  const res = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name, role: "worker", ...extra }] },
    ctx("ses-lead"),
  );
  return JSON.parse(String(res.output ?? res)).spawned[0];
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

const MODEL: RuntimeModelInfo = {
  providerID: "opencode", modelID: "grok-4.5", tier: "zen",
  modalities: { input: ["text", "image"], output: ["text"] },
  cost: { input: 2, output: 6 },
};

describe("catalog helpers", () => {
  test("hasCapability: text always; image/pdf from modalities; no modalities = text-only", () => {
    expect(hasCapability(MODEL, "image")).toBe(true);
    expect(hasCapability(MODEL, "pdf")).toBe(false);
    expect(hasCapability(MODEL, "text")).toBe(true);
    const textOnly: RuntimeModelInfo = { providerID: "opencode-go", modelID: "deepseek-v4-flash", tier: "go" };
    expect(hasCapability(textOnly, "image")).toBe(false);
    expect(hasCapability(textOnly, "text")).toBe(true);
  });

  test("cheapestWithCapability ranks by provider cost then tier", () => {
    const models: RuntimeModelInfo[] = [
      MODEL,
      { ...MODEL, modelID: "mimo", cost: { input: 0.14, output: 0.28 }, modalities: { input: ["text", "image", "pdf"], output: ["text"] } },
      { ...MODEL, modelID: "luna", cost: { input: 0.2, output: 1.2 }, modalities: { input: ["text", "image", "pdf"], output: ["text"] } },
    ];
    const best = cheapestWithCapability(models, "pdf");
    expect(best?.modelID).toBe("mimo");
    const imageList = modelsWithCapability(models, "image");
    expect(imageList.map((m) => m.modelID)).toEqual(["mimo", "luna", "grok-4.5"]);
  });

  test("fallback catalog prices models without provider cost", () => {
    const flash: RuntimeModelInfo = { providerID: "opencode-go", modelID: "deepseek-v4-flash", tier: "go" };
    expect(modelInputPrice(flash)).toBe(0.14);
    expect(priceLabel(flash)).toContain("0.140");
  });
});

describe("swarm_models capability view", () => {
  test("capability filter lists only capable models, cheapest first, with prices", async () => {
    await initPlugin();
    const res = await tool.swarm_models.execute({ capability: "image" }, ctx("ses-lead"));
    const out = String(res.output ?? res);
    expect(out).toContain("MODELS WITH CAPABILITY 'image'");
    expect(out).toContain("mimo-v2.5");
    expect(out).toContain("gpt-5.6-luna");
    expect(out).toContain("grok-4.5");
    expect(out).not.toContain("deepseek-v4-flash");
    // Cheapest first + marker.
    const mimo = out.indexOf("mimo-v2.5");
    const luna = out.indexOf("gpt-5.6-luna");
    const grok = out.indexOf("grok-4.5");
    expect(mimo).toBeGreaterThan(-1);
    expect(mimo).toBeLessThan(luna);
    expect(luna).toBeLessThan(grok);
    expect(out).toContain("★ CHEAPEST");
    expect(out).toContain("per 1M");
  });

  test("pdf capability excludes vision-only models", async () => {
    await initPlugin();
    const res = await tool.swarm_models.execute({ capability: "pdf" }, ctx("ses-lead"));
    const out = String(res.output ?? res);
    expect(out).toContain("mimo-v2.5");
    expect(out).toContain("gpt-5.6-luna");
    expect(out).not.toContain("grok-4.5");
  });

  test("cheapest limit caps the listing", async () => {
    await initPlugin();
    const res = await tool.swarm_models.execute({ capability: "image", cheapest: 2 }, ctx("ses-lead"));
    const out = String(res.output ?? res);
    expect(out).toContain("mimo-v2.5");
    expect(out).toContain("gpt-5.6-luna");
    expect(out).not.toContain("grok-4.5");
  });
});

describe("capability-aware spawn", () => {
  test("member with capability spawns on the CHEAPEST capable model (source capability)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("cap-spawn");
    const m = await spawn(swarmId, "vision-reader", { capability: "pdf" });
    expect(m.model?.modelID).toBe("mimo-v2.5");
    expect(m.modelSource).toBe("capability");
    expect(String(m.modelNote)).toContain("cheapest model with 'pdf' capability");
  });

  test("explicit model beats capability", async () => {
    await initPlugin();
    const swarmId = await createSwarm("cap-explicit");
    const m = await spawn(swarmId, "reader", { capability: "image", model: { providerID: "opencode", modelID: "grok-4.5" } });
    expect(m.model?.modelID).toBe("grok-4.5");
    expect(m.modelSource).toBe("requested");
  });

  test("capability with no capable model falls through to the default chain with a note", async () => {
    await initPlugin();
    const swarmId = await createSwarm("cap-none");
    const m = await spawn(swarmId, "reader", { capability: "video" });
    expect(m.model?.modelID).toBe("deepseek-v4-flash");
    expect(m.modelSource).toBe("default");
    expect(String(m.modelNote)).toContain("no configured model can consume 'video'");
  });
});
