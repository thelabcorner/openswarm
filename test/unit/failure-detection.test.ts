import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, disposeSwarmRuntime, handleOpenCodeEvent } from "../../src/plugin.ts";
import type { SwarmPluginRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import {
  classifyFailure,
  lastAssistantMessage,
  extractMessageFailureText,
  truncateFailure,
} from "../../src/supervisor/stalls.ts";

/**
 * Chat failure detection tests (task t-fail-detect): classify upstream/provider
 * errors (auth/quota/rate/model-not-found/context/other) in member sessions and
 * surface them in stall diagnosis + coordinator notification:
 *  (a) classifyFailure unit table — every pattern + negatives;
 *  (b) the diagnoser surfaces provider-error with the remedy when the member's
 *      LAST assistant message is an upstream failure (error-part message);
 *  (c) quota/rate messages map to the usage-limit reason (existing notify path);
 *  (d) notifyProviderError dedupes — repeated ladder passes -> ONE notification;
 *  (e) session.error event with an auth message notifies the coordinator once;
 *  (f) swarm_stalls report renders provider-error with the subtype + remedy.
 */

let dirs: string[] = [];
let hooks: Hooks | undefined;
let tool: Record<string, any>;

// ==== mutable fake-runtime state (reset per plugin init) ====
let statusData: Record<string, any> = {};
let messagesData: Record<string, any[]> = {};
const sessions = new Map<string, any>();

const fakeClient = {
  config: {
    providers: async () => ({
      data: {
        providers: [
          { id: "opencode-go", models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash (2x usage)" } } },
        ],
      },
      error: undefined,
    }),
  },
  session: {
    create: async (opts: any) => {
      const id = `ses-fd-${Math.random().toString(36).slice(2, 8)}`;
      const s = { id, title: opts.body?.title, parentID: undefined, directory: "." };
      sessions.set(id, s);
      return { data: s, error: undefined };
    },
    get: async (opts: any) => {
      const s = sessions.get(opts?.path?.id);
      if (!s) return { data: null, error: undefined };
      return { data: { ...s }, error: undefined };
    },
    children: async () => ({ data: [], error: undefined }),
    messages: async (opts: any) => ({ data: messagesData[opts?.path?.id] ?? [], error: undefined }),
    status: async () => ({ data: statusData, error: undefined }),
    abort: async () => ({ data: undefined, error: undefined }),
    update: async () => ({ data: {}, error: undefined }),
    prompt: async () => ({ data: { info: {} }, error: undefined }),
    promptAsync: async () => ({ data: undefined, error: undefined }),
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-fd" },
  directory: ".",
  worktree: ".",
  experimental_workspace: { register() {} },
  serverUrl: new URL("http://x"),
  $: {},
};

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

async function initPlugin(): Promise<void> {
  disposeSwarmRuntime();
  const dir = mkdtempSync(join(tmpdir(), "swarms-fd-"));
  dirs.push(dir);
  statusData = {};
  messagesData = {};
  sessions.clear();
  hooks = await swarmPlugin(pluginInput, { dataDir: dir });
  tool = hooks.tool ?? {};
}

async function runtime(): Promise<SwarmPluginRuntime> {
  const mod = await import("../../src/plugin.ts");
  const rt = mod.swarmRuntime();
  if (!rt) throw new Error("no swarm runtime initialized");
  return rt;
}

/** Create a swarm + spawn a worker member; returns ids + the runtime. */
async function makeSwarmWithWorker(name: string, workerName = "worker") {
  const coordSession = `ses-fd-lead-${Math.random().toString(36).slice(2, 8)}`;
  const createRes = await tool.swarm_create.execute({ name }, ctx(coordSession));
  const created = JSON.parse(String(createRes.output ?? createRes));
  const swarmId = created.swarm.id as string;
  const spawnRes = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name: workerName, role: "impl" }] },
    ctx(coordSession),
  );
  const spawned = JSON.parse(String(spawnRes.output ?? spawnRes));
  const workerSessionId = spawned.spawned[0].sessionId as string;
  const rt = await runtime();
  const workerMember = await rt.store.getMemberBySessionId(workerSessionId);
  if (!workerMember) throw new Error("worker member not found after spawn");
  return { rt, swarmId, coordSession, workerSessionId, workerMember };
}

/** Put an assistant message with the given parts as the member's LAST message. */
function lastMessage(sessionId: string, role: "assistant" | "user", parts: Array<{ type: string; text: string }>, createdAgoMs = 30_000): void {
  messagesData[sessionId] = [
    { info: { id: `m-${Math.random().toString(36).slice(2, 8)}`, role, time: { created: Date.now() - createdAgoMs }, parts } },
  ];
}

/** Coordinator [CHAT FAILURE] digest text for a swarm (t-flood-aggregate:
 * advisories are delivered as lines of the debounced coordinator digest
 * instead of mailbox findings). Forces a flush and returns the rendered
 * digest ("" when nothing was pending). */
async function chatFailureDigest(swarmId: string): Promise<string> {
  const rt = await runtime();
  return (await rt.notices.flush(swarmId)) ?? "";
}

async function reportFor(swarmId: string, sessionID: string): Promise<string> {
  const res = await tool.swarm_stalls.execute({ swarmId, action: "report" }, ctx(sessionID));
  return String(res.output ?? res);
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe("a. classifyFailure unit table", () => {
  test("auth: invalid bearer credential upstream failure", () => {
    expect(classifyFailure("Upstream request failed: [invalid_bearer_credential] Missing or invalid bearer credential")).toEqual({
      reason: "provider-error",
      subtype: "auth",
      remedy: "check provider credentials (opencode auth login / provider API key)",
    });
    // underscore variant + case-insensitive
    expect(classifyFailure("upstream request failed: invalid_bearer_credential")).toMatchObject({ subtype: "auth" });
  });

  test("auth: 401 / 403 / unauthorized", () => {
    expect(classifyFailure("401 Unauthorized: the API key is invalid")).toMatchObject({ reason: "provider-error", subtype: "auth" });
    expect(classifyFailure("403 Forbidden: provider rejected the request")).toMatchObject({ reason: "provider-error", subtype: "auth" });
    expect(classifyFailure("unauthorized")).toMatchObject({ subtype: "auth" });
  });

  test("quota: insufficient quota / quota exceeded -> usage-limit quota", () => {
    expect(classifyFailure("Insufficient quota for model deepseek-v4-flash")).toEqual({
      reason: "usage-limit",
      subtype: "quota",
      remedy: "wait for the limit window / change model",
    });
    expect(classifyFailure("quota exceeded: free tier limit reached")).toMatchObject({ subtype: "quota" });
    expect(classifyFailure("insufficient_quota")).toMatchObject({ subtype: "quota" });
  });

  test("rate: rate-limit -> usage-limit rate", () => {
    expect(classifyFailure("provider 429: rate limit hit for model deepseek-v4-flash")).toEqual({
      reason: "usage-limit",
      subtype: "rate",
      remedy: "wait for the limit window / change model",
    });
    expect(classifyFailure("ratelimit exceeded")).toMatchObject({ subtype: "rate" });
    expect(classifyFailure("rate-limit exceeded")).toMatchObject({ subtype: "rate" });
  });

  test("model-not-found -> provider-error model-not-found (swarm_model set recipe)", () => {
    expect(classifyFailure("Model not found: deepseek-v4-flash is not a valid model id")).toEqual({
      reason: "provider-error",
      subtype: "model-not-found",
      remedy: "the member's model no longer exists — switch it: swarm_model(action: 'set', member: '<name>', model: { providerID, modelID })",
    });
    expect(classifyFailure("model_not_found")).toMatchObject({ subtype: "model-not-found" });
  });

  test("context: context length / maximum context -> provider-error context", () => {
    expect(classifyFailure("context length exceeded: maximum context window is 128000 tokens")).toEqual({
      reason: "provider-error",
      subtype: "context",
      remedy: "context too long — compact the session",
    });
    expect(classifyFailure("maximum context reached")).toMatchObject({ subtype: "context" });
    expect(classifyFailure("context_length exceeded")).toMatchObject({ subtype: "context" });
  });

  test("generic upstream failure -> provider-error other", () => {
    expect(classifyFailure("Upstream request failed: provider connection reset")).toEqual({
      reason: "provider-error",
      subtype: "other",
      remedy: "check the provider status / credentials",
    });
  });

  test("negatives: normal text / aborts / empty are NOT chat failures", () => {
    expect(classifyFailure("all systems normal, continuing the task")).toBeUndefined();
    expect(classifyFailure("connection reset by peer")).toBeUndefined();
    expect(classifyFailure("The operation was aborted.")).toBeUndefined();
    expect(classifyFailure("")).toBeUndefined();
    expect(classifyFailure("  ")).toBeUndefined();
    expect(classifyFailure("quota") /* bare word — no phrase match */).toBeUndefined();
  });
});

describe("b. diagnoser surfaces provider-error from the last assistant message", () => {
  test("error-part message -> provider-error/auth + remedy + nextAction", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("fd-provider");
    lastMessage(workerSessionId, "assistant", [
      { type: "error", text: "Upstream request failed: [invalid_bearer_credential] Missing or invalid bearer credential" },
    ]);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 30_000 });

    const report = await rt.stalls.diagnose(swarmId);
    const me = report.members.find((x) => x.memberName === "worker");
    expect(me?.reason).toBe("provider-error");
    expect(me?.subtype).toBe("auth");
    expect(me?.nextAction).toBe("provider-error-notify");
    expect(me?.evidence[0]).toContain("chat failure: auth");
    expect(me?.evidence[0]).toContain("invalid_bearer_credential");
    expect(me?.recipe).toContain("check provider credentials");
    expect(report.verdict).toBe("stalled");
    expect(report.causes).toContain("provider-error");
  });

  test("text-part upstream failure in a reasoning part is also caught", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("fd-reasoning");
    lastMessage(workerSessionId, "assistant", [
      { type: "reasoning", text: "Upstream request failed: [invalid_bearer_credential] credential missing on retry" },
    ]);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 30_000 });
    const report = await rt.stalls.diagnose(swarmId);
    const me = report.members.find((x) => x.memberName === "worker");
    expect(me?.reason).toBe("provider-error");
    expect(me?.subtype).toBe("auth");
  });

  test("a normal last message keeps the member working (not a chat failure)", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("fd-normal");
    lastMessage(workerSessionId, "assistant", [{ type: "text", text: "Task in progress, continuing..." }]);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 1000 });
    const report = await rt.stalls.diagnose(swarmId);
    const me = report.members.find((x) => x.memberName === "worker");
    expect(me?.reason).toBe("working");
    expect(report.verdict).toBe("healthy");
  });

  test("long failure text is truncated to 120 chars in the evidence", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("fd-trunc");
    const long = "Upstream request failed: [invalid_bearer_credential] Missing or invalid bearer credential — please re-run `opencode auth login` and make sure the provider API key is valid before continuing with the task at hand today";
    lastMessage(workerSessionId, "assistant", [{ type: "error", text: long }]);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 30_000 });
    const report = await rt.stalls.diagnose(swarmId);
    const me = report.members.find((x) => x.memberName === "worker");
    expect(me?.reason).toBe("provider-error");
    const evidence = me?.evidence[0] ?? "";
    expect(evidence.length).toBeLessThanOrEqual("chat failure: auth — ".length + 120 + 2);
    expect(evidence).toContain(truncateFailure(long));
  });
});

describe("c. quota/rate map to usage-limit reason", () => {
  test("quota message -> usage-limit quota + recorded limit signal", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("fd-quota");
    lastMessage(workerSessionId, "assistant", [{ type: "text", text: "Insufficient quota for model deepseek-v4-flash" }]);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 30_000 });

    const report = await rt.stalls.diagnose(swarmId);
    const me = report.members.find((x) => x.memberName === "worker");
    expect(me?.reason).toBe("usage-limit");
    expect(me?.subtype).toBe("quota");
    expect(me?.nextAction).toBe("usage-notify");
    expect(me?.evidence[0]).toContain("chat failure: quota");
    expect(me?.recipe).toContain("wait for the limit window");
    // The existing usage-limit notify path gets a recorded signal too.
    const limits = await rt.stalls.reportLimits(swarmId);
    expect(limits.length).toBe(1);
    expect(limits[0]!.signal).toContain("Insufficient quota");
  });

  test("rate-limit message -> usage-limit rate", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("fd-rate");
    lastMessage(workerSessionId, "assistant", [{ type: "error", text: "provider 429: rate limit hit for model deepseek-v4-flash" }]);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 30_000 });
    const report = await rt.stalls.diagnose(swarmId);
    const me = report.members.find((x) => x.memberName === "worker");
    expect(me?.reason).toBe("usage-limit");
    expect(me?.subtype).toBe("rate");
  });
});

describe("d. notifyProviderError dedup (ladder passes)", () => {
  test("two ladder passes on the same failure -> ONE coordinator notification", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("fd-dedup");
    lastMessage(workerSessionId, "assistant", [
      { type: "error", text: "Upstream request failed: [invalid_bearer_credential] Missing or invalid bearer credential" },
    ], 60_000);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 60_000 });

    await rt.stalls.executeNext(swarmId);
    await rt.stalls.executeNext(swarmId);
    await rt.stalls.executeNext(swarmId);

    const text = await chatFailureDigest(swarmId);
    const chatFailureLines = text.split("\n").filter((l) => l.includes("[CHAT FAILURE]"));
    expect(chatFailureLines.length).toBe(1);
    expect(chatFailureLines[0]).toContain("member 'worker' hit auth");
    expect(chatFailureLines[0]).toContain("remedy: check provider credentials");
  });

  test("distinct subtypes notify independently (auth via CHAT FAILURE, quota via USAGE LIMIT)", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("fd-subtypes");
    lastMessage(workerSessionId, "assistant", [
      { type: "error", text: "Upstream request failed: [invalid_bearer_credential] Missing or invalid bearer credential" },
    ], 120_000);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 120_000 });
    await rt.stalls.executeNext(swarmId);

    // Same member now hits a quota failure: reason flips to usage-limit, which
    // routes through the EXISTING usage-notify path ([USAGE LIMIT] message) —
    // the provider-error [CHAT FAILURE] lane stays deduped per subtype.
    lastMessage(workerSessionId, "assistant", [{ type: "text", text: "Insufficient quota for model deepseek-v4-flash" }], 30_000);
    await rt.stalls.executeNext(swarmId);

    // Both markers land in the same flushed digest (distinct channels kept).
    const text = await chatFailureDigest(swarmId);
    const chatFailureLines = text.split("\n").filter((l) => l.includes("[CHAT FAILURE]"));
    expect(chatFailureLines.length).toBe(1);
    expect(chatFailureLines[0]).toContain("hit auth");
    const usageLimitLines = text.split("\n").filter((l) => l.includes("[USAGE LIMIT]"));
    expect(usageLimitLines.length).toBe(1);
    expect(usageLimitLines[0]).toContain("Insufficient quota");
  });
});

describe("e. session.error event classification", () => {
  test("session.error with an auth failure notifies the coordinator once (deduped)", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId } = await makeSwarmWithWorker("fd-sesserr");
    const evt = {
      type: "session.error",
      properties: {
        sessionID: workerSessionId,
        error: "Upstream request failed: [invalid_bearer_credential] Missing or invalid bearer credential",
      },
    };
    await handleOpenCodeEvent(rt, evt as never);
    await handleOpenCodeEvent(rt, evt as never); // same failure — deduped

    const text = await chatFailureDigest(swarmId);
    const chatFailureLines = text.split("\n").filter((l) => l.includes("[CHAT FAILURE]"));
    expect(chatFailureLines.length).toBe(1);
    expect(chatFailureLines[0]).toContain("member 'worker' hit auth");
    expect(chatFailureLines[0]).toContain("remedy: check provider credentials");
  });

  test("session.error with a properties.message variant is classified too", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId } = await makeSwarmWithWorker("fd-sesserr-msg");
    await handleOpenCodeEvent(rt, {
      type: "session.error",
      properties: { sessionID: workerSessionId, message: "Model not found: deepseek-v4-flash is not a valid model id" },
    } as never);
    const text = await chatFailureDigest(swarmId);
    const chatFailureLines = text.split("\n").filter((l) => l.includes("[CHAT FAILURE]"));
    expect(chatFailureLines.length).toBe(1);
    expect(chatFailureLines[0]).toContain("hit model-not-found");
    expect(chatFailureLines[0]).toContain("swarm_model(action: 'set'");
  });

  test("a non-chat-failure session.error (abort) does not notify [CHAT FAILURE]", async () => {
    await initPlugin();
    const { rt, swarmId, workerSessionId } = await makeSwarmWithWorker("fd-sesserr-abort");
    await handleOpenCodeEvent(rt, {
      type: "session.error",
      properties: { sessionID: workerSessionId, error: "AbortError: The operation was aborted" },
    } as never);
    const text = await chatFailureDigest(swarmId);
    expect(text.includes("[CHAT FAILURE]")).toBe(false);
  });
});

describe("f. swarm_stalls report rendering", () => {
  test("report renders provider-error with subtype + evidence + remedy", async () => {
    await initPlugin();
    const { swarmId, workerSessionId, workerMember, rt } = await makeSwarmWithWorker("fd-report");
    lastMessage(workerSessionId, "assistant", [
      { type: "error", text: "Upstream request failed: [invalid_bearer_credential] Missing or invalid bearer credential" },
    ]);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 30_000 });

    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("verdict: STALLED");
    expect(out).toContain("causes: provider-error");
    expect(out).toContain("provider-error (auth)");
    expect(out).toContain("chat failure: auth");
    expect(out).toContain("invalid_bearer_credential");
    expect(out).toContain("resolve: check provider credentials");
    expect(out).toContain("provider-error-notify");
  });

  test("report renders quota chat failures under usage-limit with the limit signal", async () => {
    await initPlugin();
    const { swarmId, workerSessionId, workerMember, rt } = await makeSwarmWithWorker("fd-report-quota");
    lastMessage(workerSessionId, "assistant", [{ type: "text", text: "Insufficient quota for model deepseek-v4-flash" }]);
    await rt.store.updateMemberStatus(workerMember.id, "working", { lastActiveAt: Date.now() - 30_000 });

    const out = await reportFor(swarmId, workerSessionId);
    expect(out).toContain("usage-limit (quota)");
    expect(out).toContain("chat failure: quota");
    expect(out).toContain("USAGE LIMITS");
    expect(out).toContain("Insufficient quota");
  });
});

describe("g. message text extraction helpers", () => {
  test("lastAssistantMessage skips trailing user turns", () => {
    const msgs = [
      { id: "u1", role: "user" as const, createdAt: 1, parts: [] },
      { id: "a1", role: "assistant" as const, createdAt: 2, parts: [] },
      { id: "u2", role: "user" as const, createdAt: 3, parts: [] },
    ];
    expect(lastAssistantMessage(msgs)?.id).toBe("a1");
    expect(lastAssistantMessage([])).toBeUndefined();
    expect(lastAssistantMessage(undefined)).toBeUndefined();
  });

  test("extractMessageFailureText joins text/error/reasoning parts only", () => {
    const msg = {
      id: "a1",
      role: "assistant" as const,
      createdAt: 1,
      parts: [
        { type: "tool", text: "ignored" },
        { type: "text", text: "first" },
        { type: "error", text: "second" },
        { type: "reasoning", text: "third" },
      ],
    };
    expect(extractMessageFailureText(msg)).toBe("first\nsecond\nthird");
    expect(extractMessageFailureText({ ...msg, parts: [] })).toBeUndefined();
    expect(extractMessageFailureText(undefined)).toBeUndefined();
  });
});
