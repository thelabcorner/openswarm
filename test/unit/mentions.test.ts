import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import { formatEnvelope } from "../../src/messaging/formatter.ts";
import { extractAllMentionTokens, extractFileMentions, extractMemberMentions, extractTaskMentions } from "../../src/messaging/mentions.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * @mention feature (rich-agent-mentions): extraction, auto-notify delivery in
 * sendMessage/replyToMessage, envelope `mentions:` hint, and tool-output
 * resolution (resolved/unresolved buckets for @name / @file:path / #task).
 */

let dirs: string[] = [];

function makeClient() {
  return {
    config: {
      providers: async () => ({
        data: { providers: [{ id: "opencode-go", models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash (2x usage)" } } }] },
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
  project: { id: "proj-mentions" },
  directory: ".",
  worktree: ".",
  experimental_workspace: { register() {} },
  serverUrl: new URL("http://x"),
  $: {},
});

function ctx(sessionID: string, directory = "."): any {
  return {
    sessionID,
    messageID: "msg-call",
    agent: "build",
    directory,
    worktree: directory,
    abort: new AbortController().signal,
    metadata() {},
    ask: () => {},
  };
}

let hooks: Hooks | undefined;
let tool: Record<string, any>;

async function initPlugin(): Promise<string> {
  disposeSwarmRuntime();
  const dir = mkdtempSync(join(tmpdir(), "swarms-mentions-"));
  dirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient()), { dataDir: dir });
  tool = hooks.tool ?? {};
  return dir;
}

function rt() {
  const r = swarmRuntime();
  if (!r) throw new Error("swarm runtime not initialized");
  return r;
}

async function createSwarm(name: string, directory = "."): Promise<string> {
  const created = await tool.swarm_create.execute({ name }, ctx("ses-lead", directory));
  return JSON.parse(String(created.output ?? created)).swarm.id;
}

async function spawnWorker(swarmId: string, name: string): Promise<{ memberId: string; sessionId: string }> {
  const res = await tool.swarm_spawn.execute({ swarmId, members: [{ name, role: "worker" }] }, ctx("ses-lead"));
  return JSON.parse(String(res.output ?? res)).spawned[0];
}

async function send(swarmId: string, from: string, to: string, message: string, extra: Record<string, unknown> = {}) {
  const res = await tool.swarm_message.execute({ swarmId, to, message, ...extra }, ctx(from));
  return JSON.parse(String(res.output ?? res));
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe("extractors (pure)", () => {
  test("extractMemberMentions matches member names case-insensitively, excludes @file:", () => {
    const names = ["writer-a", "Reviewer", "q"];
    expect(extractMemberMentions("ask @writer-a and @WRITER-A please", names)).toEqual(["writer-a"]);
    expect(extractMemberMentions("cc @reviewer", names)).toEqual(["Reviewer"]);
    expect(extractMemberMentions("see @file:src/x.ts and @writer-a", names)).toEqual(["writer-a"]);
    expect(extractMemberMentions("@unknown", names)).toEqual([]);
    expect(extractMemberMentions("no mentions", names)).toEqual([]);
  });

  test("extractFileMentions and extractTaskMentions", () => {
    expect(extractFileMentions("look at @file:src/wire.ts and @file:README.md")).toEqual(["src/wire.ts", "README.md"]);
    expect(extractTaskMentions("blocked on #t1 and #build-kernel")).toEqual(["t1", "build-kernel"]);
  });

  test("extractAllMentionTokens reports unresolved tokens too", () => {
    expect(extractAllMentionTokens("ping @writer-a and @nobody")).toEqual(["writer-a", "nobody"]);
  });
});

describe("mention auto-notify in sendMessage", () => {
  test("mentioning a non-recipient member ALSO delivers the message to them", async () => {
    await initPlugin();
    const swarmId = await createSwarm("m-send");
    const { sessionId: aliceS } = await spawnWorker(swarmId, "alice");
    const { memberId: bobId } = await spawnWorker(swarmId, "bob");
    const { memberId: carolId } = await spawnWorker(swarmId, "carol");
    const res = await send(swarmId, aliceS, "bob", "draft done, please review @carol");
    // Message rows go to the explicit recipient (bob) AND the mentioned
    // member (carol). Delivery may be queued by the per-member cooldown, so
    // assert on the message targets, not just the delivered subset.
    expect(res.messages.length).toBe(2);
    const ids = res.messages.map((m: any) => m.to.memberId);
    expect(ids).toContain(bobId);
    expect(ids).toContain(carolId);
    expect([...(res.deliveredTo ?? []), ...(res.pendingFor ?? [])]).toContain("carol");
  });

  test("mention resolution is reported in the output (resolved/unresolved)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("m-resolve");
    const { sessionId } = await spawnWorker(swarmId, "writer-a");
    await spawnWorker(swarmId, "reviewer");
    const res = await send(swarmId, sessionId, "reviewer", "ping @writer-a and @ghost and #t1", {});
    expect(res.mentions.mentionedResolved).toContain("writer-a");
    expect(res.mentions.mentionedUnresolved).toContain("ghost");
    expect(res.mentions.tasksUnresolved).toContain("t1");
  });

  test("mention of a stopped member does not deliver to them", async () => {
    await initPlugin();
    const swarmId = await createSwarm("m-stopped");
    const { memberId, sessionId } = await spawnWorker(swarmId, "writer-a");
    const { memberId: reviewerId } = await spawnWorker(swarmId, "reviewer");
    await rt().core.store.updateMemberStatus(memberId, "stopped", { currentTaskId: null, lastActiveAt: Date.now() });
    const res = await send(swarmId, sessionId, "reviewer", "cc @writer-a", {});
    expect(res.messages.length).toBe(1);
    expect(res.messages[0].to.memberId).toBe(reviewerId);
  });

  test("mention of the primary recipient does not duplicate delivery", async () => {
    await initPlugin();
    const swarmId = await createSwarm("m-dup");
    const { sessionId } = await spawnWorker(swarmId, "writer-a");
    await spawnWorker(swarmId, "reviewer");
    const res = await send(swarmId, sessionId, "reviewer", "hi @reviewer", {});
    expect(res.messages.length).toBe(1);
  });

  test("self-mention never delivers to the sender (loop-safety invariant)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("m-selfment");
    const { sessionId } = await spawnWorker(swarmId, "alice");
    const { memberId: bobId } = await spawnWorker(swarmId, "bob");
    // Sender mentions THEMSELVES (e.g. a confused agent echoing its own name).
    const res = await send(swarmId, sessionId, "bob", "per @alice note this");
    expect(res.messages.length).toBe(1);
    expect(res.messages[0].to.memberId).toBe(bobId);
  });
});

describe("mention auto-notify in replyToMessage", () => {
  test("replying with a mention delivers the reply to the mentioned member too", async () => {
    await initPlugin();
    const swarmId = await createSwarm("m-reply");
    const { memberId: aliceId, sessionId: aliceS } = await spawnWorker(swarmId, "alice");
    const { sessionId: bobS } = await spawnWorker(swarmId, "bob");
    const { memberId: carolId } = await spawnWorker(swarmId, "carol");
    // alice -> bob
    const sent = await send(swarmId, aliceS, "bob", "question for you");
    const reqId = sent.messages[0].id;
    // bob replies, mentioning carol -> the reply goes to alice (original
    // sender) AND carol (mentioned).
    const reply = await tool.swarm_reply.execute({ swarmId, toMessageId: reqId, message: "answer, cc @carol" }, ctx(bobS));
    const replyJson = JSON.parse(String(reply.output ?? reply));
    const targets = replyJson.delivered.map((m: any) => m.to.memberId);
    expect(targets).toContain(aliceId);
    expect(targets).toContain(carolId);
    expect(targets.length).toBe(2);
  });
});

describe("file and task mention resolution", () => {
  test("@file:path resolves against the swarm worktree; missing files are unresolved", async () => {
    const dataDir = await initPlugin();
    // Root the swarm in the dataDir so the worktree path matches.
    const swarmId = await createSwarm("m-file", dataDir);
    const { sessionId } = await spawnWorker(swarmId, "writer-a");
    await spawnWorker(swarmId, "reviewer");
    writeFileSync(join(dataDir, "wire.ts"), "export const wire = 1;");
    const res = await send(swarmId, sessionId, "reviewer", "see @file:wire.ts and @file:missing.ts", {});
    expect(res.mentions.filesResolved).toContain("wire.ts");
    expect(res.mentions.filesUnresolved).toContain("missing.ts");
  });

  test("#task resolves to id + title", async () => {
    await initPlugin();
    const swarmId = await createSwarm("m-task");
    const { sessionId } = await spawnWorker(swarmId, "writer-a");
    const coord = await rt().core.store.getMemberBySessionId("ses-lead");
    const task = await rt().core.createTask({ swarmId, id: "t1", title: "build kernel", createdByMemberId: coord!.id });
    await rt().core.store.updateTaskStatus(task.id, "ready");
    await spawnWorker(swarmId, "reviewer");
    const res = await send(swarmId, sessionId, "reviewer", "work on #t1", {});
    // The scheduler may have already assigned the task, so only assert the
    // id/title resolution, not a specific status.
    expect(res.mentions.tasksResolved).toContainEqual(expect.objectContaining({ id: "t1", title: "build kernel" }));
  });
});

describe("envelope mentions hint", () => {
  test("formatEnvelope renders a mentions line when the body references members", () => {
    const m = {
      id: "m1", swarmId: "s", fromMemberId: "mem-a",
      to: { type: "member", memberId: "mem-b" },
      kind: "message", priority: "normal",
      body: { text: "ping @writer-a" },
      deliveryState: "queued", attemptCount: 0, createdAt: 0,
    } as any;
    const names = new Map([["mem-a", "alice"], ["mem-b", "bob"], ["mem-c", "writer-a"]]);
    const out = formatEnvelope(m, names);
    expect(out).toContain("mentions: writer-a");
  });

  test("formatEnvelope omits the mentions line when nothing is referenced", () => {
    const m = {
      id: "m1", swarmId: "s", fromMemberId: "mem-a",
      to: { type: "member", memberId: "mem-b" },
      kind: "message", priority: "normal",
      body: { text: "plain body" },
      deliveryState: "queued", attemptCount: 0, createdAt: 0,
    } as any;
    const out = formatEnvelope(m, new Map([["mem-a", "alice"], ["mem-b", "bob"]]));
    expect(out).not.toContain("mentions:");
  });
});
