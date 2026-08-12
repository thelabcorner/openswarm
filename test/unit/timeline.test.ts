import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { Permission } from "@opencode-ai/sdk";
import { renderTimeline } from "../../src/messaging/timeline.ts";
import type { SwarmEvent, SwarmMember, SwarmTask } from "../../src/core/types.ts";

/**
 * Swarm timeline / replay tests (task t-timeline): event recording fires on the
 * wired paths (send / reply / spawn / task-complete / permission-ask) via the
 * plugin-tool harness; renderTimeline emits readable human sentences with
 * chronological (newest last) ordering and time-bucket grouping; swarm_status
 * detail:"timeline" surfaces the recorded events; since/limit are respected.
 */

let dir: string;
let hooks: Hooks;
let tool: Record<string, any>;

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
      const sessionID = `ses-tl-${Math.random().toString(36).slice(2, 8)}`;
      if (opts.body?.parentID !== undefined) {
        throw new Error("session.create received a parentID; members must be root sessions");
      }
      return { data: { id: sessionID, title: opts.body?.title, parentID: undefined, directory: "." }, error: undefined };
    },
    get: async () => ({ data: null, error: undefined }),
    children: async () => ({ data: [], error: undefined }),
    messages: async () => ({ data: [], error: undefined }),
    status: async () => ({ data: {}, error: undefined }),
    abort: async () => ({ data: undefined, error: undefined }),
    update: async () => ({ data: {}, error: undefined }),
    prompt: async () => ({ data: { info: {} }, error: undefined }),
    promptAsync: async () => ({ data: undefined, error: undefined }),
    postSessionIdPermissionsPermissionId: async () => ({ data: { ok: true }, error: undefined }),
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-tl" },
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

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-tl-test-"));
  hooks = await swarmPlugin(pluginInput, { dataDir: dir });
  tool = hooks.tool ?? {};
});

afterAll(async () => {
  disposeSwarmRuntime();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

async function runtime() {
  const mod = await import("../../src/plugin.ts");
  return mod.swarmRuntime();
}

/** Create a swarm (optionally with a seeded task) + spawn a worker member. */
async function makeSwarm(name: string, workerName = "worker", task?: { id: string; title: string }) {
  const coordSession = `ses-tl-lead-${Math.random().toString(36).slice(2, 8)}`;
  const createRes = await tool.swarm_create.execute(
    { name, tasks: task ? [{ id: task.id, title: task.title }] : undefined },
    ctx(coordSession),
  );
  const created = JSON.parse(String(createRes.output ?? createRes));
  const swarmId = created.swarm.id as string;
  const spawnRes = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name: workerName, role: "impl" }] },
    ctx(coordSession),
  );
  const spawned = JSON.parse(String(spawnRes.output ?? spawnRes));
  const workerSessionId = spawned.spawned[0].sessionId as string;
  const rt = await runtime();
  const workerMember = await rt!.store.getMemberBySessionId(workerSessionId);
  return { rt, swarmId, coordSession, workerSessionId, workerMember };
}

/** Event types recorded in a swarm, in stream order. */
async function eventTypes(rt: any, swarmId: string): Promise<string[]> {
  const events = await rt!.store.listEvents(swarmId, { limit: 200 });
  return events.map((e: SwarmEvent) => e.type);
}

describe("a. event recording fires on the wired paths (plugin-tool harness)", () => {
  test("member.spawned + message.sent + message.replied", async () => {
    const { rt, swarmId, coordSession, workerSessionId } = await makeSwarm("tl-send");

    // member.spawned — recorded by core.spawnMember.
    expect(await eventTypes(rt, swarmId)).toContain("member.spawned");
    const spawned = (await rt!.store.listEvents(swarmId, { limit: 200 })).find((e: SwarmEvent) => e.type === "member.spawned");
    expect(spawned).toBeDefined();
    expect(JSON.parse(spawned!.payloadJson!)).toMatchObject({ name: "worker", role: "impl" });

    // message.sent — already wired in core.sendMessage.
    const sendRes = await tool.swarm_message.execute(
      { swarmId, to: "worker", kind: "message", message: "hello from coordinator" },
      ctx(coordSession),
    );
    const sent = JSON.parse(String(sendRes.output ?? sendRes));
    const msgId = sent.messages[0].id as string;
    const sentEvents = (await rt!.store.listEvents(swarmId, { limit: 200 })).filter((e: SwarmEvent) => e.type === "message.sent");
    expect(sentEvents.length).toBeGreaterThanOrEqual(1);
    const lastSent = sentEvents[sentEvents.length - 1]!;
    expect(JSON.parse(lastSent.payloadJson!)).toMatchObject({ to: "worker" });
    expect(lastSent.entityId).toBe(msgId);

    // message.replied — the worker replies to the coordinator's message.
    await tool.swarm_reply.execute(
      { swarmId, toMessageId: msgId, message: "got it" },
      ctx(workerSessionId),
    );
    const replyEvents = (await rt!.store.listEvents(swarmId, { limit: 200 })).filter((e: SwarmEvent) => e.type === "message.replied");
    expect(replyEvents.length).toBe(1);
    expect(replyEvents[0]!.entityId).toBe(msgId); // points at the ORIGINAL message
  });

  test("task.completed (terminal transition via swarm_tasks)", async () => {
    const { rt, swarmId, coordSession } = await makeSwarm("tl-task", "worker", { id: "t-tl", title: "build the thing" });

    const res = await tool.swarm_tasks.execute(
      { swarmId, action: "complete", taskId: "t-tl" },
      ctx(coordSession),
    );
    expect(String(res.output ?? res)).toContain("complete");

    const done = (await rt!.store.listEvents(swarmId, { limit: 200 })).find((e: SwarmEvent) => e.type === "task.completed");
    expect(done).toBeDefined();
    expect(done!.entityId).toBe("t-tl");
    // The coordinator is the actor (only owner or coordinator may complete).
    expect(JSON.parse(done!.payloadJson!)).toHaveProperty("memberId");
  });

  test("permission.asked — a worker ask left 'ask' records a permission wall", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarm("tl-perm");

    const out = { status: "ask" as const };
    await hooks["permission.ask"]!(
      {
        id: "perm-tl-1",
        type: "bash",
        pattern: "*",
        sessionID: workerSessionId,
        title: "run everything",
        messageID: "msg-perm-tl-1",
        metadata: {},
        time: { created: Date.now() },
      } as Permission,
      out,
    );
    expect(out.status).toBe("ask");

    const asked = (await rt!.store.listEvents(swarmId, { limit: 200 })).find((e: SwarmEvent) => e.type === "permission.asked");
    expect(asked).toBeDefined();
    expect(asked!.actorMemberId).toBe(workerMember!.id);
    expect(JSON.parse(asked!.payloadJson!)).toMatchObject({ memberId: workerMember!.id, type: "bash" });
    expect(asked!.entityId).toBe("perm-tl-1");
  });
});

describe("b. renderTimeline — readable sentences + chronological ordering + buckets", () => {
  const now = Date.now();
  const members: SwarmMember[] = [
    { id: "mem_a", swarmId: "sw", name: "alice", role: "coordinator", sessionId: "s1", status: "idle", workspaceMode: "shared-read", createdAt: now, updatedAt: now },
    { id: "mem_b", swarmId: "sw", name: "bob", role: "impl", sessionId: "s2", status: "idle", workspaceMode: "shared-read", createdAt: now, updatedAt: now },
  ];
  const tasks: SwarmTask[] = [
    { id: "t-1", swarmId: "sw", title: "build the widget", status: "completed", priority: 0, createdByMemberId: "mem_a", createdAt: now, updatedAt: now },
  ];
  const ev = (id: number, type: string, createdAt: number, extra?: Partial<SwarmEvent>): SwarmEvent => ({
    id,
    swarmId: "sw",
    type,
    createdAt,
    ...extra,
  });

  test("human sentences are readable per type; ordering is newest LAST", () => {
    const events: SwarmEvent[] = [
      ev(1, "member.spawned", now - 5_000, { actorMemberId: "mem_a", entityType: "member", entityId: "mem_b", payloadJson: JSON.stringify({ name: "bob", role: "impl" }) }),
      ev(2, "message.sent", now - 4_000, { actorMemberId: "mem_a", entityType: "message", entityId: "msg_111", payloadJson: JSON.stringify({ kind: "message", to: "bob", recipients: 1 }) }),
      ev(3, "task.claimed", now - 3_000, { actorMemberId: "mem_b", entityType: "task", entityId: "t-1", payloadJson: JSON.stringify({ memberId: "mem_b" }) }),
      ev(4, "task.completed", now - 2_000, { actorMemberId: "mem_b", entityType: "task", entityId: "t-1", payloadJson: JSON.stringify({ memberId: "mem_b" }) }),
      ev(5, "permission.asked", now - 1_000, { actorMemberId: "mem_b", entityType: "permission", entityId: "perm-9", payloadJson: JSON.stringify({ memberId: "mem_b", type: "bash" }) }),
    ];
    const out = renderTimeline(events, members, tasks, { now });
    // Every line readable + fenced payloads present.
    expect(out).toContain("spawned member 'bob'");
    expect(out).toContain("alice sent a message to bob");
    expect(out).toContain("bob claimed task t-1");
    expect(out).toContain("bob completed task t-1");
    expect(out).toContain("bob hit a permission wall (bash)");
    expect(out).toContain("[DATA");
    // Chronological (newest last): the newest event (permission.asked) is on
    // the LAST event line; all these events fall in the 'just now' bucket.
    const lines = out.split("\n");
    const eventLines = lines.filter((l) => /^\s+\d\d:\d\d:\d\d/.test(l));
    expect(eventLines[0]).toContain("member.spawned");
    expect(eventLines[eventLines.length - 1]).toContain("permission.asked");
    expect(out).toContain("[just now]");
  });

  test("time-bucket headers + limit caps to the NEWEST events", () => {
    const events: SwarmEvent[] = [
      ev(1, "member.spawned", now - 3 * 86_400_000, { actorMemberId: "mem_a", entityType: "member", entityId: "mem_b", payloadJson: JSON.stringify({ name: "bob", role: "impl" }) }),
      ev(2, "message.sent", now - 3_600_000, { actorMemberId: "mem_a", entityType: "message", entityId: "msg_1", payloadJson: JSON.stringify({ kind: "message", to: "bob", recipients: 1 }) }),
      ev(3, "task.completed", now - 60_000, { actorMemberId: "mem_b", entityType: "task", entityId: "t-1", payloadJson: JSON.stringify({ memberId: "mem_b" }) }),
      ev(4, "permission.asked", now - 500, { actorMemberId: "mem_b", entityType: "permission", entityId: "perm-9", payloadJson: JSON.stringify({ memberId: "mem_b", type: "bash" }) }),
    ];
    const full = renderTimeline(events, members, tasks, { now });
    expect(full).toContain("[3 days ago]");
    expect(full).toContain("[1 min ago]");
    expect(full).toContain("[just now]");

    // limit: only the NEWEST `limit` events render.
    const capped = renderTimeline(events, members, tasks, { now, limit: 2 });
    expect(capped).toContain("permission.asked");
    expect(capped).toContain("task.completed");
    expect(capped).not.toContain("member.spawned");
    expect(capped).not.toContain("message.sent");
  });

  test("empty stream renders a placeholder", () => {
    expect(renderTimeline([], members, tasks, { now })).toContain("no events recorded yet");
  });

  test("peer event types (blackboard.write, deliverable.verdict) get readable sentences", () => {
    const events: SwarmEvent[] = [
      ev(1, "blackboard.write", now - 3_000, { actorMemberId: "mem_a", entityType: "blackboard", entityId: "contracts/foo", payloadJson: JSON.stringify({ version: 2, authorMemberId: "mem_a" }) }),
      ev(2, "deliverable.verdict", now - 1_000, { actorMemberId: "mem_a", entityType: "deliverable", entityId: "dlv_123", payloadJson: JSON.stringify({ verdict: "accepted" }) }),
    ];
    const out = renderTimeline(events, members, tasks, { now });
    expect(out).toContain("alice wrote blackboard key 'contracts/foo' (v2)");
    expect(out).toContain("alice marked deliverable dlv_123 as accepted");
    // Unknown types fall back to a readable generic line (never crash).
    const unknown = renderTimeline([ev(3, "mystery.event", now - 500, { actorMemberId: "mem_b" })], members, tasks, { now });
    expect(unknown).toContain("bob — mystery.event");
  });
});

describe("c. swarm_status detail:timeline — entries + since/limit respected", () => {
  test("detail:timeline renders the recorded stream with sentences", async () => {
    const { rt, swarmId, coordSession, workerSessionId } = await makeSwarm("tl-status");
    await tool.swarm_message.execute(
      { swarmId, to: "worker", kind: "message", message: "status check" },
      ctx(coordSession),
    );
    await tool.swarm_reply.execute(
      { swarmId, toMessageId: (await rt!.store.listMessagesBySwarm(swarmId, 1))[0]!.id, message: "all good" },
      ctx(workerSessionId),
    );

    const res = await tool.swarm_status.execute({ swarmId, detail: "timeline" }, ctx(coordSession));
    const out = String(res.output ?? res);
    expect(out).toContain("TIMELINE");
    expect(out).toContain("member.spawned");
    expect(out).toContain("spawned member 'worker'");
    expect(out).toContain("message.sent");
    expect(out).toContain("message.replied");
    expect(out).toContain("[just now]");
  });

  test("since filters out older events; limit caps the rendered count", async () => {
    const { rt, swarmId, coordSession } = await makeSwarm("tl-since");
    // Two synthetic events with DISTINCT backdates + unique entity ids so the
    // since/limit filtering is deterministic (the real member.spawned from
    // makeSwarm is recent and stays in the stream — we filter on OUR ids).
    const now = Date.now();
    await rt!.store.insertEvent({
      swarmId,
      type: "task.completed",
      actorMemberId: undefined,
      entityType: "task",
      entityId: "t-old-since",
      payloadJson: JSON.stringify({}),
      createdAt: now - 60_000,
    });
    await rt!.store.insertEvent({
      swarmId,
      type: "task.cancelled",
      actorMemberId: undefined,
      entityType: "task",
      entityId: "t-recent-since",
      payloadJson: JSON.stringify({}),
      createdAt: now - 10_000,
    });

    // since: only events after now-30s — the recent synthetic renders, the
    // old one is filtered out.
    const res = await tool.swarm_status.execute(
      { swarmId, detail: "timeline", since: now - 30_000 },
      ctx(coordSession),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("t-recent-since");
    expect(out).not.toContain("t-old-since");

    // limit: only the NEWEST event renders.
    const capped = await tool.swarm_status.execute(
      { swarmId, detail: "timeline", limit: 1 },
      ctx(coordSession),
    );
    const out2 = String(capped.output ?? capped);
    const eventLines = out2.split("\n").filter((l) => /^\s+\d\d:\d\d:\d\d/.test(l));
    expect(eventLines.length).toBe(1);
  });
});
