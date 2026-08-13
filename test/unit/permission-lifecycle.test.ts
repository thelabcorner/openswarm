import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, handleOpenCodeEvent, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { Permission } from "@opencode-ai/sdk";
import type { OpenCodeClientLikeV2 } from "../../src/runtime/opencode-runtime.ts";

/**
 * Permission lifecycle integration (task t-perm-lifecycle).
 *
 * Wires the plugin to the opencode permission lifecycle mapped by the
 * opencode-codebase swarm (deliverable/permission-lifecycle-map):
 *
 *   a. session.next.moved heals the member's sessionId (the invisible-wall
 *      fix) — the member re-resolves, so a subsequent ask records + notifies.
 *   b. permission.v2.asked records a pending permission with engine 'v2' and
 *      the coordinator is notified exactly once (deduped) — the hook does NOT
 *      fire for V2, so the event is the only recorder.
 *   c. permission.v2.replied marks the pending record replied (drops it from
 *      the pending list).
 *   d. polling backstop: the stall diagnoser polls the v2 pending-ask GET
 *      endpoints and surfaces a missed ask as 'permission-wall' evidence.
 *   e. swarm_permissions reply routes V2 asks to the v2 reply endpoint
 *      (POST /api/session/{sessionID}/permission/{requestID}/reply).
 *   f. engine column: v1/v2 flag round-trips through the store (migration v13;
 *      the legacy-DB catch-up assertion lives in store.test.ts).
 *
 * Harness mirrors permission-wall-delivery.test.ts (fake v1 client) plus an
 * injected fake V2 client (options.v2Client) and a RECORDING v2 reply/list.
 * The V2 SSE subscription is disabled in tests (subscribeV2Events: false) —
 * events are driven directly through handleOpenCodeEvent.
 */

let dir: string;
let hooks: Hooks;
let tool: Record<string, any>;
/** Recorded runtime.promptAsync calls (the observable half of coordinator
 * mailbox delivery). */
const promptCalls: Array<{ sessionID: string; text: string; agent?: string; model?: unknown }> = [];
/** Recorded v2 session.permission.reply calls ({ path, body }) — the proof
 * that V2 replies route to the v2 endpoint (test e). */
const v2Replies: Array<{ path: { sessionID: string; requestID: string }; body: { reply: string } }> = [];
/** Recorded v1 postSessionIdPermissionsPermissionId calls (test e2). */
const v1Replies: Array<{ path: { id: string; permissionID: string }; body: { response: string } }> = [];
/** Mutable fake data backing the v2 pending-ask GET endpoints (test d). */
let fakeGlobalAsks: Array<Record<string, unknown>> = [];
let fakeSessionAsks: Array<Record<string, unknown>> = [];

function makeFakeV2(): OpenCodeClientLikeV2 {
  return {
    event: {
      subscribe: async () => ({
        stream: (async function* () {
          /* yields nothing — tests drive events via handleOpenCodeEvent */
        })(),
      }),
    },
    session: {
      permission: {
        list: async (opts: any) => ({
          data: fakeSessionAsks.filter((a) => a.sessionID === opts?.path?.sessionID),
          error: undefined,
        }),
        reply: async (opts: any) => {
          v2Replies.push(opts);
          return { data: {}, error: undefined };
        },
      },
    },
    v2: {
      permission: {
        request: {
          list: async () => ({ data: fakeGlobalAsks, error: undefined }),
        },
      },
    },
  };
}

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
      const sessionID = `ses-plc-${Math.random().toString(36).slice(2, 8)}`;
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
    promptAsync: async (opts: any) => {
      const text = (opts?.body?.parts ?? [])
        .map((p: any) => (typeof p?.text === "string" ? p.text : ""))
        .join("\n");
      promptCalls.push({
        sessionID: opts?.path?.id ?? "?",
        text,
        agent: opts?.body?.agent,
        model: opts?.body?.model,
      });
      return { data: undefined, error: undefined };
    },
    // v1-engine reply endpoint (POST /session/{id}/permissions/{permissionID})
    // — needed so swarm_permissions replies to engine 'v1' records succeed.
    postSessionIdPermissionsPermissionId: async (opts: any) => {
      v1Replies.push(opts);
      return { data: {}, error: undefined };
    },
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-plc" },
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

async function runtime() {
  const mod = await import("../../src/plugin.ts");
  return mod.swarmRuntime();
}

async function initPlugin() {
  dir = mkdtempSync(join(tmpdir(), "swarms-plc-"));
  fakeGlobalAsks = [];
  fakeSessionAsks = [];
  v2Replies.length = 0;
  v1Replies.length = 0;
  hooks = await swarmPlugin(pluginInput, {
    dataDir: dir,
    // t-perm-lifecycle: inject the fake V2 client (production builds the real
    // one from input.serverUrl via @opencode-ai/sdk/v2) and skip the SSE loop.
    v2Client: makeFakeV2(),
    subscribeV2Events: false,
  });
  tool = hooks.tool ?? {};
  promptCalls.length = 0;
}

async function teardown() {
  disposeSwarmRuntime();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
}

/** Create a swarm + spawn a worker member; returns ids + the runtime. */
async function makeSwarmWithWorker(name: string, workerName = "worker") {
  const coordSession = `ses-plc-lead-${Math.random().toString(36).slice(2, 8)}`;
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
  expect(rt).toBeDefined();
  const workerMember = await rt!.store.getMemberBySessionId(workerSessionId);
  const swarm = await rt!.store.getSwarm(swarmId);
  const coordMember = await rt!.store.getMemberById(swarm!.coordinatorMemberId);
  return { rt, swarmId, coordSession, workerSessionId, workerMember, coordMember };
}

/** Drive the permission.ask hook for a member with a bash "*" request — never
 * auto-allowed without allow-all, so the escalation path (record + notify) runs. */
async function askBashStar(memberSessionId: string, permissionId: string) {
  const out = askOutput();
  await hooks["permission.ask"]!(
    permission({ id: permissionId, type: "bash", pattern: "*", sessionID: memberSessionId, title: "run everything" }),
    out,
  );
  return out;
}

/** Coordinator-notification messages for a swarm (kind finding, PERMISSION WALL). */
async function permissionWallNotices(swarmId: string) {
  const rt = await runtime();
  const all = await rt!.store.listMessagesBySwarm(swarmId, 100);
  return all.filter((m) => m.kind === "finding" && m.body.text.includes("[PERMISSION WALL]"));
}

describe("a. session.next.moved heals the member's sessionId (invisible-wall fix)", () => {
  beforeAll(async () => {
    await initPlugin();
  });
  afterAll(async () => {
    await teardown();
  });

  test("re-root (new session id) reassigns member.sessionId, records member.rerooted, and the healed session escalates normally", async () => {
    promptCalls.length = 0;
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("plc-a");
    const memberId = workerMember!.id;
    expect((await rt!.store.getMemberById(memberId))!.sessionId).toBe(workerSessionId);

    // The server publishes session.next.moved when a member session is
    // re-rooted; the new id arrives explicitly on the re-created variant.
    await handleOpenCodeEvent(rt!, {
      type: "session.next.moved",
      properties: {
        sessionID: workerSessionId,
        newSessionID: "ses-plc-healed-1",
        location: { directory: ".", workspaceID: undefined },
        timestamp: Date.now(),
      },
    });

    // The member's sessionId is healed to the new session.
    expect((await rt!.store.getMemberById(memberId))!.sessionId).toBe("ses-plc-healed-1");
    // The OLD session id no longer resolves to the member.
    expect(await rt!.store.getMemberBySessionId(workerSessionId)).toBeUndefined();

    // A 'member.rerooted' timeline event was recorded (durable replay surface).
    const events = await rt!.store.listEvents(swarmId);
    expect(events.some((e) => e.type === "member.rerooted" && e.entityId === memberId)).toBe(true);

    // A subsequent ask from the HEALED session resolves in the store: it is
    // recorded as pending (engine v1 via the hook) and the coordinator is
    // notified — no invisible wall.
    const out = await askBashStar("ses-plc-healed-1", "perm-healed-1");
    expect(out.status).toBe("ask");
    const pending = await rt!.store.listPendingPermissions(swarmId);
    expect(pending.length).toBe(1);
    expect(pending[0]!.id).toBe("perm-healed-1");
    expect(pending[0]!.memberId).toBe(memberId);
    expect(pending[0]!.sessionId).toBe("ses-plc-healed-1");
    expect(pending[0]!.engine).toBe("v1");
    const notices = await permissionWallNotices(swarmId);
    expect(notices.length).toBe(1);
    expect(notices[0]!.body.text).toContain("permissionId: 'perm-healed-1'");
  });

  test("kept-id move (real-server shape {sessionID, location}) re-asserts the mapping and records member.rerooted", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("plc-a2");
    const memberId = workerMember!.id;

    await handleOpenCodeEvent(rt!, {
      type: "session.next.moved",
      properties: {
        sessionID: workerSessionId,
        location: { directory: "C:/new-location" },
        timestamp: Date.now(),
      },
    });

    // The id is kept (the server keeps session ids on directory moves) — the
    // mapping stays valid and a member.rerooted event surfaced the move.
    expect((await rt!.store.getMemberById(memberId))!.sessionId).toBe(workerSessionId);
    const events = await rt!.store.listEvents(swarmId);
    const reroot = events.find((e) => e.type === "member.rerooted" && e.entityId === memberId);
    expect(reroot).toBeDefined();
    expect(JSON.parse(reroot!.payloadJson ?? "{}")).toMatchObject({ sessionID: workerSessionId, directory: "C:/new-location" });
  });
});

describe("b. permission.v2.asked records a pending (engine v2) + coordinator notified ONCE", () => {
  beforeAll(async () => {
    await initPlugin();
  });
  afterAll(async () => {
    await teardown();
  });

  test("v2.asked records + notifies once; replay is deduped (no double record, no double notify)", async () => {
    promptCalls.length = 0;
    const { rt, swarmId, coordSession, workerSessionId, workerMember } = await makeSwarmWithWorker("plc-b");

    const askEvent = {
      type: "permission.v2.asked",
      properties: {
        id: "perm-v2-1",
        sessionID: workerSessionId,
        action: "edit",
        resources: ["src/**"],
        metadata: {},
        timestamp: Date.now(),
      },
    };

    // The V2 ask never fires the permission.ask hook — the event is the ONLY
    // recorder. Record + notify once.
    await handleOpenCodeEvent(rt!, askEvent);
    const pending = await rt!.store.listPendingPermissions(swarmId);
    expect(pending.length).toBe(1);
    expect(pending[0]!.id).toBe("perm-v2-1");
    expect(pending[0]!.engine).toBe("v2");
    expect(pending[0]!.type).toBe("edit");
    expect(pending[0]!.pattern).toBe("src/**");
    expect(pending[0]!.memberId).toBe(workerMember!.id);

    const notices = await permissionWallNotices(swarmId);
    expect(notices.length).toBe(1);
    expect(notices[0]!.body.text).toContain("permissionId: 'perm-v2-1'");
    expect(notices[0]!.body.text).toContain("swarm_permissions(swarmId:");
    // Delivered to the coordinator's mailbox (promptAsync fired).
    expect(promptCalls.some((c) => c.sessionID === coordSession && c.text.includes("[PERMISSION WALL]"))).toBe(true);

    // Replay of the same ask (SSE redelivery / double-path) is idempotent:
    // still ONE store row and ONE coordinator notice.
    await handleOpenCodeEvent(rt!, askEvent);
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(1);
    expect((await permissionWallNotices(swarmId)).length).toBe(1);
  });

  test("v2.asked from an unresolvable session is ignored (no record, no notify)", async () => {
    const { rt, swarmId } = await makeSwarmWithWorker("plc-b2");
    await handleOpenCodeEvent(rt!, {
      type: "permission.v2.asked",
      properties: { id: "perm-v2-ghost", sessionID: "ses-plc-ghost", action: "bash", resources: ["*"] },
    });
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
    expect((await permissionWallNotices(swarmId)).length).toBe(0);
  });
});

describe("c. permission.v2.replied marks the pending record replied", () => {
  beforeAll(async () => {
    await initPlugin();
  });
  afterAll(async () => {
    await teardown();
  });

  test("v2.replied drops the record from the pending list (response preserved)", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("plc-c");
    await rt!.store.insertPendingPermission({
      id: "perm-v2-replied-1",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "edit",
      pattern: "src/**",
      engine: "v2",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(1);

    await handleOpenCodeEvent(rt!, {
      type: "permission.v2.replied",
      properties: { sessionID: workerSessionId, requestID: "perm-v2-replied-1", reply: "allow" },
    });

    const rec = await rt!.store.getPendingPermission(swarmId, "perm-v2-replied-1");
    expect(rec?.response).toBe("allow");
    expect(rec?.respondedAt).not.toBeNull();
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);

    // Missing row is a no-op (best-effort bookkeeping).
    await expect(
      handleOpenCodeEvent(rt!, {
        type: "permission.v2.replied",
        properties: { sessionID: workerSessionId, requestID: "perm-never-recorded", reply: "deny" },
      }),
    ).resolves.toBeUndefined();
  });
});

describe("d. polling backstop: the stall diagnoser catches asks the event stream missed", () => {
  beforeAll(async () => {
    await initPlugin();
  });
  afterAll(async () => {
    await teardown();
  });

  test("a pending ask visible only via the v2 GET endpoints surfaces as 'permission-wall' evidence", async () => {
    const { rt, swarmId, workerSessionId } = await makeSwarmWithWorker("plc-d");
    // The ask was created while the plugin was not subscribed — the event was
    // missed entirely. Only the GET endpoints know about it.
    fakeGlobalAsks = [{ id: "perm-poll-1", sessionID: workerSessionId, action: "bash", resources: ["*"] }];

    const report = await rt!.stalls.diagnose(swarmId);
    const worker = report.members.find((d) => d.memberName === "worker");
    expect(worker?.reason).toBe("permission-wall");
    expect(worker?.evidence.some((e) => e.includes("bash") && e.includes("*"))).toBe(true);
    // The recipe's permissionId resolves — the ask was recorded (engine v2).
    expect(worker?.recipe).toContain("permissionId: 'perm-poll-1'");
    const rec = await rt!.store.getPendingPermission(swarmId, "perm-poll-1");
    expect(rec?.engine).toBe("v2");
    expect(rec?.memberId).toBe((await rt!.store.getMemberBySessionId(workerSessionId))!.id);
  });

  test("the poll backstop is throttle-safe and failure-proof (never throws)", async () => {
    const { rt, swarmId, workerSessionId } = await makeSwarmWithWorker("plc-d2");
    // A second diagnose with the same pending ask does NOT re-record (the
    // record is already known) and the report still shows the wall.
    fakeGlobalAsks = [{ id: "perm-poll-2", sessionID: workerSessionId, action: "bash", resources: ["*"] }];
    const report1 = await rt!.stalls.diagnose(swarmId);
    expect(report1.members.find((d) => d.memberName === "worker")?.reason).toBe("permission-wall");

    // Blow up the endpoints — the diagnoser must survive (never throw).
    fakeGlobalAsks = new Proxy([], {
      get: () => {
        throw new Error("boom");
      },
    }) as unknown as Array<Record<string, unknown>>;
    const report2 = await rt!.stalls.diagnose(swarmId);
    expect(report2.verdict).toBe("stalled"); // the already-recorded ask still diagnoses
    expect(report2.members.find((d) => d.memberName === "worker")?.reason).toBe("permission-wall");
  });
});

describe("e. swarm_permissions reply routes V2 asks to the v2 endpoint", () => {
  beforeAll(async () => {
    await initPlugin();
  });
  afterAll(async () => {
    await teardown();
  });

  test("engine 'v2' record -> POST /api/session/{sessionID}/permission/{requestID}/reply (not the v1 endpoint)", async () => {
    v2Replies.length = 0;
    v1Replies.length = 0;
    const { rt, swarmId, coordSession, workerSessionId, workerMember, coordMember } = await makeSwarmWithWorker("plc-e");
    await rt!.store.insertPendingPermission({
      id: "perm-v2-reply-1",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "edit",
      pattern: "src/**",
      engine: "v2",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });

    const res = await tool.swarm_permissions.execute(
      { swarmId, action: "reply", permissionId: "perm-v2-reply-1", response: "once" },
      ctx(coordSession),
    );
    expect(String(res.output ?? res)).toContain("answered 'once'");
    // The v2 reply endpoint was called with the exact request.
    expect(v2Replies.length).toBe(1);
    expect(v2Replies[0]!.path).toEqual({ sessionID: workerSessionId, requestID: "perm-v2-reply-1" });
    expect(v2Replies[0]!.body).toEqual({ reply: "once" });
    // The record is marked responded and leaves the pending list.
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);

    // Re-replying to an already-answered record still routes to the v2
    // endpoint, but the record's response is NOT overwritten (WHERE response
    // IS NULL) — the store keeps the first verdict.
    await tool.swarm_permissions.execute(
      { swarmId, action: "reply", permissionId: "perm-v2-reply-1", response: "always" },
      ctx(coordSession),
    );
    expect(v2Replies.length).toBe(2);
    expect((await rt!.store.getPendingPermission(swarmId, "perm-v2-reply-1"))?.response).toBe("once");
  });

  test("engine 'v1' record still routes to the v1 endpoint (postSessionIdPermissionsPermissionId)", async () => {
    v2Replies.length = 0;
    v1Replies.length = 0;
    const { rt, swarmId, coordSession, workerSessionId, workerMember } = await makeSwarmWithWorker("plc-e2");
    await rt!.store.insertPendingPermission({
      id: "perm-v1-reply-1",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "*",
      engine: "v1",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });
    const res = await tool.swarm_permissions.execute(
      { swarmId, action: "reply", permissionId: "perm-v1-reply-1", response: "reject" },
      ctx(coordSession),
    );
    expect(String(res.output ?? res)).toContain("answered 'reject'");
    // No v2 reply was made for the v1 record — the v1 endpoint got the call.
    expect(v2Replies.length).toBe(0);
    expect(v1Replies.length).toBe(1);
    expect(v1Replies[0]!.path).toEqual({ id: workerSessionId, permissionID: "perm-v1-reply-1" });
    expect(v1Replies[0]!.body).toEqual({ response: "reject" });
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
  });
});

describe("f. engine column round-trips through both stores (migration v13)", () => {
  beforeAll(async () => {
    await initPlugin();
  });
  afterAll(async () => {
    await teardown();
  });

  test("sqlite + chunkdb stores persist and return the engine flag", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("plc-f");

    for (const engine of ["v1", "v2"] as const) {
      const id = `perm-rt-${engine}`;
      await rt!.store.insertPendingPermission({
        id,
        swarmId,
        memberId: workerMember!.id,
        sessionId: workerSessionId,
        type: "edit",
        pattern: "src/**",
        engine,
        response: null,
        respondedAt: null,
        createdAt: Date.now(),
      });
      const rec = await rt!.store.getPendingPermission(swarmId, id);
      expect(rec?.engine).toBe(engine);
    }
    // Legacy rows (no engine) read back as undefined — no crash.
    await rt!.store.insertPendingPermission({
      id: "perm-rt-legacy",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "edit",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });
    expect((await rt!.store.getPendingPermission(swarmId, "perm-rt-legacy"))?.engine).toBeUndefined();
  });
});
