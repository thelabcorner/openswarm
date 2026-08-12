import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, handleOpenCodeEvent, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { Permission } from "@opencode-ai/sdk";
import { FENCE_MARKER } from "../../src/core/fence.ts";

/**
 * Permission-stall escalation tests (task t-verify): the runtime reply
 * surface + ask-hook recording + coordinator notification + swarm_permissions
 * tool + blocked-recipient detection + permission.replied bookkeeping.
 *
 * Coverage:
 *  a. Store roundtrip: insertPendingPermission -> listPendingPermissions (only
 *     response IS NULL rows) -> listPendingForMembers -> getPendingPermission
 *     -> respondToPermission (leaves pending list, response persisted) ->
 *     markPermissionReplied.
 *  b. permission.ask hook: a worker-member ask left "ask" records a pending
 *     prompt AND notifies the swarm coordinator EXACTLY ONCE (a second
 *     identical ask does not re-notify - dedup by permission id).
 *  c. swarm_permissions tool: list renders fenced pattern + reply recipe;
 *     reply as a WORKER is rejected (coordinator-only); reply as the
 *     COORDINATOR (once/always) responds the record with a rich confirmation;
 *     unknown permissionId is a clear error; a runtime reply failure (404 /
 *     error from postSessionIdPermissionsPermissionId) takes the "already
 *     gone" path and marks the record expired.
 *  d. Blocked-recipient: messaging a member with a pending permission record
 *     surfaces blockedByPermission in the swarm_message output AND notifies
 *     the coordinator with the answer recipe.
 *  e. permission.replied event -> the recorded prompt is marked responded.
 */

let dir: string;
let hooks: Hooks;
let tool: Record<string, any>;
const coordinatorSession = "ses-esc-lead";

/** Reply-mode switch for the fake client's permission-reply endpoint. */
let replyMode: "ok" | "error" = "ok";
let replyCalls: Array<{ sessionID: string; permissionID: string; response: string }> = [];

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
      const sessionID = `ses-esc-${Math.random().toString(36).slice(2, 8)}`;
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
    // v1-gen permission reply endpoint (OpenCodeRuntime.replyPermission).
    postSessionIdPermissionsPermissionId: async (opts: any) => {
      replyCalls.push({
        sessionID: opts?.path?.id,
        permissionID: opts?.path?.permissionID,
        response: opts?.body?.response,
      });
      if (replyMode === "error") return { data: undefined, error: { code: 404, message: "permission request not found" } };
      return { data: { ok: true }, error: undefined };
    },
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-esc" },
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

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-esc-test-"));
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

/** Create a swarm + spawn a worker member; returns ids + the runtime.
 * The runtime singleton is lazily initialized by the FIRST tool call, so the
 * runtime handle is fetched AFTER swarm_create executes. Each swarm needs its
 * own coordinator session (one session runs one swarm). */
async function makeSwarmWithWorker(name: string, workerName = "worker") {
  const coordSession = `ses-esc-lead-${Math.random().toString(36).slice(2, 8)}`;
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
  return { rt, swarmId, coordSession, workerSessionId, workerMember };
}

/** Drive the permission.ask hook for a member with a bash "*" request — this
 * is NEVER auto-allowed (P-D2), so the ask stays "ask" and the escalation path
 * (record + coordinator notification) runs deterministically. */
async function askBashStar(memberSessionId: string, permissionId: string) {
  const permissionAsk = hooks["permission.ask"];
  const out = askOutput();
  await permissionAsk!(
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

describe("a. pending-permission store roundtrip (escalation contract)", () => {
  test("insert -> list -> listForMembers -> get -> respond -> markReplied lifecycle", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("esc-store");
    expect(workerMember).toBeDefined();
    const memberId = workerMember!.id;
    const now = Date.now();

    // insertPendingPermission
    await rt!.store.insertPendingPermission({
      id: "perm-rt-1",
      swarmId,
      memberId,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "*",
      title: "run",
      response: null,
      respondedAt: null,
      createdAt: now,
    });
    // A second, already-answered record must NOT appear in the pending list.
    await rt!.store.insertPendingPermission({
      id: "perm-rt-answered",
      swarmId,
      memberId,
      sessionId: workerSessionId,
      type: "edit",
      pattern: "src/**",
      title: "edit",
      response: "once",
      respondedAt: now,
      createdAt: now,
    });

    // listPendingPermissions: only response IS NULL rows.
    const pending = await rt!.store.listPendingPermissions(swarmId);
    expect(pending.map((p) => p.id)).toEqual(["perm-rt-1"]);

    // listPendingForMembers: filtered by member ids, pending only.
    const forMembers = await rt!.store.listPendingForMembers([memberId]);
    expect(forMembers.map((p) => p.id)).toEqual(["perm-rt-1"]);
    expect(await rt!.store.listPendingForMembers(["mem-nobody"])).toEqual([]);
    expect(await rt!.store.listPendingForMembers([])).toEqual([]);

    // getPendingPermission: single lookup (answered rows still readable).
    const rec = await rt!.store.getPendingPermission(swarmId, "perm-rt-1");
    expect(rec?.pattern).toBe("*");
    expect(rec?.response).toBeNull();
    expect((await rt!.store.getPendingPermission(swarmId, "perm-rt-answered"))?.response).toBe("once");
    expect(await rt!.store.getPendingPermission(swarmId, "perm-missing")).toBeUndefined();

    // respondToPermission: leaves the pending list, response persisted.
    await rt!.store.respondToPermission("perm-rt-1", "once");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
    const responded = await rt!.store.getPendingPermission(swarmId, "perm-rt-1");
    expect(responded?.response).toBe("once");
    expect(responded?.respondedAt).not.toBeNull();

    // markPermissionReplied: external answer (raw server string) persists too.
    await rt!.store.insertPendingPermission({
      id: "perm-rt-2",
      swarmId,
      memberId,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "npm *",
      title: "npm",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });
    await rt!.store.markPermissionReplied("perm-rt-2", "allow");
    expect((await rt!.store.getPendingPermission(swarmId, "perm-rt-2"))?.response).toBe("allow");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
  });
});

describe("b. permission.ask hook: record + EXACTLY ONE coordinator notification", () => {
  test("an ask left 'ask' creates a pending record and notifies the coordinator once (dedup)", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("esc-ask");

    // First ask (bash "*" is never auto-allowed -> stays ask).
    const out1 = await askBashStar(workerSessionId, "perm-ask-1");
    expect(out1.status).toBe("ask");

    // Pending record exists.
    const pending = await rt!.store.listPendingPermissions(swarmId);
    expect(pending.length).toBe(1);
    expect(pending[0]?.id).toBe("perm-ask-1");
    expect(pending[0]?.memberId).toBe(workerMember!.id);
    expect(pending[0]?.sessionId).toBe(workerSessionId);
    expect(pending[0]?.type).toBe("bash");
    expect(pending[0]?.response).toBeNull();

    // Coordinator received EXACTLY ONE notification finding with the recipe.
    let notices = await permissionWallNotices(swarmId);
    expect(notices.length).toBe(1);
    expect(notices[0]!.body.text).toContain("member 'worker' is blocked");
    expect(notices[0]!.body.text).toContain("swarm_permissions(swarmId:");
    expect(notices[0]!.body.text).toContain("permissionId: 'perm-ask-1'");

    // A second IDENTICAL ask (same permission id) does NOT re-notify (dedup),
    // and does not duplicate the pending row.
    const out2 = await askBashStar(workerSessionId, "perm-ask-1");
    expect(out2.status).toBe("ask");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(1);
    notices = await permissionWallNotices(swarmId);
    expect(notices.length).toBe(1);

    // A DIFFERENT ask (new id) DOES notify again.
    await askBashStar(workerSessionId, "perm-ask-2");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(2);
    notices = await permissionWallNotices(swarmId);
    expect(notices.length).toBe(2);
  });

  test("a coordinator-session ask is NOT recorded (prompts still go to the user)", async () => {
    const { rt, swarmId } = await makeSwarmWithWorker("esc-ask-coord");
    const out = askOutput();
    await hooks["permission.ask"]!(
      permission({ id: "perm-coord-1", type: "bash", pattern: "*", sessionID: coordinatorSession, title: "coord" }),
      out,
    );
    // The coordinator session's prompt must not create a pending record.
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
  });
});

describe("c. swarm_permissions tool", () => {
  test("list renders pending entries with a fenced pattern + reply recipe (any member)", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("esc-list");
    await rt!.store.insertPendingPermission({
      id: "perm-list-1",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "rm -rf *",
      title: "danger",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });

    // A WORKER may list (read-only surface).
    const res = await tool.swarm_permissions.execute({ swarmId, action: "list" }, ctx(workerSessionId));
    const out = String(res.output ?? res);
    expect(out).toContain("1 member(s) blocked");
    expect(out).toContain("member 'worker'");
    expect(out).toContain("bash");
    expect(out).toContain(FENCE_MARKER); // pattern rendered as untrusted data
    expect(out).toContain("answer: swarm_permissions(swarmId:");
    expect(out).toContain("permissionId: 'perm-list-1'");
  });

  test("empty swarm lists (none)", async () => {
    const { swarmId, workerSessionId } = await makeSwarmWithWorker("esc-list-empty");
    const res = await tool.swarm_permissions.execute({ swarmId, action: "list" }, ctx(workerSessionId));
    expect(String(res.output ?? res)).toContain("(none");
  });

  test("reply as a WORKER is rejected (coordinator-only)", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("esc-reply-worker");
    await rt!.store.insertPendingPermission({
      id: "perm-rw-1",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "*",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });
    const res = await tool.swarm_permissions.execute(
      { swarmId, action: "reply", permissionId: "perm-rw-1", response: "once" },
      ctx(workerSessionId),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("coordinator-only");
    // Record untouched.
    expect((await rt!.store.getPendingPermission(swarmId, "perm-rw-1"))?.response).toBeNull();
    expect(replyCalls.length).toBe(0);
  });

  test("reply as the COORDINATOR (once / always) answers the record + rich confirmation", async () => {
    replyMode = "ok";
    const { rt, swarmId, coordSession, workerSessionId, workerMember } = await makeSwarmWithWorker("esc-reply-coord");
    await rt!.store.insertPendingPermission({
      id: "perm-rc-1",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "npm *",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });

    const res = await tool.swarm_permissions.execute(
      { swarmId, action: "reply", permissionId: "perm-rc-1", response: "once" },
      ctx(coordSession),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("answered 'once' for member 'worker'");
    expect(out).toContain("unblocked");
    // The runtime reply endpoint was hit with the right payload.
    expect(replyCalls.at(-1)).toMatchObject({ sessionID: workerSessionId, permissionID: "perm-rc-1", response: "once" });
    // Record responded + no longer pending.
    expect((await rt!.store.getPendingPermission(swarmId, "perm-rc-1"))?.response).toBe("once");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);

    // "always" path too.
    await rt!.store.insertPendingPermission({
      id: "perm-rc-2",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "external_directory",
      pattern: "C:/elsewhere",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });
    const res2 = await tool.swarm_permissions.execute(
      { swarmId, action: "reply", permissionId: "perm-rc-2", response: "always" },
      ctx(coordSession),
    );
    expect(String(res2.output ?? res2)).toContain("answered 'always'");
    expect((await rt!.store.getPendingPermission(swarmId, "perm-rc-2"))?.response).toBe("always");
  });

  test("unknown permissionId is a clear error", async () => {
    const { swarmId, coordSession } = await makeSwarmWithWorker("esc-reply-unknown");
    const res = await tool.swarm_permissions.execute(
      { swarmId, action: "reply", permissionId: "perm-nope", response: "once" },
      ctx(coordSession),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("no pending permission 'perm-nope'");
    expect(out).toContain("already answered?");
  });

  test("runtime reply failure (404/error) takes the 'already gone' path and marks the record expired", async () => {
    replyMode = "error";
    const { rt, swarmId, coordSession, workerSessionId, workerMember } = await makeSwarmWithWorker("esc-reply-gone");
    await rt!.store.insertPendingPermission({
      id: "perm-gone-1",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "bash",
      pattern: "*",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });
    const res = await tool.swarm_permissions.execute(
      { swarmId, action: "reply", permissionId: "perm-gone-1", response: "once" },
      ctx(coordSession),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("already gone");
    expect(out).toContain("nothing to answer");
    // Record marked expired (replyPermission returned false -> the prompt is
    // gone on the server side), so it leaves the pending list.
    expect((await rt!.store.getPendingPermission(swarmId, "perm-gone-1"))?.response).toBe("expired");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
    replyMode = "ok";
  });
});

describe("d. blocked-recipient detection on message send", () => {
  test("messaging a member with a pending prompt surfaces blockedByPermission + notifies the coordinator", async () => {
    const { rt, swarmId, coordSession, workerSessionId } = await makeSwarmWithWorker("esc-blocked", "sender");
    // Second worker = the blocked recipient.
    const spawnB = await tool.swarm_spawn.execute(
      { swarmId, members: [{ name: "stuck", role: "impl" }] },
      ctx(coordSession),
    );
    const stuckSessionId = JSON.parse(String(spawnB.output ?? spawnB)).spawned[0].sessionId as string;
    const stuck = await rt!.store.getMemberBySessionId(stuckSessionId);
    expect(stuck).toBeDefined();

    // The recipient is stuck behind a pending permission prompt (recorded
    // directly - no ask-hook notification fired yet, so the blocked-recipient
    // path is the ONLY notifier for this prompt).
    await rt!.store.insertPendingPermission({
      id: "perm-block-1",
      swarmId,
      memberId: stuck!.id,
      sessionId: stuckSessionId,
      type: "bash",
      pattern: "rm -rf *",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });

    // Sender -> stuck member.
    const sent = await tool.swarm_message.execute(
      { swarmId, to: "stuck", kind: "request", message: "please finish the wire" },
      ctx(workerSessionId),
    );
    const out = JSON.parse(String(sent.output ?? sent));
    // Output surfaces the block.
    expect(out.blockedByPermission).toBeDefined();
    expect(out.blockedByPermission.length).toBe(1);
    expect(out.blockedByPermission[0]).toMatchObject({ memberId: stuck!.id, permissionId: "perm-block-1", type: "bash" });
    expect(out.blockedByPermission[0].pattern).toBe("rm -rf *");
    expect(String(out.note ?? "")).toContain("blocked on a permission prompt");

    // The coordinator received a notification with the answer recipe and the
    // mailbox context (message still delivered/queued normally).
    const notices = await permissionWallNotices(swarmId);
    expect(notices.length).toBe(1);
    expect(notices[0]!.body.text).toContain("permissionId: 'perm-block-1'");
    expect(notices[0]!.body.text).toContain("swarm_permissions(swarmId:");
    expect(notices[0]!.body.text).toContain("a message from 'sender' is waiting in the blocked member's mailbox");

    // The blocked member is NOT double-notified by a second send to them
    // (dedup is per permission id, not per message).
    await tool.swarm_message.execute(
      { swarmId, to: "stuck", kind: "message", message: "ping again" },
      ctx(workerSessionId),
    );
    expect((await permissionWallNotices(swarmId)).length).toBe(1);
  });
});

describe("e. permission.replied event marks the record responded", () => {
  test("handleOpenCodeEvent with a synthetic permission.replied event persists the answer", async () => {
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("esc-replied");
    await rt!.store.insertPendingPermission({
      id: "perm-replied-1",
      swarmId,
      memberId: workerMember!.id,
      sessionId: workerSessionId,
      type: "edit",
      pattern: "src/**",
      response: null,
      respondedAt: null,
      createdAt: Date.now(),
    });
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(1);

    // User answered in the app -> permission.replied event fires.
    await handleOpenCodeEvent(rt!, {
      type: "permission.replied",
      properties: { sessionID: workerSessionId, permissionID: "perm-replied-1", response: "once" },
    });

    // The recorded prompt is marked responded and leaves the pending list.
    const rec = await rt!.store.getPendingPermission(swarmId, "perm-replied-1");
    expect(rec?.response).toBe("once");
    expect(rec?.respondedAt).not.toBeNull();
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);

    // Missing row is a no-op (best-effort bookkeeping).
    await expect(
      handleOpenCodeEvent(rt!, {
        type: "permission.replied",
        properties: { sessionID: workerSessionId, permissionID: "perm-never-recorded", response: "always" },
      }),
    ).resolves.toBeUndefined();
  });
});
