import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * Graceful member removal (t-remove-grace): swarm_remove delivers a FINAL
 * notice to the removed member and a swarm broadcast (both noreply findings,
 * from the coordinator) BEFORE the member row is deleted, records a
 * member.removed timeline event, and a removed session's subsequent
 * sendMessage/replyToMessage fails with an ORPHAN-CLARITY error ("you may have
 * been removed") instead of the misleading cross-swarm force hint — which could
 * never rescue a removed member (force still requires a registered row).
 */

let dir: string;
let hooks: Hooks;
let tool: Record<string, any>;

/** Every injected prompt, in order — the only way to observe the final notice
 * (its message ROW is cascaded away with the member on deleteMember via the
 * swarm_message FK ON DELETE CASCADE). */
const prompted: Array<{ sessionID: string; text: string }> = [];

const fakeClient: any = {
  config: {
    providers: async () => ({ data: { providers: [] }, error: undefined }),
  },
  session: {
    create: async (opts: any) => {
      const sessionID = `ses-grace-${Math.random().toString(36).slice(2, 8)}`;
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
      const parts = opts.body?.parts ?? [];
      prompted.push({ sessionID: opts.path?.id, text: parts.map((p: any) => p.text ?? "").join("\n") });
      return { data: undefined, error: undefined };
    },
    postSessionIdPermissionsPermissionId: async () => ({ data: { ok: true }, error: undefined }),
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-grace" },
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
  dir = mkdtempSync(join(tmpdir(), "swarms-grace-test-"));
  hooks = await swarmPlugin(pluginInput, { dataDir: dir });
  tool = hooks.tool ?? {};
});

afterAll(async () => {
  disposeSwarmRuntime();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

function rt() {
  return swarmRuntime()!;
}

function uniqueCoord(prefix: string): string {
  return `ses-grace-lead-${prefix}-${Math.random().toString(36).slice(2, 8)}`;
}

/** Create a swarm + spawn workers through the plugin-tool harness. Returns the
 * runtime, swarm id, coordinator session, and the spawned member rows
 * ({ name, memberId, sessionId }). */
async function makeSwarm(
  name: string,
  opts: {
    task?: { id: string; title: string };
    workers?: Array<{ name: string; role?: string; taskId?: string }>;
  } = {},
) {
  const coordSession = uniqueCoord("mk");
  const createRes = await tool.swarm_create.execute(
    { name, tasks: opts.task ? [opts.task] : undefined },
    ctx(coordSession),
  );
  const created = JSON.parse(String(createRes.output ?? createRes));
  const swarmId = created.swarm.id as string;
  const workers = opts.workers ?? [{ name: "worker", role: "impl" }];
  const spawnRes = await tool.swarm_spawn.execute({ swarmId, members: workers }, ctx(coordSession));
  const spawned = JSON.parse(String(spawnRes.output ?? spawnRes));
  return {
    rt: rt(),
    swarmId,
    coordSession,
    members: (spawned.spawned ?? []) as Array<{ name: string; memberId: string; sessionId: string; status: string }>,
  };
}

async function coordinatorId(swarmId: string): Promise<string> {
  const members = await rt().store.listMembers(swarmId);
  return members.find((m: any) => m.role === "coordinator")!.id;
}

describe("graceful member removal (t-remove-grace)", () => {
  test("swarm_remove delivers a final notice to the removed member + a swarm broadcast, reason in both", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { rt, swarmId, coordSession, members } = await makeSwarm(`grace-a-${tag}`, {
      workers: [{ name: "worker1", role: "impl" }, { name: "worker2", role: "qa" }],
    });
    const worker1 = members.find((m) => m.name === "worker1")!;
    const worker2 = members.find((m) => m.name === "worker2")!;
    const coordId = await coordinatorId(swarmId);
    prompted.length = 0;

    const res = await tool.swarm_remove.execute(
      { swarmId, member: "worker1", reason: "task completed" },
      ctx(coordSession),
    );
    expect(String(res.output ?? res)).toContain("removed worker1");

    // (a) The final notice PROMPT was delivered to the removed member's session
    // BEFORE deletion (autoWake fires immediately; the message ROW then cascades
    // away with the member — the prompt is the surviving trace of delivery).
    const noticePrompt = prompted.find((p) => p.text.includes("[REMOVED]"));
    expect(noticePrompt).toBeDefined();
    expect(noticePrompt!.sessionID).toBe(worker1.sessionId);
    expect(noticePrompt!.text).toContain(`you were removed from swarm grace-a-${tag}`);
    expect(noticePrompt!.text).toContain("session is no longer a swarm member");
    // (c) reason appears in the notice
    expect(noticePrompt!.text).toContain("task completed");

    // (a') the notice message ROW existed before deletion: the send recorded a
    // message.sent event for the finding addressed to the member's NAME.
    const events = await rt.store.listEvents(swarmId, { limit: 300 });
    const noticeSent = events.find(
      (e: any) =>
        e.type === "message.sent" &&
        (JSON.parse(e.payloadJson ?? "{}").to ?? "") === "worker1",
    );
    expect(noticeSent).toBeDefined();
    expect(JSON.parse((noticeSent as any).payloadJson ?? "{}")).toMatchObject({ kind: "finding", to: "worker1", recipients: 1 });

    // (b) The swarm broadcast: a noreply finding row from the coordinator to
    // the surviving member(s) — sent AFTER the removal, so '*' no longer
    // includes worker1.
    const msgs = await rt.store.listMessagesBySwarm(swarmId, 50);
    const broadcast = msgs.find((m: any) => m.body.text.includes("was removed from the swarm"));
    expect(broadcast).toBeDefined();
    expect(broadcast!.fromMemberId).toBe(coordId);
    expect(broadcast!.kind).toBe("finding");
    expect(broadcast!.noreply).toBe(true);
    expect(broadcast!.to.type).toBe("member");
    expect(broadcast!.to.memberId).toBe(worker2.memberId);
    // (c) reason appears in the broadcast
    expect(broadcast!.body.text).toContain("task completed");
    expect(broadcast!.body.text).toContain("member worker1 was removed from the swarm");

    // (b') a message.sent event with to "*" confirms the broadcast send.
    const broadcastSent = events.find(
      (e: any) =>
        e.type === "message.sent" &&
        (JSON.parse(e.payloadJson ?? "{}").to ?? "") === "*",
    );
    expect(broadcastSent).toBeDefined();

    // (f) member.removed timeline event recorded after the removal.
    const removedEvent = events.find(
      (e: any) => e.type === "member.removed" && e.entityType === "member" && e.entityId === worker1.memberId,
    );
    expect(removedEvent).toBeDefined();
    expect(JSON.parse((removedEvent as any).payloadJson ?? "{}").reason).toBe("task completed");

    // Roster: worker1 gone, worker2 intact.
    const roster = await rt.store.listMembers(swarmId);
    expect(roster.find((m: any) => m.name === "worker1")).toBeUndefined();
    expect(roster.find((m: any) => m.name === "worker2")).toBeDefined();
  });

  test("a removed session's sendMessage/replyToMessage fails with the orphan-clarity error", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { rt, swarmId, coordSession, members } = await makeSwarm(`grace-d-${tag}`, {
      workers: [{ name: "worker1", role: "impl" }, { name: "worker2", role: "qa" }],
    });
    const worker1 = members.find((m) => m.name === "worker1")!;
    const worker2 = members.find((m) => m.name === "worker2")!;
    const orphan = "your session is not registered as a member of any swarm (you may have been removed) — only registered swarm members can message";

    // A message addressed to worker2 (survives worker1's removal) to reply to.
    const ping = await tool.swarm_message.execute(
      { swarmId, to: worker2.name, kind: "message", message: "ping" },
      ctx(coordSession),
    );
    expect(String(ping.output ?? ping)).toContain("delivered");
    const pingMsg = (await rt.store.listMessagesBySwarm(swarmId, 50)).find((m: any) => m.body.text === "ping");
    expect(pingMsg).toBeDefined();

    await tool.swarm_remove.execute({ swarmId, member: "worker1" }, ctx(coordSession));

    // (d) same swarm: the ORPHAN error — NOT 'sender is not a member of swarm'.
    await expect(
      rt.core.sendMessage({
        swarmId,
        fromSessionId: worker1.sessionId,
        to: "coordinator",
        kind: "message",
        message: "hi",
      }),
    ).rejects.toThrow(orphan);

    // force cannot rescue a removed member (it still requires a registered row)
    await expect(
      rt.core.sendMessage({
        swarmId,
        fromSessionId: worker1.sessionId,
        to: "coordinator",
        kind: "message",
        message: "hi",
        force: true,
      }),
    ).rejects.toThrow("your session is not registered as a member of any swarm (you may have been removed)");

    // replyToMessage mirrors the orphan clarity
    await expect(
      rt.core.replyToMessage({
        swarmId,
        fromSessionId: worker1.sessionId,
        toMessageId: pingMsg!.id,
        message: "hi",
      }),
    ).rejects.toThrow("your session is not registered as a member of any swarm (you may have been removed) — only registered swarm members can reply");
  });

  test("a session that IS a member of another swarm still gets the cross-swarm force hint (no regression)", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const a = await makeSwarm(`grace-ea-${tag}`, { workers: [{ name: "worker-a", role: "impl" }] });
    const workerA = a.members[0]!;
    const b = await makeSwarm(`grace-eb-${tag}`, { workers: [{ name: "worker-b", role: "impl" }] });
    const workerB = b.members[0]!;
    // The two swarms share one runtime (multi-own: a session may own several).
    expect(a.rt).toBe(b.rt);

    // (e) S (member of swarm A) messages into swarm B without force → the
    // cross-swarm force hint, NOT the orphan error.
    const sendErr = a.rt.core.sendMessage({
      swarmId: b.swarmId,
      fromSessionId: workerA.sessionId,
      to: "worker-b",
      kind: "message",
      message: "hi",
    });
    await expect(sendErr).rejects.toThrow(`sender is not a member of swarm 'grace-eb-${tag}'`);
    await expect(sendErr).rejects.toThrow("pass force: true to message across swarms");
    await expect(sendErr).rejects.not.toThrow("may have been removed");

    // force still WORKS for a genuine registered member (their row is authoritative)
    const forced = await a.rt.core.sendMessage({
      swarmId: b.swarmId,
      fromSessionId: workerA.sessionId,
      to: "worker-b",
      kind: "message",
      message: "hi",
      force: true,
    });
    expect(forced.length).toBe(1);
    expect(forced[0]!.to.memberId).toBe(workerB.memberId);

    // replyToMessage mirrors: S replies into swarm B → cross-swarm hint.
    const replyErr = a.rt.core.replyToMessage({
      swarmId: b.swarmId,
      fromSessionId: workerA.sessionId,
      toMessageId: forced[0]!.id,
      message: "back",
    });
    await expect(replyErr).rejects.toThrow("sender is not a member of swarm");
    await expect(replyErr).rejects.toThrow("pass force: true to reply across swarms");
  });

  test("swarm_remove releases the member's task before removal (DAG keeps advancing)", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const coordSession = uniqueCoord("g");
    const createRes = await tool.swarm_create.execute(
      { name: `grace-g-${tag}`, tasks: [{ id: "t-g", title: "work" }] },
      ctx(coordSession),
    );
    const swarmId = JSON.parse(String(createRes.output ?? createRes)).swarm.id as string;
    const spawnRes = await tool.swarm_spawn.execute(
      { swarmId, members: [{ name: "worker", role: "impl", taskId: "t-g" }] },
      ctx(coordSession),
    );
    const spawned = JSON.parse(String(spawnRes.output ?? spawnRes));
    const workerSession = spawned.spawned[0].sessionId as string;
    const workerMember = await rt().store.getMemberByName(swarmId, "worker");

    // The task is owned by the member (claimed by spawnMember → 'working').
    const before = (await rt().store.listTasks(swarmId)).find((t: any) => t.id === "t-g")!;
    expect(before.ownerMemberId).toBe(workerMember!.id);
    expect(["claimed", "working"].includes(before.status)).toBe(true);

    // (g) removal releases the task back to ready (release happens BEFORE
    // deleteMember, so the DAG can hand it to a replacement member).
    await tool.swarm_remove.execute({ swarmId, member: "worker", reason: "replaced" }, ctx(coordSession));

    const after = (await rt().store.listTasks(swarmId)).find((t: any) => t.id === "t-g")!;
    expect(after.status).toBe("ready");
    expect(after.ownerMemberId).toBeUndefined();
    // The removed member row is gone; its session resolves nowhere.
    expect(await rt().store.getMemberBySessionId(workerSession)).toBeUndefined();
  });
});
