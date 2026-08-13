import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { Permission } from "@opencode-ai/sdk";

/**
 * Permission-wall NOTIFICATION DELIVERY investigation (task t-perm-trace).
 *
 * The user report: "permission-wall notifications don't seem to be getting
 * sent to the coordinator". Candidate root causes under test:
 *
 *  a. allowAllMemberPermissions=true makes autoAllowSwarmPermission return
 *     output.status='allow' IMMEDIATELY, so the 'ask' branch (record +
 *     notifyPermissionPrompt) NEVER runs — the escalation feature is inert
 *     (config interaction, NOT a code bug). The user's global config
 *     (~/.config/opencode/opencode.jsonc) has allowAllMemberPermissions: true.
 *  b. notifyPermissionPrompt swallows sendMessage delivery exceptions with a
 *     bare console.error — no fallback, no surfacing to the coordinator.
 *  c/d. Delivery mechanics: sendMessage -> autoWakeRecipients ->
 *     broker.deliverToIdleMember -> runtime.promptAsync(coordinator session).
 *     Defers only while humanChat.chatting() is true; the chat.message hook
 *     SKIPS coordinator sessions (onUserMessage returns early for role
 *     'coordinator'), so humanChatAt is never set for the coordinator and
 *     delivery is NEVER deferred for it. The broker's 30s cooldown (non-urgent
 *     messages) can DELAY a notice but not lose it.
 *
 * Harness mirrors test/unit/permissions-escalation.test.ts (fake client with
 * session.create/get/promptAsync + config.providers + swarmPlugin +
 * disposeSwarmRuntime), plus a RECORDING promptAsync so mailbox delivery is
 * observable.
 */

let dir: string;
let hooks: Hooks;
let tool: Record<string, any>;
/** Recorded runtime.promptAsync calls (sessionID + text + agent/model) — the
 * observable half of "did the coordinator session actually get the inbox
 * turn?". model/agent are recorded to pin down the delivery profile (the
 * coordinator member row carries no agent -> the broker falls back to the
 * 'swarm' agent doctrine, t-perm-delivery cross-check). */
const promptCalls: Array<{ sessionID: string; text: string; agent?: string; model?: unknown }> = [];

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
      const sessionID = `ses-pwt-${Math.random().toString(36).slice(2, 8)}`;
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
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-pwt" },
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

/** Init the plugin with the given allow-all mode. MUST be paired with a prior
 * disposeSwarmRuntime() (or a previous afterAll) — the runtime is a singleton. */
async function initPlugin(allowAllMemberPermissions: boolean) {
  dir = mkdtempSync(join(tmpdir(), "swarms-pwt-"));
  hooks = await swarmPlugin(pluginInput, { dataDir: dir, allowAllMemberPermissions });
  tool = hooks.tool ?? {};
  promptCalls.length = 0;
}

async function teardown() {
  disposeSwarmRuntime();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
}

/** Create a swarm + spawn a worker member; returns ids + the runtime. */
async function makeSwarmWithWorker(name: string, workerName = "worker") {
  const coordSession = `ses-pwt-lead-${Math.random().toString(36).slice(2, 8)}`;
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
  const coordMember = await rt!.store.getMemberById((await rt!.store.getSwarm(swarmId))!.coordinatorMemberId);
  return { rt, swarmId, coordSession, workerSessionId, workerMember, coordMember };
}

/** Drive the permission.ask hook for a member with a bash "*" request — this
 * is NEVER auto-allowed when allow-all is off (P-D2), so the ask stays "ask"
 * and the escalation path (record + coordinator notification) runs. */
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

/** Allow-all high-risk advisory DIGEST lines for a swarm (t-flood-aggregate:
 * advisories are delivered as lines of the debounced coordinator digest
 * instead of mailbox findings). Forces a flush, then returns the coordinator
 * session's digest LINES containing the marker (lines, not turns). */
async function allowAllAdvisories(swarmId: string, coordSession: string): Promise<string[]> {
  const rt = await runtime();
  await rt!.notices.flush(swarmId);
  return promptCalls
    .filter((c) => c.sessionID === coordSession)
    .flatMap((c) => c.text.split("\n"))
    .filter((l) => l.includes("[PERMISSION ALLOWED]"));
}

/** promptAsync calls whose text is a permission-wall mailbox turn. */
function wallPromptCalls(): Array<{ sessionID: string; text: string; agent?: string; model?: unknown }> {
  return promptCalls.filter((c) => c.text.includes("[PERMISSION WALL]"));
}

describe("a. allowAll=false — a worker ask left 'ask' is recorded AND delivered to the coordinator", () => {
  beforeAll(async () => {
    await initPlugin(false);
  });
  afterAll(async () => {
    await teardown();
  });

  test("record + EXACTLY ONE '[PERMISSION WALL]' finding + mailbox delivery to the coordinator session", async () => {
    promptCalls.length = 0;
    const { rt, swarmId, coordSession, workerSessionId, workerMember, coordMember } = await makeSwarmWithWorker("pwt-a");
    expect(workerMember).toBeDefined();
    expect(coordMember).toBeDefined();

    // (i) The ask stays 'ask' (bash "*" never auto-allowed).
    const out = await askBashStar(workerSessionId, "perm-pwt-a1");
    expect(out.status).toBe("ask");

    // (i) A pending permission is recorded.
    const pending = await rt!.store.listPendingPermissions(swarmId);
    expect(pending.length).toBe(1);
    expect(pending[0]!.id).toBe("perm-pwt-a1");
    expect(pending[0]!.memberId).toBe(workerMember!.id);

    // (ii) The coordinator received EXACTLY ONE finding with the wall marker
    // and the swarm_permissions reply recipe.
    const notices = await permissionWallNotices(swarmId);
    expect(notices.length).toBe(1);
    expect(notices[0]!.to).toMatchObject({ type: "member", memberId: coordMember!.id });
    expect(notices[0]!.priority).toBe("high");
    expect(notices[0]!.noreply).toBe(true);
    expect(notices[0]!.body.text).toContain("[PERMISSION WALL]");
    expect(notices[0]!.body.text).toContain("member 'worker' is blocked");
    expect(notices[0]!.body.text).toContain("swarm_permissions(swarmId:");
    expect(notices[0]!.body.text).toContain("permissionId: 'perm-pwt-a1'");

    // (iii) It LANDED in the coordinator's mailbox: the broker promptAsync'd
    // the coordinator session with the inbox envelope (delivery, not just a
    // store row).
    const wallCalls = wallPromptCalls();
    expect(wallCalls.length).toBe(1);
    expect(wallCalls[0]!.sessionID).toBe(coordSession);
    expect(wallCalls[0]!.text).toContain("[NEW MESSAGE FROM:");
    expect(wallCalls[0]!.text).toContain("[PERMISSION WALL]");
    // t-perm-delivery cross-check: the coordinator member row carries no
    // agent/model, so the broker delivers its mailbox turn under the 'swarm'
    // agent with no model (the P2P doctrine needed to parse the envelope).
    expect(wallCalls[0]!.agent).toBe("swarm");
    expect(wallCalls[0]!.model).toBeUndefined();
    // The store row is marked delivered (not left queued).
    const msg = (await rt!.store.listMessagesBySwarm(swarmId, 100)).find((m) => m.body.text.includes("perm-pwt-a1"));
    expect(msg?.deliveryState).toBe("delivered");
  });

  test("INVISIBLE WALL: a member ask whose session cannot be resolved (stale/re-rooted session) still walls — and is NOT recorded, NOT notified", async () => {
    promptCalls.length = 0;
    const { rt, swarmId, workerSessionId, workerMember } = await makeSwarmWithWorker("pwt-a-stale");
    // Simulate a re-rooted member: the ask arrives from a NEW session id the
    // store does not know (nothing in the plugin observes re-rooted sessions —
    // assignMemberSession only runs on spawn/respawn). The hook's member
    // lookup fails, so neither auto-allow nor the escalation branch runs.
    const rerootedSession = "ses-pwt-reroot-123";
    const out = askOutput();
    await hooks["permission.ask"]!(
      permission({ id: "perm-stale-a1", type: "bash", pattern: "*", sessionID: rerootedSession, title: "run" }),
      out,
    );
    // The wall is NOT auto-allowed (member lookup failed)...
    expect(out.status).toBe("ask");
    // ...and NOT recorded / NOT notified — the escalation path is blind to it.
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
    expect((await permissionWallNotices(swarmId)).length).toBe(0);
    expect(wallPromptCalls().length).toBe(0);
    // Sanity: the worker's OWN session still escalates normally.
    await askBashStar(workerSessionId, "perm-stale-a2");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(1);
    expect((await permissionWallNotices(swarmId)).length).toBe(1);
  });
});

describe("b. allowAll=true — the SAME ask is auto-allowed: nothing recorded, nothing notified (config interaction)", () => {
  beforeAll(async () => {
    await initPlugin(true);
  });
  afterAll(async () => {
    await teardown();
  });

  test("status becomes 'allow', NO pending permission, NO '[PERMISSION WALL]' finding, NO mailbox turn", async () => {
    const { rt, swarmId, workerSessionId } = await makeSwarmWithWorker("pwt-b");

    const out = await askBashStar(workerSessionId, "perm-pwt-b1");
    // (a) autoAllowSwarmPermission force-allows under allow-all.
    expect(out.status).toBe("allow");

    // No pending permission is recorded.
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
    // No '[PERMISSION WALL]' escalation finding exists in the store.
    expect((await permissionWallNotices(swarmId)).length).toBe(0);
    // A second high-risk ask (bash outside worktree) is likewise not a wall.
    const permissionAsk = hooks["permission.ask"];
    const out2 = askOutput();
    await permissionAsk!(
      permission({ id: "perm-pwt-b2", type: "bash", pattern: "C:/elsewhere/*", sessionID: workerSessionId, title: "run elsewhere" }),
      out2,
    );
    expect(out2.status).toBe("allow");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
    expect((await permissionWallNotices(swarmId)).length).toBe(0);
  });

  test("REMEDIATION: high-risk asks under allow-all surface a deduped '[PERMISSION ALLOWED]' advisory to the coordinator (non-blocking, no pending record)", async () => {
    promptCalls.length = 0;
    const { rt, swarmId, coordSession, workerSessionId } = await makeSwarmWithWorker("pwt-b-rem");

    // (1) bash "*" (would be gated without allow-all) -> allowed + advisory.
    const out1 = await askBashStar(workerSessionId, "perm-rem-1");
    expect(out1.status).toBe("allow");
    let advisories = await allowAllAdvisories(swarmId, coordSession);
    expect(advisories.length).toBe(1);
    expect(advisories[0]).toContain("[PERMISSION ALLOWED]");
    expect(advisories[0]).toContain("member 'worker' requested bash");
    expect(advisories[0]).toContain("allowAllMemberPermissions");
    expect(advisories[0]).toContain("outside the member's trusted worktree scope");
    expect(advisories[0]).not.toContain("swarm_permissions(swarmId:"); // nothing to answer
    // NOT a wall: still no pending record, no '[PERMISSION WALL]' finding.
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
    expect((await permissionWallNotices(swarmId)).length).toBe(0);
    // The advisory reached the coordinator session as a digest turn
    // (promptAsync'd by the notice aggregator, not a mailbox finding).
    const advisoryCalls = await allowAllAdvisories(swarmId, coordSession);
    expect(advisoryCalls.length).toBe(1);

    // (2) Same ask id again -> deduped (still exactly one advisory).
    const out1b = await askBashStar(workerSessionId, "perm-rem-1");
    expect(out1b.status).toBe("allow");
    expect((await allowAllAdvisories(swarmId, coordSession)).length).toBe(1);

    // (3) A NEW high-risk ask (bash outside worktree) within the advisory
    // flood-cap window (t-sched-robustness): the member already got its ONE
    // [PERMISSION ALLOWED] advisory this window (1 per member per 5 min — the
    // ANVIL 'format member stuck in a noise loop' fix), so a distinct ask id is
    // SUPPRESSED. The dedup-by-id guarantee above still holds; this cap is on
    // top so a taskless member probing temp dirs can't flood the coordinator.
    const permissionAsk = hooks["permission.ask"];
    const out3 = askOutput();
    await permissionAsk!(
      permission({ id: "perm-rem-3", type: "bash", pattern: "C:/elsewhere/*", sessionID: workerSessionId, title: "run elsewhere" }),
      out3,
    );
    expect(out3.status).toBe("allow");
    expect((await allowAllAdvisories(swarmId, coordSession)).length).toBe(1);

    // (4) An IN-SCOPE ask (bash pattern inside the worktree ".") is allowed
    // WITHOUT an advisory (not high-risk — the normal scoping would allow it).
    const out4 = askOutput();
    await permissionAsk!(
      permission({ id: "perm-rem-4", type: "bash", pattern: ".", sessionID: workerSessionId, title: "in-scope" }),
      out4,
    );
    expect(out4.status).toBe("allow");
    expect((await allowAllAdvisories(swarmId, coordSession)).length).toBe(1);

    // (5) Low-risk read ops stay silent under allow-all (not in the high-risk set).
    const out5 = askOutput();
    await permissionAsk!(
      permission({ id: "perm-rem-5", type: "read", pattern: "C:/elsewhere/data.txt", sessionID: workerSessionId, title: "peek" }),
      out5,
    );
    expect(out5.status).toBe("allow");
    expect((await allowAllAdvisories(swarmId, coordSession)).length).toBe(1);

    // Still nothing pending anywhere.
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
  });

  test("USER REPRO: even under allow-all, an ask from an unresolvable member session (re-root) STILL WALLS — silently (no record, no notice)", async () => {
    const { rt, swarmId, coordSession, workerSessionId } = await makeSwarmWithWorker("pwt-b-stale");
    // allow-all only auto-allows asks whose session resolves to a member via
    // getMemberBySessionId. A re-rooted / re-created member session (new id,
    // never observed by the plugin) fails that lookup, so the ask is left
    // 'ask' — a real wall — with NO record and NO coordinator notice.
    const rerootedSession = "ses-pwt-reroot-456";
    const out = askOutput();
    await hooks["permission.ask"]!(
      permission({ id: "perm-stale-b1", type: "bash", pattern: "*", sessionID: rerootedSession, title: "run" }),
      out,
    );
    expect(out.status).toBe("ask");
    expect((await rt!.store.listPendingPermissions(swarmId)).length).toBe(0);
    expect((await permissionWallNotices(swarmId)).length).toBe(0);
    expect((await allowAllAdvisories(swarmId, coordSession)).length).toBe(0);
    expect(wallPromptCalls().length).toBe(0);
    // Sanity: the worker's own session is still auto-allowed under allow-all.
    const out2 = await askBashStar(workerSessionId, "perm-stale-b2");
    expect(out2.status).toBe("allow");
  });
});

describe("c. delivery failure is SWALLOWED by notifyPermissionPrompt (console.error only)", () => {
  beforeAll(async () => {
    await initPlugin(false);
  });
  afterAll(async () => {
    await teardown();
  });

  test("stopped coordinator member -> sendMessage throws -> only console.error; the coordinator gets nothing; the pending record survives", async () => {
    const { rt, swarmId, workerSessionId, coordMember } = await makeSwarmWithWorker("pwt-c");

    // Simulate a delivery failure: stop the coordinator member. sendMessage
    // throws "cannot message ... member is stopped" BEFORE inserting.
    await rt!.store.updateMemberStatus(coordMember!.id, "stopped");

    // Capture console.error (the only sink for the swallowed failure).
    const errorLog: string[] = [];
    const origError = console.error;
    console.error = (...args: unknown[]) => {
      errorLog.push(args.map(String).join(" "));
    };
    let out: { status: "ask" | "deny" | "allow" };
    try {
      out = await askBashStar(workerSessionId, "perm-pwt-c1");
    } finally {
      console.error = origError;
    }

    // The ask is still recorded as pending (recorded BEFORE the notification).
    expect(out!.status).toBe("ask");
    const pending = await rt!.store.listPendingPermissions(swarmId);
    expect(pending.length).toBe(1);
    expect(pending[0]!.id).toBe("perm-pwt-c1");

    // The failure was swallowed: console.error got the notice-failed message
    // and NOTHING else surfaced.
    expect(errorLog.some((l) => l.includes("permission-wall notice to coordinator failed"))).toBe(true);

    // The coordinator received NOTHING: no finding row, no mailbox turn.
    expect((await permissionWallNotices(swarmId)).length).toBe(0);
    expect(wallPromptCalls().length).toBe(0);
    expect((await rt!.store.listPendingMessages(coordMember!.id)).length).toBe(0);
  });
});

describe("d. delivery mechanics: promptAsync to the coordinator session, deferral, cooldown", () => {
  beforeAll(async () => {
    await initPlugin(false);
  });
  afterAll(async () => {
    await teardown();
  });

  test("humanChat.chatting() is FALSE for the coordinator when humanChatAt is null (chat.message skips coordinator)", async () => {
    promptCalls.length = 0;
    const { rt, swarmId, coordMember } = await makeSwarmWithWorker("pwt-d1");
    const swarm = await rt!.store.getSwarm(swarmId);
    expect(swarm).toBeDefined();
    // Default state: humanChatAt is null -> chatting() false -> delivery is
    // never deferred for the coordinator session.
    expect(coordMember!.humanChatAt == null).toBe(true);
    expect(await rt!.humanChat.chatting(coordMember!, swarm!)).toBe(false);
    // Once a humanChatAt is set (artificially — the real hook skips the
    // coordinator), chatting() flips true.
    await rt!.store.updateMemberHumanChat(coordMember!.id, Date.now());
    const refreshed = await rt!.store.getMemberById(coordMember!.id);
    expect(await rt!.humanChat.chatting(refreshed!, swarm!)).toBe(true);
  });

  test("delivery defers while the coordinator is 'chatting', then delivers after the lull clears", async () => {
    promptCalls.length = 0;
    const { rt, swarmId, coordSession, workerSessionId, coordMember } = await makeSwarmWithWorker("pwt-d2");

    // Simulate the user actively chatting with the coordinator session.
    await rt!.store.updateMemberHumanChat(coordMember!.id, Date.now());

    const out = await askBashStar(workerSessionId, "perm-pwt-d2a");
    expect(out.status).toBe("ask");
    // The finding row exists...
    const notices = await permissionWallNotices(swarmId);
    expect(notices.length).toBe(1);
    // ...but delivery is deferred: the message stays QUEUED and the
    // coordinator session was NOT promptAsync'd.
    expect(wallPromptCalls().length).toBe(0);
    const queued = await rt!.store.listPendingMessages(coordMember!.id);
    expect(queued.length).toBe(1);
    expect(queued[0]!.body.text).toContain("[PERMISSION WALL]");

    // Lull clears (humanChatAt -> null; the sweep would do this) -> the next
    // delivery attempt reaches the coordinator session.
    await rt!.store.updateMemberHumanChat(coordMember!.id, null);
    const delivered = await rt!.broker.deliverToIdleMember(coordMember!.id, coordSession);
    expect(delivered).toBe(1);
    const wallCalls = wallPromptCalls();
    expect(wallCalls.length).toBe(1);
    expect(wallCalls[0]!.sessionID).toBe(coordSession);
    expect((await rt!.store.listPendingMessages(coordMember!.id)).length).toBe(0);
  });

  test("broker 30s cooldown DELAYS a second wall notice to the same coordinator (non-urgent, priority high)", async () => {
    promptCalls.length = 0;
    const { rt, swarmId, workerSessionId, coordMember } = await makeSwarmWithWorker("pwt-d3");

    // First wall -> delivered immediately (promptAsync fired, cooldown set).
    await askBashStar(workerSessionId, "perm-pwt-d3a");
    expect(wallPromptCalls().length).toBe(1);

    // Second wall (new permission id) -> the 30s per-member cooldown blocks
    // the immediate wake: the notice stays queued, no second promptAsync.
    await askBashStar(workerSessionId, "perm-pwt-d3b");
    expect(wallPromptCalls().length).toBe(1);
    const queued = await rt!.store.listPendingMessages(coordMember!.id);
    expect(queued.length).toBe(1);
    expect(queued[0]!.body.text).toContain("perm-pwt-d3b");
    // Both store rows exist (nothing lost — just delayed).
    const all = await rt!.store.listMessagesBySwarm(swarmId, 100);
    const walls = all.filter((m) => m.kind === "finding" && m.body.text.includes("[PERMISSION WALL]"));
    expect(walls.length).toBe(2);
  });
});
