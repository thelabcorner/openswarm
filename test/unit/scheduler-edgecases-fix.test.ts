import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { SwarmPluginRuntime } from "../../src/plugin.ts";

/**
 * Scheduler edge-case fix regression tests (S-02, S-05):
 *  - S-02: leaseSweep runs BEFORE runScheduler in sweepOnce → a lease-expired
 *    task is released and the SAME sweep's scheduler pass can reassign it
 *    (previously stranded one full sweep / 10s).
 *  - S-05: the taskless-working demotion has a kickoff GRACE window — a member
 *    whose claim happened recently (lastActiveAt fresh) is not demoted
 *    mid-kickoff, so an in-flight prompt cannot be torn down.
 */
let dir: string;
let hooks: Hooks;

const fakeClient = {
  session: {
    create: async (opts: any) => {
      const sessionID = `ses-sedge-${Math.random().toString(36).slice(2, 8)}`;
      if (opts.body?.parentID !== undefined) {
        throw new Error(`session.create received a parentID (${opts.body.parentID}); members must be root sessions`);
      }
      return {
        data: { id: sessionID, title: opts.body?.title, parentID: undefined, directory: "." },
        error: undefined,
      };
    },
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

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-sedge" },
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
  dir = mkdtempSync(join(tmpdir(), "swarms-sedge-test-"));
  hooks = await swarmPlugin(pluginInput, { dataDir: dir });
});

afterAll(async () => {
disposeSwarmRuntime();
  // Reset the process-global plugin singleton so sibling test files are not
  // affected (mirror digest-exchange cleanup).
  await (hooks as any).dispose?.();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

async function rt(): Promise<SwarmPluginRuntime> {
  const r = await import("../../src/plugin.ts").then((m) => m.swarmRuntime());
  if (!r) throw new Error("swarmRuntime not initialized");
  return r;
}

async function makeSwarm(tag: string, policies: Record<string, unknown> = {}) {
  const create = await (hooks.tool as any).swarm_create.execute(
    { name: `sedge-${tag}`, policies },
    ctx(`ses-sedge-lead-${tag}`),
  );
  const created = JSON.parse(String(create.output));
  return created.swarm.id as string;
}

async function insertMember(swarmId: string, name: string, sessionId: string) {
  const r = await rt();
  const members = await r.store.listMembers(swarmId);
  const coord = members.find((m) => m.role === "coordinator")!;
  await r.store.insertMember({
    id: `mem-${swarmId}-${name}`,
    swarmId,
    name,
    role: "worker",
    sessionId,
    status: "idle",
    workspaceMode: "worktree",
    createdAt: Date.now(),
    updatedAt: Date.now(),
  });
  void coord;
}

describe("S-02 — leaseSweep before runScheduler (same-sweep reassignment)", () => {
  test("lease-expired task is released by leaseSweep and reassignable in the same pass", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag, { taskLeaseMs: 30_000 });
    const r = await rt();
    const coord = (await r.store.listMembers(swarmId)).find((m) => m.role === "coordinator")!;
    // Member A holds the task (working) with a lease that is ALREADY expired.
    await insertMember(swarmId, "a", `ses-a-${tag}`);
    await insertMember(swarmId, "b", `ses-b-${tag}`);
    const a = await r.store.getMemberByName(swarmId, "a");
    const b = await r.store.getMemberByName(swarmId, "b");
    await r.store.insertTask({
      id: `T-${tag}`,
      swarmId,
      title: "lease expired task",
      status: "ready",
      priority: 0,
      createdByMemberId: coord.id,
      createdAt: Date.now() - 1000,
      updatedAt: Date.now() - 1000,
    });
    // Claim with a SHORT lease in the past so it is already expired.
    const expiredLease = 1; // 1ms lease, already past
    await r.store.claimTask(`T-${tag}`, a!.id, expiredLease);
    await r.store.updateTaskStatus(`T-${tag}`, "working");
    await r.store.updateMemberStatus(a!.id, "working", { currentTaskId: `T-${tag}` });

    // Wait for the 1ms lease to definitely expire.
    await new Promise((res) => setTimeout(res, 20));

    // S-02 composition: leaseSweep FIRST (releases + frees the member), then
    // the scheduler pass assigns to the OTHER idle member B in the same flow.
    await r.leaseSweep(swarmId);
    const afterLease = (await r.store.listTasks(swarmId)).find((t) => t.id === `T-${tag}`);
    expect(afterLease?.status).toBe("ready");
    expect(afterLease?.ownerMemberId).toBeUndefined();
    // A's binding is cleared (S-01/S-06 family: owner reconciled).
    const aAfter = await r.store.getMemberById(a!.id);
    expect(aAfter?.currentTaskId).toBeUndefined();

    // Same-sweep scheduler pass reassigns to an AVAILABLE idle member (either
    // a or b — both are idle now; affinity+name order decides). The contract
    // is: same-pass reassignment to a valid idle member, no stale binding.
    await r.runScheduler(swarmId);
    const afterSched = (await r.store.listTasks(swarmId)).find((t) => t.id === `T-${tag}`);
    expect(afterSched?.ownerMemberId).toBeDefined();
    expect(afterSched?.status).toBe("working");
    const assigned = afterSched?.ownerMemberId ? await r.store.getMemberById(afterSched.ownerMemberId) : undefined;
    expect(assigned?.status).toBe("working");
    expect(assigned?.currentTaskId).toBe(`T-${tag}`);
    // Reassigned to one of the two now-idle members (affinity + name order).
    expect(afterSched?.ownerMemberId === a!.id || afterSched?.ownerMemberId === b!.id).toBe(true);
  });
});

describe("S-05 — taskless-working demotion kickoff grace", () => {
  test("a member whose claim is recent (lastActiveAt fresh) is NOT demoted mid-kickoff", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag);
    const r = await rt();
    await insertMember(swarmId, "c", `ses-c-${tag}`);
    const c = await r.store.getMemberByName(swarmId, "c");
    // Member c is `working` with NO currentTaskId but a FRESH lastActiveAt
    // (claim just happened; kickoff prompt in flight). Watchdog must NOT demote.
    await r.store.updateMemberStatus(c!.id, "working", { currentTaskId: null, lastActiveAt: Date.now() });

    await r.watchdog(swarmId);
    const cAfter = await r.store.getMemberById(c!.id);
    expect(cAfter?.status).toBe("working"); // NOT demoted — inside grace window
  });

  test("a member whose claim is STALE is demoted (grace window elapsed)", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const swarmId = await makeSwarm(tag);
    const r = await rt();
    await insertMember(swarmId, "d", `ses-d-${tag}`);
    const d = await r.store.getMemberByName(swarmId, "d");
    // lastActiveAt older than the 2-min grace window → demote.
    await r.store.updateMemberStatus(d!.id, "working", {
      currentTaskId: null,
      lastActiveAt: Date.now() - 3 * 60_000,
    });

    await r.watchdog(swarmId);
    const dAfter = await r.store.getMemberById(d!.id);
    expect(dAfter?.status).toBe("idle"); // demoted — outside grace window
  });
});
