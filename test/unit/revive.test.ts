import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * Operator revive/re-task UX tests (src/revive/revive.ts + swarm_list /
 * swarm_revive tools): health diagnostics, keep-revival reconciliation,
 * retask repoint (keep agents) vs fresh (new agents) with confirm gating.
 */

let dirs: string[] = [];

function makeClient() {
  return {
    config: {
      providers: async () => ({
        data: {
          providers: [
            { id: "opencode-go", models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash (2x usage)" } } },
            { id: "opencode", models: { "deepseek-v4-flash-free": { name: "DeepSeek V4 Flash Free" } } },
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
  project: { id: "proj-revive" },
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
  const dir = mkdtempSync(join(tmpdir(), "swarms-revive-"));
  dirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient()), { dataDir: dir });
  tool = hooks.tool ?? {};
}

function rt() {
  const r = swarmRuntime();
  if (!r) throw new Error("swarm runtime not initialized");
  return r;
}

async function createSwarm(name: string): Promise<string> {
  const created = await tool.swarm_create.execute({ name }, ctx("ses-lead"));
  return JSON.parse(String(created.output ?? created)).swarm.id;
}

async function spawnWorker(swarmId: string, name: string): Promise<{ memberId: string; sessionId: string }> {
  const res = await tool.swarm_spawn.execute({ swarmId, members: [{ name, role: "worker" }] }, ctx("ses-lead"));
  const spawned = JSON.parse(String(res.output ?? res)).spawned[0];
  return { memberId: spawned.memberId, sessionId: spawned.sessionId };
}

async function stopWorker(memberId: string): Promise<void> {
  await rt().core.store.updateMemberStatus(memberId, "stopped", { currentTaskId: null, lastActiveAt: Date.now() });
}

/** createTask leaves tasks 'pending'; claim requires 'ready' (DAG semantics). */
async function createReadyTask(swarmId: string, title: string, createdByMemberId: string): Promise<string> {
  const task = await rt().core.createTask({ swarmId, title, createdByMemberId });
  await rt().core.store.updateTaskStatus(task.id, "ready");
  return task.id;
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe("swarm_list", () => {
  test("lists swarms with staleness flags", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-list");
    const { memberId } = await spawnWorker(swarmId, "w1");
    const res = await tool.swarm_list.execute({}, ctx("ses-lead"));
    const out = String(res.output ?? res);
    expect(out).toContain("SWARMS (1)");
    expect(out).toContain("rev-list");
    expect(out).toContain("healthy");
    // Stop the worker -> flagged stale.
    await stopWorker(memberId);
    const res2 = await tool.swarm_list.execute({}, ctx("ses-lead"));
    expect(String(res2.output ?? res2)).toContain("STALE: stopped/failed members");
  });
});

describe("swarm_revive health", () => {
  test("healthy swarm -> verdict healthy with no-action recommendation", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-health-ok");
    await spawnWorker(swarmId, "w1");
    const res = await tool.swarm_revive.execute({ swarmId, action: "health" }, ctx("ses-lead"));
    const out = String(res.output ?? res);
    expect(out).toContain("verdict: HEALTHY");
    expect(out).toContain("no action needed");
  });

  test("stalled swarm -> verdict revive + recommended invocation", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-health-bad");
    const { memberId } = await spawnWorker(swarmId, "w1");
    await stopWorker(memberId);
    const res = await tool.swarm_revive.execute({ swarmId, action: "health" }, ctx("ses-lead"));
    const out = String(res.output ?? res);
    expect(out).toContain("verdict: REVIVE");
    expect(out).toContain("[needs respawn]");
    expect(out).toContain("swarm_revive(swarmId:");
    expect(out).toContain("includeStopped: true");
  });

  test("health is available to any member (read-only)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-health-member");
    const { sessionId } = await spawnWorker(swarmId, "w1");
    const res = await tool.swarm_revive.execute({ swarmId, action: "health" }, ctx(sessionId));
    expect(String(res.output ?? res)).toContain("verdict:");
  });
});

describe("swarm_revive revive (keep)", () => {
  test("revive is coordinator-only", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-coord-only");
    const { sessionId } = await spawnWorker(swarmId, "w1");
    const res = await tool.swarm_revive.execute({ swarmId, action: "revive", strategy: "keep" }, ctx(sessionId));
    expect(String(res.output ?? res)).toContain("only the coordinator");
  });

  test("revive+keep respawns stopped members when includeStopped, releases stuck tasks, flips status active", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-keep");
    const { memberId } = await spawnWorker(swarmId, "w1");
    const before = (await rt().core.store.getMemberById(memberId))!;
    await stopWorker(memberId);
    // A stuck task: claimed, then its lease is manually aged into the past
    // (claimTask with leaseMs<=0 means NO lease per S-10, so we claim with a
    // real lease and force expiry via SQL).
    const taskId = await createReadyTask(swarmId, "stuck task", before.id);
    await rt().core.store.claimTask(taskId, memberId, 60_000);
    (rt().core.store as any).db.run(`UPDATE swarm_task SET lease_expires_at = ? WHERE id = ?`, [Date.now() - 60_000, taskId]);

    const res = await tool.swarm_revive.execute({ swarmId, action: "revive", strategy: "keep", includeStopped: true }, ctx("ses-lead"));
    const out = String(res.output ?? res);
    expect(out).toContain("REVIVED");
    expect(out).toContain("w1: respawned");
    expect(out).toContain("released stuck tasks: 1");
    // Member got a NEW session id (respawned).
    const after = (await rt().core.store.getMemberById(memberId))!;
    expect(after.sessionId).not.toBe(before.sessionId);
    expect(after.status).toBe("working");
    // The stuck task was released back to ready.
    const tasksAfter = await rt().core.store.listTasks(swarmId);
    const stuckTask = tasksAfter.find((t) => t.id === taskId)!;
    expect(stuckTask.status).toBe("ready");
  });

  test("revive+keep without includeStopped leaves deliberately-stopped members alone", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-keep-not");
    const { memberId } = await spawnWorker(swarmId, "w1");
    const before = (await rt().core.store.getMemberById(memberId))!;
    await stopWorker(memberId);
    const res = await tool.swarm_revive.execute({ swarmId, action: "revive", strategy: "keep" }, ctx("ses-lead"));
    const out = String(res.output ?? res);
    const after = (await rt().core.store.getMemberById(memberId))!;
    expect(out).not.toContain("w1: respawned");
    expect(after.sessionId).toBe(before.sessionId);
    expect(after.status).toBe("stopped");
  });
});

describe("swarm_revive retask", () => {
  test("repoint keeps agents, cancels old in-flight tasks, seeds new DAG, notifies crew", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-repoint");
    const { memberId } = await spawnWorker(swarmId, "w1");
    const wm = (await rt().core.store.getMemberById(memberId))!;
    // Old mission: a claimed task in flight.
    const oldTaskId = await createReadyTask(swarmId, "old mission", wm.id);
    await rt().core.store.claimTask(oldTaskId, memberId, 60_000);

    const res = await tool.swarm_revive.execute(
      {
        swarmId,
        action: "retask",
        strategy: "repoint",
        tasks: [
          { id: "n1", title: "new mission alpha", priority: 1 },
          { id: "n2", title: "new mission beta", dependsOn: ["n1"] },
        ],
      },
      ctx("ses-lead"),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("strategy=repoint");
    expect(out).toContain("cancelled old in-flight tasks: 1");
    expect(out).toContain("seeded new tasks: 2");
    expect(out).toContain("crew notified of new mission: 1");
    // Old task cancelled; new tasks exist with the DAG.
    const tasks = await rt().core.store.listTasks(swarmId);
    expect(tasks.find((t) => t.id === oldTaskId)!.status).toBe("cancelled");
    expect(tasks.some((t) => t.id === "n1" && t.status !== "cancelled")).toBe(true);
    const deps = await rt().core.store.listTaskDependencies(swarmId);
    expect(deps.some((d) => d.taskId === "n2" && d.dependsOnTaskId === "n1")).toBe(true);
    // The kept member got the handoff notification.
    const msgs = await rt().core.store.listMessagesBySwarm(swarmId, 50);
    expect(msgs.some((m) => m.to.memberId === memberId && m.body.text.includes("new mission"))).toBe(true);
  });

  test("fresh requires confirm and spawns new agents, stopping old ones", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-fresh");
    const { memberId } = await spawnWorker(swarmId, "w1");

    // No confirm -> refused.
    const refused = await tool.swarm_revive.execute(
      { swarmId, action: "retask", strategy: "fresh", members: [{ name: "w2", role: "worker" }], tasks: [{ id: "f1", title: "fresh mission" }] },
      ctx("ses-lead"),
    );
    expect(String(refused.output ?? refused)).toContain("REQUIRES confirm");

    // Wrong confirm -> refused.
    const wrong = await tool.swarm_revive.execute(
      { swarmId, action: "retask", strategy: "fresh", confirm: "nope", members: [{ name: "w2", role: "worker" }] },
      ctx("ses-lead"),
    );
    expect(String(wrong.output ?? wrong)).toContain("confirm mismatch");

    // Correct confirm -> old worker stopped, new agent spawned with default model.
    const res = await tool.swarm_revive.execute(
      {
        swarmId,
        action: "retask",
        strategy: "fresh",
        confirm: "rev-fresh",
        tasks: [{ id: "f1", title: "fresh mission" }],
        members: [{ name: "w2", role: "worker" }],
      },
      ctx("ses-lead"),
    );
    const out = String(res.output ?? res);
    expect(out).toContain("strategy=fresh");
    expect(out).toContain("stopped old workers: 1");
    expect(out).toContain("spawned w2");
    expect(out).toContain("model opencode-go/deepseek-v4-flash");
    const w1 = (await rt().core.store.getMemberById(memberId))!;
    expect(w1.status).toBe("stopped");
    const w2 = await rt().core.store.getMemberByName(swarmId, "w2");
    expect(w2).toBeDefined();
    expect(w2!.model?.modelID).toBe("deepseek-v4-flash");
  });

  test("retask is coordinator-only", async () => {
    await initPlugin();
    const swarmId = await createSwarm("rev-retask-only");
    const { sessionId } = await spawnWorker(swarmId, "w1");
    const res = await tool.swarm_revive.execute(
      { swarmId, action: "retask", strategy: "repoint", tasks: [{ title: "x" }] },
      ctx(sessionId),
    );
    expect(String(res.output ?? res)).toContain("only the coordinator");
  });
});
