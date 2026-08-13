import { describe, expect, test, afterAll, beforeAll } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import { CONFIRM_CLEAR, CONFIRM_STOP, CONFIRM_NUKE } from "../../src/emergency/killswitch.js";

/**
 * Emergency kill switch tests (t-emergency): the layered freeze/stop/nuke
 * shutdown, the operator-only clear, and the automatic tripwires (spawn rate,
 * message rate, task rate, member cap). Harness mirrors model-selection.test.ts
 * (fake client + swarmPlugin + initPlugin with dispose).
 */

let dirs: string[] = [];

function makeClient(record: { promptAsync?: string[] } = {}) {
  return {
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
      promptAsync: async (o: any) => {
        const text = o?.body?.parts?.[0]?.text ?? o?.text ?? "";
        record.promptAsync?.push(text);
        return { data: undefined, error: undefined };
      },
    },
  };
}

const pluginInput = (client: unknown): any => ({
  client,
  project: { id: "proj-emergency" },
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
let promptAsyncLog: string[];
let dir: string;

/** The SwarmPluginRuntime singleton is lazily initialized by the first tool
 * call — fetch it at use time (never at plugin-init time). */
function getRt(): any {
  const rt = swarmRuntime();
  if (!rt) throw new Error("swarmRuntime is not initialized — a tool call must precede getRt()");
  return rt;
}

async function initPlugin(opts: Record<string, unknown> = {}): Promise<void> {
  disposeSwarmRuntime();
  promptAsyncLog = [];
  dir = mkdtempSync(join(tmpdir(), "swarms-emergency-"));
  dirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient({ promptAsync: promptAsyncLog })), { dataDir: dir, ...opts });
  tool = hooks.tool ?? {};
}

/** Pre-write the emergency state file BEFORE init (the auto-tripwire tests
 * configure low limits this way — the file is read at plugin init). */
function writeStateFile(partial: Record<string, unknown>): void {
  const swarmsDir = `${dir}/.opencode/swarms`;
  mkdirSync(swarmsDir, { recursive: true });
  const state = {
    tripped: false,
    level: null,
    trippedAt: null,
    trippedBy: "",
    tripwires: { maxSpawnsPerMin: 500, maxMessagesPerMin: 1000, maxMembers: 1000, maxTasksPerMin: 500 },
    ...partial,
  };
  writeFileSync(`${swarmsDir}/emergency.json`, JSON.stringify(state, null, 2), "utf8");
}

async function createSwarm(name: string): Promise<string> {
  const created = await tool.swarm_create.execute({ name }, ctx("ses-lead"));
  return JSON.parse(String(created.output ?? created)).swarm.id;
}

async function spawn(swarmId: string, name: string): Promise<any> {
  const res = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name, role: "worker" }] },
    ctx("ses-lead"),
  );
  return res.output ?? res;
}

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

describe("emergency kill switch — manual trip/clear (the feature)", () => {
  test("swarm_emergency tool is registered; status shows ALL CLEAR by default", async () => {
    await initPlugin();
    expect(typeof tool.swarm_emergency).toBe("object");
    const swarmId = await createSwarm("em-status");
    await spawn(swarmId, "worker");
    const res = await tool.swarm_emergency.execute({ swarmId, action: "status" }, ctx("ses-lead"));
    const text = String(res.output ?? res);
    expect(text).toContain("ALL CLEAR");
    expect(text).toContain("maxSpawnsPerMin");
    expect(text).toContain("swarms: 1");
  });

  test("freeze: worker trip attempt rejected; coordinator freeze pauses swarm, spawn refuses, scheduler no-op, broker delivers 0", async () => {
    await initPlugin();
    const swarmId = await createSwarm("em-freeze");
    await spawn(swarmId, "worker");

    // Worker (non-coordinator) cannot trip.
    const workerMember = await getRt().store.getMemberByName(swarmId, "worker");
    const denied = await tool.swarm_emergency.execute({ swarmId, action: "trip", level: "freeze" }, ctx(workerMember!.sessionId));
    expect(String(denied.output ?? denied)).toContain("operator-gated");

    // Coordinator trips freeze (no confirm needed for freeze).
    const tripped = await tool.swarm_emergency.execute({ swarmId, action: "trip", level: "freeze", reason: "runaway" }, ctx("ses-lead"));
    expect(String(tripped.output ?? tripped)).toContain("EMERGENCY FROZEN");
    expect(String(tripped.output ?? tripped)).toContain("paused 1 swarm");

    // Swarm is paused.
    const swarm = await getRt().store.getSwarm(swarmId);
    expect(swarm.status).toBe("paused");

    // Spawn refused with the emergency message (not a JSON error).
    const refused = await spawn(swarmId, "worker2");
    expect(String(refused)).toContain("EMERGENCY STOP ACTIVE (freeze)");
    expect(String(refused)).toContain(CONFIRM_CLEAR);

    // Scheduler is a no-op: a ready task stays ready.
    const worker2 = await getRt().store.getMemberByName(swarmId, "worker");
    const task = await getRt().core.createTask({ swarmId, title: "em-task", createdByMemberId: worker2!.id });
    await getRt().store.updateTaskStatus(task.id, "ready");
    await getRt().runScheduler(swarmId);
    const after = await getRt().store.getTaskById?.(task.id) ?? (await getRt().store.listTasks(swarmId)).find((t: any) => t.id === task.id);
    expect(after.status).toBe("ready");

    // Broker delivery halts: a message to the worker stays queued, 0 delivered.
    const worker3 = await getRt().store.getMemberByName(swarmId, "worker");
    await getRt().core.sendMessage({ swarmId, fromSessionId: "ses-lead", to: "worker", kind: "message", message: "ping while frozen" });
    const delivered = await getRt().broker.deliverToIdleMember(worker3!.id, worker3!.sessionId);
    expect(delivered).toBe(0);
  });

  test("clear requires the EXACT confirm; wrong confirm rejected; right confirm resumes + spawn works again", async () => {
    await initPlugin();
    const swarmId = await createSwarm("em-clear");
    await spawn(swarmId, "worker");
    await tool.swarm_emergency.execute({ swarmId, action: "trip", level: "freeze" }, ctx("ses-lead"));

    // Wrong confirm.
    const wrong = await tool.swarm_emergency.execute({ swarmId, action: "clear", confirm: "RESUME" }, ctx("ses-lead"));
    expect(String(wrong.output ?? wrong)).toContain("EXACT string");

    // Correct confirm.
    const cleared = await tool.swarm_emergency.execute({ swarmId, action: "clear", confirm: CONFIRM_CLEAR }, ctx("ses-lead"));
    expect(String(cleared.output ?? cleared)).toContain("EMERGENCY CLEARED");
    const swarm = await getRt().store.getSwarm(swarmId);
    expect(swarm.status).toBe("active");

    // Spawn works again.
    const spawned = await spawn(swarmId, "worker2");
    expect(String(spawned)).not.toContain("EMERGENCY");
  });

  test("stop requires confirm 'STOP ALL'; stops workers + cancels non-terminal tasks", async () => {
    await initPlugin();
    const swarmId = await createSwarm("em-stop");
    await spawn(swarmId, "worker");
    const worker = await getRt().store.getMemberByName(swarmId, "worker");
    const task = await getRt().core.createTask({ swarmId, title: "em-stop-task", createdByMemberId: worker!.id });
    await getRt().store.updateTaskStatus(task.id, "working");

    // Missing confirm.
    const missing = await tool.swarm_emergency.execute({ swarmId, action: "trip", level: "stop" }, ctx("ses-lead"));
    expect(String(missing.output ?? missing)).toContain("STOP ALL");

    // Correct confirm.
    const stopped = await tool.swarm_emergency.execute({ swarmId, action: "trip", level: "stop", confirm: CONFIRM_STOP }, ctx("ses-lead"));
    expect(String(stopped.output ?? stopped)).toContain("EMERGENCY STOPPED");
    expect(String(stopped.output ?? stopped)).toContain("stopped 1 worker");
    expect(String(stopped.output ?? stopped)).toContain("cancelled 1 task");

    const member = await getRt().store.getMemberById(worker!.id);
    expect(member.status).toBe("stopped");
    const taskAfter = (await getRt().store.listTasks(swarmId)).find((t: any) => t.id === task.id);
    expect(taskAfter.status).toBe("cancelled");
  });

  test("nuke requires confirm 'NUKE ALL'; deletes all swarms", async () => {
    await initPlugin();
    const a = await createSwarm("em-nuke-a");
    // A second swarm (multi-own allows it to reuse a session, but a distinct
    // session keeps this test isolated).
    const createdB = await tool.swarm_create.execute({ name: "em-nuke-b" }, ctx("ses-nuke-b"));
    const b = JSON.parse(String(createdB.output ?? createdB)).swarm.id;
    await spawn(a, "worker");

    const missing = await tool.swarm_emergency.execute({ swarmId: a, action: "trip", level: "nuke" }, ctx("ses-lead"));
    expect(String(missing.output ?? missing)).toContain("NUKE ALL");

    const nuked = await tool.swarm_emergency.execute({ swarmId: a, action: "trip", level: "nuke", confirm: CONFIRM_NUKE }, ctx("ses-lead"));
    expect(String(nuked.output ?? nuked)).toContain("deleted 2 swarm");
    const remaining = await getRt().store.listAllMemberSwarmIds();
    expect(remaining.length).toBe(0);
  });
});

describe("emergency kill switch — automatic tripwires", () => {
  test("spawn-rate tripwire: low maxSpawnsPerMin in state file → N+1 spawns auto-freeze + coordinator notice ONCE", async () => {
    // Configure a low limit BEFORE init via the state file (read at plugin init).
    disposeSwarmRuntime();
    promptAsyncLog = [];
    dir = mkdtempSync(join(tmpdir(), "swarms-emergency-trip-"));
    dirs.push(dir);
    writeStateFile({ tripwires: { maxSpawnsPerMin: 2, maxMessagesPerMin: 1000, maxMembers: 1000, maxTasksPerMin: 500 } });
    hooks = await swarmPlugin(pluginInput(makeClient({ promptAsync: promptAsyncLog })), { dataDir: dir });
    tool = hooks.tool ?? {};

    const swarmId = await createSwarm("em-trip");
    // 2 spawns are fine (limit = 2); the 3rd exceeds → auto-freeze + refused.
    await spawn(swarmId, "w1");
    await spawn(swarmId, "w2");
    const third = await spawn(swarmId, "w3");
    expect(String(third)).toContain("EMERGENCY STOP ACTIVE");
    expect(getRt().emergency.tripped).toBe(true);
    expect(getRt().emergency.level).toBe("freeze");
    expect(getRt().emergency.state.reason).toContain("spawn rate exceeded");

    // A 4th spawn refuses too.
    const fourth = await spawn(swarmId, "w4");
    expect(String(fourth)).toContain("EMERGENCY STOP ACTIVE");

    // Coordinator notice fired exactly once.
    const notices = promptAsyncLog.filter((t) => t.includes("[EMERGENCY] swarm auto-froze"));
    expect(notices.length).toBe(1);
    expect(notices[0]).toContain(CONFIRM_CLEAR);

    // Not deduped-spam: another trip attempt (already tripped) does not add notices.
    await tool.swarm_emergency.execute({ swarmId, action: "trip", level: "freeze" }, ctx("ses-lead"));
    expect(promptAsyncLog.filter((t) => t.includes("[EMERGENCY] swarm auto-froze")).length).toBe(1);
  });

  test("hard member cap: state-file maxMembers reached → spawn refused outright AND tripped", async () => {
    disposeSwarmRuntime();
    promptAsyncLog = [];
    dir = mkdtempSync(join(tmpdir(), "swarms-emergency-cap-"));
    dirs.push(dir);
    // Cap of 2: the coordinator (1 row) + one worker is allowed, the next refuses.
    writeStateFile({ tripwires: { maxSpawnsPerMin: 500, maxMessagesPerMin: 1000, maxMembers: 2, maxTasksPerMin: 500 } });
    hooks = await swarmPlugin(pluginInput(makeClient({ promptAsync: promptAsyncLog })), { dataDir: dir });
    tool = hooks.tool ?? {};

    const swarmId = await createSwarm("em-cap");
    // First spawn: coordinator row (1) + 1 < 2 → allowed.
    const ok = await spawn(swarmId, "w1");
    expect(String(ok)).not.toContain("EMERGENCY");
    // Second spawn: 2 >= 2 → refused + tripped.
    const refused = await spawn(swarmId, "w2");
    expect(String(refused)).toContain("EMERGENCY STOP ACTIVE");
    expect(getRt().emergency.tripped).toBe(true);
    expect(getRt().emergency.state.reason).toContain("member cap");
  });

  test("state persists across dispose/re-init (durable JSON file)", async () => {
    await initPlugin();
    const swarmId = await createSwarm("em-persist");
    await spawn(swarmId, "worker");
    await tool.swarm_emergency.execute({ swarmId, action: "trip", level: "freeze" }, ctx("ses-lead"));

    // The JSON file is on disk.
    const file = `${dir}/.opencode/swarms/emergency.json`;
    const raw = JSON.parse(readFileSync(file, "utf8"));
    expect(raw.tripped).toBe(true);

    // Re-init: a fresh runtime must still be tripped (spawn refuses). The
    // singleton is lazily initialized by the first tool call, so drive status
    // first — it also proves the tripped state is readable after restart.
    disposeSwarmRuntime();
    promptAsyncLog = [];
    hooks = await swarmPlugin(pluginInput(makeClient({ promptAsync: promptAsyncLog })), { dataDir: dir });
    tool = hooks.tool ?? {};
    const status = await tool.swarm_emergency.execute({ swarmId, action: "status" }, ctx("ses-lead"));
    expect(String(status.output ?? status)).toContain("TRIPPED");
    expect(getRt().emergency.tripped).toBe(true);
    const refused = await spawn(swarmId, "late-worker");
    expect(String(refused)).toContain("EMERGENCY STOP ACTIVE");
  });

  test("task-rate and message-rate guards are wired without breaking normal use", async () => {
    await initPlugin();
    const swarmId = await createSwarm("em-msgs");
    await spawn(swarmId, "worker");
    // Normal sends/tasks under the generous defaults do NOT trip.
    for (let i = 0; i < 5; i++) {
      await tool.swarm_message.execute({ swarmId, to: "worker", kind: "message", message: `m${i}` }, ctx("ses-lead"));
    }
    expect(getRt().emergency.tripped).toBe(false);
    const task = await tool.swarm_tasks.execute({ swarmId, action: "list" }, ctx("ses-lead"));
    expect(task).toBeTruthy();
  });
});
