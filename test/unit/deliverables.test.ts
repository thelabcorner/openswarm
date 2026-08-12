import { describe, expect, test, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime, disposeSwarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";

/**
 * Handoff ledger (deliverables) tests — the `swarm_deliverables` tool:
 *
 *   a. a kind:'handoff' message auto-creates the ledger row (summary, refs,
 *      @file: artifact paths) with verdict OPEN.
 *   b. list renders rich rows: `#N [OPEN] (age) by member — task: title —
 *      summary (FENCED) — refs — files`, names resolved.
 *   c. verdict flow: a worker's verdict attempt is coordinator-only denied; the
 *      coordinator's rejected/accepted verdicts leave the OPEN set, are final
 *      (no double-verdict), and are recorded in the event stream
 *      (deliverable.verdict) so the timeline reflects them.
 *   d. filters: member (by name, case-insensitive), taskId, verdict, limit.
 *   e. cross-swarm deliverable bus: a member of swarm A reads swarm B's ledger
 *      by passing B's id or name (read-only; verdicting stays with B's
 *      coordinator — even a FOREIGN coordinator is denied).
 *   f. unknown swarmId errors cleanly; a non-member session is rejected.
 */

let dirs: string[] = [];

function makeClient() {
  return {
    config: {
      providers: async () => ({
        data: {
          providers: [{ id: "opencode-go", models: { "deepseek-v4-flash": { name: "DeepSeek V4 Flash" } } }],
        },
        error: undefined,
      }),
    },
    session: {
      create: async (o: any) => ({
        data: { id: `ses-${Math.random().toString(36).slice(2, 8)}`, title: o?.body?.title, parentID: undefined, directory: "." },
        error: undefined,
      }),
      get: async (o: any) => ({
        data: { id: o?.path?.id, title: "t", model: undefined, directory: "." },
        error: undefined,
      }),
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
  project: { id: "proj-dlv" },
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
  const dir = mkdtempSync(join(tmpdir(), "swarms-dlv-"));
  dirs.push(dir);
  hooks = await swarmPlugin(pluginInput(makeClient()), { dataDir: dir });
  tool = hooks.tool ?? {};
}

async function createSwarm(name: string, sessionID: string): Promise<{ id: string; name: string }> {
  const res = await tool.swarm_create.execute({ name }, ctx(sessionID));
  const json = JSON.parse(String(res.output ?? res));
  return json.swarm;
}

async function spawn(swarmId: string, name: string, sessionID: string): Promise<{ memberId: string; sessionId: string; name: string }> {
  const res = await tool.swarm_spawn.execute(
    { swarmId, members: [{ name, role: "worker" }] },
    ctx(sessionID),
  );
  const json = JSON.parse(String(res.output ?? res));
  return json.spawned[0];
}

async function handoff(swarmId: string, member: { sessionId: string }, message: string, extra: Record<string, unknown> = {}): Promise<void> {
  await tool.swarm_message.execute(
    { swarmId, to: "*", kind: "handoff", message, ...extra },
    ctx(member.sessionId),
  );
}

describe("handoff ledger — auto-recording", () => {
  test("a handoff send auto-creates the ledger row (summary/refs/files) with verdict OPEN", async () => {
    await initPlugin();
    const swarmId = (await createSwarm("dlv-auto", "ses-da")).id;
    const w = await spawn(swarmId, "worker-da", "ses-da");
    await handoff(swarmId, w, "Built the ledger @file:src/ledger.ts", { refs: ["msg-ref-1"] });

    const rt = swarmRuntime()!;
    const rows = await rt.store.listDeliverables(swarmId);
    expect(rows.length).toBe(1);
    const d = rows[0]!;
    expect(d.swarmId).toBe(swarmId);
    expect(d.memberId).toBe(w.memberId);
    expect(d.summary).toContain("Built the ledger");
    expect(d.refs).toEqual(["msg-ref-1"]);
    expect(d.files).toEqual(["src/ledger.ts"]);
    expect(d.verdict).toBeNull();
  });

  test("list renders open entries with a fenced summary and resolved names", async () => {
    await initPlugin();
    const swarmId = (await createSwarm("dlv-render", "ses-dr")).id;
    const w = await spawn(swarmId, "worker-dr", "ses-dr");
    await handoff(swarmId, w, "Handoff complete", { refs: ["msg-1"] });

    const res = await tool.swarm_deliverables.execute({ swarmId, action: "list" }, ctx(w.sessionId));
    const out = String(res.output ?? res);
    expect(out).toContain("deliverable ledger (1):");
    expect(out).toContain("#1 [OPEN]");
    expect(out).toMatch(/\(\d+s\)/); // age
    expect(out).toContain("by worker-dr"); // author name resolved
    expect(out).toContain("task: (none)");
    expect(out).toContain("[DATA"); // summary is FENCED (untrusted content)
    expect(out).toContain("Handoff complete");
    expect(out).toContain("refs: msg-1");
    expect(out).toContain("files: (none)");
    expect(out).toContain(`id: ${(await swarmRuntime()!.store.listDeliverables(swarmId))[0]!.id}`);
  });
});

describe("handoff ledger — verdict flow", () => {
  test("worker denied; coordinator rejected/accepted leaves the OPEN set, is final, and records an event", async () => {
    await initPlugin();
    const swarmId = (await createSwarm("dlv-verdict", "ses-dv")).id;
    const w = await spawn(swarmId, "worker-dv", "ses-dv");
    await handoff(swarmId, w, "first handoff", { refs: ["r1"] });
    await handoff(swarmId, w, "second handoff", { refs: ["r2"] });

    const rt = swarmRuntime()!;
    const rows = await rt.store.listDeliverables(swarmId);
    expect(rows.length).toBe(2);
    const first = rows.find((r) => r.summary === "first handoff")!;
    const second = rows.find((r) => r.summary === "second handoff")!;

    // Worker cannot verdict — coordinator-only.
    const denied = await tool.swarm_deliverables.execute(
      { swarmId, action: "verdict", deliverableId: first.id, verdict: "accepted" },
      ctx(w.sessionId),
    );
    expect(String(denied.output ?? denied)).toContain("coordinator-only");

    // Coordinator rejects the first → the row leaves the OPEN set.
    const rejected = await tool.swarm_deliverables.execute(
      { swarmId, action: "verdict", deliverableId: first.id, verdict: "rejected" },
      ctx("ses-dv"),
    );
    expect(String(rejected.output ?? rejected)).toContain("deliverable rejected — recorded in the ledger");

    // Verdicts are final — a second verdict is refused.
    const again = await tool.swarm_deliverables.execute(
      { swarmId, action: "verdict", deliverableId: first.id, verdict: "accepted" },
      ctx("ses-dv"),
    );
    expect(String(again.output ?? again)).toContain("verdicts are final");

    // The row now renders [rejected] — the other is still [OPEN].
    const list = String(
      (await tool.swarm_deliverables.execute({ swarmId, action: "list" }, ctx("ses-dv"))).output ?? "",
    );
    expect(list).toContain("[rejected]");
    expect(list).toContain("[OPEN]");

    // The verdict is recorded in the event stream (timeline).
    const events = await rt.store.listEventsForEntity(swarmId, "deliverable", first.id);
    expect(events.length).toBe(1);
    expect(events[0]!.type).toBe("deliverable.verdict");
    expect(JSON.parse(events[0]!.payloadJson ?? "{}")).toEqual({ verdict: "rejected" });

    // Coordinator accepts the second.
    const accepted = await tool.swarm_deliverables.execute(
      { swarmId, action: "verdict", deliverableId: second.id, verdict: "accepted" },
      ctx("ses-dv"),
    );
    expect(String(accepted.output ?? accepted)).toContain("deliverable accepted — recorded in the ledger");

    // Verdict-filtered lists see the right halves of the ledger.
    const rejList = String(
      (await tool.swarm_deliverables.execute({ swarmId, action: "list", verdict: "rejected" }, ctx("ses-dv"))).output ?? "",
    );
    expect(rejList).toContain("first handoff");
    expect(rejList).not.toContain("second handoff");
    const accList = String(
      (await tool.swarm_deliverables.execute({ swarmId, action: "list", verdict: "accepted" }, ctx("ses-dv"))).output ?? "",
    );
    expect(accList).toContain("second handoff");
    expect(accList).not.toContain("first handoff");
  });
});

describe("handoff ledger — filters", () => {
  test("member (case-insensitive), taskId, verdict and limit filters; task titles resolved", async () => {
    await initPlugin();
    const swarmId = (await createSwarm("dlv-filters", "ses-df")).id;
    const a = await spawn(swarmId, "worker-a", "ses-df");
    const b = await spawn(swarmId, "worker-b", "ses-df");

    // Seed a task so the render shows its title (task: <title>).
    const rt = swarmRuntime()!;
    const coord = (await rt.store.listMembers(swarmId)).find((m) => m.role === "coordinator")!;
    await rt.store.insertTask({
      id: "t-dlv-1",
      swarmId,
      title: "Build the ledger",
      status: "ready",
      priority: 5,
      createdByMemberId: coord.id,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });

    await handoff(swarmId, a, "worker a's handoff", { taskId: "t-dlv-1" });
    await handoff(swarmId, b, "worker b's handoff", { taskId: "t-dlv-1" });
    await handoff(swarmId, a, "worker a's second", {});

    // member filter resolves by NAME, case-insensitively.
    const aList = String(
      (await tool.swarm_deliverables.execute({ swarmId, action: "list", member: "WORKER-A" }, ctx(a.sessionId))).output ?? "",
    );
    expect(aList).toContain("worker a's handoff");
    expect(aList).toContain("worker a's second");
    expect(aList).not.toContain("worker b's handoff");

    // taskId filter — and the task title is resolved in the render.
    const tList = String(
      (await tool.swarm_deliverables.execute({ swarmId, action: "list", taskId: "t-dlv-1" }, ctx(a.sessionId))).output ?? "",
    );
    expect(tList).toContain("worker a's handoff");
    expect(tList).toContain("worker b's handoff");
    expect(tList).not.toContain("worker a's second");
    expect(tList).toContain("task: Build the ledger");

    // limit caps the rows.
    const one = String(
      (await tool.swarm_deliverables.execute({ swarmId, action: "list", limit: 1 }, ctx(a.sessionId))).output ?? "",
    );
    expect(one).toContain("#1 [OPEN]");
    expect(one).not.toContain("#2");

    // Unknown member name → honest empty result.
    const nope = String(
      (await tool.swarm_deliverables.execute({ swarmId, action: "list", member: "ghost" }, ctx(a.sessionId))).output ?? "",
    );
    expect(nope).toContain("no member 'ghost'");
  });
});

describe("cross-swarm deliverable bus", () => {
  test("a swarm-A member lists swarm B's ledger by id or name; verdicting stays with B's coordinator", async () => {
    await initPlugin();
    const swarmA = await createSwarm("dlv-bus-a", "ses-ba");
    const swarmB = await createSwarm("dlv-bus-b", "ses-bb");
    const alice = await spawn(swarmA.id, "alice", "ses-ba");
    const bob = await spawn(swarmB.id, "bob", "ses-bb");
    await handoff(swarmB.id, bob, "bob's deliverable for B");
    const dlvId = (await swarmRuntime()!.store.listDeliverables(swarmB.id))[0]!.id;

    // alice (member of swarm A) reads swarm B's ledger — by id and by name.
    const byId = String(
      (await tool.swarm_deliverables.execute({ swarmId: swarmB.id, action: "list" }, ctx(alice.sessionId))).output ?? "",
    );
    expect(byId).toContain("bob's deliverable for B");
    expect(byId).toContain("by bob");

    const byName = String(
      (await tool.swarm_deliverables.execute({ swarmId: swarmB.name, action: "list" }, ctx(alice.sessionId))).output ?? "",
    );
    expect(byName).toContain("bob's deliverable for B");

    // Read-only: alice (a worker) cannot verdict B's deliverables.
    const denied = await tool.swarm_deliverables.execute(
      { swarmId: swarmB.id, action: "verdict", deliverableId: dlvId, verdict: "accepted" },
      ctx(alice.sessionId),
    );
    expect(String(denied.output ?? denied)).toContain("coordinator-only");

    // Even a FOREIGN coordinator cannot verdict another swarm's ledger.
    const foreign = await tool.swarm_deliverables.execute(
      { swarmId: swarmB.id, action: "verdict", deliverableId: dlvId, verdict: "accepted" },
      ctx("ses-ba"), // coordinator of swarm A
    );
    expect(String(foreign.output ?? foreign)).toContain("only the coordinator of this swarm may verdict");
  });
});

describe("handoff ledger — guards", () => {
  test("unknown swarmId errors cleanly; a non-member session is rejected", async () => {
    await initPlugin();
    const swarmId = (await createSwarm("dlv-guard", "ses-dg")).id;
    const w = await spawn(swarmId, "worker-dg", "ses-dg");

    await expect(
      tool.swarm_deliverables.execute({ swarmId: "dlv-unknown-swarm", action: "list" }, ctx(w.sessionId)),
    ).rejects.toThrow("no swarm found");

    const outsider = await tool.swarm_deliverables.execute({ swarmId, action: "list" }, ctx("ses-not-a-member"));
    expect(String(outsider.output ?? outsider)).toContain("calling session is not a swarm member");
  });
});

afterAll(async () => {
  disposeSwarmRuntime();
  for (const d of dirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});
