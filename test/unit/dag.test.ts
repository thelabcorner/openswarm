import { describe, expect, test } from "bun:test";
import { detectCycle, buildDependents, buildPrerequisites, recomputeReadiness, affinityScore } from "../../src/scheduler/dag.ts";
import { isChatting } from "../../src/scheduler/scheduler.ts";
import type { SwarmMember, Swarm } from "../../src/core/types.ts";

function member(humanChatAt: number | null | undefined): SwarmMember {
  return {
    id: "m1",
    swarmId: "s1",
    name: "m1",
    role: "worker",
    sessionId: "ses-1",
    status: "idle",
    workspaceMode: "worktree",
    humanChatAt: humanChatAt as any,
    createdAt: 0,
    updatedAt: 0,
  };
}

const swarm = { id: "s1", name: "s1", policies: { humanChatLullMs: 300_000 } } as unknown as Swarm;

describe("dag cycle detection", () => {
  test("empty graph has no cycle", () => {
    expect(detectCycle([], [])).toBeNull();
  });

  test("linear chain a->b->c has no cycle", () => {
    const deps = [
      { taskId: "b", dependsOnTaskId: "a" },
      { taskId: "c", dependsOnTaskId: "b" },
    ];
    expect(detectCycle(["a", "b", "c"], deps)).toBeNull();
  });

  test("direct cycle a<->b is detected", () => {
    const deps = [
      { taskId: "a", dependsOnTaskId: "b" },
      { taskId: "b", dependsOnTaskId: "a" },
    ];
    const cycle = detectCycle(["a", "b"], deps);
    expect(cycle).not.toBeNull();
    expect(cycle!.length).toBeGreaterThan(0);
  });

  test("self dependency is rejected as a cycle", () => {
    const deps = [{ taskId: "a", dependsOnTaskId: "a" }];
    expect(detectCycle(["a"], deps)).not.toBeNull();
  });

  test("indirect cycle a->b->c->a is detected and reports the path", () => {
    const deps = [
      { taskId: "b", dependsOnTaskId: "a" },
      { taskId: "c", dependsOnTaskId: "b" },
      { taskId: "a", dependsOnTaskId: "c" },
    ];
    const cycle = detectCycle(["a", "b", "c"], deps);
    expect(cycle).not.toBeNull();
    // The reported path contains each node exactly once and forms a cycle.
    expect(cycle!.length).toBe(3);
    for (const n of ["a", "b", "c"]) expect(cycle).toContain(n);
  });
});

describe("buildDependents / buildPrerequisites", () => {
  test("dependents maps a dependency to its dependants", () => {
    const tasks = [{ id: "a" }, { id: "b" }, { id: "c" }];
    const deps = [
      { taskId: "b", dependsOnTaskId: "a" },
      { taskId: "c", dependsOnTaskId: "a" },
    ];
    const g = buildDependents(tasks, deps);
    expect(g.get("a")).toEqual(["b", "c"]);
    expect(g.get("b")).toEqual([]);
  });

  test("prerequisites maps a task to the tasks it depends on", () => {
    const deps = [
      { taskId: "c", dependsOnTaskId: "a" },
      { taskId: "c", dependsOnTaskId: "b" },
    ];
    const g = buildPrerequisites(deps);
    expect(g.get("c")).toEqual(["a", "b"]);
  });
});

describe("recomputeReadiness", () => {
  const tasks: Array<{ id: string; status: "pending" | "completed" | "working" | "cancelled" }> = [
    { id: "research", status: "completed" },
    { id: "backend", status: "pending" },
    { id: "frontend", status: "pending" },
    { id: "integration", status: "pending" },
  ];

  test("task becomes ready when all deps complete", () => {
    const deps = [
      { taskId: "backend", dependsOnTaskId: "research" },
      { taskId: "integration", dependsOnTaskId: "backend" },
      { taskId: "integration", dependsOnTaskId: "frontend" },
    ];
    const completed = (id: string) => tasks.find((t) => t.id === id)?.status === "completed";
    const out = recomputeReadiness(tasks, deps, completed);
    expect(out.get("research")).toBe("completed");
    expect(out.get("backend")).toBe("ready");
    expect(out.get("frontend")).toBe("ready");
    expect(out.get("integration")).toBe("blocked");
  });

  test("task with unresolved deps stays blocked", () => {
    const deps = [
      { taskId: "integration", dependsOnTaskId: "backend" },
      { taskId: "integration", dependsOnTaskId: "frontend" },
    ];
    const completed = () => false;
    const out = recomputeReadiness(tasks, deps, completed);
    expect(out.get("integration")).toBe("blocked");
  });

  test("terminal and active statuses are preserved", () => {
    const t: Array<{ id: string; status: "working" | "cancelled" }> = [{ id: "x", status: "working" }, { id: "y", status: "cancelled" }];
    const out = recomputeReadiness(t, [], () => false);
    expect(out.get("x")).toBe("working");
    expect(out.get("y")).toBe("cancelled");
  });
});
describe("affinityScore", () => {
  test("editor role matches a combine task", () => {
    const gamma = affinityScore("gamma", "Editor. Completes t3 by combining alpha's and beta's haikus.", "Combine haikus");
    const alpha = affinityScore("alpha", "Haiku poet for the sea.", "Combine haikus");
    expect(gamma).toBeGreaterThan(alpha);
  });

  test("no affinity when nothing matches (fallback order preserved)", () => {
    expect(affinityScore("alpha", "poet", "garden weeding")).toBe(0);
  });

  test("name match beats no match", () => {
    const named = affinityScore("packager", "build engineer", "task for packager to run packaging");
    const generic = affinityScore("worker1", "general helper", "task for packager to run packaging");
    expect(named).toBeGreaterThan(generic);
  });
});

describe("isChatting (scheduler human-chat gate, D1)", () => {
  test("member with recent human message is chatting", () => {
    expect(isChatting(member(Date.now() - 60_000), swarm)).toBe(true);
  });

  test("member past the lull window is NOT chatting", () => {
    expect(isChatting(member(Date.now() - 400_000), swarm)).toBe(false);
  });

  test("member with no human chat is NOT chatting", () => {
    expect(isChatting(member(null), swarm)).toBe(false);
    expect(isChatting(member(undefined), swarm)).toBe(false);
  });
});
