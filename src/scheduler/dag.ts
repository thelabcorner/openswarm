import type { SwarmTask, TaskStatus } from "../core/types.js";
import type { TaskDependency } from "../storage/models.js";

export type TaskGraph = Map<string, string[]>;

/**
 * Affinity score of a member (by name/role tokens) against a task's text.
 * Used by the scheduler to hand a ready task to the specialist whose role best
 * matches it (e.g. the editor for "Combine haikus"), instead of the
 * alphabetically-first idle peer. 0 = no affinity (fallback order).
 */
export function affinityScore(name: string, role: string, taskText: string): number {
  if (!taskText) return 0;
  const tokens = [
    ...new Set(
      [...name.toLowerCase().split(/[^a-z0-9]+/), ...role.toLowerCase().split(/[^a-z0-9]+/)]
        .filter((t) => t.length >= 3),
    ),
  ];
  const words = taskText.toLowerCase().split(/\b/);
  let score = 0;
  for (const tok of tokens) {
    if (words.includes(tok)) score += 4; // whole-token match — strong
    else if (taskText.toLowerCase().includes(tok)) score += 1; // substring — weak
  }
  return score;
}

/** Build adjacency list of task -> its dependents (tasks blocked by it). */
export function buildDependents(
  tasks: Array<Pick<SwarmTask, "id">>,
  deps: TaskDependency[],
): TaskGraph {
  const graph: TaskGraph = new Map();
  for (const t of tasks) graph.set(t.id, []);
  for (const d of deps) {
    const list = graph.get(d.dependsOnTaskId);
    if (list) list.push(d.taskId);
  }
  return graph;
}

/** Build adjacency list of task -> its prerequisites (tasks it depends on). */
export function buildPrerequisites(deps: TaskDependency[]): TaskGraph {
  const graph: TaskGraph = new Map();
  for (const d of deps) {
    const list = graph.get(d.taskId) ?? [];
    list.push(d.dependsOnTaskId);
    graph.set(d.taskId, list);
  }
  return graph;
}

/**
 * Detect cycles in the dependency graph. Returns the cycle path if one
 * exists, else null. Uses DFS with three colors.
 */
export function detectCycle(
  taskIds: string[],
  deps: TaskDependency[],
): string[] | null {
  const adj: TaskGraph = new Map();
  for (const id of taskIds) adj.set(id, []);
  for (const d of deps) {
    const list = adj.get(d.taskId) ?? [];
    list.push(d.dependsOnTaskId);
    adj.set(d.taskId, list);
  }

  const WHITE = 0;
  const GRAY = 1;
  const BLACK = 2;
  const color = new Map<string, number>();
  for (const id of taskIds) color.set(id, WHITE);

  const stack: string[] = [];

  function visit(node: string): string[] | null {
    color.set(node, GRAY);
    stack.push(node);
    for (const next of adj.get(node) ?? []) {
      if (!adj.has(next)) continue;
      const c = color.get(next) ?? WHITE;
      if (c === GRAY) {
        const cycleStart = stack.indexOf(next);
        return stack.slice(cycleStart);
      }
      if (c === WHITE) {
        const cycle = visit(next);
        if (cycle) return cycle;
      }
    }
    stack.pop();
    color.set(node, BLACK);
    return null;
  }

  for (const id of taskIds) {
    if ((color.get(id) ?? WHITE) === WHITE) {
      const cycle = visit(id);
      if (cycle) return cycle;
    }
  }
  return null;
}

const TERMINAL: ReadonlySet<TaskStatus> = new Set([
  "completed",
  "failed",
  "cancelled",
]);

/** Is this task's own status one that stops scheduling (terminal or active)? */
function isSchedulable(status: TaskStatus): boolean {
  return status === "pending" || status === "blocked" || status === "ready";
}

/**
 * Recompute task readiness from dependency state. Returns a map of
 * taskId -> computed status. Deterministic; no LLM.
 */
export function recomputeReadiness(
  tasks: Array<Pick<SwarmTask, "id" | "status">>,
  deps: TaskDependency[],
  isCompleted: (id: string) => boolean,
): Map<string, TaskStatus> {
  const out = new Map<string, TaskStatus>();
  for (const t of tasks) {
    if (!isSchedulable(t.status)) {
      out.set(t.id, t.status);
      continue;
    }
    const prereqs = deps.filter((d) => d.taskId === t.id).map((d) => d.dependsOnTaskId);
    if (prereqs.length === 0) {
      out.set(t.id, "ready");
      continue;
    }
    const allDone = prereqs.every(isCompleted);
    out.set(t.id, allDone ? "ready" : "blocked");
  }
  return out;
}

/** Determine if a task is currently runnable given concurrency limits. */
export function isRunnable(
  task: Pick<SwarmTask, "status">,
  activeCount: number,
  maxConcurrent: number,
): boolean {
  return task.status === "ready" && activeCount < maxConcurrent;
}