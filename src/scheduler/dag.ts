import type { SwarmTask, TaskStatus } from "../core/types.js";
import type { TaskDependency } from "../storage/models.js";

export type TaskGraph = Map<string, string[]>;

/** Generic low-information words stripped from affinity tokens — a bare
 * shared generic like 'task'/'the'/'swarm' must not produce affinity (it is
 * not a signal of who should own the work). */
const GENERIC_WORDS: ReadonlySet<string> = new Set([
  "task", "the", "and", "swarm", "feature", "a", "an", "for", "to", "of",
  "with", "this", "that", "from", "in", "on", "by", "it", "is", "are", "be",
  "do", "you", "your", "work", "member", "role", "team", "as", "at", "or",
  "all", "any", "each", "new", "one", "use", "using", "into", "over", "out",
]);

/** Minimum length for a token to be a significant affinity signal. */
const MIN_TOKEN_LENGTH = 3;

/** Tokenize text for affinity: lowercase, split on non-alphanumerics, drop
 * short and generic low-information tokens. Both member (name/role) and task
 * text go through this, so scoring compares SIGNIFICANT tokens only. */
export function affinityTokens(text: string): string[] {
  return text
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= MIN_TOKEN_LENGTH && !GENERIC_WORDS.has(t));
}

/** Strong verbatim signal: the member's FULL NAME appears as a whole unit
 * (word or hyphenated phrase) in the task text. Kept winning over token
 * accumulation — an exact-title match is the clearest "this task is for you"
 * signal and preserves the scheduler's original exact-match behavior. */
const VERBATIM_NAME_SCORE = 100;

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Affinity score of a member (by name/role tokens) against a task's text.
 * Used by the scheduler to hand a ready task to the specialist whose role best
 * matches it (e.g. the editor for "Combine haikus"), instead of the
 * alphabetically-first idle peer. 0 = no affinity (fallback order).
 *
 * Tighter scoring (scheduler-affinity fix): a bare substring or single shared
 * token is NOT affinity — at least 2 significant tokens in common (after
 * stripping generics like 'task'/'the'/'swarm') are required, and role tokens
 * carry a bonus (a role that literally describes the work beats a coincidental
 * name overlap). The member's exact name appearing verbatim in the task text
 * still wins outright (exact-title matches preserved).
 */
export function affinityScore(name: string, role: string, taskText: string): number {
  if (!taskText) return 0;
  const lower = taskText.toLowerCase();
  // Verbatim exact-name match — the strongest signal, kept winning.
  const namePhrase = name.toLowerCase().trim();
  if (
    namePhrase.length >= MIN_TOKEN_LENGTH &&
    new RegExp(`(^|[^a-z0-9])${escapeRegExp(namePhrase)}($|[^a-z0-9])`).test(lower)
  ) {
    return VERBATIM_NAME_SCORE;
  }
  const taskTokens = new Set(affinityTokens(lower));
  if (taskTokens.size === 0) return 0;
  const common = new Set<string>();
  let roleCommon = 0;
  for (const t of affinityTokens(name)) if (taskTokens.has(t)) common.add(t);
  for (const t of affinityTokens(role)) {
    if (taskTokens.has(t)) {
      common.add(t);
      roleCommon++;
    }
  }
  // Tighter scoring: at least 2 significant tokens in common, or 0. A single
  // shared token (e.g. every 'search-*' member sharing 'search') is not a
  // reliable ownership signal and caused the affinity misassignments.
  if (common.size < 2) return 0;
  let score = common.size * 4;
  score += roleCommon * 2; // role-match bonus: roles that describe the work rank higher
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