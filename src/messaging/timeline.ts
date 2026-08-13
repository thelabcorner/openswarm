import type { SwarmEvent, SwarmMember, SwarmTask } from "../core/types.js";
import { fence } from "../core/fence.js";

/**
 * Swarm timeline / replay renderer.
 *
 * Renders the swarm's recorded event stream (swarm_event) as a RICH,
 * chronological (newest last) replay, grouped by time bucket ("just now",
 * "N min ago", "today", "yesterday", ...). Each line carries a readable human
 * sentence for the event type, the actor → entity trail, and any payload as a
 * FENCED untrusted-data line (payloads are peer-authored content — the fence
 * keeps an embedded "ignore previous instructions" visibly quoted data, never
 * a directive).
 *
 * Pure function: no I/O. The tool layer loads events + names and passes them
 * in. `opts.limit` (default 80) caps the number of rendered events — the NEWEST
 * `limit` events are kept (replay wants the recent picture).
 */

export interface TimelineContext {
  members: SwarmMember[];
  tasks: SwarmTask[];
}

export interface TimelineRenderOptions {
  /** Max events rendered (default 80) — keeps the NEWEST `limit` events. */
  limit?: number;
  /** Reference "now" for bucket/age labels (default Date.now(); tests pass a fixed value). */
  now?: number;
}

const DAY_MS = 86_400_000;

/** Relative time bucket for a timestamp. */
function bucketLabel(ts: number, now: number): string {
  const age = now - ts;
  if (age < 60_000) return "just now";
  if (age < 3_600_000) return `${Math.max(1, Math.floor(age / 60_000))} min ago`;
  const startOfDay = (t: number) => {
    const d = new Date(t);
    return new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  };
  if (startOfDay(ts) === startOfDay(now)) return "today";
  if (startOfDay(ts) === startOfDay(now) - DAY_MS) return "yesterday";
  if (age < 7 * DAY_MS) return `${Math.floor(age / DAY_MS)} days ago`;
  return new Date(ts).toISOString().slice(0, 10);
}

/** HH:MM:SS (local time) prefix for a timestamp. */
function clockTime(ts: number): string {
  const d = new Date(ts);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function memberName(ctx: TimelineContext, id?: string): string {
  if (!id) return "system";
  return ctx.members.find((m) => m.id === id)?.name ?? id;
}

/** Short entity label ("task t-1", "message msg_ab…", "member mem_x"). */
function entityLabel(ev: SwarmEvent): string {
  const type = ev.entityType ?? "entity";
  const id = (ev.entityId ?? "?").length > 30 ? `${(ev.entityId ?? "?").slice(0, 30)}…` : (ev.entityId ?? "?");
  return `${type} ${id}`;
}

/** Parse a small event payload safely (untrusted JSON). */
function payload(ev: SwarmEvent): Record<string, unknown> | undefined {
  if (!ev.payloadJson) return undefined;
  try {
    const parsed = JSON.parse(ev.payloadJson) as unknown;
    return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : undefined;
  } catch {
    return undefined;
  }
}

/** Task display: id + fenced title when resolvable (titles are untrusted). */
function taskRef(ctx: TimelineContext, taskId?: string): string {
  if (!taskId) return "?";
  const t = ctx.tasks.find((x) => x.id === taskId);
  return t ? `${taskId} (${fence(t.title)})` : taskId;
}

/**
 * Map an event type to a readable human sentence.
 * Actor names are resolved via the members map; task titles are fenced.
 */
function humanSentence(ev: SwarmEvent, ctx: TimelineContext): string {
  const actor = memberName(ctx, ev.actorMemberId);
  const p = payload(ev) ?? {};
  switch (ev.type) {
    case "message.sent": {
      const to = p.to === "*" ? "the swarm" : String(p.to ?? "?");
      const kind = String(p.kind ?? "message");
      const recipients = typeof p.recipients === "number" ? p.recipients : 0;
      const verb = kind === "message" ? "sent a message to" : `sent a ${kind} message to`;
      return `${actor} ${verb} ${to}${recipients > 1 ? ` (${recipients} recipients)` : ""}`;
    }
    case "message.replied":
      return `${actor} replied to message ${ev.entityId ?? "?"}`;
    case "member.spawned":
      return `${actor} spawned member '${String(p.name ?? "?")}'${p.role ? ` (${String(p.role)})` : ""}`;
    case "member.stopped":
      return p.name ? `${actor} stopped member '${String(p.name)}'` : `${actor} stopped a member`;
    case "member.idle":
      return `${actor} went idle`;
    case "member.respawned":
      return `${actor} was respawned after a restart${p.taskId ? ` (task ${String(p.taskId)})` : ""}`;
    case "member.model_changed": {
      const model = p.model as { providerID?: string; modelID?: string } | undefined;
      const modelText = model?.providerID && model?.modelID ? `${model.providerID}/${model.modelID}` : String(p.model ?? "?");
      return `${actor} changed the member's model to ${modelText}${p.auto ? " (auto — changed in the session)" : ""}`;
    }
    case "task.claimed":
      return `${actor} claimed task ${taskRef(ctx, ev.entityId)}`;
    case "task.completed":
      return `${actor} completed task ${taskRef(ctx, ev.entityId)}`;
    case "task.failed":
      return `${actor} failed task ${taskRef(ctx, ev.entityId)}`;
    case "task.cancelled":
      return `${actor} cancelled task ${taskRef(ctx, ev.entityId)}`;
    case "task.released":
      return `${actor} released task ${taskRef(ctx, ev.entityId)}${p.reason ? ` (${String(p.reason)})` : ""}`;
    case "task.reassigned": {
      const from = p.from ? String(p.from) : "?";
      const to = p.to ? String(p.to) : "?";
      return `${actor} reassigned task ${taskRef(ctx, ev.entityId)} from ${from} to ${to}`;
    }
    case "permission.asked":
      return `${actor} hit a permission wall (${String(p.type ?? "?")})`;
    case "blackboard.write":
      return `${actor} wrote blackboard key '${ev.entityId ?? "?"}'${typeof p.version === "number" ? ` (v${p.version})` : ""}`;
    case "deliverable.verdict":
      return `${actor} marked deliverable ${ev.entityId ?? "?"} as ${String(p.verdict ?? "?")}`;
    default:
      return `${actor} — ${ev.type}`;
  }
}

/**
 * Render the event stream as a grouped, chronological (newest last) timeline.
 * Events are sorted by (createdAt, id) ascending; only the NEWEST `limit`
 * events are rendered, then grouped into time buckets.
 */
export function renderTimeline(
  events: SwarmEvent[],
  members: SwarmMember[],
  tasks: SwarmTask[],
  opts?: TimelineRenderOptions,
): string {
  const limit = opts?.limit ?? 80;
  const now = opts?.now ?? Date.now();
  const ctx: TimelineContext = { members, tasks };

  // Chronological ascending (listEvents returns newest-first).
  const asc = [...events].sort((a, b) => a.createdAt - b.createdAt || a.id - b.id);
  const shown = asc.slice(-limit);
  if (shown.length === 0) return "TIMELINE\n  (no events recorded yet)";

  const lines: string[] = [`TIMELINE (${shown.length} event${shown.length === 1 ? "" : "s"})`];
  let currentBucket = "";
  for (const ev of shown) {
    const bucket = bucketLabel(ev.createdAt, now);
    if (bucket !== currentBucket) {
      currentBucket = bucket;
      lines.push(`[${bucket}]`);
    }
    const actor = memberName(ctx, ev.actorMemberId);
    const entity = entityLabel(ev);
    const sentence = humanSentence(ev, ctx);
    lines.push(`  ${clockTime(ev.createdAt)} ${ev.type} — ${sentence} (${actor} → ${entity})`);
    // Payloads are peer-authored / machine-authored JSON — render fenced so
    // embedded content is untrusted data, never an instruction.
    if (ev.payloadJson) {
      lines.push(`    payload: ${fence(ev.payloadJson)}`);
    }
  }
  return lines.join("\n");
}
