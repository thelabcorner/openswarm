import type { BlackboardEntry, SwarmMember, SwarmTask } from "../core/types.js";
import { fence } from "../core/fence.js";

/**
 * Hive H1 delivery half — need routing + whisper/shout tier semantics
 * (features/hive-mind-execution-layer items 6 & 8).
 *
 * Need routing: a "need" is a targeted request. Instead of broadcasting to
 * everyone (or interrupting the coordinator), recipients are SELECTED by
 * matching the need's query tokens against each member's role, current task,
 * blackboard keys/values, and (when present) hive beliefs. Pull-based: the
 * caller delivers one finding message per matching member via the broker.
 *
 * Tier semantics:
 *   - whisper: tentative / low-fanout. Direct targeted messages ONLY — no
 *     coordinator copy. (A whisper is a peer-to-peer nudge, not a swarm fact.)
 *   - shout: reinforced / swarm-visible. Goes through the NORMAL swarm
 *     notification path (coordinator copy included) so the collective hears it.
 *
 * The tier boundary is a delivery-mechanics concern; the belief-tool layer
 * (Core-Auditor's lane) decides which tier a fact earns.
 */

export type NeedTier = "whisper" | "shout";

/** Guidance returned when a member exceeds the hive_need rate cap
 * (t-flood-rate): the need is NOT routed, the caller should retry later. */
export const NEED_RATE_LIMITED_GUIDANCE = "need rate-limited — retry later";

/** A member selected as a need recipient, with the reason it matched. */
export interface NeedRecipient {
  member: SwarmMember;
  /** Human-readable match explanation ("role mentions 'auth'; task 'refresh'"). */
  reason: string;
}

/** Result of routing a need — NOT yet delivered (delivery is the caller's job). */
export interface NeedRouteResult {
  recipients: NeedRecipient[];
  /** Zero-match guidance: what the caller should do instead of broadcasting. */
  guidance: string;
}

/** Inputs a need query is matched against for one member. */
export interface NeedMatchContext {
  member: SwarmMember;
  /** The member's current task (title + description), if any. */
  task?: Pick<SwarmTask, "title" | "description">;
  /** Blackboard entries authored by or relevant to this member (key + value). */
  blackboard: Array<Pick<BlackboardEntry, "key" | "value">>;
  /** Hive beliefs authored by this member (text + tags), if the schema landed. */
  beliefs?: Array<{ text: string; tags?: string }>;
}

/** Tokenize a string into lowercase alphanumeric tokens (min length 3). */
export function needTokens(s: string): string[] {
  return [...new Set(s.toLowerCase().split(/[^a-z0-9]+/).filter((t) => t.length >= 3))];
}

/**
 * Pure need-matcher: does this member match the query tokens? Matches if ANY
 * query token appears (substring) in the member's role/name, current task
 * title/description, blackboard key/value, or belief text/tags. Deterministic
 * and cheap — pull-based selection, no semantic embedding (H2 scope).
 */
export function needMatchesQuery(
  query: string,
  ctx: NeedMatchContext,
): boolean {
  const tokens = needTokens(query);
  if (tokens.length === 0) return false; // empty query matches nobody
  const hay = [
    ctx.member.name,
    ctx.member.role ?? "",
    ctx.task?.title ?? "",
    ctx.task?.description ?? "",
    ...ctx.blackboard.flatMap((e) => [e.key, e.value]),
    ...(ctx.beliefs ?? []).flatMap((b) => [b.text, b.tags ?? ""]),
  ].join(" ").toLowerCase();
  return tokens.some((t) => hay.includes(t));
}

/** Human-readable match reason for a member (used in the finding message). */
export function needMatchReason(
  query: string,
  ctx: NeedMatchContext,
): string {
  const tokens = needTokens(query);
  const bits: string[] = [];
  const hit = (s: string, label: string) => {
    if (tokens.some((t) => s.toLowerCase().includes(t))) bits.push(label);
  };
  hit(`${ctx.member.name} ${ctx.member.role ?? ""}`, "role/name");
  hit(ctx.task?.title ?? "", "task");
  hit(ctx.task?.description ?? "", "task description");
  if (ctx.blackboard.some((e) => tokens.some((t) => e.key.toLowerCase().includes(t) || e.value.toLowerCase().includes(t)))) {
    bits.push("blackboard");
  }
  if ((ctx.beliefs ?? []).some((b) => tokens.some((t) => b.text.toLowerCase().includes(t) || (b.tags ?? "").toLowerCase().includes(t)))) {
    bits.push("beliefs");
  }
  return bits.length ? bits.join(", ") : "query token match";
}

/**
 * Route a need to matching members. Returns the recipients with reasons and
 * zero-match guidance. This is PULL-BASED selection — it does NOT broadcast
 * and does NOT deliver; the caller delivers via the broker (one finding per
 * recipient). Zero matches yields actionable guidance, not a dead end.
 */
export function routeNeed(
  query: string,
  members: Array<{
    member: SwarmMember;
    task?: Pick<SwarmTask, "title" | "description">;
    blackboard: Array<Pick<BlackboardEntry, "key" | "value">>;
    beliefs?: Array<{ text: string; tags?: string }>;
  }>,
): NeedRouteResult {
  const active = members.filter((m) => !["stopped", "stopping", "failed"].includes(m.member.status));
  const recipients: NeedRecipient[] = [];
  for (const m of active) {
    if (needMatchesQuery(query, m)) {
      recipients.push({ member: m.member, reason: needMatchReason(query, m) });
    }
  }
  const guidance = recipients.length
    ? `deliver one finding to each of ${recipients.length} matching member(s)`
    : `no member matches "${query}" — check swarm_roster for a peer with relevant work, or route a shout to the coordinator so the collective can respond`;
  return { recipients, guidance };
}

/** Render one need-delivery message body for a recipient (fenced, self-contained). */
export function renderNeedMessage(input: {
  query: string;
  need: string;
  tier: NeedTier;
  reason: string;
}): string {
  const tierLine = input.tier === "whisper"
    ? "[whisper — direct need, no coordinator copy]"
    : "[shout — swarm-visible need]";
  return [
    tierLine,
    `Need: ${fence(input.need)}`,
    `Matched you because: ${fence(input.reason)} (query: ${fence(input.query)})`,
    "Reply directly to the sender with swarm_reply if you can help.",
  ].join("\n");
}
