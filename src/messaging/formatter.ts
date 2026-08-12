import type { Swarm, SwarmMember, SwarmMessage } from "../core/types.js";
import { fenceQuote } from "../core/fence.js";
import { extractMemberMentions } from "./mentions.js";

/**
 * Unique sender names for a batch of messages (drives the inbox header).
 * Dedupes so a 3-message burst from one peer reads "FROM: X", not "X, X, X".
 */
export function senderNames(messages: SwarmMessage[], names: Map<string, string>): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const m of messages) {
    const n = names.get(m.fromMemberId) ?? m.fromMemberId;
    if (!seen.has(n)) {
      seen.add(n);
      out.push(n);
    }
  }
  return out;
}

/**
 * Render a compact inbox for one member's queued/scheduled messages.
 * Header is sender-centric ("[NEW MESSAGE FROM: X]") and the context line is
 * a single compact row — the swarm agent's system prompt already teaches the
 * reply protocol, so it is not repeated per delivery (token saving).
 */
export function formatInbox(input: {
  swarm: Swarm;
  self: SwarmMember;
  messages: SwarmMessage[];
  names?: Map<string, string>;
}): string {
  const { swarm, self, messages, names } = input;
  const nameById = names ?? new Map();
  const senders = senderNames(messages, nameById);
  const ctx = `@${self.name} | ${swarm.name} (${swarm.id})`;
  if (messages.length === 0) {
    return ["[NO NEW MESSAGES]", ctx, "No pending messages."].join("\n");
  }
  const header =
    messages.length === 1
      ? `[NEW MESSAGE FROM: ${senders.join(", ")}]`
      : `[NEW MESSAGES (${messages.length}) FROM: ${senders.join(", ")}]`;
  const envelopes = messages.map((m) => formatEnvelope(m, nameById));
  return [header, ctx, "", ...envelopes].join("\n");
}

/** Render a single message envelope (spec §16 example). */
export function formatEnvelope(
  m: SwarmMessage,
  names: Map<string, string>,
): string {
  const from = names.get(m.fromMemberId) ?? m.fromMemberId;
  const kindLabel = m.kind === "message" ? "" : ` [${m.kind}]`;
  const priorityLabel = m.priority !== "normal" ? ` (${m.priority})` : "";
  // Peer-authored body is UNTRUSTED data — render it as a `>` blockquote so an
  // embedded "ignore previous instructions" is visibly quoted data, not a
  // directive (findings/injected-content-fence-umbrella; audit/messaging F-M4).
  const body = fenceQuote(m.body.text);
  const refs = m.body.refs?.length ? `\n> refs: ${m.body.refs.join(", ")}` : "";
  // @mention hint (GitHub-style): list which swarm members this message
  // references so the recipient instantly sees who else is in the loop.
  const mentioned = extractMemberMentions(m.body.text, names.values());
  const mentionLine = mentioned.length ? `\nmentions: ${mentioned.join(", ")}` : "";
  // Reply handle: the recipient needs THIS message's id to use swarm_reply
  // (audit/messaging F-M3). Deliberately a distinct `msg:` token — not
  // "Message-ID" — so it stays compact and thread-continuation is actionable.
  const tags = [
    `[msg:${m.id}]`,
    m.noreply ? "[noreply]" : "",
    m.responseTo ? "[thread]" : "",
  ].filter(Boolean).join(" ");
  return `${from}${kindLabel}${priorityLabel}:\n${body}${refs}${mentionLine}\n${tags}`;
}

/** Format a blackboard conflict notice (spec §24). */
export function formatBlackboardConflict(input: {
  key: string;
  expectedVersion?: number;
  currentVersion: number;
}): string {
  return [
    "BLACKBOARD CONFLICT",
    "",
    input.key,
    `expected: ${input.expectedVersion ?? "required (read first)"}`,
    `current: ${input.currentVersion}`,
    "",
    "Read the current value before overwriting.",
  ].join("\n");
}
