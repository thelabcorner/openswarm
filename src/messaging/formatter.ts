import type { Swarm, SwarmMember, SwarmMessage } from "../core/types.js";
import { fence } from "../core/fence.js";

/**
 * Render a compact inbox for one member's queued/scheduled messages.
 * Uses the minimal envelope format — no raw id metadata tax.
 */
export function formatInbox(input: {
  swarm: Swarm;
  self: SwarmMember;
  messages: SwarmMessage[];
  names?: Map<string, string>;
}): string {
  const { swarm, self, messages, names } = input;
  const header = [
    `[SWARM INBOX — ${messages.length}]`,
    `You are: ${self.name}`,
    `Swarm: ${swarm.name}`,
  ];
  if (messages.length === 0) {
    return [...header, "No pending messages."].join("\n");
  }
  const lines = messages.map((m) => `- ${formatEnvelope(m, names ?? new Map())}`);
  return [...header, ...lines].join("\n");
}

/** Render a single message envelope (spec §16 example). */
export function formatEnvelope(
  m: SwarmMessage,
  names: Map<string, string>,
): string {
  const from = names.get(m.fromMemberId) ?? m.fromMemberId;
  const kindLabel = m.kind === "message" ? "" : ` [${m.kind}]`;
  const priorityLabel = m.priority !== "normal" ? ` (${m.priority})` : "";
  // Peer-authored body is UNTRUSTED data — fence it so an embedded
  // "ignore previous instructions" renders as quoted data, not a directive
  // (findings/injected-content-fence-umbrella surface (d); audit/messaging F-M4).
  const body = fence(m.body.text);
  const refs = m.body.refs?.length ? `\nrefs: ${m.body.refs.join(", ")}` : "";
  // Reply handle: the recipient needs THIS message's id to use swarm_reply
  // (audit/messaging F-M3). Deliberately a distinct `msg:` token — not
  // "Message-ID" — so it stays compact and thread-continuation is actionable.
  const idLine = `\nmsg: ${m.id}`;
  const threadHint = m.responseTo
    ? "\n(responds to your earlier message — use swarm_reply to continue the thread)"
    : "";
  return `${from}${kindLabel}${priorityLabel}: ${body}${refs}${idLine}${threadHint}`;
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