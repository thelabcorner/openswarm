import type { AgentRuntime } from "../runtime/runtime-types.js";
import type { SwarmMessage } from "../core/types.js";
import { DEFAULT_MAX_DELIVERY_ATTEMPTS } from "../core/types.js";
import type { SwarmStore } from "../storage/store.js";
import { formatEnvelope, senderNames } from "./formatter.js";
import { enrichForeignSenderNames } from "./senders.js";
import { RateLimiter, DEFAULT_MAX_INBOX_PER_MIN } from "./rate-limits.js";

export interface BrokerOptions {
  /** Maximum messages batched into a single wake delivery. */
  batchSize?: number;
  /** Minimum gap between mailbox deliveries to the same member (ms). */
  deliveryCooldownMs?: number;
  /** Delivery-attempt budget: a message is marked `failed` (and its sender
   * notified) after this many failed attempts. Defaults to
   * `DEFAULT_MAX_DELIVERY_ATTEMPTS` (3). Boundary semantics (M-4): `0` means
   * "fail on the first failed attempt" (a message with any delivery failure
   * is immediately marked failed) — consistent with maxRetriesPerTask where
   * 0 retries = fail after the first failure. Negative values are not
   * supported (treated as 0). (audit/messaging F-M5) */
  maxDeliveryAttempts?: number;
  /** Non-urgent mailbox-PROMPT budget per member per 60s (t-flood-rate inbox
   * throttle). At most this many prompts are injected into one member's
   * session per window; excess non-urgent mail stays `queued` and is
   * delivered at the next window boundary by the F-M7 pending-mail sweep.
   * URGENT always bypasses. When the option is unset the member's swarm
   * policy `maxInboxPerMin` is honored, then this default. */
  maxInboxPerMin?: number;
  /** Clock for deterministic tests (defaults to Date.now). */
  now?: () => number;
  /** Called once when a message is marked `failed` after exhausting its
   * delivery retry budget — the plugin wires this to notify the sender.
   * (audit/messaging F-M5) */
  onMessageFailed?: (failed: SwarmMessage) => void | Promise<void>;
  /** Called when a delivery attempt fails (the message is reverted to queued
   * with the error recorded). The plugin wires this to detect model usage-limit
   * signals in delivery errors (stall auto-diagnosis: a provider limit hit
   * blocks delivery with a limit/quota/rate/429/billing error). */
  onDeliveryError?: (memberId: string, error: string) => void | Promise<void>;
  /** Optional predicate: when it returns true for a member, delivery is skipped
   * (mail stays `queued`) and the next delivery attempt happens later. Used to
   * defer mail while the user is directly chatting with a member. */
  shouldDeferDelivery?: (memberId: string) => boolean | Promise<boolean>;
  /** Emergency kill switch (t-emergency): when true, ALL delivery halts —
   * deliverToIdleMember returns 0 (mail stays queued, resumes after clear).
   * Freezes swarm machinery without touching the user's chat. */
  shouldBlockDelivery?: () => boolean;
}

/**
 * Durable mailbox broker. Implements enqueue-before-delivery, idle wakeup,
 * batching and delivery-state transitions (spec §16–21).
 */
export class Broker {
  private lastDeliveryAt = new Map<string, number>();
  /** In-memory inbox throttle: timestamps of prompts injected per member
   * (t-flood-rate). */
  private readonly inboxPrompts: RateLimiter;
  private readonly now: () => number;

  constructor(
    private store: SwarmStore,
    private runtime: AgentRuntime,
    private options: BrokerOptions = {},
  ) {
    this.now = options.now ?? Date.now;
    this.inboxPrompts = new RateLimiter({ now: this.now });
  }

  /** Resolve the per-recipient inbox budget: broker option → swarm policy →
   * default. The swarm policy is read live so a policy change takes effect
   * without a restart. */
  private async inboxBudget(memberId: string): Promise<number> {
    if (this.options.maxInboxPerMin !== undefined) return this.options.maxInboxPerMin;
    const member = await this.store.getMemberById(memberId).catch(() => undefined);
    const swarm = member ? await this.store.getSwarm(member.swarmId).catch(() => undefined) : undefined;
    return swarm?.policies.maxInboxPerMin ?? DEFAULT_MAX_INBOX_PER_MIN;
  }

  /**
   * Deliver all queued messages to an idle member. Returns the number of
   * messages delivered. Concurrent wakes are serialized by the store's
   * transaction queue, and the affected-row check on markMessagesScheduled
   * guarantees a message is claimed by exactly one wake.
   */
  async deliverToIdleMember(memberId: string, memberSessionId: string): Promise<number> {
    // Emergency kill switch (t-emergency): while tripped, ALL delivery halts —
    // mail stays `queued` and is delivered after clear. The machinery freezes;
    // the user's chat is untouched.
    if (this.options.shouldBlockDelivery?.()) return 0;

    // Defer while the user is directly chatting with the member — the member is
    // answering the user, not the swarm. Mail stays `queued` (not marked
    // scheduled), so it's delivered on the first normal attempt after the lull.
    if (this.options.shouldDeferDelivery) {
      const defer = await this.options.shouldDeferDelivery(memberId);
      if (defer) return 0;
    }

    const pending = await this.store.listPendingMessages(memberId);
    if (pending.length === 0) return 0;

    // Inbox throttle (t-flood-rate): at most MAX_INBOX_PER_MIN non-urgent
    // prompts per member per 60s. Excess non-urgent mail stays QUEUED (no
    // prompt) — the F-M7 pending-mail sweep retries it, so it is delivered at
    // the next window boundary. URGENT always bypasses the budget.
    const budget = await this.inboxBudget(memberId);
    const urgent = pending.filter((m) => m.priority === "urgent");
    const nonUrgent = pending.filter((m) => m.priority !== "urgent");
    const used = this.inboxPrompts.count(`inbox:${memberId}`, 60_000);
    const remaining = Math.max(0, budget - used);
    if (remaining === 0 && urgent.length === 0) return 0; // budget spent, no urgent mail
    // Urgent first (never starved by the budget or the batch cap), then as many
    // non-urgent messages as the remaining budget allows.
    const batchLimit = this.options.batchSize ?? 10;
    const toDeliver = [...urgent, ...nonUrgent.slice(0, remaining)].slice(0, batchLimit);
    if (toDeliver.length === 0) return 0;

    // Throttle: don't inject another mailbox turn too soon after the previous
    // one — a flooded member would otherwise be preempted repeatedly and never
    // get to its task work. URGENT messages bypass the cooldown (F-M7: urgent
    // must not wait on idle/cooldown where feasible).
    const cooldown = this.options.deliveryCooldownMs ?? 30_000;
    const hasUrgent = toDeliver.some((m) => m.priority === "urgent");
    if (!hasUrgent) {
      const last = this.lastDeliveryAt.get(memberId) ?? 0;
      if (this.now() - last < cooldown) return 0;
    }
    this.lastDeliveryAt.set(memberId, this.now());

    // Atomically claim the batch. If another wake already scheduled these
    // messages, this wake aborts instead of double-delivering.
    const claimed = await this.store.markMessagesScheduled(memberId, toDeliver.map((m) => m.id));
    if (claimed === 0) return 0;
    const deliveredBatch = toDeliver.slice(0, claimed);

    const names = await this.memberNames(memberId);
    // Cross-swarm messages carry a sender from another swarm — resolve those
    // ids to `name@swarm` so the recipient always sees the origin.
    await enrichForeignSenderNames(this.store, deliveredBatch, names);
    const self = await this.store.getMemberById(memberId);
    const ctx = await this.memberContext(memberId, names);
    const inbox = deliveredBatch.map((m) => formatEnvelope(m, names)).join("\n\n");
    // Noreply: when EVERY message in the batch is fire-and-forget, the member
    // is explicitly told no response is expected — saves a mailbox turn and
    // cooldown that an ack-only reply would have burned (noreply feature).
    const allNoreply = deliveredBatch.length > 0 && deliveredBatch.every((m) => m.noreply);
    const replyLine = allNoreply
      ? "[no replies needed — do not respond unless you can act or escalate]"
      : "[reply where needed — swarm_message (to: <name>) or swarm_reply ([msg:...])]";
    const senders = senderNames(deliveredBatch, names);
    const header =
      deliveredBatch.length === 1
        ? `[NEW MESSAGE FROM: ${senders.join(", ")}]`
        : `[NEW MESSAGES (${deliveredBatch.length}) FROM: ${senders.join(", ")}]`;
    // A normal user turn (not synthetic) prefixed with the necessary swarm
    // context so the receiving agent can act: its identity, the swarm id
    // (required as a tool argument), and the sender of each message.
    const promptText = [header, ctx, replyLine, "", inbox].join("\n");

    try {
      // Deliver using the RECIPIENT's configured model AND agent — in a
      // multi-model swarm, each member speaks its own model, not the sender's
      // or default. The agent carries the member's doctrine system prompt.
      await this.runtime.promptAsync(
        { text: promptText, model: self?.model, agent: self?.agent ?? "swarm" },
        memberSessionId,
      );
    } catch (err) {
      // Delivery failed: record the error, increment the attempt count, and
      // enforce the retry budget — messages past maxDeliveryAttempts go
      // `failed` (sender notified once) instead of retrying forever (F-M5).
      await this.revertScheduled(memberId, deliveredBatch, err);
      throw err;
    }

    // A prompt actually fired — count it toward the per-minute budget (only on
    // success: a reverted delivery is a retry, not a delivered prompt).
    this.inboxPrompts.hit(`inbox:${memberId}`, 60_000);

    // Persist delivery result (mark delivered). Best-effort: even if this
    // commit fails, the message stays 'scheduled' and recovery reconciles it.
    await this.markDelivered(deliveredBatch);
    return deliveredBatch.length;
  }

  /**
   * One compact identity row. The swarm agent's system prompt already teaches
   * the reply protocol (swarm_message/swarm_reply/roster), so per-delivery
   * context is just: who I am, which swarm, and who my peers are.
   */
  private async memberContext(memberId: string, names: Map<string, string>): Promise<string> {
    const self = await this.store.getMemberById(memberId);
    if (!self) return "";
    const swarm = await this.store.getSwarm(self.swarmId);
    const peers = names.size > 1
      ? [...names.entries()].filter(([id]) => id !== memberId).map(([, n]) => n).join(", ")
      : "";
    return `@${self.name} | ${swarm?.name ?? self.swarmId} (${self.swarmId})${peers ? ` | peers: ${peers}` : ""}`;
  }

  private async memberNames(memberId: string): Promise<Map<string, string>> {
    const self = await this.store.getMemberById(memberId);
    const names = new Map<string, string>();
    if (!self) return names;
    const all = await this.store.listMembers(self.swarmId);
    for (const m of all) names.set(m.id, m.name);
    return names;
  }

  private async markDelivered(msgs: SwarmMessage[]): Promise<void> {
    await this.store.transaction(async (tx) => {
      for (const m of msgs) {
        // M-6 fix: a message whose expiresAt passed while it was `scheduled`
        // (claimed by a wake, then the member stalled past expiry) must NOT be
        // marked delivered — transition it to `expired` instead so it is never
        // delivered in-session. Without this guard, an expired-while-scheduled
        // urgent message would be delivered by the completing wake.
        if (m.expiresAt !== undefined && m.expiresAt <= Date.now()) {
          await tx.expireMessage(m.id);
          continue;
        }
        await tx.updateMessageDelivery(m.id, "delivered");
      }
    });
  }

  private async revertScheduled(memberId: string, msgs: SwarmMessage[], err: unknown): Promise<void> {
    const maxAttempts = this.options.maxDeliveryAttempts ?? DEFAULT_MAX_DELIVERY_ATTEMPTS;
    const errorText = err instanceof Error ? err.message : String(err);
    // Surface the failure to the plugin (usage-limit detection): the delivery
    // error may be a model limit hit — advisory, never fails the revert.
    if (this.options.onDeliveryError) {
      try {
        await this.options.onDeliveryError(memberId, errorText);
      } catch (notifyErr) {
        console.error(`[swarm] delivery-error hook failed: ${(notifyErr as Error).message}`);
      }
    }
    for (const m of msgs) {
      const reverted = await this.store.revertMessageToQueuedWithError(m.id, memberId, errorText);
      // Budget exhausted: mark failed and notify the sender exactly once.
      // (F-M5: no infinite retry loop for a wedged member.)
      if (reverted && reverted.attemptCount >= maxAttempts) {
        const failed = await this.store.markMessageFailed(m.id);
        if (failed && this.options.onMessageFailed) {
          try {
            await this.options.onMessageFailed(failed);
          } catch (notifyErr) {
            console.error(`[swarm] failed-delivery notice error: ${(notifyErr as Error).message}`);
          }
        }
      }
    }
  }
}