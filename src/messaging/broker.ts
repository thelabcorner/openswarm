import type { AgentRuntime } from "../runtime/runtime-types.js";
import type { SwarmMessage } from "../core/types.js";
import { DEFAULT_MAX_DELIVERY_ATTEMPTS } from "../core/types.js";
import type { SwarmStore } from "../storage/store.js";
import { formatEnvelope } from "./formatter.js";

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
  /** Called once when a message is marked `failed` after exhausting its
   * delivery retry budget — the plugin wires this to notify the sender.
   * (audit/messaging F-M5) */
  onMessageFailed?: (failed: SwarmMessage) => void | Promise<void>;
  /** Optional predicate: when it returns true for a member, delivery is skipped
   * (mail stays `queued`) and the next delivery attempt happens later. Used to
   * defer mail while the user is directly chatting with a member. */
  shouldDeferDelivery?: (memberId: string) => boolean | Promise<boolean>;
}

/**
 * Durable mailbox broker. Implements enqueue-before-delivery, idle wakeup,
 * batching and delivery-state transitions (spec §16–21).
 */
export class Broker {
  private lastDeliveryAt = new Map<string, number>();

  constructor(
    private store: SwarmStore,
    private runtime: AgentRuntime,
    private options: BrokerOptions = {},
  ) {}

  /**
   * Deliver all queued messages to an idle member. Returns the number of
   * messages delivered. Concurrent wakes are serialized by the store's
   * transaction queue, and the affected-row check on markMessagesScheduled
   * guarantees a message is claimed by exactly one wake.
   */
  async deliverToIdleMember(memberId: string, memberSessionId: string): Promise<number> {
    // Defer while the user is directly chatting with the member — the member is
    // answering the user, not the swarm. Mail stays `queued` (not marked
    // scheduled), so it's delivered on the first normal attempt after the lull.
    if (this.options.shouldDeferDelivery) {
      const defer = await this.options.shouldDeferDelivery(memberId);
      if (defer) return 0;
    }

    const pending = await this.store.listPendingMessages(memberId);
    if (pending.length === 0) return 0;

    // Throttle: don't inject another mailbox turn too soon after the previous
    // one — a flooded member would otherwise be preempted repeatedly and never
    // get to its task work. URGENT messages bypass the cooldown (F-M7: urgent
    // must not wait on idle/cooldown where feasible).
    const cooldown = this.options.deliveryCooldownMs ?? 30_000;
    const hasUrgent = pending.some((m) => m.priority === "urgent");
    if (!hasUrgent) {
      const last = this.lastDeliveryAt.get(memberId) ?? 0;
      if (Date.now() - last < cooldown) return 0;
    }
    this.lastDeliveryAt.set(memberId, Date.now());

    const batch = pending.slice(0, this.options.batchSize ?? 10);

    // Atomically claim the batch. If another wake already scheduled these
    // messages, this wake aborts instead of double-delivering.
    const claimed = await this.store.markMessagesScheduled(memberId, batch.map((m) => m.id));
    if (claimed === 0) return 0;
    const toDeliver = batch.slice(0, claimed);

    const names = await this.memberNames(memberId);
    const self = await this.store.getMemberById(memberId);
    const ctx = await this.memberContext(memberId, names);
    const inbox = toDeliver.map((m) => formatEnvelope(m, names)).join("\n");
    // A normal user turn (not synthetic) prefixed with the necessary swarm
    // context so the receiving agent can act: its identity, the swarm id
    // (required as a tool argument), and the reply protocol.
    const promptText = [
      `[SWARM INBOX — ${toDeliver.length}]`,
      ctx,
      inbox,
    ].join("\n");

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
      await this.revertScheduled(memberId, toDeliver, err);
      throw err;
    }

    // Persist delivery result (mark delivered). Best-effort: even if this
    // commit fails, the message stays 'scheduled' and recovery reconciles it.
    await this.markDelivered(toDeliver);
    return toDeliver.length;
  }

  /** Compact but sufficient context so an injected message is self-contained. */
  private async memberContext(memberId: string, names: Map<string, string>): Promise<string> {
    const self = await this.store.getMemberById(memberId);
    if (!self) return "";
    const swarm = await this.store.getSwarm(self.swarmId);
    const role = self.role && self.role.toLowerCase() !== self.name.toLowerCase() ? `, ${self.role}` : "";
    const peers = names.size > 1
      ? [...names.entries()].filter(([id]) => id !== memberId).map(([, n]) => n).join(", ")
      : "";
    const lines = [
      `You are ${self.name}${role} in swarm ${swarm?.name ?? self.swarmId} (swarmId: ${self.swarmId}).`,
      `Reply to senders with swarm_message (to: <name>)${peers ? `; peers: ${peers}` : ""}.`,
      `Use swarm_reply with the message id to continue a thread.`,
    ];
    return lines.join("\n");
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