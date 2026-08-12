import type { SwarmMember, SwarmPolicies } from "../core/types.js";
import type { SwarmStore } from "../storage/store.js";

/**
 * The human-chat state machine for swarm members.
 *
 * Members are now ROOT sessions the user can open and chat with directly. While
 * the user is talking to a member, the swarm must not fight them for the
 * session: mail delivery is deferred, the member's task is not force-continued,
 * and the scheduler does not assign new work. After a configurable lull with no
 * human message, normal machinery resumes automatically.
 *
 * State per worker member:
 *   - humanChatAt (persisted) — epoch-ms of the last DIRECT user message.
 *   - queuedHumanTurn (in-memory) — a user message arrived while the member was
 *     runtime-busy; a single `[PENDING USER REPLY]` is injected after the busy
 *     turn finishes so the member answers the user (queue-then-answer).
 *
 * `chatting` is derived: humanChatAt != null && now - humanChatAt < lull.
 *
 * The tracker is deliberately pure of OpenCode SDK types: it talks to the store
 * and to a clock, and is fully unit-testable with a fake clock and stub store.
 */
export interface ChatTrackerDeps {
  store: Pick<SwarmStore, "getMemberBySessionId" | "updateMemberHumanChat">;
  /** Injectable clock; defaults to Date.now. */
  now?: () => number;
}

export interface ChatTrackerOptions {
  /** Injection message-id registry (set of ids the plugin itself generated). */
  selfInjectionIds?: Set<string>;
  /** Lull policy lookup; defaults to the swarm's policy or 300s. */
  lullMsFor?: (swarm: { policies: SwarmPolicies }) => number;
}

export class HumanChatTracker {
  constructor(
    private deps: ChatTrackerDeps,
    private options: ChatTrackerOptions = {},
  ) {
    this.options.selfInjectionIds ??= new Set<string>();
  }

  // ---- classification -----------------------------------------------------

  /** Reserved messageID prefix the plugin uses for its own injections. */
  static readonly SELF_ID_PREFIX = "swarm-inj-";
  /** Exact text prefixes of plugin-injected prompts (belt-and-braces fallback
   * if the SDK drops messageID). Any message that matches neither the id
   * registry nor a known prefix is a HUMAN message. */
  static readonly SELF_TEXT_PREFIXES = [
    "[SWARM INBOX —",
    "[NEW MESSAGE FROM:",
    "[NEW MESSAGES (",
    "[NO NEW MESSAGES]",
    "[TEAM SYNC —",
    "You went idle while working on task",
    "[WATCHDOG]",
    "You are `",
    "[ASSIGNED TASK",
    "Resumed after a restart",
    "Resumed after a re-root",
    "[PENDING USER REPLY]",
    "[SWARM: ",
  ];

  isSelfInjection(messageID: string | undefined, text: string): boolean {
    // Primary: an id this plugin explicitly registered when it injected a prompt.
    if (messageID && this.options.selfInjectionIds!.has(messageID)) return true;
    // Belt-and-braces: if the SDK drops messageID, match known prompt prefixes.
    return HumanChatTracker.SELF_TEXT_PREFIXES.some((p) => text.startsWith(p));
  }

  /** Record a messageID as a self-injection (called when the plugin injects a
   * prompt and the SDK echoes its messageID). */
  registerInjection(messageID: string): void {
    this.options.selfInjectionIds!.add(messageID);
  }

  /** Clear a consumed self-injection id (the message was already seen). */
  consumeInjection(messageID: string): void {
    this.options.selfInjectionIds!.delete(messageID);
  }

  // ---- state machine ------------------------------------------------------

  /**
   * Handle a `chat.message` hook event for a worker member session. OpenCode
   * natively handles the actual reply: a mid-turn message is absorbed by the
   * in-flight run loop (it re-reads history each iteration and the new message
   * becomes the latest user turn), and an idle-session message starts a fresh
   * run. The plugin only needs to RECORD that the user is talking so swarm
   * machinery (mail / task continuation / scheduler) yields during the chat.
   * Returns true if the member's chat state changed.
   */
  async onUserMessage(sessionID: string, isSelf: boolean): Promise<boolean> {
    if (isSelf) return false;
    const member = await this.deps.store.getMemberBySessionId(sessionID);
    if (!member || member.role === "coordinator") return false;
    const now = this.deps.now?.() ?? Date.now();
    await this.deps.store.updateMemberHumanChat(member.id, now);
    return true;
  }

  /** Is this member currently in a human chat (within the lull window)? */
  async chatting(member: SwarmMember, swarm: { policies: SwarmPolicies }): Promise<boolean> {
    if (member.humanChatAt == null) return false;
    const lullMs = this.options.lullMsFor?.(swarm) ?? swarm.policies.humanChatLullMs ?? 300_000;
    const now = this.deps.now?.() ?? Date.now();
    return now - member.humanChatAt < lullMs;
  }

  /** Convenience: resolve a session to a member and report chat state. */
  async isChattingSession(sessionID: string, swarm: { policies: SwarmPolicies }): Promise<boolean> {
    const member = await this.deps.store.getMemberBySessionId(sessionID);
    if (!member) return false;
    return this.chatting(member, swarm);
  }

  /** Clear chat state (error/deleted/forced release). */
  async clear(sessionID: string): Promise<void> {
    const member = await this.deps.store.getMemberBySessionId(sessionID);
    if (!member) return;
    await this.deps.store.updateMemberHumanChat(member.id, null);
  }

  /** Restore chat state at startup: clear `humanChatAt` if it lapsed while the
   * plugin was down. */
  async reconcileStartup(members: SwarmMember[], swarm: { policies: SwarmPolicies }): Promise<void> {
    const now = this.deps.now?.() ?? Date.now();
    const lullMs = this.options.lullMsFor?.(swarm) ?? swarm.policies.humanChatLullMs ?? 300_000;
    for (const m of members) {
      if (m.humanChatAt != null && now - m.humanChatAt >= lullMs) {
        await this.deps.store.updateMemberHumanChat(m.id, null);
      }
    }
  }
}
