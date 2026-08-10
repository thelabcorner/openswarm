import type { SwarmStore } from "../storage/store.js";

/**
 * True when a session.error payload represents an ABORT/interrupt (the user
 * manually stopped the chat, or the run was cancelled) rather than a genuine
 * failure. OpenCode signals aborts via DOMException("AbortError") — the error
 * object's name or message carries abort/cancel/interrupted. Treating a manual
 * stop as a failure would release the member's task and panic the coordinator.
 */
export function isAbortError(e: unknown): boolean {
  if (e == null) return false;
  const hay = typeof e === "string" ? e : JSON.stringify(e);
  return /abort|cancel|interrupt/i.test(hay);
}

export type OpenCodeEvent =
  | { type: "session.status"; properties: { sessionID: string; status: { type: "idle" } | { type: "busy" } | { type: "retry"; attempt: number; message: string; next: number } } }
  | { type: "session.idle"; properties: { sessionID: string } }
  | { type: "session.error"; properties: { sessionID: string; error: string } }
  | { type: "session.created"; properties: { sessionID: string; info: { title?: string } } }
  | { type: "session.deleted"; properties: { sessionID: string } }
  | { type: "message.updated"; properties: { sessionID: string; messageID: string } }
  | { type: string; properties: Record<string, unknown> };

export interface SupervisorOptions {
  /** Minimum gap between scheduled wakeups for the same member. */
  wakeCooldownMs?: number;
}

export interface WakePlan {
  memberId: string;
  /** Whether a wake should be attempted now. */
  shouldWake: boolean;
}

/**
 * Event-driven supervisor. Reduces OpenCode events into durable state changes
 * and external effects (wakeups). Deterministic; never polls.
 *
 * Spec §34: `onOpenCodeEvent` applies state changes, then executes external
 * actions after the state transaction commits.
 */
export class Supervisor {
  private lastWakeAt = new Map<string, number>();

  constructor(
    private store: SwarmStore,
    private options: SupervisorOptions = {},
  ) {}

  /**
   * Handle one OpenCode event. Returns the set of effects the caller must
   * execute AFTER durable state is committed.
   */
  async onOpenCodeEvent(event: OpenCodeEvent): Promise<{ wake: string[]; notifyCoordinator: boolean }> {
    const effects: { wake: string[]; notifyCoordinator: boolean } = { wake: [], notifyCoordinator: false };

    const sessionID =
      (event.properties as { sessionID?: string })?.sessionID ??
      (event.properties as { id?: string })?.id;
    if (!sessionID) return effects;

    const member = await this.store.getMemberBySessionId(sessionID);
    if (!member) return effects;

    const now = Date.now();
    switch (event.type) {
      case "session.idle":
        await this.store.updateMemberStatus(member.id, "idle", { lastActiveAt: now });
        effects.wake.push(member.id);
        break;
      case "session.status":
        if ((event.properties as { status?: { type?: string } }).status?.type === "busy") {
          await this.store.updateMemberStatus(member.id, "working", { lastActiveAt: now });
        } else if ((event.properties as { status?: { type?: string } }).status?.type === "retry") {
          await this.store.updateMemberStatus(member.id, "interrupted", { lastActiveAt: now });
        }
        break;
      case "session.error":
        // Distinguish an abort/interrupt (user manually stopped the chat, or
        // the run was cancelled) from a genuine failure. An abort is NOT a
        // failure: the member is paused, its task should stay claimed so it
        // resumes, and the coordinator should NOT be panicked. Only real
        // failures mark the member failed, release the task, and notify.
        if (isAbortError((event.properties as { error?: unknown })?.error)) {
          await this.store.updateMemberStatus(member.id, "interrupted", { lastActiveAt: now });
          // Keep currentTaskId set — the task is paused, not orphaned.
          effects.notifyCoordinator = false;
          break;
        }
        await this.store.updateMemberStatus(member.id, "failed", { lastActiveAt: now });
        // A failed member's in-flight task is orphaned (it will never complete
        // it). Release it back to 'ready' so another idle member can pick it
        // up — otherwise the task blocks the DAG forever.
        if (member.currentTaskId) {
          await this.store.releaseTask(member.currentTaskId);
        }
        await this.store.updateMemberStatus(member.id, "failed", { currentTaskId: null, lastActiveAt: now });
        effects.notifyCoordinator = true;
        break;
      case "session.deleted":
        // The user deleted the member's chat in the app (Desktop) or the session
        // is gone. Release any owned task back to ready so the DAG can advance —
        // otherwise a working/claimed task with a stopped owner dead-locks
        // forever (the orphan sweep only releases ownerless tasks).
        if (member.currentTaskId) {
          await this.store.releaseTask(member.currentTaskId);
        }
        await this.store.updateMemberStatus(member.id, "stopped", { currentTaskId: null });
        break;
      case "session.created":
        // A newly created child enters "starting"; the coordinator flow
        // advances it once the session exists.
        if (member.status === "created") {
          await this.store.updateMemberStatus(member.id, "starting");
        }
        break;
      default:
        break;
    }
    return effects;
  }

  /** Whether we should wake a member, respecting cooldown. */
  shouldWake(memberId: string): boolean {
    const cooldown = this.options.wakeCooldownMs ?? 500;
    const last = this.lastWakeAt.get(memberId) ?? 0;
    if (Date.now() - last < cooldown) return false;
    this.lastWakeAt.set(memberId, Date.now());
    return true;
  }
}