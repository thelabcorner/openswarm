import type { AgentRuntime } from "../runtime/runtime-types.js";
import type { SwarmMember } from "../core/types.js";
import type { SwarmStore } from "../storage/store.js";

export interface RecoveryAction {
  memberId: string;
  memberName: string;
  previousStatus: SwarmMember["status"];
  runtimeStatus: string;
  action: "interrupted" | "unchanged" | "stopped" | "respawned";
  detail?: string;
}

export interface RecoveryResult {
  swarmId: string;
  actions: RecoveryAction[];
  staleScheduledReverted: number;
  queuedMessages: number;
  reconciledAt: number;
}

/** Re-create a member's backing session (called by recovery when a member's
 * session is absent after a restart/crash). Implemented by the plugin, which
 * owns the runtime + member briefs. Returns the new session id. */
export type RespawnFn = (member: SwarmMember) => Promise<string>;

/**
 * Recovery reconciliation (spec §35). On plugin startup, compare durable member
 * state against the live OpenCode runtime: mark stale members `interrupted`,
 * RE-SPAWN active members whose sessions vanished (restart/crash) so the swarm
 * self-heals without a human re-drive, revert stale `scheduled` deliveries to
 * `queued`, and summarize.
 */
export class Recovery {
  constructor(
    private store: SwarmStore,
    private runtime: AgentRuntime,
    private respawn?: RespawnFn,
  ) {}

  async reconcileAll(): Promise<RecoveryResult[]> {
    const swarms = await this.store.listAllMemberSwarmIds();
    const results: RecoveryResult[] = [];
    for (const swarmId of swarms) {
      results.push(await this.reconcileSwarm(swarmId));
    }
    return results;
  }

  async reconcileSwarm(swarmId: string): Promise<RecoveryResult> {
    const members = await this.store.listMembers(swarmId);
    const actions: RecoveryAction[] = [];

    for (const member of members) {
      if (member.status === "stopped") continue;
      // The coordinator is the user's own session — never respawned by the
      // swarm; only worker members are re-created after a restart.
      if (member.role === "coordinator") continue;
      // GUESTS (t-guest-messaging): role 'guest' rows back a NON-swarm chat
      // session — the user's own conversation. Never respawned or status-
      // corrected by recovery: their session is not a swarm child.
      if (member.role === "guest") continue;

      // Presence is determined by session EXISTENCE, not status: idle sessions
      // drop out of the status map, so getStatus() returning null does NOT mean
      // the session is gone. getSession() is the authoritative existence check.
      let exists = false;
      let runtimeStatus = "unknown";
      try {
        const session = await this.runtime.getSession(member.sessionId);
        exists = !!session;
      } catch {
        runtimeStatus = "error";
      }
      if (exists) {
        try {
          runtimeStatus = (await this.runtime.getStatus(member.sessionId))?.type ?? "idle";
        } catch {
          runtimeStatus = "unknown";
        }
      } else {
        runtimeStatus = "absent";
      }

      const activeInSwarm = member.status !== "failed" && member.status !== "interrupted";

      if (activeInSwarm && !exists) {
        // The member's session is gone (restart/crash). If a respawn callback
        // is available, re-create it so the swarm self-heals; otherwise release
        // its task and mark it interrupted for a human to re-drive.
        if (this.respawn) {
          try {
            const newSessionId = await this.respawn(member);
            await this.store.assignMemberSession(member.id, newSessionId);
            // NO WORKING-LIMBO: a respawned member whose task was released (or
            // never had one) must come back IDLE so the scheduler can engage it
            // on the next pass — only a member that still owns its task comes
            // back 'working' (it was re-claimed + re-prompted by respawnMember).
            const hasTask = !!member.currentTaskId;
            await this.store.updateMemberStatus(member.id, hasTask ? "working" : "idle", {
              currentTaskId: member.currentTaskId ?? null,
              lastActiveAt: Date.now(),
            });
            actions.push({
              memberId: member.id,
              memberName: member.name,
              previousStatus: member.status,
              runtimeStatus: "absent",
              action: "respawned",
              detail: `session absent; re-spawned as ${newSessionId}${hasTask ? "" : " (no task — idle, scheduler will assign)"}`,
            });
            continue;
          } catch (err) {
            // Fall through to tombstone if the re-spawn failed.
            actions.push({
              memberId: member.id,
              memberName: member.name,
              previousStatus: member.status,
              runtimeStatus: "absent",
              action: "interrupted",
              detail: `respawn failed: ${(err as Error).message}`,
            });
          }
        }
        // Release any owned task back to ready so it can be reassigned. The
        // vanished session + failed respawn is NOT the task's fault — never
        // consume the retry budget on this path (W-1, mirrors the watchdog).
        if (member.currentTaskId) {
          await this.store.releaseTask(member.currentTaskId, { countAsRetry: false });
        }
        await this.store.updateMemberStatus(member.id, "interrupted", { currentTaskId: null, lastActiveAt: Date.now() });
        if (!actions.some((a) => a.memberId === member.id)) {
          actions.push({
            memberId: member.id,
            memberName: member.name,
            previousStatus: member.status,
            runtimeStatus,
            action: "interrupted",
            detail: "session absent from runtime; task released for reassignment",
          });
        }
      } else if (activeInSwarm && member.currentTaskId && runtimeStatus === "idle") {
        // Session alive but durable status says working — a stale working flag.
        await this.store.updateMemberStatus(member.id, "idle", { lastActiveAt: Date.now() });
        actions.push({
          memberId: member.id,
          memberName: member.name,
          previousStatus: member.status,
          runtimeStatus,
          action: "unchanged",
          detail: "runtime idle; durable state corrected to idle",
        });
      } else {
        actions.push({
          memberId: member.id,
          memberName: member.name,
          previousStatus: member.status,
          runtimeStatus,
          action: "unchanged",
        });
      }
    }

    // Revert stale scheduled deliveries unless the runtime confirms they were
    // delivered.
    const staleScheduled = await this.store.revertStaleScheduledForSwarm(swarmId);

    return {
      swarmId,
      actions,
      staleScheduledReverted: staleScheduled,
      queuedMessages: 0,
      reconciledAt: Date.now(),
    };
  }
}