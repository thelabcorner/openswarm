import type { AgentRuntime } from "../runtime/runtime-types.js";
import type { PendingPermission, Swarm, SwarmMember, SwarmTask } from "../core/types.js";
import type { SwarmStore } from "../storage/store.js";
import { fence } from "../core/fence.js";

/**
 * Stall auto-diagnosis + self-heal escalation ladder (task t-stalls).
 *
 * Unifies the watchdog (silent sessions), permission walls, expired task
 * leases, and model usage-limit hits into ONE per-member stall classifier,
 * then drives a time-based escalation ladder:
 *
 *   rung 1  nudge                 — re-drive the silent member (existing watchdog nudge)
 *   rung 2  permission/usage notify — tell the coordinator (deduped per member+reason)
 *   rung 3  coordinator blocker   — the ONE "why is my swarm stuck" answer, kind 'blocker'
 *   rung 4  release-task          — store.releaseTask(countAsRetry: false)
 *
 * Usage-limit detection (go + free limits): model errors/retry-statuses matching
 * /limit|quota|rate|429|billing/i are recorded in a per-swarm in-memory map and
 * surfaced as reason 'usage-limit' → nextAction 'usage-notify' (the remedy is
 * reported to the coordinator — the diagnoser NEVER auto-changes models; the
 * coordinator decides via swarm_revive / a model change / waiting out the window).
 *
 * The diagnoser is a structural host (like ReviveEngine): the plugin injects a
 * StallHost (store + runtime + outward messaging actions) so this module never
 * imports plugin.ts and stays unit-testable with a fake host.
 */

export type StallReason =
  | "permission-wall"
  | "usage-limit"
  | "session-silent"
  | "session-absent"
  | "lease-stuck"
  | "chat-paused"
  | "awaiting-mail"
  | "working"
  | "stopped"
  | "idle";

export type StallAction =
  | "none"
  | "nudge"
  | "permission-notify"
  | "usage-notify"
  | "coordinator-blocker"
  | "release-task"
  | "respawn";

/** One member's stall diagnosis (the per-member row of the report). */
export interface StallDiagnosis {
  memberName: string;
  role: string;
  status: string;
  reason: StallReason;
  /** How long the member has been in this stalled state (ms). */
  stallMs: number;
  evidence: string[];
  /** The current ladder step's action (derived from reason + stallMs). */
  nextAction: StallAction;
  /** The exact tool call that resolves this stall (the recipe). */
  recipe: string;
}

/** Swarm-level stall report — the ONE "why is my swarm stuck" answer. */
export interface StallReport {
  swarmId: string;
  verdict: "healthy" | "stalled";
  members: StallDiagnosis[];
  /** Unique stalled reasons across members (e.g. ["permission-wall"]). */
  causes: string[];
  recommended: string;
}

/** A recorded model usage-limit hit for a member (per-swarm in-memory). */
export interface UsageLimitRecord {
  memberId: string;
  signal: string;
  evidence: string[];
  at: number;
  remedy: string;
}

/** The remedy text for a usage-limit hit (swarm_permissions-style message:
 * who hit it, and the exact answer — swarm_revive / model change / wait). */
export const USAGE_LIMIT_REMEDY = (swarmId: string, memberName: string): string =>
  `member '${memberName}' hit model usage limits — answer with swarm_revive(swarmId: '${swarmId}', action: 'revive', strategy: 'keep') / a model change, or wait for the limit window`;

/** Model usage-limit signal regex (go + free tiers): provider errors and
 * retry-status messages that indicate a limit/quota/rate/billing hit. */
export const LIMIT_SIGNAL_RE = /limit|quota|rate|429|billing/i;

/** True when an error/retry message looks like a model usage-limit hit. */
export function isLimitSignal(text: string): boolean {
  return typeof text === "string" && LIMIT_SIGNAL_RE.test(text);
}

/** Mirrors plugin.ts WATCHDOG_SILENT_MS: a session with no activity past this
 * window (excluding the diagnoser's own nudges) is "silent". */
export const STALL_SILENT_MS = 5 * 60_000;
/** Rung-2 threshold: permission/usage walls escalate to a notify. */
export const LADDER_NOTIFY_MS = 10 * 60_000;
/** Rung-3 threshold: the coordinator blocker fires. */
export const LADDER_BLOCKER_MS = 15 * 60_000;
/** Rung-4 threshold: the task is released (countAsRetry: false). */
export const LADDER_RELEASE_MS = 20 * 60_000;
/** Dedup windows — one action per member+reason per window, so the 10s sweep
 * never spams the coordinator. */
export const NUDGE_DEDUP_MS = 2 * 60_000;
export const NOTIFY_DEDUP_MS = 10 * 60_000;
export const BLOCKER_DEDUP_MS = 15 * 60_000;
export const RELEASE_DEDUP_MS = 5 * 60_000;
export const RESPAWN_DEDUP_MS = 5 * 60_000;

/** Reasons that make the swarm verdict "stalled" (chat-paused/awaiting-mail/
 * working/idle/stopped are legitimate states, not stalls). */
const STALLED_REASONS: ReadonlySet<StallReason> = new Set([
  "permission-wall",
  "usage-limit",
  "session-silent",
  "session-absent",
  "lease-stuck",
]);

/** Per-reason escalation action sequence (used by force-advance: walk one
 * action per call, bypassing time thresholds). */
const FORCE_SEQUENCES: Record<StallReason, StallAction[]> = {
  "session-silent": ["nudge", "coordinator-blocker", "release-task"],
  "permission-wall": ["permission-notify", "coordinator-blocker", "release-task"],
  "usage-limit": ["usage-notify", "coordinator-blocker", "release-task"],
  "lease-stuck": ["release-task"],
  "session-absent": ["respawn", "coordinator-blocker"],
  "chat-paused": [],
  "awaiting-mail": [],
  "working": [],
  "stopped": [],
  "idle": [],
};

/** Time+reason → current ladder rung + the action for that rung. Rung numbers
 * are canonical (1 nudge, 2 notify, 3 blocker, 4 release); session-silent
 * members map rung 2 to the blocker (nudge → blocker → release, no generic
 * notify exists for them). */
function stepFor(reason: StallReason, stallMs: number): { rung: number; action: StallAction } {
  switch (reason) {
    case "session-silent":
      if (stallMs >= LADDER_RELEASE_MS) return { rung: 4, action: "release-task" };
      if (stallMs >= LADDER_BLOCKER_MS) return { rung: 3, action: "coordinator-blocker" };
      if (stallMs >= LADDER_NOTIFY_MS) return { rung: 2, action: "coordinator-blocker" };
      return { rung: 1, action: "nudge" };
    case "permission-wall":
      // Nudging does not unblock a permission wall — notify immediately.
      if (stallMs >= LADDER_BLOCKER_MS) return { rung: 3, action: "coordinator-blocker" };
      return { rung: 2, action: "permission-notify" };
    case "usage-limit":
      if (stallMs >= LADDER_BLOCKER_MS) return { rung: 3, action: "coordinator-blocker" };
      return { rung: 2, action: "usage-notify" };
    case "lease-stuck":
      // The claim lease expired — the task is release-task material outright.
      return { rung: 4, action: "release-task" };
    case "session-absent":
      if (stallMs >= LADDER_BLOCKER_MS) return { rung: 3, action: "coordinator-blocker" };
      return { rung: 1, action: "respawn" };
    default:
      return { rung: 0, action: "none" };
  }
}

/** Outward actions the diagnoser executes. The plugin runtime implements this
 * (it owns the store, runtime adapter, and messaging primitives). */
export interface StallHost {
  store: SwarmStore;
  runtimeAdapter: AgentRuntime;
  /** Injectable clock (tests fake it). */
  now(): number;
  /** Rung 1: re-drive the silent member with a nudge. */
  nudgeMember(swarmId: string, member: SwarmMember, note: string): Promise<void>;
  /** Rung 2 (permission-wall): notify the coordinator with the reply recipe. */
  notifyPermissionWall(permission: PendingPermission, contextNote?: string): Promise<void>;
  /** Rung 2 (usage-limit): notify the coordinator with the remedy. */
  notifyUsageLimit(member: SwarmMember, record: UsageLimitRecord): Promise<void>;
  /** Rung 3: the ONE "why is my swarm stuck" blocker to the coordinator. */
  notifyCoordinatorBlocker(swarm: Swarm, member: SwarmMember, diagnosis: StallDiagnosis): Promise<void>;
  /** Rung 4: release the member's task WITHOUT consuming the retry budget. */
  releaseMemberTask(member: SwarmMember, reason: string): Promise<boolean>;
  /** Session-absent: re-create the member's backing session. */
  respawnMember(member: SwarmMember): Promise<string | undefined>;
}

export class StallDiagnoser {
  /** Per-swarm recorded usage-limit hits: swarmId -> memberId -> record. */
  private usageLimits = new Map<string, Map<string, UsageLimitRecord>>();
  /** Per-member last nudge time (liveness exclusion + rung-1 dedup). */
  private lastNudgeAt = new Map<string, number>();
  /** Per-(member,reason) last notify time (rung-2 dedup). */
  private lastNotifyAt = new Map<string, number>();
  /** Per-(member,reason) last blocker time (rung-3 dedup). */
  private lastBlockerAt = new Map<string, number>();
  /** Per-member last release time (rung-4 dedup). */
  private lastReleaseAt = new Map<string, number>();
  /** Per-member last respawn time (session-absent dedup). */
  private lastRespawnAt = new Map<string, number>();
  /** Per-member last seen activity (session messages), like the watchdog. */
  private lastSeenActivity = new Map<string, number>();
  /** Per-member force-advance index (swarm_stalls ladder tool). */
  private forceIndex = new Map<string, number>();

  constructor(private host: StallHost) {}

  /** Record a model usage-limit signal (from broker revert / kickoff error
   * paths). Only limit-like signals are recorded; unknown member ids are
   * ignored. Returns true when a limit was recorded. */
  async recordLimitSignal(memberId: string, signal: string): Promise<boolean> {
    if (!isLimitSignal(signal)) return false;
    const member = await this.host.store.getMemberById(memberId);
    if (!member) return false;
    this.recordLimit(member.swarmId, memberId, signal, [`prompt/delivery error: ${signal}`]);
    return true;
  }

  /** Recorded usage-limit hits for a swarm (names resolved for reporting). */
  async reportLimits(
    swarmId: string,
  ): Promise<Array<{ memberName: string; memberId: string; signal: string; at: number; remedy: string }>> {
    const byMember = this.usageLimits.get(swarmId);
    if (!byMember || byMember.size === 0) return [];
    const members = await this.host.store.listMembers(swarmId);
    const nameById = new Map(members.map((m) => [m.id, m.name]));
    const out: Array<{ memberName: string; memberId: string; signal: string; at: number; remedy: string }> = [];
    for (const [memberId, rec] of byMember) {
      out.push({
        memberName: nameById.get(memberId) ?? memberId,
        memberId,
        signal: rec.signal,
        at: rec.at,
        remedy: rec.remedy,
      });
    }
    return out;
  }

  /** Read-only per-member diagnosis for a whole swarm. */
  async diagnose(swarmId: string): Promise<StallReport> {
    const swarm = await this.host.store.getSwarm(swarmId);
    if (!swarm) throw new Error(`no swarm '${swarmId}'`);
    const ctx = await this.diagnosisContext(swarm);
    const members = await this.host.store.listMembers(swarmId);
    const diagnoses: StallDiagnosis[] = [];
    for (const m of members) {
      if (m.role === "coordinator") continue;
      diagnoses.push(await this.diagnoseMember(swarm, m, ctx));
    }
    const stuck = diagnoses.filter((d) => STALLED_REASONS.has(d.reason));
    const causes = [...new Set(stuck.map((d) => d.reason))];
    const verdict: StallReport["verdict"] = causes.length > 0 ? "stalled" : "healthy";
    const recommended =
      verdict === "healthy"
        ? `swarm '${swarm.name}' is healthy — no stall detected`
        : `the ONE answer: ${causes.map((c) => causeRemedy(c)).join("; ")} — per-member recipes above`;
    return { swarmId, verdict, members: diagnoses, causes, recommended };
  }

  /** Current ladder rung for a member (evidence + time), per the spec. */
  async escalationState(memberId: string): Promise<{ rung: number; diagnosis: StallDiagnosis }> {
    const member = await this.host.store.getMemberById(memberId);
    if (!member) throw new Error(`no member '${memberId}'`);
    const swarm = await this.host.store.getSwarm(member.swarmId);
    if (!swarm) throw new Error(`no swarm for member '${memberId}'`);
    const diagnosis = await this.diagnoseMember(swarm, member, await this.diagnosisContext(swarm));
    const { rung } = stepFor(diagnosis.reason, diagnosis.stallMs);
    return { rung, diagnosis };
  }

  /** Run ONE escalation step per stuck member per sweep. Called by the plugin
   * watchdog sweep after its existing pass (existing watchdog behavior is
   * untouched; the ladder layers on top). Returns the sweep's report. */
  async executeNext(swarmId: string): Promise<StallReport> {
    const report = await this.diagnose(swarmId);
    if (report.verdict === "healthy") return report;
    const swarm = await this.host.store.getSwarm(swarmId);
    if (!swarm) return report;
    // Never run ladder actions on a paused/stopping/failed swarm (e.g. an
    // emergency freeze): machinery is deliberately halted there — nudges,
    // blockers, and releases must not fight the shutdown.
    if (swarm.status !== "active") return report;
    const members = await this.host.store.listMembers(swarmId);
    const byName = new Map(members.map((m) => [m.name, m]));
    for (const d of report.members) {
      if (d.nextAction === "none") continue;
      const member = byName.get(d.memberName);
      if (!member) continue;
      try {
        await this.executeAction(swarm, member, d, { force: false });
      } catch (err) {
        console.error(`[swarm] stall ladder step for ${d.memberName} failed:`, (err as Error).message);
      }
    }
    // Healthy members drop their escalation bookkeeping (recovered / never stuck).
    for (const m of members) {
      if (m.role === "coordinator") continue;
      const d = report.members.find((x) => x.memberName === m.name);
      if (!d || d.nextAction === "none") this.clearMemberState(m.id);
    }
    return report;
  }

  /** Force-advance a named member one rung (swarm_stalls ladder —
   * coordinator-only). Bypasses time thresholds AND dedup windows: the caller
   * explicitly asked for the next step now. */
  async forceAdvance(swarmId: string, memberName: string): Promise<StallDiagnosis> {
    const member = await this.host.store.getMemberByName(swarmId, memberName);
    if (!member) throw new Error(`no member named '${memberName}'`);
    const swarm = await this.host.store.getSwarm(swarmId);
    if (!swarm) throw new Error(`no swarm '${swarmId}'`);
    const d = await this.diagnoseMember(swarm, member, await this.diagnosisContext(swarm));
    const seq = FORCE_SEQUENCES[d.reason];
    if (seq.length === 0) return d; // healthy — nothing to advance
    const idx = (this.forceIndex.get(member.id) ?? -1) + 1;
    const action = seq[Math.min(idx, seq.length - 1)]!;
    this.forceIndex.set(member.id, idx);
    const permissionId = await this.firstPendingId(member.id);
    const advanced: StallDiagnosis = { ...d, nextAction: action, recipe: this.recipeFor(swarm, member, d.reason, action, { permissionId, taskId: member.currentTaskId }) };
    await this.executeAction(swarm, member, advanced, { force: true });
    return advanced;
  }

  // ==== internals ====

  private recordLimit(swarmId: string, memberId: string, signal: string, evidence: string[]): UsageLimitRecord {
    let byMember = this.usageLimits.get(swarmId);
    if (!byMember) {
      byMember = new Map();
      this.usageLimits.set(swarmId, byMember);
    }
    const record: UsageLimitRecord = {
      memberId,
      signal,
      evidence,
      at: this.host.now(),
      remedy: `swarm_revive / model change, or wait for the limit window`,
    };
    byMember.set(memberId, record);
    return record;
  }

  private async firstPendingId(memberId: string): Promise<string | undefined> {
    const pending = await this.host.store.listPendingForMembers([memberId]);
    return pending[0]?.id;
  }

  private async diagnosisContext(swarm: Swarm): Promise<{
    tasks: SwarmTask[];
    pendingByMember: Map<string, PendingPermission[]>;
    pendingMail: Set<string>;
    now: number;
  }> {
    const now = this.host.now();
    const [tasks, pending, pendingMail] = await Promise.all([
      this.host.store.listTasks(swarm.id),
      this.host.store.listPendingPermissions(swarm.id),
      this.host.store.listMembersWithPendingMail(),
    ]);
    const pendingByMember = new Map<string, PendingPermission[]>();
    for (const p of pending) {
      const list = pendingByMember.get(p.memberId) ?? [];
      list.push(p);
      pendingByMember.set(p.memberId, list);
    }
    return { tasks, pendingByMember, pendingMail: new Set(pendingMail.map((x) => x.memberId)), now };
  }

  private async diagnoseMember(
    swarm: Swarm,
    m: SwarmMember,
    ctx: { tasks: SwarmTask[]; pendingByMember: Map<string, PendingPermission[]>; pendingMail: Set<string>; now: number },
  ): Promise<StallDiagnosis> {
    const { tasks, pendingByMember, pendingMail, now } = ctx;
    const base = { memberName: m.name, role: m.role, status: m.status };
    const lullMs = swarm.policies.humanChatLullMs ?? 300_000;

    // 1. Stopped — a tombstone, not a stall.
    if (m.status === "stopped") {
      return { ...base, reason: "stopped", stallMs: 0, evidence: ["member deliberately stopped"], nextAction: "none", recipe: "(none — healthy)" };
    }

    // 2. Permission wall — the member cannot proceed until the coordinator answers.
    const pending = pendingByMember.get(m.id) ?? [];
    if (pending.length > 0) {
      const createdAt = Math.min(...pending.map((p) => p.createdAt));
      const stallMs = Math.max(0, now - createdAt);
      const { action } = stepFor("permission-wall", stallMs);
      const p = pending[0]!;
      return {
        ...base,
        reason: "permission-wall",
        stallMs,
        evidence: pending.map((x) => `${x.type}${x.pattern ? ` ${fence(x.pattern)}` : ""} unanswered for ${Math.round((now - x.createdAt) / 1000)}s`),
        nextAction: action,
        recipe: this.recipeFor(swarm, m, "permission-wall", action, { permissionId: p.id, taskId: m.currentTaskId }),
      };
    }

    // 3. Usage limit — a recorded signal (broker revert / kickoff error)…
    const limitRec = this.usageLimits.get(swarm.id)?.get(m.id);
    if (limitRec) {
      const stallMs = Math.max(0, now - limitRec.at);
      const { action } = stepFor("usage-limit", stallMs);
      return {
        ...base,
        reason: "usage-limit",
        stallMs,
        evidence: [`model limit signal: ${limitRec.signal}`],
        nextAction: action,
        recipe: this.recipeFor(swarm, m, "usage-limit", action, { taskId: m.currentTaskId }),
      };
    }
    // …or a LIVE retry-status with a limit-like message (signal ii).
    try {
      const status = await this.host.runtimeAdapter.getStatus(m.sessionId);
      if (status && status.type === "retry" && isLimitSignal(status.message)) {
        const rec = this.recordLimit(swarm.id, m.id, status.message, [`session retry status: ${status.message}`]);
        const stallMs = Math.max(0, now - rec.at);
        const { action } = stepFor("usage-limit", stallMs);
        return {
          ...base,
          reason: "usage-limit",
          stallMs,
          evidence: [`session retry: ${status.message}`],
          nextAction: action,
          recipe: this.recipeFor(swarm, m, "usage-limit", action, { taskId: m.currentTaskId }),
        };
      }
    } catch {
      // status lookup is best-effort — the recorded-signal path still works
    }

    // 4. Chat-paused — the user is directly engaged; a legitimate pause.
    if (m.humanChatAt != null && now - m.humanChatAt < lullMs) {
      return {
        ...base,
        reason: "chat-paused",
        stallMs: Math.max(0, now - m.humanChatAt),
        evidence: ["user is directly chatting with this member"],
        nextAction: "none",
        recipe: "(none — swarm machinery resumes after the lull)",
      };
    }

    // 5. Session absent — an active member whose backing session vanished.
    let sessionExists = true;
    try {
      sessionExists = (await this.host.runtimeAdapter.getSession(m.sessionId)) != null;
    } catch {
      sessionExists = true; // runtime unreachable — treat as present
    }
    if (!sessionExists) {
      const anchor = m.lastActiveAt ?? m.createdAt;
      const stallMs = Math.max(0, now - anchor);
      const { action } = stepFor("session-absent", stallMs);
      return {
        ...base,
        reason: "session-absent",
        stallMs,
        evidence: ["session absent from the runtime (crashed or deleted)"],
        nextAction: action,
        recipe: this.recipeFor(swarm, m, "session-absent", action, { taskId: m.currentTaskId }),
      };
    }

    // 6. Lease-stuck — owns a task whose claim lease expired.
    const owned = m.currentTaskId ? tasks.find((t) => t.id === m.currentTaskId) : undefined;
    if (owned && owned.leaseExpiresAt !== undefined && owned.leaseExpiresAt < now) {
      const stallMs = Math.max(0, now - owned.leaseExpiresAt);
      const { action } = stepFor("lease-stuck", stallMs);
      return {
        ...base,
        reason: "lease-stuck",
        stallMs,
        evidence: [`task '${owned.id}' claim lease expired ${Math.round(stallMs / 1000)}s ago`],
        nextAction: action,
        recipe: this.recipeFor(swarm, m, "lease-stuck", action, { taskId: owned.id }),
      };
    }

    // Interrupted/failed with a live session: recovery/revive owns the
    // transition; not a live stall.
    if (m.status === "interrupted") {
      return { ...base, reason: "working", stallMs: 0, evidence: ["interrupted but session alive — the revive sweep resumes it"], nextAction: "none", recipe: "(none — healthy)" };
    }
    if (m.status === "failed") {
      return { ...base, reason: "stopped", stallMs: 0, evidence: ["member status is 'failed' (terminal)"], nextAction: "none", recipe: "(none — healthy)" };
    }

    // 7. Working/claimed/starting — liveness via session messages.
    if (["working", "claimed", "starting"].includes(m.status)) {
      const live = await this.memberLiveness(m, now);
      if (!live.silent) {
        return { ...base, reason: "working", stallMs: 0, evidence: ["session is producing activity"], nextAction: "none", recipe: "(none — healthy)" };
      }
      const stallMs = Math.max(0, now - live.anchor);
      const { action } = stepFor("session-silent", stallMs);
      return {
        ...base,
        reason: "session-silent",
        stallMs,
        evidence: [`no session activity for ${Math.round(stallMs / 1000)}s (silent; status ${m.status})`],
        nextAction: action,
        recipe: this.recipeFor(swarm, m, "session-silent", action, { taskId: m.currentTaskId }),
      };
    }

    // 8. Idle — awaiting assignment; with queued mail the F-M7 sweep delivers it.
    if (m.status === "idle") {
      if (pendingMail.has(m.id)) {
        return {
          ...base,
          reason: "awaiting-mail",
          stallMs: Math.max(0, now - (m.lastActiveAt ?? m.createdAt)),
          evidence: ["queued mailbox messages waiting for delivery"],
          nextAction: "none",
          recipe: `swarm_wake(swarmId: '${swarm.id}', member: '${m.name}')`,
        };
      }
      return { ...base, reason: "idle", stallMs: 0, evidence: ["idle, awaiting assignment"], nextAction: "none", recipe: "(none — healthy)" };
    }

    return { ...base, reason: "idle", stallMs: 0, evidence: [`status ${m.status}`], nextAction: "none", recipe: "(none — healthy)" };
  }

  /** Liveness = newest session message (excluding this diagnoser's own nudges),
   * floored at the member's lastActiveAt/createdAt so a freshly claimed member
   * mid-kickoff is not flagged silent (mirrors the plugin watchdog's grace). */
  private async memberLiveness(m: SwarmMember, now: number): Promise<{ silent: boolean; anchor: number }> {
    let latest = this.lastSeenActivity.get(m.id) ?? 0;
    const lastNudge = this.lastNudgeAt.get(m.id) ?? 0;
    if (this.host.runtimeAdapter.getMessages) {
      try {
        const msgs = await this.host.runtimeAdapter.getMessages(m.sessionId);
        for (let i = msgs.length - 1; i >= 0; i--) {
          const createdAt = msgs[i]?.createdAt;
          if (!createdAt) continue;
          if (lastNudge > 0 && createdAt >= lastNudge && createdAt - lastNudge < NUDGE_DEDUP_MS) continue;
          latest = Math.max(latest, createdAt);
          break;
        }
      } catch {
        // leave latest as-is
      }
    }
    const anchor = Math.max(latest, m.lastActiveAt ?? m.createdAt ?? 0);
    const silent = now - anchor > STALL_SILENT_MS;
    if (!silent) this.lastSeenActivity.set(m.id, anchor);
    return { silent, anchor };
  }

  private async executeAction(swarm: Swarm, member: SwarmMember, d: StallDiagnosis, opts: { force: boolean }): Promise<boolean> {
    const now = this.host.now();
    const key = `${member.id}:${d.reason}`;
    switch (d.nextAction) {
      case "nudge": {
        if (!opts.force && now - (this.lastNudgeAt.get(member.id) ?? 0) < NUDGE_DEDUP_MS) return false;
        this.lastNudgeAt.set(member.id, now);
        const tasks = await this.host.store.listTasks(swarm.id);
        const task = member.currentTaskId ? tasks.find((t) => t.id === member.currentTaskId) : undefined;
        const note = `[WATCHDOG] Your session appears stalled (no activity for several minutes). Continue task "${task?.title ?? member.currentTaskId ?? "(none)"}" — if you are blocked, send a "blocker" message to the coordinator.`;
        await this.host.nudgeMember(swarm.id, member, note);
        return true;
      }
      case "permission-notify": {
        if (!opts.force && now - (this.lastNotifyAt.get(key) ?? 0) < NOTIFY_DEDUP_MS) return false;
        this.lastNotifyAt.set(key, now);
        const pending = await this.host.store.listPendingForMembers([member.id]);
        for (const p of pending) {
          await this.host.notifyPermissionWall(p, `stall ladder: '${member.name}' stuck on a permission prompt for ${Math.round(d.stallMs / 1000)}s`);
        }
        return pending.length > 0;
      }
      case "usage-notify": {
        if (!opts.force && now - (this.lastNotifyAt.get(key) ?? 0) < NOTIFY_DEDUP_MS) return false;
        this.lastNotifyAt.set(key, now);
        const rec = this.usageLimits.get(swarm.id)?.get(member.id);
        if (rec) await this.host.notifyUsageLimit(member, rec);
        return !!rec;
      }
      case "coordinator-blocker": {
        if (!opts.force && now - (this.lastBlockerAt.get(key) ?? 0) < BLOCKER_DEDUP_MS) return false;
        this.lastBlockerAt.set(key, now);
        await this.host.notifyCoordinatorBlocker(swarm, member, d);
        return true;
      }
      case "release-task": {
        if (!member.currentTaskId) return false;
        if (!opts.force && now - (this.lastReleaseAt.get(member.id) ?? 0) < RELEASE_DEDUP_MS) return false;
        this.lastReleaseAt.set(member.id, now);
        return this.host.releaseMemberTask(member, `stall ladder rung 4: ${d.reason} for ${Math.round(d.stallMs / 1000)}s`);
      }
      case "respawn": {
        if (!opts.force && now - (this.lastRespawnAt.get(member.id) ?? 0) < RESPAWN_DEDUP_MS) return false;
        this.lastRespawnAt.set(member.id, now);
        const sid = await this.host.respawnMember(member);
        return !!sid;
      }
      default:
        return false;
    }
  }

  private clearMemberState(memberId: string): void {
    this.lastNudgeAt.delete(memberId);
    this.lastSeenActivity.delete(memberId);
    this.lastReleaseAt.delete(memberId);
    this.lastRespawnAt.delete(memberId);
    this.forceIndex.delete(memberId);
    // lastNotifyAt/lastBlockerAt are keyed `${memberId}:${reason}` — sweep them.
    for (const k of [...this.lastNotifyAt.keys()]) if (k.startsWith(`${memberId}:`)) this.lastNotifyAt.delete(k);
    for (const k of [...this.lastBlockerAt.keys()]) if (k.startsWith(`${memberId}:`)) this.lastBlockerAt.delete(k);
  }

  /** The exact tool call that resolves a stall, given reason + current action. */
  private recipeFor(
    swarm: Swarm,
    member: SwarmMember,
    reason: StallReason,
    action: StallAction,
    hint?: { permissionId?: string; taskId?: string },
  ): string {
    switch (reason) {
      case "permission-wall":
        return `swarm_permissions(swarmId: '${swarm.id}', action: 'reply', permissionId: '${hint?.permissionId ?? "<pending id>"}', response: 'once' | 'always' | 'reject')`;
      case "usage-limit":
        return USAGE_LIMIT_REMEDY(swarm.id, member.name);
      case "session-silent":
        if (action === "nudge") return `swarm_wake(swarmId: '${swarm.id}', member: '${member.name}')`;
        if (action === "release-task") return `swarm_tasks(swarmId: '${swarm.id}', action: 'list') — task '${hint?.taskId ?? "<task>"}' released to ready`;
        return "(blocker raised — the ONE 'why is my swarm stuck' answer is in the coordinator inbox)";
      case "lease-stuck":
        return `swarm_tasks(swarmId: '${swarm.id}', action: 'list') — task '${hint?.taskId ?? "<task>"}' lease expired; released to ready`;
      case "session-absent":
        return `swarm_revive(swarmId: '${swarm.id}', action: 'revive', strategy: 'keep')`;
      case "chat-paused":
        return "(none — user is chatting; swarm machinery resumes after the lull)";
      case "awaiting-mail":
        return `swarm_wake(swarmId: '${swarm.id}', member: '${member.name}')`;
      default:
        return "(none — healthy)";
    }
  }
}

/** One-line remedy per stall cause, for the swarm-level recommendation. */
function causeRemedy(reason: StallReason): string {
  switch (reason) {
    case "permission-wall":
      return `${reason}: answer the pending prompts via swarm_permissions reply`;
    case "usage-limit":
      return `${reason}: members hit model limits — swarm_revive / model change / wait for the limit window`;
    case "session-silent":
      return `${reason}: members silent — the escalation ladder nudges → blocks → releases`;
    case "session-absent":
      return `${reason}: sessions gone — respawn via swarm_revive revive`;
    case "lease-stuck":
      return `${reason}: expired-lease tasks released (ladder rung 4)`;
    default:
      return reason;
  }
}
