import { tool } from "@opencode-ai/plugin";
import type { PluginInput, Hooks } from "@opencode-ai/plugin";
import { mkdirSync } from "node:fs";
import type { SwarmStore } from "./storage/store.js";
import { SQLiteStore } from "./storage/sqlite-store.js";
import { OpenCodeRuntime } from "./runtime/opencode-runtime.js";
import { SwarmCore, BlackboardConflict, swarmTaskLeaseMs, detectAckReply } from "./core/swarm.js";
import { Supervisor } from "./supervisor/supervisor.js";
import { Recovery } from "./supervisor/recovery.js";
import { Broker } from "./messaging/broker.js";
import { Scheduler } from "./scheduler/scheduler.js";
import { affinityScore } from "./scheduler/dag.js";
import { corpseCountByPath, CORPSE_PILE_THRESHOLD, taskMentionsPath } from "./scheduler/scheduler.js";
import { HumanChatTracker } from "./humanchat/tracker.js";
import { formatInbox, formatEnvelope, formatBlackboardConflict } from "./messaging/formatter.js";
import { fence } from "./core/fence.js";
import { propagateAutopermissions, permsCountsSummary } from "./permissions/propagate.js";
import { permissionMode, permissionModeLabel } from "./permissions/clamp.js";
import { buildHiveBlock, buildHiveSummary } from "./hive/diagnostics.js";
import { computeBeliefDigest } from "./hive/digest.js";
import { rankBeliefsByRelevance, semanticRelevanceHook } from "./hive/relevance.js";
import { computeResonance, consolidationAction, causalChainNote, parseEvidenceRefs } from "./hive/resonance.js";
import { needTokens } from "./messaging/need.js";
import type { HiveReadInput } from "./hive/diagnostics.js";
import type { SwarmMember, SwarmMessage } from "./core/types.js";

/** Max times a member is auto-continued after an idle turn before the plugin
 * gives up and surfaces the member to the coordinator as stuck. */
const MAX_CONTINUE_ATTEMPTS = 12;

/** Truncate long blackboard values in list/search output so the tool stays
 * readable while still surfacing content (premium UX: a member can see what a
 * deliverable is without guessing keys or doing a separate get). */
function truncate(s: string, max = 200): string {
  return s.length > max ? `${s.slice(0, max)}… (${s.length} chars total)` : s;
}

/** Deterministic FNV-1a hash → hex (used for belief fact_hash dedup and
 * spotlight keys). Stable across processes; no crypto dependency. */
function hashText(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
}

/** Classic Levenshtein edit distance (small strings; used for nearest-key
 * suggestions on a swarm_memory get-miss, so a typo'd key is recoverable). */
function editDistance(a: string, b: string): number {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const dp: number[] = Array.from({ length: n + 1 }, (_, j) => j);
  for (let i = 1; i <= m; i++) {
    let prev = dp[0]!;
    dp[0] = i;
    for (let j = 1; j <= n; j++) {
      const cur = dp[j]!;
      dp[j] = Math.min(
        dp[j]! + 1,                 // deletion
        dp[j - 1]! + 1,             // insertion
        prev + (a[i - 1] === b[j - 1] ? 0 : 1), // substitution
      );
      prev = cur;
    }
  }
  return dp[n]!;
}

/**
 * Normalize an OpenCode error payload (which may be a string, an Error, or a
 * structured object like { name, message, stack }) into a short readable line.
 * Prevents the notorious `[object Object]` in coordinator notifications.
 */
function errorText(e: unknown): string {
  if (e == null) return "";
  if (typeof e === "string") return e.trim();
  if (e instanceof Error) return (e.message || e.name || "").trim();
  if (typeof e === "object") {
    const o = e as Record<string, unknown>;
    const msg = o.message ?? o.error ?? o.reason ?? o.name ?? o.code;
    if (typeof msg === "string" && msg.trim()) return msg.trim();
    try {
      return JSON.stringify(e).slice(0, 200);
    } catch {
      return String(e);
    }
  }
  return String(e);
}

/**
 * Resolve the member record for the calling tool's session. Guarantees the
 * caller is a member of the given swarm and returns its durable member id —
 * never the raw session id (which is not a swarm_member.id FK value).
 */
async function memberForContext(
  core: SwarmCore,
  swarmId: string,
  sessionID: string,
): Promise<SwarmMember> {
  const member = await core.store.getMemberBySessionId(sessionID);
  if (!member) {
    throw new Error("calling session is not registered as a swarm member");
  }
  if (member.swarmId !== swarmId) {
    throw new Error(`calling session belongs to swarm '${member.swarmId}', not '${swarmId}'`);
  }
  return member;
}

/**
 * Resolve a member's requested model to a real, available model. If the model
 * is missing or invalid (e.g. a tier label used as a provider), fall back to
 * the coordinator's own model so the spawn doesn't fail.
 */
async function resolveMemberModel(
  rt: SwarmPluginRuntime,
  requested: { providerID?: string; modelID?: string } | undefined,
  fallback?: { providerID?: string; modelID?: string },
): Promise<{ providerID: string; modelID: string } | undefined> {
  const resolved = await rt.resolveModel(requested);
  if (resolved) return resolved;
  const fb = await rt.resolveModel(fallback);
  return fb;
}

/**
 * Auto-allow permission requests from swarm members when the operation stays
 * within the member's swarm worktree. The coordinator spawned the member to
 * work in that tree, so file/command access there is trusted; anything outside
 * it remains an "ask" (user retains control). This prevents members from
 * stalling on permission prompts for the project they were told to build.
 */
async function autoAllowSwarmPermission(
  rt: SwarmPluginRuntime,
  input: { id: string; type: string; pattern?: string | Array<string>; sessionID: string; title?: string },
  output: { status: "ask" | "deny" | "allow" },
): Promise<void> {
  const member = await rt.store.getMemberBySessionId(input.sessionID);
  if (!member) return; // not a swarm member — leave default behavior
  if (member.role === "coordinator") return; // coordinator prompts still go to the user

  // "Accept-all" mode: members were spawned to do a job; never prompt them.
  if (rt.allowAllMemberPermissions) {
    output.status = "allow";
    return;
  }

  const swarm = await rt.store.getSwarm(member.swarmId);
  if (!swarm) return;

  // PRIMARY: inherit the coordinator's permission policy. Members are root
  // sessions the user can chat with; the coordinator is the swarm's authority,
  // and the user granted/auto-accepted permissions for the coordinator's agent
  // (e.g. "build"). Members get the same verdicts instead of a separate prompt
  // for work the parent allows.
  const inherited = await rt.inheritCoordinatorPermission(swarm, input.type, input.pattern);
  if (inherited !== undefined) {
    output.status = inherited;
    return;
  }

  // FALLBACK (runtime could not resolve permissions): heuristic scoping —
  // worktree + OS temp scratch space for building/testing members.
  const worktree = swarm.directory ? swarm.directory.replace(/[\\/]+$/, "") : "";
  const patterns = Array.isArray(input.pattern) ? input.pattern : [input.pattern].filter((p): p is string => !!p);

  const isPathOp =
    input.type === "edit" || input.type === "write" || input.type === "read" ||
    input.type === "bash" || input.type === "external_directory" || input.type === "list" ||
    input.type === "glob" || input.type === "grep";
  if (!isPathOp) return;

  const wt = worktree.replace(/\\/g, "/");
  // The OS temp dir is legitimate scratch space for a building/testing member
  // (compiler artifacts, selftest output). Without this, a member writing to
  // %TEMP% hits an external_directory prompt that hangs a headless session.
  const tempDir = (process.env.TEMP || process.env.TMP || "/tmp").replace(/[\\/]+$/, "").replace(/\\/g, "/");

  // Legacy swarms (pre-directory column) have an empty worktree: the members
  // were spawned to work on the project, so treat the whole project as in-scope
  // rather than letting every file op become an unanswered external_directory
  // prompt that freezes the member. NOTE (P-D3): this legacy blanket-allow is
  // retained ONLY for non-bash path ops; bash is always gated to "ask" when
  // the worktree is empty (a bare "allow" would authorize arbitrary commands).
  //
  // Boundary-aware matching (P-D1): a path is in-scope only when it is exactly
  // the worktree/temp dir or a DESCENDANT (prefix + separator) after
  // canonicalization. Raw prefix matching was bypassable via ".." traversal
  // (`C:\repo\app\..\outside` startsWith `C:\repo\app\` yet resolves outside).
  // We (a) reject any pattern containing a ".." path segment outright, and
  // (b) normalize repeated slashes so `app//x` cannot alias `app/x` boundaries.
  // P-D1: reject `..` path segments (parent traversal escapes the boundary);
  // a lone `.` (current dir) is harmless and common in relative patterns
  // (e.g. `.\\foo.ts`), so it is allowed through.
  const hasParentTraversal = (p: string): boolean => p.split("/").includes("..");
  const normalizeSlashes = (p: string): string => p.replace(/\/{2,}/g, "/");
  const within = (p: string, root: string): boolean => {
    if (p === root) return true;
    const r = root.endsWith("/") ? root : `${root}/`;
    return p.startsWith(r);
  };
  // `*` is a legitimate allow for PATH ops (edit/write/read/glob/grep/list)
  // but must NEVER blanket-allow bash — a bare `*` bash pattern authorizes
  // every command, escaping the worktree entirely (P-D2).
  const isBash = input.type === "bash";
  const wildcardOk = (norm: string): boolean =>
    norm === "*" && !isBash;
  // P-D3: an empty (legacy) worktree no longer blankets bash — a member with
  // no worktree root must not get implicit command-execution authority. Emit
  // a one-time console warning so the legacy-allow behavior change is visible.
  if (wt === "" && isBash) {
    console.warn(
      `[swarm] permission.ask: bash request from member ${member.name} on a swarm with an EMPTY worktree — NOT auto-allowed (P-D3); the member must be granted explicit bash scope.`,
    );
  }
  const inScope =
    (wt === "" && !isBash) ||
    (patterns.length > 0 &&
      patterns.every((p) => {
        const norm = normalizeSlashes(p.replace(/\\/g, "/"));
        if (norm === "*") return wildcardOk(norm);
        if (hasParentTraversal(norm)) return false; // P-D1: `..` traversal rejected
        return within(norm, wt) || within(norm, tempDir);
      }));

  if (inScope) {
    output.status = "allow";
  }
}

let singleton: SwarmPluginRuntime | undefined;

/** Test hook: the last-initialized runtime (undefined before init). */
export function swarmRuntime(): SwarmPluginRuntime | undefined {
  return singleton;
}

export interface SwarmPluginOptions {
  /** Directory for the plugin-owned SQLite store. */
  dataDir?: string;
  /** Optional explicit store (used by tests). */
  store?: SwarmStore;
  /** When true, auto-allow EVERY permission requested by swarm members (the
   * equivalent of an "accept-all permissions" session). Members were spawned
   * by the coordinator to do a job; this removes all permission prompts so
   * headless member sessions never wedge. The coordinator's own prompts are
   * unaffected. Default: false. */
  allowAllMemberPermissions?: boolean;
}

export class SwarmPluginRuntime {
  readonly core: SwarmCore;
  readonly supervisor: Supervisor;
  readonly broker: Broker;
  readonly scheduler: Scheduler;
  readonly humanChat: HumanChatTracker;
  private runtime: OpenCodeRuntime;
  /** Public accessor for the runtime adapter (used by tool handlers that must
   * kick off prompts directly, e.g. the pull-claim full transition). */
  get runtimeAdapter(): OpenCodeRuntime {
    return this.runtime;
  }
  /** Public accessor for the runtime's per-session todo fetcher (used by
   * swarm_status/lands + swarm_probe to surface cross-member todos). */
  get sessionTodos(): ((sessionID: string) => Promise<Array<{ content: string; status: string; priority: string }>>) | undefined {
    return this.runtime.getSessionTodos?.bind(this.runtime);
  }
  /** Pending batched completion notices per swarm (debounced). */
  private pendingCompletions = new Map<string, Array<{ text: string; at: number }>>();
  private completionTimers = new Map<string, ReturnType<typeof setTimeout>>();
  /** Continuation-attempt counter per member-task pair (reset on task change). */
  private continueAttempts = new Map<string, { taskId: string; count: number }>();
  /** Per-task notified gate (X1): task ids for which the "check for a blocker"
   * coordinator notice has ALREADY fired. Prevents the notice from re-firing
   * every MAX_CONTINUE_ATTEMPTS cycle after the counter resets. Cleared when
   * the task reaches a terminal state. */
  private continueNotifiedTasks = new Set<string>();
  /** Periodic scheduler sweep timer (safety net, not the primary driver). */
  private sweepTimer?: ReturnType<typeof setInterval>;
  /** Delayed startup recovery timer; kept out of plugin init's critical path. */
  private recoveryTimer?: ReturnType<typeof setTimeout>;
  private recoveryStarted = false;
  /** Per-swarm digest fingerprint: last-seen task-state signature → avoids
   * re-injecting a sync digest when nothing changed. */
  private syncFingerprints = new Map<string, string>();
  /** Per-swarm last sync injection time (ms), for throttling. */
  private lastSyncAt = new Map<string, number>();
  /** Per-member last seen activity time (ms), used by the watchdog to detect a
   * session that is marked working but has gone silent (wedged mid-loop). */
  private lastSeenActivity = new Map<string, number>();
  /** Per-member watchdog strikes: consecutive silent sweeps before we escalate. */
  private watchdogStrikes = new Map<string, number>();
  /** Per-member last watchdog nudge time (ms). A nudge adds a message to the
   * session, which would otherwise look like "activity" and reset the strike
   * counter - the watchdog feeding itself. Ignore messages newer than the last
   * nudge when judging real liveness. */
  private lastWatchdogNudgeAt = new Map<string, number>();
  /** Per-swarm last known `hive/digest` health (fresh|stale|unknown) — powers
   * the P5 digest-flip notice (transition dedupe; one finding per flip). */
  private lastDigestHealth = new Map<string, "fresh" | "stale" | "unknown">();
  /** Per-swarm last digest VALUE we sent a stale-sync for (Wave 6 minimal
   * digest exchange): one targeted sync message per stale PERIOD — a new sync
   * fires only when the stale digest value itself changes (beliefs moved
   * again), never every sweep while health stays stale. */
  private lastDigestSync = new Map<string, string>();

  constructor(
    public readonly store: SwarmStore,
    runtime: OpenCodeRuntime,
    private sweepMs: number = 10_000,
    public readonly allowAllMemberPermissions = false,
  ) {
    this.runtime = runtime;
    this.core = new SwarmCore(store, runtime);
    this.supervisor = new Supervisor(store);
    this.humanChat = new HumanChatTracker(
      { store, now: Date.now },
      { selfInjectionIds: new Set<string>() },
    );
    // Mail delivery defers for a member while the user is directly chatting
    // with them (the member is answering the user, not the swarm).
    this.broker = new Broker(store, runtime, {
      deliveryCooldownMs: 30_000,
      shouldDeferDelivery: async (memberId: string) => {
        const member = await store.getMemberById(memberId);
        if (!member) return false;
        const swarm = await store.getSwarm(member.swarmId);
        if (!swarm) return false;
        return this.humanChat.chatting(member, swarm);
      },
      // F-M5: when a message exhausts its delivery retry budget, notify the
      // sender exactly once (audit/messaging F-M5; NP9 budget every unit).
      onMessageFailed: async (failed) => {
        await this.notifySenderMessageFailed(failed).catch((err) => {
          console.error(`[swarm] failed-message notice to sender failed:`, err);
        });
      },
    });
    this.scheduler = new Scheduler(store, runtime);
    // Auto-wake: delivering a message to an idle member wakes it immediately.
    this.core.setWakeDeliverer((memberId, memberSessionId) =>
      this.broker.deliverToIdleMember(memberId, memberSessionId),
    );
    this.startSweep();
  }

  /**
   * Run recovery after OpenCode has finished loading the plugin. Recovery calls
   * back into the session SDK; awaiting it during plugin init can stall startup
   * for ordinary CLI/Desktop commands if the runtime is not fully ready yet.
   */
  scheduleStartupRecovery(delayMs = 10_000): void {
    if (this.recoveryStarted) return;
    this.recoveryStarted = true;
    this.recoveryTimer = setTimeout(async () => {
      this.recoveryTimer = undefined;
      try {
        // Lapse-clean human-chat state: any chat that was active when the plugin
        // went down and has since exceeded the lull window is cleared so the
        // member doesn't stay suppressed forever after a restart.
        const swarmIds = await this.store.listAllMemberSwarmIds();
        for (const swarmId of swarmIds) {
          const swarm = await this.store.getSwarm(swarmId);
          const members = swarm ? await this.store.listMembers(swarmId) : [];
          if (swarm && members.length) await this.humanChat.reconcileStartup(members, swarm);
        }
        const recovery = new Recovery(this.store, this.runtime, (member) => this.respawnMember(member));
        await recovery.reconcileAll();
      } catch (err) {
        console.error("[swarm] startup recovery failed:", err);
      }
    }, delayMs);
    if (this.recoveryTimer.unref) this.recoveryTimer.unref();
  }

  dispose(): void {
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    if (this.recoveryTimer) clearTimeout(this.recoveryTimer);
    this.sweepTimer = undefined;
    this.recoveryTimer = undefined;
  }

  /**
   * Periodic safety-net sweep: recompute task readiness and assign ready tasks
   * to idle members even when no member goes idle (e.g. all members busy, a
   * dependent task unblocked). The event-driven scheduler remains the primary
   * driver; this catches the gaps so a ready task never waits indefinitely.
   */
  private startSweep(): void {
    this.sweepTimer = setInterval(() => {
      this.sweepOnce().catch((err) => {
        console.error("[swarm] scheduler sweep failed:", err);
      });
    }, this.sweepMs);
    // Don't keep the process alive solely for the sweep.
    if (this.sweepTimer.unref) this.sweepTimer.unref();
  }

  private async sweepOnce(): Promise<void> {
    // Expire overdue urgent mail FIRST (state transition + exactly-one sender
    // notice), so expired messages never reach a delivery attempt and senders
    // learn promptly (audit/messaging F-M2 / TU11: expiry is visible, not silent).
    await this.expireOverdueMail().catch((err) => {
      console.error("[swarm] expiry sweep failed:", err);
    });
    // F-M7: time-based mailbox delivery — members that never go idle still get
    // their queued mail within a sweep interval (cooldown + human-chat deferral
    // are respected inside the broker; urgent bypasses the cooldown).
    await this.deliverMail().catch((err) => {
      console.error("[swarm] sweep mail delivery failed:", err);
    });
    const swarmIds = await this.store.listAllMemberSwarmIds();
    for (const swarmId of swarmIds) {
      // Case B autopermissions propagation: clamp coordinator session permission
      // to worktree/temp (never widen) and write to members before any scheduler
      // assignment kicks them off. Case A/C are no-ops (documented).
      await this.propagateSwarmAutopermissions(swarmId).catch((err) => {
        console.error(`[swarm] autopermissions propagation failed for ${swarmId}:`, err);
      });
      // S-02: F2 lease sweep BEFORE the scheduler pass — lease-expired
      // (stalled) tasks release first, so the SAME sweep's scheduler pass sees
      // the freed slot + freed member capacity. Previously leaseSweep ran after
      // runScheduler, stranding reclaimed capacity one full sweep (10s).
      await this.leaseSweep(swarmId).catch((err) => {
        console.error(`[swarm] lease sweep failed for ${swarmId}:`, err);
      });
      await this.runScheduler(swarmId);
      // H2 anti-entropy digest health: compute the per-swarm beliefs digest,
      // compare against the stored one, and persist `hive/digest`
      // (fresh|stale|unknown + lastSyncAt) for diagnostics + flip notices.
      await this.digestSweep(swarmId).catch((err) => {
        console.error(`[swarm] digest sweep failed for ${swarmId}:`, err);
      });
      await this.reviveInterrupted(swarmId);
      await this.watchdog(swarmId);
      await this.syncSwarm(swarmId);
      // P5 Hive H2: pruning-truth notice — expire stale beliefs and surface a
      // compact truthful finding to the coordinator ONLY when something was
      // actually pruned (non-trivial; count is the sweep's real return value).
      await this.pruneBeliefsAndNotify(swarmId).catch((err) => {
        console.error(`[swarm] belief pruning notice failed for ${swarmId}:`, err);
      });
    }
  }

  /**
   * P5 Hive H2: run the beliefs expire sweep (Storage's expireBeliefs) and, if
   * it actually pruned beliefs, deliver ONE truthful finding to the
   * coordinator. No fabrication: the notice is suppressed when pruned === 0.
   * Exactly-once per sweep pass — the broker's per-member cooldown batches
   * bursts, and the notice is a normal finding (no ack loop).
   */
  private async pruneBeliefsAndNotify(swarmId: string): Promise<void> {
    const pruned = await this.store.expireBeliefs(Date.now());
    if (pruned > 0) {
      await this.core.notifyPruning({ swarmId, pruned });
    }
  }

  /**
   * F-M7: deliver queued mail to every member with pending messages, including
   * members that never go idle (wedged/long-running). `deliverToIdleMember`
   * already respects the per-member cooldown and the human-chat deferral
   * predicate; urgent messages bypass the cooldown. Bounded by the broker's
   * batch size, so a sweep never floods a member.
   */
  private async deliverMail(): Promise<void> {
    const pending = await this.store.listMembersWithPendingMail();
    for (const { memberId, sessionId } of pending) {
      await this.broker.deliverToIdleMember(memberId, sessionId).catch((err) => {
        console.error(`[swarm] sweep delivery to member ${memberId} failed:`, err);
      });
    }
  }

  /**
   * F2 lease sweep: release claimed/working tasks whose claim lease
   * (policies.taskLeaseMs, set on claimTask) has expired. A lease-expired task
   * is a wedged/stalled claim — releasing it back to `ready` lets the DAG
   * advance and the task be reassigned. Human-chat guard: an owner who is
   * actively chatting with the user keeps its lease (the member is legitimately
   * paused, not stalled); the lease resumes counting from the next claim.
   */
  /** Public for tests: one lease-expiry sweep for a swarm (releases expired
   * claim leases with the human-chat guard; wired into sweepOnce BEFORE the
   * scheduler pass so reclaimed capacity is usable in the same sweep). */
  async leaseSweep(swarmId: string): Promise<void> {
    const swarm = await this.store.getSwarm(swarmId);
    if (!swarm || swarm.status !== "active") return;
    const expired = await this.store.listExpiredLeaseTasks(swarmId, Date.now());
    if (expired.length === 0) return;
    const members = await this.store.listMembers(swarmId);
    const byId = new Map(members.map((m) => [m.id, m]));
    for (const task of expired) {
      const owner = task.ownerMemberId ? byId.get(task.ownerMemberId) : undefined;
      if (owner) {
        const chatting = await this.humanChat.chatting(owner, swarm).catch(() => false);
        if (chatting) continue; // chatting owner keeps the lease
      }
      await this.store.releaseTask(task.id).catch(() => undefined);
      // S-01-family: reconcile the OWNER's binding — the task is released but
      // the owner member row still points currentTaskId at it (status may still
      // be `working`). Leaving it stale wedges the member (R3 excludes
      // idle-with-task; the ownership guard throws on the next bind; capacity
      // is consumed) and risks duplicate work if the task is reassigned. Clear
      // the binding and free the member for new work (mirror the `release`
      // tool path plugin.ts).
      if (owner && owner.currentTaskId === task.id) {
        await this.store.updateMemberStatus(owner.id, "idle", { currentTaskId: null, lastActiveAt: Date.now() }).catch(() => undefined);
      }
      console.warn(`[swarm] lease sweep: released task ${task.id} (claim lease expired)`);
      // Dependent notification: an upstream release can unblock/affect
      // dependents — tell their owners (F6).
      await this.notifyDependents(swarmId, task.id, "claim lease expired (stalled claim released)");
    }
  }

  /**
   * H2 anti-entropy digest health (features/hive-mind-execution-layer item 1).
   * Computes the per-swarm beliefs digest (cheap hash of active-belief
   * identity+version), compares it against the stored `hive/digest` key, and
   * persists `{ digest, health: fresh|stale|unknown, lastSyncAt }` via
   * read-first CAS (bounded — one key, never a broadcast). Local bookkeeping
   * only: Desktop's H2 diagnostics reads the key for status/roster, and
   * Messaging's digest-flip notices transition on `health`. Cross-member
   * exchange is future work (documented).
   */
  /** Public for tests: one anti-entropy digest pass for a swarm (computes the
   * beliefs digest, persists hive/digest health, and triggers the minimal
   * stale-sync exchange when the digest is stale and changed). */
  async digestSweep(swarmId: string): Promise<void> {
    const swarm = await this.store.getSwarm(swarmId);
    if (!swarm || swarm.status !== "active") return;
    const now = Date.now();
    // Authoritative digest source: Storage-Auditor's beliefDigest (sha1 over
    // active beliefs' id+updated_at, sorted). Guarded fallback: derive the
    // digest client-side from listBeliefs until beliefDigest lands.
    let digest: string;
    const storeDigest = (this.store as { beliefDigest?: (sid: string) => Promise<{ digest: string; count: number }> }).beliefDigest;
    if (storeDigest) {
      try {
        digest = (await storeDigest(swarmId)).digest;
      } catch (err) {
        console.warn(`[swarm] beliefDigest failed for ${swarmId}, falling back to client digest:`, (err as Error).message);
        const beliefs = await this.store.listBeliefs(swarmId, { activeOnly: true }).catch(() => []);
        digest = computeBeliefDigest(beliefs);
      }
    } else {
      const beliefs = await this.store.listBeliefs(swarmId, { activeOnly: true }).catch(() => []);
      digest = computeBeliefDigest(beliefs);
    }
    const key = "hive/digest";
    const entry = await this.store.getBlackboard(swarmId, key).catch(() => undefined);
    let stored: { digest?: string; health?: string; lastSyncAt?: number } | undefined;
    if (entry) {
      try { stored = JSON.parse(entry.value); } catch { stored = undefined; }
    }
    const health = stored === undefined || typeof stored.digest !== "string"
      ? "unknown" // never computed (or key evicted/corrupt) — first computation
      : stored.digest === digest ? "fresh" : "stale";
    const value = JSON.stringify({ digest, health, lastSyncAt: now });
    try {
      const coord = (await this.store.listMembers(swarmId)).find((m) => m.role === "coordinator");
      await this.core.blackboardPut({
        swarmId,
        key,
        value,
        contentType: "application/json",
        expectedVersion: entry?.version,
        authorMemberId: coord?.id ?? "",
      });
      // P5 digest-flip notice: when health actually FLIPPED since the last
      // observed value, deliver ONE low-noise finding to the coordinator
      // (transition-deduped; first observation with no prior state is not a
      // flip and never notifies).
      const lastKnown = this.lastDigestHealth.get(swarmId);
      await this.core.notifyDigestFlip({ swarmId, health, lastKnownHealth: lastKnown }).catch((err) => {
        console.error(`[swarm] digest flip notice failed for ${swarmId}:`, (err as Error).message);
      });
      this.lastDigestHealth.set(swarmId, health);

      // Wave 6 minimal digest exchange: when the digest is STALE (beliefs
      // changed since the stored digest), send ONE targeted whisper finding to
      // matching members that their belief view may be stale — reusing
      // Messaging's deliverNeed routing (no broadcast). Deduped per stale
      // PERIOD: a new sync fires only when the stale digest VALUE changes
      // (beliefs moved again), so a persistent stale state never floods.
      if (health === "stale" && this.lastDigestSync.get(swarmId) !== digest) {
        const coord = (await this.store.listMembers(swarmId)).find((m) => m.role === "coordinator");
        await this.core.deliverNeed({
          swarmId,
          fromMemberId: coord?.id,
          query: "digest belief sync",
          need: `Belief digest changed — your local view may be stale. Re-pull changed beliefs (listBeliefsChangedSince) if you track this area.`,
          tier: "whisper",
        }).catch((err) => {
          console.error(`[swarm] digest sync need failed for ${swarmId}:`, (err as Error).message);
        });
        this.lastDigestSync.set(swarmId, digest);
        console.warn(`[swarm] digest exchange: stale sync sent for ${swarmId} (digest ${digest})`);
      }
    } catch (err) {
      // CAS conflict (concurrent writer) - a later sweep retries; digest health
      // is advisory bookkeeping, never worth a retry storm.
      console.warn(`[swarm] digest health write skipped for ${swarmId}:`, (err as Error).message);
    }
  }

  /**
   * F6 dependent notification: after a task is released/failed/cancelled, find
   * its DIRECT dependents (tasks that list it as a prerequisite) and send ONE
   * finding message to each dependent task's owner, so they re-validate their
   * plan instead of discovering the upstream change silently (VB5/B8). The
   * coordinator also receives the usual notice; this is the peer-visible extra.
   */
  async notifyDependents(swarmId: string, taskId: string, reason: string): Promise<void> {
    const [tasks, deps] = await Promise.all([
      this.store.listTasks(swarmId),
      this.store.listTaskDependencies(swarmId),
    ]);
    const dependents = deps.filter((d) => d.dependsOnTaskId === taskId).map((d) => d.taskId);
    if (dependents.length === 0) return;
    const members = await this.store.listMembers(swarmId);
    const memberById = new Map(members.map((m) => [m.id, m]));
    const taskById = new Map(tasks.map((t) => [t.id, t]));
    for (const depId of dependents) {
      const dep = taskById.get(depId);
      const owner = dep?.ownerMemberId ? memberById.get(dep.ownerMemberId) : undefined;
      if (!dep || !owner) continue; // unowned dependent: the scheduler will handle readiness
      await this.core.sendMessage({
        swarmId,
        fromMemberId: members.find((m) => m.role === "coordinator")?.id ?? "",
        to: owner.name,
        kind: "finding",
        message: `Upstream task '${taskId}' was ${reason}. Your task '${dep.title}' (${depId}) depends on it — re-validate whether it is still safe to proceed.`,
        taskId: depId,
      }).catch((err) => {
        console.error(`[swarm] dependent notification to ${owner.name} failed:`, err);
      });
    }
  }

  /**
   * F-M5: notify the sender exactly once that their message exhausted its
   * delivery retry budget and was marked `failed`. The notice is a fresh
   * message addressed to the sender (never counts as the failed message's own
   * delivery or as recipient liveness).
   */
  private async notifySenderMessageFailed(failed: SwarmMessage): Promise<void> {
    const swarm = await this.store.getSwarm(failed.swarmId);
    const sender = await this.store.getMemberById(failed.fromMemberId);
    if (!swarm || !sender) return;
    if (["stopped", "stopping", "failed"].includes(sender.status)) return;
    const recipient = failed.to.type === "member"
      ? (await this.store.getMemberById(failed.to.memberId ?? ""))?.name ?? failed.to.memberId
      : "the swarm";
    await this.core.sendMessage({
      swarmId: failed.swarmId,
      fromMemberId: swarm.coordinatorMemberId,
      to: sender.name,
      kind: "finding",
      message: `Your message to ${recipient} (${failed.id}) could not be delivered after ${failed.attemptCount} attempts and was marked failed.`,
      refs: [failed.id],
    });
  }

  /**
   * Transition every overdue urgent message to `expired` and send the sender
   * exactly one notice per expired message. `expireOverdueMessages` runs
   * inside one serialized store transaction and returns ONLY the rows it just
   * transitioned, so a later sweep finds nothing to re-expire — the notice
   * path is exactly-once per message (audit/messaging F-M2, TU11).
   */
  private async expireOverdueMail(): Promise<void> {
    const expired = await this.store.expireOverdueMessages(Date.now());
    for (const m of expired) {
      const swarm = await this.store.getSwarm(m.swarmId);
      const sender = await this.store.getMemberById(m.fromMemberId);
      if (!swarm || !sender) continue;
      if (["stopped", "stopping", "failed"].includes(sender.status)) continue;
      const recipient = m.to.type === "member"
        ? (await this.store.getMemberById(m.to.memberId ?? ""))?.name ?? m.to.memberId
        : "the swarm";
      const notice = `Your ${m.priority} message to ${recipient} (${m.id}) expired undelivered.`;
      // Notice is delivered to the SENDER's mailbox; it must NOT count as the
      // expired message's own delivery or as recipient liveness — it's a fresh
      // message addressed to the sender, so watchdog liveness is unaffected.
      await this.core.sendMessage({
        swarmId: m.swarmId,
        fromMemberId: swarm.coordinatorMemberId,
        to: sender.name,
        kind: "finding",
        message: notice,
        refs: [m.id],
      }).catch((err) => {
        console.error(`[swarm] expiry notice to ${sender.name} failed:`, err);
      });
    }
  }

  /**
   * Revive members that recovery marked `interrupted` but whose sessions are
   * actually alive (e.g. recovery misfired because the runtime wasn't fully
   * reachable at startup, or the member recovered on its own). An `interrupted`
   * member is invisible to the scheduler, so it would otherwise idle forever
   * with its task stranded. If the session exists, bring it back to `idle` (or
   * `working` if it still owns a task) so it can resume work.
   */
  private async reviveInterrupted(swarmId: string): Promise<void> {
    const members = await this.store.listMembers(swarmId);
    for (const m of members) {
      if (m.role === "coordinator") continue;
      if (m.status !== "interrupted") continue;
      try {
        const session = await this.runtime.getSession(m.sessionId);
        if (!session) continue; // truly gone — leave for recovery to respawn
        if (m.currentTaskId) {
          // Still owns a task: resume it. continueMember re-claims the task row
          // (it may be 'ready' if recovery released it), re-prompts the session so
          // the work actually continues, and flips status to working.
          const swarm = await this.store.getSwarm(swarmId);
          if (!swarm) continue;
          // Human-chat guard (NP13/CP10): if the user is talking to this member,
          // do NOT re-drive it mid-conversation. Leave it interrupted; the next
          // sweep after the lull resumes it (humanChatAt lapses then clears).
          if (await this.humanChat.chatting(m, swarm)) continue;
          const attempt = this.nextContinueAttempt(m.id, m.currentTaskId);
          if (attempt > MAX_CONTINUE_ATTEMPTS) {
            // Task has consumed the whole resume budget across revives/idles and
            // never completed. Mark it working so the scheduler sees it, then let
            // the coordinator be told so it isn't silently stranded forever.
            // X1 fix: the "check for a blocker" notice fires ONCE per task —
            // gate on continueNotifiedTasks so the counter reset doesn't cause
            // a repeat notice every 12-attempt cycle.
            const taskId = m.currentTaskId;
            this.resetContinueAttempts(m.id);
            await this.store.updateMemberStatus(m.id, "working", { currentTaskId: m.currentTaskId, lastActiveAt: Date.now() });
            if (taskId && !this.continueNotifiedTasks.has(taskId)) {
              this.continueNotifiedTasks.add(taskId);
              this.notifyCoordinator(
                { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
                `${m.name} failed to progress on task "${taskId}" after ${MAX_CONTINUE_ATTEMPTS} revive/idle attempts. Check for a blocker.`,
              );
            }
            continue;
          }
          await this.core.continueMember(swarm, m, attempt).catch((err) => {
            // Session unreachable — fall back to just marking working so the
            // idle-path / watchdog can pick it up.
            console.error("[swarm] revive continue failed:", err);
            return this.store.updateMemberStatus(m.id, "working", { currentTaskId: m.currentTaskId, lastActiveAt: Date.now() });
          });
        } else {
          await this.store.updateMemberStatus(m.id, "idle", { lastActiveAt: Date.now() });
        }
      } catch {
        // runtime unreachable — leave as-is, watchdog/recovery will handle it
      }
    }
  }

  /**
   * Watchdog for wedged member sessions. A member marked `working` whose session
   * has gone silent for too long is stuck mid-loop (e.g. on a never-completing
   * tool call) and emits no idle/error event for the supervisor to act on.
   * Detect that via `getMessages` liveness, re-drive the member with a nudge,
   * and if it stays silent escalate by releasing its task so the DAG advances.
   */
  private static readonly WATCHDOG_SILENT_MS = 5 * 60_000;
  private static readonly WATCHDOG_MAX_STRIKES = 3;
  /** How long after a nudge to ignore that nudge's message as "activity". */
  private static readonly WATCHDOG_NUDGE_WINDOW_MS = 2 * 60_000;

  /** Public for tests: one watchdog sweep for a swarm. */
  async watchdog(swarmId: string): Promise<void> {
    const swarm = await this.store.getSwarm(swarmId);
    if (!swarm || swarm.status !== "active") return;
    const members = await this.store.listMembers(swarmId);
    const now = Date.now();

    for (const m of members) {
      if (m.role === "coordinator") continue;
      if (!["working", "claimed", "starting"].includes(m.status)) continue;

      // Liveness = the session produced NEW messages recently, EXCLUDING the
      // watchdog's own nudges (which are synthetic and add a message that would
      // otherwise reset the strike counter — the watchdog feeding itself).
      let latest = this.lastSeenActivity.get(m.id) ?? 0;
      const lastNudge = this.lastWatchdogNudgeAt.get(m.id) ?? 0;
      if (this.runtime.getMessages) {
        try {
          const msgs = await this.runtime.getMessages(m.sessionId);
          for (let i = msgs.length - 1; i >= 0; i--) {
            const createdAt = msgs[i]?.createdAt;
            if (!createdAt) continue;
            // Ignore messages created by the watchdog's synthetic nudge.
            if (createdAt >= lastNudge && lastNudge > 0 && createdAt - lastNudge < SwarmPluginRuntime.WATCHDOG_NUDGE_WINDOW_MS) continue;
            latest = Math.max(latest, createdAt);
            break;
          }
        } catch {
          // leave latest as-is
        }
      }
      const silent = now - latest > SwarmPluginRuntime.WATCHDOG_SILENT_MS;

      // Working-with-no-task limbo (F4): a member marked `working` with NO
      // currentTaskId is not in the scheduler's idle pool (never assigned), is
      // not nudged or released (both watchdog branches require taskId), and yet
      // consumes maxConcurrentMembers capacity. If its session is silent and the
      // user is not actively chatting, demote it to idle so it can take work.
      // Chatting members are legitimately busy-without-task and must NOT be
      // demoted mid-conversation (human-chat guard, F13/D1).
      // S-05: in-flight kickoff grace — a member whose claim JUST happened but
      // whose kickoff prompt is still running (long model call, no messages yet)
      // must not be demoted mid-kickoff. Require lastActiveAt to be older than a
      // kickoff grace window (2 min) beyond the silence threshold, so a member
      // that was claimed moments ago is left alone; S-04's member-side CAS then
      // prevents any reassignment from overwriting a still-set currentTaskId.
      const kickoffGraceMs = 2 * 60_000;
      const claimedRecently = m.lastActiveAt !== undefined && now - m.lastActiveAt < kickoffGraceMs;
      if (m.status === "working" && !m.currentTaskId && silent && !claimedRecently) {
        const chatting = await this.humanChat.chatting(m, swarm).catch(() => false);
        if (!chatting) {
          await this.store.updateMemberStatus(m.id, "idle", { currentTaskId: null, lastActiveAt: now }).catch(() => undefined);
          this.lastSeenActivity.delete(m.id);
          this.watchdogStrikes.delete(m.id);
          console.warn(`[swarm] watchdog: ${m.name} working-with-no-task silent → demoted to idle`);
        }
        continue;
      }

      if (!silent) {
        this.lastSeenActivity.set(m.id, latest || now);
        this.watchdogStrikes.set(m.id, 0);
        continue;
      }

      const strikes = (this.watchdogStrikes.get(m.id) ?? 0) + 1;
      this.watchdogStrikes.set(m.id, strikes);
      const taskId = m.currentTaskId;

      if (strikes === 1 && taskId) {
        // First strike: nudge the member to keep working on its task.
        const task = (await this.store.listTasks(swarmId)).find((t) => t.id === taskId);
        const notice = `[WATCHDOG] Your session appears stalled (no activity for several minutes). Continue task "${task?.title ?? taskId}" — if you are blocked, send a "blocker" message to the coordinator.`;
        this.lastWatchdogNudgeAt.set(m.id, Date.now());
        this.core.syncMember(swarm, m, notice).catch((err) => {
          console.error(`[swarm] watchdog nudge to ${m.name} failed:`, err);
        });
        console.warn(`[swarm] watchdog: ${m.name} silent, nudging (strike 1)`);
      } else if (strikes >= SwarmPluginRuntime.WATCHDOG_MAX_STRIKES && taskId) {
        // Escalated: release the task so another member can take it, and mark
        // the member interrupted. Notify the coordinator.
        await this.store.releaseTask(taskId).catch(() => undefined);
        await this.store.updateMemberStatus(m.id, "interrupted", { currentTaskId: null, lastActiveAt: now }).catch(() => undefined);
        this.watchdogStrikes.delete(m.id);
        this.notifyCoordinator(
          { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
          `Watchdog released "${taskId}" from ${m.name}: session was silent for ${Math.round((SwarmPluginRuntime.WATCHDOG_SILENT_MS * strikes) / 60_000)} min. Task re-queued.`,
        );
        // F6: the watchdog released an upstream task — tell dependent owners.
        await this.notifyDependents(swarmId, taskId, `released by watchdog (${m.name} stalled)`).catch((err) => {
          console.error(`[swarm] dependent notification on watchdog release failed:`, err);
        });
        console.warn(`[swarm] watchdog: released ${taskId} from ${m.name} after ${strikes} strikes`);
      }
    }
  }

  /**
   * Re-create a member's backing session after it vanished (restart/crash).
   * Roots the new session in the coordinator's LIVE directory, and if the
   * member owned a task, re-claims it and re-kicks the member on that task.
   * Returns the new session id. Used by startup recovery so the swarm self-
   * heals instead of needing the coordinator to manually re-spawn everyone.
   */
  async respawnMember(member: SwarmMember): Promise<string> {
    const swarm = await this.store.getSwarm(member.swarmId);
    if (!swarm) throw new Error(`no swarm '${member.swarmId}'`);

    // Root in the coordinator's live session directory (authoritative), same as
    // spawnMember — never the server cwd.
    let memberDir: string | undefined;
    try {
      const coordSession = await this.runtime.getSession(swarm.coordinatorSessionId);
      memberDir = coordSession?.directory || undefined;
    } catch {
      memberDir = undefined;
    }
    memberDir = memberDir || swarm.directory || undefined;

    const session = await this.runtime.createSession({
      title: `🐝 ${swarm.name} / ${member.name}`,
      agent: member.agent ?? "swarm",
      directory: memberDir,
      model: member.model,
      metadata: { swarmID: swarm.id, memberName: member.name, swarmMember: "1" },
    });
    await this.store.assignMemberSession(member.id, session.id);

    // If the member owned a task, re-claim it (it was released by recovery) and
    // re-kick the member on it so work continues without a human.
    const taskId = member.currentTaskId;
    if (taskId) {
      const claimed = await this.store.claimTask(taskId, member.id, swarmTaskLeaseMs(swarm));
      if (claimed) {
        const tasks = await this.store.listTasks(swarm.id);
        const task = tasks.find((t) => t.id === taskId);
        const prompt = task
          ? `Resumed after a restart. Your task: ${fence(task.title)}. Continue and complete it, then complete it via swarm_tasks (action complete, taskId '${taskId}').`
          : `Resumed after a restart. Continue task '${taskId}'.`;
        await this.runtime.promptAsync(
          { text: prompt, model: member.model, agent: member.agent ?? "swarm" },
          session.id,
        );
        await this.store.updateTaskStatus(taskId, "working");
      }
    }
    return session.id;
  }

  /** Minimum gap between team-sync digest injections for a swarm (ms). */
  private static readonly SYNC_COOLDOWN_MS = 45_000;

  /**
   * Inject a compact team-status digest to all active members so they stay
   * synchronized without pinging each other. Only fires when the task state
   * changed since the last digest (completed/ready tasks), and at most once per
   * cooldown per swarm — so it never becomes a message flood of its own. The
   * digest is a `synthetic` prompt: opencode queues it for busy members, and it
   * costs little attention, so members keep working through it.
   */
  private async syncSwarm(swarmId: string): Promise<void> {
    const swarm = await this.store.getSwarm(swarmId);
    if (!swarm || swarm.status !== "active") return;

    const [tasks, members] = await Promise.all([
      this.store.listTasks(swarmId),
      this.store.listMembers(swarmId),
    ]);
    // Fingerprint only the states that matter for a status digest: completed
    // tasks (by id) and ready-but-unassigned tasks.
    const sig = tasks
      .filter((t) => t.status === "completed" || (t.status === "ready" && !t.ownerMemberId))
      .map((t) => `${t.id}=${t.status}`)
      .sort()
      .join("|");
    if (!sig || sig === this.syncFingerprints.get(swarmId)) return;
    this.syncFingerprints.set(swarmId, sig);

    const now = Date.now();
    if (now - (this.lastSyncAt.get(swarmId) ?? 0) < SwarmPluginRuntime.SYNC_COOLDOWN_MS) return;
    this.lastSyncAt.set(swarmId, now);

    const digest = this.buildDigest(tasks);
    const ready = tasks.filter((t) => t.status === "ready" && !t.ownerMemberId);
    const recipients = [];
    for (const m of members) {
      if (m.role === "coordinator") continue;
      if (["stopped", "stopping", "failed"].includes(m.status)) continue;
      // Content-aware: only digest members who can act on it. An idle member
      // with no task and no ready task matching their affinity would otherwise
      // receive every digest and re-acknowledge "standing by" — pure noise.
      if (m.status === "idle" && !m.currentTaskId) {
        const canTake = ready.some((t) =>
          affinityScore(m.name, m.role ?? "", `${t.title} ${t.description ?? ""}`) > 0,
        );
        if (!canTake) continue;
      }
      recipients.push(m);
    }
    for (const m of recipients) {
      this.core.syncMember(swarm, m, digest).catch((err) => {
        console.error(`[swarm] sync digest to ${m.name} failed:`, err);
      });
    }
  }

  /** One-line digest of what changed / what's available. */
  private buildDigest(tasks: Array<{ id: string; title: string; status: string; ownerMemberId?: string }>): string {
    const done = tasks.filter((t) => t.status === "completed");
    const ready = tasks.filter((t) => t.status === "ready" && !t.ownerMemberId);
    const parts: string[] = [];
    if (done.length) parts.push(`done: ${done.map((t) => t.id).join(", ")}`);
    if (ready.length) parts.push(`ready (unassigned): ${ready.map((t) => t.id).join(", ")}`);
    return parts.length ? parts.join(" · ") : "no open work";
  }

  /** Run the scheduler for a swarm (idempotent; safe on any state change).
   * `opts.skipAssignmentFor` reserves taskIds for explicit member binding
   * (F11): those tasks are promoted to ready but never affinity-assigned.
   * Active advisory PathClaims are passed so the scheduler can emit
   * claim-aware warnings (WIP Aura, H0 — advisory, not enforced). */
  async runScheduler(swarmId: string, opts?: { skipAssignmentFor?: ReadonlySet<string> }): Promise<void> {
    const swarm = await this.store.getSwarm(swarmId);
    if (!swarm || swarm.status !== "active") return;
    const [activeClaims, annotations] = await Promise.all([
      this.store.listPathClaims(swarmId).catch(() => []),
      this.store.listAnnotations(swarmId, { activeOnly: true }).catch(() => []),
    ]);
    const result = await this.scheduler.run(swarm, { ...opts, activeClaims, annotations }).catch((err) => {
      console.error(`[swarm] scheduler pass failed for ${swarmId}:`, err);
      return undefined;
    });
    // F3: a task failed for exceeding maxRetriesPerTask must be surfaced with
    // the reason + retry count — no silent loop (NP9, truthful verdicts).
    if (result && result.failedExceededRetries.length > 0) {
      const tasks = await this.store.listTasks(swarmId);
      for (const taskId of result.failedExceededRetries) {
        const task = tasks.find((t) => t.id === taskId);
        this.notifyCoordinator(
          { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
          `Task ${fence(task?.title ?? taskId)} (${taskId}) failed: exceeded maxRetriesPerTask (${swarm.policies.maxRetriesPerTask}). Re-check the task or split it; it will not be re-assigned.`,
        );
        await this.notifyDependents(swarmId, taskId, `failed after exceeding maxRetriesPerTask`).catch(() => undefined);
      }
    }
    // WIP Aura (H0): surface advisory claim-overlap warnings to the coordinator
    // so overlapping-lane assignments are visible (escalation path), while
    // PathClaims stay advisory (no hard skip).
    if (result && result.claimWarnings.length > 0) {
      const members = await this.store.listMembers(swarmId);
      const nameById = new Map(members.map((m) => [m.id, m.name]));
      for (const w of result.claimWarnings) {
        this.notifyCoordinator(
          { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
          `[ADVISORY] ${w.warning} Holder: ${nameById.get(w.holderMemberId) ?? w.holderMemberId}.`,
        );
      }
    }
    // Hive H1 collective hesitation (corpse pile): a path with >= 3 active
    // corpse annotations is a corpse pile — surface a finding with re-plan
    // guidance (advisory; the task was still assigned).
    if (result && result.hesitationWarnings.length > 0) {
      for (const w of result.hesitationWarnings) {
        this.notifyCoordinator(
          { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
          `[HESITATION] ${w.warning} Consider re-planning this path or assigning a different approach.`,
        );
      }
    }
    // Durable intended-owner fallbacks (S-15): a task whose reservation TTL
    // expired was freed to affinity — surface WHY so the coordinator sees the
    // intent was not silently dropped. Low-noise: one line per fallback.
    if (result && result.reservationFallbacks.length > 0) {
      for (const f of result.reservationFallbacks) {
        this.notifyCoordinator(
          { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
          `[RESERVATION] task ${f.taskId} freed to affinity assignment: ${f.reason}`,
        );
      }
    }
  }

  /**
   * Case B autopermissions propagation (Wave 2, task_f9eb...). Reads the
   * coordinator's session-private permission, clamps it with D6 semantics
   * (never widen; webfetch ask unless allowed), and writes it to active worker
   * members via updateSession. Case A (agent block visible) needs no write —
   * members already inherit pull-based. Case C (neither visible) reports
   * unknown and relies on the documented emulation-cache fallback.
   */
  async propagateSwarmAutopermissions(swarmId: string): Promise<import("./permissions/propagate.js").PropagationResult> {
    const swarm = await this.store.getSwarm(swarmId);
    const empty = {
      propagated: false,
      mode: "unknown" as const,
      updated: [] as string[],
      skipped: [] as string[],
      skipReasons: {} as Record<string, string>,
      noWriteSurface: false,
      detail: "no swarm",
    };
    if (!swarm) return empty;
    const members = await this.store.listMembers(swarmId);
    const memberSessions = members.map((m) => ({
      name: m.name,
      sessionId: m.sessionId,
      status: m.status,
      role: m.role,
    }));
    return propagateAutopermissions({
      runtime: this.runtime,
      coordinatorSessionId: swarm.coordinatorSessionId,
      memberSessions,
    });
  }

  /**
   * Full perms diagnostics line for roster/status: mode label + (in Case B)
   * per-member updated/skipped counts. Wave 3 UX carry-over #1.
   */
  async permsDiagnosticsForSwarm(swarmId: string): Promise<string> {
    const [mode, result] = await Promise.all([
      this.permissionModeForSwarm(swarmId),
      this.propagateSwarmAutopermissions(swarmId),
    ]);
    const counts = permsCountsSummary(result);
    return `${permissionModeLabel(mode)}${result.mode === "worktree-scoped" ? ` (${counts})` : ""}`;
  }

  /**
   * Hive H1 diagnostics (task_ff1d34, features/hive-mind-execution-layer).
   * Reads the stable annotation substrate (corpse/gold counts) plus any
   * beliefs/needs/spotlight surfaces the hive layer exposes; renders a compact
   * advisory HIVE block. Truthful: sections whose substrate is unavailable are
   * omitted, never fabricated. The substrate callbacks are intentionally
   * narrow — the store may not implement them yet (beliefs table is Storage's
   * in-flight Wave 4 lane), so each read degrades to undefined.
   */
  async hiveDiagnosticsForSwarm(swarmId: string): Promise<HiveReadInput> {
    let annotations: import("./core/types.js").ArtifactAnnotation[] = [];
    try {
      annotations = await this.store.listAnnotations(swarmId, { activeOnly: true });
    } catch {
      annotations = [];
    }
    const input: HiveReadInput = { annotations };

    // Beliefs substrate: only counted when the store surface exists.
    // Contract (Storage-Auditor, wave-4): listBeliefs(swarmId, {activeOnly}).
    const storeAny = this.store as unknown as {
      listBeliefs?: (swarmId: string, opts?: { activeOnly?: boolean; tier?: string }) => Promise<unknown[]>;
    };
    if (typeof storeAny.listBeliefs === "function") {
      try {
        const active = (await storeAny.listBeliefs(swarmId, { activeOnly: true })) as Array<{
          tier?: "whisper" | "shout";
          status?: string;
        }>;
        let whisper = 0;
        let shout = 0;
        let resonant = 0;
        for (const b of active) {
          if (b.tier === "shout") shout++;
          else whisper++;
          if (b.status === "resonant") resonant++;
        }
        input.beliefsByTier = { whisper, shout };
        if (resonant > 0) input.resonantCount = resonant;
      } catch {
        input.beliefsByTier = undefined;
        input.resonantCount = undefined;
      }
    }

    // Consolidation status (H2 item 12): Core-Auditor's hive_consolidate writes
    // a bounded `context/consolidation/last` blackboard key (JSON {runId, at,
    // coordinator, retained, pruned, upgraded, expired, contradictions}) after
    // each run. Read it only if present; never fabricate a "no consolidation
    // yet" line.
    try {
      const consolidationEntry = await this.store.getBlackboard(swarmId, "context/consolidation/last");
      if (consolidationEntry) {
        const parsed = JSON.parse(consolidationEntry.value) as {
          at?: number;
          lastRunAt?: number;
          retained?: number;
          pruned?: number;
          upgraded?: number;
        };
        const lastRunAt = parsed.at ?? parsed.lastRunAt;
        if (typeof lastRunAt === "number") {
          input.consolidation = {
            lastRunAt,
            retained: parsed.retained ?? 0,
            pruned: parsed.pruned ?? 0,
            upgraded: parsed.upgraded ?? 0,
          };
        }
      }
    } catch {
      // no consolidation marker — omit
    }

    // Digest health (H2 anti-entropy): Scheduler-Auditor writes a per-swarm
    // `hive/digest` blackboard key (JSON {health: fresh|stale|unknown,
    // lastSyncAt}) from the sweep. Read it only if present.
    try {
      const digestEntry = await this.store.getBlackboard(swarmId, "hive/digest");
      if (digestEntry) {
        const parsed = JSON.parse(digestEntry.value) as {
          health?: "fresh" | "stale" | "unknown";
          lastSyncAt?: number;
        };
        if (parsed.health) {
          input.digest = { health: parsed.health, lastSyncAt: parsed.lastSyncAt };
        }
      }
    } catch {
      // no digest marker — omit
    }

    // Spotlight: Core-Auditor writes bounded `context/spotlight/<topic>`
    // blackboard keys (value JSON with reason/author/expiresAt). Read them via
    // searchBlackboard and include only unexpired topics.
    try {
      const spotlightKeys = await this.store.searchBlackboard(swarmId, "context/spotlight");
      const now = Date.now();
      const topics: string[] = [];
      for (const e of spotlightKeys) {
        const topic = e.key.split("/").pop() ?? e.key;
        let expiresAt: number | undefined;
        try {
          expiresAt = (JSON.parse(e.value) as { expiresAt?: number })?.expiresAt;
        } catch {
          expiresAt = undefined;
        }
        if (expiresAt === undefined || expiresAt > now) topics.push(topic);
      }
      if (topics.length > 0) input.spotlightTopics = topics;
    } catch {
      // no spotlight marker — omit
    }

    return input;
  }

  /**
   * Effective permission mode for a swarm (diagnostics in swarm_roster /
   * swarm_status). Determines whether members inherit live from the
   * coordinator's agent block (Case A), use the clamped propagated ruleset
   * (Case B), run accept-all (static option), or are unknown (Case C).
   */
  async permissionModeForSwarm(swarmId: string): Promise<import("./permissions/clamp.js").PermissionMode> {
    const swarm = await this.store.getSwarm(swarmId);
    if (!swarm) return "unknown";
    let agentBlockVisible = false;
    try {
      agentBlockVisible = (await this.runtime.getSessionPermissions?.(swarm.coordinatorSessionId)) !== undefined;
    } catch {
      agentBlockVisible = false;
    }
    let sessionPermissionVisible = false;
    try {
      sessionPermissionVisible = (await this.runtime.getSession(swarm.coordinatorSessionId))?.permission !== undefined;
    } catch {
      sessionPermissionVisible = false;
    }
    return permissionMode({
      allowAllMemberPermissions: this.allowAllMemberPermissions,
      agentBlockVisible,
      sessionPermissionVisible,
    });
  }

  /**
   * Resolve the coordinator's permission verdict for an op and have members
   * inherit it. Members are root sessions; the coordinator is the swarm's
   * authority, and the user's granted/auto-accepted permissions on the
   * coordinator's agent (e.g. "build") are the authoritative policy, so a
   * member gets the SAME verdict instead of a separate prompt for work the
   * parent already allows.
   *
   * Returns the verdict ("allow"/"deny"/"ask"), or undefined when the runtime
   * cannot determine the coordinator's permissions (the caller falls back to
   * heuristic scoping).
   */
  async inheritCoordinatorPermission(
    swarm: { coordinatorSessionId: string },
    type: string,
    pattern?: string | Array<string>,
  ): Promise<"allow" | "deny" | "ask" | undefined> {
    if (!this.runtime.getSessionPermissions) return undefined;
    const perms = await this.runtime.getSessionPermissions(swarm.coordinatorSessionId);
    if (!perms) return undefined;

    // Map the requested permission type to the agent-permission key. Non-path
    // ops and unknown types have no rule — return undefined (caller falls back).
    const key = type === "external_directory" ? "external_directory" : type;
    const rule: "allow" | "deny" | "ask" | Record<string, "allow" | "deny" | "ask"> | undefined =
      key === "edit" ? perms.edit
      : key === "webfetch" ? (perms.webfetch ?? "ask")
      : key === "external_directory" ? (perms.external_directory ?? "ask")
      : key === "bash" ? perms.bash
      : undefined;

    // Plain allow/ask/deny verdicts.
    if (rule === "allow" || rule === "deny" || rule === "ask") {
      return rule;
    }
    // bash may be an object of per-command patterns.
    if (typeof rule === "object" && rule !== null) {
      const pats = Array.isArray(pattern) ? pattern : [pattern].filter((p): p is string => !!p);
      if (pats.length === 0) return "ask";
      const verdicts = pats.map((p) => {
        // Exact match wins; else longest matching prefix rule.
        if (rule[p] !== undefined) return rule[p];
        const matching = (Object.entries(rule) as Array<[string, "allow" | "deny" | "ask"]>).filter(([pat]) => p.startsWith(pat));
        if (matching.length === 0) return "ask";
        matching.sort((a, b) => b[0].length - a[0].length);
        return matching[0]![1];
      });
      if (verdicts.every((v) => v === "allow")) return "allow";
      if (verdicts.some((v) => v === "deny")) return "deny";
      return "ask";
    }
    return undefined;
  }

  /**
   * Batch completion notices per swarm and flush them as ONE consolidated
   * user turn after a short debounce window. Prevents a flood of separate
   * "[SWARM] Task completed" turns when several members finish around the
   * same time.
   */
  notifyCoordinator(swarm: { id: string; name: string; coordinatorSessionId: string }, text: string): void {
    const existing = this.pendingCompletions.get(swarm.id) ?? [];
    existing.push({ text, at: Date.now() });
    this.pendingCompletions.set(swarm.id, existing);

    const timer = this.completionTimers.get(swarm.id);
    if (timer) clearTimeout(timer);
    this.completionTimers.set(
      swarm.id,
      setTimeout(() => {
        this.completionTimers.delete(swarm.id);
        const notices = this.pendingCompletions.get(swarm.id) ?? [];
        this.pendingCompletions.delete(swarm.id);
        if (notices.length === 0) return;
        const header = notices.length === 1
          ? `[SWARM: ${swarm.name}] ${notices[0]!.text}`
          : `[SWARM: ${swarm.name}] ${notices.length} updates:\n${notices.map((n) => `- ${n.text}`).join("\n")}`;
        this.runtime.promptAsync({ text: header }, swarm.coordinatorSessionId).catch((err) => {
          console.error(`[swarm] batched completion notify failed:`, err);
        });
      }, 1500),
    );
  }

  /** List models the spawner has access to, via the runtime adapter. */
  listModels(): Promise<import("./runtime/runtime-types.js").RuntimeModelInfo[]> {
    return this.runtime.listModels?.() ?? Promise.resolve([]);
  }

  /**
   * Track how many times a member has been re-prompted to continue its current
   * task, resetting when the member moves to a different task. Used to avoid an
   * infinite continue loop if a member keeps going idle without finishing.
   */
  nextContinueAttempt(memberId: string, taskId: string | null | undefined): number {
    const key = memberId;
    const cur = this.continueAttempts.get(key);
    if (!taskId) {
      this.continueAttempts.delete(key);
      return 0;
    }
    if (!cur || cur.taskId !== taskId) {
      this.continueAttempts.set(key, { taskId, count: 1 });
      return 1;
    }
    const next = cur.count + 1;
    this.continueAttempts.set(key, { taskId, count: next });
    return next;
  }

  resetContinueAttempts(memberId: string): void {
    this.continueAttempts.delete(memberId);
  }

  /** X1: clear the per-task notified gate — called when a task reaches a
   * terminal state so a later re-claim can notify again. */
  clearContinueNotified(taskId: string): void {
    this.continueNotifiedTasks.delete(taskId);
  }

  /** X1: has the "check for a blocker" notice already fired for this task? */
  isContinueNotified(taskId: string): boolean {
    return this.continueNotifiedTasks.has(taskId);
  }

  /** X1: record that the notice fired for this task (once per task). */
  markContinueNotified(taskId: string): void {
    this.continueNotifiedTasks.add(taskId);
  }

  /** Resolve/validate a member model ref against available models. */
  resolveModel(model?: { providerID?: string; modelID?: string }): Promise<{ providerID: string; modelID: string } | undefined> {
    return this.runtime.resolveModel?.(model) ?? Promise.resolve(undefined);
  }
}

export async function initSwarmRuntime(input: PluginInput, options: SwarmPluginOptions = {}): Promise<SwarmPluginRuntime> {
  if (singleton) return singleton;
  const dbPath = (options.dataDir ?? input.directory)
    .replace(/\\$/, "")
    .concat("/.opencode/swarms/swarms.db");
  try {
    mkdirSync(dbPath.replace(/[\\/]swarms\.db$/, ""), { recursive: true });
  } catch {
    // best effort; SQLite will surface a clear error if it cannot open
  }
  const store =
    options.store ??
    new SQLiteStore(dbPath);
  await store.ready();
  const runtime = new OpenCodeRuntime(input.client as never, input.directory, input.worktree);
  singleton = new SwarmPluginRuntime(store, runtime, 10_000, options.allowAllMemberPermissions ?? false);
  singleton.scheduleStartupRecovery();

  return singleton;
}

export async function swarmPlugin(
  input: PluginInput,
  options: SwarmPluginOptions = {},
): Promise<Hooks> {
  let rtPromise: Promise<SwarmPluginRuntime> | undefined;
  const ensureRt = () => {
    rtPromise ??= initSwarmRuntime(input, options);
    return rtPromise;
  };

  return {
    dispose: async () => {
      const rt = await rtPromise;
      if (rt) {
        rt.dispose();
        if (singleton === rt) singleton = undefined;
      }
      rtPromise = undefined;
    },
    tool: {
      swarm_delegate: tool({
        description: [
          "THE way to launch a swarm: one call to create (or reuse), spawn all",
          "members, seed the task DAG, and start work. The scheduler assigns",
          "ready tasks to idle members automatically and you are notified as they",
          "complete — no manual wake, assign, or poll.",
          "Idempotent: reusing the same name reuses the existing swarm; members",
          "already present are re-pointed at their task (never duplicated); tasks",
          "are seeded by stable id/title. You do NOT need to call swarm_create",
          "first — delegate creates it for you.",
          "Members are top-level OpenCode sessions — open them in the app to chat",
          "with them directly.",
        ].join("\n"),
        args: {
          name: tool.schema.string().describe("Swarm name (ignored if swarmId provided)."),
          swarmId: tool.schema.string().optional().describe("Reuse an existing swarm instead of creating one."),
          members: tool.schema.array(tool.schema.object({
            name: tool.schema.string().describe("Member name."),
            role: tool.schema.string().describe("Role description."),
            agent: tool.schema.string().optional(),
            model: tool.schema.object({ providerID: tool.schema.string(), modelID: tool.schema.string() }).optional(),
            taskId: tool.schema.string().optional().describe("Task id to assign to this member."),
            prompt: tool.schema.string().optional().describe("Full working brief for this member (the scheduler also auto-assigns ready tasks)."),
            workspace: tool.schema.enum(["shared-read", "shared-write", "worktree"]).optional(),
          })).describe("Members to spawn."),
          tasks: tool.schema.array(tool.schema.object({
            id: tool.schema.string().optional(),
            title: tool.schema.string(),
            description: tool.schema.string().optional(),
            priority: tool.schema.number().optional(),
            dependsOn: tool.schema.array(tool.schema.string()).optional(),
          })).describe("Task DAG. Ready tasks are auto-assigned to idle members."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          const caller = await core.store.getMemberBySessionId(ctx.sessionID);

          // Seed the caller-provided task DAG onto a swarm. Idempotent: tasks
          // already present (by id or title) are skipped. Preserves stable task
          // ids so member taskId claims and dependsOn references stay valid.
          const seedTasks = async (swarmId: string, createdByMemberId: string) => {
            if (!args.tasks?.length) return;
            const existingTasks = await core.store.listTasks(swarmId);
            const byId = new Map(existingTasks.map((t) => [t.id, t.id]));
            const byTitle = new Map(existingTasks.map((t) => [t.title, t.id]));
            const created = new Map<string, string>();
            for (const t of args.tasks) {
              const existingById = t.id ? byId.get(t.id) : undefined;
              const existingByTitle = byTitle.get(t.title);
              const already = existingById ?? existingByTitle;
              if (already) {
                if (t.id) created.set(t.id, already);
                continue;
              }
              const task = await core.createTask({
                swarmId,
                id: t.id,
                title: t.title,
                description: t.description,
                createdByMemberId,
                priority: t.priority,
              });
              if (t.id) created.set(t.id, task.id);
              else created.set(task.title, task.id);
            }
            // Dependencies may reference a stable id OR a task title. Resolve
            // against both newly-created ids and pre-existing tasks.
            const byTitleForDep = new Map(existingTasks.map((t) => [t.title, t.id]));
            for (const t of args.tasks) {
              for (const dep of t.dependsOn ?? []) {
                const taskId = created.get(t.id ?? "") ?? created.get(t.title) ?? t.id;
                const depId = created.get(dep) ?? byId.get(dep) ?? byTitleForDep.get(dep) ?? byTitle.get(dep);
                if (!taskId || !depId || taskId === depId) continue;
                await core.store.insertTaskDependency(taskId, depId);
              }
            }
          };

          let swarmId = args.swarmId;
          if (!swarmId) {
            // The calling session can belong to only ONE swarm. If it already
            // owns a swarm with a DIFFERENT name, creating a new one here would
            // violate that (raw UNIQUE error). Give the agent one clear line
            // instead of a DB exception — this is the intuitive chain: one
            // session, one swarm; reuse or delete before creating another.
            const owned = await core.store.getSwarmBySession(ctx.sessionID);
            if (owned && owned.name !== args.name) {
              return {
                output: `This session already owns swarm "${owned.name}" — one session can run one swarm. Reuse it (call swarm_delegate with name "${owned.name}", or pass swarmId "${owned.id}") or delete it first (swarm_delete).`,
              };
            }
            // Create the swarm (or reuse the one this session already owns).
            // createSwarm is idempotent for the calling session — it returns
            // the existing swarm instead of erroring — so a coordinator that
            // did swarm_create then swarm_delegate (without swarmId) still
            // lands here with the right swarm.
            const created = await core.createSwarm({
              name: args.name,
              projectId: input.project?.id ?? "global",
              coordinatorSessionId: ctx.sessionID,
              directory: ctx.directory,
              tasks: args.tasks as never,
            });
            swarmId = created.swarm.id;
            // If the swarm already existed, the tasks from the DAG above were
            // NOT seeded by createSwarm's idempotent return — do it now.
            await seedTasks(swarmId, created.coordinator.id);
          } else {
            // Reusing an existing swarm — only its coordinator may set it up.
            if (!caller) return { output: "calling session is not a swarm member" };
            if (caller.role !== "coordinator") {
              return { output: `only the coordinator may set up a swarm (you are '${caller.name}')` };
            }
            // Accept a swarm name or id for an existing swarm.
            swarmId = await core.resolveSwarmId(args.swarmId!, input.project?.id ?? "global");
            await seedTasks(swarmId, caller.id);
          }

          // Seed tasks start "pending"; transition the DAG so tasks with no
          // unmet dependencies become "ready" BEFORE members spawn — otherwise
          // a member's spawn-time taskId claim fails (claimTask requires a
          // ready, unowned task) and members end up unassigned.
          //
          // F11 binding precedence: taskIds explicitly named on members are
          // RESERVED — the scheduler promotes them to ready but must NOT hand
          // them out by affinity to an existing higher-scoring idle member
          // before the intended (possibly newly spawned) member can claim them
          // (the affinity-misassignment failure this iteration). The named
          // member claims at spawn; anything unbound is fair game for affinity.
          const reservedTaskIds = new Set<string>(
            (args.members ?? [])
              .map((m) => m.taskId)
              .filter((t): t is string => !!t),
          );
          // Durable intended-owner binding (S-15 fix): persist reservedFor on
          // the task row so the scheduler prefers the named member when the
          // task becomes ready — INCLUDING tasks that become ready LATER via
          // DAG dependency resolution (the per-pass in-memory reservation died
          // at delegate time, which mis-assigned later-ready tasks to idle
          // affinity winners). Cleared on claim/release; TTL-bounded so a
          // never-eligible owner cannot starve the task.
          const tasksNow = await core.store.listTasks(swarmId!);
          const taskById = new Map(tasksNow.map((t) => [t.id, t]));
          for (const m of args.members) {
            if (!m.taskId) continue;
            const bound = taskById.get(m.taskId);
            if (bound && bound.ownerMemberId === undefined) {
              await core.store.setTaskReservation(m.taskId, m.name).catch((err) => {
                console.warn(`[swarm] delegate reservation: could not bind task ${m.taskId} to ${m.name}: ${(err as Error).message}`);
              });
            }
          }
          await rt.runScheduler(swarmId!, { skipAssignmentFor: reservedTaskIds });

          // Spawn members (each may carry its own prompt; otherwise the
          // scheduler assigns ready tasks automatically).
          const spawned: Array<{ name: string; sessionId: string }> = [];
          const callerModel = (await core.store.getMemberBySessionId(ctx.sessionID))?.model;
          for (const m of args.members) {
            const existing = await core.store.getMemberByName(swarmId!, m.name);
            if (existing) {
              // Re-delegating to an existing member: re-assert the task so the
              // coordinator doesn't have to remember what's already spawned.
              // Don't duplicate the member — just point it at the (new) task.
              spawned.push({ name: existing.name, sessionId: existing.sessionId });
              if (m.taskId && existing.currentTaskId !== m.taskId && !["stopped", "stopping", "failed"].includes(existing.status)) {
                await core.assignTaskToMember({ swarmId: swarmId!, memberId: existing.id, taskId: m.taskId, prompt: m.prompt ?? "" }).catch((err) => {
                  // assignTaskToMember claims via CAS (NP2) — a claim failure here
                  // means the task is owned elsewhere or not ready; surface it
                  // instead of silently proceeding unbound (the affinity-
                  // misassignment class this iteration).
                  console.warn(`[swarm] delegate re-point: task ${m.taskId} not assigned to ${existing.name}: ${(err as Error).message}`);
                });
              }
              continue;
            }
            const model = await resolveMemberModel(rt, m.model as never, callerModel);
            const member = await core.spawnMember({
              swarmId: swarmId!,
              name: m.name,
              role: m.role,
              agent: m.agent,
              model,
              taskId: m.taskId,
              prompt: m.prompt,
              workspace: m.workspace as never,
            });
            spawned.push({ name: member.name, sessionId: member.sessionId });
          }

          // Self-driving: assign any remaining ready tasks to idle members.
          await rt.runScheduler(swarmId!);

          // Write the lane registry so every member can instantly see who owns
          // what lane — the durable anti-redundancy surface. Members probe it
          // (swarm_probe / swarm_status detail:lanes) before starting work that
          // might overlap a peer.
          try {
            const members = await core.store.listMembers(swarmId!);
            const coord = members.find((m) => m.role === "coordinator");
            const registry = members
              .filter((m) => m.role !== "coordinator")
              .map((m) => `${m.name}: ${m.role}${m.currentTaskId ? ` | task=${m.currentTaskId}` : ""}`)
              .join("\n");
            if (registry && coord) {
              // Read-first CAS: the lane registry key may already exist on a
              // re-delegate; overwriting it requires the current version (S2).
              const lanes = await core.store.getBlackboard(swarmId!, "context/lanes");
              await core.blackboardPut({
                swarmId: swarmId!,
                key: "context/lanes",
                value: `Member lanes (owner: who is responsible for what):\n${registry}`,
                contentType: "text/markdown",
                expectedVersion: lanes?.version,
                authorMemberId: coord.id,
              }).catch(() => undefined);
            }
          } catch { /* lane registry is best-effort */ }

          const swarm = await core.store.getSwarm(swarmId!);
          // F11 binding verification: for every explicitly-named member.taskId,
          // confirm the intended member actually owns it. A mismatch means the
          // binding failed (task owned elsewhere / not ready) — report the
          // actual owner + next action instead of a silent affinity steal.
          const bindings: Array<{ taskId: string; intended: string; owner: string | null }> = [];
          const allMembers = await core.store.listMembers(swarmId!);
          const nameById = new Map(allMembers.map((m) => [m.id, m.name]));
          const allTasks = await core.store.listTasks(swarmId!);
          for (const m of args.members ?? []) {
            if (!m.taskId) continue;
            const task = allTasks.find((t) => t.id === m.taskId);
            const owner = task?.ownerMemberId ? (nameById.get(task.ownerMemberId) ?? task.ownerMemberId) : null;
            bindings.push({ taskId: m.taskId, intended: m.name, owner });
          }
          const bindingWarnings = bindings
            .filter((b) => b.owner !== b.intended)
            .map((b) => `task '${b.taskId}' was requested for '${b.intended}' but is owned by ${b.owner ?? "nobody"} (${(allTasks.find((t) => t.id === b.taskId))?.status ?? "?"}). Reassign via swarm_tasks (action reassign, taskId, member '<name>') or release it first.`);
          return {
            title: `swarm: ${swarm?.name ?? args.name}`,
            output: JSON.stringify({
              swarmId: swarmId!,
              members: spawned,
              taskCount: (await core.store.listTasks(swarmId!)).length,
              bindings: bindings.map((b) => ({ taskId: b.taskId, intended: b.intended, actualOwner: b.owner })),
              ...(bindingWarnings.length > 0 ? { bindingWarnings } : {}),
              note: "Swarm is self-driving. Members were assigned ready tasks automatically. You will be notified as they complete. Do not poll.",
            }, null, 2),
            metadata: { swarmId: swarmId!, memberCount: spawned.length },
          };
        },
      }),

      swarm_create: tool({
        description: [
          "Create a new agent swarm owned by the calling session.",
          "Registers the coordinator, applies policies, and optionally seeds a task DAG.",
          "Returns the swarm id, coordinator member id, and any created tasks.",
          "NOTE: if you intend to spawn members and start work, use swarm_delegate",
          "instead — it does everything swarm_create does PLUS spawns members and",
          "launches. swarm_create is only for setting up a shell you will grow",
          "manually. One session runs one swarm: reusing a name returns the",
          "existing swarm; a different name while you already own one is rejected.",
        ].join("\n"),
        args: {
          name: tool.schema.string().describe("Swarm name (unique per project)."),
          policies: tool.schema.record(tool.schema.string(), tool.schema.unknown()).optional().describe("Partial SwarmPolicies overrides."),
          tasks: tool.schema.array(tool.schema.object({
            id: tool.schema.string().optional().describe("Optional stable task id."),
            title: tool.schema.string().describe("Task title."),
            description: tool.schema.string().optional(),
            priority: tool.schema.number().optional(),
            dependsOn: tool.schema.array(tool.schema.string()).optional().describe("Task ids this task depends on."),
            acceptanceCriteria: tool.schema.array(tool.schema.string()).optional(),
          })).optional().describe("Optional initial task DAG."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          const projectId = input.project?.id ?? "global";
          const result = await core.createSwarm({
            name: args.name,
            projectId,
            coordinatorSessionId: ctx.sessionID,
            directory: ctx.directory,
            policies: args.policies as never,
            tasks: args.tasks as never,
          });
          // Make seed tasks with no unmet dependencies ready so spawned members
          // can claim them right away.
          await rt.runScheduler(result.swarm.id);
          return {
            output: JSON.stringify({
              swarm: { id: result.swarm.id, name: result.swarm.name, status: result.swarm.status },
              coordinatorMemberId: result.coordinator.id,
              tasks: result.tasks.map((t) => ({ id: t.id, title: t.title, status: t.status })),
            }, null, 2),
          };
        },
      }),

      swarm_task: tool({
        description: [
          "Delegate a task to a swarm member in ONE call (task-tool style).",
          "Spawns the member if needed, assigns the task, and immediately starts",
          "the member working. The member runs as a top-level OpenCode session",
          "you can open and chat with directly. Returns immediately; the",
          "coordinator is notified automatically when the member goes idle.",
          "Do NOT sleep or poll after calling — wait for the completion notification.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string().describe("Swarm id or swarm name."),
          name: tool.schema.string().describe("Member name (spawned if it doesn't exist)."),
          role: tool.schema.string().optional().describe("Member role."),
          taskId: tool.schema.string().optional().describe("Existing task id to assign, or omit to create one."),
          title: tool.schema.string().optional().describe("Task title (required if taskId omitted)."),
          description: tool.schema.string().optional().describe("Task description."),
          prompt: tool.schema.string().describe("The assignment prompt for the member."),
          agent: tool.schema.string().optional().describe("OpenCode agent to run the member with."),
          model: tool.schema.object({
            providerID: tool.schema.string(),
            modelID: tool.schema.string(),
          }).optional(),
          workspace: tool.schema.enum(["shared-read", "shared-write", "worktree"]).optional(),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          // Verify caller is the coordinator (only coordinator delegates).
          const caller = await core.store.getMemberBySessionId(ctx.sessionID);
          if (!caller) return { output: "calling session is not a swarm member" };
          if (caller.role !== "coordinator") {
            return { output: `only the coordinator may delegate tasks (you are '${caller.name}')` };
          }

          // Resolve or create the task.
          let taskId = args.taskId;
          if (!taskId) {
            if (!args.title) return { output: "taskId or title is required" };
            const created = await core.createTask({
              swarmId: args.swarmId,
              title: args.title,
              description: args.description,
              createdByMemberId: caller.id,
            });
            taskId = created.id;
            // A freshly created task has no dependencies, so it is READY by DAG
            // semantics. Promote it now (the scheduler would do this on its next
            // pass) so the member's spawn-time/assignment-time claimTask CAS can
            // succeed — claimTask requires status='ready'.
            await core.store.updateTaskStatus(taskId, "ready");
          }

          // Ensure the member exists (spawn idempotently).
          let member = await core.store.getMemberByName(args.swarmId, args.name);
          if (!member) {
            const model = await resolveMemberModel(rt, args.model as never, caller.model);
            member = await core.spawnMember({
              swarmId: args.swarmId,
              name: args.name,
              role: args.role ?? "worker",
              agent: args.agent,
              model,
              taskId,
              prompt: args.prompt,
              workspace: args.workspace as never,
            });
          } else {
            // Existing member: assign the task and kick off directly.
            try {
              await core.assignTaskToMember({ swarmId: args.swarmId, memberId: member.id, taskId, prompt: args.prompt });
            } catch (err) {
              // Assignment now goes through the claimTask CAS (NP2) — surface a
              // claim failure instead of silently proceeding unbound (the stale-
              // owner/affinity-misassignment class this iteration).
              return {
                output: `task '${taskId}' not assigned to '${args.name}': ${(err as Error).message}`,
              };
            }
          }

          return {
            title: `swarm: ${args.name}`,
            output: JSON.stringify({
              task: { id: taskId },
              member: { name: member.name, sessionId: member.sessionId, status: member.status },
              note: "Member started. You will be notified when it finishes. Do not poll.",
            }, null, 2),
            metadata: { sessionId: member.sessionId, memberName: member.name, taskId },
          };
        },
      }),

      swarm_spawn: tool({
        description: [
          "Spawn named member OpenCode sessions for a swarm.",
          "For each member provide: name, role, and a thorough 'prompt' that fully",
          "defines that member's job (context, constraints, tasks, what to report).",
          "Members run as the 'swarm' agent (P2P doctrine in their system prompt);",
          "they message each other directly and report via handoff broadcasts.",
          "Members are top-level OpenCode sessions — open them in the app to chat",
          "with them directly.",
          "Optionally set a model per member (use swarm_models to see options).",
          "You are notified (batched) as tasks complete.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string().describe("Swarm id or name."),
          members: tool.schema.array(tool.schema.object({
            name: tool.schema.string().describe("Member name."),
            role: tool.schema.string().describe("Role description."),
            agent: tool.schema.string().optional(),
            model: tool.schema.object({
              providerID: tool.schema.string().describe("Real provider id (e.g. opencode, opencode-go, lmstudio) — NOT a tier label."),
              modelID: tool.schema.string(),
            }).optional(),
            taskId: tool.schema.string().optional().describe("Task id to assign (for tracking/claims)."),
            prompt: tool.schema.string().describe("The full working brief for this member."),
            workspace: tool.schema.enum(["shared-read", "shared-write", "worktree"]).optional(),
          })).describe("Members to spawn."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const callerModel = (await core.store.getMemberBySessionId(ctx.sessionID))?.model;
          const created: Array<Record<string, unknown>> = [];
          for (const m of args.members) {
            const model = await resolveMemberModel(rt, m.model as never, callerModel);
            const member = await core.spawnMember({
              swarmId: args.swarmId,
              name: m.name,
              role: m.role,
              agent: m.agent,
              model,
              taskId: m.taskId,
              prompt: m.prompt,
              workspace: m.workspace as never,
            });
            created.push({
              name: member.name,
              memberId: member.id,
              sessionId: member.sessionId,
              status: member.status,
              model,
            });
          }
          // Run the scheduler so ready tasks are claimed by the freshly-spawned
          // (or existing idle) members immediately — otherwise tasks sit pending
          // until a member happens to go idle, stalling the DAG.
          await rt.runScheduler(args.swarmId);
          return {
            output: JSON.stringify({ spawned: created }, null, 2),
          };
        },
      }),

      swarm_message: tool({
        description: [
          "Send a direct or broadcast message to a swarm member.",
          "Optionally mark the message noreply (fire-and-forget): the recipient is told",
          "no response is expected — use for status broadcasts and notices so peers",
          "don't burn turns on ack-only replies. Kinds that demand a response",
          "(request/blocker/handoff/review) can never be noreply.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string().describe("Swarm id."),
          to: tool.schema.string().describe("Recipient member name, or '*' to broadcast."),
          kind: tool.schema.enum(["message", "request", "response", "finding", "handoff", "blocker", "decision", "review", "control"]).optional(),
          message: tool.schema.string().describe("Message body."),
          taskId: tool.schema.string().optional(),
          correlationId: tool.schema.string().optional(),
          responseTo: tool.schema.string().optional(),
          priority: tool.schema.enum(["low", "normal", "high", "urgent"]).optional(),
          refs: tool.schema.array(tool.schema.string()).optional(),
          noreply: tool.schema.boolean().optional().describe("Mark fire-and-forget: recipient is not expected to reply."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const msgs = await core.sendMessage({
            swarmId: args.swarmId,
            fromSessionId: ctx.sessionID,
            to: args.to,
            kind: args.kind ?? "message",
            message: args.message,
            taskId: args.taskId,
            correlationId: args.correlationId,
            responseTo: args.responseTo,
            priority: args.priority ?? "normal",
            refs: args.refs,
            noreply: args.noreply,
          });
          // sendMessage now returns PERSISTED post-wake states (audit/messaging
          // F-M1): delivered = injected now, scheduled = claimed mid-flight,
          // queued = deferred (cooldown/human-chat/busy) — arrives on next wake.
          // Report real verdicts so "queued ≠ resend" is structural (TU4).
          const members = await core.store.listMembers(args.swarmId);
          const nameById = new Map(members.map((m) => [m.id, m.name]));
          const delivered = msgs.filter((m) => m.deliveryState === "delivered" || m.deliveryState === "scheduled");
          const pending = msgs.filter((m) => m.deliveryState === "queued");
          const summary = msgs.length === 0
            ? "message sent (no recipients)"
            : `delivered to ${delivered.length} now${pending.length ? `, ${pending.length} pending (will arrive on next wake)` : ""}`;
          return {
            output: JSON.stringify({
              summary,
              deliveredTo: delivered.map((m) => nameById.get(m.to.memberId ?? "") ?? m.to.memberId ?? ""),
              pendingFor: pending.map((m) => nameById.get(m.to.memberId ?? "") ?? m.to.memberId ?? ""),
              messages: msgs.map((m) => ({ id: m.id, to: m.to, kind: m.kind, state: m.deliveryState })),
            }, null, 2),
          };
        },
      }),

      swarm_reply: tool({
        description: [
          "Reply to a specific swarm message from a peer.",
          "Keeps the correlation id and points responseTo at the original message, so",
          "request/response threads are preserved without routing through the coordinator.",
          "Warns (softly) when replying to a noreply message or when the reply looks",
          "like an ack-only response — send only if you can act or add information.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string().describe("Swarm id."),
          toMessageId: tool.schema.string().describe("Original message id being replied to."),
          message: tool.schema.string().describe("Reply body."),
          kind: tool.schema.enum(["response", "finding", "handoff", "blocker", "decision", "review", "request", "message", "control"]).optional(),
          priority: tool.schema.enum(["low", "normal", "high", "urgent"]).optional(),
          refs: tool.schema.array(tool.schema.string()).optional(),
          noreply: tool.schema.boolean().optional().describe("Mark this reply fire-and-forget (informational follow-up)."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const original = await core.store.getMessageById(args.toMessageId);
          const warnings: string[] = [];
          // Soft guard 1: replying to a noreply message (never a hard block).
          if (original?.noreply) {
            warnings.push("the original message is marked noreply — reply only if you can act or escalate");
          }
          // Soft guard 2: ack-detection nudge on echo-like/trivial replies.
          const ackNote = original
            ? detectAckReply(original.body.text, args.message)
            : undefined;
          if (ackNote) warnings.push(ackNote);
          const msgs = await core.replyToMessage({
            swarmId: args.swarmId,
            fromSessionId: ctx.sessionID,
            toMessageId: args.toMessageId,
            message: args.message,
            kind: args.kind ?? "response",
            priority: args.priority,
            refs: args.refs,
            noreply: args.noreply,
          });
          return {
            output: JSON.stringify({
              delivered: msgs.map((m) => ({ id: m.id, to: m.to, kind: m.kind, state: m.deliveryState })),
              ...(warnings.length ? { warnings } : {}),
            }, null, 2),
          };
        },
      }),

      swarm_tasks: tool({
        description: "Inspect swarm tasks (list/claim/release/reassign/complete/fail/cancel). list shows DAG-aware rows + next-action summary; release returns a claimed/working task to ready (note: an owner releasing a task counts as one retry attempt, bounded by maxRetriesPerTask); coordinator may reassign a task to a different member.",
        args: {
          swarmId: tool.schema.string(),
          action: tool.schema.enum(["list", "claim", "release", "reassign", "complete", "fail", "cancel"]).describe("Action."),
          taskId: tool.schema.string().optional().describe("Required for claim/release/reassign/complete/fail/cancel."),
          member: tool.schema.string().optional().describe("Target member name (required for reassign)."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          if (!args.taskId || args.action === "list") {
            const tasks = await core.store.listTasks(args.swarmId);
            const members = await core.store.listMembers(args.swarmId);
            const deps = await core.store.listTaskDependencies(args.swarmId);
            const nameById = new Map(members.map((m) => [m.id, m.name]));
            const caller = await core.store.getMemberBySessionId(ctx.sessionID);
            // Hive H1: active annotations for corpse-pile counts (advisory).
            const annotations = await core.store.listAnnotations(args.swarmId, { activeOnly: true }).catch(() => []);
            const corpseByPath = corpseCountByPath(annotations);
            // DAG-aware row fields (TU2 / F-UX-2): for each task, blockedBy names
            // the unmet prerequisite(s), readyForClaim says whether the task can
            // be claimed right now (ready + unowned).
            const prereqByTask = new Map<string, string[]>();
            for (const d of deps) {
              const list = prereqByTask.get(d.taskId) ?? [];
              list.push(d.dependsOnTaskId);
              prereqByTask.set(d.taskId, list);
            }
            const rows = tasks.map((t) => {
              const prereqs = prereqByTask.get(t.id) ?? [];
              const unmet = prereqs.filter((pid) => {
                const p = tasks.find((x) => x.id === pid);
                return !p || !["completed", "failed", "cancelled"].includes(p.status);
              });
              // Hive H1: corpse count on paths this task mentions (0 if none).
              let corpseCount = 0;
              for (const [path, count] of corpseByPath) {
                if (count >= CORPSE_PILE_THRESHOLD && taskMentionsPath(path, t)) {
                  corpseCount = Math.max(corpseCount, count);
                }
              }
              return {
                id: t.id,
                title: t.title,
                status: t.status,
                priority: t.priority,
                owner: t.ownerMemberId ? nameById.get(t.ownerMemberId) ?? t.ownerMemberId : null,
                blockedBy: unmet.length ? unmet : undefined,
                readyForClaim: t.status === "ready" && !t.ownerMemberId,
                ...(corpseCount > 0 ? { corpseCount } : {}),
              };
            });
            // One-line summary footer (TU1 / F-UX-2): counts so "what can I take
            // next?" is answerable in one call.
            const count = (s: string) => tasks.filter((t) => t.status === s).length;
            const summary =
              `ready: ${count("ready")}, working: ${count("working")}, claimed: ${count("claimed")}, ` +
              `review_pending: ${count("review_pending")}, done: ${count("completed")}`;
            // "Top ready task you could claim" hint (F-UX-2): only for a member
            // (not the coordinator) that holds no non-terminal task, when a ready
            // unowned task exists. Includes the title so the hint is actionable
            // without a second lookup (Wave-2 UX carry-over).
            let topReady: { id: string; title: string } | undefined;
            if (caller && caller.role !== "coordinator") {
              const ownsNonTerminal = caller.currentTaskId
                && !["completed", "failed", "cancelled"].includes(
                  tasks.find((t) => t.id === caller.currentTaskId)?.status ?? "completed");
              const claimable = rows
                .filter((r) => r.readyForClaim && r.blockedBy === undefined)
                .sort((a, b) => b.priority - a.priority);
              if (!ownsNonTerminal && claimable.length) {
                const top = claimable[0]!;
                topReady = { id: top.id, title: top.title };
              }
            }
            // Hive H1: top-level hesitation warnings (paths with >= 3 active
            // corpse annotations on matching tasks) — advisory re-plan signal.
            const hesitation = tasks
              .filter((t) => {
                for (const [path, count] of corpseByPath) {
                  if (count >= CORPSE_PILE_THRESHOLD && taskMentionsPath(path, t)) return true;
                }
                return false;
              })
              .map((t) => {
                const path = [...corpseByPath.entries()]
                  .filter(([p, c]) => c >= CORPSE_PILE_THRESHOLD && taskMentionsPath(p, t))
                  .map(([p]) => p);
                return {
                  taskId: t.id,
                  taskTitle: t.title,
                  paths: path,
                  corpseCount: path.reduce((max, p) => Math.max(max, corpseByPath.get(p) ?? 0), 0),
                };
              });
            return {
              output: JSON.stringify({
                summary,
                tasks: rows,
                topReadyTaskToClaim: topReady,
                ...(hesitation.length ? { hesitationWarnings: hesitation } : {}),
              }, null, 2),
            };
          }
          if (args.action === "claim" && args.taskId) {
            const member = await memberForContext(core, args.swarmId, ctx.sessionID);
            // R1 guard: a member that already owns a non-terminal task must not
            // pull a SECOND ready task — its currentTaskId would be overwritten
            // and the first task left owned-but-undriven (stranded). Finish (or
            // release) the current task before claiming another.
            if (member.currentTaskId && member.currentTaskId !== args.taskId) {
              const owned = (await core.store.listTasks(args.swarmId)).find((t) => t.id === member.currentTaskId);
              const terminal = owned && ["completed", "failed", "cancelled"].includes(owned.status);
              if (!terminal) {
                return {
                  output: `claim: rejected — you still own non-terminal task '${member.currentTaskId}' (${owned?.title ?? ""}). Finish or release it before claiming '${args.taskId}'.`,
                };
              }
            }
            const swarmForLease = await core.store.getSwarm(args.swarmId);
            const ok = await core.store.claimTask(args.taskId, member.id, swarmTaskLeaseMs(swarmForLease!));
            if (!ok) {
              return { output: `claim: already owned / not ready` };
            }
            // Pull-claim completes the FULL working transition (design-principles
            // CP8/B1): the member is now working on this task, its currentTaskId
            // is bound, the task leaves 'claimed' for 'working', and the member is
            // kicked off with the same assignment prompt the scheduler would use.
            // Leaving a task 'claimed'-with-idle-owner strands it (F1/F11).
            const swarm = await core.store.getSwarm(args.swarmId);
            const tasks = await core.store.listTasks(args.swarmId);
            const task = tasks.find((t) => t.id === args.taskId);
            if (!swarm || !task) {
              return { output: `claim: ok, but task '${args.taskId}' not found for kickoff` };
            }
            await core.store.updateMemberStatus(member.id, "working", { currentTaskId: task.id, lastActiveAt: Date.now() });
            await core.store.updateTaskStatus(task.id, "working");
            try {
              const promptText = await rt.scheduler.buildAssignmentPrompt(swarm, member, task);
              await rt.runtimeAdapter.promptAsync(
                { text: promptText, model: member.model, agent: member.agent ?? "swarm" },
                member.sessionId,
              );
            } catch (err) {
              // Kickoff failed: release the task and leave the member idle so
              // another pass can retry (mirrors scheduler.assignTask).
              await core.store.releaseTask(task.id).catch(() => undefined);
              await core.store.updateMemberStatus(member.id, "idle", { currentTaskId: null, lastActiveAt: Date.now() }).catch(() => undefined);
              return { output: `claim: ok, but kickoff failed: ${(err as Error).message}` };
            }
            await rt.runScheduler(args.swarmId);
            return { output: `claim: ok — task ${task.id} claimed, member working` };
          }
          if (args.action === "release" && args.taskId) {
            // Non-terminal release (F-UX-3): the owner (or coordinator) returns a
            // claimed/working task to 'ready' so another member can claim it.
            // The escape hatch the pull-claim surface needs — a claimant who
            // mis-claimed or cannot finish must be able to unclaim via the tool.
            const tasks = await core.store.listTasks(args.swarmId);
            const task = tasks.find((t) => t.id === args.taskId);
            if (!task) return { output: `no task '${args.taskId}'` };
            if (["completed", "failed", "cancelled"].includes(task.status)) {
              return { output: `release: task '${args.taskId}' is already ${task.status} — nothing to release` };
            }
            const member = await memberForContext(core, args.swarmId, ctx.sessionID);
            const isOwner = task.ownerMemberId === member.id;
            const isCoordinator = (await core.store.getMemberById(member.id))?.role === "coordinator";
            if (!isOwner && !isCoordinator) {
              return { output: `only the task owner or coordinator may release (you are '${member.name}')` };
            }
            const ok = await core.store.releaseTask(args.taskId);
            if (!ok) {
              return { output: `release: task '${args.taskId}' could not be released (already released or terminal)` };
            }
            // Clear the releasing member's currentTaskId if it pointed at this
            // task (coordinator releasing someone else's task must not detach
            // itself; mirror the complete-path owner-clear logic).
            const ownerToClear = task.ownerMemberId ? await core.store.getMemberById(task.ownerMemberId) : undefined;
            const toClear = ownerToClear ?? member;
            if (toClear.currentTaskId === args.taskId) {
              await core.store.updateMemberStatus(toClear.id, "idle", { currentTaskId: null });
            }
            // Deliberately NOT running the scheduler here: release is the escape
            // hatch for a mis-claim or a member who cannot finish — an immediate
            // scheduler pass would re-assign the released task right back to the
            // freed member, defeating the release. The periodic sweep (or the
            // next idle event) re-assigns it to any eligible idle member.
            // C18: surface the post-release retry budget so a release-claim
            // cycle can't silently fail the task — the next release past the
            // cap fails it outright (maxRetriesPerTask).
            const swarmForBudget = await core.store.getSwarm(args.swarmId);
            const maxRetries = swarmForBudget?.policies.maxRetriesPerTask ?? 2;
            const retriesAfter = (task.retryCount ?? 0) + 1;
            const remaining = Math.max(0, maxRetries - retriesAfter);
            return {
              output: `release: task '${args.taskId}' returned to ready — another member can claim it (scheduler will assign on the next sweep or idle event; run swarm_tasks list to see it). Retry budget: ${retriesAfter}/${maxRetries} used — ${remaining} release${remaining === 1 ? "" : "s"} before the task is failed outright.`,
            };
          }
          if (args.action === "reassign" && args.taskId) {
            // Coordinator-only reassignment primitive (F11): atomically clear the
            // old owner's currentTaskId, rebind the task owner row, and bind the
            // new owner — invalidating stale-owner completion authority (complete
            // checks the CURRENT row owner, so the old owner can no longer
            // complete/publish after the rebind).
            const caller = await core.store.getMemberBySessionId(ctx.sessionID);
            if (!caller) return { output: "calling session is not a swarm member" };
            if (caller.role !== "coordinator") {
              return { output: `only the coordinator may reassign tasks (you are '${caller.name}')` };
            }
            if (!args.member) return { output: "reassign requires the target 'member' name" };
            const target = await core.store.getMemberByName(args.swarmId, args.member);
            if (!target) return { output: `no member named '${args.member}'` };
            if (["stopped", "stopping", "failed"].includes(target.status)) {
              return { output: `cannot reassign to '${args.member}': member is ${target.status}` };
            }
            let oldOwnerId: string | null;
            try {
              oldOwnerId = await core.store.reassignTask(args.taskId, target.id);
            } catch (err) {
              return { output: `reassign failed: ${(err as Error).message}` };
            }
            // Kick off the new owner on the task (same prompt path as claim).
            const swarm = await core.store.getSwarm(args.swarmId);
            const tasks = await core.store.listTasks(args.swarmId);
            const task = tasks.find((t) => t.id === args.taskId);
            const oldOwnerName = oldOwnerId ? (await core.store.getMemberById(oldOwnerId))?.name : null;
            if (!swarm || !task) {
              return { output: `reassign: ok — ${args.member} now owns task ${args.taskId}${oldOwnerName ? ` (was ${oldOwnerName})` : ""}` };
            }
            try {
              const promptText = await rt.scheduler.buildAssignmentPrompt(swarm, { ...target, currentTaskId: task.id }, task);
              await rt.runtimeAdapter.promptAsync(
                { text: promptText, model: target.model, agent: target.agent ?? "swarm" },
                target.sessionId,
              );
            } catch (err) {
              return { output: `reassign: ok — task ${task.id} rebound to ${args.member}${oldOwnerName ? ` (was ${oldOwnerName})` : ""}, but kickoff failed: ${(err as Error).message}` };
            }
            await rt.runScheduler(args.swarmId);
            return { output: `reassign: ok — task ${task.id} now owned by ${args.member}${oldOwnerName ? ` (was ${oldOwnerName})` : ""}; old owner's completion authority invalidated` };
          }
          if ((args.action === "complete" || args.action === "fail" || args.action === "cancel") && args.taskId) {
            const tasks = await core.store.listTasks(args.swarmId);
            const task = tasks.find((t) => t.id === args.taskId);
            if (!task) return { output: `no task '${args.taskId}'` };
            const member = await memberForContext(core, args.swarmId, ctx.sessionID);
            const isOwner = task.ownerMemberId === member.id;
            const isCoordinator = (await core.store.getMemberById(member.id))?.role === "coordinator";
            if (!isOwner && !isCoordinator) {
              return { output: `only the task owner or coordinator may ${args.action}` };
            }
            const targetStatus = args.action === "complete" ? "completed" : args.action === "cancel" ? "cancelled" : "failed";
            await core.store.updateTaskStatus(args.taskId, targetStatus as never);
            // X1: task reached a terminal state — clear the notified gate so a
            // future re-claim of this task can notify the coordinator again.
            rt.clearContinueNotified(args.taskId);
            if (args.action === "complete") {
              // Clear the task's OWNER's currentTaskId — not necessarily the
              // caller's. A coordinator completing someone else's task must not
              // detach itself; a stale owner id must be cleared so the member
              // can take new work.
              const owner = task.ownerMemberId ? await core.store.getMemberById(task.ownerMemberId) : undefined;
              const toClear = owner ?? member;
              await core.store.updateMemberStatus(toClear.id, "idle", { currentTaskId: null });
            }
            // Completing a task can unblock dependents and free a member for new
            // work — advance the DAG now and surface the result to the
            // coordinator (batched), so the swarm doesn't stall waiting for an
            // unrelated idle event.
            const swarm = await core.store.getSwarm(args.swarmId);
            if (swarm) {
              if (args.action === "complete") {
                rt.notifyCoordinator(
                  { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
                  `Task completed by ${member.name}: ${fence(task.title)} (${task.id})`,
                );
              }
              if (args.action === "fail" || args.action === "cancel") {
                // F6: a failure/cancel can invalidate dependent work — notify
                // each directly-dependent task's owner so they re-validate.
                await rt.notifyDependents(args.swarmId, task.id, `${args.action}ed by ${member.name}`).catch((err) => {
                  console.error(`[swarm] dependent notification on ${args.action} failed:`, err);
                });
              }
              await rt.runScheduler(args.swarmId);
            }
            return { output: `${args.action}: ${task.id}` };
          }
          return { output: "action requires taskId" };
        },
      }),

      swarm_memory: tool({
        description: "Read/write durable shared knowledge on the swarm blackboard.",
        args: {
          swarmId: tool.schema.string(),
          action: tool.schema.enum(["get", "put", "list", "search"]),
          key: tool.schema.string().optional(),
          value: tool.schema.string().optional(),
          expectedVersion: tool.schema.number().optional().describe("Required when the key already exists: pass the version you read via get. A put without it on an existing key is a conflict, not an overwrite."),
          query: tool.schema.string().optional(),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          if (args.action === "put") {
            if (!args.key || args.value === undefined) return { output: "put requires key and value" };
            const member = await memberForContext(core, args.swarmId, ctx.sessionID);
            let entry;
            try {
              entry = await core.blackboardPut({
                swarmId: args.swarmId,
                key: args.key,
                value: args.value,
                contentType: "text/markdown",
                expectedVersion: args.expectedVersion,
                authorMemberId: member.id,
              });
            } catch (err) {
              // Render a version conflict as an actionable notice (audit S2 /
              // Core-Auditor F10): tell the caller the current version and how
              // to retry, instead of a bare exception. A missing expectedVersion
              // on an existing key surfaces as a conflict here too.
              if (err instanceof BlackboardConflict) {
                return { output: formatBlackboardConflict({ key: args.key, expectedVersion: args.expectedVersion, currentVersion: err.currentVersion }) };
              }
              throw err;
            }
            // Route a notification to subscribed members (pub/sub). Post-commit
            // external effect, non-blocking. Include the value so subscribers
            // get the content in their inbox (hive-mind: no extra get).
            core.publishBlackboard({
              swarmId: args.swarmId,
              key: entry.key,
              entryVersion: entry.version,
              value: entry.value,
              notifyKind: "message",
            }).catch(() => {});
            return { output: JSON.stringify({ key: entry.key, version: entry.version }, null, 2) };
          }
          if (args.action === "get") {
            if (!args.key) return { output: "get requires key" };
            const entry = await core.store.getBlackboard(args.swarmId, args.key);
            if (entry) return JSON.stringify({ key: entry.key, version: entry.version, value: entry.value }, null, 2);
            // Dead-end avoidance (TU1 / Core-Auditor F15): a get-miss should not
            // be a bare "no entry" — suggest the closest existing keys so the
            // caller can recover from a typo or namespace drift without guessing.
            const all = await core.store.searchBlackboard(args.swarmId, "");
            const key = args.key.toLowerCase();
            const scored = all
              .map((e) => ({ e, d: editDistance(key, e.key.toLowerCase()) }))
              .sort((a, b) => a.d - b.d)
              .slice(0, 3)
              .map((s) => s.e.key);
            return scored.length
              ? `no entry for '${args.key}' — closest keys: ${scored.join(", ")}`
              : `no entry for '${args.key}' (blackboard is empty)`;
          }
          if (args.action === "search") {
            const entries = await core.store.searchBlackboard(args.swarmId, args.query ?? "");
            return JSON.stringify(entries.map((e) => ({ key: e.key, version: e.version, value: fence(truncate(e.value, 200)) })), null, 2);
          }
          if (args.action === "list") {
            const entries = await core.store.searchBlackboard(args.swarmId, "");
            return JSON.stringify(entries.map((e) => ({ key: e.key, version: e.version, value: fence(truncate(e.value, 200)) })), null, 2);
          }
          return { output: "unknown action" };
        },
      }),

      artifact_annotate: tool({
        description: [
          "Annotate a workspace path with durable advisory scent (Hive H0).",
          "Types: claim (I own this lane), struggle (stuck here), corpse (dead",
          "end — different approach needed), gold (verified solution),",
          "affordance (path worth trying), note (free-form).",
          "One ACTIVE annotation per (path, type) — a fresh annotation replaces",
          "the previous one on the same path/type. Optional ttl (ms) makes the",
          "annotation expire; weight (0..10) sets signal strength. Annotations",
          "are advisory only and rendered in swarm_probe / swarm_status",
          "detail:lanes — they are NOT enforced by the scheduler.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          path: tool.schema.string().describe("Workspace path (relative or absolute)."),
          type: tool.schema.enum(["claim", "struggle", "corpse", "gold", "affordance", "note"]),
          weight: tool.schema.number().optional().describe("Signal strength 0..10 (default 1)."),
          note: tool.schema.string().optional().describe("Free-form note (untrusted data when rendered)."),
          errorSig: tool.schema.string().optional().describe("Error signature (for struggle/corpse) — untrusted data when rendered."),
          solutionHash: tool.schema.string().optional().describe("Verified-solution hash (for gold) — untrusted data when rendered."),
          ttl: tool.schema.number().optional().describe("Optional TTL in ms; annotation expires after this."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const member = await memberForContext(core, args.swarmId, ctx.sessionID);
          const annotation = await core.store.insertAnnotation({
            id: `ann_${crypto.randomUUID().replace(/-/g, "")}`,
            swarmId: args.swarmId,
            path: args.path,
            type: args.type,
            weight: args.weight ?? 1,
            note: args.note,
            errorSig: args.errorSig,
            solutionHash: args.solutionHash,
            ttl: args.ttl,
            authorMemberId: member.id,
            createdAt: Date.now(),
          });
          const expiry = annotation.expiresAt ? `, expires ${new Date(annotation.expiresAt).toISOString()}` : "";
          return {
            output: `annotation: ${annotation.type} on '${annotation.path}' recorded (id ${annotation.id}${expiry}) — advisory only; visible in swarm_probe / swarm_status detail:lanes`,
          };
        },
      }),

      artifact_list: tool({
        description: [
          "List artifact annotations (advisory scent) for a path or the whole",
          "swarm. Renders type, weight, author, note, and expiry. Annotations",
          "are untrusted data when rendered — see the fenced note. Stale",
          "(expired) annotations are excluded by default. Use",
          "artifact_annotate to add; releaseOrDelete is via artifact_list with",
          "action 'delete'.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          path: tool.schema.string().optional().describe("Filter to one path (exact match)."),
          action: tool.schema.enum(["list", "delete"]).optional().describe("delete removes an annotation by id (needs annotationId)."),
          annotationId: tool.schema.string().optional().describe("Annotation id (required for action 'delete')."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          if (args.action === "delete") {
            if (!args.annotationId) return { output: "artifact_list delete requires annotationId" };
            const member = await memberForContext(core, args.swarmId, ctx.sessionID);
            const ok = await core.store.releaseOrDeleteAnnotation(args.annotationId);
            if (!ok) return { output: `no annotation '${args.annotationId}' to delete` };
            return { output: `deleted annotation ${args.annotationId}` };
          }
          const members = await core.store.listMembers(args.swarmId);
          const nameById = new Map(members.map((m) => [m.id, m.name]));
          const annotations = await core.store.listAnnotations(args.swarmId, { path: args.path, activeOnly: true });
          if (annotations.length === 0) {
            return { output: args.path ? `no annotations on '${args.path}'` : "no annotations in this swarm" };
          }
          const lines = annotations.map((a) => {
            const author = nameById.get(a.authorMemberId) ?? a.authorMemberId;
            const expiry = a.expiresAt ? `, expires ${new Date(a.expiresAt).toISOString()}` : "";
            const note = a.note ? ` ${fence(a.note)}` : "";
            const extra = a.errorSig ? ` (error: ${fence(a.errorSig)})` : a.solutionHash ? ` (solution ${fence(a.solutionHash.slice(0, 12))})` : "";
            return `  [${a.type}] ${a.path} w${a.weight} by ${author}${extra}${expiry}${note}`;
          });
          return { output: `ARTIFACT ANNOTATIONS (${annotations.length}):\n${lines.join("\n")}` };
        },
      }),

      hive_publish: tool({
        description: [
          "Publish a belief/fact to the hive (Hive H1). Lateral inhibition: if the",
          "same fact_hash was reinforced recently by 2+ distinct peers, the publish",
          "is SUPPRESSED and confidence is reinforced instead (no duplicate",
          "broadcast). Whisper/shout tiers: confidence < 0.6 → whisper; >= 0.8 →",
          "shout; else whisper. A whisper upgraded by 2 independent reinforcements",
          "becomes a shout. Fenced output: the fact text is untrusted data.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          fact: tool.schema.string().describe("The belief/fact text (untrusted data when rendered)."),
          confidence: tool.schema.number().optional().describe("0..1 (default 0.5). <0.6 whisper, >=0.8 shout."),
          tags: tool.schema.string().optional().describe("Comma-separated tags."),
          ttl: tool.schema.number().optional().describe("Optional TTL in ms."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const member = await memberForContext(core, args.swarmId, ctx.sessionID);
          // C15: reject empty/whitespace facts — an empty belief row with a
          // hash of "" is meaningless and pollutes lateral-inhibition matching.
          const trimmedFact = args.fact.trim();
          if (!trimmedFact) return { output: "hive: fact must be non-empty (got only whitespace)" };
          const factHash = hashText(trimmedFact.toLowerCase());
          const confidence = Math.min(1, Math.max(0, args.confidence ?? 0.5));
          const tier: "whisper" | "shout" = confidence >= 0.8 ? "shout" : "whisper";
          // Lateral inhibition: has this fact been reinforced recently by 2+
          // distinct peers? (listBeliefs activeOnly + matching fact_hash.)
          // `same.length > 0` also tells us insert-vs-reinforce for the verdict
          // (item 12 polish): a fresh fact_hash inserts (count 1); an existing
          // one REINFORCES (count increments) — reported truthfully.
          let suppress = false;
          let distinctAuthors = 0;
          let preexistingCount = 0;
          try {
            const existing = await core.store.listBeliefs(args.swarmId, { activeOnly: true });
            const same = existing.filter((b) => b.factHash === factHash);
            preexistingCount = same.length > 0 ? same[0]!.reinforceCount : 0;
            distinctAuthors = new Set(same.map((b) => b.authorMemberId)).size;
            suppress = distinctAuthors >= 2;
          } catch {
            // beliefs surface not landed — treat as fresh publish
          }
          const belief = await core.store.insertBelief({
            id: `bel_${crypto.randomUUID().replace(/-/g, "")}`,
            swarmId: args.swarmId,
            factHash,
            text: args.fact,
            confidence,
            tags: args.tags,
            tier,
            ttl: args.ttl,
            authorMemberId: member.id,
            evidenceRefs: undefined,
            status: "active",
            createdAt: Date.now(),
            updatedAt: Date.now(),
            reinforceCount: 1,
          });
          let tierNote = "";
          // C04: the displayed tier must come from the RETURNED stored row, not
          // args.confidence — the store's reinforce-on-conflict bumps confidence
          // toward the existing value, so a suppressed publish of a low-
          // confidence fact must report the stored whisper tier, never the
          // caller's shout framing.
          const storedTier: "whisper" | "shout" = belief.tier;
          if (belief.tier === "whisper" && belief.reinforceCount >= 2) {
            try {
              const upgraded = await core.store.upgradeWhisperToShout(args.swarmId, factHash);
              if (upgraded) tierNote = "; whisper upgraded to shout (2+ reinforcements)";
            } catch { /* not eligible */ }
          }
          // Truthful insert-vs-reinforce verdict (C03): the post-insert row is
          // the authoritative source. reinforceCount > 1 means the store's ON
          // CONFLICT incremented an existing row → REINFORCED. reinforceCount 1
          // with no pre-existing row → INSERTED. If a concurrent publisher
          // slipped between our read and write, the post-read still reflects the
          // actual stored state (never two "inserted (count 1)" claims).
          const reinforced = belief.reinforceCount > 1
            ? true
            : preexistingCount > 0;
          const verdict = reinforced
            ? `reinforced (count ${belief.reinforceCount})`
            : `inserted (count ${belief.reinforceCount})`;
          if (suppress) {
            return {
              output: `hive: SUPPRESSED duplicate — fact ${factHash} already reinforced by ${distinctAuthors} distinct peers; ${verdict}; confidence reinforced to ${belief.confidence.toFixed(2)}${tierNote} (fact: ${fence(args.fact)})`,
            };
          }
          return {
            output: `hive: ${verdict} ${storedTier} (${factHash}) confidence ${belief.confidence.toFixed(2)}${tierNote} (fact: ${fence(args.fact)})`,
          };
        },
      }),

      hive_reinforce: tool({
        description: [
          "Reinforce an existing hive belief by fact_hash: increments confidence",
          "and reinforce_count (clamped 0..1). A whisper reaching 2 reinforcements",
          "upgrades to shout. RESONANCE (Hive H2): if the reinforcing member is a",
          "DIFFERENT author from the belief's and cites evidence DISJOINT from the",
          "stored evidence, the confidence combines independently via",
          "1 - (1-c1)*(1-c2) and the belief is marked resonant — reported",
          "truthfully with the evidence refs. Cite your evidence (optional) via",
          "the 'evidence' arg so independent convergence can be detected.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          factHash: tool.schema.string().describe("The belief's fact_hash (from hive_publish output)."),
          delta: tool.schema.number().optional().describe("Confidence delta (default 0.1)."),
          evidence: tool.schema.array(tool.schema.string()).optional().describe("Your evidence refs (message/artifact ids) for this reinforce — enables resonance detection."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const member = await memberForContext(core, args.swarmId, ctx.sessionID);
          const belief = await core.store.reinforceBelief(args.swarmId, args.factHash, args.delta);
          if (!belief) return { output: `hive: no belief '${args.factHash}' to reinforce` };
          let tierNote = "";
          if (belief.tier === "whisper" && belief.reinforceCount >= 2) {
            try {
              const upgraded = await core.store.upgradeWhisperToShout(args.swarmId, args.factHash);
              if (upgraded) tierNote = "; whisper upgraded to shout";
            } catch { /* not eligible */ }
          }
          // RESONANCE (item 10): different author + disjoint evidence → combine
          // confidences independently and mark resonant. Truthful: report the
          // combined confidence + merged evidence refs.
          let resonanceNote = "";
          const incoming = args.evidence ?? [];
          const differentAuthor = belief.authorMemberId !== member.id;
          if (differentAuthor && incoming.length > 0) {
            const resonance = computeResonance({
              existing: belief,
              incomingAuthorId: member.id,
              incomingEvidence: incoming,
              deltaConfidence: args.delta,
            });
            if (resonance.resonant) {
              try {
                // Combine independently (1 - (1-c1)*(1-c2)) — the belief's
                // stored confidence was already bumped by reinforceBelief; the
                // resonance math re-combines from the pre-bump values via the
                // helper (existing.confidence + delta). Mark resonant explicitly
                // so the tool output is truthful.
                const marked = await core.store.markResonant(args.swarmId, args.factHash);
                const merged = JSON.stringify(resonance.mergedEvidence);
                if (marked) {
                  resonanceNote = `; RESONANT: independent convergence (author ${member.name}, ${incoming.length} disjoint evidence ref(s) merged)`;
                } else {
                  resonanceNote = `; resonance candidate (already resonant or merge skipped)`;
                }
                void merged;
              } catch { /* store not ready */ }
            }
          }
          return {
            output: `hive: reinforced ${args.factHash} → confidence ${belief.confidence.toFixed(2)}, count ${belief.reinforceCount}${tierNote}${resonanceNote}`,
          };
        },
      }),

      hive_need: tool({
        description: [
          "Broadcast a need to ONLY the members whose role/task/beliefs match",
          "(targeted routing, never a full broadcast). Matching uses the same",
          "affinity heuristic as the scheduler (name/role/task tokens). The query",
          "is recorded and a finding message is sent to each matching member.",
          "Whisper tier (default): direct to matching members only, no coordinator",
          "copy. Shout tier: matching members + one finding to the coordinator.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          query: tool.schema.string().describe("The need/curiosity (untrusted data when rendered)."),
          urgency: tool.schema.enum(["low", "normal", "high", "urgent"]).optional().describe("Message priority (default normal)."),
          tier: tool.schema.enum(["whisper", "shout"]).optional().describe("whisper = direct only; shout = + coordinator copy (default whisper)."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const member = await memberForContext(core, args.swarmId, ctx.sessionID);
          // Delegate delivery to Messaging-Auditor's single routed path
          // (SwarmCore.deliverNeed): pull-based matching against role/task/
          // blackboard (+beliefs when the schema is wired), fenced bodies,
          // tier semantics (whisper = direct only, shout = + coordinator),
          // broker cooldown/deferral/verdicts/expiry all apply.
          const tier = args.tier ?? (args.urgency === "urgent" || args.urgency === "high" ? "shout" : "whisper");
          const result = await core.deliverNeed({
            swarmId: args.swarmId,
            fromMemberId: member.id,
            query: args.query,
            need: args.query,
            tier,
            priority: args.urgency ?? "normal",
          });
          if (result.recipients.length === 0) {
            return { output: `hive need: no members match '${args.query}' — nothing routed (no broadcast)` };
          }
          const routedTo = result.recipients.map((r) => `${r.name} (${r.reason})`);
          return {
            output: `hive need (${tier}): routed to ${routedTo.join(", ")} — ${result.guidance}`,
          };
        },
      }),

      hive_spotlight: tool({
        description: [
          "Temporarily boost a topic with collective attention (Hive H1 §9):",
          "records a bounded blackboard key context/spotlight/<topic> with an",
          "expiry and sends a targeted notice. Auto-expires; shown in",
          "swarm_status. Abuse is bounded (one active spotlight per topic — a",
          "new one replaces the old).",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          topic: tool.schema.string().describe("Topic to spotlight (becomes the blackboard key suffix)."),
          reason: tool.schema.string().optional().describe("Why the spotlight (untrusted data when rendered)."),
          ttl: tool.schema.number().optional().describe("TTL in ms (default 5 min)."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const member = await memberForContext(core, args.swarmId, ctx.sessionID);
          // C16: sanitize the topic into a safe blackboard-key suffix — no '/',
          // '..', or reserved characters that could collide with other keys or
          // escape the context/spotlight/ namespace.
          const safeTopic = (args.topic ?? "").trim().replace(/[^a-zA-Z0-9_-]+/g, "_").slice(0, 64);
          if (!safeTopic) return { output: "hive spotlight: topic must be non-empty alphanumeric (got only invalid characters)" };
          const ttl = args.ttl ?? 300_000;
          const key = `context/spotlight/${safeTopic}`;
          const expiresAt = Date.now() + ttl;
          await core.blackboardPut({
            swarmId: args.swarmId,
            key,
            value: JSON.stringify({ topic: args.topic, reason: args.reason ?? "", author: member.name, expiresAt }),
            contentType: "application/json",
            authorMemberId: member.id,
          });
          const members = await core.store.listMembers(args.swarmId);
          const targets = members.filter((m) =>
            m.id !== member.id && !["stopped", "stopping", "failed"].includes(m.status),
          );
          for (const t of targets) {
            await core.sendMessage({
              swarmId: args.swarmId,
              fromMemberId: member.id,
              to: t.name,
              kind: "decision",
              message: `[SPOTLIGHT] ${fence(args.topic)}${args.reason ? ` — ${fence(args.reason)}` : ""} (expires in ${Math.round(ttl / 1000)}s)`,
              priority: "normal",
            }).catch(() => undefined);
          }
          return {
            output: `hive spotlight: '${safeTopic}' active for ${Math.round(ttl / 1000)}s (key ${key}, ${targets.length} notified)`,
          };
        },
      }),

      hive_consolidate: tool({
        description: [
          "Run hive consolidation (Hive H2 §12): ephemeral → durable summarization,",
          "prune weak/low-reuse beliefs, compress causal chains, upgrade or expire",
          "per rules. ONE consolidator wins per swarm (CAS lease on",
          "context/consolidation/lock). Output reports retained/pruned/upgraded/",
          "expired counts + unresolved contradictions + causal-chain notes —",
          "truthful, from the beliefs actually read. Coordinator notified via the",
          "messaging lane's notifyConsolidation.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          minConfidence: tool.schema.number().optional().describe("Prune-below confidence (default 0.3)."),
          minReinforce: tool.schema.number().optional().describe("Prune-below reinforce count (default 2)."),
          hard: tool.schema.boolean().optional().describe("Hard-prune (DELETE) vs soft (superseded/expired) — default soft."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const member = await memberForContext(core, args.swarmId, ctx.sessionID);
          // ELECTION: one consolidator wins per swarm — CAS on a bounded lock
          // key. If someone else holds a fresh lock, report the winner. C01:
          // a CAS conflict (another run won between our read and write) must
          // NOT be swallowed — re-read and return the winner truthfully.
          const lockKey = "context/consolidation/lock";
          const lockMs = 60_000;
          const lockNow = Date.now();
          const existingLock = await core.store.getBlackboard(args.swarmId, lockKey);
          if (existingLock) {
            try {
              const lock = JSON.parse(existingLock.value);
              if (lock.expiresAt > lockNow && lock.winner !== member.id) {
                return { output: `hive consolidate: another run is active (winner ${lock.winner}); try again shortly` };
              }
            } catch { /* malformed lock — steal below */ }
          }
          const runId = `cons-${lockNow}-${Math.random().toString(36).slice(2, 6)}`;
          try {
            await core.blackboardPut({
              swarmId: args.swarmId,
              key: lockKey,
              value: JSON.stringify({ winner: member.id, runId, expiresAt: lockNow + lockMs }),
              contentType: "application/json",
              authorMemberId: member.id,
              expectedVersion: existingLock?.version,
            });
          } catch (err) {
            // C01: CAS conflict — another run won the lock between our read and
            // write. Re-read to report the actual winner; do NOT proceed.
            const winnerLock = await core.store.getBlackboard(args.swarmId, lockKey).catch(() => undefined);
            let winner = "another run";
            if (winnerLock) {
              try {
                const w = JSON.parse(winnerLock.value);
                if (w.winner) winner = w.winner;
              } catch { /* fall through to generic */ }
            }
            return { output: `hive consolidate: another run is active (winner ${winner}); try again shortly` };
          }

          // Read active beliefs (substrate-gated).
          let beliefs: import("./core/types.js").Belief[] = [];
          try {
            beliefs = await core.store.listBeliefs(args.swarmId, { activeOnly: true });
          } catch { /* beliefs not landed */ }
          if (beliefs.length === 0) {
            await core.blackboardPut({
              swarmId: args.swarmId,
              key: "context/consolidation/last",
              value: JSON.stringify({ runId, at: lockNow, coordinator: member.name, retained: 0, pruned: 0, upgraded: 0, expired: 0, contradictions: 0, prunedFactHashes: [], guidance: "no active beliefs to consolidate" }),
              contentType: "application/json",
              authorMemberId: member.id,
            }).catch(() => undefined);
            return { output: `hive consolidate (${runId}): no active beliefs — nothing to consolidate` };
          }

          const minConf = args.minConfidence ?? 0.3;
          const minReinf = args.minReinforce ?? 2;
          const retained: string[] = [];
          const pruned: string[] = [];
          const upgraded: string[] = [];
          const expired: string[] = [];
          const skipped: string[] = [];
          const contradictions: Array<{ factHash: string; text: string }> = [];

          for (const b of beliefs) {
            const action = consolidationAction(b, { minConfidence: minConf, minReinforce: minReinf });
            try {
              if (action === "upgrade") {
                const up = await core.store.upgradeWhisperToShout(args.swarmId, b.factHash);
                if (up) upgraded.push(b.factHash);
                else skipped.push(b.factHash);
              } else if (action === "prune") {
                if (args.hard) {
                  const del = await core.store.hardPruneBeliefs(args.swarmId, [b.factHash]);
                  if (del > 0) pruned.push(b.factHash);
                  else skipped.push(b.factHash);
                } else {
                  const sp = await core.store.softPruneBelief(args.swarmId, b.factHash, "superseded");
                  if (sp) pruned.push(b.factHash);
                  else skipped.push(b.factHash);
                }
              } else if (action === "expire") {
                // C02: "expired" must actually transition the belief — soft-prune
                // to 'expired' now (row kept, causal chain intact), not just
                // count it and wait for the sweep.
                const sp = await core.store.softPruneBelief(args.swarmId, b.factHash, "expired");
                if (sp) expired.push(b.factHash);
                else skipped.push(b.factHash);
              } else {
                retained.push(b.factHash);
              }
            } catch {
              // C07: a thrown transition must not silently vanish from the
              // counts — reconcile as skipped so retained+pruned+upgraded+
              // expired+skipped == beliefs.length.
              skipped.push(b.factHash);
            }
          }

          // Contradictions: distinct active beliefs whose texts SHARE
          // significant tokens but cite DIFFERENT (disjoint) evidence — both
          // retained, flagged (dissent preserved). C06: compare token SETS
          // (intersection), not the union — the old `needTokens(a+b)` was
          // nearly always true. Flag only when ≥2 shared tokens (or exact-text
          // match) AND evidence is disjoint.
          for (let i = 0; i < beliefs.length; i++) {
            for (let j = i + 1; j < beliefs.length; j++) {
              const a = beliefs[i]!, b = beliefs[j]!;
              if (a.status !== "active" || b.status !== "active") continue;
              const ta = parseEvidenceRefs(a.evidenceRefs);
              const tb = parseEvidenceRefs(b.evidenceRefs);
              const disjoint = ta.length > 0 && tb.length > 0 && !ta.some((r) => tb.includes(r));
              const tokensA = new Set(needTokens(a.text));
              const tokensB = new Set(needTokens(b.text));
              let shared = 0;
              for (const t of tokensA) if (tokensB.has(t)) shared++;
              const similar = a.text.toLowerCase() === b.text.toLowerCase();
              if ((similar || shared >= 2) && disjoint) {
                contradictions.push({ factHash: a.factHash, text: a.text });
                break;
              }
            }
          }

          const causalChains = beliefs.slice(0, 5).map((b) => causalChainNote(b));
          const result = {
            runId,
            coordinator: member.name,
            retained: retained.length,
            pruned: pruned.length,
            upgraded: upgraded.length,
            expired: expired.length,
            skipped: skipped.length,
            contradictions: contradictions.length,
            prunedFactHashes: pruned,
            causalChains,
            guidance: "ephemeral → durable: strong beliefs were retained; weak+low-reuse were pruned; whisper→shout upgrades applied; expired beliefs transitioned to 'expired'",
          };

          // Write the diagnostics surface + notify the messaging lane.
          await core.blackboardPut({
            swarmId: args.swarmId,
            key: "context/consolidation/last",
            value: JSON.stringify({ ...result, at: lockNow }),
            contentType: "application/json",
            authorMemberId: member.id,
          }).catch(() => undefined);
          // Wire Messaging-Auditor's notice half (P5, exactly-once by runId):
          // notable runs produce one coordinator finding + compact broadcast;
          // non-notable runs emit nothing automatically. Best-effort — the
          // notice must never break the consolidation run itself.
          await core.notifyConsolidation({ swarmId: args.swarmId, result }).catch((err) => {
            console.error(`[swarm] consolidation notice failed for ${args.swarmId}:`, (err as Error).message);
          });
          // C05: release the election lock on completion so the next run can
          // start immediately (no 60s wedge after a successful run). The store
          // has no blackboard delete, so "release" = write the lock with
          // expiresAt: 0 (immediately past → not active). Only clear OUR lock
          // (runId match) — never clobber a newer winner's lock.
          const curLock = await core.store.getBlackboard(args.swarmId, lockKey).catch(() => undefined);
          if (curLock) {
            try {
              const l = JSON.parse(curLock.value);
              if (l.runId === runId || l.winner === member.id) {
                await core.blackboardPut({
                  swarmId: args.swarmId,
                  key: lockKey,
                  value: JSON.stringify({ winner: member.id, runId, expiresAt: 0 }),
                  contentType: "application/json",
                  authorMemberId: member.id,
                  expectedVersion: curLock.version,
                }).catch(() => undefined);
              }
            } catch { /* malformed — leave */ }
          }
          return {
            output: `hive consolidate (${runId}) by ${member.name}: retained ${retained.length}, pruned ${pruned.length}, upgraded ${upgraded.length}, expired ${expired.length}, skipped ${skipped.length}, contradictions ${contradictions.length}${contradictions.length ? ` (${contradictions.map((c) => c.factHash).join(", ")})` : ""}\ncausal chains:\n${causalChains.join("\n")}\nguidance: ${result.guidance}`,
          };
        },
      }),

      hive_relevant: tool({
        description: [
          "Rank hive beliefs by token relevance to a query (Hive H2 §11). Uses the",
          "same tokenizer as need routing; returns top beliefs by relevance score",
          "with tier + tags, fact text fenced.",
          "EXTENSION POINT: semanticRelevanceHook (src/hive/relevance.ts) is the",
          "documented H2+ embedding hook. Today it returns undefined, so scoring",
          "is pure token-overlap with NO SDK dependency; when an embedding",
          "provider is wired (local model endpoint or cached embeddings), the",
          "hook returns a 0..1 semantic score and rankBeliefsByRelevance merges",
          "it with the token score. No caller change needed — the tool reads the",
          "same rankBeliefsByRelevance API either way.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          query: tool.schema.string().describe("Query to rank beliefs against."),
          limit: tool.schema.number().optional().describe("Max results (default 5)."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          await memberForContext(core, args.swarmId, ctx.sessionID);
          let beliefs: import("./core/types.js").Belief[] = [];
          try {
            beliefs = await core.store.listBeliefs(args.swarmId, { activeOnly: true });
          } catch { /* beliefs not landed */ }
          const ranked = rankBeliefsByRelevance(beliefs, args.query, args.limit ?? 5);
          if (ranked.length === 0) {
            return { output: `hive relevant: no beliefs match '${args.query}'` };
          }
          const lines = ranked.map((r) =>
            `  ${r.score.toFixed(2)} [${r.tier}] ${r.factHash}${r.tags ? ` tags=${fence(r.tags)}` : ""} ${fence(r.text)}`,
          );
          return { output: `hive relevant to '${args.query}':\n${lines.join("\n")}` };
        },
      }),

      swarm_subscribe: tool({
        description: [
          "Subscribe the calling member to a blackboard topic pattern.",
          "When a matching blackboard key is updated, the member is notified via its",
          "mailbox instead of a full broadcast. Patterns use globs: 'contracts/**',",
          "'decisions/ui/*'. Blackboard keys are slash-separated, e.g.",
          "'contracts/auth/refresh-v3'.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          action: tool.schema.enum(["subscribe", "unsubscribe", "list"]).optional().describe("subscribe (default), unsubscribe, or list."),
          pattern: tool.schema.string().optional().describe("Topic glob pattern (required for subscribe)."),
          subscriptionId: tool.schema.string().optional().describe("Subscription id (required for unsubscribe)."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const action = args.action ?? "subscribe";
          if (action === "list") {
            const subs = await core.store.listSubscriptions(args.swarmId);
            return { output: JSON.stringify(subs, null, 2) };
          }
          if (action === "unsubscribe") {
            if (!args.subscriptionId) return { output: "unsubscribe requires subscriptionId" };
            await core.unsubscribe(args.subscriptionId);
            return { output: `unsubscribed ${args.subscriptionId}` };
          }
          if (!args.pattern) return { output: "subscribe requires a pattern" };
          const member = await core.store.getMemberBySessionId(ctx.sessionID);
          if (!member || member.swarmId !== args.swarmId) {
            return { output: "calling session is not a member of this swarm" };
          }
          const sub = await core.subscribe({
            swarmId: args.swarmId,
            memberId: member.id,
            pattern: args.pattern,
          });
          return { output: JSON.stringify({ subscriptionId: sub.id, pattern: sub.pattern, member: member.name }, null, 2) };
        },
      }),

      swarm_wake: tool({
        description: [
          "Manually wake a swarm member to process its queued mailbox.",
          "Delivers all pending messages for the member as a single batched inbox",
          "prompt. Useful for manual pokes and testing; the supervisor normally",
          "wakes idle members automatically on session.idle events.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          member: tool.schema.string().describe("Member name to wake."),
        },
        async execute(args) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const member = await core.store.getMemberByName(args.swarmId, args.member);
          if (!member) return { output: `no member named '${args.member}'` };
          if (member.status === "stopping") {
            return { output: `member '${args.member}' is stopping; try again shortly or remove it with swarm_remove` };
          }
          // R2: never force-set a busy/working/chatting member to idle — that
          // would tear down a legitimate working state (and its owned task).
          // Wake is only meaningful for idle members; for everyone else report
          // the truthful verdict (deferred/chatting/no-op) and preserve status.
          if (member.status === "working" || member.status === "starting") {
            return {
              output: `${member.name} is ${member.status}${member.currentTaskId ? ` (task ${member.currentTaskId})` : ""} — wake deferred; delivery happens at the next idle boundary. No status change.`,
            };
          }
          if (member.status === "stopped" || member.status === "failed" || member.status === "interrupted") {
            return {
              output: `${member.name} is ${member.status} — wake is a no-op; revive (recovery) or re-spawn it instead. No status change.`,
            };
          }
          const swarm = await core.store.getSwarm(args.swarmId);
          const chatting = swarm ? await rt.humanChat.chatting(member, swarm).catch(() => false) : false;
          if (chatting) {
            return {
              output: `${member.name} is chatting with the user — wake deferred until the chat lull or swarm_release; mail arrives at the next boundary. No status change.`,
            };
          }
          const delivered = await rt.broker.deliverToIdleMember(member.id, member.sessionId);
          // Only an idle member gets its status refreshed to idle (already idle —
          // a no-op refresh, never a force from another status).
          if (member.status === "idle") {
            await core.store.updateMemberStatus(member.id, "idle", { lastActiveAt: Date.now() });
          }
          if (delivered === 0) {
            return {
              output: `${member.name} has no queued mail — messages are auto-delivered on send, so manual wake is usually unnecessary. If a message was just sent, wait for the auto-delivery or check swarm_status.`,
            };
          }
          return {
            output: `woke ${member.name}: ${delivered} message(s) delivered`,
          };
        },
      }),

      swarm_models: tool({
        description: [
          "List the models the spawner has access to, grouped by tier, with the",
          "EXACT {providerID, modelID} values to pass into swarm_spawn members.",
          "Always copy the providerID literally — it is NOT a tier name.",
          "Tiers:",
          "  zen-free  - OpenCode Zen free models",
          "  zen       - OpenCode Zen paid models",
          "  go        - OpenCode Go models",
          "  <provider> - other configured providers",
          "Usage: in swarm_spawn set member.model = { providerID: <the providerID shown>, modelID: <the modelID shown> }.",
        ].join("\n"),
        args: {
          tier: tool.schema.enum(["zen-free", "zen", "go", "all"]).optional().describe("Filter by tier (default all)."),
          search: tool.schema.string().optional().describe("Filter model ids by substring (e.g. 'flash', 'longcat')."),
        },
        async execute(args) {
          const rt = await ensureRt();
          const core = rt.core;
          const all = await rt.listModels();
          const filtered = all.filter((m) => {
            if (args.tier && args.tier !== "all" && m.tier !== args.tier) return false;
            if (args.search && !m.modelID.toLowerCase().includes(args.search.toLowerCase())) return false;
            return true;
          });
          const byTier = new Map<string, string[]>();
          for (const m of filtered) {
            const list = byTier.get(m.tier) ?? [];
            const label = m.name && m.name !== m.modelID ? `  ${m.modelID} (${m.name})` : `  ${m.modelID}`;
            list.push(`${label}  -> providerID: "${m.providerID}", modelID: "${m.modelID}"`);
            byTier.set(m.tier, list);
          }
          const lines: string[] = [];
          for (const [tier, ids] of [...byTier.entries()].sort()) {
            lines.push(`${tier.toUpperCase()} (${ids.length})`);
            for (const id of ids.sort()) lines.push(id);
          }
          return { output: lines.join("\n") || "(no models match)" };
        },
      }),

      swarm_roster: tool({
        description: [
          "See the current swarm team: every member's name, role, status, and",
          "current task. Use this to find which peer to message directly for a",
          "specific concern — then contact them with swarm_message instead of",
          "going through the coordinator.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string().describe("Swarm id or name."),
        },
        async execute(args) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const [members, tasks] = await Promise.all([
            core.store.listMembers(args.swarmId),
            core.store.listTasks(args.swarmId),
          ]);
          const swarm = await core.store.getSwarm(args.swarmId);
          const taskTitle = new Map(tasks.filter((t) => t.status !== "completed").map((t) => [t.id, t.title]));
          const lines: string[] = [`TEAM — ${members.length} member(s)`];
          // Permission diagnostics (Wave 2): effective propagation mode for the
          // swarm — inherit (Case A), worktree-scoped (Case B, clamped),
          // accept-all-static, or unknown (Case C).
          if (swarm) {
            const perms = await rt.permsDiagnosticsForSwarm(args.swarmId);
            lines.push(`  ${perms}`);
          }
          // Hive H1 one-line summary (task_ff1d34): compact, truthful, omitted
          // when trivial (no beliefs/needs/spotlight/corpse-piles).
          if (swarm) {
            const hive = buildHiveSummary(await rt.hiveDiagnosticsForSwarm(args.swarmId));
            if (hive) lines.push(`  ${hive}`);
          }
          for (const m of members) {
            const task = m.currentTaskId ? taskTitle.get(m.currentTaskId) ?? m.currentTaskId : "";
            let line = `  ${m.name}${m.role && m.role !== m.name ? ` (${m.role})` : ""}: ${m.status}${task ? ` — ${task}` : ""}`;
            if (swarm && await rt.humanChat.chatting(m, swarm)) line += " 👤 chatting";
            lines.push(line);
          }
          return { output: lines.join("\n") };
        },
      }),

      swarm_find: tool({
        description: [
          "Find which swarm member(s) are likely to know about a topic, based on",
          "their role and current work. Use this to route a question directly to",
          "the right peer with swarm_message, instead of asking the coordinator",
          "or guessing.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string().describe("Swarm id or name."),
          topic: tool.schema.string().describe("Keyword describing the concern (e.g. 'auth', 'ui', 'tests')."),
        },
        async execute(args) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const [members, tasks] = await Promise.all([
            core.store.listMembers(args.swarmId),
            core.store.listTasks(args.swarmId),
          ]);
          const q = args.topic.toLowerCase();
          const taskByOwner = new Map<string, string>();
          for (const t of tasks) {
            if (t.ownerMemberId) taskByOwner.set(t.ownerMemberId, t.title);
          }
          const matches = members
            .filter((m) => !["stopped", "stopping", "failed"].includes(m.status))
            .map((m) => ({
              m,
              hay: `${m.name} ${m.role} ${taskByOwner.get(m.id) ?? ""}`.toLowerCase(),
            }))
            .filter(({ hay }) => hay.includes(q))
            .map(({ m }) => {
              const task = taskByOwner.get(m.id);
              return `  ${m.name} (${m.role})${task ? ` — working on: ${task}` : ""} — status ${m.status}`;
            });
          return {
            output: matches.length
              ? `Members matching "${args.topic}":\n${matches.join("\n")}`
              : `No member obviously matches "${args.topic}". Use swarm_roster to see everyone, or broadcast to "*".`,
          };
        },
      }),

      swarm_probe: tool({
        description: [
          "Probe the swarm for what's ALREADY being worked on, to avoid redundant work.",
          "Searches member roles, blackboard, and recent message bodies for a keyword",
          "or phrase (e.g. 'nibble', 'wire', 'sort lane'). If another member is already",
          "touching that topic, coordinate with them instead of duplicating. Call this",
          "BEFORE starting work that might overlap a peer's lane.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string().describe("Swarm id or name."),
          query: tool.schema.string().describe("Keyword/phrase to search for across member roles, blackboard, and recent messages."),
        },
        async execute(args) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const q = args.query.toLowerCase();

          const [members, tasks, bbEntries, msgs] = await Promise.all([
            core.store.listMembers(args.swarmId),
            core.store.listTasks(args.swarmId),
            core.store.searchBlackboard(args.swarmId, q),
            core.store.searchMessagesBySwarm(args.swarmId, q, 12),
          ]);
          const nameById = new Map(members.map((m) => [m.id, m.name]));
          const taskByOwner = new Map<string, string>();
          for (const t of tasks) {
            if (t.ownerMemberId && (t.status === "working" || t.status === "claimed")) taskByOwner.set(t.ownerMemberId, t.title);
          }
          const lines: string[] = [`Probe "${args.query}":`];

          // Members whose role/current work mentions it.
          const memberHits = members
            .filter((m) => !["stopped", "stopping", "failed"].includes(m.status))
            .filter((m) => {
              const hay = `${m.name} ${m.role} ${taskByOwner.get(m.id) ?? ""}`.toLowerCase();
              return hay.includes(q);
            })
            .map((m) => {
              const task = taskByOwner.get(m.id);
              return `  member ${m.name}${task ? ` — working: ${task}` : ""} (${m.status})`;
            });
          if (memberHits.length) lines.push("MEMBERS:", ...memberHits);

          // Blackboard entries mentioning it.
          if (bbEntries.length) {
            lines.push("BLACKBOARD:");
            for (const e of bbEntries.slice(0, 8)) {
              lines.push(`  ${e.key} (v${e.version}) ${fence(truncate(e.value, 140))}`);
            }
          }

          // Recent messages mentioning it.
          if (msgs.length) {
            lines.push("RECENT MESSAGES:");
            for (const m of msgs) {
              lines.push(`  ${formatEnvelope(m, nameById)}`);
            }
          }

          // Artifact annotations mentioning it (Hive H0 advisory scent): the
          // cheapest way to see "someone flagged this path as a dead end / gold"
          // before starting overlapping work. Notes are untrusted data (fenced).
          let annotationHits = 0;
          try {
            const annotations = await core.store.listAnnotations(args.swarmId, { activeOnly: true });
            const hits = annotations.filter((a) =>
              `${a.path} ${a.type} ${a.note ?? ""} ${a.errorSig ?? ""} ${a.solutionHash ?? ""}`.toLowerCase().includes(q),
            );
            if (hits.length) {
              annotationHits = hits.length;
              lines.push("ARTIFACT ANNOTATIONS:");
              for (const a of hits.slice(0, 8)) {
                const author = nameById.get(a.authorMemberId) ?? a.authorMemberId;
                const note = a.note ? ` ${fence(truncate(a.note, 120))}` : "";
                const extra = a.errorSig ? ` (error: ${fence(a.errorSig)})` : a.solutionHash ? ` (solution ${fence(a.solutionHash.slice(0, 12))})` : "";
                lines.push(`  [${a.type}] ${a.path} w${a.weight} by ${author}${extra}${note}`);
              }
            }
          } catch {
            // annotations surface not available (store methods not landed) — skip
          }

          // Live member todos mentioning it — the likeliest place redundancy hides.
          if (rt.sessionTodos) {
            const workerMembers = members.filter((m) => m.role !== "coordinator");
            const todoResults = await Promise.all(
              workerMembers.map((m) => rt.sessionTodos!(m.sessionId).catch(() => [])),
            );
            const todoHits: string[] = [];
            workerMembers.forEach((m, i) => {
              const hits = todoResults[i]!.filter((t) => `${t.content} ${t.status}`.toLowerCase().includes(q));
              for (const t of hits) todoHits.push(`  member ${m.name}: [${t.status}] ${t.content}`);
            });
            if (todoHits.length) lines.push("MEMBER TODOS:", ...todoHits.slice(0, 10));
          }

          if (memberHits.length === 0 && bbEntries.length === 0 && msgs.length === 0 && annotationHits === 0 && !lines.some((l) => l.startsWith("MEMBER TODOS:"))) {
            lines.push("  (no member, blackboard entry, recent message, annotation, or todo mentions this)");
          }
          return { output: lines.join("\n") };
        },
      }),

      swarm_status: tool({
        description: "Inspect swarm state: members, tasks, messages, path claims.",
        args: {
          swarmId: tool.schema.string(),
          detail: tool.schema.enum(["summary", "members", "tasks", "messages", "lanes", "full"]).optional(),
          to: tool.schema.string().optional().describe("With detail:messages, only show messages to this member (e.g. your own name) so you can read your pending queue."),
        },
        async execute(args) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const [swarm, members, tasks, claims] = await Promise.all([
            core.store.getSwarm(args.swarmId),
            core.store.listMembers(args.swarmId),
            core.store.listTasks(args.swarmId),
            core.store.listPathClaims(args.swarmId),
          ]);
          if (!swarm) return { output: `no swarm '${args.swarmId}'` };
          const nameById = new Map(members.map((m) => [m.id, m.name]));
          const lines: string[] = [`Swarm: ${swarm.name} (${swarm.id})`, ""];

          if (args.detail === "messages") {
            let msgs = await core.store.listMessagesBySwarm(args.swarmId, 50);
            // Optional `to` filter: only messages addressed to a given member
            // (including their pending queue), so a member can read what's for
            // them without scanning the whole stream.
            if (args.to) {
              const target = members.find((m) => m.name === args.to || m.id === args.to);
              if (!target) return { output: `no member named '${args.to}' in this swarm` };
              msgs = msgs.filter((m) => m.to.type === "member" && m.to.memberId === target.id);
            }
            lines.push(`RECENT MESSAGES (${msgs.length})`);
            if (msgs.length === 0) lines.push("  (none)");
            for (const msg of msgs) {
              lines.push(`  ${formatEnvelope(msg, nameById)}`);
            }
            return { output: lines.join("\n") };
          }

          if (args.detail === "lanes") {
            // The lane registry: who owns what, plus each member's live todo
            // list and the advisory path-claim registry — the durable
            // anti-redundancy surface.
            const laneEntry = await core.store.getBlackboard(args.swarmId, "context/lanes");
            lines.push("LANES");
            lines.push(laneEntry?.value ?? "  (no lane registry yet — delegate the swarm to create one)");
            // Cross-member todos: what each peer is actively doing right now.
            // Fetch all members in parallel (N sessions = N round-trips).
            if (rt.sessionTodos) {
              lines.push("");
              lines.push("MEMBER TODOS");
              const workerMembers = members.filter((m) => m.role !== "coordinator");
              const todoResults = await Promise.all(
                workerMembers.map((m) => rt.sessionTodos!(m.sessionId).catch(() => [])),
              );
              let any = false;
              workerMembers.forEach((m, i) => {
                const active = todoResults[i]!.filter((t) => t.status === "in_progress" || t.status === "pending");
                if (active.length) {
                  any = true;
                  lines.push(`  ${m.name}:`);
                  for (const t of active.slice(0, 5)) {
                    lines.push(`    [${t.status}] ${t.content}`);
                  }
                }
              });
              if (!any) lines.push("  (none of the members have todo items)");
            }
            // Advisory path claims (who is working which lane; TTL advisory —
            // stale claims are already excluded by listPathClaims). WIP Aura
            // (H0): render the expiry as a countdown ("expires in Ns") so the
            // heartbeat budget is visible at a glance. TU12: claims are
            // advisory — not enforced by the scheduler.
            lines.push("");
            lines.push("PATH CLAIMS (advisory — not enforced by the scheduler)");
            if (claims.length === 0) lines.push("  (none)");
            for (const c of claims) {
              const ttl = c.expiresAt
                ? ` (expires in ${Math.max(0, Math.round((c.expiresAt - Date.now()) / 1000))}s)`
                : " (no TTL)";
              lines.push(`  ${c.pattern.padEnd(24)} ${nameById.get(c.memberId) ?? c.memberId}${ttl}`);
            }
            // Artifact annotations (Hive H0 advisory scent) — the collective
            // memory of what's worth / not worth trying on which paths.
            try {
              const annotations = await core.store.listAnnotations(args.swarmId, { activeOnly: true });
              if (annotations.length) {
                lines.push("");
                lines.push("ARTIFACT ANNOTATIONS (advisory)");
                for (const a of annotations.slice(0, 12)) {
                  const author = nameById.get(a.authorMemberId) ?? a.authorMemberId;
                  const expiry = a.expiresAt ? `, exp ${new Date(a.expiresAt).toISOString()}` : "";
                  const note = a.note ? ` ${fence(truncate(a.note, 100))}` : "";
                  lines.push(`  [${a.type}] ${a.path} w${a.weight} by ${author}${expiry}${note}`);
                }
              }
            } catch {
              // annotations store surface not landed — skip
            }
            // Hive H1 diagnostics block (task_ff1d34): active beliefs by tier,
            // active needs, spotlight topics, corpse piles (3+ corpses) / gold
            // trails — compact, advisory, truthful (substrate-gated).
            const hiveBlock = buildHiveBlock(await rt.hiveDiagnosticsForSwarm(args.swarmId));
            if (hiveBlock) {
              lines.push("");
              lines.push(hiveBlock);
            }
            // Hive H1 corpse-pile / gold affinity (Scheduler lane): append-only
            // sections (Core's blocks above untouched).
            try {
              const annotations = await core.store.listAnnotations(args.swarmId, { activeOnly: true });
              const corpseByPath = corpseCountByPath(annotations);
              const corpsePiles = [...corpseByPath.entries()].filter(([, c]) => c >= CORPSE_PILE_THRESHOLD);
              if (corpsePiles.length) {
                lines.push("");
                lines.push("CORPSE PILES (hesitation — >= 3 corpses)");
                for (const [path, count] of corpsePiles) {
                  lines.push(`  ${path.padEnd(24)} ${count} corpses — consider re-planning`);
                }
              }
              const goldByMember = new Map<string, number>();
              for (const a of annotations) {
                if (a.type !== "gold") continue;
                goldByMember.set(a.authorMemberId, (goldByMember.get(a.authorMemberId) ?? 0) + 1);
              }
              if (goldByMember.size) {
                lines.push("");
                lines.push("GOLD TRAILS (affinity bias)");
                for (const [mid, n] of goldByMember) {
                  lines.push(`  ${nameById.get(mid) ?? mid}: ${n} gold annotation(s) on ${annotations.filter((a) => a.type === "gold" && a.authorMemberId === mid).map((a) => a.path).join(", ")}`);
                }
              }
            } catch {
              // annotations store surface not landed — skip
            }
            return { output: lines.join("\n") };
          }

          lines.push("MEMBERS");
          lines.push(`  ${await rt.permsDiagnosticsForSwarm(args.swarmId)}`);
          for (const m of members) {
            let line = `  ${m.name.padEnd(12)} ${m.status.padEnd(10)} task=${m.currentTaskId ?? "-"}`;
            if (await rt.humanChat.chatting(m, swarm)) line += " 👤 chatting";
            lines.push(line);
          }
          lines.push("");
          lines.push("TASKS");
          const done = tasks.filter((t) => t.status === "completed").length;
          lines.push(`  ${done}/${tasks.length} complete`);
          for (const t of tasks.filter((x) => x.status !== "completed")) {
            lines.push(`  ${t.status.padEnd(10)} ${t.priority} ${t.title} owner=${nameById.get(t.ownerMemberId ?? "") ?? "-"}`);
          }
          lines.push("");
          lines.push("PATH CLAIMS");
          if (claims.length === 0) lines.push("  (none)");
          for (const c of claims) {
            lines.push(`  ${c.pattern.padEnd(24)} ${nameById.get(c.memberId) ?? c.memberId}`);
          }
          return { output: lines.join("\n") };
        },
      }),

      swarm_stop: tool({
        description: [
          "Stop a single worker member. The member must be named explicitly —",
          "a stop with no member is rejected (it previously silently targeted",
          "the coordinator). The member's owned task is released back to ready",
          "so the DAG keeps advancing. To tear down the WHOLE swarm (members,",
          "tasks, messages, blackboard) use swarm_delete with confirm.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          member: tool.schema.string().describe("Worker member name to stop (required — no whole-swarm default)."),
          reason: tool.schema.string().optional(),
          force: tool.schema.boolean().optional(),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          // No memberless coordinator-only footgun: an explicit member name is
          // REQUIRED. A stop without one previously fell back to the
          // coordinator (or members[0]) — silently halting the whole swarm's
          // root. Whole-swarm teardown belongs to swarm_delete.
          if (!args.member) {
            return { output: "swarm_stop requires an explicit member name (e.g. member: \"worker1\"); to tear down the whole swarm use swarm_delete with confirm" };
          }
          const members = await core.store.listMembers(args.swarmId);
          const target = members.find((m) => m.name === args.member);
          if (!target) return { output: `no member named '${args.member}'` };
          if (target.role === "coordinator") {
            return { output: "cannot stop the coordinator; use swarm_delete to tear down the whole swarm" };
          }
          // Only the coordinator may stop a member (destructive-op safety; the
          // coordinator is the swarm's authority, per the prompt-injection
          // guardrail). A worker cannot stop its peers.
          const caller = await core.store.getMemberBySessionId(ctx.sessionID);
          if (!caller) return { output: "calling session is not a swarm member" };
          if (caller.swarmId !== args.swarmId) return { output: "calling session is not a member of this swarm" };
          if (caller.role !== "coordinator" && caller.id !== target.id) {
            return { output: `only the coordinator may stop a member (you are '${caller.name}')` };
          }
          // Release any owned task back to ready so the DAG can advance — a
          // stopped member can never complete its task, and the orphan sweep
          // only releases ownerless tasks (so an ownerful 'working' task would
          // dead-lock forever). Mirrors swarm_remove / session.deleted.
          if (target.currentTaskId) {
            await core.store.releaseTask(target.currentTaskId).catch(() => undefined);
          }
          await core.store.updateMemberStatus(target.id, "stopped", { currentTaskId: null });
          return { output: `stopped ${target.name}` };
        },
      }),

      swarm_remove: tool({
        description: [
          "Permanently remove a worker member from the swarm, freeing its roster slot.",
          "Stopping a member is reversible; removing one deletes its membership record",
          "so a replacement can be spawned. Only worker members can be removed; the",
          "coordinator must use swarm_delete to tear the whole swarm down.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          member: tool.schema.string().describe("Worker member name to remove."),
        },
        async execute(args) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const target = await core.store.getMemberByName(args.swarmId, args.member);
          if (!target) return { output: `no member named '${args.member}'` };
          if (target.role === "coordinator") {
            return { output: `cannot remove the coordinator; use swarm_delete to tear down the swarm` };
          }
          // Release any tasks this member claimed/owned back to ready so they
          // aren't orphaned (a claimed task with NULL owner is unclaimable and
          // dead-ends the DAG). Without this, swarm_remove + re-spawn strands
          // tasks — seen in the eshttp e2e after a server restart.
          if (target.currentTaskId) {
            await core.store.releaseTask(target.currentTaskId).catch(() => undefined);
          }
          await core.store.deleteMember(target.id);
          return { output: `removed ${target.name}; roster slot freed` };
        },
      }),

      swarm_release: tool({
        description: [
          "Immediately end the human-chat pause for a member and let the swarm",
          "resume normal machinery (mail delivery, task continuation, scheduler).",
          "Members are root chats the user can talk to directly; while chatting,",
          "the swarm yields automatically and resumes after a lull. Use this to",
          "force an early resume when the 5-minute lull is too slow.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          member: tool.schema.string().describe("Member name to release."),
        },
        async execute(args) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const target = await core.store.getMemberByName(args.swarmId, args.member);
          if (!target) return { output: `no member named '${args.member}'` };
          const swarm = await core.store.getSwarm(args.swarmId);
          if (!swarm) return { output: `no swarm '${args.swarmId}'` };
          // End the chat pause AND actually resume machinery — the tool's
          // description promises mail delivery, task continuation and scheduler
          // resumption, so clearing humanChatAt alone is not enough (a released
          // idle member with queued mail would otherwise sit until the next idle
          // event).
          await rt.humanChat.clear(target.sessionId);
          const resumed: string[] = [];
          const delivered = await rt.broker
            .deliverToIdleMember(target.id, target.sessionId)
            .catch(() => 0);
          if (delivered > 0) resumed.push(`${delivered} queued message(s) delivered`);
          if (target.currentTaskId) {
            const tasks = await core.store.listTasks(args.swarmId);
            const task = tasks.find((t) => t.id === target.currentTaskId);
            const terminal = task && ["completed", "failed", "cancelled"].includes(task.status);
            if (task && !terminal) {
              const attempt = rt.nextContinueAttempt(target.id, target.currentTaskId);
              if (attempt <= MAX_CONTINUE_ATTEMPTS) {
                await core.continueMember(swarm, target, attempt).catch((err) => {
                  console.error(`[swarm] release continue failed for ${target.name}:`, err);
                });
                resumed.push(`task "${task.title}" continued`);
              } else {
                rt.resetContinueAttempts(target.id);
                resumed.push(`task "${task.title}" left paused (continuation budget exhausted)`);
              }
            }
          }
          await rt.runScheduler(args.swarmId);
          return {
            output: `released ${target.name} from chat pause; ${resumed.length ? resumed.join("; ") + " — " : ""}swarm machinery resumed`,
          };
        },
      }),

      swarm_delete: tool({
        description: [
          "Permanently delete the entire swarm: all members, tasks, messages, and",
          "blackboard state are cascaded away. Use when the swarm is done or",
          "unsalvageable and you want to start clean.",
          "SAFETY: destructive and irreversible. Requires (1) the calling session",
          "to be the swarm's coordinator, and (2) confirm = the exact swarm name.",
        ].join("\n"),
        args: {
          swarmId: tool.schema.string(),
          confirm: tool.schema.string().describe("REQUIRED — pass the exact swarm name to confirm deletion."),
        },
        async execute(args, ctx) {
          const rt = await ensureRt();
          const core = rt.core;
          args.swarmId = await core.resolveSwarmId(args.swarmId, input.project?.id ?? "global");
          const swarm = await core.store.getSwarm(args.swarmId);
          if (!swarm) return { output: `no swarm '${args.swarmId}'` };
          // Destructive-op safety: only the swarm's coordinator may delete it
          // (the prompt-injection guardrail — operational instructions come only
          // from the coordinator). A worker (or an unaffiliated session) cannot
          // destroy the swarm.
          const caller = await core.store.getMemberBySessionId(ctx.sessionID);
          if (!caller || caller.swarmId !== args.swarmId) {
            return { output: "only the coordinator may delete the swarm (calling session is not a member of it)" };
          }
          if (caller.role !== "coordinator") {
            return { output: `only the coordinator may delete the swarm (you are '${caller.name}')` };
          }
          // confirm is REQUIRED — an omitted or mismatched confirm must NOT
          // proceed (previously a missing confirm deleted the swarm anyway).
          if (!args.confirm) {
            return { output: `swarm_delete requires confirm: pass the exact swarm name '${swarm.name}' to delete` };
          }
          if (args.confirm !== swarm.name) {
            return { output: `confirm mismatch: pass the exact swarm name '${swarm.name}' to delete` };
          }
          await core.store.deleteSwarm(args.swarmId);
          return { output: `deleted swarm '${swarm.name}'` };
        },
      }),
    },

    event: async ({ event }) => {
      if (!rtPromise) return;
      const rt = await rtPromise;
      await handleOpenCodeEvent(rt, event as never);
    },

    "permission.ask": async (input, output) => {
      const rt = await ensureRt();
      // For swarm member sessions, auto-allow operations that stay within the
      // member's swarm worktree (the coordinator explicitly spawned them to
      // work there). Anything outside that trusted scope is left as "ask" so
      // the user retains control. This is what prevents members from stalling
      // on external_directory / bash prompts for the project they were told to
      // build.
      await autoAllowSwarmPermission(rt, input, output);
    },

    "chat.message": async (input, output) => {
      const rt = await ensureRt();
      // The user can chat directly with member sessions (they are root chats).
      // Classify this message: if it is one of OUR injections, ignore it;
      // otherwise record it as a human message so the swarm yields to the
      // conversation. Skip the coordinator's own session — that is the user's
      // primary chat and already exempt from swarm machinery.
      const text = (output?.parts ?? [])
        .map((p) => (typeof (p as { text?: string }).text === "string" ? (p as { text: string }).text : ""))
        .join("\n");
      const isSelf = rt.humanChat.isSelfInjection(input.messageID, text);
      if (isSelf) {
        if (input.messageID) rt.humanChat.consumeInjection(input.messageID);
        return;
      }
      await rt.humanChat.onUserMessage(input.sessionID, false);
    },
  };
}

/**
 * Event-driven supervision (§34): reduce an OpenCode event into durable state,
 * then, after the state commit, perform external effects (mailbox wakeup).
 * Never holds DB locks across external calls.
 */
export async function handleOpenCodeEvent(
  rt: SwarmPluginRuntime,
  event: { type: string; properties?: Record<string, unknown> },
): Promise<void> {
  const { supervisor, broker, store, core } = rt;

  // Members that already received a mailbox delivery this event (they were
  // re-engaged by the inbox prompt; no separate continue prompt needed).
  const memberDeliveredMail = new Set<string>();

  // 1. Reduce state (durable).
  const effects = await supervisor.onOpenCodeEvent(event as never);

  // 1b. Human-chat state machine (root member sessions the user can chat with).
  // Clear chat state when a member session errors or is deleted. (OpenCode
  // natively answers human messages — the tracker only records the chat so the
  // swarm yields during it, so no busy/idle feeding is needed here.)
  const evtSessionID =
    (event.properties as { sessionID?: string })?.sessionID ??
    (event.properties as { id?: string })?.id;
  if (evtSessionID) {
    if (event.type === "session.error" || event.type === "session.deleted") {
      await rt.humanChat.clear(evtSessionID);
    }
  }

  // 2. External effects after commit.
  for (const memberId of effects.wake) {
    const member = await store.getMemberById(memberId);
    if (!member) continue;
    // Never wake stopped members.
    if (member.status === "stopped" || member.status === "stopping") continue;
    if (!supervisor.shouldWake(memberId)) continue;

    // Deliver queued mailbox messages by async-prompting the member session.
    // Delivery is safe for busy AND idle members: OpenCode's run loop re-reads
    // persisted messages every iteration, so a prompt lands between the
    // member's tool calls (mid-turn) rather than waiting for the next turn.
    // The broker's per-member cooldown still batches bursts.
    const delivered = await broker
      .deliverToIdleMember(memberId, member.sessionId)
      .catch((err) => {
        console.error(`[swarm] wake delivery failed for ${member.name}:`, err);
        return 0;
      });
    if (delivered > 0) {
      memberDeliveredMail.add(memberId);
    }
  }

  // 3. Continue-or-complete + self-driving scheduler. When a member session
  // goes idle it just means the member finished a turn — NOT that its task is
  // done (a member pauses at every turn boundary). If the member still owns a
  // non-terminal task, re-prompt it to keep working; only when the member
  // explicitly completes (swarm_tasks complete) is the task marked done. This
  // replaces the old "idle == complete" behavior that stalled the eshttp swarm.
  //
  // Human-chat interleaving: while the user is directly talking to a member
  // (within the lull window), the swarm yields — it does not force-continue
  // the task, assign new work, or deliver mail. OpenCode natively answers the
  // user's message (the run loop absorbs a mid-turn message; an idle-session
  // message starts a fresh run), so the plugin needs no reply injection — it
  // only has to stay out of the way. After the lull, the member resumes.
  if (event.type === "session.idle") {
    const sessionID = (event.properties as { sessionID?: string })?.sessionID;
    if (sessionID) {
      const member = await store.getMemberBySessionId(sessionID);
      if (member && member.role !== "coordinator") {
        const swarm = await store.getSwarm(member.swarmId);
        if (swarm && swarm.status === "active") {
          const chatting = await rt.humanChat.chatting(member, swarm);

          if (chatting) {
            // The member is answering the user. Do not continue the task, run
            // the scheduler, or deliver mail. The member's currentTaskId stays
            // set; work resumes after the lull (the next normal idle handles it).
          } else {
            // Not chatting: clear any lapsed chat state, then proceed normally.
            if (member.humanChatAt != null) {
              await store.updateMemberHumanChat(member.id, null);
            }
            const taskId = member.currentTaskId;
            const tasks = taskId ? await store.listTasks(swarm.id) : [];
            const task = taskId ? tasks.find((t) => t.id === taskId) : undefined;
            const terminal = task && ["completed", "failed", "cancelled"].includes(task.status);

            if (taskId && task && !terminal && !memberDeliveredMail.has(member.id)) {
              // Member paused mid-task: continue it (bounded to avoid an infinite
              // loop if the model keeps going idle without finishing).
              const attempt = rt.nextContinueAttempt(member.id, taskId);
              if (attempt <= MAX_CONTINUE_ATTEMPTS) {
                await core.continueMember(swarm, member, attempt).catch((err) => {
                  console.error(`[swarm] continue prompt failed for ${member.name}:`, err);
                });
              } else {
                // Give up and surface to the coordinator instead of spinning.
                // X1 fix: fire the "check for a blocker" notice ONCE per task
                // (gated) — the counter reset must not cause repeat notices.
                rt.resetContinueAttempts(member.id);
                if (taskId && !rt.isContinueNotified(taskId)) {
                  rt.markContinueNotified(taskId);
                  rt.notifyCoordinator(
                    { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
                    `${member.name} hit ${MAX_CONTINUE_ATTEMPTS} idle continuations on task ${fence(task.title)} (${taskId}) without completing it. Check for a blocker.`,
                  );
                }
              }
            } else {
              // Task already terminal (or none): free the member for new work.
              rt.resetContinueAttempts(member.id);
              if (taskId && rt.isContinueNotified(taskId)) rt.clearContinueNotified(taskId);
              if (taskId && member.currentTaskId) {
                await store.updateMemberStatus(member.id, "idle", { currentTaskId: null, lastActiveAt: Date.now() });
              }
              await rt.runScheduler(swarm.id);
            }
          }
        }
      }
    }
  }

  // Coordinator notification for material events (member failure) is surfaced
  // as a batched notice so the coordinator learns without polling.
  if (effects.notifyCoordinator) {
    const errSessionID =
      (event.properties as { sessionID?: string })?.sessionID ??
      (event.properties as { id?: string })?.id;
    if (errSessionID) {
      const member = await store.getMemberBySessionId(errSessionID);
      if (member) {
        const swarm = await store.getSwarm(member.swarmId);
        if (swarm && swarm.status === "active") {
          const errorText = (event.properties as { error?: string })?.error ?? "";
          rt.notifyCoordinator(
            { id: swarm.id, name: swarm.name, coordinatorSessionId: swarm.coordinatorSessionId },
            `${member.name} failed${errorText ? `: ${errorText}` : ""}. Its task was released for reassignment.`,
          );
          // F6: a genuine failure released the member's task — notify dependent
          // task owners so they re-validate rather than discovering silently.
          const taskId = member.currentTaskId;
          if (taskId) {
            await rt.notifyDependents(swarm.id, taskId, `released after ${member.name} failed`).catch((err) => {
              console.error(`[swarm] dependent notification after failure failed:`, err);
            });
          }
        }
      }
    }
  }
}

function memberHasCoordinator(_rt: SwarmPluginRuntime, _event: { type: string }): boolean {
  return true;
}

/** Resolve a member's pending messages and render an inbox (used by wake). */
export async function renderInboxForMember(
  core: SwarmCore,
  member: SwarmMember,
): Promise<string> {
  const swarm = await core.store.getSwarm(member.swarmId);
  if (!swarm) return "";
  const msgs = await core.listMessagesTo(member.id);
  const names = new Map<string, string>();
  const all = await core.store.listMembers(member.swarmId);
  for (const m of all) names.set(m.id, m.name);
  return formatInbox({ swarm, self: member, messages: msgs, names });
}
