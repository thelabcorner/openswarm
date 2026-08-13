import type { AgentRuntime } from "../runtime/runtime-types.js";
import type { Swarm, SwarmMember, SwarmTask } from "../core/types.js";
import type { SwarmStore } from "../storage/store.js";
import { Recovery } from "../supervisor/recovery.js";

/**
 * Operator ↔ coordinator swarm lifecycle: revive stalled swarms, re-task them,
 * and answer the "keep the existing agents or spin fresh ones?" decision.
 *
 * A structural host (the plugin runtime) is injected rather than imported so
 * this module never creates a circular dependency with plugin.ts. The host
 * must expose: store, runtimeAdapter (AgentRuntime), respawnMember,
 * runScheduler, resolveSpawnModel + recordUsedModel (model selection chain),
 * and core.{createTask, insertTaskDependency, spawnMember}.
 */

export type ModelRef = { providerID: string; modelID: string };

export interface ReviveHost {
  store: SwarmStore;
  runtimeAdapter: AgentRuntime;
  respawnMember(member: SwarmMember): Promise<string>;
  runScheduler(swarmId: string, opts?: { skipAssignmentFor?: ReadonlySet<string> }): Promise<void>;
  resolveSpawnModel(swarmId: string, requested?: { providerID?: string; modelID?: string }): Promise<{ model?: ModelRef; source: string; note?: string }>;
  recordUsedModel(swarmId: string, model: ModelRef): Promise<void>;
  core: {
    createTask(input: { swarmId: string; id?: string; title: string; description?: string; createdByMemberId: string; priority?: number }): Promise<SwarmTask>;
    spawnMember(input: { swarmId: string; name: string; role: string; agent?: string; model?: ModelRef; taskId?: string; prompt?: string; workspace?: string }): Promise<SwarmMember>;
    sendMessage(input: {
      swarmId: string; fromMemberId?: string; fromSessionId?: string; to: string | "*";
      kind: string; message: string; taskId?: string; correlationId?: string; responseTo?: string;
      priority?: string; refs?: string[]; noreply?: boolean; force?: boolean;
    }): Promise<unknown>;
  };
}

export interface HealthMember {
  name: string;
  role: string;
  status: SwarmMember["status"];
  model: string;
  currentTaskTitle?: string;
  humanChatPaused: boolean;
  pendingPermissions: number;
  needsRespawn: boolean;
}

export interface SwarmHealth {
  swarmId: string;
  swarmName: string;
  projectId: string;
  status: Swarm["status"];
  members: HealthMember[];
  tasks: { ready: number; working: number; stuck: number; completed: number; cancelled: number; failed: number; other: number };
  stuckTasks: Array<{ id: string; title: string; status: string; owner: string; leaseExpiresAt?: number; reservedFor?: string }>;
  pendingPermissions: Array<{ memberName: string; type: string; pattern?: string }>;
  staleScheduled: number;
  verdict: "healthy" | "revive";
  recommended: string;
}

export interface ReviveReport {
  swarmId: string;
  strategy: "keep";
  swarmStatus: Swarm["status"];
  recoveredMembers: Array<{ name: string; action: string; detail?: string }>;
  includeStoppedRevived: number;
  releasedStuckTasks: number;
  staleScheduledReverted: number;
  schedulerKicked: boolean;
}

export interface RetaskReport {
  swarmId: string;
  strategy: "repoint" | "fresh";
  cancelledTasks: number;
  seededTasks: number;
  stoppedOldMembers: number;
  spawnedMembers: Array<{ name: string; model?: ModelRef; modelSource: string }>;
  notifiedMembers: number;
  schedulerKicked: boolean;
  note?: string;
}

export interface SwarmSummary {
  id: string;
  name: string;
  projectId: string;
  status: Swarm["status"];
  memberCount: number;
  workerCount: number;
  taskCounts: { ready: number; working: number; completed: number };
  hasStopped: boolean;
  hasStuckTasks: boolean;
  hasPendingPermission: boolean;
  hasChatPausedMember: boolean;
}

/** Terminal task statuses that a re-task must NOT cancel. */
const NON_TERMINAL: Array<SwarmTask["status"]> = [
  "pending", "blocked", "ready", "claimed", "working", "review_pending", "changes_requested",
];

export class ReviveEngine {
  constructor(private host: ReviveHost) {}

  /** Read-only health diagnostics + a synthesized verdict with the exact next
   * invocation to run. Never mutates state. */
  async health(swarmId: string): Promise<SwarmHealth> {
    const { store } = this.host;
    const swarm = await store.getSwarm(swarmId);
    if (!swarm) throw new Error(`no swarm '${swarmId}'`);
    const [members, tasks, pending, msgs] = await Promise.all([
      store.listMembers(swarmId),
      store.listTasks(swarmId),
      store.listPendingPermissions(swarmId),
      store.listMessagesBySwarm(swarmId, 200),
    ]);
    const now = Date.now();
    const taskTitle = new Map(tasks.map((t) => [t.id, t.title]));
    const lullMs = swarm.policies.humanChatLullMs ?? 300_000;
    const pendingByMember = new Map<string, Array<{ type: string; pattern?: string }>>();
    for (const p of pending) {
      const list = pendingByMember.get(p.memberId) ?? [];
      list.push({ type: p.type, pattern: p.pattern });
      pendingByMember.set(p.memberId, list);
    }
    const memberName = new Map(members.map((m) => [m.id, m.name]));

    const healthMembers: HealthMember[] = [];
    for (const m of members) {
      const perms = pendingByMember.get(m.id) ?? [];
      const paused = m.humanChatAt ? now - m.humanChatAt < lullMs : false;
      healthMembers.push({
        name: m.name,
        role: m.role,
        status: m.status,
        model: m.model ? `${m.model.providerID}/${m.model.modelID}` : "default",
        currentTaskTitle: m.currentTaskId ? taskTitle.get(m.currentTaskId) : undefined,
        humanChatPaused: paused,
        pendingPermissions: perms.length,
        needsRespawn: m.status === "stopped" || m.status === "failed" || m.status === "interrupted",
      });
    }

    const statusCount = (s: string) => tasks.filter((t) => t.status === s).length;
    const stuck = tasks.filter(
      (t) => (t.status === "claimed" || t.status === "working") && t.leaseExpiresAt !== undefined && t.leaseExpiresAt < now,
    );
    const staleScheduled = msgs.filter((m) => m.deliveryState === "scheduled").length;

    const problemSignals: string[] = [];
    if (swarm.status !== "active") problemSignals.push(`swarm status is '${swarm.status}' (not active)`);
    const dead = healthMembers.filter((m) => m.role !== "coordinator" && m.needsRespawn);
    if (dead.length) problemSignals.push(`${dead.length} member(s) need respawn (${dead.map((m) => m.name).join(", ")})`);
    if (stuck.length) problemSignals.push(`${stuck.length} task(s) have an expired lease (${stuck.map((t) => t.id).join(", ")})`);
    if (pending.length) problemSignals.push(`${pending.length} permission prompt(s) unanswered (members are blocked)`);
    if (staleScheduled > 0) problemSignals.push(`${staleScheduled} scheduled message(s) never confirmed delivered`);

    const verdict = problemSignals.length === 0 ? "healthy" : "revive";
    const recommended =
      verdict === "healthy"
        ? `swarm '${swarm.name}' is healthy — no action needed`
        : `swarm_revive(swarmId: '${swarmId}', action: 'revive', strategy: 'keep'${dead.some((m) => m.status === "stopped") ? ", includeStopped: true" : ""}) to reconcile`;

    return {
      swarmId,
      swarmName: swarm.name,
      projectId: swarm.projectId,
      status: swarm.status,
      members: healthMembers,
      tasks: {
        ready: statusCount("ready"), working: statusCount("working"), stuck: stuck.length,
        completed: statusCount("completed"), cancelled: statusCount("cancelled"), failed: statusCount("failed"),
        other: tasks.length - statusCount("ready") - statusCount("working") - statusCount("completed") - statusCount("cancelled") - statusCount("failed"),
      },
      stuckTasks: stuck.map((t) => ({
        id: t.id, title: t.title, status: t.status,
        owner: t.ownerMemberId ? (memberName.get(t.ownerMemberId) ?? t.ownerMemberId) : "none",
        leaseExpiresAt: t.leaseExpiresAt, reservedFor: t.reservedFor,
      })),
      pendingPermissions: pending.map((p) => ({
        memberName: memberName.get(p.memberId) ?? p.memberId,
        type: p.type, pattern: p.pattern,
      })),
      staleScheduled,
      verdict,
      recommended,
    };
  }

  /** Revive strategy "keep": reconcile the EXISTING swarm — respawn dead
   * sessions (optionally including deliberately-stopped members), release
   * expired-lease tasks back to ready, revert stale deliveries, and re-kick
   * the scheduler. */
  async reviveKeep(swarmId: string, opts: { includeStopped?: boolean } = {}): Promise<ReviveReport> {
    const { store } = this.host;
    const swarm = await store.getSwarm(swarmId);
    if (!swarm) throw new Error(`no swarm '${swarmId}'`);

    const recovered: Array<{ name: string; action: string; detail?: string }> = [];
    let includeStoppedRevived = 0;

    // 1. Flip the swarm back to active (a completed/failed swarm is revivable).
    const priorStatus = swarm.status;
    if (priorStatus !== "active") {
      await store.updateSwarmStatus(swarmId, "active");
      recovered.push({ name: "(swarm)", action: "status", detail: `status '${priorStatus}' -> active` });
    }

    // 2. Standard reconciliation: absent sessions get respawned, stale working
    //    flags corrected, stale `scheduled` deliveries reverted to queued.
    const recovery = new Recovery(store, this.host.runtimeAdapter, (m) => this.host.respawnMember(m));
    const result = await recovery.reconcileSwarm(swarmId);
    for (const a of result.actions) {
      if (a.action !== "unchanged") {
        recovered.push({ name: a.memberName, action: a.action, detail: a.detail });
      }
    }

    // 3. Optionally revive deliberately-stopped/failed workers too.
    if (opts.includeStopped) {
      const members = await store.listMembers(swarmId);
      for (const m of members) {
        if (m.role === "coordinator") continue;
        if (m.status !== "stopped" && m.status !== "stopping" && m.status !== "failed") continue;
        try {
          const newSessionId = await this.host.respawnMember(m);
          await store.assignMemberSession(m.id, newSessionId);
          await store.updateMemberStatus(m.id, "working", { currentTaskId: m.currentTaskId ?? null, lastActiveAt: Date.now() });
          includeStoppedRevived += 1;
          recovered.push({ name: m.name, action: "respawned", detail: `was ${m.status}; re-spawned as ${newSessionId}` });
        } catch (err) {
          recovered.push({ name: m.name, action: "respawn-failed", detail: (err as Error).message });
        }
      }
    }

    // 4. Release tasks whose claim lease expired (they stalled mid-flight).
    let releasedStuckTasks = 0;
    const expired = await store.listExpiredLeaseTasks(swarmId, Date.now());
    for (const t of expired) {
      const ok = await store.releaseTask(t.id, { countAsRetry: false });
      if (ok) releasedStuckTasks += 1;
    }

    // 5. Re-kick the scheduler so ready tasks get assigned to the revived crew.
    await this.host.runScheduler(swarmId);

    return {
      swarmId,
      strategy: "keep",
      swarmStatus: "active",
      recoveredMembers: recovered,
      includeStoppedRevived,
      releasedStuckTasks,
      staleScheduledReverted: result.staleScheduledReverted,
      schedulerKicked: true,
    };
  }

  /** Re-task the swarm: "repoint" keeps the existing agents (new mission),
   * "fresh" stops them and spawns new ones (new agents). */
  async retask(
    swarmId: string,
    strategy: "repoint" | "fresh",
    opts: { tasks?: Array<{ id?: string; title: string; description?: string; priority?: number; dependsOn?: string[] }>; members?: Array<{ name: string; role: string; agent?: string; model?: ModelRef; taskId?: string; prompt?: string; workspace?: string }>; prompt?: string },
  ): Promise<RetaskReport> {
    const { store, core } = this.host;
    const swarm = await store.getSwarm(swarmId);
    if (!swarm) throw new Error(`no swarm '${swarmId}'`);
    // Multi-own (migration v12): resolve THE coordinator of THIS swarm via the
    // (session, swarm) pair — first-match would pick another swarm's
    // coordinator row when the session co-owns N swarms.
    const coordinator = await store.getMemberBySessionAndSwarm(swarm.coordinatorSessionId, swarmId);
    if (!coordinator) throw new Error(`swarm '${swarm.name}' has no coordinator member`);

    let cancelledTasks = 0;
    let stoppedOldMembers = 0;
    let notifiedMembers = 0;
    const spawnedMembers: RetaskReport["spawnedMembers"] = [];

    if (strategy === "fresh") {
      // Stop all old workers (reversible — they stay as history until removed).
      const members = await store.listMembers(swarmId);
      for (const m of members) {
        if (m.role === "coordinator") continue;
        if (m.status === "stopped" || m.status === "stopping" || m.status === "failed") continue;
        if (m.currentTaskId) {
          await store.releaseTask(m.currentTaskId, { countAsRetry: false }).catch(() => undefined);
        }
        await store.updateMemberStatus(m.id, "stopped", { currentTaskId: null, lastActiveAt: Date.now() });
        stoppedOldMembers += 1;
      }
    }

    // Cancel the old mission's in-flight tasks (completed history stays).
    const tasks = await store.listTasks(swarmId);
    for (const t of tasks) {
      if (!NON_TERMINAL.includes(t.status)) continue;
      const ok = await store.updateTaskStatus(t.id, "cancelled");
      if (ok) {
        cancelledTasks += 1;
        await store.setTaskReservation(t.id, null).catch(() => undefined);
      }
    }

    // Seed the new task DAG (idempotent by id/title).
    let seededTasks = 0;
    const existing = await store.listTasks(swarmId);
    const byId = new Map(existing.map((t) => [t.id, t.id]));
    const byTitle = new Map(existing.map((t) => [t.title, t.id]));
    const createdId = new Map<string, string>();
    for (const t of opts.tasks ?? []) {
      const already = (t.id ? byId.get(t.id) : undefined) ?? byTitle.get(t.title);
      if (already) {
        if (t.id) createdId.set(t.id, already);
        continue;
      }
      const task = await core.createTask({
        swarmId, id: t.id, title: t.title, description: t.description,
        createdByMemberId: coordinator.id, priority: t.priority,
      });
      if (t.id) createdId.set(t.id, task.id);
      else createdId.set(task.title, task.id);
      seededTasks += 1;
    }
    // Dependencies may reference stable ids or titles.
    const existingById = new Map(existing.map((t) => [t.id, t.id]));
    const existingByTitle = new Map(existing.map((t) => [t.title, t.id]));
    for (const t of opts.tasks ?? []) {
      for (const dep of t.dependsOn ?? []) {
        const taskId = createdId.get(t.id ?? "") ?? createdId.get(t.title) ?? t.id;
        const depId = createdId.get(dep) ?? existingById.get(dep) ?? existingByTitle.get(dep);
        if (!taskId || !depId || taskId === depId) continue;
        await store.insertTaskDependency(taskId, depId);
      }
    }

    // Fresh: spawn the NEW agents (model resolution chain, like swarm_spawn).
    if (strategy === "fresh") {
      for (const m of opts.members ?? []) {
        const { model, source, note } = await this.host.resolveSpawnModel(swarmId, m.model);
        if (model) await this.host.recordUsedModel(swarmId, model);
        await core.spawnMember({
          swarmId, name: m.name, role: m.role, agent: m.agent, model,
          taskId: m.taskId, prompt: m.prompt, workspace: m.workspace as never,
        });
        spawnedMembers.push({ name: m.name, model, modelSource: source });
        void note;
      }
    }

    // Tell the (kept) crew about the new mission — the rich re-task touch.
    if (strategy === "repoint") {
      const members = await store.listMembers(swarmId);
      const mission = (opts.tasks ?? []).map((t) => `'${t.title}'`).join(", ") || "see swarm_tasks";
      const note = opts.prompt ?? `Swarm re-tasked. New mission: ${mission}. Check swarm_tasks for your assignment.`;
      for (const m of members) {
        if (m.role === "coordinator") continue;
        if (m.status === "stopped" || m.status === "stopping" || m.status === "failed") continue;
        try {
          await core.sendMessage({
            swarmId, fromMemberId: coordinator.id, to: m.name,
            kind: "finding", message: note, noreply: true,
          });
          notifiedMembers += 1;
        } catch { /* best-effort per member */ }
      }
    }

    // 6. Kick the scheduler so the new DAG starts moving.
    await this.host.runScheduler(swarmId);

    return {
      swarmId,
      strategy,
      cancelledTasks,
      seededTasks,
      stoppedOldMembers,
      spawnedMembers,
      notifiedMembers,
      schedulerKicked: true,
      note: strategy === "fresh"
        ? "old agents stopped (reversible via swarm_revive includeStopped:true, or remove them with swarm_remove); new agents spawned"
        : "existing agents kept; in-flight tasks cancelled; new mission seeded",
    };
  }

  /** All swarms in this store with one-line staleness flags — the operator's
   * "what do I have" surface. */
  async listSwarms(): Promise<SwarmSummary[]> {
    const { store } = this.host;
    const swarmIds = await store.listAllMemberSwarmIds();
    const out: SwarmSummary[] = [];
    const now = Date.now();
    for (const id of swarmIds) {
      const swarm = await store.getSwarm(id);
      if (!swarm) continue;
      const [members, tasks, pending] = await Promise.all([
        store.listMembers(id),
        store.listTasks(id),
        store.listPendingPermissions(id),
      ]);
      const lullMs = swarm.policies.humanChatLullMs ?? 300_000;
      const taskCounts = { ready: 0, working: 0, completed: 0 };
      for (const t of tasks) {
        if (t.status === "ready") taskCounts.ready += 1;
        else if (t.status === "claimed" || t.status === "working") taskCounts.working += 1;
        else if (t.status === "completed") taskCounts.completed += 1;
      }
      const stuckCount = tasks.filter(
        (t) => (t.status === "claimed" || t.status === "working") && t.leaseExpiresAt !== undefined && t.leaseExpiresAt < now,
      ).length;
      out.push({
        id,
        name: swarm.name,
        projectId: swarm.projectId,
        status: swarm.status,
        memberCount: members.length,
        workerCount: members.filter((m) => m.role !== "coordinator").length,
        taskCounts,
        hasStopped: members.some((m) => m.status === "stopped" || m.status === "failed"),
        hasStuckTasks: stuckCount > 0,
        hasPendingPermission: pending.length > 0,
        hasChatPausedMember: members.some((m) => m.humanChatAt && now - m.humanChatAt < lullMs),
      });
    }
    return out;
  }
}
