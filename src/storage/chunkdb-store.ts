import { createHash } from "node:crypto";
import { ChunkDB } from "../../vendor/chunkdb/src/index.js";
import type {
  ArtifactAnnotation,
  Belief,
  BeliefStatus,
  BlackboardEntry,
  ContractDefinition,
  Deliverable,
  PathClaim,
  PendingPermission,
  Swarm,
  SwarmEvent,
  SwarmMember,
  SwarmMessage,
  SwarmTask,
  TopicSubscription,
} from "../core/types.js";
import type {
  NewArtifactAnnotation,
  NewBelief,
  NewBlackboardEntry,
  NewContractDefinition,
  NewDeliverable,
  NewMessage,
  NewPathClaim,
  NewSwarm,
  NewSwarmEvent,
  NewSwarmMember,
  NewTask,
  TaskDependency,
} from "./models.js";
import type { SwarmStore, SwarmStoreTx } from "./store.js";

/**
 * ChunkDbStore — a full SwarmStore implementation backed by the vendored
 * chunkDB (vendor/chunkdb, a bun:sqlite-backed compressed KV store).
 *
 * LAYOUT
 * ------
 * Namespaces: 'swarm' | 'member' | 'task' | 'dependency' | 'message' |
 * 'blackboard' | 'belief' | 'annotation' | 'claim' | 'subscription' |
 * 'permission' | 'event' | 'deliverable' | 'contract' | 'meta'.
 *
 * Entity key scheme: swarmId + SEP ("\u001F") + entityKey where: (SEP is NUL-free — node:sqlite, the Desktop host driver, truncates text bindings at NUL, so "\0" separators would collide keys)
 *   swarm        = id            (the swarm id IS the key — no prefix needed)
 *   member       = id
 *   task         = id
 *   dependency   = taskId + '::' + depId
 *   message      = id
 *   blackboard   = key
 *   belief       = factHash      (dedupe by (swarmId, factHash) — re-insert
 *                                 REINFORCES, mirroring sqlite)
 *   annotation   = path + SEP + type   (replace semantics, id preserved)
 *   claim        = id
 *   subscription = id
 *   permission   = id
 *   event        = pad12(seq)    (per-swarm counter persisted under meta
 *                                 'evtseq:'+swarmId; key DESC = newest first)
 *   deliverable  = id
 *   contract     = keyPattern
 *
 * Scans use chunkDB.keys(namespace, prefix) (directory UNION delta) + getMany
 * to decode, then filter/sort in JS. Global lookups (by member id / session,
 * pending mail, message by id) scan the whole namespace — fine at swarm scale.
 *
 * SEMANTICS
 * ---------
 * Mirrors src/storage/sqlite-store.ts exactly: return shapes, defaults
 * (listDeliverables limit 50, listEvents limit 100, listAnnotations activeOnly
 * true, listBeliefs activeOnly true), orderings, CAS guards, and belief/
 * annotation dedupe rules. chunkDB has no transaction API, so `transaction()`
 * is a serialization boundary: the body runs in one slot of the promise-chain
 * queue (never interleaved with other ops) and the Tx facade IS this store —
 * methods invoked inside the slot execute directly instead of re-queueing
 * (which would deadlock).
 */

const SEP = "\u001F"; // unit separator — NUL-free (node:sqlite truncates text bindings at NUL)

const NS = {
  swarm: "swarm",
  member: "member",
  task: "task",
  dependency: "dependency",
  message: "message",
  blackboard: "blackboard",
  belief: "belief",
  annotation: "annotation",
  claim: "claim",
  subscription: "subscription",
  permission: "permission",
  event: "event",
  deliverable: "deliverable",
  contract: "contract",
  meta: "meta",
} as const;

/** Every per-swarm entity namespace (deleteSwarm cascades across these). */
const ENTITY_NAMESPACES: string[] = [
  NS.member, NS.task, NS.dependency, NS.message, NS.blackboard, NS.belief,
  NS.annotation, NS.claim, NS.subscription, NS.permission, NS.event,
  NS.deliverable, NS.contract,
];

const keyOf = (swarmId: string, entityKey: string): string => swarmId + SEP + entityKey;
const evtSeqKey = (swarmId: string): string => `evtseq:${swarmId}`;
const pad12 = (n: number): string => String(n).padStart(12, "0");

const PRIORITY_RANK: Record<string, number> = { urgent: 0, high: 1, normal: 2 };

/** Parse a belief's evidence_refs JSON array into a ref-id set (resonance). */
function evidenceRefSet(refs: string | undefined): Set<string> {
  if (!refs) return new Set();
  try {
    const parsed = JSON.parse(refs);
    return new Set(Array.isArray(parsed) ? parsed.map(String) : []);
  } catch {
    return new Set();
  }
}

function beliefEvidenceDisjoint(a: Belief, b: Belief): boolean {
  const sa = evidenceRefSet(a.evidenceRefs);
  const sb = evidenceRefSet(b.evidenceRefs);
  if (sa.size === 0 || sb.size === 0) return false;
  for (const id of sa) {
    if (sb.has(id)) return false;
  }
  return true;
}

async function sha1Hex(s: string): Promise<string> {
  return createHash("sha1").update(s, "utf8").digest("hex");
}

export class ChunkDbStore implements SwarmStore {
  private db!: ChunkDB;
  private txnQueue: Promise<void> = Promise.resolve();
  /** >0 while a transaction body is running: store methods called from inside
   * it execute directly instead of re-entering the queue (the transaction
   * already owns the queue slot — re-queueing would self-deadlock). */
  private txDepth = 0;
  private closed = false;

  constructor(private path: string) {}

  /** Open the backing chunkDB (idempotent; the constructor is synchronous). */
  async ready(): Promise<void> {
    if (this.closed) throw new Error("store is closed");
    if (this.db) return;
    this.db = new ChunkDB(this.path);
    await this.db.ready();
  }

  private serialized<T>(op: () => Promise<T>): Promise<T> {
    if (this.txDepth > 0) return Promise.resolve().then(op);
    const run = this.txnQueue.then(op);
    this.txnQueue = run.then(() => undefined, () => undefined);
    return run;
  }

  transaction<T>(fn: (tx: SwarmStoreTx) => Promise<T>): Promise<T> {
    return this.serialized(async () => {
      this.txDepth++;
      try {
        return await fn(this as unknown as SwarmStoreTx);
      } finally {
        this.txDepth--;
      }
    });
  }

  async close(): Promise<void> {
    await this.txnQueue.catch(() => undefined);
    if (this.db && !this.closed) {
      this.db.close();
    }
    this.closed = true;
  }

  // ==== internal scan helpers (all synchronous over chunkDB) ====

  /** Keys in a namespace whose full key starts with the swarm prefix. */
  private keysFor(ns: string, swarmId: string): string[] {
    return this.db.keys(ns, keyOf(swarmId, ""));
  }

  /** Decode every live value under a swarm prefix (tombstoned keys skipped —
   * getMany is the liveness authority). */
  private scan<T>(ns: string, swarmId: string): T[] {
    const keys = this.keysFor(ns, swarmId);
    return this.decodeKeys<T>(ns, keys);
  }

  /** Decode every live value in a namespace (global scan). */
  private scanAll<T>(ns: string): T[] {
    const keys = this.db.keys(ns);
    return this.decodeKeys<T>(ns, keys);
  }

  private decodeKeys<T>(ns: string, keys: string[]): T[] {
    if (keys.length === 0) return [];
    const got = this.db.getMany<T>(ns, keys);
    const out: T[] = [];
    for (const k of keys) {
      const v = got.get(k);
      if (v !== undefined) out.push(v);
    }
    return out;
  }

  private getOne<T>(ns: string, swarmId: string, entityKey: string): T | undefined {
    return this.db.get<T>(ns, keyOf(swarmId, entityKey));
  }

  private putOne(ns: string, swarmId: string, entityKey: string, value: unknown): void {
    this.db.put(ns, keyOf(swarmId, entityKey), value);
  }

  private delOne(ns: string, swarmId: string, entityKey: string): void {
    this.db.delete(ns, keyOf(swarmId, entityKey));
  }

  private findGlobal<T extends { id: string }>(ns: string, id: string): T | undefined {
    return this.scanAll<T>(ns).find((v) => v.id === id);
  }

  private evtNextSeq(swarmId: string): number {
    const cur = this.db.get<number>(NS.meta, evtSeqKey(swarmId)) ?? 0;
    const seq = cur + 1;
    this.db.put(NS.meta, evtSeqKey(swarmId), seq);
    return seq;
  }

  private evtPinnedSeq(swarmId: string, pinned: number): void {
    const cur = this.db.get<number>(NS.meta, evtSeqKey(swarmId)) ?? 0;
    if (pinned > cur) this.db.put(NS.meta, evtSeqKey(swarmId), pinned);
  }

  // ==== swarm ====

  async insertSwarm(s: NewSwarm): Promise<Swarm> {
    return this.serialized(async () => {
      const dup = this.scanAll<Swarm>(NS.swarm).find(
        (x) => x.projectId === s.projectId && x.name === s.name,
      );
      if (dup) {
        throw new Error(`UNIQUE constraint failed: swarm (project_id, name)`);
      }
      this.db.put(NS.swarm, s.id, s);
      return s as Swarm;
    });
  }

  async getSwarm(id: string): Promise<Swarm | undefined> {
    return this.serialized(async () => this.db.get<Swarm>(NS.swarm, id));
  }

  async getSwarmBySession(sessionID: string): Promise<Swarm | undefined> {
    return this.serialized(async () => {
      const member = this.scanAll<SwarmMember>(NS.member).find((m) => m.sessionId === sessionID);
      if (!member) return undefined;
      return this.db.get<Swarm>(NS.swarm, member.swarmId);
    });
  }

  async getSwarmByName(projectId: string, name: string): Promise<Swarm | undefined> {
    return this.serialized(async () =>
      this.scanAll<Swarm>(NS.swarm).find((x) => x.projectId === projectId && x.name === name),
    );
  }

  async updateSwarmDirectory(swarmId: string, directory: string): Promise<void> {
    return this.serialized(async () => {
      const swarm = this.db.get<Swarm>(NS.swarm, swarmId);
      if (!swarm) return;
      swarm.directory = directory;
      swarm.updatedAt = Date.now();
      this.db.put(NS.swarm, swarmId, swarm);
    });
  }

  async updateSwarmCoordinator(swarmId: string, coordinatorSessionId: string): Promise<void> {
    return this.serialized(async () => {
      const swarm = this.db.get<Swarm>(NS.swarm, swarmId);
      if (!swarm) return;
      swarm.coordinatorSessionId = coordinatorSessionId;
      swarm.updatedAt = Date.now();
      this.db.put(NS.swarm, swarmId, swarm);
    });
  }

  async updateSwarmStatus(swarmId: string, status: Swarm["status"]): Promise<void> {
    return this.serialized(async () => {
      const swarm = this.db.get<Swarm>(NS.swarm, swarmId);
      if (!swarm) return;
      swarm.status = status;
      swarm.updatedAt = Date.now();
      this.db.put(NS.swarm, swarmId, swarm);
    });
  }

  async deleteSwarm(swarmId: string): Promise<void> {
    return this.serialized(async () => {
      for (const ns of ENTITY_NAMESPACES) {
        for (const key of this.keysFor(ns, swarmId)) {
          this.db.delete(ns, key);
        }
      }
      this.db.delete(NS.swarm, swarmId);
      this.db.delete(NS.meta, evtSeqKey(swarmId));
    });
  }

  async listAllMemberSwarmIds(): Promise<string[]> {
    return this.serialized(async () => {
      const ids = new Set<string>();
      for (const m of this.scanAll<SwarmMember>(NS.member)) ids.add(m.swarmId);
      return [...ids];
    });
  }

  // ==== members ====

  async insertMember(m: NewSwarmMember): Promise<SwarmMember> {
    return this.serialized(async () => {
      // session_id is globally unique; (swarm_id, name) is unique per swarm.
      const bySession = this.scanAll<SwarmMember>(NS.member).find((x) => x.sessionId === m.sessionId);
      if (bySession && bySession.id !== m.id) {
        throw new Error(`UNIQUE constraint failed: swarm_member.session_id`);
      }
      const byName = this.scan<SwarmMember>(NS.member, m.swarmId).find((x) => x.name === m.name);
      if (byName && byName.id !== m.id) {
        throw new Error(`UNIQUE constraint failed: swarm_member (swarm_id, name)`);
      }
      this.putOne(NS.member, m.swarmId, m.id, m);
      return m as SwarmMember;
    });
  }

  async listMembers(swarmId: string): Promise<SwarmMember[]> {
    return this.serialized(async () =>
      this.scan<SwarmMember>(NS.member, swarmId).sort((a, b) => a.createdAt - b.createdAt),
    );
  }

  async getMemberById(memberId: string): Promise<SwarmMember | undefined> {
    return this.serialized(async () => this.findGlobal<SwarmMember>(NS.member, memberId));
  }

  async getMemberBySessionId(sessionID: string): Promise<SwarmMember | undefined> {
    return this.serialized(async () =>
      this.scanAll<SwarmMember>(NS.member).find((m) => m.sessionId === sessionID),
    );
  }

  async getMemberByName(swarmId: string, name: string): Promise<SwarmMember | undefined> {
    return this.serialized(async () =>
      this.scan<SwarmMember>(NS.member, swarmId).find((m) => m.name === name),
    );
  }

  async assignMemberSession(memberId: string, sessionId: string): Promise<void> {
    return this.serialized(async () => {
      const member = this.findGlobal<SwarmMember>(NS.member, memberId);
      if (!member) return;
      member.sessionId = sessionId;
      member.updatedAt = Date.now();
      this.putOne(NS.member, member.swarmId, member.id, member);
    });
  }

  async deleteMember(memberId: string): Promise<void> {
    return this.serialized(async () => {
      // Mirror sqlite: cascade authored rows (blackboard/annotation/belief FKs
      // have no ON DELETE action) so removal always succeeds.
      const member = this.findGlobal<SwarmMember>(NS.member, memberId);
      if (!member) return;
      for (const entry of this.scan<BlackboardEntry>(NS.blackboard, member.swarmId)) {
        if (entry.authorMemberId === memberId) this.delOne(NS.blackboard, member.swarmId, entry.key);
      }
      for (const ann of this.scan<ArtifactAnnotation>(NS.annotation, member.swarmId)) {
        if (ann.authorMemberId === memberId) {
          this.db.delete(NS.annotation, keyOf(member.swarmId, ann.path + SEP + ann.type));
        }
      }
      for (const b of this.scan<Belief>(NS.belief, member.swarmId)) {
        if (b.authorMemberId === memberId) this.delOne(NS.belief, member.swarmId, b.factHash);
      }
      this.delOne(NS.member, member.swarmId, memberId);
    });
  }

  async updateMemberStatus(
    memberId: string,
    status: SwarmMember["status"],
    fields?: Partial<Pick<SwarmMember, "lastActiveAt">> & { currentTaskId?: string | null },
  ): Promise<void> {
    return this.serialized(async () => {
      const member = this.findGlobal<SwarmMember>(NS.member, memberId);
      if (!member) return;
      const current = fields?.currentTaskId === undefined ? (member.currentTaskId ?? null) : fields.currentTaskId;
      // Ownership guard (audit S9): an explicit non-null currentTaskId must
      // reference a task in the same swarm owned by this member or unowned.
      if (current !== null && current !== undefined) {
        const task = this.findGlobal<SwarmTask>(NS.task, current);
        if (!task) throw new Error(`updateMemberStatus: task '${current}' does not exist`);
        if (task.swarmId !== member.swarmId) {
          throw new Error(`updateMemberStatus: task '${current}' is not in the same swarm as member '${memberId}'`);
        }
        if (task.ownerMemberId !== undefined && task.ownerMemberId !== memberId) {
          throw new Error(
            `updateMemberStatus: member '${memberId}' does not own task '${current}' (owned by '${task.ownerMemberId}')`,
          );
        }
      }
      member.status = status;
      member.currentTaskId = current ?? undefined;
      member.lastActiveAt = fields?.lastActiveAt ?? Date.now();
      member.updatedAt = Date.now();
      this.putOne(NS.member, member.swarmId, member.id, member);
    });
  }

  async updateMemberHumanChat(memberId: string, humanChatAt: number | null): Promise<void> {
    return this.serialized(async () => {
      const member = this.findGlobal<SwarmMember>(NS.member, memberId);
      if (!member) return;
      member.humanChatAt = humanChatAt;
      member.updatedAt = Date.now();
      this.putOne(NS.member, member.swarmId, member.id, member);
    });
  }

  // ==== tasks ====

  async insertTask(t: NewTask): Promise<SwarmTask> {
    return this.serialized(async () => {
      const retryCount =
        typeof t.retryCount === "number"
          ? t.retryCount
          : typeof t.metadata?.retryCount === "number"
            ? t.metadata.retryCount
            : 0;
      const task: SwarmTask = {
        ...t,
        retryCount,
      };
      this.putOne(NS.task, t.swarmId, t.id, task);
      return task;
    });
  }

  async listTasks(swarmId: string): Promise<SwarmTask[]> {
    return this.serialized(async () =>
      this.scan<SwarmTask>(NS.task, swarmId).sort(
        (a, b) => b.priority - a.priority || a.createdAt - b.createdAt,
      ),
    );
  }

  async listExpiredLeaseTasks(swarmId: string, now: number): Promise<SwarmTask[]> {
    return this.serialized(async () =>
      this.scan<SwarmTask>(NS.task, swarmId).filter(
        (t) =>
          (t.status === "claimed" || t.status === "working") &&
          t.leaseExpiresAt !== undefined &&
          t.leaseExpiresAt < now,
      ),
    );
  }

  async insertTaskDependency(taskId: string, dependsOnTaskId: string): Promise<void> {
    return this.serialized(async () => {
      if (taskId === dependsOnTaskId) {
        throw new Error(`CHECK constraint failed: task cannot depend on itself`);
      }
      const task = this.findGlobal<SwarmTask>(NS.task, taskId);
      if (!task) throw new Error(`FOREIGN KEY constraint failed: task '${taskId}' does not exist`);
      this.putOne(NS.dependency, task.swarmId, `${taskId}::${dependsOnTaskId}`, {
        taskId,
        dependsOnTaskId,
      });
    });
  }

  async listTaskDependencies(swarmId: string): Promise<TaskDependency[]> {
    return this.serialized(async () => this.scan<TaskDependency>(NS.dependency, swarmId));
  }

  async claimTask(taskId: string, memberId: string, leaseMs?: number): Promise<boolean> {
    return this.serialized(async () => {
      // Member-side CAS: a member already owning a DIFFERENT non-terminal task
      // must not claim another (read-then-write is atomic here — the method
      // runs synchronously inside one queue slot).
      const member = this.findGlobal<SwarmMember>(NS.member, memberId);
      if (member && member.currentTaskId && member.currentTaskId !== taskId) return false;
      const task = this.findGlobal<SwarmTask>(NS.task, taskId);
      if (!task || task.ownerMemberId !== undefined || task.status !== "ready") return false;
      const now = Date.now();
      const leaseExpiresAt = (leaseMs !== undefined && leaseMs !== null && leaseMs > 0) ? now + leaseMs : null;
      task.ownerMemberId = memberId;
      task.status = "claimed";
      task.claimedAt = now;
      task.leaseExpiresAt = leaseExpiresAt ?? undefined;
      task.reservedFor = undefined;
      task.reservedAt = undefined;
      task.updatedAt = now;
      this.putOne(NS.task, task.swarmId, task.id, task);
      return true;
    });
  }

  async updateTaskStatus(taskId: string, status: SwarmTask["status"]): Promise<boolean> {
    return this.serialized(async () => {
      const task = this.findGlobal<SwarmTask>(NS.task, taskId);
      if (!task || ["completed", "failed", "cancelled"].includes(task.status)) return false;
      const now = Date.now();
      task.status = status;
      task.updatedAt = now;
      if (["completed", "failed", "cancelled"].includes(status)) task.completedAt = now;
      this.putOne(NS.task, task.swarmId, task.id, task);
      return true;
    });
  }

  async releaseTask(taskId: string, opts?: { countAsRetry?: boolean }): Promise<boolean> {
    return this.serialized(async () => {
      const task = this.findGlobal<SwarmTask>(NS.task, taskId);
      if (!task || !["claimed", "working", "review_pending", "changes_requested"].includes(task.status)) {
        return false;
      }
      const countAsRetry = opts?.countAsRetry ?? true;
      const now = Date.now();
      task.ownerMemberId = undefined;
      task.status = "ready";
      task.retryCount = (task.retryCount ?? 0) + (countAsRetry ? 1 : 0);
      task.claimedAt = undefined;
      task.leaseExpiresAt = undefined;
      task.reservedFor = undefined;
      task.reservedAt = undefined;
      task.updatedAt = now;
      this.putOne(NS.task, task.swarmId, task.id, task);
      return true;
    });
  }

  async setTaskReservation(taskId: string, memberName: string | null): Promise<boolean> {
    return this.serialized(async () => {
      const task = this.findGlobal<SwarmTask>(NS.task, taskId);
      if (!task) return false;
      const now = Date.now();
      task.reservedFor = memberName ?? undefined;
      task.reservedAt = memberName ? now : undefined;
      task.updatedAt = now;
      this.putOne(NS.task, task.swarmId, task.id, task);
      return true;
    });
  }

  async reassignTask(taskId: string, newOwnerMemberId: string): Promise<string | null> {
    return this.serialized(async () => {
      const task = this.findGlobal<SwarmTask>(NS.task, taskId);
      if (!task) throw new Error(`no task '${taskId}'`);
      if (["completed", "failed", "cancelled"].includes(task.status)) {
        throw new Error(`task '${taskId}' is terminal (${task.status}); reassignment is not allowed`);
      }
      const newOwner = this.findGlobal<SwarmMember>(NS.member, newOwnerMemberId);
      if (!newOwner) throw new Error(`no member '${newOwnerMemberId}'`);
      if (["stopped", "stopping", "failed"].includes(newOwner.status)) {
        throw new Error(`cannot reassign to '${newOwner.name}': member is ${newOwner.status}`);
      }
      const oldOwnerId = task.ownerMemberId ?? null;
      if (oldOwnerId && oldOwnerId !== newOwnerMemberId) {
        const oldOwner = this.findGlobal<SwarmMember>(NS.member, oldOwnerId);
        if (oldOwner?.currentTaskId === taskId) {
          oldOwner.currentTaskId = undefined;
          oldOwner.updatedAt = Date.now();
          this.putOne(NS.member, oldOwner.swarmId, oldOwner.id, oldOwner);
        }
      }
      const now = Date.now();
      task.ownerMemberId = newOwnerMemberId;
      task.status = "working";
      task.reservedFor = newOwner.name;
      task.reservedAt = now;
      task.updatedAt = now;
      this.putOne(NS.task, task.swarmId, task.id, task);
      newOwner.currentTaskId = taskId;
      newOwner.status = "working";
      newOwner.updatedAt = now;
      this.putOne(NS.member, newOwner.swarmId, newOwner.id, newOwner);
      return oldOwnerId;
    });
  }

  // ==== path claims ====

  async listPathClaims(swarmId: string, now = Date.now()): Promise<PathClaim[]> {
    return this.serialized(async () =>
      this.scan<PathClaim>(NS.claim, swarmId)
        .filter(
          (c) =>
            c.releasedAt === undefined &&
            (c.expiresAt === undefined || c.expiresAt > now),
        )
        .sort((a, b) => a.createdAt - b.createdAt),
    );
  }

  async insertPathClaim(claim: NewPathClaim): Promise<PathClaim> {
    return this.serialized(async () => {
      const dup = this.scan<PathClaim>(NS.claim, claim.swarmId).find(
        (c) => c.memberId === claim.memberId && c.pattern === claim.pattern && c.releasedAt === undefined,
      );
      if (dup) {
        throw new Error(`UNIQUE constraint failed: active path claim (swarm_id, member_id, pattern)`);
      }
      const stored: PathClaim = {
        id: claim.id,
        swarmId: claim.swarmId,
        memberId: claim.memberId,
        pattern: claim.pattern,
        mode: claim.mode ?? "advisory",
        createdAt: claim.createdAt,
        expiresAt: claim.expiresAt,
        releasedAt: claim.releasedAt,
      };
      this.putOne(NS.claim, claim.swarmId, claim.id, stored);
      return stored;
    });
  }

  async releasePathClaim(claimId: string): Promise<boolean> {
    return this.serialized(async () => {
      const claim = this.findGlobal<PathClaim>(NS.claim, claimId);
      if (!claim || claim.releasedAt !== undefined) return false;
      claim.releasedAt = Date.now();
      this.putOne(NS.claim, claim.swarmId, claim.id, claim);
      return true;
    });
  }

  async deletePathClaim(claimId: string): Promise<void> {
    return this.serialized(async () => {
      const claim = this.findGlobal<PathClaim>(NS.claim, claimId);
      if (claim) this.delOne(NS.claim, claim.swarmId, claim.id);
    });
  }

  async refreshPathClaim(claimId: string, ttlMs: number): Promise<PathClaim | undefined> {
    return this.serialized(async () => {
      const claim = this.findGlobal<PathClaim>(NS.claim, claimId);
      if (!claim) return undefined;
      claim.expiresAt = Date.now() + ttlMs;
      claim.releasedAt = undefined;
      this.putOne(NS.claim, claim.swarmId, claim.id, claim);
      return claim;
    });
  }

  // ==== subscriptions ====

  async listSubscriptions(swarmId: string): Promise<TopicSubscription[]> {
    return this.serialized(async () =>
      this.scan<TopicSubscription>(NS.subscription, swarmId).sort((a, b) => a.createdAt - b.createdAt),
    );
  }

  async addSubscription(swarmId: string, memberId: string, pattern: string): Promise<TopicSubscription> {
    return this.serialized(async () => {
      const existing = this.scan<TopicSubscription>(NS.subscription, swarmId).find(
        (s) => s.memberId === memberId && s.pattern === pattern,
      );
      if (existing) return existing;
      const sub: TopicSubscription = {
        id: `sub_${crypto.randomUUID().replace(/-/g, "")}`,
        swarmId,
        memberId,
        pattern,
        createdAt: Date.now(),
      };
      this.putOne(NS.subscription, swarmId, sub.id, sub);
      return sub;
    });
  }

  async removeSubscription(subscriptionId: string): Promise<void> {
    return this.serialized(async () => {
      const sub = this.findGlobal<TopicSubscription>(NS.subscription, subscriptionId);
      if (sub) this.delOne(NS.subscription, sub.swarmId, sub.id);
    });
  }

  // ==== messages ====

  async insertMessages(msgs: NewMessage[]): Promise<SwarmMessage[]> {
    return this.serialized(async () => {
      if (msgs.length === 0) return [];
      // Bulk-write through putMany: values land in compressed chunks and any
      // pre-existing delta overrides for these keys are flushed (overwrite
      // semantics — same as a fresh INSERT).
      const entries = msgs.map((m) => [keyOf(m.swarmId, m.id), m] as [string, NewMessage]);
      this.db.putMany(NS.message, entries);
      return msgs as SwarmMessage[];
    });
  }

  async listPendingMessages(toMemberId: string): Promise<NewMessage[]> {
    return this.serialized(async () => {
      const now = Date.now();
      return this.scanAll<SwarmMessage>(NS.message)
        .filter(
          (m) =>
            m.to.type === "member" &&
            m.to.memberId === toMemberId &&
            m.deliveryState === "queued" &&
            (m.expiresAt === undefined || m.expiresAt > now),
        )
        .sort(
          (a, b) =>
            (PRIORITY_RANK[a.priority] ?? 3) - (PRIORITY_RANK[b.priority] ?? 3) ||
            a.createdAt - b.createdAt,
        );
    });
  }

  async listMembersWithPendingMail(): Promise<Array<{ memberId: string; sessionId: string; count: number }>> {
    return this.serialized(async () => {
      const now = Date.now();
      const pending = this.scanAll<SwarmMessage>(NS.message).filter(
        (m) =>
          m.deliveryState === "queued" &&
          (m.expiresAt === undefined || m.expiresAt > now),
      );
      const counts = new Map<string, number>();
      const memberIdToSession = new Map<string, string>();
      for (const m of pending) {
        if (m.to.type !== "member" || !m.to.memberId) continue;
        counts.set(m.to.memberId, (counts.get(m.to.memberId) ?? 0) + 1);
      }
      for (const member of this.scanAll<SwarmMember>(NS.member)) {
        if (["stopped", "stopping", "failed"].includes(member.status)) continue;
        if (counts.has(member.id)) memberIdToSession.set(member.id, member.sessionId);
      }
      const out: Array<{ memberId: string; sessionId: string; count: number }> = [];
      for (const [memberId, count] of counts) {
        const sessionId = memberIdToSession.get(memberId);
        if (sessionId !== undefined) out.push({ memberId, sessionId, count });
      }
      return out;
    });
  }

  async listMessagesBySwarm(swarmId: string, limit = 20): Promise<NewMessage[]> {
    return this.serialized(async () =>
      this.scan<SwarmMessage>(NS.message, swarmId)
        .sort((a, b) => b.createdAt - a.createdAt)
        .slice(0, limit),
    );
  }

  async searchMessagesBySwarm(swarmId: string, query: string, limit = 15): Promise<NewMessage[]> {
    return this.serialized(async () => {
      const q = query.toLowerCase();
      return this.scan<SwarmMessage>(NS.message, swarmId)
        .filter((m) => m.body.text.toLowerCase().includes(q))
        .sort((a, b) => b.createdAt - a.createdAt)
        .slice(0, limit);
    });
  }

  async getMessageById(messageId: string): Promise<NewMessage | undefined> {
    return this.serialized(async () => this.findGlobal<SwarmMessage>(NS.message, messageId));
  }

  async getMessagesByIds(messageIds: string[]): Promise<NewMessage[]> {
    return this.serialized(async () => {
      if (messageIds.length === 0) return [];
      const want = new Set(messageIds);
      return this.scanAll<SwarmMessage>(NS.message).filter((m) => want.has(m.id));
    });
  }

  async markMessagesScheduled(toMemberId: string, messageIds: string[]): Promise<number> {
    return this.serialized(async () => {
      if (messageIds.length === 0) return 0;
      const want = new Set(messageIds);
      let changed = 0;
      for (const m of this.scanAll<SwarmMessage>(NS.message)) {
        if (!want.has(m.id)) continue;
        if (m.to.type === "member" && m.to.memberId === toMemberId && m.deliveryState === "queued") {
          m.deliveryState = "scheduled";
          this.putOne(NS.message, m.swarmId, m.id, m);
          changed++;
        }
      }
      return changed;
    });
  }

  async expireMessage(messageId: string): Promise<void> {
    return this.serialized(async () => {
      const m = this.findGlobal<SwarmMessage>(NS.message, messageId);
      if (m && (m.deliveryState === "queued" || m.deliveryState === "scheduled")) {
        m.deliveryState = "expired";
        this.putOne(NS.message, m.swarmId, m.id, m);
      }
    });
  }

  async updateMessageDelivery(messageId: string, state: SwarmMessage["deliveryState"]): Promise<void> {
    return this.serialized(async () => {
      const m = this.findGlobal<SwarmMessage>(NS.message, messageId);
      if (!m || m.deliveryState === "expired" || m.deliveryState === "failed") return;
      m.deliveryState = state;
      if (state === "delivered") m.deliveredAt = m.deliveredAt ?? Date.now();
      this.putOne(NS.message, m.swarmId, m.id, m);
    });
  }

  async revertMessageToQueuedWithError(messageId: string, toMemberId: string, error: string): Promise<NewMessage | undefined> {
    return this.serialized(async () => {
      const m = this.findGlobal<SwarmMessage>(NS.message, messageId);
      if (!m || m.deliveryState !== "scheduled") return undefined;
      if (m.to.type !== "member" || m.to.memberId !== toMemberId) return undefined;
      m.deliveryState = "queued";
      m.attemptCount = (m.attemptCount ?? 0) + 1;
      m.lastError = error.slice(0, 500);
      this.putOne(NS.message, m.swarmId, m.id, m);
      return m;
    });
  }

  async markMessageFailed(messageId: string): Promise<NewMessage | undefined> {
    return this.serialized(async () => {
      const m = this.findGlobal<SwarmMessage>(NS.message, messageId);
      if (!m || m.deliveryState !== "queued") return undefined;
      m.deliveryState = "failed";
      m.lastError = m.lastError ?? "delivery retry budget exhausted";
      this.putOne(NS.message, m.swarmId, m.id, m);
      return m;
    });
  }

  async revertMessageToQueued(messageId: string, toMemberId: string): Promise<void> {
    return this.serialized(async () => {
      const m = this.findGlobal<SwarmMessage>(NS.message, messageId);
      if (!m || m.deliveryState !== "scheduled") return;
      if (m.to.type !== "member" || m.to.memberId !== toMemberId) return;
      m.deliveryState = "queued";
      this.putOne(NS.message, m.swarmId, m.id, m);
    });
  }

  async revertStaleScheduledForSwarm(swarmId: string): Promise<number> {
    return this.serialized(async () => {
      const now = Date.now();
      let changed = 0;
      for (const m of this.scan<SwarmMessage>(NS.message, swarmId)) {
        if (m.deliveryState !== "scheduled") continue;
        if (m.expiresAt !== undefined && m.expiresAt <= now) {
          m.deliveryState = "expired";
          changed++;
        } else {
          m.deliveryState = "queued";
          changed++;
        }
        this.putOne(NS.message, m.swarmId, m.id, m);
      }
      return changed;
    });
  }

  async expireOverdueMessages(now: number): Promise<NewMessage[]> {
    return this.transaction(async (tx) => {
      const rows = this.scanAll<SwarmMessage>(NS.message).filter(
        (m) =>
          m.expiresAt !== undefined &&
          m.expiresAt <= now &&
          (m.deliveryState === "queued" || m.deliveryState === "scheduled"),
      );
      for (const r of rows) {
        await tx.expireMessage(r.id);
      }
      return rows;
    });
  }

  // ==== blackboard ====

  async insertBlackboard(e: NewBlackboardEntry): Promise<BlackboardEntry> {
    return this.serialized(async () => {
      this.putOne(NS.blackboard, e.swarmId, e.key, e);
      return e as BlackboardEntry;
    });
  }

  async getBlackboard(swarmId: string, key: string): Promise<BlackboardEntry | undefined> {
    return this.serialized(async () => this.getOne<BlackboardEntry>(NS.blackboard, swarmId, key));
  }

  async searchBlackboard(swarmId: string, query: string): Promise<BlackboardEntry[]> {
    return this.serialized(async () => {
      const q = query.toLowerCase();
      return this.scan<BlackboardEntry>(NS.blackboard, swarmId).filter(
        (e) => e.key.toLowerCase().includes(q) || e.value.toLowerCase().includes(q),
      );
    });
  }

  async listBlackboardEntries(swarmId: string): Promise<BlackboardEntry[]> {
    return this.serialized(async () =>
      this.scan<BlackboardEntry>(NS.blackboard, swarmId).sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)),
    );
  }

  async upsertBlackboard(entry: BlackboardEntry, expectedVersion?: number): Promise<void> {
    return this.serialized(async () => {
      const existing = this.scan<BlackboardEntry>(NS.blackboard, entry.swarmId).find(
        (e) => e.id === entry.id,
      );
      if (existing) {
        if (expectedVersion !== undefined && existing.version !== expectedVersion) {
          throw new Error(
            `blackboard conflict on '${entry.key}': expected version ${expectedVersion}, current ${existing.version}`,
          );
        }
        // Update in place by id — the key may have changed; move the row.
        if (existing.key !== entry.key) this.delOne(NS.blackboard, entry.swarmId, existing.key);
        this.putOne(NS.blackboard, entry.swarmId, entry.key, entry);
        return;
      }
      // Insert path: UNIQUE(swarm_id, key) — a different row owning the key is
      // a constraint violation (sqlite bubbles it up).
      const byKey = this.getOne<BlackboardEntry>(NS.blackboard, entry.swarmId, entry.key);
      if (byKey && byKey.id !== entry.id) {
        throw new Error(`UNIQUE constraint failed: swarm_blackboard (swarm_id, key)`);
      }
      this.putOne(NS.blackboard, entry.swarmId, entry.key, entry);
    });
  }

  // ==== Hive H0 artifact annotations ====

  async insertAnnotation(a: NewArtifactAnnotation): Promise<ArtifactAnnotation> {
    return this.serialized(async () => {
      const now = a.createdAt;
      const expiresAt = a.expiresAt ?? (a.ttl !== undefined && a.ttl > 0 ? now + a.ttl : undefined);
      const entityKey = a.path + SEP + a.type;
      // Replace semantics: a fresh annotation for the same (swarm, path, type)
      // overwrites the previous one IN PLACE — the original id is kept so
      // releaseOrDeleteAnnotation stays valid (mirrors sqlite Edge S1).
      const existing = this.getOne<ArtifactAnnotation>(NS.annotation, a.swarmId, entityKey);
      if (existing) {
        existing.path = a.path;
        existing.type = a.type;
        existing.weight = a.weight;
        existing.note = a.note;
        existing.errorSig = a.errorSig;
        existing.solutionHash = a.solutionHash;
        existing.ttl = a.ttl;
        existing.expiresAt = expiresAt;
        existing.authorMemberId = a.authorMemberId;
        existing.createdAt = now;
        this.putOne(NS.annotation, a.swarmId, entityKey, existing);
        return existing;
      }
      const stored: ArtifactAnnotation = {
        id: a.id,
        swarmId: a.swarmId,
        path: a.path,
        type: a.type,
        weight: a.weight,
        note: a.note,
        errorSig: a.errorSig,
        solutionHash: a.solutionHash,
        ttl: a.ttl,
        expiresAt,
        authorMemberId: a.authorMemberId,
        createdAt: now,
      };
      this.putOne(NS.annotation, a.swarmId, entityKey, stored);
      return stored;
    });
  }

  async listAnnotations(
    swarmId: string,
    opts?: { path?: string; activeOnly?: boolean; now?: number },
  ): Promise<ArtifactAnnotation[]> {
    return this.serialized(async () => {
      const activeOnly = opts?.activeOnly ?? true;
      const now = opts?.now ?? Date.now();
      return this.scan<ArtifactAnnotation>(NS.annotation, swarmId)
        .filter((a) => {
          if (opts?.path !== undefined && a.path !== opts.path) return false;
          if (activeOnly && a.expiresAt !== undefined && a.expiresAt <= now) return false;
          return true;
        })
        .sort((a, b) => b.createdAt - a.createdAt);
    });
  }

  async releaseOrDeleteAnnotation(annotationId: string): Promise<boolean> {
    return this.serialized(async () => {
      for (const a of this.scanAll<ArtifactAnnotation>(NS.annotation)) {
        if (a.id === annotationId) {
          this.db.delete(NS.annotation, keyOf(a.swarmId, a.path + SEP + a.type));
          return true;
        }
      }
      return false;
    });
  }

  // ==== Hive H1 beliefs ====

  async insertBelief(b: NewBelief): Promise<Belief> {
    return this.serialized(async () => {
      const now = b.createdAt;
      const expiresAt = b.expiresAt ?? (b.ttl !== undefined && b.ttl > 0 ? now + b.ttl : undefined);
      const status = b.status ?? "active";
      const confidence = Math.min(1, Math.max(0, b.confidence));
      // Dedupe by (swarmId, factHash): re-insert REINFORCES instead of
      // replacing (lateral inhibition, item 7). Soft-pruned rows are revived;
      // resonant rows keep their status. The original id is preserved.
      const existing = this.getOne<Belief>(NS.belief, b.swarmId, b.factHash);
      if (existing) {
        existing.reinforceCount = existing.reinforceCount + 1;
        existing.confidence = Math.min(1, existing.confidence + 0.1);
        existing.evidenceRefs = b.evidenceRefs;
        if (existing.status === "superseded" || existing.status === "expired") existing.status = "active";
        existing.expiresAt = expiresAt;
        existing.updatedAt = b.updatedAt;
        this.putOne(NS.belief, b.swarmId, b.factHash, existing);
        return existing;
      }
      const stored: Belief = {
        id: b.id,
        swarmId: b.swarmId,
        factHash: b.factHash,
        text: b.text,
        confidence,
        tags: b.tags,
        tier: b.tier,
        ttl: b.ttl,
        expiresAt,
        authorMemberId: b.authorMemberId,
        evidenceRefs: b.evidenceRefs,
        reinforceCount: b.reinforceCount ?? 1,
        status,
        createdAt: now,
        updatedAt: b.updatedAt,
      };
      this.putOne(NS.belief, b.swarmId, b.factHash, stored);
      return stored;
    });
  }

  async reinforceBelief(swarmId: string, factHash: string, deltaConfidence = 0.1): Promise<Belief | undefined> {
    return this.serialized(async () => {
      const belief = this.getOne<Belief>(NS.belief, swarmId, factHash);
      if (!belief) return undefined;
      belief.reinforceCount = belief.reinforceCount + 1;
      belief.confidence = Math.min(1, Math.max(0, belief.confidence + deltaConfidence));
      belief.updatedAt = Date.now();
      this.putOne(NS.belief, swarmId, factHash, belief);
      return belief;
    });
  }

  async upgradeWhisperToShout(swarmId: string, factHash: string): Promise<Belief | undefined> {
    return this.serialized(async () => {
      const belief = this.getOne<Belief>(NS.belief, swarmId, factHash);
      if (!belief || belief.tier !== "whisper" || belief.reinforceCount < 2) return undefined;
      belief.tier = "shout";
      belief.updatedAt = Date.now();
      this.putOne(NS.belief, swarmId, factHash, belief);
      return belief;
    });
  }

  async listBeliefs(
    swarmId: string,
    opts?: { activeOnly?: boolean; tier?: Belief["tier"]; minConfidence?: number; query?: string; status?: Belief["status"]; now?: number },
  ): Promise<Belief[]> {
    return this.serialized(async () => {
      const activeOnly = opts?.status === undefined ? (opts?.activeOnly ?? true) : (opts?.activeOnly ?? false);
      const now = opts?.now ?? Date.now();
      const q = opts?.query?.toLowerCase();
      return this.scan<Belief>(NS.belief, swarmId)
        .filter((b) => {
          if (opts?.tier !== undefined && b.tier !== opts.tier) return false;
          if (opts?.status !== undefined && b.status !== opts.status) return false;
          if (opts?.minConfidence !== undefined && b.confidence < opts.minConfidence) return false;
          if (q !== undefined && q !== "" && !b.text.toLowerCase().includes(q)) return false;
          if (activeOnly) {
            if (b.status === "expired" || b.status === "superseded") return false;
            if (b.expiresAt !== undefined && b.expiresAt <= now) return false;
          }
          return true;
        })
        .sort((a, b) => b.confidence - a.confidence || b.createdAt - a.createdAt);
    });
  }

  async expireBeliefs(now: number): Promise<number> {
    return this.serialized(async () => {
      let changed = 0;
      for (const b of this.scanAll<Belief>(NS.belief)) {
        if (b.status === "active" && b.expiresAt !== undefined && b.expiresAt <= now) {
          b.status = "expired";
          b.updatedAt = now;
          this.putOne(NS.belief, b.swarmId, b.factHash, b);
          changed++;
        }
      }
      return changed;
    });
  }

  async markResonant(swarmId: string, factHash: string): Promise<Belief | undefined> {
    return this.serialized(async () => {
      const belief = this.getOne<Belief>(NS.belief, swarmId, factHash);
      if (!belief) return undefined;
      if (belief.status === "active") {
        const now = Date.now();
        belief.status = "resonant";
        belief.resonantAt = now;
        belief.updatedAt = now;
        this.putOne(NS.belief, swarmId, factHash, belief);
      }
      return belief;
    });
  }

  async listBeliefsForPruning(
    swarmId: string,
    opts?: { maxConfidence?: number; minReinforce?: number; olderThanMs?: number; limit?: number },
  ): Promise<Belief[]> {
    return this.serialized(async () => {
      const olderThan = opts?.olderThanMs !== undefined ? Date.now() - opts.olderThanMs : undefined;
      return this.scan<Belief>(NS.belief, swarmId)
        .filter((b) => {
          if (b.status !== "active") return false;
          if (opts?.maxConfidence !== undefined && b.confidence > opts.maxConfidence) return false;
          if (opts?.minReinforce !== undefined && b.reinforceCount > opts.minReinforce) return false;
          if (olderThan !== undefined && b.updatedAt >= olderThan) return false;
          return true;
        })
        .sort((a, b) => a.confidence - b.confidence || a.updatedAt - b.updatedAt)
        .slice(0, opts?.limit ?? 50);
    });
  }

  async softPruneBelief(
    swarmId: string,
    factHash: string,
    to: Extract<BeliefStatus, "superseded" | "expired">,
  ): Promise<Belief | undefined> {
    return this.serialized(async () => {
      const belief = this.getOne<Belief>(NS.belief, swarmId, factHash);
      if (!belief) return undefined;
      belief.status = to;
      belief.updatedAt = Date.now();
      this.putOne(NS.belief, swarmId, factHash, belief);
      return belief;
    });
  }

  async hardPruneBeliefs(swarmId: string, factHashes: string[]): Promise<number> {
    return this.serialized(async () => {
      if (factHashes.length === 0) return 0;
      const want = new Set(factHashes);
      let deleted = 0;
      for (const b of this.scan<Belief>(NS.belief, swarmId)) {
        if (want.has(b.factHash)) {
          this.delOne(NS.belief, swarmId, b.factHash);
          deleted++;
        }
      }
      return deleted;
    });
  }

  async beliefDigest(swarmId: string): Promise<{ digest: string; count: number }> {
    return this.serialized(async () => {
      const tuples = this.scan<Belief>(NS.belief, swarmId)
        .filter((b) => b.status === "active")
        .map((b) => `${b.id}:${b.updatedAt}:${b.reinforceCount}:${b.confidence}:${b.tier}`)
        .sort();
      const digest = await sha1Hex(tuples.join("\n"));
      return { digest, count: tuples.length };
    });
  }

  async listBeliefsChangedSince(swarmId: string, since: number): Promise<Belief[]> {
    return this.serialized(async () =>
      this.scan<Belief>(NS.belief, swarmId)
        .filter((b) => b.updatedAt > since)
        .sort((a, b) => a.updatedAt - b.updatedAt),
    );
  }

  beliefEvidenceDisjoint(a: Belief, b: Belief): boolean {
    return beliefEvidenceDisjoint(a, b);
  }

  // ==== pending permissions ====

  async insertPendingPermission(p: PendingPermission): Promise<void> {
    return this.serialized(async () => {
      this.putOne(NS.permission, p.swarmId, p.id, p);
    });
  }

  async listPendingPermissions(swarmId: string): Promise<PendingPermission[]> {
    return this.serialized(async () =>
      this.scan<PendingPermission>(NS.permission, swarmId)
        .filter((p) => p.response === null)
        .sort((a, b) => b.createdAt - a.createdAt),
    );
  }

  async listPendingForMembers(memberIds: string[]): Promise<PendingPermission[]> {
    return this.serialized(async () => {
      if (memberIds.length === 0) return [];
      const want = new Set(memberIds);
      return this.scanAll<PendingPermission>(NS.permission)
        .filter((p) => p.response === null && want.has(p.memberId))
        .sort((a, b) => b.createdAt - a.createdAt);
    });
  }

  async getPendingPermission(swarmId: string, permissionId: string): Promise<PendingPermission | undefined> {
    return this.serialized(async () =>
      this.scan<PendingPermission>(NS.permission, swarmId).find((p) => p.id === permissionId),
    );
  }

  async respondToPermission(permissionId: string, response: "once" | "always" | "reject"): Promise<void> {
    return this.serialized(async () => {
      const p = this.findGlobal<PendingPermission>(NS.permission, permissionId);
      if (!p || p.response !== null) return;
      p.response = response;
      p.respondedAt = Date.now();
      this.putOne(NS.permission, p.swarmId, p.id, p);
    });
  }

  async markPermissionReplied(permissionId: string, response?: string): Promise<void> {
    return this.serialized(async () => {
      const p = this.findGlobal<PendingPermission>(NS.permission, permissionId);
      if (!p) return;
      p.response = response ?? "replied";
      p.respondedAt = Date.now();
      this.putOne(NS.permission, p.swarmId, p.id, p);
    });
  }

  // ==== event stream (timeline / replay) ====

  async insertEvent(e: NewSwarmEvent): Promise<void> {
    return this.serialized(async () => {
      // Migration preserves original ids: a caller pinning `id` (survives as a
      // runtime field) forces the seq and bumps the counter past it.
      const pinned = (e as { id?: number }).id;
      const seq = pinned !== undefined && Number.isInteger(pinned) ? pinned : this.evtNextSeq(e.swarmId);
      if (pinned !== undefined && Number.isInteger(pinned)) this.evtPinnedSeq(e.swarmId, pinned);
      const stored: SwarmEvent = {
        id: seq,
        swarmId: e.swarmId,
        type: e.type,
        actorMemberId: e.actorMemberId,
        entityType: e.entityType,
        entityId: e.entityId,
        payloadJson: e.payloadJson,
        createdAt: e.createdAt,
      };
      this.db.put(NS.event, keyOf(e.swarmId, pad12(seq)), stored);
    });
  }

  async listEvents(swarmId: string, opts?: { limit?: number; since?: number }): Promise<SwarmEvent[]> {
    return this.serialized(async () => {
      const limit = opts?.limit ?? 100;
      const rows = this.scan<SwarmEvent>(NS.event, swarmId).filter(
        (e) => opts?.since === undefined || e.createdAt > opts.since,
      );
      rows.sort((a, b) => b.id - a.id);
      return rows.slice(0, limit);
    });
  }

  async listEventsForEntity(
    swarmId: string,
    entityType: string,
    entityId: string,
    opts?: { limit?: number },
  ): Promise<SwarmEvent[]> {
    return this.serialized(async () => {
      const rows = this.scan<SwarmEvent>(NS.event, swarmId).filter(
        (e) => e.entityType === entityType && e.entityId === entityId,
      );
      rows.sort((a, b) => b.id - a.id);
      return rows.slice(0, opts?.limit ?? 50);
    });
  }

  // ==== handoff ledger (deliverables) ====

  async insertDeliverable(d: NewDeliverable): Promise<Deliverable> {
    return this.serialized(async () => {
      const id = d.id ?? `dlv_${crypto.randomUUID().replace(/-/g, "")}`;
      const stored: Deliverable = {
        id,
        swarmId: d.swarmId,
        memberId: d.memberId,
        taskId: d.taskId,
        summary: d.summary,
        refs: d.refs,
        files: d.files,
        verdict: d.verdict ?? null,
        createdAt: d.createdAt,
      };
      this.putOne(NS.deliverable, d.swarmId, id, stored);
      return stored;
    });
  }

  async listDeliverables(
    swarmId: string,
    opts?: { verdict?: "accepted" | "rejected"; memberId?: string; taskId?: string; limit?: number },
  ): Promise<Deliverable[]> {
    return this.serialized(async () =>
      this.scan<Deliverable>(NS.deliverable, swarmId)
        .filter((d) => {
          if (opts?.verdict !== undefined && d.verdict !== opts.verdict) return false;
          if (opts?.memberId !== undefined && d.memberId !== opts.memberId) return false;
          if (opts?.taskId !== undefined && d.taskId !== opts.taskId) return false;
          return true;
        })
        .sort((a, b) => b.createdAt - a.createdAt)
        .slice(0, opts?.limit ?? 50),
    );
  }

  async setDeliverableVerdict(deliverableId: string, verdict: "accepted" | "rejected", byMemberId: string): Promise<boolean> {
    return this.serialized(async () => {
      const d = this.findGlobal<Deliverable>(NS.deliverable, deliverableId);
      if (!d || d.verdict !== null) return false;
      d.verdict = verdict;
      d.verdictBy = byMemberId;
      d.verdictAt = Date.now();
      this.putOne(NS.deliverable, d.swarmId, d.id, d);
      return true;
    });
  }

  async getDeliverable(deliverableId: string): Promise<Deliverable | undefined> {
    return this.serialized(async () => this.findGlobal<Deliverable>(NS.deliverable, deliverableId));
  }

  // ==== typed blackboard contracts ====

  async insertContract(c: NewContractDefinition): Promise<ContractDefinition> {
    return this.serialized(async () => {
      const id = c.id ?? `ctr_${crypto.randomUUID().replace(/-/g, "")}`;
      const now = Date.now();
      const stored: ContractDefinition = {
        id,
        swarmId: c.swarmId,
        keyPattern: c.keyPattern,
        schemaJson: c.schemaJson,
        description: c.description,
        createdBy: c.createdBy,
        createdAt: now,
        updatedAt: now,
      };
      this.putOne(NS.contract, c.swarmId, c.keyPattern, stored);
      return stored;
    });
  }

  async listContracts(swarmId: string): Promise<ContractDefinition[]> {
    return this.serialized(async () =>
      this.scan<ContractDefinition>(NS.contract, swarmId).sort((a, b) =>
        a.keyPattern < b.keyPattern ? -1 : a.keyPattern > b.keyPattern ? 1 : 0,
      ),
    );
  }

  async getContract(swarmId: string, key: string): Promise<ContractDefinition | undefined> {
    return this.serialized(async () => this.getOne<ContractDefinition>(NS.contract, swarmId, key));
  }

  async deleteContract(swarmId: string, key: string): Promise<boolean> {
    return this.serialized(async () => {
      const existing = this.getOne<ContractDefinition>(NS.contract, swarmId, key);
      if (!existing) return false;
      this.delOne(NS.contract, swarmId, key);
      return true;
    });
  }
}
