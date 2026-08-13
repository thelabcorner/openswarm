import type {
  ArtifactAnnotation,
  Belief,
  BeliefStatus,
  BlackboardEntry,
  ContractDefinition,
  Deliverable,
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
  PathClaim,
  PendingPermission,
  Swarm,
  SwarmEvent,
  SwarmMember,
  SwarmMessage,
  SwarmTask,
  TaskDependency,
  TopicSubscription,
} from "./models.js";

export interface SwarmStoreTx {
  insertSwarm(swarm: NewSwarm): Promise<Swarm>;
  insertMember(member: NewSwarmMember): Promise<SwarmMember>;
  insertTask(task: NewTask): Promise<SwarmTask>;
  insertMessages(msgs: NewMessage[]): Promise<SwarmMessage[]>;
  insertBlackboard(entry: NewBlackboardEntry): Promise<BlackboardEntry>;
  insertTaskDependency(taskId: string, dependsOnTaskId: string): Promise<void>;
  /** Delete a member row (used to roll back a reservation when session create fails). */
  deleteMember(memberId: string): Promise<void>;
  /** Permanently delete a swarm and all its cascaded rows (members, tasks,
   * messages, blackboard, claims, subscriptions). */
  deleteSwarm(swarmId: string): Promise<void>;
  /** Assign the real backing session id to a member row (spawn phase 3). */
  assignMemberSession(memberId: string, sessionId: string): Promise<void>;

  getSwarm(id: string): Promise<Swarm | undefined>;
  getSwarmBySession(sessionID: string): Promise<Swarm | undefined>;
  getSwarmByName(projectId: string, name: string): Promise<Swarm | undefined>;
  /** Refresh the working directory a swarm roots its members in. Used to heal
   * legacy swarms persisted before the directory column existed. */
  updateSwarmDirectory(swarmId: string, directory: string): Promise<void>;
  /** Rebind a swarm to a new coordinator session (used when a swarm is reused
   * from a different chat/session so the caller becomes the coordinator). */
  updateSwarmCoordinator(swarmId: string, coordinatorSessionId: string): Promise<void>;
  /** Set a swarm's lifecycle status (used by revive to flip a completed/failed
   * swarm back to active, and by normal lifecycle transitions). */
  updateSwarmStatus(swarmId: string, status: Swarm["status"]): Promise<void>;
  listMembers(swarmId: string): Promise<SwarmMember[]>;
  getMemberById(memberId: string): Promise<SwarmMember | undefined>;
  /** FIRST-MATCH session lookup (backward compat): returns the first member
   * row with that session. With multi-own (migration v12) a session may own N
   * swarms — callers resolving a member within a SPECIFIC swarm must use
   * getMemberBySessionAndSwarm instead. */
  getMemberBySessionId(sessionID: string): Promise<SwarmMember | undefined>;
  /** Multi-own (migration v12): ALL member rows with this session across every
   * swarm, created_at order. Powers per-session effects (human-chat pause,
   * session-event reduction) that must apply to every swarm the session
   * belongs to. */
  listMembersBySessionId(sessionID: string): Promise<SwarmMember[]>;
  /** Multi-own (migration v12): the exact (session, swarm) member row — the
   * swarm-scoped coordinator lookup. Every 'the coordinator member of swarm X'
   * resolution MUST go through this so swarm B's operations never pick up
   * swarm A's coordinator row. */
  getMemberBySessionAndSwarm(sessionID: string, swarmId: string): Promise<SwarmMember | undefined>;
  /** Multi-own (migration v12): every swarm this session is a member of
   * (distinct by swarm id). The swarms the session OWNS/coordinates are those
   * whose coordinatorSessionId is the session — callers filter when ownership
   * semantics matter. */
  listSwarmsBySession(sessionID: string): Promise<Swarm[]>;
  getMemberByName(swarmId: string, name: string): Promise<SwarmMember | undefined>;
  listTasks(swarmId: string): Promise<SwarmTask[]>;
  listTaskDependencies(swarmId: string): Promise<TaskDependency[]>;
  /**
   * Active (non-released, non-expired) advisory path claims in a swarm.
   * Stale claims (expires_at passed) are not counted as active even though
   * their rows still exist.
   */
  listPathClaims(swarmId: string, now?: number): Promise<PathClaim[]>;
  /** Claim a path pattern (advisory, TTL-optional). INSERT with
   * UNIQUE(swarm_id, member_id, pattern) for active claims — a member can hold
   * at most one ACTIVE claim per pattern; released/expired rows don't block a
   * fresh claim. Throws on duplicate active claim. */
  insertPathClaim(claim: NewPathClaim): Promise<PathClaim>;
  /** Release an active claim (sets released_at). Returns false if already
   * released / not found. */
  releasePathClaim(claimId: string): Promise<boolean>;
  /** Hard-delete a claim row (also clears TTL state). */
  deletePathClaim(claimId: string): Promise<void>;
  /** WIP Aura heartbeat: extend an ACTIVE claim's expires_at by ttlMs from now
   * (returns the claim with the new expiry). Refreshing a claim that is stale
   * (expires_at passed) or released re-activates it (clears released_at) — the
   * owner is still working the lane. Returns undefined when the claim id does
   * not exist. */
  refreshPathClaim(claimId: string, ttlMs: number): Promise<PathClaim | undefined>;
  listSubscriptions(swarmId: string): Promise<TopicSubscription[]>;
  addSubscription(swarmId: string, memberId: string, pattern: string): Promise<TopicSubscription>;
  removeSubscription(subscriptionId: string): Promise<void>;

  updateMemberStatus(
    memberId: string,
    status: SwarmMember["status"],
    fields?: Partial<Pick<SwarmMember, "lastActiveAt">> & { currentTaskId?: string | null },
  ): Promise<void>;

  /** Record or clear the last time the user directly messaged this member
   * (the human-chat state machine's persisted timestamp). */
  updateMemberHumanChat(memberId: string, humanChatAt: number | null): Promise<void>;

  /**
   * Atomically claim a ready task for a member. Returns true only if the
   * claim succeeded; SQLite affects-row is the source of truth.
   * `leaseMs` (from policies.taskLeaseMs) sets claimed_at/lease_expires_at.
   */
  claimTask(taskId: string, memberId: string, leaseMs?: number): Promise<boolean>;

  /** Set a task's status (complete/fail/cancel). Returns false if the task
   * is already in a terminal state. */
  updateTaskStatus(taskId: string, status: SwarmTask["status"]): Promise<boolean>;

  /**
   * Release a claimed/working task (owner cleared, status back to ready).
   * By default the release counts as one retry attempt (F3 retry-cap). Pass
   * `{ countAsRetry: false }` for releases that are NOT the task's fault
   * (e.g. a scheduler kickoff failure — the task never ran, so it must not
   * consume the retry budget; S-01).
   */
  releaseTask(taskId: string, opts?: { countAsRetry?: boolean }): Promise<boolean>;

  /** Durable intended-owner binding (delegate member.taskId): the scheduler
   * prefers this member when the task becomes ready, including later-ready
   * DAG tasks. Pass null to clear. */
  setTaskReservation(taskId: string, memberName: string | null): Promise<boolean>;

  /** Claimed/working tasks whose claim lease has expired (lease_expires_at <
   * now) — candidates for the sweep to release, subject to the human-chat
   * guard (a chatting owner keeps its lease). */
  listExpiredLeaseTasks(swarmId: string, now: number): Promise<SwarmTask[]>;

  /**
   * Atomic coordinator reassignment primitive (F11): rebind a task to a NEW
   * owner member — clearing the OLD owner's currentTaskId (when it points at
   * this task) and setting the new owner's currentTaskId, all in one write.
   * Completing the full transition is the caller's job (kickoff prompt etc.).
   * Returns the previous ownerMemberId (or null). Throws if the task or new
   * owner member does not exist, or the task is terminal.
   */
  reassignTask(taskId: string, newOwnerMemberId: string): Promise<string | null>;

  getBlackboard(swarmId: string, key: string): Promise<BlackboardEntry | undefined>;
  searchBlackboard(swarmId: string, query: string): Promise<BlackboardEntry[]>;
  /** Every blackboard entry for a swarm, ordered by key — used by the
   * sqlite→chunkdb migration to copy the full blackboard through the public
   * read API. */
  listBlackboardEntries(swarmId: string): Promise<BlackboardEntry[]>;
  /**
   * Insert or update a blackboard row. When `expectedVersion` is provided, the
   * update is CAS-guarded at the store level: it only applies if the existing
   * row's version matches, otherwise a conflict error is thrown (audit S2).
   */
  upsertBlackboard(entry: BlackboardEntry, expectedVersion?: number): Promise<void>;

  listPendingMessages(toMemberId: string): Promise<NewMessage[]>;
  /** Members that currently have at least one queued (undelivered) message —
   * powers the F-M7 sweep mailbox delivery so never-idle members still get
   * their mail. Excludes members whose only pending mail is expired. */
  listMembersWithPendingMail(): Promise<Array<{ memberId: string; sessionId: string; count: number }>>;
  /** Recent messages in a swarm (any delivery state), newest first, for the
   * `swarm_status detail:"messages"` read surface. */
  listMessagesBySwarm(swarmId: string, limit?: number): Promise<NewMessage[]>;
  /** Recent messages whose body text matches a substring (case-insensitive),
   * newest first — powers the redundancy-probe tool. */
  searchMessagesBySwarm(swarmId: string, query: string, limit?: number): Promise<NewMessage[]>;
  getMessageById(messageId: string): Promise<NewMessage | undefined>;
  /** Fetch messages by id (any delivery state). Used to re-read the persisted
   * delivery state after auto-wake so senders see real verdicts, not the
   * pre-wake `queued` snapshot. */
  getMessagesByIds(messageIds: string[]): Promise<NewMessage[]>;
  /** Marks queued messages as scheduled. Returns the number actually
   * transitioned (0 if another wake already claimed them). */
  markMessagesScheduled(
    toMemberId: string,
    messageIds: string[],
  ): Promise<number>;
  /** Mark a message as expired (used when its expiresAt passed). */
  expireMessage(messageId: string): Promise<void>;
  updateMessageDelivery(messageId: string, state: SwarmMessage["deliveryState"]): Promise<void>;
  /**
   * Revert a scheduled message to queued after a FAILED delivery attempt.
   * Records the failure: increments attempt_count and stores last_error.
   * Returns the updated message (with attemptCount/lastError) so the caller
   * can enforce the delivery retry budget (audit/messaging F-M5).
   */
  revertMessageToQueuedWithError(messageId: string, toMemberId: string, error: string): Promise<NewMessage | undefined>;
  /** Mark a message `failed` after its delivery retry budget is exhausted and
   * return the updated row (sender-notification path is exactly-once). */
  markMessageFailed(messageId: string): Promise<NewMessage | undefined>;
  /** Revert a scheduled message to queued (plain state change, no attempt
   * bookkeeping — used by tests/recovery paths that don't represent a failed
   * delivery attempt). */
  revertMessageToQueued(messageId: string, toMemberId: string): Promise<void>;
  listAllMemberSwarmIds(): Promise<string[]>;
  /** Revert stale `scheduled` deliveries back to `queued` on startup recovery,
   * EXCEPT rows whose expiresAt has passed — those are marked `expired` instead
   * of being resurrected (a message past its expiry must not come back to life
   * after a restart; cross-ref audit/storage S5, audit/messaging F-M2). */
  revertStaleScheduledForSwarm(swarmId: string): Promise<number>;

  // ==== Hive H0 artifact annotations (features/hive-mind-execution-layer) ====

  /**
   * Insert (or replace) an artifact annotation on a workspace path. One row
   * per (swarm_id, path, type): a fresh annotation for the same path/type
   * replaces the previous one (scent is point-in-time, latest wins). TTL is
   * converted to `expiresAt` at insert.
   */
  insertAnnotation(a: NewArtifactAnnotation): Promise<ArtifactAnnotation>;
  /**
   * List artifact annotations for a swarm. `opts.path` filters to one path;
   * `opts.activeOnly` (default true) excludes stale rows (expires_at passed)
   * and uses `opts.now` (default Date.now()) as the reference time. A row with
   * no expires_at never goes stale.
   */
  listAnnotations(
    swarmId: string,
    opts?: { path?: string; activeOnly?: boolean; now?: number },
  ): Promise<ArtifactAnnotation[]>;
  /** Delete an annotation row. Returns false if it did not exist. */
  releaseOrDeleteAnnotation(annotationId: string): Promise<boolean>;
  // ==== Hive H1 beliefs/facts substrate (features/hive-mind-execution-layer) ====

  /**
   * Insert a belief/fact, DEDUPLICATED by fact_hash (UNIQUE(swarm_id,
   * fact_hash)): re-inserting an existing fact REINFORCES it — reinforce_count
   * +1 and confidence bumped (clamped 0..1) — instead of creating a duplicate
   * (lateral inhibition, item 7). Returns the resulting row.
   */
  insertBelief(b: NewBelief): Promise<Belief>;
  /** Increment reinforce_count + confidence (clamped 0..1) for a fact. Returns
   * undefined if no belief with that fact_hash exists in the swarm. */
  reinforceBelief(swarmId: string, factHash: string, deltaConfidence?: number): Promise<Belief | undefined>;
  /** Promote a whisper to shout when reinforce_count >= 2 (item 8). Returns the
   * updated row, or undefined if the belief is missing / not eligible. The
   * caller (hive tool layer) decides WHEN to call it so tool output is truthful. */
  upgradeWhisperToShout(swarmId: string, factHash: string): Promise<Belief | undefined>;
  /**
   * List beliefs for a swarm. `opts.activeOnly` (default true) excludes
   * expired/superseded statuses AND rows whose expires_at passed (`opts.now`
   * default Date.now()); `opts.tier` filters whisper|shout; `opts.minConfidence`
   * filters confidence >= threshold; `opts.query` matches text via ESCAPE-safe
   * LIKE; `opts.status` filters an exact status (e.g. 'resonant' for
   * Hive H2 resonance queries). Newest first.
   */
  listBeliefs(
    swarmId: string,
    opts?: { activeOnly?: boolean; tier?: Belief["tier"]; minConfidence?: number; query?: string; status?: Belief["status"]; now?: number },
  ): Promise<Belief[]>;
  /** Sweep: mark beliefs `expired` whose expires_at passed (status + TTL both
   * honored). Returns the number transitioned. Idempotent. */
  expireBeliefs(now: number): Promise<number>;

  // ==== Hive H2 resonance / consolidation / anti-entropy (items 10, 12) ====

  /** Transition a belief active→resonant (sets status='resonant' + resonant_at).
   * Idempotent: already-resonant is a no-op returning the row; missing →
   * undefined. The caller (resonance detector) decides WHEN. */
  markResonant(swarmId: string, factHash: string): Promise<Belief | undefined>;
  /** Pruning candidates: low confidence (<= maxConfidence) AND low reuse
   * (<= minReinforce) AND old (updated_at < now - olderThanMs). `limit` caps
   * the result (default 50). */
  listBeliefsForPruning(
    swarmId: string,
    opts?: { maxConfidence?: number; minReinforce?: number; olderThanMs?: number; limit?: number },
  ): Promise<Belief[]>;
  /** Soft-prune: transition a belief to 'superseded' | 'expired' (row kept —
   * causal chain via evidence_refs intact). undefined when missing. */
  softPruneBelief(swarmId: string, factHash: string, to: Extract<BeliefStatus, "superseded" | "expired">): Promise<Belief | undefined>;
  /** Hard-prune: DELETE the belief rows (returns count deleted). */
  hardPruneBeliefs(swarmId: string, factHashes: string[]): Promise<number>;
  /** Anti-entropy digest: stable hash over (id, updated_at) tuples + count.
   * Two peers with the same beliefs produce the same digest — divergence is
   * detected by comparing digests. */
  beliefDigest(swarmId: string): Promise<{ digest: string; count: number }>;
  /** Anti-entropy pull: beliefs whose updated_at > since (changed since the
   * peer's last digest timestamp). */
  listBeliefsChangedSince(swarmId: string, since: number): Promise<Belief[]>;

  // ==== Pending permission prompts (coordinator answers member stalls) ====

  /** Record a permission prompt that stayed "ask" for a member session.
   * Overwrites an existing row with the same id (the ask id is unique per
   * request; re-raises just refresh the row). */
  insertPendingPermission(p: PendingPermission): Promise<void>;
  /** Pending (unanswered) permission prompts for a swarm, newest first. */
  listPendingPermissions(swarmId: string): Promise<PendingPermission[]>;
  /** Pending (unanswered) prompts for SPECIFIC member ids (cross-swarm safe:
   * ids are global). Used to detect "recipient is stuck behind a permissions
   * wall" at message-send time. */
  listPendingForMembers(memberIds: string[]): Promise<PendingPermission[]>;
  /** One record for a swarm (undefined when absent). */
  getPendingPermission(swarmId: string, permissionId: string): Promise<PendingPermission | undefined>;
  /** Mark a prompt answered by the coordinator (once/always/reject). */
  respondToPermission(permissionId: string, response: "once" | "always" | "reject"): Promise<void>;
  /** Mark a prompt answered externally (the `permission.replied` event — the
   * user answered in the app). `response` may be a raw server string. */
  markPermissionReplied(permissionId: string, response?: string): Promise<void>;

  // ==== Event stream (timeline / replay) ====

  /** Append one event to the swarm's timeline (fire-and-forget recording). */
  insertEvent(e: NewSwarmEvent): Promise<void>;
  /** Read the timeline newest-first; `opts.limit` (default 100) caps rows and
   * `opts.since` filters to events after a timestamp. */
  listEvents(swarmId: string, opts?: { limit?: number; since?: number }): Promise<SwarmEvent[]>;
  /** Read events for one entity (e.g. changelog for a blackboard key). */
  listEventsForEntity(swarmId: string, entityType: string, entityId: string, opts?: { limit?: number }): Promise<SwarmEvent[]>;

  // ==== Handoff ledger (deliverables) ====

  insertDeliverable(d: NewDeliverable): Promise<Deliverable>;
  /** Deliverables newest-first; filters optional. */
  listDeliverables(
    swarmId: string,
    opts?: { verdict?: "accepted" | "rejected"; memberId?: string; taskId?: string; limit?: number },
  ): Promise<Deliverable[]>;
  /** Attach a verdict (accepted/rejected) to an open deliverable. Returns
   * false when the id is unknown or already verdict-ed. */
  setDeliverableVerdict(deliverableId: string, verdict: "accepted" | "rejected", byMemberId: string): Promise<boolean>;
  /** One ledger row by id (undefined when absent). Used by the verdict path to
   * verify the deliverable belongs to the calling coordinator's swarm before
   * recording a verdict on it. */
  getDeliverable(deliverableId: string): Promise<Deliverable | undefined>;

  // ==== Typed blackboard contracts ====

  insertContract(c: NewContractDefinition): Promise<ContractDefinition>;
  /** All contracts for a swarm. */
  listContracts(swarmId: string): Promise<ContractDefinition[]>;
  /** The contract governing a blackboard key (exact match). */
  getContract(swarmId: string, key: string): Promise<ContractDefinition | undefined>;
  deleteContract(swarmId: string, key: string): Promise<boolean>;
}

export interface SwarmStore extends SwarmStoreTx {
  transaction<T>(fn: (tx: SwarmStoreTx) => Promise<T>): Promise<T>;
  /** Open/initialize the backing store (idempotent). */
  ready(): Promise<void>;
  close(): Promise<void>;
  /** Atomically transition every queued/scheduled message whose `expiresAt`
   * has passed to `expired`, and return the affected messages so the caller
   * can send the sender exactly one notice. Runs inside the store's serialized
   * transaction; no-op rows are never returned twice, so the notice path is
   * exactly-once per message (audit/messaging F-M2). */
  expireOverdueMessages(now: number): Promise<NewMessage[]>;
  /** True when two beliefs' evidence_refs JSON arrays share no ref id — the
   * resonance eligibility signal (disjoint evidence = independent
   * confirmation). Pure helper, exported for the resonance detector. */
  beliefEvidenceDisjoint(a: Belief, b: Belief): boolean;
}