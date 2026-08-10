export type MemberStatus =
  | "created"
  | "starting"
  | "working"
  | "waiting"
  | "idle"
  | "blocked"
  | "interrupted"
  | "failed"
  | "stopping"
  | "stopped";

export type TaskStatus =
  | "pending"
  | "blocked"
  | "ready"
  | "claimed"
  | "working"
  | "review_pending"
  | "changes_requested"
  | "completed"
  | "failed"
  | "cancelled";

export type MessageKind =
  | "message"
  | "request"
  | "response"
  | "finding"
  | "handoff"
  | "blocker"
  | "decision"
  | "review"
  | "control";

export type MessagePriority = "low" | "normal" | "high" | "urgent";

export type DeliveryState =
  | "queued"
  | "scheduled"
  | "delivered"
  | "acknowledged"
  | "expired"
  | "failed";

export type WorkspaceMode = "shared-read" | "shared-write" | "worktree";

export type CoordinatorMode = "normal" | "delegate-only";

export type MessageDeliveryMode = "idle" | "tool-boundary" | "hybrid";

export type RetentionMode = "ephemeral" | "session" | "project";

export interface SwarmPolicies {
  maxMembers: number;
  maxConcurrentMembers: number;
  allowMemberSpawn: boolean;
  maxSpawnDepth: number;
  coordinatorMode: CoordinatorMode;
  defaultWorkspace: WorkspaceMode;
  messageDelivery: MessageDeliveryMode;
  autoWake: boolean;
  autoReview: boolean;
  abortChildrenOnSwarmStop: boolean;
  idleTimeoutMs?: number;
  taskLeaseMs?: number;
  maxRetriesPerTask: number;
  /** How many failed delivery attempts a message may have before it is marked
   * `failed` and the sender is notified. Defaults to 3. (audit/messaging F-M5:
   * a wedged member must not accumulate undeliverable mail forever.) */
  maxDeliveryAttempts?: number;
  retention: RetentionMode;
  /** How long (ms) after the last direct user message in a member session
   * before swarm machinery (mail delivery, task continuation, scheduler
   * assignment) auto-resumes for that member. Default 5 minutes. */
  humanChatLullMs?: number;
}

export const DEFAULT_POLICIES: SwarmPolicies = {
  maxMembers: 10,
  maxConcurrentMembers: 10,
  allowMemberSpawn: false,
  maxSpawnDepth: 1,
  coordinatorMode: "normal",
  defaultWorkspace: "worktree",
  messageDelivery: "idle",
  autoWake: true,
  autoReview: false,
  abortChildrenOnSwarmStop: true,
  maxRetriesPerTask: 2,
  retention: "project",
  humanChatLullMs: 300_000,
};

/** Default delivery-attempt budget for messages (audit/messaging F-M5). */
export const DEFAULT_MAX_DELIVERY_ATTEMPTS = 3;

export type SwarmStatus =
  | "creating"
  | "active"
  | "paused"
  | "stopping"
  | "completed"
  | "failed";

export interface Swarm {
  id: string;
  projectId: string;
  name: string;
  coordinatorSessionId: string;
  coordinatorMemberId: string;
  /** The coordinator session's working directory — members are rooted here so
   * they share the project worktree (avoids external_directory prompts). */
  directory: string;
  status: SwarmStatus;
  policies: SwarmPolicies;
  createdAt: number;
  updatedAt: number;
  completedAt?: number;
}

export interface SwarmMember {
  id: string;
  swarmId: string;
  name: string;
  role: string;
  sessionId: string;
  agent?: string;
  model?: {
    providerID: string;
    modelID: string;
  };
  status: MemberStatus;
  workspaceMode: WorkspaceMode;
  workspacePath?: string;
  branch?: string;
  currentTaskId?: string;
  /** Epoch-ms of the last DIRECT user message in this member's chat session,
   * or null if the user has never messaged them directly. While `now - this <
   * policies.humanChatLullMs` the swarm defers machinery for the member. */
  humanChatAt?: number | null;
  createdAt: number;
  updatedAt: number;
  lastActiveAt?: number;
}

export interface SwarmTask {
  id: string;
  swarmId: string;
  title: string;
  description?: string;
  status: TaskStatus;
  priority: number;
  ownerMemberId?: string;
  createdByMemberId: string;
  acceptanceCriteria?: string[];
  metadata?: Record<string, unknown>;
  /** Number of times this task has been released from an active state and
   * re-queued for reassignment (bounded by policies.maxRetriesPerTask).
   * First-class column `retry_count` (NOT the metadata hack). */
  retryCount?: number;
  /** Epoch-ms when the task was last claimed (set on claimTask). */
  claimedAt?: number;
  /** Epoch-ms when the task's claim lease expires (set on claimTask from
   * policies.taskLeaseMs; null/undefined = no lease). The sweep releases
   * claimed/working tasks past this timestamp. */
  leaseExpiresAt?: number;
  createdAt: number;
  updatedAt: number;
  completedAt?: number;
}

export interface MessageTarget {
  type: "member" | "broadcast";
  memberId?: string;
  broadcast?: boolean;
}

export interface SwarmMessage {
  id: string;
  swarmId: string;
  fromMemberId: string;
  to: MessageTarget;
  kind: MessageKind;
  taskId?: string;
  correlationId?: string;
  responseTo?: string;
  priority: MessagePriority;
  body: {
    text: string;
    refs?: string[];
  };
  deliveryState: DeliveryState;
  attemptCount: number;
  /** Last delivery-failure reason (written by the broker on revert; surfaced
   * to the sender when the retry budget is exhausted, F-M5). */
  lastError?: string;
  createdAt: number;
  deliveredAt?: number;
  acknowledgedAt?: number;
  expiresAt?: number;
}

export interface BlackboardEntry {
  id: string;
  swarmId: string;
  key: string;
  value: string;
  contentType: "text/plain" | "text/markdown" | "application/json";
  version: number;
  authorMemberId: string;
  taskId?: string;
  createdAt: number;
  updatedAt: number;
}

export interface PathClaim {
  id: string;
  swarmId: string;
  memberId: string;
  pattern: string;
  mode: "advisory";
  createdAt: number;
  releasedAt?: number;
  /** TTL advisory expiry (epoch-ms). When set, the claim stops counting as
   * active once `now >= expiresAt` — stale claims are invisible to
   * listPathClaims without an explicit release. Optional: no expiry = claim
   * lives until released/removed. */
  expiresAt?: number;
}

/** Hive H0 artifact-annotation scent types (features/hive-mind-execution-layer
 * Phase H0). A member annotates a workspace path with durable signal: claim
 * (I own this lane), struggle (stuck here), corpse (dead end), gold (verified
 * solution), affordance (path worth trying), note (free-form). */
export type ArtifactAnnotationType =
  | "claim"
  | "struggle"
  | "corpse"
  | "gold"
  | "affordance"
  | "note";/** One member's durable annotation on a workspace path (advisory scent).
 * `expiresAt` is derived from `ttl` at insert (epoch-ms); when set, the
 * annotation stops being listed once `now >= expiresAt` (stale exclusion).
 * One ACTIVE annotation per (swarm_id, path, type) — a fresh annotation
 * replaces the previous one on the same path/type. */
export interface ArtifactAnnotation {
  id: string;
  swarmId: string;
  path: string;
  type: ArtifactAnnotationType;
  /** Signal strength (0..10); gold/corpse carry higher absolute weight. */
  weight: number;
  note?: string;
  /** For `struggle`/`corpse`: the error signature the member hit. */
  errorSig?: string;
  /** For `gold`: hash of the verified solution so duplicates can be
   * recognized/reused. */
  solutionHash?: string;
  /** Optional TTL (ms) — converted to `expiresAt` at insert. */
  ttl?: number;
  expiresAt?: number;
  authorMemberId: string;
  createdAt: number;
}

/** Hive H1 belief-tiers (features/hive-mind-execution-layer items 7–10):
 * whisper = tentative local fact; shout = reinforced, swarm-visible fact.
 * Status lifecycle: active → superseded (contradicted) | expired (TTL) |
 * resonant (spotlighted by the hive tool layer). */
export type BeliefTier = "whisper" | "shout";
export type BeliefStatus = "active" | "superseded" | "expired" | "resonant";

/** A durable, deduplicated belief/fact in the hive substrate. One row per
 * (swarm_id, fact_hash): re-inserting the same fact REINFORCES it (increments
 * reinforce_count, bumps confidence) rather than duplicating. Evidence refs
 * are the source messages/artifacts that support the fact (lateral-inhibition
 * resonance inputs, item 10). */
export interface Belief {
  id: string;
  swarmId: string;
  factHash: string;
  text: string;
  /** 0..1 — clamp at the store boundary. */
  confidence: number;
  /** Free-form tags, comma-separated (e.g. "nibble,wire,fixed"). */
  tags?: string;
  tier: BeliefTier;
  ttl?: number;
  expiresAt?: number;
  authorMemberId: string;
  /** Evidence refs (message ids / artifact paths) as a JSON array string. */
  evidenceRefs?: string;
  reinforceCount: number;
  status: BeliefStatus;
  /** Set by markResonant (item 10) — epoch-ms when the belief became resonant. */
  resonantAt?: number;
  createdAt: number;
  updatedAt: number;
}

/**
 * A member's topic subscription. Topic patterns use `*` wildcards, e.g.
 * `contracts/**`, `decisions/ui/*`. When a blackboard key is published, only
 * subscribed members are notified — information routing instead of broadcast.
 */
export interface TopicSubscription {
  id: string;
  swarmId: string;
  memberId: string;
  pattern: string;
  createdAt: number;
}

/**
 * Glob-style matcher for topic patterns. Supports `*` (single segment) and
 * `**` (any depth). Used for blackboard pub/sub routing.
 */
export function topicMatches(pattern: string, topic: string): boolean {
  const p = pattern.split("/");
  const t = topic.split("/");
  let i = 0;
  let j = 0;
  while (i < p.length) {
    const seg = p[i];
    if (seg === "**") {
      // ** can match zero or more segments
      if (i === p.length - 1) return true;
      for (let k = j; k <= t.length; k++) {
        if (topicMatches(p.slice(i + 1).join("/"), t.slice(k).join("/"))) return true;
      }
      return false;
    }
    if (seg === "*") {
      if (j >= t.length) return false;
    } else {
      if (j >= t.length || seg !== t[j]) return false;
    }
    i++;
    j++;
  }
  return j === t.length;
}