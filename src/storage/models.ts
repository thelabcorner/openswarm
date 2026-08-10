import type {
  ArtifactAnnotation,
  Belief,
  BeliefStatus,
  BlackboardEntry,
  PathClaim,
  Swarm,
  SwarmMember,
  SwarmMessage,
  SwarmPolicies,
  SwarmTask,
  TopicSubscription,
} from "../core/types.js";

/** Re-export domain types so storage code can import from one place. */
export type {
  ArtifactAnnotation,
  Belief,
  BeliefStatus,
  BlackboardEntry,
  PathClaim,
  Swarm,
  SwarmMember,
  SwarmMessage,
  SwarmPolicies,
  SwarmTask,
  TopicSubscription,
};

export type NewSwarm = Omit<Swarm, "createdAt" | "updatedAt"> & {
  createdAt: number;
  updatedAt: number;
};

export type NewSwarmMember = Omit<
  SwarmMember,
  "createdAt" | "updatedAt"
> & {
  createdAt: number;
  updatedAt: number;
};

export type NewTask = Omit<SwarmTask, "createdAt" | "updatedAt"> & {
  createdAt: number;
  updatedAt: number;
};

export type NewMessage = Omit<SwarmMessage, "createdAt"> & {
  createdAt: number;
};

export type NewBlackboardEntry = Omit<
  BlackboardEntry,
  "createdAt" | "updatedAt"
> & {
  createdAt: number;
  updatedAt: number;
};

export type TaskDependency = {
  taskId: string;
  dependsOnTaskId: string;
};

export type NewPathClaim = Omit<PathClaim, "createdAt"> & {
  createdAt: number;
};

export type NewArtifactAnnotation = Omit<
  ArtifactAnnotation,
  "createdAt" | "expiresAt"
> & {
  createdAt: number;
  /** Derived from `ttl` at insert; callers may pass an explicit expiry. */
  expiresAt?: number;
};

export type NewBelief = Omit<
  Belief,
  "createdAt" | "updatedAt" | "expiresAt" | "reinforceCount" | "status"
> & {
  createdAt: number;
  updatedAt: number;
  /** Derived from `ttl` at insert; callers may pass an explicit expiry. */
  expiresAt?: number;
  /** Fresh inserts start at 1 (this insert counts as the first reinforce). */
  reinforceCount?: number;
  /** Defaults to 'active' at insert. */
  status?: Belief["status"];
};