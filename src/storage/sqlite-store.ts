import { openSqlite, type SqliteLike } from "./sqlite.js";
import { createHash } from "node:crypto";
import type {
  ArtifactAnnotation,
  Belief,
  BeliefStatus,
  BlackboardEntry,
  PathClaim,
  Swarm,
  SwarmMember,
  SwarmMessage,
  SwarmTask,
  TopicSubscription,
} from "../core/types.js";
import type {
  NewArtifactAnnotation,
  NewBelief,
  NewBlackboardEntry,
  NewMessage,
  NewPathClaim,
  NewSwarm,
  NewSwarmMember,
  NewTask,
  TaskDependency,
} from "./models.js";
import type { SwarmStore, SwarmStoreTx } from "./store.js";

/** Escape LIKE metacharacters so a search term matches literally. */
function escapeLike(s: string): string {
  return s.replace(/[\\%_]/g, (c) => `\\${c}`);
}

/** Add a column to a table if it is absent. Idempotent — safe to call
 * repeatedly (migration steps and defensive re-apply). */
function addColumn(db: SqliteLike, table: string, column: string, ddl: string): void {
  const cols = db.query<{ name: string }>(`PRAGMA table_info(${table})`).all();
  if (!cols.some((c) => c.name === column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${ddl};`);
  }
}

/** Parse a belief's evidence_refs JSON array into a ref-id set (Hive H2
 * resonance, item 10). Malformed/absent refs yield an empty set. */
function evidenceRefSet(refs: string | undefined): Set<string> {
  if (!refs) return new Set();
  try {
    const parsed = JSON.parse(refs);
    return new Set(Array.isArray(parsed) ? parsed.map(String) : []);
  } catch {
    return new Set();
  }
}

/** True when two beliefs' evidence sets share no ref id — independent
 * confirmation (disjoint evidence) is the resonance eligibility signal. */
function beliefEvidenceDisjoint(a: Belief, b: Belief): boolean {
  const sa = evidenceRefSet(a.evidenceRefs);
  const sb = evidenceRefSet(b.evidenceRefs);
  if (sa.size === 0 || sb.size === 0) return false; // no evidence = not disjoint-provable
  for (const id of sa) {
    if (sb.has(id)) return false;
  }
  return true;
}

/** sha1 hex of a string (used for the anti-entropy belief digest). Async to
 * keep the store surface promise-based. */
async function sha1Hex(s: string): Promise<string> {
  return createHash("sha1").update(s, "utf8").digest("hex");
}

const SCHEMA = /* sql */ `
${/* Set pragmas defensively (journal_mode can only be set outside txn). */ ""}
CREATE TABLE IF NOT EXISTS swarm (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  name TEXT NOT NULL,
  coordinator_session_id TEXT NOT NULL,
  coordinator_member_id TEXT NOT NULL,
  directory TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  policies_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  completed_at INTEGER,
  UNIQUE(project_id, name)
);

CREATE TABLE IF NOT EXISTS swarm_member (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  name TEXT NOT NULL,
  role TEXT NOT NULL,
  session_id TEXT NOT NULL UNIQUE,
  agent TEXT,
  provider_id TEXT,
  model_id TEXT,
  status TEXT NOT NULL,
  workspace_mode TEXT NOT NULL,
  workspace_path TEXT,
  branch TEXT,
  current_task_id TEXT,
  human_chat_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  last_active_at INTEGER,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  UNIQUE(swarm_id, name)
);
CREATE INDEX IF NOT EXISTS idx_member_status ON swarm_member(swarm_id, status);

CREATE TABLE IF NOT EXISTS swarm_task (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  status TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  owner_member_id TEXT,
  created_by_member_id TEXT NOT NULL,
  acceptance_json TEXT,
  metadata_json TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  claimed_at INTEGER,
  lease_expires_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  completed_at INTEGER,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(owner_member_id) REFERENCES swarm_member(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_task_ready ON swarm_task(swarm_id, status, priority DESC);

CREATE TABLE IF NOT EXISTS swarm_task_dependency (
  task_id TEXT NOT NULL,
  depends_on_task_id TEXT NOT NULL,
  PRIMARY KEY(task_id, depends_on_task_id),
  FOREIGN KEY(task_id) REFERENCES swarm_task(id) ON DELETE CASCADE,
  FOREIGN KEY(depends_on_task_id) REFERENCES swarm_task(id) ON DELETE CASCADE,
  CHECK(task_id <> depends_on_task_id)
);

CREATE TABLE IF NOT EXISTS swarm_message (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  from_member_id TEXT NOT NULL,
  to_member_id TEXT,
  is_broadcast INTEGER NOT NULL DEFAULT 0,
  kind TEXT NOT NULL,
  task_id TEXT,
  correlation_id TEXT,
  response_to TEXT,
  priority TEXT NOT NULL,
  body_text TEXT NOT NULL,
  refs_json TEXT,
  delivery_state TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  delivered_at INTEGER,
  acknowledged_at INTEGER,
  expires_at INTEGER,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(from_member_id) REFERENCES swarm_member(id) ON DELETE CASCADE,
  FOREIGN KEY(to_member_id) REFERENCES swarm_member(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_message_mailbox
  ON swarm_message(to_member_id, delivery_state, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_message_correlation ON swarm_message(correlation_id);

CREATE TABLE IF NOT EXISTS swarm_blackboard (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  content_type TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  author_member_id TEXT NOT NULL,
  task_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(author_member_id) REFERENCES swarm_member(id),
  UNIQUE(swarm_id, key)
);

CREATE TABLE IF NOT EXISTS swarm_path_claim (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  member_id TEXT NOT NULL,
  pattern TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'advisory',
  created_at INTEGER NOT NULL,
  expires_at INTEGER,
  released_at INTEGER,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(member_id) REFERENCES swarm_member(id) ON DELETE CASCADE
);
-- One ACTIVE claim per (swarm, member, pattern): released/expired rows are
-- excluded from the uniqueness scope so a member can re-claim a lane after
-- releasing it (advisory PathClaims, audit S1).
CREATE UNIQUE INDEX IF NOT EXISTS idx_path_claim_active_uniq
  ON swarm_path_claim(swarm_id, member_id, pattern)
  WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_path_claim_active ON swarm_path_claim(swarm_id, released_at);

-- Hive H0: durable artifact annotations (advisory scent on workspace paths).
-- One ACTIVE row per (swarm_id, path, type): a fresh annotation replaces the
-- previous one on the same path/type (scent is point-in-time, latest wins).
-- expires_at (derived from ttl at insert) makes the row stale-invisible; the
-- row itself remains until releaseOrDeleteAnnotation.
CREATE TABLE IF NOT EXISTS swarm_artifact_annotation (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  path TEXT NOT NULL,
  type TEXT NOT NULL,
  weight INTEGER NOT NULL DEFAULT 0,
  note TEXT,
  error_sig TEXT,
  solution_hash TEXT,
  ttl INTEGER,
  expires_at INTEGER,
  author_member_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(author_member_id) REFERENCES swarm_member(id),
  UNIQUE(swarm_id, path, type)
);
CREATE INDEX IF NOT EXISTS idx_annotation_swarm
  ON swarm_artifact_annotation(swarm_id, expires_at);

-- Hive H1: durable belief/fact substrate (semantic gossip data model, lateral
-- inhibition fact_hash, whisper/shout tiers, resonance evidence refs). One row
-- per (swarm_id, fact_hash): re-inserting the same fact REINFORCES it instead
-- of duplicating (dedupe by fact_hash). status: active | superseded | expired |
-- resonant. evidence_refs stores a JSON array of supporting message/artifact ids.
CREATE TABLE IF NOT EXISTS swarm_belief (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  fact_hash TEXT NOT NULL,
  text TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0.5,
  tags TEXT,
  tier TEXT NOT NULL DEFAULT 'whisper',
  ttl INTEGER,
  expires_at INTEGER,
  author_member_id TEXT NOT NULL,
  evidence_refs TEXT,
  reinforce_count INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'active',
  resonant_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(author_member_id) REFERENCES swarm_member(id),
  UNIQUE(swarm_id, fact_hash)
);
CREATE INDEX IF NOT EXISTS idx_belief_swarm
  ON swarm_belief(swarm_id, status, tier, confidence DESC);

CREATE TABLE IF NOT EXISTS swarm_subscription (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  member_id TEXT NOT NULL,
  pattern TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(member_id) REFERENCES swarm_member(id) ON DELETE CASCADE,
  UNIQUE(swarm_id, member_id, pattern)
);
CREATE INDEX IF NOT EXISTS idx_subscription_swarm ON swarm_subscription(swarm_id);

CREATE TABLE IF NOT EXISTS swarm_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  swarm_id TEXT NOT NULL,
  type TEXT NOT NULL,
  actor_member_id TEXT,
  entity_type TEXT,
  entity_id TEXT,
  payload_json TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_event_order ON swarm_event(swarm_id, id);
`;

interface RowMember {
  id: string; swarm_id: string; name: string; role: string; session_id: string;
  agent: string | null; provider_id: string | null; model_id: string | null;
  status: string; workspace_mode: string; workspace_path: string | null; branch: string | null;
  current_task_id: string | null; human_chat_at: number | null;
  created_at: number; updated_at: number; last_active_at: number | null;
}

interface RowTask {
  id: string; swarm_id: string; title: string; description: string | null;
  status: string; priority: number; owner_member_id: string | null;
  created_by_member_id: string; acceptance_json: string | null; metadata_json: string | null;
  retry_count: number; claimed_at: number | null; lease_expires_at: number | null;
  created_at: number; updated_at: number; completed_at: number | null;
}

interface RowMessage {
  id: string; swarm_id: string; from_member_id: string; to_member_id: string | null;
  is_broadcast: number; kind: string; task_id: string | null; correlation_id: string | null;
  response_to: string | null; priority: string; body_text: string; refs_json: string | null;
  delivery_state: string; attempt_count: number; last_error: string | null;
  created_at: number; delivered_at: number | null; acknowledged_at: number | null;
  expires_at: number | null;
}

interface RowBlackboard {
  id: string; swarm_id: string; key: string; value: string; content_type: string;
  version: number; author_member_id: string; task_id: string | null;
  created_at: number; updated_at: number;
}

interface RowPathClaim {
  id: string; swarm_id: string; member_id: string; pattern: string;
  mode: string; created_at: number; expires_at: number | null; released_at: number | null;
}

interface RowAnnotation {
  id: string; swarm_id: string; path: string; type: string;
  weight: number; note: string | null; error_sig: string | null; solution_hash: string | null;
  ttl: number | null; expires_at: number | null; author_member_id: string; created_at: number;
}

interface RowBelief {
  id: string; swarm_id: string; fact_hash: string; text: string;
  confidence: number; tags: string | null; tier: string; ttl: number | null; expires_at: number | null;
  author_member_id: string; evidence_refs: string | null; reinforce_count: number;
  status: string; resonant_at: number | null; created_at: number; updated_at: number;
}

interface RowSwarm {
  id: string; project_id: string; name: string; coordinator_session_id: string;
  coordinator_member_id: string; directory: string; status: string; policies_json: string;
  created_at: number; updated_at: number; completed_at: number | null;
}

function toSwarm(r: RowSwarm): Swarm {
  return {
    id: r.id, projectId: r.project_id, name: r.name, coordinatorSessionId: r.coordinator_session_id,
    coordinatorMemberId: r.coordinator_member_id, directory: r.directory, status: r.status as Swarm["status"],
    policies: JSON.parse(r.policies_json), createdAt: r.created_at, updatedAt: r.updated_at,
    completedAt: r.completed_at ?? undefined,
  };
}

function toMember(r: RowMember): SwarmMember {
  return {
    id: r.id, swarmId: r.swarm_id, name: r.name, role: r.role, sessionId: r.session_id,
    agent: r.agent ?? undefined,
    model: r.provider_id && r.model_id ? { providerID: r.provider_id, modelID: r.model_id } : undefined,
    status: r.status as SwarmMember["status"],
    workspaceMode: r.workspace_mode as SwarmMember["workspaceMode"],
    workspacePath: r.workspace_path ?? undefined,
    branch: r.branch ?? undefined,
    currentTaskId: r.current_task_id ?? undefined,
    humanChatAt: r.human_chat_at ?? null,
    createdAt: r.created_at, updatedAt: r.updated_at,
    lastActiveAt: r.last_active_at ?? undefined,
  };
}

function toTask(r: RowTask): SwarmTask {
  return {
    id: r.id, swarmId: r.swarm_id, title: r.title, description: r.description ?? undefined,
    status: r.status as SwarmTask["status"], priority: r.priority,
    ownerMemberId: r.owner_member_id ?? undefined,
    createdByMemberId: r.created_by_member_id,
    acceptanceCriteria: r.acceptance_json ? JSON.parse(r.acceptance_json) : undefined,
    metadata: r.metadata_json ? JSON.parse(r.metadata_json) : undefined,
    retryCount: r.retry_count,
    claimedAt: r.claimed_at ?? undefined,
    leaseExpiresAt: r.lease_expires_at ?? undefined,
    createdAt: r.created_at, updatedAt: r.updated_at,
    completedAt: r.completed_at ?? undefined,
  };
}

function toMessage(r: RowMessage): SwarmMessage {
  return {
    id: r.id, swarmId: r.swarm_id, fromMemberId: r.from_member_id,
    to: r.is_broadcast
      ? { type: "broadcast" }
      : { type: "member", memberId: r.to_member_id ?? "" },
    kind: r.kind as SwarmMessage["kind"],
    taskId: r.task_id ?? undefined,
    correlationId: r.correlation_id ?? undefined,
    responseTo: r.response_to ?? undefined,
    priority: r.priority as SwarmMessage["priority"],
    body: { text: r.body_text, refs: r.refs_json ? JSON.parse(r.refs_json) : undefined },
    deliveryState: r.delivery_state as SwarmMessage["deliveryState"],
    attemptCount: r.attempt_count,
    lastError: r.last_error ?? undefined,
    createdAt: r.created_at,
    deliveredAt: r.delivered_at ?? undefined,
    acknowledgedAt: r.acknowledged_at ?? undefined,
    expiresAt: r.expires_at ?? undefined,
  };
}

function toBlackboard(r: RowBlackboard): BlackboardEntry {
  return {
    id: r.id, swarmId: r.swarm_id, key: r.key, value: r.value,
    contentType: r.content_type as BlackboardEntry["contentType"],
    version: r.version, authorMemberId: r.author_member_id,
    taskId: r.task_id ?? undefined,
    createdAt: r.created_at, updatedAt: r.updated_at,
  };
}

function toPathClaim(r: RowPathClaim): PathClaim {
  return {
    id: r.id, swarmId: r.swarm_id, memberId: r.member_id, pattern: r.pattern,
    mode: r.mode as PathClaim["mode"],
    createdAt: r.created_at, expiresAt: r.expires_at ?? undefined, releasedAt: r.released_at ?? undefined,
  };
}

function toAnnotation(r: RowAnnotation): ArtifactAnnotation {
  return {
    id: r.id, swarmId: r.swarm_id, path: r.path, type: r.type as ArtifactAnnotation["type"],
    weight: r.weight,
    note: r.note ?? undefined,
    errorSig: r.error_sig ?? undefined,
    solutionHash: r.solution_hash ?? undefined,
    ttl: r.ttl ?? undefined,
    expiresAt: r.expires_at ?? undefined,
    authorMemberId: r.author_member_id,
    createdAt: r.created_at,
  };
}

function toBelief(r: RowBelief): Belief {
  return {
    id: r.id, swarmId: r.swarm_id, factHash: r.fact_hash, text: r.text,
    confidence: r.confidence,
    tags: r.tags ?? undefined,
    tier: r.tier as Belief["tier"],
    ttl: r.ttl ?? undefined,
    expiresAt: r.expires_at ?? undefined,
    authorMemberId: r.author_member_id,
    evidenceRefs: r.evidence_refs ?? undefined,
    reinforceCount: r.reinforce_count,
    status: r.status as Belief["status"],
    resonantAt: r.resonant_at ?? undefined,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

export class SQLiteStore implements SwarmStore {
  private db: SqliteLike;
  private tx: Tx;
  /** Serializes BEGIN/COMMIT so concurrent transactions never overlap on the
   * single shared connection. SQLite is the authority (§50); this makes the
   * app-level async boundaries safe. */
  private txnQueue: Promise<void> = Promise.resolve();
  private closed = false;

  constructor(private path: string) {
    // Deferred open so callers can `await store.ready`.
    this.db = undefined as unknown as SqliteLike;
    this.tx = undefined as unknown as Tx;
  }

  /** Open the database (async because the driver may be node:sqlite). */
  async ready(): Promise<void> {
    if (this.closed) throw new Error("store is closed");
    if (this.db) return;
    this.db = await openSqlite(this.path);
    this.db.exec("PRAGMA journal_mode = WAL;");
    this.db.exec("PRAGMA busy_timeout = 5000;");
    this.db.exec("PRAGMA foreign_keys = ON;");
    this.tx = new Tx(this.db);
    this.migrate();
  }

  /**
   * Apply schema changes to databases created by older plugin versions.
   * `CREATE TABLE IF NOT EXISTS` is a no-op on existing tables, so any column
   * added after the initial release must be added explicitly here. Without
   * this, every insert on a pre-existing database fails with e.g. "table swarm
   * has no column named directory" — the failure that broke swarm_create.
   *
   * Versioned by PRAGMA user_version: the ordered MIGRATIONS list below runs
   * exactly once each, in order. `addMissingColumn` is idempotent and safe to
   * run against already-migrated databases (a no-op when the column exists),
   * so steps may be re-run defensively without corrupting state.
   */
  private migrate(): void {
    this.db.transaction(() => {
      // Step 0: current full schema (CREATE TABLE IF NOT EXISTS — a no-op on
      // existing tables, but creates any table added after the initial release,
      // e.g. swarm_path_claim expires_at is column-level and handled below).
      this.db.exec(SCHEMA);
      this.runMigrations();
      // Defensive re-apply: idempotent, keeps legacy DBs on the latest shape
      // even if a prior migration list was shorter.
      this.addMissingColumn("swarm", "directory", "TEXT NOT NULL DEFAULT ''");
      this.addMissingColumn("swarm_member", "human_chat_at", "INTEGER");
      this.addMissingColumn("swarm_path_claim", "expires_at", "INTEGER");
      // Wave 2 (leases/retries): claim lease columns — mirrored in MIGRATIONS v4;
      // this re-apply is the version-agnostic safety net for pre-chain DBs.
      this.addMissingColumn("swarm_task", "claimed_at", "INTEGER");
      this.addMissingColumn("swarm_task", "lease_expires_at", "INTEGER");
      // Wave 5 (Hive H2): resonant_at — mirrored in MIGRATIONS v7.
      this.addMissingColumn("swarm_belief", "resonant_at", "INTEGER");
    });
  }

  /**
   * Ordered migration chain. Each step is keyed to a `user_version`; the chain
   * advances `user_version` so a database migrated by an older plugin version
   * catches up incrementally instead of only via the defensive re-apply above.
   * Steps must be append-only: never edit a shipped step, only add new ones.
   */
  private static readonly MIGRATIONS: ReadonlyArray<{
    version: number;
    label: string;
    up: (db: SqliteLike) => void;
  }> = [
    {
      version: 1,
      label: "add swarm.directory",
      up: (db) => addColumn(db, "swarm", "directory", "TEXT NOT NULL DEFAULT ''"),
    },
    {
      version: 2,
      label: "add swarm_member.human_chat_at",
      up: (db) => addColumn(db, "swarm_member", "human_chat_at", "INTEGER"),
    },
    {
      version: 3,
      label: "add swarm_path_claim.expires_at",
      up: (db) => addColumn(db, "swarm_path_claim", "expires_at", "INTEGER"),
    },
    {
      version: 4,
      label: "add swarm_task.claimed_at + lease_expires_at (lease/retry lane)",
      up: (db) => {
        addColumn(db, "swarm_task", "claimed_at", "INTEGER");
        addColumn(db, "swarm_task", "lease_expires_at", "INTEGER");
      },
    },
    {
      version: 5,
      label: "create swarm_artifact_annotation (Hive H0)",
      up: (db) => {
        // CREATE TABLE IF NOT EXISTS is a no-op on fresh DBs (SCHEMA already
        // created it); this step exists so legacy DBs also reach v5 and the
        // table/index exist before the first insert.
        db.exec(`
          CREATE TABLE IF NOT EXISTS swarm_artifact_annotation (
            id TEXT PRIMARY KEY,
            swarm_id TEXT NOT NULL,
            path TEXT NOT NULL,
            type TEXT NOT NULL,
            weight INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            error_sig TEXT,
            solution_hash TEXT,
            ttl INTEGER,
            expires_at INTEGER,
            author_member_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
            FOREIGN KEY(author_member_id) REFERENCES swarm_member(id),
            UNIQUE(swarm_id, path, type)
          );
          CREATE INDEX IF NOT EXISTS idx_annotation_swarm
            ON swarm_artifact_annotation(swarm_id, expires_at);
        `);
      },
    },
    {
      version: 6,
      label: "create swarm_belief (Hive H1 belief substrate)",
      up: (db) => {
        // CREATE TABLE IF NOT EXISTS is a no-op on fresh DBs (SCHEMA already
        // created it); this step exists so legacy DBs also reach v6 and the
        // table/index exist before the first insert.
        db.exec(`
          CREATE TABLE IF NOT EXISTS swarm_belief (
            id TEXT PRIMARY KEY,
            swarm_id TEXT NOT NULL,
            fact_hash TEXT NOT NULL,
            text TEXT NOT NULL,
            confidence REAL NOT NULL DEFAULT 0.5,
            tags TEXT,
            tier TEXT NOT NULL DEFAULT 'whisper',
            ttl INTEGER,
            expires_at INTEGER,
            author_member_id TEXT NOT NULL,
            evidence_refs TEXT,
            reinforce_count INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'active',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
            FOREIGN KEY(author_member_id) REFERENCES swarm_member(id),
            UNIQUE(swarm_id, fact_hash)
          );
          CREATE INDEX IF NOT EXISTS idx_belief_swarm
            ON swarm_belief(swarm_id, status, tier, confidence DESC);
        `);
      },
    },
    {
      version: 7,
      label: "add swarm_belief.resonant_at (Hive H2 resonance)",
      up: (db) => addColumn(db, "swarm_belief", "resonant_at", "INTEGER"),
    },
  ];

  private runMigrations(): void {
    let current = this.getUserVersion();
    for (const step of SQLiteStore.MIGRATIONS) {
      if (step.version <= current) continue;
      step.up(this.db);
      current = step.version;
    }
    if (current > this.getUserVersion()) {
      // PRAGMA does not accept bound parameters in bun:sqlite — the version
      // must be a literal (it is an integer we just computed from the chain).
      this.db.exec(`PRAGMA user_version = ${current}`);
    }
  }

  private getUserVersion(): number {
    const r = this.db.query<{ user_version: number }, []>(`PRAGMA user_version`).get();
    return r?.user_version ?? 0;
  }

  private addMissingColumn(table: string, column: string, ddl: string): void {
    addColumn(this.db, table, column, ddl);
  }

  /**
   * Serialize a store operation behind the transaction queue (audit S8).
   *
   * SQLite is a single shared connection: every statement executes on it. A
   * raw store method invoked while `transaction()` holds `BEGIN IMMEDIATE`
   * would execute INSIDE the open transaction — silently joining it, so its
   * writes could be rolled back (or committed) with the txn. Routing every
   * raw method through the same queue as `transaction()` guarantees a raw
   * write NEVER overlaps an open transaction: it either runs before the txn
   * begins or after it ends, never inside it.
   *
   * Re-entrancy is safe: transaction bodies call `tx.*` (the Tx passed in),
   * not these delegates, so no operation here is ever called from inside a
   * queued transaction.
   */
  private serialized<T>(op: () => Promise<T>): Promise<T> {
    const run = this.txnQueue.then(op);
    this.txnQueue = run.then(() => undefined, () => undefined);
    return run;
  }

  transaction<T>(fn: (tx: SwarmStoreTx) => Promise<T>): Promise<T> {
    return this.serialized(async () => {
      this.db.exec("BEGIN IMMEDIATE;");
      const tx = new Tx(this.db);
      try {
        const result = await fn(tx);
        this.db.exec("COMMIT;");
        return result;
      } catch (err) {
        try { this.db.exec("ROLLBACK;"); } catch { /* already aborted */ }
        throw err;
      }
    });
  }

  async close(): Promise<void> {
    // Wait for in-flight transactions before closing.
    await this.txnQueue.catch(() => undefined);
    if (this.db && !this.closed) {
      this.db.close();
    }
    this.closed = true;
  }

  // All raw methods are routed through serialized() (audit S8): a raw write
  // must never join an in-flight transaction on the shared connection.
  insertSwarm(s: NewSwarm): Promise<Swarm> { return this.serialized(() => this.tx.insertSwarm(s)); }
  insertMember(m: NewSwarmMember): Promise<SwarmMember> { return this.serialized(() => this.tx.insertMember(m)); }
  insertTask(t: NewTask): Promise<SwarmTask> { return this.serialized(() => this.tx.insertTask(t)); }
  insertMessages(msgs: NewMessage[]): Promise<SwarmMessage[]> { return this.serialized(() => this.tx.insertMessages(msgs)); }
  insertBlackboard(e: NewBlackboardEntry): Promise<BlackboardEntry> { return this.serialized(() => this.tx.insertBlackboard(e)); }
  insertTaskDependency(taskId: string, dependsOnTaskId: string): Promise<void> { return this.serialized(() => this.tx.insertTaskDependency(taskId, dependsOnTaskId)); }
  deleteMember(memberId: string): Promise<void> { return this.serialized(() => this.tx.deleteMember(memberId)); }
  deleteSwarm(swarmId: string): Promise<void> { return this.serialized(() => this.tx.deleteSwarm(swarmId)); }
  updateSwarmDirectory(swarmId: string, directory: string): Promise<void> { return this.serialized(() => this.tx.updateSwarmDirectory(swarmId, directory)); }
  updateSwarmCoordinator(swarmId: string, coordinatorSessionId: string): Promise<void> { return this.serialized(() => this.tx.updateSwarmCoordinator(swarmId, coordinatorSessionId)); }
  assignMemberSession(memberId: string, sessionId: string): Promise<void> { return this.serialized(() => this.tx.assignMemberSession(memberId, sessionId)); }
  getSwarm(id: string): Promise<Swarm | undefined> { return this.serialized(() => this.tx.getSwarm(id)); }
  getSwarmBySession(sessionID: string): Promise<Swarm | undefined> { return this.serialized(() => this.tx.getSwarmBySession(sessionID)); }
  getSwarmByName(projectId: string, name: string): Promise<Swarm | undefined> { return this.serialized(() => this.tx.getSwarmByName(projectId, name)); }
  listMembers(swarmId: string): Promise<SwarmMember[]> { return this.serialized(() => this.tx.listMembers(swarmId)); }
  getMemberById(memberId: string): Promise<SwarmMember | undefined> { return this.serialized(() => this.tx.getMemberById(memberId)); }
  getMemberBySessionId(sessionID: string): Promise<SwarmMember | undefined> { return this.serialized(() => this.tx.getMemberBySessionId(sessionID)); }
  getMemberByName(swarmId: string, name: string): Promise<SwarmMember | undefined> { return this.serialized(() => this.tx.getMemberByName(swarmId, name)); }
  listTasks(swarmId: string): Promise<SwarmTask[]> { return this.serialized(() => this.tx.listTasks(swarmId)); }
  listTaskDependencies(swarmId: string): Promise<TaskDependency[]> { return this.serialized(() => this.tx.listTaskDependencies(swarmId)); }
  listPathClaims(swarmId: string, now?: number): Promise<PathClaim[]> { return this.serialized(() => this.tx.listPathClaims(swarmId, now)); }
  insertPathClaim(claim: NewPathClaim): Promise<PathClaim> { return this.serialized(() => this.tx.insertPathClaim(claim)); }
  releasePathClaim(claimId: string): Promise<boolean> { return this.serialized(() => this.tx.releasePathClaim(claimId)); }
  deletePathClaim(claimId: string): Promise<void> { return this.serialized(() => this.tx.deletePathClaim(claimId)); }
  refreshPathClaim(claimId: string, ttlMs: number): Promise<PathClaim | undefined> { return this.serialized(() => this.tx.refreshPathClaim(claimId, ttlMs)); }
  listSubscriptions(swarmId: string): Promise<TopicSubscription[]> { return this.serialized(() => this.tx.listSubscriptions(swarmId)); }
  addSubscription(swarmId: string, memberId: string, pattern: string): Promise<TopicSubscription> { return this.serialized(() => this.tx.addSubscription(swarmId, memberId, pattern)); }
  removeSubscription(subscriptionId: string): Promise<void> { return this.serialized(() => this.tx.removeSubscription(subscriptionId)); }
  updateMemberStatus(memberId: string, status: SwarmMember["status"], fields?: Partial<Pick<SwarmMember, "lastActiveAt">> & { currentTaskId?: string | null }): Promise<void> { return this.serialized(() => this.tx.updateMemberStatus(memberId, status, fields)); }
  updateMemberHumanChat(memberId: string, humanChatAt: number | null): Promise<void> { return this.serialized(() => this.tx.updateMemberHumanChat(memberId, humanChatAt)); }
  claimTask(taskId: string, memberId: string, leaseMs?: number): Promise<boolean> { return this.serialized(() => this.tx.claimTask(taskId, memberId, leaseMs)); }
  updateTaskStatus(taskId: string, status: SwarmTask["status"]): Promise<boolean> { return this.serialized(() => this.tx.updateTaskStatus(taskId, status)); }
  releaseTask(taskId: string, opts?: { countAsRetry?: boolean }): Promise<boolean> { return this.serialized(() => this.tx.releaseTask(taskId, opts)); }
  reassignTask(taskId: string, newOwnerMemberId: string): Promise<string | null> { return this.serialized(() => this.tx.reassignTask(taskId, newOwnerMemberId)); }
  listExpiredLeaseTasks(swarmId: string, now: number): Promise<SwarmTask[]> { return this.serialized(() => this.tx.listExpiredLeaseTasks(swarmId, now)); }
  getBlackboard(swarmId: string, key: string): Promise<BlackboardEntry | undefined> { return this.serialized(() => this.tx.getBlackboard(swarmId, key)); }
  searchBlackboard(swarmId: string, query: string): Promise<BlackboardEntry[]> { return this.serialized(() => this.tx.searchBlackboard(swarmId, query)); }
  upsertBlackboard(entry: BlackboardEntry, expectedVersion?: number): Promise<void> { return this.serialized(() => this.tx.upsertBlackboard(entry, expectedVersion)); }
  listPendingMessages(toMemberId: string): Promise<NewMessage[]> { return this.serialized(() => this.tx.listPendingMessages(toMemberId)); }
  listMembersWithPendingMail(): Promise<Array<{ memberId: string; sessionId: string; count: number }>> { return this.serialized(() => this.tx.listMembersWithPendingMail()); }
  listMessagesBySwarm(swarmId: string, limit?: number): Promise<NewMessage[]> { return this.serialized(() => this.tx.listMessagesBySwarm(swarmId, limit)); }
  searchMessagesBySwarm(swarmId: string, query: string, limit?: number): Promise<NewMessage[]> { return this.serialized(() => this.tx.searchMessagesBySwarm(swarmId, query, limit)); }
  getMessageById(messageId: string): Promise<NewMessage | undefined> { return this.serialized(() => this.tx.getMessageById(messageId)); }
  getMessagesByIds(messageIds: string[]): Promise<NewMessage[]> { return this.serialized(() => this.tx.getMessagesByIds(messageIds)); }
  markMessagesScheduled(toMemberId: string, messageIds: string[]): Promise<number> { return this.serialized(() => this.tx.markMessagesScheduled(toMemberId, messageIds)); }
  expireMessage(messageId: string): Promise<void> { return this.serialized(() => this.tx.expireMessage(messageId)); }
  /**
   * Atomically transition every queued/scheduled message whose `expiresAt` has
   * passed to `expired`, and return the affected messages so the caller can
   * send the sender exactly one notice. Runs inside the store's serialized
   * transaction so a concurrent sweep cannot double-claim a row; rows already
   * `expired` are never returned, so the notice path is exactly-once per
   * message (audit/messaging F-M2).
   */
  async expireOverdueMessages(now: number): Promise<NewMessage[]> {
    return this.transaction(async (tx) => {
      // M-6 fix: sweep QUEUED and SCHEDULED rows past expiry. The markDelivered
      // guard (broker) prevents an in-flight wake from delivering an
      // expired-while-scheduled message; this sweep catches any scheduled row
      // whose expiry passed without a completing wake, so it is not left
      // `scheduled` until startup recovery. Rows already `expired` are never
      // returned (exactly-once notice path preserved).
      const rows = this.db.query<RowMessage, [number]>(
        `SELECT * FROM swarm_message
         WHERE expires_at IS NOT NULL AND expires_at <= ?
           AND delivery_state IN ('queued', 'scheduled')`,
      ).all(now);
      for (const r of rows) {
        await tx.expireMessage(r.id);
      }
      return rows.map(toMessage);
    });
  }
  updateMessageDelivery(messageId: string, state: SwarmMessage["deliveryState"]): Promise<void> { return this.serialized(() => this.tx.updateMessageDelivery(messageId, state)); }
  revertMessageToQueuedWithError(messageId: string, toMemberId: string, error: string): Promise<NewMessage | undefined> { return this.serialized(() => this.tx.revertMessageToQueuedWithError(messageId, toMemberId, error)); }
  markMessageFailed(messageId: string): Promise<NewMessage | undefined> { return this.serialized(() => this.tx.markMessageFailed(messageId)); }
  revertMessageToQueued(messageId: string, toMemberId: string): Promise<void> { return this.serialized(() => this.tx.revertMessageToQueued(messageId, toMemberId)); }
  listAllMemberSwarmIds(): Promise<string[]> { return this.serialized(() => this.tx.listAllMemberSwarmIds()); }
  revertStaleScheduledForSwarm(swarmId: string): Promise<number> { return this.serialized(() => this.tx.revertStaleScheduledForSwarm(swarmId)); }

  // ==== Hive H0 artifact annotations ====

  insertAnnotation(a: NewArtifactAnnotation): Promise<ArtifactAnnotation> { return this.serialized(() => this.tx.insertAnnotation(a)); }
  listAnnotations(swarmId: string, opts?: { path?: string; activeOnly?: boolean; now?: number }): Promise<ArtifactAnnotation[]> { return this.serialized(() => this.tx.listAnnotations(swarmId, opts)); }
  releaseOrDeleteAnnotation(annotationId: string): Promise<boolean> { return this.serialized(() => this.tx.releaseOrDeleteAnnotation(annotationId)); }

  // ==== Hive H1 beliefs ====

  insertBelief(b: NewBelief): Promise<Belief> { return this.serialized(() => this.tx.insertBelief(b)); }
  reinforceBelief(swarmId: string, factHash: string, deltaConfidence?: number): Promise<Belief | undefined> { return this.serialized(() => this.tx.reinforceBelief(swarmId, factHash, deltaConfidence)); }
  upgradeWhisperToShout(swarmId: string, factHash: string): Promise<Belief | undefined> { return this.serialized(() => this.tx.upgradeWhisperToShout(swarmId, factHash)); }
  listBeliefs(swarmId: string, opts?: { activeOnly?: boolean; tier?: Belief["tier"]; minConfidence?: number; query?: string; status?: Belief["status"]; now?: number }): Promise<Belief[]> { return this.serialized(() => this.tx.listBeliefs(swarmId, opts)); }
  expireBeliefs(now: number): Promise<number> { return this.serialized(() => this.tx.expireBeliefs(now)); }
  markResonant(swarmId: string, factHash: string): Promise<Belief | undefined> { return this.serialized(() => this.tx.markResonant(swarmId, factHash)); }
  beliefEvidenceDisjoint(a: Belief, b: Belief): boolean { return beliefEvidenceDisjoint(a, b); }
  listBeliefsForPruning(swarmId: string, opts?: { maxConfidence?: number; minReinforce?: number; olderThanMs?: number; limit?: number }): Promise<Belief[]> { return this.serialized(() => this.tx.listBeliefsForPruning(swarmId, opts)); }
  softPruneBelief(swarmId: string, factHash: string, to: Extract<BeliefStatus, "superseded" | "expired">): Promise<Belief | undefined> { return this.serialized(() => this.tx.softPruneBelief(swarmId, factHash, to)); }
  hardPruneBeliefs(swarmId: string, factHashes: string[]): Promise<number> { return this.serialized(() => this.tx.hardPruneBeliefs(swarmId, factHashes)); }
  beliefDigest(swarmId: string): Promise<{ digest: string; count: number }> { return this.serialized(() => this.tx.beliefDigest(swarmId)); }
  listBeliefsChangedSince(swarmId: string, since: number): Promise<Belief[]> { return this.serialized(() => this.tx.listBeliefsChangedSince(swarmId, since)); }
}

class Tx implements SwarmStoreTx {
  constructor(private db: SqliteLike) {}

  async insertSwarm(s: NewSwarm): Promise<Swarm> {
    this.db.run(
      `INSERT INTO swarm (id, project_id, name, coordinator_session_id, coordinator_member_id, directory, status, policies_json, created_at, updated_at, completed_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
      [s.id, s.projectId, s.name, s.coordinatorSessionId, s.coordinatorMemberId, s.directory ?? "", s.status, JSON.stringify(s.policies), s.createdAt, s.updatedAt, s.completedAt ?? null],
    );
    return s as Swarm;
  }

  async insertMember(m: NewSwarmMember): Promise<SwarmMember> {
    this.db.run(
      `INSERT INTO swarm_member (id, swarm_id, name, role, session_id, agent, provider_id, model_id, status, workspace_mode, workspace_path, branch, current_task_id, created_at, updated_at, last_active_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [m.id, m.swarmId, m.name, m.role, m.sessionId, m.agent ?? null, m.model?.providerID ?? null, m.model?.modelID ?? null, m.status, m.workspaceMode, m.workspacePath ?? null, m.branch ?? null, m.currentTaskId ?? null, m.createdAt, m.updatedAt, m.lastActiveAt ?? null],
    );
    return m as SwarmMember;
  }

  async insertTask(t: NewTask): Promise<SwarmTask> {
    // retry_count is first-class (retryCount field); keep the legacy metadata
    // fallback for tasks created before the field existed, but never let the
    // metadata hack be the source of truth going forward.
    const retryCount =
      typeof t.retryCount === "number"
        ? t.retryCount
        : typeof t.metadata?.retryCount === "number"
          ? t.metadata.retryCount
          : 0;
    this.db.run(
      `INSERT INTO swarm_task (id, swarm_id, title, description, status, priority, owner_member_id, created_by_member_id, acceptance_json, metadata_json, retry_count, claimed_at, lease_expires_at, created_at, updated_at, completed_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [t.id, t.swarmId, t.title, t.description ?? null, t.status, t.priority, t.ownerMemberId ?? null, t.createdByMemberId, t.acceptanceCriteria ? JSON.stringify(t.acceptanceCriteria) : null, t.metadata ? JSON.stringify(t.metadata) : null, retryCount, t.claimedAt ?? null, t.leaseExpiresAt ?? null, t.createdAt, t.updatedAt, t.completedAt ?? null],
    );
    return t as SwarmTask;
  }

  async insertMessages(msgs: NewMessage[]): Promise<SwarmMessage[]> {
    const stmt = this.db.prepare(
      `INSERT INTO swarm_message (id, swarm_id, from_member_id, to_member_id, is_broadcast, kind, task_id, correlation_id, response_to, priority, body_text, refs_json, delivery_state, attempt_count, created_at, delivered_at, acknowledged_at, expires_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    );
    for (const m of msgs) {
      stmt.run(
        m.id, m.swarmId, m.fromMemberId,
        m.to.type === "member" ? m.to.memberId ?? null : null,
        m.to.type === "broadcast" ? 1 : 0,
        m.kind, m.taskId ?? null, m.correlationId ?? null, m.responseTo ?? null,
        m.priority, m.body.text,
        m.body.refs && m.body.refs.length ? JSON.stringify(m.body.refs) : null,
        m.deliveryState, m.attemptCount, m.createdAt,
        m.deliveredAt ?? null, m.acknowledgedAt ?? null, m.expiresAt ?? null,
      );
    }
    return msgs as SwarmMessage[];
  }

  async insertBlackboard(e: NewBlackboardEntry): Promise<BlackboardEntry> {
    this.db.run(
      `INSERT INTO swarm_blackboard (id, swarm_id, key, value, content_type, version, author_member_id, task_id, created_at, updated_at)
       VALUES (?,?,?,?,?,?,?,?,?,?)`,
      [e.id, e.swarmId, e.key, e.value, e.contentType, e.version, e.authorMemberId, e.taskId ?? null, e.createdAt, e.updatedAt],
    );
    return e as BlackboardEntry;
  }

  async insertTaskDependency(taskId: string, dependsOnTaskId: string): Promise<void> {
    this.db.run(
      `INSERT OR IGNORE INTO swarm_task_dependency (task_id, depends_on_task_id) VALUES (?,?)`,
      [taskId, dependsOnTaskId],
    );
  }

  async deleteMember(memberId: string): Promise<void> {
    // Edge S2: the blackboard/annotation/belief author_member_id FKs have no
    // ON DELETE action, so a plain member DELETE would throw FOREIGN KEY
    // constraint failed when the member authored any row (blocking swarm_remove
    // forever). Cascade the authored rows first so removal always succeeds.
    this.db.run(`DELETE FROM swarm_blackboard WHERE author_member_id = ?`, [memberId]);
    this.db.run(`DELETE FROM swarm_artifact_annotation WHERE author_member_id = ?`, [memberId]);
    this.db.run(`DELETE FROM swarm_belief WHERE author_member_id = ?`, [memberId]);
    this.db.run(`DELETE FROM swarm_member WHERE id = ?`, [memberId]);
  }

  async deleteSwarm(swarmId: string): Promise<void> {
    this.db.run(`DELETE FROM swarm WHERE id = ?`, [swarmId]);
  }

  async updateSwarmDirectory(swarmId: string, directory: string): Promise<void> {
    this.db.run(`UPDATE swarm SET directory = ?, updated_at = ? WHERE id = ?`, [directory, Date.now(), swarmId]);
  }

  async updateSwarmCoordinator(swarmId: string, coordinatorSessionId: string): Promise<void> {
    this.db.run(
      `UPDATE swarm SET coordinator_session_id = ?, updated_at = ? WHERE id = ?`,
      [coordinatorSessionId, Date.now(), swarmId],
    );
  }

  async assignMemberSession(memberId: string, sessionId: string): Promise<void> {
    this.db.run(
      `UPDATE swarm_member SET session_id = ?, updated_at = ? WHERE id = ?`,
      [sessionId, Date.now(), memberId],
    );
  }

  async getSwarm(id: string): Promise<Swarm | undefined> {
    const r = this.db.query<RowSwarm, [string]>(
      `SELECT * FROM swarm WHERE id = ?`,
    ).get(id);
    return r ? toSwarm(r) : undefined;
  }

  async getSwarmBySession(sessionID: string): Promise<Swarm | undefined> {
    const r = this.db.query<{ swarm_id: string }, [string]>(
      `SELECT swarm_id FROM swarm_member WHERE session_id = ? LIMIT 1`,
    ).get(sessionID);
    if (!r) return undefined;
    return this.getSwarm(r.swarm_id);
  }

  async getSwarmByName(projectId: string, name: string): Promise<Swarm | undefined> {
    const r = this.db.query<RowSwarm, [string, string]>(
      `SELECT * FROM swarm WHERE project_id = ? AND name = ?`,
    ).get(projectId, name);
    return r ? toSwarm(r) : undefined;
  }

  async listMembers(swarmId: string): Promise<SwarmMember[]> {
    const rows = this.db.query<RowMember, [string]>(
      `SELECT * FROM swarm_member WHERE swarm_id = ? ORDER BY created_at`,
    ).all(swarmId);
    return rows.map(toMember);
  }

  async getMemberById(memberId: string): Promise<SwarmMember | undefined> {
    const r = this.db.query<RowMember, [string]>(
      `SELECT * FROM swarm_member WHERE id = ?`,
    ).get(memberId);
    return r ? toMember(r) : undefined;
  }

  async getMemberBySessionId(sessionID: string): Promise<SwarmMember | undefined> {
    const r = this.db.query<RowMember, [string]>(
      `SELECT * FROM swarm_member WHERE session_id = ? LIMIT 1`,
    ).get(sessionID);
    return r ? toMember(r) : undefined;
  }

  async getMemberByName(swarmId: string, name: string): Promise<SwarmMember | undefined> {
    const r = this.db.query<RowMember, [string, string]>(
      `SELECT * FROM swarm_member WHERE swarm_id = ? AND name = ?`,
    ).get(swarmId, name);
    return r ? toMember(r) : undefined;
  }

  async listTasks(swarmId: string): Promise<SwarmTask[]> {
    const rows = this.db.query<RowTask, [string]>(
      `SELECT * FROM swarm_task WHERE swarm_id = ? ORDER BY priority DESC, created_at`,
    ).all(swarmId);
    return rows.map(toTask);
  }

  async listExpiredLeaseTasks(swarmId: string, now: number): Promise<SwarmTask[]> {
    const rows = this.db.query<RowTask, [string, number]>(
      `SELECT * FROM swarm_task
       WHERE swarm_id = ? AND status IN ('claimed','working')
         AND lease_expires_at IS NOT NULL AND lease_expires_at < ?`,
    ).all(swarmId, now);
    return rows.map(toTask);
  }

  async listTaskDependencies(swarmId: string): Promise<TaskDependency[]> {
    const rows = this.db.query<{ task_id: string; depends_on_task_id: string }, [string]>(
      `SELECT d.task_id, d.depends_on_task_id
       FROM swarm_task_dependency d
       JOIN swarm_task t ON t.id = d.task_id
       WHERE t.swarm_id = ?`,
    ).all(swarmId);
    return rows.map((r) => ({ taskId: r.task_id, dependsOnTaskId: r.depends_on_task_id }));
  }

  async listPathClaims(swarmId: string, now = Date.now()): Promise<PathClaim[]> {
    // Active claims only: released rows excluded, and TTL claims whose
    // expires_at has passed are not counted as active (advisory TTL — stale
    // rows are invisible to consumers without an explicit release).
    const rows = this.db.query<RowPathClaim, [string, number]>(
      `SELECT * FROM swarm_path_claim
       WHERE swarm_id = ? AND released_at IS NULL
         AND (expires_at IS NULL OR expires_at > ?)
       ORDER BY created_at`,
    ).all(swarmId, now);
    return rows.map(toPathClaim);
  }

  async insertPathClaim(claim: NewPathClaim): Promise<PathClaim> {
    // UNIQUE(swarm_id, member_id, pattern) enforced for ACTIVE claims by the
    // partial index idx_path_claim_active_uniq (WHERE released_at IS NULL): a
    // member can hold at most one active claim per pattern. Released/expired
    // rows are outside the uniqueness scope, so re-claiming a lane works.
    this.db.run(
      `INSERT INTO swarm_path_claim (id, swarm_id, member_id, pattern, mode, created_at, expires_at, released_at)
       VALUES (?,?,?,?,?,?,?,?)`,
      [claim.id, claim.swarmId, claim.memberId, claim.pattern, claim.mode ?? "advisory", claim.createdAt, claim.expiresAt ?? null, claim.releasedAt ?? null],
    );
    return claim as PathClaim;
  }

  async releasePathClaim(claimId: string): Promise<boolean> {
    const res = this.db.run(
      `UPDATE swarm_path_claim SET released_at = ? WHERE id = ? AND released_at IS NULL`,
      [Date.now(), claimId],
    );
    return res.changes > 0;
  }

  async deletePathClaim(claimId: string): Promise<void> {
    this.db.run(`DELETE FROM swarm_path_claim WHERE id = ?`, [claimId]);
  }

  async refreshPathClaim(claimId: string, ttlMs: number): Promise<PathClaim | undefined> {
    const now = Date.now();
    // WIP Aura heartbeat: extend expires_at by ttlMs. Also clears released_at
    // so a refreshed claim counts as active again (the owner is still working
    // the lane), and never lets a claim expire while being refreshed.
    const res = this.db.run(
      `UPDATE swarm_path_claim
       SET expires_at = ?, released_at = NULL, created_at = created_at
       WHERE id = ?`,
      [now + ttlMs, claimId],
    );
    if (res.changes === 0) return undefined;
    const r = this.db.query<RowPathClaim, [string]>(
      `SELECT * FROM swarm_path_claim WHERE id = ?`,
    ).get(claimId);
    return r ? toPathClaim(r) : undefined;
  }

  // ==== Hive H0 artifact annotations ====

  async insertAnnotation(a: NewArtifactAnnotation): Promise<ArtifactAnnotation> {
    // UNIQUE(swarm_id, path, type): a fresh annotation for the same path/type
    // REPLACES the previous one (scent is point-in-time, latest wins). TTL is
    // converted to expiresAt here so callers never manage the derivation.
    // Edge S9: ttl<=0 means "no expiry" (null expiresAt) — a zero/negative TTL
    // must not make the row instantly stale.
    const now = a.createdAt;
    const expiresAt = a.expiresAt ?? (a.ttl !== undefined && a.ttl > 0 ? now + a.ttl : undefined);
    this.db.run(
      `INSERT INTO swarm_artifact_annotation
         (id, swarm_id, path, type, weight, note, error_sig, solution_hash, ttl, expires_at, author_member_id, created_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
       ON CONFLICT(swarm_id, path, type) DO UPDATE SET
         weight = excluded.weight,
         note = excluded.note,
         error_sig = excluded.error_sig,
         solution_hash = excluded.solution_hash,
         ttl = excluded.ttl,
         expires_at = excluded.expires_at,
         author_member_id = excluded.author_member_id,
         created_at = excluded.created_at`,
      [a.id, a.swarmId, a.path, a.type, a.weight, a.note ?? null, a.errorSig ?? null, a.solutionHash ?? null, a.ttl ?? null, expiresAt ?? null, a.authorMemberId, now],
    );
    // Edge S1: on replace, the stored row KEEPS its original id (ON CONFLICT
    // updates in place). Re-query by (swarm_id, path, type) so the returned row
    // is the real stored row — the returned id must be usable for
    // releaseOrDeleteAnnotation (mirrors insertBelief).
    const r = this.db.query<RowAnnotation, [string, string, string]>(
      `SELECT * FROM swarm_artifact_annotation WHERE swarm_id = ? AND path = ? AND type = ?`,
    ).get(a.swarmId, a.path, a.type);
    if (r) return toAnnotation(r);
    return {
      id: a.id, swarmId: a.swarmId, path: a.path, type: a.type,
      weight: a.weight,
      note: a.note,
      errorSig: a.errorSig,
      solutionHash: a.solutionHash,
      ttl: a.ttl,
      expiresAt,
      authorMemberId: a.authorMemberId,
      createdAt: now,
    };
  }

  async listAnnotations(
    swarmId: string,
    opts?: { path?: string; activeOnly?: boolean; now?: number },
  ): Promise<ArtifactAnnotation[]> {
    const activeOnly = opts?.activeOnly ?? true;
    const now = opts?.now ?? Date.now();
    const conditions = [`swarm_id = ?`];
    const params: unknown[] = [swarmId];
    if (opts?.path !== undefined) {
      conditions.push(`path = ?`);
      params.push(opts.path);
    }
    if (activeOnly) {
      // Stale/expired exclusion: no expires_at = never stale; otherwise only
      // rows whose expiry is still in the future.
      conditions.push(`(expires_at IS NULL OR expires_at > ?)`);
      params.push(now);
    }
    const rows = this.db.query<RowAnnotation, unknown[]>(
      `SELECT * FROM swarm_artifact_annotation
       WHERE ${conditions.join(" AND ")}
       ORDER BY created_at DESC`,
    ).all(...params);
    return rows.map(toAnnotation);
  }

  async releaseOrDeleteAnnotation(annotationId: string): Promise<boolean> {
    const res = this.db.run(
      `DELETE FROM swarm_artifact_annotation WHERE id = ?`,
      [annotationId],
    );
    return res.changes > 0;
  }

  // ==== Hive H1 beliefs ====

  async insertBelief(b: NewBelief): Promise<Belief> {
    const now = b.createdAt;
    // Edge S9: ttl<=0 means "no expiry" (null expiresAt).
    const expiresAt = b.expiresAt ?? (b.ttl !== undefined && b.ttl > 0 ? now + b.ttl : undefined);
    const status = b.status ?? "active";
    // Edge S7: clamp confidence 0..1 on insert (was stored verbatim).
    const confidence = Math.min(1, Math.max(0, b.confidence));
    // Dedupe by fact_hash (UNIQUE(swarm_id, fact_hash)): on conflict, REINFORCE
    // instead of replace — reinforce_count +1, confidence bumped toward 1
    // (lateral inhibition, item 7). Edge S5: a re-insert of a fact that was
    // soft-pruned (superseded/expired) REVIVES it — status resets to 'active'
    // and expires_at refreshes — so re-publishing a fact is not silently lost
    // on a dead row. Resonant rows keep their status (resonance is sticky).
    this.db.run(
      `INSERT INTO swarm_belief
         (id, swarm_id, fact_hash, text, confidence, tags, tier, ttl, expires_at, author_member_id, evidence_refs, reinforce_count, status, created_at, updated_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
       ON CONFLICT(swarm_id, fact_hash) DO UPDATE SET
         reinforce_count = swarm_belief.reinforce_count + 1,
         confidence = MIN(1.0, swarm_belief.confidence + 0.1),
         evidence_refs = excluded.evidence_refs,
         status = CASE WHEN swarm_belief.status IN ('superseded','expired')
                       THEN 'active' ELSE swarm_belief.status END,
         expires_at = excluded.expires_at,
         updated_at = excluded.updated_at`,
      [b.id, b.swarmId, b.factHash, b.text, confidence, b.tags ?? null, b.tier, b.ttl ?? null, expiresAt ?? null, b.authorMemberId, b.evidenceRefs ?? null, b.reinforceCount ?? 1, status, now, b.updatedAt],
    );
    // Return the (possibly reinforced) row.
    const r = this.db.query<RowBelief, [string, string]>(
      `SELECT * FROM swarm_belief WHERE swarm_id = ? AND fact_hash = ?`,
    ).get(b.swarmId, b.factHash);
    return r ? toBelief(r) : {
      id: b.id, swarmId: b.swarmId, factHash: b.factHash, text: b.text,
      confidence, tags: b.tags, tier: b.tier,
      ttl: b.ttl, expiresAt, authorMemberId: b.authorMemberId,
      evidenceRefs: b.evidenceRefs, reinforceCount: b.reinforceCount ?? 1,
      status, createdAt: now, updatedAt: b.updatedAt,
    };
  }

  async reinforceBelief(swarmId: string, factHash: string, deltaConfidence = 0.1): Promise<Belief | undefined> {
    const res = this.db.run(
      `UPDATE swarm_belief
       SET reinforce_count = reinforce_count + 1,
           confidence = MIN(1.0, MAX(0.0, confidence + ?)),
           updated_at = ?
       WHERE swarm_id = ? AND fact_hash = ?`,
      [deltaConfidence, Date.now(), swarmId, factHash],
    );
    if (res.changes === 0) return undefined;
    const r = this.db.query<RowBelief, [string, string]>(
      `SELECT * FROM swarm_belief WHERE swarm_id = ? AND fact_hash = ?`,
    ).get(swarmId, factHash);
    return r ? toBelief(r) : undefined;
  }

  async upgradeWhisperToShout(swarmId: string, factHash: string): Promise<Belief | undefined> {
    // Whisper → shout when reinforce_count >= 2 (item 8). The store owns the
    // RULE; the caller (hive tool) decides WHEN to invoke it so its output is
    // truthful. Returns undefined when missing or not eligible.
    const res = this.db.run(
      `UPDATE swarm_belief
       SET tier = 'shout', updated_at = ?
       WHERE swarm_id = ? AND fact_hash = ? AND tier = 'whisper' AND reinforce_count >= 2`,
      [Date.now(), swarmId, factHash],
    );
    if (res.changes === 0) return undefined;
    const r = this.db.query<RowBelief, [string, string]>(
      `SELECT * FROM swarm_belief WHERE swarm_id = ? AND fact_hash = ?`,
    ).get(swarmId, factHash);
    return r ? toBelief(r) : undefined;
  }

  async listBeliefs(
    swarmId: string,
    opts?: { activeOnly?: boolean; tier?: Belief["tier"]; minConfidence?: number; query?: string; status?: Belief["status"]; now?: number },
  ): Promise<Belief[]> {
    // Edge S6: an EXPLICIT status filter overrides the activeOnly default —
    // querying {status:'expired'} must actually return expired rows without
    // the caller remembering to pass activeOnly:false. activeOnly only applies
    // when no explicit status is given (its default stays true).
    const activeOnly = opts?.status === undefined ? (opts?.activeOnly ?? true) : (opts?.activeOnly ?? false);
    const now = opts?.now ?? Date.now();
    const conditions = [`swarm_id = ?`];
    const params: unknown[] = [swarmId];
    if (opts?.tier !== undefined) {
      conditions.push(`tier = ?`);
      params.push(opts.tier);
    }
    if (opts?.status !== undefined) {
      conditions.push(`status = ?`);
      params.push(opts.status);
    }
    if (opts?.minConfidence !== undefined) {
      conditions.push(`confidence >= ?`);
      params.push(opts.minConfidence);
    }
    if (opts?.query !== undefined && opts.query !== "") {
      conditions.push(`lower(text) LIKE ? ESCAPE '\\'`);
      params.push(`%${escapeLike(opts.query.toLowerCase())}%`);
    }
    if (activeOnly) {
      // Exclude terminal statuses AND rows whose TTL passed.
      conditions.push(`status NOT IN ('expired','superseded')`);
      conditions.push(`(expires_at IS NULL OR expires_at > ?)`);
      params.push(now);
    }
    const rows = this.db.query<RowBelief, unknown[]>(
      `SELECT * FROM swarm_belief
       WHERE ${conditions.join(" AND ")}
       ORDER BY confidence DESC, created_at DESC`,
    ).all(...params);
    return rows.map(toBelief);
  }

  async expireBeliefs(now: number): Promise<number> {
    const res = this.db.run(
      `UPDATE swarm_belief SET status = 'expired', updated_at = ?
       WHERE status = 'active' AND expires_at IS NOT NULL AND expires_at <= ?`,
      [now, now],
    );
    return res.changes;
  }

  // ==== Hive H2 resonance / consolidation / anti-entropy ====

  async markResonant(swarmId: string, factHash: string): Promise<Belief | undefined> {
    // active→resonant (item 10). Idempotent: already-resonant is a no-op that
    // returns the row. Caller (resonance detector) decides WHEN.
    const now = Date.now();
    const res = this.db.run(
      `UPDATE swarm_belief SET status = 'resonant', resonant_at = ?, updated_at = ?
       WHERE swarm_id = ? AND fact_hash = ? AND status = 'active'`,
      [now, now, swarmId, factHash],
    );
    if (res.changes === 0) {
      // Either missing, or already resonant/superseded/expired — read and
      // return if present (idempotent no-op for already-resonant).
      const existing = this.db.query<RowBelief, [string, string]>(
        `SELECT * FROM swarm_belief WHERE swarm_id = ? AND fact_hash = ?`,
      ).get(swarmId, factHash);
      return existing ? toBelief(existing) : undefined;
    }
    const r = this.db.query<RowBelief, [string, string]>(
      `SELECT * FROM swarm_belief WHERE swarm_id = ? AND fact_hash = ?`,
    ).get(swarmId, factHash);
    return r ? toBelief(r) : undefined;
  }

  async listBeliefsForPruning(
    swarmId: string,
    opts?: { maxConfidence?: number; minReinforce?: number; olderThanMs?: number; limit?: number },
  ): Promise<Belief[]> {
    // Pruning candidates (item 12): low confidence AND low reuse AND old.
    const conditions = [`swarm_id = ?`, `status = 'active'`];
    const params: unknown[] = [swarmId];
    if (opts?.maxConfidence !== undefined) {
      conditions.push(`confidence <= ?`);
      params.push(opts.maxConfidence);
    }
    if (opts?.minReinforce !== undefined) {
      conditions.push(`reinforce_count <= ?`);
      params.push(opts.minReinforce);
    }
    if (opts?.olderThanMs !== undefined) {
      conditions.push(`updated_at < ?`);
      params.push(Date.now() - opts.olderThanMs);
    }
    const limit = opts?.limit ?? 50;
    const rows = this.db.query<RowBelief, unknown[]>(
      `SELECT * FROM swarm_belief
       WHERE ${conditions.join(" AND ")}
       ORDER BY confidence ASC, updated_at ASC
       LIMIT ?`,
    ).all(...params, limit);
    return rows.map(toBelief);
  }

  async softPruneBelief(
    swarmId: string,
    factHash: string,
    to: Extract<BeliefStatus, "superseded" | "expired">,
  ): Promise<Belief | undefined> {
    // Row kept (causal chain via evidence_refs intact); only status moves.
    const res = this.db.run(
      `UPDATE swarm_belief SET status = ?, updated_at = ?
       WHERE swarm_id = ? AND fact_hash = ?`,
      [to, Date.now(), swarmId, factHash],
    );
    if (res.changes === 0) return undefined;
    const r = this.db.query<RowBelief, [string, string]>(
      `SELECT * FROM swarm_belief WHERE swarm_id = ? AND fact_hash = ?`,
    ).get(swarmId, factHash);
    return r ? toBelief(r) : undefined;
  }

  async hardPruneBeliefs(swarmId: string, factHashes: string[]): Promise<number> {
    if (factHashes.length === 0) return 0;
    // Chunked DELETE to stay within SQLite's variable limit (999 per stmt).
    let deleted = 0;
    for (let i = 0; i < factHashes.length; i += 900) {
      const chunk = factHashes.slice(i, i + 900);
      const placeholders = chunk.map(() => "?").join(",");
      const res = this.db.run(
        `DELETE FROM swarm_belief WHERE swarm_id = ? AND fact_hash IN (${placeholders})`,
        [swarmId, ...chunk],
      );
      deleted += res.changes;
    }
    return deleted;
  }

  async beliefDigest(swarmId: string): Promise<{ digest: string; count: number }> {
    // Anti-entropy digest (item 12): sha1 over ACTIVE beliefs' (id, updated_at,
    // reinforce_count, confidence, tier) tuples, sorted for cross-peer
    // stability. Two peers with identical beliefs produce identical digests;
    // divergence is detected by comparison.
    //
    // Edge S3: id+updated_at alone was a same-ms blind spot — a reinforce
    // within the same millisecond as the prior write left the digest unchanged
    // even though reinforce_count changed, so anti-entropy never detected the
    // divergence. Including monotonic state fields (reinforce_count,
    // confidence, tier) makes any belief mutation change the digest.
    const rows = this.db.query<
      { id: string; updated_at: number; reinforce_count: number; confidence: number; tier: string },
      [string]
    >(
      `SELECT id, updated_at, reinforce_count, confidence, tier FROM swarm_belief
       WHERE swarm_id = ? AND status = 'active'`,
    ).all(swarmId);
    const tuples = rows
      .map((r) => `${r.id}:${r.updated_at}:${r.reinforce_count}:${r.confidence}:${r.tier}`)
      .sort();
    const canonical = tuples.join("\n");
    const digest = await sha1Hex(canonical);
    return { digest, count: rows.length };
  }

  async listBeliefsChangedSince(swarmId: string, since: number): Promise<Belief[]> {
    const rows = this.db.query<RowBelief, [string, number]>(
      `SELECT * FROM swarm_belief
       WHERE swarm_id = ? AND updated_at > ?
       ORDER BY updated_at`,
    ).all(swarmId, since);
    return rows.map(toBelief);
  }

  async listSubscriptions(swarmId: string): Promise<TopicSubscription[]> {
    const rows = this.db.query<{ id: string; swarm_id: string; member_id: string; pattern: string; created_at: number }, [string]>(
      `SELECT * FROM swarm_subscription WHERE swarm_id = ? ORDER BY created_at`,
    ).all(swarmId);
    return rows.map((r) => ({ id: r.id, swarmId: r.swarm_id, memberId: r.member_id, pattern: r.pattern, createdAt: r.created_at }));
  }

  async addSubscription(swarmId: string, memberId: string, pattern: string): Promise<TopicSubscription> {
    const id = `sub_${crypto.randomUUID().replace(/-/g, "")}`;
    this.db.run(
      `INSERT OR IGNORE INTO swarm_subscription (id, swarm_id, member_id, pattern, created_at) VALUES (?,?,?,?,?)`,
      [id, swarmId, memberId, pattern, Date.now()],
    );
    const r = this.db.query<{ id: string; swarm_id: string; member_id: string; pattern: string; created_at: number }, [string, string, string]>(
      `SELECT * FROM swarm_subscription WHERE swarm_id = ? AND member_id = ? AND pattern = ?`,
    ).get(swarmId, memberId, pattern);
    return { id: r?.id ?? id, swarmId, memberId, pattern, createdAt: r?.created_at ?? Date.now() };
  }

  async removeSubscription(subscriptionId: string): Promise<void> {
    this.db.run(`DELETE FROM swarm_subscription WHERE id = ?`, [subscriptionId]);
  }

  async updateMemberStatus(
    memberId: string,
    status: SwarmMember["status"],
    fields?: Partial<Pick<SwarmMember, "lastActiveAt">> & { currentTaskId?: string | null },
  ): Promise<void> {
    const current = fields?.currentTaskId === undefined
      ? this.getCurrentTask(memberId)
      : fields.currentTaskId;
    // Ownership guard (audit S9 / build-pathclaims-schema): an EXPLICIT non-null
    // currentTaskId bind must reference a task in the same swarm that is either
    // owned by this member or unowned (claim-in-flight / reservation). Binding a
    // member to a task owned by a DIFFERENT member is the corruption behind the
    // affinity-misassignment class — reject it. NULL clears and status-only
    // updates (undefined) are never validated.
    if (current !== null && current !== undefined) {
      const task = this.db.query<{ swarm_id: string; owner_member_id: string | null }, [string]>(
        `SELECT swarm_id, owner_member_id FROM swarm_task WHERE id = ?`,
      ).get(current);
      if (!task) {
        throw new Error(`updateMemberStatus: task '${current}' does not exist`);
      }
      const member = this.db.query<{ swarm_id: string }, [string]>(
        `SELECT swarm_id FROM swarm_member WHERE id = ?`,
      ).get(memberId);
      if (!member || task.swarm_id !== member.swarm_id) {
        throw new Error(`updateMemberStatus: task '${current}' is not in the same swarm as member '${memberId}'`);
      }
      if (task.owner_member_id !== null && task.owner_member_id !== memberId) {
        throw new Error(
          `updateMemberStatus: member '${memberId}' does not own task '${current}' (owned by '${task.owner_member_id}')`,
        );
      }
    }
    const lastActive = fields?.lastActiveAt ?? Date.now();
    this.db.run(
      `UPDATE swarm_member SET status = ?, current_task_id = ?, last_active_at = ?, updated_at = ? WHERE id = ?`,
      [status, current, lastActive, Date.now(), memberId],
    );
  }

  async updateMemberHumanChat(memberId: string, humanChatAt: number | null): Promise<void> {
    this.db.run(
      `UPDATE swarm_member SET human_chat_at = ?, updated_at = ? WHERE id = ?`,
      [humanChatAt, Date.now(), memberId],
    );
  }

  private getCurrentTask(memberId: string): string | null {
    const r = this.db
      .query<{ current_task_id: string | null }, [string]>(
        "SELECT current_task_id FROM swarm_member WHERE id = ?",
      )      .get(memberId);
    return r?.current_task_id ?? null;
  }

  async claimTask(taskId: string, memberId: string, leaseMs?: number): Promise<boolean> {
    const now = Date.now();
    // claimed_at anchors the lease; lease_expires_at = now + leaseMs (null when
    // no lease configured). The sweep releases claimed/working tasks whose
    // lease_expires_at is in the past (respecting the human-chat guard).
    // S-04: member-side CAS — a member that ALREADY owns a DIFFERENT
    // non-terminal task (current_task_id set to something else) must not claim
    // another. The claim is allowed when the member's current_task_id is NULL
    // OR already equals this task (spawnMember pre-sets current_task_id to the
    // task it is about to claim). The caller wraps this in serialized()
    // (BEGIN IMMEDIATE txnQueue), so the read-then-claim is atomic: two
    // concurrent claims of DIFFERENT tasks by the same member cannot both pass
    // (R1 double-claim TOCTOU closed).
    const member = this.db.query<{ current_task_id: string | null }, [string]>(
      `SELECT current_task_id FROM swarm_member WHERE id = ?`,
    ).get(memberId);
    if (member && member.current_task_id && member.current_task_id !== taskId) return false;
    // S-10 lease convention (aligned with Storage's ttl=0 = "no expiry"):
    // leaseMs <= 0 (or absent) means NO lease — lease_expires_at NULL, never
    // sweepable. Only a POSITIVE leaseMs anchors an expiry. This makes a
    // mis-set `taskLeaseMs: 0` mean "no lease" (intended), never an instant
    // expiry that would sweep the task immediately.
    const leaseExpiresAt = (leaseMs !== undefined && leaseMs !== null && leaseMs > 0) ? now + leaseMs : null;
    const res = this.db.run(
      `UPDATE swarm_task
       SET owner_member_id = ?, status = 'claimed', claimed_at = ?, lease_expires_at = ?, updated_at = ?
       WHERE id = ? AND owner_member_id IS NULL AND status = 'ready'`,
      [memberId, now, leaseExpiresAt, now, taskId],
    );
    return res.changes > 0;
  }

  async updateTaskStatus(taskId: string, status: SwarmTask["status"]): Promise<boolean> {
    const res = this.db.run(
      `UPDATE swarm_task SET status = ?, updated_at = ?,
         completed_at = CASE WHEN ? IN ('completed','failed','cancelled') THEN ? ELSE completed_at END
       WHERE id = ? AND status NOT IN ('completed','failed','cancelled')`,
      [status, Date.now(), status, Date.now(), taskId],
    );
    return res.changes > 0;
  }

  async releaseTask(taskId: string, opts?: { countAsRetry?: boolean }): Promise<boolean> {
    // Release from an ACTIVE state counts as one retry attempt (F3): the task
    // is re-queued for reassignment, and maxRetriesPerTask bounds how often a
    // persistently-failing task can bounce before it is failed outright (the
    // scheduler enforces the cap in its run pass).
    // S-01: releases that are NOT the task's fault (e.g. a scheduler kickoff
    // failure — the task never ran) must not consume the retry budget.
    const countAsRetry = opts?.countAsRetry ?? true;
    const res = this.db.run(
      `UPDATE swarm_task
       SET owner_member_id = NULL, status = 'ready',
           retry_count = retry_count + ${countAsRetry ? 1 : 0},
           claimed_at = NULL, lease_expires_at = NULL, updated_at = ?
       WHERE id = ? AND status IN ('claimed','working','review_pending','changes_requested')`,
      [Date.now(), taskId],
    );
    return res.changes > 0;
  }

  /**
   * Atomic coordinator reassignment (F11). Rebinds the task to a new owner in
   * ONE transaction-worthy write sequence:
   *  - clears the OLD owner's currentTaskId when it points at this task (so the
   *    old owner is free to take new work and cannot be reported as mid-task);
   *  - sets task.owner_member_id = new owner and status = 'working';
   *  - sets the NEW owner's currentTaskId = task.
   * Returning the previous owner id lets the caller report the handoff. The
   * caller (coordinator-only tool) performs the kickoff prompt afterwards.
   * Throws when the task or target member is missing, or the task is terminal.
   */
  async reassignTask(taskId: string, newOwnerMemberId: string): Promise<string | null> {
    const task = this.db.query<RowTask, [string]>(`SELECT * FROM swarm_task WHERE id = ?`).get(taskId);
    if (!task) throw new Error(`no task '${taskId}'`);
    if (["completed", "failed", "cancelled"].includes(task.status)) {
      throw new Error(`task '${taskId}' is terminal (${task.status}); reassignment is not allowed`);
    }
    const newOwner = this.db.query<RowMember, [string]>(`SELECT * FROM swarm_member WHERE id = ?`).get(newOwnerMemberId);
    if (!newOwner) throw new Error(`no member '${newOwnerMemberId}'`);
    // S-03: store-level guard — a stopped/stopping/failed member must not
    // receive a reassignment (the task would be bound to a dead session and
    // orphaned until lease). Defense-in-depth: the tool-level guard already
    // rejects this, but any future caller of the store method must be safe.
    if (["stopped", "stopping", "failed"].includes(newOwner.status)) {
      throw new Error(`cannot reassign to '${newOwner.name}': member is ${newOwner.status}`);
    }

    const oldOwnerId = task.owner_member_id ?? null;
    // Clear the OLD owner's currentTaskId when it points at this task.
    if (oldOwnerId && oldOwnerId !== newOwnerMemberId) {
      const oldOwner = this.db.query<RowMember, [string]>(`SELECT * FROM swarm_member WHERE id = ?`).get(oldOwnerId);
      if (oldOwner?.current_task_id === taskId) {
        this.db.run(
          `UPDATE swarm_member SET current_task_id = NULL, updated_at = ? WHERE id = ?`,
          [Date.now(), oldOwnerId],
        );
      }
    }
    const now = Date.now();
    this.db.run(
      `UPDATE swarm_task SET owner_member_id = ?, status = 'working', updated_at = ? WHERE id = ?`,
      [newOwnerMemberId, now, taskId],
    );
    this.db.run(
      `UPDATE swarm_member SET current_task_id = ?, status = 'working', updated_at = ? WHERE id = ?`,
      [taskId, now, newOwnerMemberId],
    );
    return oldOwnerId;
  }

  async getBlackboard(swarmId: string, key: string): Promise<BlackboardEntry | undefined> {
    const r = this.db.query<RowBlackboard, [string, string]>(
      `SELECT * FROM swarm_blackboard WHERE swarm_id = ? AND key = ?`,
    ).get(swarmId, key);
    return r ? toBlackboard(r) : undefined;
  }

  async searchBlackboard(swarmId: string, query: string): Promise<BlackboardEntry[]> {
    // Case-insensitive and wildcard-safe: escape LIKE metacharacters so a user
    // probing "C++" or "a_b" matches literally, and lower() both sides so case
    // never hides a hit (SQLite LIKE is case-sensitive for non-ASCII). The
    // ESCAPE '\' clause makes the backslash escapes in escapeLike() actually
    // take effect — without it, '\%'/'\_' are inert and literal-_/% searches
    // silently return nothing (audit S3).
    const like = `%${escapeLike(query.toLowerCase())}%`;
    const rows = this.db.query<RowBlackboard, [string, string, string]>(
      `SELECT * FROM swarm_blackboard WHERE swarm_id = ? AND (lower(key) LIKE ? ESCAPE '\\' OR lower(value) LIKE ? ESCAPE '\\')`,
    ).all(swarmId, like, like);
    return rows.map(toBlackboard);
  }

  /**
   * Update an existing blackboard row (by id), or insert if absent.
   *
   * When `expectedVersion` is provided, the UPDATE is conditional on the row's
   * current version matching — a store-level compare-and-set guard (audit S2).
   * A 0-row UPDATE is then either an absent row (fresh insert) or a version
   * conflict (the row exists with a different version); the conflict is
   * surfaced as an error instead of a silent last-write-wins overwrite.
   */
  async upsertBlackboard(entry: BlackboardEntry, expectedVersion?: number): Promise<void> {
    const params: unknown[] = [entry.value, entry.contentType, entry.version, entry.authorMemberId, entry.taskId ?? null, entry.updatedAt, entry.id];
    let sql = `UPDATE swarm_blackboard
       SET value = ?, content_type = ?, version = ?, author_member_id = ?, task_id = ?, updated_at = ?
       WHERE id = ?`;
    if (expectedVersion !== undefined) {
      sql += ` AND version = ?`;
      params.push(expectedVersion);
    }
    const res = this.db.run(sql, params);
    if (res.changes === 0) {
      // Distinguish "row absent" (insert path) from "row present, version
      // mismatch" (conflict — must not silently overwrite).
      if (expectedVersion !== undefined) {
        const row = this.db.query<{ version: number }, [string]>(
          `SELECT version FROM swarm_blackboard WHERE id = ?`,
        ).get(entry.id);
        if (row) {
          throw new Error(
            `blackboard conflict on '${entry.key}': expected version ${expectedVersion}, current ${row.version}`,
          );
        }
      }
      // Insert path (fresh row). Conflict on UNIQUE(swarm_id,key) bubbles up.
      this.db.run(
        `INSERT INTO swarm_blackboard (id, swarm_id, key, value, content_type, version, author_member_id, task_id, created_at, updated_at)
         VALUES (?,?,?,?,?,?,?,?,?,?)`,
        [entry.id, entry.swarmId, entry.key, entry.value, entry.contentType, entry.version, entry.authorMemberId, entry.taskId ?? null, entry.createdAt, entry.updatedAt],
      );
    }
  }

  async listPendingMessages(toMemberId: string): Promise<NewMessage[]> {
    // Exclude expired messages (urgent messages carry an expiresAt). Priority
    // is ranked explicitly (audit S4): TEXT ordering would sort 'high' below
    // 'low' lexicographically. urgent > high > normal > low, then oldest first.
    const rows = this.db.query<RowMessage, [string, number]>(
      `SELECT * FROM swarm_message
       WHERE to_member_id = ? AND delivery_state = 'queued'
         AND (expires_at IS NULL OR expires_at > ?)
       ORDER BY CASE priority
         WHEN 'urgent' THEN 0
         WHEN 'high' THEN 1
         WHEN 'normal' THEN 2
         ELSE 3 END, created_at`,
    ).all(toMemberId, Date.now());
    return rows.map(toMessage);
  }

  async listMembersWithPendingMail(): Promise<Array<{ memberId: string; sessionId: string; count: number }>> {
    const now = Date.now();
    const rows = this.db.query<{ member_id: string; session_id: string; count: number }, [number]>(
      `SELECT sm.id AS member_id, sm.session_id, COUNT(*) AS count
       FROM swarm_message m
       JOIN swarm_member sm ON sm.id = m.to_member_id
       WHERE m.delivery_state = 'queued'
         AND (m.expires_at IS NULL OR m.expires_at > ?)
         AND sm.status NOT IN ('stopped', 'stopping', 'failed')
       GROUP BY sm.id, sm.session_id`,
    ).all(now);
    return rows.map((r) => ({ memberId: r.member_id, sessionId: r.session_id, count: r.count }));
  }

  async expireMessage(messageId: string): Promise<void> {
    // M-6 fix: expire both queued AND scheduled rows (a message claimed by a
    // wake whose expiry passes before the delivery commit must never be
    // delivered — markDelivered now guards, and this makes the transition
    // possible from the scheduled state).
    this.db.run(
      `UPDATE swarm_message SET delivery_state = 'expired' WHERE id = ? AND delivery_state IN ('queued','scheduled')`,
      [messageId],
    );
  }

  async listMessagesBySwarm(swarmId: string, limit = 20): Promise<NewMessage[]> {
    const rows = this.db.query<RowMessage, [string, number]>(
      `SELECT * FROM swarm_message WHERE swarm_id = ?
       ORDER BY created_at DESC LIMIT ?`,
    ).all(swarmId, limit);
    return rows.map(toMessage);
  }

  async searchMessagesBySwarm(swarmId: string, query: string, limit = 15): Promise<NewMessage[]> {
    const like = `%${escapeLike(query.toLowerCase())}%`;
    const rows = this.db.query<RowMessage, [string, string, number]>(
      `SELECT * FROM swarm_message
       WHERE swarm_id = ? AND lower(body_text) LIKE ? ESCAPE '\\'
       ORDER BY created_at DESC LIMIT ?`,
    ).all(swarmId, like, limit);
    return rows.map(toMessage);
  }

  async getMessageById(messageId: string): Promise<NewMessage | undefined> {
    const r = this.db.query<RowMessage, [string]>(
      `SELECT * FROM swarm_message WHERE id = ?`,
    ).get(messageId);
    return r ? toMessage(r) : undefined;
  }

  async getMessagesByIds(messageIds: string[]): Promise<NewMessage[]> {
    if (messageIds.length === 0) return [];
    // Bound the IN-list (batching cap is 10; a broadcast could fan out to
    // maxMembers rows — chunk defensively so SQLite's variable limit is never
    // hit even for the largest swarm).
    const out: NewMessage[] = [];
    const chunkSize = 200;
    for (let i = 0; i < messageIds.length; i += chunkSize) {
      const chunk = messageIds.slice(i, i + chunkSize);
      const placeholders = chunk.map(() => "?").join(",");
      const rows = this.db.query<RowMessage, string[]>(
        `SELECT * FROM swarm_message WHERE id IN (${placeholders})`,
      ).all(...chunk);
      out.push(...rows.map(toMessage));
    }
    return out;
  }

  async markMessagesScheduled(toMemberId: string, messageIds: string[]): Promise<number> {
    if (messageIds.length === 0) return 0;
    const stmt = this.db.prepare(
      `UPDATE swarm_message SET delivery_state = 'scheduled' WHERE id = ? AND to_member_id = ? AND delivery_state = 'queued'`,
    );
    let changed = 0;
    for (const id of messageIds) {
      changed += stmt.run(id, toMemberId).changes;
    }
    return changed;
  }

  async updateMessageDelivery(messageId: string, state: SwarmMessage["deliveryState"]): Promise<void> {
    // M-3 fix: attempt_count counts FAILURE attempts only (incremented by
    // revertMessageToQueuedWithError). A successful delivery transition must
    // NOT inflate the retry-budget counter — otherwise a delivered message
    // shows attempts=1 (misleading vs F-M5 semantics). delivered_at is set on
    // the delivered transition only.
    //
    // S4 fix: reject transitions OUT of terminal states (expired/failed) — a
    // message that expired must not be resurrected to 'delivered' by a stale
    // broker call (the F-M2 crash-window risk made structural).
    this.db.run(
      `UPDATE swarm_message SET delivery_state = ?, delivered_at = COALESCE(delivered_at, ?)
       WHERE id = ? AND delivery_state NOT IN ('expired','failed')`,
      [state, state === "delivered" ? Date.now() : null, messageId],
    );
  }

  async revertMessageToQueuedWithError(messageId: string, toMemberId: string, error: string): Promise<NewMessage | undefined> {
    const res = this.db.run(
      `UPDATE swarm_message
       SET delivery_state = 'queued', attempt_count = attempt_count + 1, last_error = ?
       WHERE id = ? AND to_member_id = ? AND delivery_state = 'scheduled'`,
      [error.slice(0, 500), messageId, toMemberId],
    );
    if (res.changes === 0) return undefined;
    return this.getMessageById(messageId);
  }

  async markMessageFailed(messageId: string): Promise<NewMessage | undefined> {
    const res = this.db.run(
      `UPDATE swarm_message
       SET delivery_state = 'failed', last_error = COALESCE(last_error, 'delivery retry budget exhausted')
       WHERE id = ? AND delivery_state = 'queued'`,
      [messageId],
    );
    if (res.changes === 0) return undefined;
    return this.getMessageById(messageId);
  }

  async revertMessageToQueued(messageId: string, toMemberId: string): Promise<void> {
    this.db.run(
      `UPDATE swarm_message SET delivery_state = 'queued' WHERE id = ? AND to_member_id = ? AND delivery_state = 'scheduled'`,
      [messageId, toMemberId],
    );
  }

  async listAllMemberSwarmIds(): Promise<string[]> {
    const rows = this.db.query<{ swarm_id: string }, []>(
      `SELECT DISTINCT swarm_id FROM swarm_member`,
    ).all();
    return rows.map((r) => r.swarm_id);
  }

  async revertStaleScheduledForSwarm(swarmId: string): Promise<number> {
    // Revert scheduled rows to queued EXCEPT those whose expiresAt already
    // passed — a message past its expiry must not be resurrected by recovery
    // (cross-ref audit/storage S5, audit/messaging F-M2). Expired-scheduled
    // rows are transitioned to 'expired' instead.
    const now = Date.now();
    const res = this.db.run(
      `UPDATE swarm_message
       SET delivery_state = 'queued'
       WHERE swarm_id = ? AND delivery_state = 'scheduled'
         AND (expires_at IS NULL OR expires_at > ?)`,
      [swarmId, now],
    );
    const expired = this.db.run(
      `UPDATE swarm_message
       SET delivery_state = 'expired'
       WHERE swarm_id = ? AND delivery_state = 'scheduled'
         AND expires_at IS NOT NULL AND expires_at <= ?`,
      [swarmId, now],
    );
    // M-5 fix: return the TOTAL rows transitioned (queued-reverted +
    // expired-scheduled), so recovery's `staleScheduledReverted` telemetry
    // reflects both state changes instead of under-reporting expired rows.
    return res.changes + expired.changes;
  }
}