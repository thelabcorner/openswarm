-- Agent Swarms SQLite schema (plugin-owned).
-- Mirrors the spec §38. Applies the pragmas recommended by the architecture.

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

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

CREATE INDEX IF NOT EXISTS idx_member_status
  ON swarm_member(swarm_id, status);

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
  reserved_for TEXT,
  reserved_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  completed_at INTEGER,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(owner_member_id) REFERENCES swarm_member(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_task_ready
  ON swarm_task(swarm_id, status, priority DESC);

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
  noreply INTEGER NOT NULL DEFAULT 0,
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

CREATE INDEX IF NOT EXISTS idx_message_correlation
  ON swarm_message(correlation_id);

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

CREATE INDEX IF NOT EXISTS idx_path_claim_active
  ON swarm_path_claim(swarm_id, released_at);

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

CREATE INDEX IF NOT EXISTS idx_subscription_swarm
  ON swarm_subscription(swarm_id);

CREATE TABLE IF NOT EXISTS swarm_pending_permission (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  member_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  type TEXT NOT NULL,
  pattern TEXT,
  title TEXT,
  response TEXT,
  responded_at INTEGER,
  created_at INTEGER NOT NULL,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(member_id) REFERENCES swarm_member(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pending_perm_swarm
  ON swarm_pending_permission(swarm_id, response);

CREATE TABLE IF NOT EXISTS swarm_deliverable (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  member_id TEXT NOT NULL,
  task_id TEXT,
  summary TEXT NOT NULL,
  refs_json TEXT,
  files_json TEXT,
  verdict TEXT,
  verdict_by TEXT,
  verdict_at INTEGER,
  created_at INTEGER NOT NULL,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  FOREIGN KEY(member_id) REFERENCES swarm_member(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_deliverable_swarm
  ON swarm_deliverable(swarm_id, verdict, created_at DESC);

CREATE TABLE IF NOT EXISTS swarm_contract (
  id TEXT PRIMARY KEY,
  swarm_id TEXT NOT NULL,
  key_pattern TEXT NOT NULL,
  schema_json TEXT NOT NULL,
  description TEXT,
  created_by TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY(swarm_id) REFERENCES swarm(id) ON DELETE CASCADE,
  UNIQUE(swarm_id, key_pattern)
);
CREATE INDEX IF NOT EXISTS idx_contract_swarm ON swarm_contract(swarm_id);

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

CREATE INDEX IF NOT EXISTS idx_event_order
  ON swarm_event(swarm_id, id);