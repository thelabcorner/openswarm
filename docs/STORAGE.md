# openswarm Storage Layer

The data layer of openswarm: durable, SQLite-backed persistence for swarms,
members, tasks, messages, blackboard, path claims, and the Hive belief
substrate. This document is the reference for `src/storage/*` and the domain
types in `src/core/types.ts`. Every claim here is checkable against the code
and the unit tests in `test/unit/store.test.ts`.

## 1. Overview

All durable state lives in a single SQLite database per workspace, managed by
`SQLiteStore` (`src/storage/sqlite-store.ts`).

- **Schema source of truth**: the DDL is defined twice — as a standalone
  `src/storage/schema.sql` and as the `SCHEMA` template constant inside
  `sqlite-store.ts`. A drift test (`test/unit/schema-drift.test.ts`) compares
  the two statement-by-statement so they can never diverge silently.
- **Pragmas**: `journal_mode = WAL`, `foreign_keys = ON`,
  `busy_timeout = 5000` are applied on open (`ready()`).
- **Cross-runtime driver**: `src/storage/sqlite.ts` picks `bun:sqlite` under
  Bun and `node:sqlite` (DatabaseSync, Node 22+) under Node, exposing a common
  surface.
- **Serialized write queue**: the store is a single shared connection. Every
  operation — transactions AND raw methods — routes through one promise queue
  (`serialized()`), so a raw write can never execute while a transaction holds
  `BEGIN IMMEDIATE` on the connection. This closes the classic lost-update
  window where a raw write would silently join (and be rolled back with) an
  in-flight transaction.

## 2. Schema tour

Tables (from `schema.sql`):

| Table | Purpose |
|---|---|
| `swarm` | One swarm per (project, name); coordinator session binding; policy JSON; directory root. |
| `swarm_member` | Member rows: unique session id, status, workspace mode, current task binding, human-chat timestamp. |
| `swarm_task` | Tasks: status, priority, owner, DAG metadata, retry count, claim lease timestamps. |
| `swarm_task_dependency` | DAG edges (`task_id` depends on `depends_on_task_id`), no self-loops. |
| `swarm_message` | Mailbox rows: sender/recipient/broadcast, kind, priority, delivery state machine. |
| `swarm_blackboard` | Versioned key/value store (optimistic concurrency). |
| `swarm_path_claim` | Advisory lane claims (partial UNIQUE, TTL, heartbeat). |
| `swarm_artifact_annotation` | Hive H0: durable scent on workspace paths. |
| `swarm_belief` | Hive H1/H2: deduplicated facts with confidence/tier/status. |
| `swarm_subscription` | Topic subscriptions for blackboard pub/sub. |
| `swarm_event` | Append-only event log (reserved). |

### `swarm_message` — delivery state machine

`delivery_state` transitions among `queued`, `scheduled`, `delivered`,
`acknowledged`, `expired`, `failed`:

- Messages are **enqueued** (`queued`), atomically **claimed** by a wake
  (`scheduled`, exactly-once via an affected-row check), and **delivered**
  (set by the broker after the prompt succeeds).
- A failed delivery reverts to `queued` with `attempt_count + 1` and a
  `last_error` (bounded retry budget, then `failed`).
- Expiry: rows with an `expires_at` in the past are excluded from the pending
  queue; a sweep transitions overdue rows to `expired`. Once `expired` or
  `failed`, a row is terminal — `updateMessageDelivery` refuses to resurrect it
  (state guard).
- `attempt_count` counts **failure attempts only**; successful delivery does
  not inflate it.

### `swarm_blackboard` — versioned CAS entries

Each key has a monotonically increasing `version`. Overwriting an existing key
requires the caller to pass the version it read (`expectedVersion`): a mismatch
throws a conflict instead of silently overwriting (last-write-wins is
deliberately banned).

### `swarm_path_claim` — advisory lane claims

One **active** claim per `(swarm_id, member_id, pattern)` — enforced by a
*partial* unique index `WHERE released_at IS NULL`, so released or expired rows
don't block re-claiming a lane. Claims carry an optional `expires_at` (TTL);
`listPathClaims` excludes stale claims. A heartbeat method
(`refreshPathClaim`) extends `expires_at` to keep a live claim alive.

### `swarm_artifact_annotation` — Hive H0 scent

Annotation types: `claim`, `struggle`, `corpse`, `gold`, `affordance`, `note`,
with a `weight` (signal strength) and optional `note`, `error_sig`,
`solution_hash`, and `ttl`. One row per `(swarm_id, path, type)` — re-annotating
the same path/type **replaces** the previous row (scent is point-in-time,
latest wins). `ttl <= 0` means *no expiry*; otherwise `expires_at` is derived
at insert and stale rows are excluded from active listings.

### `swarm_belief` — Hive H1/H2 fact substrate

- One row per `(swarm_id, fact_hash)` — re-inserting the same fact
  **reinforces** it (dedupe, `reinforce_count + 1`, confidence bumped) instead
  of duplicating.
- `confidence` is clamped to 0..1 at insert and on reinforce.
- `tier` is `whisper` (tentative) or `shout` (reinforced ≥ 2) — promoted via an
  explicit store call so tool output is truthful.
- `status` lifecycle: `active` → `resonant` (independent confirmation with
  disjoint evidence) | `superseded` (contradicted) | `expired` (TTL).
- Re-inserting a soft-pruned fact (superseded/expired) **revives** it to
  `active` and refreshes its expiry.
- `evidence_refs` stores a JSON array of supporting refs (resonance / causal
  chain).
- Anti-entropy: `beliefDigest` returns a sha1 over active rows' `(id,
  updated_at, reinforce_count, confidence, tier)` tuples — monotonic fields so
  any mutation changes the digest; `listBeliefsChangedSince` is the pull side.

## 3. Migrations

Schema changes are versioned by `PRAGMA user_version` with an append-only,
ordered migration chain inside `migrate()`:

| Version | Change |
|---|---|
| 1 | `swarm.directory` column |
| 2 | `swarm_member.human_chat_at` column |
| 3 | `swarm_path_claim.expires_at` column |
| 4 | `swarm_task.claimed_at` + `lease_expires_at` (claim leases) |
| 5 | `swarm_artifact_annotation` table (Hive H0) |
| 6 | `swarm_belief` table (Hive H1) |
| 7 | `swarm_belief.resonant_at` column (Hive H2) |

- Each step runs exactly once (guarded by `user_version`); steps are
  idempotent and append-only — never edit a shipped step, only add new ones.
- A defensive `addMissingColumn` re-apply runs every open so databases created
  by older versions catch up even if a prior chain was shorter.
- Partial migration failure self-heals: `CREATE TABLE IF NOT EXISTS` +
  idempotent steps mean reopening a database after a failed step re-runs the
  chain cleanly.
- The schema-drift test keeps `schema.sql` and the runtime `SCHEMA` constant in
  lock-step (comment- and whitespace-normalized, CRLF-safe).

## 4. Concurrency

- **Blackboard CAS**: `blackboardPut` requires `expectedVersion` when the key
  already exists (omitting it on an existing key is a conflict, not an
  overwrite); the store-level `upsertBlackboard(entry, expectedVersion)` adds a
  SQL guard (`WHERE id = ? AND version = ?`) as defense in depth, and the tool
  layer renders an actionable conflict notice.
- **Task claim CAS**: `claimTask` is a single conditional UPDATE
  (`owner IS NULL AND status = 'ready'`) — exactly one claimant wins; a
  member-side check additionally rejects claiming a second task while already
  bound to a non-terminal one.
- **Serialized queue**: `transaction()` and every raw store method share one
  promise queue; concurrent transactions serialize without nesting errors, and
  raw writes never join an open transaction (regression-tested).
- **Exactly-once message claims**: `markMessagesScheduled` transitions only
  `queued → scheduled` and reports the affected-row count, so concurrent wakes
  cannot double-deliver.

## 5. Search

- Blackboard and message search are **case-insensitive** and **wildcard-safe**:
  the query escapes LIKE metacharacters and every LIKE predicate carries an
  explicit `ESCAPE '\'` clause, so searching for literal `_` or `%` finds the
  matching rows instead of treating them as wildcards.
- Mailbox ordering ranks priority explicitly (`urgent > high > normal > low`,
  then oldest first) rather than relying on lexicographic TEXT ordering, which
  would sort `high` below `low`.

## 6. Hive storage semantics

- **Annotations**: `ttl <= 0` = no expiry; replace-on-insert for the same
  path/type; stale rows invisible to active listings but retained until
  deleted.
- **Beliefs**: dedupe→reinforce on `fact_hash`; confidence clamped 0..1;
  whisper→shout at `reinforce_count >= 2` (explicit promotion); revive-on-
  reinsert after soft prune; soft-prune (`superseded`/`expired`, row kept for
  causal chain) vs hard-prune (delete); `beliefDigest` anti-entropy hash.
- Pruning candidates: `listBeliefsForPruning` returns active beliefs that are
  low-confidence AND low-reuse AND old (thresholds as options).

## 7. Key invariants

- **Ownership guard on `current_task_id`**: a member can only be bound to a
  task it owns (or an unowned reservation); binding to another member's task is
  rejected at the store.
- **Cascade deletes**: deleting a swarm cascades to all member rows, tasks,
  messages, blackboard entries, path claims, annotations, beliefs, and
  subscriptions (`ON DELETE CASCADE` on `swarm_id`).
- **Member removal**: `deleteMember` cascades the member's authored
  blackboard/annotation/belief rows first, so removing a member who authored
  content never trips a foreign-key constraint.
- **TTL boundary**: `ttl <= 0` is "no expiry" across path claims, annotations,
  and beliefs — a zero TTL never makes a row instantly stale.
- **Terminal message states**: `expired`/`failed` are terminal; no delivery
  transition can resurrect them.
