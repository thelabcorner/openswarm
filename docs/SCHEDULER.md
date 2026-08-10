# openswarm Scheduler & Task Orchestration

Reference for how openswarm schedules work, assigns work to members, and keeps
the task graph moving. This is the task-orchestration contract: lifecycle,
claims, leases, retries, ownership, failure handling, and human-in-the-loop
behavior.

The scheduler is **stateless, deterministic, and event-driven**. It recomputes
task readiness from the dependency graph on every state change and assigns
ready work to idle members under explicit concurrency limits. It never polls
for work on its own; a periodic safety-net sweep catches gaps between events.

---

## 1. Task model & lifecycle

A task is a bounded piece of work with a title, an optional description,
optional acceptance criteria, a priority, a creator, and an optional owner
member. Every task lives in one of these states:

```
pending ──► blocked ──► ready ──► claimed ──► working ──► completed
   │            │           │                        │
   │            │           │                        └──► failed / cancelled
   └────────────┴───────────┴────────────────────────────► (any non-terminal
                                                           can be cancelled)
```

- **`pending`** — created, not yet evaluated against the DAG.
- **`blocked`** — has at least one prerequisite that is not done.
- **`ready`** — all prerequisites are satisfied (or none exist); claimable.
- **`claimed`** — atomically reserved by a member (transient; immediately
  moved to `working` when the kickoff prompt is sent).
- **`working`** — a member owns it and is executing it.
- **`review_pending` / `changes_requested`** — reserved review-gate states
  (the plumbing is designed for future review workflows; not produced by the
  scheduler today).
- **`completed` / `failed` / `cancelled`** — terminal states. Terminal tasks
  never re-enter the scheduling flow; `completed` is the only success state.

A task cannot be moved out of a terminal state (the store guards this), and a
terminal task satisfies its dependents' prerequisites.

### Dependencies (DAG)

Tasks can declare `dependsOn` prerequisites. The scheduler builds the
dependency graph from these edges and recomputes readiness on every pass:

- a task with **no** unmet prerequisites becomes `ready`;
- a task with any unmet prerequisite stays/becomes `blocked`;
- a task whose prerequisite reaches a terminal state (`completed`, `failed`,
  or `cancelled`) counts that prerequisite as done — a failed or cancelled
  upstream still unblocks dependents (they run with knowledge that the
  upstream did not succeed; dependents are notified — see §8).

Cycle detection runs at graph-creation time: a DAG that contains a cycle
(including a self-dependency) is rejected with a clear error.

---

## 2. Scheduling

Each scheduling pass:

1. **Recomputes readiness** from the dependency graph (deterministic; no model
   inference).
2. **Persists transitions** — `pending`/`blocked` → `ready` when prerequisites
   are met.
3. **Reconciles orphans** — tasks in an active state with **no owner** (e.g. a
   member was removed, or a restart orphaned the claim) are released back to
   `ready` without consuming retry budget; review states are excluded (an
   ownerless `review_pending`/`changes_requested` task is legitimate, not an
   orphan).
4. **Enforces the retry budget** — a `ready` task whose retry count exceeds
   `maxRetriesPerTask` is failed outright (see §5).
5. **Assigns** ready, unowned tasks to idle members, honoring concurrency.

### Determinism & affinity

Assignment order is deterministic. Ready tasks are processed by priority
(descending); within equal priority, by creation order. Idle candidates are
ordered by **affinity score**: a member whose name or role shares a token with
the task title/description ranks first, so the right specialist gets the work
(e.g. a task about "packaging" goes to the member whose role mentions
"packaging"). Ties break by member name. Members with recent success
annotations on a matching path (gold dust) receive a soft affinity boost.

### Capacity

Concurrency is bounded by `maxConcurrentMembers`. Only members that are
`working` **and actually own a task** count against capacity. A member marked
`working` with no task is in limbo and does not consume a slot (it is demoted
by the watchdog — see §7).

### Pass triggers

- **Event-driven:** any task state change (complete, fail, cancel, claim,
  release, reassign), member idle event, or delegation/spawn triggers a pass.
- **Safety-net sweep:** every 10 seconds the sweep recomputes readiness and
  assigns anything left ready, so a ready task never waits indefinitely even
  when no event fires.

Passes are **serialized per swarm** so two concurrent triggers cannot assign
two different tasks to the same member.

---

## 3. Claims

### Atomic claim (CAS)

Claiming is a compare-and-swap at the store level. A claim succeeds only when
the task is `ready` **and** unowned, **and** the claiming member is eligible:

- **Task side:** `owner IS NULL AND status = 'ready'`.
- **Member side:** the member's `current_task_id` is `NULL` (or already equals
  this task, e.g. a spawn-time reservation).

Both checks happen inside one serialized transaction, so two concurrent claims
of different tasks by the same member cannot both succeed — a member can never
hold two active tasks at once.

### Full working transition

A successful claim completes the full transition: the member is marked
`working`, its `current_task_id` is bound, the task moves to `working`, and the
member is kicked off with the assignment prompt (which states the task, its
criteria, the completion protocol, and the assignment marker). The prompt's
first line is deliberately shaped so the human-chat tracker recognizes it as a
machine injection, not a user message (see §9).

### Pull-claim (`swarm_tasks claim`)

A member can claim a ready task directly from the tool surface. The pull-claim
performs the same full transition and kickoff as the scheduler. The **R1
overlap guard** rejects a pull-claim when the caller already owns a
non-terminal task — you must finish or release your current task before
claiming another.

### Release

A task owner (or the coordinator) can release a `claimed`/`working` task back
to `ready` via the `swarm_tasks release` action — the escape hatch for a
mis-claim or a task that cannot be finished. Releasing **counts as one retry**
(bounded by `maxRetriesPerTask`); the release deliberately does not re-run the
scheduler immediately, so the released task is not handed straight back to the
same member. The owner's binding is cleared and the member is freed for new
work.

---

## 4. Leases

Every claim is leased. `taskLeaseMs` (default **30 minutes**) sets how long a
claim may stay active before the sweep considers it stalled.

- `claimTask` records `claimed_at` and `lease_expires_at = claimed_at +
  taskLeaseMs`.
- A lease value `<= 0` or absent means **no lease** — the task is never
  sweep-expired (a mis-set `0` means "no lease", never instant expiry).
- **leaseSweep** runs **before** the scheduler pass in the sweep, so a
  lease-expired (stalled) task is released and re-assigned in the **same**
  sweep — reclaimed capacity is usable immediately, not one sweep later.
- Releasing on lease expiry clears the owner's binding and demotes the owner
  to `idle`, so the member is free to take new work.
- **Human-chat guard:** an owner who is actively chatting with the user keeps
  its lease (the member is legitimately paused, not stalled).

---

## 5. Retries

- `retryCount` is a first-class field on the task.
- Releasing a task from an active state **increments** `retryCount` — this is
  the "release counts as a retry" rule.
- Releases that are **not the task's fault** do **not** consume the budget:
  scheduler kickoff failures and orphan (ownerless) releases pass
  `countAsRetry: false`.
- `maxRetriesPerTask` (default **2**) bounds how often a task may be released
  before the scheduler fails it outright. A ready task whose `retryCount`
  exceeds `maxRetriesPerTask` is transitioned to `failed` with a reason, and
  the coordinator is notified (the task will not be re-assigned). A value of
  `0` means "fail after the first real failed attempt".
- Retry-limit failures also notify dependent task owners (see §8).

---

## 6. Ownership

### Explicit binding (reservation)

When a delegation or spawn names a `member.taskId`, that task is **reserved**
for the named member: the scheduler promotes it to `ready` but never hands it
out by affinity. The named member claims it at spawn. Affinity only ever
chooses among *unbound* idle candidates, so an existing higher-affinity member
cannot steal a task explicitly intended for a specialist.

If binding fails (the task is already owned elsewhere, or not ready), the
delegation output reports the **actual owner** and the next action instead of
silently proceeding unbound.

### Reassignment

The coordinator can reassign a task to a different member via
`swarm_tasks action=reassign`. The primitive atomically:

- clears the **old owner's** `current_task_id` (when it points at this task),
- rebinds the task's owner row to the new member,
- binds the new member's `current_task_id`,
- invalidates the old owner's completion authority (completion checks the
  current row owner).

The store rejects reassignment to a `stopped`/`stopping`/`failed` member, and
to a terminal task.

### Ownership guard

Writing a member's `current_task_id` to a task owned by a *different* member is
rejected at the store level — the corruption class behind double-assignment
cannot be introduced by any caller.

---

## 7. Failure handling

### Watchdog

A member marked `working` whose session goes silent is watched:

- **Silence threshold:** 5 minutes with no new session messages.
- **Strike 1:** the member is nudged with a prompt to continue its task (the
  nudge's own message is excluded from the liveness check, so the watchdog
  does not feed itself).
- **Strike 3:** the watchdog releases the member's task back to `ready`,
  marks the member interrupted, and notifies the coordinator.

**Taskless-working demotion:** a member marked `working` with **no**
`current_task_id` is in limbo — it is not an idle candidate, cannot be nudged
or released (both watchdog branches need a task), and yet consumed capacity.
If its session is silent, its `last_active_at` is older than a **2-minute
kickoff grace window**, and it is not chatting, the watchdog demotes it to
`idle` so it can take work. The grace window protects a member whose claim
just happened and whose kickoff prompt is still running.

The watchdog's escalated release clears the owner's binding.

### session.error vs abort

- A **genuine failure** marks the member `failed`, releases its task, and
  notifies the coordinator.
- An **abort / interrupt** (user manually stopped the chat, or the run was
  cancelled) marks the member `interrupted` and **keeps** the task claimed —
  the work is paused, not orphaned.

### session.deleted

Deleting a member's chat releases any owned task back to `ready` and marks the
member `stopped` — a working task with a stopped owner would otherwise
dead-lock the DAG forever.

### Recovery & respawn

On startup (and periodically), recovery reconciles durable state against the
live runtime:

- members whose sessions vanished are **respawned** (a fresh session is
  created and re-grounded with the peers, task, and completion protocol), or
  marked interrupted with their task released when respawn is unavailable;
- members marked `interrupted` whose sessions are actually alive are revived
  and re-engaged on their task.

---

## 8. Dependent notifications

When a task is **released, failed, or cancelled**, the scheduler computes its
direct dependents and sends **one `finding` message to each dependent task's
owner**, naming the upstream task and the reason, so they re-validate whether
their work is still safe to proceed. This applies across all release paths:
lease expiry, watchdog escalation, session failure, explicit fail/cancel, and
retry-limit failure.

---

## 9. Human-in-the-loop

Members are root chat sessions the user can open and talk to directly. The
human-chat tracker detects when the user is messaging a member:

- While a member is **chatting**, the scheduler does not assign it new work,
  revive does not re-prompt it, and mail delivery is deferred — the member is
  answering the user, not the swarm.
- The scheduler's idle-member filter excludes chatting members; the watchdog
  and lease sweep use the same guard before releasing a task mid-conversation.
- After the chat lull (default 5 minutes) or an explicit `swarm_release`, swarm
  machinery resumes for that member.

Machine injections (assignment prompts, inbox batches, team digests, watchdog
nudges) are recognized by a marker so they are never mistaken for user
messages — a scheduler assignment does not look like a human chat.
