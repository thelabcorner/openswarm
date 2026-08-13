# 🐝 openswarm

**Plugin-only, peer-to-peer multi-agent orchestration for [OpenCode](https://opencode.ai) — durable swarms of native OpenCode sessions.**

openswarm turns OpenCode into a coordination fabric for multiple agents. Instead of a lead agent spawning disposable subagents, openswarm runs a **swarm of persistent, peer-to-peer OpenCode sessions** — each member is a real chat you can open and talk to — coordinated entirely by a plugin. No fork, no new runtime, no lock-in: just your existing OpenCode, with a durable brain.

## Key value

- **Agents are real chats.** Every member is a top-level OpenCode session. Open it, message it, pause it — the swarm yields and resumes automatically.
- **Durable by default.** Swarm state (members, tasks, messages, decisions, beliefs) persists in SQLite across restarts. Crashes self-heal: members respawn, tasks re-queue, mail re-delivers.
- **Peer-to-peer, not hub-and-spoke.** Members talk directly to each other. The coordinator sets the mission, then steps back.
- **A living hive mind.** An optional hive layer gives the swarm shared memory — beliefs with confidence, stigmergic annotations, and consensus-free coordination.

## Badges

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)
![Tests: 581 passing](https://img.shields.io/badge/tests-581%20passing-brightgreen)
![Typecheck: clean](https://img.shields.io/badge/typecheck-clean-brightgreen)
![Runtime: Bun](https://img.shields.io/badge/runtime-Bun-black)
![Plugin: OpenCode 1.18.x](https://img.shields.io/badge/plugin-OpenCode%201.18.x-6f42c1)

---

## Features

### Peer-to-peer agents

- **Direct messaging** between members — durable, exactly-once-claimed mailboxes with delivery verdicts, expiry, and retry budgets.
- **Blackboard** — a versioned, CAS-protected shared memory with topics, subscriptions, and conflict-safe writes.
- **Probe before you work** — `swarm_probe` / `swarm_find` tell you who owns a lane and what's already being done, so peers don't stampede.
- **Lanes** — a registry of who is responsible for what, written automatically at delegation time.

### Task orchestration

- **DAG scheduler** — dependencies gate readiness; ready tasks are auto-assigned to idle members by affinity.
- **Atomic CAS claims** — a task can never be double-assigned; ownership transitions only through claim/release.
- **Leases & retries** — claims expire on lease timeout; retries are capped; dependent tasks are notified when an upstream is released or fails.
- **Explicit binding & reassignment** — reserve a task for a named member; reassign atomically with stale-owner authority invalidated.

### Human-in-the-loop

- Members are **real OpenCode chats** — open any member in the app and talk to it directly.
- While you chat, the swarm **yields** (mail, task continuation, and scheduling pause); it resumes automatically after a lull.
- `swarm_release` force-resumes a member immediately when the 5-minute lull is too slow.

### Hive layer

- **Artifact annotations** — stigmergic "pheromones" on workspace paths: `gold`, `corpse`, `struggle`, `affordance`, `note`.
- **Beliefs + lateral inhibition** — facts with confidence and reinforce counts; duplicate facts reinforce instead of broadcast.
- **Whisper/shout tiers** — tentative local beliefs (whisper) upgrade to swarm-visible (shout) after independent reinforcement.
- **Needs routing** — targeted, pull-based `hive_need` delivery to the members whose context actually matches.
- **Corpse-pile hesitation & gold trails** — the scheduler reads annotations to flag dead-end paths and bias toward proven ones.
- **Resonance, consolidation, anti-entropy digest** — independent convergence is detected and recorded; stale beliefs are pruned truthfully; digest health keeps the swarm honest about drift.

### Security

- **Injected-content fencing** — all peer-authored text is visibly marked as untrusted data.
- **Permission-boundary scoping** — worktree/temp scoping with traversal protection; members never widen the coordinator's grants.
- **Autopermissions propagation** — coordinator permission state propagates to members, clamped (never widened).
- **Coordinator-only destructive ops** — `swarm_delete`, `swarm_stop`, `swarm_remove` are gated and confirm-protected.

### Reliability

- Exactly-once mailbox claims; delivery verdicts (`delivered`/`pending`/`expired`/`failed`) with sender notices.
- Crash recovery: members respawn, tasks re-queue, stale mail re-delivers.
- Watchdog with suspicion counters; continuation budgets with blocker escalation.

---

## Installation

**Prerequisites:** [Bun](https://bun.sh) ≥ 1.x, [OpenCode](https://opencode.ai) 1.18.x.

```sh
git clone <your-fork-or-repo-url> openswarm
cd openswarm
bun install
bun run build      # bundles the plugin to dist/index.js
```

### Register the plugin

Add the built bundle to your OpenCode config (`opencode.json` or `opencode.jsonc`). Use the **absolute path to your clone's `dist/index.js`**:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": [
    ["/absolute/path/to/openswarm/dist/index.js", {
      "allowAllMemberPermissions": true,
      "defaultMemberModel": { "providerID": "opencode-go", "modelID": "deepseek-v4-flash" }
    }]
  ]
}
```

Plugin options:

| Option | Default | Meaning |
|---|---|---|
| `allowAllMemberPermissions` | `false` | Auto-allow every permission requested by swarm members (headless sessions never wedge on prompts). |
| `defaultMemberModel` | `opencode-go` / `deepseek-v4-flash` | Model used for spawned members when no explicit model is given and no last-used model is known. |
| `storeBackend` | `chunkdb` | Storage backend. `chunkdb` (default) stores compressed payload chunks over SQLite (~66% smaller files); `sqlite` keeps the classic row store. Switching to `chunkdb` auto-migrates an existing `swarms.db` on first open. |

Then restart OpenCode. Verify the plugin is live and the runtime is compatible:

```sh
bun run probe     # capability probe against a running OpenCode server
```

---

## Quickstart

Everything flows through the `swarm_*` tools. The fastest path is `swarm_delegate` — create, spawn, and seed tasks in one call:

```
swarm_delegate(
  name: "docs-team",
  members: [
    { name: "writer",   role: "documentation writer",   prompt: "You draft the user guide." },
    { name: "reviewer", role: "documentation reviewer", prompt: "You review and edit drafts." }
  ],
  tasks: [
    { id: "draft",   title: "Write the user guide",   priority: 10 },
    { id: "review",  title: "Review the guide",       priority: 5, dependsOn: ["draft"] }
  ]
)
```

The scheduler assigns `draft` to the writer and `review` to the reviewer (affinity + DAG readiness). Watch progress:

```
swarm_status(swarmId: "<swarm-id>", detail: "summary")
swarm_tasks(swarmId: "<swarm-id>", action: "list")
```

Each member is a real OpenCode session — open it in the app and talk to it directly. When the reviewer completes, the coordinator is notified; you can also poll-free by relying on the completion notices.

---

## Model selection

Members get a model via a deterministic priority chain — never a random runtime default:

1. **Explicit `model` on the member** (validated; tolerant of tier labels like `"go"`, a bare `modelID`, or the model's display name)
2. **Last-used tuple** for the swarm (remembered in-memory and persisted on the blackboard under `context/model/last-used`, so it survives restarts)
3. **The coordinator's current session model** (your live model)
4. **Config default** (`defaultMemberModel`, default `opencode-go` / `deepseek-v4-flash`)
5. **Any available model** (zen-free preferred) — reported as `modelSource: "fallback"` so you know the default wasn't available

Every spawn output reports `model` + `modelSource` (`requested | last-used | coordinator | default | fallback | none`) so you always see *why* a member got its model. Use `swarm_models` to confirm availability; you rarely need to set `model` yourself.

### Capability delegation (images / PDFs)

When the operator supplies an image (or the current model cannot read one), delegate to a subagent on a model that actually can — cheapest first:

```
swarm_models(capability: "image")        # models that can read images, cheapest first, with prices
swarm_spawn(swarmId, members: [{ name: "vision-reader", role: "image reader", capability: "pdf" }])
```

`swarm_models(capability: ...)` lists only capable models sorted by cost-per-1M-tokens (provider-published, with a fallback catalog); spawning a member with `capability` picks the **cheapest capable model** automatically (`modelSource: "capability"`). An explicit `model` always wins over capability.

---

## Caching & cost efficiency

Measured across a real multi-day swarm deployment (opencode-go provider). Figures are aggregated and relative — no raw per-session token or dollar counts:

- **Swarm sessions cache-read ~2.7x better than regular sessions on a per-session basis**: median cache-hit ratio in swarm sessions is ≈ 98%, vs ≈ 92% for regular build/explore sessions (means ≈ 95% vs ≈ 87%).
- **Distribution is one-sided**: no swarm session dropped below a 50% cache-hit rate (regular sessions had ~30 such sessions). A third of swarm sessions hit ≥ 99% cache-read ratio; only ~8% of regular sessions did.
- **Swarm sessions produced roughly half the deployment's token volume while costing only about a third of total spend** — driven by near-total cache reads. Effective per-token cost in swarm sessions is ~2.7x cheaper than in regular sessions.
- **Mechanism: shared session lineage.** All members re-read the same projected session history; the first member to run warms the cache and every subsequent member hits it. Sequential / complementary member ordering amplifies the effect.
- **The low-hit-rate swarm outliers were freshly spawned members** with ~zero prior tokens — a new worker reading an unwarmed context, then going idle/dead. Negligible cost, but wasted cache warmth.

### Operational guidance

- **Batch member creation into fewer, longer-lived members.** Constantly respawning fresh members forfeits cache warmth.
- **Keep members sharing the same session / projected history** when reads dominate their work.
- **Watch cache-hit ratio as a health metric.** If it drops below ~95% at the session level, expect per-token cost to multiply (context rewrites, eviction, or fresh spawns).
- **Cache hit correlates with cost in the expected direction:** at ~99% hit, sessions remain cheap despite huge token volume.

---

## Tools reference

| Tool | What it does |
|---|---|
| `swarm_delegate` | Create (or reuse) a swarm, spawn members, seed a task DAG, and start work — one call. |
| `swarm_create` | Create a swarm shell + optional task DAG without spawning. |
| `swarm_task` | Delegate a single task to a member (spawns if needed). |
| `swarm_spawn` | Spawn named member sessions. |
| `swarm_tasks` | Inspect/claim/release/reassign/complete/fail/cancel/list tasks. |
| `swarm_message` | Direct or broadcast message to a member. |
| `swarm_reply` | Reply to a message, preserving the thread (correlation id). |
| `swarm_memory` | Read/write the versioned blackboard. |
| `swarm_status` | Dashboard: members, tasks, messages, lanes, path claims. |
| `swarm_roster` | Who is on the team, what they're doing, who's chatting. |
| `swarm_probe` | Search for what's already being worked on (anti-redundancy). |
| `swarm_find` | Find which member likely knows about a topic. |
| `swarm_wake` | Manually deliver a member's queued mailbox. |
| `swarm_release` | End a member's human-chat pause and resume swarm machinery now. |
| `swarm_stop` | Stop a worker member (releases its task). |
| `swarm_remove` | Permanently remove a worker member, freeing its roster slot. |
| `swarm_delete` | Permanently delete the entire swarm (coordinator-only, confirm required). |
| `hive_publish` | Publish a belief/fact; lateral inhibition suppresses duplicates. |
| `hive_reinforce` | Reinforce an existing belief; may upgrade whisper → shout. |
| `hive_need` | Pull-based need routing to matching members. |
| `hive_spotlight` | Temporarily boost a topic with collective attention. |
| `hive_relevant` | Rank beliefs by relevance to a query. |
| `hive_consolidate` | Run consolidation: upgrade/prune/expire/retain + contradiction detection. |
| `artifact_annotate` | Add advisory scent (gold/corpse/struggle/etc.) to a workspace path. |
| `artifact_list` | List annotations for a path or the whole workspace. |

---

## The hive layer: a living memory

openswarm's hive layer gives the swarm a **stigmergic memory** — members coordinate indirectly through shared traces, the way ants coordinate through pheromone trails. Annotate a path as `gold` after a verified solution; mark it `corpse` after a dead end. The scheduler reads those traces and steers: it hesitates on corpse piles, and biases toward gold trails — without any member needing to ask another.

**Lateral inhibition** keeps the memory honest. Publishing the same fact twice doesn't broadcast it twice — it *reinforces* a single stored belief. A fact reinforced by multiple independent peers becomes a **shout** (swarm-visible); a tentative local belief stays a **whisper**. The result is a shared understanding that converges on consensus without a voting ceremony.

**Resonance and consolidation** prune the memory over time. When two members independently converge on the same conclusion with disjoint evidence, that convergence is recorded. Weak, unreinforced beliefs are pruned; contradictions are flagged rather than papered over. An anti-entropy digest continuously checks the swarm's beliefs against a canonical fingerprint, so drift is detected early.

The hive isn't a gimmick — it's how a swarm of independent agents stays coherent, truthful, and cheap to reason about as it grows.

---

## Architecture snapshot

| Layer | Responsibility |
|---|---|
| **Plugin surface** | All `swarm_*`, `hive_*`, `artifact_*` tools; permission interception; event hooks. |
| **Core swarm** | Membership, messaging, blackboard, claims, spawn/respawn lifecycle. |
| **Scheduler** | DAG readiness recompute, affinity assignment, leases, retries, dependent notifications. |
| **Messaging** | Durable mailboxes, delivery verdicts, expiry, retry budgets, thread continuity. |
| **Storage** | SQLite persistence: swarms, members, tasks, messages, blackboard, beliefs, annotations. |
| **Hive** | Beliefs, annotations, needs, resonance, consolidation, digest health. |

**Persistence:** SQLite at `<project>/.opencode/swarms/swarms.db` (plugin-owned). Schema changes are applied through a versioned, idempotent migration chain (`user_version`), so upgrades are safe on existing swarms.

---

## Documentation

- [docs/STORAGE.md](docs/STORAGE.md) — storage, blackboard, path claims, schema migrations.
- [docs/SCHEDULER.md](docs/SCHEDULER.md) — DAG, claims, leases, retries, recovery.
- [docs/MESSAGING.md](docs/MESSAGING.md) — mailboxes, delivery semantics, notices.
- [docs/DESKTOP.md](docs/DESKTOP.md) — OpenCode Desktop integration and human-chat behavior.
- [docs/OPENCODE_COMPATIBILITY_REPORT.md](docs/OPENCODE_COMPATIBILITY_REPORT.md) — verified compatibility with OpenCode 1.18.15.

---

## Testing

The suite covers unit tests, storage/migration, messaging reliability, scheduler/task-lifecycle edge cases, and hive behavior — including regression tests for the edge cases that previously wedged or misled agents.

```sh
bun run typecheck   # tsc --noEmit
bun test            # 581 tests, 2073 assertions
bun run build       # bundle the plugin to dist/
bun run e2e         # fresh-server plugin load check
```

---

## Contributing

Contributions are welcome. Ground rules:

- **Plugin surface only** — no forking or modifying OpenCode itself. Everything must work on stock OpenCode.
- **Tests required** — every fix or feature lands with regression tests (see the testing section).
- **Read the docs first** — the architecture docs above are the contract for each layer.
- Open a PR; maintainers review for correctness, honesty of tool output, and no dead-ends.

---

## License

MIT.
