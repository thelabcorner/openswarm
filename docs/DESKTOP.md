# OpenCode Desktop Integration

This document describes how the openswarm plugin integrates with the OpenCode
Desktop app and the OpenCode runtime: how swarm members appear as real chats,
how the plugin yields to human conversations, and how member permissions are
derived, propagated, and probed.

All behavior described here is implemented plugin-only. The OpenCode app and
server are never modified or forked.

---

## 1. Members are real OpenCode sessions

Swarm members are created as **root OpenCode sessions** (no parent session):

- `session.create` is called without a `parentID`, so each member appears in the
  desktop app's Home list and opens as a normal chat tab.
- The coordinator→member relationship lives entirely in the plugin's own
  database (`swarm.coordinatorSessionId`, `member.sessionId`) — OpenCode
  parentage is not used.
- Sessions carry an explicit title (`🐝 <swarm> / <member>`) so the auto-title
  agent never renames them, plus swarm metadata for identification.

Because members are ordinary chats, the user can open any member session and
talk to it directly. See `src/runtime/opencode-runtime.ts` and
`src/core/swarm.ts` (`spawnMember`, `respawnMember`).

### Pause / resume semantics

- While the user is talking to a member, the swarm **yields** for that member:
  mail delivery is deferred, task continuation is suppressed, and the scheduler
  does not assign new work.
- After a configurable lull with no human message (default 5 minutes,
  `humanChatLullMs`), normal machinery resumes automatically.
- `swarm_release` ends the pause immediately (see §2).

### Mid-turn absorption

OpenCode's run loop re-reads the session history each iteration. A message the
user sends while a member is mid-task is **absorbed natively** — the member
answers the user between tool calls without any plugin-side reply injection.
The plugin only records that a human conversation is happening; it never
injects synthetic replies.

---

## 2. Human-in-the-loop

### Chat detection and classification

The plugin hooks `chat.message` and classifies each message as either a human
message or one of its own injections:

- **Self-injection registry:** when the plugin injects a prompt it records the
  message id; a matching id is treated as self.
- **Prefix fallback:** if the SDK does not echo the id, known prefix strings
  (`[SWARM INBOX —`, `[TEAM SYNC —`, `[WATCHDOG]`, `You are \``, `[ASSIGNED TASK`,
  etc.) classify the message as self.

Anything that matches neither is a **human message** and starts/extends the
member's chat state. The coordinator's own session is exempt.

See `src/humanchat/tracker.ts` and `test/unit/humanchat.test.ts`.

### Swarm yields during chat

While a member is "chatting" (within the lull window):

- the broker **defers mail** for that member (messages stay `queued`);
- the `session.idle` handler **does not continue** the member's task;
- the scheduler sweep **skips** the member as an assignment candidate;
- `reviveInterrupted` does not re-drive a chatting member.

The watchdog still treats human chat as liveness, and recovery/startup
reconcile lapsed chat state so a member is not suppressed forever after a
plugin restart.

### Resume

- **Lull expiry:** the next normal sweep/idle after `humanChatLullMs` resumes
  the member automatically.
- **`swarm_release <member>`:** clears the chat pause immediately and resumes
  machinery — queued mail is delivered, an owned task is continued, and the
  scheduler runs. The tool reports what actually resumed.

See `src/plugin.ts` (`chat.message` hook, `swarm_release`, sweep wiring) and
`test/unit/tools.test.ts`.

---

## 3. Permissions model

The plugin answers member `permission.ask` requests through the
`permission.ask` hook (`src/plugin.ts`, `autoAllowSwarmPermission`):

1. **Coordinator inheritance (primary):** the coordinator is the swarm's
   authority. The member's verdict is resolved from the coordinator's agent
   permission block at ask time (`inheritCoordinatorPermission`).
2. **Worktree/temp scoping (fallback):** when the coordinator's permissions
   cannot be resolved, path operations are auto-allowed only when the pattern
   is **exactly the swarm worktree / OS temp dir, or a descendant** (prefix +
   separator). A sibling directory sharing a string prefix is NOT in scope.

### Boundary hardening

The boundary check is deliberately strict:

- **No substring bypass:** a sibling path such as `C:/repo/app-evil` does not
  match worktree `C:/repo/app`.
- **No `..` traversal:** any pattern containing a `..` path segment is rejected
  (a pattern like `C:/repo/app/../outside` resolves outside the boundary and is
  never auto-allowed). A lone `.` (current directory) in a relative pattern is
  harmless and allowed.
- **No bare-`*` for bash:** `*` may auto-allow file operations, but a bare `*`
  bash pattern is never auto-allowed (it would authorize every command).
- **Legacy empty worktree:** a swarm with no directory still auto-allows
  non-bash path operations (legacy behavior), but **bash is never blanket
  allowed** — it stays `ask` with a console warning.
- **webfetch is `ask` by default** unless the coordinator explicitly allows it.

See `test/unit/tools.test.ts` (permission tests) for the boundary regressions.

---

## 4. Autopermissions propagation

When the user toggles autopermissions in the coordinator session, the plugin
can propagate the state to existing members. Three cases are distinguished
(`src/probe/autopermissions.ts`, `src/permissions/*`):

- **Case A — agent block visible:** the coordinator's agent permission block is
  the live source. Members already inherit pull-based on each `permission.ask`;
  no write is needed.
- **Case B — session-private permission:** the coordinator's session carries its
  own `permission` field. The plugin **clamps** it and writes the clamped
  ruleset to every active worker member via `updateSession({ permission })` on
  the periodic sweep.
- **Case C — no surface:** neither source is observable plugin-only. No write is
  performed; a documented emulation fallback (coordinator-verdict cache) is the
  escape hatch.

### The clamp never widens

`clampPropagatedPermissions` (`src/permissions/clamp.ts`) is monotone:

- **only `edit`** may propagate as `allow` (the worktree-shaped surface);
- **`bash`, `webfetch`, `external_directory` are always `ask`** even when the
  coordinator's ruleset allows them — a blanket allow for these is not
  path-scoped and is never copied verbatim to members;
- per-command bash object rules are not propagated (paths may be out of scope).

So propagation is strictly narrower than the coordinator — never wider.

### Per-member diagnostics

`swarm_roster` and `swarm_status` render a `perms:` line with the effective
mode (`inherit`, `worktree-scoped`, `accept-all-static`, `unknown`) and, in
Case B, per-member updated/skipped counts with reasons (e.g. `2/3 updated; 1
skipped: stopped`). See `test/unit/autopermissions.test.ts`.

---

## 5. Probe

`bun run probe` runs a compatibility probe against a running OpenCode server
(`src/probe/compat.ts`). It checks server health, session create/list/roots,
events, prompts, and — for this integration — the **autopermissions
classification**:

- `autopermissions.probe` classifies the observed permission surface as
  Case A / Case B / Case C / mixed against a coordinator session.
- **Graceful degradation:** when no surface is observable (Case C), the check is
  reported as informational rather than failing the whole probe.

See `src/probe/autopermissions.ts` and `test/unit/probe-compat.test.ts`.

---

## 6. Destructive operations

Destructive tool surfaces are gated to protect the swarm:

- `swarm_stop` requires an **explicit member name** (no silent whole-swarm
  default), is coordinator-gated (workers may stop themselves), cannot target
  the coordinator, and **releases the member's owned task** before stopping.
- `swarm_remove` releases the member's tasks and frees its roster slot.
- `swarm_delete` requires **`confirm` = the exact swarm name** and is
  coordinator-only; it tears down members, tasks, messages, and blackboard
  state.

Releasing a task on stop/delete keeps the DAG advancing — an owned task is
never stranded with a dead owner. See `test/unit/tools.test.ts`.

---

## Verification

- `bun run typecheck && bun test && bun run build`
- Permission behavior: `test/unit/autopermissions.test.ts`,
  `test/unit/tools.test.ts` (permission.ask tests).
- Human-chat state machine: `test/unit/humanchat.test.ts`.
- Probe classification: `test/unit/probe-compat.test.ts`.
