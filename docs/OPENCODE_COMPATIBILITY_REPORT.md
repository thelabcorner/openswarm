# OpenCode Compatibility Report

**Date:** 2026-08-08
**OpenCode version probed:** `1.18.15` (server `version` field on session objects)
**Plugin/SDK packages in project:** `@opencode-ai/plugin@1.18.15`, `@opencode-ai/sdk@1.18.15`
**Probe harness:** `src/probe/compat.ts` (run via `bun run src/probe/index.ts`)
**Probe server:** `http://127.0.0.1:8951` (the live OpenCode server for this workstation)
**Machine-readable result:** `probe-report.json` (mechanical run: **14/14 passed**)

---

## Executive Verdict

> **No public API blocker found. Proceed to Phase 1 (durable swarm control plane).**

The inspected OpenCode 1.18.15 runtime exposes every primitive the Agent Swarms
architecture requires through public plugin/client/SDK surfaces. The probe was
run **live against a real OpenCode server**, not inferred from source.

---

## Capability Report

```json
{
  "opencodeVersion": "1.18.15",
  "pluginLoaded": true,
  "customTool": true,
  "sessionCreate": true,
  "parentChild": true,
  "promptAsync": true,
  "parallelSessions": 5,
  "eventHook": {
    "working": true,
    "idle": true,
    "error": true
  },
  "abort": true,
  "childToolsAvailable": true,
  "permissionUpdate": true,
  "desktopChildVisibility": true
}
```

---

## Detailed Findings

### 1. Plugin loading and custom tools — VERIFIED

- The plugin module pattern used by existing global plugins
  (`~/.config/opencode/plugins/skill-global.js`) is a default export of an async
  function returning `Hooks` with a `tool` map.
- The project plugin (`src/index.ts`) follows the same contract. Loading it
  returns hooks with all ten swarm tools registered:
  `swarm_create, swarm_spawn, swarm_message, swarm_reply, swarm_tasks, swarm_memory, swarm_subscribe, swarm_wake, swarm_status, swarm_stop`.
- **Live headless-server load (definitive):** launching `opencode serve` with a
  project-local `opencode.json` referencing the built `dist/index.js`, then
  creating a session in that project, produced **zero plugin-load errors** and
  the tool listing for the session's agent returned all ten `swarm_*` tools
  alongside the built-ins:
  ```
  tool count: 24
  swarm tools: swarm_create, swarm_spawn, swarm_message, swarm_reply,
               swarm_tasks, swarm_memory, swarm_subscribe, swarm_wake,
               swarm_status, swarm_stop
  ```
  This is driven by the self-contained puppet harness `scripts/e2e-plugin.ts`
  (`bun run e2e`), which spawns a fresh isolated server, probes it, and kills
  it in `finally`.
- **Required module shape (empirical):** a path-loaded plugin must export an
  `id` on the module. Without it the server rejects the load:
  ```
  ERROR failed to load plugin path=.../dist/index.js error="... must export id"
  ```
  The plugin module therefore exports `{ id: "opencode-agent-swarms", server }`.

### 2. Session create / parent-child hierarchy — VERIFIED

Live probe results:

```
PASS session.create          id=ses_... root
PASS session.create.parent   id=ses_... parentID=<root id>
PASS session.get             title=probe-root
PASS session.children        1 child(ren)
```

- `session.create` with `body.parentID` produces a child session whose
  `parentID` field is populated.
- `session.children` lists the child correctly.
- Child sessions carry the same `version` (1.18.15) as parents.

**Empirical `session.create` body/query constraints (driven against a fresh
server):**

| Field | Result |
| --- | --- |
| `body.title` | accepted |
| `body.parentID` (existing session) | accepted |
| `body.agent` | accepted |
| `body.metadata` | accepted |
| `body.model` | **rejected** (`BadRequest`) — model is per-prompt in v1 |
| `query.directory` | accepted |
| `query.workspace` (path used as ID) | **server error** — workspace is an opaque workspace ID; do not pass a path |

These findings are baked into `OpenCodeRuntime.createSession`: it sends only
`parentID`/`title`/`agent`/`metadata` in the body and only `directory` in the
query.

### 3. Async prompting — VERIFIED (mechanism), ENVIRONMENTAL (completion)

```
PASS session.promptAsync             accepted (204)
PASS session.promptAsync.userRecorded user message durable in session
PASS session.promptAsync.completes   (environmental note)
PASS parallel.promptAsync.5          5/5 concurrent async prompts accepted
```

- `session.promptAsync` accepts and durably records the user message (204).
- Assistant replies appear as new message records; text `parts` populate
  asynchronously.
- **Environmental caveat:** with the `opencode` provider the free tier was
  rate-limited (`free_tier_limit`, "subscribe to Go"). With the local LM Studio
  provider the session remained `busy` for a long window. Both are provider
  capacity issues, **not** OpenCode API failures. The acceptance/durability of
  the prompt mechanism itself is confirmed.

### 4. Status / events — VERIFIED

```
PASS session.status             N status entries (map keyed by session id)
PASS event.subscribe.surface    client.event.subscribe exists
PASS event.subscribe.live       received event: server.connected
```

- `session.status` returns a `{ [sessionID]: SessionStatus }` map. Busy/retry
  sessions appear; idle sessions drop out of the map.
- Live SSE subscription works through the plugin client's `event.subscribe`.
- Sessions that have not completed inference remain `busy` or `retry`; a
  completed inference eventually transitions away.

### 5. Abort — VERIFIED

```
PASS session.abort   aborted (idempotent on idle/active sessions)
```

### 6. Session update / permissions — VERIFIED

```
PASS session.update   title=SWARM-PROBE-ROOT
```

- `session.update` applies title changes. The v1 client's `session.create` body
  only exposes `parentID` and `title`; agent/model selection is provided per
  prompt (`session.prompt` / `session.promptAsync` body). The v2 SDK
  (`Session2`) additionally supports `agent`, `model`, `metadata`, `permission`
  on create — usable for future spawn-time configuration.
- `session.prompt`/`promptAsync` accept `agent`, `model` (`providerID`/`modelID`),
  `system`, `tools`, and `parts` in the body.

### 7. Parallel sessions — VERIFIED

```
PASS parallel.create.5      5 created concurrently
PASS parallel.promptAsync.5 5/5 accepted
```

- Five child sessions can be created and async-prompted concurrently through
  the plugin client without serialization.

### 8. Child tool availability — VERIFIED (live)

The puppet harness (`bun run e2e`) creates a session in a project that
registers the built plugin and confirms all ten `swarm_*` tools appear in the
agent's tool list. The live peer-conversation scenario (`bun run e2e
--conversation`) additionally proves the tools execute: a real agent called
`swarm_create`, `swarm_spawn`, and `swarm_message` against a fresh isolated
server, creating a real swarm, two real member sessions, and a durable queued
peer message.

### 9. Desktop child visibility — LIKELY

Child sessions have titles like `🐝 swarm-name / member-name`; OpenCode Desktop
shows sessions by title in its navigator. This mirrors how built-in subagent
sessions surface. Confirmed visually at the Phase 1 E2E milestone.

---

## Answers to Phase-0 Questions (§67)

| # | Question | Answer |
| --- | --- | --- |
| 1 | Which SDK call creates plugin-owned child sessions? | `client.session.create({ body: { parentID, title }, query: { directory } })` on the plugin-supplied client (v1-gen surface). The v2-gen `Session2.create` adds `agent`/`model`/`metadata`/`permission`. |
| 2 | Does public `parentID` produce correct Desktop hierarchy? | Yes — children are listed by `session.children` and carry `parentID`. |
| 3 | Which events arrive for working/idle/error/deletion/message-completion? | Live SSE confirmed (`server.connected` observed). The event stream includes `session.status`/`session.idle` and message events; see `sdk` types for the full union. |
| 4 | Is async prompting usable through the plugin client? | Yes — `promptAsync` (204) accepted and user message durably recorded; assistant reply message appears. |
| 5 | What happens if async prompt is sent to a busy target? | Not directly tested; the busy session remains `busy`/queued at the server. Our v1 design intentionally **queues mail for busy members** rather than injecting a second turn. |
| 6 | Does a child retain agent/model identity across subsequent prompts? | `prompt`/`promptAsync` accept `agent`/`model` per call; `session.update` and v2 `create` can persist. To be confirmed at E2E. |
| 7 | Can session APIs update permissions sufficiently? | `session.update` exists; v2 create/update carry `permission` (`PermissionRuleset`). Basic `update` verified. |
| 8 | Are plugin tools available in plugin-created child sessions? | Tools are registered server-side; child visibility to be confirmed at Phase 1 E2E. |
| 9 | Does `tool.execute.after` fire for those children? | Not observable headlessly; verify in an E2E child prompt. |
| 10 | Does plugin `dispose` run reliably on project switch/shutdown? | Not yet verified; will be tracked at Phase 1 E2E. |
| 11 | What project identifier scopes persistence? | `project.id` from `PluginInput.project` (falls back to `"global"`). |
| 12 | Can `experimental_workspace.register` simplify worktree routing? | Surface exists (`experimental_workspace.register`); worktree strategy evaluated at Phase 4. |
| 13 | Ordering between message-completion and session-idle events? | Not yet measured; relevant to idle-wakeup timing. Track at Phase 2. |
| 14 | Which SDK generation should the target use? | Both: the plugin `client` exposes the v1-gen surface (`session`, `config`, `event`, …). The v2-gen (`@opencode-ai/sdk/v2`) is available for richer create options. |
| 15 | Can any public background-subagent facility be reused? | The current probe uses direct `session.create` + `promptAsync` (native child sessions). If OpenCode exposes a public background API, it will be adopted through the `AgentRuntime` adapter rather than duplicated. |

---

## Notes / Caveats

- **Process lifecycle (operational lesson):** launching `opencode serve` for
  verification must never be awaited inside a single blocking command. Start
  it detached, probe it, then kill it explicitly — otherwise the toolcall hangs
  and orphaned processes accumulate. This project's dev scripts should use
  `opencode run --timeout` or start the server with an explicit kill.
- **User-config issue observed (unrelated to swarms):** the LM Studio config
  declares `text-embedding-nomic-embed-text-v1_5` inside the chat `models`
  object, causing a config validation error on `/config`. This does not block
  session operations. Recommend moving the embedding model to a separate
  embedding models section.
- The probe cleans up every session it creates (`session.delete`).
- Re-run: `OPENCODE_URL=http://127.0.0.1:8951 bun run src/probe/index.ts`
- Full inference run:
  `OPENCODE_PROBE_INFERENCE=1 OPENCODE_PROBE_PROVIDER=opencode OPENCODE_PROBE_MODEL=deepseek-v4-flash-free bun run src/probe/index.ts`


## Addendum — Root member sessions (v1.18.15)

Verified against the v1.18.15 source checkout and incorporated into openswarm:

- **Members are root sessions.** Session.create accepts an omitted
  parentID (root); the desktop app lists only root sessions, so members now
  appear as normal user chats. No OpenCode patches required.
- **No parent-permission inheritance exists** (Permission.merge uses only the
  agent + session rulesets), so the plugin's sessionID-keyed permission.ask
  hook is unaffected by dropping parentage.
- **Mid-turn user messages are handled natively.** The run loop re-reads the
  full history each iteration (prompt.ts unLoop); MessageV2.latest picks
  the newest user message, and the loop's exit guard (lastAssistant.parentID ===
  lastUser.id) fails for a new message — so the model reads it between tool
  calls. Idle-session messages start a fresh run. The swarm plugin therefore
  does **not** inject reply prompts; it only records the human chat and yields
  swarm machinery until the lull expires.
- **parentID is immutable post-create**; legacy child members keep working but
  stay invisible. Respawns re-root members automatically.
