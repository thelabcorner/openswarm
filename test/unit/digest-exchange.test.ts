import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { swarmPlugin, swarmRuntime } from "../../src/plugin.ts";
import type { Hooks } from "@opencode-ai/plugin";
import type { SwarmPluginRuntime } from "../../src/plugin.ts";

/**
 * Wave 6 minimal digest-exchange tests:
 *  - stale digest triggers AT MOST ONE targeted whisper sync (deduped per stale
 *    period; a new sync only when the stale digest VALUE changes).
 *  - fresh digest does NOT trigger a sync.
 *  - the sync reuses deliverNeed (matching members only, coordinator excluded,
 *    whisper = no coordinator copy).
 */
let dir: string;
let hooks: Hooks;

const coordinatorSession = "ses-tsync-lead";

const fakeClient = {
  session: {
    create: async (opts: any) => {
      const sessionID = `ses-tsync-${Math.random().toString(36).slice(2, 8)}`;
      if (opts.body?.parentID !== undefined) {
        throw new Error(`session.create received a parentID (${opts.body.parentID}); members must be root sessions`);
      }
      return {
        data: { id: sessionID, title: opts.body?.title, parentID: undefined, directory: "." },
        error: undefined,
      };
    },
    get: async () => ({ data: null, error: undefined }),
    children: async () => ({ data: [], error: undefined }),
    messages: async () => ({ data: [], error: undefined }),
    status: async () => ({ data: {}, error: undefined }),
    abort: async () => ({ data: undefined, error: undefined }),
    update: async () => ({ data: {}, error: undefined }),
    prompt: async () => ({ data: { info: {} }, error: undefined }),
    promptAsync: async () => ({ data: undefined, error: undefined }),
  },
};

const pluginInput: any = {
  client: fakeClient,
  project: { id: "proj-tsync" },
  directory: ".",
  worktree: ".",
  experimental_workspace: { register() {} },
  serverUrl: new URL("http://x"),
  $: {},
};

function ctx(sessionID: string): any {
  return {
    sessionID,
    messageID: "msg-call",
    agent: "build",
    directory: ".",
    worktree: ".",
    abort: new AbortController().signal,
    metadata() {},
    ask: () => {},
  };
}

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "swarms-tsync-test-"));
  hooks = await swarmPlugin(pluginInput, { dataDir: dir });
});

afterAll(async () => {
  // Reset the process-global plugin singleton so sibling test files that
  // assert `swarmRuntime() === undefined` at load (e.g. tools.test.ts) are not
  // affected by our swarmPlugin() initialization, regardless of file order.
  await (hooks as any).dispose?.();
  try { rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
});

async function rt(): Promise<SwarmPluginRuntime> {
  const r = await import("../../src/plugin.ts").then((m) => m.swarmRuntime());
  if (!r) throw new Error("swarmRuntime not initialized");
  return r;
}

async function makeSwarmWithMember(tag: string) {
  const create = await (hooks.tool as any).swarm_create.execute(
    { name: `tsync-${tag}` },
    ctx(`ses-tsync-lead-${tag}`),
  );
  const created = JSON.parse(String(create.output));
  const swarmId = created.swarm.id;
  // One worker member whose role matches a "beliefs" query.
  const del = await (hooks.tool as any).swarm_delegate.execute(
    {
      name: `tsync-${tag}`,
      members: [{ name: "keeper", role: "beliefs digest keeper", prompt: "stand by" }],
    },
    ctx(`ses-tsync-lead-${tag}`),
  );
  const out = JSON.parse(String(del.output ?? del));
  void out;
  return { swarmId };
}

async function insertBelief(swarmId: string, id: string, text: string, memberName: string) {
  const r = await rt();
  const member = await r.store.getMemberByName(swarmId, memberName);
  // Insert a REAL belief (the digest hashes beliefs, not annotations); unique
  // fact_hash per insert so each one changes the digest.
  await r.store.insertBelief({
    id,
    swarmId,
    factHash: `fh-${id}`,
    text,
    confidence: 0.6,
    tier: "whisper",
    authorMemberId: member!.id,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  });
}

describe("Wave 6 — minimal digest exchange (stale sync)", () => {
  // Count only the digest-sync findings (distinct body marker), addressed to
  // the keeper — deterministic regardless of async flip notices/kickoffs.
  async function syncCount(swarmId: string): Promise<number> {
    const r = await rt();
    const keeper = await r.store.getMemberByName(swarmId, "keeper");
    const msgs = await r.store.listMessagesBySwarm(swarmId, 100);
    return msgs.filter(
      (m) =>
        m.to.type === "member" &&
        m.to.memberId === keeper?.id &&
        m.kind === "finding" &&
        m.body.text.includes("Belief digest changed"),
    ).length;
  }

  test("stale digest triggers AT MOST ONE targeted whisper sync per stale period", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId } = await makeSwarmWithMember(tag);

    // First pass: no stored digest → health "unknown", no sync.
    await (await rt()).digestSweep(swarmId);
    const unknownEntry = await (await rt()).store.getBlackboard(swarmId, "hive/digest");
    expect(unknownEntry).toBeDefined();
    expect(JSON.parse(unknownEntry!.value).health).toBe("unknown");
    expect(await syncCount(swarmId)).toBe(0);

    // Change the belief set so the digest becomes stale.
    await insertBelief(swarmId, `b-${tag}-1`, "stale-trigger fact", "keeper");
    await (await rt()).digestSweep(swarmId);
    const staleEntry = await (await rt()).store.getBlackboard(swarmId, "hive/digest");
    expect(JSON.parse(staleEntry!.value).health).toBe("stale");
    expect(await syncCount(swarmId)).toBe(1);

    // Same stale digest value again → NO second sync (dedupe per period).
    await (await rt()).digestSweep(swarmId);
    expect(await syncCount(swarmId)).toBe(1);
  });

  test("fresh digest does NOT trigger a sync", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId } = await makeSwarmWithMember(tag);

    // First pass: unknown (no sync).
    await (await rt()).digestSweep(swarmId);
    expect(await syncCount(swarmId)).toBe(0);

    // Second pass with NO belief change: health stays fresh → no sync.
    await (await rt()).digestSweep(swarmId);
    const entry = await (await rt()).store.getBlackboard(swarmId, "hive/digest");
    expect(JSON.parse(entry!.value).health).toBe("fresh");
    expect(await syncCount(swarmId)).toBe(0);
  });

  test("a NEW stale digest value (beliefs moved again) fires one more sync", async () => {
    const tag = Math.random().toString(36).slice(2, 8);
    const { swarmId } = await makeSwarmWithMember(tag);

    await (await rt()).digestSweep(swarmId); // unknown
    await insertBelief(swarmId, `b-${tag}-a`, "first change", "keeper");
    await (await rt()).digestSweep(swarmId); // stale → sync #1
    expect(await syncCount(swarmId)).toBe(1);

    // Beliefs change again → NEW stale digest value → one more sync.
    await insertBelief(swarmId, `b-${tag}-b`, "second change", "keeper");
    await (await rt()).digestSweep(swarmId);
    expect(await syncCount(swarmId)).toBe(2);

    // Same value again → no further sync.
    await (await rt()).digestSweep(swarmId);
    expect(await syncCount(swarmId)).toBe(2);
  });
});
