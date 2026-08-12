import { describe, expect, test } from "bun:test";
import { HumanChatTracker } from "../../src/humanchat/tracker.ts";
import type { SwarmMember, SwarmPolicies } from "../../src/core/types.ts";

const POLICIES: SwarmPolicies = {
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

class FakeClock {
  t = 1_000_000;
  now(): number { return this.t; }
  advance(ms: number): void { this.t += ms; }
}

/** In-memory store stub with just the member methods the tracker needs. */
class StubStore {
  members = new Map<string, SwarmMember>();
  bySession = new Map<string, SwarmMember>();
  chatLog: Array<{ id: string; at: number | null }> = [];

  set(m: SwarmMember): void {
    this.members.set(m.id, m);
    this.bySession.set(m.sessionId, m);
  }
  async getMemberBySessionId(sessionID: string): Promise<SwarmMember | undefined> {
    return this.bySession.get(sessionID);
  }
  async updateMemberHumanChat(memberId: string, humanChatAt: number | null): Promise<void> {
    this.chatLog.push({ id: memberId, at: humanChatAt });
    const m = this.members.get(memberId);
    if (m) m.humanChatAt = humanChatAt;
  }
}

function makeMember(id: string, sessionID: string, over: Partial<SwarmMember> = {}): SwarmMember {
  return {
    id,
    swarmId: "s1",
    name: id,
    role: "worker",
    sessionId: sessionID,
    status: "idle",
    workspaceMode: "worktree",
    humanChatAt: null,
    createdAt: 0,
    updatedAt: 0,
    ...over,
  };
}

function makeTracker(clock: FakeClock, store: StubStore, lullMs = 300_000) {
  return new HumanChatTracker(
    { store, now: () => clock.now() },
    { selfInjectionIds: new Set<string>(), lullMsFor: () => lullMs },
  );
}

describe("HumanChatTracker — classification", () => {
  test("self-injection ids are recognized and consumed", () => {
    const t = makeTracker(new FakeClock(), new StubStore());
    t.registerInjection("swarm-inj-m1-123");
    expect(t.isSelfInjection("swarm-inj-m1-123", "anything")).toBe(true);
    expect(t.isSelfInjection("some-other-id", "anything")).toBe(false);
    t.consumeInjection("swarm-inj-m1-123");
    expect(t.isSelfInjection("swarm-inj-m1-123", "anything")).toBe(false);
  });

  test("known injected text prefixes classify as self", () => {
    const t = makeTracker(new FakeClock(), new StubStore());
    expect(t.isSelfInjection(undefined, "[NEW MESSAGES (3) FROM: alice, bob]")).toBe(true);
    expect(t.isSelfInjection(undefined, "[TEAM SYNC — s1]")).toBe(true);
    expect(t.isSelfInjection(undefined, "You went idle while working on task")).toBe(true);
    expect(t.isSelfInjection(undefined, "[WATCHDOG] your session appears stalled")).toBe(true);
    expect(t.isSelfInjection(undefined, "You are `worker`")).toBe(true);
    expect(t.isSelfInjection(undefined, "Resumed after a restart")).toBe(true);
    expect(t.isSelfInjection(undefined, "[PENDING USER REPLY]")).toBe(true);
    expect(t.isSelfInjection(undefined, "Resumed after a re-root")).toBe(true);
  });

  test("an unrecognized message is HUMAN", () => {
    const t = makeTracker(new FakeClock(), new StubStore());
    expect(t.isSelfInjection(undefined, "hey, can you change the timeout to 5s?")).toBe(false);
    expect(t.isSelfInjection("user-msg-1", "how is the DLL build going?")).toBe(false);
  });

  test("scheduler assignment kickoff classifies as SELF (D3 regression)", () => {
    // buildAssignmentPrompt (scheduler.ts) starts with "You are `name`" (backtick)
    // and contains "[ASSIGNED TASK <id>]" — both must classify as self so the
    // chat.message hook never mistakes a scheduler kickoff for a human message
    // (which would set a spurious humanChatAt and pause machinery for the lull).
    const t = makeTracker(new FakeClock(), new StubStore());
    const kickoff =
      "You are `backend`, impl, a peer in swarm `s1` (swarmId: s1).\n[ASSIGNED TASK t-42]\npack the array";
    expect(t.isSelfInjection(undefined, kickoff)).toBe(true);
    expect(t.isSelfInjection(undefined, "[ASSIGNED TASK t-7]")).toBe(true);
    expect(t.isSelfInjection(undefined, "You are `backend`, impl, a peer in swarm `s1`.")).toBe(true);
  });
});

describe("HumanChatTracker — state machine", () => {
  test("a human message records humanChatAt (E1)", async () => {
    const clock = new FakeClock();
    const store = new StubStore();
    const m = makeMember("m1", "ses-1");
    store.set(m);
    const t = makeTracker(clock, store);

    expect(await t.onUserMessage("ses-1", false)).toBe(true);
    expect(store.members.get("m1")!.humanChatAt).toBe(clock.t);
    // chatting while within lull
    expect(await t.chatting(m, { policies: POLICIES })).toBe(true);
    clock.advance(299_000);
    expect(await t.chatting(m, { policies: POLICIES })).toBe(true);
    clock.advance(2_000); // past lull
    expect(await t.chatting(m, { policies: POLICIES })).toBe(false);
  });

  test("a self-injection does NOT record humanChatAt", async () => {
    const clock = new FakeClock();
    const store = new StubStore();
    store.set(makeMember("m1", "ses-1"));
    const t = makeTracker(clock, store);
    t.registerInjection("swarm-inj-m1-1");
    expect(await t.onUserMessage("ses-1", true)).toBe(false);
    expect(store.members.get("m1")!.humanChatAt).toBeNull();
  });

  test("a human message records chat state even while busy (E2/E3 — native reply, no injection)", async () => {
    const clock = new FakeClock();
    const store = new StubStore();
    store.set(makeMember("m1", "ses-1"));
    const t = makeTracker(clock, store);

    // OpenCode natively absorbs a mid-turn message; the tracker only records
    // that the user is chatting. Busy or not, chat state is set and machinery
    // yields.
    await t.onUserMessage("ses-1", false);
    expect(store.members.get("m1")!.humanChatAt).toBe(clock.t);
    expect(await t.chatting(store.members.get("m1")!, { policies: POLICIES })).toBe(true);
  });

  test("clear() resets chat state (E8/E11)", async () => {
    const clock = new FakeClock();
    const store = new StubStore();
    store.set(makeMember("m1", "ses-1"));
    const t = makeTracker(clock, store);
    await t.onUserMessage("ses-1", false);
    expect(store.members.get("m1")!.humanChatAt).toBe(clock.t);

    await t.clear("ses-1");
    expect(store.members.get("m1")!.humanChatAt).toBeNull();
    expect(await t.chatting(store.members.get("m1")!, { policies: POLICIES })).toBe(false);
  });

  test("reconcileStartup clears lapsed chat (E7)", async () => {
    const clock = new FakeClock();
    const store = new StubStore();
    const m = makeMember("m1", "ses-1", { humanChatAt: clock.t - 400_000 });
    store.set(m);
    const t = makeTracker(clock, store);
    await t.reconcileStartup([m], { policies: POLICIES });
    expect(store.members.get("m1")!.humanChatAt).toBeNull();
  });

  test("reconcileStartup keeps fresh chat", async () => {
    const clock = new FakeClock();
    const store = new StubStore();
    const m = makeMember("m1", "ses-1", { humanChatAt: clock.t - 60_000 });
    store.set(m);
    const t = makeTracker(clock, store);
    await t.reconcileStartup([m], { policies: POLICIES });
    expect(store.members.get("m1")!.humanChatAt).toBe(clock.t - 60_000);
  });

  test("coordinator messages never register (E10)", async () => {
    const clock = new FakeClock();
    const store = new StubStore();
    const coord = makeMember("coord", "ses-coord", { role: "coordinator" });
    store.set(coord);
    const t = makeTracker(clock, store);
    expect(await t.onUserMessage("ses-coord", false)).toBe(false);
    expect(store.members.get("coord")!.humanChatAt).toBeNull();
  });
});
