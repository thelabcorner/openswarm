import type { SwarmStore } from "../storage/store.js";

/** Anything carrying a sender/author member id — SwarmMessage satisfies this,
 * and so does a blackboard entry mapped to { fromMemberId } (t-cross-memory
 * author enrichment reuses the same foreign-name rendering). */
export type SenderRef = { fromMemberId: string };

/**
 * Enrich a sender-name map with FOREIGN senders (cross-swarm messages): a
 * `from_member_id` that is not a member of the rendering swarm is resolved
 * globally and displayed as `name@swarm`, so a recipient always sees where a
 * cross-swarm message actually came from. In-swarm senders are already in the
 * map and pass through untouched. Best-effort: unknown members are left to
 * render as their raw id (caller's existing fallback). Also used to render
 * blackboard AUTHORS in cross-swarm reads: the reader's swarm map already
 * holds in-swarm names, foreign authors resolve to `name@homeSwarm`.
 */
export async function enrichForeignSenderNames(
  store: SwarmStore,
  messages: SenderRef[],
  names: Map<string, string>,
): Promise<void> {
  for (const m of messages) {
    if (names.has(m.fromMemberId)) continue;
    const member = await store.getMemberById(m.fromMemberId);
    if (!member) continue;
    const home = await store.getSwarm(member.swarmId);
    names.set(m.fromMemberId, home?.name ? `${member.name}@${home.name}` : member.name);
  }
}
