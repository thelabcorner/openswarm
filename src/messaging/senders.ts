import type { SwarmMessage } from "../core/types.js";
import type { SwarmStore } from "../storage/store.js";

/**
 * Enrich a sender-name map with FOREIGN senders (cross-swarm messages): a
 * `from_member_id` that is not a member of the rendering swarm is resolved
 * globally and displayed as `name@swarm`, so a recipient always sees where a
 * cross-swarm message actually came from. In-swarm senders are already in the
 * map and pass through untouched. Best-effort: unknown members are left to
 * render as their raw id (caller's existing fallback).
 */
export async function enrichForeignSenderNames(
  store: SwarmStore,
  messages: SwarmMessage[],
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
