import type { SwarmStore } from "../storage/store.js";
import type { NewSwarmEvent } from "../storage/models.js";

/**
 * Best-effort timeline recording (swarm timeline/replay).
 *
 * Event recording is telemetry, NEVER a control path: a store hiccup must not
 * fail the operation that triggered the event (a message send, a task claim, a
 * permission ask). `recordEvent` swallows every failure and only logs a
 * warning — callers can fire it without a try/catch.
 *
 * Every event is required to carry a `createdAt` (epoch ms) so replay is
 * deterministic; the helper defaults it to now when omitted.
 */
export async function recordEvent(
  store: SwarmStore,
  e: Omit<NewSwarmEvent, "createdAt"> & { createdAt?: number },
): Promise<void> {
  try {
    await store.insertEvent({ createdAt: Date.now(), ...e });
  } catch (err) {
    console.warn(`[swarm] timeline event recording failed (${e.type}): ${(err as Error).message}`);
  }
}
