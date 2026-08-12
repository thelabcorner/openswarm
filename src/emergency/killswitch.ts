/**
 * Emergency kill switch (task t-emergency): a layered shutoff so the operator
 * can instantly halt runaway swarm activity — and automatic tripwires that
 * freeze the system without a human. The coordinator's own session is NEVER
 * touched; the kill switch freezes swarm MACHINERY (scheduler, spawns, message
 * delivery), not the user's chat.
 *
 * Backend-agnostic: state persists to a JSON file (dataDir/.opencode/swarms/
 * emergency.json) so it survives restarts without touching the store layer.
 */

import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

export type EmergencyLevel = "freeze" | "stop" | "nuke";

export interface EmergencyTripwires {
  /** Max member spawns per minute across the project before auto-freeze. */
  maxSpawnsPerMin: number;
  /** Max messages per swarm per minute before auto-freeze. */
  maxMessagesPerMin: number;
  /** Hard cap on total member rows across the store — refuse spawn outright. */
  maxMembers: number;
  /** Max task creations per minute before auto-freeze. */
  maxTasksPerMin: number;
}

export interface EmergencyState {
  tripped: boolean;
  level: EmergencyLevel | null;
  trippedAt: number | null;
  trippedBy: string;
  reason?: string;
  tripwires: EmergencyTripwires;
}

export const DEFAULT_TRIPWIRES: EmergencyTripwires = {
  maxSpawnsPerMin: 500,
  maxMessagesPerMin: 1000,
  maxMembers: 1000,
  maxTasksPerMin: 500,
};

function defaultState(): EmergencyState {
  return {
    tripped: false,
    level: null,
    trippedAt: null,
    trippedBy: "",
    tripwires: { ...DEFAULT_TRIPWIRES },
  };
}

/** The exact confirm strings the operator must pass (never guessable). */
export const CONFIRM_CLEAR = "I CONFIRM RESUME";
export const CONFIRM_STOP = "STOP ALL";
export const CONFIRM_NUKE = "NUKE ALL";

/** The refusal message every guarded spawn/delegate/task path returns while
 * the kill switch is tripped. */
export function emergencyBlockMessage(level: EmergencyLevel | null): string {
  return `EMERGENCY STOP ACTIVE (${level ?? "freeze"}) — clear with swarm_emergency(action: clear, confirm: "${CONFIRM_CLEAR}")`;
}

/** The distinct header used for auto-trip coordinator notices. */
export function emergencyAutoTripHeader(reason: string): string {
  return (
    `[EMERGENCY] swarm auto-froze: ${reason}. Review with swarm_emergency(action: status). ` +
    `Clear with swarm_emergency(action: clear, confirm: "${CONFIRM_CLEAR}").`
  );
}

export class EmergencyGuard {
  private _state: EmergencyState;
  /** In-memory per-minute rate counters: key -> array of epoch-ms timestamps
   * within the current 60s window. Pruned on every bump. */
  private counters = new Map<string, number[]>();
  /** The trippedAt of the trip whose coordinator notices have already fired
   * (dedup: one notice per trip, never more). */
  private notifiedTripAt: number | null = null;

  constructor(private file: string) {
    this._state = defaultState();
  }

  // ==== state accessors ====

  get tripped(): boolean {
    return this._state.tripped;
  }

  get level(): EmergencyLevel | null {
    return this._state.level;
  }

  get state(): EmergencyState {
    return this._state;
  }

  memberCap(): number {
    return this._state.tripwires.maxMembers;
  }

  /** True when total member rows meet or exceed the hard cap. */
  memberCapExceeded(total: number): boolean {
    return total >= this._state.tripwires.maxMembers;
  }

  // ==== persistence ====

  /** Read the state file into the in-memory cache. Missing/corrupt file keeps
   * untripped defaults (a fresh start must never start tripped). */
  async load(): Promise<void> {
    try {
      if (!this.file || !existsSync(this.file)) return;
      const raw = JSON.parse(readFileSync(this.file, "utf8")) as Partial<EmergencyState>;
      this._state = {
        ...defaultState(),
        ...raw,
        tripwires: { ...DEFAULT_TRIPWIRES, ...(raw.tripwires ?? {}) },
      };
    } catch {
      // corrupt file — keep untripped defaults, never start tripped
      this._state = defaultState();
    }
  }

  /** Atomic-ish write: tmp file + rename. Best-effort (a failed save must not
   * throw into the hot path — the in-memory state is still authoritative). */
  async save(): Promise<void> {
    if (!this.file) return;
    try {
      mkdirSync(dirname(this.file), { recursive: true });
      const tmp = `${this.file}.tmp`;
      writeFileSync(tmp, JSON.stringify(this._state, null, 2), "utf8");
      renameSync(tmp, this.file);
    } catch (err) {
      console.error(`[swarm] emergency state save failed:`, err);
    }
  }

  // ==== trip / clear ====

  /** Trip the kill switch at the given level. Records who/why. */
  async trip(level: EmergencyLevel, by: string, reason?: string): Promise<void> {
    this._state = {
      ...this._state,
      tripped: true,
      level,
      trippedAt: Date.now(),
      trippedBy: by,
      reason,
    };
    await this.save();
  }

  /** Untrip, reset rate counters, keep the tripwire config. */
  async clear(): Promise<void> {
    this._state = { ...this._state, tripped: false, level: null, trippedAt: null, trippedBy: "", reason: undefined };
    this.counters.clear();
    this.notifiedTripAt = null;
    await this.save();
  }

  // ==== auto-trip notice dedup (one notice per trip, never more) ====

  /** Whether the coordinator notice for the current trip still needs firing. */
  needNotify(): boolean {
    return this._state.tripped && this.notifiedTripAt !== this._state.trippedAt;
  }

  markNotified(): void {
    this.notifiedTripAt = this._state.trippedAt;
  }

  // ==== rate counters (per-minute windows) ====

  private bump(key: string, limit: number, amount = 1): { count: number; exceeded: boolean } {
    const now = Date.now();
    const arr = (this.counters.get(key) ?? []).filter((t) => now - t < 60_000);
    for (let i = 0; i < amount; i++) arr.push(now);
    this.counters.set(key, arr);
    return { count: arr.length, exceeded: arr.length > limit };
  }

  /** Record spawns; returns count in the window + whether the limit is exceeded. */
  recordSpawn(amount = 1): { count: number; exceeded: boolean } {
    return this.bump("spawns", this._state.tripwires.maxSpawnsPerMin, amount);
  }

  /** Record a message send for a swarm; per-swarm per-minute window. */
  recordMessage(swarmId: string): { count: number; exceeded: boolean } {
    return this.bump(`msg:${swarmId}`, this._state.tripwires.maxMessagesPerMin);
  }

  /** Record a task creation; per-minute window. */
  recordTask(amount = 1): { count: number; exceeded: boolean } {
    return this.bump("tasks", this._state.tripwires.maxTasksPerMin, amount);
  }
}
