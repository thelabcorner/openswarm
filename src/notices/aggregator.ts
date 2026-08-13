/**
 * Notice aggregator (t-flood-aggregate) — the anti-flood core.
 *
 * One per SwarmPluginRuntime. All coordinator-facing NOTICES (completion
 * batches, member failures, watchdog releases/respawns, stuck members, F6
 * dependent fan-outs, [STICKY]/[ADVISORY] scheduler warnings, [PERMISSION
 * ALLOWED] / [CHAT FAILURE] / [USAGE LIMIT] advisories, emergency auto-trips)
 * funnel through `notifySwarm` and are delivered as ONE debounced digest turn
 * to the coordinator session per swarm — instead of N separate messages.
 *
 * Flood semantics (real deployment evidence: a member received 47 inbox
 * deliveries with 17 inside 60s; the previous 1.5s completion batcher fired
 * every window under sustained churn):
 *   - Debounced flush: default 5s window (policy noticeFlushMs), reset on
 *     every notifySwarm. No entries -> no timer -> no message (zero noise).
 *   - One message per swarm per flush: '[SWARM: <name>] N update(s):' +
 *     '- kind: text' lines, text truncated to 160 chars.
 *   - Line cap: default 10 lines (policy noticeLineCap) + '+M more' overflow.
 *   - Churn collapse: entries sharing (taskId, kind) within the window become
 *     'task X: released x2, claimed x3' — one line per task.
 *   - Advisory suppression counters: when the flood caps suppress an advisory,
 *     recordSuppressed() bumps a per-swarm counter; the digest renders
 *     '+N suppressed' at flush.
 *
 * The TIMELINE is untouched — it stays the per-event detail store; this class
 * only collapses the coordinator NOTICE stream.
 *
 * Peer messaging (swarm_message to members) is deliberately NOT routed here —
 * only system notices to the coordinator session.
 */

/** One coordinator-facing notice. */
export interface NoticeEntry {
  /** Notice class: 'completed' | 'failed' | 'watchdog' | 'stuck' |
   * 'advisory' | 'sticky' | 'delivery' | 'dependents' | 'chat-failure' |
   * 'usage-limit' | 'emergency' | ... (free-form, rendered in the line). */
  kind: string;
  /** Member the notice is about (optional). */
  memberId?: string;
  /** Task the notice is about (optional — enables churn collapse). */
  taskId?: string;
  /** One-line human text (truncated to NOTICE_TEXT_MAX in the digest). */
  text: string;
}

/** The minimal swarm shape the aggregator needs to deliver a digest. */
export interface NoticeSwarmRef {
  id: string;
  name: string;
  coordinatorSessionId: string;
  policies?: { noticeFlushMs?: number; noticeLineCap?: number };
}

export interface NoticeAggregatorDeps {
  /** Resolve a swarm (id/name/coordinator session + notice policies). The
   * store's getSwarm returns undefined for a missing swarm. */
  getSwarm(swarmId: string): Promise<NoticeSwarmRef | null | undefined>;
  /** Deliver the digest turn to the coordinator session. */
  promptAsync(input: { text: string }, sessionID: string): Promise<unknown>;
}

/** Default debounce window before a swarm's notices are flushed as ONE digest. */
export const DEFAULT_NOTICE_FLUSH_MS = 5_000;
/** Default max digest lines per flush (further lines collapse into '+M more'). */
export const DEFAULT_NOTICE_LINE_CAP = 10;
/** Max chars for an entry's text inside a digest line. */
export const NOTICE_TEXT_MAX = 160;

interface PendingSwarm {
  entries: NoticeEntry[];
  /** Advisories suppressed by the flood caps since the last flush. */
  suppressed: number;
  timer?: ReturnType<typeof setTimeout>;
}

/** Truncate a single line of text to NOTICE_TEXT_MAX (+ ellipsis when cut). */
function truncateText(s: string): string {
  if (s.length <= NOTICE_TEXT_MAX) return s;
  return `${s.slice(0, NOTICE_TEXT_MAX)}…`;
}

export class NoticeAggregator {
  private readonly pending = new Map<string, PendingSwarm>();

  constructor(
    private readonly deps: NoticeAggregatorDeps,
    private readonly defaults: { flushMs?: number; lineCap?: number } = {},
  ) {}

  /**
   * THE single entry point for coordinator-facing notices. Queues the entry
   * and (re)arms the per-swarm debounce. Never throws.
   */
  async notifySwarm(swarmId: string, entry: NoticeEntry): Promise<void> {
    let pending = this.pending.get(swarmId);
    if (!pending) {
      pending = { entries: [], suppressed: 0 };
      this.pending.set(swarmId, pending);
    }
    pending.entries.push(entry);

    let flushMs = this.defaults.flushMs ?? DEFAULT_NOTICE_FLUSH_MS;
    try {
      const swarm = await this.deps.getSwarm(swarmId);
      flushMs = swarm?.policies?.noticeFlushMs ?? flushMs;
    } catch {
      // Unreachable store — keep the default window; the notice still lands.
    }
    // A concurrent flush() may have drained this swarm while we awaited —
    // only (re)arm the timer when our bucket is still the live one.
    if (this.pending.get(swarmId) !== pending) return;
    if (pending.timer) clearTimeout(pending.timer);
    pending.timer = setTimeout(() => {
      pending.timer = undefined;
      void this.flush(swarmId).catch((err) => {
        console.error(`[swarm] notice digest flush failed for ${swarmId}:`, err);
      });
    }, flushMs);
  }

  /** Count one advisory suppressed by the flood caps (renders '+N suppressed'). */
  recordSuppressed(swarmId: string): void {
    let pending = this.pending.get(swarmId);
    if (!pending) {
      pending = { entries: [], suppressed: 0 };
      this.pending.set(swarmId, pending);
    }
    pending.suppressed++;
  }

  /**
   * Flush a swarm's pending notices NOW, delivering one digest turn. Returns
   * the rendered digest text (or null when nothing was pending). Used by the
   * emergency auto-trip path (a freeze must not wait out the debounce) and by
   * tests to avoid real 5s waits. Never throws.
   */
  async flush(swarmId: string): Promise<string | null> {
    const pending = this.pending.get(swarmId);
    if (!pending) return null;
    if (pending.timer) {
      clearTimeout(pending.timer);
      pending.timer = undefined;
    }
    this.pending.delete(swarmId);
    // Zero-noise rule: no entries -> no message (a lone '+N suppressed' is not
    // worth a turn; it rides with the next real notice or is dropped).
    if (pending.entries.length === 0) return null;
    let swarm: NoticeSwarmRef | null | undefined = null;
    try {
      swarm = await this.deps.getSwarm(swarmId);
    } catch {
      swarm = null;
    }
    if (!swarm) return null; // swarm gone — nothing to deliver to
    const lineCap =
      swarm.policies?.noticeLineCap ??
      this.defaults.lineCap ??
      DEFAULT_NOTICE_LINE_CAP;
    const text = renderDigest(swarm, pending, lineCap);
    try {
      await this.deps.promptAsync({ text }, swarm.coordinatorSessionId);
    } catch (err) {
      console.error(`[swarm] notice digest delivery to coordinator failed for ${swarmId}:`, err);
    }
    return text;
  }

  /** Clear all pending buckets + timers (plugin dispose). */
  dispose(): void {
    for (const pending of this.pending.values()) {
      if (pending.timer) clearTimeout(pending.timer);
    }
    this.pending.clear();
  }
}

/** Build the digest body: collapse churn by task, cap lines, add overflow. */
export function renderDigest(
  swarm: NoticeSwarmRef,
  pending: { entries: NoticeEntry[]; suppressed: number },
  lineCap = DEFAULT_NOTICE_LINE_CAP,
): string {
  // Churn collapse: group task entries by taskId -> kind -> entries. A kind
  // seen >= 2 times in the window collapses to 'kind xN' (text dropped — pure
  // churn noise); a kind seen once keeps its full text so the reason survives
  // ('t-bench: released x2, claimed x3' / 't-bench: failed: <reason>').
  const byTask = new Map<string, Map<string, NoticeEntry[]>>();
  const free: NoticeEntry[] = [];
  for (const e of pending.entries) {
    if (e.taskId) {
      let kinds = byTask.get(e.taskId);
      if (!kinds) {
        kinds = new Map();
        byTask.set(e.taskId, kinds);
      }
      const arr = kinds.get(e.kind);
      if (arr) arr.push(e);
      else kinds.set(e.kind, [e]);
    } else {
      free.push(e);
    }
  }
  const lines: string[] = [];
  for (const [taskId, kinds] of byTask) {
    const parts: string[] = [];
    for (const [kind, entries] of kinds) {
      if (entries.length > 1) parts.push(`${kind} x${entries.length}`);
      else parts.push(`${kind}: ${truncateText(entries[0]!.text)}`);
    }
    lines.push(`${taskId}: ${parts.join(", ")}`);
  }
  for (const e of free) {
    // Emergency notices are once-per-trip and carry the EXACT clear recipe —
    // never truncate them. Everything else is capped at NOTICE_TEXT_MAX.
    lines.push(`${e.kind}: ${e.kind === "emergency" ? e.text : truncateText(e.text)}`);
  }
  if (pending.suppressed > 0) {
    lines.push(`+${pending.suppressed} suppressed`);
  }
  const capped = lines.slice(0, lineCap);
  if (lines.length > lineCap) {
    capped.push(`+${lines.length - lineCap} more`);
  }
  return `[SWARM: ${swarm.name}] ${pending.entries.length} update(s):\n${capped.map((l) => `- ${l}`).join("\n")}`;
}
