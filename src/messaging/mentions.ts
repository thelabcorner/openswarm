/**
 * @mention extraction (features/rich-agent-mentions).
 *
 * GitHub-inspired references inside swarm message bodies:
 *   - `@name`      mention a swarm member (auto-notify: the message is ALSO
 *                  delivered to them). Case-insensitive, word-boundary match.
 *   - `@file:path` reference a file relative to the swarm worktree (resolved +
 *                  validated at send time; unresolved refs are surfaced).
 *   - `#task`      reference a task by id or title (resolved at send time;
 *                  unresolved refs are surfaced).
 *
 * All extractors are PURE — no store/filesystem access — so the formatter can
 * render a `mentions:` hint and the core can auto-notify without coupling.
 */

/** `@name` tokens that resolve to a member in `memberNames` (returns the
 * canonical casing from the input list). `@file:` references are excluded so
 * they never read as a member named "file". */
export function extractMemberMentions(body: string, memberNames: Iterable<string>): string[] {
  const byLower = new Map<string, string>();
  for (const n of memberNames) byLower.set(n.toLowerCase(), n);
  if (byLower.size === 0) return [];
  // Strip @file: refs first so "@file:src/x.ts" never matches a member "file".
  const cleaned = body.replace(/@file:[^\s,]+/g, "");
  const found = new Set<string>();
  const re = /@([A-Za-z0-9][A-Za-z0-9_.-]*)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(cleaned)) !== null) {
    const canonical = byLower.get(m[1]!.toLowerCase());
    if (canonical) found.add(canonical);
  }
  return [...found];
}

/** Every `@token` in the body (excluding `@file:` refs) — used to report which
 * mentions did NOT resolve to a member. */
export function extractAllMentionTokens(body: string): string[] {
  const cleaned = body.replace(/@file:[^\s,]+/g, "");
  const out: string[] = [];
  const re = /@([A-Za-z0-9][A-Za-z0-9_.-]*)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(cleaned)) !== null) out.push(m[1]!);
  return out;
}

/** `@file:<path>` references (path token, up to whitespace/comma). */
export function extractFileMentions(body: string): string[] {
  const out: string[] = [];
  const re = /@file:([^\s,]+)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(body)) !== null) out.push(m[1]!);
  return out;
}

/** `#token` references (task ids / titles). */
export function extractTaskMentions(body: string): string[] {
  const out: string[] = [];
  const re = /#([A-Za-z0-9][A-Za-z0-9_-]*)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(body)) !== null) out.push(m[1]!);
  return out;
}
