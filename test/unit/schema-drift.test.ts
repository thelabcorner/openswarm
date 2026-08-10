import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

/**
 * Schema drift test (build-pathclaims-schema, audit S12): the runtime SCHEMA
 * constant in sqlite-store.ts and the standalone src/storage/schema.sql are
 * TWO sources of truth for the same DDL. If they drift, a fresh database
 * (schema.sql applied by hand) and the plugin's own SCHEMA (applied by
 * ready()) diverge — inserts succeed against one and fail against the other.
 *
 * Normalize both (strip comments, collapse whitespace, drop the template
 * expression marker) and compare the ordered statement list. Any column/table/
 * index added to one must be mirrored in the other.
 */

const STORE_PATH = join(import.meta.dir, "../../src/storage/sqlite-store.ts");
const SCHEMA_SQL_PATH = join(import.meta.dir, "../../src/storage/schema.sql");

function extractSchemaConst(): string {
  const src = readFileSync(STORE_PATH, "utf8");
  // const SCHEMA = /* sql */ ` ... `;
  const m = src.match(/const SCHEMA = \/\* sql \*\/ `([\s\S]*?)`;/);
  if (!m) throw new Error("SCHEMA const not found in sqlite-store.ts");
  return m[1]!;
}

/** Strip SQL comments, the template-expression marker line, and normalize
 * whitespace so formatting differences don't count as drift. CRLF-aware: the
 * repo may be checked out with \r\n, and `$` does not match before \r. */
function normalize(sql: string): string[] {
  return sql
    .replace(/\r\n/g, "\n")
    .split("\n")
    // drop the ${/* ... */ ""} template marker line
    .filter((l) => !l.includes("${"))
    // strip inline -- comments (after CRLF normalization)
    .map((l) => l.replace(/--.*$/, ""))
    .join("\n")
    // collapse all whitespace runs to a single space
    .replace(/\s+/g, " ")
    .split(";")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

describe("schema drift (schema.sql vs SCHEMA const)", () => {
  test("runtime SCHEMA const and schema.sql describe the same DDL", () => {
    const fromConst = normalize(extractSchemaConst());
    const fromFile = normalize(readFileSync(SCHEMA_SQL_PATH, "utf8"));

    // PRAGMA lines are applied separately in ready() (journal_mode cannot be
    // set inside a transaction), so schema.sql's PRAGMA block is not expected
    // in the const — filter it from the file side.
    const fileStmts = fromFile.filter((s) => !s.startsWith("PRAGMA"));

    expect(fromConst.length).toBeGreaterThan(0);
    expect(fileStmts.length).toBe(fromConst.length);
    for (let i = 0; i < fromConst.length; i++) {
      expect(fileStmts[i]).toBe(fromConst[i]);
    }
  });

  test("every column added post-v1 is present in BOTH copies", () => {
    const constText = extractSchemaConst();
    const fileText = readFileSync(SCHEMA_SQL_PATH, "utf8");
    // Columns that must be mirrored (add to this list when a new column ships).
    const sentinelColumns = [
      "directory TEXT NOT NULL DEFAULT ''", // swarm (v2)
      "human_chat_at INTEGER", // swarm_member (v3)
      "expires_at INTEGER", // swarm_path_claim (v4) + swarm_message
      "error_sig TEXT", // swarm_artifact_annotation (v5, Hive H0)
      "solution_hash TEXT", // swarm_artifact_annotation (v5, Hive H0)
      "fact_hash TEXT", // swarm_belief (v6, Hive H1)
      "reinforce_count INTEGER", // swarm_belief (v6, Hive H1)
      "evidence_refs TEXT", // swarm_belief (v6, Hive H1)
      "resonant_at INTEGER", // swarm_belief (v7, Hive H2)
    ];
    for (const col of sentinelColumns) {
      expect(constText).toContain(col);
      expect(fileText).toContain(col);
    }
  });
});
