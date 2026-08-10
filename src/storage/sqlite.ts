/**
 * Cross-runtime SQLite adapter.
 *
 * OpenCode's CLI runs the server under Bun, but the OpenCode Desktop app runs
 * its embedded server inside an Electron sidecar under Node. A plugin that
 * imports `bun:sqlite` unconditionally fails to load in Desktop. This module
 * picks the driver based on the running runtime:
 *
 *   - Bun   -> `bun:sqlite` (Database)
 *   - Node  -> `node:sqlite` (DatabaseSync, Node 22+)
 *
 * Both expose a compatible surface used by SQLiteStore:
 *   exec(sql), run(sql, params) -> { changes }, prepare(sql), query<T,P>(),
 *   transaction(), close().
 */

export type SQLBindings =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | Array<SQLBindings>;

export interface RunResult {
  changes: number;
  lastInsertRowid?: number | bigint;
}

export interface PreparedStatement {
  run(...params: unknown[]): RunResult;
  get(...params: unknown[]): Record<string, unknown> | undefined;
  all(...params: unknown[]): Record<string, unknown>[];
}

export interface Query<T> {
  get(...params: unknown[]): T | undefined;
  all(...params: unknown[]): T[];
}

/** Bun's Database.run/query accept params either variadic or as a single array. */
function flattenParams(params: unknown[]): unknown[] {
  if (params.length === 1 && Array.isArray(params[0])) return params[0] as unknown[];
  return params;
}

export interface SqliteLike {
  exec(sql: string): void;
  run(sql: string, ...params: unknown[]): RunResult;
  prepare(sql: string): PreparedStatement;
  /** `P` is a bindings type param kept for Bun-API source compatibility. */
  query<T, P = unknown[]>(sql: string): Query<T>;
  transaction<T>(fn: () => T): T;
  close(): void;
}

const isBun = typeof process === "object" && "isBun" in process && (process as { isBun?: boolean }).isBun === true;

/** Open a SQLite database using the appropriate driver for the runtime. */
export async function openSqlite(path: string): Promise<SqliteLike> {
  if (isBun) {
    const { Database } = await import("bun:sqlite");
    return new BunSqliteAdapter(new Database(path));
  }
  // Node 22+ built-in sqlite (experimental but stable enough for our use).
  const { DatabaseSync } = await import("node:sqlite");
  return new NodeSqliteAdapter(new DatabaseSync(path) as unknown as NodeDatabaseSync);
}

class BunSqliteAdapter implements SqliteLike {
  private db: { exec(s: string): void; run(s: string, ...p: unknown[]): { changes: number; lastInsertRowid: number | bigint }; prepare(s: string): { run(...p: unknown[]): { changes: number; lastInsertRowid: number | bigint }; get(...p: unknown[]): Record<string, unknown> | undefined; all(...p: unknown[]): Record<string, unknown>[] }; query(s: string): { get(...p: unknown[]): Record<string, unknown> | undefined; all(...p: unknown[]): Record<string, unknown>[] }; transaction(f: () => unknown): () => unknown; close(): void };
  constructor(db: unknown) {
    this.db = db as typeof this.db;
  }
  exec(sql: string): void { this.db.exec(sql); }
  run(sql: string, ...params: unknown[]): RunResult {
    const r = this.db.run(sql, ...flattenParams(params));
    return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
  }
  prepare(sql: string): PreparedStatement {
    const stmt = this.db.prepare(sql);
    return {
      run: (...p: unknown[]) => {
        const r = stmt.run(...flattenParams(p));
        return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
      },
      get: (...p: unknown[]) => stmt.get(...flattenParams(p)),
      all: (...p: unknown[]) => stmt.all(...flattenParams(p)),
    };
  }
  query<T>(sql: string): Query<T> {
    const q = this.db.query(sql);
    return { get: (...p: unknown[]) => q.get(...flattenParams(p)) as T | undefined, all: (...p: unknown[]) => q.all(...flattenParams(p)) as T[] };
  }
  transaction<T>(fn: () => T): T {
    const tx = this.db.transaction(fn);
    return tx() as T;
  }
  close(): void { this.db.close(); }
}

interface NodeStatementSync {
  run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  get(...params: unknown[]): Record<string, unknown> | undefined;
  all(...params: unknown[]): Record<string, unknown>[];
}
interface NodeDatabaseSync {
  exec(sql: string): void;
  prepare(sql: string): NodeStatementSync;
  close(): void;
}

class NodeSqliteAdapter implements SqliteLike {
  private db: NodeDatabaseSync;
  constructor(db: NodeDatabaseSync) { this.db = db; }
  exec(sql: string): void { this.db.exec(sql); }
  run(sql: string, ...params: unknown[]): RunResult {
    const r = this.db.prepare(sql).run(...flattenParams(params));
    return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
  }
  prepare(sql: string): PreparedStatement {
    const stmt = this.db.prepare(sql);
    return {
      run: (...p: unknown[]) => {
        const r = stmt.run(...flattenParams(p));
        return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
      },
      get: (...p: unknown[]) => stmt.get(...flattenParams(p)),
      all: (...p: unknown[]) => stmt.all(...flattenParams(p)),
    };
  }
  query<T>(sql: string): Query<T> {
    const stmt = this.db.prepare(sql);
    return { get: (...p: unknown[]) => stmt.get(...flattenParams(p)) as T | undefined, all: (...p: unknown[]) => stmt.all(...flattenParams(p)) as T[] };
  }
  transaction<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE;");
    try {
      const r = fn();
      this.db.exec("COMMIT;");
      return r;
    } catch (e) {
      try { this.db.exec("ROLLBACK;"); } catch { /* already aborted */ }
      throw e;
    }
  }
  close(): void { this.db.close(); }
}