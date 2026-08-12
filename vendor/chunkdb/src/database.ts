/**
 * Cross-runtime SQLite driver for chunkDB.
 *
 * OpenCode's CLI runs the server under Bun, but the OpenCode Desktop app runs
 * its embedded server inside an Electron sidecar under Node. A plugin that
 * imports `bun:sqlite` unconditionally fails to load in Desktop (the bundle
 * dies at module load — "Received protocol 'bun:'"). This module picks the
 * driver at OPEN time via dynamic import (load-safe in both runtimes):
 *
 *   - Bun  -> `bun:sqlite` (Database)
 *   - Node -> `node:sqlite` (DatabaseSync, Node 22+)
 *
 * Both are wrapped to the surface chunkDB uses:
 * run(sql, params) / query(sql).{get,all,run} / exec / transaction / close,
 * with named-parameter objects (`{namespace, key}` for `$namespace` SQL).
 */

export interface ChunkRunResult {
  changes: number;
  lastInsertRowid: number | bigint;
}

export interface ChunkStatement {
  get(...params: unknown[]): Record<string, unknown> | undefined;
  all(...params: unknown[]): Record<string, unknown>[];
  run(...params: unknown[]): ChunkRunResult;
}

export interface ChunkSqlite {
  exec(sql: string): void;
  run(sql: string, ...params: unknown[]): ChunkRunResult;
  query(sql: string): ChunkStatement;
  /** bun-style: returns a CALLABLE that runs the transaction when invoked. */
  transaction<T>(fn: () => T): () => T;
  close(): void;
}

/** Bun's Database.run/query accept params either variadic or as a single
 * array; node:sqlite is variadic only. Normalize to variadic. */
function flattenParams(params: unknown[]): unknown[] {
  if (params.length === 1 && Array.isArray(params[0])) return params[0] as unknown[];
  return params;
}

const isBun = typeof process === "object" && "isBun" in process && (process as { isBun?: boolean }).isBun === true;

/** Open a SQLite database using the appropriate driver for the runtime. */
export async function openChunkDatabase(path: string, opts: { create?: boolean; strict?: boolean }): Promise<ChunkSqlite> {
  if (isBun) {
    const { Database } = await import("bun:sqlite");
    return new BunChunkSqlite(new Database(path, { create: opts.create ?? true, strict: opts.strict ?? true }));
  }
  const { DatabaseSync } = await import("node:sqlite");
  return new NodeChunkSqlite(new DatabaseSync(path) as unknown as NodeChunkDatabaseSync);
}

class BunChunkSqlite implements ChunkSqlite {
  private db: {
    exec(s: string): void;
    run(s: string, ...p: unknown[]): { changes: number; lastInsertRowid: number | bigint };
    query(s: string): {
      get(...p: unknown[]): Record<string, unknown> | undefined;
      all(...p: unknown[]): Record<string, unknown>[];
      run(...p: unknown[]): { changes: number; lastInsertRowid: number | bigint };
    };
    transaction(f: () => unknown): () => unknown;
    close(): void;
  };
  constructor(db: unknown) {
    this.db = db as typeof this.db;
  }
  exec(sql: string): void { this.db.exec(sql); }
  run(sql: string, ...params: unknown[]): ChunkRunResult {
    const r = this.db.run(sql, ...flattenParams(params));
    return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
  }
  query(sql: string): ChunkStatement {
    const q = this.db.query(sql);
    return {
      get: (...p: unknown[]) => q.get(...flattenParams(p)),
      all: (...p: unknown[]) => q.all(...flattenParams(p)),
      run: (...p: unknown[]) => {
        const r = q.run(...flattenParams(p));
        return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
      },
    };
  }
  transaction<T>(fn: () => T): () => T {
    const tx = this.db.transaction(fn);
    return () => tx() as T;
  }
  close(): void { this.db.close(); }
}

interface NodeChunkStatementSync {
  run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  get(...params: unknown[]): Record<string, unknown> | undefined;
  all(...params: unknown[]): Record<string, unknown>[];
}
interface NodeChunkDatabaseSync {
  exec(sql: string): void;
  prepare(sql: string): NodeChunkStatementSync;
  close(): void;
}

class NodeChunkSqlite implements ChunkSqlite {
  private db: NodeChunkDatabaseSync;
  constructor(db: NodeChunkDatabaseSync) { this.db = db; }
  exec(sql: string): void { this.db.exec(sql); }
  run(sql: string, ...params: unknown[]): ChunkRunResult {
    const r = this.db.prepare(sql).run(...flattenParams(params));
    return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
  }
  query(sql: string): ChunkStatement {
    const stmt = this.db.prepare(sql);
    return {
      get: (...p: unknown[]) => stmt.get(...flattenParams(p)),
      all: (...p: unknown[]) => stmt.all(...flattenParams(p)),
      run: (...p: unknown[]) => {
        const r = stmt.run(...flattenParams(p));
        return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
      },
    };
  }
  transaction<T>(fn: () => T): () => T {
    return () => {
      this.db.exec("BEGIN IMMEDIATE;");
      try {
        const r = fn();
        this.db.exec("COMMIT;");
        return r;
      } catch (e) {
        try { this.db.exec("ROLLBACK;"); } catch { /* already aborted */ }
        throw e;
      }
    };
  }
  close(): void { this.db.close(); }
}
