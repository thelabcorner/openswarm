import { openChunkDatabase, type ChunkSqlite, type ChunkStatement } from "./database";
import { compress, decompress, type CodecName } from "./codec";
import { encode, decode } from "./serde";
import { packChunk, parseChunk, decodeBlock, type ParsedChunk } from './chunk-format';

export interface ChunkDBOptions {
  /** Physical layout preset. Explicit chunkRecords/blockRecords override it. */
  profile?: "point" | "balanced" | "ratio";
  codec?: CodecName;
  chunkRecords?: number;
  blockRecords?: number;
  wal?: boolean;
  synchronous?: "OFF" | "NORMAL" | "FULL";
  chunkCacheSize?: number;
  blockCacheSize?: number;
}
interface DeltaRow { codec: string; payload: Uint8Array | null; tombstone: number; }
interface PointRow { chunk_id:number; slot:number; payload:Uint8Array; }
interface ChunkPayloadRow { id:number; payload:Uint8Array; }

export class ChunkDB {
  private _sqlite: ChunkSqlite | undefined;
  readonly codec: CodecName;
  readonly chunkRecords: number;
  readonly blockRecords: number;
  readonly chunkCacheSize: number;
  readonly blockCacheSize: number;
  readonly path: string;
  private readonly options: ChunkDBOptions;
  private chunkCache = new Map<number, ParsedChunk>();
  private blockCache = new Map<string, any[]>();
  private locCache = new Map<string, { chunk_id:number; slot:number } | null>();
  private deltaCache = new Map<string, { tombstone:boolean; value?:any } | null>();

  private qChunkInsert!: ChunkStatement; private qDirInsert!: ChunkStatement; private qDeltaUpsert!: ChunkStatement; private qDeltaGet!: ChunkStatement;
  private qPointGet!: ChunkStatement; private qChunkGet!: ChunkStatement; private qDeleteDirNamespace!: ChunkStatement; private qDeleteChunksNamespace!: ChunkStatement;
  private qDeleteDeltasNamespace!: ChunkStatement; private qDeltaDelete!: ChunkStatement;
  private qKeys!: ChunkStatement; private qDirCount!: ChunkStatement;

  /** Namespaces whose delta override rows are currently being flushed by
   * compact() - suppresses re-entrant auto-compaction from putMany. */
  private compacting = new Set<string>();

  constructor(path = "chunkdb.sqlite", opts: ChunkDBOptions = {}) {
    // Opening is DEFERRED to ready(): the backing driver is picked per runtime
    // via dynamic import (Bun vs Node/Desktop), so merely importing this class
    // must never touch `bun:sqlite` (a static import would kill the whole
    // plugin bundle under the Node-based Desktop host).
    this.path = path;
    this.options = opts;
    this.codec = opts.codec ?? "auto-speed";
    // Empirically tuned Bun profiles. Macrochunks amortize SQLite row/page overhead;
    // microblocks independently bound decompression amplification.
    const profile = opts.profile ?? "balanced";
    const profileBlock = profile === "point" ? 32 : profile === "ratio" ? 512 : 128;
    this.chunkRecords = opts.chunkRecords ?? 2048;
    this.blockRecords = opts.blockRecords ?? profileBlock;
    this.chunkCacheSize = opts.chunkCacheSize ?? 512;
    this.blockCacheSize = opts.blockCacheSize ?? 4096;
  }

  /** Open the backing database (idempotent). Must complete before any other
   * method. Picks bun:sqlite under Bun and node:sqlite under Node. */
  async ready(): Promise<void> {
    if (this._sqlite) return;
    const sqlite = await openChunkDatabase(this.path, { create: true, strict: true });
    this._sqlite = sqlite;
    const opts = this.options;
    if (opts.wal ?? true) this._sqlite.run("PRAGMA journal_mode=WAL;");
    this._sqlite.run(`PRAGMA synchronous=${opts.synchronous ?? "NORMAL"};`);
    this._sqlite.run("PRAGMA temp_store=MEMORY;");
    this._sqlite.run("PRAGMA foreign_keys=ON;");
    this._sqlite.run("PRAGMA busy_timeout=5000;");
    this._sqlite.run("PRAGMA cache_size=-16384;");
    this.init();
    this.qChunkInsert = this._sqlite.query(`INSERT INTO cdb_chunks(namespace, codec, record_count, raw_size, stored_size, payload) VALUES($namespace,'blocked-v2',$recordCount,$rawSize,$storedSize,$payload)`);
    this.qDirInsert = this._sqlite.query(`INSERT OR REPLACE INTO cdb_directory(namespace,key,chunk_id,slot) VALUES($namespace,$key,$chunkId,$slot)`);
    this.qDeltaUpsert = this._sqlite.query(`INSERT INTO cdb_delta(namespace,key,codec,raw_size,stored_size,payload,tombstone,updated_at) VALUES($namespace,$key,$codec,$rawSize,$storedSize,$payload,$tombstone,$updatedAt) ON CONFLICT(namespace,key) DO UPDATE SET codec=excluded.codec,raw_size=excluded.raw_size,stored_size=excluded.stored_size,payload=excluded.payload,tombstone=excluded.tombstone,updated_at=excluded.updated_at`);
    this.qDeltaGet = this._sqlite.query(`SELECT codec,payload,tombstone FROM cdb_delta WHERE namespace=$namespace AND key=$key`);
    this.qPointGet = this._sqlite.query(`SELECT d.chunk_id,d.slot,c.payload FROM cdb_directory d JOIN cdb_chunks c ON c.id=d.chunk_id WHERE d.namespace=$namespace AND d.key=$key`);
    this.qChunkGet = this._sqlite.query(`SELECT id,payload FROM cdb_chunks WHERE id=$id`);
    this.qDeleteDirNamespace = this._sqlite.query(`DELETE FROM cdb_directory WHERE namespace=$namespace`);
    this.qDeleteChunksNamespace = this._sqlite.query(`DELETE FROM cdb_chunks WHERE namespace=$namespace`);
    this.qDeleteDeltasNamespace = this._sqlite.query(`DELETE FROM cdb_delta WHERE namespace=$namespace`);
    this.qDeltaDelete = this._sqlite.query(`DELETE FROM cdb_delta WHERE namespace=$namespace AND key=$key`);
    this.qKeys = this._sqlite.query(`SELECT key FROM cdb_directory WHERE namespace=$ns UNION SELECT key FROM cdb_delta WHERE namespace=$ns`);
    this.qDirCount = this._sqlite.query(`SELECT COUNT(*) n FROM cdb_directory WHERE namespace=$ns`);
  }

  /** The opened database — throws until ready() completes (single guard point
   * for every public method). */
  get sqlite(): ChunkSqlite {
    if (!this._sqlite) throw new Error("ChunkDB: call await ready() before use");
    return this._sqlite;
  }

  private init() {
    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS cdb_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS cdb_chunks(id INTEGER PRIMARY KEY,namespace TEXT NOT NULL,codec TEXT NOT NULL,record_count INTEGER NOT NULL,raw_size INTEGER NOT NULL,stored_size INTEGER NOT NULL,payload BLOB NOT NULL);
      CREATE INDEX IF NOT EXISTS cdb_chunks_ns ON cdb_chunks(namespace);
      CREATE TABLE IF NOT EXISTS cdb_directory(namespace TEXT NOT NULL,key TEXT NOT NULL,chunk_id INTEGER NOT NULL,slot INTEGER NOT NULL,PRIMARY KEY(namespace,key),FOREIGN KEY(chunk_id) REFERENCES cdb_chunks(id) ON DELETE CASCADE) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS cdb_delta(namespace TEXT NOT NULL,key TEXT NOT NULL,codec TEXT NOT NULL,raw_size INTEGER NOT NULL,stored_size INTEGER NOT NULL,payload BLOB,tombstone INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL,PRIMARY KEY(namespace,key)) WITHOUT ROWID;
      INSERT OR REPLACE INTO cdb_meta(key,value) VALUES('format','chunkdb-bun-v2');
    `);
  }

  private ck(namespace:string,key:string){ return namespace + "\0" + key; }
  put(namespace:string,key:string,value:any):void {
    const raw=encode(value), out=compress(this.codec,raw);
    this.qDeltaUpsert.run({namespace,key,codec:out.codec,rawSize:raw.byteLength,storedSize:out.data.byteLength,payload:out.data,tombstone:0,updatedAt:Date.now()});
    this.deltaCache.set(this.ck(namespace,key), { tombstone:false, value });
    this.bumpDeltaCount(namespace, 1);
    this.maybeAutoCompact(namespace);
  }
  delete(namespace:string,key:string):void { this.qDeltaUpsert.run({namespace,key,codec:'none',rawSize:0,storedSize:0,payload:null,tombstone:1,updatedAt:Date.now()}); this.deltaCache.set(this.ck(namespace,key), { tombstone:true }); this.bumpDeltaCount(namespace, 1); this.maybeAutoCompact(namespace); }

  /**
   * List live-or-tombstoned keys in a namespace. Prefix filter is applied in
   * SQL via a range bound when possible AND in JS (authoritative) so arbitrary
   * key characters can never confuse the scan. The caller decides liveness:
   * tombstoned keys are included here but are ABSENT from getMany() results —
   * the sanctioned "scan + getMany" pattern relies on that to skip deletes.
   */
  keys(namespace:string, prefix?:string):string[] {
    let rows: Array<{key:string}>;
    if (prefix === undefined || prefix === "") {
      rows = this.qKeys.all({ns:namespace}) as Array<{key:string}>;
    } else {
      const end = prefixEnd(prefix);
      rows = end === null
        ? this.qKeys.all({ns:namespace}) as Array<{key:string}>
        : this.sqlite.query(`SELECT key FROM cdb_directory WHERE namespace=$ns AND key >= $start AND key < $end UNION SELECT key FROM cdb_delta WHERE namespace=$ns AND key >= $start AND key < $end`).all({ns:namespace,start:prefix,end}) as Array<{key:string}>;
    }
    const out:string[] = [];
    for (const r of rows) { if (prefix === undefined || prefix === "" || r.key.startsWith(prefix)) out.push(r.key); }
    return out;
  }

  // ==== delta accumulation tracking + threshold auto-compaction ====
  // cdb_delta holds every put()/delete() override since the last putMany()
  // flush or compact(). Overrides+tombstones accumulate forever, keeping the
  // file large (old payload bytes are not reclaimed). Track the per-namespace
  // delta count in cdb_meta (survives restarts) and compact() when the delta
  // pile grows past a threshold — either an absolute write count (5000) or a
  // ratio of deltas to directory records (~30%).
  private deltaCountKey(namespace:string){ return "delta_count:" + namespace; }
  private getDeltaCount(namespace:string):number {
    const r = this.sqlite.query(`SELECT value FROM cdb_meta WHERE key=$k`).get({k:this.deltaCountKey(namespace)}) as {value:string}|undefined;
    if (!r) return 0;
    const n = Number(r.value);
    return Number.isFinite(n) ? n : 0;
  }
  private setDeltaCount(namespace:string, n:number):void {
    this.sqlite.query(`INSERT INTO cdb_meta(key,value) VALUES($k,$v) ON CONFLICT(key) DO UPDATE SET value=excluded.value`).run({k:this.deltaCountKey(namespace), v:String(Math.max(0,n))});
  }
  private bumpDeltaCount(namespace:string, delta:number):void { this.setDeltaCount(namespace, this.getDeltaCount(namespace) + delta); }
  private maybeAutoCompact(namespace:string):void {
    if (this.compacting.has(namespace)) return;
    const count = this.getDeltaCount(namespace);
    // Absolute threshold: after ~5000 unflushed writes, reclaim.
    if (count >= 5000) { this.compact(namespace); return; }
    // Ratio threshold: only query the directory count once there is real
    // accumulation, so the hot single-write path stays cheap.
    if (count >= 200) {
      const dir = Number((this.qDirCount.get({ns:namespace}) as {n:number}).n);
      if (dir > 0 && count >= dir * 0.3) this.compact(namespace);
    }
  }

  private touchChunk(id:number,payload:Uint8Array):ParsedChunk {
    let p=this.chunkCache.get(id);
    if (p) { this.chunkCache.delete(id); this.chunkCache.set(id,p); return p; }
    p=parseChunk(payload); this.chunkCache.set(id,p);
    while(this.chunkCache.size>this.chunkCacheSize){const k=this.chunkCache.keys().next().value as number|undefined;if(k===undefined)break;this.chunkCache.delete(k);}
    return p;
  }
  private readSlot(id:number,slot:number,payload:Uint8Array):any {
    const parsed=this.touchChunk(id,payload);
    const bi=Math.floor(slot/parsed.blockRecords); const cacheKey=id+':'+bi;
    let vals=this.blockCache.get(cacheKey);
    if(vals){this.blockCache.delete(cacheKey);this.blockCache.set(cacheKey,vals);} else {
      vals=decodeBlock(parsed,bi); this.blockCache.set(cacheKey,vals);
      while(this.blockCache.size>this.blockCacheSize){const k=this.blockCache.keys().next().value as string|undefined;if(k===undefined)break;this.blockCache.delete(k);}
    }
    return vals[slot-bi*parsed.blockRecords];
  }

  get<T=any>(namespace:string,key:string):T|undefined {
    const cacheKey=this.ck(namespace,key);
    let dc=this.deltaCache.get(cacheKey);
    if(dc === undefined){
      const d=this.qDeltaGet.get({namespace,key}) as unknown as DeltaRow | null;
      if(d){ dc=d.tombstone ? {tombstone:true} : {tombstone:false,value:decode(decompress(d.codec,d.payload!))}; }
      else dc=null;
      this.deltaCache.set(cacheKey,dc);
    }
    if(dc){ return dc.tombstone ? undefined : dc.value as T; }
    let loc=this.locCache.get(cacheKey);
    if(loc === undefined){
      const r=this.qPointGet.get({namespace,key}) as unknown as PointRow | null;
      if(!r){ this.locCache.set(cacheKey,null); return undefined; }
      loc={chunk_id:r.chunk_id,slot:r.slot}; this.locCache.set(cacheKey,loc);
      return this.readSlot(r.chunk_id,r.slot,r.payload) as T;
    }
    if(!loc)return undefined;
    const parsed=this.chunkCache.get(loc.chunk_id);
    if(parsed)return this.readSlot(loc.chunk_id,loc.slot,parsed.payload) as T;
    const row=this.qChunkGet.get({id:loc.chunk_id}) as unknown as ChunkPayloadRow | null;
    if(!row)return undefined;
    return this.readSlot(loc.chunk_id,loc.slot,row.payload) as T;
  }

  getMany<T=any>(namespace:string,keys:string[]):Map<string,T> {
    const out=new Map<string,T>(); if(!keys.length)return out;
    // Fast path in bounded batches to stay under SQLite variable limits.
    const BATCH=400;
    for(let start=0;start<keys.length;start+=BATCH){
      const ks=keys.slice(start,start+BATCH);
      const placeholders=ks.map((_,i)=>`$k${i}`).join(',');
      const params:any={namespace}; ks.forEach((k,i)=>params[`k${i}`]=k);
      const deltas=this.sqlite.query(`SELECT key,codec,payload,tombstone FROM cdb_delta WHERE namespace=$namespace AND key IN (${placeholders})`).all(params) as any[];
      const deltaKeys=new Set<string>();
      for(const d of deltas){deltaKeys.add(d.key);if(!d.tombstone)out.set(d.key,decode(decompress(d.codec,d.payload)));}
      const remaining=ks.filter(k=>!deltaKeys.has(k)); if(!remaining.length)continue;
      const ph=remaining.map((_,i)=>`$r${i}`).join(','); const p2:any={namespace}; remaining.forEach((k,i)=>p2[`r${i}`]=k);
      const rows=this.sqlite.query(`SELECT d.key,d.chunk_id,d.slot,c.payload FROM cdb_directory d JOIN cdb_chunks c ON c.id=d.chunk_id WHERE d.namespace=$namespace AND d.key IN (${ph})`).all(p2) as any[];
      for(const r of rows) out.set(r.key,this.readSlot(r.chunk_id,r.slot,r.payload));
    }
    return out;
  }

  putMany(namespace:string,entries:Iterable<[string,any]>,chunkRecords=this.chunkRecords,blockRecords?:number):number {
    const all=Array.from(entries); const blockRecordsFor=this.blockRecords;
    this.compacting.add(namespace);
    try {
    const tx=this.sqlite.transaction(()=>{
      for(let i=0;i<all.length;i+=chunkRecords){
        const batch=all.slice(i,i+chunkRecords);
        // Adaptive block sizing: pick the block width from the VALUES in this
        // batch — tiny records get small blocks (less decompression overhead
        // per read), large payloads get bigger blocks (better ratio). Simple
        // size threshold: encoded bytes < 2KB -> point profile (32), else
        // balanced (128). An explicit blockRecords argument wins (used by the
        // store when it wants a fixed layout).
        let br = blockRecords ?? blockRecordsFor;
        if (blockRecords === undefined) {
          let bytes = 0;
          for (const [,v] of batch) {
            const raw = encode(v);
            bytes += raw.byteLength;
          }
          const avg = bytes / Math.max(1, batch.length);
          br = avg < 2048 ? 32 : 128;
        }
        const packed=packChunk(batch.map(x=>x[1]),this.codec,br);
        const r=this.qChunkInsert.run({namespace,recordCount:batch.length,rawSize:packed.rawSize,storedSize:packed.storedSize,payload:packed.payload});
        const chunkId=Number(r.lastInsertRowid);
        // Batch directory maintenance. Native SQLite can process hundreds of rows per
        // statement far more cheaply than crossing the JS/native boundary per key.
        const DIR_BATCH = 200;
        for (let base = 0; base < batch.length; base += DIR_BATCH) {
          const part = batch.slice(base, base + DIR_BATCH);
          const vals:string[] = []; const params:any = { namespace };
          for (let j=0;j<part.length;j++) {
            vals.push(`($namespace,$k${j},$c${j},$s${j})`);
            params[`k${j}`]=part[j]![0]; params[`c${j}`]=chunkId; params[`s${j}`]=base+j;
          }
          this.sqlite.query(`INSERT OR REPLACE INTO cdb_directory(namespace,key,chunk_id,slot) VALUES ${vals.join(',')}`).run(params);

          const dels:string[]=[]; const dp:any={namespace};
          for(let j=0;j<part.length;j++){dels.push(`$k${j}`);dp[`k${j}`]=part[j]![0];}
          this.sqlite.query(`DELETE FROM cdb_delta WHERE namespace=$namespace AND key IN (${dels.join(',')})`).run(dp);
        }
        for(let slot=0;slot<batch.length;slot++){
          const key=batch[slot]![0]; const cacheKey=this.ck(namespace,key);
          this.locCache.set(cacheKey,{chunk_id:chunkId,slot}); this.deltaCache.set(cacheKey,null);
        }
      }
    }); tx();
    // putMany flushes the delta rows for the keys it wrote — the accumulated
    // delta pile shrank by exactly that many entries.
    this.setDeltaCount(namespace, Math.max(0, this.getDeltaCount(namespace) - all.length));
    return all.length;
    } finally { this.compacting.delete(namespace); }
  }

  compact(namespace:string):{records:number;chunks:number}{
    if (this.compacting.has(namespace)) return { records: 0, chunks: 0 };
    this.compacting.add(namespace);
    try {
    const keys=this.sqlite.query(`SELECT key FROM cdb_directory WHERE namespace=$namespace UNION SELECT key FROM cdb_delta WHERE namespace=$namespace`).all({namespace}) as Array<{key:string}>;
    const keyList=keys.map(x=>x.key); const got=this.getMany(namespace,keyList); const entries:Array<[string,any]>=[];
    for(const k of keyList){if(got.has(k))entries.push([k,got.get(k)]);}
    this.sqlite.transaction(()=>{this.qDeleteDirNamespace.run({namespace});this.qDeleteChunksNamespace.run({namespace});this.qDeleteDeltasNamespace.run({namespace});})();
    this.chunkCache.clear();this.blockCache.clear();
    for(const k of keyList){const ck=this.ck(namespace,k);this.locCache.delete(ck);this.deltaCache.delete(ck);}
    this.setDeltaCount(namespace, 0);
    this.putMany(namespace,entries);
    return{records:entries.length,chunks:Math.ceil(entries.length/this.chunkRecords)};
    } finally { this.compacting.delete(namespace); }
  }

  stats(namespace?:string){const where=namespace?' WHERE namespace=$namespace':'';const params=namespace?{namespace}:undefined;const chunks=this.sqlite.query(`SELECT COUNT(*) n,COALESCE(SUM(record_count),0) records,COALESCE(SUM(raw_size),0) raw,COALESCE(SUM(stored_size),0) stored FROM cdb_chunks${where}`).get(params as any) as any;const deltas=this.sqlite.query(`SELECT COUNT(*) n,COALESCE(SUM(raw_size),0) raw,COALESCE(SUM(stored_size),0) stored FROM cdb_delta${where}`).get(params as any) as any;return{chunks,deltas,logicalRawBytes:Number(chunks.raw)+Number(deltas.raw),storedPayloadBytes:Number(chunks.stored)+Number(deltas.stored),payloadRatio:(Number(chunks.stored)+Number(deltas.stored))/Math.max(1,Number(chunks.raw)+Number(deltas.raw))};}
  close(){ if (this._sqlite) { this._sqlite.close(); this._sqlite = undefined; } }
}

/** Exclusive upper bound for a SQLite prefix range scan: `prefixEnd("abc")`
 * returns "abd" (all keys starting with "abc" sort below it). Returns null for
 * a prefix that cannot be bounded (ends in "\uFFFF"), which the caller treats
 * as "no range bound — filter in JS". */
function prefixEnd(prefix: string): string | null {
  const chars = prefix.split("");
  for (let i = chars.length - 1; i >= 0; i--) {
    const code = chars[i]!.charCodeAt(0);
    if (code < 0xffff) {
      chars[i] = String.fromCharCode(code + 1);
      return chars.slice(0, i + 1).join("");
    }
  }
  return null;
}
