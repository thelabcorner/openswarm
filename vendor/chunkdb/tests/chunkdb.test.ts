import { test, expect } from "bun:test";
import { ChunkDB } from "../src";

test("chunk roundtrip and delta override", () => {
  const db = new ChunkDB(":memory:", { chunkRecords: 8 });
  db.putMany("n", Array.from({length:32},(_,i)=>[`k${i}`,{i,s:"repeat repeat repeat",a:[i,i+1]}] as [string,any]));
  expect(db.get("n","k17")).toEqual({i:17,s:"repeat repeat repeat",a:[17,18]});
  db.put("n","k17",{i:170});
  expect(db.get("n","k17")).toEqual({i:170});
  db.delete("n","k18");
  expect(db.get("n","k18")).toBeUndefined();
  db.compact("n");
  expect(db.get("n","k17")).toEqual({i:170});
  expect(db.get("n","k18")).toBeUndefined();
  db.close();
});

test("keys(namespace, prefix?) lists live keys with JS-verified prefix filtering", () => {
  const db = new ChunkDB(":memory:", { chunkRecords: 8 });
  db.putMany("n", [["alpha/1",1],["alpha/2",2],["beta/1",3]] as [string,any][]);
  db.put("n","alpha/3",4);            // delta row
  db.delete("n","beta/1");            // tombstone delta
  const all = db.keys("n").sort();
  expect(all).toEqual(["alpha/1","alpha/2","alpha/3","beta/1"]);
  // Prefix scan: range bound + JS filter.
  expect(db.keys("n","alpha/").sort()).toEqual(["alpha/1","alpha/2","alpha/3"]);
  // getMany is the liveness authority: tombstoned keys are absent.
  const got = db.getMany("n", db.keys("n"));
  expect([...got.keys()].sort()).toEqual(["alpha/1","alpha/2","alpha/3"]);
  db.close();
});

test("auto-compaction: absolute threshold flushes after 5000 writes", () => {
  const db = new ChunkDB(":memory:", { chunkRecords: 16 });
  // A large directory makes the ratio path (>30% of 20k records = 6000 deltas)
  // unreachable for a 5000-write burst, so the ABSOLUTE threshold (5000
  // unflushed writes) is what fires — proving the write-count bound works.
  db.putMany("n", Array.from({length:20000},(_,i)=>[`k${i}`,i] as [string,any]));
  for (let i = 0; i < 5000; i++) db.put("n", `d${i}`, i);
  const s = db.stats("n");
  expect(Number(s.deltas.n)).toBe(0);
  expect(Number(s.chunks.records)).toBeGreaterThan(24000);
  expect(db.get("n","k19999")).toBe(19999);
  expect(db.get("n","d4999")).toBe(4999);
  db.close();
});

test("auto-compaction: ratio threshold flushes at ~30% of directory records", () => {
  const db = new ChunkDB(":memory:", { chunkRecords: 8 });
  db.putMany("n", Array.from({length:1000},(_,i)=>[`k${i}`,i] as [string,any]));
  for (let i = 0; i < 300; i++) db.put("n", `k${i}`, i + 1000);
  // 300 deltas = exactly 30% of the 1000 directory records -> the ratio check
  // fires on the 300th write and flushes; loop ends right after, so deltas = 0.
  const s = db.stats("n");
  expect(Number(s.deltas.n)).toBe(0);
  expect(db.get("n","k299")).toBe(1299);
  db.close();
});

test("adaptive block sizing handles small and large values", () => {
  const db = new ChunkDB(":memory:", { chunkRecords: 4 });
  const small = Array.from({length:8},(_,i)=>[`s${i}`,{t:"tiny"}] as [string,any]);
  const large = Array.from({length:8},(_,i)=>[`l${i}`,{blob:"x".repeat(10000),i}] as [string,any]);
  db.putMany("n", small);
  db.putMany("n", large);
  for (let i = 0; i < 8; i++) {
    expect(db.get("n",`s${i}`)).toEqual({t:"tiny"});
    expect(db.get("n",`l${i}`)!.blob.length).toBe(10000);
  }
  db.close();
});
