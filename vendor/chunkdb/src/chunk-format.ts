import { compress, decompress, type CodecName } from './codec';
import { encode, decode } from './serde';

const MAGIC = 0x32424443; // CDB2 little-endian
const HEADER = 12;
const ENTRY = 16;

const CODEC_TO_ID: Record<string, number> = {
  'none': 0,
  'deflate-fast': 1,
  'deflate-ratio': 2,
  'gzip-fast': 3,
  'brotli': 4,
  'zstd-fast': 5,
  'zstd-ratio': 6,
};
const ID_TO_CODEC = ['none','deflate-fast','deflate-ratio','gzip-fast','brotli','zstd-fast','zstd-ratio'] as const;

export interface PackedChunk {
  payload: Uint8Array;
  rawSize: number;
  storedSize: number;
  recordCount: number;
  blockRecords: number;
  blockCount: number;
}

export interface ParsedChunk {
  payload: Uint8Array;
  recordCount: number;
  blockRecords: number;
  blockCount: number;
  dataStart: number;
  dv: DataView;
}

export function packChunk(values: any[], policy: CodecName, blockRecords = 16): PackedChunk {
  blockRecords = Math.max(1, Math.min(65535, blockRecords | 0));
  const blockCount = Math.ceil(values.length / blockRecords);
  const blocks: { codecId:number; raw:number; data:Uint8Array }[] = new Array(blockCount);
  let rawSize = 0;
  let dataBytes = 0;
  for (let i=0;i<blockCount;i++) {
    const raw = encode(values.slice(i*blockRecords, Math.min(values.length, (i+1)*blockRecords)));
    const out = compress(policy, raw);
    const codecId = CODEC_TO_ID[out.codec];
    if (codecId === undefined) throw new Error(`Unsupported block codec ${out.codec}`);
    blocks[i] = { codecId, raw: raw.byteLength, data: out.data };
    rawSize += raw.byteLength;
    dataBytes += out.data.byteLength;
  }
  const dataStart = HEADER + blockCount * ENTRY;
  const payload = new Uint8Array(dataStart + dataBytes);
  const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  dv.setUint32(0, MAGIC, true);
  dv.setUint32(4, values.length, true);
  dv.setUint16(8, blockRecords, true);
  dv.setUint16(10, blockCount, true);
  let off = 0;
  for (let i=0;i<blockCount;i++) {
    const e = HEADER + i*ENTRY;
    const b = blocks[i]!;
    dv.setUint32(e, off, true);
    dv.setUint32(e+4, b.data.byteLength, true);
    dv.setUint32(e+8, b.raw, true);
    dv.setUint8(e+12, b.codecId);
    payload.set(b.data, dataStart + off);
    off += b.data.byteLength;
  }
  return { payload, rawSize, storedSize: payload.byteLength, recordCount: values.length, blockRecords, blockCount };
}

export function parseChunk(payload: Uint8Array): ParsedChunk {
  const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  if (payload.byteLength < HEADER || dv.getUint32(0,true) !== MAGIC) throw new Error('Invalid ChunkDB v2 chunk');
  const recordCount = dv.getUint32(4,true);
  const blockRecords = dv.getUint16(8,true);
  const blockCount = dv.getUint16(10,true);
  return { payload, recordCount, blockRecords, blockCount, dataStart: HEADER + blockCount*ENTRY, dv };
}

export function decodeBlock(parsed: ParsedChunk, blockIndex: number): any[] {
  if (blockIndex < 0 || blockIndex >= parsed.blockCount) return [];
  const e = HEADER + blockIndex*ENTRY;
  const off = parsed.dv.getUint32(e,true);
  const stored = parsed.dv.getUint32(e+4,true);
  const codecId = parsed.dv.getUint8(e+12);
  const codec = ID_TO_CODEC[codecId];
  if (!codec) throw new Error(`Invalid codec id ${codecId}`);
  const data = parsed.payload.subarray(parsed.dataStart + off, parsed.dataStart + off + stored);
  return decode(decompress(codec, data));
}
