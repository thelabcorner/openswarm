import { brotliCompressSync, brotliDecompressSync, zstdCompressSync, zstdDecompressSync, constants as zc } from "node:zlib";

export type CodecName = "none" | "deflate-fast" | "deflate-ratio" | "gzip-fast" | "brotli" | "zstd-fast" | "zstd-ratio" | "auto-speed" | "auto-ratio";

export interface CodecResult { codec: Exclude<CodecName, "auto-speed" | "auto-ratio">; data: Uint8Array; }

function u8(x: Uint8Array | ArrayBuffer): Uint8Array {
  return x instanceof Uint8Array ? x : new Uint8Array(x);
}

export function compress(codec: CodecName, input: Uint8Array): CodecResult {
  // node:zlib / Bun typings require an ArrayBuffer-backed view; the callers
  // (TextEncoder output, sqlite blobs) always provide one.
  const buf = input as Uint8Array<ArrayBuffer>;
  if (codec === "auto-speed") {
    if (input.byteLength < 256) return { codec: "none", data: input };
    // Zstd level 1 is an unusually good fit for ChunkDB's KB-scale blocks in Bun:
    // close to libdeflate's speed but materially denser on structured payloads.
    const out = input.byteLength >= 768
      ? u8(zstdCompressSync(buf, { params: { [zc.ZSTD_c_compressionLevel]: 1 } } as any))
      : u8(Bun.deflateSync(buf, { level: 1, library: "libdeflate", windowBits: -15 } as any));
    const name = input.byteLength >= 768 ? "zstd-fast" as const : "deflate-fast" as const;
    return out.byteLength + 8 < input.byteLength * 0.95 ? { codec: name, data: out } : { codec: "none", data: input };
  }
  if (codec === "auto-ratio") {
    if (input.byteLength < 192) return { codec: "none", data: input };
    const d = u8(Bun.deflateSync(buf, { level: 9, library: "libdeflate", windowBits: -15 } as any));
    const b = u8(brotliCompressSync(buf, { params: { [zc.BROTLI_PARAM_QUALITY]: 5 } }));
    const z = u8(zstdCompressSync(buf, { params: { [zc.ZSTD_c_compressionLevel]: 5 } } as any));
    const best = z.byteLength < b.byteLength && z.byteLength < d.byteLength
      ? { codec: "zstd-ratio" as const, data: z }
      : b.byteLength < d.byteLength ? { codec: "brotli" as const, data: b } : { codec: "deflate-ratio" as const, data: d };
    return best.data.byteLength + 8 < input.byteLength * 0.98 ? best : { codec: "none", data: input };
  }
  switch (codec) {
    case "none": return { codec, data: input };
    case "deflate-fast": return { codec, data: u8(Bun.deflateSync(buf, { level: 1, library: "libdeflate", windowBits: -15 } as any)) };
    case "deflate-ratio": return { codec, data: u8(Bun.deflateSync(buf, { level: 9, library: "libdeflate", windowBits: -15 } as any)) };
    case "gzip-fast": return { codec, data: u8(Bun.gzipSync(buf, { level: 1, library: "libdeflate" })) };
    case "brotli": return { codec, data: u8(brotliCompressSync(buf, { params: { [zc.BROTLI_PARAM_QUALITY]: 5 } })) };
    case "zstd-fast": return { codec, data: u8(zstdCompressSync(buf, { params: { [zc.ZSTD_c_compressionLevel]: 1 } } as any)) };
    case "zstd-ratio": return { codec, data: u8(zstdCompressSync(buf, { params: { [zc.ZSTD_c_compressionLevel]: 5 } } as any)) };
  }
}

export function decompress(codec: string, input: Uint8Array): Uint8Array {
  const buf = input as Uint8Array<ArrayBuffer>;
  switch (codec) {
    case "none": return input;
    case "deflate-fast":
    case "deflate-ratio": return u8(Bun.inflateSync(buf, { windowBits: -15 } as any));
    case "gzip-fast": return u8(Bun.gunzipSync(buf));
    case "brotli": return u8(brotliDecompressSync(buf));
    case "zstd-fast":
    case "zstd-ratio": return u8(zstdDecompressSync(buf));
    default: throw new Error(`Unknown codec: ${codec}`);
  }
}
