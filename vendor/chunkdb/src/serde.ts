const te = new TextEncoder();
const td = new TextDecoder();

type Tagged = any;

function replacer(_k: string, v: any): any {
  if (typeof v === "bigint") return { $chunkdb: "bigint", v: v.toString() };
  if (v instanceof Uint8Array) return { $chunkdb: "u8", v: Buffer.from(v).toString("base64") };
  if (v instanceof ArrayBuffer) return { $chunkdb: "ab", v: Buffer.from(v).toString("base64") };
  if (v instanceof Date) return { $chunkdb: "date", v: v.toISOString() };
  return v;
}

function reviver(_k: string, v: Tagged): any {
  if (!v || typeof v !== "object" || !v.$chunkdb) return v;
  switch (v.$chunkdb) {
    case "bigint": return BigInt(v.v);
    case "u8": return new Uint8Array(Buffer.from(v.v, "base64"));
    case "ab": { const b = Buffer.from(v.v, "base64"); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); }
    case "date": return new Date(v.v);
    default: return v;
  }
}

function needsExtendedEncoding(root: any): boolean {
  if (typeof root === "bigint" || root instanceof Uint8Array || root instanceof ArrayBuffer || root instanceof Date) return true;
  if (root === null || typeof root !== "object") return false;
  const stack = [root];
  const seen = new WeakSet<object>();
  while (stack.length) {
    const v = stack.pop();
    if (v === null || typeof v !== "object") {
      if (typeof v === "bigint") return true;
      continue;
    }
    if (v instanceof Uint8Array || v instanceof ArrayBuffer || v instanceof Date) return true;
    if (seen.has(v)) continue;
    seen.add(v);
    if (Array.isArray(v)) {
      for (let i = 0; i < v.length; i++) {
        const x = v[i];
        if (typeof x === "bigint") return true;
        if (x !== null && typeof x === "object") stack.push(x);
      }
    } else {
      for (const k in v) {
        if (!Object.prototype.hasOwnProperty.call(v, k)) continue;
        const x = v[k];
        if (typeof x === "bigint") return true;
        if (x !== null && typeof x === "object") stack.push(x);
      }
    }
  }
  return false;
}

export function encode(value: any): Uint8Array {
  const text = needsExtendedEncoding(value) ? JSON.stringify(value, replacer) : JSON.stringify(value);
  return te.encode(text);
}
export function decode(bytes: Uint8Array): any {
  const text = td.decode(bytes);
  // The reviver is extremely expensive because JSON.parse invokes it for every
  // property. Normal JSON-compatible data is by far the common case, so only
  // pay that cost when the ChunkDB escape marker is actually present.
  return text.indexOf('"$chunkdb"') === -1 ? JSON.parse(text) : JSON.parse(text, reviver);
}
