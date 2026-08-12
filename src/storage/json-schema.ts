/**
 * Minimal JSON-schema (draft-07 subset) validator for typed blackboard
 * contracts (task t-contracts).
 *
 * Supported keywords: `type`, `properties`, `required`, `items`, `enum`,
 * `anyOf`. Every OTHER keyword (minLength, pattern, format, additionalProperties,
 * …) passes through UNTYPED — it is treated as valid, so a contract author can
 * rely only on the supported subset. This is a deliberate scope decision: the
 * validator is small, dependency-free, and deterministic; it validates the
 * shape of structured blackboard values (objects/arrays/scalars), which is what
 * typed contracts exist for.
 *
 * The value passed in is ALREADY parsed JSON (blackboardPut parses the raw
 * string first, so a non-JSON value fails before it reaches the schema).
 */

/** A JSON-schema as a plain object (unknown keys are ignored/passed through). */
export type JsonSchema = Record<string, unknown>;

function typeName(v: unknown): string {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  if (typeof v === "number") return Number.isInteger(v) ? "integer" : "number";
  return typeof v; // string | boolean | object | undefined
}

/** Does `v` satisfy the type keyword value `t` (a single draft-07 type)? */
function matchesType(v: unknown, t: string): boolean {
  switch (t) {
    case "null":
      return v === null;
    case "array":
      return Array.isArray(v);
    case "object":
      return typeof v === "object" && v !== null && !Array.isArray(v);
    case "string":
      return typeof v === "string";
    case "number":
      return typeof v === "number";
    case "integer":
      return typeof v === "number" && Number.isInteger(v);
    case "boolean":
      return typeof v === "boolean";
    default:
      // Unknown type keyword — pass through (treat as satisfied).
      return true;
  }
}

/** Structural deep equality (JSON values only). */
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return a === b;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((el, i) => deepEqual(el, b[i]));
  }
  if (typeof a === "object" && typeof b === "object" && !Array.isArray(a) && !Array.isArray(b)) {
    const ka = Object.keys(a as Record<string, unknown>);
    const kb = Object.keys(b as Record<string, unknown>);
    if (ka.length !== kb.length) return false;
    return ka.every((k) => deepEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]));
  }
  return false;
}

const isSubschema = (s: unknown): s is JsonSchema =>
  typeof s === "object" && s !== null && !Array.isArray(s);

/**
 * Validate a parsed JSON value against a schema. Returns a list of human-
 * readable reasons (empty list = valid). `path` is a JSON-pointer-style
 * location (default `$`) used to point at the offending property.
 */
export function validateValueAgainstSchema(
  schema: JsonSchema,
  value: unknown,
  path = "$",
): string[] {
  const errors: string[] = [];

  // -- type ---------------------------------------------------------------
  const type = schema.type;
  if (type !== undefined) {
    const types = Array.isArray(type) ? (type as unknown[]) : [type];
    const ok = types.some((t) => typeof t === "string" && matchesType(value, t));
    if (!ok) {
      errors.push(`${path}: expected type ${types.map(String).join("|")}, got ${typeName(value)}`);
    }
  }

  // -- enum ----------------------------------------------------------------
  if (Array.isArray(schema.enum)) {
    if (!schema.enum.some((e) => deepEqual(e, value))) {
      errors.push(`${path}: value not in enum ${JSON.stringify(schema.enum)}`);
    }
  }

  // -- object: properties + required ---------------------------------------
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>;
    if (Array.isArray(schema.required)) {
      for (const r of schema.required) {
        if (typeof r === "string" && !(r in obj)) {
          errors.push(`${path}: missing required property '${r}'`);
        }
      }
    }
    const props = schema.properties;
    if (isSubschema(props)) {
      for (const [name, sub] of Object.entries(props)) {
        if (!(name in obj)) continue; // absent properties are not validated
        if (isSubschema(sub)) {
          errors.push(...validateValueAgainstSchema(sub, obj[name], `${path}.${name}`));
        }
      }
    }
  }

  // -- array: items --------------------------------------------------------
  if (Array.isArray(value) && schema.items !== undefined) {
    if (Array.isArray(schema.items)) {
      // Tuple form: validate positionally where a per-index schema exists.
      value.forEach((el, i) => {
        const sub = (schema.items as unknown[])[i];
        if (isSubschema(sub)) errors.push(...validateValueAgainstSchema(sub, el, `${path}[${i}]`));
      });
    } else if (isSubschema(schema.items)) {
      value.forEach((el, i) => {
        errors.push(...validateValueAgainstSchema(schema.items as JsonSchema, el, `${path}[${i}]`));
      });
    }
  }

  // -- anyOf ---------------------------------------------------------------
  if (Array.isArray(schema.anyOf)) {
    const subs = schema.anyOf.filter(isSubschema);
    if (subs.length > 0) {
      const ok = subs.some((s) => validateValueAgainstSchema(s, value, path).length === 0);
      if (!ok) {
        errors.push(`${path}: value matches none of anyOf`);
      }
    }
  }

  return errors;
}

/**
 * Compact one-line type summary of a schema, e.g.
 *   {type:"object",properties:{title:{type:"string"},version:{type:"number"}}}
 *   → `object{title:string, version:number}`
 * Used by swarm_contract's define confirmation and list rendering.
 */
export function summarizeSchema(schema: JsonSchema): string {
  if (!isSubschema(schema)) return "any";
  if (Array.isArray(schema.anyOf)) {
    const subs = schema.anyOf.filter(isSubschema);
    if (subs.length > 0) return `anyOf(${subs.map(summarizeSchema).join(" | ")})`;
  }
  const t = schema.type;
  if (t === "object" || (t === undefined && schema.properties !== undefined)) {
    const props = schema.properties;
    if (isSubschema(props)) {
      const entries = Object.entries(props);
      if (entries.length > 0) {
        const inner = entries
          .map(([name, sub]) => `${name}:${summarizeSchema(isSubschema(sub) ? sub : { type: typeName(sub) })}`)
          .join(", ");
        return `object{${inner}}`;
      }
    }
    return "object";
  }
  if (t === "array" || (t === undefined && schema.items !== undefined)) {
    if (isSubschema(schema.items)) return `array<${summarizeSchema(schema.items)}>`;
    if (Array.isArray(schema.items)) return `array<[tuple]>`;
    return "array";
  }
  if (Array.isArray(schema.enum)) {
    return `enum[${schema.enum.map((e) => (typeof e === "string" ? e : JSON.stringify(e))).join(", ")}]`;
  }
  if (typeof t === "string") return t;
  if (Array.isArray(t)) return t.map(String).join("|");
  return "any";
}
