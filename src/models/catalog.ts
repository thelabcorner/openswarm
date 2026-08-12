import type { RuntimeModelInfo } from "../runtime/runtime-types.js";

/**
 * Capability-aware model selection (features/capability-delegation).
 *
 * When an operator supplies an image/PDF (or a model without vision is asked
 * to read one), the coordinator delegates to a subagent on a model that
 * ACTUALLY has the capability — preferring the CHEAPEST such model. Capability
 * and cost data come from the provider config when published (modalities +
 * cost per 1M tokens); this module adds the fallback catalog + the
 * cheapest-with-capability ranking.
 */

export type ModelCapability = "text" | "image" | "pdf" | "audio" | "video";

/** Fallback USD-per-1M-token prices for models whose provider config omits
 * `cost` (substring-matched on modelID; the API is the primary source). */
const FALLBACK_COST: Array<{ match: string; input: number; output: number }> = [
  { match: "deepseek-v4-flash", input: 0.14, output: 0.28 },
  { match: "deepseek-v4-pro", input: 0.435, output: 0.87 },
  { match: "deepseek-v4", input: 0.14, output: 0.28 },
];

/** Whether a model can consume the given input kind. Models with no
 * modalities published are assumed TEXT-ONLY (safe default — the capability
 * list shown by swarm_models is the explicit truth). */
export function hasCapability(model: RuntimeModelInfo, capability: ModelCapability): boolean {
  if (capability === "text") return true;
  if (!model.modalities) return false;
  return model.modalities.input.includes(capability);
}

/** Effective per-1M-token input price (provider cost, then fallback catalog,
 * then Infinity for unknown — unknown prices sort last). */
export function modelInputPrice(model: RuntimeModelInfo): number {
  if (model.cost?.input) return model.cost.input;
  const id = model.modelID.toLowerCase();
  for (const f of FALLBACK_COST) {
    if (id.includes(f.match)) return f.input;
  }
  return Infinity;
}

export function modelOutputPrice(model: RuntimeModelInfo): number {
  if (model.cost?.output) return model.cost.output;
  const id = model.modelID.toLowerCase();
  for (const f of FALLBACK_COST) {
    if (id.includes(f.match)) return f.output;
  }
  return Infinity;
}

/** All models that can consume `capability`, cheapest-first (stable: equal
 * prices keep tier/availability order — zen-free before paid before unknown). */
export function modelsWithCapability(models: RuntimeModelInfo[], capability: ModelCapability): RuntimeModelInfo[] {
  const capable = models.filter((m) => hasCapability(m, capability));
  return capable.sort((a, b) => {
    const pa = modelInputPrice(a);
    const pb = modelInputPrice(b);
    if (pa !== pb) return pa - pb;
    const ta = a.tier === "zen-free" ? 0 : a.tier === "go" ? 1 : 2;
    const tb = b.tier === "zen-free" ? 0 : b.tier === "go" ? 1 : 2;
    return ta - tb;
  });
}

/** The cheapest model with the capability (undefined when none has it). */
export function cheapestWithCapability(models: RuntimeModelInfo[], capability: ModelCapability): RuntimeModelInfo | undefined {
  return modelsWithCapability(models, capability)[0];
}

/** Compact price label for tool output ("$0.14/M" or "?" when unknown). */
export function priceLabel(model: RuntimeModelInfo): string {
  const input = modelInputPrice(model);
  const output = modelOutputPrice(model);
  if (!Number.isFinite(input) && !Number.isFinite(output)) return "price ?";
  const fmt = (n: number) => (Number.isFinite(n) ? `$${n.toFixed(n < 1 ? 3 : 2)}` : "?");
  return `${fmt(input)} in / ${fmt(output)} out per 1M`;
}

/** Compact capability list ("text,image,pdf" / "text" for unknown). */
export function capabilityLabel(model: RuntimeModelInfo): string {
  if (!model.modalities) return "text";
  return model.modalities.input.length ? model.modalities.input.join(",") : "text";
}
