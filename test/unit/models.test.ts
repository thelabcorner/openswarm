import { describe, expect, test } from "bun:test";
import { tierForProvider as tierFor, isZenFree as free } from "../../src/runtime/opencode-runtime.ts";

// These are internal helpers; testing them here validates the tier mapping
// that swarm_models uses.
describe("model tier classification", () => {
  test("zen-free models are detected by -free suffix", () => {
    expect(free("deepseek-v4-flash-free")).toBe(true);
    expect(free("longcat-2.0-free")).toBe(true);
    expect(free("ling-3.0-tiny-free")).toBe(true);
    expect(free("deepseek-v4-flash")).toBe(false);
    expect(free("gpt-5")).toBe(false);
  });

  test("provider -> tier mapping", () => {
    expect(tierFor("opencode")).toBe("zen");
    expect(tierFor("opencode-go")).toBe("go");
    expect(tierFor("lmstudio")).toBe("lmstudio");
    expect(tierFor("openai")).toBe("openai");
  });
});