import { describe, expect, test } from "bun:test";
import { buildHiveBlock, buildHiveSummary } from "../../src/hive/diagnostics.ts";
import type { ArtifactAnnotation } from "../../src/core/types.ts";

function ann(over: Partial<ArtifactAnnotation> & { type: ArtifactAnnotation["type"]; path: string }): ArtifactAnnotation {
  return {
    id: "a1",
    swarmId: "s1",
    weight: 1,
    authorMemberId: "m1",
    createdAt: 0,
    ...over,
  };
}

describe("buildHiveBlock", () => {
  test("renders beliefs by tier when substrate present", () => {
    const block = buildHiveBlock({ annotations: [], beliefsByTier: { whisper: 2, shout: 1 } });
    expect(block).toContain("beliefs: 3 active (1 shout, 2 whisper)");
    expect(block).toContain("HIVE (advisory");
  });

  test("renders needs count and spotlight topics", () => {
    const block = buildHiveBlock({
      annotations: [],
      activeNeeds: 4,
      spotlightTopics: ["migration", "auth"],
    });
    expect(block).toContain("needs: 4 active");
    expect(block).toContain("spotlight: migration, auth");
  });

  test("corpse pile only when >= 3 corpses on a path", () => {
    const annotations = [
      ann({ type: "corpse", path: "src/a.ts" }),
      ann({ type: "corpse", path: "src/a.ts" }),
      ann({ type: "corpse", path: "src/a.ts" }),
      ann({ type: "corpse", path: "src/b.ts" }), // only 1 — not a pile
    ];
    const block = buildHiveBlock({ annotations });
    expect(block).toContain("corpse piles: src/a.ts (3)");
    expect(block).not.toContain("src/b.ts");
  });

  test("gold trails shown when no corpse pile dominates", () => {
    const annotations = [ann({ type: "gold", path: "src/ok.ts" }), ann({ type: "gold", path: "src/ok.ts" })];
    const block = buildHiveBlock({ annotations });
    expect(block).toContain("gold trails: src/ok.ts");
    expect(block).not.toContain("corpse piles");
  });

  test("returns undefined when nothing truthful to show", () => {
    expect(buildHiveBlock({ annotations: [] })).toBeUndefined();
    expect(buildHiveBlock({ annotations: [], beliefsByTier: { whisper: 0, shout: 0 } })).toBeUndefined();
  });

  test("H2: resonant count renders when nonzero", () => {
    const block = buildHiveBlock({ annotations: [], resonantCount: 3 });
    expect(block).toContain("resonant: 3");
  });

  test("H2: consolidation status renders age + counts", () => {
    const block = buildHiveBlock({
      annotations: [],
      consolidation: { lastRunAt: Date.now() - 90_000, retained: 5, pruned: 2, upgraded: 1 },
    });
    expect(block).toContain("consolidation: 90s ago (retained 5, pruned 2, upgraded 1)");
  });

  test("H2: digest health renders with last-sync age", () => {
    const block = buildHiveBlock({
      annotations: [],
      digest: { health: "stale", lastSyncAt: Date.now() - 300_000 },
    });
    expect(block).toContain("digest: stale (last sync 300s ago)");
  });

  test("H2: resonant/consolidation/digest omitted when absent (truthful gating)", () => {
    const block = buildHiveBlock({ annotations: [] });
    expect(block).toBeUndefined();
  });
});

describe("buildHiveSummary", () => {
  test("one-line summary with beliefs + needs + corpse piles", () => {
    const annotations = [
      ann({ type: "corpse", path: "src/x.ts" }),
      ann({ type: "corpse", path: "src/x.ts" }),
      ann({ type: "corpse", path: "src/x.ts" }),
    ];
    const summary = buildHiveSummary({ annotations, beliefsByTier: { whisper: 1, shout: 2 }, activeNeeds: 3 });
    expect(summary).toContain("hive: 3 beliefs · 3 needs · 1 corpse pile(s)");
  });

  test("H2: resonant + consolidation + digest health in summary", () => {
    const summary = buildHiveSummary({
      annotations: [],
      resonantCount: 2,
      consolidation: { lastRunAt: Date.now() - 60_000, retained: 4, pruned: 1, upgraded: 0 },
      digest: { health: "fresh", lastSyncAt: Date.now() - 10_000 },
    });
    expect(summary).toContain("hive: 2 resonant · consolidated 60s ago · digest fresh");
  });

  test("H2: digest unknown omitted from summary (non-truthful to claim)", () => {
    const summary = buildHiveSummary({ annotations: [], digest: { health: "unknown" } });
    expect(summary).toBeUndefined();
  });

  test("undefined when trivial", () => {
    expect(buildHiveSummary({ annotations: [] })).toBeUndefined();
  });
});
