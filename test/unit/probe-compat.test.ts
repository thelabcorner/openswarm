import { describe, expect, test } from "bun:test";
import { probeAutopermissions } from "../../src/probe/autopermissions.ts";

/**
 * Verifies the autopermissions probe entry used by `bun run probe`
 * (src/probe/compat.ts) classifies the coordinator session correctly against
 * a fake client — the same surface the wired probe check calls.
 */

function makeClient(agents: any[], sessionPermission?: any): any {
  return {
    session: {
      get: async () => ({ data: { id: "ses-coord", agent: "build", permission: sessionPermission }, error: undefined }),
      create: async () => ({ data: { id: "ses-x" }, error: undefined }),
      children: async () => ({ data: [], error: undefined }),
      messages: async () => ({ data: [], error: undefined }),
      status: async () => ({ data: {}, error: undefined }),
      abort: async () => ({ data: undefined, error: undefined }),
      update: async () => ({ data: {}, error: undefined }),
      prompt: async () => ({ data: { info: {} }, error: undefined }),
      promptAsync: async () => ({ data: undefined, error: undefined }),
    },
    app: {
      agents: async () => ({ data: { agents }, error: undefined }),
    },
  };
}

describe("probeAutopermissions (compat.ts wiring surface)", () => {
  test("Case A classification when the agent block is visible", async () => {
    const client = makeClient([{ name: "build", permission: { edit: "allow", bash: "ask" } }]);
    const { OpenCodeRuntime } = await import("../../src/runtime/opencode-runtime.ts");
    const rt = new OpenCodeRuntime(client as never, ".");
    const result = await probeAutopermissions(rt, "ses-coord");
    expect(result.case).toBe("A");
    expect(result.agentBlockVisible).toBe(true);
  });

  test("Case B classification when only session permission is visible", async () => {
    const client = makeClient([], { edit: "allow", webfetch: "ask" });
    const { OpenCodeRuntime } = await import("../../src/runtime/opencode-runtime.ts");
    const rt = new OpenCodeRuntime(client as never, ".");
    const result = await probeAutopermissions(rt, "ses-coord");
    expect(result.case).toBe("B");
    expect(result.sessionPermissionVisible).toBe(true);
  });

  test("Case C (graceful degradation) when nothing is visible", async () => {
    const client = makeClient([]);
    const { OpenCodeRuntime } = await import("../../src/runtime/opencode-runtime.ts");
    const rt = new OpenCodeRuntime(client as never, ".");
    const result = await probeAutopermissions(rt, "ses-coord");
    expect(result.case).toBe("C");
    expect(result.detail).toContain("Case C");
  });
});
