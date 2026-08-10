import { describe, expect, test } from "bun:test";
import { OpenCodeRuntime } from "../../src/runtime/opencode-runtime.ts";
import { probeAutopermissions } from "../../src/probe/autopermissions.ts";

function makeClient(agents: any[], todoData: any[] = [], sessionData: any = {}): any {
  return {
    session: {
      get: async ({ path }: any) => ({ data: { id: path.id, agent: "build", ...sessionData }, error: undefined }),
      create: async () => ({ data: { id: "ses-1" }, error: undefined }),
      children: async () => ({ data: [], error: undefined }),
      messages: async () => ({ data: [], error: undefined }),
      status: async () => ({ data: {}, error: undefined }),
      abort: async () => ({ data: undefined, error: undefined }),
      update: async () => ({ data: {}, error: undefined }),
      prompt: async () => ({ data: { info: {} }, error: undefined }),
      promptAsync: async () => ({ data: undefined, error: undefined }),
      todo: async () => ({ data: todoData, error: undefined }),
    },
    config: { providers: async () => ({ data: { providers: [] }, error: undefined }) },
    app: {
      agents: async () => ({ data: { agents }, error: undefined }),
    },
  };
}

describe("OpenCodeRuntime.getSessionPermissions", () => {
  test("resolves the session's agent permission ruleset", async () => {
    const rt = new OpenCodeRuntime(makeClient([
      {
        name: "build",
        permission: {
          edit: "allow",
          bash: { "npm *": "allow", "git *": "allow" },
          webfetch: "allow",
          external_directory: "deny",
        },
      },
    ]) as never, ".");

    const perms = await rt.getSessionPermissions("ses-coord");
    expect(perms?.edit).toBe("allow");
    expect(perms?.webfetch).toBe("allow");
    expect(perms?.external_directory).toBe("deny");
    expect(perms?.bash).toEqual({ "npm *": "allow", "git *": "allow" });
  });

  test("returns undefined when no agents are configured", async () => {
    const rt = new OpenCodeRuntime(makeClient([]) as never, ".");
    expect(await rt.getSessionPermissions("ses-coord")).toBeUndefined();
  });

  test("falls back to known agents when the session has none", async () => {
    const client = makeClient([
      { name: "build", permission: { edit: "allow", bash: "ask" } },
    ]);
    // Session payload without an agent name.
    client.session.get = async () => ({ data: { id: "ses-x" }, error: undefined });
    const rt = new OpenCodeRuntime(client as never, ".");
    const perms = await rt.getSessionPermissions("ses-x");
    expect(perms?.edit).toBe("allow");
  });
});

describe("OpenCodeRuntime.getSessionTodos", () => {
  test("returns the session's todo list", async () => {
    const rt = new OpenCodeRuntime(makeClient([], [
      { id: "t1", content: "pack the nibble lane", status: "in_progress", priority: "high" },
      { id: "t2", content: "verify sort parity", status: "pending", priority: "medium" },
    ]) as never, ".");
    const todos = await rt.getSessionTodos("ses-1");
    expect(todos.length).toBe(2);
    expect(todos[0]?.content).toBe("pack the nibble lane");
    expect(todos[0]?.status).toBe("in_progress");
  });

  test("returns [] when the client has no todo surface", async () => {
    const client = makeClient([]);
    delete client.session.todo;
    const rt = new OpenCodeRuntime(client as never, ".");
    expect(await rt.getSessionTodos("ses-1")).toEqual([]);
  });
});

describe("OpenCodeRuntime session permission surface (Case B probe)", () => {
  test("getSession maps the session's OWN permission field (session-private read)", async () => {
    // Case B: the toggle lives in session-private state, exposed on session.get.
    const rt = new OpenCodeRuntime(
      makeClient([], [], { permission: { edit: "allow", bash: "ask", webfetch: "deny" } }) as never,
      ".",
    );
    const session = await rt.getSession("ses-coord");
    expect(session?.permission).toEqual({ edit: "allow", bash: "ask", webfetch: "deny", external_directory: "ask" });
  });

  test("getSession leaves permission undefined when the payload has none", async () => {
    const rt = new OpenCodeRuntime(makeClient([], []) as never, ".");
    const session = await rt.getSession("ses-coord");
    expect(session?.permission).toBeUndefined();
  });

  test("updateSession forwards a permission patch to session.update (session-private write)", async () => {
    let sent: any;
    const client = makeClient([]);
    client.session.update = async ({ path, body }: any) => {
      sent = { path, body };
      return { data: {}, error: undefined };
    };
    const rt = new OpenCodeRuntime(client as never, ".");
    const patch = {
      permission: { edit: "deny" as const, bash: "ask" as const, webfetch: "ask" as const, external_directory: "ask" as const },
    };
    await rt.updateSession!("ses-mem", patch);
    expect(sent.body.permission).toEqual(patch.permission);
    // title/metadata must remain optional — forwarding must not force them.
    expect(sent.body.title).toBeUndefined();
    expect(sent.body.metadata).toBeUndefined();
  });
});

describe("probeAutopermissions (Case A/B/C classification)", () => {
  test("Case A: agent block visible, no session permission field", async () => {
    const rt = new OpenCodeRuntime(
      makeClient([{ name: "build", permission: { edit: "allow", bash: "ask" } }]) as never,
      ".",
    );
    const result = await probeAutopermissions(rt, "ses-coord");
    expect(result.case).toBe("A");
    expect(result.agentBlockVisible).toBe(true);
    expect(result.sessionPermissionVisible).toBe(false);
  });

  test("Case B: session permission field visible, no agent block", async () => {
    const rt = new OpenCodeRuntime(
      makeClient([], [], { permission: { edit: "allow" } }) as never,
      ".",
    );
    const result = await probeAutopermissions(rt, "ses-coord");
    expect(result.case).toBe("B");
    expect(result.agentBlockVisible).toBe(false);
    expect(result.sessionPermissionVisible).toBe(true);
  });

  test("Case C: neither surface visible", async () => {
    const rt = new OpenCodeRuntime(makeClient([]) as never, ".");
    const result = await probeAutopermissions(rt, "ses-coord");
    expect(result.case).toBe("C");
    expect(result.agentBlockVisible).toBe(false);
    expect(result.sessionPermissionVisible).toBe(false);
  });

  test("mixed: both surfaces visible", async () => {
    const rt = new OpenCodeRuntime(
      makeClient([{ name: "build", permission: { edit: "allow", bash: "ask" } }], [], {
        permission: { edit: "deny" },
      }) as never,
      ".",
    );
    const result = await probeAutopermissions(rt, "ses-coord");
    expect(result.case).toBe("mixed");
    expect(result.agentBlockVisible).toBe(true);
    expect(result.sessionPermissionVisible).toBe(true);
  });
});

