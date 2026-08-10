import type { AgentRuntime, CreateRuntimeSessionInput, RuntimeMessage, RuntimeModelInfo, RuntimePromptInput, RuntimeSession, RuntimeSessionStatus } from "./runtime-types.js";

/**
 * Categorize a provider into a spawner-friendly tier.
 *   - opencode  -> "zen" (OpenCode Zen); models with a "-free" suffix -> "zen-free"
 *   - opencode-go -> "go"
 *   - everything else keeps its provider id as the tier.
 */
function tierForProvider(providerID: string): string {
  if (providerID === "opencode-go") return "go";
  if (providerID === "opencode") return "zen";
  return providerID;
}

/** Whether a zen model is a free model (suffix "-free" or prefix "free-"). */
function isZenFree(modelID: string): boolean {
  return /(^|-)(free|free-)/i.test(modelID);
}

export { tierForProvider, isZenFree };

/** Minimal structural client surface we require from the plugin-provided SDK client. */
export interface OpenCodeClientLike {
  session: {
    create(options: any): Promise<any>;
    get(options: any): Promise<any>;
    children(options: any): Promise<any>;
    messages(options: any): Promise<any>;
    status(options?: any): Promise<any>;
    abort(options: any): Promise<any>;
    update(options: any): Promise<any>;
    prompt(options: any): Promise<any>;
    promptAsync(options: any): Promise<any>;
    todo(options: any): Promise<any>;
  };
  event?: {
    subscribe(options?: any): Promise<any>;
  };
  config?: {
    providers(options?: any): Promise<any>;
  };
  app?: {
    agents(options?: any): Promise<any>;
  };
}

/** The permission ruleset for an agent: tool-level allow/ask/deny + bash
 * pattern overrides + the external_directory verdict. */
export interface AgentPermissions {
  edit: "ask" | "allow" | "deny";
  bash: "ask" | "allow" | "deny" | { [key: string]: "ask" | "allow" | "deny" };
  webfetch?: "ask" | "allow" | "deny";
  external_directory?: "ask" | "allow" | "deny";
}

/**
 * Production runtime adapter over the OpenCode SDK client supplied to
 * plugins via `input.client`. All OpenCode-specific calls are isolated here
 * so swarm core logic never imports the SDK directly.
 */
export class OpenCodeRuntime implements AgentRuntime {
  readonly kind = "opencode";
  private client: OpenCodeClientLike;

  constructor(
    client: OpenCodeClientLike,
    private directory?: string,
    private workspace?: string,
    private coordinatorSessionId?: string,
    private coordinatorAgent = "build",
  ) {
    this.client = client;
  }

  private q(extra?: Record<string, unknown>): Record<string, unknown> {
    // Only `directory` is sent: `workspace` on the v1 surface is an opaque
    // workspace ID and passing a path there causes a server error (verified
    // empirically). Worktree routing is handled by `directory`.
    return {
      directory: this.directory,
      ...(extra ?? {}),
    };
  }

  async createSession(input: CreateRuntimeSessionInput): Promise<RuntimeSession> {
    // v1 session.create accepts parentID/title/agent/metadata. `model` is NOT
    // part of the v1 create body (verified empirically) — model is applied per
    // prompt via session.prompt/promptAsync. Keep the fallback retry for
    // robustness against schema drift. NOTE: members are ROOT sessions — no
    // parentID is ever sent, so they appear as normal user chats in the app.
    const body: Record<string, unknown> = {
      title: input.title,
      agent: input.agent,
      metadata: input.metadata,
    };
    const query = input.directory
      ? { directory: input.directory }
      : this.q();
    const attempt = async (extra: Record<string, unknown>) => {
      const res = await this.client.session.create({
        body: { ...body, ...extra },
        query,
      });
      return res as any;
    };

    let res = await attempt({});
    if ((res as any).error) {
      // Retry with the minimal body in case the server rejects extra fields.
      res = await attempt({ title: input.title });
    }
    const data = (res as any).data;
    if (!data) throw new Error(`session.create failed: ${JSON.stringify((res as any).error ?? res)}`);
    return this.toSession(data);
  }

  async getSession(sid: string): Promise<RuntimeSession | null> {
    const res = await this.client.session.get({ path: { id: sid }, query: this.q() });
    const data = (res as any).data;
    return data ? this.toSession(data) : null;
  }

  async listChildren(parentSID: string): Promise<RuntimeSession[]> {
    const res = await this.client.session.children({ path: { id: parentSID }, query: this.q() });
    const data = (res as any).data;
    if (!Array.isArray(data)) return [];
    return data.map((s: any) => this.toSession(s));
  }

  async prompt(input: RuntimePromptInput, sessionID: string): Promise<RuntimeMessage> {
    const parts = input.parts ?? [{ type: "text" as const, text: input.text, messageID: input.messageID }];
    const res = await this.client.session.prompt({
      path: { id: sessionID },
      body: {
        parts,
        system: input.system,
        model: input.model,
        agent: input.agent,
        messageID: input.messageID,
      },
      query: this.q(),
    });
    const data = (res as any).data;
    if (!data) throw new Error(`session.prompt failed: ${JSON.stringify((res as any).error ?? res)}`);
    return this.toMessage(data.info);
  }

  async promptAsync(input: RuntimePromptInput, sessionID: string): Promise<void> {
    // A `synthetic` prompt is injected as a background system message (matching
    // the built-in task tool's result injection): it doesn't require a user
    // turn and carries no user-authored content, so busy members process it at
    // their next boundary without interrupting their current work.
    const parts = input.synthetic
      ? [{ type: "text" as const, text: input.text, synthetic: true, messageID: input.messageID }]
      : (input.parts ?? [{ type: "text" as const, text: input.text, messageID: input.messageID }]);
    const res = await this.client.session.promptAsync({
      path: { id: sessionID },
      body: {
        parts,
        system: input.system,
        model: input.model,
        agent: input.agent,
        messageID: input.messageID,
      },
      query: this.q(),
    });
    // 204 => no data; treat presence of an error object as failure
    const err = (res as any).error;
    if (err) throw new Error(`session.promptAsync failed: ${JSON.stringify(err)}`);
  }

  async abort(sid: string): Promise<void> {
    const res = await this.client.session.abort({ path: { id: sid }, query: this.q() });
    const err = (res as any).error;
    if (err) throw new Error(`session.abort failed: ${JSON.stringify(err)}`);
  }

  async getStatus(sid: string): Promise<RuntimeSessionStatus | null> {
    const res = await this.client.session.status({ query: this.q() });
    const data = (res as any).data;
    if (!data || typeof data !== "object") return null;
    return (data as Record<string, RuntimeSessionStatus>)[sid] ?? null;
  }

  async getMessages(sid: string): Promise<RuntimeMessage[]> {
    const res = await this.client.session.messages({ path: { id: sid }, query: this.q() });
    const data = (res as any).data;
    if (!Array.isArray(data)) return [];
    return data.map((m: any) => this.toMessage(m.info));
  }

  async updateSession?(sid: string, patch: { title?: string; metadata?: Record<string, unknown>; permission?: AgentPermissions }): Promise<void> {
    const body: Record<string, unknown> = {};
    if (patch.title !== undefined) body.title = patch.title;
    if (patch.metadata !== undefined) body.metadata = patch.metadata;
    // OpenCode's session.update accepts a `permission` field (title/metadata/
    // permission/archived-time; verified in ROOT_MEMBER_SESSIONS_PLAN fact 9).
    // Forwarding it here is the plugin-only WRITE path for session-private
    // permission state (Case B propagation). It does NOT widen permissions by
    // itself — the caller must pass the exact ruleset to write, and any
    // propagation layer must clamp to worktree/temp (D6).
    if (patch.permission !== undefined) body.permission = patch.permission;
    const res = await this.client.session.update({
      path: { id: sid },
      body,
      query: this.q(),
    });
    const err = (res as any).error;
    if (err) throw new Error(`session.update failed: ${JSON.stringify(err)}`);
  }

  async listModels(): Promise<RuntimeModelInfo[]> {
    if (!this.client.config?.providers) return [];
    const res = await this.client.config.providers({});
    const payload = (res as any).data;
    const providers = payload?.providers ?? payload;
    if (!Array.isArray(providers)) return [];
    const out: RuntimeModelInfo[] = [];
    for (const p of providers) {
      const pid = p?.id ?? "";
      const models = p?.models ?? {};
      for (const [modelID, m] of Object.entries(models)) {
        const info = (m as any) ?? {};
        const base = tierForProvider(pid);
        const tier = base === "zen" && isZenFree(modelID) ? "zen-free" : base;
        out.push({
          providerID: pid,
          modelID,
          name: typeof info?.name === "string" ? info.name : undefined,
          tier,
        });
      }
    }
    return out;
  }

  /**
   * Resolve a session's agent permission ruleset. The session's agent (e.g.
   * "build" for the coordinator, "swarm" for members) is looked up in the
   * configured agents; its `permission` block is the inheritance source so a
   * member can be auto-allowed for exactly what its coordinator's agent allows.
   * Returns undefined when the runtime cannot determine it (caller falls back).
   */
  async getSessionPermissions(sessionID: string): Promise<AgentPermissions | undefined> {
    try {
      let agentName: string | undefined;
      try {
        const session = await this.getSession(sessionID);
        agentName = (session as any)?.agent;
      } catch {
        agentName = undefined;
      }
      // If the session payload doesn't expose its agent, try the common ones:
      // the coordinator typically runs "build" (or the user's primary agent);
      // members run "swarm". Prefer an agent we can actually resolve.
      if (!this.client.app?.agents) return undefined;
      const res = await this.client.app.agents({});
      const agents = (res as any).data?.agents ?? (res as any).data;
      if (!Array.isArray(agents)) return undefined;
      const names = agentName ? [agentName] : [this.coordinatorAgent, "build", "swarm"];
      const agent = names
        .map((n) => agents.find((a: any) => a?.name === n))
        .find((a: any) => a?.permission);
      if (!agent?.permission) return undefined;
      return {
        edit: agent.permission.edit ?? "ask",
        bash: agent.permission.bash ?? "ask",
        webfetch: agent.permission.webfetch ?? "ask",
        external_directory: agent.permission.external_directory ?? "ask",
      };
    } catch {
      return undefined;
    }
  }

  /**
   * Resolve a member model reference to a real, available model. Maps tier
   * labels used by mistake ("go"/"zen"/"zen-free") to their real provider ids
   * and returns undefined if the provider/model pair is unavailable — the
   * caller then falls back to a default instead of erroring the spawn.
   */
  async resolveModel(model?: { providerID?: string; modelID?: string }): Promise<{ providerID: string; modelID: string } | undefined> {
    if (!model?.providerID || !model.modelID) return undefined;
    let providerID = model.providerID;
    if (providerID === "go") providerID = "opencode-go";
    else if (providerID === "zen" || providerID === "zen-free") providerID = "opencode";
    const all = await this.listModels();
    const hit = all.find((m) => m.providerID === providerID && m.modelID === model.modelID);
    return hit ? { providerID: hit.providerID, modelID: hit.modelID } : undefined;
  }

  /**
   * Fetch a session's todo list (the OpenCode todolist tool state, per-session).
   * Lets swarm members read each other's in-progress todo items so they can see
   * what a peer is actively doing — a cross-member redundancy probe. Returns
   * [] on any error (todo may not exist / be unreachable).
   */
  async getSessionTodos(sessionID: string): Promise<Array<{ content: string; status: string; priority: string }>> {
    try {
      if (!this.client.session.todo) return [];
      const res = await this.client.session.todo({ path: { id: sessionID }, query: this.q() });
      const data = (res as any).data;
      if (!Array.isArray(data)) return [];
      return data.map((t: any) => ({
        content: t?.content ?? "",
        status: t?.status ?? "pending",
        priority: t?.priority ?? "medium",
      }));
    } catch {
      return [];
    }
  }

  private toSession(s: any): RuntimeSession {
    return {
      id: s.id,
      title: s.title ?? "",
      directory: s.directory ?? "",
      parentID: s.parentID,
      model: s.model
        ? { providerID: s.model.providerID ?? "", modelID: s.model.modelID ?? s.model.id ?? "" }
        : undefined,
      metadata: s.metadata,
      // Map the session's OWN permission ruleset if the payload exposes one.
      // This is the READ path for session-private permission state (Case B in
      // decisions/autopermissions-propagation-plan): without it the plugin
      // cannot tell "toggle lives in the agent config" (Case A) from "toggle
      // lives in session-private state" (Case B). No widening happens here.
      permission: s.permission
        ? {
            edit: s.permission.edit ?? "ask",
            bash: s.permission.bash ?? "ask",
            webfetch: s.permission.webfetch ?? "ask",
            external_directory: s.permission.external_directory ?? "ask",
          }
        : undefined,
    };
  }

  private toMessage(info: any): RuntimeMessage {
    return {
      id: info.id,
      role: info.role === "user" ? "user" : "assistant",
      createdAt: info.time?.created ?? info.createdAt ?? Date.now(),
      parts: info.parts
        ? info.parts.map((p: any) => ({ type: p.type, text: p.text }))
        : [],
      modelID: info.model?.modelID,
    };
  }
}