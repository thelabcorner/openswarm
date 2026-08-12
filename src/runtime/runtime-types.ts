import type { SwarmMember } from "../core/types.js";

export type RuntimeSessionStatus =
  | { type: "idle" }
  | { type: "retry"; attempt: number; message: string; next: number }
  | { type: "busy" };

export interface RuntimeSession {
  id: string;
  title: string;
  directory: string;
  parentID?: string;
  model?: {
    providerID: string;
    modelID: string;
  };
  metadata?: Record<string, unknown>;
  /** The session's OWN permission ruleset (session-private state), when the
   * runtime exposes it on session.get. Distinct from the AGENT's permission
   * block: autopermissions toggles (Ctrl+Shift+A / settings) may live here
   * rather than in the agent config — probing this field distinguishes
   * "Case A (agent permission visible)" from "Case B (session-private)". */
  permission?: AgentPermissions;
}

export interface RuntimeMessage {
  id: string;
  role: "user" | "assistant";
  createdAt: number;
  parts: Array<{ type: string; text?: string }>;
  modelID?: string;
}

export interface CreateRuntimeSessionInput {
  title: string;
  agent?: string;
  model?: {
    providerID: string;
    modelID: string;
  };
  metadata?: Record<string, unknown>;
  /** Override the session's working directory (root the member in the swarm's
   * worktree rather than the plugin default). */
  directory?: string;
}

export interface RuntimePromptInput {
  text: string;
  /**
   * Optional message id to attach to the created user message. Lets the plugin
   * recognize its own injections in the `chat.message` hook (self-injection
   * classification), so a human chat can be told apart from swarm machinery.
   */
  messageID?: string;
  /**
   * Extra context injected as a separate user part or appended to system.
   */
  system?: string;
  model?: {
    providerID: string;
    modelID: string;
  };
  /**
   * The agent to run this prompt as. When set (e.g. "swarm"), OpenCode applies
   * that agent's system prompt and permissions for the turn.
   */
  agent?: string;
  /**
   * When true, the prompt is a synthetic system-injected message (e.g. a
   * background-task completion notification), matching the task tool's
   * synthetic result injection. It does not require a user turn.
   */
  synthetic?: boolean;
  parts?: Array<{ type: "text"; text: string; synthetic?: boolean; messageID?: string }>;
}

export interface RuntimeModelInfo {
  providerID: string;
  modelID: string;
  name?: string;
  /** Provider tier: "zen" (OpenCode Zen), "zen-free", "go" (OpenCode Go), or provider id. */
  tier: string;
  /** Capabilities (from the provider config `modalities`): which input kinds
   * the model can consume (text/image/pdf/audio/video). A model with no
   * modalities declared is assumed text-only (safe for the capability filter). */
  modalities?: { input: string[]; output: string[] };
  /** USD per 1M tokens (provider config `cost`). Absent when unknown — the
   * pricing fallback catalog or tier ordering applies then. */
  cost?: { input: number; output: number; cacheRead?: number; cacheWrite?: number };
  /** Context window in tokens (provider config `limit.context`). */
  contextLimit?: number;
}

export interface AgentRuntime {
  readonly kind: string;
  createSession(input: CreateRuntimeSessionInput): Promise<RuntimeSession>;
  getSession(sid: string): Promise<RuntimeSession | null>;
  listChildren(parentSID: string): Promise<RuntimeSession[]>;
  prompt(input: RuntimePromptInput, sessionID: string): Promise<RuntimeMessage>;
  promptAsync(input: RuntimePromptInput, sessionID: string): Promise<void>;
  abort(sid: string): Promise<void>;
  getStatus(sid: string): Promise<RuntimeSessionStatus | null>;
  getMessages(sid: string): Promise<RuntimeMessage[]>;
  updateSession?(sid: string, patch: { title?: string; metadata?: Record<string, unknown>; permission?: AgentPermissions }): Promise<void>;
  /** List models the runtime has access to, categorized by provider tier. */
  listModels?(): Promise<RuntimeModelInfo[]>;
  /** Resolve/validate a member model ref; returns undefined if unavailable. */
  resolveModel?(model?: { providerID?: string; modelID?: string }): Promise<{ providerID: string; modelID: string } | undefined>;
  /** Resolve the agent permission ruleset for a session (undefined if the
   * runtime cannot determine it). Members inherit the coordinator's verdicts
   * so they don't get separate permission prompts for work the parent allows. */
  getSessionPermissions?(sessionID: string): Promise<AgentPermissions | undefined>;
  /** Fetch a session's todo list (per-session todolist tool state). Lets members
   * read each other's in-progress items to avoid redundant work. */
  getSessionTodos?(sessionID: string): Promise<Array<{ content: string; status: string; priority: string }>>;
  /** Answer a pending permission prompt for a session (the coordinator replying
   * to a member's stall). Returns false when the prompt is already gone
   * (answered/expired) or the runtime cannot reach it. */
  replyPermission?(sessionID: string, permissionID: string, response: "once" | "always" | "reject"): Promise<boolean>;
}

/** The permission ruleset for an agent (mirrors AgentPermissions in
 * opencode-runtime.ts; kept here so core/types never imports the runtime). */
export interface AgentPermissions {
  edit: "ask" | "allow" | "deny";
  bash: "ask" | "allow" | "deny" | { [key: string]: "ask" | "allow" | "deny" };
  webfetch?: "ask" | "allow" | "deny";
  external_directory?: "ask" | "allow" | "deny";
}

/** Resolve an OpenCode SDK session ID for a member's backing runtime session. */
export function memberRuntimeID(member: Pick<SwarmMember, "sessionId">): string {
  return member.sessionId;
}