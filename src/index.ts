import type { PluginModule } from "@opencode-ai/plugin";
import { swarmPlugin } from "./plugin.js";

export { swarmPlugin } from "./plugin.js";
export { OpenCodeRuntime } from "./runtime/opencode-runtime.js";
export { SQLiteStore } from "./storage/sqlite-store.js";
export { SwarmCore } from "./core/swarm.js";
export type { AgentRuntime, RuntimeSession, RuntimeMessage, RuntimeSessionStatus, RuntimePromptInput, CreateRuntimeSessionInput } from "./runtime/runtime-types.js";
export type { SwarmStore, SwarmStoreTx } from "./storage/store.js";
export type * from "./core/types.js";

const plugin: PluginModule = {
  id: "opencode-agent-swarms",
  server: swarmPlugin,
};

export default plugin;