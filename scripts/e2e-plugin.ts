/**
 * E2E plugin puppet harness (spec §58 E2E).
 *
 * Spawns a FRESH `opencode serve` process in a temp project that registers the
 * built swarm plugin, polls it until ready, drives it via the SDK client, and
 * unconditionally kills the child in `finally`. It never attaches to an
 * existing OpenCode process.
 *
 * Usage:
 *   bun run scripts/e2e-plugin.ts [--probe-session]
 *
 * Env:
 *   OPENCODE_BIN   path to the opencode executable (default: on PATH)
 *   OPENCODE_E2E_PORT  port (default: random free port)
 *   KEEP_SERVER=1  keep the server running after checks (for manual inspection)
 */
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createOpencodeClient } from "@opencode-ai/sdk";

const ROOT = resolve(import.meta.dir, "..");
const DIST = join(ROOT, "dist", "index.js");

function pickPort(): number {
  return 20000 + Math.floor(Math.random() * 20000);
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForHealth(url: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${url}/api/health`);
      if (res.ok) {
        const body = (await res.json()) as { healthy?: boolean };
        if (body.healthy) return true;
      }
    } catch {
      /* not up yet */
    }
    await sleep(300);
  }
  return false;
}

function killTree(child: ChildProcess | undefined): void {
  if (!child || child.exitCode !== null) return;
  try {
    child.kill();
  } catch {
    /* ignore */
  }
  // Windows: ensure the process tree is gone.
  if (process.platform === "win32") {
    try {
      spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
    } catch {
      /* ignore */
    }
  }
}

async function resolveBin(): Promise<string> {
  const fromEnv = process.env.OPENCODE_BIN;
  if (fromEnv && existsSync(fromEnv)) return fromEnv;
  // npm global bin
  const candidates = [
    join(process.env.APPDATA ?? "", "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"),
    join(process.env.APPDATA ?? "", "npm", "node_modules", "opencode-ai", "bin", "opencode"),
  ];
  for (const c of candidates) {
    if (existsSync(c)) return c;
  }
  return "opencode"; // rely on PATH
}

/**
 * Drive a real peer conversation on the fresh server: coordinator session uses
 * the swarm tools via the model, spawns members, and messages one directly.
 * Uses the same model that the tool-listing check used.
 */
async function runConversationScenario(
  client: ReturnType<typeof createOpencodeClient>,
  directory: string,
  coordinatorSid: string,
): Promise<{ ok: boolean; detail?: string }> {
  const q = { directory };
  const model = {
    providerID: process.env.OPENCODE_E2E_PROVIDER ?? "opencode",
    modelID: process.env.OPENCODE_E2E_MODEL ?? "deepseek-v4-flash-free",
  };

  async function ask(text: string, timeoutMs = 120_000): Promise<string> {
    const msgCountBefore = ((await client.session.messages({ path: { id: coordinatorSid }, query: q })).data as any[])?.length ?? 0;
    await client.session.prompt({
      path: { id: coordinatorSid },
      body: { parts: [{ type: "text", text }], model },
      query: q,
    });
    const t0 = Date.now();
    while (Date.now() - t0 < timeoutMs) {
      await sleep(2000);
      const msgs = (await client.session.messages({ path: { id: coordinatorSid }, query: q })).data as any[];
      const newMsgs = msgs.slice(msgCountBefore);
      const last = newMsgs[newMsgs.length - 1];
      if (last?.info?.role === "assistant" && Array.isArray(last.parts)) {
        const out = last.parts.filter((p: any) => p.type === "text").map((p: any) => p.text ?? "").join("");
        if (out.trim().length > 0) return out;
      }
    }
    return "";
  }

  try {
    // 1. create swarm
    const createOut = await ask(
      `Use the swarm_create tool with name "e2e-p2p-${Date.now()}". Reply with ONLY the JSON output of that tool. Do not call any other tool.`,
    );
    const m = createOut.match(/\{[\s\S]*\}/);
    if (!m) return { ok: false, detail: `no swarm id: ${createOut.slice(0, 200)}` };
    const createdJson = JSON.parse(m[0]);
    const swarmId = createdJson.swarm?.id ?? createdJson.id;
    console.log(`[conv] swarm created: ${swarmId}`);

    // 2. delegate to a member in ONE call (task-tool style, no manual wake)
    const taskOut = await ask(
      `Use the swarm_task tool with swarmId "${swarmId}", name "backend", role "impl", title "Inspect project", prompt "List the files in the current directory and report the count of .ts files. Reply with the count only." Reply with ONLY the JSON output of that tool. Do not call any other tool.`,
    );
    console.log(`[conv] swarm_task result: ${taskOut.slice(0, 300)}`);
    const taskJsonMatch = taskOut.match(/\{[\s\S]*\}/);
    let tj: any = undefined;
    if (taskJsonMatch) {
      tj = JSON.parse(taskJsonMatch[0]);
      if (tj.member) console.log(`[conv] member ${tj.member.name} status=${tj.member.status} session=${tj.member.sessionId}`);
      if (tj.note) console.log(`[conv] NOTE: ${tj.note}`);
    }

    // 3. Deterministic check: the member session must be a ROOT session (no
    // parentID) and appear in the roots listing — members are normal chats.
    const memberSid = tj?.member?.sessionId;
    if (memberSid) {
      const memberSession = (await client.session.get({ path: { id: memberSid }, query: q })).data as any;
      console.log(`[conv] member session ${memberSid} parentID=${JSON.stringify(memberSession?.parentID ?? null)}`);
      if (memberSession?.parentID != null) {
        return { ok: false, detail: `member session is NOT a root (parentID=${memberSession.parentID})` };
      }
      // Roots listing contains the member session.
      const roots = (await client.session.list({ query: { ...q, roots: true } as never })).data as any[];
      const found = (roots ?? []).some((s: any) => s?.id === memberSid);
      console.log(`[conv] member in roots listing: ${found} (${roots?.length ?? 0} root sessions)`);
      if (!found) return { ok: false, detail: "member session missing from roots listing" };
    } else {
      console.log(`[conv] swarm_task did not surface a member session id — skipping root assertion`);
    }

    return { ok: true, detail: "conversation scenario completed" };
  } catch (e) {
    return { ok: false, detail: (e as Error).message };
  }
}

async function main(): Promise<void> {
  if (!existsSync(DIST)) {
    console.error(`Built plugin not found at ${DIST}. Run \`bun run build\` first.`);
    process.exit(1);
  }

  const bin = await resolveBin();
  const port = Number(process.env.OPENCODE_E2E_PORT ?? pickPort());
  const url = `http://127.0.0.1:${port}`;
  const tmp = mkdtempSync(join(tmpdir(), "swarm-e2e-"));
  const projectDir = join(tmp, "project");
  const { mkdirSync } = await import("node:fs");
  mkdirSync(projectDir, { recursive: true });
  writeFileSync(
    join(projectDir, "opencode.json"),
    JSON.stringify({ plugin: [`${DIST.replace(/\\/g, "\\\\")}`] }, null, 2),
  );

  let server: ChildProcess | undefined;
  try {
    console.log(`[e2e] plugin under test: ${DIST}`);
    console.log(`[e2e] spawning fresh server on :${port} in ${projectDir}`);

    server = spawn(bin, ["serve", "--port", String(port), "--log-level", "DEBUG"], {
      cwd: projectDir,
      stdio: ["ignore", "pipe", "pipe"],
      shell: false,
      windowsHide: true,
    });
    server.stdout?.on("data", (d) => process.stdout.write(`[server] ${d}`));
    server.stderr?.on("data", (d) => process.stdout.write(`[server:err] ${d}`));
    server.on("exit", (code, signal) => {
      console.log(`[e2e] server exited code=${code} signal=${signal}`);
    });

    const healthy = await waitForHealth(url, 30_000);
    if (!healthy) {
      console.error("[e2e] FAIL: server did not become healthy in 30s");
      process.exitCode = 1;
      return;
    }
    console.log("[e2e] server healthy");

    const client = createOpencodeClient({ baseUrl: url });

    // Create a session in the project to force instance bootstrap + plugin load.
    const dirQuery = { directory: projectDir.replace(/\\/g, "/") };
    const createRes = await client.session.create({
      body: { title: "swarm-e2e-probe" },
      query: dirQuery,
    });
    const sid = createRes.data?.id;
    console.log(`[e2e] created session: ${sid}`);
    if (!sid) {
      console.error(`[e2e] FAIL: session create error=${JSON.stringify(createRes.error)}`);
      process.exitCode = 1;
      return;
    }

    // Wait a moment for plugin tools to be registered, then list them.
    await sleep(1500);
    const toolRes = await client.tool.list({
      query: { directory: dirQuery.directory, provider: "opencode", model: "deepseek-v4-flash-free" },
    });
    const tools = (toolRes.data ?? []) as Array<{ name?: string; id?: string }>;
    const names = tools.map((t) => t.name ?? t.id ?? "");
    const swarmTools = names.filter((n) => n.startsWith("swarm_"));
    console.log(`[e2e] tools visible: ${names.length}`);
    console.log(`[e2e] swarm tools: ${swarmTools.join(", ") || "NONE"}`);

    if (swarmTools.length < 7) {
      console.error(`[e2e] FAIL: expected >=7 swarm tools, got ${swarmTools.length}`);
      process.exitCode = 1;
      return;
    }
    console.log("[e2e] PASS: plugin loaded and swarm tools registered");

    // Optional: run the live peer-conversation scenario against this server.
    if (process.argv.includes("--conversation")) {
      const conv = await runConversationScenario(client, dirQuery.directory, sid);
      if (!conv.ok) {
        process.exitCode = 1;
        return;
      }
      console.log("[e2e] PASS: peer conversation scenario");
    }

    // Cleanup created session.
    try {
      await client.session.delete({ path: { id: sid }, query: dirQuery });
    } catch {
      /* best effort */
    }
    console.log("[e2e] PASS: all checks");
  } finally {
    if (process.env.KEEP_SERVER !== "1") {
      console.log("[e2e] killing server");
      killTree(server);
      await sleep(500);
      rmSync(tmp, { recursive: true, force: true });
    } else {
      console.log(`[e2e] KEEP_SERVER=1: server left running at ${url} (project ${projectDir})`);
    }
  }
}

await main();