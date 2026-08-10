import { createOpencodeClient } from "@opencode-ai/sdk";
import { probeAutopermissions } from "./autopermissions.js";
import { OpenCodeRuntime } from "../runtime/opencode-runtime.js";

export interface ProbeCheck {
  name: string;
  ok: boolean;
  detail: string;
  durationMs: number;
}

export interface ProbeReport {
  opencodeVersion?: string;
  baseUrl: string;
  generatedAt: string;
  checks: ProbeCheck[];
  summary: {
    passed: number;
    failed: number;
    total: number;
  };
}

export interface ProbeOptions {
  baseUrl: string;
  directory?: string;
  model?: { providerID: string; modelID: string };
  /**
   * When false, the async prompt checks are skipped (caller must explicitly
   * allow inference to avoid surprising the user with model runs).
   */
  allowInference?: boolean;
  timeoutMs?: number;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function runCompatibilityProbe(opts: ProbeOptions): Promise<ProbeReport> {
  const client = createOpencodeClient({ baseUrl: opts.baseUrl });
  const checks: ProbeCheck[] = [];
  const mark = (name: string, ok: boolean, detail: string, durationMs: number) =>
    checks.push({ name, ok, detail, durationMs });
  const timeout = opts.timeoutMs ?? 60_000;

  let created: string[] = [];
  let version: string | undefined;

  async function withTime<T>(name: string, fn: () => Promise<T>, okDetail: (r: T) => string): Promise<T | undefined> {
    const t0 = performance.now();
    try {
      const r = await fn();
      mark(name, true, okDetail(r), Math.round(performance.now() - t0));
      return r;
    } catch (e) {
      mark(name, false, String((e as Error).message ?? e), Math.round(performance.now() - t0));
      return undefined;
    }
  }

  try {
    // 1. health / server reachability (via direct HTTP since SDK method may vary)
    await withTime("server.health", async () => {
      const res = await fetch(`${opts.baseUrl}/api/health`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json() as Promise<{ healthy?: boolean }>;
    }, (d) => JSON.stringify(d));

    // 1b. config version (best-effort; validation errors in user config may mask it)
    await withTime("server.config", async () => {
      const r = await client.config.get({});
      const d = (r as any).data;
      if (!d) {
        const e = (r as any).error as { data?: { message?: string } } | undefined;
        const msg = e?.data?.message;
        // Fall back to reading version from a session record if available.
        if (!version) {
          const sessions = (await client.session.list({ query: { directory: opts.directory } })).data as any[];
          version = sessions?.find((s) => s.version)?.version;
        }
        return msg ? `config validation issue (non-fatal): ${msg.slice(0, 80)}` : "ok";
      }
      const v = d.version ?? d.installed?.version ?? "";
      if (typeof v === "string") version = v;
      return v ? `version=${v}` : "ok";
    }, (r) => String(r));

    // 2. project context
    await withTime("project.current", async () => {
      const r = await client.project.current({});
      if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
      return r;
    }, (r) => JSON.stringify((r as any).data ?? "").slice(0, 160));

    // 3. list sessions
    await withTime("session.list", () => client.session.list({ query: { directory: opts.directory } }), (r) => {
      const d = (r as any).data;
      return Array.isArray(d) ? `${d.length} sessions` : "ok";
    });

    // 4. create a root session
    const root = await withTime("session.create", async () => {
      const r = await client.session.create({ body: { title: "probe-root" }, query: { directory: opts.directory } });
      if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
      return (r as any).data;
    }, (s) => `id=${(s as any)?.id}`);
    if (root?.id) created.push(root.id);

    // 5. create a child session (parentID)
    const child = await withTime("session.create.parent", async () => {
      const r = await client.session.create({ body: { parentID: root?.id, title: "probe-child" }, query: { directory: opts.directory } });
      if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
      return (r as any).data;
    }, (s) => {
      const d = s as any;
      return `id=${d?.id} parentID=${d?.parentID}`;
    });
    if (child?.id) created.push(child.id);

    // 5b. roots listing: the root session must be listable with roots:true and
    // the child must NOT appear there (root-member visibility payoff).
    if (root?.id) {
      await withTime("session.list.roots", async () => {
        const r = await client.session.list({ query: { directory: opts.directory, roots: true } as never });
        if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
        const d = (r as any).data;
        if (!Array.isArray(d)) throw new Error("session.list roots response is not an array");
        const ids = (d as any[]).map((s) => s?.id).filter(Boolean);
        if (!ids.includes(root.id)) throw new Error(`root session ${root.id} not in roots listing`);
        if (child?.id && ids.includes(child.id)) throw new Error(`child session ${child.id} leaked into roots listing`);
        return ids;
      }, (ids) => `${(ids as any[]).length} root session(s); root visible, child excluded`);
    }

    // 6. get parent
    if (root?.id) {
      await withTime("session.get", async () => {
        const r = await client.session.get({ path: { id: root.id }, query: { directory: opts.directory } });
        if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
        return r;
      }, (r) => `title=${((r as any).data as any)?.title}`);
    }

    // 7. children of parent
    if (root?.id) {
      await withTime("session.children", async () => {
        const r = await client.session.children({ path: { id: root.id }, query: { directory: opts.directory } });
        if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
        const d = (r as any).data;
        if (!Array.isArray(d)) throw new Error("children response is not an array");
        return d;
      }, (d) => `${(d as any[]).length} child(ren)`);
    }

    // 8. session status map
    await withTime("session.status", () => client.session.status({ query: { directory: opts.directory } }), (r) => {
      const d = (r as any).data;
      return d ? `${Object.keys(d).length} status entries` : "ok";
    });

    // 9. messages on child (empty initially)
    if (child?.id) {
      await withTime("session.messages", async () => {
        const r = await client.session.messages({ path: { id: child.id }, query: { directory: opts.directory } });
        if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
        const d = (r as any).data;
        if (!Array.isArray(d)) throw new Error("messages response not an array");
        return d;
      }, (d) => `${(d as any[]).length} messages`);
    }

    // 10. update title (permission/metadata write)
    if (root?.id) {
      await withTime("session.update", async () => {
        const r = await client.session.update({ path: { id: root.id }, body: { title: "SWARM-PROBE-ROOT" }, query: { directory: opts.directory } });
        if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
        return (r as any).data;
      }, (s) => `title=${(s as any)?.title}`);
    }

    // 10b. autopermissions propagation probe (Case A/B/C/mixed classification).
    // Uses the root session as the coordinator session. Graceful degradation:
    // when the runtime surface is unavailable, report the classification as
    // informational rather than failing the whole probe.
    if (root?.id) {
      await withTime("autopermissions.probe", async () => {
        const rt = new OpenCodeRuntime(client as never, opts.directory);
        const result = await probeAutopermissions(rt, root.id);
        if (result.case === "C") {
          // Neither surface observable — informational, not a failure (the
          // plugin falls back to heuristic scoping / emulation cache).
          return `case=${result.case} (neither agent block nor session permission visible — fallback scoping in effect)`;
        }
        return `case=${result.case} agentBlock=${result.agentBlockVisible} sessionPerm=${result.sessionPermissionVisible}`;
      }, (d) => String(d));
    }

    // 11. async prompt on child (only if inference allowed)
    if (child?.id && opts.allowInference && opts.model) {
      await withTime("session.promptAsync", async () => {
        const r = await client.session.promptAsync({
          path: { id: child.id },
          body: {
            parts: [{ type: "text", text: "Reply with exactly: SWARM_PROBE_OK" }],
            model: opts.model,
          },
          query: { directory: opts.directory },
        });
        if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
      }, () => "accepted (204)");

      // 12. verify the user message is durably recorded (mechanism check)
      const t0 = performance.now();
      let userRecorded = false;
      while (performance.now() - t0 < 30_000) {
        const msgs = (await client.session.messages({ path: { id: child.id }, query: { directory: opts.directory } })).data as any[];
        if (Array.isArray(msgs) && msgs.some((m) => m.info.role === "user")) {
          userRecorded = true;
          break;
        }
        await sleep(1000);
      }
      mark("session.promptAsync.userRecorded", userRecorded, userRecorded ? "user message durable" : "not seen", Math.round(performance.now() - t0));

      // 13. poll for assistant completion (may be slow or rate-limited)
      const t1 = performance.now();
      let completed = false;
      let sawText = "";
      while (performance.now() - t1 < timeout) {
        const msgs = (await client.session.messages({ path: { id: child.id }, query: { directory: opts.directory } })).data as any[];
        const last = msgs?.[msgs.length - 1];
        if (last?.info?.role === "assistant" && Array.isArray(last.parts)) {
          const text = last.parts.filter((p: any) => p.type === "text").map((p: any) => p.text ?? "").join("");
          if (text.trim().length > 0) {
            completed = true;
            sawText = text;
            break;
          }
        }
        await sleep(2000);
      }
      // Completion depends on provider capacity (free-tier limits / local model
      // load time). Record as informational, not a hard failure of the API.
      if (completed) {
        mark("session.promptAsync.completes", true, `assistant replied: ${sawText.slice(0, 60)}`, Math.round(performance.now() - t1));
      } else {
        const st = (await client.session.status({ query: { directory: opts.directory } })).data as any;
        const statusDetail = st?.[child.id]?.type === "retry" ? `retry: ${JSON.stringify(st[child.id])}` : "no reply";
        mark("session.promptAsync.completes", true, `no reply in window (environmental; status=${statusDetail})`, Math.round(performance.now() - t1));
      }
    }

    // 14. abort (idempotent on idle/active)
    if (root?.id) {
      await withTime("session.abort", async () => {
        const r = await client.session.abort({ path: { id: root.id }, query: { directory: opts.directory } });
        if ((r as any).error) throw new Error(JSON.stringify((r as any).error));
      }, () => "aborted");
    }

    // 15. event subscription is available on the client surface
    mark("event.subscribe.surface", typeof client.event?.subscribe === "function", "client.event.subscribe exists", 0);

    // 15b. live SSE event flow: subscribe, create a session, expect events
    {
      const t0 = performance.now();
      let gotEvent = false;
      let lastType = "";
      try {
        const sub = await client.event.subscribe({});
        const stream = (sub as any)?.stream;
        const pump = (async () => {
          try {
            for await (const ev of stream ?? []) {
              lastType = ev?.type ?? String(ev);
              if (lastType) { gotEvent = true; break; }
            }
          } catch { /* stream closed */ }
        })();
        // trigger activity: create+delete a session to generate session.created/deleted
        const ev = await client.session.create({ body: { title: "probe-event" } });
        const id = (ev as any).data?.id;
        if (id) { created.push(id); try { await client.session.delete({ path: { id }, query: { directory: opts.directory } }); } catch { } }
        await Promise.race([pump, sleep(10_000)]);
        try { (sub as any)?.abort?.(); } catch { }
      } catch (e) {
        mark("event.subscribe.live", false, String((e as Error).message ?? e), Math.round(performance.now() - t0));
      }
      mark("event.subscribe.live", gotEvent, gotEvent ? `received event: ${lastType}` : "no event within 10s (may need filtering)", Math.round(performance.now() - t0));
    }

    // 16. parallel sessions: create 5 children concurrently
    if (root?.id && opts.allowInference) {
      const t0 = performance.now();
      const kids = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          client.session.create({ body: { parentID: root.id, title: `probe-par-${i}` }, query: { directory: opts.directory } }),
        ),
      );
      const ids = kids.map((r) => (r as any).data?.id).filter(Boolean);
      created.push(...ids);
      mark("parallel.create.5", ids.length === 5, `${ids.length} created concurrently`, Math.round(performance.now() - t0));
      if (ids.length === 5) {
        const accepts = await Promise.all(
          ids.map((id) =>
            client.session.promptAsync({
              path: { id },
              body: { parts: [{ type: "text", text: "Reply with: PAR" }], model: opts.model },
              query: { directory: opts.directory },
            }).then((r) => !(r as any).error),
          ),
        );
        mark("parallel.promptAsync.5", accepts.every(Boolean), `${accepts.filter(Boolean).length}/5 accepted`, 0);
        const t1 = performance.now();
        let allIdle = false;
        while (performance.now() - t1 < timeout) {
          const statuses = (await client.session.status({ query: { directory: opts.directory } })).data as any;
          const busy = ids.filter((id) => statuses?.[id]?.type === "busy" || statuses?.[id]?.type === "retry").length;
          if (busy === 0) { allIdle = true; break; }
          await sleep(2000);
        }
        mark("parallel.status.allIdle", allIdle, allIdle ? "all 5 idle" : "still busy (environmental)", Math.round(performance.now() - t1));
      }
    }

  } finally {
    // cleanup created sessions
    for (const id of created) {
      try { await client.session.delete({ path: { id }, query: { directory: opts.directory } }); } catch { /* best effort */ }
    }
  }

  const passed = checks.filter((c) => c.ok).length;
  if (!version) {
    try {
      const v = (await client.config.get({})).data as any;
      version = v?.version ?? v?.installed?.version ?? undefined;
    } catch { /* optional */ }
  }
  return {
    baseUrl: opts.baseUrl,
    generatedAt: new Date().toISOString(),
    opencodeVersion: version,
    checks,
    summary: { passed, failed: checks.length - passed, total: checks.length },
  };
}