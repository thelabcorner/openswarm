import { createOpencodeClient } from "@opencode-ai/sdk";

const baseUrl = process.env.OPENCODE_URL ?? "http://127.0.0.1:8951";
const client = createOpencodeClient({ baseUrl });

type Step = { name: string; ok: boolean; detail: string | number | boolean };
const steps: Step[] = [];
const mark = (name: string, ok: boolean, detail: string | number | boolean = "") =>
  steps.push({ name, ok, detail });

let created: string | null = null;
let child: string | null = null;

try {
  // 1. list sessions — use the current working directory (or OPENSWARM_DIR
  // override) so the probe is portable and never embeds a machine-specific path.
  const probeDir = process.env.OPENSWARM_DIR ?? process.cwd();
  const list = await client.session.list({ directory: probeDir });
  const listData = list.data;
  mark("session.list", !!listData, Array.isArray(listData) ? `${listData.length} sessions` : String(!!listData));
  console.log("list result keys:", Object.keys(list), "has error:", !!list.error, "isErr:", list.data === undefined && !!list);

  // 2. create root session
  const createRes = await client.session.create({ body: { title: "probe-root" } });
  const root = createRes.data as any;
  mark("session.create", !!root, root?.id ?? "no id");
  console.log("root:", JSON.stringify(root));
  created = root?.id ?? null;

  // 3. create child session of root
  if (created) {
    const childRes = await client.session.create({ body: { parentID: created, title: "probe-child" } });
    child = childRes.data?.id ?? null;
    mark("session.create(parent)", !!child, child ?? "no id");
    console.log("child:", JSON.stringify(childRes.data));

    // 4. get parent session and inspect parentID
    const getRes = await client.session.get({ path: { id: created } });
    const got = getRes.data as any;
    mark("session.get", !!got, got?.title ?? "?");
    console.log("get:", JSON.stringify(got));

    // 5. list children
    const childrenRes = await client.session.children({ path: { id: created } });
    const kids = childrenRes.data as any;
    mark("session.children", Array.isArray(kids), Array.isArray(kids) ? `${kids.length}` : "n/a");
    console.log("children:", JSON.stringify(kids));
  }

  // 6. session.status
  const statusRes = await client.session.status({});
  const status = statusRes.data as any;
  mark("session.status", !!status, status ? `${Object.keys(status || {}).length} entries` : "none");
  console.log("status:", JSON.stringify(status) ?? "null");

  // 7. session messages (on child or root)
  const target = child ?? created;
  if (target) {
    const msgRes = await client.session.messages({ path: { id: target } });
    const msgs = msgRes.data as any;
    mark("session.messages", Array.isArray(msgs), Array.isArray(msgs) ? `${msgs.length} messages` : "n/a");
    console.log("messages:", JSON.stringify(msgs));

    // 8. abort (no-op on idle)
    const abortRes = await client.session.abort({ path: { id: target } });
    mark("session.abort", (abortRes as any).data !== undefined, `status=${(abortRes as any).status}`);
  }

  // 9. update title
  if (created) {
    const updRes = await client.session.update({ path: { id: created }, body: { title: "SWARM-ROOT-renamed" } });
    const upd = updRes.data as any;
    mark("session.update", !!upd, `data=${typeof upd}`);
    console.log("updated:", JSON.stringify(upd));
  }
} catch (e) {
  console.error("PROBE ERROR step-by-step:", steps);
  mark("uncaught", false, String((e as Error).message ?? e));
}

// cleanup
for (const id of [child, created]) {
  if (!id) continue;
  try { await client.session.delete({ path: { id } }); console.log("deleted:", id); } catch (e) { console.error("delete failed:", id, e); }
}

console.log("\n===== PROBE STEPS =====");
for (const s of steps) console.log(`${s.ok ? "PASS" : "FAIL"} ${s.name} :: ${s.detail}`);
const failures = steps.filter((s) => !s.ok).length;
console.log(`\nTOTAL: ${steps.length}, failures: ${failures}`);
process.exit(failures > 0 ? 2 : 0);