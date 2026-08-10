import { runCompatibilityProbe } from "./compat.js";

const url = process.env.OPENCODE_URL ?? "http://127.0.0.1:8951";
const dir = process.env.OPENCODE_DIRECTORY ?? process.cwd();
const allowInference = process.env.OPENCODE_PROBE_INFERENCE === "1";

const report = await runCompatibilityProbe({
  baseUrl: url,
  directory: dir,
  allowInference,
  model: allowInference
    ? {
        providerID: process.env.OPENCODE_PROBE_PROVIDER ?? "opencode",
        modelID: process.env.OPENCODE_PROBE_MODEL ?? "deepseek-v4-flash-free",
      }
    : undefined,
  timeoutMs: Number(process.env.OPENCODE_PROBE_TIMEOUT ?? 120_000),
});

await Bun.write("probe-report.json", JSON.stringify(report, null, 2));

console.log(`OpenCode probe ${report.baseUrl}`);
console.log(`version: ${report.opencodeVersion ?? "unknown"}`);
for (const c of report.checks) {
  console.log(`${c.ok ? "PASS" : "FAIL"}  ${c.name}  (${c.durationMs}ms)  ${c.detail}`);
}
console.log(`\nsummary: ${report.summary.passed}/${report.summary.total} passed, ${report.summary.failed} failed`);
console.log("report written to probe-report.json");

process.exit(report.summary.failed > 0 ? 2 : 0);