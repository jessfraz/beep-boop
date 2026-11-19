import { spawn } from "node:child_process";
import { readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const directory = new URL("../dist/test/", import.meta.url);
const browser = "browser-host.test.js";
const files = (await readdir(directory)).filter(name => name.endsWith(".test.js")).sort();
if (!files.includes(browser)) throw new Error("Build the runtime tests before running them");

// Real Chrome keeps its concurrent-owner assertions and deadlines, without
// competing against other native-heavy test files for the runner's resources.
for (const batch of [files.filter(name => name !== browser), [browser]]) {
  if (batch.length === 0) continue;
  const child = spawn(process.execPath, ["--test", ...process.argv.slice(2),
    ...batch.map(name => fileURLToPath(new URL(name, directory)))], { stdio: "inherit" });
  let interrupted: NodeJS.Signals | undefined;
  const interrupt = () => { interrupted = "SIGINT"; child.kill("SIGINT"); };
  const terminate = () => { interrupted = "SIGTERM"; child.kill("SIGTERM"); };
  process.on("SIGINT", interrupt); process.on("SIGTERM", terminate);
  let outcome: { code: number | null; signal: NodeJS.Signals | null };
  try {
    outcome = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal }));
    });
  } finally {
    process.off("SIGINT", interrupt); process.off("SIGTERM", terminate);
  }
  const signal = interrupted ?? outcome.signal;
  if (signal !== null) { process.kill(process.pid, signal); break; }
  if (outcome.code !== 0) { process.exitCode = outcome.code ?? 1; break; }
}
