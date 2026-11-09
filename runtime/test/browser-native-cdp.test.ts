import assert from "node:assert/strict";
import { execFile, execFileSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { setTimeout } from "node:timers/promises";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { chromium, type Browser } from "playwright-core";
import type { BrowserAction } from "../src/generated/BrowserAction.js";
import type { BrowserReply } from "../src/generated/BrowserReply.js";
import type { BrowserRequest } from "../src/generated/BrowserRequest.js";

const chrome = process.env.AGENT_TEST_CHROME;
const skip = chrome === undefined && process.env.CI !== "true";

function object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function groupAlive(group: number): Promise<boolean> {
  try { process.kill(-group, 0); return true; }
  catch (error) {
    if (object(error) && error.code === "ESRCH") return false;
    if (!object(error) || error.code !== "EPERM") throw error;
    // Darwin's killpg filters zombies and returns EPERM when none remain
    // signalable. A denied probe does not mean the group is gone: keep waiting
    // for live members, leaving actual signal permission failures authoritative.
    const { stdout } = await promisify(execFile)("/bin/ps", ["-ax", "-o", "pgid=,stat="]);
    for (const line of stdout.split("\n").filter(line => line.trim() !== "")) {
      const fields = line.trim().split(/\s+/);
      assert.ok(fields.length === 2 && /^\d+$/.test(fields[0]!), "Native group inspection must provide group and state");
      if (fields[0] === String(group) && !fields[1]!.startsWith("Z")) return true;
    }
    return false;
  }
}

test("owned Chrome group cleanup recognizes exited native children before their parent reaps them", { timeout: 10_000 }, async () => {
  const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore", detached: true });
  const exited = new Promise<void>((resolve, reject) => { child.once("close", () => resolve()); child.once("error", reject); });
  const group = child.pid; assert.ok(group !== undefined && group > 0);
  try {
    // Synchronous process inspection leaves this owned child unreaped until the
    // assertion. No sleep or scheduler-dependent SIGCHLD race is needed.
    const deadline = performance.now() + 3_000;
    let status = "";
    while (performance.now() < deadline) {
      status = execFileSync("/bin/ps", ["-p", String(group), "-o", "stat="], { encoding: "utf8" }).trim();
      if (status.startsWith("Z")) break;
    }
    assert.ok(status.startsWith("Z"), "The exact fixture child must have exited but remain unreaped");
    if (process.platform === "darwin") {
      assert.throws(() => process.kill(-group, 0), error => object(error) && error.code === "EPERM",
        "Darwin's zombie-only process group reports EPERM rather than ESRCH");
      assert.equal(await groupAlive(group), false, "An exited child cannot retain profile writers");
    }
    await exited;
    assert.equal(await groupAlive(group), false);
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await exited;
  }
});

test("a permission-limited native group probe preserves live membership", {
  skip: process.platform !== "darwin" || process.getuid?.() === 0,
}, async () => {
  // Probe only: signal 0 and ps cannot terminate or modify foreign processes.
  // A protected system group supplies the real EPERM boundary without mocks.
  const { stdout } = await promisify(execFile)("/bin/ps", ["-ax", "-o", "pgid=,stat="]);
  const groups = [...new Set(stdout.split("\n").flatMap(line => {
    const [pgid, state] = line.trim().split(/\s+/);
    const group = Number(pgid);
    return Number.isInteger(group) && group > 1 && state !== undefined && !state.startsWith("Z") ? [group] : [];
  }))].sort((left, right) => left - right);
  const group = groups.find(candidate => {
    try { process.kill(-candidate, 0); return false; }
    catch (error) { return object(error) && error.code === "EPERM"; }
  });
  assert.ok(group !== undefined, "Darwin must expose a live permission-protected process group");
  assert.equal(await groupAlive(group), true, "A denied existence probe cannot imply exit or abort the drain wait");
});

function reply(value: unknown): value is BrowserReply {
  if (!object(value) || typeof value.operation_id !== "string" || !object(value.response)) return false;
  const response = value.response;
  if (typeof response.session_id !== "string") return false;
  if (["failed", "disconnected", "uncertain"].includes(String(response.type))) return typeof response.message === "string";
  if (response.type === "user_tabs") return Array.isArray(response.tabs) && response.tabs.every((tab: unknown) =>
    object(tab) && typeof tab.target_id === "string" && typeof tab.url === "string" && typeof tab.title === "string");
  return (response.type === "closed" || response.type === "ready" && typeof response.is_error === "boolean")
    && typeof response.output === "string" && Array.isArray(response.tabs)
    && response.tabs.every((tab: unknown) => object(tab) && typeof tab.id === "number"
      && typeof tab.url === "string" && typeof tab.title === "string");
}

/** Exercises the shipped helper entry point and its actual configuration consumer. */
class NativeHelper {
  private readonly child;
  private readonly exited: Promise<void>;
  private readonly lines;
  constructor(port: number, broker = process.env.AGENT_TEST_BROWSER_BROKER ?? fileURLToPath(new URL("../../../target/debug/examples/browser_fixture", import.meta.url))) {
    const runtime = process.env.AGENT_TEST_BROWSER_RUNTIME ?? fileURLToPath(new URL("../src/bridge.js", import.meta.url));
    this.child = spawn(broker, [runtime,
      JSON.stringify({ user_data_dir: null, profile_dir_name: null, debugging_port: port, credential_command: null })],
      { stdio: ["pipe", "pipe", "ignore"], env: { ...process.env, PLAYWRIGHT_MCP_EXTENSION_TOKEN: "" } });
    this.exited = new Promise(resolve => this.child.once("close", () => resolve()));
    this.lines = createInterface({ input: this.child.stdout, crlfDelay: Infinity })[Symbol.asyncIterator]();
  }
  async execute(request: BrowserRequest): Promise<BrowserReply> {
    this.child.stdin.write(JSON.stringify(request) + "\n");
    const timer = new AbortController();
    try {
      const line = await Promise.race([this.lines.next(), setTimeout(35_000, undefined, { signal: timer.signal })
        .then(() => { throw new Error("Native helper did not confirm its request"); })]);
      assert.equal(line.done, false, "Native helper closed before replying to " + request.action.type);
      const value: unknown = JSON.parse(line.value);
      assert.ok(reply(value));
      assert.equal(value.operation_id, request.operation_id);
      assert.equal(value.response.session_id, request.session_id);
      return value;
    } finally { timer.abort(); }
  }
  async close(): Promise<void> {
    this.child.stdin.end();
    if (await Promise.race([this.exited.then(() => true), setTimeout(5000, false)])) return;
    this.child.kill("SIGTERM");
    if (await Promise.race([this.exited.then(() => true), setTimeout(5000, false)])) return;
    this.child.kill("SIGKILL"); await this.exited;
  }
}

test("native CDP keeps full Playwright control and exact ownership despite a stalled existing renderer",
  { skip, timeout: 45_000 }, async t => {
  assert.ok(chrome, "AGENT_TEST_CHROME must select an explicit disposable test binary");
  const directory = await mkdtemp(join(tmpdir(), "agent-native-cdp-"));
  const profile = join(directory, "profile");
  let stopChrome: (() => Promise<void>) | undefined;
  let stopServer: (() => Promise<void>) | undefined;
  t.after(async () => {
    const failures: unknown[] = [];
    try { await stopChrome?.(); } catch (error) { failures.push(error); }
    try { await stopServer?.(); } catch (error) { failures.push(error); }
    // Retain the disposable profile and ownership diagnostics if reaping failed.
    if (failures.length) throw new AggregateError(failures, `Fixture cleanup incomplete: ${directory}`);
    await rm(directory, { recursive: true, force: true });
    t.diagnostic("Disposable profile removed after owned Chrome group cleanup");
  });
  const version = await promisify(execFile)(chrome, ["--version", `--user-data-dir=${profile}`,
    "--use-mock-keychain", "--password-store=basic"], { timeout: 5_000 });
  assert.match(version.stdout, /^Google Chrome for Testing /, "Only disposable Chrome for Testing may run this fixture");
  const extension = join(directory, "inline-menu"); await mkdir(extension);
  await writeFile(join(extension, "manifest.json"), JSON.stringify({ manifest_version: 3,
    name: "Invented inline menu", version: "1.0",
    content_scripts: [{ matches: ["http://127.0.0.1/*"], js: ["inject.js"], run_at: "document_start" }],
    web_accessible_resources: [{ resources: ["menu.html"], matches: ["http://127.0.0.1/*"] }] }));
  await writeFile(join(extension, "menu.html"), "<!doctype html><p>Invented extension menu</p>");
  await writeFile(join(extension, "inject.js"), `let inserted = false;
document.addEventListener('focusin', event => {
  if (inserted || !(event.target instanceof HTMLInputElement)) return;
  inserted = true;
  const host = document.createElement('fixture-inline-menu');
  const frame = document.createElement('iframe'); frame.src = chrome.runtime.getURL('menu.html');
  host.attachShadow({mode:'closed'}).append(frame); document.documentElement.append(host);
  queueMicrotask(() => frame.focus());
});`);
  const inventedPassword = "Invented-native-password-42";
  const credentialCli = join(directory, "credential-cli.mjs");
  await writeFile(credentialCli, "import assert from 'node:assert/strict';\n"
    + "assert.deepEqual(process.argv.slice(2), ['item','get','invented-login','--format','json']);\n"
    + "process.stdout.write(" + JSON.stringify(JSON.stringify({ fields: [
      { id: "username", value: "Native form value" }, { id: "password", value: inventedPassword }
    ] })) + ");");
  let submissions = 0;
  let accountServed = false;
  let finishLatePopup: (() => void) | undefined;
  const popupEffects: string[] = [];
  const server = createServer((request, response) => {
    response.setHeader("Content-Type", "text/html; charset=utf-8");
    if (request.url === "/account") {
      if (!request.headers.cookie?.includes("synthetic_login=present")) { response.writeHead(401).end("Missing login"); return; }
      if (accountServed) { response.writeHead(429).end("<!doctype html><h1>Fresh navigation unavailable</h1>"); return; }
      accountServed = true;
      response.end(`<!doctype html><title>Signed-in account</title><h1>Hi, Example!</h1>
        <label>Draft<input value="Unsent fixture draft"></label>
        <label>Saved password<input type="password" value="Invented-existing-secret-42"></label>
        <button onclick="document.querySelector('output').textContent='Tracking: FIXTURE12345'">Track package</button><output></output>`);
    } else if (request.url === "/orphan-account") {
      response.end(`<!doctype html><title>Recoverable account</title>
        <label>Draft<input value="Unsent orphan draft"></label>
        <label>Password<input type="password" value="Invented-orphan-secret"></label>`);
    } else if (request.url === "/late-owned") {
      response.end(`<!doctype html><title>Late popup</title><h1>Untrusted popup</h1>
        <label>Popup password<input type="password" value="Invented-popup-secret"></label><output>Waiting for capture</output>
        <script>const timer = setInterval(() => {
          const name = Object.getOwnPropertyNames(globalThis).find(name => name.startsWith('agentCapture_'));
          if (name && Reflect.get(globalThis, name)?.active === true) {
            Reflect.deleteProperty(globalThis, Symbol.for('beep-boop.browser-control')); clearInterval(timer);
            document.querySelector('output').textContent = 'Ownership tamper attempted';
          }
        }, 10);</script>`);
    } else if (request.url?.startsWith("/popup-effect?")) {
      popupEffects.push(new URL(request.url, origin).searchParams.get("owner") ?? ""); response.end("Recorded");
    } else if (request.url === "/late-popup-gate") {
      finishLatePopup = () => response.end("Ready");
    } else if (request.url === "/seed") {
      response.setHeader("Set-Cookie", "synthetic_login=present; HttpOnly; Path=/; SameSite=Lax");
      response.end("<!doctype html><h1>User tab sentinel</h1>");
    } else if (request.url === "/login") {
      if (!request.headers.cookie?.includes("synthetic_login=present")) { response.writeHead(401).end("Missing login"); return; }
      response.end(`<!doctype html><title>Owned login</title><form method="post" action="/submit" target="result">
        <label>Name<input name="name"></label><label>Password<input type="password" name="password"></label>
        <button>Save</button></form><iframe name="result" title="Result" src="/frame"></iframe>`);
    } else if (request.url === "/frame") {
      response.end('<!doctype html><label>Frame note<input name="note"></label>');
    } else if (request.url === "/submit") {
      let body = "";
      request.setEncoding("utf8"); request.on("data", (chunk: string) => { body += chunk; });
      request.on("end", () => {
        const form = new URLSearchParams(body);
        if (form.get("name") !== "Native form value" || form.get("password") !== inventedPassword) {
          response.writeHead(400).end("Unexpected fixture form"); return;
        }
        submissions++;
        response.end("<!doctype html><h1>Saved</h1>");
      });
    } else response.end("<!doctype html><h1>Unrelated page</h1>");
  });
  stopServer = () => {
    server.closeAllConnections();
    return new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  };
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address(); assert.ok(address !== null && typeof address !== "string");
  const origin = `http://127.0.0.1:${address.port}`;
  const args = [`--user-data-dir=${profile}`, "--remote-debugging-port=0", "--headless=new",
    `--load-extension=${extension}`, `--disable-extensions-except=${extension}`, "--use-mock-keychain",
    "--password-store=basic", "--disable-background-networking", "--no-first-run", "--no-default-browser-check", "about:blank"];
  if (process.platform === "linux" || process.getuid?.() === 0) args.unshift("--no-sandbox");
  const processChrome = spawn(chrome, args, { stdio: "ignore", detached: true });
  processChrome.once("error", () => {});
  const exited = new Promise<void>(resolve => processChrome.once("close", () => resolve()));
  let observer: Browser | undefined;
  let recovering: Browser | undefined;
  const hosts: NativeHelper[] = [];
  const nativeTargets = new Set<string>();
  const group = processChrome.pid;
  assert.ok(group !== undefined && group > 0, "The fixture must own an explicit detached Chrome group");
  const signalGroup = async (signal: NodeJS.Signals) => {
    try { process.kill(-group, signal); }
    catch (error) {
      if (object(error) && error.code === "ESRCH") return;
      if (object(error) && error.code === "EPERM" && !await groupAlive(group)) return;
      throw error;
    }
  };
  stopChrome = async () => {
    const failures: unknown[] = [];
    for (const host of hosts) {
      try { await host.close(); } catch (error) { failures.push(error); }
    }
    // The browser's main process may already be gone while its renderer group
    // remains. Reap the group allocated by this spawn, never a discovered port.
    await signalGroup("SIGTERM");
    let deadline = Date.now() + 3_000;
    while (await groupAlive(group) && Date.now() < deadline) await setTimeout(25);
    if (await groupAlive(group)) await signalGroup("SIGKILL");
    deadline = Date.now() + 3_000;
    while (await groupAlive(group) && Date.now() < deadline) await setTimeout(25);
    const empty = !await groupAlive(group);
    t.diagnostic(JSON.stringify({ fixturePid: group, fixtureGroup: group, nativeTargets: [...nativeTargets], groupEmpty: empty }));
    assert.ok(empty, "Every process in the explicitly owned Chrome group must exit before profile removal");
    await exited;
    try { await observer?.close(); } catch (error) { failures.push(error); }
    try { await recovering?.close(); } catch (error) { failures.push(error); }
    if (failures.length) throw new AggregateError(failures, "Owned browser helper cleanup failed");
  };
  let port = 0;
  for (let attempt = 0; attempt < 200; attempt++) {
    assert.equal(processChrome.exitCode, null, "Disposable Chrome must remain running");
    try {
      const first = (await readFile(join(profile, "DevToolsActivePort"), "utf8")).split("\n")[0];
      const parsed = Number(first);
      if (Number.isInteger(parsed) && parsed > 0 && parsed <= 65535) { port = parsed; break; }
    } catch { /* Wait only for the exact disposable profile's endpoint. */ }
    await setTimeout(50);
  }
  assert.notEqual(port, 0, "Disposable Chrome must publish its native endpoint");
  observer = await chromium.connectOverCDP(`http://127.0.0.1:${port}`, { noDefaults: true, timeout: 10_000 });
  const context = observer.contexts()[0]; assert.ok(context);
  const browserSession = await observer.newBrowserCDPSession();
  browserSession.on("Target.targetCreated", ({ targetInfo }: { targetInfo: { targetId: string } }) => {
    nativeTargets.add(targetInfo.targetId);
  });
  await browserSession.send("Target.setDiscoverTargets", { discover: true });
  const currentTargets = async () => {
    const targets = await browserSession.send("Target.getTargets");
    for (const target of targets.targetInfos) nativeTargets.add(target.targetId);
    return new Set(targets.targetInfos.map(target => target.targetId));
  };
  await currentTargets();
  const sentinel = await context.newPage(); await sentinel.goto(`${origin}/seed`);
  const sentinelSession = await context.newCDPSession(sentinel);
  const sentinelId = (await sentinelSession.send("Target.getTargetInfo")).targetInfo.targetId;
  nativeTargets.add(sentinelId); await sentinelSession.detach();
  // A separate site gives the stalled user tab its own renderer. Existing
  // renderer initialization must not gate the new task's unrelated web page.
  const stalled = await context.newPage();
  await stalled.goto(`http://localhost:${address.port}/stalled`);
  const stalledSession = await context.newCDPSession(stalled);
  const stalledId = (await stalledSession.send("Target.getTargetInfo")).targetInfo.targetId;
  nativeTargets.add(stalledId);
  void stalledSession.send("Runtime.evaluate", { expression: "while(true){}" }).catch(() => {});
  const frameTree = stalledSession.send("Page.getFrameTree");
  const frameTreeState = await Promise.race([
    frameTree.then(() => "resolved", () => "rejected"), setTimeout(250, "pending"),
  ]);
  assert.equal(frameTreeState, "pending", "The pre-existing renderer must actually stall Page.getFrameTree");
  await assert.rejects(
    chromium.connectOverCDP(`http://127.0.0.1:${port}`, { noDefaults: false, timeout: 500 }),
    error => error instanceof Error && error.name === "TimeoutError",
    "The normal initialization barrier must remain in effect without noDefaults",
  );
  await currentTargets();
  const initialPages = context.pages().length;
  // Separate helpers reconnect to the same browser without extension pairing.
  for (let helper = 0; helper < 2; helper++) {
    const host = new NativeHelper(port);
    hosts.push(host);
    const owner_id = randomUUID(), session_id = randomUUID();
    const execute = async (action: BrowserAction) => (await host.execute({ owner_id, session_id, operation_id: randomUUID(), action })).response;
    const opened = await execute({ type: "open", url: `${origin}/login` });
    assert.ok(opened.type === "ready", JSON.stringify(opened));
    await currentTargets();
    assert.equal(opened.tabs.length, 1); const tab = opened.tabs[0]; assert.ok(tab);
    const status = await execute({ type: "status" });
    assert.ok(status.type === "ready", JSON.stringify(status));
    assert.equal(status.output.includes(inventedPassword), false);
    const filled = await execute({ type: "run_code", tab_id: tab.id, code: `async page => {
      const { execFile } = await import('node:child_process');
      const { promisify } = await import('node:util');
      const { execPath } = await import('node:process');
      const { stdout } = await promisify(execFile)(execPath,
        [${JSON.stringify(credentialCli)},'item','get','invented-login','--format','json'], {timeout:5000});
      const { fields } = JSON.parse(stdout);
      const username = fields.find(field => field.id === 'username').value;
      const password = fields.find(field => field.id === 'password').value;
      // Let the inline menu take focus before inserting credentials.
      const attached = page.waitForEvent('framenavigated', {predicate: frame => frame.url().startsWith('chrome-extension://'), timeout:5000});
      await page.getByLabel('Name', {exact:true}).focus();
      const foreign = await attached;
      if (!foreign || foreign.isDetached()) throw new Error('Foreign extension frame must remain attached');
      await foreign.waitForLoadState('domcontentloaded');
      await page.getByLabel('Name', {exact:true}).fill(username);
      const ordinary = page.frameLocator('iframe[title="Result"]');
      await ordinary.getByLabel('Frame note').fill('Ordinary frame works');
      await page.getByLabel('Password', {exact:true}).fill(password);
      if (await page.getByLabel('Name', {exact:true}).inputValue() !== username) throw new Error('Name field did not retain the filled credential');
      if (await page.getByLabel('Password', {exact:true}).inputValue() !== password) throw new Error('Password field did not retain the filled credential');
      console.log(password);
      return { foreignFramePresent: !foreign.isDetached(), ordinaryFrameValue: await ordinary.getByLabel('Frame note').inputValue(), password };
    }` });
    assert.ok(filled.type === "ready", JSON.stringify(filled));
    assert.equal(filled.output.includes(inventedPassword), false, "A filled password must not enter tool output");
    assert.ok(filled.output.includes('"foreignFramePresent":true'));
    assert.ok(filled.output.includes("Ordinary frame works"));
    const submitted = await execute({ type: "run_code", tab_id: tab.id, code: `async page => {
      const foreign = page.frames().find(frame => frame.url().startsWith('chrome-extension://'));
      await page.getByRole('button', {name:'Save',exact:true}).click();
      await page.frameLocator('iframe[title="Result"]').getByRole('heading', {name:'Saved',exact:true}).waitFor();
      return { foreignFramePresent: !!foreign && !foreign.isDetached() };
    }` });
    assert.ok(submitted.type === "ready", JSON.stringify(submitted));
    assert.equal(submitted.output.includes(inventedPassword), false);
    assert.ok(submitted.output.includes('"foreignFramePresent":true'));
    assert.equal(submissions, helper + 1, "The real server must receive exactly one correct submission per helper");
    const released = await execute({ type: "release" });
    assert.equal(released.type, "closed", JSON.stringify(released));
    assert.equal(context.pages().length, initialPages, "Release must close every exact owned page");
    assert.equal(sentinel.isClosed(), false);
    assert.equal(stalled.isClosed(), false);
    const remainingTargets = await currentTargets();
    assert.ok(remainingTargets.has(sentinelId), "Release must preserve the exact pre-existing sentinel target");
    assert.ok(remainingTargets.has(stalledId), "Release must preserve the exact pre-existing stalled target");
    assert.equal(processChrome.exitCode, null, "Disconnect must preserve the user's Chrome process");
    assert.equal(processChrome.signalCode, null);
  }
  // A connection established while the user page is stalled must retain full
  // binding support when that same renderer recovers later.
  recovering = await chromium.connectOverCDP(`http://127.0.0.1:${port}`, { noDefaults: true, timeout: 5_000 });
  const recoveringContext = recovering.contexts()[0]; assert.ok(recoveringContext);
  const recoveredPage = recoveringContext.waitForEvent("page", {
    predicate: page => page.url() === `http://localhost:${address.port}/stalled`, timeout: 5_000,
  });
  await stalledSession.send("Runtime.terminateExecution");
  await frameTree;
  const recovered = await recoveredPage;
  const recoveredSession = await recoveringContext.newCDPSession(recovered);
  assert.equal((await recoveredSession.send("Target.getTargetInfo")).targetInfo.targetId, stalledId);
  await recoveredSession.detach();
  await recovered.exposeBinding("fixtureRecovered", (_source, value: unknown) => value);
  assert.equal(await recovered.evaluate("window.fixtureRecovered('Recovered binding')"), "Recovered binding");
  await stalledSession.detach();

  await t.test("native conversations recover an existing signed-in tab without owning its draft", async () => {
    const account = await context.newPage(); await account.goto(`${origin}/account`);
    const setter = await account.evaluateHandle("Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set");
    const executable = process.env.AGENT_TEST_BROWSER_BROKER ?? fileURLToPath(new URL("../../../target/debug/examples/browser_fixture", import.meta.url));
    const host = new NativeHelper(port, executable); hosts.push(host);
    const first = { owner_id: randomUUID(), session_id: randomUUID() };
    const second = { owner_id: randomUUID(), session_id: randomUUID() };
    const execute = async (owner: typeof first, action: BrowserAction) => (await host.execute({ ...owner, operation_id: randomUUID(), action })).response;
    const fresh = await execute(first, { type: "open", url: `${origin}/account` });
    assert.ok(fresh.type === "ready", JSON.stringify(fresh)); assert.ok(fresh.output.includes("Fresh navigation unavailable"));
    const disposable = fresh.tabs[0]; assert.ok(disposable);
    const inventory = await execute(first, { type: "list_user_tabs" });
    assert.ok(inventory.type === "user_tabs", JSON.stringify(inventory));
    const original = inventory.tabs.find(tab => tab.url === `${origin}/account` && tab.title === "Signed-in account"); assert.ok(original);
    await account.evaluate("document.title = 'Updated signed-in account'");
    assert.equal((await execute(first, { type: "claim_user_tab", tab: original })).type, "failed", "Changed user-tab identities must fail closed");
    const current = await execute(first, { type: "list_user_tabs" }); assert.ok(current.type === "user_tabs");
    const selected = current.tabs.find(tab => tab.target_id === original.target_id); assert.ok(selected);
    const competing = await execute(second, { type: "list_user_tabs" }); assert.ok(competing.type === "user_tabs");
    assert.deepEqual(competing.tabs.filter(tab => tab.url === `${origin}/account`), [selected], "Another connection's disposable tab is not a user tab");
    const claimed = await execute(first, { type: "claim_user_tab", tab: selected });
    assert.ok(claimed.type === "ready", JSON.stringify(claimed));
    assert.equal(claimed.output.includes("Invented-existing-secret-42"), false);
    const borrowed = claimed.tabs.find(tab => tab.id !== disposable.id); assert.ok(borrowed);
    assert.equal((await execute(second, { type: "claim_user_tab", tab: selected })).type, "failed", "Two conversations cannot control the same user tab");
    const tracked = await execute(first, { type: "run_code", tab_id: borrowed.id, code: `async page => {
      await page.getByRole('button', {name:'Track package',exact:true}).click();
      return {tracking:await page.locator('output').innerText(),password:await page.getByLabel('Saved password').inputValue()};
    }` });
    assert.ok(tracked.type === "ready", JSON.stringify(tracked));
    const output: unknown = JSON.parse(tracked.output); assert.ok(object(output));
    assert.deepEqual(output.result, { tracking: "Tracking: FIXTURE12345", password: "[redacted]" });
    assert.equal(tracked.output.includes("Unsent fixture draft"), false);
    const failed = await execute(first, { type: "run_code", tab_id: borrowed.id, code: "async () => { throw new Error('Invented-existing-secret-42'); }" });
    assert.equal(failed.type, "uncertain"); assert.equal(JSON.stringify(failed).includes("Invented-existing-secret-42"), false);
    assert.equal((await execute(second, { type: "claim_user_tab", tab: selected })).type, "failed", "Uncertainty cannot release the existing claim");
    assert.equal((await execute(first, { type: "release" })).type, "closed");
    assert.equal(account.isClosed(), false); assert.equal(sentinel.isClosed(), false);
    assert.equal(await account.getByLabel("Draft").inputValue(), "Unsent fixture draft");
    assert.equal(await account.getByLabel("Saved password").inputValue(), "Invented-existing-secret-42");
    assert.equal(await account.getByLabel("Draft").evaluate((input: object, previous: unknown) =>
      Object.getOwnPropertyDescriptor(Object.getPrototypeOf(input), "value")?.set === previous, setter), true);
    assert.equal(await account.evaluate("Object.getOwnPropertyNames(globalThis).some(name => name.startsWith('agentPassword_') || name.startsWith('agentCapture_'))"), false,
      "Release must remove capture bindings and scripts from borrowed pages");
    assert.equal(await account.evaluate("Reflect.has(globalThis, Symbol.for('beep-boop.browser-control'))"), false);
    assert.equal(context.pages().some(page => page !== account && page.url() === `${origin}/account`), false, "Disposable tabs must close");
    const reclaimed = await execute(second, { type: "claim_user_tab", tab: selected });
    assert.ok(reclaimed.type === "ready", JSON.stringify(reclaimed));
    const userTab = reclaimed.tabs[0]; assert.ok(userTab);
    assert.equal((await execute(second, { type: "close_tab", tab_id: userTab.id })).type, "failed");
    assert.equal((await execute(second, { type: "release" })).type, "closed");
    assert.equal(account.isClosed(), false); await setter.dispose();
  });

  await t.test("an exited helper releases its claims without closing the borrowed user page", async () => {
    const account = await context.newPage(); await account.goto(`${origin}/orphan-account`);
    const host = new NativeHelper(port); hosts.push(host);
    const first = { owner_id: randomUUID(), session_id: randomUUID() };
    const second = { owner_id: randomUUID(), session_id: randomUUID() };
    const third = { owner_id: randomUUID(), session_id: randomUUID() };
    const execute = async (owner: typeof first, action: BrowserAction) => (await host.execute({ ...owner, operation_id: randomUUID(), action })).response;
    const listed = await execute(first, { type: "list_user_tabs" }); assert.ok(listed.type === "user_tabs");
    const target = listed.tabs.find(tab => tab.url === `${origin}/orphan-account`); assert.ok(target);
    const claimed = await execute(first, { type: "claim_user_tab", tab: target }); assert.ok(claimed.type === "ready");
    const tab = claimed.tabs[0]; assert.ok(tab);
    const receipt = await execute(first, { type: "run_code", tab_id: tab.id,
      code: "async () => ({helperPid:(await import('node:process')).default.pid})" });
    assert.ok(receipt.type === "ready", JSON.stringify(receipt));
    const result: unknown = JSON.parse(receipt.output);
    assert.ok(object(result) && object(result.result) && typeof result.result.helperPid === "number"
      && Number.isInteger(result.result.helperPid) && result.result.helperPid > 1);
    const helperPid = result.result.helperPid;
    process.kill(helperPid, "SIGKILL");
    let exited = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      try {
        const state = execFileSync("/bin/ps", ["-p", String(helperPid), "-o", "stat="], { encoding: "utf8" }).trim();
        exited = state.startsWith("Z");
      } catch (error) {
        if (!object(error) || error.status !== 1) throw error;
        exited = true;
      }
      if (exited) break;
      await setTimeout(10);
    }
    assert.equal(exited, true, "The exact owned helper must have terminated");
    let available = await execute(second, { type: "list_user_tabs" }); assert.ok(available.type === "user_tabs");
    for (let attempt = 0; attempt < 20 && !available.tabs.some(tab => tab.target_id === target.target_id); attempt++) {
      await setTimeout(10);
      available = await execute(second, { type: "list_user_tabs" }); assert.ok(available.type === "user_tabs");
    }
    assert.ok(available.tabs.some(tab => tab.target_id === target.target_id), "A proven helper exit must release the preserved page without another call to the dead owner");
    const recovered = await execute(second, { type: "claim_user_tab", tab: target }); assert.ok(recovered.type === "ready", JSON.stringify(recovered));
    const recoveredTab = recovered.tabs[0]; assert.ok(recoveredTab);
    const observed = await execute(second, { type: "run_code", tab_id: recoveredTab.id,
      code: "async page => ({draft:await page.getByLabel('Draft').inputValue(),password:await page.getByLabel('Password').inputValue()})" });
    assert.ok(observed.type === "ready"); assert.equal(observed.output.includes("Invented-orphan-secret"), false);
    const observedResult: unknown = JSON.parse(observed.output); assert.ok(object(observedResult));
    assert.deepEqual(observedResult.result, { draft: "Unsent orphan draft", password: "[redacted]" });
    const stale = await execute(first, { type: "status" }); assert.ok(stale.type === "uncertain" || stale.type === "disconnected");
    const competing = await execute(third, { type: "list_user_tabs" }); assert.ok(competing.type === "user_tabs");
    assert.equal(competing.tabs.some(tab => tab.target_id === target.target_id), false, "Dead-session cleanup cannot release the replacement owner's claim");
    assert.equal((await execute(second, { type: "release" })).type, "closed");
    assert.equal((await execute(third, { type: "release" })).type, "closed");
    assert.equal(account.isClosed(), false); assert.equal(await account.getByLabel("Draft").inputValue(), "Unsent orphan draft");
  });

  await t.test("a late native popup stays exclusive despite page tampering before its opener's next receipt", async () => {
    const executable = process.env.AGENT_TEST_BROWSER_BROKER ?? fileURLToPath(new URL("../../../target/debug/examples/browser_fixture", import.meta.url));
    const host = new NativeHelper(port, executable); hosts.push(host);
    const first = { owner_id: randomUUID(), session_id: randomUUID() };
    const second = { owner_id: randomUUID(), session_id: randomUUID() };
    const execute = async (owner: typeof first, action: BrowserAction) => (await host.execute({ ...owner, operation_id: randomUUID(), action })).response;
    const opened = await execute(first, { type: "open", url: `${origin}/late-opener` }); assert.ok(opened.type === "ready");
    const opener = opened.tabs[0]; assert.ok(opener);
    const armed = await execute(first, { type: "run_code", tab_id: opener.id, code: `async page => {
      await page.evaluate(() => {
        const button = document.createElement('button'); button.textContent = 'Schedule popup';
        button.onclick = () => { fetch('/late-popup-gate').then(() => window.open('/late-owned')); };
        document.body.append(button);
      });
      await page.getByRole('button', {name:'Schedule popup'}).click(); return {armed:true};
    }` });
    assert.ok(armed.type === "ready");
    for (let attempt = 0; finishLatePopup === undefined && attempt < 100; attempt++) await setTimeout(10);
    assert.ok(finishLatePopup);
    const created = context.waitForEvent("page", { timeout: 5000 }); finishLatePopup();
    const popup = await created; await popup.waitForLoadState("domcontentloaded");
    await popup.getByText("Ownership tamper attempted", { exact: true }).waitFor();
    const listed = await execute(second, { type: "list_user_tabs" }); assert.ok(listed.type === "user_tabs");
    assert.equal(listed.tabs.some(tab => tab.url === `${origin}/late-owned`), false, "The host excludes late owned targets before another helper can claim them");
    const native = (await browserSession.send("Target.getTargets")).targetInfos.find(tab => tab.url === `${origin}/late-owned`); assert.ok(native);
    const target = { target_id: native.targetId, url: native.url, title: native.title };
    assert.equal((await execute(second, { type: "claim_user_tab", tab: target })).type, "failed",
      "Untrusted page JavaScript cannot release the native owner's lease between broker receipts");
    await popup.reload(); await popup.getByText("Ownership tamper attempted", { exact: true }).waitFor();
    const navigated = await execute(second, { type: "list_user_tabs" }); assert.ok(navigated.type === "user_tabs");
    assert.equal(navigated.tabs.some(tab => tab.target_id === target.target_id), false);
    assert.equal((await execute(second, { type: "claim_user_tab", tab: target })).type, "failed", "Navigation preserves the native lease");
    const effect = await execute(first, { type: "run_code", tab_id: opener.id, code: `async page => {
      const popup = page.context().pages().find(page => page.url().endsWith('/late-owned'));
      if (!popup) throw new Error('Owned popup missing');
      await popup.evaluate(async () => { await fetch('/popup-effect?owner=A', {method:'POST'}); });
      return {password:await popup.getByLabel('Popup password').inputValue()};
    }` });
    assert.ok(effect.type === "ready", JSON.stringify(effect)); assert.deepEqual(popupEffects, ["A"]);
    assert.equal(effect.output.includes("Invented-popup-secret"), false); assert.ok(effect.output.includes("[redacted]"));
    const observed = await execute(first, { type: "status" }); assert.ok(observed.type === "ready");
    assert.ok(observed.tabs.some(tab => tab.url === `${origin}/late-owned`));
    assert.equal((await execute(first, { type: "release" })).type, "closed");
    assert.equal(popup.isClosed(), true, "Disposable popup cleanup belongs to its opener");
    assert.equal((await execute(second, { type: "release" })).type, "closed");
  });
});
