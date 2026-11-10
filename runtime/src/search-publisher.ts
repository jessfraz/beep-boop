import { DatabaseSync } from "node:sqlite";
import { parentPort, workerData } from "node:worker_threads";
import { mutatePublished, type SearchMutation } from "./search-mutations.js";
import { backfillCandidates } from "./search-candidates.js";
import type { SearchWork, SearchReply } from "./search.js";

const location: { path: string; extension: string; cancellation: SharedArrayBuffer } = workerData;
const cancellation = new Int32Array(location.cancellation);
if (parentPort === null) throw new Error("History publication requires its owner");
const owner = parentPort;
const database = new DatabaseSync(location.path, { allowExtension: true });
database.loadExtension(location.extension); database.enableLoadExtension(false);
database.exec("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA mmap_size=0");
let backfill: NodeJS.Immediate | undefined;
function prepareCandidates(): void {
  if (backfill !== undefined || Atomics.load(cancellation, 0) !== 0) return;
  backfill = setImmediate(() => {
    backfill = undefined;
    try { if (backfillCandidates(database, () => Atomics.load(cancellation, 0) !== 0)) prepareCandidates(); }
    catch (error) { if (Atomics.load(cancellation, 0) === 0) process.emitWarning(error instanceof Error ? error.message : "History candidate preparation failed"); }
  });
}
prepareCandidates();
owner.on("message", (request: SearchWork<SearchMutation>) => {
  let reply: SearchReply<void>;
  try { mutatePublished(database, request.input, () => Atomics.load(cancellation, 0) !== 0); reply = { id: request.id, type: "result", result: undefined }; }
  catch (error) { reply = { id: request.id, type: "failed", message: error instanceof Error ? error.message : "History publication failed" }; }
  owner.postMessage(reply);
  if (request.input.type === "publish" || request.input.type === "reset") prepareCandidates();
});
owner.once("close", () => { if (backfill !== undefined) clearImmediate(backfill); database.close(); });
