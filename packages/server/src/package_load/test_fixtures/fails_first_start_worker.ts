// A package-load worker stand-in for pool tests: the first one started fails
// before it is ready, with an uncaught error, and every later one starts and
// answers each load with its own thread id as the package name. The first
// start is recorded in failsFirstStartMarker(), a file named after the
// process, which the test deletes before and after. (Not an environment
// variable: a spec that replaces `process.env` would hide it from workers.)
import * as fs from "node:fs";
import { parentPort, threadId } from "node:worker_threads";
import { failsFirstStartMarker } from "./fails_first_start_marker";

const port = parentPort;
if (!port) throw new Error("fails_first_start_worker must run as a worker");
const marker = failsFirstStartMarker();

if (!fs.existsSync(marker)) {
   fs.writeFileSync(marker, "started once");
   setTimeout(() => {
      throw new Error("simulated start failure");
   }, 50);
} else {
   port.on("message", (message: { type: string; requestId?: string }) => {
      if (message.type === "shutdown") {
         setImmediate(() => process.exit(0));
         return;
      }
      if (message.type !== "load-package") return;
      port.postMessage({
         type: "load-package-result",
         requestId: message.requestId,
         packageMetadata: { name: `thread-${threadId}` },
         models: [],
         loadDurationMs: 1,
         timings: {
            compileDurationMs: 0,
            schemaFetchDurationMs: 0,
            schemaFetchCount: 0,
         },
      });
   });
   setTimeout(() => port.postMessage({ type: "ready" }), 20);
}
