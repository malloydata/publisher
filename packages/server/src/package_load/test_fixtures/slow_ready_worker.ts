// A package-load worker stand-in for pool dispatch tests: it becomes ready
// only after a delay, the window in which a pool sees a freshly spawned worker
// with nothing in flight, and answers every load with its own thread id as
// the package name, so a test can tell which worker served a job.
import { parentPort, threadId } from "node:worker_threads";

const port = parentPort;
if (!port) throw new Error("slow_ready_worker must run as a worker thread");

port.on("message", (message: { type: string; requestId?: string }) => {
   if (message.type === "shutdown") {
      setImmediate(() => process.exit(0));
      return;
   }
   if (message.type !== "load-package") return;
   setTimeout(() => {
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
   }, 50);
});

setTimeout(() => port.postMessage({ type: "ready" }), 200);
