// A package-load worker stand-in for the protocol checks: it answers each load
// with a result shaped by the request's package name, so one pool can be asked
// for a well-formed result, one on another protocol version, one with no
// version, and one carrying a malformed model.
import { parentPort } from "node:worker_threads";
import { PACKAGE_LOAD_PROTOCOL_VERSION } from "../protocol";

const port = parentPort;
if (!port) throw new Error("bad_result_worker must run as a worker thread");

port.on(
   "message",
   (message: { type: string; requestId?: string; packageName?: string }) => {
      if (message.type === "shutdown") {
         setImmediate(() => process.exit(0));
         return;
      }
      if (message.type !== "load-package") return;
      const result: Record<string, unknown> = {
         type: "load-package-result",
         requestId: message.requestId,
         protocolVersion: PACKAGE_LOAD_PROTOCOL_VERSION,
         packageMetadata: { name: message.packageName },
         models: [{ modelPath: "m.malloy", modelType: "model" }],
         loadDurationMs: 1,
         timings: {
            compileDurationMs: 0,
            schemaFetchDurationMs: 0,
            schemaFetchCount: 0,
         },
      };
      switch (message.packageName) {
         case "wrong-version":
            result.protocolVersion = PACKAGE_LOAD_PROTOCOL_VERSION + 1;
            break;
         case "no-version":
            delete result.protocolVersion;
            break;
         case "bad-model":
            result.models = [
               { modelPath: "m.malloy", modelType: "model", sources: "nope" },
            ];
            break;
      }
      port.postMessage(result);
   },
);

setTimeout(() => port.postMessage({ type: "ready" }), 20);
