// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A package-load worker stand-in whose served-copy jobs fail as jobs: it
// answers a load with a well-formed empty result (so a test can bring it up
// ready and idle), and a served-copy compile by exiting mid-job
// (`SERVED_COPY_TEST_WORKER_MODE=exit`) or by never answering (`hang`, for the
// pool's job timeout).
import { parentPort } from "node:worker_threads";
import { PACKAGE_LOAD_PROTOCOL_VERSION } from "../protocol";

const port = parentPort;
if (!port) throw new Error("failing_served_copy_worker must run as a worker");
const mode = process.env.SERVED_COPY_TEST_WORKER_MODE ?? "exit";

port.on("message", (message: { type: string; requestId?: string }) => {
   if (message.type === "shutdown") {
      setImmediate(() => process.exit(0));
      return;
   }
   if (message.type === "load-package") {
      port.postMessage({
         type: "load-package-result",
         requestId: message.requestId,
         protocolVersion: PACKAGE_LOAD_PROTOCOL_VERSION,
         packageMetadata: { name: "pkg" },
         models: [],
         loadDurationMs: 1,
         timings: {
            compileDurationMs: 0,
            schemaFetchDurationMs: 0,
            schemaFetchCount: 0,
         },
      });
      return;
   }
   if (message.type === "compile-served-copy" && mode === "exit") {
      process.exit(1);
   }
   // `hang`: no answer; the pool's job timeout ends it.
});

port.postMessage({ type: "ready" });
