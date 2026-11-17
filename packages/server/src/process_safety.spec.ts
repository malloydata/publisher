// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import {
   describeRejection,
   installProcessSafetyHandlers,
   recordUnhandledRejection,
} from "./process_safety";
import { resetInstrumentsForTesting } from "./telemetry";
import { startMetricsHarness } from "./test_helpers/metrics_harness";

// A real unhandled rejection fails bun's running test, so each is raised in a
// child process that installs the backstop and reports whether it survived.
const MODULE = new URL("./process_safety.ts", import.meta.url).pathname;
const CHILD = `
import { installProcessSafetyHandlers } from ${JSON.stringify(MODULE)};
installProcessSafetyHandlers();
installProcessSafetyHandlers();
const errMessageGetter = new Error("x");
Object.defineProperty(errMessageGetter, "message", { get() { throw new Error("boom"); } });
const errStackGetter = new Error("y");
Object.defineProperty(errStackGetter, "stack", { get() { throw new Error("boom"); } });
const cases = {
  error: () => new Error("cancel failed upstream"),
  string: () => "plain string reason",
  nullProto: () => Object.create(null),
  throwingToString: () => ({ toString() { throw new Error("boom"); } }),
  throwingToPrimitive: () => ({ [Symbol.toPrimitive]() { throw new Error("boom"); } }),
  errMessageGetter: () => errMessageGetter,
  errStackGetter: () => errStackGetter,
  proxyProto: () => new Proxy({}, { getPrototypeOf() { throw new Error("boom"); } }),
};
Promise.reject(cases[process.argv[1]]());
await new Promise((r) => setTimeout(r, 100));
console.log("STILL_RUNNING");
`;

function raise(which: string) {
   const run = spawnSync(process.execPath, ["-e", CHILD, which], {
      encoding: "utf8",
      env: { ...process.env, NODE_ENV: "test" },
   });
   return {
      status: run.status,
      stdout: run.stdout,
      out: run.stdout + run.stderr,
   };
}

describe("installProcessSafetyHandlers", () => {
   it("logs an unhandled rejection with its message and stack and keeps the process running", () => {
      const run = raise("error");
      expect(run.status).toBe(0);
      expect(run.stdout).toContain("STILL_RUNNING");
      expect(run.out).toContain("Unhandled promise rejection");
      expect(run.out).toContain("cancel failed upstream");
      expect(run.out).toContain("stack");
      expect(run.out).toContain("[eval]:");
   });

   it("logs a non-Error reason without a stack", () => {
      const run = raise("string");
      expect(run.status).toBe(0);
      expect(run.out).toContain("plain string reason");
      expect(run.out).not.toContain("stack");
   });

   for (const which of [
      "nullProto",
      "throwingToString",
      "throwingToPrimitive",
      "errMessageGetter",
      "errStackGetter",
      "proxyProto",
   ]) {
      it(`still logs, and keeps running, for a hostile reason: ${which}`, () => {
         const run = raise(which);
         expect(run.status).toBe(0);
         expect(run.stdout).toContain("STILL_RUNNING");
         expect(run.out).toContain("Unhandled promise rejection");
      });
   }

   it("counts each rejection on publisher_unhandled_rejection_total", async () => {
      const harness = await startMetricsHarness();
      try {
         recordUnhandledRejection({ message: "counted" });
         expect(
            await harness.collectCounter("publisher_unhandled_rejection_total"),
         ).toBe(1);
      } finally {
         await harness.shutdown();
         resetInstrumentsForTesting();
      }
   });

   describe("listener", () => {
      let uninstall: (() => void) | undefined;
      afterEach(() => {
         uninstall?.();
         uninstall = undefined;
      });

      it("is installed once, and only the call that installed it removes it", () => {
         const before = process.listenerCount("unhandledRejection");
         uninstall = installProcessSafetyHandlers();
         const second = installProcessSafetyHandlers();
         expect(process.listenerCount("unhandledRejection")).toBe(before + 1);
         second();
         expect(process.listenerCount("unhandledRejection")).toBe(before + 1);
         uninstall();
         uninstall = undefined;
         expect(process.listenerCount("unhandledRejection")).toBe(before);
      });

      it("hands a rejection to a custom reporter", () => {
         const seen: string[] = [];
         uninstall = installProcessSafetyHandlers((r) => seen.push(r.message));
         process.emit(
            "unhandledRejection",
            new Error("to worker"),
            Promise.resolve(),
         );
         expect(seen).toEqual(["to worker"]);
      });
   });
});

describe("describeRejection", () => {
   it("falls back through String, Object.prototype.toString and a constant", () => {
      expect(describeRejection(Object.create(null)).message).toBe(
         "[object Object]",
      );
      expect(
         describeRejection(
            new Proxy(
               {},
               {
                  getPrototypeOf() {
                     throw new Error("x");
                  },
               },
            ),
         ).message,
      ).toBeString();
      expect(describeRejection(Symbol("s")).message).toBe("Symbol(s)");
   });
});
