// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import { installProcessSafetyHandlers } from "./process_safety";

// The rejection itself is raised in a child process: bun's test runner fails
// the running test on any unhandled rejection, listener or not, so it cannot
// be raised in-process.
const CHILD = `
import { installProcessSafetyHandlers } from ${JSON.stringify(
   new URL("./process_safety.ts", import.meta.url).pathname,
)};
installProcessSafetyHandlers();
installProcessSafetyHandlers();
Promise.reject(new Error("cancel failed upstream"));
await new Promise((r) => setTimeout(r, 100));
console.log("STILL_RUNNING");
`;

describe("installProcessSafetyHandlers", () => {
   it("logs an unhandled rejection with its message and stack and keeps the process running", () => {
      const run = spawnSync(process.execPath, ["-e", CHILD], {
         encoding: "utf8",
         env: { ...process.env, NODE_ENV: "test" },
      });
      const out = run.stdout + run.stderr;
      expect(run.status).toBe(0);
      expect(run.stdout).toContain("STILL_RUNNING");
      expect(out).toContain("Unhandled promise rejection");
      expect(out).toContain("cancel failed upstream");
      expect(out).toContain("stack");
      expect(out).toContain("[eval]:");
   });

   describe("listener", () => {
      let uninstall: (() => void) | undefined;
      afterEach(() => {
         uninstall?.();
         uninstall = undefined;
      });

      it("is installed once and removed on request", () => {
         const before = process.listenerCount("unhandledRejection");
         uninstall = installProcessSafetyHandlers();
         installProcessSafetyHandlers();
         expect(process.listenerCount("unhandledRejection")).toBe(before + 1);
         uninstall();
         uninstall = undefined;
         expect(process.listenerCount("unhandledRejection")).toBe(before);
      });
   });
});
