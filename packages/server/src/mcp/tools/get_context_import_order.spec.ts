// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * get_context_tool.ts imports the engine, and the engine imports modules the
 * tool's own helpers import. Either must be importable first.
 *
 * Each order runs in a fresh process, so a module another spec already loaded
 * cannot hide a cycle that reads a binding before it is initialised.
 */

import { describe, expect, it } from "bun:test";
import * as path from "path";

const dir = import.meta.dir;

function importInFreshProcess(first: string, second?: string) {
   const script = [first, ...(second ? [second] : [])]
      .map((file) => `await import(${JSON.stringify(path.join(dir, file))});`)
      .join("\n");
   const result = Bun.spawnSync(["bun", "-e", script], {
      stdout: "pipe",
      stderr: "pipe",
   });
   return {
      code: result.exitCode,
      stderr: result.stderr.toString(),
   };
}

describe("get_context module load order", () => {
   it("loads the engine first", () => {
      const out = importInFreshProcess("../context/engine.ts");
      expect(out.stderr).not.toContain("before initialization");
      expect(out.code).toBe(0);
   });

   it("loads the tool first", () => {
      const out = importInFreshProcess("get_context_tool.ts");
      expect(out.stderr).not.toContain("before initialization");
      expect(out.code).toBe(0);
   });

   it("loads the engine, then the tool", () => {
      const out = importInFreshProcess(
         "../context/engine.ts",
         "get_context_tool.ts",
      );
      expect(out.code).toBe(0);
   });
});
