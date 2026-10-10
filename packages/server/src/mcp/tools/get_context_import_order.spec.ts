// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * get_context_retrievers.ts and get_context_tool.ts import each other. Either
 * must be importable first. Importing the retrievers first used to throw
 * "Cannot access 'semanticRetriever' before initialization", because the tool
 * read both retrievers into a constant while it was still being evaluated.
 *
 * Each order runs in a fresh process, so a module another spec already loaded
 * cannot hide the problem.
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
   it("loads the retrievers first", () => {
      const out = importInFreshProcess("get_context_retrievers.ts");
      expect(out.stderr).not.toContain("before initialization");
      expect(out.code).toBe(0);
   });

   it("loads the tool first", () => {
      const out = importInFreshProcess("get_context_tool.ts");
      expect(out.stderr).not.toContain("before initialization");
      expect(out.code).toBe(0);
   });

   it("loads the retrievers, then the tool", () => {
      const out = importInFreshProcess(
         "get_context_retrievers.ts",
         "get_context_tool.ts",
      );
      expect(out.code).toBe(0);
   });
});
