// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `GovernedRunnable` is a type brand, so the compiler cannot stop a file from
 * minting one: this spec does. Only `QueryGovernor.authorizeAndBindRunnable` (which
 * holds the explicit bypass branch too) may call `mintGovernedRunnable`, and
 * no production file may cast to the type. A new minter is a new way to run a
 * query nobody governed; adding one means changing this spec on purpose.
 */
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";

const SRC = path.resolve(__dirname, "..", "..");
const MODULE = path.join("service", "governance", "governed_runnable.ts");
const GOVERNOR = path.join("service", "governance", "query_governor.ts");

function sourceFiles(dir: string): string[] {
   return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) return sourceFiles(full);
      return full.endsWith(".ts") && !full.endsWith(".spec.ts") ? [full] : [];
   });
}

const production = () =>
   sourceFiles(SRC)
      .map((file) => path.relative(SRC, file))
      .filter((file) => !file.startsWith("test_helpers"))
      .map((file) => ({
         file,
         text: fs.readFileSync(path.join(SRC, file), "utf8"),
      }));

/** The body of `QueryGovernor.authorizeAndBindRunnable`, up to its closing brace. */
function authorizeAndBindBody(text: string): { start: number; end: number } {
   const start = text.indexOf("   public async authorizeAndBindRunnable(");
   expect(start).toBeGreaterThan(-1);
   const end = text.indexOf("\n   }\n", start);
   expect(end).toBeGreaterThan(start);
   return { start, end };
}

describe("GovernedRunnable minting", () => {
   it("only the QueryGovernor calls mintGovernedRunnable", () => {
      const callers = production()
         .filter(
            ({ file, text }) =>
               file !== MODULE && text.includes("mintGovernedRunnable("),
         )
         .map(({ file }) => file);
      expect(callers).toEqual([GOVERNOR]);
   });

   it("every mint in the governor is inside authorizeAndBindRunnable", () => {
      const text = fs.readFileSync(path.join(SRC, GOVERNOR), "utf8");
      const { start, end } = authorizeAndBindBody(text);
      const sites: number[] = [];
      for (
         let i = text.indexOf("mintGovernedRunnable(");
         i !== -1;
         i = text.indexOf("mintGovernedRunnable(", i + 1)
      ) {
         sites.push(i);
      }
      // The bypass branch and the one non-bypass exit (`bound`).
      expect(sites).toHaveLength(2);
      for (const site of sites) {
         expect(site > start && site < end).toBe(true);
      }
   });

   it("no production file casts to GovernedRunnable", () => {
      const casts = production()
         .filter(
            ({ file, text }) =>
               file !== MODULE && /as\s+GovernedRunnable\b/.test(text),
         )
         .map(({ file }) => file);
      expect(casts).toEqual([]);
   });

   it("prepareAndRun accepts only a GovernedRunnable", () => {
      const text = fs.readFileSync(
         path.join(SRC, "service", "query_execution.ts"),
         "utf8",
      );
      expect(text).toMatch(
         /export async function prepareAndRun\(\s*(?:\/\*\*[^]*?\*\/\s*)?runnable: GovernedRunnable,/,
      );
   });
});
