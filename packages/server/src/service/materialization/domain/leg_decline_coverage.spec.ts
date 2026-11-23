// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every decline code a serving leg can throw has its row in the decline
 * table, so the path report and the route metric give it its own reason. A
 * code the table lacks would read as `MISSING_FIELD`, the reason for
 * anything unexplained, and no one would notice.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { declineOfCode } from "./leg_decline";

const SERVE_DIR = path.join(__dirname, "../serve");

/** Each non-spec source file that may throw or build a leg decline. */
function sources(): { file: string; text: string }[] {
   return fs
      .readdirSync(SERVE_DIR)
      .filter((f) => f.endsWith(".ts") && !f.endsWith(".spec.ts"))
      .map((f) => ({
         file: f,
         text: fs.readFileSync(path.join(SERVE_DIR, f), "utf8"),
      }));
}

/** The first argument of each `new LegDecline(…)` in `text`, as written. */
function firstArguments(text: string): string[] {
   const out: string[] = [];
   for (const m of text.matchAll(/new LegDecline\(/g)) {
      let depth = 0;
      let i = (m.index ?? 0) + m[0].length;
      const start = i;
      for (; i < text.length; i++) {
         const c = text[i];
         if (c === "(" || c === "[" || c === "{") depth++;
         else if (c === ")" || c === "]" || c === "}") {
            if (depth === 0) break;
            depth--;
         } else if (c === "," && depth === 0) break;
      }
      out.push(text.slice(start, i).trim());
   }
   return out;
}

const LEG_CODE = /"((?:rollup|view|query|served_copy)_[a-z_]+)"/g;

/**
 * Every code a leg names: each literal in the first argument of a
 * `new LegDecline(…)` (a ternary's branches included), and any
 * `code: "<leg>_…"` a leg builds its refusal from.
 */
function codesNamed(text: string): string[] {
   const found = new Set<string>();
   for (const arg of firstArguments(text)) {
      for (const m of arg.matchAll(LEG_CODE)) found.add(m[1]);
   }
   for (const m of text.matchAll(
      /\bcode:\s*"((?:rollup|view|query|served_copy)_[a-z_]+)"/g,
   )) {
      found.add(m[1]);
   }
   return [...found];
}

describe("the decline table covers every code a leg throws", () => {
   it("finds the legs' codes at all", () => {
      const all = sources().flatMap(({ text }) => codesNamed(text));
      // A scan that matched nothing would pass vacuously.
      expect(all.length).toBeGreaterThan(20);
   });

   it("gives each its own row, never the MISSING_FIELD default", () => {
      const missing = sources().flatMap(({ file, text }) =>
         codesNamed(text)
            .filter((code) => declineOfCode(code) === undefined)
            .map((code) => `${file}: ${code}`),
      );
      expect(missing).toEqual([]);
   });

   it("throws no code by a computed expression the scan cannot read", () => {
      // A first argument with no literal must pass through a code that is
      // already a typed decline code (its own decline's, or a refusal's).
      const computed = sources().flatMap(({ file, text }) =>
         firstArguments(text)
            .filter((arg) => !/"[a-z_]+"/.test(arg))
            .map((arg) => `${file}: ${arg}`),
      );
      for (const site of computed) {
         expect(site).toMatch(
            /: (summary|refusal|loose|own|decline)\.code$|: code$/,
         );
      }
   });
});
