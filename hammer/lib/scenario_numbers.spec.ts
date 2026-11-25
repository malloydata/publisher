// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Every scenario directory's number is unique within its suite. The number is
// how people name a scenario in a review or a log ("scenario 119"), so two
// scenarios under one number make that name ambiguous; it decides nothing at
// run time (the id in the front matter does).

import { describe, expect, it } from "bun:test";
import { readdirSync } from "fs";
import path from "path";

const SCENARIOS = path.resolve(import.meta.dir, "..", "scenarios");

function suites(): string[] {
   return readdirSync(SCENARIOS, { withFileTypes: true })
      .filter((e) => e.isDirectory())
      .map((e) => e.name);
}

describe("scenario numbers", () => {
   for (const suite of suites()) {
      it(`are unique in ${suite}`, () => {
         const byNumber = new Map<number, string[]>();
         for (const entry of readdirSync(path.join(SCENARIOS, suite), {
            withFileTypes: true,
         })) {
            const match = entry.isDirectory() && /^(\d+)-/.exec(entry.name);
            if (!match) continue;
            const n = Number(match[1]);
            byNumber.set(n, [...(byNumber.get(n) ?? []), entry.name]);
         }
         const shared = [...byNumber.values()].filter(
            (names) => names.length > 1,
         );
         expect(shared).toEqual([]);
      });
   }
});
