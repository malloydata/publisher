// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R-05: a `freshness.window=` that is not a positive duration is refused with
 * the rule it breaks, a whole number greater than zero and one unit. `"0h"`
 * was refused as "not a whole number and a unit", which it is. Every reader
 * of the group says the same: `#@ persist` (annotation_reader.ts),
 * `#(materialize)` (the same check) and the rollup routes
 * (freshness_declaration.ts), and each keeps its load-failure code.
 */

import { Annotations } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   freshnessValueProblem,
   PERSIST_INVALID_CODE,
   readRouteTag,
} from "../../annotation_reader";
import { parseRouteTag } from "../../motly";
import { FRESHNESS_WINDOW_PROBLEM } from "../domain/freshness";
import { readFreshnessGroup } from "./freshness_declaration";

/** Annotations over `lines`, one note per line, as the translator records them. */
function annotationsOf(lines: string[]): Annotations {
   return new Annotations({
      notes: lines.map((text, line) => ({
         text,
         at: {
            url: "file:///m.malloy",
            range: {
               start: { line, character: 0 },
               end: { line, character: text.length },
            },
         },
      })),
   } as ConstructorParameters<typeof Annotations>[0]);
}

const POSITIVE =
   "has a freshness.window= that is not a positive duration: write a whole " +
   'number greater than zero and one unit (s, m, h, d or w), such as freshness.window="24h"';

describe("a freshness.window= that is not a positive duration", () => {
   it("is described by the rule it breaks", () => {
      expect(FRESHNESS_WINDOW_PROBLEM).toBe(POSITIVE);
   });

   for (const window of ["0h", "0s", "-1h", "1.5h"]) {
      it(`refuses ${window} on a rollup route as not positive`, () => {
         const { tag } = parseRouteTag(
            annotationsOf([`#(rollup) freshness.window="${window}"\n`]),
            "rollup",
         );
         expect(readFreshnessGroup(tag, ["freshness"])).toEqual({
            invalid: POSITIVE,
         });
      });

      it(`refuses ${window} on #(materialize) as not positive`, () => {
         const { tag } = parseRouteTag(
            annotationsOf([`#(materialize) freshness.window="${window}"\n`]),
            "materialize",
         );
         expect(freshnessValueProblem(tag.tag("freshness")!)).toBe(POSITIVE);
      });

      it(`fails the load on #@ persist for ${window}, saying it is not positive`, () => {
         const { findings } = readRouteTag(
            annotationsOf([`#@ persist freshness.window="${window}"\n`]),
            "@",
         );
         expect(findings).toContainEqual(
            expect.objectContaining({
               tier: "malformed",
               code: PERSIST_INVALID_CODE,
               message: `\`#@ persist\` ${POSITIVE}`,
            }),
         );
      });
   }

   it("reads a positive window", () => {
      const { tag } = parseRouteTag(
         annotationsOf(['#(rollup) freshness.window="24h"\n']),
         "rollup",
      );
      expect(readFreshnessGroup(tag, ["freshness"])).toEqual({
         freshness: { window: "24h" },
      });
   });
});
