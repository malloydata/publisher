// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every conformance shape under the shipped default, which no
 * other spec runs. The default turns `PUBLISHER_MATERIALIZE` on, so the
 * `#(materialize)` shapes run here too; 6f found one defect only the default
 * reached when it was off (the cutover read `#(preaggregate)` with it off),
 * which `preaggregate_variants.spec.ts` pins with the switch set `false`.
 * Here only `PERSIST_STORAGE_MODE=on` is set: each query is answered live
 * before any table is bound, then after the bind, and the two must agree on
 * rows and on each column's type and annotations (a number's subtype and
 * the `drill_expression` of Malloy's own `#(malloy)` annotations aside, a
 * nest's fields included).
 */

import { afterAll, describe, expect, it } from "bun:test";
import { parseAnnotation } from "@malloydata/malloy-tag";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES } from "./conformance/data";
import {
   type Answer,
   freshLoadPool,
   loadFixture,
   type Outcome,
} from "./conformance/package_harness";
import { LIVE_WRONG_NEST, renderFiles, SHAPES } from "./conformance/shapes";
import { rowDifference } from "./harness";

/**
 * Shapes whose served answer marks a measure it read from a stored column as
 * a dimension: Malloy's `#(malloy)` annotation on it loses `calculation` and
 * gains a drill `code`, so a renderer reads the column as a dimension. Known for a persisted
 * view, whose table stores each measure as a column; set aside for these
 * shapes alone until the view leg keeps the measure's kind.
 */
const STORED_MEASURE_AS_DIMENSION = new Set(["persisted view"]);

/**
 * A `#(malloy)` annotation with only its `drill_expression` set aside, which
 * the served copy writes differently for a dimension it stores (its code is the
 * stored column, or the definition, not live's): the conformance spec's
 * `drill-metadata` tolerance. Every other key Malloy writes there
 * (`reference_id`, `calculation`, `drill_filters`, `limit`, `ordered_by`,
 * `query_timezone`) is still compared, `calculation` aside for
 * {@link STORED_MEASURE_AS_DIMENSION}. Any other annotation is compared as is.
 */
function withoutDrillExpression(
   note: string,
   measureKind: "compare" | "ignore",
): unknown {
   if (!note.startsWith("#(malloy)")) return note;
   const { tag, log } = parseAnnotation(note);
   if (log.length > 0) return note;
   const { drill_expression: _d, calculation, ...rest } = tag.getProperties();
   const kept =
      calculation && measureKind === "compare"
         ? { calculation, ...rest }
         : rest;
   return Object.fromEntries(
      Object.entries(kept).map(([k, v]) => [k, v.toJSON()]),
   );
}

function metadataDifference(
   a: Answer,
   b: Answer,
   measureKind: "compare" | "ignore",
): string | undefined {
   if (a.fields.length !== b.fields.length) return "field count";
   // A number's subtype and the drill expression in Malloy's own `#(malloy)`
   // annotations are set aside at every depth, a nest's fields included, as
   // the conformance spec's `number-subtype` and `drill-metadata` tolerances do.
   const strip = (t: unknown): unknown => {
      if (Array.isArray(t)) {
         return t.map((a) =>
            typeof (a as { value?: unknown })?.value === "string"
               ? {
                    ...(a as object),
                    value: withoutDrillExpression(
                       (a as { value: string }).value,
                       measureKind,
                    ),
                 }
               : strip(a),
         );
      }
      if (t && typeof t === "object") {
         return Object.fromEntries(
            Object.entries(t as Record<string, unknown>)
               .filter(([k]) => k !== "subtype")
               .map(([k, v]) => [k, strip(v)]),
         );
      }
      return t;
   };
   const notes = (n: string[] | undefined) =>
      (n ?? []).map((x) => withoutDrillExpression(x, measureKind));
   for (let i = 0; i < a.fields.length; i++) {
      const [x, y] = [a.fields[i], b.fields[i]];
      if (JSON.stringify(strip(x.type)) !== JSON.stringify(strip(y.type))) {
         return `${x.name}: type ${JSON.stringify(x.type)} vs ${JSON.stringify(y.type)}`;
      }
      if (
         JSON.stringify(notes(x.annotations)) !==
         JSON.stringify(notes(y.annotations))
      ) {
         return `${x.name}: annotations ${JSON.stringify(notes(x.annotations))} vs ${JSON.stringify(notes(y.annotations))}`;
      }
   }
   return undefined;
}

let servedCount = 0;

afterAll(async () => {
   await freshLoadPool();
});

describe("the conformance shapes under the default settings", () => {
   for (const shape of SHAPES) {
      for (const variant of shape.variants ?? ["storage", "colocated"]) {
         it(`${shape.name} [${variant}]: every answer equals live`, async () => {
            const restore = installSettings({
               env: { PERSIST_STORAGE_MODE: "on" },
            }).restore;
            await freshLoadPool();
            const entry = Object.keys(shape.files)[0];
            const loader = await loadFixture({
               ...DATA_FILES,
               ...renderFiles(shape, variant),
            });
            const problems: string[] = [];
            try {
               const run = (
                  t: { run: typeof loader.run },
                  q: (typeof shape.queries)[number],
               ) => t.run(q.model ?? entry, q.malloy, q.givens as never);
               const live = new Map<string, Outcome>();
               for (const q of shape.queries) {
                  live.set(q.name, await run(loader, q));
               }
               const fixture = await loader.bind({ includeRollups: true });
               for (const q of shape.queries) {
                  const l = live.get(q.name)!;
                  const s = await run(fixture, q);
                  if ("error" in l || "error" in s) {
                     // A refusal stays a refusal.
                     if (!("error" in l && "error" in s)) {
                        problems.push(
                           `${q.name}: live ${"error" in l ? l.error : "answered"}, default ${"error" in s ? s.error : "answered"}`,
                        );
                     }
                     continue;
                  }
                  // Live itself is wrong on this query (known limitation 41);
                  // the stored answer is checked against the truth in
                  // served_copy_conformance_gaps.
                  const gaps = { ...shape.gaps, ...q.gaps };
                  if (
                     [`${variant}/cutover`, variant, "*"].some(
                        (key) => gaps[key] === LIVE_WRONG_NEST,
                     )
                  ) {
                     if (s.answer.servedBy !== "live") servedCount++;
                     continue;
                  }
                  const why =
                     rowDifference(l.answer.rows, s.answer.rows) ??
                     metadataDifference(
                        l.answer,
                        s.answer,
                        STORED_MEASURE_AS_DIMENSION.has(shape.name)
                           ? "ignore"
                           : "compare",
                     );
                  if (why) problems.push(`${q.name}: ${why}`);
                  if (s.answer.servedBy !== "live") servedCount++;
               }
            } finally {
               restore();
               await loader.dispose();
            }
            expect(problems).toEqual([]);
         }, 180_000);
      }
   }

   it("answers from tables, not only live", () => {
      expect(servedCount).toBeGreaterThan(80);
   });
});
