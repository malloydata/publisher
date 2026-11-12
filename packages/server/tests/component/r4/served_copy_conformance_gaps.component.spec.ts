// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's conformance gaps the public fixture (#1650,
 * ms2data/service#7098) declared, closed: each case below is a shape from
 * conformance/shapes.ts that main answered live, through the serve shape, or
 * with the wrong field metadata, and that the served copy now answers equal to
 * live (rows, field types and annotations) with the switch on.
 *
 * It reads the fixture's own shapes and harness, and asserts the cases the
 * fixture still declares as gaps, so it fails on main and passes here; the
 * fixture's declarations go when both have merged. The two tolerances the
 * fixture applies to every stored read (a number's subtype, the `#(malloy)`
 * drill annotation) apply here too: they are not these gaps.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   type Answer,
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
   type WireField,
} from "./conformance/package_harness";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";
import { rowDifference } from "./harness";

interface Case {
   /** The gap, as the PR names it. */
   gap: string;
   shape: string;
   variant: Variant;
   /** The shape's query names; all of them when absent. */
   queries?: string[];
}

const CASES: Case[] = [
   {
      gap: "a warehouse join beside a stored source (closed on main by #1643; kept as a regression)",
      shape: "a warehouse join beside a stored source",
      variant: "storage",
      queries: ["the persisted source alone"],
   },
   {
      gap: "a warehouse join beside a stored source (closed on main by #1643; kept as a regression)",
      shape: "a warehouse join beside a stored source",
      variant: "materialize",
      queries: ["the persisted source alone"],
   },
   { gap: "nests", shape: "query-shaped: a nest", variant: "storage" },
   { gap: "nests", shape: "query-shaped: a nest", variant: "materialize" },
   { gap: "reserved words", shape: "reserved-word names", variant: "storage" },
   {
      gap: "reserved words",
      shape: "reserved-word names",
      variant: "materialize",
   },
   {
      gap: "#(materialize) with rename:",
      shape: "renames",
      variant: "materialize",
   },
   {
      gap: "metadata on a persisted query's output fields (#7089)",
      shape: "multi-line annotations",
      variant: "storage",
      queries: ["an annotated output field and an annotated measure"],
   },
   {
      gap: "metadata on a persisted query's output fields (#7089)",
      shape: "doc, percent and timeframes carried",
      variant: "storage",
      queries: ["a stored month, with its doc", "a percent column selected"],
   },
   {
      gap: "a joined persisted source through an index (#6643)",
      shape: "joined persisted source from another file",
      variant: "storage",
      queries: ["a persisted fact through its join, from the index"],
   },
   {
      gap: "a joined persisted source through an index (#6643)",
      shape: "joined persisted source from another file",
      variant: "materialize",
      queries: ["a persisted fact through its join, from the index"],
   },
   {
      gap: "a raw SQL source",
      shape: "persist on a raw SQL source",
      variant: "storage",
   },
];

/** The fixture's two tolerated differences on every stored read. */
function comparable(fields: WireField[]): unknown[] {
   return fields.map((f) => {
      const type =
         f.type && typeof f.type === "object" && "subtype" in f.type
            ? { ...(f.type as Record<string, unknown>), subtype: undefined }
            : f.type;
      return {
         name: f.name,
         type: JSON.parse(JSON.stringify(type)),
         annotations: f.annotations.filter((a) => !a.startsWith("#(malloy)")),
      };
   });
}

function answerOf(outcome: Outcome | undefined): Answer {
   if (!outcome) throw new Error("no outcome");
   if ("error" in outcome) throw new Error(outcome.error);
   return outcome.answer;
}

beforeAll(freshLoadPool);
afterAll(freshLoadPool);

for (const c of CASES) {
   const shape = SHAPES.find((s) => s.name === c.shape);
   if (!shape) throw new Error(`no conformance shape ${c.shape}`);
   const queries = shape.queries.filter(
      (q) => !c.queries || c.queries.includes(q.name),
   );
   describe(`served copy gap closed: ${c.gap} [${c.shape}, ${c.variant}]`, () => {
      let restore: () => void = () => {};
      let fixture: LoadedFixture | undefined;
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome>();
      const entry = Object.keys(shape.files)[0];

      beforeAll(async () => {
         restore = withSwitch("on");
         const loader = await loadFixture({
            ...DATA_FILES,
            ...renderFiles(shape, c.variant),
         });
         for (const q of queries) {
            live.set(
               q.name,
               await loader.run(q.model ?? entry, q.malloy, q.givens as never),
            );
         }
         fixture = await loader.bind();
         for (const q of queries) {
            served.set(
               q.name,
               await fixture.run(q.model ?? entry, q.malloy, q.givens as never),
            );
         }
      });
      afterAll(async () => {
         restore();
         await fixture?.dispose();
      });

      for (const q of queries) {
         it(`${q.name}: the served copy answers, equal to live`, () => {
            const l = answerOf(live.get(q.name));
            const s = answerOf(served.get(q.name));
            expect(s.servedBy).toBe("materialized");
            expect(s.servedFrom).toBe("storage");
            expect(s.via).toBe("served_copy");
            expect(rowDifference(l.rows, s.rows)).toBeUndefined();
            expect(comparable(s.fields)).toEqual(comparable(l.fields));
         });
      }
   });
}
