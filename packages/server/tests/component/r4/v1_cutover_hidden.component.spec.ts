// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's G2 (materialization v2, phase 3 pull request 6): a v1
 * `#@ persist` table, colocated or `storage=`, holds its source's hidden
 * columns (built from the source's own query), so the served copy binds them
 * and keeps the author's text after the query, which hides them again. A
 * measure over an `internal` column is served from the table, and the column
 * stays exactly as hidden as live: a caller can neither select, group by nor
 * filter on it, nor read a `private` measure, and is refused with live's own
 * words.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "./conformance/package_harness";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";
import { rowDifference } from "./harness";

const shapeNamed = (name: string) => SHAPES.find((s) => s.name === name)!;

/** Each query, the model it runs in, and whether live refuses it. */
interface Case {
   name: string;
   model: string;
   malloy: string;
   /** Live's refusal, by the words it ends with; absent: live answers. */
   refused?: "is private" | "is internal" | "is not defined";
}

const JOINED: Case[] = [
   {
      name: "an internal column grouped by",
      model: "dims.malloy",
      malloy: "run: customer_dim -> { group_by: home_region }",
      refused: "is internal",
   },
   {
      name: "an internal column selected",
      model: "dims.malloy",
      malloy: "run: customer_dim -> { select: id, home_region }",
      refused: "is internal",
   },
   {
      name: "an internal column filtered on",
      model: "dims.malloy",
      malloy:
         "run: customer_dim -> { where: home_region = 'west'; aggregate: n is count() }",
      refused: "is internal",
   },
   {
      name: "a private measure",
      model: "dims.malloy",
      malloy: "run: customer_dim -> { aggregate: west_rows }",
      refused: "is private",
   },
   {
      name: "an internal column through the join",
      model: "index.malloy",
      malloy: "run: order_facts -> { group_by: c.home_region }",
      refused: "is internal",
   },
   {
      name: "every column, with select: *",
      model: "dims.malloy",
      malloy: "run: customer_dim -> { select: *; order_by: id }",
   },
   {
      name: "a public measure over the internal column",
      model: "dims.malloy",
      malloy: "run: customer_dim -> { group_by: segment; aggregate: west_n }",
   },
   {
      name: "the measure through the join, from the index",
      model: "index.malloy",
      malloy:
         "run: order_facts -> { group_by: c.segment; aggregate: w is c.west_n, t is amount.sum() }",
   },
];

const REHIDDEN: Case[] = [
   {
      name: "a re-hidden column grouped by",
      model: "m.malloy",
      malloy: "run: rehidden -> { group_by: region_raw }",
      refused: "is internal",
   },
   {
      name: "a re-hidden column by its stored name",
      model: "m.malloy",
      malloy: "run: rehidden -> { group_by: home_region }",
      refused: "is not defined",
   },
   {
      name: "an excepted column",
      model: "m.malloy",
      malloy: "run: dropped -> { group_by: status }",
      refused: "is not defined",
   },
   {
      name: "every column of the excepting source, with select: *",
      model: "m.malloy",
      malloy: "run: dropped -> { select: *; order_by: id }",
   },
   {
      name: "every column of the re-hiding source, with select: *",
      model: "m.malloy",
      malloy: "run: rehidden -> { select: *; order_by: id }",
   },
   {
      name: "a measure over the re-hidden column",
      model: "m.malloy",
      malloy: "run: rehidden -> { group_by: segment; aggregate: n, west_n }",
   },
];

/** An error's first line: the compiler's message, as a caller reads it. */
const firstLine = (error: string) => error.split("\n")[0];

function describeShape(
   shapeName: string,
   variant: Variant,
   cases: readonly Case[],
): void {
   describe(`v1 cutover: a ${variant} table's hidden columns (G2), ${shapeName}`, () => {
      let restore: () => void = () => {};
      let fixture: LoadedFixture;
      const live = new Map<string, Outcome>();
      let dispose: () => Promise<void>;

      beforeAll(async () => {
         await freshLoadPool();
         restore = withSwitch("cutover");
         const loader = await loadFixture({
            ...DATA_FILES,
            ...renderFiles(shapeNamed(shapeName), variant),
         });
         dispose = loader.dispose;
         // Live: before any table is bound.
         for (const c of cases) {
            live.set(c.malloy, await loader.run(c.model, c.malloy));
         }
         fixture = await loader.bind();
      });
      afterAll(async () => {
         restore();
         await dispose?.();
         await freshLoadPool();
      });

      it("builds and serves every source from its table", () => {
         expect(fixture.buildErrors).toEqual({});
         expect(fixture.built.every((b) => b.tier === variant)).toBe(true);
         expect(fixture.verdicts.every((v) => v.endsWith(":serves"))).toBe(
            true,
         );
         expect(
            fixture.warnings.filter((w) => w.code === "served-copy-not-served"),
         ).toEqual([]);
      });

      for (const c of cases) {
         const refused = c.refused;
         if (refused) {
            it(`refuses ${c.name}, in live's words`, async () => {
               const l = live.get(c.malloy)!;
               if (!("error" in l)) throw new Error("live answered");
               expect(firstLine(l.error)).toEndWith(refused);
               const out = await fixture.run(c.model, c.malloy);
               if (!("error" in out)) {
                  throw new Error(
                     `served answered ${JSON.stringify(out.answer.rows)}`,
                  );
               }
               expect(firstLine(out.error)).toBe(firstLine(l.error));
            });
         } else {
            it(`serves ${c.name} from the table, equal to live`, async () => {
               const l = live.get(c.malloy)!;
               if ("error" in l) throw new Error(l.error);
               const served = await fixture.run(c.model, c.malloy);
               if ("error" in served) throw new Error(served.error);
               expect(served.answer.servedBy).toBe("materialized");
               expect(served.answer.via).toBe("served_copy");
               expect(served.answer.fields.map((f) => f.name)).toEqual(
                  l.answer.fields.map((f) => f.name),
               );
               expect(
                  rowDifference(l.answer.rows, served.answer.rows),
               ).toBeUndefined();
            });
         }
      }
   });
}

for (const variant of ["colocated", "storage"] as const) {
   describeShape(
      "joined persisted source with hidden fields, from an index",
      variant,
      JOINED,
   );
   describeShape(
      "an extend block that drops and re-hides stored columns",
      variant,
      REHIDDEN,
   );
}

/**
 * `#(carries)` reads what a caller can reach, never what a table stores: a
 * hidden column of a class off the list is allowed in a `storage=` table, as
 * in a colocated one or a `#(materialize)` one, since the served copy hides it
 * again and the ceiling is checked on the served copy's own compile too. A
 * public field that reads it carries its class, so the package does not load,
 * on every tier alike, before anything is built.
 */
const carriesModel = (persist: string, exposed: string) => ({
   "c.malloy": `##! experimental { persistence access_modifiers }
import "raw.malloy"

#(carries) ["none"]
${persist}
source: customer_c is customers_raw -> {
  #(classification) "none"
  select: id, segment
  #(classification) "pii.location"
  select: home_region
} include { internal: home_region; public: * } extend {
  #(classification) "none"
  measure: n is count()
${exposed}}
`,
});

const PERSIST: Record<"colocated" | "storage", string> = {
   colocated: "#@ persist",
   storage: '#@ persist storage="lake"',
};

for (const variant of ["colocated", "storage"] as const) {
   describe(`v1 cutover: #(carries) over a ${variant} table's hidden column`, () => {
      let restore: () => void = () => {};
      let fixture: LoadedFixture;
      let dispose: () => Promise<void>;
      const live = new Map<string, Outcome>();
      const queries = [
         "run: customer_c -> { group_by: segment; aggregate: n }",
         "run: customer_c -> { select: id, segment; order_by: id }",
         "run: customer_c -> { group_by: home_region }",
      ];

      beforeAll(async () => {
         await freshLoadPool();
         restore = withSwitch("cutover");
         const loader = await loadFixture({
            ...DATA_FILES,
            ...renderFiles({ name: "carries", files: {} } as never, variant),
            ...carriesModel(PERSIST[variant], ""),
         });
         dispose = loader.dispose;
         for (const q of queries) live.set(q, await loader.run("c.malloy", q));
         fixture = await loader.bind();
      });
      afterAll(async () => {
         restore();
         await dispose?.();
         await freshLoadPool();
      });

      it("loads, and stores the hidden column of a class off the list", () => {
         expect(fixture.buildErrors).toEqual({});
         expect(fixture.verdicts).toEqual(["customer_c:serves"]);
         if (variant === "storage") {
            const [entry] = Object.values(fixture.stored);
            expect(
               (entry.schema as { name: string }[]).map((c) => c.name),
            ).toContain("home_region");
         }
      });

      it("serves the public surface from the table, equal to live, without the hidden column", async () => {
         for (const q of queries.slice(0, 2)) {
            const l = live.get(q)!;
            if ("error" in l) throw new Error(l.error);
            const served = await fixture.run("c.malloy", q);
            if ("error" in served) throw new Error(served.error);
            expect(served.answer.via).toBe("served_copy");
            expect(served.answer.fields.map((f) => f.name)).toEqual(
               l.answer.fields.map((f) => f.name),
            );
            expect(served.answer.fields.map((f) => f.name)).not.toContain(
               "home_region",
            );
            expect(
               rowDifference(l.answer.rows, served.answer.rows),
            ).toBeUndefined();
         }
      });

      it("refuses the hidden column, in live's words", async () => {
         const q = queries[2];
         const l = live.get(q)!;
         if (!("error" in l)) throw new Error("live answered");
         const served = await fixture.run("c.malloy", q);
         if (!("error" in served)) throw new Error("served answered");
         expect(firstLine(served.error)).toBe(firstLine(l.error));
         expect(firstLine(l.error)).toEndWith("is internal");
      });

      it("refuses the load when a public field reads the hidden column (a leak)", async () => {
         let error: unknown;
         try {
            const loader = await loadFixture({
               ...DATA_FILES,
               ...renderFiles({ name: "carries", files: {} } as never, variant),
               ...carriesModel(
                  PERSIST[variant],
                  "  dimension: region_copy is home_region\n",
               ),
            });
            await loader.dispose();
         } catch (err) {
            error = err;
         }
         expect(String(error)).toContain("region_copy");
         expect(String(error)).toContain("pii.location");
      });
   });
}
