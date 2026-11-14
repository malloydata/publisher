// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's conformance gaps the public fixture (#1650,
 * ms2data/service#7098) declared, closed: each case below is a shape from
 * conformance/shapes.ts that main answered live, through the serve shape, or
 * with the wrong field metadata, and that the served copy now answers equal to
 * live (rows, field types and annotations) with the switch on.
 *
 * It reads the fixture's own shapes and harness. The fixture no longer
 * declares these cases as gaps; this spec also pins that the served copy, not
 * the serve shape or live, is what answers them. The two tolerances the
 * fixture applies to every stored read (a number's subtype, the `#(malloy)`
 * drill annotation) apply here too: they are not these gaps.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
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
   // "through the nested rows" is checked against the truth below: Malloy
   // 0.0.438 answers it wrong live.
   {
      gap: "nests",
      shape: "query-shaped: a nest",
      variant: "storage",
      queries: ["the top level"],
   },
   {
      gap: "nests",
      shape: "query-shaped: a nest",
      variant: "materialize",
      queries: ["the top level"],
   },
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

/**
 * Malloy 0.0.438 (malloydata/malloy#3208) skips an aggregate's rows where its
 * source's key is NULL, and an unnested record's key is its parent's key with
 * the element's ordinal: a query-shaped parent grouped by a NULL `region`
 * has a NULL key, so live drops that group's nested rows from a sum through
 * the nest (malloydata/malloy#3220). The served copy reads the stored nest
 * and answers the true sum.
 * This pins both: once Malloy is fixed, live equals the truth again, this
 * fails, and the query goes back to the gap cases above.
 */
for (const variant of ["storage", "materialize"] as const) {
   describe(`a sum through a stored nest under a NULL parent key, against the truth [${variant}]`, () => {
      const shape = SHAPES.find((s) => s.name === "query-shaped: a nest");
      if (!shape) throw new Error("no conformance shape query-shaped: a nest");
      const entry = Object.keys(shape.files)[0];
      const nested =
         "run: nested -> { group_by: by_status.status; aggregate: n is by_status.n.sum() }";
      const truth =
         "run: orders_raw -> { group_by: status; aggregate: n is count() }";
      let restore: () => void = () => {};
      let fixture: LoadedFixture | undefined;
      let live: Outcome | undefined;
      let want: Outcome | undefined;
      let served: Outcome | undefined;
      beforeAll(async () => {
         restore = withSwitch("on");
         const loader = await loadFixture({
            ...DATA_FILES,
            ...renderFiles(shape, variant),
         });
         live = await loader.run(entry, nested);
         want = await loader.run(entry, truth);
         fixture = await loader.bind();
         served = await fixture.run(entry, nested);
      });
      afterAll(async () => {
         restore();
         await fixture?.dispose();
      });

      it("the served copy answers the truth; live does not", () => {
         const s = answerOf(served);
         expect(s.servedBy).toBe("materialized");
         expect(s.via).toBe("served_copy");
         const truthRows = answerOf(want).rows;
         expect(rowDifference(truthRows, s.rows)).toBeUndefined();
         expect(rowDifference(truthRows, answerOf(live).rows)).toBeDefined();
      });
   });
}

/**
 * #6643's guards, two callers: a per-caller `where:` on a persisted source
 * the index reaches only through a join. Each caller must read only its own
 * rows, equal to live, from the index and from the joining file. From the
 * index the served copy does not re-root the joined source (the governor
 * could not prove its filter there and refused every caller), so the query
 * through the join answers live; from the joining file it is served.
 */
describe("served copy gap closed: a given term on a source reached only through a join (#6643), two callers", () => {
   const GIVENS = "##! experimental { persistence givens }";
   const files = {
      // The given is imported by name (Malloy merges a given one import hop
      // deep); customer_dim itself stays reachable only through the join.
      "index.malloy": `${GIVENS}
import "facts.malloy"
import { REGION } from "dims.malloy"
export { order_facts }
`,
      "dims.malloy": `${GIVENS}
import "raw.malloy"

given: REGION :: string is 'north'

#@ persist name="customer_dim" storage="lake"
source: customer_dim is customers_raw -> {
  select: id, segment, home_region
} extend {
  primary_key: id
  where: home_region = $REGION
}
`,
      "facts.malloy": `${GIVENS}
import "raw.malloy"
import "dims.malloy"

#@ persist name="order_facts" storage="lake"
source: order_facts is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  join_one: c is customer_dim on customer_id = c.id
}
`,
   };
   const query =
      "run: order_facts -> { group_by: c.segment, c.home_region; aggregate: t is amount.sum(), n is count() }";
   const callers = ["north", "east"];
   let restore: () => void = () => {};
   let fixture: LoadedFixture | undefined;
   const live = new Map<string, Outcome>();
   const served = new Map<string, Outcome>();
   /** The same query in the joining file, where the source is a node. */
   const servedInFacts = new Map<string, Outcome>();
   const liveInFacts = new Map<string, Outcome>();

   beforeAll(async () => {
      restore = withSwitch("on");
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         ...files,
      });
      for (const region of callers) {
         live.set(
            region,
            await loader.run("index.malloy", query, { REGION: region }),
         );
         liveInFacts.set(
            region,
            await loader.run("facts.malloy", query, { REGION: region }),
         );
      }
      fixture = await loader.bind();
      for (const region of callers) {
         served.set(
            region,
            await fixture.run("index.malloy", query, { REGION: region }),
         );
         servedInFacts.set(
            region,
            await fixture.run("facts.malloy", query, { REGION: region }),
         );
      }
   });
   afterAll(async () => {
      restore();
      await fixture?.dispose();
   });

   /** What a caller got: its rows' regions, or the error. */
   function seen(outcome: Outcome | undefined): unknown {
      if (!outcome) throw new Error("no outcome");
      if ("error" in outcome) return { error: outcome.error };
      return {
         regions: [
            ...new Set(
               outcome.answer.rows
                  .map((r) => (r as Record<string, unknown>)["home_region"])
                  .filter((v) => v !== null),
            ),
         ],
      };
   }

   for (const region of callers) {
      it(`caller ${region}, from the index: that caller's rows only, equal to live`, () => {
         const l = answerOf(live.get(region));
         const s = answerOf(served.get(region));
         expect(rowDifference(l.rows, s.rows)).toBeUndefined();
         expect(seen(served.get(region))).toEqual({ regions: [region] });
      });

      it(`caller ${region}, from the joining file: that caller's rows only, equal to live`, () => {
         const l = answerOf(liveInFacts.get(region));
         const s = answerOf(servedInFacts.get(region));
         expect(rowDifference(l.rows, s.rows)).toBeUndefined();
         expect(seen(servedInFacts.get(region))).toEqual({ regions: [region] });
      });

      it(`caller ${region}, from the joining file: the served copy answers`, () => {
         expect(answerOf(servedInFacts.get(region)).via).toBe("served_copy");
      });
   }
});

/**
 * Decision 3: a connection-rooted `#@ persist` source re-roots with its own
 * extend run applied again over the table. A `where:` there that reads a
 * given must answer each caller as live does, whatever the build applied.
 */
describe("served copy gap closed: a raw SQL source whose where: reads a given, two callers", () => {
   const files = {
      "m.malloy": `##! experimental { persistence givens }

given: REGION :: string is 'north'

#@ persist name="raw_scoped" storage="lake"
source: raw_scoped is duckdb.sql("""
  SELECT region, CAST(amount AS DOUBLE) AS amount
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
""") extend {
  where: region = $REGION
  measure: total is amount.sum()
}
`,
   };
   const query = "run: raw_scoped -> { group_by: region; aggregate: total }";
   const callers = ["north", "east"];
   let restore: () => void = () => {};
   let fixture: LoadedFixture | undefined;
   const live = new Map<string, Outcome>();
   const served = new Map<string, Outcome>();

   beforeAll(async () => {
      restore = withSwitch("on");
      const loader = await loadFixture({ ...DATA_FILES, ...files });
      for (const region of callers) {
         live.set(
            region,
            await loader.run("m.malloy", query, { REGION: region }),
         );
      }
      fixture = await loader.bind();
      for (const region of callers) {
         served.set(
            region,
            await fixture.run("m.malloy", query, { REGION: region }),
         );
      }
   });
   afterAll(async () => {
      restore();
      await fixture?.dispose();
   });

   for (const region of callers) {
      it(`caller ${region}: that caller's rows, equal to live`, () => {
         const l = answerOf(live.get(region));
         const s = answerOf(served.get(region));
         expect(rowDifference(l.rows, s.rows)).toBeUndefined();
         expect(
            s.rows.map((r) => (r as Record<string, unknown>)["region"]),
         ).toEqual([region]);
      });
   }
});
