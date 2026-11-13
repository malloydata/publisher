// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// v1 colocated tables under the v1 cutover. Each query is answered live before any table is bound,
// then served, callers interleaved and repeated; the served answer must equal
// live per caller by the strict comparison, and a refusal must stay a
// refusal. Wide column types keep their table and live's result types.

import { afterAll, describe, expect, it } from "bun:test";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   type Outcome,
   type Switch,
   withSwitch,
} from "./conformance/package_harness";

type Q = {
   name: string;
   model: string;
   malloy: string;
   givens?: Record<string, unknown>;
   /** The served run must read a table (default true unless live refuses). */
   stored?: boolean;
};

async function probe(
   sw: Switch,
   files: Record<string, string>,
   qs: Q[],
): Promise<{ live: Map<string, Outcome>; served: Map<string, Outcome> }> {
   const live = new Map<string, Outcome>();
   const served = new Map<string, Outcome>();
   await freshLoadPool();
   const restore = withSwitch(sw);
   const loader = await loadFixture({
      ...DATA_FILES,
      "raw.malloy": RAW_MODEL,
      ...files,
   });
   try {
      for (const q of qs)
         live.set(
            q.name,
            await loader.run(q.model, q.malloy, q.givens as never),
         );
      const f = await loader.bind();
      for (const round of [0, 1, 2]) {
         for (const q of round === 1 ? [...qs].reverse() : qs) {
            const out = await f.run(q.model, q.malloy, q.givens as never);
            const prev = served.get(q.name);
            if (!prev) served.set(q.name, out);
            else if ("answer" in prev && "answer" in out) {
               expect(
                  strictRowDifference(prev.answer.rows, out.answer.rows),
               ).toBeUndefined();
            } else expect("error" in prev).toBe("error" in out);
         }
      }
      return { live, served };
   } finally {
      restore();
      await loader.dispose();
      await freshLoadPool();
   }
}

function check(
   r: { live: Map<string, Outcome>; served: Map<string, Outcome> },
   q: Q,
) {
   const l = r.live.get(q.name)!;
   const s = r.served.get(q.name)!;
   if ("error" in l) {
      expect("error" in s).toBe(true);
      return;
   }
   if ("error" in s) throw new Error(`served errored: ${s.error}`);
   expect(strictRowDifference(l.answer.rows, s.answer.rows)).toBeUndefined();
   if (q.stored !== false) expect(s.answer.servedBy).not.toBe("live");
}

const GOVERNED = `##! experimental { persistence givens access_modifiers }
import "raw.malloy"

#(secure)
given: TENANTS :: string[]
given: ROLE :: string

// A row-scoping given term on a colocated persisted source's extend block.
#@ persist name="scoped"
source: scoped is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  where: region in $TENANTS
  measure: total is amount.sum()
}

// A row-level gate and a whole-source gate, over a table with hidden columns.
#(authorize) 'analyst' = $ROLE
#(access_filter) home_region in $TENANTS
#@ persist name="cust"
source: cust is customers_raw -> {
  select: id, segment, home_region, name
} include { internal: home_region; private: name; public: * } extend {
  primary_key: id
  measure: n is count()
  measure: named is count() { where: name != null }
  view: by_segment is { group_by: segment; aggregate: n }
}

source: cust_ext is cust extend {
  dimension: region_copy is home_region
}

#@ persist name="facts"
source: facts is orders_raw -> {
  select: id, customer_id, amount
} extend {
  join_one: c is cust on customer_id = c.id
  measure: total is amount.sum()
}
`;

const CALLERS = [
   { ROLE: "analyst", TENANTS: ["north"] },
   { ROLE: "analyst", TENANTS: ["south", "east"] },
];
const DENIED = { ROLE: "intern", TENANTS: ["north"] };

const GOVERNED_QS: Q[] = [
   ...CALLERS.map((g, i) => ({
      name: `given term, caller ${i}`,
      model: "g.malloy",
      malloy: "run: scoped -> { group_by: region; aggregate: total }",
      givens: g,
   })),
   ...CALLERS.map((g, i) => ({
      name: `gated named view, caller ${i}`,
      model: "g.malloy",
      malloy: "run: cust -> by_segment",
      givens: g,
   })),
   {
      name: "gated named view, denied role",
      model: "g.malloy",
      malloy: "run: cust -> by_segment",
      givens: DENIED,
   },
   {
      name: "gated, no givens at all",
      model: "g.malloy",
      malloy: "run: cust -> by_segment",
   },
   ...CALLERS.map((g, i) => ({
      name: `gated join through a persisted fact, caller ${i}`,
      model: "g.malloy",
      malloy:
         "run: facts -> { group_by: c.segment; aggregate: total, k is c.n }",
      givens: g,
   })),
   {
      name: "gated join, denied role",
      model: "g.malloy",
      malloy: "run: facts -> { group_by: c.segment; aggregate: total }",
      givens: DENIED,
   },
   // Hidden columns, every caller path: each must be refused as live refuses.
   ...[
      ["ad-hoc group_by internal", "run: cust -> { group_by: home_region }"],
      ["ad-hoc private measure", "run: cust -> { aggregate: named }"],
      ["ad-hoc select private", "run: cust -> { select: name }"],
      [
         "nest over internal",
         "run: cust -> { group_by: segment; nest: x is { group_by: home_region } }",
      ],
      [
         "refinement of a view with internal",
         "run: cust -> by_segment + { group_by: home_region }",
      ],
      ["join path to internal", "run: facts -> { group_by: c.home_region }"],
      ["join path to private", "run: facts -> { group_by: c.name }"],
      [
         "filter on internal",
         "run: cust -> { where: home_region = 'north'; aggregate: n }",
      ],
      [
         "extension re-exposing internal as a public dimension",
         "run: cust_ext -> { group_by: region_copy; aggregate: n }",
      ],
      ["select * hides internal", "run: cust -> { select: * }"],
   ].map(([name, malloy]) => ({
      name,
      model: "g.malloy",
      malloy,
      givens: CALLERS[0],
   })),
];

for (const sw of ["cutover"] as const) {
   describe(`[${sw}] colocated gates, givens and hidden columns`, () => {
      let r: Awaited<ReturnType<typeof probe>>;
      it("loads", async () => {
         r = await probe(sw, { "g.malloy": GOVERNED }, GOVERNED_QS);
      }, 300_000);
      for (const q of GOVERNED_QS) {
         it(q.name, () => check(r, q));
      }
   });
}

// A BIGINT beyond 2^53 and a TIMESTAMPTZ column on a colocated table: today's
// manifest path keeps Malloy's own definition; the served copy declares them.
const WIDE = `##! experimental { persistence }
source: wide_raw is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 9007199254740993::BIGINT, 12345678901234567.123456789::DECIMAL(38,9), TIMESTAMPTZ '2026-01-15 23:30:00+00'),
    (2, 42::BIGINT, 0.5::DECIMAL(38,9), TIMESTAMPTZ '2026-02-03 01:00:00+00')
  ) AS t(id, big, amt, tz)
""")

#@ persist name="wide"
source: wide is wide_raw -> { select: id, big, amt }

#@ persist name="wide_tz"
source: wide_tz is wide_raw -> { select: id, tz }
`;
// A BIGINT beyond 2^53 reads back as a double after the cutover (the type:
// declares a plain number, limitation 12): a decision, left out here (A1).
const WIDE_QS: Q[] = [
   {
      name: "a decimal(38,9)",
      model: "w.malloy",
      malloy: "run: wide -> { group_by: id, amt; order_by: id }",
   },
   {
      name: "a timestamptz by day under a zone",
      model: "w.malloy",
      malloy:
         "run: wide_tz -> { timezone: 'America/Los_Angeles'; group_by: d is tz.day; aggregate: n is count() }",
   },
];
for (const sw of ["cutover"] as const) {
   describe(`[${sw}] colocated wide types`, () => {
      let r: Awaited<ReturnType<typeof probe>>;
      it("loads", async () => {
         r = await probe(sw, { "w.malloy": WIDE }, WIDE_QS);
      }, 300_000);
      for (const q of WIDE_QS) {
         it(q.name, () => check(r, q));
         it(`${q.name}: result types`, () => {
            const l = r.live.get(q.name)!;
            const s = r.served.get(q.name)!;
            if (!("answer" in l) || !("answer" in s)) return;
            const t = (f: { name: string; type: unknown }[]) =>
               f.map((x) => [x.name, JSON.stringify(x.type)]);
            expect(
               t(s.answer.fields).map(([n, x]) => [
                  n,
                  x.replace(/,"subtype":"[a-z]+"/g, ""),
               ]),
            ).toEqual(
               t(l.answer.fields).map(([n, x]) => [
                  n,
                  x.replace(/,"subtype":"[a-z]+"/g, ""),
               ]),
            );
         });
      }
   });
}

afterAll(async () => {
   await freshLoadPool();
});

// Which column type keeps a colocated `wide` source on its table under the
// cutover, and a timestamptz column's result types with the build's captured
// schema.
describe("colocated wide types: the table under the cutover", () => {
   const variant = (
      cols: string,
      select: string,
   ) => `##! experimental { persistence }
   source: wide_raw is duckdb.sql("""
     SELECT * FROM (VALUES (1, ${cols})) AS t(id, x)
   """)

   #@ persist name="wide"
   source: wide is wide_raw -> { select: ${select} }
   `;
   for (const [name, cols] of [
      ["int", "5::INTEGER"],
      ["bigint", "5::BIGINT"],
      ["decimal", "0.5::DECIMAL(38,9)"],
      ["double", "0.5::DOUBLE"],
      ["tstz", "TIMESTAMPTZ '2026-01-15 23:30:00+00'"],
      ["varchar", "'a'"],
   ]) {
      it(`a ${name} column is still served from the table under the cutover`, async () => {
         await freshLoadPool();
         const restore = withSwitch("cutover");
         const loader = await loadFixture({
            "w.malloy": variant(cols, "id, x"),
         });
         try {
            const f = await loader.bind();
            const out = await f.run(
               "w.malloy",
               "run: wide -> { group_by: id, x }",
            );
            // Served from the table today (off); must stay so under the cutover.
            expect("answer" in out && out.answer.servedBy).toBe("materialized");
         } finally {
            restore();
            await loader.dispose();
         }
      }, 120_000);
   }

   it("a TIMESTAMP WITH TIME ZONE column keeps live's result type when served from the build's captured schema", async () => {
      await freshLoadPool();
      const restore = withSwitch("cutover");
      const loader = await loadFixture({
         "w.malloy": variant("TIMESTAMPTZ '2026-01-15 23:30:00+00'", "id, x"),
      });
      try {
         const Q =
            "run: wide -> { timezone: 'America/Los_Angeles'; group_by: id, x, d is x.day }";
         const live = await loader.run("w.malloy", Q);
         const f = await loader.bind();
         const wh = (await f.pkg.getMalloyConnection("duckdb")) as unknown as {
            runSQL(s: string, o?: unknown): Promise<{ rows: unknown[] }>;
         };
         const entries: Record<string, unknown> = {};
         for (const s of Object.values(f.pkg.getBuildPlan()?.sources ?? {})) {
            const table = (s.annotationFields?.name || s.name).trim();
            const d = await wh.runSQL(`DESCRIBE "${table}"`, {
               rowLimit: 1000,
            });
            entries[s.sourceEntityId] = {
               tableName: table,
               connectionName: s.connectionName ?? "duckdb",
               schema: (
                  d.rows as { column_name: string; column_type: string }[]
               ).map((r) => ({ name: r.column_name, type: r.column_type })),
            };
         }
         f.pkg.bindColocatedServeManifest({});
         f.pkg.bindColocatedServeManifest(entries as never);
         for (const p of f.pkg.getModelPaths())
            await f.pkg.getModel(p)?.prepareServedCopy();
         const served = await f.run("w.malloy", Q);
         if (!("answer" in live) || !("answer" in served))
            throw new Error("errored");
         expect(served.answer.via).toBe("served_copy");
         // The result types must be live's (today's off path keeps timestamptz).
         // A number's subtype is G6, accepted; compared without it.
         const kinds = (fs: { type: unknown }[]) =>
            fs.map((f) =>
               JSON.stringify(f.type).replace(/,"subtype":"[a-z]+"/g, ""),
            );
         expect(kinds(served.answer.fields)).toEqual(kinds(live.answer.fields));
      } finally {
         restore();
         await loader.dispose();
      }
   }, 120_000);
});
