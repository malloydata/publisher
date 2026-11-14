// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// v1 colocated tables under the v1 cutover. Each query is
// answered live before any table is bound, then under the cutover; the served
// answer must equal live per caller, and a refusal must stay a refusal.

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "./conformance/package_harness";
import { RAW_MODEL } from "./conformance/data";
import { rowDifference } from "./harness";

type Q = {
   name: string;
   model: string;
   malloy: string;
   givens?: Record<string, unknown>;
};

const GATED = `##! experimental { persistence givens access_modifiers }
import "raw.malloy"

given: ROLE :: string
given: REGIONS :: string[]

#(authorize) 'analyst' = $ROLE
#(access_filter) region in $REGIONS
#@ persist name="locked_orders"
source: locked_orders is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

#(access_filter) home_region in $REGIONS
#@ persist name="cust_hidden"
source: cust_hidden is customers_raw -> {
  select: id, segment, home_region
} include { internal: home_region; public: * } extend {
  primary_key: id
  measure: n is count()
}

#@ persist name="facts"
source: facts is orders_raw -> {
  select: id, customer_id, amount
} extend {
  join_one: c is cust_hidden on customer_id = c.id
  measure: total is amount.sum()
}
`;

const DERIVED = `##! experimental { persistence }
import "raw.malloy"

#@ persist name="shadowed"
source: shadowed is orders_raw -> {
  select: id, region, amount
} extend {
  rename: amount_raw is amount
  dimension: amount is amount_raw * 2
  measure: total is amount.sum()
  measure: total_raw is amount_raw.sum()
}

#@ persist name="dropped"
source: dropped is orders_raw -> {
  select: id, region, status, amount
} extend {
  except: status
  measure: total is amount.sum()
}
`;

const GATED_QS: Q[] = [
   ...["north", "west"].map((r) => ({
      name: `locked+filtered, analyst ${r}`,
      model: "g.malloy",
      malloy: "run: locked_orders -> { group_by: region; aggregate: total }",
      givens: { ROLE: "analyst", REGIONS: [r] },
   })),
   {
      name: "locked, wrong role (refused)",
      model: "g.malloy",
      malloy: "run: locked_orders -> { group_by: region; aggregate: total }",
      givens: { ROLE: "intern", REGIONS: ["north"] },
   },
   ...["south", "east"].map((r) => ({
      name: `gate on an internal column, caller ${r}`,
      model: "g.malloy",
      malloy: "run: cust_hidden -> { group_by: segment; aggregate: n }",
      givens: { REGIONS: [r] },
   })),
   {
      name: "internal column still refused past the gate",
      model: "g.malloy",
      malloy: "run: cust_hidden -> { group_by: home_region; aggregate: n }",
      givens: { REGIONS: ["south"] },
   },
   ...["south", "north"].map((r) => ({
      name: `gated join (internal column) through a persisted fact, caller ${r}`,
      model: "g.malloy",
      malloy:
         "run: facts -> { group_by: c.segment; aggregate: total, k is c.n }",
      givens: { REGIONS: [r] },
   })),
];

const DERIVED_QS: Q[] = [
   {
      name: "a computed dimension shadowing a renamed stored column",
      model: "d.malloy",
      malloy:
         "run: shadowed -> { group_by: region; aggregate: total, total_raw }",
   },
   {
      name: "a stored column the extend block excepts",
      model: "d.malloy",
      malloy: "run: dropped -> { group_by: region; aggregate: total }",
   },
];

async function probe(files: Record<string, string>, qs: Q[]) {
   const live = new Map<string, Outcome>();
   const served = new Map<string, Outcome>();
   await freshLoadPool();
   const restore = withSwitch("cutover");
   const loader = await loadFixture({
      ...DATA_FILES,
      "raw.malloy": RAW_MODEL,
      ...files,
   });
   let fixture: LoadedFixture | undefined;
   try {
      for (const q of qs)
         live.set(
            q.name,
            await loader.run(q.model, q.malloy, q.givens as never),
         );
      fixture = await loader.bind();
      console.log(
         "VERDICTS",
         JSON.stringify(fixture.verdicts),
         JSON.stringify(fixture.warnings),
      );
      // Interleave and repeat the callers, as #1637's leak needed.
      for (const round of [0, 1]) {
         for (const q of round ? [...qs].reverse() : qs) {
            const out = await fixture.run(q.model, q.malloy, q.givens as never);
            if (!served.has(q.name)) served.set(q.name, out);
            else {
               const a = served.get(q.name)!;
               if ("answer" in a && "answer" in out) {
                  expect(
                     rowDifference(a.answer.rows, out.answer.rows),
                  ).toBeUndefined();
               }
            }
         }
      }
      return {
         live,
         served,
         buildErrors: fixture.buildErrors,
         warnings: fixture.warnings,
      };
   } finally {
      restore();
      await loader.dispose();
      await freshLoadPool();
   }
}

function check(r: Awaited<ReturnType<typeof probe>>, q: Q) {
   const l = r.live.get(q.name)!;
   const s = r.served.get(q.name)!;
   const report = {
      live: "error" in l ? l.error : `${l.answer.rows.length} rows`,
      served:
         "error" in s
            ? s.error
            : `${s.answer.servedBy}/${s.answer.via ?? "-"} ${s.answer.rows.length} rows`,
   };
   console.log("PROBE", q.name, JSON.stringify(report));
   if ("error" in l) {
      expect("error" in s).toBe(true);
      return;
   }
   if ("error" in s) throw new Error(`served errored: ${s.error}`);
   expect(rowDifference(l.answer.rows, s.answer.rows)).toBeUndefined();
}

describe("v1 cutover on: gates on colocated tables (G3, G2)", () => {
   let r: Awaited<ReturnType<typeof probe>>;
   beforeAll(async () => {
      r = await probe({ "g.malloy": GATED }, GATED_QS);
      console.log("BUILD", JSON.stringify(r.buildErrors));
   }, 300_000);
   for (const q of GATED_QS) it(q.name, () => check(r, q));
});

describe("v1 cutover on: derived schema traps (G1)", () => {
   let r: Awaited<ReturnType<typeof probe>>;
   beforeAll(async () => {
      r = await probe({ "d.malloy": DERIVED }, DERIVED_QS);
      console.log("BUILD", JSON.stringify(r.buildErrors));
   }, 300_000);
   for (const q of DERIVED_QS) it(q.name, () => check(r, q));
});

afterAll(async () => {
   await freshLoadPool();
});

// The same `except:` source with its build's captured schema (a table built
// after 6a), and as a v1 `storage=` source: both read the served copy alone
// under the cutover.
describe("v1 cutover on: a stored column the extend block excepts", () => {
   const Q = "run: dropped -> { group_by: region; aggregate: total }";
   const FILE = DERIVED;
   it("colocated, with the build's captured schema", async () => {
      await freshLoadPool();
      const restore = withSwitch("cutover");
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "d.malloy": FILE,
      });
      try {
         const live = await loader.run("d.malloy", Q);
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
         const served = await f.run("d.malloy", Q);
         if ("error" in live || "error" in served) throw new Error("errored");
         console.log("CAPTURED", served.answer.servedBy, served.answer.via);
         expect(served.answer.via).toBe("served_copy");
      } finally {
         restore();
         await loader.dispose();
         await freshLoadPool();
      }
   }, 300_000);

   it("storage=, served by the served copy", async () => {
      await freshLoadPool();
      const restore = withSwitch("cutover");
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "d.malloy": FILE.replace(
            '#@ persist name="dropped"',
            '#@ persist name="dropped" storage="lake"',
         ),
      });
      try {
         const f = await loader.bind();
         const after = await f.run("d.malloy", Q);
         if ("error" in after) throw new Error(after.error);
         expect(after.answer.servedBy).toBe("materialized");
         expect(after.answer.via).toBe("served_copy");
      } finally {
         restore();
         await loader.dispose();
         await freshLoadPool();
      }
   }, 300_000);
});

// G3's per-answer decision: a gated colocated source whose answer also reads
// a `storage=` table must not be read past its gate without
// PUBLISHER_MATERIALIZE_GATED_SOURCES.
describe("v1 cutover on: a gated colocated source joining a storage= table", () => {
   const FILE = `##! experimental { persistence givens }
import "raw.malloy"

given: REGIONS :: string[]

#@ persist name="cust_lake" storage="lake"
source: cust_lake is customers_raw -> {
  select: id, segment
} extend { primary_key: id }

#(access_filter) region in $REGIONS
#@ persist name="gated_orders"
source: gated_orders is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  join_one: c is cust_lake on customer_id = c.id
  measure: total is amount.sum()
}
`;
   it("is live when the answer reads the storage= table, served when it does not", async () => {
      await freshLoadPool();
      const restore = withSwitch("cutover");
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "j.malloy": FILE,
      });
      try {
         const f = await loader.bind();
         const g = { REGIONS: ["north"] } as never;
         const own = await f.run(
            "j.malloy",
            "run: gated_orders -> { group_by: region; aggregate: total }",
            g,
         );
         const joined = await f.run(
            "j.malloy",
            "run: gated_orders -> { group_by: c.segment; aggregate: total }",
            g,
         );
         if ("error" in own || "error" in joined) throw new Error("errored");
         console.log(
            "MIXED own",
            own.answer.servedBy,
            own.answer.via,
            "joined",
            joined.answer.servedBy,
            joined.answer.via,
         );
         expect(own.answer.via).toBe("served_copy");
         expect(joined.answer.via).toBeUndefined();
      } finally {
         restore();
         await loader.dispose();
         await freshLoadPool();
      }
   }, 300_000);
});

// G2: a hidden stored column renamed in the extend block, its old name
// reused by a public computed dimension. The column must stay as hidden as
// live (a caller that live refuses must be refused served).
describe("v1 cutover on: a renamed internal column whose name is reused", () => {
   const FILE = `##! experimental { persistence access_modifiers }
import "raw.malloy"

#@ persist name="cust_a"
source: cust_a is customers_raw -> {
  select: id, segment, home_region
} extend {
  rename: region_raw is home_region
} include { internal: region_raw; public: * } extend {
  measure: n is count()
}

#@ persist name="cust_b"
source: cust_b is customers_raw -> {
  select: id, segment, home_region
} extend {
  rename: region_raw is home_region
} include { internal: region_raw; public: * } extend {
  dimension: home_region is 'hidden'
  measure: n is count()
  measure: west is count() { where: region_raw = 'west' }
}
`;
   const QS: Q[] = [
      {
         name: "A: renamed internal, grouped",
         model: "h.malloy",
         malloy: "run: cust_a -> { group_by: region_raw; aggregate: n }",
      },
      {
         name: "A: public measure",
         model: "h.malloy",
         malloy: "run: cust_a -> { group_by: segment; aggregate: n }",
      },
      {
         name: "B: renamed internal, grouped",
         model: "h.malloy",
         malloy: "run: cust_b -> { group_by: region_raw; aggregate: n }",
      },
      {
         name: "B: reused name, grouped",
         model: "h.malloy",
         malloy: "run: cust_b -> { group_by: home_region; aggregate: n }",
      },
      {
         name: "B: measure over the renamed internal",
         model: "h.malloy",
         malloy: "run: cust_b -> { group_by: segment; aggregate: west }",
      },
   ];
   let r: Awaited<ReturnType<typeof probe>>;
   beforeAll(async () => {
      r = await probe({ "h.malloy": FILE }, QS);
      console.log(
         "BUILD",
         JSON.stringify(r.buildErrors),
         JSON.stringify(r.warnings.map((w) => w.code)),
      );
   }, 300_000);
   for (const q of QS) it(q.name, () => check(r, q));
});

// Containment (#1700, row 34) under the cutover: one v1 colocated source
// whose re-root does not compile must cost that source alone.
describe("v1 cutover on: one failing re-root costs that source alone", () => {
   const FILE = `##! experimental { persistence access_modifiers }
import "raw.malloy"

#@ persist name="plain"
source: plain is orders_raw -> {
  select: id, region, amount
} extend {
  measure: total is amount.sum()
}

#@ persist name="cust_b"
source: cust_b is customers_raw -> {
  select: id, segment, home_region
} extend {
  rename: region_raw is home_region
} include { internal: region_raw; public: * } extend {
  dimension: home_region is 'hidden'
  measure: n is count()
}
`;
   const QS: Q[] = [
      {
         name: "a sibling source in the same file",
         model: "c.malloy",
         malloy: "run: plain -> { group_by: region; aggregate: total }",
      },
   ];
   let r: Awaited<ReturnType<typeof probe>>;
   beforeAll(async () => {
      r = await probe({ "c.malloy": FILE }, QS);
   }, 300_000);
   it("serves the sibling from its table", () => {
      check(r, QS[0]);
      const s = r.served.get(QS[0].name)!;
      if ("error" in s) throw new Error(s.error);
      expect(s.answer.via).toBe("served_copy");
   });
});

describe("v1 cutover on: containment beside a re-hidden sibling", () => {
   const A = `#@ persist name="cust_a"
source: cust_a is customers_raw -> {
  select: id, segment, home_region
} extend {
  rename: region_raw is home_region
} include { internal: region_raw; public: * } extend {
  measure: n is count()
}
`;
   const B = `#@ persist name="cust_b"
source: cust_b is customers_raw -> {
  select: id, segment, home_region
} extend {
  rename: region_raw is home_region
} include { internal: region_raw; public: * } extend {
  dimension: home_region is 'hidden'
  measure: n is count()
}
`;
   const HEAD = `##! experimental { persistence access_modifiers }
import "raw.malloy"
`;
   const Q: Q = {
      name: "cust_a's public measure",
      model: "x.malloy",
      malloy: "run: cust_a -> { group_by: segment; aggregate: n }",
   };
   for (const [label, text] of [
      ["cust_a alone", HEAD + A],
      ["cust_a beside a failing cust_b", HEAD + A + B],
   ]) {
      it(
         label,
         async () => {
            const r = await probe({ "x.malloy": text }, [Q]);
            check(r, Q);
            const s = r.served.get(Q.name)!;
            if ("error" in s) throw new Error(s.error);
            expect(s.answer.via).toBe("served_copy");
         },
         300_000,
      );
   }
});
