// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover serving v1 `#@ persist` tables: gates on database tables
 * (G3), hidden columns (G2), the derived schema (G1), casts on an older
 * build's table, wide types, a drill expression, and gated queries on rollups.
 * Every answer is compared with live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
   type Switch,
} from "../../../src/test_helpers/conformance/package_runner";
import {
   rowDifference,
   buildScenario,
   disposeScenarios,
   type R4Fixture,
   type R4Query,
   DESTINATION,
   type R4Run,
   type Row,
} from "./harness";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import {
   installSettings,
   useMaterializationOn,
} from "../../../src/test_helpers/settings_env";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";
import { AccessDeniedError } from "../../../src/errors";

/* eslint-disable @typescript-eslint/no-explicit-any -- reads internals */
/**
 * Database v1 `#@ persist` sources behind gates,
 * under the cutover (contracts at read for database, G3), two callers per
 * gate kind, each answer against live (the switch off, before any bind).
 * Gates on the persisted source itself, on a same-file base it extends, and
 * a source gate (#(authorize)). The serve verdict (path report) is recorded.
 */
describe("database gates", () => {
   const MODEL = `##! experimental { persistence givens }
import "raw.malloy"

given: REGIONS :: string[]
given: ROLE :: string

#(access_filter) region in $REGIONS
#@ persist name="own_gate"
source: own_gate is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

#(access_filter) region in $REGIONS
source: gated_base is orders_raw extend {
  measure: n is count()
}
#@ persist name="over_gated_base"
source: over_gated_base is gated_base -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

#(authorize) $ROLE = 'admin'
#@ persist name="authz"
source: authz is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

#@ persist name="plain"
source: plain is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}
#(access_filter) region in $REGIONS
source: gated_ext is plain extend {}
`;

   const FILES = { ...DATA_FILES, "raw.malloy": RAW_MODEL, "m.malloy": MODEL };

   const CASES: [string, string, Record<string, string | string[]>][] = [
      [
         "own gate, caller 1",
         "run: own_gate -> { group_by: region; aggregate: total }",
         { REGIONS: ["north", "east"] },
      ],
      [
         "own gate, caller 2",
         "run: own_gate -> { group_by: region; aggregate: total }",
         { REGIONS: ["west"] },
      ],
      [
         "own gate, no group",
         "run: own_gate -> { group_by: region; aggregate: total }",
         { REGIONS: [] },
      ],
      [
         "over gated base, caller 1",
         "run: over_gated_base -> { group_by: region; aggregate: total }",
         { REGIONS: ["north", "east"] },
      ],
      [
         "over gated base, caller 2",
         "run: over_gated_base -> { group_by: region; aggregate: total }",
         { REGIONS: ["west"] },
      ],
      [
         "authorize, admin",
         "run: authz -> { group_by: region; aggregate: total }",
         { ROLE: "admin" },
      ],
      [
         "authorize, viewer",
         "run: authz -> { group_by: region; aggregate: total }",
         { ROLE: "viewer" },
      ],
      [
         "authorize, default",
         "run: authz -> { group_by: region; aggregate: total }",
         {},
      ],
      [
         "gated extension, caller 1",
         "run: gated_ext -> { group_by: region; aggregate: total }",
         { REGIONS: ["south"] },
      ],
      [
         "gated extension, caller 2",
         "run: gated_ext -> { group_by: region; aggregate: total }",
         { REGIONS: ["north", "west"] },
      ],
      [
         "gated extension, caller join",
         "run: plain extend { join_one: g is gated_ext on g.id = id } -> { group_by: g.region; aggregate: total }",
         { REGIONS: ["south"] },
      ],
      [
         "gated extension, hidden region via nest",
         "run: gated_ext -> { nest: x is { group_by: region; aggregate: total } }",
         { REGIONS: ["south"] },
      ],
   ];

   describe("database v1 gates under the cutover, two callers per kind", () => {
      let restore: () => void = () => {};
      let fixture: LoadedFixture;
      let dispose: () => Promise<void>;
      const live = new Map<string, Outcome>();

      beforeAll(async () => {
         await freshLoadPool();
         restore = withSwitch("cutover");
         const loader = await loadFixture(FILES);
         dispose = loader.dispose;
         restore();
         restore = withSwitch("off");
         for (const [name, q, g] of CASES)
            live.set(name, await loader.run("m.malloy", q, g));
         restore();
         restore = withSwitch("cutover");
         fixture = await loader.bind();
      });
      afterAll(async () => {
         restore();
         await dispose?.();
         await freshLoadPool();
      });

      for (const [name, q, g] of CASES) {
         // Interleave: run each case twice, around the others.
         it(`${name}: as live`, async () => {
            for (let i = 0; i < 2; i++) {
               const out = await fixture.run("m.malloy", q, g);
               const l = live.get(name)!;
               if ("error" in l) {
                  expect("error" in out).toBe(true);
               } else {
                  if ("error" in out) throw new Error(out.error);
                  expect(
                     rowDifference(l.answer.rows, out.answer.rows),
                  ).toBeUndefined();
               }
            }
         });
      }
   });

   describe("the database source over a gated base, today (switch off, tables bound)", () => {
      it("answers each caller with live's rows", async () => {
         await freshLoadPool();
         const restore = withSwitch("off");
         const loader = await loadFixture(FILES);
         try {
            const q =
               "run: over_gated_base -> { group_by: region; aggregate: total }";
            const live1 = await loader.run("m.malloy", q, {
               REGIONS: ["west"],
            });
            const fixture = await loader.bind();
            const out = await fixture.run("m.malloy", q, { REGIONS: ["west"] });
            if ("error" in out || "error" in live1) throw new Error("failed");
            expect(
               rowDifference(live1.answer.rows, out.answer.rows),
            ).toBeUndefined();
         } finally {
            restore();
            await loader.dispose();
            await freshLoadPool();
         }
      }, 120_000);
   });
});

// v1 database tables under the v1 cutover. Each query is answered live before any table is bound,
// then served, callers interleaved and repeated; the served answer must equal
// live per caller by the strict comparison, and a refusal must stay a
// refusal. Wide column types keep their table and live's result types.
describe("database parity", () => {
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

// A row-scoping given term on a database persisted source's extend block.
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
      describe(`[${sw}] database gates, givens and hidden columns`, () => {
         let r: Awaited<ReturnType<typeof probe>>;
         it("loads", async () => {
            r = await probe(sw, { "g.malloy": GOVERNED }, GOVERNED_QS);
         }, 300_000);
         for (const q of GOVERNED_QS) {
            it(q.name, () => check(r, q));
         }
      });
   }

   // A BIGINT beyond 2^53 and a TIMESTAMPTZ column on a database table: today's
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
      describe(`[${sw}] database wide types`, () => {
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

   // Which column type keeps a database `wide` source on its table under the
   // cutover, and a timestamptz column's result types with the build's captured
   // schema.
   describe("database wide types: the table under the cutover", () => {
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
      // Values a read through a double, or a lost fraction, would change.
      for (const [name, cols] of [
         ["int", "2147483647::INTEGER"],
         ["bigint", "9007199254740993::BIGINT"],
         ["decimal", "12345678901234567890.123456789::DECIMAL(38,9)"],
         ["double", "0.1::DOUBLE"],
         ["tstz", "TIMESTAMPTZ '2026-01-15 23:30:00.123456+00'"],
         ["varchar", "'a'"],
      ]) {
         it(`a ${name} column is still served from the table under the cutover, equal to live`, async () => {
            await freshLoadPool();
            const restore = withSwitch("cutover");
            const loader = await loadFixture({
               "w.malloy": variant(cols, "id, x"),
            });
            try {
               const query = "run: wide -> { group_by: id, x }";
               const live = await loader.run("w.malloy", query);
               const f = await loader.bind();
               const out = await f.run("w.malloy", query);
               // Served from the table today (off); must stay so under the cutover.
               expect("answer" in out && out.answer.servedBy).toBe(
                  "materialized",
               );
               // Value for value, each as a string, never through a double.
               const exact = (o: Outcome) =>
                  "answer" in o
                     ? o.answer.rows.map((r) =>
                          Object.values(r).map((v) =>
                             v instanceof Date ? v.toISOString() : String(v),
                          ),
                       )
                     : o.error;
               expect("answer" in live && live.answer.servedBy).toBe("live");
               expect(exact(out)).toEqual(exact(live));
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
            "w.malloy": variant(
               "TIMESTAMPTZ '2026-01-15 23:30:00+00'",
               "id, x",
            ),
         });
         try {
            const Q =
               "run: wide -> { timezone: 'America/Los_Angeles'; group_by: id, x, d is x.day }";
            const live = await loader.run("w.malloy", Q);
            const f = await loader.bind();
            const wh = (await f.pkg.getMalloyConnection(
               "duckdb",
            )) as unknown as {
               runSQL(s: string, o?: unknown): Promise<{ rows: unknown[] }>;
            };
            const entries: Record<string, unknown> = {};
            for (const s of Object.values(
               f.pkg.getBuildPlan()?.sources ?? {},
            )) {
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
            f.pkg.materialization().bind({ database: {} });
            f.pkg.materialization().bind({ database: entries as never });
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
            expect(kinds(served.answer.fields)).toEqual(
               kinds(live.answer.fields),
            );
         } finally {
            restore();
            await loader.dispose();
         }
      }, 120_000);
   });
});

// The drill expression a nested field
// carries must name the author's field, not the served copy's internal column.
describe("a drill expression", () => {
   const SEED = [
      `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 10.0, TIMESTAMP '2026-01-15 00:00:00', 1),
      (2, 20.0, TIMESTAMP '2026-02-01 00:00:00', 0)
   ) AS t(id, amt, ts, flag_int)`,
   ];
   const MODEL = `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="t" storage="lake"
source: t is ev -> {
  select: id, amt, d is sql_date("\${ts}"), flag is sql_boolean("\${flag_int}"), m is ts.month
}
`;
   const drills = (fields: { name: string; type: unknown }[]) =>
      JSON.stringify(fields).match(/drill_expression \{[^}]*\}/g) ?? [];

   for (const [mode, env, uncast] of [
      ["serve_shape uncast", {}, true],
      [
         "served_copy (materialize) cast",
         { PUBLISHER_MATERIALIZE: "true" },
         false,
      ],
      ["cutover uncast", { PUBLISHER_MATERIALIZE: "true" }, true],
      ["cutover cast", { PUBLISHER_MATERIALIZE: "true" }, false],
   ] as const) {
      describe(mode, () => {
         afterAll(disposeScenarios);
         for (const q of [
            "run: t -> { group_by: flag; nest: x is { group_by: d, m; aggregate: s is amt.sum() } }",
         ]) {
            it(q, async () => {
               const restore = installSettings({
                  env: { PERSIST_STORAGE_MODE: "on", ...env },
               }).restore;
               try {
                  const f = await buildScenario({
                     files: { "m.malloy": MODEL },
                     seedSql: SEED,
                     uncast,
                  });
                  const run = await f.run({ name: q, malloy: q });
                  // A drill code must name the author's field, never the internal column.
                  expect(
                     drills(run.servedFields).filter((d) =>
                        d.includes("__stored"),
                     ),
                  ).toEqual([]);
               } finally {
                  restore();
               }
            });
         }
      });
   }
});

/**
 * The v1 cutover's G2 (materialization v2, phase 3 pull request 6): a v1
 * `#@ persist` table, database or `storage=`, holds its source's hidden
 * columns (built from the source's own query), so the served copy binds them
 * and keeps the author's text after the query, which hides them again. A
 * measure over an `internal` column is served from the table, and the column
 * stays exactly as hidden as live: a caller can neither select, group by nor
 * filter on it, nor read a `private` measure, and is refused with live's own
 * words.
 */
describe("hidden columns (G2)", () => {
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
         name: "every column through a caller's include { public: * }",
         model: "dims.malloy",
         malloy:
            "run: customer_dim include { public: * } -> { select: *; order_by: id }",
      },
      {
         name: "a public measure over the internal column",
         model: "dims.malloy",
         malloy:
            "run: customer_dim -> { group_by: segment; aggregate: west_n }",
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
               fixture.warnings.filter(
                  (w) => w.code === "served-copy-not-served",
               ),
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

   for (const variant of ["database", "storage"] as const) {
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
    * in a database one or a `#(materialize)` one, since the served copy hides it
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

   const PERSIST: Record<"database" | "storage", string> = {
      database: "#@ persist",
      storage: '#@ persist storage="lake"',
   };

   for (const variant of ["database", "storage"] as const) {
      describe(`v1 cutover: #(carries) over a ${variant} table's hidden column`, () => {
         let restore: () => void = () => {};
         let fixture: LoadedFixture;
         let dispose: () => Promise<void>;
         const live = new Map<string, Outcome>();
         const queries = [
            "run: customer_c -> { group_by: segment; aggregate: n }",
            "run: customer_c -> { select: id, segment; order_by: id }",
            "run: customer_c -> { group_by: home_region }",
            "run: customer_c -> { select: *; order_by: id }",
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
            for (const q of queries)
               live.set(q, await loader.run("c.malloy", q));
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
            for (const q of [queries[0], queries[1], queries[3]]) {
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
                  ...renderFiles(
                     { name: "carries", files: {} } as never,
                     variant,
                  ),
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
});

// v1 database tables under the v1 cutover. Each query is
// answered live before any table is bound, then under the cutover; the served
// answer must equal live per caller, and a refusal must stay a refusal.
describe("the cutover on", () => {
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
               const out = await fixture.run(
                  q.model,
                  q.malloy,
                  q.givens as never,
               );
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

   describe("v1 cutover on: gates on database tables (G3, G2)", () => {
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
      it("database, with the build's captured schema", async () => {
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
            const wh = (await f.pkg.getMalloyConnection(
               "duckdb",
            )) as unknown as {
               runSQL(s: string, o?: unknown): Promise<{ rows: unknown[] }>;
            };
            const entries: Record<string, unknown> = {};
            for (const s of Object.values(
               f.pkg.getBuildPlan()?.sources ?? {},
            )) {
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
            f.pkg.materialization().bind({ database: {} });
            f.pkg.materialization().bind({ database: entries as never });
            for (const p of f.pkg.getModelPaths())
               await f.pkg.getModel(p)?.prepareServedCopy();
            const served = await f.run("d.malloy", Q);
            if ("error" in live || "error" in served)
               throw new Error("errored");
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

   // G3's per-answer decision: a gated database source whose answer also reads
   // a `storage=` table must not be read past its gate without
   // PUBLISHER_MATERIALIZE_GATED_SOURCES.
   describe("v1 cutover on: a gated database source joining a storage= table", () => {
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

   // Containment (#1700, row 34) under the cutover: one v1 database source
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
});

// 6b / G5: a v1 `storage=` table an older build typed, under the v1 cutover. Every answer must equal
// live by both the R4 rule and the strict rule (answer_difference.ts), with the same wire
// types, and a query reading a left-out column must answer live.
describe("casts on stored columns (6b)", () => {
   useMaterializationOn();
   afterAll(async () => {
      await disposeScenarios();
   });

   async function expectEqualLive(fixture: R4Fixture, q: R4Query) {
      const run = await fixture.run(q);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(strictRowDifference(run.live, run.served)).toBeUndefined();
      // G6 (a number's subtype) is accepted, and a nest's reference ids differ
      // per compile: neither is compared.
      const shape = (f: typeof run.liveFields) =>
         f.map((x) => [
            x.name,
            JSON.stringify(x.type)
               .replace(/,"subtype":"(integer|decimal|bigint)"/g, "")
               .replace(/reference_id = \\"[^\\]*\\"/g, "reference_id"),
         ]);
      expect(shape(run.servedFields)).toEqual(shape(run.liveFields));
      if (q.expectLive) expect(run.servedFrom).not.toBe("storage");
      else expect(run.servedFrom).toBe("storage");
      return run;
   }

   const SEED = [
      `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 12345678901234567.123456789::DECIMAL(38,9), 9007199254740993::BIGINT, 170141183460469231731687303715884105727::HUGEINT, TIMESTAMP '2026-01-15 23:30:00', TIMESTAMPTZ '2026-01-15 23:30:00+00', '007', '2026-01-31', 1, TIMESTAMP '2026-01-15 00:00:00', TIMESTAMPTZ '2026-01-15 00:00:00+00'),
      (2, 0.000000001::DECIMAL(38,9), -9007199254740993::BIGINT, 1::HUGEINT, TIMESTAMP '2026-02-03 01:00:00', TIMESTAMPTZ '2026-02-03 01:00:00+00', '7', '2026-02-28', 0, TIMESTAMP '2026-02-03 00:00:00', TIMESTAMPTZ '2026-02-03 00:00:00+00'),
      (3, NULL::DECIMAL(38,9), NULL::BIGINT, NULL::HUGEINT, NULL::TIMESTAMP, NULL::TIMESTAMPTZ, NULL, NULL, NULL, NULL, NULL),
      (4, 5.5::DECIMAL(38,9), 42::BIGINT, 2::HUGEINT, TIMESTAMP '2026-02-28 12:00:00', TIMESTAMPTZ '2026-02-28 12:00:00+00', '0007', '2026-03-01', 1, TIMESTAMP '2026-02-28 00:00:00', TIMESTAMPTZ '2026-02-28 00:00:00+00')
   ) AS t(id, amt, big, huge, ts, tz, code, d_text, flag_int, dts, dtz)`,
   ];

   const MODEL = `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="typed" storage="lake"
source: typed is ev -> {
  select:
    id, amt, big, huge, ts, tz, code,
    d_from_ts is sql_date("\${dts}"),
    d_from_tz is sql_date("\${dtz}"),
    month_ts is sql_date("date_trunc('month', \${ts})"),
    d_from_text is sql_date("\${d_text}"),
    flag is sql_boolean("\${flag_int}"),
    code_num is sql_number("\${code}")
} extend {
  measure: total is amt.sum()
}

source: other is ev extend {
  join_one: typed on typed.id = id
}
`;

   describe("6b: casts on an older build's table under on", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": MODEL },
            seedSql: SEED,
            uncast: true,
         });
      }, 120_000);

      const stored: R4Query[] = [
         {
            name: "decimal 38,9 exact",
            malloy:
               "run: typed -> { group_by: id, amt; aggregate: total; order_by: id }",
         },
         // A BIGINT or HUGEINT beyond 2^53 reads back as a double from any
         // stored table (the type: declares a plain number, limitation 12): left
         // out here, a decision for the cutover (A1).
         {
            name: "leading-zero strings, grouped and filtered",
            malloy:
               "run: typed -> { where: code = '007' or code = '0007'; group_by: code; aggregate: n is count() }",
         },
         {
            name: "a boolean cast from 2, 0, NULL, 1: grouped",
            malloy: "run: typed -> { group_by: flag; aggregate: n is count() }",
         },
         {
            name: "a boolean cast: filtered true and not",
            malloy:
               "run: typed -> { aggregate: t is count() { where: flag }, f is count() { where: not flag }, u is count() { where: flag = null } }",
         },
         {
            name: "a date cast from a timestamp, with NULL, grouped and filtered",
            malloy:
               "run: typed -> { where: d_from_ts >= @2026-02-01 or d_from_ts = null; group_by: d_from_ts; aggregate: n is count() }",
         },
         {
            name: "a date cast from a timestamp, by month and year",
            malloy:
               "run: typed -> { group_by: m is d_from_ts.month, y is d_from_ts.year; aggregate: n is count() }",
         },
         {
            name: "a date cast from a timestamp under a query time zone",
            malloy:
               "run: typed -> { timezone: 'Asia/Tokyo'; group_by: d_from_ts; aggregate: n is count() }",
         },
         {
            name: "a date cast from a timestamptz (no source time zone)",
            malloy:
               "run: typed -> { group_by: d_from_tz; aggregate: n is count() }",
         },
         {
            name: "a date cast from a timestamptz under a QUERY time zone",
            malloy:
               "run: typed -> { timezone: 'Asia/Tokyo'; group_by: d_from_tz; aggregate: n is count() }",
         },
         {
            name: "a date cast from a timestamptz, filtered under a query time zone",
            malloy:
               "run: typed -> { timezone: 'America/Los_Angeles'; where: d_from_tz = @2026-01-15; aggregate: n is count() }",
         },
         {
            name: "a truncation stored as a timestamp, by quarter",
            malloy:
               "run: typed -> { group_by: q is month_ts.quarter; aggregate: total }",
         },
         {
            name: "timestamp vs timestamptz columns, by hour under a zone",
            malloy:
               "run: typed -> { timezone: 'America/New_York'; group_by: h1 is ts.hour, h2 is tz.hour; aggregate: n is count() }",
         },
         {
            name: "through a join: a cast boolean and a cast date (across connections: live by design)",
            malloy:
               "run: other -> { group_by: typed.flag, typed.d_from_ts; aggregate: n is count() }",
            expectLive: true,
         },
         {
            name: "nest over a cast column",
            malloy:
               "run: typed -> { group_by: flag; nest: by_day is { group_by: d_from_ts; aggregate: total } }",
         },
      ];
      for (const q of stored) {
         it(q.name, async () => {
            await expectEqualLive(fixture, q);
         });
      }

      const live: R4Query[] = [
         {
            name: "a date stored as text is left out: reading it answers live",
            malloy:
               "run: typed -> { group_by: d_from_text; aggregate: n is count() }",
            expectLive: true,
         },
         {
            name: "mixed: a left-out column with stored and cast ones",
            malloy:
               "run: typed -> { group_by: d_from_text, flag, code; aggregate: total }",
            expectLive: true,
         },
         {
            name: "a left-out number (leading zeros as text) filtered",
            malloy:
               "run: typed -> { where: code_num = 7; aggregate: n is count() }",
            expectLive: true,
         },
         {
            name: "a left-out column through a join",
            malloy:
               "run: other -> { group_by: typed.d_from_text; aggregate: n is count() }",
            expectLive: true,
         },
         {
            name: "a left-out column in a nest",
            malloy:
               "run: typed -> { group_by: flag; nest: x is { group_by: code_num } }",
            expectLive: true,
         },
      ];
      for (const q of live) {
         it(q.name, async () => {
            await expectEqualLive(fixture, q);
         });
      }
   });

   // A rename over a cast column, and an except of one.
   describe("6b: the author's extend block over a cast column", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: {
               "m.malloy": `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="renamed" storage="lake"
source: renamed is ev -> {
  select: id, amt, flag is sql_boolean("\${flag_int}"), d is sql_date("\${dts}")
} extend {
  rename: is_on is flag
  rename: on_day is d
  measure: total is amt.sum()
}

#@ persist name="excepted" storage="lake"
source: excepted is ev -> {
  select: id, amt, flag is sql_boolean("\${flag_int}"), d is sql_date("\${dts}")
} extend {
  except: flag
  measure: total is amt.sum()
}
`,
            },
            seedSql: SEED,
            uncast: true,
         });
      }, 120_000);
      for (const q of [
         {
            name: "renamed cast boolean and date",
            malloy:
               "run: renamed -> { group_by: is_on, on_day; aggregate: total }",
         },
         {
            name: "renamed cast date by month",
            malloy:
               "run: renamed -> { group_by: m is on_day.month; aggregate: total }",
         },
         {
            name: "except of a cast column, rest served",
            malloy: "run: excepted -> { group_by: d; aggregate: total }",
         },
      ]) {
         it(q.name, async () => {
            await expectEqualLive(fixture, q);
         });
      }
   });

   // withoutLeftOutColumn matches an undefined name against every candidate's
   // left-out columns. With another source naming its same-named left-out column
   // (`loud_ext`), today the quiet source also answers live, rows equal: a lost
   // speedup only, left as is (A6), so only the plain case runs.
   for (const withLoudExt of [false])
      describe(`6b: two sources with a same-named left-out column (loud_ext ${withLoudExt})`, () => {
         let fixture: R4Fixture;
         beforeAll(async () => {
            fixture = await buildScenario({
               files: {
                  "m.malloy": `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="quiet" storage="lake"
source: quiet is ev -> {
  select: id, amt, code_num is sql_number("\${code}")
} extend { measure: total is amt.sum() }

#@ persist name="loud" storage="lake"
source: loud is ev -> {
  select: id, amt, code_num is sql_number("\${code}")
}

${withLoudExt ? "source: loud_ext is loud extend { dimension: twice is code_num * 2 }" : ""}
`,
               },
               seedSql: SEED,
               uncast: true,
            });
         }, 120_000);
         if (withLoudExt)
            it("the source whose left-out column is named answers live", async () => {
               await expectEqualLive(fixture, {
                  name: "loud",
                  malloy: "run: loud -> { aggregate: s is amt.sum() }",
                  expectLive: true,
               });
            });
         it("the other source keeps its table", async () => {
            await expectEqualLive(fixture, {
               name: "quiet",
               malloy: "run: quiet -> { aggregate: total }",
            });
         });
      });

   // The same date and boolean casts over values a cast keeps faithful (midnight
   // timestamps, 0/1 flags): a date read under a query or source time zone.
   const MIDNIGHT_SEED = [
      `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 10.0, TIMESTAMP '2026-01-15 00:00:00', TIMESTAMPTZ '2026-01-15 00:00:00+00', 1),
      (2, 20.0, TIMESTAMP '2026-02-01 00:00:00', TIMESTAMPTZ '2026-02-01 00:00:00+00', 0),
      (3, NULL, NULL, NULL, NULL),
      (4, 5.0, TIMESTAMP '2026-03-31 00:00:00', TIMESTAMPTZ '2026-03-31 00:00:00+00', 1)
   ) AS t(id, amt, ts, tz, flag_int)`,
   ];
   describe("6b: faithful casts under time zones", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: {
               "m.malloy": `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="mid" storage="lake"
source: mid is ev -> {
  select: id, amt,
    d_ts is sql_date("\${ts}"),
    d_tz is sql_date("\${tz}"),
    flag is sql_boolean("\${flag_int}")
} extend { measure: total is amt.sum() }

#@ persist name="mid_zoned" storage="lake"
source: mid_zoned is ev -> {
  timezone: 'Asia/Tokyo'
  select: id, amt, d_ts is sql_date("\${ts}"), d_tz is sql_date("\${tz}")
}
`,
            },
            seedSql: MIDNIGHT_SEED,
            uncast: true,
         });
      }, 120_000);
      for (const q of [
         "run: mid -> { group_by: d_ts, d_tz, flag; aggregate: total }",
         "run: mid -> { timezone: 'Asia/Tokyo'; group_by: d_ts, d_tz; aggregate: total }",
         "run: mid -> { timezone: 'America/Los_Angeles'; group_by: d_ts, d_tz; aggregate: total }",
         "run: mid -> { timezone: 'America/Los_Angeles'; where: d_tz = @2026-01-15; aggregate: n is count() }",
         "run: mid -> { timezone: 'America/Los_Angeles'; group_by: w is d_tz.week, m is d_ts.month; aggregate: n is count() }",
         "run: mid -> { where: flag; group_by: d_ts.month; aggregate: total }",
         "run: mid_zoned -> { group_by: d_ts; aggregate: n is count() }",
         "run: mid_zoned -> { aggregate: s is amt.sum() }",
      ]) {
         it(q, async () => {
            await expectEqualLive(fixture, {
               name: q,
               malloy: q,
               expectLive: q.includes("mid_zoned -> { group_by: d_tz"),
            });
         });
      }
      it("a zoned source's instant date answers live", async () => {
         await expectEqualLive(fixture, {
            name: "zoned d_tz",
            malloy:
               "run: mid_zoned -> { group_by: d_tz; aggregate: n is count() }",
            expectLive: true,
         });
      });
   });
});

/**
 * R4 for gated queries on rollups (known limitation 1, R3 for rollups: route,
 * then govern). With PUBLISHER_MATERIALIZE_GATED_SOURCES on, a query whose
 * entry point carries a row-level gate or lock is answered from a
 * `#(rollup)` rollup over its source when every gate is provable on
 * the rollup (each field it reads a grain dimension): the run target is
 * redeclared over the rollup in its declaring file, gates written on it, and
 * `govern()` grafts and proves them there.
 *
 * Two callers per gate kind, interleaved and repeated: each answer equals
 * live, is served by the rollup and holds only that caller's groups; the
 * origin table emptied proves the rollup is read; a caller with no givens is
 * denied. Negatives answer live (a gate field outside the grain, a gate
 * through a join, a caller join, a caller `count()`, an extension whose own
 * gate the rollup cannot prove), and a caller `except:` or `rename:` of the
 * gate field is denied, as live is. Database and `storage=` (the latter
 * behind its review seam, as question 14 leaves it off in production).
 */
describe("gated rollups", () => {
   const seedSql = [
      `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'A', 'east', 'AA', TIMESTAMP '2026-01-01 08:00:00'),
      (2, 20.0, 'A', 'west', 'BB', TIMESTAMP '2026-01-01 09:00:00'),
      (3, 30.0, 'B', 'east', 'AA', TIMESTAMP '2026-02-01 10:00:00'),
      (4,  5.0, 'B', 'west', 'CC', TIMESTAMP '2026-02-01 11:00:00'),
      (5, 12.5, 'C', 'west', 'AA', TIMESTAMP '2026-01-01 23:30:00'),
      (6,  7.5, 'C', 'east', 'BB', TIMESTAMP '2026-02-01 12:00:00')
    ) AS t(id, amount, category, region, carrier, order_time)`,
      `CREATE TABLE lines_raw AS SELECT * FROM (VALUES
      (1, 2.0), (1, 3.0), (2, 4.0), (3, 1.0), (5, 6.0), (6, 2.5)
    ) AS t(order_id, qty)`,
      `CREATE TABLE carriers_raw AS SELECT * FROM (VALUES
      ('AA', 5), ('BB', 7), ('CC', 11)
    ) AS t(code, fleet)`,
   ];

   function model(placement: string): string {
      const p = placement;
      return `##! experimental { givens access_modifiers }
given: REGION :: string
given: REG :: string
given: ON_DAY :: timestamp
given: ROLE :: string
given: FLEET :: number
given: CARRIER :: string

source: carriers is duckdb.table('carriers_raw') extend { primary_key: code }

// Ungated, so the companion is never empty.
source: open is duckdb.table('orders_raw') extend {
  #@ preaggregate grain="category"${p}
  measure: open_total is amount.sum()
}

#(access_filter) region = $REGION
source: by_column is duckdb.table('orders_raw') extend {
  #@ preaggregate grain="category,region"${p}
  measure: total is amount.sum()
  #@ preaggregate grain="category,region"${p}
  measure: biggest is amount.max()
  view: by_category is { group_by: category; aggregate: total }
}

#(access_filter) reg = $REG
source: by_computed is duckdb.table('orders_raw') extend {
  dimension: reg is upper(region)
  #@ preaggregate grain="category,reg"${p}
  measure: total is amount.sum()
}

#(access_filter) order_day = $ON_DAY
source: by_truncation is duckdb.table('orders_raw') extend {
  dimension: order_day is order_time.day
  #@ preaggregate grain="category,order_day"${p}
  measure: total is amount.sum()
}

// A source with its own time zone: the rollup truncated in it, and the
// variant reads the stored times in none, so it is never read (TIME_ZONE).
#(access_filter) order_day = $ON_DAY
source: by_zoned_truncation is duckdb.table('orders_raw') extend {
  timezone: 'America/New_York'
  dimension: order_day is order_time.day
  #@ preaggregate grain="category,order_day"${p}
  measure: total is amount.sum()
}

#(access_filter) tenant = $REGION
source: by_internal is duckdb.table('orders_raw') extend {
  internal dimension: tenant is region
  #@ preaggregate grain="category,tenant"${p}
  measure: total is amount.sum()
}

#(access_filter) region = $REGION
source: gated_base is duckdb.table('orders_raw')
source: by_inherited is gated_base extend {
  #@ preaggregate grain="category,region"${p}
  measure: total is amount.sum()
  // A column of its own, so its table is not another source's.
  #@ preaggregate grain="category,region"${p}
  measure: inh_n is count()
}

#(authorize) 'admin' = $ROLE
source: by_lock is duckdb.table('orders_raw') extend {
  #@ preaggregate grain="category"${p}
  measure: total is amount.sum()
}

#(authorize) 'admin' = $ROLE
#(access_filter) region = $REGION
source: by_lock_and_filter is duckdb.table('orders_raw') extend {
  #@ preaggregate grain="category,region"${p}
  measure: total is amount.sum()
  // A column of its own, so its table is not another source's.
  #@ preaggregate grain="category,region"${p}
  measure: lf_n is count()
}

#(authorize) false
source: locked is duckdb.table('orders_raw')

#(authorize) true
#(access_filter) region = $REGION
source: reopened is locked extend {
  #@ preaggregate grain="category,region"${p}
  measure: total is amount.sum()
  // A column of its own, so its table is not another source's.
  #@ preaggregate grain="category,region"${p}
  measure: ro_n is count()
}

// An extension of a gated rollup base (row 22): one adding only fields is
// answered from its base's rollup past the inherited gate; one adding a gate
// on a field the rollup does not keep is not.
source: ext_open is by_column extend {
  dimension: cat_lower is lower(category)
}
#(access_filter) carrier = $CARRIER
source: ext_gated is by_column extend {}

// A gated parent joining a rollup's base (A7): the join branch redeclares the
// parent around the join, carrying no gate to prove, so it declines.
source: order_lines is duckdb.table('lines_raw') extend {
  #@ preaggregate grain="order_id"${p}
  measure: qty_total is qty.sum()
}
#(access_filter) region = $REGION
source: gated_parent is duckdb.table('orders_raw') extend {
  primary_key: id
  join_many: order_lines on order_lines.order_id = id
}

// Negatives: never built (A4), so answered live.
#(access_filter) region = $REGION
source: outside is duckdb.table('orders_raw') extend {
  #@ preaggregate grain="category"${p}
  measure: total is amount.sum()
}

#(access_filter) carriers.fleet = $FLEET
source: through_join is duckdb.table('orders_raw') extend {
  join_one: carriers with carrier
  #@ preaggregate grain="category,carrier"${p}
  measure: total is amount.sum()
}
`;
   }

   interface Caller {
      name: string;
      givens: Record<string, string | number>;
      /** The rows this caller's gate admits, as SQL over `orders_raw`. */
      admits: string;
   }

   interface GateCase {
      kind: string;
      source: string;
      callers: [Caller, Caller];
      /** A query grouping by the gate's (public) field, to check each row. */
      byGateField?: {
         malloy: string;
         column: string;
         value: (c: Caller) => unknown;
      };
   }

   const EAST: Caller = {
      name: "east",
      givens: { REGION: "east", ROLE: "admin" },
      admits: "region = 'east'",
   };
   const WEST: Caller = {
      name: "west",
      givens: { REGION: "west", ROLE: "admin" },
      admits: "region = 'west'",
   };

   const CASES: GateCase[] = [
      {
         kind: "a column",
         source: "by_column",
         callers: [EAST, WEST],
         byGateField: {
            malloy:
               "run: by_column -> { group_by: region, category; aggregate: total }",
            column: "region",
            value: (c) => c.givens.REGION,
         },
      },
      {
         kind: "a computed dimension",
         source: "by_computed",
         callers: [
            {
               name: "EAST",
               givens: { REG: "EAST" },
               admits: "upper(region) = 'EAST'",
            },
            {
               name: "WEST",
               givens: { REG: "WEST" },
               admits: "upper(region) = 'WEST'",
            },
         ],
         byGateField: {
            malloy:
               "run: by_computed -> { group_by: reg, category; aggregate: total }",
            column: "reg",
            value: (c) => c.givens.REG,
         },
      },
      {
         kind: "a truncation",
         source: "by_truncation",
         callers: [
            {
               name: "jan 1",
               givens: { ON_DAY: "2026-01-01 00:00:00" },
               admits: "date_trunc('day', order_time) = DATE '2026-01-01'",
            },
            {
               name: "feb 1",
               givens: { ON_DAY: "2026-02-01 00:00:00" },
               admits: "date_trunc('day', order_time) = DATE '2026-02-01'",
            },
         ],
      },
      {
         kind: "an internal gate field",
         source: "by_internal",
         callers: [EAST, WEST],
      },
      {
         kind: "an inherited gate on an extension's own rollup",
         source: "by_inherited",
         callers: [EAST, WEST],
      },
      {
         kind: "a lock alone, at a grain with no gate field",
         source: "by_lock",
         callers: [
            { name: "admin", givens: { ROLE: "admin" }, admits: "true" },
            { name: "admin again", givens: { ROLE: "admin" }, admits: "true" },
         ],
      },
      {
         kind: "a lock and a filter",
         source: "by_lock_and_filter",
         callers: [EAST, WEST],
      },
      {
         kind: "a locked base re-opened by true beside a filter",
         source: "reopened",
         callers: [EAST, WEST],
      },
      {
         kind: "an extension of a gated base that adds only fields (row 22)",
         source: "ext_open",
         callers: [EAST, WEST],
      },
   ];

   /** The truth: the caller's admitted rows, totalled by category. */
   function truthByCategory(c: Caller): string {
      return `SELECT category, CAST(SUM(amount) AS DOUBLE) AS total FROM orders_raw WHERE ${c.admits} GROUP BY 1`;
   }

   function expectRollup(run: R4Run): void {
      expect(run.servedBy?.kind).toBe("rollup");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   }

   function expectLive(run: R4Run): void {
      expect(run.servedBy?.kind).not.toBe("rollup");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   }

   for (const placement of ["database", "storage"] as const) {
      describe(`v1 cutover R4: gated queries on ${placement} rollups (row 1, gated sources on)`, () => {
         let restore: () => void;
         let fixture: R4Fixture;
         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_MATERIALIZE: "true",
                  PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
               },
            }).restore;
            fixture = await buildScenario({
               files: {
                  "m.malloy": model(
                     placement === "storage" ? ` storage="${DESTINATION}"` : "",
                  ),
               },
               seedSql,
               preaggregate: true,
            });
         });
         afterAll(async () => {
            restore();
            await disposeScenarios();
         });

         for (const c of CASES) {
            it(`answers two callers through ${c.kind} from the rollup, each equal to live and to its own rows`, async () => {
               // Interleaved and repeated: A, B, A, B.
               for (const caller of [...c.callers, ...c.callers]) {
                  const run = await fixture.run({
                     name: `${c.source} ${caller.name}`,
                     malloy: `run: ${c.source} -> { group_by: category; aggregate: total }`,
                     givens: caller.givens,
                     truthSql: truthByCategory(caller),
                  });
                  expectRollup(run);
                  expect(
                     rowDifference(run.truth ?? [], run.served),
                  ).toBeUndefined();
                  expect(run.served.length).toBeGreaterThan(0);
                  const grand = await fixture.run({
                     name: `${c.source} ${caller.name} total`,
                     malloy: `run: ${c.source} -> { aggregate: total }`,
                     givens: caller.givens,
                  });
                  expectRollup(grand);
               }
            });
            if (c.byGateField) {
               const g = c.byGateField;
               it(`holds only each caller's groups through ${c.kind}`, async () => {
                  for (const caller of c.callers) {
                     const run = await fixture.run({
                        name: `${c.source} ${caller.name} by gate field`,
                        malloy: g.malloy,
                        givens: caller.givens,
                     });
                     expectRollup(run);
                     expect(run.served.length).toBeGreaterThan(0);
                     for (const row of run.served) {
                        expect(row[g.column]).toBe(g.value(caller));
                     }
                  }
               });
            }
         }

         it("governs each answer on the author's source: its name, gates and decision equal live's", async () => {
            // The variant keeps the run target's name and `sourceID`, so the
            // governor's fold of saved gates by name and its gate-set equality
            // see the same gates on the routed compile as on live.
            const { unrecordedAudit } = await import(
               "../../../src/service/access_record"
            );
            const { QueryPrivileges } = await import(
               "../../../src/service/model_query_request"
            );
            const governed = async (
               malloy: string,
               givens: Caller["givens"],
            ) => {
               const audit = unrecordedAudit();
               const result = await fixture.model.runQuery(
                  audit,
                  { query: malloy, filterParams: {}, givens },
                  QueryPrivileges.of({ bypassFilters: true }),
               );
               return { audit, result };
            };
            for (const c of CASES) {
               const malloy = `run: ${c.source} -> { group_by: category; aggregate: total }`;
               const caller = c.callers[0];
               fixture.setBindings([]);
               const live = await governed(malloy, caller.givens);
               fixture.setBindings(fixture.bindings);
               const served = await governed(malloy, caller.givens);
               expect(served.result.path?.servedBy?.kind).toBe("rollup");
               expect(served.audit.summary?.source).toBe(
                  live.audit.summary?.source,
               );
               expect(served.audit.summary?.decision).toBe(
                  live.audit.summary?.decision,
               );
               const sorted = (g?: {
                  authorize: string[];
                  accessFilter: string[];
               }) =>
                  g && {
                     authorize: [...new Set(g.authorize)].sort(),
                     accessFilter: [...new Set(g.accessFilter)].sort(),
                  };
               expect(sorted(served.audit.summary?.gates)).toEqual(
                  sorted(live.audit.summary?.gates),
               );
               expect(
                  (served.audit.summary?.gates.authorize.length ?? 0) +
                     (served.audit.summary?.gates.accessFilter.length ?? 0),
               ).toBeGreaterThan(0);
            }
         });

         it("redeclares an internal gate field internal on the rollup's variant", async () => {
            await fixture.run({
               name: "internal",
               malloy:
                  "run: by_internal -> { group_by: category; aggregate: total }",
               givens: EAST.givens,
            });
            const compiled = (
               fixture.model as unknown as {
                  rollupVariants: {
                     compiled: Map<
                        string,
                        Promise<{
                           modelDef?: { contents?: Record<string, unknown> };
                        }>
                     >;
                  };
               }
            ).rollupVariants.compiled;
            const defs = await Promise.all([...compiled.values()]);
            const fields = defs
               .map(
                  (d) =>
                     (
                        d.modelDef?.contents?.by_internal as
                           | {
                                fields?: {
                                   name?: string;
                                   as?: string;
                                   accessModifier?: string;
                                }[];
                             }
                           | undefined
                     )?.fields,
               )
               .find((f) => f !== undefined);
            const tenant = fields?.find((f) => (f.as ?? f.name) === "tenant");
            expect(tenant?.accessModifier).toBe("internal");
         });

         it("answers a named view on a gated source from the rollup", async () => {
            for (const caller of [EAST, WEST]) {
               expectRollup(
                  await fixture.run({
                     name: `view ${caller.name}`,
                     malloy: "run: by_column -> by_category",
                     givens: caller.givens,
                     truthSql: truthByCategory(caller),
                  }),
               );
            }
         });

         it("denies a caller with no givens, as live does", async () => {
            fixture.setBindings(fixture.bindings);
            for (const source of ["by_column", "by_lock", "by_internal"]) {
               await expect(
                  fixture.runAsIs(
                     `run: ${source} -> { group_by: category; aggregate: total }`,
                  ),
               ).rejects.toThrow(AccessDeniedError);
            }
         });

         it("denies a caller the lock refuses, as live does", async () => {
            fixture.setBindings(fixture.bindings);
            await expect(
               fixture.runAsIs(
                  "run: by_lock -> { group_by: category; aggregate: total }",
                  { ROLE: "viewer" },
               ),
            ).rejects.toThrow(AccessDeniedError);
         });

         const NEGATIVES: [string, string, Record<string, string | number>][] =
            [
               [
                  "a gate field outside the grain (never built)",
                  "run: outside -> { group_by: category; aggregate: total }",
                  EAST.givens,
               ],
               [
                  "a gate through a join (never built)",
                  "run: through_join -> { group_by: category; aggregate: total }",
                  { FLEET: 5 },
               ],
               [
                  "a caller count()",
                  "run: by_column -> { group_by: category; aggregate: n is count() }",
                  EAST.givens,
               ],
               [
                  "an extension whose own gate the rollup cannot prove",
                  "run: ext_gated -> { group_by: category; aggregate: total }",
                  { ...EAST.givens, CARRIER: "AA" },
               ],
            ];
         for (const [label, malloy, givens] of NEGATIVES) {
            it(`answers ${label} live, equal to live`, async () => {
               expectLive(await fixture.run({ name: label, malloy, givens }));
            });
         }

         it("never answers a source with its own time zone from its rollup: a truncation gate admits only live's rows", async () => {
            // Never read, so never planned or built (#1686's decision 1).
            expect(
               Object.keys(fixture.rollupTables).filter((n) =>
                  n.startsWith("by_zoned_truncation"),
               ),
            ).toEqual([]);
            // 05:00 UTC is midnight in New York: read without the source's zone,
            // the stored days equal it, while live's New York days never do.
            for (const day of [
               "2026-01-01 00:00:00",
               "2026-01-01 05:00:00",
               "2026-02-01 00:00:00",
               "2026-02-01 05:00:00",
            ]) {
               for (const malloy of [
                  "run: by_zoned_truncation -> { group_by: category; aggregate: total }",
                  "run: by_zoned_truncation -> { group_by: category, order_day; aggregate: total }",
               ]) {
                  expectLive(
                     await fixture.run({
                        name: `zoned ${day}`,
                        malloy,
                        givens: { ON_DAY: day },
                     }),
                  );
               }
            }
         });

         it("reports an unprovable extension's skip as CONTRACT without naming its gate", async () => {
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(
               "run: ext_gated -> { group_by: category; aggregate: total }",
               { ...EAST.givens, CARRIER: "AA" },
            );
            const skipped = (path?.considered ?? []).filter(
               (c) => (c as { reason?: string }).reason === "ROW_LEVEL_GATE",
            );
            expect(skipped.length).toBeGreaterThan(0);
            for (const c of skipped) {
               const text = JSON.stringify(c);
               expect(text).not.toContain("carrier");
               expect(text).not.toContain("CARRIER");
               expect(text).not.toContain("region");
            }
         });

         it("answers a joined source's rollup under a gated parent (A7) as live: from the rollup on the database tier (g-mixed), live without naming the gate in a storage destination it is not stored in", async () => {
            const malloy =
               "run: gated_parent -> { group_by: category; aggregate: order_lines.qty_total }";
            const run = await fixture.run({
               name: "gated parent",
               malloy,
               givens: EAST.givens,
            });
            if (placement === "database") {
               // The parent's own gate (a plain column) is grafted on the
               // variant, which keeps the author's rows (question 14).
               expect(run.servedBy?.kind).toBe("rollup");
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               return;
            }
            // In a storage destination the parent here is not stored there,
            // so the branch declines (`CROSS_CONNECTION`) before its gate is
            // read; no decline names the gate.
            expectLive(run);
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(malloy, EAST.givens);
            const skipped = path?.considered ?? [];
            expect(skipped.length).toBeGreaterThan(0);
            for (const c of skipped) {
               expect(JSON.stringify(c)).not.toContain("region");
            }
         });

         it("answers a caller join into a gated source as live does", async () => {
            const malloy =
               "run: carriers extend { join_many: o is by_column on o.carrier = code } -> { group_by: code; aggregate: t is o.total }";
            fixture.setBindings([]);
            const live = await fixture
               .runAsIs(malloy, EAST.givens)
               .then((r) => ({ ok: true as const, r }))
               .catch(() => ({ ok: false as const }));
            fixture.setBindings(fixture.bindings);
            const served = await fixture
               .runAsIs(malloy, EAST.givens)
               .then((r) => ({ ok: true as const, r }))
               .catch(() => ({ ok: false as const }));
            expect(served.ok).toBe(live.ok);
            if (live.ok && served.ok) {
               expect(served.r.path?.servedBy?.kind).not.toBe("rollup");
               expect(
                  rowDifference(live.r.rows, served.r.rows),
               ).toBeUndefined();
            }
         });

         for (const [label, malloy] of [
            [
               "an except: of the gate field",
               "run: by_column extend { except: region } -> { group_by: category; aggregate: total }",
            ],
            [
               "a rename: of the gate field",
               "run: by_column extend { rename: r is region } -> { group_by: category; aggregate: total }",
            ],
         ]) {
            it(`denies ${label}, as live does`, async () => {
               fixture.setBindings([]);
               await expect(
                  fixture.runAsIs(malloy, EAST.givens),
               ).rejects.toThrow(AccessDeniedError);
               fixture.setBindings(fixture.bindings);
               await expect(
                  fixture.runAsIs(malloy, EAST.givens),
               ).rejects.toThrow(AccessDeniedError);
            });
         }

         // Last: it empties the origin table.
         it("reads the rollup, not the origin: answers survive the origin table emptied", async () => {
            const queries = CASES.map((c) => ({
               c,
               malloy: `run: ${c.source} -> { group_by: category; aggregate: total }`,
            }));
            const before: Row[][][] = [];
            for (const q of queries) {
               const runs: Row[][] = [];
               for (const caller of q.c.callers) {
                  runs.push(
                     (
                        await fixture.run({
                           name: `before ${q.c.source}`,
                           malloy: q.malloy,
                           givens: caller.givens,
                        })
                     ).served,
                  );
               }
               before.push(runs);
            }
            await fixture.storedSQL(() => "DELETE FROM orders_raw");
            fixture.setBindings(fixture.bindings);
            for (const [i, q] of queries.entries()) {
               for (const [j, caller] of q.c.callers.entries()) {
                  const after = await fixture.runAsIs(q.malloy, caller.givens);
                  expect(after.path?.servedBy?.kind).toBe("rollup");
                  expect(
                     rowDifference(before[i]?.[j] ?? [], after.rows),
                  ).toBeUndefined();
                  expect(after.rows.length).toBeGreaterThan(0);
               }
            }
         });
      });
   }

   describe("v1 cutover R4: gated queries on database rollups with gated sources off (G3)", () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model("") },
            seedSql,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("builds no rollup over an inherited gate (G3 keeps only a base's own)", () => {
         expect(
            Object.keys(fixture.rollupTables).filter((n) =>
               n.startsWith("by_inherited__rollup__"),
            ),
         ).toEqual([]);
      });

      for (const c of CASES) {
         const own = c.source !== "by_inherited";
         it(`answers ${c.kind} ${own ? "from the rollup, each caller's own rows" : "live"}, equal to live`, async () => {
            for (const caller of [...c.callers, ...c.callers]) {
               const run = await fixture.run({
                  name: `${c.source} ${caller.name}`,
                  malloy: `run: ${c.source} -> { group_by: category; aggregate: total }`,
                  givens: caller.givens,
                  truthSql: truthByCategory(caller),
                  ...(own ? {} : { expectLive: true }),
               });
               if (own) expectRollup(run);
               else expectLive(run);
               expect(
                  rowDifference(run.truth ?? [], run.served),
               ).toBeUndefined();
            }
         });
      }
   });
});
