// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for gated queries on rollups (known limitation 1, R3 for rollups: route,
 * then govern). With PUBLISHER_MATERIALIZE_GATED_SOURCES on, a query whose
 * entry point carries a row-level gate or lock is answered from a
 * `#(preaggregate)` rollup over its source when every gate is provable on
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
 * gate field is denied, as live is. Colocated and `storage=` (the latter
 * behind its review seam, as question 14 leaves it off in production).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
import { allowGatedStorageRollupsForTests } from "../../../src/service/materialization/plan/rollup_plan";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
   type Row,
} from "./harness";

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

// A gated parent joining a rollup's base (A7): the join leg redeclares the
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

for (const placement of ["colocated", "storage"] as const) {
   describe(`v1 cutover R4: gated queries on ${placement} rollups (row 1, gated sources on)`, () => {
      let restore: () => void;
      let restoreReview: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
            },
         }).restore;
         // `storage=` waits for question 14's review in production; the
         // seam stands in for it here.
         restoreReview = allowGatedStorageRollupsForTests(
            placement === "storage",
         );
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
         restoreReview();
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
         const governed = async (malloy: string, givens: Caller["givens"]) => {
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

      const NEGATIVES: [string, string, Record<string, string | number>][] = [
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
            (c) => (c as { reason?: string }).reason === "CONTRACT",
         );
         expect(skipped.length).toBeGreaterThan(0);
         for (const c of skipped) {
            const text = JSON.stringify(c);
            expect(text).not.toContain("carrier");
            expect(text).not.toContain("CARRIER");
            expect(text).not.toContain("region");
         }
      });

      it("declines a joined source's rollup under a gated parent (A7), as CONTRACT without naming the gate", async () => {
         const malloy =
            "run: gated_parent -> { group_by: category; aggregate: order_lines.qty_total }";
         expectLive(
            await fixture.run({
               name: "gated parent",
               malloy,
               givens: EAST.givens,
            }),
         );
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy, EAST.givens);
         const contract = (path?.considered ?? []).filter(
            (c) => (c as { reason?: string }).reason === "CONTRACT",
         );
         expect(contract.length).toBeGreaterThan(0);
         for (const c of contract) {
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
            expect(rowDifference(live.r.rows, served.r.rows)).toBeUndefined();
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
            await expect(fixture.runAsIs(malloy, EAST.givens)).rejects.toThrow(
               AccessDeniedError,
            );
            fixture.setBindings(fixture.bindings);
            await expect(fixture.runAsIs(malloy, EAST.givens)).rejects.toThrow(
               AccessDeniedError,
            );
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

describe("v1 cutover R4: gated queries on rollups with gated sources off", () => {
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

   it("builds no rollup over a gated base (row 21)", () => {
      expect(
         Object.keys(fixture.rollupTables).filter(
            (n) =>
               !n.startsWith("open__rollup__") &&
               !n.startsWith("order_lines__rollup__"),
         ),
      ).toEqual([]);
   });

   for (const c of CASES) {
      it(`answers ${c.kind} live, equal to live`, async () => {
         for (const caller of c.callers) {
            expectLive(
               await fixture.run({
                  name: `${c.source} ${caller.name}`,
                  malloy: `run: ${c.source} -> { group_by: category; aggregate: total }`,
                  givens: caller.givens,
                  expectLive: true,
               }),
            );
         }
      });
   }
});

describe("v1 cutover: storage= rollups over gated bases without the review seam", () => {
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
         files: { "m.malloy": model(` storage="${DESTINATION}"`) },
         seedSql,
         preaggregate: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds no storage= rollup over a gated base", () => {
      expect(
         Object.keys(fixture.rollupTables).filter(
            (n) =>
               !n.startsWith("open__rollup__") &&
               !n.startsWith("order_lines__rollup__"),
         ),
      ).toEqual([]);
      expect(
         fixture.bindings
            .map((b) => b.tablePath)
            .filter((n) => n.includes("__preagg__")),
      ).toEqual([]);
   });

   for (const c of CASES) {
      it(`answers ${c.kind} live, equal to live`, async () => {
         for (const caller of c.callers) {
            expectLive(
               await fixture.run({
                  name: `${c.source} ${caller.name}`,
                  malloy: `run: ${c.source} -> { group_by: category; aggregate: total }`,
                  givens: caller.givens,
                  expectLive: true,
               }),
            );
         }
      });
   }
});
