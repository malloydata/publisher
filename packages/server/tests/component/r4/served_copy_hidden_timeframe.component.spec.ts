// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for hidden timeframe fields (known limitation 8, P3 with decision 3): a
 * `#(materialize)` source's private and internal timeframe truncations are
 * stored, and the served copy restores each one's timeframe with the
 * `__stored` redefinition, under the author's modifier. The stored column's
 * internal name is `private`, so no caller reaches it by any path, and the
 * redefined field is exactly as hidden as the author's: a caller selects
 * neither, and every answer (a gate over an internal timeframe field, a view
 * over a private one, a caller's own `extend` over an internal one) equals
 * live's, read from storage.
 *
 * Gated serving is behind PUBLISHER_MATERIALIZE_GATED_SOURCES: on, the gated
 * source is answered from storage; off, it stays live. The ungated source is
 * answered from storage either way.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { MaterializePlan } from "../../../src/service/materialization/plan/build_file_generator";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'acme', 'east', 10.0, TIMESTAMP '2026-01-03 10:00:00'),
      (2, 'acme', 'west', 20.0, TIMESTAMP '2026-02-11 11:00:00'),
      (3, 'globex', 'east', 30.0, TIMESTAMP '2026-02-20 09:00:00'),
      (4, 'globex', 'west', 40.0, TIMESTAMP '2026-03-05 16:00:00'),
      (5, 'initech', 'east', 50.0, TIMESTAMP '2026-03-28 08:00:00')
    ) AS t(id, org_id, region, amount, ordered_at)`,
];

/** The hidden values a refusal must never carry. */
const HIDDEN_VALUES = /acme|globex|initech|2026-0[123]/;

/**
 * In its own file, so `orders` inherits: `order_month` an internal timeframe
 * field (the gate reads it), `order_day` a public one that `orders` hides
 * with an `include`, and `org_id` internal (the gate's tenant column).
 */
const base = `##! experimental { access_modifiers }

source: base is duckdb.table('orders') include { internal: org_id; public: * } extend {
  internal dimension: order_month is ordered_at.month
  dimension: order_day is ordered_at.day
}
`;

/**
 * `orders`: gated over the internal `order_month` and `org_id`; `order_day`
 * private through its own head's `include`; `order_hour` a private timeframe
 * field its own `extend` defines (kept as written, computed at read). `wide`:
 * ungated, a private timeframe field through an `include`, and an internal
 * one it inherits.
 */
const model = `##! experimental { givens access_modifiers }
import "base.malloy"

given: GROUPS :: string[]

#(access_filter) org_id in $GROUPS and order_month >= @2026-02
#(materialize) storage="lake"
source: orders is base include { private: order_day; internal: org_id, order_month; public: * } extend {
  private dimension: order_hour is ordered_at.hour
  measure: total is amount.sum(), n is count()
  measure: day_count is count(order_day), hour_count is count(order_hour)
  view: by_day is { group_by: order_day; aggregate: total }
  view: by_month is { group_by: order_month; aggregate: total, n }
}

#(materialize) storage="lake"
source: wide is base include { private: order_day; internal: org_id, order_month; public: * } extend {
  measure: total is amount.sum(), day_count is count(order_day)
  view: by_day is { group_by: order_day; aggregate: total }
}
`;

/** Each answered from storage with exactly live's rows. */
const answered: [string, string, boolean][] = [
   // [name, query, gated]
   [
      "a gate over an internal timeframe field",
      "run: orders -> { aggregate: total, n }",
      true,
   ],
   [
      "a measure over a private timeframe field",
      "run: orders -> { group_by: region; aggregate: day_count, hour_count }",
      true,
   ],
   [
      "the author's view over a private timeframe field",
      "run: orders -> by_day",
      true,
   ],
   [
      "the author's view over an internal timeframe field",
      "run: orders -> by_month",
      true,
   ],
   [
      "a caller's own extend over the internal timeframe field",
      "run: orders extend { dimension: m is order_month } -> { group_by: m; aggregate: total }",
      true,
   ],
   [
      "a caller's include lifting the internal timeframe field",
      "run: orders include { public: order_month, total } -> { group_by: order_month; aggregate: total }",
      true,
   ],
   [
      "a truncation of the internal timeframe field",
      "run: orders extend { dimension: q is order_month.quarter } -> { group_by: q; aggregate: total }",
      true,
   ],
   ["select: *", "run: orders -> { select: * }", true],
   [
      "an ungated view over a private timeframe field",
      "run: wide -> by_day",
      false,
   ],
   [
      "an ungated measure over a private timeframe field",
      "run: wide -> { group_by: region; aggregate: day_count }",
      false,
   ],
   [
      "an ungated caller extend over an inherited internal timeframe field",
      "run: wide extend { dimension: m is order_month } -> { group_by: m; aggregate: total }",
      false,
   ],
];

/** Live refuses each; the served copy must too, carrying no hidden value. */
const refused: [string, string][] = [
   ["the internal timeframe field", "run: orders -> { select: order_month }"],
   ["the private timeframe field", "run: orders -> { group_by: order_day }"],
   [
      "the private own-text timeframe field",
      "run: orders -> { group_by: order_hour }",
   ],
   [
      "the private field's stored column",
      "run: orders -> { group_by: order_day__stored }",
   ],
   [
      "the internal field's stored column",
      "run: orders -> { group_by: order_month__stored }",
   ],
   [
      "the private field's stored column through a caller extend",
      "run: orders extend { dimension: x is order_day__stored } -> { group_by: x }",
   ],
   [
      "the internal field's stored column through a caller extend",
      "run: orders extend { dimension: x is order_month__stored } -> { group_by: x }",
   ],
   [
      "the private field through a caller extend",
      "run: orders extend { dimension: x is order_day } -> { group_by: x }",
   ],
   [
      "the private field made public by a caller include",
      "run: orders include { public: order_day } -> { group_by: order_day }",
   ],
   [
      "a stored column made public by a caller include",
      "run: orders include { public: order_month__stored } -> { group_by: order_month__stored }",
   ],
   [
      "a stored column after a caller's include wildcard",
      "run: orders include { public: * } -> { group_by: order_day__stored }",
   ],
   [
      "a stored column renamed by a caller",
      "run: orders extend { rename: x is order_month__stored } -> { group_by: x }",
   ],
   [
      "the private field in a nest",
      "run: orders -> { nest: x is { group_by: order_day } }",
   ],
   [
      "the internal field indexed by name",
      "run: orders -> { index: order_month }",
   ],
   [
      "raw SQL naming a stored column",
      'run: orders -> { group_by: x is sql_string("order_day__stored") }',
   ],
   ["the ungated private field", "run: wide -> { group_by: order_day }"],
   [
      "the ungated private field's stored column through a caller extend",
      "run: wide extend { dimension: x is order_day__stored } -> { group_by: x }",
   ],
];

/** The error a run throws, or undefined when it answers. */
async function failure(run: () => Promise<unknown>): Promise<unknown> {
   try {
      await run();
      return undefined;
   } catch (error) {
      return error;
   }
}

const GIVENS = { GROUPS: ["acme", "globex"] };

for (const gated of [true, false]) {
   describe(`R4: hidden timeframe fields stored, gated serving ${gated ? "on" : "off"}`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               ...(gated
                  ? { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" }
                  : {}),
            },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model, "base.malloy": base },
            seedSql,
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("stores each hidden timeframe field under its own name", async () => {
         const table = gated ? "orders" : "wide";
         const rows = await fixture.storedSQL(
            (t) =>
               `SELECT id, order_month::DATE::VARCHAR AS m, order_day::DATE::VARCHAR AS d FROM ${t(table)} ORDER BY id`,
         );
         expect(rows.map((r) => [Number(r.id), r.m, r.d])).toEqual([
            [1, "2026-01-01", "2026-01-03"],
            [2, "2026-02-01", "2026-02-11"],
            [3, "2026-02-01", "2026-02-20"],
            [4, "2026-03-01", "2026-03-05"],
            [5, "2026-03-01", "2026-03-28"],
         ]);
      });

      it("serves each field exactly as hidden as the author's, its stored column private, with its timeframe", async () => {
         if (gated) expect(await fixture.servedCopyProblems()).toEqual([]);
         const def = (await fixture.servedCopy().compiledModelDef()) as {
            contents: Record<
               string,
               {
                  fields: {
                     name: string;
                     as?: string;
                     accessModifier?: string;
                     timeframe?: string;
                  }[];
               }
            >;
         };
         for (const source of gated ? ["orders", "wide"] : ["wide"]) {
            const fields = new Map(
               def.contents[source].fields.map((f) => [f.as ?? f.name, f]),
            );
            const access = (n: string) =>
               fields.get(n)?.accessModifier ?? "public";
            expect(access("order_month")).toBe("internal");
            expect(access("order_day")).toBe("private");
            expect(fields.get("order_month")?.timeframe).toBe("month");
            expect(fields.get("order_day")?.timeframe).toBe("day");
            expect(access("order_month__stored")).toBe("private");
            expect(access("order_day__stored")).toBe("private");
         }
      });

      for (const [name, malloy, isGated] of answered) {
         it(`answers ${name} with live's rows`, async () => {
            const run = await fixture.run({ name, malloy, givens: GIVENS });
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (gated || !isGated) {
               expect(run.servedVia).toBe("served_copy");
               expect(run.servedFrom).toBe("storage");
            } else {
               expect(run.servedFrom).not.toBe("storage");
            }
         });
      }

      it("scopes two callers to their own rows through the gate over the internal timeframe field", async () => {
         if (!gated) return;
         const a = await fixture.run({
            name: "acme",
            malloy: "run: orders -> { aggregate: total }",
            givens: { GROUPS: ["acme"] },
         });
         const b = await fixture.run({
            name: "globex",
            malloy: "run: orders -> { aggregate: total }",
            givens: { GROUPS: ["globex"] },
         });
         // order_month >= February: acme's January order is never admitted.
         expect(a.served).toEqual([{ total: 20 }]);
         expect(b.served).toEqual([{ total: 70 }]);
      });

      for (const [name, malloy] of refused) {
         it(`refuses ${name}, from the served copy as from live`, async () => {
            fixture.setBindings([]);
            let live: Row[] | undefined;
            const liveError = await failure(async () => {
               live = (await fixture.runAsIs(malloy, GIVENS)).rows;
            });
            expect(live).toBeUndefined();
            expect(liveError).toBeDefined();
            const copy = await failure(async () => {
               const { runnable, virtualMap } = await fixture
                  .servedCopy()
                  .loadQuery(malloy, GIVENS);
               await runnable.getSQL({ virtualMap } as never);
            });
            expect(copy).toBeDefined();
            fixture.setBindings(fixture.bindings);
            const served = await failure(() => fixture.runAsIs(malloy, GIVENS));
            expect(served).toBeDefined();
            expect(String(served)).not.toMatch(HIDDEN_VALUES);
         });
      }

      if (gated) {
         it("mutation: a re-root that loses a timeframe field's modifier is refused by the access check, never served", async () => {
            const served = fixture.servedCopy();
            const plans = Object.values(
               (
                  served as unknown as {
                     materializePlans: Record<string, MaterializePlan>;
                  }
               ).materializePlans,
            );
            const kept = plans.map((p) => p.columns);
            try {
               // As if the plan forgot the timeframe columns are hidden: the
               // redefinition is then public and its stored column internal.
               for (const p of plans) {
                  p.columns = p.columns.map((c) => {
                     if (
                        c.dimension !== "order_month" &&
                        c.dimension !== "order_day"
                     ) {
                        return c;
                     }
                     const { access: _access, ...rest } = c;
                     return rest;
                  });
               }
               served.invalidate();
               const problems = await fixture.servedCopyProblems();
               expect(problems).toContainEqual({
                  sourceName: "orders",
                  reason: "fields_not_carried",
               });
               expect(problems).toContainEqual({
                  sourceName: "wide",
                  reason: "fields_not_carried",
               });
               for (const [, malloy] of refused) {
                  const error = await failure(async () => {
                     const { runnable, virtualMap } = await fixture
                        .servedCopy()
                        .loadQuery(malloy, GIVENS);
                     await runnable.getSQL({ virtualMap } as never);
                  });
                  expect(error).toBeDefined();
               }
            } finally {
               plans.forEach((p, i) => (p.columns = kept[i]));
               served.invalidate();
            }
            expect(await fixture.servedCopyProblems()).toEqual([]);
         });

         it("mutation: a private field's stored column served internal is refused by the access check", async () => {
            const served = fixture.servedCopy();
            const plans = Object.values(
               (
                  served as unknown as {
                     materializePlans: Record<string, MaterializePlan>;
                  }
               ).materializePlans,
            );
            const kept = plans.map((p) => p.columns);
            try {
               // `order_day` as internal: its redefinition and stored column
               // are then internal, less hidden than the author's private.
               for (const p of plans) {
                  p.columns = p.columns.map((c) =>
                     c.dimension === "order_day"
                        ? { ...c, access: "internal" as const }
                        : c,
                  );
               }
               served.invalidate();
               const problems = await fixture.servedCopyProblems();
               expect(problems).toContainEqual({
                  sourceName: "orders",
                  reason: "fields_not_carried",
               });
            } finally {
               plans.forEach((p, i) => (p.columns = kept[i]));
               served.invalidate();
            }
            expect(await fixture.servedCopyProblems()).toEqual([]);
         });

         // Last: it empties the origin table, so only the stored table can
         // answer, and the gate over the internal timeframe field must hold.
         it("answers from the stored table alone, the gate over the internal timeframe field grafted on it", async () => {
            await fixture.storedSQL(() => "DELETE FROM orders");
            const run = await fixture.run({
               name: "after the origin emptied",
               malloy: "run: orders -> { aggregate: total, n }",
               givens: { GROUPS: ["acme"] },
            });
            expect(run.live.map((r) => Number(r.n))).toEqual([0]);
            expect(run.servedVia).toBe("served_copy");
            expect(
               run.served.map((r) => [Number(r.total), Number(r.n)]),
            ).toEqual([[20, 1]]);
         });
      }
   });
}

/**
 * An author's `rename:` beside a stored hidden timeframe field or a hidden
 * inlined join: the re-root hides their stored columns with labelled renames
 * in its first block, not an `include`, since Malloy refuses a `rename:` in
 * the block just after an include and the served copy (every source in the
 * model) would not compile. Each source answers from storage with live's
 * rows, a source beside them too, and every refusal holds. A hidden column
 * kept under its own name still needs the include, so that source alone is
 * left live (known limitation 25).
 */
describe("R4: hidden stored columns beside an author's rename", () => {
   const renameSeed = [
      ...seedSql,
      `CREATE TABLE regions AS SELECT * FROM (VALUES
         ('east', 'East'), ('west', 'West')
       ) AS t(code, label)`,
   ];
   const renameBase = `##! experimental { access_modifiers }

source: regions is duckdb.table('regions') extend { primary_key: code }

source: tbase is duckdb.table('orders') extend {
  internal dimension: order_month is ordered_at.month
}

source: cbase is duckdb.table('orders') include { internal: org_id; public: * }
`;
   const renameModel = `##! experimental { access_modifiers }
import "base.malloy"

#(materialize) storage="lake"
source: renamed is tbase extend {
  rename: amt is amount
  measure: total is amt.sum()
  view: by_month is { group_by: order_month; aggregate: total }
}

#(materialize) storage="lake" inline=["r"]
source: renamed_join is duckdb.table('orders') extend {
  rename: amt is amount
  internal join_one: r is regions with region
  dimension: region_label is r.label
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: beside is duckdb.table('orders') extend {
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: renamed_hidden is cbase extend {
  rename: amt is amount
  measure: total is amt.sum()
}
`;
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": renameModel, "base.malloy": renameBase },
         seedSql: renameSeed,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   // A hidden column kept under its own name needs the include, and the
   // inverse rename block would follow it, which Malloy refuses (known
   // limitation 25): that source alone is left live, never the whole copy.
   it("compiles the served copy with every other source re-rooted", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([
         { sourceName: "renamed_hidden", reason: "definition_not_parsed" },
      ]);
   });

   it("answers the source it could not re-root live, with live's rows", async () => {
      const run = await fixture.run({
         name: "renamed_hidden",
         malloy:
            "run: renamed_hidden -> { group_by: region; aggregate: total }",
      });
      expect(run.servedFrom).not.toBe("storage");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   for (const [name, malloy] of [
      ["the source beside them", "run: beside -> { aggregate: total }"],
      ["the author's view over the timeframe", "run: renamed -> by_month"],
      [
         "a caller's extend over the internal timeframe field",
         "run: renamed extend { dimension: m is order_month } -> { group_by: m; aggregate: total }",
      ],
      [
         "the author's dimension over the internal join",
         "run: renamed_join -> { group_by: region_label; aggregate: total }",
      ],
      [
         "a caller's extend over the internal join",
         "run: renamed_join extend { dimension: l is r.label } -> { group_by: l; aggregate: total }",
      ],
   ] as const) {
      it(`answers ${name} from storage with live's rows`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(run.servedFrom).toBe("storage");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   for (const [name, malloy] of [
      [
         "the internal timeframe field",
         "run: renamed -> { group_by: order_month }",
      ],
      [
         "its stored column through a caller extend",
         "run: renamed extend { dimension: x is order_month__stored } -> { group_by: x }",
      ],
      [
         "its stored column made public by a caller include",
         "run: renamed include { public: order_month__stored } -> { group_by: order_month__stored }",
      ],
      [
         "the internal join's field",
         "run: renamed_join -> { group_by: r.label }",
      ],
      [
         "the join's stored column through a caller extend",
         "run: renamed_join extend { dimension: x is r__label__stored } -> { group_by: x }",
      ],
   ] as const) {
      it(`refuses ${name}, from the served copy as from live`, async () => {
         fixture.setBindings([]);
         expect(await failure(() => fixture.runAsIs(malloy))).toBeDefined();
         const copy = await failure(async () => {
            const { runnable, virtualMap } = await fixture
               .servedCopy()
               .loadQuery(malloy, {});
            await runnable.getSQL({ virtualMap } as never);
         });
         expect(copy).toBeDefined();
         fixture.setBindings(fixture.bindings);
         const served = await failure(() => fixture.runAsIs(malloy));
         expect(served).toBeDefined();
         expect(String(served)).not.toMatch(HIDDEN_VALUES);
         expect(String(served)).not.toMatch(/East|West/);
      });
   }
});
