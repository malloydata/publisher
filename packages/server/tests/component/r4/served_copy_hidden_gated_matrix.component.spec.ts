// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for P3 (materialization v2, phase 3), under a row-level gate, as one
 * matrix: a `#(materialize)` source's private and internal fields are stored
 * under their own names, built from a copy of the source with its own access
 * modifiers lifted, and the served copy keeps the author's modifiers, so a
 * caller can no more select a hidden field from the stored table than from
 * live, and a gate over a hidden column is answered from the stored table
 * with each caller's live rows.
 *
 * Each scenario runs twice, gated serving on and off
 * (PUBLISHER_MATERIALIZE_GATED_SOURCES, plan question 14): on, the gated
 * source is answered from storage; off, it stays live. An ungated source is
 * answered from storage either way.
 *
 * The scenarios:
 *
 * - hidden plain fields: a gate over an internal `org_id`, a private computed
 *   dimension, an internal inherited one, and a private head column through
 *   an `include`;
 * - hidden timeframe fields (known limitation 8, P3 with decision 3): private
 *   and internal timeframe truncations are stored, and the served copy
 *   restores each one's timeframe with the `__stored` redefinition, under the
 *   author's modifier. The stored column's internal name is `private`, so no
 *   caller reaches it by any path, and the redefined field is exactly as
 *   hidden as the author's.
 *
 * The ungated hidden-field scenarios are in
 * served_copy_hidden_parity_matrix.component.spec.ts.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
import type { MaterializePlan } from "../../../src/service/materialization/domain/table_plans";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

/** The error a run throws, or undefined when it answers. */
async function failure(run: () => Promise<unknown>): Promise<unknown> {
   try {
      await run();
      return undefined;
   } catch (error) {
      return error;
   }
}

type Givens = Record<string, string[]>;

/** A query answered from storage (or live, for a gated one with gated serving off). */
interface AnsweredRow {
   name: string;
   malloy: string;
   /** The caller's groups. */
   groups: string[];
   /** Ungated: answered from storage even with gated serving off. */
   ungated?: boolean;
}

/** One scenario: its package, what live refuses and answers, and its own checks. */
interface GatedHiddenScenario {
   title: (gated: "on" | "off") => string;
   files: Record<string, string>;
   seedSql: string[];
   /** The hidden values a refusal must never carry. */
   hiddenValues: RegExp;
   /** Ends each answered row's test name, after `answers ${name} `. */
   answeredPhrase: string;
   answered: AnsweredRow[];
   refused: [string, string][];
   /** The givens each refused query runs with. */
   refusedGivens: Givens;
   /** How the served copy's own compile is asked for a refused query's SQL. */
   copyCompile: {
      /** Hand `getSQL` the served copy's virtual map. */
      virtualMap: boolean;
      /** What the compile's refusal must say, when it is pinned. */
      error?: RegExp;
   };
   /** Checks of the scenario's own, ahead of the answered rows. */
   before: (gated: boolean, fixture: () => R4Fixture) => void;
   /** Checks of the scenario's own, between the answered and refused rows. */
   between: (gated: boolean, fixture: () => R4Fixture) => void;
   /** Checks of the scenario's own, after the refused rows. */
   after: (gated: boolean, fixture: () => R4Fixture) => void;
}

/** Each field's access modifier on a source of the compiled served copy. */
type ServedDef = {
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

// Hidden plain fields.

const plainSeedSql = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'acme', 'east', 10.0), (2, 'acme', 'west', 20.0),
      (3, 'globex', 'east', 30.0), (4, 'globex', 'west', 40.0),
      (5, 'initech', 'east', 50.0)
    ) AS t(id, org_id, region, amount)`,
];

/**
 * On `orders`: `org_id` (the gate's column) and `region_code` are internal on
 * the source it extends, and `secret` is a private computed dimension in its
 * own `extend`; all three are stored. `id` is private on the source it
 * extends, so `orders` cannot read it and it is never stored. A gate is
 * grafted as a caller-level `extend`, which reads an internal field and never
 * a private one, so a gate over a private column denies every caller, live or
 * served; `org_id` is internal for that reason.
 *
 * On `wide`: `amount` is private through an `include` on its own head, which
 * its own `extend` still reads; it is stored too.
 */
const plainModel = `##! experimental { givens access_modifiers }

given: GROUPS :: string[]

source: base is duckdb.table('orders') include { private: id; internal: org_id; public: * } extend {
  internal dimension: region_code is upper(region)
}

#(access_filter) org_id in $GROUPS
#(materialize) storage="lake"
source: orders is base extend {
  private dimension: secret is amount * 2
  dimension: east is region_code = 'EAST'
  measure: total is amount.sum(), n is count(), secret_total is secret.sum()
}

source: east_orders is orders extend {
  where: east
}

#(materialize) storage="lake"
source: wide is duckdb.table('orders') include { private: amount; public: * } extend {
  measure: amt is amount.sum()
}
`;

const byRegion =
   "run: orders -> { group_by: region; aggregate: total, n, secret_total }";

const plainRefused: [string, string][] = [
   ["an internal raw column", "run: orders -> { select: org_id }"],
   ["a private computed dimension", "run: orders -> { select: secret }"],
   [
      "an internal inherited dimension",
      "run: orders -> { select: region_code }",
   ],
   [
      "a private column of the source it extends",
      "run: orders -> { select: id }",
   ],
   [
      "a private column through a caller extend",
      "run: orders extend { dimension: o is secret } -> { select: o }",
   ],
   [
      "an internal column through an extension",
      "run: east_orders -> { group_by: org_id }",
   ],
   ["a private raw column of its own head", "run: wide -> { select: amount }"],
   [
      "a private raw column of its own head through a caller extend",
      "run: wide extend { dimension: a is amount } -> { select: a }",
   ],
];

// Two callers, each answered with exactly live's rows for them.
const plainAnswered: AnsweredRow[] = [
   { name: "acme", malloy: byRegion, groups: ["acme"] },
   {
      name: "globex and initech",
      malloy: byRegion,
      groups: ["globex", "initech"],
   },
   {
      name: "acme, through an extension that reads an internal field",
      malloy: "run: east_orders -> { aggregate: total, n }",
      groups: ["acme"],
   },
   {
      name: "an ungated source whose own extend reads its private column",
      malloy: "run: wide -> { group_by: region; aggregate: amt }",
      groups: ["acme"],
      ungated: true,
   },
   {
      // Malloy lets a caller's own extend read an internal field, live and
      // served alike; the gate still scopes it.
      name: "acme, reading the internal org_id through a caller extend",
      malloy:
         "run: orders extend { dimension: org is org_id } -> { group_by: org; aggregate: total }",
      groups: ["acme"],
   },
];

// Hidden timeframe fields.

const timeframeSeedSql = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'acme', 'east', 10.0, TIMESTAMP '2026-01-03 10:00:00'),
      (2, 'acme', 'west', 20.0, TIMESTAMP '2026-02-11 11:00:00'),
      (3, 'globex', 'east', 30.0, TIMESTAMP '2026-02-20 09:00:00'),
      (4, 'globex', 'west', 40.0, TIMESTAMP '2026-03-05 16:00:00'),
      (5, 'initech', 'east', 50.0, TIMESTAMP '2026-03-28 08:00:00')
    ) AS t(id, org_id, region, amount, ordered_at)`,
];

/**
 * In its own file, so `orders` inherits: `order_month` an internal timeframe
 * field (the gate reads it), `order_day` a public one that `orders` hides
 * with an `include`, and `org_id` internal (the gate's tenant column).
 */
const timeframeBase = `##! experimental { access_modifiers }

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
const timeframeModel = `##! experimental { givens access_modifiers }
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

const TIMEFRAME_GROUPS = ["acme", "globex"];

/** Each answered from storage with exactly live's rows. */
const timeframeAnswered: AnsweredRow[] = (
   [
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
   ] as [string, string, boolean][]
).map(([name, malloy, gated]) => ({
   name,
   malloy,
   groups: TIMEFRAME_GROUPS,
   ungated: !gated,
}));

/** Live refuses each; the served copy must too, carrying no hidden value. */
const timeframeRefused: [string, string][] = [
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

/** The served copy's materialize plans, which a mutation test edits in place. */
function plansOf(fixture: R4Fixture): {
   served: ReturnType<R4Fixture["servedCopy"]>;
   plans: MaterializePlan[];
} {
   const served = fixture.servedCopy();
   const plans = Object.values(
      (
         served as unknown as {
            materializePlans: Record<string, MaterializePlan>;
         }
      ).materializePlans,
   );
   return { served, plans };
}

const scenarios: GatedHiddenScenario[] = [
   {
      title: (gated) =>
         `R4 P3: hidden fields stored and kept hidden, gated serving ${gated}`,
      files: { "m.malloy": plainModel },
      seedSql: plainSeedSql,
      hiddenValues: /acme|globex|initech/,
      answeredPhrase: "with that caller's live rows",
      answered: plainAnswered,
      refused: plainRefused,
      refusedGivens: { GROUPS: ["acme"] },
      // The served copy's own compile refuses it: the stored column carries
      // the author's modifier, so no fallback is needed.
      copyCompile: { virtualMap: false, error: /private|internal|not defined/ },
      before: (gated, get) => {
         it("stores every hidden field under its own name, and never one private on a source it extends", async () => {
            const fixture = get();
            const rows = await fixture.storedSQL(
               (t) =>
                  `SELECT org_id, secret, region_code FROM ${t("orders")} ORDER BY secret`,
            );
            expect(
               rows.map((r) => [r.org_id, Number(r.secret), r.region_code]),
            ).toEqual([
               ["acme", 20, "EAST"],
               ["acme", 40, "WEST"],
               ["globex", 60, "EAST"],
               ["globex", 80, "WEST"],
               ["initech", 100, "EAST"],
            ]);
            const columns = await fixture.storedSQL(
               (t) => `SELECT * FROM ${t("orders")} LIMIT 0`,
            );
            expect(columns).toEqual([]);
            const described = await fixture.storedSQL(
               (t) => `DESCRIBE SELECT * FROM ${t("orders")}`,
            );
            expect(described.map((c) => c.column_name)).not.toContain("id");
            const wide = await fixture.storedSQL(
               (t) => `SELECT sum(amount)::DOUBLE AS s FROM ${t("wide")}`,
            );
            expect(Number(wide[0].s)).toBe(150);
         });

         it("keeps each field's access modifier on the served source", async () => {
            if (!gated) return;
            const servedDef = (await get()
               .servedCopy()
               .compiledModelDef()) as ServedDef;
            const access = Object.fromEntries(
               servedDef.contents.orders.fields.map((f) => [
                  f.as ?? f.name,
                  f.accessModifier ?? "public",
               ]),
            );
            // `id`, private on the source it extends, is not stored, and the
            // served source does not have it: it could not read it anyway.
            expect(access.id).toBeUndefined();
            expect(access).toMatchObject({
               org_id: "internal",
               secret: "private",
               region_code: "internal",
               region: "public",
               amount: "public",
            });
            const wide = Object.fromEntries(
               servedDef.contents.wide.fields.map((f) => [
                  f.as ?? f.name,
                  f.accessModifier ?? "public",
               ]),
            );
            expect(wide).toMatchObject({ amount: "private", region: "public" });
         });
      },
      between: (_gated, get) => {
         it("gives two callers different rows, each its own", async () => {
            const a = await get().run({
               name: "acme",
               malloy: "run: orders -> { aggregate: total }",
               givens: { GROUPS: ["acme"] },
            });
            const b = await get().run({
               name: "globex",
               malloy: "run: orders -> { aggregate: total }",
               givens: { GROUPS: ["globex"] },
            });
            expect(a.served).toEqual([{ total: 30 }]);
            expect(b.served).toEqual([{ total: 70 }]);
         });
      },
      after: (gated, get) => {
         it("never answers a caller who supplies no group", async () => {
            const refusal = await failure(() =>
               get().run({ name: "anonymous", malloy: byRegion }),
            );
            expect(refusal).toBeInstanceOf(AccessDeniedError);
         });

         if (!gated) return;

         it("mutation: a re-root that loses the modifiers is refused by the access check, never served", async () => {
            const fixture = get();
            const { served, plans } = plansOf(fixture);
            const kept = plans.map((p) => p.columns);
            try {
               // As if the plan forgot which columns are hidden: the re-root
               // then writes no `include`, and drops `secret`'s definition.
               for (const p of plans) {
                  p.columns = p.columns.map((c) => {
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
               for (const [, malloy] of plainRefused) {
                  const error = await failure(async () => {
                     const { runnable } = await fixture
                        .servedCopy()
                        .loadQuery(malloy, { GROUPS: ["acme"] });
                     await runnable.getSQL();
                  });
                  expect(error).toBeDefined();
               }
            } finally {
               plans.forEach((p, i) => (p.columns = kept[i]));
               served.invalidate();
            }
            expect(await fixture.servedCopyProblems()).toEqual([]);
         });

         // Last: it empties the origin table, so only the stored table can
         // answer, and the gate over the hidden org_id must still hold.
         it("answers from the stored table alone, the gate over the hidden org_id grafted on it", async () => {
            const fixture = get();
            await fixture.storedSQL(() => "DELETE FROM orders");
            const run = await fixture.run({
               name: "after the origin emptied",
               malloy: "run: orders -> { aggregate: total, n }",
               givens: { GROUPS: ["globex"] },
            });
            expect(run.live.map((r) => Number(r.n))).toEqual([0]);
            expect(run.servedVia).toBe("served_copy");
            expect(
               run.served.map((r) => [Number(r.total), Number(r.n)]),
            ).toEqual([[70, 2]]);
         });
      },
   },
   {
      title: (gated) =>
         `R4: hidden timeframe fields stored, gated serving ${gated}`,
      files: { "m.malloy": timeframeModel, "base.malloy": timeframeBase },
      seedSql: timeframeSeedSql,
      hiddenValues: /acme|globex|initech|2026-0[123]/,
      answeredPhrase: "with live's rows",
      answered: timeframeAnswered,
      refused: timeframeRefused,
      refusedGivens: { GROUPS: TIMEFRAME_GROUPS },
      copyCompile: { virtualMap: true },
      before: (gated, get) => {
         it("stores each hidden timeframe field under its own name", async () => {
            const table = gated ? "orders" : "wide";
            const rows = await get().storedSQL(
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
            const fixture = get();
            if (gated) expect(await fixture.servedCopyProblems()).toEqual([]);
            const def = (await fixture
               .servedCopy()
               .compiledModelDef()) as ServedDef;
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
      },
      between: (gated, get) => {
         it("scopes two callers to their own rows through the gate over the internal timeframe field", async () => {
            if (!gated) return;
            const a = await get().run({
               name: "acme",
               malloy: "run: orders -> { aggregate: total }",
               givens: { GROUPS: ["acme"] },
            });
            const b = await get().run({
               name: "globex",
               malloy: "run: orders -> { aggregate: total }",
               givens: { GROUPS: ["globex"] },
            });
            // order_month >= February: acme's January order is never admitted.
            expect(a.served).toEqual([{ total: 20 }]);
            expect(b.served).toEqual([{ total: 70 }]);
         });
      },
      after: (gated, get) => {
         if (!gated) return;
         const GIVENS = { GROUPS: TIMEFRAME_GROUPS };

         it("mutation: a re-root that loses a timeframe field's modifier is refused by the access check, never served", async () => {
            const fixture = get();
            const { served, plans } = plansOf(fixture);
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
               for (const [, malloy] of timeframeRefused) {
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
            const fixture = get();
            const { served, plans } = plansOf(fixture);
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
            const fixture = get();
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
      },
   },
];

for (const scenario of scenarios) {
   for (const gated of [true, false]) {
      describe(scenario.title(gated ? "on" : "off"), () => {
         let restore: () => void;
         let fixture: R4Fixture;
         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PUBLISHER_PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_MATERIALIZE_ENABLED: "true",
                  ...(gated
                     ? { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" }
                     : {}),
               },
            }).restore;
            fixture = await buildScenario({
               files: scenario.files,
               seedSql: scenario.seedSql,
               materialize: true,
            });
         });
         afterAll(async () => {
            restore();
            await disposeScenarios();
         });
         const get = () => fixture;

         scenario.before(gated, get);

         for (const { name, malloy, groups, ungated } of scenario.answered) {
            it(`answers ${name} ${scenario.answeredPhrase}`, async () => {
               const run = await fixture.run({
                  name,
                  malloy,
                  givens: { GROUPS: groups },
               });
               expect(run.live.length).toBeGreaterThan(0);
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               if (gated || ungated) {
                  expect(run.servedVia).toBe("served_copy");
                  expect(run.servedFrom).toBe("storage");
               } else {
                  expect(run.servedFrom).not.toBe("storage");
               }
            });
         }

         scenario.between(gated, get);

         for (const [name, malloy] of scenario.refused) {
            it(`refuses ${name}, from the served copy as from live`, async () => {
               const givens = scenario.refusedGivens;
               fixture.setBindings([]);
               let live: Row[] | undefined;
               const liveError = await failure(async () => {
                  live = (await fixture.runAsIs(malloy, givens)).rows;
               });
               expect(live).toBeUndefined();
               expect(liveError).toBeDefined();
               expect(liveError).not.toBeInstanceOf(AccessDeniedError);
               const copy = await failure(async () => {
                  const { runnable, virtualMap } = await fixture
                     .servedCopy()
                     .loadQuery(malloy, givens);
                  await (scenario.copyCompile.virtualMap
                     ? runnable.getSQL({ virtualMap } as never)
                     : runnable.getSQL());
               });
               expect(copy).toBeDefined();
               if (scenario.copyCompile.error) {
                  expect(String(copy)).toMatch(scenario.copyCompile.error);
               }
               fixture.setBindings(fixture.bindings);
               const served = await failure(() =>
                  fixture.runAsIs(malloy, givens),
               );
               expect(served).toBeDefined();
               expect(String(served)).not.toMatch(scenario.hiddenValues);
            });
         }

         scenario.after(gated, get);
      });
   }
}
