// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for P3 (materialization v2, phase 3): a `#(materialize)` source's private
 * and internal fields are stored under their own names, built from a copy of
 * the source with its own access modifiers lifted, and the served copy keeps
 * the author's modifiers, so a caller can no more select a hidden field from
 * the stored table than from live, and a gate over a hidden `org_id` is
 * answered from the stored table with each caller's live rows.
 *
 * Gated serving is behind PUBLISHER_MATERIALIZE_GATED_SOURCES (plan question
 * 14): on, the gated source is answered from storage; off, it stays live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
import type { MaterializePlan } from "../../../src/service/materialization/plan/build_file_generator";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
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
const model = `##! experimental { givens access_modifiers }

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

const hiddenSelects: [string, string][] = [
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

/** The error a run throws, or undefined when it answers. */
async function failure(run: () => Promise<unknown>): Promise<unknown> {
   try {
      await run();
      return undefined;
   } catch (error) {
      return error;
   }
}

for (const gated of [true, false]) {
   describe(`R4 P3: hidden fields stored and kept hidden, gated serving ${gated ? "on" : "off"}`, () => {
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
            files: { "m.malloy": model },
            seedSql,
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("stores every hidden field under its own name, and never one private on a source it extends", async () => {
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
         const servedDef = (await fixture.servedCopy().compiledModelDef()) as {
            contents: Record<
               string,
               {
                  fields: {
                     name: string;
                     as?: string;
                     accessModifier?: string;
                  }[];
               }
            >;
         };
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

      // Two callers, each answered with exactly live's rows for them.
      const callers: [string, string, string[], boolean?][] = [
         ["acme", byRegion, ["acme"]],
         ["globex and initech", byRegion, ["globex", "initech"]],
         [
            "acme, through an extension that reads an internal field",
            "run: east_orders -> { aggregate: total, n }",
            ["acme"],
         ],
         [
            "an ungated source whose own extend reads its private column",
            "run: wide -> { group_by: region; aggregate: amt }",
            ["acme"],
            true,
         ],
         [
            // Malloy lets a caller's own extend read an internal field, live
            // and served alike; the gate still scopes it.
            "acme, reading the internal org_id through a caller extend",
            "run: orders extend { dimension: org is org_id } -> { group_by: org; aggregate: total }",
            ["acme"],
         ],
      ];
      for (const [name, malloy, groups, ungated] of callers) {
         it(`answers ${name} with that caller's live rows`, async () => {
            const run = await fixture.run({
               name,
               malloy,
               givens: { GROUPS: groups },
            });
            if (gated || ungated) {
               expect(run.servedVia).toBe("served_copy");
               expect(run.servedFrom).toBe("storage");
            } else {
               expect(run.servedFrom).not.toBe("storage");
            }
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }

      it("gives two callers different rows, each its own", async () => {
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
         expect(a.served).toEqual([{ total: 30 }]);
         expect(b.served).toEqual([{ total: 70 }]);
      });

      for (const [name, malloy] of hiddenSelects) {
         it(`refuses ${name}, from the served copy as from live`, async () => {
            const givens = { GROUPS: ["acme"] };
            fixture.setBindings([]);
            const live = await failure(() => fixture.runAsIs(malloy, givens));
            expect(live).toBeDefined();
            expect(live).not.toBeInstanceOf(AccessDeniedError);
            // The served copy's own compile refuses it: the stored column
            // carries the author's modifier, so no fallback is needed.
            const copy = await failure(async () => {
               const { runnable } = await fixture
                  .servedCopy()
                  .loadQuery(malloy, givens);
               await runnable.getSQL();
            });
            expect(String(copy)).toMatch(/private|internal|not defined/);
            fixture.setBindings(fixture.bindings);
            const served = await failure(() => fixture.runAsIs(malloy, givens));
            expect(served).toBeDefined();
            expect(String(served)).not.toMatch(/acme|globex|initech/);
         });
      }

      it("never answers a caller who supplies no group", async () => {
         const refusal = await failure(() =>
            fixture.run({ name: "anonymous", malloy: byRegion }),
         );
         expect(refusal).toBeInstanceOf(AccessDeniedError);
      });

      if (gated) {
         it("mutation: a re-root that loses the modifiers is refused by the access check, never served", async () => {
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
               for (const [, malloy] of hiddenSelects) {
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
      }
   });
}
