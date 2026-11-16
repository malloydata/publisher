// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { MaterializationEligibilityError } from "../../../errors";
import { planPackages } from "../../../test_helpers/materialization_fixture";
import { BASE } from "../../../test_helpers/materialize_spec_models";
import type { TablePlan } from "../domain/table_plans";
import { collidingRollups } from "./eligibility_collect";
import { assertTierEligible } from "./materialization_eligibility";

/**
 * The plan-wide collision checks in `eligibility_collect.ts`: two rollups
 * that would share one table, and a `#(materialize)` table another source
 * also writes, refused with its `#@ persist` partner.
 */

const packages = planPackages({}, { keepIncrementalUnbuildable: true });

const rollupModel = (
   id: number,
   category: string,
) => `##! experimental.persistence
source: orders is duckdb.sql("SELECT ${id} AS id, ${id * 10} AS amount, '${category}' AS category") extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}`;

/** `source`, answering `name` for its name and itself for everything else. */
function renamed(source: PersistSource, name: string): PersistSource {
   return new Proxy(source, {
      get: (target, prop) =>
         prop === "name" ? name : Reflect.get(target, prop, target),
   });
}

describe("collidingRollups", () => {
   async function twoModelsRollups() {
      const { c } = await packages.plan({
         "a.malloy": rollupModel(1, "A"),
         "b.malloy": rollupModel(2, "B"),
      });
      const rollups = Object.entries(c.tables).filter(([, t]) => t.rollup);
      expect(rollups).toHaveLength(2);
      return { c, rollups };
   }

   it(
      "finds none among two models' same-named bases, whose rollup names carry the declaring model",
      async () => {
         const { c, rollups } = await twoModelsRollups();
         const names = rollups.map(([, t]) => t.source.name);
         expect(new Set(names).size).toBe(2);
         expect(collidingRollups(c).size).toBe(0);
      },
      { timeout: 30000 },
   );

   it(
      "refuses both rollups of one physical table with different contents, naming each model",
      async () => {
         const { c, rollups } = await twoModelsRollups();
         const tables: Record<string, TablePlan> = Object.fromEntries(
            rollups.map(([id, t]) => [
               id,
               { ...t, source: renamed(t.source, "orders__rollup__category") },
            ]),
         );
         const colliding = collidingRollups({
            tables,
            connectionDigests: c.connectionDigests,
            tableFolds: c.tableFolds,
         });
         expect([...colliding.keys()].sort()).toEqual(
            rollups.map(([id]) => id).sort(),
         );
         for (const message of colliding.values()) {
            expect(message).toBe(
               "Pre-aggregation rollup 'orders__rollup__category' is generated " +
                  "by more than one model (a.malloy, b.malloy) from different " +
                  "sources with the same name and grain, so they would share " +
                  "one table. Neither is built or served from it; each model's " +
                  "queries answer from its base. Rename one of the base sources.",
            );
         }

         const [[id, table]] = Object.entries(tables);
         let refused: unknown;
         try {
            assertTierEligible(table.source, false, {
               tables,
               collidingRollups: colliding,
            });
         } catch (err) {
            refused = err;
         }
         expect(refused).toBeInstanceOf(MaterializationEligibilityError);
         expect((refused as MaterializationEligibilityError).reason).toBe(
            "rollup_name_collision",
         );
         expect((refused as Error).message).toBe(colliding.get(id)!);
      },
      { timeout: 30000 },
   );

   it(
      "shares one table, legitimately, between two entries with one content address",
      async () => {
         const { c, rollups } = await twoModelsRollups();
         const [id, table] = rollups[0];
         const colliding = collidingRollups({
            tables: { [id]: table, [`${id}#again`]: table },
            connectionDigests: c.connectionDigests,
            tableFolds: c.tableFolds,
         });
         expect(colliding.size).toBe(0);
      },
      { timeout: 30000 },
   );
});

describe("refuseCollidingMaterialize", () => {
   it(
      "refuses a #(materialize) source and its #@ persist partner on one table, each as invalid, naming the other",
      async () => {
         const { c } = await packages.plan(`##! experimental.persistence
${BASE}
#@ persist name="shared"
source: placed is orders_raw -> { select: * }

#(materialize) name="shared"
source: orders is orders_raw extend { dimension: big is amount > 10 }
`);
         const reason = (other: string) =>
            `its name= \`shared\` is a table in "duckdb", which "${other}" ` +
            `(m.malloy) also writes, so each build would overwrite the other ` +
            `and one source would read the other's rows; give each source a ` +
            `distinct name=.`;
         expect(
            (c.annotationFindings ?? []).filter(
               (f) => f.code === "materialization-table-collision",
            ),
         ).toEqual([
            {
               route: "materialize",
               tier: "malformed",
               code: "materialization-table-collision",
               message: `#(materialize) on source "orders" is invalid: ${reason("placed")}`,
               subject: "orders",
               model: "m.malloy",
            },
            {
               route: "@",
               tier: "malformed",
               code: "materialization-table-collision",
               message: `#@ persist source "placed" is invalid: ${reason("orders")}`,
               subject: "placed",
               model: "m.malloy",
            },
         ]);
         const planned = Object.values(c.tables).map((t) => t.source.name);
         expect(planned).not.toContain("placed");
         expect(planned).not.toContain("orders");
      },
      { timeout: 30000 },
   );

   it(
      "leaves a group of #@ persist sources alone to the persistence check",
      async () => {
         const { c } = await packages.plan(`##! experimental.persistence
${BASE}
#@ persist name="shared"
source: one is orders_raw -> { select: * }

#@ persist name="shared"
source: two is orders_raw -> { select: id }
`);
         expect(
            (c.annotationFindings ?? []).filter(
               (f) =>
                  f.code === "materialization-table-collision" &&
                  f.message.includes("is invalid: its name="),
            ),
         ).toEqual([]);
      },
      { timeout: 30000 },
   );
});
