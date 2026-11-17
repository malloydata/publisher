// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { planPackages } from "../../../test_helpers/materialization_fixture";
import { BASE } from "../../../test_helpers/materialize_spec_models";
import { computeSourceEntityId } from "./addressing";
import { deriveBuildPlanOutcome } from "./build_plan";

/**
 * The plan-wide collision checks in `eligibility_collect.ts`: rollups that
 * never share one table, and a `#(materialize)` table another source also
 * writes, refused with its `#@ persist` partner.
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

describe("rollups of same-named bases in two models", () => {
   it(
      "get distinct names, addresses and tables, so neither is refused",
      async () => {
         const { c } = await packages.plan({
            "a.malloy": rollupModel(1, "A"),
            "b.malloy": rollupModel(2, "B"),
         });
         const rollups = Object.entries(c.tables).filter(([, t]) => t.rollup);
         expect(rollups).toHaveLength(2);
         // A rollup's name digests its declaring sourceID, so two models'
         // rollups of one base name and grain never share a table.
         const names = rollups.map(([, t]) => t.source.name);
         expect(new Set(names).size).toBe(2);
         const addresses = rollups.map(([id]) =>
            computeSourceEntityId(
               c.tables[id].source,
               c.connectionDigests,
               c.tables[id].target,
               c.tableFolds,
            ),
         );
         expect(new Set(addresses).size).toBe(2);
         const outcome = deriveBuildPlanOutcome(c, null);
         for (const [id] of rollups) {
            expect(outcome.plan?.refusedSources?.[id]).toBeUndefined();
            expect(outcome.plan?.sources[id]).toBeDefined();
         }
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
               // Refused alone under PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED=false.
               refused: expect.objectContaining({
                  name: "orders",
                  reason: "table_name_collision",
                  tier: "colocated",
               }),
            },
            {
               route: "@",
               tier: "malformed",
               code: "materialization-table-collision",
               message: `#@ persist source "placed" is invalid: ${reason("orders")}`,
               subject: "placed",
               model: "m.malloy",
               // Refused alone under PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED=false.
               refused: expect.objectContaining({
                  name: "placed",
                  reason: "table_name_collision",
                  tier: "colocated",
               }),
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
