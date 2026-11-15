// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a colocated table and a `storage=` one give the same answer, served the
 * same way, for shapes where they used to differ by accident. Every
 * shape runs in both tiers, compares rows (and field types where they matter)
 * to live, and asserts what served it:
 *
 * 1. A `#(materialize)` source with a zoned timestamp (and a UUID) column is
 *    served from its table, the timestamp typed as live types it: a colocated
 *    table is bound with the columns its build captured that the plan cannot
 *    spell, as a `storage=` one is with every column, where the planned
 *    columns alone left the zoned timestamp out and sent the whole source
 *    live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

/** Each tier: its placement on a declaration, and its `name=` when colocated. */
const TIERS = [
   {
      label: "storage=",
      place: (_name: string) => ` storage="${DESTINATION}"`,
      servedFrom: "storage",
   },
   {
      label: "colocated",
      place: (name: string) => ` name="${name}"`,
      servedFrom: null,
   },
] as const;

/** Field types, without what a stored table cannot carry (KL 12). */
function comparableFields(fields: { name: string; type: unknown }[]) {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype" || key === "annotations" ? undefined : value,
      ),
   );
}

/** A run the served copy answered, as live did, typed as live types it. */
function expectServedCopy(run: R4Run): void {
   expect(run.live.length).toBeGreaterThan(0);
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(comparableFields(run.servedFields)).toEqual(
      comparableFields(run.liveFields),
   );
   expect(run.servedVia).toBe("served_copy");
}

for (const tier of TIERS) {
   describe(`R4 parity (${tier.label}): a #(materialize) table with a zoned timestamp and a UUID`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": `#(materialize)${tier.place("parity_zoned")}
source: s is duckdb.table('ex') extend { primary_key: id }
`,
            },
            seedSql: [
               `CREATE TABLE ex AS SELECT * FROM (VALUES
                  (1, 'a', 1.5, '11111111-2222-3333-4444-555555555555'::UUID, TIMESTAMPTZ '2024-01-02 03:04:05+00'),
                  (2, 'b', 2.5, NULL, NULL),
                  (3, 'a', 3.5, '31111111-2222-3333-4444-555555555555'::UUID, TIMESTAMPTZ '2024-06-30 23:00:00+00')
                ) AS t(id, g, amount, x, seen)`,
            ],
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("binds the source with no problem", async () => {
         expect(await fixture.servedCopyProblems()).toEqual([]);
      });

      it.each([
         "run: s -> { group_by: g; aggregate: n is count(), total is amount.sum(); order_by: g }",
         "run: s -> { select: id, seen; order_by: id }",
         "run: s -> { timezone: 'America/Los_Angeles'; group_by: d is seen.day; aggregate: n is count(); order_by: d }",
      ])("serves, typed and equal to live: %s", async (malloy) => {
         expectServedCopy(await fixture.run({ name: malloy, malloy }));
      });

      it("answers a query reading the UUID live, equal to live", async () => {
         const malloy = "run: s -> { select: id, x; order_by: id }";
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.live.length).toBe(3);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedVia).toBeUndefined();
      });
   });

}
