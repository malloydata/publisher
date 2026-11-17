// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the types a `storage=` rollup reads its stored table with (v1
 * `#@ preaggregate` and v2 `#(preaggregate)`), against live, beside the same
 * rollup colocated:
 *
 *  - a BIGINT sum past 2^53, stored as a HUGEINT partial: read through its
 *    native type, so every digit comes back;
 *  - a TIMESTAMPTZ grain: declared `timestamptz`, so it types as live's;
 *  - a big-integer partial whose stored column no cast reads exactly (a
 *    float): the rollup declines and the query answers live, never rounded.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

/** Field types with each number's subtype left out. */
function withoutSubtype(fields: { name: string; type: unknown }[]): unknown {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype" ? undefined : value,
      ),
   );
}

const SEED = [
   `CREATE TABLE typed AS SELECT * FROM (VALUES
      (CAST('9007199254740993' AS BIGINT), TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a'),
      (CAST('9007199254740991' AS BIGINT), TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a'),
      (CAST('-9007199254740993' AS BIGINT), TIMESTAMPTZ '2024-06-30 23:30:00+00', 'b')
    ) AS t(big, tstz, g)`,
];

const ROUTES = {
   v2: (grain: string, placement: string) =>
      `#(preaggregate) grain=["${grain}"]${placement}`,
   v1: (grain: string, placement: string) =>
      `#@ preaggregate grain="${grain}"${placement}`,
};

const BIG_SUM =
   "run: typed -> { group_by: g; aggregate: big_sum; order_by: g }";

for (const [route, annotate] of Object.entries(ROUTES)) {
   for (const [label, placement] of [
      ["colocated", ""],
      ["storage=", ` storage="${DESTINATION}"`],
   ] as const) {
      describe(`R4: the stored types a ${route} rollup reads [${label}]`, () => {
         let fixture: R4Fixture;
         useMaterializationOn();
         beforeAll(async () => {
            fixture = await buildScenario({
               files: {
                  "m.malloy": `##! experimental { persistence }
source: typed is duckdb.table('typed') extend {
  ${annotate("g", placement)}
  measure: big_sum is big.sum()
  ${annotate("tstz", placement)}
  measure: n_by_tstz is count()
}
`,
               },
               seedSql: SEED,
               preaggregate: true,
            });
         });
         afterAll(async () => {
            await disposeScenarios();
         });

         for (const [name, malloy] of [
            ["a BIGINT sum past 2^53", BIG_SUM],
            [
               "a TIMESTAMPTZ grain",
               "run: typed -> { group_by: tstz; aggregate: n_by_tstz; order_by: tstz }",
            ],
         ] as const) {
            it(`${name}: served from the rollup, rows and types equal to live`, async () => {
               const run = await fixture.run({ name, malloy });
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               expect(
                  strictRowDifference(run.live, run.served),
               ).toBeUndefined();
               expect(run.servedBy?.kind).toBe("rollup");
               // A stored number loses its integer subtype (known limitation
               // 12), so subtypes are not compared.
               expect(withoutSubtype(run.servedFields)).toEqual(
                  withoutSubtype(run.liveFields),
               );
            });
         }

         it("a BIGINT sum past 2^53: every digit, as live's", async () => {
            const run = await fixture.run({ name: "digits", malloy: BIG_SUM });
            expect(run.servedBy?.kind).toBe("rollup");
            expect(run.served.map((r) => String(r.big_sum))).toEqual([
               "18014398509481984",
               "-9007199254740993",
            ]);
         });

         if (placement) {
            it("a big-integer partial stored as a float: declined, answered live and exact", async () => {
               // The captured schema of a table whose partial lost its digits.
               const floated = fixture.bindings.map((b) =>
                  b.schema.some((c) => c.name === "big_sum__partial")
                     ? {
                          ...b,
                          schema: b.schema.map((c) =>
                             c.name === "big_sum__partial"
                                ? { ...c, type: "DOUBLE" }
                                : c,
                          ),
                       }
                     : b,
               );
               expect(floated).not.toEqual(fixture.bindings);
               fixture.setBindings([]);
               const live = await fixture.runAsIs(BIG_SUM);
               fixture.setBindings(floated);
               const served = await fixture.runAsIs(BIG_SUM);
               fixture.setBindings(fixture.bindings);
               expect(served.path?.servedBy.kind).toBe("live");
               expect(served.path?.considered).toContainEqual(
                  expect.objectContaining({
                     kind: "rollup",
                     outcome: "skipped",
                     reason: "MISSING_FIELD",
                  }),
               );
               expect(served.rows.map((r) => String(r.big_sum))).toEqual([
                  "18014398509481984",
                  "-9007199254740993",
               ]);
               expect(rowDifference(live.rows, served.rows)).toBeUndefined();
            });
         }
      });
   }
}
