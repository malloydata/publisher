// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R3 over a `private` tenant column: the row-level gate is read where it was
 * declared, so it reads the column the source hides, live and on the served
 * path alike, with PUBLISHER_MATERIALIZE_GATED_SOURCES on and off. Each of two
 * callers with different groups gets exactly the rows live returns to it, and
 * never the private column itself.
 *
 * Whether the stored table can answer depends on whether the build stores the
 * hidden column; either way the answer must be live's rows for that caller,
 * and an answer from storage must come through the served copy, where the
 * gate is grafted and proven.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
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

const model = `##! experimental { givens access_modifiers }

#(secure)
given: GROUPS :: string[]

#(access_filter) org_id in $GROUPS
#(materialize) storage="lake"
source: orders is duckdb.table('orders') include { private: org_id; public: * } extend {
  dimension: big is amount > 25
  measure: total is amount.sum(), n is count()
}

source: east_orders is orders extend {
  where: region = 'east'
}
`;

const byRegion =
   "run: orders -> { group_by: region; aggregate: total, n; order_by: region }";

const expected: Record<string, { region: string; total: number; n: number }[]> =
   {
      acme: [
         { region: "east", total: 10, n: 1 },
         { region: "west", total: 20, n: 1 },
      ],
      "globex,initech": [
         { region: "east", total: 80, n: 2 },
         { region: "west", total: 40, n: 1 },
      ],
   };

for (const gated of [true, false]) {
   describe(`R3: a row-level gate over a private column, gated serving ${gated ? "on" : "off"}`, () => {
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

      for (const groups of [["acme"], ["globex", "initech"]]) {
         it(`answers ${groups.join("+")} with its own rows only, equal to live`, async () => {
            const run = await fixture.run({
               name: groups.join("+"),
               malloy: byRegion,
               givens: { GROUPS: groups },
            });
            if (run.servedFrom === "storage") {
               expect(gated).toBe(true);
               expect(run.servedVia).toBe("served_copy");
            }
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(
               run.served.map((r) => ({
                  region: r.region,
                  total: Number(r.total),
                  n: Number(r.n),
               })),
            ).toEqual(expected[groups.join(",")]);
         });
      }

      it("answers through an extension with the caller's rows only", async () => {
         const run = await fixture.run({
            name: "extension",
            malloy: "run: east_orders -> { group_by: big; aggregate: total }",
            givens: { GROUPS: ["globex", "initech"] },
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.served.map((r) => [r.big, Number(r.total)]).sort()).toEqual(
            [[true, 80]],
         );
      });

      it("never lets a caller read the private column", async () => {
         let refusal: unknown;
         try {
            await fixture.run({
               name: "select private",
               malloy: "run: orders -> { group_by: org_id; aggregate: total }",
               givens: { GROUPS: ["acme"] },
            });
         } catch (error) {
            refusal = error;
         }
         expect(refusal).toBeDefined();
         expect(String(refusal)).toMatch(/private/);
      });

      it("never answers a caller who supplies no group", async () => {
         let refusal: unknown;
         try {
            await fixture.run({ name: "anonymous", malloy: byRegion });
         } catch (error) {
            refusal = error;
         }
         expect(refusal).toBeInstanceOf(AccessDeniedError);
      });
   });
}
