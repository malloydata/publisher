// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R3, contracts at read (materialization v2, phase 3 PR 5): the served copy is
 * the author's own text, gates included, so a query through a row-level gate
 * may be answered from the stored table with the gate grafted on the served
 * compile, returning exactly the rows live returns to the same caller. Behind
 * PUBLISHER_MATERIALIZE_GATED_SOURCES (off by default until the managed-tier
 * review, plan question 14); off, the gate keeps every stored variant away,
 * as today.
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

const model = `##! experimental.givens

given: GROUPS :: string[]

#(access_filter) org_id in $GROUPS
#(materialize) storage="lake"
source: orders is duckdb.table('orders') extend {
  dimension: big is amount > 25
  measure: total is amount.sum(), n is count()
}

source: east_orders is orders extend {
  where: region = 'east'
}
`;

const byOrg = "run: orders -> { group_by: org_id; aggregate: total, n }";

for (const gated of [true, false]) {
   describe(`R3: a row-level gate on a materialized source, gated serving ${gated ? "on" : "off"}`, () => {
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

      const cases: [string, string, { GROUPS: string[] }][] = [
         ["one group", byOrg, { GROUPS: ["acme"] }],
         ["two groups", byOrg, { GROUPS: ["acme", "globex"] }],
         ["no group", byOrg, { GROUPS: [] }],
         [
            "a stored computed dimension, through an extension",
            "run: east_orders -> { group_by: big; aggregate: total }",
            { GROUPS: ["globex", "initech"] },
         ],
      ];
      for (const [name, malloy, givens] of cases) {
         it(`answers ${name} with the caller's rows only, equal to live`, async () => {
            const run = await fixture.run({ name, malloy, givens });
            if (gated) {
               expect(run.servedVia).toBe("served_copy");
               expect(run.servedFrom).toBe("storage");
            } else {
               expect(run.servedFrom).not.toBe("storage");
            }
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            const orgs = new Set(run.served.map((r) => r.org_id));
            for (const org of orgs) {
               if (org !== undefined) {
                  expect(givens.GROUPS).toContain(org as string);
               }
            }
         });
      }

      it("answers a caller join into the gated source live, as with the switch off", async () => {
         // A caller join's gate is never grafted on a routed compile, so the
         // served copy must not answer past it (it would be denied).
         const run = await fixture.run({
            name: "caller join",
            malloy:
               "run: orders extend { join_one: o2 is orders on id = o2.id } -> { group_by: o2.org_id; aggregate: total }",
            givens: { GROUPS: ["acme"] },
         });
         expect(run.servedFrom).not.toBe("storage");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.served.map((r) => r.org_id)).toEqual(["acme"]);
      });

      if (gated) {
         // Last: it empties the origin table, so only the stored table can
         // answer, and the answer must still be the gate's rows of it.
         it("answers from the stored table, with the gate grafted on it", async () => {
            const truth = await fixture.storedSQL(
               (t) =>
                  `SELECT org_id, sum(amount)::DOUBLE AS total, count(*) AS n FROM ${t("orders")}
                   WHERE org_id IN ('globex') GROUP BY 1`,
            );
            await fixture.storedSQL(() => "DELETE FROM orders");
            const run = await fixture.run({
               name: "after the origin emptied",
               malloy: byOrg,
               givens: { GROUPS: ["globex"] },
            });
            expect(run.live).toEqual([]);
            expect(run.servedVia).toBe("served_copy");
            expect(rowDifference(truth, run.served)).toBeUndefined();
         });
      }

      it("never answers a caller who supplies no group", async () => {
         // Denied as a gate denial (403), not failed for some other reason.
         let refusal: unknown;
         try {
            await fixture.run({ name: "anonymous", malloy: byOrg });
         } catch (error) {
            refusal = error;
         }
         expect(refusal).toBeInstanceOf(AccessDeniedError);
      });
   });
}

describe("R3: a gated read whose stored table fails at run time falls back live", () => {
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
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("grafts the gate on the author's model for the retry, not the served copy's", async () => {
      for (const b of fixture.bindings) b.freshnessFallback = "live";
      const before = await fixture.run({
         name: "served",
         malloy: byOrg,
         givens: { GROUPS: ["acme"] },
      });
      expect(before.servedVia).toBe("served_copy");
      await fixture.storedSQL((t) => `DROP TABLE ${t("orders")}`);
      const run = await fixture.run({
         name: "after the table is gone",
         malloy: byOrg,
         givens: { GROUPS: ["acme"] },
      });
      expect(run.servedFrom).toBe("live_fallback");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.served.map((r) => r.org_id)).toEqual(["acme"]);
   });
});
