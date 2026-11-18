// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a rollup's hidden fields (coverage row `b-hidden`), for `#(rollup)`
 * and `#@ preaggregate`, in the source's own database and in a `storage=`
 * destination:
 *
 * - **A private dimension a source the base extends declares.** In the same
 *   file, the rollup's build copy lifts that source's statement, and each
 *   between it and the base, so the build reads it; the rollup stores it,
 *   redeclares it `private`, and answers a query grouping by the rest of the
 *   grain, equal to live. Declared in another file, which the copy cannot
 *   lift, the rollup is refused (`hidden_field`) and the query answers live.
 * - **No hidden field read through a rollup.** Every way a caller could reach
 *   a hidden measure or grain dimension (`private` or `internal`) is run live
 *   and served: where live refuses, the served run refuses with live's own
 *   error and no rows; where live answers (a caller's `extend` may read an
 *   `internal` field), the served run answers live's rows. A hidden measure
 *   is never built: refused (`hidden_field`), or for a v1 line left to its
 *   own validation's warning.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'A', 'east'), (2, 20.0, 'A', 'west'), (3, 30.0, 'B', 'east'),
      (4, 5.0, 'B', 'west'), (5, 12.5, 'C', 'west'), (6, 7.5, 'C', 'east')
    ) AS t(id, amount, category, region)`,
];

const TRUTH =
   "SELECT region, CAST(SUM(amount) AS DOUBLE) AS total FROM orders_raw GROUP BY 1";

type Mechanism = "v1" | "v2";

function files(mechanism: Mechanism, storage: string): Record<string, string> {
   const pre = (grain: string[]) =>
      mechanism === "v1"
         ? `#@ preaggregate grain="${grain.join(",")}"${storage}`
         : `#(rollup) grain=[${grain.map((g) => `"${g}"`).join(", ")}]${storage}`;
   return {
      "m.malloy": `##! experimental { access_modifiers }
import "elsewhere.malloy"

source: orders_base is duckdb.table('orders_raw') extend {
  private dimension: hidden_cat is category
}
// A private dimension of the source it extends.
source: inherits_private is orders_base extend {
  ${pre(["hidden_cat", "region"])}
  measure: total is amount.sum()
}
// Two statements down: both lifted in the build copy. (Its own measure, so
// its table is not inherits_private's: one build SQL is one table.)
source: between is orders_base extend {
  dimension: doubled is amount * 2
}
source: inherits_twice is between extend {
  ${pre(["hidden_cat", "region"])}
  measure: top is amount.max()
}
// Declared in another file: the build copy cannot lift it.
source: inherits_foreign is foreign_base extend {
  ${pre(["foreign_cat", "region"])}
  measure: total is amount.sum()
}

source: own_hidden is duckdb.table('orders_raw') extend {
  private dimension: secret_cat is category
  internal dimension: int_cat is category
  ${pre(["secret_cat", "region"])}
  private measure: secret_total is amount.sum()
  ${pre(["secret_cat", "region"])}
  internal measure: internal_m is amount.max()
  ${pre(["secret_cat", "region"])}
  measure: total is amount.sum()
  ${pre(["int_cat", "region"])}
  measure: t2 is amount.sum()
}
`,
      "elsewhere.malloy": `##! experimental { access_modifiers }
source: foreign_base is duckdb.table('orders_raw') extend {
  private dimension: foreign_cat is category
}
`,
   };
}

/** Queries a caller could use to reach a hidden field, live and served alike. */
const REACHES = [
   "run: own_hidden -> { group_by: region; aggregate: secret_total }",
   "run: own_hidden -> { group_by: region; aggregate: internal_m }",
   "run: own_hidden extend { measure: x is internal_m } -> { group_by: region; aggregate: x }",
   "run: own_hidden extend { measure: x is secret_total } -> { group_by: region; aggregate: x }",
   "run: own_hidden extend { dimension: x is secret_cat } -> { group_by: x; aggregate: total }",
   "run: own_hidden extend { dimension: x is int_cat } -> { group_by: x; aggregate: t2 }",
   "run: own_hidden -> { group_by: secret_cat; aggregate: total }",
   "run: own_hidden -> { group_by: int_cat; aggregate: t2 }",
   "run: own_hidden include { public: * } -> { group_by: secret_cat; aggregate: total }",
   "run: own_hidden include { public: secret_cat } -> { group_by: secret_cat; aggregate: total }",
   "run: own_hidden -> { where: secret_cat = 'A'; group_by: region; aggregate: total }",
   "run: inherits_private -> { group_by: hidden_cat; aggregate: total }",
   "run: inherits_private extend { dimension: x is hidden_cat } -> { group_by: x; aggregate: total }",
   "run: inherits_twice -> { group_by: hidden_cat; aggregate: top }",
   "run: inherits_twice include { public: * } -> { group_by: hidden_cat; aggregate: top }",
];

type Outcome = { rows: Row[]; by?: string } | { error: string };

for (const mechanism of ["v2", "v1"] as const) {
   for (const placement of ["database", "storage"] as const) {
      describe(`R4: a rollup's hidden fields (b-hidden), ${mechanism}, ${placement}`, () => {
         let restore: () => void;
         let fixture: R4Fixture;
         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "on",
               },
            }).restore;
            fixture = await buildScenario({
               files: files(
                  mechanism,
                  placement === "storage" ? ` storage="${DESTINATION}"` : "",
               ),
               seedSql,
               preaggregate: true,
            });
         });
         afterAll(async () => {
            restore();
            await disposeScenarios();
         });

         const outcome = async (
            malloy: string,
            served: boolean,
         ): Promise<Outcome> => {
            fixture.setBindings(served ? fixture.bindings : []);
            try {
               const r = await fixture.runAsIs(malloy);
               return { rows: r.rows, by: r.path?.servedBy?.kind };
            } catch (e) {
               return { error: e instanceof Error ? e.message : String(e) };
            }
         };

         for (const [source, measure, truthSql] of [
            ["inherits_private", "total", TRUTH],
            [
               "inherits_twice",
               "top",
               "SELECT region, CAST(MAX(amount) AS DOUBLE) AS top FROM orders_raw GROUP BY 1",
            ],
         ] as const) {
            it(`answers ${source} from a rollup over a private dimension of a source it extends, equal to live`, async () => {
               const run = await fixture.run({
                  name: source,
                  malloy: `run: ${source} -> { group_by: region; aggregate: ${measure} }`,
                  truthSql,
               });
               expect(run.servedBy?.kind).toBe("rollup");
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               expect(
                  rowDifference(run.truth ?? [], run.served),
               ).toBeUndefined();
            });
         }

         it("refuses a rollup over a private dimension declared in another file, and answers it live", async () => {
            expect(
               fixture.rollupRefusals.filter(
                  (r) => r.sourceName === "inherits_foreign",
               ),
            ).toEqual([
               { reason: "hidden_field", sourceName: "inherits_foreign" },
            ]);
            const run = await fixture.run({
               name: "foreign",
               malloy:
                  "run: inherits_foreign -> { group_by: region; aggregate: total }",
               truthSql: TRUTH,
               expectLive: true,
            });
            expect(run.servedBy?.kind).not.toBe("rollup");
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });

         it("never builds a rollup of a hidden measure", () => {
            // v1's own validation warns for its lines (preaggregate-hidden-field).
            expect(
               fixture.rollupRefusals.filter(
                  (r) => r.sourceName === "own_hidden",
               ),
            ).toEqual(
               mechanism === "v1"
                  ? []
                  : [
                       { reason: "hidden_field", sourceName: "own_hidden" },
                       { reason: "hidden_field", sourceName: "own_hidden" },
                    ],
            );
         });

         it("still answers a public measure from the rollup whose grain holds a private dimension", async () => {
            const run = await fixture.run({
               name: "public",
               malloy:
                  "run: own_hidden -> { group_by: region; aggregate: total }",
               truthSql: TRUTH,
            });
            expect(run.servedBy?.kind).toBe("rollup");
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });

         for (const malloy of REACHES) {
            it(`reads no more through a rollup than live: ${malloy}`, async () => {
               const live = await outcome(malloy, false);
               const served = await outcome(malloy, true);
               if ("error" in live) {
                  expect(served).toEqual({ error: live.error });
                  return;
               }
               expect("rows" in served).toBe(true);
               if ("rows" in served) {
                  expect(rowDifference(live.rows, served.rows)).toBeUndefined();
               }
            });
         }
      });
   }
}
