// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for three rollup shapes the served copy already handled and a rollup
 * refused (opportunity 13). Each answer served from the rollup equals live,
 * and equals the truth computed by SQL over the seed tables.
 *
 * - **A hidden grain dimension.** A `private` or `internal` dimension in a
 *   grain is stored, and redeclared with its modifier on the variant, so no
 *   query selects it and a query grouping by the rest of the grain is read
 *   from the rollup. A `private` dimension of a source the rollup's base
 *   extends is never stored: refused (`hidden_field`), answered live.
 * - **A given-reading `where:`** whose every field is a grain dimension is
 *   left out of the build, so the table holds every caller's groups, and is
 *   written again on the variant, where each caller's givens bind it (the
 *   pre-proof and `govern()` re-prove it, as a gate). Two callers,
 *   interleaved and repeated, each get exactly their own rows. A term over a
 *   field the grain does not keep, or one inherited from another source, is
 *   refused (`reads_given`) and answered live.
 * - **A zoned query that reads no time.** A query that sets its own
 *   `timezone:` is read from the rollup when nothing it reads depends on the
 *   zone; one reading a stored truncation is still answered live.
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

const seedSql = [
   `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'A', 'east', 't1', TIMESTAMP '2026-01-01 03:00:00'),
      (2, 20.0, 'A', 'west', 't2', TIMESTAMP '2026-01-01 09:00:00'),
      (3, 30.0, 'B', 'east', 't1', TIMESTAMP '2026-02-01 10:00:00'),
      (4,  5.0, 'B', 'west', 't2', TIMESTAMP '2026-02-01 02:00:00'),
      (5, 12.5, 'C', 'west', 't1', TIMESTAMP '2026-01-01 23:30:00'),
      (6,  7.5, 'C', 'east', 't2', TIMESTAMP '2026-02-01 12:00:00')
    ) AS t(id, amount, category, region, tenant, order_time)`,
   `CREATE TABLE cats_raw AS SELECT DISTINCT category FROM orders_raw`,
];

const model = (p: string) => `##! experimental { givens access_modifiers }
given: REGION :: string

// (a) A private and an internal grain dimension.
source: hidden is duckdb.table('orders_raw') extend {
  private dimension: secret_cat is category
  internal dimension: tenant_i is tenant
  #(preaggregate) grain=["secret_cat", "region"]${p}
  measure: total is amount.sum()
  #(preaggregate) grain=["tenant_i", "region"]${p}
  measure: t_total is amount.sum()
  view: by_secret is { group_by: secret_cat; aggregate: total }
}
source: private_base is duckdb.table('orders_raw') extend {
  private dimension: hidden_cat is category
}
// A private dimension of the source it extends: never stored.
source: inherits_private is private_base extend {
  #(preaggregate) grain=["hidden_cat", "region"]${p}
  measure: total is amount.sum()
}

// (b) A given-reading where: over grain dimensions.
source: scoped is duckdb.table('orders_raw') extend {
  where: region = $REGION
  #(preaggregate) grain=["category", "region"]${p}
  measure: total is amount.sum()
  #(preaggregate) grain=["category", "region"]${p}
  measure: n is count()
}
// A parent joining it: the join reads the rollup's every-caller groups
// unless the term goes with it, so the join branch is refused (live).
source: cats is duckdb.table('cats_raw') extend {
  primary_key: category
  join_many: scoped on scoped.category = category
}

// Over a field the grain does not keep: refused.
source: scoped_outside is duckdb.table('orders_raw') extend {
  where: region = $REGION
  #(preaggregate) grain=["category"]${p}
  measure: total is amount.sum()
}
// Inherited from the source it extends: refused.
source: scoped_inherited is scoped extend {
  #(preaggregate) grain=["category", "region"]${p}
  measure: inherited_total is amount.sum()
}

// (c) Zoned queries.
source: timed is duckdb.table('orders_raw') extend {
  dimension: order_day is order_time.day
  #(preaggregate) grain=["category", "order_day"]${p}
  measure: total is amount.sum()
}
`;

function expectRollup(run: R4Run): void {
   expect(run.servedBy?.kind).toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

function expectLive(run: R4Run): void {
   expect(run.servedBy?.kind).not.toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

for (const placement of ["colocated", "storage"] as const) {
   describe(`R4: rollup gaps closed (opportunity 13), ${placement}`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "on" },
         }).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": model(
                  placement === "storage" ? ` storage="${DESTINATION}"` : "",
               ),
            },
            seedSql,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      describe("a hidden grain dimension", () => {
         it("answers from a rollup whose grain holds a private dimension, equal to live", async () => {
            expectRollup(
               await fixture.run({
                  name: "private grain",
                  malloy:
                     "run: hidden -> { group_by: region; aggregate: total }",
                  truthSql:
                     "SELECT region, CAST(SUM(amount) AS DOUBLE) AS total FROM orders_raw GROUP BY 1",
               }),
            );
            expectRollup(
               await fixture.run({
                  name: "view grouping by the private dimension",
                  malloy: "run: hidden -> by_secret",
                  truthSql:
                     "SELECT category AS secret_cat, CAST(SUM(amount) AS DOUBLE) AS total FROM orders_raw GROUP BY 1",
               }),
            );
         });

         it("answers from a rollup whose grain holds an internal dimension, equal to live", async () => {
            expectRollup(
               await fixture.run({
                  name: "internal grain",
                  malloy:
                     "run: hidden -> { group_by: region; aggregate: t_total }",
                  truthSql:
                     "SELECT region, CAST(SUM(amount) AS DOUBLE) AS t_total FROM orders_raw GROUP BY 1",
               }),
            );
         });

         it("never lets a query select the hidden dimension, as live does not", async () => {
            fixture.setBindings(fixture.bindings);
            for (const field of ["secret_cat", "tenant_i"]) {
               await expect(
                  fixture.runAsIs(
                     `run: hidden -> { group_by: ${field}; aggregate: total }`,
                  ),
               ).rejects.toThrow();
            }
         });

         it.skipIf(placement === "storage")(
            "redeclares each hidden grain dimension with its modifier on the variant",
            async () => {
               const compiled = () =>
                  (
                     fixture.model as unknown as {
                        rollupVariants: {
                           compiled: Map<
                              string,
                              Promise<{
                                 modelDef?: {
                                    contents?: Record<string, unknown>;
                                 };
                              }>
                           >;
                        };
                     }
                  ).rollupVariants.compiled;
               const modifiers = new Map<string, string | undefined>();
               for (const measure of ["total", "t_total"]) {
                  expectRollup(
                     await fixture.run({
                        name: `variant ${measure}`,
                        malloy: `run: hidden -> { group_by: region; aggregate: ${measure} }`,
                     }),
                  );
                  for (const d of await Promise.all([...compiled().values()])) {
                     const fields =
                        (
                           d.modelDef?.contents?.hidden as
                              | {
                                   fields?: {
                                      name?: string;
                                      as?: string;
                                      accessModifier?: string;
                                   }[];
                                }
                              | undefined
                        )?.fields ?? [];
                     for (const f of fields) {
                        modifiers.set(f.as ?? f.name ?? "", f.accessModifier);
                     }
                  }
               }
               expect(modifiers.get("secret_cat")).toBe("private");
               expect(modifiers.get("tenant_i")).toBe("internal");
            },
         );

         it("refuses a private dimension of the source it extends, and answers live", async () => {
            expect(
               fixture.rollupRefusals.filter(
                  (r) => r.sourceName === "inherits_private",
               ),
            ).toEqual([
               { reason: "hidden_field", sourceName: "inherits_private" },
            ]);
            expectLive(
               await fixture.run({
                  name: "inherited private",
                  malloy:
                     "run: inherits_private -> { group_by: region; aggregate: total }",
                  expectLive: true,
               }),
            );
         });
      });

      describe("a given-reading where: over grain dimensions", () => {
         const truth = (region: string) =>
            `SELECT category, CAST(SUM(amount) AS DOUBLE) AS total, COUNT(*) AS n FROM orders_raw WHERE region = '${region}' GROUP BY 1`;

         it.skipIf(placement === "storage")(
            "stores every caller's groups: the build left the term out",
            async () => {
               const table = Object.entries(fixture.rollupTables).find(
                  ([name]) => name.startsWith("scoped__rollup__"),
               )?.[1];
               expect(table).toBeDefined();
               const rows = await fixture.duckdb.runSQL(
                  `SELECT DISTINCT region FROM ${table} ORDER BY 1`,
               );
               expect(rows.rows.map((r) => r.region)).toEqual(["east", "west"]);
            },
         );

         it("answers two callers, interleaved and repeated, each with exactly its own rows", async () => {
            for (const region of ["east", "west", "east", "west"]) {
               const run = await fixture.run({
                  name: `scoped ${region}`,
                  malloy:
                     "run: scoped -> { group_by: category; aggregate: total, n }",
                  givens: { REGION: region },
                  truthSql: truth(region),
               });
               expectRollup(run);
               expect(
                  rowDifference(run.truth ?? [], run.served),
               ).toBeUndefined();
               expect(run.served.length).toBeGreaterThan(0);
               const byRegion = await fixture.run({
                  name: `scoped ${region} by region`,
                  malloy:
                     "run: scoped -> { group_by: region, category; aggregate: total }",
                  givens: { REGION: region },
               });
               expectRollup(byRegion);
               for (const row of byRegion.served)
                  expect(row.region).toBe(region);
            }
         });

         it("governs the routed answer on the author's source, as live", async () => {
            const { unrecordedAudit } = await import(
               "../../../src/service/access_record"
            );
            const { QueryPrivileges } = await import(
               "../../../src/service/model_query_request"
            );
            const governed = async (givens: Record<string, string>) => {
               const audit = unrecordedAudit();
               const result = await fixture.model.runQuery(
                  audit,
                  {
                     query: "run: scoped -> { group_by: category; aggregate: total }",
                     filterParams: {},
                     givens,
                  },
                  QueryPrivileges.of({ bypassFilters: true }),
               );
               return { audit, result };
            };
            fixture.setBindings(fixture.bindings);
            const served = await governed({ REGION: "west" });
            expect(served.result.path?.servedBy?.kind).toBe("rollup");
            fixture.setBindings([]);
            const live = await governed({ REGION: "west" });
            expect(served.audit.summary?.source).toBe(
               live.audit.summary?.source,
            );
            expect(served.audit.summary?.decision).toBe(
               live.audit.summary?.decision,
            );
            fixture.setBindings(fixture.bindings);
         });

         it("answers a parent joining it live, never from its rollup through the join, each caller's rows", async () => {
            for (const region of ["east", "west", "east", "west"]) {
               const run = await fixture.run({
                  name: `cats ${region}`,
                  malloy:
                     "run: cats -> { group_by: category; aggregate: t is scoped.total }",
                  givens: { REGION: region },
                  truthSql: `SELECT c.category, CAST(SUM(o.amount) AS DOUBLE) AS t FROM cats_raw c LEFT JOIN orders_raw o ON o.category = c.category AND o.region = '${region}' GROUP BY 1`,
                  expectLive: true,
               });
               expectLive(run);
               expect(
                  rowDifference(run.truth ?? [], run.served),
               ).toBeUndefined();
            }
            // Refused for its re-applied term, not by chance.
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(
               "run: cats -> { group_by: category; aggregate: t is scoped.total }",
               { REGION: "east" },
            );
            expect(JSON.stringify(path?.considered ?? [])).toContain(
               "re-applies a `where:` that reads a given",
            );
         });

         // In a destination the join branch needs the parent stored beside it,
         // which `cats` is not, so it declines there before any variant.
         it.skipIf(placement === "storage")(
            "denies a join answer that reaches it past the leg's refusal (govern(), the second layer)",
            async () => {
               const { allowGivenTermJoinsForTests } = await import(
                  "../../../src/service/materialization/serve/join_rollup"
               );
               const { AccessDeniedError } = await import(
                  "../../../src/errors"
               );
               // The join branches are planned once per model: forget them, so
               // they are planned again under the seam (and after it).
               const forget = () => {
                  (
                     fixture.model as unknown as {
                        rollupVariants: { planMemo?: unknown };
                     }
                  ).rollupVariants.planMemo = undefined;
               };
               const restoreSeam = allowGivenTermJoinsForTests();
               forget();
               try {
                  fixture.setBindings(fixture.bindings);
                  await expect(
                     fixture.runAsIs(
                        "run: cats -> { group_by: category; aggregate: t is scoped.total }",
                        { REGION: "east" },
                     ),
                  ).rejects.toThrow(AccessDeniedError);
               } finally {
                  restoreSeam();
                  forget();
                  fixture.setBindings(fixture.bindings);
               }
            },
         );

         it("refuses a term over a field the grain does not keep, and one inherited, and answers both live", async () => {
            expect(
               fixture.rollupRefusals
                  .filter((r) => r.sourceName.startsWith("scoped_"))
                  .sort((a, b) => a.sourceName.localeCompare(b.sourceName)),
            ).toEqual([
               {
                  reason: "inherited_where_with_given",
                  sourceName: "scoped_inherited",
               },
               {
                  reason: "given_in_persisted_query",
                  sourceName: "scoped_outside",
               },
            ]);
            for (const source of ["scoped_outside", "scoped_inherited"]) {
               expectLive(
                  await fixture.run({
                     name: `${source} live`,
                     malloy: `run: ${source} -> { group_by: category; aggregate: total }`,
                     givens: { REGION: "east" },
                     expectLive: true,
                  }),
               );
            }
         });
      });

      describe("a zoned query", () => {
         it("is answered from the rollup when it reads no time", async () => {
            for (const malloy of [
               "run: timed -> { timezone: 'America/New_York'; group_by: category; aggregate: total }",
               "run: timed -> { group_by: category; nest: n is { timezone: 'America/New_York'; group_by: category; aggregate: total } }",
            ]) {
               expectRollup(
                  await fixture.run({
                     name: `zoned ${malloy}`,
                     malloy,
                     truthSql: malloy.includes("nest")
                        ? undefined
                        : "SELECT category, CAST(SUM(amount) AS DOUBLE) AS total FROM orders_raw GROUP BY 1",
                  }),
               );
            }
         });

         it("is answered live when it reads a stored truncation, a filter on one included", async () => {
            for (const malloy of [
               "run: timed -> { timezone: 'America/New_York'; group_by: order_day; aggregate: total }",
               "run: timed -> { timezone: 'America/New_York'; where: order_day = @2026-01-01; group_by: category; aggregate: total }",
            ]) {
               expectLive(
                  await fixture.run({
                     name: `zoned truncation ${malloy}`,
                     malloy,
                     expectLive: true,
                  }),
               );
            }
         });

         it("reads the truncation differently in the zone, which is why it stays live", async () => {
            // 03:00 UTC on Jan 1 is Dec 31 in New York: live's days differ from
            // the stored ones, so serving would be wrong.
            fixture.setBindings([]);
            const { rows } = await fixture.runAsIs(
               "run: timed -> { timezone: 'America/New_York'; group_by: order_day; aggregate: total; order_by: order_day }",
            );
            expect(rows.length).toBeGreaterThan(2);
            fixture.setBindings(fixture.bindings);
         });
      });
   });
}
