// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database table that fails at run time, under `freshness.fallback="live"`:
 * a column renamed after the build, so the stored read is a binder error.
 * Every mechanism answers live with live's rows (`servedFrom: live_fallback`).
 *
 * Before the fix a database rollup returned the error: it reads its table
 * through the build manifest, not a virtual map, and only an answer read
 * through a virtual map was retried live, though a rollup only accelerates.
 * A database `#@ persist` or `#(materialize)` table, which the served copy
 * reads through `virtual()`, already fell back. And the path report names the
 * failed table `READ_FAILED`, never as the table that answered.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { buildScenario, disposeScenarios, type R4Fixture } from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES (1, 'a', 10.0), (2, 'b', 20.0)) AS t(id, cat, amt)`,
];

const QUERY = "run: m -> { group_by: cat; aggregate: total }";
const LIVE_ROWS = [
   { cat: "a", total: 10 },
   { cat: "b", total: 20 },
];

const CASES = {
   materialize: {
      model: `source: ev is duckdb.table('ev')
#(materialize) name="runfail_m" freshness.window="24h" freshness.fallback="live"
source: m is ev extend { measure: total is amt.sum() }
`,
      scenario: { materialize: true },
      servedBy: "materialized",
      renamed: "amt",
   },
   persist: {
      model: `##! experimental { persistence }
#@ persist name="runfail_m" freshness.window="24h" freshness.fallback="live"
source: m is duckdb.sql("SELECT * FROM ev") extend { measure: total is amt.sum() }
`,
      scenario: { database: ["m"] },
      servedBy: "materialized",
      renamed: "amt",
   },
   rollup: {
      model: `source: m is duckdb.table('ev') extend {
  #(rollup) grain=["cat"] freshness.window="24h" freshness.fallback="live"
  measure: total is amt.sum()
}
`,
      scenario: { preaggregate: true },
      servedBy: "rollup",
      renamed: "cat",
   },
} as const;

const byCat = (rows: unknown) =>
   [...(rows as { cat: string }[])].sort((a, b) => a.cat.localeCompare(b.cat));

for (const [kind, c] of Object.entries(CASES)) {
   describe(`a database ${kind} table that fails at run time, fallback=live`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": c.model },
            seedSql,
            ...c.scenario,
         });
      }, 120_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("is served from its table, then answers live with live's rows once the table breaks", async () => {
         for (const b of fixture.bindings) b.freshnessFallback = "live";
         const before = await fixture.run({ name: "before", malloy: QUERY });
         expect(before.servedFrom).toBe("database");
         expect(before.servedBy?.kind).toBe(c.servedBy);
         expect(byCat(before.served)).toEqual(LIVE_ROWS);

         const tables = await fixture.duckdb.runSQL(
            "SELECT table_name FROM information_schema.tables WHERE table_name <> 'ev'",
         );
         const [stored] = (tables.rows as { table_name: string }[]).map(
            (r) => r.table_name,
         );
         expect(stored).toBeDefined();
         await fixture.duckdb.runSQL(
            `ALTER TABLE "${stored}" RENAME COLUMN ${c.renamed} TO ${c.renamed}_gone`,
         );

         const after = await fixture.run({ name: "after", malloy: QUERY });
         expect(after.servedFrom).toBe("live_fallback");
         expect(byCat(after.served)).toEqual(LIVE_ROWS);
         // The path report says no table answered: the one tried failed.
         expect(after.servedBy?.kind).not.toBe(c.servedBy);
      });
   });
}
