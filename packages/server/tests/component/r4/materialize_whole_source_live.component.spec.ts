// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-07 (campaign cases P2-26 and P2-31): which columns send a whole
 * `#(materialize)` source live, pinned as documented ("A column the served
 * copy cannot carry", docs/materialization.md).
 *
 * - A struct (record) column of the source's own. Malloy types it as a join,
 *   so the plan stores no column for it, and the served copy cannot carry the
 *   field: the source is left as written (`fields_not_carried`), in a
 *   `storage=` table and a database one alike. Every query over the source
 *   is answered live, equal to live.
 * - An inlined join's column with no stored declaration (a MAP, standing in
 *   for a warehouse GEOGRAPHY): the join's record cannot be declared, so the
 *   source is left as written (`column_types`).
 * - The control: a list column (`BIGINT[]` past 2^53) is left out per query,
 *   as on `#@ persist`, so a query that does not read it is still served.
 *
 * Each query's path report says live, skipping the table as `WARNED` with
 * the `served-copy-not-served` code (the package warning of that code names
 * the served copy's reason).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const OWN_SEED = [
   `CREATE TABLE ex AS SELECT * FROM (VALUES
      (1, 'a', 1.5, {'a': 9007199254740993::BIGINT}, [9007199254740993::BIGINT, 1]),
      (2, 'b', 2.5, {'a': 2::BIGINT}, [2::BIGINT]),
      (3, 'a', 3.5, {'a': 3::BIGINT}, [3::BIGINT])) AS t(id, g, amount, st, xs)`,
];

const NOT_READING =
   "run: s -> { group_by: g; aggregate: n is count(), total is amount.sum(); order_by: g }";

/** The path report of the served run: answered live, the table skipped. */
async function expectWholeSourceLive(fixture: R4Fixture, malloy: string) {
   const run = await fixture.run({ name: malloy, malloy, expectLive: true });
   expect(run.live.length).toBeGreaterThan(0);
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(run.servedVia).toBeUndefined();
   fixture.setBindings(fixture.bindings);
   const { path } = await fixture.runAsIs(malloy);
   expect(path?.servedBy?.kind).toBe("live");
   expect(
      (path?.considered ?? []).map((c) => [c.outcome, c.reason, c.code]),
   ).toEqual([["skipped", "NOT_SERVED", "served_copy"]]);
}

describe.each([
   {
      tier: "storage=",
      model: `#(materialize) storage="lake"
source: s is duckdb.table('ex') extend { primary_key: id }
`,
      database: undefined as string[] | undefined,
   },
   {
      tier: "database (a database origin)",
      model: `#(materialize) name="p2_26_database"
source: s is duckdb.table('ex') extend { primary_key: id }
`,
      database: ["s"] as string[] | undefined,
   },
])(
   "P2-26: a #(materialize) $tier source with a STRUCT(a BIGINT) column answers live as a whole",
   ({ model, database }) => {
      let fixture: R4Fixture;
      useMaterializationOn();
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql: OWN_SEED,
            materialize: true,
            ...(database ? { database } : {}),
         });
      });
      afterAll(async () => {
         await disposeScenarios();
      });

      it("stores no column for the struct, and the served copy leaves the source as written (fields_not_carried)", async () => {
         const [binding] = fixture.bindings;
         expect(binding.schema.map((c) => c.name)).not.toContain("st");
         expect(
            (await fixture.servedCopyProblems()).map((p) => [
               p.sourceName,
               p.reason,
            ]),
         ).toEqual([["s", "fields_not_carried"]]);
      });

      it.each([
         NOT_READING,
         "run: s -> { select: id, a is st.a; order_by: id }",
         "run: s -> { aggregate: m is xs.each.max() }",
      ])(
         "answers live, equal to live, and the path report says so: %s",
         async (malloy) => {
            await expectWholeSourceLive(fixture, malloy);
         },
      );
   },
);

describe("P2-26 control: a #(materialize) storage= source with only a BIGINT[] column leaves the column out per query", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `#(materialize) storage="lake"
source: s is duckdb.table('ex') extend { primary_key: id }
`,
         },
         // The same rows without the struct.
         seedSql: [
            OWN_SEED[0].replace(
               "CREATE TABLE ex AS SELECT *",
               "CREATE TABLE ex AS SELECT id, g, amount, xs",
            ),
         ],
         materialize: true,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("serves a query that does not read it", async () => {
      const run = await fixture.run({
         name: "not reading",
         malloy: NOT_READING,
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBe("served_copy");
   });

   it("answers a query reading it live, equal to live, declining the table for the missing field", async () => {
      const malloy = "run: s -> { aggregate: m is xs.each.max() }";
      const run = await fixture.run({ name: malloy, malloy, expectLive: true });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.live).toEqual([{ m: 9007199254740993n }]);
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(path?.servedBy?.kind).toBe("live");
      expect((path?.considered ?? []).map((c) => c.reason)).toEqual([
         "MISSING_FIELD",
      ]);
   });
});

describe("P2-31: an inline= join whose joined source has a column with no stored declaration answers live as a whole", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: par is duckdb.table('par') extend { primary_key: pid }

#(materialize) storage="lake" inline=["par"]
source: s is duckdb.table('ex') extend {
  primary_key: id
  join_one: par with pid
}
`,
         },
         seedSql: [
            `CREATE TABLE ex AS SELECT * FROM (VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 10)) AS t(id, g, pid)`,
            // A MAP stands in for a warehouse GEOGRAPHY: a joined column no
            // `type:` declares.
            `CREATE TABLE par AS SELECT * FROM (VALUES (10, 'x', MAP {'k': 1}), (20, 'y', MAP {'k': 2})) AS t(pid, nm, geo)`,
         ],
         materialize: true,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("stores the joined column, and the served copy leaves the source as written (column_types)", async () => {
      const [binding] = fixture.bindings;
      expect(binding.schema.map((c) => c.name)).toContain("par__geo");
      expect(
         (await fixture.servedCopyProblems()).map((p) => [
            p.sourceName,
            p.reason,
         ]),
      ).toEqual([["s", "column_types"]]);
   });

   it.each([
      "run: s -> { group_by: g; aggregate: n is count(); order_by: g }",
      "run: s -> { group_by: par.nm; aggregate: n is count(); order_by: nm }",
   ])(
      "answers live, equal to live, and the path report says so: %s",
      async (malloy) => {
         await expectWholeSourceLive(fixture, malloy);
      },
   );
});
