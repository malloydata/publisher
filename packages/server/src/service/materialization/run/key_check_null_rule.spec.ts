// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of the key checks' text and NULL rule, per writer, before
 * they share one emitter (step 2i). Two rules hold today, and each writer's
 * is pinned here by running its text over a NULL key, a repeated key, and a
 * clean one:
 *
 *  - ignored: an inlined join_one's check and a rollup's own key check. A
 *    NULL key meets no joining row, so it cannot inflate the join.
 *  - fails: a pair table's per-level check, a query source's primary key
 *    check, and a stored table's key check (raw SQL on the destination).
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   buildSourceName,
   generateBuildFile,
   type MaterializePlan,
} from "../plan/build_file_generator";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import {
   parentKeyCheckQuery,
   rollupBaseAlias,
   rollupKeyCheckQuery,
   type RollupVariantPlan,
} from "../plan/rollup_plan";
import { storedKeyCheckSql } from "./inline_uniqueness";

const duckdb = new DuckDBConnection("key_checks", ":memory:");
afterAll(() => duckdb.close());

/** The three key columns each rule is run over. */
const KEYS = {
   clean: "(1), (2), (3)",
   repeated: "(1), (1), (2)",
   oneNull: "(1), (NULL), (2)",
   twoNulls: "(1), (NULL), (NULL)",
} as const;
type Keys = keyof typeof KEYS;

/** Whether a check returns a row (a failed or not-unique key) per key column. */
type Verdicts = Record<Keys, boolean>;

const IGNORES_NULL: Verdicts = {
   clean: false,
   repeated: true,
   oneNull: false,
   twoNulls: false,
};
const FAILS_ON_NULL: Verdicts = {
   clean: false,
   repeated: true,
   oneNull: true,
   twoNulls: true,
};

function rowsOf(keys: Keys): string {
   return `SELECT * FROM (VALUES ${KEYS[keys]}) AS t(k)`;
}

/** Run a Malloy check over a source named `alias` holding the key rows. */
async function runMalloyCheck(
   alias: string,
   keys: Keys,
   check: string,
): Promise<boolean> {
   const url = "file:///checks/m.malloy";
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map([
            [
               url,
               `source: ${alias} is key_checks.sql("""${rowsOf(keys)}""")\n${check}\n`,
            ],
         ]),
      ),
      connections: new FixedConnectionMap(
         new Map([["key_checks", duckdb]]),
         "key_checks",
      ),
   });
   const model = runtime.loadModel(new URL(url));
   const result = await model.loadFinalQuery().run();
   return result.data.rowCount > 0;
}

async function verdicts(run: (keys: Keys) => Promise<boolean>) {
   const out = {} as Verdicts;
   for (const keys of Object.keys(KEYS) as Keys[]) out[keys] = await run(keys);
   return out;
}

describe("a stored table's key check (raw SQL on the destination)", () => {
   it("is this SQL", () => {
      expect(storedKeyCheckSql('"lake"."t"', 'k"ey')).toBe(
         'SELECT "k""ey" FROM "lake"."t" GROUP BY "k""ey" HAVING COUNT(*) > 1 OR "k""ey" IS NULL LIMIT 1',
      );
   });

   it("fails on a NULL key", async () => {
      expect(
         await verdicts(async (keys) => {
            const sql = storedKeyCheckSql(`(${rowsOf(keys)})`, "k");
            return (await duckdb.runSQL(sql)).rows.length > 0;
         }),
      ).toEqual(FAILS_ON_NULL);
   });
});

describe("a rollup's own key check", () => {
   const plan = {
      grainDimensions: ["k"],
      baseDeclaredName: "base",
   } as unknown as RollupVariantPlan;

   it("is this Malloy, only for a one-dimension colocated grain", () => {
      expect(rollupKeyCheckQuery(plan)).toBe(`run: base__rollup_base -> {
  where: k != null
  group_by: k
  aggregate: key_rows__check is count()
  having: key_rows__check > 1
  limit: 1
}`);
      expect(
         rollupKeyCheckQuery({ ...plan, grainDimensions: ["k", "j"] }),
      ).toBeUndefined();
      expect(
         rollupKeyCheckQuery({ ...plan, storage: "lake" } as never),
      ).toBeUndefined();
   });

   it("ignores a NULL key", async () => {
      expect(
         await verdicts((keys) =>
            runMalloyCheck(
               rollupBaseAlias("base"),
               keys,
               rollupKeyCheckQuery(plan)!,
            ),
         ),
      ).toEqual(IGNORES_NULL);
   });
});

describe("a query source's primary key check", () => {
   it("is this Malloy", () => {
      expect(parentKeyCheckQuery("flights", "id")).toBe(`run: flights -> {
  group_by: key__check is id
  aggregate: key_rows__check is count()
} -> {
  where: key_rows__check > 1 or key__check = null
  select: key__check
  limit: 1
}`);
   });

   it("fails on a NULL key", async () => {
      expect(
         await verdicts((keys) =>
            runMalloyCheck("parent", keys, parentKeyCheckQuery("parent", "k")),
         ),
      ).toEqual(FAILS_ON_NULL);
   });
});

describe("an inlined join_one's check and a pair table's key check", () => {
   const packages = tempBuildPlanPackages();
   let restore: () => void = () => {};
   beforeEach(() => {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
   });
   afterEach(async () => {
      restore();
      await packages.cleanup();
   });

   const plan: MaterializePlan = {
      sourceName: "orders",
      sourceID: "orders@file:///pkg/m.malloy",
      buildSourceName: buildSourceName("orders"),
      columns: [{ dimension: "id", column: "id", computed: false }],
      persist: {},
   };

   it("writes the two forms into the companion", () => {
      const text = generateBuildFile(
         [
            {
               ...plan,
               uniquenessChecks: [
                  {
                     join: "regions",
                     keyColumn: "code",
                     queryName: "orders__mat__unique__regions",
                     joinedSourceID: "regions@file:///pkg/m.malloy",
                  },
                  {
                     join: "",
                     keyColumn: "id",
                     queryName: "orders__mat__pair_key",
                     joinedSourceID: "orders@file:///pkg/m.malloy",
                     pairLevel: 0,
                  },
               ],
            },
         ],
         "m.malloy",
      )!;
      expect(text.slice(text.indexOf("query: "))).toBe(
         `query: orders__mat__unique__regions is orders__mat__unique__regions__source -> {
  where: code is not null
  group_by: code
  aggregate: matches is count()
  having: matches > 1
  limit: 1
}

query: orders__mat__pair_key is orders__mat__pair_key__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}
`,
      );
   });

   /** The compiled check of the one plan a model yields, run on DuckDB. */
   async function checkFinds(
      model: string,
      pairLevel: number | undefined,
   ): Promise<boolean> {
      const pkg = await packages.create(model);
      const c = await compilePackageBuildPlan(pkg);
      const [compiled] = Object.values(c.materializePlans ?? {});
      const check = compiled?.uniquenessChecks?.find(
         (x) => x.pairLevel === pairLevel,
      );
      if (!check?.sql) throw new Error("no compiled check");
      const conn = await pkg.getMalloyConnection("duckdb");
      return (await conn.runSQL(check.sql)).rows.length > 0;
   }

   it("an inlined join_one's check ignores a NULL key", async () => {
      expect(
         await verdicts((keys) =>
            checkFinds(
               `source: regions is duckdb.sql("""${rowsOf(keys)}""") extend { primary_key: k }
source: orders_raw is duckdb.sql("""SELECT 1 AS id, 1 AS region""")
#(materialize) name="orders_all" inline=["regions"] storage="lake"
source: orders is orders_raw extend {
  join_one: regions with region
}
`,
               undefined,
            ),
         ),
      ).toEqual(IGNORES_NULL);
   });

   it("a pair table's own key check fails on a NULL key", async () => {
      expect(
         await verdicts((keys) =>
            checkFinds(
               `source: lines is duckdb.sql("""SELECT 1 AS line_id, 1 AS order_id""") extend { primary_key: line_id }
source: orders_raw is duckdb.sql("""SELECT k AS id FROM (${rowsOf(keys)})""")
#(materialize) name="orders_all" storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
`,
               0,
            ),
         ),
      ).toEqual(FAILS_ON_NULL);
   });
});
