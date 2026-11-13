// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   applyDeltaScript,
   planIncrementalStep,
   type SqlRunner,
   warehouseDeltaTarget,
} from "../run/incremental_apply";
import { incrementalLineage } from "../run/incremental_build";
import {
   buildCopyText,
   givenReadTerms,
   type MaterializePlan,
} from "./build_file_generator";
import {
   addressSQL,
   buildSQLOf,
   collectIncrementalDeclarations,
} from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";
import { leftOutWindowTerms } from "./rolling_window";

/**
 * Upper-bound rolling windows on the `#(materialize)` build side (plan,
 * Rolling windows; question 2 decided 2026-11-12, option A): with no refresh
 * interval the widest set any read needs is every row that exists at build
 * time, so a `where:` keeping rows older than a moving cut-off is left out of
 * the build, own or inherited, and re-applied at read by the served copy. A
 * two-sided window builds its lower bound and leaves its upper bound out. No
 * clock literal is written, so the address does not move between builds.
 */

const packages = tempBuildPlanPackages();
let restore: () => void;

beforeEach(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});
const opened: { runSQL: (sql: string) => Promise<unknown> }[] = [];
afterEach(async () => {
   restore?.();
   // DuckDB shares one in-memory database by connection name across spec
   // files, so leave none of these tables behind.
   for (const db of opened.splice(0)) {
      for (const t of ["rwu_ev", "rwu_inc", "rwu_full"]) {
         await db.runSQL(`DROP TABLE IF EXISTS ${t}`).catch(() => {});
      }
   }
   await packages.cleanup();
});

// DuckDB shares one in-memory database by connection name, so each package
// replaces the table. One row an hour, from 120 days before the warehouse's own clock to 2 days
// before it: every row exists at build time.
const SEED = `CREATE OR REPLACE TABLE rwu_ev AS
  SELECT ts, row_number() OVER (ORDER BY ts) AS amount
  FROM range(date_trunc('hour', LOCALTIMESTAMP) - INTERVAL 120 DAY,
             date_trunc('hour', LOCALTIMESTAMP) - INTERVAL 2 DAY,
             INTERVAL 1 HOUR) t(ts)`;

async function compiled(files: string | Record<string, string>) {
   const pkg = await packages.create(files);
   const duckdb = await pkg.getMalloyConnection!("duckdb");
   opened.push(duckdb);
   await duckdb.runSQL(SEED);
   return { pkg, duckdb, c: await compilePackageBuildPlan(pkg) };
}

type Compiled = Awaited<ReturnType<typeof compilePackageBuildPlan>>;

function planNamed(c: Compiled, name: string): MaterializePlan {
   const plan = Object.values(c.materializePlans ?? {}).find(
      (p) => p.sourceName === name,
   );
   if (!plan) {
      throw new Error(
         `${name} was not planned: ${JSON.stringify(c.annotationFindings)}`,
      );
   }
   return plan;
}

function buildSQL(c: Compiled, name: string): string {
   const plan = planNamed(c, name);
   return buildSQLOf(
      c.sources[plan.sourceID],
      { entries: {} },
      c.targets?.[plan.sourceID],
   );
}

const NOW = /LOCALTIMESTAMP|CURRENT_TIMESTAMP/g;

const MODEL = `source: ev is duckdb.table('rwu_ev')

#(materialize) storage="lake"
source: older is ev extend {
  where: ts < now - 30 days
  where: amount > 1
}

source: older_base is ev extend { where: now - 30 days > ts }

#(materialize) storage="lake"
source: older_inherited is older_base extend { where: amount > 1 }

#(materialize) storage="lake"
source: between is ev extend {
  where: ts >= now - 60 days and ts < now - 30 days
}

#(materialize) storage="lake"
source: between_two is ev extend {
  where: ts >= now - 60 days
  where: ts < now - 30 days
}
`;

async function count(
   duckdb: { runSQL: (sql: string) => Promise<{ rows: unknown[] }> },
   sql: string,
): Promise<number> {
   const [row] = (await duckdb.runSQL(`SELECT count(*) AS n FROM (${sql}) t`))
      .rows as { n: unknown }[];
   return Number(row.n);
}

describe("an upper-bound window is left out of the build and kept for the read", () => {
   it(
      "builds every row there is for an own and an inherited upper bound, and keeps the fixed terms",
      async () => {
         const { duckdb, c } = await compiled(MODEL);
         const all = await count(duckdb, "SELECT * FROM rwu_ev");
         for (const name of ["older", "older_inherited"]) {
            const plan = planNamed(c, name);
            expect(
               plan.readTimeTerms?.map((t) => [
                  t.kind,
                  t.code,
                  "leftOut" in t && t.leftOut !== undefined,
               ]),
            ).toEqual([
               [
                  "current_time",
                  name === "older"
                     ? "ts < now - 30 days"
                     : "now - 30 days > ts",
                  true,
               ],
            ]);
            const sql = buildSQL(c, name);
            // No clock in the build at all: the upper bound is left out.
            expect(sql.match(NOW)).toBeNull();
            // Every row but the fixed term's one (amount 1).
            expect(await count(duckdb, sql)).toBe(all - 1);
         }
      },
      { timeout: 30000 },
   );

   it(
      "builds a two-sided window's lower bound and leaves its upper bound out, written as one term or two",
      async () => {
         const { duckdb, c } = await compiled(MODEL);
         const lower = await count(
            duckdb,
            "SELECT * FROM rwu_ev WHERE ts >= LOCALTIMESTAMP - INTERVAL 60 DAY",
         );
         for (const name of ["between", "between_two"]) {
            const sql = buildSQL(c, name);
            // One clock reference: the lower bound.
            expect(sql.match(NOW)).toHaveLength(1);
            expect(await count(duckdb, sql)).toBe(lower);
         }
      },
      { timeout: 30000 },
   );

   it(
      "writes the build copy with the term replaced by true, own and inherited, and the lower bound in the build query",
      async () => {
         const { c } = await compiled(MODEL);
         const own = buildCopyText(MODEL, planNamed(c, "older"));
         if (!("copy" in own)) throw new Error(own.reason);
         expect(own.copy).toContain("where: true\n  where: amount > 1");
         const inherited = buildCopyText(
            MODEL,
            planNamed(c, "older_inherited"),
         );
         if (!("copy" in inherited)) throw new Error(inherited.reason);
         expect(inherited.copy).toContain(
            "source: older_base is ev extend { where: true }",
         );
         // Each copy takes out its own source's terms, never another's.
         expect(inherited.copy).toContain("where: ts < now - 30 days");
         const between = buildCopyText(MODEL, planNamed(c, "between"));
         if (!("copy" in between)) throw new Error(between.reason);
         expect(between.copy).toContain(
            "source: between is ev extend {\n  where: true\n}",
         );
      },
      { timeout: 30000 },
   );

   it(
      "addresses the build without a clock literal, so two builds share an address",
      async () => {
         const first = await compiled(MODEL);
         const second = await compiled(MODEL);
         for (const name of ["older", "older_inherited", "between"]) {
            const a = planNamed(first.c, name);
            const b = planNamed(second.c, name);
            const sqlA = addressSQL(
               first.c.sources[a.sourceID],
               first.c.targets?.[a.sourceID],
            );
            const sqlB = addressSQL(
               second.c.sources[b.sourceID],
               second.c.targets?.[b.sourceID],
            );
            expect(sqlA).not.toMatch(/TIMESTAMP '20\d\d-/);
            expect(sqlA).not.toMatch(/20\d\d-\d\d-\d\d \d\d:\d\d/);
            expect(sqlB).toBe(sqlA);
         }
      },
      { timeout: 60000 },
   );

   it(
      "leaves out a given term and an upper bound together",
      async () => {
         const text = `##! experimental { givens }
given: ORG :: number is 1
source: ev is duckdb.table('rwu_ev')
#(materialize) storage="lake"
source: older is ev extend {
  where: ts < now - 30 days
  where: amount = $ORG
}
`;
         const { duckdb, c } = await compiled(text);
         const plan = planNamed(c, "older");
         expect(givenReadTerms(plan).map((t) => t.code)).toEqual([
            "amount = $ORG",
         ]);
         expect(leftOutWindowTerms(plan.readTimeTerms)).toHaveLength(1);
         const sql = buildSQL(c, "older");
         expect(sql.match(NOW)).toBeNull();
         expect(await count(duckdb, sql)).toBe(
            await count(duckdb, "SELECT * FROM rwu_ev"),
         );
      },
      { timeout: 30000 },
   );

   it(
      "does not chain an extension over a parent whose build leaves an upper bound out",
      async () => {
         const text = `source: ev is duckdb.table('rwu_ev')
#(materialize) storage="lake"
source: older is ev extend { where: ts < now - 30 days }
#(materialize) storage="lake"
source: older_big is older extend { where: amount > 100 }
`;
         const { duckdb, c } = await compiled(text);
         const plan = planNamed(c, "older_big");
         expect(plan.buildsOver).toBeUndefined();
         const sql = buildSQL(c, "older_big");
         expect(sql.match(NOW)).toBeNull();
         expect(await count(duckdb, sql)).toBe(
            await count(duckdb, "SELECT * FROM rwu_ev WHERE amount > 100"),
         );
      },
      { timeout: 30000 },
   );
});

describe("what is still refused", () => {
   it(
      "refuses an upper bound written in another file, which the build copy cannot reach",
      async () => {
         const { c } = await compiled({
            "base.malloy": `source: ev is duckdb.table('rwu_ev')
source: older_base is ev extend { where: ts < now - 30 days }
`,
            "m.malloy": `import { older_base } from 'base.malloy'
#(materialize) storage="lake"
source: older is older_base extend { where: amount > 1 }
`,
         });
         expect(
            Object.values(c.materializePlans ?? {}).map((p) => p.sourceName),
         ).not.toContain("older");
         const reason = (c.annotationFindings ?? []).find(
            (f) => f.code === "materialize-not-built" && f.subject === "older",
         )?.message;
         expect(reason).toContain("written in another file");
      },
      { timeout: 30000 },
   );

   it(
      "refuses any other now-reading shape, and now through a dimension",
      async () => {
         const { c } = await compiled(`source: ev is duckdb.table('rwu_ev')
#(materialize) storage="lake"
source: shifted is ev extend { where: ts < now.day - 30 days }
#(materialize) storage="lake"
source: mixed is ev extend { where: ts < now - 30 days and amount > 1 }
#(materialize) storage="lake"
source: either is ev extend { where: ts < now - 30 days or ts > now }
#(materialize) storage="lake"
source: via_dim is ev extend {
  dimension: is_old is ts < now - 30 days
  where: is_old
}
`);
         expect(Object.keys(c.materializePlans ?? {})).toEqual([]);
         const refused = (c.annotationFindings ?? [])
            .filter((f) => f.code === "materialize-not-built")
            .map((f) => f.subject)
            .sort();
         expect(refused).toEqual(["either", "mixed", "shifted", "via_dim"]);
      },
      { timeout: 30000 },
   );
});

describe("an incremental refresh equals a full rebuild", () => {
   const INCREMENTAL = `source: ev is duckdb.table('rwu_ev')
#(materialize) refresh="incremental" watermark="ts" storage="lake"
source: older is ev extend { where: ts < now - 30 days }
#(materialize) refresh="incremental" watermark="ts" storage="lake"
source: between is ev extend { where: ts >= now - 60 days and ts < now - 30 days }
`;

   it(
      "carries only a lower bound to the refresh's delete",
      async () => {
         const { c } = await compiled(INCREMENTAL);
         const declarations = collectIncrementalDeclarations(
            c.sources,
            c.materializePlans,
         );
         expect(
            declarations[planNamed(c, "older").sourceID]?.rollingWindow,
         ).toBeUndefined();
         expect(
            declarations[planNamed(c, "between").sourceID]?.rollingWindow,
         ).toEqual([
            {
               field: "ts",
               op: ">=",
               offset: { sign: -1, amount: 60, unit: "day" },
            },
         ]);
      },
      { timeout: 30000 },
   );

   for (const name of ["older", "between"]) {
      it(
         `appends above the watermark and reads equal to a full rebuild: ${name}`,
         async () => {
            const { duckdb, c } = await compiled(INCREMENTAL);
            const runner: SqlRunner = async (sql) => ({
               rows: (await duckdb.runSQL(sql)).rows as unknown[],
            });
            const plan = planNamed(c, name);
            const sql = buildSQL(c, name);
            // The seed, then a day of new rows the next refresh appends.
            await duckdb.runSQL(`CREATE OR REPLACE TABLE rwu_inc AS ${sql}`);
            const [seeded] = (
               await duckdb.runSQL("SELECT max(ts) AS m FROM rwu_inc")
            ).rows as { m: unknown }[];
            await duckdb.runSQL(`INSERT INTO rwu_ev
               SELECT ts, 1000000 + row_number() OVER (ORDER BY ts)
               FROM range(date_trunc('hour', LOCALTIMESTAMP) - INTERVAL 2 DAY,
                          date_trunc('hour', LOCALTIMESTAMP) - INTERVAL 1 DAY,
                          INTERVAL 1 HOUR) t(ts)`);
            const declaration = collectIncrementalDeclarations(
               c.sources,
               c.materializePlans,
            )[plan.sourceID];
            const lineage = incrementalLineage({
               declaration,
               // As stored from a Postgres origin: a DuckDB origin has no
               // incremental range to read; the SQL itself runs on DuckDB.
               dialect: "postgres",
               targetDialect: "duckdb",
               physicalTableName: "memory.rwu_inc",
               connectionName: "duckdb",
               sourceEntityId: "sid",
            });
            if (!lineage) throw new Error("no incremental lineage");
            const step = await planIncrementalStep({
               target: warehouseDeltaTarget({
                  dialect: "duckdb",
                  runner,
                  quotedTablePath: `"rwu_inc"`,
                  lineage,
                  sourceSQL: sql,
               }),
               lineage,
               ledgerEntry: {
                  sourceEntityId: "sid",
                  coveredThroughValue: new Date(String(seeded.m))
                     .toISOString()
                     .replace("Z", "")
                     .replace(/\.000$/, ""),
                  coveredThroughType: "timestamp",
                  watermarkDimension: "ts",
                  mergeKeyDimensions: [],
                  derivedStrategy: "range_replace",
                  physicalTableName: "memory.rwu_inc",
                  connectionName: "duckdb",
               },
               forceRefresh: false,
               now: new Date(),
               columns: plan.columns.map((col) => col.column),
            });
            expect(step.mode === "seed" ? step.reason : step.mode).toBe(
               "delta",
            );
            if (step.mode !== "delta") return;
            // An upper bound deletes nothing; a two-sided window deletes
            // below its lower bound only.
            // (The range replace's own DELETE bounds the range from below.)
            const windowDelete =
               /^DELETE FROM "rwu_inc" WHERE "ts" < TIMESTAMP '[^']*'$/;
            expect(step.statements.some((s) => windowDelete.test(s))).toBe(
               name === "between",
            );
            await applyDeltaScript(runner, "duckdb", step.statements);
            await duckdb.runSQL(`CREATE OR REPLACE TABLE rwu_full AS ${sql}`);
            if (name === "older") {
               // Every row there is, exactly as a full rebuild stores it.
               const diff = await count(
                  duckdb,
                  "(SELECT * FROM rwu_inc EXCEPT ALL SELECT * FROM rwu_full) UNION ALL (SELECT * FROM rwu_full EXCEPT ALL SELECT * FROM rwu_inc)",
               );
               expect(diff).toBe(0);
               expect(await count(duckdb, "SELECT * FROM rwu_inc")).toBe(
                  await count(duckdb, "SELECT * FROM rwu_ev"),
               );
            }
            // Read as the served copy reads: the window re-applied.
            const read = (table: string) =>
               `SELECT * FROM ${table} WHERE ts < LOCALTIMESTAMP - INTERVAL 30 DAY` +
               (name === "between"
                  ? " AND ts >= LOCALTIMESTAMP - INTERVAL 60 DAY"
                  : "");
            expect(await count(duckdb, read("rwu_inc"))).toBe(
               await count(duckdb, read("rwu_full")),
            );
            expect(await count(duckdb, read("rwu_inc"))).toBeGreaterThan(0);
         },
         { timeout: 30000 },
      );
   }
});
