// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * End to end, through the real package and environment: a database-tier
 * `#@ persist` and `#(materialize)` table storing `day`, `week`, `month`,
 * `quarter` and `year` truncations of a date (and of a timestamp) holds each
 * as the warehouse returned it, often a timestamp where Malloy declares a
 * `date`. That is the type its build wrote, so the bind's column read is no
 * drift: no `storage-column-type-mismatch` warning, no
 * `publisher_storage_column_type_mismatch_total` count, and every answer is
 * live's, in UTC and under a caller `timezone:`. A truncation column altered
 * after its build is still drift, and still warns.
 *
 * On DuckDB, on a DuckLake a DuckDB connection attached and USEd, and on
 * Postgres when `PUBLISHER_TEST_POSTGRES_URL` names one (whose build reads no
 * schema back from an unqualified table, so only native types are captured:
 * the shape that raised the false warning).
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { resetMaterializationTelemetryForTesting } from "../../src/materialization_metrics";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../src/test_helpers/metrics_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const MISMATCH = "storage-column-type-mismatch";
const METRIC = "publisher_storage_column_type_mismatch_total";

const OUTPUT = `{
  group_by:
    d_day is d.day, d_week is d.week, d_month is d.month,
    d_quarter is d.quarter, d_year is d.year,
    t_day is ts.day, t_week is ts.week, t_month is ts.month,
    t_quarter is ts.quarter, t_year is ts.year
  aggregate: total is n.sum()
}`;

const MODEL = `##! experimental { persistence }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    (CAST('2024-01-15' AS DATE), CAST('2024-01-15 23:30:00' AS TIMESTAMP), 1),
    (CAST('2024-02-29' AS DATE), CAST('2024-02-29 00:30:00' AS TIMESTAMP), 2),
    (CAST('2024-12-31' AS DATE), CAST('2024-12-31 23:59:00' AS TIMESTAMP), 3)
  ) AS t(d, ts, n)
""")

#(materialize) name="trunc_mat"
source: mat is raw -> ${OUTPUT}

#@ persist name="trunc_v1"
source: v1 is raw -> ${OUTPUT}

source: live is raw -> ${OUTPUT}
`;

const COLUMNS =
   "d_day, d_week, d_month, d_quarter, d_year, " +
   "t_day, t_week, t_month, t_quarter, t_year, total";

interface Fixture {
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   db: DuckDBConnection;
   service: MaterializationService;
}

async function fixture(root: string, warehouse: unknown): Promise<Fixture> {
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, PKG),
      { "m.malloy": MODEL },
      { manifest: { name: PKG, description: "fixture" } },
   );
   const db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV, envPath],
   );
   const repo = createDuckDBRepository(db);
   const env = await Environment.create(ENV, envPath, [warehouse as never], []);
   await env.addPackage(PKG);
   const pkg = await env.getPackage(parsePackageName(PKG), false);
   const service = serviceOverEnvironment(
      () => repo,
      () => env,
   );
   return { env, pkg, repo, db, service };
}

/** One auto-run, to its settled record. */
async function build(f: Fixture): Promise<Materialization> {
   const created = await f.service.createMaterialization(ENV, PKG, {});
   const running = (
      f.service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 120_000;
   for (;;) {
      const m = await f.repo.getMaterializationById(created.id);
      if (
         m &&
         ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
      ) {
         while (running.has(created.id)) {
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await f.repo.getMaterializationById(created.id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error("run never settled");
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

/** The answer to `source`'s columns, with what served it. */
async function ask(
   f: Fixture,
   source: string,
   timezone?: string,
): Promise<{ rows: unknown[]; kind?: string }> {
   const tz = timezone ? `timezone: '${timezone}'\n` : "";
   const result = await f.pkg.getModel("m.malloy")!.runQuery(
      unrecordedAudit(),
      {
         query: `run: ${source} -> { ${tz}select: ${COLUMNS}\norder_by: total }`,
      } as never,
      QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
   );
   return {
      rows: result.compactResult as unknown[],
      kind: result.path?.servedBy?.kind,
   };
}

function mismatches(f: Fixture): { subject?: string; message: string }[] {
   return (f.pkg.getPackageMetadata().warnings ?? []).filter(
      (w) => w.code === MISMATCH,
   ) as { subject?: string; message: string }[];
}

const tiers: {
   label: string;
   warehouse: (
      root: string,
   ) => Promise<{ config: unknown; drop?: () => Promise<void> }>;
   /** Changes `column`'s type (or removes it) after the build. */
   alter: (table: string, column: string) => string;
}[] = [
   {
      label: "DuckDB",
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         },
      }),
      alter: (table, column) =>
         `ALTER TABLE ${table} ALTER "${column}" TYPE VARCHAR`,
   },
   ...(process.platform === "win32"
      ? []
      : [
           {
              label: "DuckLake",
              warehouse: async (root: string) => {
                 const lake = path.join(root, "lake");
                 fs.mkdirSync(path.join(lake, "data"), { recursive: true });
                 const setupSQL = [
                    "INSTALL ducklake",
                    "LOAD ducklake",
                    `ATTACH IF NOT EXISTS 'ducklake:${path.join(lake, "origin.ducklake")}' AS origin_lake (DATA_PATH '${path.join(lake, "data")}/')`,
                    "USE origin_lake",
                 ].join(";\n");
                 return {
                    config: {
                       name: "warehouse",
                       type: "duckdb",
                       duckdbConnection: { setupSQL },
                    },
                 };
              },
              // DuckLake alters a type only to a wider one: drop it instead.
              alter: (table: string, column: string) =>
                 `ALTER TABLE ${table} DROP COLUMN "${column}"`,
           },
        ]),
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              warehouse: async () => {
                 const db = `trunc_${process.pid}_${Date.now()}`;
                 const admin = new PostgresConnection({
                    name: "admin",
                    connectionString: POSTGRES_URL,
                 });
                 await admin.runSQL(`CREATE DATABASE ${db}`);
                 const parsed = new URL(POSTGRES_URL);
                 return {
                    config: {
                       name: "warehouse",
                       type: "postgres",
                       postgresConnection: {
                          host: parsed.hostname,
                          port: Number(parsed.port || 5432),
                          userName: decodeURIComponent(parsed.username),
                          password: decodeURIComponent(parsed.password),
                          databaseName: db,
                       },
                    },
                    drop: async () => {
                       await admin
                          .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                          .catch(() => undefined);
                       await admin.close();
                    },
                 };
              },
              alter: (table: string, column: string) =>
                 `ALTER TABLE ${table} ALTER COLUMN "${column}" TYPE VARCHAR USING "${column}"::varchar`,
           },
        ]
      : []),
];

for (const tier of tiers) {
   describe(`a stored time truncation's own type is not drift (${tier.label})`, () => {
      let restore: () => void;
      let root: string;
      let f: Fixture;
      let run: Materialization;
      let drop: (() => Promise<void>) | undefined;
      let harness: MetricsHarness;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         harness = await startMetricsHarness();
         resetMaterializationTelemetryForTesting();
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "database-truncation-")),
         );
         const w = await tier.warehouse(root);
         drop = w.drop;
         f = await fixture(root, w.config);
         run = await build(f);
         expect(run.status).toBe("MANIFEST_FILE_READY");
      }, 240_000);
      afterAll(async () => {
         await f?.env.closeAllConnections().catch(() => undefined);
         await f?.db.close();
         await drop?.();
         fs.rmSync(root, { recursive: true, force: true });
         await harness?.shutdown();
         resetMaterializationTelemetryForTesting();
         restore();
         await freshLoadPool();
      });

      it("raises no type warning and counts no mismatch for the table its build wrote", async () => {
         expect(mismatches(f)).toEqual([]);
         expect(await harness.collectCounter(METRIC)).toBe(0);
      });

      it("serves each truncation from its table, as live answers it", async () => {
         const live = await ask(f, "live");
         for (const source of ["mat", "v1"]) {
            const served = await ask(f, source);
            expect(served.kind).toBe("materialized");
            expect(served.rows).toEqual(live.rows);
         }
      });

      it("answers as live under a caller time zone", async () => {
         const tz = "America/Los_Angeles";
         const live = await ask(f, "live", tz);
         for (const source of ["mat", "v1"]) {
            expect((await ask(f, source, tz)).rows).toEqual(live.rows);
         }
      });

      it("still warns, and counts, once a truncation column changes after its build", async () => {
         const warehouse = await f.env.getMalloyConnection("warehouse");
         const entries = Object.values(
            (
               run.manifest as {
                  entries: Record<string, { physicalTableName: string }>;
               }
            ).entries,
         );
         expect(entries).toHaveLength(2);
         for (const { physicalTableName } of entries) {
            await warehouse.runSQL(tier.alter(physicalTableName, "d_month"));
         }
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundDatabaseEntries() ?? {},
         );
         const warned = mismatches(f);
         expect(warned.map((w) => w.subject).sort()).toEqual(["mat", "v1"]);
         for (const w of warned) expect(w.message).toContain("`d_month`");
         expect(await harness.collectCounter(METRIC)).toBe(2);
         // And the changed column answers live's value, never the text.
         const live = await ask(f, "live");
         for (const source of ["mat", "v1"]) {
            expect((await ask(f, source)).rows).toEqual(live.rows);
         }
      });
   });
}
