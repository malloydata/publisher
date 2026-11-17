// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * End to end, through the real package and environment: a colocated table
 * built by a real run, then altered in its warehouse (`ALTER TABLE`), is read
 * with the type it holds once the package binds again
 * (`Package.reloadAllModels`, the path a run's commit, a manifest bind and a
 * restart take). A number altered to text answers live's `10`, never the
 * text maximum `"9.0"`. The production wiring is what is held: a bind that
 * did not read the warehouse (`observed` undefined) answers `"9.0"` and
 * fails here.
 *
 * Every mechanism: `#(materialize)`, `#@ persist` and a `#(preaggregate)`
 * rollup, on DuckDB, and on Postgres when `PUBLISHER_TEST_POSTGRES_URL`
 * names one.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
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
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

const MODEL = `##! experimental { persistence }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    ('a', CAST(9.0 AS DECIMAL(10,1))), ('a', CAST(10.0 AS DECIMAL(10,1))),
    ('b', CAST(2.5 AS DECIMAL(10,1)))
  ) AS t(cat, amt)
""")

#(materialize) name="cv5_mat"
source: mat is raw extend {
  measure: top is amt.max()
}

#@ persist name="cv5_v1"
source: v1 is raw -> { select: cat, amt } extend {
  measure: top is amt.max()
}

source: rolled is raw extend {
  #(preaggregate) grain=["cat"]
  measure: top is amt.max()
}
`;

interface Fixture {
   root: string;
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   db: DuckDBConnection;
   service: MaterializationService;
   cleanup: () => Promise<void>;
}

async function fixture(warehouse: unknown): Promise<Fixture> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "colocated-alter-")),
   );
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
   return {
      root,
      env,
      pkg,
      repo,
      db,
      service,
      cleanup: async () => {
         await env.closeAllConnections().catch(() => undefined);
         await db.close();
         fs.rmSync(root, { recursive: true, force: true });
      },
   };
}

/** One auto-run, to its settled record. */
async function build(f: Fixture): Promise<Materialization> {
   const created = await f.service.createMaterialization(ENV, PKG, {});
   const running = (
      f.service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 60_000;
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

/** The answer to `query` through the package, with what served it. */
async function ask(
   f: Fixture,
   query: string,
): Promise<{ rows: Record<string, unknown>[]; kind?: string }> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   return {
      rows: result.compactResult as Record<string, unknown>[],
      kind: result.path?.servedBy?.kind,
   };
}

const QUERIES = {
   materialize: "run: mat -> { aggregate: top }",
   persist: "run: v1 -> { aggregate: top }",
   rollup: "run: rolled -> { aggregate: top }",
} as const;

const tiers: {
   label: string;
   warehouse: () => Promise<{ config: unknown; drop?: () => Promise<void> }>;
   alter: (table: string, column: string) => string;
   /** Lists a table's column names, as the dialect's connection returns rows. */
   columnsSql: (table: string) => string;
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
      columnsSql: (table) =>
         `SELECT column_name FROM information_schema.columns WHERE table_name = '${table}'`,
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              warehouse: async () => {
                 const db = `cv5_alter_${process.pid}_${Date.now()}`;
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
              // Wrapped as Malloy's Postgres connection reads every result.
              columnsSql: (table: string) =>
                 `SELECT row_to_json(c) AS row FROM (SELECT column_name FROM information_schema.columns WHERE table_name = '${table}') c`,
           },
        ]
      : []),
];

for (const tier of tiers) {
   describe(`a colocated table altered after its build is read as it now is, through the real bind (${tier.label})`, () => {
      let restore: () => void;
      let f: Fixture;
      let drop: (() => Promise<void>) | undefined;
      const before: Record<string, { rows: unknown; kind?: string }> = {};
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         const w = await tier.warehouse();
         drop = w.drop;
         f = await fixture(w.config);
         const run = await build(f);
         expect(run.status).toBe("MANIFEST_FILE_READY");
         for (const [name, query] of Object.entries(QUERIES)) {
            before[name] = await ask(f, query);
         }
         // Out of band: the warehouse's own ALTER, after the build, of the
         // column each table stores `amt`'s values in.
         const warehouse = await f.env.getMalloyConnection("warehouse");
         const entries = Object.values(
            (
               run.manifest as {
                  entries: Record<string, { physicalTableName: string }>;
               }
            ).entries,
         );
         expect(entries).toHaveLength(3);
         for (const { physicalTableName } of entries) {
            const unqualified = physicalTableName.split(".").pop()!;
            const columns = (
               await warehouse.runSQL(tier.columnsSql(unqualified))
            ).rows as Record<string, unknown>[];
            const names = columns.map((c) => {
               const row = (c.row ?? c) as Record<string, unknown>;
               return String(row.column_name);
            });
            const column = names.find(
               (n) =>
                  n === "amt" || (n.includes("top") && n.includes("partial")),
            );
            expect(column).toBeDefined();
            await warehouse.runSQL(tier.alter(physicalTableName, column!));
         }
         // The bind every run, manifest and restart takes.
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundColocatedEntries() ?? {},
         );
      }, 180_000);
      afterAll(async () => {
         await f?.cleanup();
         await drop?.();
         restore();
         await freshLoadPool();
      });

      it("served each mechanism from its table before the alter", () => {
         expect(before.materialize.kind).toBe("materialized");
         expect(before.persist.kind).toBe("materialized");
         expect(before.rollup.kind).toBe("rollup");
         for (const b of Object.values(before)) {
            expect(Number((b.rows as { top: unknown }[])[0].top)).toBe(10);
         }
      });

      for (const [name, query] of Object.entries(QUERIES)) {
         it(`${name}: a number now stored as text answers live's 10, not "9.0"`, async () => {
            const after = await ask(f, query);
            expect(after.rows).toEqual([{ top: 10 }]);
            expect(after.kind).toBe("live");
         });
      }
   });
}
