// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An ungated `where:` over a database table compares its column as the table
 * now holds it: a column whose collation was changed after the build (here,
 * to a case-insensitive one) admits other rows than the source's comparison,
 * though its type did not move. Through the real package bind, such a scope
 * answers live, on an extension of a `#@ persist` source and through a
 * composite over it; the same scope over an unaltered table is still served.
 * On DuckDB, and on Postgres when `PUBLISHER_TEST_POSTGRES_URL` names one.
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

const MODEL = `##! experimental { persistence composite_sources }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    (1, 'electronics'), (2, 'books'), (3, 'books'), (4, 'Books'), (5, 'toys')
  ) AS t(id, category)
""")

#@ persist name="dbs_plain"
source: p_plain is raw -> {
  group_by: category, tenant is concat(category, '-x')
  aggregate: n is count()
}
source: w_ext is p_plain extend { where: tenant = 'books-x' }
source: comp is compose(p_plain, p_plain extend { except: n }) extend {
  where: tenant = 'books-x'
}

// The control: the same scope over a table nobody altered.
#@ persist name="dbs_ctrl"
source: p_ctrl is raw -> {
  group_by: category, tenant is concat(category, '-x')
  aggregate: n is count()
}
source: w_ctrl is p_ctrl extend { where: tenant = 'books-x' }
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
      fs.mkdtempSync(path.join(os.tmpdir(), "database-alter-")),
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
   extension: "run: w_ext -> { select: category, tenant; order_by: category }",
   composite: "run: comp -> { select: category, tenant; order_by: category }",
} as const;
const CONTROL =
   "run: w_ctrl -> { select: category, tenant; order_by: category }";
const LIVE = [{ category: "books", tenant: "books-x" }];

const tiers: {
   label: string;
   warehouse: () => Promise<{ config: unknown; drop?: () => Promise<void> }>;
   /** Make `column` of `table` compare case-insensitively. */
   alter: (table: string, column: string) => string[];
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
      alter: (table, column) => [
         `ALTER TABLE ${table} ALTER "${column}" SET DATA TYPE VARCHAR COLLATE NOCASE`,
      ],
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              warehouse: async () => {
                 const db = `dbs_coll_${process.pid}_${Date.now()}`;
                 const admin = new PostgresConnection({
                    name: "admin",
                    connectionString: POSTGRES_URL,
                 });
                 // A C default compares bytes, as the table did before its alter:
                 // under another default an ordering or a computed string
                 // compares under that default, which answers live already.
                 await admin.runSQL(
                    `CREATE DATABASE ${db} LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0`,
                 );
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
              alter: (table: string, column: string) => [
                 `DO $$ BEGIN CREATE COLLATION IF NOT EXISTS dbs_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); EXCEPTION WHEN others THEN NULL; END $$`,
                 `ALTER TABLE ${table} ALTER COLUMN "${column}" TYPE text COLLATE dbs_ci`,
              ],
           },
        ]
      : []),
];

for (const tier of tiers) {
   describe(`a scope over a database table whose column collation changed after its build answers live (${tier.label})`, () => {
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
         // Out of band, after the build: the warehouse's own ALTER of the
         // altered table's string columns, which leaves their type as it was.
         const warehouse = await f.env.getMalloyConnection("warehouse");
         const entries = Object.values(
            (
               run.manifest as {
                  entries: Record<
                     string,
                     { physicalTableName: string; sourceName?: string }
                  >;
               }
            ).entries,
         );
         const altered = entries.filter((e) =>
            e.physicalTableName.includes("dbs_plain"),
         );
         expect(altered).toHaveLength(1);
         for (const column of ["category", "tenant"]) {
            for (const sql of tier.alter(
               altered[0].physicalTableName,
               column,
            )) {
               await warehouse.runSQL(sql);
            }
         }
         // The bind every run, manifest and restart takes.
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundDatabaseEntries() ?? {},
         );
      }, 180_000);
      afterAll(async () => {
         await f?.cleanup();
         await drop?.();
         restore();
         await freshLoadPool();
      });

      it("served each scope from its table, as live answers, before the alter", () => {
         for (const b of Object.values(before)) {
            expect(b.kind).toBe("materialized");
            expect(b.rows).toEqual(LIVE);
         }
      });

      for (const [name, query] of Object.entries(QUERIES)) {
         it(`${name}: answers live's rows, from live`, async () => {
            const after = await ask(f, query);
            expect(after.rows).toEqual(LIVE);
            expect(after.kind).toBe("live");
         });
      }

      it("still serves the scope over the unaltered table", async () => {
         const got = await ask(f, CONTROL);
         expect(got.rows).toEqual(LIVE);
         expect(got.kind).toBe("materialized");
      });
   });
}
