// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P2-45 (#1438): a `rename:` in a persisted query's trailing `extend` broke
 * the `storage=` build ("Column kind in REPLACE list not found"): the build
 * cast the declared columns under their renamed names, while the SELECT it
 * stores outputs the names before the rename. Each column is now declared
 * under its output name, and the serve side renames it again.
 *
 * End to end on a real DuckLake, with the lifecycle spec's seams
 * (materialization_ducklake_lifecycle): a `#@ persist storage=` source, a
 * `#(materialize) storage=` source and a rollup over a renamed base each
 * build, serve from the table, and equal live, a renamed date (a cast
 * column) and a renamed 128-bit sum (stored as DECIMAL(38,0)) included.
 * Only the `#@ persist` case and the build's schema depend on the fix; the
 * `#(materialize)` and rollup cases pass without it too, and pin that those
 * keep building and serving over a renamed base.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import * as connection from "../../src/service/connection";
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
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { recordConnectionsAsForTests } from "../../src/service/connection_type";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const LAKE = "lake";

/** A BIGINT past 2^53; with 1 added, a sum no DOUBLE holds. */
const BIG = 4530000000000225186n;
const SUM = `${BIG + 1n}`;
const GROUPS = 600;

const MODEL = `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT 'k' || lpad((i % ${GROUPS})::VARCHAR, 5, '0') AS sub,
         (CASE WHEN i < ${GROUPS} THEN ${BIG} ELSE 1 END)::BIGINT AS v,
         TIMESTAMP '2026-01-01 00:00:00' AS ts
  FROM range(${2 * GROUPS}) AS t(i)
""")

// The P2-45 shape: a trailing extend renames a grouped column, a date the
// build casts (a day truncation the warehouse returns as a timestamp), and
// a 128-bit sum.
#@ persist storage="lake"
source: daily is raw -> {
  group_by: sub, d is ts::date + 1 month
  aggregate: n is count(), total is v.sum()
} extend {
  rename: kind is sub
  rename: on_day is d
  rename: amount is total
  dimension: kind2 is concat(kind, '!')
}

#(materialize) storage="lake"
source: m_daily is raw -> {
  group_by: sub
  aggregate: total is v.sum()
} extend {
  rename: kind is sub
  rename: amount is total
}

source: r_daily is raw -> {
  group_by: sub
  aggregate: total is v.sum()
} extend {
  rename: kind is sub
  #(preaggregate) grain=["kind"] storage="lake"
  measure: amount is total.sum()
}
`;

/** Each query, and the column every row of it holds as SUM. */
const CASES: { name: string; malloy: string; column: string }[] = [
   {
      name: "P2-45 #@ persist storage=: renamed columns, a renamed date and a renamed 128-bit sum",
      malloy:
         "run: daily -> { select: kind, kind2, on_day, n, amount; order_by: kind }",
      column: "amount",
   },
   {
      name: "P2-45 #(materialize) storage=: renamed columns",
      malloy: "run: m_daily -> { select: kind, amount; order_by: kind }",
      column: "amount",
   },
   {
      name: "P2-45 #(preaggregate) storage=: a rollup over a renamed base",
      malloy:
         "run: r_daily -> { group_by: kind; aggregate: amount; order_by: kind }",
      column: "amount",
   },
];

type Answer = {
   rows: Record<string, unknown>[];
   servedFrom: string | null;
   path: unknown;
   servedBy: { kind: string; tableId?: string; tier?: string; via?: string };
};

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by several in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "P2-45: a rename: in a trailing extend builds and serves from storage",
   () => {
      let root: string;
      let lakeDir: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let pkg: Package;
      let service: MaterializationService;
      let restore: () => void;
      let serve: MalloyDuckDBConnection | undefined;
      let opened = 0;
      const spies: { mockRestore(): void }[] = [];
      /** Every catalog attach the run layer asked for, in order. */
      const attaches: { fn: string; dbName: string }[] = [];

      const catalogFile = () => path.join(lakeDir, "catalog.ducklake");

      async function attachFileLake(
         session: MalloyDuckDBConnection,
         dbName: string,
         readOnly: boolean,
      ): Promise<void> {
         await session.runSQL("INSTALL ducklake");
         await session.runSQL("LOAD ducklake");
         await session.runSQL(
            `ATTACH 'ducklake:${catalogFile()}' AS ${dbName} ` +
               `(DATA_PATH '${path.join(lakeDir, "data")}/'` +
               `${readOnly ? ", READ_ONLY" : ""})`,
         );
      }

      /**
       * Close the serve connection and hand the package a new provider, which
       * drops every compiled serving leg; the next query opens a new instance
       * (lazily: before the first build there is no catalog to attach).
       */
      async function reopenServe(): Promise<void> {
         await serve?.close().catch(() => undefined);
         serve = undefined;
         const config = new MalloyConfig({ connections: {} });
         config.wrapConnections(() => ({
            lookupConnection: async (name?: string) => {
               if (name !== undefined && name !== LAKE) {
                  throw new Error(`no destination ${name}`);
               }
               if (!serve) {
                  const c = new MalloyDuckDBConnection(
                     LAKE,
                     path.join(root, `serve_${++opened}.duckdb`),
                  );
                  await attachFileLake(c, LAKE, true);
                  serve = c;
               }
               return serve;
            },
         }));
         pkg.materialization().setServeDestinationConfig(() => config);
      }

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               // The `warehouse` DuckDB connection needs a setupSQL or an
               // attached database to be valid; a no-op setupSQL is enough.
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "ducklake-bigint-")),
         );
         lakeDir = path.join(root, "lake");
         fs.mkdirSync(path.join(lakeDir, "data"), { recursive: true });
         const envPath = path.join(root, "env");
         fs.mkdirSync(envPath);
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": MODEL },
            { manifest: { name: PKG, description: "fixture" } },
         );
         db = new DuckDBConnection(":memory:");
         await db.initialize();
         await initializeSchema(db);
         await db.run(
            "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
            [ENV_ID, ENV, envPath],
         );
         repo = createDuckDBRepository(db);

         spies.push(
            spyOn(connection, "attachDuckLakeReadWrite").mockImplementation(
               async (session, dbName) => {
                  attaches.push({ fn: "readWrite", dbName });
                  await attachFileLake(session as never, dbName, false);
               },
            ),
            spyOn(connection, "attachDuckLakeReadOnly").mockImplementation(
               async (session, dbName) => {
                  attaches.push({ fn: "readOnly", dbName });
                  await attachFileLake(session as never, dbName, true);
               },
            ),
            spyOn(
               connection,
               "attachDuckLakeReadOnlyIfPresent",
            ).mockImplementation(async (session, dbName) => {
               attaches.push({ fn: "readOnlyIfPresent", dbName });
               if (!fs.existsSync(catalogFile())) return false;
               await attachFileLake(session as never, dbName, true);
               return true;
            }),
            spyOn(
               connection,
               "federateSourceForPassthrough",
            ).mockImplementation(async (session, sourceType) => {
               await session.runSQL(
                  "CREATE OR REPLACE TEMP MACRO postgres_query(alias, q) AS TABLE SELECT * FROM query(q)",
               );
               return { handle: "warehouse", sourceType };
            }),
         );

         env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "warehouse",
                  type: "duckdb",
                  duckdbConnection: { setupSQL: "SELECT 1" },
               } as never,
            ],
            [
               {
                  name: LAKE,
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: {
                           host: "h",
                           databaseName: "d",
                           userName: "u",
                        },
                     },
                     storage: { bucketUrl: lakeDir },
                  },
               } as never,
            ],
         );
         // The plan reads the warehouse's recorded type, so it is reported as
         // Postgres there too, or its `storage=` sources would be refused.
         spies.push({
            mockRestore: recordConnectionsAsForTests({ warehouse: "postgres" }),
         });
         const apiConnection = env.getApiConnection.bind(env);
         spies.push(
            spyOn(env, "getApiConnection").mockImplementation((name) =>
               name === "warehouse"
                  ? ({
                       name: "warehouse",
                       type: "postgres",
                       postgresConnection: {
                          host: "h",
                          databaseName: "d",
                          userName: "u",
                       },
                    } as never)
                  : apiConnection(name),
            ),
         );
         await env.addPackage(PKG);
         pkg = await env.getPackage(parsePackageName(PKG), false);
         await reopenServe();
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
      });

      afterAll(async () => {
         for (const spy of spies) spy.mockRestore();
         await serve?.close().catch(() => undefined);
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         fs.rmSync(root, { recursive: true, force: true });
         restore?.();
      });

      /** Start a run and wait until it is terminal and its worker is gone. */
      async function build(forceRefresh = false): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, {
            forceRefresh,
         });
         const deadline = Date.now() + 120_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               const running = (
                  service as unknown as {
                     runningAbortControllers: Map<string, unknown>;
                  }
               ).runningAbortControllers;
               while (running.has(created.id)) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
               }
               const settled = (await repo.getMaterializationById(created.id))!;
               await reopenServe();
               return settled;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 20));
         }
      }

      async function ask(malloy: string): Promise<Answer> {
         const model = pkg.getModel("m.malloy")!;
         const result = await model.runQuery(
            unrecordedAudit(),
            { query: malloy } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         return {
            // By key: a view's own order leaves ties in any order.
            rows: (result.compactResult as Record<string, unknown>[])
               .slice()
               .sort((x, y) => String(x.kind).localeCompare(String(y.kind))),
            servedFrom: result.servedFrom ?? null,
            path: result.path,
            servedBy: (result.path?.servedBy ?? { kind: "live" }) as never,
         };
      }

      const live: Record<string, Answer["rows"]> = {};

      it("answers every case live before anything is built", async () => {
         for (const c of CASES) {
            const answer = await ask(c.malloy);
            expect(answer.servedBy.kind).toBe("live");
            expect(answer.rows).toHaveLength(GROUPS);
            for (const row of answer.rows) {
               expect(String(row[c.column])).toBe(SUM);
            }
            live[c.name] = answer.rows;
         }
      });

      it("builds every table, the columns under the names the SELECT outputs", async () => {
         const run = await build();
         expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const entries = Object.values(
            (
               run.manifest as {
                  entries: Record<
                     string,
                     {
                        sourceName: string;
                        schema?: { name: string; type: string }[];
                     }
                  >;
               }
            ).entries,
         );
         const daily = entries.find((e) => e.sourceName === "daily")!;
         // Under the names the SELECT outputs; the date DuckDB computes as a
         // TIMESTAMP is cast to DATE, the 128-bit sum to DECIMAL(38,0).
         expect(
            [...(daily.schema ?? [])].sort((a, b) =>
               a.name.localeCompare(b.name),
            ),
         ).toEqual([
            { name: "d", type: "DATE" },
            { name: "n", type: "BIGINT" },
            { name: "sub", type: "VARCHAR" },
            { name: "total", type: "DECIMAL(38,0)" },
         ]);
      });

      for (const c of CASES) {
         it(c.name, async () => {
            const answer = await ask(c.malloy);
            expect(answer.rows).toEqual(live[c.name]);
            for (const row of answer.rows) {
               expect(String(row[c.column])).toBe(SUM);
            }
            expect(answer.servedFrom).toBe("storage");
            expect(answer.servedBy.kind).not.toBe("live");
         });
      }
   },
);
