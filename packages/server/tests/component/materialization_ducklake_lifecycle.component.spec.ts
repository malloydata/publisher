// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of the `storage=` lifecycle on a DuckLake destination, end
 * to end through a real Environment and MaterializationService: build, bind,
 * serve, refresh in place and reclaim. It pins what today's run layer
 * (MaterializationService, StorageBuildStrategy, the build session and the
 * Reclaimer) does, so the run layer's tier refactor can be shown not to change
 * it. Every fact asserted here is today's behaviour, whatever it is.
 *
 * Postgres-free. A configured DuckLake destination must name a Postgres
 * catalog, and a storage build reads its source through a native warehouse
 * passthrough (BigQuery, Snowflake or Postgres), so three seams are faked and
 * everything between them runs for real:
 *
 *  - the destination's catalog. It is configured with a shape-valid Postgres
 *    catalog nobody connects to, and the three exported attach functions the
 *    run layer calls (`attachDuckLakeReadWrite`, `attachDuckLakeReadOnly`,
 *    `attachDuckLakeReadOnlyIfPresent`) attach a DuckLake catalog kept in a
 *    local DuckDB file instead, in the mode each was asked for.
 *  - the source warehouse. The package's `warehouse` is a DuckDB connection,
 *    which answers the live queries. The build reads the warehouse's API
 *    config through `Environment.getApiConnection`, which here reports it as
 *    Postgres so the passthrough build is taken; `federateSourceForPassthrough`
 *    then defines `postgres_query(alias, sql)` on the build session as a macro
 *    that runs `sql` in place. The compiled SQL reads only inline VALUES, so
 *    running it on the build session is the same rows the warehouse returns.
 *  - the serve connection. The environment's own `lake` connection would
 *    attach the Postgres catalog, so the package's serve destination config is
 *    replaced (as the conformance harness does) by a DuckDB connection named
 *    `lake` with the file catalog attached read-only under the same name, the
 *    name every bound `storage=` path starts with.
 *
 * DuckDB file locking, found empirically: every build, list and drop opens
 * its own in-memory instance and attaches the catalog file, and the serve
 * connection is another instance on the same file. Within one process these
 * do not block each other, but an instance that attached the catalog earlier
 * keeps reading the catalog as it was when it attached, so the serve
 * connection is reopened after every run or drop that changes the catalog
 * (`reopenServe`), and an inspection always opens a fresh instance.
 */

import {
   readableTableName,
   sourceLabel,
} from "../../src/service/materialization/domain/table_name";
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
import * as dialects from "../../src/service/materialization/domain/dialect";
import { backupSuffix } from "../../src/service/materialization/run/materialization_run_support";
import { listStorageTables } from "../../src/service/materialization/run/materialization_build_session";
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

/**
 * One source per `storage=` kind: a v1 persist (`daily`), a v2 materialize
 * (`orders`) and a v2 rollup (`sales`, built as a table of its own).
 */
const MODEL = `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")

#@ persist storage="lake"
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }

#(materialize) storage="lake"
source: orders is raw extend {
  measure: order_total is amt.sum()
}

source: sales is raw extend {
  #(preaggregate) grain=["cat"] storage="lake"
  measure: amount is amt.sum()
}
`;

/** A query per stored table, and the leg that answers it once bound. */
const QUERIES = {
   daily: "run: daily -> { group_by: cat; aggregate: t is total.sum() }",
   orders: "run: orders -> { group_by: cat; aggregate: order_total }",
   sales: "run: sales -> { group_by: cat; aggregate: amount }",
} as const;
type QueryName = keyof typeof QUERIES;
const NAMES = Object.keys(QUERIES) as QueryName[];

/** The manifest's name for each query's table (a rollup gets its own). */
const ENTRY_NAME: Record<QueryName, RegExp> = {
   daily: /^daily$/,
   orders: /^orders$/,
   sales: /^sales__rollup__cat__[0-9a-f]+$/,
};

type Entry = {
   sourceName: string;
   sourceEntityId: string;
   physicalTableName: string;
   connectionName?: string;
   storageDestinationName?: string;
   schema?: { name: string; type: string }[];
   realization?: string;
   rowCount?: number;
   origin?: string;
};

type Answer = {
   rows: Record<string, unknown>[];
   servedFrom: string | null;
   servedBy: { kind: string; tableId?: string; tier?: string; via?: string };
};

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by several in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "the storage= lifecycle on a DuckLake destination with a file catalog",
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

      /** A fresh instance with the catalog attached read-only, for a look. */
      async function inspect<T>(
         fn: (c: MalloyDuckDBConnection) => Promise<T>,
      ): Promise<T> {
         const c = new MalloyDuckDBConnection(
            "inspect",
            path.join(root, `inspect_${++opened}.duckdb`),
         );
         try {
            await attachFileLake(c, LAKE, true);
            return await fn(c);
         } finally {
            await c.close();
         }
      }

      /**
       * The catalog's tables, as the run layer's own listing reports them.
       * The listing's own attach is not recorded: it is this spec looking.
       */
      async function catalogTables(): Promise<string[]> {
         const recorded = attaches.length;
         try {
            return await listStorageTables({
               destinationName: LAKE,
               destinationConnection: env.getStorageDestination(LAKE),
               schemaName: "main",
               environmentPath: env.getEnvironmentPath(),
            });
         } finally {
            attaches.length = recorded;
         }
      }

      /** Each catalog table's current Parquet files, by table name. */
      function dataFiles(): Promise<Record<string, string[]>> {
         return inspect(async (c) => {
            const tables = await c.runSQL(
               `SELECT table_name FROM duckdb_tables() WHERE database_name = '${LAKE}'`,
            );
            const files: Record<string, string[]> = {};
            for (const row of tables.rows) {
               const table = String(row.table_name);
               const listed = await c.runSQL(
                  `SELECT data_file FROM ducklake_list_files('${LAKE}', '${table}')`,
               );
               files[table] = listed.rows
                  .map((r) => String(r.data_file))
                  .sort();
            }
            return files;
         });
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
            fs.mkdtempSync(path.join(os.tmpdir(), "ducklake-lifecycle-")),
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

      async function ask(name: QueryName): Promise<Answer> {
         const model = pkg.getModel("m.malloy")!;
         const result = await model.runQuery(
            unrecordedAudit(),
            { query: QUERIES[name] } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         const rows = (result.compactResult as Record<string, unknown>[])
            .slice()
            .sort((x, y) => String(x.cat).localeCompare(String(y.cat)));
         return {
            rows,
            servedFrom: result.servedFrom ?? null,
            servedBy: (result.path?.servedBy ?? { kind: "live" }) as never,
         };
      }

      function entriesOf(m: Materialization): Record<QueryName, Entry> {
         const all = Object.values(
            (m.manifest as { entries: Record<string, Entry> }).entries,
         );
         expect(all).toHaveLength(NAMES.length);
         return Object.fromEntries(
            NAMES.map((name) => {
               const hits = all.filter((e) =>
                  ENTRY_NAME[name].test(e.sourceName),
               );
               expect(hits).toHaveLength(1);
               return [name, hits[0]];
            }),
         ) as Record<QueryName, Entry>;
      }

      const live: Partial<Record<QueryName, Answer["rows"]>> = {};
      let first: Materialization;
      let second: Materialization;
      let firstEntries: Record<QueryName, Entry>;

      it("serves live before anything is built, and no catalog exists", async () => {
         for (const name of NAMES) {
            const answer = await ask(name);
            expect(answer.servedFrom).toBeNull();
            expect(answer.servedBy.kind).toBe("live");
            live[name] = answer.rows;
         }
         expect(live.daily).toEqual([
            { cat: "a", t: 3 },
            { cat: "b", t: 4 },
         ]);
         expect(fs.existsSync(catalogFile())).toBe(false);
         // The listing looks without creating: no catalog is a 404, not empty.
         await expect(catalogTables()).rejects.toThrow(/has no catalog yet/);
         // Loading the package and answering live attach no catalog.
         expect(attaches).toEqual([]);
      });

      it("builds every storage= source into the catalog as Parquet, and records each in the manifest", async () => {
         attaches.length = 0;
         first = await build();
         expect([first.status, first.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(first.metadata).toMatchObject({
            mode: "auto",
            sourcesBuilt: 3,
            sourcesReused: 0,
         });
         firstEntries = entriesOf(first);
         const { daily, orders, sales } = firstEntries;
         // Self-assigned names: the readable `<pkg>__<file>__<source>__p<hash>`.
         expect(daily.physicalTableName).toMatch(
            /^pkg__m__daily__p[0-9a-f]{8}$/,
         );
         expect(orders.physicalTableName).toMatch(
            /^pkg__m__orders__p[0-9a-f]{8}$/,
         );
         expect(sales.physicalTableName).toMatch(
            /^pkg__m__sales_by_cat__p[0-9a-f]{8}$/,
         );
         for (const entry of [daily, orders, sales]) {
            expect(entry).toMatchObject({
               connectionName: "warehouse",
               storageDestinationName: LAKE,
               realization: "COPY",
            });
         }
         expect(daily.origin).toBeUndefined();
         expect(orders.origin).toBeUndefined();
         expect(sales.origin).toBe("preaggregate");
         expect(daily.schema).toEqual([
            { name: "cat", type: "VARCHAR" },
            { name: "total", type: "DECIMAL(38,1)" },
         ]);
         expect(orders.schema).toEqual([
            { name: "cat", type: "VARCHAR" },
            { name: "amt", type: "DECIMAL(2,1)" },
         ]);
         expect(sales.schema).toEqual([
            { name: "cat", type: "VARCHAR" },
            { name: "amount__partial", type: "DECIMAL(38,1)" },
         ]);
         expect([daily.rowCount, orders.rowCount, sales.rowCount]).toEqual([
            2, 3, 2,
         ]);

         // One read-write attach per table built, on the destination's name.
         expect(attaches).toEqual(
            NAMES.map(() => ({ fn: "readWrite", dbName: LAKE })),
         );

         const tables = [daily, orders, sales]
            .map((e) => e.physicalTableName)
            .sort();
         expect(await catalogTables()).toEqual(tables);
         // Stored as Parquet in the data path, never inlined in the catalog.
         await inspect(async (c) => {
            for (const table of tables) {
               const files = await c.runSQL(
                  `SELECT data_file FROM ducklake_list_files('${LAKE}', '${table}')`,
               );
               expect(files.rows.length).toBeGreaterThan(0);
               for (const row of files.rows) {
                  const file = String(row.data_file);
                  expect(file.endsWith(".parquet")).toBe(true);
                  expect(file.startsWith(path.join(lakeDir, "data"))).toBe(
                     true,
                  );
               }
            }
            const stored = await c.runSQL(
               `SELECT cat, total FROM ${LAKE}.main."${daily.physicalTableName}" ORDER BY cat`,
            );
            expect(stored.rows.map((r) => [r.cat, Number(r.total)])).toEqual([
               ["a", 3],
               ["b", 4],
            ]);
         });
      });

      it("binds the built tables: each query is served from storage, equal to live", async () => {
         for (const name of NAMES) {
            const answer = await ask(name);
            expect(answer.servedFrom).toBe("storage");
            expect(answer.rows).toEqual(live[name]!);
            expect(answer.servedBy.tableId).toBe(
               firstEntries[name].sourceEntityId,
            );
         }
         expect((await ask("daily")).servedBy).toMatchObject({
            kind: "materialized",
            via: "served_copy",
         });
         expect((await ask("orders")).servedBy).toMatchObject({
            kind: "materialized",
            via: "served_copy",
         });
         expect((await ask("sales")).servedBy.kind).toBe("rollup");
      });

      it("reuses every table on an unforced run, touching no catalog", async () => {
         const tablesBefore = await catalogTables();
         const filesBefore = await dataFiles();
         attaches.length = 0;
         const reused = await build();
         expect([reused.status, reused.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(reused.metadata).toMatchObject({
            forceRefresh: false,
            sourcesBuilt: 0,
            sourcesReused: 3,
         });
         // The entries are carried forward as the first run recorded them.
         expect(reused.manifest?.entries).toEqual(first.manifest!.entries);
         expect(attaches).toEqual([]);
         expect(await catalogTables()).toEqual(tablesBefore);
         expect(await dataFiles()).toEqual(filesBefore);

         // Deleting the record (record-only) touches no catalog and
         // leaves the first run serving.
         attaches.length = 0;
         await service.deleteMaterialization(ENV, PKG, reused.id);
         expect(attaches).toEqual([]);
         await reopenServe();
         for (const name of NAMES) {
            const answer = await ask(name);
            expect(answer.servedFrom).toBe("storage");
            expect(answer.servedBy.tableId).toBe(
               firstEntries[name].sourceEntityId,
            );
         }
      });

      it("refreshes in place on a forced run: the same physical tables, rewritten", async () => {
         const filesBefore = await dataFiles();
         attaches.length = 0;
         second = await build(true);
         expect([second.status, second.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(second.metadata).toMatchObject({
            forceRefresh: true,
            sourcesBuilt: 3,
            sourcesReused: 0,
         });
         const entries = entriesOf(second);
         for (const name of NAMES) {
            // Same source identity and the same physical name: a forced run
            // replaces the table it serves rather than building a new one.
            expect(entries[name]).toMatchObject({
               sourceEntityId: firstEntries[name].sourceEntityId,
               physicalTableName: firstEntries[name].physicalTableName,
               storageDestinationName: LAKE,
               schema: firstEntries[name].schema,
               rowCount: firstEntries[name].rowCount,
            });
         }
         // One read-write attach per table built.
         expect(attaches).toEqual(
            NAMES.map(() => ({ fn: "readWrite", dbName: LAKE })),
         );
         expect(await catalogTables()).toEqual(
            NAMES.map((n) => firstEntries[n].physicalTableName).sort(),
         );
         // Rewritten as new Parquet files; the replaced files stay on disk
         // (DuckLake retires them; only a file cleanup deletes them).
         const filesAfter = await dataFiles();
         for (const name of NAMES) {
            const table = firstEntries[name].physicalTableName;
            expect(filesAfter[table].length).toBeGreaterThan(0);
            for (const file of filesAfter[table]) {
               expect(filesBefore[table]).not.toContain(file);
            }
            for (const file of filesBefore[table]) {
               expect(fs.existsSync(file)).toBe(true);
            }
         }
         for (const name of NAMES) {
            const answer = await ask(name);
            expect(answer.servedFrom).toBe("storage");
            expect(answer.rows).toEqual(live[name]!);
            expect(answer.servedBy.tableId).toBe(entries[name].sourceEntityId);
         }
      });

      it("deletes are record-only: the tables stay in the catalog, and once no run is left serving goes live", async () => {
         const filesBefore = await dataFiles();
         const tables = NAMES.map(
            (n) => firstEntries[n].physicalTableName,
         ).sort();

         // Deleting the superseded run touches no catalog, and the forced
         // run still serves the tables both name.
         attaches.length = 0;
         await service.deleteMaterialization(ENV, PKG, first.id);
         expect(attaches).toEqual([]);
         expect(await catalogTables()).toEqual(tables);
         await reopenServe();
         for (const name of NAMES) {
            expect((await ask(name)).servedFrom).toBe("storage");
         }

         // Deleting the last run touches no catalog either: its tables stay
         // for the operator to drop through the connection.
         await service.deleteMaterialization(ENV, PKG, second.id);
         expect(attaches).toEqual([]);
         expect(await repo.getMaterializationById(second.id)).toBeFalsy();
         expect(await catalogTables()).toEqual(tables);
         expect(await dataFiles()).toEqual(filesBefore);

         // No run is left, so the package is rebound to nothing and serves live.
         await reopenServe();
         for (const name of NAMES) {
            const answer = await ask(name);
            expect(answer.servedFrom).toBeNull();
            expect(answer.servedBy.kind).toBe("live");
            expect(answer.rows).toEqual(live[name]!);
         }
      });
   },
);

/**
 * The same three fakes as the suite above (a file catalog behind the attach
 * functions, a DuckDB warehouse reported as Postgres, a file-catalog serve
 * connection), set up for a model of a suite's own, so the cases below can
 * each pin one path through the run layer without sharing the lifecycle's
 * catalog. `files` is called with the fixture's root, so a model can name a
 * file under it.
 *
 * Every build session the run layer opens is seen at federation: its
 * statements from then on are recorded in `statements`, and `onStatement`
 * (when a case sets it) is called before each one runs, so a case can fail a
 * build at a statement it chooses.
 */
class LakeFixture {
   root = "";
   lakeDir = "";
   db!: DuckDBConnection;
   repo!: ResourceRepository;
   env!: Environment;
   pkg!: Package;
   service!: MaterializationService;
   /** Every catalog attach the run layer asked for, in order. */
   readonly attaches: { fn: string; dbName: string }[] = [];
   /** Every statement a build session ran after its source was federated. */
   readonly statements: string[] = [];
   /** Called before each recorded statement runs; throwing fails it. */
   onStatement?: (sql: string) => void | Promise<void>;
   /** Called after each recorded statement succeeded. */
   afterStatement?: (sql: string) => void | Promise<void>;
   private serve: MalloyDuckDBConnection | undefined;
   private opened = 0;
   private restore?: () => void;
   private readonly spies: { mockRestore(): void }[] = [];

   constructor(
      private readonly label: string,
      private readonly files: (root: string) => Record<string, string>,
      /** More settings for the suite, as environment variables. */
      private readonly settings: Record<string, string> = {},
   ) {}

   catalogFile(): string {
      return path.join(this.lakeDir, "catalog.ducklake");
   }

   async attachFileLake(
      session: MalloyDuckDBConnection,
      dbName: string,
      readOnly: boolean,
   ): Promise<void> {
      await session.runSQL("INSTALL ducklake");
      await session.runSQL("LOAD ducklake");
      await session.runSQL(
         `ATTACH 'ducklake:${this.catalogFile()}' AS ${dbName} ` +
            `(DATA_PATH '${path.join(this.lakeDir, "data")}/'` +
            `${readOnly ? ", READ_ONLY" : ""})`,
      );
   }

   /**
    * A fresh instance with the catalog attached, for a look or (`readOnly`
    * false) a plant. The catalog is detached before the instance closes, so
    * a plant's writes are checkpointed into the catalog file and no instance
    * outlives its look holding the file (found empirically: without it, a
    * later attach intermittently fails to deserialize the catalog).
    */
   async inspect<T>(
      fn: (c: MalloyDuckDBConnection) => Promise<T>,
      readOnly = true,
   ): Promise<T> {
      const c = new MalloyDuckDBConnection(
         "inspect",
         path.join(this.root, `inspect_${++this.opened}.duckdb`),
      );
      try {
         await this.attachFileLake(c, LAKE, readOnly);
         try {
            return await fn(c);
         } finally {
            await c.runSQL(`DETACH ${LAKE}`);
         }
      } finally {
         await c.close();
      }
   }

   /** The catalog's tables, as the run layer's own listing reports them. */
   async catalogTables(): Promise<string[]> {
      const recorded = this.attaches.length;
      try {
         return await listStorageTables({
            destinationName: LAKE,
            destinationConnection: this.env.getStorageDestination(LAKE),
            schemaName: "main",
            environmentPath: this.env.getEnvironmentPath(),
         });
      } finally {
         this.attaches.length = recorded;
      }
   }

   /** Each catalog table's current Parquet files, by table name. */
   dataFiles(): Promise<Record<string, string[]>> {
      return this.inspect(async (c) => {
         const tables = await c.runSQL(
            `SELECT table_name FROM duckdb_tables() WHERE database_name = '${LAKE}'`,
         );
         const files: Record<string, string[]> = {};
         for (const row of tables.rows) {
            const table = String(row.table_name);
            const listed = await c.runSQL(
               `SELECT data_file FROM ducklake_list_files('${LAKE}', '${table}')`,
            );
            files[table] = listed.rows.map((r) => String(r.data_file)).sort();
         }
         return files;
      });
   }

   /**
    * A stored table's rows, sorted, with every value that reads as a number
    * (a DECIMAL arrives as text, a BIGINT as a bigint) read as one.
    */
   storedRows(table: string, columns: string): Promise<unknown[][]> {
      return this.inspect(async (c) => {
         const result = await c.runSQL(
            `SELECT ${columns} FROM ${LAKE}.main."${table}" ORDER BY ALL`,
         );
         return result.rows.map((r) => Object.values(r).map(asNumber));
      });
   }

   /** Each catalog table's comment, by table name. */
   tableComments(): Promise<Record<string, string | null>> {
      return this.inspect(async (c) => {
         const result = await c.runSQL(
            `SELECT table_name, comment FROM duckdb_tables() WHERE database_name = '${LAKE}' ORDER BY table_name`,
         );
         return Object.fromEntries(
            result.rows.map((r) => [
               String(r.table_name),
               (r.comment as string | null) ?? null,
            ]),
         );
      });
   }

   /** See the suite above: the serve connection is reopened after a change. */
   async reopenServe(): Promise<void> {
      await this.serve?.close().catch(() => undefined);
      this.serve = undefined;
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(() => ({
         lookupConnection: async (name?: string) => {
            if (name !== undefined && name !== LAKE) {
               throw new Error(`no destination ${name}`);
            }
            if (!this.serve) {
               const c = new MalloyDuckDBConnection(
                  LAKE,
                  path.join(this.root, `serve_${++this.opened}.duckdb`),
               );
               await this.attachFileLake(c, LAKE, true);
               this.serve = c;
            }
            return this.serve;
         },
      }));
      this.pkg.materialization().setServeDestinationConfig(() => config);
   }

   async setUp(): Promise<void> {
      this.restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            ...this.settings,
         },
      }).restore;
      this.root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), `ducklake-${this.label}-`)),
      );
      this.lakeDir = path.join(this.root, "lake");
      fs.mkdirSync(path.join(this.lakeDir, "data"), { recursive: true });
      const envPath = path.join(this.root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(path.join(envPath, PKG), this.files(this.root), {
         manifest: { name: PKG, description: "fixture" },
      });
      this.db = new DuckDBConnection(":memory:");
      await this.db.initialize();
      await initializeSchema(this.db);
      await this.db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      this.repo = createDuckDBRepository(this.db);

      this.spies.push(
         spyOn(connection, "attachDuckLakeReadWrite").mockImplementation(
            async (session, dbName) => {
               this.attaches.push({ fn: "readWrite", dbName });
               await this.attachFileLake(session as never, dbName, false);
            },
         ),
         spyOn(connection, "attachDuckLakeReadOnly").mockImplementation(
            async (session, dbName) => {
               this.attaches.push({ fn: "readOnly", dbName });
               await this.attachFileLake(session as never, dbName, true);
            },
         ),
         spyOn(
            connection,
            "attachDuckLakeReadOnlyIfPresent",
         ).mockImplementation(async (session, dbName) => {
            this.attaches.push({ fn: "readOnlyIfPresent", dbName });
            if (!fs.existsSync(this.catalogFile())) return false;
            await this.attachFileLake(session as never, dbName, true);
            return true;
         }),
         spyOn(connection, "federateSourceForPassthrough").mockImplementation(
            async (session, sourceType) => {
               await session.runSQL(
                  "CREATE OR REPLACE TEMP MACRO postgres_query(alias, q) AS TABLE SELECT * FROM query(q)",
               );
               // Recorded from here on: the build's own statements.
               const run = session.runSQL.bind(session);
               session.runSQL = (async (sql: string, ...rest: unknown[]) => {
                  // The session's lockdown (duckdb_lockdown.ts) runs once the
                  // source is federated; it is not the build's own SQL.
                  if (!LOCKDOWN_STATEMENT.test(sql)) this.statements.push(sql);
                  await this.onStatement?.(sql);
                  const result = await (run as (...a: unknown[]) => unknown)(
                     sql,
                     ...rest,
                  );
                  await this.afterStatement?.(sql);
                  return result;
               }) as typeof session.runSQL;
               return { handle: "warehouse", sourceType };
            },
         ),
      );

      this.env = await Environment.create(
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
                  storage: { bucketUrl: this.lakeDir },
               },
            } as never,
         ],
      );
      // The plan reads the warehouse's recorded type, so it is reported as
      // Postgres there too, or its `storage=` sources would be refused.
      this.spies.push({
         mockRestore: recordConnectionsAsForTests({ warehouse: "postgres" }),
      });
      const apiConnection = this.env.getApiConnection.bind(this.env);
      this.spies.push(
         spyOn(this.env, "getApiConnection").mockImplementation((name) =>
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
      await this.env.addPackage(PKG);
      this.pkg = await this.env.getPackage(parsePackageName(PKG), false);
      await this.reopenServe();
      this.service = serviceOverEnvironment(
         () => this.repo,
         () => this.env,
      );
   }

   async tearDown(): Promise<void> {
      for (const spy of this.spies) spy.mockRestore();
      await this.serve?.close().catch(() => undefined);
      await this.env?.closeAllConnections().catch(() => undefined);
      await this.db?.close();
      if (this.root) fs.rmSync(this.root, { recursive: true, force: true });
      this.restore?.();
   }

   private get running(): Map<string, AbortController> {
      return (
         this.service as unknown as {
            runningAbortControllers: Map<string, AbortController>;
         }
      ).runningAbortControllers;
   }

   /** Abort every active run, as a stop does. */
   abortRunning(): void {
      for (const controller of this.running.values()) controller.abort();
   }

   /** Start a run and wait until it is terminal and its worker is gone. */
   async run(
      options: Parameters<
         MaterializationService["createMaterialization"]
      >[2] = {},
   ): Promise<Materialization> {
      const created = await this.service.createMaterialization(
         ENV,
         PKG,
         options,
      );
      const deadline = Date.now() + 120_000;
      for (;;) {
         const m = await this.repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            while (this.running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            const settled = (await this.repo.getMaterializationById(
               created.id,
            ))!;
            await this.reopenServe();
            return settled;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   /** Run `query` against `m.malloy`; rows sorted, DECIMALs as numbers. */
   async ask(query: string): Promise<Answer> {
      const model = this.pkg.getModel("m.malloy")!;
      const result = await model.runQuery(
         unrecordedAudit(),
         { query } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
      const rows = (result.compactResult as Record<string, unknown>[])
         .map((r) =>
            Object.fromEntries(
               Object.entries(r).map(([k, v]) => [k, asNumber(v)]),
            ),
         )
         .sort((x, y) => JSON.stringify(x).localeCompare(JSON.stringify(y)));
      return {
         rows,
         servedFrom: result.servedFrom ?? null,
         servedBy: (result.path?.servedBy ?? { kind: "live" }) as never,
      };
   }

   /** A run's manifest entries, by source name. */
   entriesByName(m: Materialization): Record<string, Entry> {
      const all = Object.values(
         (m.manifest as { entries: Record<string, Entry> } | null)?.entries ??
            {},
      );
      return Object.fromEntries(all.map((e) => [e.sourceName, e]));
   }
}

/** `v` as a number when it reads as one, else as it is. */
function asNumber(v: unknown): unknown {
   if (typeof v === "bigint") return Number(v);
   if (typeof v === "string" && v.trim() !== "" && !isNaN(Number(v))) {
      return Number(v);
   }
   return v;
}

/**
 * The readable name auto-run gives the table of `name` declared in `m.malloy`
 * (domain/table_name.ts), as it names a table in a destination.
 */
function selfName(name: string): string {
   return readableTableName({
      identity: {
         environmentName: ENV,
         packageName: PKG,
         modelPath: "m.malloy",
         sourceName: name,
      },
      label: sourceLabel(name),
      dialect: "duckdb",
      inDestination: true,
   });
}

/** Two plain `storage=` sources over inline rows: a v1 persist and a v2 one. */
const TWO_SOURCES = `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")

#@ persist storage="lake"
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }

#(materialize) storage="lake"
source: orders is raw extend {
  measure: order_total is amt.sum()
}
`;
const TWO_QUERIES = {
   daily: "run: daily -> { group_by: cat; aggregate: t is total.sum() }",
   orders: "run: orders -> { group_by: cat; aggregate: order_total }",
} as const;

/**
 * A build statement as a shape: the passthrough describe without its SQL, and
 * every other statement up to its SELECT, so a sequence reads as what was
 * done to which table.
 */
function statementShape(sql: string): string {
   if (sql.startsWith("DESCRIBE (")) return "DESCRIBE (passthrough)";
   return sql.split(" AS (")[0].split("\n")[0];
}

describe.skipIf(process.platform === "win32")(
   "an instructed (host) run into a DuckLake storage= destination",
   () => {
      const lake = new LakeFixture("instructed", () => ({
         "m.malloy": TWO_SOURCES,
      }));
      beforeAll(() => lake.setUp());
      afterAll(() => lake.tearDown());

      /** One instruction per planned source, at `<name>_<generation>`. */
      function instructionsFor(generation: string) {
         return Object.values(lake.pkg.getBuildPlan()!.sources)
            .map((s) => ({
               sourceEntityId: s.sourceEntityId,
               sourceID: s.sourceID,
               materializedTableId: `mt-${s.name}-${generation}`,
               physicalTableName: `${s.name}_${generation}`,
               realization: "COPY" as const,
               destination: LAKE,
            }))
            .sort((x, y) =>
               x.physicalTableName.localeCompare(y.physicalTableName),
            );
      }

      const live: Record<string, Answer["rows"]> = {};
      let g1Files: Record<string, string[]>;

      it("builds each instructed source at the host's name, keyed by its address, and leaves serving to the host", async () => {
         for (const [name, query] of Object.entries(TWO_QUERIES)) {
            live[name] = (await lake.ask(query)).rows;
         }
         const instructions = instructionsFor("g1");
         lake.attaches.length = 0;
         lake.statements.length = 0;
         const m = await lake.run({ buildInstructions: instructions });
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(m.metadata).toMatchObject({
            mode: "orchestrated",
            sourcesBuilt: 2,
            sourcesReused: 0,
         });
         // Keyed by the instruction's address, carrying the host's table name
         // and surrogate id verbatim.
         const entries = (m.manifest as { entries: Record<string, Entry> })
            .entries;
         expect(Object.keys(entries).sort()).toEqual(
            instructions.map((i) => i.sourceEntityId).sort(),
         );
         for (const instruction of instructions) {
            expect(entries[instruction.sourceEntityId]).toMatchObject({
               sourceEntityId: instruction.sourceEntityId,
               materializedTableId: instruction.materializedTableId,
               physicalTableName: instruction.physicalTableName,
               connectionName: "warehouse",
               storageDestinationName: LAKE,
               realization: "COPY",
            });
         }
         const byName = lake.entriesByName(m);
         expect(byName.daily.schema).toEqual([
            { name: "cat", type: "VARCHAR" },
            { name: "total", type: "DECIMAL(38,1)" },
         ]);
         expect(byName.orders.schema).toEqual([
            { name: "cat", type: "VARCHAR" },
            { name: "amt", type: "DECIMAL(2,1)" },
         ]);
         expect([byName.daily.rowCount, byName.orders.rowCount]).toEqual([
            2, 3,
         ]);

         // One read-write session per source, each a describe of the
         // passthrough, then the CTAS and its read-back in one transaction,
         // then the count. `orders` builds first (the plan's build order).
         expect(lake.attaches).toEqual([
            { fn: "readWrite", dbName: LAKE },
            { fn: "readWrite", dbName: LAKE },
         ]);
         expect(lake.statements.map(statementShape)).toEqual([
            "DESCRIBE (passthrough)",
            "BEGIN TRANSACTION",
            'CREATE OR REPLACE TABLE "lake"."orders_g1"',
            'DESCRIBE "lake"."orders_g1"',
            "COMMIT",
            'SELECT count(*) AS n FROM "lake"."orders_g1"',
            "DESCRIBE (passthrough)",
            "BEGIN TRANSACTION",
            'CREATE OR REPLACE TABLE "lake"."daily_g1"',
            'DESCRIBE "lake"."daily_g1"',
            "COMMIT",
            'SELECT count(*) AS n FROM "lake"."daily_g1"',
         ]);

         expect(await lake.catalogTables()).toEqual(["daily_g1", "orders_g1"]);
         expect(await lake.storedRows("daily_g1", "cat, total")).toEqual(
            live.daily.map((r) => [r.cat, r.t]),
         );
         expect(await lake.storedRows("orders_g1", "cat, amt")).toEqual([
            ["a", 1],
            ["a", 2],
            ["b", 4],
         ]);
         g1Files = await lake.dataFiles();

         // An instructed run loads nothing: the host distributes the
         // manifest, so the package still answers live.
         for (const [name, query] of Object.entries(TWO_QUERIES)) {
            const answer = await lake.ask(query);
            expect(answer.servedFrom).toBeNull();
            expect(answer.servedBy.kind).toBe("live");
            expect(answer.rows).toEqual(live[name]);
         }
      });

      it("builds a second generation beside the first, which it neither reuses nor reclaims", async () => {
         lake.attaches.length = 0;
         const m = await lake.run({ buildInstructions: instructionsFor("g2") });
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(m.metadata).toMatchObject({
            mode: "orchestrated",
            sourcesBuilt: 2,
            sourcesReused: 0,
         });
         expect(
            Object.values(lake.entriesByName(m)).map(
               (e) => e.physicalTableName,
            ),
         ).toEqual(expect.arrayContaining(["daily_g2", "orders_g2"]));
         expect(lake.attaches).toHaveLength(2);
         expect(await lake.catalogTables()).toEqual([
            "daily_g1",
            "daily_g2",
            "orders_g1",
            "orders_g2",
         ]);
         // The first generation is untouched.
         const files = await lake.dataFiles();
         expect(files.daily_g1).toEqual(g1Files.daily_g1);
         expect(files.orders_g1).toEqual(g1Files.orders_g1);
         expect(await lake.storedRows("daily_g2", "cat, total")).toEqual(
            live.daily.map((r) => [r.cat, r.t]),
         );
      });

      it("a stopped instructed run reclaims the table it built, and records no manifest", async () => {
         // Stop the run once the first source's CTAS has committed: the loop
         // sees the stop before the next source and fails the run.
         lake.afterStatement = (sql) => {
            if (sql === "COMMIT") lake.abortRunning();
         };
         lake.attaches.length = 0;
         lake.statements.length = 0;
         let m: Materialization;
         try {
            m = await lake.run({ buildInstructions: instructionsFor("g3") });
         } finally {
            lake.afterStatement = undefined;
         }
         expect(m.status).toBe("CANCELLED");
         expect(m.manifest ?? null).toBeNull();
         // `orders_g3` was built, then dropped by the failed-run reclaim on
         // a read-write session of its own; `daily_g3` was never started.
         expect(lake.statements.filter((s) => s.startsWith("CREATE"))).toEqual([
            expect.stringContaining('"lake"."orders_g3"'),
         ]);
         expect(lake.attaches).toEqual([
            { fn: "readWrite", dbName: LAKE },
            { fn: "readWrite", dbName: LAKE },
         ]);
         expect(await lake.catalogTables()).toEqual([
            "daily_g1",
            "daily_g2",
            "orders_g1",
            "orders_g2",
         ]);
      });
   },
);

describe.skipIf(process.platform === "win32")(
   "an incremental storage= source refreshed by a delta inside the DuckLake session",
   () => {
      // The warehouse rows live in a CSV, which the warehouse and every build
      // session read (the fake passthrough runs locally), so a case can add
      // rows between runs. It sits where both are confined to
      // (duckdb_lockdown.ts): the environment directory, for the warehouse,
      // and its storage-destinations root, for a build session.
      const csv = (root: string) =>
         path.join(root, "env", ".storage-destinations", "events.csv");
      const lake = new LakeFixture("incremental", (root) => {
         fs.mkdirSync(path.dirname(csv(root)), { recursive: true });
         fs.writeFileSync(
            csv(root),
            "id,cat,amt,wm\n1,a,1.0,10\n2,a,2.0,20\n3,b,4.0,30\n",
         );
         return {
            "m.malloy": `##! experimental.persistence
#(materialize) storage="lake" refresh="incremental" watermark="wm"
source: events is warehouse.sql("""
  SELECT id, cat, amt, wm FROM read_csv('${csv(root)}',
    columns = {'id': 'INTEGER', 'cat': 'VARCHAR', 'amt': 'DECIMAL(4,1)', 'wm': 'INTEGER'})
""") extend {
  measure: total is amt.sum()
}
`,
         };
      });
      // The warehouse fake is DuckDB, a dialect incremental refresh does not
      // take as a source; the build sees it as Postgres (see the header), and
      // DuckDB reads as having an incremental source range for this suite so
      // the package loads and the run plans a delta. Every other check runs.
      let widened: ReturnType<typeof spyOn> | undefined;
      beforeAll(async () => {
         const original = dialects.capabilitiesOf;
         widened = spyOn(dialects, "capabilitiesOf").mockImplementation(
            (dialect) =>
               dialect === "duckdb"
                  ? { ...original("duckdb"), incrementalSourceRange: true }
                  : original(dialect),
         );
         await lake.setUp();
         // The load plans on its worker thread, which the widened capability
         // does not reach, so it refuses the source alone (incremental rule
         // 6). Plan the package again on this thread, as a load the worker
         // left no plan for does.
         await (
            lake.pkg.materialization() as unknown as {
               applyLoadBuildPlan(plan: undefined): Promise<unknown>;
            }
         ).applyLoadBuildPlan(undefined);
      });
      afterAll(async () => {
         await lake.tearDown();
         widened?.mockRestore();
      });

      const TABLE = selfName("events");
      const BY_CAT = "run: events -> { group_by: cat; aggregate: total }";
      const ROWS = "run: events -> { select: id, cat, amt, wm }";
      /** The warehouse's own rows below `wm`, read from the CSV directly. */
      const warehouseRowsBelow = (wm: number) =>
         lake.inspect(async (c) => {
            const result = await c.runSQL(
               `SELECT id, cat, amt::DOUBLE AS amt, wm FROM read_csv('${csv(lake.root)}') WHERE wm < ${wm} ORDER BY ALL`,
            );
            return result.rows.map((r) => Object.values(r).map(asNumber));
         });
      const asRows = (rows: Answer["rows"]) =>
         rows.map((r) => [r.id, r.cat, r.amt, r.wm]);
      const ledgerRow = () =>
         lake.repo.getIncrementalLedgerEntry(ENV_ID, {
            connectionName: "warehouse",
            storageDestinationName: LAKE,
            physicalTableName: TABLE,
         });
      const ledgerOf = (coveredThrough: string, sourceEntityId: string) => ({
         connectionName: "warehouse",
         physicalTableName: TABLE,
         storageDestinationName: LAKE,
         coveredThrough,
         coveredThroughType: "number",
         watermark: "wm",
         strategy: "range_replace",
         sourceEntityId,
      });

      let seed: Materialization;
      let address: string;

      it("seeds with a full build and records the boundary its table reaches", async () => {
         const live = await lake.ask(BY_CAT);
         expect(live.rows).toEqual([
            { cat: "a", total: 3 },
            { cat: "b", total: 4 },
         ]);
         lake.statements.length = 0;
         seed = await lake.run();
         expect([seed.status, seed.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(seed.metadata).toMatchObject({
            mode: "auto",
            sourcesBuilt: 1,
            sourcesReused: 0,
         });
         const entry = lake.entriesByName(seed).events as Entry & {
            refresh?: string;
            ledger?: unknown;
         };
         address = entry.sourceEntityId;
         expect(entry).toMatchObject({
            physicalTableName: TABLE,
            storageDestinationName: LAKE,
            rowCount: 3,
            refresh: "full",
         });
         // The seed's boundary is its table's greatest watermark.
         expect(entry.ledger).toEqual(ledgerOf("30", address));
         expect(await ledgerRow()).toMatchObject({
            coveredThroughValue: "30",
            coveredThroughType: "number",
            derivedStrategy: "range_replace",
            advancedByMaterializationId: seed.id,
         });
         // No boundary yet, so no probe: the seed is the ordinary CTAS.
         const shapes = lake.statements.map(statementShape);
         expect(shapes).toContain(`CREATE OR REPLACE TABLE "lake"."${TABLE}"`);
         expect(shapes.filter((s) => s.startsWith("BEGIN;"))).toEqual([]);
         expect(await lake.catalogTables()).toEqual([TABLE]);
         expect(await lake.storedRows(TABLE, "id, cat, amt, wm")).toEqual([
            [1, "a", 1, 10],
            [2, "a", 2, 20],
            [3, "b", 4, 30],
         ]);
         const served = await lake.ask(BY_CAT);
         expect(served.servedFrom).toBe("storage");
         expect(served.servedBy).toMatchObject({
            kind: "materialized",
            tableId: address,
         });
         expect(served.rows).toEqual(live.rows);
      });

      it("applies a delta in place on the next run: probes, then one DELETE and INSERT script, the range read inside the passthrough", async () => {
         fs.appendFileSync(csv(lake.root), "4,b,8.0,40\n5,c,16.0,50\n");
         const filesBefore = await lake.dataFiles();
         lake.attaches.length = 0;
         lake.statements.length = 0;
         const m = await lake.run();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         // Not carried forward: an incremental source is refreshed every run.
         expect(m.metadata).toMatchObject({
            sourcesBuilt: 1,
            sourcesReused: 0,
         });
         const entry = lake.entriesByName(m).events as Entry & {
            refresh?: string;
            ledger?: unknown;
         };
         expect(entry).toMatchObject({
            sourceEntityId: address,
            physicalTableName: TABLE,
            storageDestinationName: LAKE,
            refresh: "delta",
            rowCount: 4,
         });
         // A numeric watermark's range ends at the source's frontier,
         // exclusive: the boundary advances to it.
         expect(entry.ledger).toEqual(ledgerOf("50", address));
         expect(await ledgerRow()).toMatchObject({
            coveredThroughValue: "50",
            advancedByMaterializationId: m.id,
         });
         expect(lake.attaches).toEqual([{ fn: "readWrite", dbName: LAKE }]);
         // No CTAS: the recast probe, the stored types (a DOUBLE column would
         // be probed in Postgres for a numeric to re-seed; there is none), the
         // shape and emptiness probes on the lake table, the frontier through
         // the passthrough, the delta's column types (a describe, which runs
         // nothing), then the delta as one script.
         expect(lake.statements.map(statementShape)).toEqual([
            `DESCRIBE "lake"."${TABLE}"`,
            `DESCRIBE "lake"."${TABLE}"`,
            'SELECT column_name AS "column_name"',
            `SELECT 1 AS present FROM "lake"."${TABLE}" LIMIT 1`,
            'SELECT * FROM postgres_query(\'warehouse\', \'SELECT MAX("wm") AS "watermark_max" FROM (SELECT ',
            "DESCRIBE (passthrough)",
            "BEGIN;",
            `DESCRIBE "lake"."${TABLE}"`,
            `SELECT count(*) AS n FROM "lake"."${TABLE}"`,
         ]);
         const script = lake.statements.find((s) => s.startsWith("BEGIN;"))!;
         expect(script.split("\n")[1]).toBe(
            `DELETE FROM "lake"."${TABLE}" WHERE "wm" >= 30 AND "wm" < 50;`,
         );
         expect(script).toContain(
            `INSERT INTO "lake"."${TABLE}" ("id", "cat", "amt", "wm") SELECT "id", "cat", "amt", "wm" FROM (SELECT * FROM postgres_query('warehouse', `,
         );
         // The range is applied inside the passthrough, not after it.
         expect(script).toContain(`) AS __d WHERE "wm" >= 30 AND "wm" < 50)`);
         expect(script.endsWith("COMMIT;")).toBe(true);

         // In place: the same table, its earlier files kept.
         expect(await lake.catalogTables()).toEqual([TABLE]);
         const filesAfter = await lake.dataFiles();
         for (const file of filesBefore[TABLE]) {
            expect(filesAfter[TABLE]).toContain(file);
         }
         // The row at the frontier (wm = 50) is outside the exclusive range,
         // so the table holds live's rows below it, and is served as such.
         expect(await lake.storedRows(TABLE, "id, cat, amt, wm")).toEqual([
            [1, "a", 1, 10],
            [2, "a", 2, 20],
            [3, "b", 4, 30],
            [4, "b", 8, 40],
         ]);
         const served = await lake.ask(ROWS);
         expect(served.servedFrom).toBe("storage");
         expect(served.servedBy.tableId).toBe(address);
         expect(asRows(served.rows)).toEqual(await warehouseRowsBelow(50));
         expect((await lake.ask(BY_CAT)).rows).toEqual([
            { cat: "a", total: 3 },
            { cat: "b", total: 12 },
         ]);
      });

      it("skips a run whose frontier has not advanced past the boundary, then takes the held-back row on the next advance", async () => {
         lake.statements.length = 0;
         const skipped = await lake.run();
         expect([skipped.status, skipped.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const skippedEntry = lake.entriesByName(skipped).events as Entry & {
            refresh?: string;
            ledger?: unknown;
         };
         expect(skippedEntry).toMatchObject({ refresh: "none", rowCount: 4 });
         expect(skippedEntry.ledger).toEqual(ledgerOf("50", address));
         expect(
            lake.statements.filter(
               (s) => s.startsWith("BEGIN") || s.startsWith("CREATE"),
            ),
         ).toEqual([]);

         fs.appendFileSync(csv(lake.root), "6,c,32.0,60\n");
         lake.statements.length = 0;
         const advanced = await lake.run();
         const entry = lake.entriesByName(advanced).events as Entry & {
            refresh?: string;
            ledger?: unknown;
         };
         expect(entry).toMatchObject({ refresh: "delta", rowCount: 5 });
         expect(entry.ledger).toEqual(ledgerOf("60", address));
         const script = lake.statements.find((s) => s.startsWith("BEGIN;"))!;
         expect(script.split("\n")[1]).toBe(
            `DELETE FROM "lake"."${TABLE}" WHERE "wm" >= 50 AND "wm" < 60;`,
         );
         expect(await lake.storedRows(TABLE, "id, cat, amt, wm")).toEqual([
            [1, "a", 1, 10],
            [2, "a", 2, 20],
            [3, "b", 4, 30],
            [4, "b", 8, 40],
            [5, "c", 16, 50],
         ]);
         const served = await lake.ask(BY_CAT);
         expect(served.servedFrom).toBe("storage");
         expect(served.rows).toEqual([
            { cat: "a", total: 3 },
            { cat: "b", total: 12 },
            { cat: "c", total: 16 },
         ]);
         expect(asRows((await lake.ask(ROWS)).rows)).toEqual(
            await warehouseRowsBelow(60),
         );
      });
   },
);

/** The ownership mark another package's build leaves on its table. */
const OTHER_MARK = "malloy-publisher:table owner=other%2Fpkg2";
const OWN_MARK = `malloy-publisher:table owner=${ENV}%2F${PKG}`;

describe.skipIf(process.platform === "win32")(
   "table ownership on a DuckLake destination: a name another package marked",
   () => {
      const lake = new LakeFixture("owned", () => ({
         "m.malloy": `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")

#(materialize) storage="lake" name="shared_t"
source: owned is raw extend {
  dimension: doubled is amt * 2
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: plain is raw extend { measure: total is amt.sum() }
`,
      }));
      beforeAll(() => lake.setUp());
      afterAll(() => lake.tearDown());

      const OWNED = "run: owned -> { group_by: cat; aggregate: total }";
      const PLAIN = selfName("plain");
      let refusedRun: Materialization;
      let builtRun: Materialization;

      it("refuses to build at an author's name= another package's table holds, and leaves that table as it was", async () => {
         const live = (await lake.ask(OWNED)).rows;
         await lake.inspect(async (c) => {
            await c.runSQL(
               `CREATE TABLE ${LAKE}.main.shared_t AS SELECT 'x' AS cat, 9.0 AS amt`,
            );
            await c.runSQL(
               `COMMENT ON TABLE ${LAKE}.main.shared_t IS '${OTHER_MARK}'`,
            );
         }, false);
         const filesBefore = await lake.dataFiles();
         lake.attaches.length = 0;
         lake.statements.length = 0;
         refusedRun = await lake.run();
         // A partial run: the refused source fails, its sibling builds.
         expect([refusedRun.status, refusedRun.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(refusedRun.metadata).toMatchObject({
            sourcesBuilt: 1,
            sourcesReused: 0,
         });
         const failures = Object.values(
            (
               refusedRun.manifest as {
                  failures: Record<string, Record<string, unknown>>;
               }
            ).failures,
         );
         expect(failures).toEqual([
            {
               sourceEntityId: expect.any(String),
               sourceName: "owned",
               materializedTableId: expect.stringMatching(/^local-/),
               physicalTableName: "shared_t",
               reason:
                  "'owned' is not built (table-owned-elsewhere): its name= 'shared_t' in storage destination 'lake' is held by a table package 'other/pkg2' built, and two packages never write one table. It is answered live. Give the source another name=, or drop that table if nothing needs it.",
               retryable: false,
               connectionName: "warehouse",
               storageDestinationName: LAKE,
            },
         ]);
         expect(lake.entriesByName(refusedRun).plain).toMatchObject({
            physicalTableName: PLAIN,
            storageDestinationName: LAKE,
         });
         // Two read-write sessions: the name's probe, which refuses before
         // anything is written, and `plain`'s build (the only one federated).
         expect(lake.attaches).toEqual([
            { fn: "readWrite", dbName: LAKE },
            { fn: "readWrite", dbName: LAKE },
         ]);
         expect(
            lake.statements
               .filter((s) => s.startsWith("CREATE"))
               .map(statementShape),
         ).toEqual([`CREATE OR REPLACE TABLE "lake"."${PLAIN}"`]);
         // The other package's table: its rows, files and mark untouched.
         expect(await lake.catalogTables()).toEqual([PLAIN, "shared_t"]);
         expect(await lake.storedRows("shared_t", "cat, amt")).toEqual([
            ["x", 9],
         ]);
         expect((await lake.dataFiles()).shared_t).toEqual(
            filesBefore.shared_t,
         );
         expect(await lake.tableComments()).toEqual({
            [PLAIN]: null,
            shared_t: OTHER_MARK,
         });
         // The refused source is answered live.
         const answer = await lake.ask(OWNED);
         expect(answer.servedFrom).toBeNull();
         expect(answer.servedBy.kind).toBe("live");
         expect(answer.rows).toEqual(live);
      });

      it("builds at the name once it is free, and marks the table as this package's", async () => {
         await lake.inspect(async (c) => {
            await c.runSQL(`DROP TABLE ${LAKE}.main.shared_t`);
         }, false);
         lake.attaches.length = 0;
         lake.statements.length = 0;
         builtRun = await lake.run();
         expect([builtRun.status, builtRun.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         // `plain` is carried forward; `owned` builds at its name=.
         expect(builtRun.metadata).toMatchObject({
            sourcesBuilt: 1,
            sourcesReused: 1,
         });
         expect(builtRun.manifest?.failures).toBeUndefined();
         expect(lake.entriesByName(builtRun).owned).toMatchObject({
            physicalTableName: "shared_t",
            storageDestinationName: LAKE,
            rowCount: 3,
         });
         // Three read-write sessions, one of them the federated build.
         expect(lake.attaches).toHaveLength(3);
         expect(
            lake.statements.filter((s) => s.startsWith("DESCRIBE (")),
         ).toHaveLength(1);
         expect(await lake.tableComments()).toEqual({
            [PLAIN]: null,
            shared_t: OWN_MARK,
         });
         const answer = await lake.ask(OWNED);
         expect(answer.servedFrom).toBe("storage");
         expect(answer.servedBy.tableId).toBe(
            lake.entriesByName(builtRun).owned.sourceEntityId,
         );
      });

      it("a reclaim's drop skips a table another package has marked since, and drops this package's table with its backup name", async () => {
         const owned = lake.entriesByName(builtRun).owned;
         // Another package has built `plain`'s table since, and an earlier
         // release left `shared_t`'s backup name behind.
         const backup = `shared_t${backupSuffix(owned.sourceEntityId)}`;
         await lake.inspect(async (c) => {
            await c.runSQL(
               `COMMENT ON TABLE ${LAKE}.main."${PLAIN}" IS '${OTHER_MARK}'`,
            );
            await c.runSQL(
               `CREATE TABLE ${LAKE}.main."${backup}" AS SELECT 1 AS x`,
            );
         }, false);
         expect(await lake.catalogTables()).toEqual([
            PLAIN,
            "shared_t",
            backup,
         ]);

         // The built run's tables, dropped as a reclaim drops them: `plain`
         // is probed and skipped; `shared_t` is probed, then dropped with its
         // backup name in one session.
         lake.attaches.length = 0;
         await lake.service.reclaimer.dropManifestEntries(
            lake.env,
            ENV,
            PKG,
            builtRun.id,
            "reclaim",
            Object.values(builtRun.manifest?.entries ?? {}),
            new Set(),
            (name) => lake.pkg.getMalloyConnection(name),
         );
         expect(lake.attaches).toEqual([
            { fn: "readWrite", dbName: LAKE },
            { fn: "readWrite", dbName: LAKE },
            { fn: "readWrite", dbName: LAKE },
         ]);
         expect(await lake.catalogTables()).toEqual([PLAIN]);
         expect(await lake.tableComments()).toEqual({ [PLAIN]: OTHER_MARK });
         expect(await lake.storedRows(PLAIN, "cat, amt")).toEqual([
            ["a", 1],
            ["a", 2],
            ["b", 4],
         ]);
      });
   },
);

describe.skipIf(process.platform === "win32")(
   "table ownership on a DuckLake destination: a refused name= with a sibling on its address",
   () => {
      // `owned` and `plain` compile to the same build SQL. `owned` names its
      // own table, so its name= is its own (foldDistinctTableNames): the
      // refusal is `owned`'s alone, `plain` builds its own table, and the
      // other package's table and its mark stay as they are.
      const lake = new LakeFixture("owned-twin", () => ({
         "m.malloy": `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")

#(materialize) storage="lake" name="shared_t"
source: owned is raw extend { measure: total is amt.sum() }

#(materialize) storage="lake"
source: plain is raw extend { measure: total is amt.sum() }
`,
      }));
      beforeAll(() => lake.setUp());
      afterAll(() => lake.tearDown());

      it("refuses the name= source alone; its same-SQL sibling builds its own table, and the other package's table and mark stay", async () => {
         await lake.inspect(async (c) => {
            await c.runSQL(
               `CREATE TABLE ${LAKE}.main.shared_t AS SELECT 'x' AS cat, 9.0 AS amt`,
            );
            await c.runSQL(
               `COMMENT ON TABLE ${LAKE}.main.shared_t IS '${OTHER_MARK}'`,
            );
         }, false);
         const m = await lake.run();
         // A partial run: `owned` fails, `plain` builds.
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const failures = Object.values(
            (m.manifest as { failures: Record<string, { sourceName: string }> })
               .failures,
         ).map((f) => f.sourceName);
         expect(failures).toEqual(["owned"]);
         expect(lake.entriesByName(m).plain).toMatchObject({
            physicalTableName: selfName("plain"),
            storageDestinationName: LAKE,
         });
         expect(lake.entriesByName(m).owned).toMatchObject({
            error: expect.stringContaining("(table-owned-elsewhere)"),
         });
         expect((await lake.catalogTables()).sort()).toEqual(
            [selfName("plain"), "shared_t"].sort(),
         );
         expect(await lake.storedRows("shared_t", "cat, amt")).toEqual([
            ["x", 9],
         ]);
         expect(await lake.tableComments()).toEqual({
            shared_t: OTHER_MARK,
            [selfName("plain")]: null,
         });
      });
   },
);

describe.skipIf(process.platform === "win32")(
   "a storage= build failing mid-run on a DuckLake destination",
   () => {
      const lake = new LakeFixture("failing", () => ({
         "m.malloy": TWO_SOURCES,
      }));
      beforeAll(() => lake.setUp());
      afterAll(() => lake.tearDown());

      const DAILY = selfName("daily");
      const ORDERS = selfName("orders");
      /** Fail `orders`' read-back, inside the CTAS's transaction. */
      const failOrdersReadBack = (sql: string) => {
         if (sql === `DESCRIBE "lake"."${ORDERS}"`) {
            throw new Error("injected: read-back failed");
         }
      };
      const live: Record<string, Answer["rows"]> = {};
      let first: Materialization;
      let partial: Materialization;
      let filesBefore: Record<string, string[]>;

      async function failing<T>(
         hook: (sql: string) => void,
         fn: () => Promise<T>,
      ): Promise<T> {
         lake.onStatement = hook;
         try {
            return await fn();
         } finally {
            lake.onStatement = undefined;
         }
      }

      it("rolls a failed rebuild back to the generation it replaced, and unbinds it", async () => {
         for (const [name, query] of Object.entries(TWO_QUERIES)) {
            live[name] = (await lake.ask(query)).rows;
         }
         first = await lake.run();
         expect(first.status).toBe("MANIFEST_FILE_READY");
         filesBefore = await lake.dataFiles();
         const ordersId = lake.entriesByName(first).orders.sourceEntityId;

         lake.attaches.length = 0;
         lake.statements.length = 0;
         partial = await failing(failOrdersReadBack, () =>
            lake.run({ forceRefresh: true }),
         );
         // A partial run: committed, `orders` failed, `daily` rebuilt.
         expect([partial.status, partial.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(partial.metadata).toMatchObject({
            forceRefresh: true,
            sourcesBuilt: 1,
            sourcesReused: 0,
         });
         const reason =
            "Failed to materialize source 'orders' into storage destination 'lake': injected: read-back failed";
         const manifest = partial.manifest as {
            entries: Record<string, Record<string, unknown>>;
            failures: Record<string, Record<string, unknown>>;
         };
         expect(manifest.failures).toEqual({
            [ordersId]: {
               sourceEntityId: ordersId,
               sourceName: "orders",
               materializedTableId: expect.stringMatching(/^local-/),
               physicalTableName: ORDERS,
               reason,
               connectionName: "warehouse",
               storageDestinationName: LAKE,
            },
         });
         // The failure is mirrored into `entries` with no table fields.
         expect(manifest.entries[ordersId]).toEqual({
            sourceEntityId: ordersId,
            sourceName: "orders",
            physicalTableName: ORDERS,
            materializedTableId: expect.stringMatching(/^local-/),
            error: reason,
         });
         expect(lake.entriesByName(partial).daily).toMatchObject({
            physicalTableName: DAILY,
            storageDestinationName: LAKE,
            rowCount: 2,
         });
         // `orders`' CTAS and read-back ran in one transaction, rolled back
         // on the failure; `daily` built after it on a session of its own.
         expect(lake.attaches).toEqual([
            { fn: "readWrite", dbName: LAKE },
            { fn: "readWrite", dbName: LAKE },
         ]);
         expect(lake.statements.map(statementShape)).toEqual([
            "DESCRIBE (passthrough)",
            "BEGIN TRANSACTION",
            `CREATE OR REPLACE TABLE "lake"."${ORDERS}"`,
            `DESCRIBE "lake"."${ORDERS}"`,
            "ROLLBACK",
            "DESCRIBE (passthrough)",
            "BEGIN TRANSACTION",
            `CREATE OR REPLACE TABLE "lake"."${DAILY}"`,
            `DESCRIBE "lake"."${DAILY}"`,
            "COMMIT",
            `SELECT count(*) AS n FROM "lake"."${DAILY}"`,
         ]);
         // No staging or backup name: the catalog holds the same two tables,
         // `orders` still the first run's files and rows, `daily` rewritten.
         expect(await lake.catalogTables()).toEqual([DAILY, ORDERS]);
         const filesAfter = await lake.dataFiles();
         expect(filesAfter[ORDERS]).toEqual(filesBefore[ORDERS]);
         for (const file of filesAfter[DAILY]) {
            expect(filesBefore[DAILY]).not.toContain(file);
         }
         expect(await lake.storedRows(ORDERS, "cat, amt")).toEqual([
            ["a", 1],
            ["a", 2],
            ["b", 4],
         ]);
         // The partial run binds what it built: `daily` from its new
         // generation, and `orders` live, though its earlier table stands.
         const daily = await lake.ask(TWO_QUERIES.daily);
         expect(daily.servedFrom).toBe("storage");
         expect(daily.rows).toEqual(live.daily);
         const orders = await lake.ask(TWO_QUERIES.orders);
         expect(orders.servedFrom).toBeNull();
         expect(orders.servedBy.kind).toBe("live");
         expect(orders.rows).toEqual(live.orders);
      });

      it("F-17: fails a run whose every build fails, records each source's failure on a manifest that is never served, and leaves the catalog and serving as they were", async () => {
         const filesBefore = await lake.dataFiles();
         const failed = await failing(
            (sql) => {
               if (sql.startsWith("CREATE OR REPLACE TABLE")) {
                  throw new Error("injected: write failed");
               }
            },
            () => lake.run({ forceRefresh: true }),
         );
         expect(failed.status).toBe("FAILED");
         // Each source's own reason, and no entry: the run names no table.
         // This is the full service path, so a failure dropped anywhere
         // between the build and the record fails here.
         expect(failed.manifest?.entries).toEqual({});
         // Nothing was built, so no build time: `completedAt` says when.
         expect(failed.manifest?.builtAt).toBeUndefined();
         expect(
            Object.values(failed.manifest?.failures ?? {})
               .map((f) => [f.sourceName, f.reason])
               .sort(),
         ).toEqual([
            [
               "daily",
               "Failed to materialize source 'daily' into storage destination 'lake': injected: write failed",
            ],
            [
               "orders",
               "Failed to materialize source 'orders' into storage destination 'lake': injected: write failed",
            ],
         ]);
         expect(failed.error).toBe(
            "orders: Failed to materialize source 'orders' into storage destination 'lake': injected: write failed; " +
               "daily: Failed to materialize source 'daily' into storage destination 'lake': injected: write failed",
         );
         expect(await lake.catalogTables()).toEqual([DAILY, ORDERS]);
         expect(await lake.dataFiles()).toEqual(filesBefore);
         // Still bound to the partial run.
         expect((await lake.ask(TWO_QUERIES.daily)).servedFrom).toBe("storage");
         expect((await lake.ask(TWO_QUERIES.orders)).servedFrom).toBeNull();
      });

      it("a failed first build leaves no table behind", async () => {
         // Every run deleted and its tables dropped by hand, so `orders` has
         // never been built.
         for (const m of await lake.service.listMaterializations(ENV, PKG)) {
            await lake.service.deleteMaterialization(ENV, PKG, m.id);
         }
         const left = await lake.catalogTables();
         await lake.inspect(async (c) => {
            for (const table of left) {
               await c.runSQL(`DROP TABLE ${LAKE}.main."${table}"`);
            }
         }, false);
         expect(await lake.catalogTables()).toEqual([]);
         lake.statements.length = 0;
         const m = await failing(failOrdersReadBack, () => lake.run());
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(m.metadata).toMatchObject({ sourcesBuilt: 1 });
         expect(lake.statements.map(statementShape).slice(0, 5)).toEqual([
            "DESCRIBE (passthrough)",
            "BEGIN TRANSACTION",
            `CREATE OR REPLACE TABLE "lake"."${ORDERS}"`,
            `DESCRIBE "lake"."${ORDERS}"`,
            "ROLLBACK",
         ]);
         // The rollback leaves no `orders` table, which no reclaim would
         // otherwise find: the failed source has no table entry.
         expect(await lake.catalogTables()).toEqual([DAILY]);
         expect((await lake.ask(TWO_QUERIES.orders)).servedFrom).toBeNull();
         expect((await lake.ask(TWO_QUERIES.daily)).servedFrom).toBe("storage");
      });
   },
);

/** A statement of a build session's lockdown, not of the build. */
const LOCKDOWN_STATEMENT =
   /^(SELECT current_setting\(|SELECT extension_name FROM duckdb_extensions\(\)|SELECT count\(\*\) FROM duckdb_secrets\(\)|SET (GLOBAL )?(enable_global_s3_configuration|s3_|autoinstall_known_extensions|autoload_known_extensions|allowed_directories|allowed_paths|enable_external_access|TimeZone|file_search_path)|(INSTALL|LOAD) excel)/;

const SHAPES_MODEL = `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES (1, 'a', 1.0), (2, 'a', 2.0), (3, 'b', 4.0)) AS t(id, cat, amt)
""") extend { primary_key: id }

source: kids is warehouse.sql("""
  SELECT * FROM (VALUES (1, 1, 5), (2, 1, 6), (3, 3, 7)) AS t(kid, parent_id, qty)
""") extend { primary_key: kid }

source: ops is raw extend {
  measure: n is count()
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: n }
}

#(materialize) storage="lake" inline=["k"]
source: pairs is raw extend {
  join_many: k is kids on k.parent_id = id
  measure: kq is k.qty.sum()
}

#(materialize) storage="lake"
source: parent is raw extend {
  dimension: big is amt > 1
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: child is parent extend {
  where: cat = 'a'
  dimension: label is concat(cat, '!')
}
`;

describe.skipIf(process.platform === "win32")(
   "a persisted view, a pair table and a chained extension table in a DuckLake destination",
   () => {
      // Pair tables are a gated source kind.
      const lake = new LakeFixture(
         "shapes",
         () => ({ "m.malloy": SHAPES_MODEL }),
         { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" },
      );
      beforeAll(() => lake.setUp());
      afterAll(() => lake.tearDown());

      const QUERIES = {
         view: "run: ops -> by_cat",
         pairs: "run: pairs -> { group_by: cat; aggregate: kq }",
         parent: "run: parent -> { group_by: big; aggregate: total }",
         child: "run: child -> { group_by: label, big; aggregate: total }",
      };

      it("builds each into the catalog (the extension over its parent's stored table) and serves each from storage, equal to live", async () => {
         const live: Record<string, Answer["rows"]> = {};
         for (const [name, query] of Object.entries(QUERIES)) {
            const answer = await lake.ask(query);
            expect(answer.servedFrom).toBeNull();
            live[name] = answer.rows;
         }
         expect(live.pairs).toEqual([
            { cat: "a", kq: 11 },
            { cat: "b", kq: 7 },
         ]);
         expect(
            Object.values(lake.pkg.getBuildPlan()!.sources)
               .map((s) => s.name)
               .sort(),
         ).toEqual(["child", "ops__by_cat__view", "pairs", "parent"]);

         lake.attaches.length = 0;
         lake.statements.length = 0;
         const m = await lake.run();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(m.metadata).toMatchObject({
            sourcesBuilt: 4,
            sourcesReused: 0,
            inlineUniquenessChecks: {
               checks: [{ sourceName: "pairs", join: "", outcome: "ran" }],
            },
         });
         const entries = lake.entriesByName(m);
         expect(Object.keys(entries).sort()).toEqual([
            "child",
            "ops__by_cat__view",
            "pairs",
            "parent",
         ]);
         for (const [name, entry] of Object.entries(entries)) {
            expect(entry).toMatchObject({
               physicalTableName: selfName(name),
               connectionName: "warehouse",
               storageDestinationName: LAKE,
               realization: "COPY",
            });
         }
         expect(entries.ops__by_cat__view).toMatchObject({
            schema: [
               { name: "cat", type: "VARCHAR" },
               { name: "n", type: "BIGINT" },
            ],
            rowCount: 2,
         });
         // A pair table: the parent's columns, each child's prefixed, and
         // the parent-row flag; one row per child, and one per childless
         // parent.
         expect(entries.pairs.schema?.map((c) => c.name)).toEqual([
            "id",
            "cat",
            "amt",
            "k__kid",
            "k__parent_id",
            "k__qty",
            "__parent_row",
         ]);
         expect(entries.pairs.schema).toContainEqual({
            name: "__parent_row",
            type: "BOOLEAN",
         });
         expect(entries.pairs.rowCount).toBe(4);
         expect(entries.parent.rowCount).toBe(3);
         // The extension reads its parent's stored table in the destination
         // (no passthrough): it is the one build that federates nothing.
         expect(entries.child).toMatchObject({
            rowCount: 2,
            upstreamReuse: "reused",
         });
         expect(entries.child.schema?.map((c) => c.name)).toEqual([
            "id",
            "cat",
            "amt",
            "big",
            "label",
         ]);
         expect(lake.attaches).toEqual(
            Array.from({ length: 4 }, () => ({
               fn: "readWrite",
               dbName: LAKE,
            })),
         );
         expect(
            lake.statements
               .filter((s) => s.startsWith("CREATE"))
               .map(statementShape),
         ).toEqual([
            `CREATE OR REPLACE TABLE "lake"."${selfName("pairs")}"`,
            `CREATE OR REPLACE TABLE "lake"."${selfName("parent")}"`,
            `CREATE OR REPLACE TABLE "lake"."${selfName("ops__by_cat__view")}"`,
         ]);
         expect(await lake.catalogTables()).toEqual(
            Object.keys(entries)
               .sort()
               .map((name) => selfName(name)),
         );
         expect(
            await lake.storedRows(selfName("child"), "id, cat, label"),
         ).toEqual([
            [1, "a", "a!"],
            [2, "a", "a!"],
         ]);

         for (const [name, query] of Object.entries(QUERIES)) {
            const answer = await lake.ask(query);
            expect(answer.servedFrom).toBe("storage");
            expect(answer.rows).toEqual(live[name]);
         }
         expect((await lake.ask(QUERIES.view)).servedBy).toEqual({
            kind: "view",
            tableId: entries.ops__by_cat__view.sourceEntityId,
            tier: "storage",
         });
         for (const name of ["pairs", "parent", "child"] as const) {
            expect((await lake.ask(QUERIES[name])).servedBy).toEqual({
               kind: "materialized",
               tableId: entries[name].sourceEntityId,
               tier: "storage",
               via: "served_copy",
            });
         }
      });
   },
);

describe.skipIf(process.platform === "win32")(
   "a storage= source on the package's DuckDB sandbox beside a warehouse one",
   () => {
      // A `storage=` build passes a query through to the source's warehouse
      // and cannot read the package's DuckDB sandbox. Every run used to fail
      // such a source with "Connection duckdb not found" (and a run with
      // nothing else to build failed whole); now the plan refuses it on every
      // route, it answers live, and the run builds its warehouse sibling.
      const SANDBOX = `duckdb.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")`;
      const lake = new LakeFixture(
         "sandbox",
         () => ({
            "m.malloy": `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")
source: sb_raw is ${SANDBOX}

#@ persist storage="lake"
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }

#@ persist storage="lake"
source: sb_daily is sb_raw -> { group_by: cat; aggregate: total is amt.sum() }

#(materialize) storage="lake"
source: sb_orders is sb_raw extend { measure: total is amt.sum() }

source: sb_viewed is sb_raw extend {
  measure: total is amt.sum()
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: total }
}

#@ persist storage="lake"
query: sb_query is sb_raw -> { group_by: cat; aggregate: total is amt.sum() }

source: sb_sales is sb_raw extend {
  #(preaggregate) grain=["cat"] storage="lake"
  measure: total is amt.sum()
}
`,
         }),
         { PUBLISHER_MATERIALIZE: "true" },
      );
      beforeAll(() => lake.setUp());
      afterAll(() => lake.tearDown());

      const LIVE = [
         { cat: "a", total: 3 },
         { cat: "b", total: 4 },
      ];

      it("loads, refusing each sandbox route under its own not-built code", () => {
         const warnings = lake.pkg.getPackageMetadata().warnings ?? [];
         const refusals = warnings
            .filter((w) => w.message?.includes("cannot be passed through"))
            .map((w) => `${w.code}:${w.subject}`)
            .sort();
         expect(refusals).toEqual(
            [
               "materialize-not-built:sb_orders",
               "persist-not-built:sb_daily",
               "persist-query-not-built:sb_query__query",
               "persist-view-not-built:sb_viewed__by_cat__view",
               "preaggregate-not-built:sb_sales",
               // No repeat under the former `persist-source-refused`: no
               // earlier release raised this refusal.
            ].sort(),
         );
         const plan = lake.pkg.getBuildPlan();
         expect(
            Object.values(plan?.refusedSources ?? {})
               .map((r) => r.reason)
               .filter((r) => r === "storage_source_unsupported"),
         ).toHaveLength(5);
         expect(Object.values(plan?.sources ?? {}).map((s) => s.name)).toEqual([
            "daily",
         ]);
      });

      it("builds the warehouse sibling, fails nothing, and serves it from storage", async () => {
         const m = await lake.run();
         expect(m.status).toBe("MANIFEST_FILE_READY");
         expect(m.error ?? null).toBeNull();
         expect(
            (m.manifest as { failures?: Record<string, unknown> } | null)
               ?.failures ?? {},
         ).toEqual({});
         expect(Object.keys(lake.entriesByName(m))).toEqual(["daily"]);
         const served = await lake.ask(
            "run: daily -> { group_by: cat; aggregate: t is total.sum() }",
         );
         expect(served.servedFrom).toBe("storage");
         expect(served.rows).toEqual([
            { cat: "a", t: 3 },
            { cat: "b", t: 4 },
         ]);
      });

      it("answers each refused sandbox source live", async () => {
         for (const query of [
            "run: sb_daily -> { group_by: cat; aggregate: total is total.sum() }",
            "run: sb_orders -> { group_by: cat; aggregate: total }",
            "run: sb_viewed -> by_cat",
            "run: sb_query",
            "run: sb_sales -> { group_by: cat; aggregate: total }",
         ]) {
            const answer = await lake.ask(query);
            expect(answer.servedBy.kind, query).toBe("live");
            expect(answer.rows, query).toEqual(LIVE);
         }
      });
   },
);
