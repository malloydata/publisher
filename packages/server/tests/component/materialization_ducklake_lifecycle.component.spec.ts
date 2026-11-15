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

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import * as connection from "../../src/service/connection";
import { Environment } from "../../src/service/environment";
import type { EnvironmentStore } from "../../src/service/environment_store";
import { parsePackageName } from "../../src/service/identity";
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
   servedBy: { kind: string; tableId?: string; via?: string };
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
         pkg.setServeDestinationConfig(() => config);
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
         service = new MaterializationService({
            storageManager: { getRepository: () => repo },
            getEnvironment: async () => env,
            getLoadedEnvironments: () => [env],
         } as unknown as EnvironmentStore);
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

         // Deleting the record alone (no dropTables) touches no catalog and
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

      it("reclaims: a superseded run's drop keeps the shared tables, the last run's drop removes them and serving goes live", async () => {
         const filesBefore = await dataFiles();
         const tables = NAMES.map(
            (n) => firstEntries[n].physicalTableName,
         ).sort();

         // The first run's tables carry the same names the forced run serves,
         // so its drop skips every one: no read-write attach at all.
         attaches.length = 0;
         await service.deleteMaterialization(ENV, PKG, first.id, {
            dropTables: true,
         });
         expect(attaches).toEqual([]);
         expect(await catalogTables()).toEqual(tables);
         await reopenServe();
         for (const name of NAMES) {
            expect((await ask(name)).servedFrom).toBe("storage");
         }

         // The last run's drop: per entry, a read-write session that checks no
         // other package has marked the table, then one that drops it (and
         // its backup name).
         attaches.length = 0;
         await service.deleteMaterialization(ENV, PKG, second.id, {
            dropTables: true,
         });
         expect(attaches).toEqual(
            [...NAMES, ...NAMES].map(() => ({ fn: "readWrite", dbName: LAKE })),
         );
         expect(await repo.getMaterializationById(second.id)).toBeFalsy();
         expect(await catalogTables()).toEqual([]);
         await inspect(async (c) => {
            const left = await c.runSQL(
               `SELECT table_name FROM duckdb_tables() WHERE database_name = '${LAKE}'`,
            );
            expect(left.rows).toEqual([]);
         });
         // A drop retires the table in the catalog; its Parquet files stay on
         // disk until a file cleanup, which the reclaim does not run.
         for (const table of tables) {
            for (const file of filesBefore[table]) {
               expect(fs.existsSync(file)).toBe(true);
            }
         }

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
