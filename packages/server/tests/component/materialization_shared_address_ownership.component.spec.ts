// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A refused `name=` refuses every source sharing its table, on a DuckLake
 * `storage=` destination, end to end through a real Environment and
 * MaterializationService.
 *
 * `owned` and `plain` compile to the same build SQL, so they share one content
 * address, and auto-run derives ONE instruction for it, at `owned`'s `name=`.
 * When another package's table holds that name, the ownership check refuses
 * the instruction: no source on the address may write it. `solo` is a table
 * of its own, so the run commits and its manifest can be read.
 *
 * The DuckLake seams are faked as in the other DuckLake component specs: the
 * three exported attaches attach a DuckLake catalog kept in a local file, the
 * `warehouse` (a DuckDB connection) is reported to the build as Postgres so
 * the passthrough build is taken, with `postgres_query` defined as a macro
 * that runs its SQL in place, and the package's serve destination is a DuckDB
 * connection with the file catalog attached read-only as `lake`. Every
 * instance that attached the catalog reads it as it was when it attached, so
 * the serve connection is reopened after each run and every look opens a
 * fresh instance.
 *
 * The colocated tier and a `scope: package` version are
 * src/service/materialization/run/shared_address_ownership.spec.ts.
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
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { ownedTableComment } from "../../src/service/materialization/run/table_ownership";
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

/** The mark another package's build leaves on its table. */
const OTHER_MARK = ownedTableComment("other/pkg2");
const OWN_MARK = ownedTableComment(`${ENV}/${PKG}`);

const MODEL = `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")

#(materialize) storage="lake" name="shared_t"
source: owned is raw extend { measure: total is amt.sum() }

#(materialize) storage="lake"
source: plain is raw extend { measure: total is amt.sum() }

#(materialize) storage="lake"
source: solo is raw extend { where: cat = 'a' }
`;

const QUERY = (source: string) =>
   source === "solo"
      ? "run: solo -> { group_by: cat }"
      : `run: ${source} -> { group_by: cat; aggregate: total }`;

type Entry = {
   sourceName: string;
   physicalTableName?: string;
   error?: unknown;
};

/** `v` as a number when it reads as one, else as it is. */
function asNumber(v: unknown): unknown {
   if (typeof v === "bigint") return Number(v);
   if (typeof v === "string" && v.trim() !== "" && !isNaN(Number(v))) {
      return Number(v);
   }
   return v;
}

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by several in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "a refused name= on a DuckLake destination refuses the sibling sharing its table",
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
       * A fresh instance with the catalog attached, for a look or (`readOnly`
       * false) a plant, detached before it closes so a plant is checkpointed.
       */
      async function inspect<T>(
         fn: (c: MalloyDuckDBConnection) => Promise<T>,
         readOnly = true,
      ): Promise<T> {
         const c = new MalloyDuckDBConnection(
            "inspect",
            path.join(root, `inspect_${++opened}.duckdb`),
         );
         try {
            await attachFileLake(c, LAKE, readOnly);
            try {
               return await fn(c);
            } finally {
               await c.runSQL(`DETACH ${LAKE}`);
            }
         } finally {
            await c.close();
         }
      }

      /** The catalog's tables, each with its comment, rows and files. */
      function catalog(): Promise<
         Record<
            string,
            { comment: string | null; rows: unknown[][]; files: string[] }
         >
      > {
         return inspect(async (c) => {
            const tables = await c.runSQL(
               `SELECT table_name, comment FROM duckdb_tables() WHERE database_name = '${LAKE}' ORDER BY table_name`,
            );
            const out: Record<
               string,
               { comment: string | null; rows: unknown[][]; files: string[] }
            > = {};
            for (const row of tables.rows) {
               const table = String(row.table_name);
               const rows = await c.runSQL(
                  `SELECT * FROM ${LAKE}.main."${table}" ORDER BY ALL`,
               );
               const files = await c.runSQL(
                  `SELECT data_file FROM ducklake_list_files('${LAKE}', '${table}')`,
               );
               out[table] = {
                  comment: (row.comment as string | null) ?? null,
                  rows: rows.rows.map((r) => Object.values(r).map(asNumber)),
                  files: files.rows.map((r) => String(r.data_file)).sort(),
               };
            }
            return out;
         });
      }

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
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "ducklake-shared-address-")),
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
                  await attachFileLake(session as never, dbName, false);
               },
            ),
            spyOn(connection, "attachDuckLakeReadOnly").mockImplementation(
               async (session, dbName) => {
                  await attachFileLake(session as never, dbName, true);
               },
            ),
            spyOn(
               connection,
               "attachDuckLakeReadOnlyIfPresent",
            ).mockImplementation(async (session, dbName) => {
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
         if (root) fs.rmSync(root, { recursive: true, force: true });
         restore?.();
      });

      async function run(): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, {});
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         const deadline = Date.now() + 120_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
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

      async function ask(
         source: string,
      ): Promise<{ rows: unknown[]; servedFrom: string | null }> {
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query: QUERY(source) } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         const rows = (result.compactResult as Record<string, unknown>[])
            .map((r) =>
               Object.fromEntries(
                  Object.entries(r).map(([k, v]) => [k, asNumber(v)]),
               ),
            )
            .sort((x, y) => String(x.cat).localeCompare(String(y.cat)));
         return { rows, servedFrom: result.servedFrom ?? null };
      }

      function outcome(m: Materialization) {
         const manifest = m.manifest as {
            entries?: Record<string, Entry>;
            failures?: Record<string, { sourceName: string; reason: string }>;
         } | null;
         return {
            built: Object.values(manifest?.entries ?? {})
               .filter((e) => !e.error && e.physicalTableName)
               .map((e) => e.sourceName)
               .sort(),
            failures: Object.values(manifest?.failures ?? {}),
         };
      }

      const LIVE = [
         { cat: "a", total: 3 },
         { cat: "b", total: 4 },
      ];

      it("refuses the name= source AND its same-address sibling, leaving the other package's table and mark untouched", async () => {
         for (const source of ["owned", "plain"]) {
            expect(await ask(source)).toEqual({ rows: LIVE, servedFrom: null });
         }
         await inspect(async (c) => {
            await c.runSQL(
               `CREATE TABLE ${LAKE}.main.shared_t AS SELECT 'x' AS cat, 9.0 AS amt`,
            );
            await c.runSQL(
               `COMMENT ON TABLE ${LAKE}.main.shared_t IS '${OTHER_MARK}'`,
            );
         }, false);
         const before = (await catalog()).shared_t;
         expect(before).toMatchObject({
            comment: OTHER_MARK,
            rows: [["x", 9]],
         });

         const m = await run();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(m.metadata).toMatchObject({ sourcesBuilt: 1 });
         const { built, failures } = outcome(m);
         // Only `solo`: nothing on the refused address was built.
         expect(built).toEqual(["solo"]);
         expect(failures).toHaveLength(1);
         expect(failures[0].reason).toContain(
            "is not built (table-owned-elsewhere): its name= 'shared_t' in storage destination 'lake' is held by a table package 'other/pkg2' built",
         );

         // The other package's table: its rows, files and mark, as they were.
         expect((await catalog()).shared_t).toEqual(before);

         // Both sources sharing the refused table answer live.
         for (const source of ["owned", "plain"]) {
            expect(await ask(source)).toEqual({ rows: LIVE, servedFrom: null });
         }
         expect((await ask("solo")).servedFrom).toBe("storage");
      });

      it("once the name is free, builds the shared table once, marks it as this package's, and serves both from it", async () => {
         await inspect(async (c) => {
            await c.runSQL(`DROP TABLE ${LAKE}.main.shared_t`);
         }, false);
         const m = await run();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(outcome(m).failures).toEqual([]);
         const shared = (await catalog()).shared_t;
         expect(shared.comment).toBe(OWN_MARK);
         expect(shared.rows).toEqual([
            ["a", 1],
            ["a", 2],
            ["b", 4],
         ]);
         for (const source of ["owned", "plain"]) {
            expect(await ask(source)).toEqual({
               rows: LIVE,
               servedFrom: "storage",
            });
         }
      });
   },
);
