// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` `#@ persist` source declared as an `extend` of a query that
 * is not itself persisted, over an upstream stored in the same destination
 * (`user_sessions is _sessionized extend {…}`, `_sessionized` a two-stage
 * query over the stored `engaged_events`): its build runs that query over
 * the upstream's table and compares the strings the query compares, so its
 * plan records those reads and the run builds it there, equal to live.
 * Before, the plan read every `extend` as a projection of a stored source,
 * recorded no reads, and every run failed with "its string reads were not
 * recorded". An `extend` of a stored source that shares its build is still
 * copied from that source's table as a projection, and the workaround shape
 * (`-> { select: * } extend {…}`) still builds.
 *
 * The destination is a DuckLake catalog in a local file, with the seams
 * materialization_storage_stacking.component.spec.ts fakes faked the same
 * way. A statement that reads the warehouse's rows names RAW_MARK.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyConfig } from "@malloydata/malloy";
import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   setDefaultTimeout,
   spyOn,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import * as connection from "../../src/service/connection";
import * as dialects from "../../src/service/dialects";
import { recordConnectionsAsForTests } from "../../src/service/connection_type";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import * as numericPrecision from "../../src/service/materialization/run/passthrough/postgres";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const LAKE = "lake";

setDefaultTimeout(120_000);

/** Names every statement that reads the warehouse's rows. */
const RAW_MARK = "raw_rows_mark";
const RAW = `warehouse.sql("""
  SELECT id, cat, sub, amt::DOUBLE AS amt, mark FROM (VALUES
    (1, 'a', 'x', 1.5, '${RAW_MARK}'), (2, 'a', 'y', 2.0, '${RAW_MARK}'),
    (3, 'b', 'x', 4.25, '${RAW_MARK}'), (4, 'b', 'x', NULL, '${RAW_MARK}'),
    (5, 'c', 'z', -3.0, '${RAW_MARK}')
  ) AS t(id, cat, sub, amt, mark)
""")`;

/** Rows with every value as a string, sorted, to compare across engines. */
const asStrings = (rows: Record<string, unknown>[]) =>
   rows
      .map((r) =>
         Object.fromEntries(
            Object.entries(r).map(([k, v]) => [
               k,
               v === null ? null : String(v),
            ]),
         ),
      )
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

type Entry = {
   sourceName: string;
   schema?: { name: string; type: string }[];
   sourceEntityId: string;
   physicalTableName: string;
   storageDestinationName?: string;
   upstreamReuse?: string;
};

const plain = (rows: Record<string, unknown>[]) =>
   rows
      .map((r) =>
         Object.fromEntries(
            Object.entries(r).map(([k, v]) => [
               k,
               typeof v === "bigint" ? Number(v) : v,
            ]),
         ),
      )
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by several in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "storage= builds over a stored table in their destination",
   () => {
      let root: string;
      let envPath: string;
      let lakeDir: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      let restore: () => void;
      let serve: MalloyDuckDBConnection | undefined;
      const retired: MalloyDuckDBConnection[] = [];
      let opened = 0;
      let statements: string[];
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
       * A fresh serve connection, so the catalog is read as it now is. The
       * previous one is retired, not closed: a bind's read of the stored
       * tables, or a served copy compiling in the background, may still be
       * on it. Every one is closed after the test.
       */
      async function reopenServe(): Promise<void> {
         if (serve) retired.push(serve);
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
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         pkg.materialization().setServeDestinationConfig(() => config);
      }

      async function setUp(files: Record<string, string>): Promise<void> {
         writePackageFiles(path.join(envPath, PKG), files, {
            manifest: { name: PKG, description: "fixture" },
         });
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
         await reopenServe();
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
      }

      beforeEach(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await __setPackageLoadPoolForTests(null);
         const storage = await openTempStorage("storage-stacking-", {
            id: ENV_ID,
            name: ENV,
         });
         ({ root, db, repo, envPath } = storage);
         lakeDir = path.join(root, "lake");
         fs.mkdirSync(path.join(lakeDir, "data"), { recursive: true });
         statements = [];
         const runSQL = MalloyDuckDBConnection.prototype.runSQL;
         spies.push(
            spyOn(
               MalloyDuckDBConnection.prototype,
               "runSQL",
            ).mockImplementation(function (
               this: MalloyDuckDBConnection,
               sql: string,
               ...rest: unknown[]
            ) {
               statements.push(sql);
               return (runSQL as (...a: unknown[]) => unknown).call(
                  this,
                  sql,
                  ...rest,
               );
            } as never),
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
            spyOn(dialects, "federateSourceForPassthrough").mockImplementation(
               async (session, sourceType) => {
                  await session.runSQL(
                     "CREATE OR REPLACE TEMP MACRO postgres_query(alias, q) AS TABLE SELECT * FROM query(q)",
                  );
                  return { handle: "warehouse", sourceType };
               },
            ),
            // The stand-in warehouse is DuckDB, which has no Postgres type
            // catalog to probe; it holds no numeric to read as text.
            spyOn(
               numericPrecision,
               "postgresNumericColumns",
            ).mockImplementation(async () => ({ columns: [], numeric: [] })),
            {
               mockRestore: recordConnectionsAsForTests({
                  warehouse: "postgres",
               }),
            },
         );
      });

      afterEach(async () => {
         for (const spy of spies.splice(0)) spy.mockRestore();
         await serve?.close().catch(() => undefined);
         serve = undefined;
         for (const c of retired.splice(0)) {
            await c.close().catch(() => undefined);
         }
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         fs.rmSync(root, { recursive: true, force: true });
         restore?.();
         await __setPackageLoadPoolForTests(null);
      });

      async function build(
         options: Record<string, unknown> = {},
      ): Promise<Materialization> {
         statements.length = 0;
         const created = await service.createMaterialization(ENV, PKG, options);
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

      const entriesOf = (m: Materialization) =>
         Object.values(
            (m.manifest as { entries: Record<string, Entry> }).entries,
         );
      const entryOf = (m: Materialization, name: string | RegExp) => {
         const hit = entriesOf(m).find((e) =>
            typeof name === "string"
               ? e.sourceName === name
               : name.test(e.sourceName),
         );
         if (!hit) throw new Error(`no entry for ${String(name)}`);
         return hit;
      };
      /** The statement that created `table`. */
      const createdBy = (table: string) => {
         const hit = statements.find(
            (s) => /\bCREATE\b/i.test(s) && s.includes(table),
         );
         if (!hit) throw new Error(`no CREATE of ${table}`);
         return hit;
      };

      /** Rows of a stored table, read from the catalog. */
      async function storedRows(table: string) {
         const c = new MalloyDuckDBConnection(
            "reader",
            path.join(root, `reader_${++opened}.duckdb`),
         );
         try {
            await attachFileLake(c, "rd", true);
            const result = await c.runSQL(`SELECT * FROM rd."${table}"`);
            return plain(result.rows as Record<string, unknown>[]);
         } finally {
            await c.close();
         }
      }

      /** Rows of the planned (warehouse) build of the table `entry` names. */
      async function plannedRows(entry: Entry) {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const planned = Object.values(pkg.getBuildPlan()!.sources).find(
            (s) => s.sourceEntityId === entry.sourceEntityId,
         )!;
         expect(planned.sql).toContain(RAW_MARK);
         const c = new MalloyDuckDBConnection(
            "planned",
            path.join(root, `planned_${++opened}.duckdb`),
         );
         try {
            const result = await c.runSQL(planned.sql);
            return plain(result.rows as Record<string, unknown>[]);
         } finally {
            await c.close();
         }
      }

      async function answer(model: string, query: string) {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const result = await pkg
            .getModel(model)!
            .runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            rows: plain(result.compactResult as Record<string, unknown>[]),
            servedBy: result.path?.servedBy?.kind ?? "live",
         };
      }

      /**
       * The stored upstream; an unpersisted query over it and its persisted
       * `extend`; a stored query over it (`daily`) and an `extend` of that
       * which shares its build; and the workaround shape.
       */
      const EXTEND_MODEL = `##! experimental.persistence
source: raw is ${RAW}

#@ persist storage="${LAKE}" name="ext_engaged"
source: engaged is raw -> { select: id, cat, sub, amt } extend {
  dimension: big is amt > 1
}

source: _sessionized is engaged -> {
  select: *
  calculate: prev_amt is lag(amt) { partition_by: cat; order_by: id }
} -> {
  group_by: cat
  aggregate: n is count(), xs is count() { where: sub = 'x' }
}

#@ persist storage="${LAKE}" name="ext_sessions"
source: sessions is _sessionized extend {
  dimension: loud is upper(cat)
  measure: total_n is n.sum()
}

#@ persist storage="${LAKE}" name="ext_daily"
source: daily is engaged -> { group_by: cat; aggregate: n is count() }

#@ persist storage="${LAKE}" name="ext_daily_copy"
source: daily_copy is daily extend {
  dimension: loud is upper(cat)
}

#@ persist storage="${LAKE}" name="ext_reshaped"
source: reshaped is _sessionized -> { select: * } extend {
  measure: total_n is n.sum()
}
`;
      /** No string is ordered: a query that orders one answers live. */
      const SESSIONS_QUERY =
         "run: sessions -> { group_by: cat, loud; aggregate: total_n, x is xs.sum() }";

      it("builds a storage= extend of an unpersisted query over its stored upstream's table, and serves it equal to live", async () => {
         await setUp({ "m.malloy": EXTEND_MODEL });
         const live = await answer("m.malloy", SESSIONS_QUERY);
         expect(live.servedBy).toBe("live");
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const upstream = entryOf(m, "engaged");
         const sessions = entryOf(m, "sessions") as Entry & {
            error?: string;
            builtFrom?: { sourceEntityId: string }[];
         };
         expect(sessions.error).toBeUndefined();
         expect(sessions.upstreamReuse).toBe("reused");
         expect(sessions.builtFrom?.map((b) => b.sourceEntityId)).toEqual([
            upstream.sourceEntityId,
         ]);
         // Built over the upstream's table, never from the warehouse.
         expect(createdBy(sessions.physicalTableName)).not.toContain(RAW_MARK);
         expect(await storedRows(sessions.physicalTableName)).toEqual(
            await plannedRows(sessions),
         );
         const served = await answer("m.malloy", SESSIONS_QUERY);
         expect(served.servedBy).not.toBe("live");
         expect(asStrings(served.rows)).toEqual(asStrings(live.rows));
      });

      it("still copies a storage= extend of a stored source that shares its build from that source's table, as a projection", async () => {
         await setUp({ "m.malloy": EXTEND_MODEL });
         const m = await build();
         const daily = entryOf(m, "daily");
         const copy = entryOf(m, "daily_copy") as Entry & { error?: string };
         expect(copy.error).toBeUndefined();
         expect(copy.upstreamReuse).toBe("reused");
         // A projection: its columns, read whole from `daily`'s table.
         const created = createdBy(copy.physicalTableName);
         expect(created).toContain(
            `FROM "${LAKE}"."${daily.physicalTableName}"`,
         );
         expect(created).not.toContain(RAW_MARK);
         expect(created).not.toMatch(/\bGROUP BY\b/i);
         expect(await storedRows(copy.physicalTableName)).toEqual(
            await storedRows(daily.physicalTableName),
         );
      });

      it("builds the workaround shape, an extend over `-> { select: * }` of the unpersisted query", async () => {
         await setUp({ "m.malloy": EXTEND_MODEL });
         const query = "run: reshaped -> { group_by: cat; aggregate: total_n }";
         const live = await answer("m.malloy", query);
         const m = await build();
         const reshaped = entryOf(m, "reshaped") as Entry & { error?: string };
         expect(reshaped.error).toBeUndefined();
         expect(reshaped.upstreamReuse).toBe("reused");
         expect(createdBy(reshaped.physicalTableName)).not.toContain(RAW_MARK);
         const served = await answer("m.malloy", query);
         expect(served.servedBy).not.toBe("live");
         expect(asStrings(served.rows)).toEqual(asStrings(live.rows));
      });
   },
);
