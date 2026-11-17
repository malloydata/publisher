// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P4-01: DuckLake (DuckDB 1.5) writes a 128-bit integer column to Parquet as
 * a DOUBLE while its catalog still reports HUGEINT, so a sum of BIGINTs
 * stored in a `storage=` table came back rounded
 * (4530000000000225187 as 4530000000000225280), and Publisher, trusting the
 * catalog, read it as an exact `HUGEINT` and reported it materialized.
 *
 * End to end on a real DuckLake (file catalog, Parquet data), with the
 * lifecycle spec's three seams (materialization_ducklake_lifecycle):
 *
 * - The fix: a build stores a 128-bit integer it computed as DECIMAL(38,0),
 *   which Parquet keeps exactly, so a `#@ persist storage=` source, a
 *   `#(materialize)` source, a `#(preaggregate)` rollup and a persisted view
 *   are served from the table and equal live digit for digit.
 * - The guard: a value past 38 digits fails that cast, the build stores the
 *   128-bit type (as an older build did), and every answer reading it is
 *   answered live, the path report saying why.
 * - Controls: a `BIGINT` and a `DECIMAL(38,0)` column serve as before.
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

/** A BIGINT past 2^53; with 1 added, the sum P4-01 stored rounded. */
const BIG = 4530000000000225186n;
const SUM = `${BIG + 1n}`;
/** Groups: each sums BIG and 1, so every stored row is SUM. */
const GROUPS = 600;
/** Past DECIMAL(38,0), within HUGEINT: no exact storage type in DuckLake. */
const WIDE = "150000000000000000000000000000000000001";

/**
 * `raw` has, per key `k`, one row of BIG and one of 1 (`v`, a BIGINT), and
 * `d`, the same values as a DECIMAL(38,0); `sums` is the per-key sum as the
 * warehouse computes it, a HUGEINT in DuckDB.
 */
const MODEL = `##! experimental.persistence
source: raw is warehouse.sql("""
  SELECT 'k' || lpad((i % ${GROUPS})::VARCHAR, 5, '0') AS k,
         (CASE WHEN i < ${GROUPS} THEN ${BIG} ELSE 1 END)::BIGINT AS v,
         (CASE WHEN i < ${GROUPS} THEN ${BIG} ELSE 1 END)::DECIMAL(38,0) AS d
  FROM range(${2 * GROUPS}) AS t(i)
""") extend {
  #(preaggregate) grain=["k"] storage="lake"
  measure: v_total is v.sum()
  #@ persist storage="lake"
  view: by_k is { group_by: k; aggregate: total is v.sum() }
  #@ persist storage="lake"
  view: by_k_top is { group_by: k; aggregate: top is v.max() }
}

// The control rollup has a table of its own: a rollup declines as a whole
// when any column it stores has no exact read.
source: raw_top is raw extend {
  #(preaggregate) grain=["k"] storage="lake"
  measure: v_top is v.max()
}

source: sums is warehouse.sql("""
  SELECT k, SUM(v) AS total FROM (
    SELECT 'k' || lpad((i % ${GROUPS})::VARCHAR, 5, '0') AS k,
           (CASE WHEN i < ${GROUPS} THEN ${BIG} ELSE 1 END)::BIGINT AS v
    FROM range(${2 * GROUPS}) AS t(i)
  ) GROUP BY k
""")

#@ persist storage="lake"
source: p_sum is raw -> { group_by: k; aggregate: total is v.sum(), n is count() }

#@ persist storage="lake"
source: p_ctl is raw -> { group_by: k; aggregate: top is v.max(), lo is v.min() }

#(materialize) storage="lake"
source: m_sum is sums extend { primary_key: k }

#(materialize) storage="lake"
source: m_ctl is raw extend {}

// One HUGEINT per key past DECIMAL(38,0): every table over it falls back to
// the 128-bit type, which DuckLake stores as a DOUBLE.
source: wide_base is warehouse.sql("""
  SELECT 'k' || lpad(i::VARCHAR, 5, '0') AS k, ${WIDE}::HUGEINT AS h
  FROM range(${GROUPS}) AS t(i)
""")

source: wide is wide_base extend {
  #(preaggregate) grain=["k"] storage="lake"
  measure: h_total is h.sum()
  #@ persist storage="lake"
  view: by_k_wide is { group_by: k; aggregate: ht is h.sum() }
}

#@ persist storage="lake"
source: p_wide is wide -> { group_by: k; aggregate: hs is h.sum(), n is count() }

#(materialize) storage="lake"
source: m_wide is wide_base extend {}
`;

/** Each query, what it reads, and whether the table answers it. */
const CASES: {
   name: string;
   malloy: string;
   /** The column holding the big value, and the value every row holds. */
   exact?: { column: string; value: string };
   /** Answered from the table (a control); else live. */
   served?: true;
   /** The path report's kind for the table that would answer it. */
   leg?: "materialized" | "rollup" | "view";
}[] = [
   {
      name: "P4-01 #@ persist storage=: a stored sum of BIGINTs",
      malloy: "run: p_sum -> { select: k, total; order_by: k }",
      exact: { column: "total", value: SUM },
      served: true,
   },
   {
      name: "P4-01 #(materialize) storage=: a stored HUGEINT column",
      malloy: "run: m_sum -> { select: k, total; order_by: k }",
      exact: { column: "total", value: SUM },
      served: true,
   },
   {
      name: "P4-01 #(preaggregate) storage=: a rollup of a BIGINT sum",
      malloy: "run: raw -> { group_by: k; aggregate: v_total; order_by: k }",
      exact: { column: "v_total", value: SUM },
      served: true,
   },
   {
      name: "P4-01 #@ persist on a view, storage=: a stored BIGINT sum",
      malloy: "run: raw -> by_k",
      exact: { column: "total", value: SUM },
      served: true,
   },
   {
      name: "P4-01 guard, #@ persist storage=: a 128-bit sum past 38 digits answers live",
      malloy: "run: p_wide -> { select: k, hs; order_by: k }",
      exact: { column: "hs", value: WIDE },
      leg: "materialized",
   },
   {
      name: "P4-01 guard, #(materialize) storage=: a HUGEINT past 38 digits answers live",
      malloy: "run: m_wide -> { select: k, h; order_by: k }",
      exact: { column: "h", value: WIDE },
      leg: "materialized",
   },
   {
      name: "P4-01 guard, #(preaggregate) storage=: a rollup of it answers live",
      malloy: "run: wide -> { group_by: k; aggregate: h_total; order_by: k }",
      exact: { column: "h_total", value: WIDE },
      leg: "rollup",
   },
   {
      name: "P4-01 guard, a persisted view of it answers live",
      malloy: "run: wide -> by_k_wide",
      exact: { column: "ht", value: WIDE },
      leg: "view",
   },
   {
      name: "P4-01 control: a stored BIGINT (#@ persist) still serves",
      malloy: "run: p_ctl -> { select: k, top; order_by: k }",
      exact: { column: "top", value: `${BIG}` },
      served: true,
   },
   {
      name: "P4-01 control: stored BIGINT and DECIMAL(38,0) columns (#(materialize)) still serve",
      malloy:
         "run: m_ctl -> { group_by: k; aggregate: vs is v.sum(), ds is d.sum(); order_by: k }",
      exact: { column: "vs", value: SUM },
      served: true,
   },
   {
      name: "P4-01 control: a rollup of a BIGINT max still serves",
      malloy: "run: raw_top -> { group_by: k; aggregate: v_top; order_by: k }",
      exact: { column: "v_top", value: `${BIG}` },
      served: true,
   },
   {
      name: "P4-01 control: a persisted view of a BIGINT max still serves",
      malloy: "run: raw -> by_k_top",
      exact: { column: "top", value: `${BIG}` },
      served: true,
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
   "P4-01: a 128-bit integer stored in DuckLake is answered live, never read as exact",
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
               .sort((x, y) => String(x.k).localeCompare(String(y.k))),
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
            live[c.name] = answer.rows;
            // Live keeps every digit.
            expect(answer.rows).toHaveLength(GROUPS);
            if (c.exact) {
               for (const row of answer.rows) {
                  expect(String(row[c.exact.column])).toBe(c.exact.value);
               }
            }
         }
      });

      it("builds every table into the lake as Parquet: a computed sum as DECIMAL(38,0), every digit kept", async () => {
         const run = await build();
         expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const entries = Object.values(
            (
               run.manifest as {
                  entries: Record<
                     string,
                     {
                        sourceName: string;
                        physicalTableName: string;
                        schema?: { name: string; type: string }[];
                     }
                  >;
               }
            ).entries,
         );
         const entry = (name: string) =>
            entries.find((e) => e.sourceName === name)!;
         for (const name of ["p_sum", "m_sum"]) {
            expect(entry(name).schema).toContainEqual({
               name: "total",
               type: "DECIMAL(38,0)",
            });
         }
         // Past 38 digits the cast fails and the 128-bit type is stored.
         expect(entry("p_wide").schema).toContainEqual({
            name: "hs",
            type: "HUGEINT",
         });
         const stored = (name: string, column: string) =>
            inspect(async (c) => {
               const table = entry(name).physicalTableName;
               const files = await c.runSQL(
                  `SELECT data_file FROM ducklake_list_files('${LAKE}', '${table}')`,
               );
               expect(files.rows.length).toBeGreaterThan(0);
               const rows = await c.runSQL(
                  `SELECT DISTINCT ${column}::VARCHAR AS v FROM ${LAKE}.main."${table}"`,
               );
               return rows.rows.map((r) => String(r.v));
            });
         expect(await stored("p_sum", "total")).toEqual([SUM]);
         // What the guard answers live: the digits are gone at rest.
         expect(await stored("p_wide", "hs")).not.toContain(WIDE);
      });

      for (const c of CASES) {
         it(c.name, async () => {
            const answer = await ask(c.malloy);
            expect(answer.rows).toEqual(live[c.name]);
            if (c.exact) {
               for (const row of answer.rows) {
                  expect(String(row[c.exact.column])).toBe(c.exact.value);
               }
            }
            if (c.served) {
               expect(answer.servedFrom).toBe("storage");
               expect(answer.servedBy.kind).not.toBe("live");
            } else {
               expect(answer.servedFrom).toBeNull();
               expect(answer.servedBy.kind).toBe("live");
               // The path report says why: the table that would answer was
               // skipped for a field it cannot give exactly.
               const considered = (
                  answer.path as {
                     considered: {
                        kind: string;
                        outcome: string;
                        reason?: string;
                        detail?: string;
                     }[];
                  }
               ).considered.filter((t) => t.kind === c.leg);
               expect(considered.length).toBeGreaterThan(0);
               for (const t of considered) {
                  expect(t).toMatchObject({
                     outcome: "skipped",
                     reason: "MISSING_FIELD",
                  });
                  if (c.leg !== "materialized") {
                     expect(t.detail).toMatch(
                        /\(HUGEINT\) cannot hold live's integer exactly/,
                     );
                  }
               }
            }
         });
      }
   },
);
