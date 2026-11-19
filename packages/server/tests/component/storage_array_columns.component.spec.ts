// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-07: a `#(materialize) storage=` source over a BigQuery `ARRAY` column
 * (`array_agg(...)`, a REPEATED field) answered every query live, whole
 * source, with `served-copy-not-served` (`column_types`). Its build
 * projected the stored table to the source's ATOMIC fields, and Malloy
 * types an array column as a source of its own (an `ExploreField`), so the
 * build never wrote it; the served copy then found a declared column the
 * table lacks and left the whole source as written.
 *
 * End to end on a real DuckLake (file catalog, Parquet data), with the
 * seams of the lifecycle spec (materialization_ducklake_lifecycle): the
 * build runs through the real service and passthrough CTAS; only the
 * warehouse read is stood in for. `bq` is a real BigQuery connection whose
 * schema probe answers a fixed REPEATED schema, so the model compiles in
 * BigQuery's dialect; its passthrough read runs the build SQL in DuckDB
 * (the build SQL selects only by quoted name over the author's SQL, which is
 * written to run in both). Live BigQuery is never reached: it answers no
 * rows, and counts each read, so a query it answers is seen to go live. `warehouse` is a DuckDB twin of the same rows
 * whose live answers are the reference, and is itself a `storage=` source
 * read through the Postgres passthrough, which projected its lists away
 * the same way.
 *
 * - A list of scalars (`string[]`, `number[]`) and a list of records are
 *   stored as DuckDB lists of the mapped element type.
 * - The served copy declares them: a query reading one, and a query that
 *   does not, are served from the table, equal to live.
 */

import { BigQueryConnection } from "@malloydata/db-bigquery";
import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import * as connection from "../../src/service/connection";
import { recordConnectionsAsForTests } from "../../src/service/connection_type";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const LAKE = "lake";

/** Runs in DuckDB and, quoted by name, under BigQuery's build SQL. */
const ROWS = `SELECT * FROM (VALUES
    (1, 'acme', ['x', 'y'], [1, 2], [{'a': 'p', 'n': 1}]),
    (2, 'acme', ['y'], [3], [{'a': 'r', 'n': 3}, {'a': 's', 'n': 4}]),
    (3, 'globex', ['x', 'z', 'w'], [4, 5, 6], [{'a': 'p', 'n': 6}]),
    (4, 'globex', []::VARCHAR[], []::INTEGER[], [{'a': 'q', 'n': 2}])
  ) AS t(sid, org, v, w, lr)`;

/** What BigQuery's dry run reports for {@link ROWS}: three REPEATED columns. */
const BIGQUERY_SCHEMA = {
   fields: [
      { name: "sid", type: "INTEGER", mode: "NULLABLE" },
      { name: "org", type: "STRING", mode: "NULLABLE" },
      { name: "v", type: "STRING", mode: "REPEATED" },
      { name: "w", type: "INTEGER", mode: "REPEATED" },
      {
         name: "lr",
         type: "RECORD",
         mode: "REPEATED",
         fields: [
            { name: "a", type: "STRING", mode: "NULLABLE" },
            { name: "n", type: "INTEGER", mode: "NULLABLE" },
         ],
      },
   ],
};

const MODEL = `##! experimental { persistence access_modifiers }
// The reference: live answers over the same rows.
source: ref is warehouse.sql("""${ROWS}""") extend {
  primary_key: sid
  measure: n is count()
}

#(materialize) storage="lake"
source: s_arr is bq.sql("""${ROWS}""") extend {
  primary_key: sid
  measure: n is count()
}

#(materialize) storage="lake"
source: d_arr is warehouse.sql("""${ROWS}""") extend {
  primary_key: sid
  measure: n is count()
}

// v1 persisted queries: one stores the lists it selects, one a nest.
#@ persist storage="lake"
source: p_list is ref -> { select: sid, v, lr }

#@ persist storage="lake"
source: p_nest is ref -> {
  group_by: org
  nest: by_sid is { group_by: sid; order_by: sid }
}

// Hidden lists a measure reads: private and internal, each with its live
// twin. Each reads its own SQL, so no two share a table.
source: h_ref is warehouse.sql("""SELECT * FROM (${ROWS}) AS h_ref""") include { private: v; public: * } extend {
  primary_key: sid
  measure: n is count()
  measure: nv is count(v.each)
}

#(materialize) storage="lake"
source: h_private is warehouse.sql("""SELECT * FROM (${ROWS}) AS h_private""") include { private: v; public: * } extend {
  primary_key: sid
  measure: n is count()
  measure: nv is count(v.each)
}

// A hidden column live types as warehouse-native (a UUID), left out per query.
#(materialize) storage="lake"
source: h_uuid is warehouse.sql("""SELECT sid, org, ('00000000-0000-0000-0000-00000000000' || sid)::UUID AS u FROM (${ROWS}) AS h_uuid""") include { private: u; public: * } extend {
  primary_key: sid
  measure: n is count()
  view: by_u is { group_by: u; aggregate: n }
}

#(materialize) storage="lake"
source: h_internal is warehouse.sql("""SELECT * FROM (${ROWS}) AS h_internal""") include { internal: v; public: * } extend {
  primary_key: sid
  measure: n is count()
  measure: nv is count(v.each)
}
`;

/**
 * Queries that read a list of strings, or no list: served from the table on
 * either origin. Each runs on `ref` for live's answer.
 */
const SERVED = (source: string) => [
   `run: ${source} -> { select: sid, v; order_by: sid }`,
   `run: ${source} -> { aggregate: m is count(v.each) }`,
   `run: ${source} -> { group_by: e is v.each; aggregate: c is count(); order_by: e }`,
   `run: ${source} -> { where: v.each = 'x'; aggregate: c is count() }`,
   `run: ${source} -> { aggregate: n }`,
   `run: ${source} -> { group_by: org; aggregate: n; order_by: org }`,
   `run: ${source} -> { select: sid, org; order_by: sid }`,
];

/**
 * Queries that read a list holding integers. DuckDB's `INTEGER` is exact in
 * a JavaScript number, so `d_arr` serves them; BigQuery's `INT64` is a big
 * integer live keeps exact, which no list declaration reads back, so
 * `s_arr` leaves those columns out and answers just these queries live.
 */
const READS_INTEGER_LISTS = (source: string) => [
   `run: ${source} -> { select: sid, w; order_by: sid }`,
   `run: ${source} -> { aggregate: t is w.each.sum() }`,
   `run: ${source} -> { group_by: lr.a; aggregate: t is lr.n.sum(); order_by: a }`,
   `run: ${source} -> { select: sid, lr; order_by: sid }`,
];

/** v1 persisted queries, each with its live twin on `ref`. */
const PERSISTED: [string, string][] = [
   [
      "run: p_list -> { select: sid, v, lr; order_by: sid }",
      "run: ref -> { select: sid, v, lr; order_by: sid }",
   ],
   [
      "run: p_list -> { group_by: e is v.each; aggregate: c is count(); order_by: e }",
      "run: ref -> { group_by: e is v.each; aggregate: c is count(); order_by: e }",
   ],
   [
      "run: p_nest -> { select: org, by_sid; order_by: org }",
      "run: ref -> { group_by: org; nest: by_sid is { group_by: sid; order_by: sid }; order_by: org }",
   ],
];

/** Queries over a source whose measure reads its hidden list `v`. */
const HIDDEN_LIST = (source: string) => [
   `run: ${source} -> { aggregate: nv }`,
   `run: ${source} -> { group_by: org; aggregate: nv, n; order_by: org }`,
   `run: ${source} -> { aggregate: n }`,
];

/** Rows with every integer a number: BigQuery's INT64 reads as a bigint. */
function plain(rows: Record<string, unknown>[]): unknown {
   return JSON.parse(
      JSON.stringify(rows, (_k, v) => (typeof v === "bigint" ? Number(v) : v)),
   );
}

type Answer = {
   rows: Record<string, unknown>[];
   servedFrom: string | null;
   servedBy: { kind: string };
   path: unknown;
};

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by several in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "F-07: a storage= array column, BigQuery- or DuckDB-origin, is stored as a list and served",
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
      let run: Materialization;
      let liveBigQueryReads = 0;
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
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "ducklake-arrays-")),
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
            // BigQuery's dry run, answered: the model compiles in its dialect.
            // (Private on the class, so reached through a structural view.)
            spyOn(
               BigQueryConnection.prototype as unknown as {
                  getSQLBlockSchema(): Promise<unknown>;
               },
               "getSQLBlockSchema",
            ).mockImplementation(async () => BIGQUERY_SCHEMA),
            // Live BigQuery answers nothing, and counts that it was asked.
            spyOn(BigQueryConnection.prototype, "runSQL").mockImplementation(
               async () => {
                  liveBigQueryReads++;
                  return { rows: [], totalRows: 0 } as never;
               },
            ),
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
            // Each engine's passthrough read runs the build SQL in DuckDB;
            // BigQuery's quotes names with backticks.
            spyOn(
               connection,
               "federateSourceForPassthrough",
            ).mockImplementation(async (session, sourceType) => {
               await session.runSQL(
                  "CREATE OR REPLACE TEMP MACRO postgres_query(alias, q) AS TABLE SELECT * FROM query(q)",
               );
               await session.runSQL(
                  "CREATE OR REPLACE TEMP MACRO bigquery_query(project, q) AS TABLE SELECT * FROM query(replace(q, chr(96), chr(34)))",
               );
               return { handle: sourceType, sourceType };
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
               {
                  name: "bq",
                  type: "bigquery",
                  bigqueryConnection: { defaultProjectId: "proj" },
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
         // The DuckDB twin reads through the Postgres passthrough.
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
         run = await build();
      }, 180_000);

      afterAll(async () => {
         for (const spy of spies) spy.mockRestore();
         await serve?.close().catch(() => undefined);
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         fs.rmSync(root, { recursive: true, force: true });
         restore?.();
      });

      /** Start a run and wait until it is terminal and its worker is gone. */
      async function build(): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, {});
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
            rows: result.compactResult as Record<string, unknown>[],
            servedFrom: result.servedFrom ?? null,
            servedBy: (result.path?.servedBy ?? { kind: "live" }) as never,
            path: result.path,
         };
      }

      function storedSchema(source: string): { name: string; type: string }[] {
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
         return entries.find((e) => e.sourceName === source)?.schema ?? [];
      }

      it("builds every source", () => {
         expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
      });

      it.each(["s_arr", "d_arr"])(
         "stores %s's arrays as DuckDB lists of the mapped element type",
         (source) => {
            const types = Object.fromEntries(
               storedSchema(source).map((c) => [c.name, c.type]),
            );
            expect(types.v).toBe("VARCHAR[]");
            expect(types.w).toMatch(/^(INTEGER|BIGINT)\[\]$/);
            expect(types.lr).toMatch(
               /^STRUCT\(a VARCHAR, n (INTEGER|BIGINT)\)\[\]$/,
            );
         },
      );

      it("stores a v1 persisted query's selected lists and its nest", () => {
         const list = Object.fromEntries(
            storedSchema("p_list").map((c) => [c.name, c.type]),
         );
         expect(list.v).toBe("VARCHAR[]");
         expect(list.lr).toMatch(/^STRUCT\(a VARCHAR, n INTEGER\)\[\]$/);
         const nest = Object.fromEntries(
            storedSchema("p_nest").map((c) => [c.name, c.type]),
         );
         expect(nest.by_sid).toMatch(/^STRUCT\(sid INTEGER\)\[\]$/);
      });

      for (const source of ["s_arr", "d_arr"]) {
         it.each(SERVED(source))(
            "serves from the table, equal to live: %s",
            async (malloy) => {
               const live = await ask(malloy.replace(source, "ref"));
               expect(live.servedBy.kind).toBe("live");
               expect(live.rows.length).toBeGreaterThan(0);
               const before = liveBigQueryReads;
               const answer = await ask(malloy);
               expect(answer.servedFrom).toBe("storage");
               expect(liveBigQueryReads).toBe(before);
               expect(plain(answer.rows)).toEqual(plain(live.rows));
            },
         );
      }

      it.each(READS_INTEGER_LISTS("d_arr"))(
         "serves a DuckDB list of integers from the table, equal to live: %s",
         async (malloy) => {
            const live = await ask(malloy.replace("d_arr", "ref"));
            const answer = await ask(malloy);
            expect(answer.servedFrom).toBe("storage");
            expect(plain(answer.rows)).toEqual(plain(live.rows));
         },
      );

      it.each(READS_INTEGER_LISTS("s_arr"))(
         "answers a query reading a BigQuery list of INT64 live, that query only: %s",
         async (malloy) => {
            const before = liveBigQueryReads;
            const answer = await ask(malloy);
            expect(answer.servedBy.kind).toBe("live");
            expect(liveBigQueryReads).toBe(before + 1);
            // The path report names the column it left out, with its types.
            const details = (
               answer.path as { considered: { detail?: string }[] }
            ).considered
               .map((t) => t.detail ?? "")
               .join(" ");
            expect(details).toMatch(
               /column `w` \(declared `number\[\]`, stored as (INTEGER|BIGINT)\[\]\)|column `lr` \(declared `\{ a::string, n::number \}\[\]`, stored as STRUCT\(a VARCHAR, n (INTEGER|BIGINT)\)\[\]\)/,
            );
         },
      );

      it.each(PERSISTED)(
         "serves a v1 persisted query's list or nest from the table, equal to live: %s",
         async (malloy, twin) => {
            const live = await ask(twin);
            expect(live.rows.length).toBeGreaterThan(0);
            const answer = await ask(malloy);
            expect(answer.servedFrom).toBe("storage");
            expect(plain(answer.rows)).toEqual(plain(live.rows));
         },
      );

      it.each(["h_private", "h_internal"])(
         "stores %s's hidden list",
         (source) => {
            const types = Object.fromEntries(
               storedSchema(source).map((c) => [c.name, c.type]),
            );
            expect(types.v).toBe("VARCHAR[]");
         },
      );

      for (const source of ["h_private", "h_internal"]) {
         it.each(HIDDEN_LIST(source))(
            "serves a measure over a hidden list from the table, equal to live: %s",
            async (malloy) => {
               const live = await ask(malloy.replace(source, "h_ref"));
               expect(live.servedBy.kind).toBe("live");
               const answer = await ask(malloy);
               expect(answer.servedFrom).toBe("storage");
               expect(plain(answer.rows)).toEqual(plain(live.rows));
            },
         );
      }

      it.each(["h_ref", "h_private", "h_internal"])(
         "refuses a query naming %s's hidden list, live or served",
         async (source) => {
            await expect(
               ask(`run: ${source} -> { select: sid, v }`),
            ).rejects.toThrow();
         },
      );

      it("names a hidden column only as a hidden column, in the warning and the path", async () => {
         // `h_uuid`'s own view reads its private UUID, which no declaration
         // reads back as live, so the source answers live as a whole.
         const answer = await ask("run: h_uuid -> by_u");
         expect(answer.servedBy.kind).toBe("live");
         const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
            (w) => w.code === "served-copy-not-served",
         );
         expect(warnings.map((w) => [w.subject, w.message])).toEqual([
            [
               "h_uuid",
               expect.stringContaining(
                  ": a hidden column has no declaration that reads it back as live answers it.",
               ),
            ],
         ]);
         const text = JSON.stringify([warnings, answer.path]);
         expect(text).not.toContain("`u`");
         expect(text).not.toContain("column `u");
         // Nor its types, which would say what it holds.
         expect(text).not.toContain("sql native");
         expect(text).not.toContain("UUID");
      });

      it("raises no served-copy-not-served warning for a list: none answers live whole", () => {
         const warnings = pkg.getPackageMetadata().warnings ?? [];
         expect(
            warnings
               .filter((w) => w.code === "served-copy-not-served")
               .map((w) => w.subject),
         ).toEqual(["h_uuid"]);
      });
   },
);
