// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-07 follow-up: a `#(materialize) storage=` source with a record (STRUCT)
 * column of its own, as a GA4-style BigQuery export has (`device`, `geo`,
 * `traffic_source`), answered every query live, whole source, with
 * `served-copy-not-served` (`column_types`): its build projected the stored
 * table to the source's atomic fields and its arrays, so it never wrote the
 * struct, and the served copy then found a declared column the table lacks.
 *
 * End to end on a real DuckLake, with the harness of
 * storage_array_columns.component.spec.ts: the build runs through the real
 * service and passthrough CTAS; `bq` is a real BigQuery connection whose
 * schema probe answers a fixed schema, its passthrough read runs the build
 * SQL in DuckDB, and live BigQuery answers no rows and counts each read.
 * `warehouse` is a DuckDB twin whose live answers are the reference.
 *
 * - A struct of scalars, a struct holding a struct, and a struct holding a
 *   list are stored as DuckDB `STRUCT(...)` of the mapped field types, and
 *   the served copy declares them: a query reading a field (selected,
 *   grouped, filtered, nested, aggregated) is served from the table, equal
 *   to live.
 * - A BigQuery struct holding an `INT64`, which live keeps exact, has no
 *   faithful declaration: it is left out per query, so only the queries
 *   that read it answer live.
 * - A hidden struct a measure reads is stored and served; a query naming it
 *   is refused, and a caller's text never names it.
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
    (1, 'acme', {'category': 'mobile', 'os': 'ios'}, {'country': 'US', 'loc': {'city': 'nyc', 'lat': 40.5::DOUBLE}}, {'tags': ['x', 'y'], 'src': 'ads'}, {'qty': 2, 'rev': 9.5::DOUBLE}),
    (2, 'acme', {'category': 'desktop', 'os': 'mac'}, {'country': 'US', 'loc': {'city': 'sf', 'lat': 37.5::DOUBLE}}, {'tags': ['y'], 'src': 'mail'}, {'qty': 1, 'rev': 3.25::DOUBLE}),
    (3, 'globex', {'category': 'mobile', 'os': 'android'}, {'country': 'FR', 'loc': {'city': 'paris', 'lat': 48.75::DOUBLE}}, {'tags': ['x', 'z', 'w'], 'src': 'ads'}, {'qty': 5, 'rev': 20.0::DOUBLE}),
    (4, 'globex', NULL, {'country': 'FR', 'loc': NULL}, {'tags': []::VARCHAR[], 'src': NULL}, {'qty': NULL, 'rev': 1.5::DOUBLE})
  ) AS t(sid, org, device, geo, params, ecom)`;

const STRING = (name: string) => ({ name, type: "STRING", mode: "NULLABLE" });

/** What BigQuery's dry run reports for {@link ROWS}: four RECORD columns. */
const BIGQUERY_SCHEMA = {
   fields: [
      { name: "sid", type: "INTEGER", mode: "NULLABLE" },
      STRING("org"),
      {
         name: "device",
         type: "RECORD",
         mode: "NULLABLE",
         fields: [STRING("category"), STRING("os")],
      },
      {
         name: "geo",
         type: "RECORD",
         mode: "NULLABLE",
         fields: [
            STRING("country"),
            {
               name: "loc",
               type: "RECORD",
               mode: "NULLABLE",
               fields: [
                  STRING("city"),
                  { name: "lat", type: "FLOAT", mode: "NULLABLE" },
               ],
            },
         ],
      },
      {
         name: "params",
         type: "RECORD",
         mode: "NULLABLE",
         fields: [
            { name: "tags", type: "STRING", mode: "REPEATED" },
            STRING("src"),
         ],
      },
      {
         name: "ecom",
         type: "RECORD",
         mode: "NULLABLE",
         fields: [
            { name: "qty", type: "INTEGER", mode: "NULLABLE" },
            { name: "rev", type: "FLOAT", mode: "NULLABLE" },
         ],
      },
   ],
};

const MODEL = `##! experimental { access_modifiers }
// The reference: live answers over the same rows.
source: ref is warehouse.sql("""${ROWS}""") extend {
  primary_key: sid
  measure: n is count()
}

#(materialize) storage="lake"
source: s_rec is bq.sql("""${ROWS}""") extend {
  primary_key: sid
  measure: n is count()
}

#(materialize) storage="lake"
source: d_rec is warehouse.sql("""${ROWS}""") extend {
  primary_key: sid
  measure: n is count()
}

// Hidden structs a measure reads: private and internal, each with its live
// twin. Each reads its own SQL, so no two share a table.
source: h_ref is warehouse.sql("""SELECT * FROM (${ROWS}) AS h_ref""") include { private: ecom; public: * } extend {
  primary_key: sid
  measure: n is count()
  measure: rev is ecom.rev.sum()
}

#(materialize) storage="lake"
source: h_private is warehouse.sql("""SELECT * FROM (${ROWS}) AS h_private""") include { private: ecom; public: * } extend {
  primary_key: sid
  measure: n is count()
  measure: rev is ecom.rev.sum()
}

#(materialize) storage="lake"
source: h_internal is warehouse.sql("""SELECT * FROM (${ROWS}) AS h_internal""") include { internal: ecom; public: * } extend {
  primary_key: sid
  measure: n is count()
  measure: rev is ecom.rev.sum()
}

// A hidden BigQuery struct holding an INT64, which no declaration reads back,
// read by the source's own measure: the source answers live as a whole.
#(materialize) storage="lake"
source: h_bq is bq.sql("""SELECT * FROM (${ROWS}) AS h_bq""") include { private: ecom; public: * } extend {
  primary_key: sid
  measure: n is count()
  measure: qty is ecom.qty.sum()
}
`;

/**
 * Queries that read a struct of strings or doubles, nested or holding a
 * list, or no struct: served from the table on either origin. Each runs on
 * `ref` for live's answer.
 */
const SERVED = (source: string) => [
   `run: ${source} -> { select: sid, device.category, device.os; order_by: sid }`,
   `run: ${source} -> { select: sid, device; order_by: sid }`,
   `run: ${source} -> { group_by: device.os; aggregate: n; order_by: os }`,
   `run: ${source} -> { where: device.category = 'mobile'; aggregate: n }`,
   `run: ${source} -> { group_by: geo.loc.city; aggregate: lat is geo.loc.lat.avg(); order_by: city }`,
   `run: ${source} -> { select: sid, geo; order_by: sid }`,
   `run: ${source} -> { where: geo.country = 'FR'; aggregate: n }`,
   `run: ${source} -> { group_by: geo.country; nest: by_city is { group_by: geo.loc.city; aggregate: n; order_by: city }; order_by: country }`,
   `run: ${source} -> { group_by: t is params.tags.each; aggregate: c is count(); order_by: t }`,
   `run: ${source} -> { select: sid, params; order_by: sid }`,
   `run: ${source} -> { group_by: params.src; aggregate: n; order_by: src }`,
   `run: ${source} -> { aggregate: n }`,
   `run: ${source} -> { group_by: org; aggregate: n; order_by: org }`,
];

/**
 * Queries that read the struct holding an integer. DuckDB's `INTEGER` is
 * exact in a JavaScript number, so `d_rec` serves them; BigQuery's `INT64`
 * is a big integer live keeps exact, which no declaration reads back, so
 * `s_rec` leaves that column out and answers just these queries live.
 */
const READS_INTEGER_STRUCT = (source: string) => [
   `run: ${source} -> { select: sid, ecom; order_by: sid }`,
   `run: ${source} -> { aggregate: q is ecom.qty.sum() }`,
   `run: ${source} -> { group_by: ecom.qty; aggregate: r is ecom.rev.sum(); order_by: qty }`,
   `run: ${source} -> { where: ecom.rev > 2; aggregate: n }`,
];

/** Queries over a source whose measure reads its hidden struct `ecom`. */
const HIDDEN_STRUCT = (source: string) => [
   `run: ${source} -> { aggregate: rev }`,
   `run: ${source} -> { group_by: org; aggregate: rev, n; order_by: org }`,
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
   "F-07: a storage= struct column, BigQuery- or DuckDB-origin, is stored as a STRUCT and served",
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
            fs.mkdtempSync(path.join(os.tmpdir(), "ducklake-structs-")),
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
            // The collation probe's catalog reads answer no collated
            // column, and are not live reads of the source.
            spyOn(BigQueryConnection.prototype, "runSQL").mockImplementation(
               async (sql: string) => {
                  if (!/INFORMATION_SCHEMA/.test(sql)) liveBigQueryReads++;
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
                  // A location, so the collation probe can read the project
                  // an `.sql()` source reads.
                  bigqueryConnection: {
                     defaultProjectId: "proj",
                     location: "US",
                  },
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

      it.each(["s_rec", "d_rec"])(
         "stores %s's structs as DuckDB STRUCTs of the mapped field types",
         (source) => {
            const types = Object.fromEntries(
               storedSchema(source).map((c) => [c.name, c.type]),
            );
            expect(types.device).toBe("STRUCT(category VARCHAR, os VARCHAR)");
            expect(types.geo).toBe(
               "STRUCT(country VARCHAR, loc STRUCT(city VARCHAR, lat DOUBLE))",
            );
            expect(types.params).toBe("STRUCT(tags VARCHAR[], src VARCHAR)");
            expect(types.ecom).toMatch(
               /^STRUCT\(qty (INTEGER|BIGINT), rev DOUBLE\)$/,
            );
         },
      );

      for (const source of ["s_rec", "d_rec"]) {
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

      it.each(READS_INTEGER_STRUCT("d_rec"))(
         "serves a DuckDB struct holding an integer from the table, equal to live: %s",
         async (malloy) => {
            const live = await ask(malloy.replace("d_rec", "ref"));
            const answer = await ask(malloy);
            expect(answer.servedFrom).toBe("storage");
            expect(plain(answer.rows)).toEqual(plain(live.rows));
         },
      );

      it.each(READS_INTEGER_STRUCT("s_rec"))(
         "answers a query reading a BigQuery struct holding an INT64 live, that query only: %s",
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
               /column `ecom` \(declared `\{ qty::number, rev::number \}`, stored as STRUCT\(qty (INTEGER|BIGINT), rev DOUBLE\)\)/,
            );
         },
      );

      it.each(["h_private", "h_internal"])(
         "stores %s's hidden struct",
         (source) => {
            const types = Object.fromEntries(
               storedSchema(source).map((c) => [c.name, c.type]),
            );
            expect(types.ecom).toBe("STRUCT(qty INTEGER, rev DOUBLE)");
         },
      );

      for (const source of ["h_private", "h_internal"]) {
         it.each(HIDDEN_STRUCT(source))(
            "serves a measure over a hidden struct from the table, equal to live: %s",
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
         "refuses a query naming %s's hidden struct, live or served",
         async (source) => {
            await expect(
               ask(`run: ${source} -> { select: sid, ecom.rev }`),
            ).rejects.toThrow();
            await expect(
               ask(`run: ${source} -> { select: sid, ecom }`),
            ).rejects.toThrow();
         },
      );

      it("names a hidden struct only as a hidden column, with no types, in the warning and the path", async () => {
         // `h_bq`'s own measure reads its private INT64 struct, which no
         // declaration reads back as live, so the source answers live whole.
         const answer = await ask("run: h_bq -> { aggregate: qty }");
         expect(answer.servedBy.kind).toBe("live");
         const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
            (w) => w.code === "served-copy-not-served",
         );
         expect(warnings.map((w) => [w.subject, w.message])).toEqual([
            [
               "h_bq",
               expect.stringContaining(
                  ": a hidden column has no declaration that reads it back as live answers it.",
               ),
            ],
         ]);
         const text = JSON.stringify([warnings, answer.path]);
         expect(text).not.toContain("`ecom`");
         expect(text).not.toContain("column `ecom");
         // Nor its types or field names, which would say what it holds.
         expect(text).not.toContain("STRUCT");
         expect(text).not.toContain("qty::");
         expect(text).not.toContain("rev::");
      });

      it("raises no served-copy-not-served warning for a struct a source does not read in its own text", () => {
         const warnings = pkg.getPackageMetadata().warnings ?? [];
         expect(
            warnings
               .filter((w) => w.code === "served-copy-not-served")
               .map((w) => w.subject),
         ).toEqual(["h_bq"]);
      });
   },
);
