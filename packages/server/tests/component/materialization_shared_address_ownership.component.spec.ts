// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One table per name, on a DuckLake `storage=` destination, end to end
 * through a real Environment and MaterializationService.
 *
 * `owned` and `plain` compile to the same build SQL, but `owned` names its
 * table (`name=`) and `plain` does not, so each is addressed apart
 * (foldDistinctTableNames) and builds its own table. When another package's
 * table holds `owned`'s name, the ownership check refuses `owned` alone, and
 * `plain` never writes that table. `solo` is a table of its own.
 *
 * Two gated sources of one build with different `name=`s build two tables,
 * each served to its own callers as live answers them; and two sources of
 * one build that store different columns (`private:` keeps `status`,
 * `except:` drops it) each store their own, in either declaration order.
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
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { recordConnectionsAsForTests } from "../../src/service/connection_type";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const LAKE = "lake";

/** The mark another package's build leaves on its table. */
const OTHER_MARK = ownedTableComment("other/pkg2");
const OWN_MARK = ownedTableComment(`${ENV}/${PKG}`);

const MODEL = `##! experimental { persistence givens access_modifiers }
given: ROLE :: string
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")

#(materialize) storage="lake" name="shared_t"
source: owned is raw extend { measure: total is amt.sum() }

#(materialize) storage="lake"
source: plain is raw extend { measure: total is amt.sum() }

#(materialize) storage="lake"
source: solo is raw extend { where: cat = 'a' }

source: orders_raw is warehouse.sql("""
  SELECT * FROM (VALUES (1, 'east', 'open'), (2, 'west', 'closed')) AS t(id, region, status)
""")

#(authorize) true
#(materialize) storage="lake" name="open_t"
source: open_orders is orders_raw -> { select: id, region }

#(authorize) $ROLE = 'admin'
#(materialize) storage="lake" name="admin_t"
source: admin_orders is orders_raw -> { select: id, region }

#@ persist storage="lake"
source: b_hides is orders_raw -> { select: id, region, status } include { public: id, region; private: status }

#@ persist storage="lake"
source: a_drops is orders_raw -> { select: id, region, status } extend { except: status }

#@ persist storage="lake"
source: a_drops2 is orders_raw -> { where: id > 0; select: id, region, status } extend { except: status }

#@ persist storage="lake"
source: b_hides2 is orders_raw -> { where: id > 0; select: id, region, status } include { public: id, region; private: status }
`;

/** The suite's settings. */
const SUITE_ENV = {
   PERSIST_STORAGE_MODE: "on",
   PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
   PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
};

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
   "one table per name= and per stored projection on a DuckLake destination",
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
               ...SUITE_ENV,
            },
         }).restore;
         // The package-load worker plans under the settings it was spawned
         // with: one spawned now plans the gated sources as the run does.
         await freshLoadPool();
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
         await freshLoadPool();
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
         return askQuery(QUERY(source));
      }

      async function askQuery(
         query: string,
         givens?: Record<string, string>,
      ): Promise<{ rows: unknown[]; servedFrom: string | null }> {
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query, ...(givens ? { givens } : {}) } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         const rows = (result.compactResult as Record<string, unknown>[])
            .map((r) =>
               Object.fromEntries(
                  Object.entries(r).map(([k, v]) => [k, asNumber(v)]),
               ),
            )
            .sort((x, y) =>
               String(x.cat ?? x.id).localeCompare(String(y.cat ?? y.id)),
            );
         return { rows, servedFrom: result.servedFrom ?? null };
      }

      /** What `query` answers for `givens`, or the error it is refused with. */
      async function answerOf(
         query: string,
         givens?: Record<string, string>,
      ): Promise<{ rows: unknown[] } | { error: string }> {
         try {
            return { rows: (await askQuery(query, givens)).rows };
         } catch (error) {
            return { error: (error as Error).message };
         }
      }

      /** The table the run built for `source`. */
      function tableOf(m: Materialization, source: string): string {
         const entries = Object.values(
            (m.manifest as { entries: Record<string, Entry> }).entries,
         ).filter((e) => e.sourceName === source && e.physicalTableName);
         expect(entries).toHaveLength(1);
         return entries[0].physicalTableName!;
      }

      /** Each gated or projected case, as every caller asks it. */
      type Case = [string, Record<string, string> | undefined];
      const CASES: Case[] = [
         ...["open_orders", "admin_orders"].flatMap((source) =>
            [{ ROLE: "admin" }, { ROLE: "viewer" }].map(
               (givens): Case => [
                  `run: ${source} -> { select: id, region }`,
                  givens,
               ],
            ),
         ),
         ...["b_hides", "a_drops", "a_drops2", "b_hides2"].map(
            (source): Case => [
               `run: ${source} -> { select: id, region }`,
               undefined,
            ],
         ),
      ];
      const live = new Map<string, unknown>();
      let firstRun: Materialization;

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

      it("refuses the name= source alone, leaving the other package's table and mark untouched, and builds its same-build sibling a table of its own", async () => {
         for (const source of ["owned", "plain"]) {
            expect(await ask(source)).toEqual({ rows: LIVE, servedFrom: null });
         }
         for (const [query, givens] of CASES) {
            live.set(
               JSON.stringify([query, givens]),
               await answerOf(query, givens),
            );
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
         firstRun = m;
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const { built, failures } = outcome(m);
         // Everything but `owned`: `plain` names no table of its own, so it
         // is not on `owned`'s address (foldDistinctTableNames).
         expect(built).toEqual([
            "a_drops",
            "a_drops2",
            "admin_orders",
            "b_hides",
            "b_hides2",
            "open_orders",
            "plain",
            "solo",
         ]);
         expect(failures).toHaveLength(1);
         expect(failures[0].sourceName).toBe("owned");
         expect(failures[0].reason).toContain(
            "is not built (table-owned-elsewhere): its name= 'shared_t' in storage destination 'lake' is held by a table package 'other/pkg2' built",
         );

         // The other package's table: its rows, files and mark, as they were.
         expect((await catalog()).shared_t).toEqual(before);

         // The refused source answers live; its sibling from its own table.
         expect(await ask("owned")).toEqual({ rows: LIVE, servedFrom: null });
         expect(tableOf(m, "plain")).not.toBe("shared_t");
         expect(await ask("plain")).toEqual({
            rows: LIVE,
            servedFrom: "storage",
         });
         expect((await ask("solo")).servedFrom).toBe("storage");
      });

      it("builds two gated sources of one build a table each, under each name=, each serving its own callers as live", async () => {
         const tables = await catalog();
         expect(tableOf(firstRun, "open_orders")).toBe("open_t");
         expect(tableOf(firstRun, "admin_orders")).toBe("admin_t");
         for (const table of ["open_t", "admin_t"]) {
            expect(tables[table]?.rows).toEqual([
               [1, "east"],
               [2, "west"],
            ]);
         }
         for (const [query, givens] of CASES.slice(0, 4)) {
            const served = await answerOf(query, givens);
            expect(served).toEqual(
               live.get(JSON.stringify([query, givens])) as never,
            );
            if ("rows" in served) {
               expect((await askQuery(query, givens)).servedFrom).toBe(
                  "storage",
               );
            }
         }
         // The gate still decides: a viewer is refused admin_orders, served
         // or live.
         expect(await answerOf(CASES[3][0], CASES[3][1])).toMatchObject({
            error: expect.any(String),
         });
      });

      it("stores each projection of one build in its own table, in either order: a column dropped with except: is never at rest", async () => {
         const tables = await catalog();
         const names = ["b_hides", "a_drops", "a_drops2", "b_hides2"].map(
            (source) => tableOf(firstRun, source),
         );
         expect(new Set(names).size).toBe(4);
         const columns = await inspect(async (c) =>
            Object.fromEntries(
               await Promise.all(
                  names.map(async (table) => [
                     table,
                     (
                        await c.runSQL(
                           `SELECT column_name FROM duckdb_columns() WHERE database_name = '${LAKE}' AND table_name = '${table}' ORDER BY column_index`,
                        )
                     ).rows.map((r) => String(r.column_name)),
                  ]),
               ),
            ),
         );
         const [bHides, aDrops, aDrops2, bHides2] = names;
         expect(columns[aDrops]).toEqual(["id", "region"]);
         expect(columns[aDrops2]).toEqual(["id", "region"]);
         expect(columns[bHides]).toContain("status");
         expect(columns[bHides2]).toContain("status");
         expect(Object.keys(tables)).toEqual(expect.arrayContaining(names));
         // Each answers as live. The `except:` sources are read from their own
         // tables; a v1 `storage=` source with a `private:` column is not yet
         // re-rooted by the served copy on this branch (#1788), so it answers
         // live from beside a table that is its own either way.
         for (const [query, givens] of CASES.slice(4)) {
            const served = await askQuery(query, givens);
            expect(served.rows).toEqual(
               (
                  live.get(JSON.stringify([query, givens])) as {
                     rows: unknown[];
                  }
               ).rows,
            );
            if (query.includes("a_drops")) {
               expect(served.servedFrom).toBe("storage");
            }
         }
      });

      it("once the name is free, builds owned's table, marks it as this package's, and serves it, while plain keeps its own", async () => {
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
         expect(tableOf(m, "owned")).toBe("shared_t");
         expect(tableOf(m, "plain")).toBe(tableOf(firstRun, "plain"));
      });

      /** Reload the package with `extra` declared after the model. */
      async function reload(extra: string): Promise<void> {
         fs.writeFileSync(
            path.join(root, "env", PKG, "m.malloy"),
            `${MODEL}\n${extra}\n`,
         );
         pkg = await env.getPackage(parsePackageName(PKG), true);
         await reopenServe();
      }

      /** A table's columns in the lake, in order. */
      function columnsOf(table: string): Promise<string[]> {
         return inspect(async (c) =>
            (
               await c.runSQL(
                  `SELECT column_name FROM duckdb_columns() WHERE database_name = '${LAKE}' AND table_name = '${table}' ORDER BY column_index`,
               )
            ).rows.map((r) => String(r.column_name)),
         );
      }

      /** The manifest's entries on `table`, by source name. */
      function entriesOn(m: Materialization, table: string): string[] {
         return Object.values(
            (m.manifest as { entries: Record<string, Entry> }).entries,
         )
            .filter((e) => e.physicalTableName === table)
            .map((e) => e.sourceName)
            .sort();
      }

      /**
       * A source that drops `status` (`except:`) and one that keeps it
       * (`private:`) on one build and one `name=`: two definitions of one
       * table, the author's collision. Enforced (the default), the package
       * does not load. Under the opt-out, neither is built once both are
       * declared, though the first is unchanged and would be carried: the
       * table keeps what was written before, no manifest puts both on it,
       * and both answer live.
       */
      for (const [label, first, second, where] of [
         ["the dropping source first", "drops", "hides", "id > 0"],
         ["the keeping source first", "hides", "drops", "id > -1"],
      ] as const) {
         it(`never writes a second projection into one name= when the second source arrives later: ${label}`, async () => {
            const table = `proj_${first}_t`;
            const query = `orders_raw -> { where: ${where}; select: id, region, status }`;
            const declare = (kind: "drops" | "hides") =>
               kind === "drops"
                  ? `#@ persist name="${table}" storage="lake"\nsource: ${table}_drops is ${query} extend { except: status }`
                  : `#@ persist name="${table}" storage="lake"\nsource: ${table}_hides is ${query} include { public: id, region; private: status }`;
            // Phase 0: the first, beside an unnamed sibling that keeps every
            // column, so the first is on an address of its own already.
            const sibling = `#@ persist storage="lake"\nsource: ${table}_all is ${query}`;
            await reload([sibling, declare(first)].join("\n\n"));
            const before = await run();
            expect([before.status, before.error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            expect(entriesOn(before, table)).toEqual([`${table}_${first}`]);
            const columns = await columnsOf(table);
            expect(columns.includes("status")).toBe(first === "hides");

            // Phase 1: the second arrives; the first is unchanged. Enforced,
            // the load fails, naming the collision last on its line.
            // An unrelated source arrives with it: refused collisions are
            // refused per source, so it still builds in that run.
            const unrelated = `#@ persist storage="lake"\nsource: ${table}_new is orders_raw -> { where: id > 1; select: id, region }`;
            const both = [
               sibling,
               declare(first),
               declare(second),
               unrelated,
            ].join("\n\n");
            const refusal = await reload(both).then(
               () => undefined,
               (err: unknown) => (err as Error).message,
            );
            expect(refusal).toContain(
               "the package does not load until each has a distinct #@ persist name=",
            );
            expect(refusal?.trimEnd()).toEndWith(
               "[materialization-table-collision]",
            );
            // The opt-out loads it, each colliding source refused alone.
            const optOut = installSettings({
               env: { ...SUITE_ENV, PERSIST_COLLISION_ENFORCE: "false" },
            }).restore;
            let after: Materialization;
            try {
               await freshLoadPool();
               await reload(both);
               after = await run();
            } finally {
               optOut();
               await freshLoadPool();
            }
            expect([after.status, after.error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            expect(entriesOn(after, table)).toEqual([]);
            expect(outcome(after).built).toContain(`${table}_new`);
            expect(await columnsOf(table)).toEqual(columns);
            for (const kind of ["drops", "hides"]) {
               expect(
                  (
                     await askQuery(
                        `run: ${table}_${kind} -> { select: id, region }`,
                     )
                  ).servedFrom,
               ).toBeNull();
            }
            const refused = Object.values(
               (
                  after.metadata as {
                     refusedSources?: Record<
                        string,
                        { name: string; reason: string }
                     >;
                  }
               ).refusedSources ?? {},
            )
               .filter((r) => r.name.startsWith(table))
               .map((r) => `${r.name}:${r.reason}`)
               .sort();
            expect(refused).toEqual([
               `${table}_drops:table_name_collision`,
               `${table}_hides:table_name_collision`,
            ]);
         });
      }
   },
);
