// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One freshness model: `freshness.window=` and `freshness.fallback=` mean the
 * same thing on `#@ persist`, `#(materialize)` and `#(preaggregate)`, colocated
 * and `storage=`, built by a standalone run. End to end through a real
 * Environment and MaterializationService:
 *
 *  - a standalone run stamps each table's declared window, fallback and data
 *    anchor on its manifest entry, as a host stamps the manifest it binds;
 *  - a table inside its window answers; past it, `fallback="live"` answers
 *    live (the path report says `STALE`) and `fallback="stale_ok"` keeps
 *    answering from the table, with the same rows either way;
 *  - the next standalone run rebuilds a table past its window rather than
 *    carrying it forward;
 *  - a host's run stamps nothing: the host's manifest decides.
 *
 * The `storage=` tier runs against a DuckLake catalog in a local file, with
 * the seams materialization_ducklake_lifecycle.component.spec.ts fakes (the
 * catalog attach, the warehouse passthrough, the serve connection) faked the
 * same way.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyConfig } from "@malloydata/malloy";
import {
   afterAll,
   beforeAll,
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
import { recordConnectionsAsForTests } from "../../src/service/connection_type";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const LAKE = "lake";
/** Long enough for a run to finish and be queried inside it. */
const WINDOW_SECONDS = 6;

// Each staleness check waits out the window.
setDefaultTimeout(120_000);

type Fallback = "live" | "stale_ok";
type Tier = "colocated" | "storage";

/** One source per mechanism, each declaring the same window and fallback. */
function modelOf(tier: Tier, fallback: Fallback): string {
   const conn = tier === "colocated" ? "duckdb" : "warehouse";
   const place = tier === "storage" ? ` storage="${LAKE}"` : "";
   const fresh = `freshness.window="${WINDOW_SECONDS}s" freshness.fallback="${fallback}"`;
   return `##! experimental.persistence
source: raw is ${conn}.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""")

#@ persist${place} ${fresh}
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }

#(materialize)${place} ${fresh}
source: orders is raw extend {
  measure: order_total is amt.sum()
}

source: sales is raw extend {
  #(preaggregate) grain=["cat"]${place} ${fresh}
  measure: amount is amt.sum()
}
`;
}

const QUERIES = {
   daily: "run: daily -> { group_by: cat; aggregate: t is total.sum() }",
   orders: "run: orders -> { group_by: cat; aggregate: order_total }",
   sales: "run: sales -> { group_by: cat; aggregate: amount }",
} as const;
type QueryName = keyof typeof QUERIES;
const NAMES = Object.keys(QUERIES) as QueryName[];

type Entry = {
   sourceName: string;
   sourceEntityId: string;
   physicalTableName: string;
   dataAsOf?: string;
   freshnessWindowSeconds?: number;
   freshnessFallback?: string;
};

type Answer = {
   rows: Record<string, unknown>[];
   servedBy: { kind: string };
   considered: { kind: string; outcome: string; reason?: string }[];
};

/** A number however the connection returns it. */
function plain(row: Record<string, unknown>): Record<string, unknown> {
   return Object.fromEntries(
      Object.entries(row).map(([k, v]) => [
         k,
         typeof v === "bigint" || (typeof v === "string" && k !== "cat")
            ? Number(v)
            : v,
      ]),
   );
}

/** Sleep until `ms` past `isoInstant`. */
async function until(isoInstant: string, ms: number): Promise<void> {
   const wait = Date.parse(isoInstant) + ms - Date.now();
   if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
}

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by several in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "one freshness model for every mechanism and tier, on a standalone server",
   () => {
      let root: string;
      let lakeDir: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      let restore: () => void;
      let serve: MalloyDuckDBConnection | undefined;
      let opened = 0;
      const spies: { mockRestore(): void }[] = [];
      const catalogFile = () => path.join(lakeDir, "catalog.ducklake");
      const PACKAGES: { name: string; tier: Tier; fallback: Fallback }[] = [
         { name: "colocated_live", tier: "colocated", fallback: "live" },
         { name: "colocated_ok", tier: "colocated", fallback: "stale_ok" },
         { name: "storage_live", tier: "storage", fallback: "live" },
         { name: "storage_ok", tier: "storage", fallback: "stale_ok" },
      ];

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

      /** A fresh serve connection for every storage package. */
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
         for (const p of PACKAGES.filter((p) => p.tier === "storage")) {
            const pkg = await env.getPackage(parsePackageName(p.name), false);
            pkg.materialization().setServeDestinationConfig(() => config);
         }
      }

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         const storage = await openTempStorage("freshness-model-", {
            id: ENV_ID,
            name: ENV,
         });
         ({ root, db, repo } = storage);
         const { envPath } = storage;
         lakeDir = path.join(root, "lake");
         fs.mkdirSync(path.join(lakeDir, "data"), { recursive: true });
         for (const p of PACKAGES) {
            writePackageFiles(
               path.join(envPath, p.name),
               { "m.malloy": modelOf(p.tier, p.fallback) },
               { manifest: { name: p.name, description: "fixture" } },
            );
         }
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
         for (const p of PACKAGES) await env.addPackage(p.name);
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

      async function build(
         pkgName: string,
         options: Record<string, unknown> = {},
      ): Promise<Materialization> {
         const created = await service.createMaterialization(
            ENV,
            pkgName,
            options,
         );
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

      async function ask(pkgName: string, name: QueryName): Promise<Answer> {
         const pkg = await env.getPackage(parsePackageName(pkgName), false);
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query: QUERIES[name] } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            rows: (result.compactResult as Record<string, unknown>[])
               .map(plain)
               .sort((x, y) => String(x.cat).localeCompare(String(y.cat))),
            servedBy: (result.path?.servedBy ?? { kind: "live" }) as never,
            considered: (result.path?.considered ?? []) as never,
         };
      }

      function entriesOf(m: Materialization): Record<QueryName, Entry> {
         const all = Object.values(
            (m.manifest as { entries: Record<string, Entry> }).entries,
         );
         const byName = (re: RegExp) => {
            const hits = all.filter((e) => re.test(e.sourceName));
            expect(hits).toHaveLength(1);
            return hits[0];
         };
         return {
            daily: byName(/^daily$/),
            orders: byName(/^orders$/),
            sales: byName(/^sales__rollup__cat__[0-9a-f]+$/),
         };
      }

      const LIVE_ROWS: Record<QueryName, Record<string, unknown>[]> = {
         daily: [
            { cat: "a", t: 3 },
            { cat: "b", t: 4 },
         ],
         orders: [
            { cat: "a", order_total: 3 },
            { cat: "b", order_total: 4 },
         ],
         sales: [
            { cat: "a", amount: 3 },
            { cat: "b", amount: 4 },
         ],
      };

      for (const p of PACKAGES) {
         describe(`${p.tier}, fallback="${p.fallback}"`, () => {
            let first: Materialization;

            it("stamps each table's declared window, fallback and anchor on its entry", async () => {
               const before = Date.now();
               first = await build(p.name);
               expect([first.status, first.error]).toEqual([
                  "MANIFEST_FILE_READY",
                  null,
               ]);
               expect(first.metadata).toMatchObject({ sourcesBuilt: 3 });
               for (const entry of Object.values(entriesOf(first))) {
                  expect(entry).toMatchObject({
                     freshnessWindowSeconds: WINDOW_SECONDS,
                     freshnessFallback: p.fallback,
                  });
                  const anchor = Date.parse(entry.dataAsOf!);
                  expect(anchor).toBeGreaterThanOrEqual(before - 1000);
                  expect(anchor).toBeLessThanOrEqual(Date.now());
               }
            });

            it("answers from every table inside its window", async () => {
               for (const name of NAMES) {
                  const answer = await ask(p.name, name);
                  expect(answer.servedBy.kind).not.toBe("live");
                  expect(answer.rows).toEqual(LIVE_ROWS[name]);
               }
            });

            it(
               p.fallback === "live"
                  ? "answers every query live past the window, saying STALE"
                  : "keeps answering every query from its table past the window",
               async () => {
                  const anchor = Object.values(entriesOf(first))[0].dataAsOf!;
                  await until(anchor, WINDOW_SECONDS * 1000 + 500);
                  for (const name of NAMES) {
                     const answer = await ask(p.name, name);
                     expect(answer.rows).toEqual(LIVE_ROWS[name]);
                     if (p.fallback === "live") {
                        expect(answer.servedBy.kind).toBe("live");
                        expect(
                           answer.considered.map((c) => c.reason),
                        ).toContain("STALE");
                     } else {
                        expect(answer.servedBy.kind).not.toBe("live");
                     }
                  }
               },
            );

            it("rebuilds every table past its window on the next run, rather than carrying it", async () => {
               const second = await build(p.name);
               expect([second.status, second.error]).toEqual([
                  "MANIFEST_FILE_READY",
                  null,
               ]);
               expect(second.metadata).toMatchObject({
                  sourcesBuilt: 3,
                  sourcesReused: 0,
               });
               const before = entriesOf(first);
               const after = entriesOf(second);
               for (const name of NAMES) {
                  expect(Date.parse(after[name].dataAsOf!)).toBeGreaterThan(
                     Date.parse(before[name].dataAsOf!),
                  );
                  const answer = await ask(p.name, name);
                  expect(answer.servedBy.kind).not.toBe("live");
                  expect(answer.rows).toEqual(LIVE_ROWS[name]);
               }
            });

            it("carries every table inside its window forward, keeping its anchor", async () => {
               const third = await build(p.name);
               expect(third.metadata).toMatchObject({
                  sourcesBuilt: 0,
                  sourcesReused: 3,
               });
            });
         });
      }

      it("stamps nothing on a host's run: the host's manifest decides", async () => {
         const pkg = await env.getPackage(
            parsePackageName("colocated_live"),
            false,
         );
         const sources = Object.values(pkg.getBuildPlan()?.sources ?? {});
         expect(sources.length).toBe(3);
         const m = await build("colocated_live", {
            buildInstructions: sources.map((s) => ({
               sourceEntityId: s.sourceEntityId,
               materializedTableId: `host-${s.name}`,
               physicalTableName: `host_${s.name.replace(/[^A-Za-z0-9_]/g, "_")}`,
               realization: "COPY",
            })),
         });
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(m.metadata).toMatchObject({ mode: "orchestrated" });
         for (const entry of Object.values(entriesOf(m))) {
            expect(entry.dataAsOf).toBeUndefined();
            expect(entry.freshnessWindowSeconds).toBeUndefined();
            expect(entry.freshnessFallback).toBeUndefined();
         }
      });

      // Both declare: the model a window, the host its own facts on the
      // manifest it binds (`manifestLocation`). The host's facts are bound as
      // written, whichever way they disagree with the model's.
      for (const [what, hostWindow, declared, expected] of [
         [
            "the host's window has passed while the model's has not",
            3600,
            "1d",
            "live",
         ],
         [
            "the model's window has passed while the host's has not",
            86_400,
            "1h",
            "table",
         ],
      ] as const) {
         it(`binds a host's freshness as the host wrote it: ${what}`, async () => {
            const name = `host_${hostWindow}`;
            const dir = path.join(env.getEnvironmentPath(), name);
            const model = `##! experimental.persistence
#@ persist freshness.window="${declared}" freshness.fallback="live"
source: daily is duckdb.sql("""
  SELECT * FROM (VALUES ('a', 1.0), ('a', 2.0), ('b', 4.0)) AS t(cat, amt)
""") -> { group_by: cat; aggregate: total is amt.sum() }
`;
            writePackageFiles(
               dir,
               { "m.malloy": model },
               { manifest: { name, description: "fixture" } },
            );
            await env.addPackage(name);
            const built = await build(name);
            expect([built.status, built.error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            const [entry] = Object.values(
               (built.manifest as { entries: Record<string, Entry> }).entries,
            );
            const hostManifest = path.join(root, `${name}.manifest.json`);
            fs.writeFileSync(
               hostManifest,
               JSON.stringify({
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     [entry.sourceEntityId]: {
                        ...entry,
                        dataAsOf: new Date(Date.now() - 7200_000).toISOString(),
                        freshnessWindowSeconds: hostWindow,
                        freshnessFallback: "live",
                     },
                  },
               }),
            );
            writePackageFiles(
               dir,
               { "m.malloy": model },
               {
                  manifest: {
                     name,
                     description: "fixture",
                     manifestLocation: hostManifest,
                  } as never,
               },
            );
            const pkg = await env.getPackage(parsePackageName(name), true);
            const result = await pkg
               .getModel("m.malloy")!
               .runQuery(
                  unrecordedAudit(),
                  { query: QUERIES.daily } as never,
                  QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
               );
            const kind = result.path?.servedBy?.kind ?? "live";
            if (expected === "live") {
               expect(kind).toBe("live");
               expect(
                  (result.path?.considered ?? []).map((c) => c.reason),
               ).toContain("STALE");
            } else {
               expect(kind).not.toBe("live");
            }
         });
      }
   },
);
