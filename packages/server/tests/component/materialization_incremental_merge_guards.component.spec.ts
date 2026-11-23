// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An incremental `#@ persist` source with a `merge_key=` whose delta reads
 * the same key on two rows of the source, on Postgres, in both tiers: its
 * table in the warehouse (database) and in a DuckLake destination
 * (`storage=`, whose merge runs in DuckDB).
 *
 * A delta is the source's own rows in a range, so a key it repeats is one the
 * source repeats, and live returns both rows. A merge cannot store both:
 * Postgres refused the delta ("MERGE command cannot affect row a second
 * time") on every run, and DuckDB merged it silently, keeping one row or
 * inserting both by whether the key was stored. The delta now fails its key
 * guard, rolls back, and the source is rebuilt in full, which stores live's
 * rows as they are: reason `merge_key_not_unique`, a package warning naming
 * the source, cleared once a delta of it commits.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../src/test_helpers/temp_storage";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const WARNING = "persist-incremental-merge-key-not-unique";

setDefaultTimeout(600_000);

const TIERS = [
   { label: "database", storage: "", servedFrom: "database" },
   {
      label: "storage= (DuckLake)",
      storage: ' storage="lake"',
      servedFrom: "storage",
   },
];

type Entry = { sourceName: string; refresh?: string };

/** Rows `ago` minutes before now, in the session's zone (UTC). */
const stamped = (rows: [number | null, number, number][]) =>
   rows
      .map(
         ([id, ago, amt]) =>
            `(${id ?? "NULL"}, LOCALTIMESTAMP(0) - INTERVAL '${ago} minutes', ${amt})`,
      )
      .join(", ");

describe.skipIf(!POSTGRES_URL)(
   "an incremental merge whose delta repeats a key",
   () => {
      for (const tier of TIERS) {
         describe(`[${tier.label}]`, () => {
            let token = "";
            let warehouse = "";
            let catalog = "";
            let admin: PostgresConnection;
            let wh: PostgresConnection;
            let root: string;
            let db: DuckDBConnection;
            let repo: ResourceRepository;
            let env: Environment | undefined;
            let service: MaterializationService;
            let restore = () => {};

            const model = () => `##! experimental.persistence
source: ev is pg.table('public.ev')

#@ persist name="events" refresh="incremental" watermark="loaded_at" merge_key="id"${tier.storage}
source: events is ev -> { select: id, loaded_at, amt }

source: live_events is ev -> { select: id, loaded_at, amt }

source: evj is pg.table('public.evj')

#@ persist name="events_json" refresh="incremental" watermark="loaded_at" merge_key="payload"${tier.storage}
source: events_json is evj -> { select: payload, loaded_at, amt }
`;

            async function build(
               options: { reseed?: boolean } = {},
            ): Promise<Materialization> {
               const created = await service.createMaterialization(
                  ENV,
                  PKG,
                  options as never,
               );
               const running = (
                  service as unknown as {
                     runningAbortControllers: Map<string, unknown>;
                  }
               ).runningAbortControllers;
               const deadline = Date.now() + 300_000;
               for (;;) {
                  const m = await repo.getMaterializationById(created.id);
                  if (
                     m &&
                     ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(
                        m.status,
                     )
                  ) {
                     while (running.has(created.id)) {
                        await new Promise((resolve) => setTimeout(resolve, 10));
                     }
                     const settled = (await repo.getMaterializationById(
                        created.id,
                     ))!;
                     expect([settled.status, settled.error]).toEqual([
                        "MANIFEST_FILE_READY",
                        null,
                     ]);
                     const manifest = settled.manifest as {
                        failures?: unknown;
                     };
                     expect(manifest.failures ?? {}).toEqual({});
                     return settled;
                  }
                  if (Date.now() > deadline)
                     throw new Error("run never settled");
                  await new Promise((resolve) => setTimeout(resolve, 50));
               }
            }

            const refreshOf = (
               m: Materialization,
               source = "events",
            ): string | undefined =>
               Object.values(
                  (m.manifest as { entries: Record<string, Entry> }).entries,
               ).find((e) => e.sourceName === source)?.refresh;

            /** Every row, served from the table and live, and what served it. */
            async function servedAndLive(): Promise<{
               served: string;
               live: string;
               servedFrom: string | null;
            }> {
               const pkg = await env!.getPackage(parsePackageName(PKG), false);
               const ask = (source: string) =>
                  pkg.getModel("m.malloy")!.runQuery(
                     unrecordedAudit(),
                     {
                        query:
                           `run: ${source} -> { select: id, amt, ` +
                           `t is loaded_at::string; order_by: id, t, amt }`,
                     } as never,
                     QueryPrivileges.of({
                        includeHiddenFilesAndSources: true,
                     }),
                  );
               const served = await ask("events");
               const live = await ask("live_events");
               return {
                  served: JSON.stringify(served.compactResult),
                  live: JSON.stringify(live.compactResult),
                  servedFrom: served.servedFrom ?? null,
               };
            }

            async function warned(
               code = WARNING,
            ): Promise<{ code?: string; subject?: string; message: string }[]> {
               const pkg = await env!.getPackage(parsePackageName(PKG), false);
               return (
                  ((await pkg.getPackageMetadata()).warnings ?? []) as {
                     code?: string;
                     subject?: string;
                     message: string;
                  }[]
               ).filter((w) => w.code === code);
            }

            /** Insert rows, then let the clock pass a whole second. */
            async function insert(
               rows: [number | null, number, number][],
            ): Promise<void> {
               await wh.runSQL(`INSERT INTO ev VALUES ${stamped(rows)}`);
               await new Promise((resolve) => setTimeout(resolve, 1100));
            }

            async function expectEqualsLive(): Promise<void> {
               const { served, live, servedFrom } = await servedAndLive();
               expect(servedFrom).toBe(tier.servedFrom);
               expect(served).toBe(live);
            }

            beforeEach(async () => {
               token = Math.random().toString(36).slice(2, 8);
               warehouse = `mkg_wh_${token}`;
               catalog = `mkg_lake_${token}`;
               restore = installSettings({
                  env: {
                     PUBLISHER_MATERIALIZE: "true",
                     PERSIST_STORAGE_MODE: "on",
                  },
               }).restore;
               await freshLoadPool();
               ({ root, db, repo } = await openTempStorage("merge-guard-", {
                  id: ENV_ID,
                  name: ENV,
               }));
               admin = new PostgresConnection({
                  name: "admin",
                  connectionString: POSTGRES_URL!,
               });
               // A binary (`C`) warehouse: a `storage=` copy orders a string
               // as live does only under a binary collation.
               await admin.runSQL(
                  `CREATE DATABASE ${warehouse} LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0`,
               );
               if (tier.storage)
                  await admin.runSQL(`CREATE DATABASE ${catalog}`);
               const parsed = new URL(POSTGRES_URL!);
               const pg = {
                  host: parsed.hostname,
                  port: Number(parsed.port || 5432),
                  userName: decodeURIComponent(parsed.username),
                  password: decodeURIComponent(parsed.password),
               };
               wh = new PostgresConnection({
                  name: "wh",
                  connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
               });
               await wh.runSQL(
                  `CREATE TABLE ev (id integer, loaded_at timestamp, amt integer)`,
               );
               // A json merge key: Postgres has no equality for json, so the
               // repeated-key check after a rebuild cannot run there.
               await wh.runSQL(
                  `CREATE TABLE evj (payload json, loaded_at timestamp, amt integer)`,
               );
               await wh.runSQL(
                  `INSERT INTO evj VALUES ('{"a":1}', LOCALTIMESTAMP(0) - INTERVAL '90 minutes', 1)`,
               );
               await wh.runSQL(
                  `INSERT INTO ev VALUES ${stamped([
                     [1, 180, 10],
                     [2, 170, 20],
                  ])}`,
               );

               const envPath = path.join(root, "env");
               writePackageFiles(
                  path.join(envPath, PKG),
                  { "m.malloy": model() },
                  { manifest: { name: PKG, description: "merge guard" } },
               );
               const bucket = path.join(root, "bucket");
               fs.mkdirSync(bucket);
               env = await Environment.create(
                  ENV,
                  envPath,
                  [
                     {
                        name: "pg",
                        type: "postgres",
                        postgresConnection: { ...pg, databaseName: warehouse },
                     } as never,
                  ],
                  tier.storage
                     ? [
                          {
                             name: "lake",
                             type: "ducklake",
                             ducklakeConnection: {
                                catalog: {
                                   postgresConnection: {
                                      ...pg,
                                      databaseName: catalog,
                                   },
                                },
                                storage: { bucketUrl: bucket },
                             },
                          } as never,
                       ]
                     : [],
               );
               await env.addPackage(PKG);
               service = serviceOverEnvironment(
                  () => repo,
                  () => env!,
               );
               expect(refreshOf(await build())).toBe("full");
            });

            afterEach(async () => {
               await env?.closeAllConnections().catch(() => undefined);
               await wh?.close().catch(() => undefined);
               if (db) await disposeTempStorage({ db, root });
               for (const name of [warehouse, catalog]) {
                  await admin
                     ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
                     .catch(() => undefined);
               }
               await admin?.close().catch(() => undefined);
               restore();
               await freshLoadPool();
            });

            it("rebuilds while the source repeats a key, equal to live, and deltas again once it does not", async () => {
               // Key 3, new, at two watermarks; key 1, stored, restated twice.
               await insert([
                  [3, 60, 30],
                  [3, 30, 31],
                  [1, 50, 11],
                  [1, 40, 12],
               ]);
               expect(refreshOf(await build())).toBe("full");
               await expectEqualsLive();
               const [w, ...rest] = await warned();
               expect(rest).toEqual([]);
               expect(w.subject).toBe("events");
               expect(w.message).toContain('merge_key="id" is not unique');

               // The table now holds key 3 twice, as live does. A delta
               // reading it once would rewrite both stored rows as that one,
               // so no boundary was recorded: the next run rebuilds again.
               await insert([[4, 10, 40]]);
               expect(refreshOf(await build())).toBe("full");
               await expectEqualsLive();
               expect((await warned()).length).toBe(1);

               // The repeats removed upstream: the rebuild finds the key
               // unique, records its boundary, and the warning clears.
               await wh.runSQL(
                  `DELETE FROM ev WHERE (id, amt) IN ((3, 30), (1, 10), (1, 11))`,
               );
               expect(refreshOf(await build())).toBe("full");
               await expectEqualsLive();
               expect(await warned()).toEqual([]);

               await insert([[7, 5, 70]]);
               expect(refreshOf(await build())).toBe("delta");
               await expectEqualsLive();
               expect(await warned()).toEqual([]);
            });

            it("rebuilds a first seed that repeats a key on every run, equal to live", async () => {
               // Repeated from the start: the seed itself records no boundary.
               await wh.runSQL(
                  `INSERT INTO ev VALUES ${stamped([[2, 160, 21]])}`,
               );
               expect(refreshOf(await build({ reseed: true }))).toBe("full");
               expect((await warned()).length).toBe(1);
               await insert([[2, 10, 22]]);
               expect(refreshOf(await build())).toBe("full");
               await expectEqualsLive();
            });

            it("warns, naming the source and the failure, when the repeated-key check cannot run", async () => {
               const failed = (
                  await warned("persist-incremental-check-failed")
               ).filter((w) => w.subject === "events_json");
               if (tier.storage) {
                  // DuckDB groups json: the check runs, and the source deltas.
                  expect(failed).toEqual([]);
                  await insert([[8, 5, 80]]);
                  expect(refreshOf(await build(), "events_json")).toBe("delta");
                  return;
               }
               expect(failed.length).toBe(1);
               expect(failed[0].message).toContain("42883");
               // No boundary was recorded: every refresh rebuilds, still warned.
               expect(refreshOf(await build(), "events_json")).toBe("full");
               expect(
                  (await warned("persist-incremental-check-failed")).length,
               ).toBe(1);
            });

            it("rebuilds a delta that repeats a key at one watermark (a tie), equal to live", async () => {
               await insert([
                  [5, 20, 50],
                  [5, 20, 51],
               ]);
               expect(refreshOf(await build())).toBe("full");
               await expectEqualsLive();
               expect((await warned()).length).toBe(1);
            });

            it("rebuilds a delta that repeats a NULL key, as the merge matches NULL to NULL", async () => {
               await insert([
                  [null, 25, 60],
                  [null, 15, 61],
               ]);
               expect(refreshOf(await build())).toBe("full");
               await expectEqualsLive();
               expect((await warned()).length).toBe(1);
            });

            it("merges a NULL key beside distinct keys as a delta", async () => {
               await insert([
                  [null, 25, 60],
                  [6, 15, 61],
               ]);
               expect(refreshOf(await build())).toBe("delta");
               await expectEqualsLive();
               expect(await warned()).toEqual([]);
            });
         });
      }
   },
);
