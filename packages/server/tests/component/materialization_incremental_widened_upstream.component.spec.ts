// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An incremental `#@ persist` source whose upstream column is widened after
 * its table was built, on Postgres, in both tiers: its table in the warehouse
 * (database) and in a DuckLake destination (`storage=`).
 *
 * Malloy reads an INTEGER and a BIGINT, a REAL and a DOUBLE PRECISION, all
 * as `number`, so nothing the plan sees moves. The next delta wrote the wider
 * values into the narrower stored column: a BIGINT past 2^31 failed the run
 * on every refresh, and a DOUBLE was rounded into the REAL with no error, a
 * wrong answer. The planner now compares each delta column's type with the
 * stored column's and rebuilds the table in full (`shape_mismatch`) when the
 * stored one does not hold it; an unchanged type still deltas.
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
const stamped = (rows: [string, number, string][]) =>
   rows
      .map(
         ([id, ago, amt]) =>
            `(${id}, LOCALTIMESTAMP(0) - INTERVAL '${ago} minutes', ${amt})`,
      )
      .join(", ");

describe.skipIf(!POSTGRES_URL)(
   "an incremental source whose upstream column is widened",
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

#@ persist name="events" refresh="incremental" watermark="loaded_at"${tier.storage}
source: events is ev -> { select: id, loaded_at, amt }

source: live_events is ev -> { select: id, loaded_at, amt }
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

            const refreshOf = (m: Materialization): string | undefined =>
               Object.values(
                  (m.manifest as { entries: Record<string, Entry> }).entries,
               ).find((e) => e.sourceName === "events")?.refresh;

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

            /** Insert rows, then let the clock pass a whole second. */
            async function insert(
               rows: [string, number, string][],
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
               warehouse = `wid_wh_${token}`;
               catalog = `wid_lake_${token}`;
               restore = installSettings({
                  env: {
                     PUBLISHER_MATERIALIZE: "true",
                     PERSIST_STORAGE_MODE: "on",
                  },
               }).restore;
               await freshLoadPool();
               ({ root, db, repo } = await openTempStorage("widened-", {
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
                  `CREATE TABLE ev (id integer, loaded_at timestamp, amt real)`,
               );
               await wh.runSQL(
                  `INSERT INTO ev VALUES ${stamped([
                     ["1", 180, "1.5"],
                     ["2", 170, "2.5"],
                  ])}`,
               );

               const envPath = path.join(root, "env");
               writePackageFiles(
                  path.join(envPath, PKG),
                  { "m.malloy": model() },
                  { manifest: { name: PKG, description: "widened upstream" } },
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

            it("rebuilds after an INTEGER column is widened to BIGINT, equal to live", async () => {
               await wh.runSQL(`ALTER TABLE ev ALTER COLUMN id TYPE bigint`);
               // Past 2^31: an INTEGER column cannot hold it.
               await insert([["3000000000", 60, "3.5"]]);
               expect(refreshOf(await build())).toBe("full");
               await expectEqualsLive();
               // The table is now BIGINT: the next refresh deltas.
               await insert([["3000000001", 30, "4.5"]]);
               expect(refreshOf(await build())).toBe("delta");
               await expectEqualsLive();
            });

            it("rebuilds after a REAL column is widened to DOUBLE PRECISION, equal to live", async () => {
               await wh.runSQL(
                  `ALTER TABLE ev ALTER COLUMN amt TYPE double precision`,
               );
               // A REAL rounds it: 0.123456789 is stored as 0.12345679.
               await insert([["3", 60, "0.123456789"]]);
               expect(refreshOf(await build())).toBe("full");
               await expectEqualsLive();
            });

            it("still deltas when no type moved", async () => {
               await insert([["3", 60, "3.5"]]);
               expect(refreshOf(await build())).toBe("delta");
               await expectEqualsLive();
            });
         });
      }
   },
);
