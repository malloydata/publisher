// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A delta advances an incremental table only when the run's pre-run read held
 * that very table to its contract. The ledger alone never licenses one: it
 * outlives a run that seeded the table and then failed before it committed,
 * and a table the source left for another destination, and in neither case
 * did any read look at the table. Both reseed (`ledger_unheld`), so a column
 * retyped out of band (`numeric(10,1)` to `numeric(10,0)`, still a number)
 * never takes the delta's rows rounded. The served answer equals live's.
 *
 * - A seed whose run never committed: run 1 seeds `a` and fails at commit, so
 *   no committed entry names `a`; `a` is retyped and a row lands; run 2
 *   reseeds it.
 * - A `storage=` destination switch, A to B and back to A: A's ledger
 *   survives the move to B; back at A, the table there is retyped and a row
 *   lands, and the run reseeds it.
 * - A forced run (`forceRefresh`, as every scheduled fire sends) is held
 *   too: it carries nothing, but its incremental sources still take a delta,
 *   so it reads the tables those advance. The uncommitted seed above, and a
 *   committed table retyped out of band, both reseed under one.
 *
 * Postgres only, when `PUBLISHER_TEST_POSTGRES_URL` names one: incremental
 * refresh reads its range out of Postgres, Snowflake or BigQuery only.
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
import { logger } from "../../src/logger";
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
const PKG = "pkg";
const DAY = 86_400_000;

setDefaultTimeout(600_000);

const at = (ms: number) =>
   new Date(Date.now() - ms).toISOString().slice(0, 19).replace("T", " ");

const rows = (r: [number, number, number][]) =>
   r
      .map(
         ([id, ago, amt]) =>
            `(${id}, CAST('${at(ago)}' AS TIMESTAMP), CAST(${amt} AS NUMERIC(10,1)))`,
      )
      .join(", ");

const model = (storage: string) => `##! experimental.persistence
source: raw is warehouse.table('public.raw')

#@ persist name="a" refresh="incremental" watermark="ts" merge_key="id"${storage}
source: a is raw -> { select: id, ts, amt }

source: live_a is raw -> { select: id, ts, amt }
`;

interface Entry {
   sourceName?: string;
   physicalTableName: string;
   storageDestinationName?: string;
   refresh?: string;
   ledger?: { seededBy?: string; advancedBy?: string };
}

const entryOfA = (m: Materialization) =>
   Object.values(
      (m.manifest as { entries?: Record<string, Entry> } | null)?.entries ?? {},
   ).find((e) => e.sourceName === "a");

describe.skipIf(!POSTGRES_URL)(
   "a delta advances only a table the run held (Postgres)",
   () => {
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
      let envPath = "";
      let restore = () => {};
      let unheld: string[] = [];
      let unpatchLogger = () => {};

      async function build(
         opts: { forceRefresh?: boolean } = {},
      ): Promise<Materialization> {
         const { id } = await service.createMaterialization(ENV, PKG, opts);
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         const deadline = Date.now() + 300_000;
         for (;;) {
            const m = await repo.getMaterializationById(id);
            if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
               while (running.has(id)) {
                  await new Promise((r) => setTimeout(r, 10));
               }
               return (await repo.getMaterializationById(id)) ?? m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((r) => setTimeout(r, 20));
         }
      }

      /** Point `a` at a destination (`storage=` or none) and reload. */
      async function placeAt(storage: string): Promise<void> {
         fs.writeFileSync(path.join(envPath, PKG, "m.malloy"), model(storage));
         await env!.getPackage(parsePackageName(PKG), true);
      }

      /** Every row of `a`, served and live, and what served it. */
      async function servedAndLive(): Promise<{
         served: unknown;
         live: unknown;
         servedFrom: string | null;
      }> {
         const pkg = await env!.getPackage(parsePackageName(PKG), false);
         const ask = (source: string) =>
            pkg.getModel("m.malloy")!.runQuery(
               unrecordedAudit(),
               {
                  query: `run: ${source} -> { group_by: id, amt; order_by: id }`,
               } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         const norm = (r: unknown) =>
            (r as { id: unknown; amt: unknown }[]).map((x) => ({
               id: Number(x.id),
               amt: Number(x.amt),
            }));
         const served = await ask("a");
         const live = await ask("live_a");
         return {
            served: norm(served.compactResult),
            live: norm(live.compactResult),
            servedFrom: served.servedFrom ?? null,
         };
      }

      beforeEach(async () => {
         unheld = [];
         token = Math.random().toString(36).slice(2, 8);
         warehouse = `unheld_wh_${token}`;
         catalog = `unheld_lake_${token}`;
         restore = installSettings({
            env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "on" },
         }).restore;
         await freshLoadPool();
         ({ root, db, repo } = await openTempStorage("unheld-", {
            id: "env-1",
            name: ENV,
         }));
         admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL!,
         });
         // A binary (`C`) warehouse: a `storage=` copy orders a string as
         // live does only under a binary collation.
         await admin.runSQL(
            `CREATE DATABASE ${warehouse} LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0`,
         );
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
            `CREATE TABLE public.raw AS SELECT * FROM (VALUES ${rows([
               [1, 5 * DAY, 2.4],
               [2, 3 * DAY, 7.6],
            ])}) AS t(id, ts, amt)`,
         );
         envPath = path.join(root, "env");
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": model("") },
            { manifest: { name: PKG, description: "ledger unheld" } },
         );
         const bucket = path.join(root, "bucket");
         fs.mkdirSync(bucket);
         env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "warehouse",
                  type: "postgres",
                  postgresConnection: { ...pg, databaseName: warehouse },
               } as never,
            ],
            [
               {
                  name: "lake",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: { ...pg, databaseName: catalog },
                     },
                     storage: { bucketUrl: bucket },
                  },
               } as never,
            ],
         );
         await env.addPackage(PKG);
         service = serviceOverEnvironment(
            () => repo,
            () => env!,
         );
         const warn = logger.warn.bind(logger);
         (logger as { warn: unknown }).warn = (
            message: string,
            meta?: Record<string, unknown>,
         ) => {
            if (meta?.reasonCode === "ledger_unheld") {
               unheld.push(String(meta.sourceName));
            }
            return warn(message, meta as never);
         };
         unpatchLogger = () => {
            (logger as { warn: unknown }).warn = warn;
         };
      });

      afterEach(async () => {
         unpatchLogger();
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

      /** Retype `a`'s table in the warehouse out of band, then land a row. */
      async function retypeAndLandARow(table: string): Promise<void> {
         await wh.runSQL(
            `ALTER TABLE ${table} ALTER COLUMN amt TYPE numeric(10,0) USING amt::numeric(10,0)`,
         );
         await wh.runSQL(
            `INSERT INTO public.raw SELECT * FROM (VALUES ${rows([
               [3, 3_600_000, 4.4],
            ])}) AS t(id, ts, amt)`,
         );
      }

      it("reseeds a table whose seeding run never committed", async () => {
         // Run 1 seeds `a`, writing its ledger, and then fails at commit.
         const committer = (
            service as unknown as {
               committer: { commit: (...args: unknown[]) => Promise<unknown> };
            }
         ).committer;
         const commit = committer.commit.bind(committer);
         committer.commit = async () => {
            throw new Error("commit failed (test)");
         };
         const first = await build();
         committer.commit = commit;
         expect(first.status).toBe("FAILED");
         expect(
            (await repo.listMaterializations("env-1", PKG)).filter(
               (m) => m.status === "MANIFEST_FILE_READY",
            ),
         ).toEqual([]);

         await retypeAndLandARow("a");
         const second = await build();
         expect(second.status).toBe("MANIFEST_FILE_READY");
         const a = entryOfA(second)!;
         expect(a.refresh).toBe("full");
         expect(a.ledger?.seededBy).toBe(second.id);
         expect(unheld.length).toBe(1);

         const { served, live, servedFrom } = await servedAndLive();
         expect(servedFrom).toBe("database");
         expect(served).toEqual(live);
      });

      it("reseeds an uncommitted seed on a forced run", async () => {
         const committer = (
            service as unknown as {
               committer: { commit: (...args: unknown[]) => Promise<unknown> };
            }
         ).committer;
         const commit = committer.commit.bind(committer);
         committer.commit = async () => {
            throw new Error("commit failed (test)");
         };
         const first = await build();
         committer.commit = commit;
         expect(first.status).toBe("FAILED");

         await retypeAndLandARow("a");
         const second = await build({ forceRefresh: true });
         expect(second.status).toBe("MANIFEST_FILE_READY");
         const a = entryOfA(second)!;
         expect(a.refresh).toBe("full");
         expect(a.ledger?.seededBy).toBe(second.id);
         expect(unheld.length).toBe(1);

         const { served, live, servedFrom } = await servedAndLive();
         expect(servedFrom).toBe("database");
         expect(served).toEqual(live);
      });

      it("reseeds a committed table retyped out of band on a forced run", async () => {
         const first = await build();
         expect(first.status).toBe("MANIFEST_FILE_READY");
         expect(entryOfA(first)?.ledger?.seededBy).toBe(first.id);

         await retypeAndLandARow("a");
         const second = await build({ forceRefresh: true });
         expect(second.status).toBe("MANIFEST_FILE_READY");
         const a = entryOfA(second)!;
         expect(a.refresh).toBe("full");
         expect(a.ledger?.seededBy).toBe(second.id);

         const { served, live, servedFrom } = await servedAndLive();
         expect(servedFrom).toBe("database");
         expect(served).toEqual(live);
      });

      it("reseeds a table the source left for another destination and came back to", async () => {
         const first = await build();
         expect(entryOfA(first)?.storageDestinationName).toBeUndefined();

         await placeAt(' storage="lake"');
         const second = await build();
         expect(second.status).toBe("MANIFEST_FILE_READY");
         expect(entryOfA(second)?.storageDestinationName).toBe("lake");

         await placeAt("");
         // The warehouse table A left behind, if the move reclaimed it, is
         // put back as it stood: the ledger at A still describes it.
         const exists = await wh.runSQL(
            `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'a'`,
         );
         if (exists.rows.length === 0) {
            await wh.runSQL(
               `CREATE TABLE public.a AS SELECT id, ts, amt FROM public.raw`,
            );
         }
         await retypeAndLandARow("a");
         const third = await build();
         expect(third.status).toBe("MANIFEST_FILE_READY");
         const a = entryOfA(third)!;
         expect(a.storageDestinationName).toBeUndefined();
         expect(a.refresh).toBe("full");
         expect(a.ledger?.seededBy).toBe(third.id);
         expect(unheld.length).toBe(1);

         const { served, live, servedFrom } = await servedAndLive();
         expect(servedFrom).toBe("database");
         expect(served).toEqual(live);
      });
   },
);
