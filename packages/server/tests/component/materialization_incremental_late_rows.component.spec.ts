// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An incremental `#@ persist` source with a timestamp watermark and a
 * `merge_key=`, on Postgres, keeps up with rows that land late: stamped after
 * the newest row the table stores, but before the previous run started.
 *
 * The defect this pins (F-23): a delta started from the previous run's START
 * TIME, so such a row was below every later delta's range and never written,
 * while the source reported itself up to date and served an undercount. A
 * delta now starts from the newest watermark the table holds.
 *
 * Also pinned here, end to end: `lookback=` re-reads a window below that
 * boundary, and a row stamped below it (which no delta reads) is counted on
 * the run's manifest entry and raised as a package warning.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const SCHEMA = `late_rows_${Math.random().toString(36).slice(2, 8)}`;
const q = (name: string) => `${SCHEMA}.${name}`;

function modelOf(persist: string): string {
   return `##! experimental.persistence
source: ev is warehouse.table('${q("ev")}')

#@ persist name="${q("events")}" refresh="incremental" watermark="loaded_at" merge_key="id" ${persist}
source: events is ev -> { select: id, loaded_at, amt }
`;
}

type Row = Record<string, unknown>;
type Entry = {
   sourceName: string;
   physicalTableName: string;
   refresh?: string;
   ledger?: { coveredThrough: string };
   lateRows?: { count: number; from: string; to: string };
};

describe.skipIf(!POSTGRES_URL)(
   "an incremental source with a timestamp watermark and late rows",
   () => {
      let root: string;
      let envPath: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      let restore: () => void;

      async function sql(text: string): Promise<Row[]> {
         const connection = await env.getMalloyConnection("warehouse");
         return (await connection.runSQL(text)).rows as Row[];
      }

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
               expect([settled.status, settled.error]).toEqual([
                  "MANIFEST_FILE_READY",
                  null,
               ]);
               return settled;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 20));
         }
      }

      const entryOf = (m: Materialization): Entry =>
         Object.values(
            (m.manifest as { entries: Record<string, Entry> }).entries,
         ).find((e) => e.sourceName === "events")!;

      /** The served answer (from the stored table) and the live one. */
      async function servedAndLive(): Promise<{
         served: { n: number; total: number; servedFrom?: string };
         live: { n: number; total: number };
      }> {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const result = await pkg.getModel("m.malloy")!.runQuery(
            unrecordedAudit(),
            {
               query: "run: events -> { aggregate: n is count(), total is amt.sum() }",
            } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         const row = (result.compactResult as unknown as Row[])[0];
         // Malloy's Postgres connection returns the column named `row`.
         const [live] = await sql(
            `SELECT row_to_json(t) AS "row" FROM (SELECT count(*) AS n, ` +
               `sum(amt) AS total FROM ${q("ev")}) AS t`,
         );
         return {
            served: {
               n: Number(row.n),
               total: Number(row.total),
               servedFrom: result.servedFrom ?? undefined,
            },
            live: { n: Number(live.n), total: Number(live.total) },
         };
      }

      async function warnings(): Promise<{ code?: string; message: string }[]> {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         return ((await pkg.getPackageMetadata()).warnings ?? []) as {
            code?: string;
            message: string;
         }[];
      }

      async function writeModel(persist: string): Promise<void> {
         fs.writeFileSync(
            path.join(envPath, PKG, "m.malloy"),
            modelOf(persist),
         );
         await env.getPackage(parsePackageName(PKG), true);
      }

      beforeEach(async () => {
         restore = installSettings({ env: {} }).restore;
         await __setPackageLoadPoolForTests(null);
         ({ root, envPath, db, repo } = await openTempStorage("late-rows-", {
            id: ENV_ID,
            name: ENV,
         }));
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": modelOf("") },
            { manifest: { name: PKG, description: "fixture" } },
         );
         env = await Environment.create(ENV, envPath, [
            {
               name: "warehouse",
               type: "postgres",
               postgresConnection: { connectionString: POSTGRES_URL },
            } as never,
         ]);
         await sql(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
         await sql(`CREATE SCHEMA ${SCHEMA}`);
         // Stamped well before the first run, in the session's zone (UTC, as
         // Malloy's Postgres connection sets it).
         await sql(`CREATE TABLE ${q("ev")} AS SELECT * FROM (VALUES
           (1, LOCALTIMESTAMP(0) - INTERVAL '3 hours', 10.0),
           (2, LOCALTIMESTAMP(0) - INTERVAL '2 hours', 20.0)
         ) AS t(id, loaded_at, amt)`);
         await env.addPackage(PKG);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
      });

      afterEach(async () => {
         await sql(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`).catch(
            () => undefined,
         );
         await env.closeAllConnections().catch(() => undefined);
         await disposeTempStorage({ db, root });
         restore();
         await __setPackageLoadPoolForTests(null);
      });

      it("writes a row stamped between the newest stored row and the last run's start", async () => {
         const seed = await build();
         expect(entryOf(seed).refresh).toBe("full");

         // Lands after the seed, stamped before it started: above the newest
         // stored row (2 hours ago), below the seed's start time.
         await sql(`INSERT INTO ${q("ev")} VALUES
           (3, LOCALTIMESTAMP(0) - INTERVAL '1 hour', 300.0)`);
         // The boundary is whole seconds: let the next run's clock pass it.
         await new Promise((resolve) => setTimeout(resolve, 1100));

         const delta = await build();
         expect(entryOf(delta).refresh).toBe("delta");
         const { served, live } = await servedAndLive();
         expect(served.servedFrom).toBe("database");
         expect({ n: served.n, total: served.total }).toEqual(live);
         expect(live).toEqual({ n: 3, total: 330 });
         // Nothing below the boundary was missed.
         expect(entryOf(delta).lateRows).toBeUndefined();
         expect(
            (await warnings()).filter(
               (w) => w.code === "persist-incremental-late-rows",
            ),
         ).toEqual([]);
      });

      it("counts and warns of a row stamped below the newest stored row", async () => {
         await build();
         // Stamped between the two stored rows: below the delta's lower
         // boundary (the newest stored row), so no delta reads it.
         await sql(`INSERT INTO ${q("ev")} VALUES
           (4, LOCALTIMESTAMP(0) - INTERVAL '150 minutes', 400.0)`);
         await new Promise((resolve) => setTimeout(resolve, 1100));

         const delta = await build();
         expect(entryOf(delta).refresh).toBe("delta");
         expect(entryOf(delta).lateRows?.count).toBe(1);
         const late = (await warnings()).filter(
            (w) => w.code === "persist-incremental-late-rows",
         );
         expect(late.length).toBe(1);
         expect(late[0].message).toContain("1 row");
         // The served table undercounts, which is what the warning says.
         const { served, live } = await servedAndLive();
         expect(served.n).toBe(live.n - 1);
      });

      it("reads a lookback= window below the boundary, so the merge key catches the row", async () => {
         await writeModel(`lookback="1 hour"`);
         await build();
         await sql(`INSERT INTO ${q("ev")} VALUES
           (4, LOCALTIMESTAMP(0) - INTERVAL '150 minutes', 400.0)`);
         await new Promise((resolve) => setTimeout(resolve, 1100));

         const delta = await build();
         expect(entryOf(delta).refresh).toBe("delta");
         const { served, live } = await servedAndLive();
         expect({ n: served.n, total: served.total }).toEqual(live);
         expect(live).toEqual({ n: 3, total: 430 });
         expect(entryOf(delta).lateRows).toBeUndefined();
      });
   },
);
