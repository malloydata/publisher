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
import {
   countLateRows,
   probeStoredWatermark,
   warehouseDeltaTarget,
} from "../../src/service/materialization/run/incremental_apply";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const SCHEMA = `late_rows_${Math.random().toString(36).slice(2, 8)}`;
const q = (name: string) => `${SCHEMA}.${name}`;

function modelOf(persist: string, mergeKey = 'merge_key="id"'): string {
   return `##! experimental.persistence
source: ev is warehouse.table('${q("ev")}')

#@ persist name="${q("events")}" refresh="incremental" watermark="loaded_at" ${mergeKey} ${persist}
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

      async function build(
         options: { reseed?: boolean } = {},
      ): Promise<Materialization> {
         const created = await service.createMaterialization(
            ENV,
            PKG,
            options as never,
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

      async function writeModel(
         persist: string,
         mergeKey?: string,
      ): Promise<void> {
         fs.writeFileSync(
            path.join(envPath, PKG, "m.malloy"),
            modelOf(persist, mergeKey),
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

         // Delta 3, with row 4 still missing: the window has slid up past
         // it, but what delta 2 found is carried, so the warning stays.
         await new Promise((resolve) => setTimeout(resolve, 1100));
         const again = await build();
         expect(entryOf(again).refresh).toBe("delta");
         expect(entryOf(again).lateRows).toEqual(entryOf(delta).lateRows);
         expect(
            (await warnings()).filter(
               (w) => w.code === "persist-incremental-late-rows",
            ).length,
         ).toBe(1);

         // A rebuild recovers the row and clears both.
         const rebuilt = await build({ reseed: true });
         expect(entryOf(rebuilt).refresh).toBe("full");
         expect(entryOf(rebuilt).lateRows).toBeUndefined();
         expect(
            (await warnings()).filter(
               (w) => w.code === "persist-incremental-late-rows",
            ),
         ).toEqual([]);
      });

      async function storedIds(): Promise<number[]> {
         const rows = await sql(
            `SELECT row_to_json(t) AS "row" FROM (SELECT id FROM ` +
               `${q("events")} ORDER BY id) AS t`,
         );
         return rows.map((r) => Number(r.id));
      }

      for (const [label, mergeKey, expected] of [
         ["without a merge key, replaces the range it re-reads", "", [1, 5]],
         [
            "with a merge key, keeps rows the source dropped",
            'merge_key="id"',
            [1, 2, 5],
         ],
      ] as const) {
         it(`a delta from the newest stored row, ${label}`, async () => {
            await writeModel("", mergeKey);
            await build();
            // Row 2, the newest stored, leaves the source; row 5 arrives
            // above it. The delta reads from row 2's instant.
            await sql(`DELETE FROM ${q("ev")} WHERE id = 2`);
            await sql(`INSERT INTO ${q("ev")} VALUES
              (5, LOCALTIMESTAMP(0) - INTERVAL '1 hour', 500.0)`);
            await new Promise((resolve) => setTimeout(resolve, 1100));
            const delta = await build();
            expect(entryOf(delta).refresh).toBe("delta");
            expect(await storedIds()).toEqual([...expected]);
         });
      }

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

      it("stops a timed-out late-row check's statement, so it holds nothing", async () => {
         const connection = await env.getMalloyConnection("warehouse");
         const lineage = {
            physicalTableName: q("ev"),
            connectionName: "warehouse",
            sourceEntityId: "sid",
            watermarkName: "loaded_at",
            watermarkType: "timestamp",
            mergeKeys: ["id"],
            scopeColumns: [],
            strategy: "merge" as const,
         };
         const found = await countLateRows({
            target: warehouseDeltaTarget({
               dialect: "postgres",
               runner: (sql, options) => connection.runSQL(sql, options),
               quotedTablePath: q("ev"),
               lineage,
               // Seconds per row: far past the timeout.
               sourceSQL: `SELECT id, loaded_at, amt, pg_sleep(3) AS s FROM ${q("ev")}`,
            }),
            lineage,
            start: {
               malloyType: "timestamp",
               value: new Date().toISOString().slice(0, 19),
            },
            end: {
               malloyType: "timestamp",
               // A window of four hours below the start: both stored rows.
               value: new Date(Date.now() + 4 * 3_600_000)
                  .toISOString()
                  .slice(0, 19),
            },
            timeoutMs: 300,
         });
         // Whichever stops it first: the check's own timer, or the
         // statement timeout it ran under on the server.
         expect(found.error).toMatch(/longer than 300 ms|statement timeout/);
         // The server cancelled it: nothing of it is still running, and a
         // rename (as a reseed's swap takes) does not wait behind it.
         const [active] = await sql(
            `SELECT row_to_json(t) AS "row" FROM (SELECT count(*) AS n ` +
               `FROM pg_stat_activity WHERE state = 'active' AND ` +
               `query LIKE '%pg_sleep(3)%' AND pid <> pg_backend_pid()) AS t`,
         );
         expect(Number(active.n)).toBe(0);
         const renamed = Date.now();
         await sql(
            `SET LOCAL lock_timeout = '2s'; ALTER TABLE ${q("ev")} RENAME TO ev_moved`,
         );
         await sql(`ALTER TABLE ${SCHEMA}.ev_moved RENAME TO ev`);
         expect(Date.now() - renamed).toBeLessThan(2_000);
      });

      it("refuses a stored watermark read in a session that is not UTC", async () => {
         const connection = await env.getMalloyConnection("warehouse");
         await sql(`CREATE TABLE ${q("tz")} AS SELECT * FROM (VALUES
           (TIMESTAMPTZ '2026-11-01 05:30:00+00')) AS t(w)`);
         const probe = (prefix: string) =>
            probeStoredWatermark({
               runner: (text) => connection.runSQL(prefix + text),
               dialect: "postgres",
               quotedTablePath: q("tz"),
               column: "w",
               atOrBelow: {
                  malloyType: "timestamp",
                  value: "2026-11-02T00:00:00",
               },
            });
         // UTC, as Malloy's connection sets it: the instant itself.
         expect(await probe("")).toEqual({
            bound: { malloyType: "timestamp", value: "2026-11-01 05:30:00" },
         });
         // New York's fall-back hour: 01:30 happens twice that night, so the
         // value is refused (and the source rebuilt), never misplaced.
         const ny = await probe("SET LOCAL TIME ZONE 'America/New_York'; ");
         expect(ny.unparseable).toBe(true);
      });
   },
);
