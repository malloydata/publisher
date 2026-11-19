// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A rolling-window scope (`where: created_at >= now - 3650 days`) compares a
 * timestamp with time arithmetic and no string, so no collation changes what
 * it admits: its `storage=` table is served, equal to live, on DuckDB and on
 * a Postgres warehouse whose default collation is not binary (`en_US`), where
 * every scope the comparison rules cannot read as comparing no string fails
 * closed to live. A window whose day count is computed from a string column
 * (`length(region) days`) still reads a string, so on Postgres it answers
 * live, equal to live.
 *
 * DuckDB through the conformance fixture (a sandbox source, built by hand
 * into the lake). Postgres through the real path: an Environment whose
 * `warehouse` is the source, a DuckLake `lake` destination on a Postgres
 * catalog, a MaterializationService run, its bind, and the package's own
 * query; opt-in, with PUBLISHER_TEST_POSTGRES_URL.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../../src/service/access_record";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import type { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type { Package } from "../../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import { openTempStorage } from "../../../src/test_helpers/temp_storage";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

// Row 4 is older than the window, so the window cuts the rows.
const MODEL = (
   connection: string,
   lake: string,
) => `##! experimental.persistence
source: raw is ${connection}.sql("""
  SELECT CAST(id AS INTEGER) AS id, region, CAST(created_at AS TIMESTAMP) AS created_at
  FROM (VALUES (1, 'a', '2024-01-05 08:00:00'), (2, 'b', '2024-06-01 08:00:00'),
               (3, 'a', '2025-02-01 08:00:00'), (4, 'b', '1990-01-01 08:00:00')
  ) AS t(id, region, created_at)
""")

#@ persist name="rw_read" storage="${lake}"
source: recent_read is raw -> { select: id, region, created_at } extend {
  where: created_at >= now - 3650 days
}

#@ persist name="rw_plain" storage="${lake}"
source: plain is raw -> { select: id, region, created_at }

source: ext_window is plain extend { where: created_at >= now - 3650 days }
source: ext_month is plain extend { where: created_at.month >= @2000-01-01 00:00 + 1 month }
source: ext_by_string is plain extend {
  where: created_at >= @2000-01-01 00:00 + length(region) days
}
`;

const query = (s: string) => `run: ${s} -> { select: id; order_by: id }`;

/** Each source, and whether a non-binary Postgres default still serves it. */
const SCOPES: [string, "served" | "live on Postgres"][] = [
   ["recent_read", "served"],
   ["ext_window", "served"],
   ["ext_month", "served"],
   ["ext_by_string", "live on Postgres"],
];

const pgParts = () => {
   const parsed = new URL(url!);
   return {
      host: parsed.hostname,
      port: Number(parsed.port || 5432),
      userName: decodeURIComponent(parsed.username),
      password: decodeURIComponent(parsed.password),
   };
};

describe("a rolling-window scope over a storage= table (DuckDB)", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live: Record<string, Outcome> = {};

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ "m.malloy": MODEL("duckdb", LAKE) });
      for (const [s] of SCOPES) {
         live[s] = await loader.run("m.malloy", query(s), {});
      }
      fixture = await loader.bind();
   }, 300_000);

   afterAll(async () => {
      await fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   it("builds every stored table", () => {
      expect(fixture.buildErrors).toEqual({});
   });

   for (const [s] of SCOPES) {
      it(`${s}: answers from its stored table, equal to live`, async () => {
         const got = await fixture.run("m.malloy", query(s), {});
         const want = live[s];
         expect("answer" in want, JSON.stringify(want)).toBe(true);
         expect("answer" in got, JSON.stringify(got)).toBe(true);
         if (!("answer" in got) || !("answer" in want)) return;
         expect(got.answer.via).toBe("served_copy");
         expect(got.answer.rows).toEqual(want.answer.rows);
      });
   }
});

describe.skipIf(!url)(
   "a rolling-window scope over a storage= table (Postgres, en_US default)",
   () => {
      const token = `${process.pid}_${Date.now().toString(36)}`;
      const catalog = `rw_lake_${token}`;
      const warehouseDb = `rw_wh_${token}`;
      let admin: PostgresConnection;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let pkg: Package;
      let service: MaterializationService;
      let restore: () => void;
      const live: Record<string, number[]> = {};
      const served: Record<string, { ids: number[]; kind?: string }> = {};

      const ask = async (s: string) => {
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query: query(s) } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            ids: (result.compactResult as { id: unknown }[]).map((r) =>
               Number(r.id),
            ),
            kind: result.path?.servedBy?.kind,
         };
      };

      const build = async (): Promise<Materialization> => {
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
                  await new Promise((r) => setTimeout(r, 10));
               }
               return (await repo.getMaterializationById(created.id)) ?? m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((r) => setTimeout(r, 20));
         }
      };

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
            },
         }).restore;
         await freshLoadPool();
         let envPath: string;
         ({ root, envPath, db, repo } = await openTempStorage(
            "rolling-window-",
            { id: ENV_ID, name: ENV },
         ));
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": MODEL("warehouse", "lake") },
            { manifest: { name: PKG, description: "fixture" } },
         );
         admin = new PostgresConnection({
            name: "admin",
            connectionString: url!,
         });
         await admin.runSQL(`CREATE DATABASE ${catalog}`);
         await admin.runSQL(
            `CREATE DATABASE ${warehouseDb} LOCALE 'en_US.utf8' ENCODING 'UTF8' TEMPLATE template0`,
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
                  postgresConnection: {
                     ...pgParts(),
                     databaseName: warehouseDb,
                  },
               } as never,
            ],
            [
               {
                  name: "lake",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: {
                           ...pgParts(),
                           databaseName: catalog,
                        },
                     },
                     storage: { bucketUrl: bucket },
                  },
               } as never,
            ],
         );
         await env.addPackage(PKG);
         pkg = await env.getPackage(parsePackageName(PKG), false);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         // Before any build, every answer is live's.
         for (const [s] of SCOPES) live[s] = (await ask(s)).ids;
         const run = await build();
         expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
         pkg = await env.getPackage(parsePackageName(PKG), false);
         for (const [s] of SCOPES) served[s] = await ask(s);
      }, 300_000);

      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         for (const name of [catalog, warehouseDb]) {
            await admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin?.close().catch(() => undefined);
         if (root) fs.rmSync(root, { recursive: true, force: true });
         restore?.();
         await freshLoadPool();
      });

      it("cuts the rows: the window keeps three of four", () => {
         expect(live.recent_read).toEqual([1, 2, 3]);
         expect(live.ext_window).toEqual([1, 2, 3]);
      });

      for (const [s, expected] of SCOPES) {
         it(`${s}: answers equal to live, ${expected === "served" ? "from its stored table" : "live"}`, () => {
            expect(served[s].ids).toEqual(live[s]);
            if (expected === "served") {
               expect(served[s].kind).not.toBe("live");
               expect(served[s].kind).toBeDefined();
            } else {
               expect(served[s].kind).toBe("live");
            }
         });
      }
   },
);
