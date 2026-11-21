// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package over a real Postgres warehouse (`en_US` default collation) with a
 * DuckLake `lake` destination on a Postgres catalog, through the real path:
 * an Environment, a MaterializationService run, its bind, and the package's
 * own queries. For specs that compare a stored answer with live's. Opt-in,
 * with PUBLISHER_TEST_POSTGRES_URL.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { expect } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../../src/service/access_record";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import type { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type { Package } from "../../../src/service/package";
import type { ResourceRepository } from "../../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { freshLoadPool } from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import { openTempStorage } from "../../../src/test_helpers/temp_storage";

const ENV = "e1";
const PKG = "pkg";

export const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

/** One answer: its rows, every value a string, and what served it. */
export interface Answer {
   rows: Record<string, string | null>[];
   kind: string | undefined;
   detail: string | undefined;
}

export interface PostgresLake {
   /** Run `query` against `model` (`givens`, if any) under the current bind. */
   ask(
      model: string,
      query: string,
      givens?: Record<string, unknown>,
   ): Promise<Answer>;
   /** Build every planned source, then bind the built tables. */
   build(): Promise<void>;
   close(): Promise<void>;
}

const asString = (v: unknown): string | null =>
   v === null || v === undefined
      ? null
      : typeof v === "object"
        ? JSON.stringify(v)
        : String(v);

/**
 * Open the package `files` over a fresh Postgres warehouse database seeded
 * with `seedSql`, as the connection `warehouse`, and a DuckLake `lake`.
 */
export async function openPostgresLake(
   prefix: string,
   files: Record<string, string>,
   seedSql: readonly string[],
): Promise<PostgresLake> {
   const restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on" },
   }).restore;
   await freshLoadPool();
   const token = `${process.pid}_${Date.now().toString(36)}`;
   const catalog = `${prefix}_lake_${token}`;
   const warehouseDb = `${prefix}_wh_${token}`;
   const { root, envPath, db, repo } = await openTempStorage(`${prefix}-`, {
      id: "env-1",
      name: ENV,
   });
   writePackageFiles(path.join(envPath, PKG), files, {
      manifest: { name: PKG, description: "fixture" },
   });
   const parsed = new URL(POSTGRES_URL!);
   const pg = {
      host: parsed.hostname,
      port: Number(parsed.port || 5432),
      userName: decodeURIComponent(parsed.username),
      password: decodeURIComponent(parsed.password),
   };
   const admin = new PostgresConnection({
      name: "admin",
      connectionString: POSTGRES_URL!,
   });
   await admin.runSQL(`CREATE DATABASE ${catalog}`);
   await admin.runSQL(
      `CREATE DATABASE ${warehouseDb} LOCALE 'en_US.utf8' ENCODING 'UTF8' TEMPLATE template0`,
   );
   const seeder = new PostgresConnection({
      name: "seed",
      connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouseDb}`,
   });
   for (const sql of seedSql) await seeder.runSQL(sql);
   await seeder.close();
   const bucket = path.join(root, "bucket");
   fs.mkdirSync(bucket);
   const env = await Environment.create(
      ENV,
      envPath,
      [
         {
            name: "warehouse",
            type: "postgres",
            postgresConnection: { ...pg, databaseName: warehouseDb },
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
   let pkg: Package = await env.getPackage(parsePackageName(PKG), false);
   const service: MaterializationService = serviceOverEnvironment(
      () => repo as ResourceRepository,
      () => env,
   );

   return {
      async ask(model, query, givens) {
         const result = await pkg
            .getModel(model)!
            .runQuery(
               unrecordedAudit(),
               { query, ...(givens ? { givens } : {}) } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         const considered = (result.path?.considered ?? []) as {
            detail?: string;
         }[];
         return {
            rows: (result.compactResult as Record<string, unknown>[]).map((r) =>
               Object.fromEntries(
                  Object.entries(r).map(([k, v]) => [k, asString(v)]),
               ),
            ),
            kind: result.path?.servedBy?.kind,
            detail: considered.find((c) => c.detail)?.detail,
         };
      },
      async build() {
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
               const run = (await repo.getMaterializationById(created.id)) ?? m;
               expect([run.status, run.error]).toEqual([
                  "MANIFEST_FILE_READY",
                  null,
               ]);
               break;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((r) => setTimeout(r, 20));
         }
         pkg = await env.getPackage(parsePackageName(PKG), false);
      },
      async close() {
         await env.closeAllConnections().catch(() => undefined);
         await (db as DuckDBConnection).close();
         for (const name of [catalog, warehouseDb]) {
            await admin
               .runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin.close().catch(() => undefined);
         fs.rmSync(root, { recursive: true, force: true });
         restore();
         await freshLoadPool();
      },
   };
}
