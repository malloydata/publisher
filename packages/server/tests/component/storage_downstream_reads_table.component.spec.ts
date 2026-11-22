// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` downstream is built from its upstream's stored table, never by
 * recomputing the upstream from raw: the drop-raw proof, for a downstream in
 * a DuckLake destination. The upstream is built alone; its raw rows are then
 * deleted out of band; the downstream, built next, still builds, reports
 * `upstreamReuse: "reused"`, names the upstream's table in `builtFrom`, and
 * answers as live answered before the delete.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the warehouse and the
 * DuckLake catalog.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
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
const PKG = "pkg";
const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

const RAW_ROWS = `SELECT * FROM (VALUES
    ('a', 'x', 9.0::DOUBLE PRECISION), ('a', 'y', 10.5::DOUBLE PRECISION),
    ('b', 'x', 2.5::DOUBLE PRECISION)
  ) AS t(cat, sub, amt)`;

const HEAD = `##! experimental.persistence
source: raw is warehouse.table('public.sdr_raw')
`;

interface Mechanism {
   label: string;
   model: string;
   /** Built alone first. */
   upstream: string;
   /** The downstream's entry's `sourceName`. */
   downstream: string;
   query: string;
}

const DATABASE_BASE = `#@ persist name="public.sdr_base"
source: base is raw -> { select: cat, sub, amt }
`;

const MECHANISMS: Mechanism[] = [
   {
      label: "storage= view over a database #@ persist",
      model: `${HEAD}${DATABASE_BASE}
source: s is base extend {
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      upstream: "base",
      downstream: "s__by_cat__view",
      query: "run: s -> by_cat -> { select: *; order_by: cat }",
   },
   {
      label: "storage= query over a database #@ persist",
      model: `${HEAD}${DATABASE_BASE}
#@ persist storage="lake"
query: q is base -> { group_by: sub; aggregate: total is amt.sum() }
`,
      upstream: "base",
      downstream: "q__query",
      query: "run: q -> { select: *; order_by: sub }",
   },
];

interface Entry {
   sourceName?: string;
   sourceEntityId: string;
   physicalTableName: string;
   storageDestinationName?: string;
   upstreamReuse?: string;
   error?: string;
   builtFrom?: { sourceEntityId: string; physicalTableName: string }[];
}

const entriesOf = (m: Materialization): Entry[] =>
   Object.values(
      (m.manifest as { entries?: Record<string, Entry> }).entries ?? {},
   );

for (const mechanism of MECHANISMS) {
   describe.skipIf(!url)(
      `a storage= downstream builds from its upstream's table with its raw rows gone (${mechanism.label})`,
      () => {
         const token = Math.random().toString(36).slice(2, 8);
         const catalog = `sdr_lake_${token}`;
         const warehouse = `sdr_wh_${token}`;
         let admin: PostgresConnection;
         let root: string;
         let db: DuckDBConnection;
         let repo: ResourceRepository;
         let env: Environment;
         let service: MaterializationService;
         let restore: () => void;
         let first: Materialization;
         let second: Materialization;
         let liveBefore: unknown;
         let served: { rows: unknown; kind?: string };

         const ask = async (query: string) => {
            const pkg = await env.getPackage(parsePackageName(PKG), false);
            const result = await pkg
               .getModel("m.malloy")!
               .runQuery(
                  unrecordedAudit(),
                  { query } as never,
                  QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
               );
            return {
               rows: JSON.parse(JSON.stringify(result.compactResult)),
               kind: result.path?.servedBy?.kind,
            };
         };
         const build = async (opts: object): Promise<Materialization> => {
            const created = await service.createMaterialization(ENV, PKG, opts);
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            const deadline = Date.now() + 120_000;
            for (;;) {
               const m = await repo.getMaterializationById(created.id);
               if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
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
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "on",
               },
            }).restore;
            let envPath: string;
            ({ root, envPath, db, repo } = await openTempStorage("sdr-", {
               id: ENV_ID,
               name: ENV,
            }));
            writePackageFiles(
               path.join(envPath, PKG),
               { "m.malloy": mechanism.model },
               { manifest: { name: PKG, description: "fixture" } },
            );
            admin = new PostgresConnection({
               name: "admin",
               connectionString: url!,
            });
            await admin.runSQL(`CREATE DATABASE ${catalog}`);
            await admin.runSQL(`CREATE DATABASE ${warehouse}`);
            const parsed = new URL(url!);
            const pg = {
               host: parsed.hostname,
               port: Number(parsed.port || 5432),
               userName: decodeURIComponent(parsed.username),
               password: decodeURIComponent(parsed.password),
            };
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
            const wh = await env.getMalloyConnection("warehouse");
            await wh.runSQL(`CREATE TABLE public.sdr_raw AS ${RAW_ROWS}`);
            await env.addPackage(PKG);
            service = serviceOverEnvironment(
               () => repo,
               () => env,
            );
            liveBefore = (await ask(mechanism.query)).rows;
            first = await build({ sourceNames: [mechanism.upstream] });
            // Out of band: the raw rows are gone. A downstream that
            // recomputed its upstream from raw would hold no rows.
            await wh.runSQL("DELETE FROM public.sdr_raw");
            second = await build({});
            served = await ask(mechanism.query);
         }, 300_000);

         afterAll(async () => {
            await env?.closeAllConnections().catch(() => undefined);
            await db?.close();
            for (const name of [catalog, warehouse]) {
               await admin
                  ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
                  .catch(() => undefined);
            }
            await admin?.close().catch(() => undefined);
            fs.rmSync(root, { recursive: true, force: true });
            restore?.();
         });

         it("builds the downstream into the destination, reusing its upstream's table, named in builtFrom", () => {
            expect([first.status, first.error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            expect(second.status).toBe("MANIFEST_FILE_READY");
            const upstream = entriesOf(first).find(
               (e) => e.sourceName === mechanism.upstream,
            )!;
            const downstream = entriesOf(second).find(
               (e) => e.sourceName === mechanism.downstream,
            )!;
            expect(downstream.error).toBeUndefined();
            expect(downstream.storageDestinationName).toBe("lake");
            expect(downstream.upstreamReuse).toBe("reused");
            expect(
               (downstream.builtFrom ?? []).map((b) => b.sourceEntityId),
            ).toEqual([upstream.sourceEntityId]);
         });

         it("serves its table, equal to live before the delete", () => {
            expect(served.kind).not.toBe("live");
            expect(served.rows).toEqual(liveBefore);
         });
      },
   );
}
