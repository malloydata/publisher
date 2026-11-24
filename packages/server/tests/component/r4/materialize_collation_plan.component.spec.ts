// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` view over a `#(materialize)` Postgres source is built over
 * that source's table in its destination, which compares strings
 * byte-exact. Before the table is bound, the plan judges each string the
 * view groups or orders by how the source compares it live, as its models'
 * bind read recorded it: a top-N by a `C` column is planned and built over
 * the table in the first run; one by an `en_US` column is refused at plan (`upstream_table_unusable`), never left to fail at build.
 * The load's plan is derived before that read lands, and again once it
 * does, so a refusal for a read not yet landed never sticks; after the bind,
 * the plan judges by the table's contract, as before.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create and
 * drop databases on.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import type { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import type {
   Materialization,
   ResourceRepository,
} from "../../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { freshLoadPool } from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { planRefusalsOnceRead } from "../../../src/test_helpers/string_capability";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../../src/test_helpers/temp_storage";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";

setDefaultTimeout(600_000);

const MODEL = `##! experimental { persistence }
source: items_src is pg.table('public.items') extend {
  primary_key: id
  measure: n is count()
}

#(materialize) storage="lake"
source: m is items_src extend {
  #@ persist name="v_by_cc" storage="lake"
  view: by_cc is { group_by: cc; aggregate: n; order_by: cc; limit: 2 }
  #@ persist name="v_by_cat" storage="lake"
  view: by_cat is { group_by: cat; aggregate: n; order_by: cat; limit: 2 }
}
`;

describe.skipIf(!POSTGRES_URL)(
   "a storage= view over an unbound #(materialize) table is judged at plan by its source's collation",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const warehouse = `mz_coll_wh_${token}`;
      const catalog = `mz_coll_lake_${token}`;
      let admin: PostgresConnection;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment | undefined;
      let service: MaterializationService;
      let restore = () => {};
      let first: Materialization | undefined;
      let second: Materialization | undefined;
      let beforeRun: Record<string, { reason: string; message: string }> = {};
      let afterBind: Record<string, { reason: string; message: string }> = {};

      const pkg = () => env!.getPackage(parsePackageName(PKG), false);

      async function build(): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, {});
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
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               while (running.has(created.id)) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
               }
               return (await repo.getMaterializationById(created.id)) ?? m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 50));
         }
      }

      /** Each entry of `run`'s manifest, by source name. */
      function entries(
         run: Materialization | undefined,
      ): Record<string, Record<string, unknown>> {
         const manifest = run?.manifest as {
            entries?: Record<string, Record<string, unknown>>;
         };
         return Object.fromEntries(
            Object.values(manifest?.entries ?? {}).map((e) => [
               String(e.sourceName),
               e,
            ]),
         );
      }

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "on",
            },
         }).restore;
         await freshLoadPool();
         ({ root, db, repo } = await openTempStorage("mz-coll-", {
            id: ENV_ID,
            name: ENV,
         }));
         admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL!,
         });
         await admin.runSQL(
            `CREATE DATABASE ${warehouse} LOCALE 'en_US.utf8' ENCODING 'UTF8' TEMPLATE template0`,
         );
         await admin.runSQL(`CREATE DATABASE ${catalog}`);
         const parsed = new URL(POSTGRES_URL!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const seeder = new PostgresConnection({
            name: "seed",
            connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
         });
         for (const sql of [
            `CREATE TABLE items (id int, cat text, cc text COLLATE "C")`,
            `INSERT INTO items VALUES (1, 'apple', 'apple'), (2, 'Apple', 'Apple'), (3, 'banana', 'banana'), (4, 'Banana', 'Banana')`,
         ]) {
            await seeder.runSQL(sql);
         }
         await seeder.close();

         const envPath = path.join(root, "env");
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": MODEL },
            { manifest: { name: PKG, description: "materialize collation" } },
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
         beforeRun = await planRefusalsOnceRead(await pkg());
         first = await build();
         // The bind's capability read derives the plan again, by m's contract.
         const deadline = Date.now() + 30_000;
         for (;;) {
            afterBind = await planRefusalsOnceRead(await pkg());
            const message = afterBind.m__by_cat__view?.message ?? "";
            if (message.includes("its upstream's table records")) break;
            if (Date.now() > deadline) break;
            await new Promise((resolve) => setTimeout(resolve, 50));
         }
         second = await build();
      });

      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
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

      it("refuses at plan, before m is bound, only the view ordering a non-C column", () => {
         expect(beforeRun.m__by_cc__view).toBeUndefined();
         expect(beforeRun.m__by_cat__view?.reason).toBe(
            "upstream_table_unusable",
         );
         expect(beforeRun.m__by_cat__view?.message).toContain(
            "it orders 'cat' from 'm', whose table is not bound yet and whose source compares it by equality only",
         );
      });

      it("builds the C view over m's table in the first run, and fails nothing at build", () => {
         expect([first?.status, first?.error ?? null]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const manifest = first?.manifest as { failures?: unknown };
         expect(manifest.failures ?? {}).toEqual({});
         const got = entries(first);
         expect(got.m?.error).toBeUndefined();
         expect(got.m__by_cc__view?.error).toBeUndefined();
         expect(got.m__by_cc__view?.upstreamReuse).toBe("reused");
         expect(got.m__by_cat__view).toBeUndefined();
      });

      it("judges by m's bound contract after the bind, to the same verdicts", () => {
         expect(afterBind.m__by_cc__view).toBeUndefined();
         expect(afterBind.m__by_cat__view?.reason).toBe(
            "upstream_table_unusable",
         );
         expect(afterBind.m__by_cat__view?.message).toContain(
            "which its upstream's table records as compared by equality only",
         );
         expect(second?.status).toBe("MANIFEST_FILE_READY");
         expect(entries(second).m__by_cc__view?.error).toBeUndefined();
      });
   },
);
