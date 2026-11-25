// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A query filter on a computed dimension that compares no string
 * (`visible is restricted = 0 or g.opp_id is not null`, `big is amount >
 * 150`) admits the same rows under any collation, so its `storage=` table is
 * served, equal to live: in a `where:` and in a filtered measure, on a
 * query-shaped source that joins a stored grant table scoped per caller, on
 * a query-shaped source that joins nothing, and on a SQL source. A computed
 * dimension that orders a string (`late is category > 'M'`) is still judged:
 * on a Postgres warehouse whose default collation is `en_US` it answers
 * live, equal to live.
 *
 * Through the real path: an Environment whose `warehouse` is the source, a
 * DuckLake `lake` destination on a Postgres catalog, a MaterializationService
 * run, its bind, and the package's own query. Opt-in, with
 * PUBLISHER_TEST_POSTGRES_URL.
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
import { freshLoadPool } from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import { openTempStorage } from "../../../src/test_helpers/temp_storage";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

// Opp 1 is unrestricted; 2 and 3 are restricted in org 1; 4 is org 2's.
// User 7 holds a grant on opp 2, user 8 on opps 2 and 3.
const MODEL = `##! experimental.persistence
##! experimental.givens

given:
  ORG_ID  :: number is 1
  USER_ID :: number is 7

source: raw_opps is warehouse.sql("""
  SELECT * FROM (VALUES (1, 1, 0, 100, 'books'), (2, 1, 1, 200, 'Toys'),
                        (3, 1, 1, 400, 'apple'), (4, 2, 1, 800, 'toys')
  ) AS t(opp_id, org_id, restricted, amount, category)
""")
source: raw_grants is warehouse.sql("""
  SELECT * FROM (VALUES (1, 7, 2), (1, 8, 2), (1, 8, 3), (2, 9, 4)
  ) AS t(org_id, user_id, opp_id)
""")

#@ persist name="cd_grants" storage="lake"
source: grants is raw_grants -> { select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}

#@ persist name="cd_opps" storage="lake"
source: opps is raw_opps -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on opp_id = g.opp_id
  dimension: visible is restricted = 0 or g.opp_id is not null
  measure:
    visible_amount is amount.sum() { where: visible }
    hidden is count() { where: not visible }
}

#@ persist name="cd_plain" storage="lake"
source: plain is raw_opps -> { select: * } extend {
  dimension:
    big is amount > 150
    late is category > 'M'
  measure: big_amount is amount.sum() { where: big }
}

#@ persist name="cd_tab" storage="lake"
source: tab is raw_opps extend {
  dimension: big is amount > 150
  measure: big_amount is amount.sum() { where: big }
}
`;

interface Case {
   name: string;
   query: string;
   givens?: Record<string, unknown>;
   expected: "served" | "live on Postgres";
}

const CASES: Case[] = [
   {
      name: "a grant join, user 7",
      query: "run: opps -> { aggregate: visible_amount, hidden }",
      givens: { ORG_ID: 1, USER_ID: 7 },
      expected: "served",
   },
   {
      name: "a grant join, user 8",
      query: "run: opps -> { aggregate: visible_amount, hidden }",
      givens: { ORG_ID: 1, USER_ID: 8 },
      expected: "served",
   },
   {
      name: "a grant join, in a where:",
      query: "run: opps -> { where: visible; select: opp_id; order_by: opp_id }",
      givens: { ORG_ID: 1, USER_ID: 8 },
      expected: "served",
   },
   {
      name: "a query-shaped source with no join, in a where:",
      query: "run: plain -> { where: big; select: opp_id; order_by: opp_id }",
      expected: "served",
   },
   {
      name: "a query-shaped source with no join, in a filtered measure",
      query: "run: plain -> { aggregate: big_amount }",
      expected: "served",
   },
   {
      name: "a SQL source with no join, in a where:",
      query: "run: tab -> { where: big; select: opp_id; order_by: opp_id }",
      expected: "served",
   },
   {
      name: "a SQL source with no join, in a filtered measure",
      query: "run: tab -> { aggregate: big_amount }",
      expected: "served",
   },
   {
      name: "a computed dimension that orders a string",
      query: "run: plain -> { where: late; select: opp_id; order_by: opp_id }",
      expected: "live on Postgres",
   },
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

describe.skipIf(!url)(
   "a query filter on a computed dimension over storage= tables (Postgres, en_US default)",
   () => {
      const token = `${process.pid}_${Date.now().toString(36)}`;
      const catalog = `cd_lake_${token}`;
      const warehouseDb = `cd_wh_${token}`;
      let admin: PostgresConnection;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let pkg: Package;
      let service: MaterializationService;
      let restore: () => void;
      const live: Record<string, unknown[]> = {};
      const served: Record<string, { rows: unknown[]; kind?: string }> = {};

      const ask = async (c: Case) => {
         const result = await pkg.getModel("m.malloy")!.runQuery(
            unrecordedAudit(),
            {
               query: c.query,
               ...(c.givens ? { givens: c.givens } : {}),
            } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         return {
            rows: (result.compactResult as Record<string, unknown>[]).map((r) =>
               Object.fromEntries(
                  Object.entries(r).map(([k, v]) => [k, Number(v)]),
               ),
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
            env: { PERSIST_STORAGE_MODE: "on" },
         }).restore;
         await freshLoadPool();
         let envPath: string;
         ({ root, envPath, db, repo } = await openTempStorage(
            "computed-dimension-filter-",
            { id: ENV_ID, name: ENV },
         ));
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": MODEL },
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
         for (const c of CASES) live[c.name] = (await ask(c)).rows;
         const run = await build();
         expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
         pkg = await env.getPackage(parsePackageName(PKG), false);
         for (const c of CASES) served[c.name] = await ask(c);
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

      it("answers per caller live: each user sees the opps granted to them", () => {
         expect(live["a grant join, user 7"]).toEqual([
            { visible_amount: 300, hidden: 1 },
         ]);
         expect(live["a grant join, user 8"]).toEqual([
            { visible_amount: 700, hidden: 0 },
         ]);
         expect(
            live["a query-shaped source with no join, in a where:"],
         ).toEqual([{ opp_id: 2 }, { opp_id: 3 }, { opp_id: 4 }]);
      });

      for (const c of CASES) {
         it(`${c.name}: answers equal to live, ${c.expected === "served" ? "from its stored table" : "live"}`, () => {
            expect(served[c.name].rows).toEqual(live[c.name]);
            if (c.expected === "served") {
               expect(served[c.name].kind).not.toBe("live");
               expect(served[c.name].kind).toBeDefined();
            } else {
               expect(served[c.name].kind).toBe("live");
            }
         });
      }
   },
);
