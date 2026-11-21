// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The gtm-funnel shape (finding 2) on a Postgres origin with a DuckLake
 * `storage=` destination, through the production path (Environment, Package,
 * MaterializationService's build and bind): `crm.malloy` stores
 * `crm_current` and an unrelated `crm_unrelated`; `deals.malloy` imports it,
 * joins `crm_current` from an unpersisted `d_raw`, stores `deals11` over a
 * `->` stage that reads the join, and stores an unrelated `d_unrelated`;
 * `pipeline.malloy` imports `deals.malloy`. A second shape adds the
 * gtm-funnel package's own: the `->` stage reached through
 * `include { public: … }` and a join keyed through the stored join.
 *
 * The join is left out of `deals.malloy`'s served copy (a per-source
 * warning on `d_raw`), so the model compiles and its stored sources serve.
 * A stored source queried through a model that imports it was answered live
 * under a default (`en_US`) Postgres database, though: the string-collation
 * check took its query's entry point by name only, and Malloy hands a query
 * on an imported source its struct rather than its name. Equal to live, as
 * strings, row for row, and ordered strings still answered live.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
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
import { unrecordedAudit } from "../../../src/service/access_record";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import type { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { freshLoadPool } from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
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

// Mixed case, so a string order differs between `en_US` and byte order.
const SEED = `CREATE TABLE public.orders AS SELECT * FROM (VALUES
   ('a', 'east', 'shipped', 10.0), ('a', 'east', 'cancelled', 5.0),
   ('b', 'west', 'shipped', 7.0), ('b', 'east', 'Shipped', 3.0),
   ('B', 'west', 'returned', 2.0)
 ) AS t(category, region, status, amount)`;

const CRM = `source: crm_raw is e2e_pg.table('public.orders')

#@ persist storage="lake" name="gtmval_crm_current"
source: crm_current is crm_raw -> {
  group_by: category, region
  aggregate: amt is amount.sum()
}

#@ persist storage="lake" name="gtmval_crm_unrelated"
source: crm_unrelated is crm_raw -> { group_by: status; aggregate: n is count() }

// A second cross-connection join, in the imported file: what made the
// importing models' whole copies fail (cross_connection_unbuilt).
source: crm_including_deleted is crm_raw extend {
  join_one: cur is crm_current on category = cur.category and region = cur.region
  measure: order_count is count()
}
`;

const UNRELATED = `
#@ persist storage="lake" name="gtmval_d_unrelated"
source: d_unrelated is e2e_pg.table('public.orders') -> {
  group_by: region
  aggregate: n is count()
}
`;

const SHAPES: Record<string, string> = {
   "the reported shape": `import "crm.malloy"

source: d_raw is e2e_pg.table('public.orders') extend {
  join_one: cur is crm_current on category = cur.category and region = cur.region
}

source: d11_incl is d_raw -> {
  group_by: category, region, is_deleted is status = 'cancelled', cur_amt is cur.amt
  aggregate: amt is amount.sum()
}

#@ persist storage="lake" name="gtmval_deals11"
source: deals11 is d11_incl extend { where: not is_deleted }
${UNRELATED}`,
   "an include block and a join keyed through the stored join": `##! experimental.access_modifiers
import "crm.malloy"

source: attrs_raw is e2e_pg.table('public.orders') -> {
  group_by: category, status
  aggregate: k is count()
}

source: d_raw is e2e_pg.table('public.orders') extend {
  join_one: companies is crm_current on category = companies.category and region = companies.region
  join_one: deal_attributes is attrs_raw on deal_attributes.category = companies.category and deal_attributes.status = status
  join_one: plain is attrs_raw on plain.category = category and plain.status = status
}

source: d_pub is d_raw include {
  public: category, region, status, amount, companies, deal_attributes, plain
}

source: d11_incl is d_pub -> {
  group_by: category, region, is_deleted is status = 'cancelled', cur_amt is companies.amt, k is plain.k
  aggregate: amt is amount.sum()
}

#@ persist storage="lake" name="gtmval_deals11"
source: deals11 is d11_incl extend { where: not is_deleted }
${UNRELATED}`,
};

const PIPELINE = `import "deals.malloy"
`;

/** Unordered rows as strings, each row's values in column order. */
function asStrings(rows: unknown): string[] {
   return (rows as Record<string, unknown>[])
      .map((r) => JSON.stringify(Object.values(r).map((v) => String(v))))
      .sort();
}

/** Rows as strings, in their order. */
function inOrder(rows: unknown): string[] {
   return (rows as Record<string, unknown>[]).map((r) =>
      JSON.stringify(Object.values(r).map((v) => String(v))),
   );
}

interface Answer {
   rows: unknown;
   servedFrom: string | null;
}

for (const [shape, deals] of Object.entries(SHAPES)) {
   describe.skipIf(!POSTGRES_URL)(
      `gtm-funnel finding 2 on Postgres: ${shape}`,
      () => {
         const token = Math.random().toString(36).slice(2, 8);
         const warehouse = `gtm_wh_${token}`;
         const catalog = `gtm_lake_${token}`;
         let admin: PostgresConnection;
         let root: string;
         let db: DuckDBConnection;
         let repo: ResourceRepository;
         let env: Environment | undefined;
         let service: MaterializationService;
         let restore = () => {};
         let run: Materialization | undefined;
         const live = new Map<string, Answer>();

         async function ask(model: string, malloy: string): Promise<Answer> {
            const pkg = await env!.getPackage(parsePackageName(PKG), false);
            const m = pkg.getModel(model);
            if (!m) throw new Error(`${model} did not load`);
            const result = await m.runQuery(
               unrecordedAudit(),
               { query: malloy } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
            return {
               rows: result.compactResult,
               servedFrom: result.servedFrom ?? null,
            };
         }

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
                  ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(
                     m.status,
                  )
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

         const queries: [string, string][] = [
            [
               "deals.malloy",
               "run: d_unrelated -> { group_by: region; aggregate: t is n.sum() }",
            ],
            [
               "deals.malloy",
               "run: crm_unrelated -> { group_by: status; aggregate: t is n.sum() }",
            ],
            [
               "deals.malloy",
               "run: deals11 -> { group_by: category; aggregate: a is amt.sum() }",
            ],
            [
               "pipeline.malloy",
               "run: d_unrelated -> { group_by: region; aggregate: t is n.sum() }",
            ],
            [
               "deals.malloy",
               "run: crm_unrelated -> { group_by: status; aggregate: t is n.sum(); order_by: status }",
            ],
         ];
         queries.push(
            [
               "deals.malloy",
               "run: crm_including_deleted -> { group_by: region; aggregate: order_count }",
            ],
            [
               "deals.malloy",
               "run: d_raw -> { group_by: region; aggregate: c is count() }",
            ],
         );
         const key = (model: string, q: string) => `${model}\u0000${q}`;

         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "on",
               },
            }).restore;
            await freshLoadPool();
            ({ root, db, repo } = await openTempStorage("gtm-pg-", {
               id: ENV_ID,
               name: ENV,
            }));
            admin = new PostgresConnection({
               name: "admin",
               connectionString: POSTGRES_URL!,
            });
            // The server's default collation (`en_US.utf8` on the postgres
            // image), as a warehouse nobody configured has.
            await admin.runSQL(`CREATE DATABASE ${warehouse}`);
            await admin.runSQL(`CREATE DATABASE ${catalog}`);
            const parsed = new URL(POSTGRES_URL!);
            const pg = {
               host: parsed.hostname,
               port: Number(parsed.port || 5432),
               userName: decodeURIComponent(parsed.username),
               password: decodeURIComponent(parsed.password),
            };
            const seed = new PostgresConnection({
               name: "seed",
               connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
            });
            await seed.runSQL(SEED);
            await seed.close();
            const envPath = path.join(root, "env");
            writePackageFiles(
               path.join(envPath, PKG),
               {
                  "crm.malloy": CRM,
                  "deals.malloy": deals,
                  "pipeline.malloy": PIPELINE,
               },
               { manifest: { name: PKG, description: "gtm-funnel finding 2" } },
            );
            const bucket = path.join(root, "bucket");
            fs.mkdirSync(bucket);
            env = await Environment.create(
               ENV,
               envPath,
               [
                  {
                     name: "e2e_pg",
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
            for (const [model, q] of queries) {
               live.set(key(model, q), await ask(model, q));
            }
            run = await build();
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

         it("builds every stored source", () => {
            expect([run?.status, run?.error ?? null]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
         });

         const served: [string, string, string][] = [
            ["an unrelated stored source", ...queries[0]],
            ["a stored source imported into the model", ...queries[1]],
            ["the stored source over the joining stage", ...queries[2]],
            ["a stored source through an importing model", ...queries[3]],
         ];
         for (const [name, model, q] of served) {
            it(`serves ${name} from storage, equal to live`, async () => {
               const l = live.get(key(model, q))!;
               expect(l.servedFrom).toBeNull();
               const s = await ask(model, q);
               expect(asStrings(s.rows)).toEqual(asStrings(l.rows));
               expect(s.servedFrom).toBe("storage");
            });
         }

         it("answers an imported source's string order live, in live's order", async () => {
            const [model, q] = queries[4];
            const l = live.get(key(model, q))!;
            const s = await ask(model, q);
            expect(inOrder(s.rows)).toEqual(inOrder(l.rows));
            expect(s.servedFrom).toBeNull();
         });

         for (const [name, i] of [
            ["the imported joining source", 5],
            ["the importer's joining source", 6],
         ] as const) {
            it(`answers ${name} live, equal to live`, async () => {
               const [model, q] = queries[i];
               const l = live.get(key(model, q))!;
               const s = await ask(model, q);
               expect(asStrings(s.rows)).toEqual(asStrings(l.rows));
               expect(s.servedFrom).toBeNull();
            });
         }

         it("warns of the left-out join on its own source, not the model", async () => {
            const pkg = await env!.getPackage(parsePackageName(PKG), false);
            const problems = (await pkg
               .getModel("deals.malloy")!
               .servedCopyProblems()) as {
               sourceName?: string;
               reason: string;
            }[];
            expect(
               problems.map((p) => [p.sourceName, p.reason]).sort(),
            ).toEqual([
               ["crm_including_deleted", "cross_connection_join"],
               ["d_raw", "cross_connection_join"],
            ]);
         });
      },
   );
}
