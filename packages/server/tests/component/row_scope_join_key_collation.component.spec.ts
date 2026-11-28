// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A row scope that tests a string for null through a join on a computed
 * string key, over a stored query-shaped source (gtme's `pipeline_now`:
 * `deals extend { where: is_open }`, `is_open is first_close.close_type is
 * null`, joined on a `jsonb` extract). On Postgres, through the real package
 * bind, database tier.
 *
 * The collation check refused it: it failed closed on any non-string field
 * of a query-shaped source, though `is_open` is no stage output but a field
 * its `extend` declares, whose expression reads a judged path. Now served
 * from the tables. A join key under a nondeterministic collation (here a
 * case-insensitive ICU one) matches rows a byte-wise stored copy would not,
 * so it is refused and answered live; before, it was not judged at all.
 *
 * Opt-in: PUBLISHER_TEST_POSTGRES_URL, a server the test may create and drop
 * databases on.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

const MODEL = `##! experimental { persistence }
source: events is warehouse.sql("""
  SELECT data, stage, seq FROM (VALUES
    ('{"id":"d1"}'::jsonb, 'won', 1),
    ('{"id":"d2"}'::jsonb, 'open', 1),
    ('{"id":"d3"}'::jsonb, 'lost', 1),
    ('{"id":"d4"}'::jsonb, 'open', 1)
  ) AS t(data, stage, seq)
""") extend {
  dimension: deal_id is jsonb_extract_path_text!string(data, 'id')
}

#(materialize) name="p6_first_close"
source: deal_first_close is events -> {
  where: stage = 'won' or stage = 'lost'
  group_by: deal_id, close_type is stage
}

#(materialize) name="p6_deals_inc"
source: deals_including_deleted is events -> { group_by: deal_id } extend {
  join_one: deal_first_close on deal_id = deal_first_close.deal_id
  dimension: is_open is deal_first_close.close_type is null
}

source: pipeline_now is deals_including_deleted extend { where: is_open }
source: funnel_deals is deals_including_deleted extend { measure: c is count() }
`;

const CI_MODEL = `##! experimental { persistence }
source: events is warehouse.sql("""
  SELECT (data->>'id') COLLATE ci AS deal_id, stage FROM (VALUES
    ('{"id":"d1"}'::jsonb, 'won'),
    ('{"id":"D1"}'::jsonb, 'open'),
    ('{"id":"d2"}'::jsonb, 'open'),
    ('{"id":"d3"}'::jsonb, 'lost')
  ) AS t(data, stage)
""")

#(materialize) name="p6ci_first_close"
source: deal_first_close is events -> {
  where: stage = 'won' or stage = 'lost'
  group_by: deal_id, close_type is stage
}

#(materialize) name="p6ci_deals_inc"
source: deals_including_deleted is events -> { group_by: deal_id } extend {
  join_one: deal_first_close on deal_id = deal_first_close.deal_id
  dimension: is_open is deal_first_close.close_type is null
}

source: pipeline_now is deals_including_deleted extend { where: is_open }
source: funnel_deals is deals_including_deleted extend { measure: c is count() }
`;

const QUERIES: Record<string, string> = {
   pipeline_now: "run: pipeline_now -> { aggregate: n is count() }",
   pipeline_ids:
      "run: pipeline_now -> { group_by: deal_id; order_by: deal_id }",
   funnel_deals: "run: funnel_deals -> { aggregate: n is count() }",
};

interface Fixture {
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   service: MaterializationService;
   cleanup: () => Promise<void>;
}

async function fixture(warehouse: unknown, text: string): Promise<Fixture> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "shared-name-collation-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, PKG),
      { "m.malloy": text },
      { manifest: { name: PKG, description: "fixture" } },
   );
   const db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV, envPath],
   );
   const repo = createDuckDBRepository(db);
   const env = await Environment.create(ENV, envPath, [warehouse as never], []);
   await env.addPackage(PKG);
   const pkg = await env.getPackage(parsePackageName(PKG), false);
   const service = serviceOverEnvironment(
      () => repo,
      () => env,
   );
   return {
      env,
      pkg,
      repo,
      service,
      cleanup: async () => {
         await env.closeAllConnections().catch(() => undefined);
         await db.close();
         fs.rmSync(root, { recursive: true, force: true });
      },
   };
}

async function build(f: Fixture): Promise<Materialization> {
   const created = await f.service.createMaterialization(ENV, PKG, {});
   const running = (
      f.service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 60_000;
   for (;;) {
      const m = await f.repo.getMaterializationById(created.id);
      if (
         m &&
         ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
      ) {
         while (running.has(created.id)) {
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await f.repo.getMaterializationById(created.id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error("run never settled");
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

async function ask(
   f: Fixture,
   query: string,
   givens?: Record<string, unknown>,
): Promise<{ rows: unknown; kind?: string }> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query, ...(givens ? { givens } : {}) } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   return {
      rows: JSON.stringify(result.compactResult),
      kind: result.path?.servedBy?.kind,
   };
}

const CASES: {
   locale: string;
   text: string;
   pipeline: "materialized" | "live";
}[] = [
   { locale: "C", text: MODEL, pipeline: "materialized" },
   { locale: "en_US.utf8", text: MODEL, pipeline: "materialized" },
   // A case-insensitive join key: live matches `D1` to `d1`; a stored copy
   // compares bytes. Refused, answered live.
   { locale: "en_US.utf8+ci", text: CI_MODEL, pipeline: "live" },
];

for (const c of CASES) {
   describe.skipIf(!POSTGRES_URL)(
      `a null test read through a join on a computed string key (Postgres ${c.locale})`,
      () => {
         const live: Record<string, { rows: unknown; kind?: string }> = {};
         const served: Record<string, { rows: unknown; kind?: string }> = {};
         let cleanup: () => Promise<void> = async () => {};
         beforeAll(async () => {
            const { restore } = installSettings({
               env: {
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "off",
               },
            });
            await freshLoadPool();
            const db = `p6_${process.pid}_${Date.now()}`;
            const admin = new PostgresConnection({
               name: "admin",
               connectionString: POSTGRES_URL!,
            });
            await admin.runSQL(
               `CREATE DATABASE ${db} LOCALE '${c.locale.split("+")[0]}' ENCODING 'UTF8' TEMPLATE template0`,
            );
            const parsed = new URL(POSTGRES_URL!);
            if (c.locale.endsWith("+ci")) {
               const conn = new PostgresConnection({
                  name: "c",
                  connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${db}`,
               });
               await conn.runSQL(
                  "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
               );
               await conn.close();
            }
            const f = await fixture(
               {
                  name: "warehouse",
                  type: "postgres",
                  postgresConnection: {
                     host: parsed.hostname,
                     port: Number(parsed.port || 5432),
                     userName: decodeURIComponent(parsed.username),
                     password: decodeURIComponent(parsed.password),
                     databaseName: db,
                  },
               },
               c.text,
            );
            cleanup = async () => {
               await f.cleanup();
               await admin
                  .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                  .catch(() => undefined);
               await admin.close();
               restore();
               await freshLoadPool();
            };
            for (const [n, q] of Object.entries(QUERIES))
               live[n] = await ask(f, q);
            const run = await build(f);
            expect(run.status).toBe("MANIFEST_FILE_READY");
            for (const [n, q] of Object.entries(QUERIES))
               served[n] = await ask(f, q);
         }, 300_000);
         afterAll(async () => cleanup());

         for (const name of Object.keys(QUERIES)) {
            it(`${name}: the answer equals live`, () => {
               expect(served[name].rows).toEqual(live[name].rows);
            });
         }
         it(`answers the null test through the join ${c.pipeline === "live" ? "live" : "from the tables"}`, () => {
            expect(served.pipeline_now.kind).toBe(c.pipeline);
            expect(served.funnel_deals.kind).toBe("materialized");
         });
      },
   );
}
