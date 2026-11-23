// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Two stored database sources joined, each holding a `name` column under a
 * different collation, through the real package bind under shadow
 * admission: read admission's probes and its lookups judge every answer
 * alike, and each answer equals live.
 *
 * A read names its column, not the table that holds it, so `name` is held
 * by both tables an answer reads. Neither side may take one table's
 * collation for the other's (a lookup keyed by the bare name kept whichever
 * table came last): each compares the name under every collation a table
 * read holds it under, and fails closed when they differ. Postgres under
 * `C` and an ICU collation; DuckDB under binary and `NOCASE`.
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
import {
   clearParityDrifts,
   expectNoDrift,
} from "../../src/test_helpers/admission_parity";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

/** The model, with each side's `name` under the collation `collate` gives. */
const model = (
   collateA: string,
   collateB: string,
) => `##! experimental { persistence }
#(materialize) name="sn_a"
source: a_tab is warehouse.sql("""
  SELECT id, CAST(name AS VARCHAR) ${collateA} AS name FROM (VALUES
    (1, 'b'), (2, 'B'), (3, 'a'), (4, 'c')
  ) AS t(id, name)
""") extend { primary_key: id }

#(materialize) name="sn_b"
source: b_tab is warehouse.sql("""
  SELECT id, CAST(name AS VARCHAR) ${collateB} AS name FROM (VALUES
    (1, 'B'), (2, 'b'), (3, 'c'), (4, 'a')
  ) AS t(id, name)
""") extend { primary_key: id }

source: j is a_tab extend {
  join_one: bb is b_tab on id = bb.id
}
`;

const QUERIES: Record<string, string> = {
   "an equality on the entry's name": `run: j -> { where: name = 'b'; select: id; order_by: id }`,
   "an equality on the joined name": `run: j -> { where: bb.name = 'b'; select: id; order_by: id }`,
   "an ordering on the entry's name": `run: j -> { where: name > 'a'; select: id; order_by: id }`,
   "a grouping by both names": `run: j -> { group_by: n is name, m is bb.name; aggregate: c is count(); order_by: n, m }`,
   "the entry alone": `run: a_tab -> { where: name = 'b'; select: id; order_by: id }`,
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
): Promise<{ rows: unknown; kind?: string }> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   return {
      rows: JSON.stringify(result.compactResult),
      kind: result.path?.servedBy?.kind,
   };
}

const tiers: {
   label: string;
   collateA: string;
   collateB: string;
   warehouse: () => Promise<{ config: unknown; drop?: () => Promise<void> }>;
}[] = [
   {
      label: "DuckDB",
      collateA: "",
      collateB: "COLLATE NOCASE",
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         },
      }),
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              collateA: `COLLATE "C"`,
              collateB: `COLLATE "und-x-icu"`,
              warehouse: async () => {
                 const db = `sn_coll_${process.pid}_${Date.now()}`;
                 const admin = new PostgresConnection({
                    name: "admin",
                    connectionString: POSTGRES_URL,
                 });
                 await admin.runSQL(
                    `CREATE DATABASE ${db} LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0`,
                 );
                 const parsed = new URL(POSTGRES_URL);
                 return {
                    config: {
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
                    drop: async () => {
                       await admin
                          .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                          .catch(() => undefined);
                       await admin.close();
                    },
                 };
              },
           },
        ]
      : []),
];

for (const tier of tiers) {
   describe(`two joined stored sources sharing a column name under different collations (${tier.label})`, () => {
      let restore: () => void;
      let f: Fixture;
      let drop: (() => Promise<void>) | undefined;
      const live: Record<string, unknown> = {};
      const served: Record<string, { rows: unknown; kind?: string }> = {};
      const drift: Record<string, string | undefined> = {};
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
               PUBLISHER_ADMISSION_MODE: "shadow",
            },
         }).restore;
         await freshLoadPool();
         const w = await tier.warehouse();
         drop = w.drop;
         f = await fixture(w.config, model(tier.collateA, tier.collateB));
         for (const [name, q] of Object.entries(QUERIES)) {
            live[name] = (await ask(f, q)).rows;
         }
         const run = await build(f);
         expect(run.status).toBe("MANIFEST_FILE_READY");
         clearParityDrifts();
         for (const [name, q] of Object.entries(QUERIES)) {
            served[name] = await ask(f, q);
            try {
               expectNoDrift();
               drift[name] = undefined;
            } catch (e) {
               drift[name] = (e as Error).message;
            }
         }
      }, 300_000);
      afterAll(async () => {
         await f?.cleanup();
         await drop?.();
         restore();
         await freshLoadPool();
      });

      for (const name of Object.keys(QUERIES)) {
         it(`${name}: probe and lookup agree, and the answer equals live`, () => {
            expect(drift[name]).toBeUndefined();
            expect(served[name].rows).toEqual(live[name]);
         });
      }

      it("serves the entry alone from its table", () => {
         expect(served["the entry alone"].kind).toBe("materialized");
      });
   });
}
