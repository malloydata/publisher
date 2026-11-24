// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Two stored database sources joined, each holding a `name` column under a
 * different collation, through the real package bind: each answer equals
 * live.
 *
 * A read names its column, not the table that holds it, so `name` is held
 * by both tables an answer reads. Neither side may take one table's
 * collation for the other's (a lookup keyed by the bare name kept whichever
 * table came last): each compares the name under every collation a table
 * read holds it under, and fails closed when they differ. Postgres under
 * `C` and an ICU collation; DuckDB under binary and `NOCASE`.
 *
 * The same tables, and a gated source beside them, are judged with no
 * catalog read, no extra compile and no walk of the IR per query: what each
 * source compares under was read at the bind, and what each stored table
 * holds was recorded by its build.
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
import {
   type MetricsHarness,
   startMetricsHarness,
} from "../../src/test_helpers/metrics_harness";
import { spyAdmissionIO } from "../../src/test_helpers/admission_io_spy";
import { resetMaterializationTelemetryForTesting } from "../../src/materialization_metrics";
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
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
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
         for (const [name, q] of Object.entries(QUERIES)) {
            served[name] = await ask(f, q);
         }
      }, 300_000);
      afterAll(async () => {
         await f?.cleanup();
         await drop?.();
         restore();
         await freshLoadPool();
      });

      for (const name of Object.keys(QUERIES)) {
         it(`${name}: the answer equals live`, () => {
            expect(served[name].rows).toEqual(live[name]);
         });
      }

      it("serves the entry alone from its table", () => {
         expect(served["the entry alone"].kind).toBe("materialized");
      });
   });
}

/** {@link model}, with a gated source over a stored table beside it. */
const gatedModel = (collateA: string, collateB: string) =>
   model(collateA, collateB).replace(
      "##! experimental { persistence }",
      "##! experimental { givens persistence access_modifiers }\ngiven: NAME :: string",
   ) +
   `
#(access_filter) name = $NAME
#(materialize) name="sn_g"
source: g_tab is warehouse.sql("""
  SELECT id, CAST(name AS VARCHAR) ${collateA} AS name FROM (VALUES
    (1, 'b'), (2, 'B'), (3, 'a'), (4, 'b')
  ) AS t(id, name)
""") extend { primary_key: id }
`;

const GATED: [string, Record<string, unknown> | undefined][] = [
   ...Object.values(QUERIES).map((q) => [q, undefined] as [string, undefined]),
   [
      "run: g_tab -> { group_by: name; aggregate: c is count(); order_by: name }",
      { NAME: "b" },
   ],
   ["run: g_tab -> { where: id > 1; select: id; order_by: id }", { NAME: "B" }],
];

for (const tier of tiers) {
   describe(`admission over stored database tables (${tier.label})`, () => {
      let restore: () => void;
      let f: Fixture;
      let drop: (() => Promise<void>) | undefined;
      let metrics: MetricsHarness;
      let io: { calls: readonly string[]; seen: number; decisions: number };
      const live: unknown[] = [];
      const served: { rows: unknown; kind?: string }[] = [];
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         const w = await tier.warehouse();
         drop = w.drop;
         f = await fixture(w.config, gatedModel(tier.collateA, tier.collateB));
         for (const [q, g] of GATED) live.push((await ask(f, q, g)).rows);
         const run = await build(f);
         expect(run.status).toBe("MANIFEST_FILE_READY");
         metrics = await startMetricsHarness();
         resetMaterializationTelemetryForTesting();
         const spy = spyAdmissionIO();
         try {
            for (const [q, g] of GATED) served.push(await ask(f, q, g));
         } finally {
            spy.restore();
         }
         io = {
            calls: [...spy.calls],
            seen: spy.seen(),
            decisions: spy.decisions(),
         };
      }, 300_000);
      afterAll(async () => {
         await metrics?.shutdown();
         resetMaterializationTelemetryForTesting();
         await f?.cleanup();
         await drop?.();
         restore();
         await freshLoadPool();
      });

      it("answers every query as live does, the gated source from its table", () => {
         expect(served.map((s) => s.rows)).toEqual(live);
         expect(served.at(-1)?.kind).toBe("materialized");
      });

      it("runs no SQL, compiles nothing and walks nothing to judge them", async () => {
         // The spy saw the queries' own SQL, and admission decided.
         expect(io.seen).toBeGreaterThan(0);
         expect(io.decisions).toBeGreaterThan(0);
         expect(io.calls).toEqual([]);
         expect(
            await metrics.collectCounter("publisher_query_walks_total"),
         ).toBe(0);
      });
   });
}
