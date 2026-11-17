// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * End to end, through the real package and environment: a database rollup or
 * persisted view altered after its build by a move inside one Malloy type
 * that can change a value is never served from its table. Every answer equals
 * live's, read before the first build, when nothing is built.
 *
 * - C1: a gate column on a gated rollup, a BIGINT past 2^53 altered to
 *   DOUBLE: two tenants' values would collapse into one, and the served
 *   answer would hold the other tenant's row. The column is left out, and
 *   the gated query answers live with its own tenant's rows.
 * - H1: a rollup partial and a persisted view column altered from
 *   DECIMAL(10,1) to DECIMAL(10,0): served, they would answer the rounded
 *   `9`, where live answers `9.4`. Left out, they answer live.
 * - H2: the gate column altered to text, which no read gives as live's
 *   number: the gated rollup declines (`MISSING_FIELD`) and the query answers
 *   live, never a denial.
 * - M1: a rollup read through its typed source after a lossless widening
 *   reports `servedFrom: "database"`, the tier its path report names.
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
const TENANT = 9007199254740992;
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

/** The moves the scenario makes, each spelled by the warehouse's own types. */
type Move = "double" | "text" | "decimal10_0" | "widerCount" | "bigint";

interface Tier {
   label: string;
   warehouse: () => Promise<{ config: unknown; drop?: () => Promise<void> }>;
   alter: (table: string, column: string, move: Move) => string;
   /** Lists a table's column names, as the dialect's connection returns rows. */
   columnsSql: (table: string) => string;
}

const DUCKDB_TYPES: Record<Move, string> = {
   double: "DOUBLE",
   text: "VARCHAR",
   decimal10_0: "DECIMAL(10,0)",
   // A count's partial is a BIGINT: HUGEINT holds every value it held.
   widerCount: "HUGEINT",
   bigint: "BIGINT",
};

const POSTGRES_TYPES: Record<Move, string> = {
   double: "double precision",
   text: "varchar",
   decimal10_0: "numeric(10,0)",
   widerCount: "numeric(30,0)",
   bigint: "bigint",
};

const tiers: Tier[] = [
   {
      label: "DuckDB",
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         },
      }),
      alter: (table, column, move) =>
         `ALTER TABLE ${table} ALTER "${column}" TYPE ${DUCKDB_TYPES[move]}`,
      columnsSql: (table) =>
         `SELECT column_name FROM information_schema.columns WHERE table_name = '${table}'`,
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              warehouse: async () => {
                 const db = `drift_loss_${process.pid}_${Date.now()}`;
                 const admin = new PostgresConnection({
                    name: "admin",
                    connectionString: POSTGRES_URL,
                 });
                 await admin.runSQL(`CREATE DATABASE ${db}`);
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
              alter: (table: string, column: string, move: Move) =>
                 `ALTER TABLE ${table} ALTER COLUMN "${column}" TYPE ${POSTGRES_TYPES[move]} USING "${column}"::${POSTGRES_TYPES[move]}`,
              // Wrapped as Malloy's Postgres connection reads every result.
              columnsSql: (table: string) =>
                 `SELECT row_to_json(c) AS row FROM (SELECT column_name FROM information_schema.columns WHERE table_name = '${table}') c`,
           } satisfies Tier,
        ]
      : []),
];

const MODEL = `##! experimental { persistence givens }
given: TENANT :: number
given: ORG :: number
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    ('a', CAST(9.4 AS DECIMAL(10,1)), CAST(9007199254740993 AS DECIMAL(20,0))),
    ('b', CAST(2.5 AS DECIMAL(10,1)), CAST(9007199254740992 AS DECIMAL(20,0)))
  ) AS t(cat, amt, big)
""") extend {
  measure: top is amt.max()
}

source: viewed is raw extend {
  #@ persist
  view: v_top is { group_by: cat; aggregate: top; order_by: 1 }
}

#(access_filter) big = $TENANT
source: gated is raw extend {
  #(preaggregate) grain=["cat", "big"]
  measure: gtop is amt.max()
}

source: rolled is raw extend {
  #(preaggregate) grain=["cat", "big"]
  measure: rtop is amt.max()
  #(preaggregate) grain=["cat", "big"]
  measure: rn is count()
}

source: mat_raw is warehouse.sql("""
  SELECT * FROM (VALUES
    ('a', CAST(9.4 AS DECIMAL(10,1))),
    ('b', CAST(2.5 AS DECIMAL(10,1)))
  ) AS t(cat, amt)
""")

source: wmat_raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', CAST(1 AS INTEGER)), ('b', CAST(2 AS INTEGER))) AS t(cat, n)
""")

source: gmat_raw is warehouse.sql("""
  SELECT * FROM (VALUES
    ('a', CAST(10.0 AS DECIMAL(10,1))),
    ('b', CAST(10.4 AS DECIMAL(10,1)))
  ) AS t(cat, org)
""")

#(materialize)
source: mat is mat_raw extend {
  measure: m is amt.max()
}

#(materialize)
source: wmat is wmat_raw extend {
  measure: s is n.sum()
}

#(access_filter) org = $ORG
#(materialize)
source: gmat is gmat_raw extend {
  measure: c is count()
}
`;

const QUERIES: Record<string, { query: string; givens?: object }> = {
   view: { query: "run: viewed -> v_top" },
   rollup: {
      query: "run: rolled -> { group_by: cat; aggregate: rtop; order_by: cat }",
   },
   rollup_big: {
      query: `run: rolled -> { where: big = ${TENANT}; group_by: cat; aggregate: rn; order_by: cat }`,
   },
   gated: {
      query: "run: gated -> { group_by: cat; aggregate: gtop; order_by: cat }",
      givens: { TENANT },
   },
   rollup_count: {
      query: "run: rolled -> { group_by: cat; aggregate: rn; order_by: cat }",
   },
   mat_narrow: {
      query: "run: mat -> { group_by: cat; aggregate: m; order_by: cat }",
   },
   mat_widen: {
      query: "run: wmat -> { group_by: cat; aggregate: s; order_by: cat }",
   },
   gmat: {
      query: "run: gmat -> { group_by: cat; aggregate: c; order_by: cat }",
      givens: { ORG: 10 },
   },
};

interface Fixture {
   root: string;
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   db: DuckDBConnection;
   service: MaterializationService;
   cleanup: () => Promise<void>;
}

async function fixture(warehouse: unknown): Promise<Fixture> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "drift-value-loss-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, PKG),
      { "m.malloy": MODEL },
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
      root,
      env,
      pkg,
      repo,
      db,
      service,
      cleanup: async () => {
         await env.closeAllConnections().catch(() => undefined);
         await db.close();
         fs.rmSync(root, { recursive: true, force: true });
      },
   };
}

/** One auto-run, to its settled record. */
async function build(f: Fixture): Promise<Materialization> {
   const created = await f.service.createMaterialization(ENV, PKG, {});
   const running = (
      f.service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 120_000;
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

interface Answer {
   rows?: Record<string, unknown>[];
   /** The error a denied or failed query threw. */
   error?: string;
   kind?: string;
   tier?: string;
   servedFrom?: string;
}

/** The answer to `name`'s query through the package, with what served it. */
async function ask(f: Fixture, name: string): Promise<Answer> {
   const { query, givens } = QUERIES[name];
   try {
      const result = await f.pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query, ...(givens ? { givens } : {}) } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      const report = result.path as
         | { servedBy?: { kind?: string; tier?: string } }
         | undefined;
      return {
         rows: JSON.parse(JSON.stringify(result.compactResult)) as Record<
            string,
            unknown
         >[],
         kind: report?.servedBy?.kind,
         tier: report?.servedBy?.tier,
         servedFrom: (result as { servedFrom?: string }).servedFrom,
      };
   } catch (err) {
      return { error: err instanceof Error ? err.message : String(err) };
   }
}

/** The column names of a table in the scenario's warehouse. */
async function columnsOf(
   warehouse: { runSQL(sql: string): Promise<{ rows: unknown[] }> },
   tier: Tier,
   table: string,
): Promise<string[]> {
   const unqualified = table.split(".").pop()!.replace(/"/g, "");
   const rows = (await warehouse.runSQL(tier.columnsSql(unqualified)))
      .rows as Record<string, unknown>[];
   return rows.map((c) => {
      const row = (c.row ?? c) as Record<string, unknown>;
      return String(row.column_name);
   });
}

/**
 * Build, then alter each table out of band and bind again: `big` to
 * `bigTo`, the `top` and `rtop` partial to DECIMAL(10,0), and the `rn`
 * partial widened to HUGEINT (a move that loses nothing).
 */
function scenario(tier: Tier, bigTo: "double" | "text") {
   const live: Record<string, Answer> = {};
   const before: Record<string, Answer> = {};
   let f: Fixture;
   let drop: (() => Promise<void>) | undefined;
   let restore: () => void;
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
      f = await fixture(w.config);
      for (const name of Object.keys(QUERIES)) live[name] = await ask(f, name);
      const run = await build(f);
      expect(run.status).toBe("MANIFEST_FILE_READY");
      for (const name of Object.keys(QUERIES)) {
         before[name] = await ask(f, name);
      }
      const warehouse = await f.env.getMalloyConnection("warehouse");
      const tables = Object.values(
         (
            run.manifest as {
               entries: Record<string, { physicalTableName: string }>;
            }
         ).entries,
      ).map((e) => e.physicalTableName);
      for (const table of tables) {
         const columns = await columnsOf(warehouse, tier, table);
         if (columns.includes("top")) {
            await warehouse.runSQL(tier.alter(table, "top", "decimal10_0"));
         }
         for (const c of columns) {
            if (c.startsWith("rtop") && c.includes("partial")) {
               await warehouse.runSQL(tier.alter(table, c, "decimal10_0"));
            }
            if (c.startsWith("rn") && c.includes("partial")) {
               await warehouse.runSQL(tier.alter(table, c, "widerCount"));
            }
         }
         // The served copy's tables: `amt` rounded, `n` widened, and the
         // gate column `org` rounded so tenants 10.0 and 10.4 both read 10.
         if (columns.length === 2 && columns.includes("amt")) {
            await warehouse.runSQL(tier.alter(table, "amt", "decimal10_0"));
         }
         if (columns.length === 2 && columns.includes("n")) {
            await warehouse.runSQL(tier.alter(table, "n", "bigint"));
         }
         if (columns.includes("org")) {
            await warehouse.runSQL(tier.alter(table, "org", "decimal10_0"));
         }
         if (columns.includes("big")) {
            await warehouse.runSQL(tier.alter(table, "big", bigTo));
         }
      }
      await f.pkg.reloadAllModels(
         f.pkg.materialization().boundColocatedEntries() ?? {},
      );
   }, 300_000);
   afterAll(async () => {
      await f?.cleanup();
      await drop?.();
      restore();
      await freshLoadPool();
   });
   return {
      live,
      before,
      ask: (name: string) => ask(f, name),
   };
}

for (const tier of tiers) {
   describe(`C1, H1, M1: a value-losing move inside one type is left out, a widening read as it is (${tier.label})`, () => {
      const s = scenario(tier, "double");

      it("served every query from a table, equal to live, before the alter", () => {
         for (const name of Object.keys(QUERIES)) {
            expect({ name, rows: s.before[name].rows }).toEqual({
               name,
               rows: s.live[name].rows,
            });
            expect({ name, kind: s.before[name].kind }).not.toEqual({
               name,
               kind: "live",
            });
         }
      });

      it("C1: the gated rollup answers live with the caller's own tenant, never the other's", async () => {
         const after = await s.ask("gated");
         expect(after.error).toBeUndefined();
         expect(after.rows).toEqual(s.live.gated.rows);
         expect(after.rows?.map((r) => r.cat)).toEqual(["b"]);
         expect(after.kind).toBe("live");
      });

      it("C1: a filter on the rounded column answers live", async () => {
         const after = await s.ask("rollup_big");
         expect(after.rows).toEqual(s.live.rollup_big.rows);
         expect(after.kind).toBe("live");
      });

      for (const name of ["view", "rollup"]) {
         it(`H1: ${name} over a column rounded to DECIMAL(10,0) answers live's 9.4, not 9`, async () => {
            const after = await s.ask(name);
            expect(after.rows).toEqual(s.live[name].rows);
            expect(after.kind).toBe("live");
         });
      }

      it("M1: a widened partial is still served, read through its typed source, from the database tier", async () => {
         const after = await s.ask("rollup_count");
         expect(after.rows).toEqual(s.live.rollup_count.rows);
         expect(after.kind).toBe("rollup");
         expect(after.tier).toBe("database");
         expect(after.servedFrom).toBe("database");
      });
   });

   describe(`the served copy of a #(materialize) table: a narrowing is left out, a widening read as it is (${tier.label})`, () => {
      const s = scenario(tier, "double");

      it("served each from its table, equal to live, before the alter", () => {
         for (const name of ["mat_narrow", "mat_widen", "gmat"]) {
            expect({ name, rows: s.before[name].rows }).toEqual({
               name,
               rows: s.live[name].rows,
            });
            expect({ name, kind: s.before[name].kind }).not.toEqual({
               name,
               kind: "live",
            });
         }
      });

      it("a column rounded to DECIMAL(10,0): answers live's 9.4, not 9", async () => {
         const after = await s.ask("mat_narrow");
         expect(after.rows).toEqual(s.live.mat_narrow.rows);
         expect(after.kind).toBe("live");
      });

      it("a gate column rounded so two tenants read alike: answers live, the caller's own tenant only", async () => {
         const after = await s.ask("gmat");
         expect(after.error).toBeUndefined();
         expect(after.rows).toEqual(s.live.gmat.rows);
         expect(after.rows?.map((r) => r.cat)).toEqual(["a"]);
      });

      it("a column widened from INTEGER to BIGINT: still served, equal to live", async () => {
         const after = await s.ask("mat_widen");
         expect(after.rows).toEqual(s.live.mat_widen.rows);
         expect(after.kind).not.toBe("live");
      });
   });

   describe(`H2: a gate column no read gives as live's value declines the gated rollup, never denies (${tier.label})`, () => {
      const s = scenario(tier, "text");

      it("the gated query answers live, equal to live", async () => {
         const after = await s.ask("gated");
         expect(after.error).toBeUndefined();
         expect(after.rows).toEqual(s.live.gated.rows);
         expect(after.kind).toBe("live");
      });
   });
}
