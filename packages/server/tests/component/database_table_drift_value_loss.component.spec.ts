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

const MODEL = `##! experimental { persistence givens }
given: TENANT :: number
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

async function fixture(): Promise<Fixture> {
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
   const env = await Environment.create(
      ENV,
      envPath,
      [
         {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         } as never,
      ],
      [],
   );
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
   table: string,
): Promise<string[]> {
   const unqualified = table.split(".").pop()!.replace(/"/g, "");
   const rows = (
      await warehouse.runSQL(
         `SELECT column_name FROM information_schema.columns WHERE table_name = '${unqualified}'`,
      )
   ).rows as { column_name: string }[];
   return rows.map((r) => r.column_name);
}

/**
 * Build, then alter each table out of band and bind again: `big` to
 * `bigTo`, the `top` and `rtop` partial to DECIMAL(10,0), and the `rn`
 * partial widened to HUGEINT (a move that loses nothing).
 */
function scenario(bigTo: "DOUBLE" | "VARCHAR") {
   const live: Record<string, Answer> = {};
   const before: Record<string, Answer> = {};
   let f: Fixture;
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
      f = await fixture();
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
         const columns = await columnsOf(warehouse, table);
         if (columns.includes("top")) {
            await warehouse.runSQL(
               `ALTER TABLE ${table} ALTER "top" TYPE DECIMAL(10,0)`,
            );
         }
         for (const c of columns) {
            if (c.startsWith("rtop") && c.includes("partial")) {
               await warehouse.runSQL(
                  `ALTER TABLE ${table} ALTER "${c}" TYPE DECIMAL(10,0)`,
               );
            }
            if (c.startsWith("rn") && c.includes("partial")) {
               await warehouse.runSQL(
                  `ALTER TABLE ${table} ALTER "${c}" TYPE HUGEINT`,
               );
            }
         }
         if (columns.includes("big")) {
            await warehouse.runSQL(
               `ALTER TABLE ${table} ALTER "big" TYPE ${bigTo}`,
            );
         }
      }
      await f.pkg.reloadAllModels(
         f.pkg.materialization().boundColocatedEntries() ?? {},
      );
   }, 300_000);
   afterAll(async () => {
      await f?.cleanup();
      restore();
      await freshLoadPool();
   });
   return {
      live,
      before,
      ask: (name: string) => ask(f, name),
   };
}

describe("C1, H1, M1: a value-losing move inside one type is left out, a widening read as it is", () => {
   const s = scenario("DOUBLE");

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
      expect(after.rows).toEqual([{ cat: "b", gtop: 2.5 }]);
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

describe("H2: a gate column no read gives as live's value declines the gated rollup, never denies", () => {
   const s = scenario("VARCHAR");

   it("the gated query answers live, equal to live", async () => {
      const after = await s.ask("gated");
      expect(after.error).toBeUndefined();
      expect(after.rows).toEqual(s.live.gated.rows);
      expect(after.kind).toBe("live");
   });
});
