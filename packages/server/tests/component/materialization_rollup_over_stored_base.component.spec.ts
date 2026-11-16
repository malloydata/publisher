// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A colocated rollup over a colocated `#@ persist` base builds from the
 * base's stored table when the run holds it, rather than from the warehouse,
 * and stores the same rows. End to end through a real Environment and
 * MaterializationService on DuckDB:
 *
 *  - built in the same run as its base, the rollup reads the base's table
 *    (`upstreamReuse: "reused"`), and its rows equal the rows of the rollup
 *    built from the warehouse (its planned SQL), which equal live's answer;
 *  - built in a run that rebuilds only the rollup, it reads the base's table
 *    carried from the run before;
 *  - with no base table in the run, it builds from the warehouse as planned;
 *  - a rollup in a `storage=` destination, or over a base that is not
 *    persisted, is never offered the base's table.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import type { EnvironmentStore } from "../../src/service/environment_store";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";

/** The raw rows, read only through `raw`, so a build that reads them names RAW_MARK. */
const RAW_MARK = "raw_rows_mark";
const RAW = `duckdb.sql("""
  SELECT * FROM (VALUES
    ('a', 'x', 1.5, '${RAW_MARK}'), ('a', 'y', 2.0, '${RAW_MARK}'),
    ('b', 'x', 4.25, '${RAW_MARK}'), ('b', 'x', NULL, '${RAW_MARK}'),
    ('c', 'z', -3.0, '${RAW_MARK}')
  ) AS t(cat, sub, amt, mark)
""")`;

function modelOf(rollupLine = '#@ preaggregate grain="cat"'): string {
   return `##! experimental.persistence
source: raw is ${RAW}

#@ persist
source: daily is raw -> {
  group_by: cat, sub
  aggregate: total is amt.sum(), n is count(), lo is amt.min()
} extend {
  ${rollupLine}
  measure: grand is total.sum()
  ${rollupLine}
  measure: rows is n.sum()
  ${rollupLine}
  measure: least is lo.min()
}

source: plain is raw extend {
  #@ preaggregate grain="cat"
  measure: plain_total is amt.sum()
}
`;
}

const ROLLUP_QUERY =
   "run: daily -> { group_by: cat; aggregate: grand, rows, least; order_by: cat }";

type Entry = {
   sourceName: string;
   sourceEntityId: string;
   physicalTableName: string;
   upstreamReuse?: string;
   origin?: string;
};

describe("a colocated rollup over a colocated #@ persist base", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let statements: string[];
   let spy: { mockRestore(): void } | undefined;

   async function setUp(model = modelOf()): Promise<void> {
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": model },
         { manifest: { name: PKG, description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, []);
      await env.addPackage(PKG);
      service = new MaterializationService({
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => env,
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore);
   }

   beforeEach(async () => {
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "rollup-over-base-")),
      );
      envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      statements = [];
      const runSQL = MalloyDuckDBConnection.prototype.runSQL;
      spy = spyOn(
         MalloyDuckDBConnection.prototype,
         "runSQL",
      ).mockImplementation(function (
         this: MalloyDuckDBConnection,
         sql: string,
         ...rest: unknown[]
      ) {
         statements.push(sql);
         return (runSQL as (...a: unknown[]) => unknown).call(
            this,
            sql,
            ...rest,
         );
      } as never);
   });

   afterEach(async () => {
      spy?.mockRestore();
      await env?.closeAllConnections().catch(() => undefined);
      await db.close();
      fs.rmSync(root, { recursive: true, force: true });
   });

   async function build(
      options: Record<string, unknown> = {},
   ): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, options);
      const deadline = Date.now() + 60_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   function entries(m: Materialization): Entry[] {
      return Object.values(
         (m.manifest as { entries: Record<string, Entry> }).entries,
      );
   }
   const rollupOf = (m: Materialization, base: string) => {
      const hit = entries(m).find(
         (e) =>
            e.origin === "preaggregate" &&
            e.sourceName.startsWith(`${base}__rollup__`),
      );
      if (!hit) throw new Error(`no rollup of ${base}`);
      return hit;
   };

   /** The statement that created a table, from the run's statements. */
   function createdBy(table: string): string {
      const hit = statements.find(
         (s) => /\bCREATE\b/i.test(s) && s.includes(table),
      );
      if (!hit) throw new Error(`no CREATE of ${table}`);
      return hit;
   }

   async function connection(): Promise<MalloyDuckDBConnection> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      return (await pkg.getMalloyConnection(
         "duckdb",
      )) as MalloyDuckDBConnection;
   }

   async function rowsOf(sql: string): Promise<Record<string, unknown>[]> {
      const result = await (await connection()).runSQL(sql);
      return (result.rows as Record<string, unknown>[])
         .map((r) =>
            Object.fromEntries(
               Object.entries(r).map(([k, v]) => [
                  k,
                  typeof v === "bigint" ? Number(v) : v,
               ]),
            ),
         )
         .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
   }

   async function answer(query: string) {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const result = await pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         rows: (result.compactResult as Record<string, unknown>[]).map((r) =>
            Object.fromEntries(
               Object.entries(r).map(([k, v]) => [
                  k,
                  typeof v === "bigint" ? Number(v) : v,
               ]),
            ),
         ),
         servedBy: result.path?.servedBy?.kind ?? "live",
      };
   }

   it("builds from the base's table in the run that builds the base, storing the warehouse build's rows", async () => {
      await setUp();
      const live = await answer(ROLLUP_QUERY);
      const m = await build();
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const base = entries(m).find((e) => e.sourceName === "daily")!;
      const rollup = rollupOf(m, "daily");
      expect(rollup.upstreamReuse).toBe("reused");
      // The rollup's build reads the base's table, never the raw rows.
      const create = createdBy(rollup.physicalTableName);
      expect(create).toContain(base.physicalTableName);
      expect(create).not.toContain(RAW_MARK);

      // Parity: the rows the warehouse build (the planned SQL) stores.
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const planned = Object.values(pkg.getBuildPlan()!.sources).find(
         (s) => s.sourceEntityId === rollup.sourceEntityId,
      )!;
      expect(planned.sql).toContain(RAW_MARK);
      expect(await rowsOf(`SELECT * FROM ${rollup.physicalTableName}`)).toEqual(
         await rowsOf(planned.sql),
      );

      // And the served answer is live's.
      const served = await answer(ROLLUP_QUERY);
      expect(served.servedBy).toBe("rollup");
      expect(served.rows).toEqual(live.rows);
   });

   it("reads the base's table carried from an earlier run when only the rollup is rebuilt", async () => {
      await setUp();
      const first = await build();
      expect(first.status).toBe("MANIFEST_FILE_READY");
      const base = entries(first).find((e) => e.sourceName === "daily")!;
      // Another rollup at a new grain: the base is unchanged and carried.
      fs.writeFileSync(
         path.join(envPath, PKG, "m.malloy"),
         modelOf('#@ preaggregate grain="sub"'),
      );
      await env.getPackage(parsePackageName(PKG), true);
      statements.length = 0;
      const second = await build();
      expect([second.status, second.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      expect(second.metadata).toMatchObject({ sourcesReused: 2 });
      const rollup = rollupOf(second, "daily");
      expect(rollup.upstreamReuse).toBe("reused");
      const create = createdBy(rollup.physicalTableName);
      expect(create).toContain(base.physicalTableName);
      expect(create).not.toContain(RAW_MARK);
      const served = await answer(
         "run: daily -> { group_by: sub; aggregate: grand, rows, least; order_by: sub }",
      );
      expect(served.servedBy).toBe("rollup");
      expect(served.rows).toEqual([
         { sub: "x", grand: 5.75, rows: 3, least: 1.5 },
         { sub: "y", grand: 2, rows: 1, least: 2 },
         { sub: "z", grand: -3, rows: 1, least: -3 },
      ]);
   });

   it("builds from the warehouse when the run holds no table of the base", async () => {
      await setUp();
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const rollupName = Object.values(pkg.getBuildPlan()!.sources).find(
         (s) => s.origin === "preaggregate" && s.name.startsWith("daily__"),
      )!.name;
      const m = await build({ sourceNames: [rollupName] });
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const rollup = rollupOf(m, "daily");
      expect(entries(m).map((e) => e.sourceName)).toEqual([rollup.sourceName]);
      // Its `upstreamReuse` reads `reused` here too: the classification reads
      // the author's text, where the base is persisted. The build reads the
      // raw rows.
      expect(createdBy(rollup.physicalTableName)).toContain(RAW_MARK);
      const served = await answer(ROLLUP_QUERY);
      expect(served.servedBy).toBe("rollup");
   });

   it("builds a rollup over a base that is not persisted from the warehouse", async () => {
      await setUp();
      const m = await build();
      const rollup = rollupOf(m, "plain");
      expect(rollup.upstreamReuse).toBeUndefined();
      expect(createdBy(rollup.physicalTableName)).toContain(RAW_MARK);
   });
});
