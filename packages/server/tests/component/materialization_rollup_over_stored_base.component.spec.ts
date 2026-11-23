// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database rollup over a database `#@ persist` base builds from the
 * base's stored table when the run holds it, rather than from the warehouse,
 * and stores the same rows. End to end through a real Environment and
 * MaterializationService on DuckDB:
 *
 *  - built in the same run as its base, the rollup reads the base's table
 *    (`upstreamReuse: "reused"`), and its rows equal the rows of the rollup
 *    built from the warehouse (its planned SQL), which equal live's answer;
 *  - built in a run that rebuilds only the rollup, it reads the base's table
 *    carried from the run before;
 *  - a run of the rollup alone builds its base first and reads that table;
 *  - a host's reference to the base at another tier refuses the rollup,
 *    never builds it from the warehouse;
 *  - a rollup in a `storage=` destination, or over a base that is not
 *    persisted, is never offered the base's table.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
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
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
/** The destination a wrong-tier reference names: configured, never written. */
const LAKE_DESTINATION = {
   name: "lake",
   type: "ducklake",
   ducklakeConnection: {
      catalog: {
         postgresConnection: { host: "h", databaseName: "d", userName: "u" },
      },
      storage: { bucketUrl: "/nonexistent-lake" },
   },
} as never;
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

function modelOf(
   rollupLine = '#@ preaggregate grain="cat"',
   persistLine = "#@ persist",
): string {
   return `##! experimental.persistence
source: raw is ${RAW}

${persistLine}
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

/** Sleep until `ms` past `isoInstant`. */
async function until(isoInstant: string, ms: number): Promise<void> {
   const wait = Date.parse(isoInstant) + ms - Date.now();
   if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
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

describe("a database rollup over a database #@ persist base", () => {
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
      // A storage destination the host's references may name (the
      // wrong-tier test); nothing here builds into it.
      env = await Environment.create(ENV, envPath, [], [LAKE_DESTINATION]);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   }

   beforeEach(async () => {
      ({ root, envPath, db, repo } = await openTempStorage(
         "rollup-over-base-",
         {
            id: ENV_ID,
            name: ENV,
         },
      ));
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
      await disposeTempStorage({ db, root });
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
            e.origin === "rollup" &&
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

   /** A query's rows, each value as a string (never through a double), and which table answered. */
   async function exactAnswer(query: string) {
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
               Object.entries(r).map(([k, v]) => [k, String(v)]),
            ),
         ),
         servedBy: result.path?.servedBy?.kind ?? "live",
      };
   }

   /**
    * The rollup `m` built answers `daily` by `sub` equal to live (the same
    * sums over `raw`, which nothing stores). Then the rollup's table and the
    * base's are tampered, each with its own suffix, so an answer read from the
    * base's table, or live, is told apart from one read from the rollup's.
    */
   async function expectBySubFromRollup(m: Materialization): Promise<void> {
      const bySub =
         "run: daily -> { group_by: sub; aggregate: grand; order_by: sub }";
      const live = await exactAnswer(
         "run: raw -> { group_by: sub; aggregate: grand is amt.sum(); order_by: sub }",
      );
      expect(live.servedBy).toBe("live");
      const served = await exactAnswer(bySub);
      expect(served.servedBy).toBe("rollup");
      expect(served.rows).toEqual(live.rows);
      const base = entries(m).find((e) => e.sourceName === "daily")!;
      for (const [table, tag] of [
         [rollupOf(m, "daily").physicalTableName, "_rollup"],
         [base.physicalTableName, "_base"],
      ]) {
         await (
            await connection()
         ).runSQL(`UPDATE ${table} SET sub = sub || '${tag}'`);
      }
      const tampered = await exactAnswer(bySub);
      expect(tampered.servedBy).toBe("rollup");
      expect(tampered.rows).toEqual(
         live.rows.map((r) => ({ ...r, sub: `${r.sub}_rollup` })),
      );
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

   it("builds the base first when a run names only the rollup, and reads its table", async () => {
      await setUp();
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const rollupName = Object.values(pkg.getBuildPlan()!.sources).find(
         (s) => s.origin === "rollup" && s.name.startsWith("daily__"),
      )!.name;
      const m = await build({ sourceNames: [rollupName] });
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const rollup = rollupOf(m, "daily");
      const base = entries(m).find((e) => e.sourceName === "daily")!;
      expect(base).toBeDefined();
      expect(rollup.upstreamReuse).toBe("reused");
      expect(createdBy(rollup.physicalTableName)).toContain(
         base.physicalTableName,
      );
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
   describe("with freshness", () => {
      /** Rebuild only the rollup: a new grain, the base unchanged. */
      async function regrain(rollupLine: string, persistLine?: string) {
         fs.writeFileSync(
            path.join(envPath, PKG, "m.malloy"),
            modelOf(rollupLine, persistLine),
         );
         await env.getPackage(parsePackageName(PKG), true);
         statements.length = 0;
      }
      const entryOf = (m: Materialization, name: string) =>
         entries(m).find((e) => e.sourceName === name)! as Entry & {
            dataAsOf?: string;
         };

      it("rebuilds a carried base of unknown age for a windowed rollup, builds the rollup from it, and serves it", async () => {
         await setUp(
            modelOf('#@ preaggregate grain="cat" freshness.window="1h"'),
         );
         const first = await build();
         expect(rollupOf(first, "daily").upstreamReuse).toBe("reused");
         // The base declares nothing, so its carried entry has no anchor.
         expect(entryOf(first, "daily").dataAsOf).toBeUndefined();
         await regrain('#@ preaggregate grain="sub" freshness.window="1h"');
         const started = Date.now();
         const second = await build();
         // The rollup's edge to its base: a table of unknown age is rebuilt,
         // not carried, and the rollup reads the rebuilt table.
         const rollup = rollupOf(second, "daily") as Entry & {
            dataAsOf?: string;
         };
         expect(rollup.upstreamReuse).toBe("reused");
         expect(createdBy(rollup.physicalTableName)).not.toContain(RAW_MARK);
         expect(Date.parse(rollup.dataAsOf!)).toBeGreaterThanOrEqual(
            started - 1000,
         );
         await expectBySubFromRollup(second);
      });

      it("rebuilds a carried base older than a rollup's narrower window, and builds the rollup from it, so it is served fresh rather than stale on arrival", async () => {
         const base = '#@ persist freshness.window="1h"';
         await setUp(
            modelOf('#@ preaggregate grain="cat" freshness.window="2s"', base),
         );
         const first = await build();
         const baseAnchor = entryOf(first, "daily").dataAsOf!;
         await until(baseAnchor, 2500);
         await regrain(
            '#@ preaggregate grain="sub" freshness.window="2s"',
            base,
         );
         const started = Date.now();
         const second = await build();
         // Past the rollup's window, so rebuilt for it.
         expect(
            Date.parse(entryOf(second, "daily").dataAsOf!),
         ).toBeGreaterThanOrEqual(started - 1000);
         const rollup = rollupOf(second, "daily") as Entry & {
            dataAsOf?: string;
         };
         expect(rollup.upstreamReuse).toBe("reused");
         expect(createdBy(rollup.physicalTableName)).not.toContain(RAW_MARK);
         expect(Date.parse(rollup.dataAsOf!)).toBeGreaterThanOrEqual(
            started - 1000,
         );
         await expectBySubFromRollup(second);
      });

      it("builds a windowed rollup from a carried base inside its window, anchored at the base", async () => {
         const base = '#@ persist freshness.window="1h"';
         await setUp(
            modelOf('#@ preaggregate grain="cat" freshness.window="1h"', base),
         );
         const first = await build();
         const baseAnchor = entryOf(first, "daily").dataAsOf!;
         await regrain(
            '#@ preaggregate grain="sub" freshness.window="1h"',
            base,
         );
         const second = await build();
         const rollup = rollupOf(second, "daily") as Entry & {
            dataAsOf?: string;
         };
         expect(rollup.upstreamReuse).toBe("reused");
         expect(createdBy(rollup.physicalTableName)).not.toContain(RAW_MARK);
         expect(rollup.dataAsOf).toBe(baseAnchor);
      });

      it("rebuilds a carried upstream of unknown age that a rebuilt windowed #@ persist reads, so its anchor is true", async () => {
         const chained = (where: string) => `##! experimental.persistence
source: raw is ${RAW}

#@ persist
source: daily is raw -> { group_by: cat, sub; aggregate: total is amt.sum() }

#@ persist freshness.window="1h"
source: summed is daily -> { ${where}group_by: cat; aggregate: grand is total.sum() }
`;
         await setUp(chained(""));
         const first = await build();
         expect(first.metadata).toMatchObject({ sourcesBuilt: 2 });
         fs.writeFileSync(
            path.join(envPath, PKG, "m.malloy"),
            chained("where: cat != 'zz'; "),
         );
         await env.getPackage(parsePackageName(PKG), true);
         statements.length = 0;
         const started = Date.now();
         const second = await build();
         // `daily` is unchanged but of unknown age, so it is rebuilt too.
         expect(second.metadata).toMatchObject({
            sourcesBuilt: 2,
            sourcesReused: 0,
         });
         const summed = entryOf(second, "summed");
         expect(Date.parse(summed.dataAsOf!)).toBeGreaterThanOrEqual(
            started - 1000,
         );
      });
   });

   it("refuses a rollup on a host's run whose reference to the base is at another tier", async () => {
      await setUp();
      const first = await build();
      const base = entries(first).find((e) => e.sourceName === "daily")!;
      const rollup = rollupOf(first, "daily");
      const host = (reference: Record<string, unknown>) =>
         build({
            buildInstructions: [
               {
                  sourceEntityId: rollup.sourceEntityId,
                  materializedTableId: "host-rollup",
                  physicalTableName: `host_rollup_${Math.random().toString(36).slice(2, 8)}`,
                  realization: "COPY",
               },
            ],
            referenceManifest: [
               {
                  sourceEntityId: base.sourceEntityId,
                  physicalTableName: base.physicalTableName,
                  connectionName: "duckdb",
                  ...reference,
               },
            ],
         });
      // At the base's own tier, the host's reference is read.
      statements.length = 0;
      const same = await host({});
      expect([same.status, same.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const read = rollupOf(same, "daily");
      expect(read.upstreamReuse).toBe("reused");
      expect(createdBy(read.physicalTableName)).toContain(
         base.physicalTableName,
      );
      // At another tier, it is not, and the rollup is refused as the
      // host's: never built from the warehouse.
      statements.length = 0;
      const moved = await host({ storageDestinationName: "lake" });
      expect(moved.status).toBe("FAILED");
      expect(JSON.stringify(moved.manifest ?? moved.error)).toContain(
         "supplied at the wrong tier",
      );
      expect(statements.some((s) => s.includes(RAW_MARK))).toBe(false);
   });
   it("rebuilds a windowed table whose carried entry has no anchor", async () => {
      await setUp();
      const first = await build();
      const daily = entries(first).find((e) => e.sourceName === "daily")!;
      expect((daily as { dataAsOf?: string }).dataAsOf).toBeUndefined();
      // The window does not move the address: the same table, now windowed.
      fs.writeFileSync(
         path.join(envPath, PKG, "m.malloy"),
         modelOf(undefined, '#@ persist freshness.window="1h"'),
      );
      await env.getPackage(parsePackageName(PKG), true);
      const started = Date.now();
      const second = await build();
      const rebuilt = entries(second).find(
         (e) => e.sourceName === "daily",
      )! as Entry & {
         dataAsOf?: string;
         freshnessWindowSeconds?: number;
      };
      expect(rebuilt.sourceEntityId).toBe(daily.sourceEntityId);
      expect(second.metadata).toMatchObject({ sourcesReused: 1 });
      expect(rebuilt.freshnessWindowSeconds).toBe(3600);
      expect(Date.parse(rebuilt.dataAsOf!)).toBeGreaterThanOrEqual(
         started - 1000,
      );
   });

   it("builds a rollup over a gated base from the base's table, storing the warehouse build's rows, and answers each caller as live", async () => {
      const restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      // The package-load worker reads its settings when it starts.
      await __setPackageLoadPoolForTests(null);
      try {
         await setUp(`##! experimental { persistence givens }
given: CAT :: string
source: raw is ${RAW}

#(access_filter) cat = $CAT
#@ persist
source: daily is raw -> {
  group_by: cat, sub
  aggregate: total is amt.sum()
} extend {
  #@ preaggregate grain="cat"
  measure: grand is total.sum()
}
`);
         const plannedSources = Object.values(
            (await env.getPackage(parsePackageName(PKG), false)).getBuildPlan()!
               .sources,
         );
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const base = entries(m).find((e) => e.sourceName === "daily")!;
         const rollup = rollupOf(m, "daily");
         expect(rollup.upstreamReuse).toBe("reused");
         expect(createdBy(rollup.physicalTableName)).toContain(
            base.physicalTableName,
         );
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const planned = plannedSources.find(
            (s) => s.sourceEntityId === rollup.sourceEntityId,
         )!;
         expect(
            await rowsOf(`SELECT * FROM ${rollup.physicalTableName}`),
         ).toEqual(await rowsOf(planned.sql));
         for (const cat of ["a", "b", "zz"]) {
            const result = await pkg.getModel("m.malloy")!.runQuery(
               unrecordedAudit(),
               {
                  query: "run: daily -> { group_by: cat; aggregate: grand }",
                  givens: { CAT: cat },
               } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
            expect(result.compactResult).toEqual(
               cat === "a"
                  ? [{ cat: "a", grand: 3.5 }]
                  : cat === "b"
                    ? [{ cat: "b", grand: 4.25 }]
                    : [],
            );
         }
      } finally {
         restore();
         await __setPackageLoadPoolForTests(null);
      }
   });
});
