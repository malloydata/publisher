// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A colocated rollup over a colocated `#(materialize)` base builds from the
 * base's stored table when the run holds it, rather than from the warehouse,
 * and stores the same rows, as one over a `#@ persist` base does
 * (materialization_rollup_over_stored_base.component.spec.ts). End to end
 * through a real Environment and MaterializationService on DuckDB:
 *
 *  - built in the same run as its base, the rollup reads the base's table
 *    (`upstreamReuse: "reused"`), and its rows equal the rows of the rollup
 *    built from the warehouse (its planned SQL), which equal live's answer;
 *  - built in a run that rebuilds only the rollup, it reads the base's table
 *    carried from the run before, a computed grain dimension read from the
 *    stored column;
 *  - with no base table in the run, a base too old for the rollup's window,
 *    or a host's reference to the base at another tier, it builds from the
 *    warehouse as planned;
 *  - over a gated base, under G3 or with gated sources on, it stores the
 *    warehouse build's rows and answers each caller as live;
 *  - over an extension table (P10), it reads the extension's own table;
 *  - a base whose stored copy does not compile builds its rollup from the
 *    warehouse, and leaves every other rollup's reuse alone.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   setDefaultTimeout,
   spyOn,
} from "bun:test";
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

setDefaultTimeout(120_000);

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

/** The model, its rollup at `grain`, each declaration with its extra keys. */
function modelOf(
   grain = "cat",
   rollupExtra = "",
   materializeExtra = "",
): string {
   const rollup = `#(preaggregate) grain=["${grain}"]${rollupExtra}`;
   return `##! experimental.persistence
source: raw is ${RAW}

#(materialize) name="orders_mat"${materializeExtra}
source: orders is raw extend {
  dimension: upper_cat is upper(cat)
  ${rollup}
  measure: grand is amt.sum()
  ${rollup}
  measure: rows is count()
  ${rollup}
  measure: least is amt.min()
}

source: plain is raw extend {
  #(preaggregate) grain=["cat"]
  measure: plain_total is amt.sum()
}
`;
}

/** Sleep until `ms` past `isoInstant`. */
async function until(isoInstant: string, ms: number): Promise<void> {
   const wait = Date.parse(isoInstant) + ms - Date.now();
   if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
}

const queryAt = (grain: string) =>
   `run: orders -> { group_by: ${grain}; aggregate: grand, rows, least; order_by: ${grain} }`;

type Entry = {
   sourceName: string;
   sourceEntityId: string;
   physicalTableName: string;
   upstreamReuse?: string;
   origin?: string;
   dataAsOf?: string;
};

describe("a colocated rollup over a colocated #(materialize) base", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let statements: string[];
   let spy: { mockRestore(): void } | undefined;
   let restore: (() => void) | undefined;

   async function setUp(
      model = modelOf(),
      settings: Record<string, string> = {},
   ): Promise<void> {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true", ...settings },
      }).restore;
      // The package-load worker reads its settings when it starts.
      await __setPackageLoadPoolForTests(null);
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": model },
         { manifest: { name: PKG, description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, []);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   }

   beforeEach(async () => {
      ({ root, envPath, db, repo } = await openTempStorage(
         "rollup-over-materialize-",
         { id: ENV_ID, name: ENV },
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
      restore?.();
      restore = undefined;
      await __setPackageLoadPoolForTests(null);
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
   const entryOf = (m: Materialization, name: string) => {
      const hit = entries(m).find((e) => e.sourceName === name);
      if (!hit) throw new Error(`no entry for ${name}`);
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

   const normalized = (rows: Record<string, unknown>[]) =>
      rows.map((r) =>
         Object.fromEntries(
            Object.entries(r).map(([k, v]) => [
               k,
               typeof v === "bigint" ? Number(v) : v,
            ]),
         ),
      );

   async function rowsOf(sql: string): Promise<Record<string, unknown>[]> {
      const result = await (await connection()).runSQL(sql);
      return normalized(result.rows as Record<string, unknown>[]).sort((a, b) =>
         JSON.stringify(a).localeCompare(JSON.stringify(b)),
      );
   }

   async function answer(query: string, givens?: Record<string, string>) {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const result = await pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query, ...(givens ? { givens } : {}) } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         rows: normalized(result.compactResult as Record<string, unknown>[]),
         servedBy: result.path?.servedBy?.kind ?? "live",
      };
   }

   /** The rollup's planned SQL: what its warehouse build runs. */
   async function plannedSQL(rollup: Entry): Promise<string> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      return Object.values(pkg.getBuildPlan()!.sources).find(
         (s) => s.sourceEntityId === rollup.sourceEntityId,
      )!.sql;
   }

   /** Rewrite the model and reload, clearing the statements seen so far. */
   async function rewrite(model: string): Promise<void> {
      fs.writeFileSync(path.join(envPath, PKG, "m.malloy"), model);
      await env.getPackage(parsePackageName(PKG), true);
      statements.length = 0;
   }

   it("builds from the base's table in the run that builds the base, storing the warehouse build's rows", async () => {
      await setUp();
      const live = await answer(queryAt("cat"));
      const m = await build();
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const base = entryOf(m, "orders");
      const rollup = rollupOf(m, "orders");
      expect(rollup.upstreamReuse).toBe("reused");
      // The rollup's build reads the base's table, never the raw rows.
      const create = createdBy(rollup.physicalTableName);
      expect(create).toContain(`"${base.physicalTableName}"`);
      expect(create).not.toContain(RAW_MARK);

      // Parity: the rows the warehouse build (the planned SQL) stores.
      const planned = await plannedSQL(rollup);
      expect(planned).toContain(RAW_MARK);
      expect(await rowsOf(`SELECT * FROM ${rollup.physicalTableName}`)).toEqual(
         await rowsOf(planned),
      );

      // And the served answer is live's.
      const served = await answer(queryAt("cat"));
      expect(served.servedBy).toBe("rollup");
      expect(served.rows).toEqual(live.rows);
   });

   it("reads the base's table carried from an earlier run when only the rollup is rebuilt, at a computed grain", async () => {
      await setUp();
      const first = await build();
      expect(first.status).toBe("MANIFEST_FILE_READY");
      const base = entryOf(first, "orders");
      // The rollup at a computed grain: the base is unchanged and carried.
      await rewrite(modelOf("upper_cat"));
      const live = await answer(queryAt("upper_cat"));
      const second = await build();
      expect([second.status, second.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      expect(second.metadata).toMatchObject({ sourcesReused: 2 });
      const rollup = rollupOf(second, "orders");
      expect(rollup.upstreamReuse).toBe("reused");
      const create = createdBy(rollup.physicalTableName);
      expect(create).toContain(`"${base.physicalTableName}"`);
      expect(create).not.toContain(RAW_MARK);
      // The stored column stands in for the computed dimension.
      expect(create).not.toMatch(/UPPER\(/i);
      expect(await rowsOf(`SELECT * FROM ${rollup.physicalTableName}`)).toEqual(
         await rowsOf(await plannedSQL(rollup)),
      );
      const served = await answer(queryAt("upper_cat"));
      expect(served.servedBy).toBe("rollup");
      expect(served.rows).toEqual(live.rows);
      expect(served.rows).toEqual([
         { upper_cat: "A", grand: 3.5, rows: 2, least: 1.5 },
         { upper_cat: "B", grand: 4.25, rows: 2, least: 4.25 },
         { upper_cat: "C", grand: -3, rows: 1, least: -3 },
      ]);
   });

   it("builds from the warehouse when the run holds no table of the base", async () => {
      await setUp();
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const rollupName = Object.values(pkg.getBuildPlan()!.sources).find(
         (s) => s.origin === "preaggregate" && s.name.startsWith("orders__"),
      )!.name;
      const m = await build({ sourceNames: [rollupName] });
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const rollup = rollupOf(m, "orders");
      expect(entries(m).map((e) => e.sourceName)).toEqual([rollup.sourceName]);
      // It read the raw rows, and says so: no `upstreamReuse`.
      expect(rollup.upstreamReuse).toBeUndefined();
      expect(createdBy(rollup.physicalTableName)).toContain(RAW_MARK);
      const served = await answer(queryAt("cat"));
      expect(served.servedBy).toBe("rollup");
   });

   it("builds a rollup over a base that is not materialized from the warehouse", async () => {
      await setUp();
      const m = await build();
      const rollup = rollupOf(m, "plain");
      expect(rollup.upstreamReuse).toBeUndefined();
      expect(createdBy(rollup.physicalTableName)).toContain(RAW_MARK);
   });

   it("leaves out only a base whose stored copy does not compile: the other rollups still read their bases' tables", async () => {
      // `clipped`'s re-rooted copy keeps `except: mark`, and its build source
      // has no `mark`: the copy does not compile, so its rollup builds from
      // the warehouse, while the `#@ persist` and the other `#(materialize)`
      // rollups still read their bases' tables.
      await setUp(`##! experimental.persistence
source: raw is ${RAW}

#@ persist name="porders_tbl"
source: porders is raw extend {
  #(preaggregate) grain=["cat"]
  measure: pgrand is amt.sum()
}

#(materialize) name="orders_mat"
source: orders is raw extend {
  #(preaggregate) grain=["cat"]
  measure: grand is amt.sum()
}

#(materialize) name="clipped_mat"
source: clipped is raw extend {
  except: mark
  #(preaggregate) grain=["cat"]
  measure: cgrand is amt.sum()
}
`);
      const query = (base: string, measure: string) =>
         `run: ${base} -> { group_by: cat; aggregate: ${measure}; order_by: cat }`;
      const live = await answer(query("clipped", "cgrand"));
      const m = await build();
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);

      for (const name of ["porders", "orders"]) {
         const rollup = rollupOf(m, name);
         expect([name, rollup.upstreamReuse]).toEqual([name, "reused"]);
         const create = createdBy(rollup.physicalTableName);
         expect(create).toContain(`"${entryOf(m, name).physicalTableName}"`);
         expect(create).not.toContain(RAW_MARK);
      }

      const clipped = rollupOf(m, "clipped");
      expect(clipped.upstreamReuse).toBeUndefined();
      expect(createdBy(clipped.physicalTableName)).toContain(RAW_MARK);
      expect(
         await rowsOf(`SELECT * FROM ${clipped.physicalTableName}`),
      ).toEqual(await rowsOf(await plannedSQL(clipped)));
      const served = await answer(query("clipped", "cgrand"));
      expect(served.servedBy).toBe("rollup");
      expect(served.rows).toEqual(live.rows);
   });

   describe("with freshness", () => {
      it("builds a windowed rollup from the warehouse over a carried base of unknown age, and serves it", async () => {
         const windowed = ' freshness.window="1h"';
         await setUp(modelOf("cat", windowed));
         const first = await build();
         expect(rollupOf(first, "orders").upstreamReuse).toBe("reused");
         // The base declares nothing, so its carried entry has no anchor.
         expect(entryOf(first, "orders").dataAsOf).toBeUndefined();
         await rewrite(modelOf("sub", windowed));
         const started = Date.now();
         const second = await build();
         expect(second.metadata).toMatchObject({ sourcesReused: 2 });
         const rollup = rollupOf(second, "orders");
         expect(rollup.upstreamReuse).toBeUndefined();
         expect(createdBy(rollup.physicalTableName)).toContain(RAW_MARK);
         expect(Date.parse(rollup.dataAsOf!)).toBeGreaterThanOrEqual(
            started - 1000,
         );
         const served = await answer(queryAt("sub"));
         expect(served.servedBy).toBe("rollup");
      });

      it("builds a rollup whose window is narrower than its carried base's age from the warehouse", async () => {
         const base = ' freshness.window="1h"';
         const rollupWindow = ' freshness.window="2s"';
         await setUp(modelOf("cat", rollupWindow, base));
         const first = await build();
         const baseAnchor = entryOf(first, "orders").dataAsOf!;
         await until(baseAnchor, 2500);
         await rewrite(modelOf("sub", rollupWindow, base));
         const second = await build();
         // The base is inside its own window, so it is carried.
         expect(entryOf(second, "orders").dataAsOf).toBe(baseAnchor);
         const rollup = rollupOf(second, "orders");
         expect(rollup.upstreamReuse).toBeUndefined();
         expect(createdBy(rollup.physicalTableName)).toContain(RAW_MARK);
         const served = await answer(queryAt("sub"));
         expect(served.servedBy).toBe("rollup");
      });

      it("builds a windowed rollup from a carried base inside its window, anchored at the base", async () => {
         const windowed = ' freshness.window="1h"';
         await setUp(modelOf("cat", windowed, windowed));
         const first = await build();
         const baseAnchor = entryOf(first, "orders").dataAsOf!;
         await rewrite(modelOf("sub", windowed, windowed));
         const second = await build();
         const rollup = rollupOf(second, "orders");
         expect(rollup.upstreamReuse).toBe("reused");
         expect(createdBy(rollup.physicalTableName)).not.toContain(RAW_MARK);
         expect(rollup.dataAsOf).toBe(baseAnchor);
      });
   });

   it("builds from the warehouse on a host's run whose reference to the base is at another tier", async () => {
      await setUp();
      const first = await build();
      const base = entryOf(first, "orders");
      const rollup = rollupOf(first, "orders");
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
      const read = rollupOf(same, "orders");
      expect(read.upstreamReuse).toBe("reused");
      expect(createdBy(read.physicalTableName)).toContain(
         `"${base.physicalTableName}"`,
      );
      // At another tier, it is not: the rollup reads the warehouse.
      statements.length = 0;
      const moved = await host({ storageDestinationName: "lake" });
      expect([moved.status, moved.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      const warehouse = rollupOf(moved, "orders");
      expect(warehouse.upstreamReuse).toBeUndefined();
      expect(createdBy(warehouse.physicalTableName)).toContain(RAW_MARK);
   });

   const GATED = `##! experimental { persistence givens }
given: CAT :: string
source: raw is ${RAW}

#(access_filter) cat = $CAT
#(materialize) name="orders_mat"
source: orders is raw extend {
  #(preaggregate) grain=["cat"]
  measure: grand is amt.sum()
}
`;
   const GATED_QUERY = "run: orders -> { group_by: cat; aggregate: grand }";
   const gatedAnswer = (cat: string) =>
      cat === "a"
         ? [{ cat: "a", grand: 3.5 }]
         : cat === "b"
           ? [{ cat: "b", grand: 4.25 }]
           : [];

   /** Build GATED: the rollup reads the base's table, with the warehouse build's rows, and each caller is answered as live. */
   async function expectGatedReuse(): Promise<void> {
      const m = await build();
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const base = entryOf(m, "orders");
      const rollup = rollupOf(m, "orders");
      expect(rollup.upstreamReuse).toBe("reused");
      const create = createdBy(rollup.physicalTableName);
      expect(create).toContain(`"${base.physicalTableName}"`);
      expect(create).not.toContain(RAW_MARK);
      expect(await rowsOf(`SELECT * FROM ${rollup.physicalTableName}`)).toEqual(
         await rowsOf(await plannedSQL(rollup)),
      );
      for (const cat of ["a", "b", "zz"]) {
         const served = await answer(GATED_QUERY, { CAT: cat });
         expect(served.servedBy).toBe("rollup");
         expect(served.rows).toEqual(gatedAnswer(cat));
      }
   }

   it("builds a rollup over a base gated by its own gate (G3) from the base's table, and answers each caller as live", async () => {
      // Gated sources off: G3 keeps the base and the rollup, every gate the
      // base's own, each applied when a query reads the table.
      await setUp(GATED);
      await expectGatedReuse();
   });

   it("builds a rollup over a gated base from the base's table, storing the warehouse build's rows, and answers each caller as live", async () => {
      await setUp(GATED, { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" });
      await expectGatedReuse();
   });

   it("builds a rollup over an extension table from the extension's own table", async () => {
      await setUp(`##! experimental.persistence
source: raw is ${RAW}

#(materialize) name="orders_mat"
source: orders is raw extend {
  dimension: upper_cat is upper(cat)
}

#(materialize) name="tagged_mat"
source: tagged is orders extend {
  dimension: tag is concat(upper_cat, '-', sub)
  #(preaggregate) grain=["tag"]
  measure: grand is amt.sum()
}
`);
      const live = await answer(
         "run: tagged -> { group_by: tag; aggregate: grand; order_by: tag }",
      );
      const m = await build();
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const extension = entryOf(m, "tagged");
      const rollup = rollupOf(m, "tagged");
      expect(rollup.upstreamReuse).toBe("reused");
      const create = createdBy(rollup.physicalTableName);
      expect(create).toContain(`"${extension.physicalTableName}"`);
      expect(create).not.toContain(RAW_MARK);
      expect(await rowsOf(`SELECT * FROM ${rollup.physicalTableName}`)).toEqual(
         await rowsOf(await plannedSQL(rollup)),
      );
      const served = await answer(
         "run: tagged -> { group_by: tag; aggregate: grand; order_by: tag }",
      );
      expect(served.servedBy).toBe("rollup");
      expect(served.rows).toEqual(live.rows);
   });
});
