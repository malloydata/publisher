// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `#(materialize)` extension whose parent is declared in another file
 * builds over the parent's table, as a chain in one file does (P10), rather
 * than from the warehouse: its build copy imports a copy of the parent's
 * file with the parent re-rooted on its build source. End to end through a
 * real Environment and MaterializationService on DuckDB.
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
const PKG = "pkg";

/** The raw rows, read only through `raw`, so a build that reads them names RAW_MARK. */
const RAW_MARK = "raw_rows_mark";
const RAW = `duckdb.sql("""
  SELECT id, cat, sub, amt::DOUBLE AS amt, mark FROM (VALUES
    (1, 'a', 'x', 1.5, '${RAW_MARK}'), (2, 'a', 'y', 2.0, '${RAW_MARK}'),
    (3, 'b', 'x', 4.25, '${RAW_MARK}'), (4, 'b', 'x', NULL, '${RAW_MARK}'),
    (5, 'c', 'z', -3.0, '${RAW_MARK}')
  ) AS t(id, cat, sub, amt, mark)
""")`;

type Entry = {
   schema?: { name: string }[];
   sourceName: string;
   sourceEntityId: string;
   physicalTableName: string;
   upstreamReuse?: string;
   dataAsOf?: string;
};

const TWO_FILES = {
   "a.malloy": `source: raw is ${RAW}

#(materialize) name="parent_t"
source: parent is raw extend {
  dimension: big is amt > 2
  measure: total is amt.sum()
}
`,
   "b.malloy": `import "a.malloy"

#(materialize) name="child_t"
source: child is parent extend {
  where: cat != 'c'
  dimension: label is concat(cat, '-', sub)
  measure: n is count()
}
`,
};

describe("a #(materialize) chain across files", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let statements: string[];
   let spy: { mockRestore(): void } | undefined;
   let restore: () => void;

   async function setUp(files: Record<string, string>): Promise<void> {
      writePackageFiles(path.join(envPath, PKG), files, {
         manifest: { name: PKG, description: "fixture" },
      });
      env = await Environment.create(ENV, envPath, []);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   }

   beforeEach(async () => {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      ({ root, envPath, db, repo } = await openTempStorage("cross-file-", {
         id: ENV_ID,
         name: ENV,
      }));
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
      restore();
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

   const entryOf = (m: Materialization, name: string) => {
      const hit = Object.values(
         (m.manifest as { entries: Record<string, Entry> }).entries,
      ).find((e) => e.sourceName === name);
      if (!hit) throw new Error(`no entry for ${name}`);
      return hit;
   };

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

   const plain = (rows: Record<string, unknown>[]) =>
      rows
         .map((r) =>
            Object.fromEntries(
               Object.entries(r).map(([k, v]) => [
                  k,
                  // A count, as a connection or the query API returns it.
                  typeof v === "bigint" ||
                  (typeof v === "string" && /^-?\d+$/.test(v))
                     ? Number(v)
                     : v,
               ]),
            ),
         )
         .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

   async function rowsOf(sql: string): Promise<Record<string, unknown>[]> {
      const result = await (await connection()).runSQL(sql);
      return plain(result.rows as Record<string, unknown>[]);
   }

   async function answer(
      model: string,
      query: string,
      givens?: Record<string, unknown>,
   ) {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const result = await pkg
         .getModel(model)!
         .runQuery(
            unrecordedAudit(),
            { query, ...(givens ? { givens } : {}) } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         rows: plain(result.compactResult as Record<string, unknown>[]),
         servedBy: result.path?.servedBy?.kind ?? "live",
      };
   }

   /** The planned (warehouse) build SQL of the table `entry` names. */
   async function plannedSQL(entry: Entry): Promise<string> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const planned = Object.values(pkg.getBuildPlan()!.sources).find(
         (s) => s.sourceEntityId === entry.sourceEntityId,
      );
      if (!planned) throw new Error(`no planned source ${entry.sourceName}`);
      return planned.sql;
   }

   /** Live answers, taken before any table is built. */
   const live = new Map<string, Record<string, unknown>[]>();
   const liveKey = (query: string, givens?: Record<string, unknown>) =>
      `${query}${JSON.stringify(givens ?? {})}`;
   /**
    * Live's answer to each query, and the rows of each chained table's
    * planned columns (`[child, parent, child's model]`), before any build.
    */
   async function takeLive(
      queries: readonly (readonly [string, string])[],
      chain: readonly (readonly [string, string, string])[],
      givens?: Record<string, unknown>,
   ): Promise<void> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const sources = Object.values(pkg.getBuildPlan()!.sources);
      const selects = chain.map(([child, , model]) => {
         const planned = sources.find((s) => s.name === child);
         if (!planned?.columns) throw new Error(`no planned ${child}`);
         return [
            model,
            `run: ${child} -> { select: ${planned.columns.map((c) => c.name).join(", ")} }`,
         ] as const;
      });
      for (const [model, query] of [...queries, ...selects]) {
         const answered = await answer(model, query, givens);
         expect(answered.servedBy).toBe("live");
         live.set(liveKey(query, givens), answered.rows);
      }
   }

   /**
    * Each chained table (`[child, parent, child's model]`) read its parent's
    * table, never the raw rows, and stores the rows its planned (warehouse)
    * build stores, which are live's rows of its columns. Each query is
    * answered as live; `served` says whether it must be from a table.
    */
   async function expectChained(
      m: Materialization,
      chain: readonly (readonly [string, string, string])[],
      queries: readonly (readonly [string, string])[],
      served: boolean,
      givens?: Record<string, unknown>,
   ): Promise<void> {
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      for (const [child, parent, model] of chain) {
         const c = entryOf(m, child);
         const create = createdBy(c.physicalTableName);
         expect(create).toContain(entryOf(m, parent).physicalTableName);
         expect(create).not.toContain(RAW_MARK);
         const sql = await plannedSQL(c);
         expect(sql).toContain(RAW_MARK);
         const stored = await rowsOf(`SELECT * FROM ${c.physicalTableName}`);
         expect(stored.length).toBeGreaterThan(0);
         expect(stored).toEqual(await rowsOf(sql));
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const planned = Object.values(pkg.getBuildPlan()!.sources).find(
            (s) => s.name === child,
         )!;
         void model;
         expect(
            live.get(
               liveKey(
                  `run: ${child} -> { select: ${planned.columns!.map((col) => col.name).join(", ")} }`,
                  givens,
               ),
            ),
         ).toEqual(stored);
      }
      for (const [model, query] of queries) {
         const answered = await answer(model, query, givens);
         if (served) expect(answered.servedBy).toBe("materialized");
         expect(answered.rows).toEqual(live.get(liveKey(query, givens))!);
      }
   }

   it("builds a child over its parent's table in another file: the warehouse build's rows, and live's answers", async () => {
      await setUp(TWO_FILES);
      const chain = [["child", "parent", "b.malloy"]] as const;
      const queries = [
         [
            "b.malloy",
            "run: child -> { group_by: label, big; aggregate: total, n }",
         ],
         ["b.malloy", "run: child -> { aggregate: total, n }"],
      ] as const;
      await takeLive(queries, chain);
      const m = await build();
      await expectChained(m, chain, queries, true);
   });

   // The served copy answers a chain across three files, or one through an
   // extension it does not re-root, live (as it did before tables were
   // chained across files); its answers are compared all the same.
   it("builds a child over a query-shaped parent's table in another file", async () => {
      await setUp({
         "a.malloy": `source: raw is ${RAW}

#(materialize) name="parent_t"
source: parent is raw -> {
  group_by: cat, sub
  aggregate: total is amt.sum(), rows is count()
}
`,
         "b.malloy": `import "a.malloy"

#(materialize) name="child_t"
source: child is parent extend {
  where: cat != 'c'
  measure: grand is total.sum()
}
`,
      });
      const chain = [["child", "parent", "b.malloy"]] as const;
      const queries = [
         ["b.malloy", "run: child -> { group_by: cat; aggregate: grand }"],
      ] as const;
      await takeLive(queries, chain);
      const m = await build();
      await expectChained(m, chain, queries, true);
   });

   it("builds a chain over three files, each table over the one before", async () => {
      await setUp({
         ...TWO_FILES,
         "c.malloy": `import "b.malloy"

#(materialize) name="grandchild_t"
source: grandchild is child extend {
  where: big
  measure: top is amt.max()
}
`,
      });
      const chain = [
         ["child", "parent", "b.malloy"],
         ["grandchild", "child", "c.malloy"],
      ] as const;
      const queries = [
         [
            "c.malloy",
            "run: grandchild -> { group_by: cat; aggregate: total, n, top }",
         ],
         ["b.malloy", "run: child -> { group_by: sub; aggregate: n }"],
      ] as const;
      await takeLive(queries, chain);
      const m = await build();
      await expectChained(m, chain, queries, false);
   });

   it("builds a child over its parent's table through extensions that are not materialized, in either file", async () => {
      await setUp({
         "a.malloy": `${TWO_FILES["a.malloy"]}
source: wide is parent extend {
  dimension: sub_up is upper(sub)
}
`,
         "b.malloy": `import { wide } from "a.malloy"

source: mid is wide extend {
  measure: avg_amt is amt.avg()
}

#(materialize) name="child_t"
source: child is mid extend {
  dimension: tag is concat(sub_up, '!')
}
`,
      });
      const chain = [["child", "parent", "b.malloy"]] as const;
      const queries = [
         [
            "b.malloy",
            "run: child -> { group_by: tag; aggregate: total, avg_amt }",
         ],
      ] as const;
      await takeLive(queries, chain);
      const m = await build();
      await expectChained(m, chain, queries, false);
   });

   /** A child in b.malloy over `parent` in a.malloy, through c.malloy's `mid`. */
   const THROUGH_THIRD = {
      "a.malloy": TWO_FILES["a.malloy"],
      "c.malloy": `import "a.malloy"
source: mid is parent extend {
  measure: avg_amt is amt.avg()
  dimension: sub_up is upper(sub)
}
`,
      "b.malloy": `import "c.malloy"

#(materialize) name="child_t"
source: child is mid extend {
  where: cat != 'c'
  dimension: label is concat(cat, '-', sub_up)
}
`,
   };

   it("builds a child over its parent's table through a third file: the warehouse build's rows, and live's answers", async () => {
      await setUp(THROUGH_THIRD);
      const chain = [["child", "parent", "b.malloy"]] as const;
      const queries = [
         [
            "b.malloy",
            "run: child -> { group_by: label, big; aggregate: total, avg_amt }",
         ],
      ] as const;
      await takeLive(queries, chain);
      const m = await build();
      await expectChained(m, chain, queries, false);
   });

   it("builds a child over its parent's table through two files between, with a selective import on the way", async () => {
      await setUp({
         "a.malloy": TWO_FILES["a.malloy"],
         "c.malloy": `import "a.malloy"
source: mid is parent extend {
  dimension: sub_up is upper(sub)
}
`,
         "d.malloy": `import { mid } from "c.malloy"
source: mid2 is mid extend {
  measure: avg_amt is amt.avg()
}
`,
         "b.malloy": `import "d.malloy"

#(materialize) name="child_t"
source: child is mid2 extend {
  dimension: label is concat(cat, '-', sub_up)
}
`,
      });
      const chain = [["child", "parent", "b.malloy"]] as const;
      const queries = [
         [
            "b.malloy",
            "run: child -> { group_by: label; aggregate: total, avg_amt }",
         ],
      ] as const;
      await takeLive(queries, chain);
      const m = await build();
      await expectChained(m, chain, queries, false);
   });

   it("builds a child over its parent's table through a file between that declares a #(materialize) it refuses, with the same rows", async () => {
      await setUp({
         ...THROUGH_THIRD,
         // Refused, for itself alone: its inlined join's rows read the
         // current time, which a stored copy would freeze. The child reads
         // the join through the served join, so it is planned.
         "c.malloy": `import "a.malloy"
source: cats is raw -> { group_by: cat } extend {
  primary_key: cat
  where: year(now()) > 2000
}
#(materialize) inline=["k"]
source: mid is parent extend {
  join_one: k is cats on cat = k.cat
  measure: avg_amt is amt.avg()
  dimension: sub_up is upper(sub)
}
`,
      });
      const m = await build();
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      expect(
         Object.values(pkg.getBuildPlan()!.sources).some(
            (s) => s.name === "mid",
         ),
      ).toBe(false);
      // `mid` has no table, so the walk passes through it, as through any
      // declaration, to `parent`: never over the raw chain.
      const child = entryOf(m, "child");
      const create = createdBy(child.physicalTableName);
      expect(create).toContain(entryOf(m, "parent").physicalTableName);
      expect(create).not.toContain(RAW_MARK);
      expect(await rowsOf(`SELECT * FROM ${child.physicalTableName}`)).toEqual(
         await rowsOf(await plannedSQL(child)),
      );
   });

   it("rebuilds a parent of unknown age before a windowed child reads it, rather than read it stale", async () => {
      const childFile = (where: string) =>
         TWO_FILES["b.malloy"]
            .replace(
               '#(materialize) name="child_t"',
               '#(materialize) name="child_t" freshness.window="1h"',
            )
            .replace("where: cat != 'c'", where);
      await setUp({ ...TWO_FILES, "b.malloy": childFile("where: cat != 'c'") });
      const first = await build();
      expect([first.status, first.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      // The parent declares no window, so its carried table has no anchor.
      expect(entryOf(first, "parent").dataAsOf).toBeUndefined();
      // Only the child changes.
      fs.writeFileSync(
         path.join(envPath, PKG, "b.malloy"),
         childFile("where: cat != 'zz'"),
      );
      await env.getPackage(parsePackageName(PKG), true);
      statements.length = 0;
      const started = Date.now();
      const second = await build();
      expect([second.status, second.error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      expect(second.metadata).toMatchObject({
         sourcesBuilt: 2,
         sourcesReused: 0,
      });
      const parent = entryOf(second, "parent");
      const child = entryOf(second, "child");
      // The parent was rebuilt first, and the child read that table.
      expect(createdBy(parent.physicalTableName)).toContain(RAW_MARK);
      expect(createdBy(child.physicalTableName)).toContain(
         parent.physicalTableName,
      );
      expect(Date.parse(child.dataAsOf!)).toBeGreaterThanOrEqual(
         started - 1000,
      );
   });

   it("builds a child over a gated parent's table, and answers each caller as live", async () => {
      const gated = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      try {
         await setUp({
            "a.malloy": `##! experimental.givens
given: CAT :: string
${TWO_FILES["a.malloy"].replace(
   '#(materialize) name="parent_t"',
   '#(access_filter) cat = $CAT\n#(materialize) name="parent_t"',
)}`,
            // The child declares the gate it reads too: one it only
            // inherits is refused (it could not be applied over its table).
            "b.malloy": `##! experimental.givens
${TWO_FILES["b.malloy"].replace(
   '#(materialize) name="child_t"',
   '#(access_filter) cat = $CAT\n#(materialize) name="child_t"',
)}`,
         });
         const chain = [["child", "parent", "b.malloy"]] as const;
         const query = [
            "b.malloy",
            "run: child -> { group_by: cat, label; aggregate: total, n }",
         ] as const;
         for (const CAT of ["a", "b", "zz"]) {
            await takeLive([query], [], { CAT });
         }
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         for (const [child, parent] of chain) {
            const c = entryOf(m, child);
            expect(createdBy(c.physicalTableName)).toContain(
               entryOf(m, parent).physicalTableName,
            );
            expect(
               await rowsOf(`SELECT * FROM ${c.physicalTableName}`),
            ).toEqual(await rowsOf(await plannedSQL(c)));
         }
         for (const CAT of ["a", "b", "zz"]) {
            await expectChained(m, [], [query], false, { CAT });
         }
      } finally {
         gated();
      }
   });
});
