// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   materializePlansIn,
   modelPathsIn,
   sourcesIn,
   targetsIn,
} from "../../../test_helpers/materialization_test_fixtures";
import { describe, expect, it } from "bun:test";
import {
   type ModelFiles,
   planPackages,
} from "../../../test_helpers/materialization_fixture";
import {
   addressSQL,
   buildSQLOf,
   computeSourceEntityId,
   derivePackagePlan,
} from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";

/**
 * These plan incremental machinery on DuckDB, which no load or run builds
 * (the dialect cannot bound a delta: incremental_policy's rule 6), so keep
 * those sources rather than refuse them alone.
 */
const KEEP_INCREMENTAL = { keepIncrementalUnbuildable: true };
import { collectIncrementalDeclarations } from "./build_plan";
import { incrementalLineage } from "../run/incremental_build";
import { incrementalPolicyRejections } from "./incremental_policy";
import {
   assertInlinedJoinsUnique,
   pairLeafKeyChecksOf,
   runKeyCheck,
} from "../run/inline_uniqueness";
import { rollingWindowsOf } from "../domain/read_time_terms";
import { buildCopyText } from "./materialize_build_copy";
import {
   buildSourceName,
   generateBuildFile,
   generateBuildFiles,
} from "./build_file_generator";
import {
   BASE,
   DATED,
   examplePlan,
   generatedPlans,
   JOINED,
   PAIRED,
   pairSource,
   pathSource,
   PICKED,
} from "../../../test_helpers/materialize_spec_models";

/**
 * The `#(materialize)` build-file generator (`build_file_generator.ts`,
 * materialization v2, phase 3, PR 2b): the generated companion's text for
 * each shape (level files, carried keys, pair tables and their key checks,
 * incremental pair watermarks), and that it compiles and builds what the
 * source computes. What is planned or refused is `materialize_plan.spec.ts`'s;
 * a chained `storage=` extension's text is `materialize_stored_chain.spec.ts`'s.
 */

const packages = planPackages({}, { keepIncrementalUnbuildable: true });

const compiled = async (files: ModelFiles) => (await packages.plan(files)).c;

describe("planModelMaterialize and generateBuildFile", () => {
   const plan = examplePlan;

   it("writes one query-shaped #@ persist source per materialized source", () => {
      expect(generateBuildFile([plan({})], "m.malloy")).toBe(
         `##! experimental.persistence
import { orders__mat__base is orders } from 'm.malloy'

#@ persist
source: orders__mat is orders__mat__base -> {
  select:
    id,
    big
}
`,
      );
   });

   it("writes an extension table's build copy over its parent's build source, and a level file per depth (P10)", () => {
      const text = `##! experimental.givens
given: R :: string is 'east'
source: orders is duckdb.table('ev')

source: stored is orders extend {
  dimension: big is amt > 10
  measure: total is amt.sum()
}

source: ext is stored extend {
  where: region = $R
}
`;
      const lines = text.split("\n");
      const at = (name: string, endLine: number) => {
         const line = lines.findIndex((l) =>
            l.startsWith(`source: ${name} is`),
         );
         return {
            url: "file:///pkg/m.malloy",
            range: {
               start: { line, character: 8 },
               end: { line: endLine, character: 1 },
            },
         };
      };
      const parent = plan({
         sourceName: "stored",
         sourceID: "stored@file:///pkg/m.malloy",
         buildSourceName: buildSourceName("stored"),
      });
      const ext = plan({
         sourceName: "ext",
         sourceID: "ext@file:///pkg/m.malloy",
         buildSourceName: buildSourceName("ext"),
         readTimeTerms: [
            {
               kind: "given",
               code: "region = $R",
               givens: ["R"],
               columns: ["region"],
               at: [
                  {
                     url: "file:///pkg/m.malloy",
                     range: {
                        start: { line: 10, character: 9 },
                        end: { line: 10, character: 20 },
                     },
                  },
               ],
            },
         ],
         declaration: at("ext", 11),
         buildsOver: {
            sourceID: parent.sourceID,
            sourceName: "stored",
            mode: { tier: "same" },
            declaration: at("stored", 7),
            path: [at("ext", 11)],
         },
      });
      const copy = buildCopyText(text, ext, {
         parent,
         importFrom: "m.malloy.0.materialize-level.malloy",
      });
      // The parent re-rooted on its build source, less the dimension its
      // table stores; the extension's given term out of its build (P2).
      expect(copy).toEqual({
         copy: `##! experimental.givens
import { stored__mat } from 'm.malloy.0.materialize-level.malloy'
given: R :: string is 'east'
source: orders is duckdb.table('ev')

source: stored is stored__mat extend {
  measure: total is amt.sum()
}

source: ext is stored extend {
  where: true
}
`,
      });
      const files = generateBuildFiles(
         [parent, ext],
         "m.malloy",
         undefined,
         new Map([[ext.sourceID, "m.malloy.0.materialize-build.malloy"]]),
      )!;
      expect([...files.levels.keys()]).toEqual([
         "m.malloy.0.materialize-level.malloy",
         "m.malloy.1.materialize-level.malloy",
      ]);
      expect(files.entry).toBe(`##! experimental.persistence
import { stored__mat } from 'm.malloy.0.materialize-level.malloy'
import { ext__mat } from 'm.malloy.1.materialize-level.malloy'
`);
      expect(files.levels.get("m.malloy.1.materialize-level.malloy")).toContain(
         "import { ext__mat__base is ext } from 'm.malloy.0.materialize-build.malloy'",
      );
      // Without a chain, the companion is the one file it always was.
      expect(generateBuildFiles([parent], "m.malloy")?.entry).toBe(
         generateBuildFile([parent], "m.malloy"),
      );
   });

   it("carries the declaration's persist keys, quoted, in a fixed order", () => {
      const text = generateBuildFile(
         [
            plan({
               persist: {
                  storage: "my-lake",
                  name: "orders_all",
                  refresh: "incremental",
                  watermark: "order_date",
               },
            }),
         ],
         "m.malloy",
      );
      expect(text).toContain(
         '#@ persist name="orders_all" storage="my-lake" refresh="incremental" watermark="order_date"',
      );
   });

   it("quotes names Malloy would not read bare", () => {
      const text = generateBuildFile(
         [
            plan({
               sourceName: "source",
               buildSourceName: buildSourceName("source"),
               columns: [
                  { dimension: "date", column: "date", computed: false },
                  { dimension: "count", column: "count", computed: true },
               ],
            }),
         ],
         "m.malloy",
      );
      expect(text).toContain("import { source__mat__base is `source` }");
      expect(text).toContain("    `date`,\n    `count`");
   });

   it("carries a group's properties as dotted keys", () => {
      const text = generateBuildFile(
         [
            plan({
               persist: {
                  name: "orders_all",
                  "freshness.window": "24h",
                  "queryMetadata.team": "ops",
               },
            }),
         ],
         "m.malloy",
      );
      expect(text).toContain(
         '#@ persist name="orders_all" freshness.window="24h" queryMetadata.team="ops"',
      );
   });

   it("generates nothing for a model that declares nothing", () => {
      expect(generateBuildFile([], "m.malloy")).toBeUndefined();
   });
});

describe("the generated companion, compiled into the build plan", () => {
   it(
      "stores a persisted query's nest as a column, not a declared join",
      async () => {
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("""select 'east' as code""")

#(materialize) storage="lake"
source: nested is orders_raw -> {
  group_by: region
  aggregate: total is amount.sum()
  nest: by_day is { group_by: order_date; aggregate: n is count() }
} extend {
  join_many: r is regions on r.code = region
}
`);
         const [p] = generatedPlans(c);
         expect(p.columns).toEqual([
            { dimension: "region", column: "region", computed: false },
            { dimension: "total", column: "total", computed: false },
            { dimension: "by_day", column: "by_day", computed: false },
         ]);
         // The build selects it, so the table stores the nest's rows.
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "nested",
         )!;
         expect(
            buildSQLOf(source, { entries: {} }, targetsIn(c)?.[sourceID]),
         ).toContain('"by_day"');
      },
      { timeout: 30000 },
   );

   it(
      "carries a rolling window from the compiled source to the refresh's lineage, and builds the term as written",
      async () => {
         const c = await compiled(`source: events_raw is duckdb.sql("""
  select * from (values
    (1, timestamp '2026-01-01 00:00:00'),
    (2, timestamp '2026-01-02 00:00:00')
  ) as t(id, ts)
""")

#(materialize) refresh="incremental" watermark="ts" storage="lake"
source: recent is events_raw extend {
  where: ts >= now - 30 days
}
`);
         const [p] = generatedPlans(c);
         expect(p.readTimeTerms).toEqual([
            {
               kind: "current_time",
               code: "ts >= now - 30 days",
               columns: ["ts"],
               window: {
                  field: "ts",
                  op: ">=",
                  offset: { sign: -1, amount: 30, unit: "day" },
               },
            },
         ]);
         // The keying rule: the plan, the source and the declaration all sit
         // under the author's sourceID.
         const declaration = collectIncrementalDeclarations(c.tables)[
            p.sourceID
         ];
         expect(declaration?.rollingWindow).toEqual([
            rollingWindowsOf(p.readTimeTerms)[0],
         ]);
         expect(
            incrementalLineage({
               declaration,
               // As stored from a Postgres origin: a DuckDB origin has no
               // incremental range to read.
               dialect: "postgres",
               targetDialect: "duckdb",
               physicalTableName: "recent_events",
               connectionName: "duckdb",
               sourceEntityId: "sid",
            })?.rollingWindow,
         ).toEqual([rollingWindowsOf(p.readTimeTerms)[0]]);
         // Built as written: the address reads the warehouse's own clock, never
         // a literal of Publisher's, so it does not move between builds.
         const sql = addressSQL(
            sourcesIn(c)[p.sourceID],
            targetsIn(c)?.[p.sourceID],
         );
         expect(sql).toContain("LOCALTIMESTAMP");
         expect(sql).not.toMatch(/TIMESTAMP '20\d\d-/);
      },
      { timeout: 30000 },
   );

   it(
      "builds exactly what the source computes",
      async () => {
         const pkg = await packages.create(`${BASE}
#(materialize) storage="lake"
source: orders is orders_raw extend { dimension: big is amount > 10 }
`);
         const c = await compilePackageBuildPlan(
            pkg,
            undefined,
            KEEP_INCREMENTAL,
         );
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(
            source,
            { entries: {} },
            targetsIn(c)?.[sourceID],
         );
         const duckdb = await pkg.getMalloyConnection!("duckdb");
         const rows = (
            await duckdb.runSQL(
               `SELECT id, region, amount, big FROM (${sql}) t ORDER BY id`,
            )
         ).rows;
         expect(
            rows.map((r) => [Number(r.id), r.region, Number(r.amount), r.big]),
         ).toEqual([
            [1, "east", 10.5, true],
            [2, "west", 20, true],
            [3, "east", 5.25, false],
         ]);
      },
      { timeout: 30000 },
   );

   it(
      "carries incremental and storage keys onto the generated #@ persist",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) refresh="incremental" watermark="order_date" storage="lake"
source: orders is orders_raw
`);
         const [p] = generatedPlans(c);
         expect(p.persist).toEqual({
            name: "orders",
            refresh: "incremental",
            watermark: "order_date",
            storage: "lake",
         });
         expect(
            Object.values(sourcesIn(c)).some((s) => s.name === "orders"),
         ).toBe(true);
      },
      { timeout: 30000 },
   );

   it(
      "builds a materialized extension from its parent's build source, after it (P10)",
      async () => {
         const pkg = await packages.create(`${BASE}
#(materialize) storage="lake"
source: orders is orders_raw extend {
  dimension: big is amount > 10
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: orders_ext is orders extend {
  where: region = 'east'
  dimension: east_big is big and region = 'east'
}

#(materialize) storage="lake"
source: orders_ext2 is orders_ext extend { where: east_big }
`);
         const c = await compilePackageBuildPlan(
            pkg,
            undefined,
            KEEP_INCREMENTAL,
         );
         expect(
            generatedPlans(c)
               .map((p) => p.buildSourceName)
               .sort(),
         ).toEqual(["orders__mat", "orders_ext2__mat", "orders_ext__mat"]);
         const plan = (name: string) =>
            generatedPlans(c).find((p) => p.sourceName === name)!;
         expect(plan("orders").buildsOver).toBeUndefined();
         expect(plan("orders_ext").buildsOver?.sourceName).toBe("orders");
         expect(plan("orders_ext2").buildsOver?.sourceName).toBe("orders_ext");
         expect(plan("orders_ext").columns.map((col) => col.column)).toContain(
            "east_big",
         );
         // Malloy's planner orders the builds: each reads its parent's table.
         // The sources a build copy re-roots share their parent's table and
         // are no build of their own, so only author sourceIDs remain.
         const [build] = c.builds;
         const ids = ["orders", "orders_ext", "orders_ext2"].map(
            (n) => plan(n).sourceID,
         );
         expect(build.order).toEqual(ids);
         expect(build.dependsOn).toEqual({
            [ids[0]]: [],
            [ids[1]]: [ids[0]],
            [ids[2]]: [ids[1]],
         });
         // Its table holds what the extension computes, read over the parent's.
         const sourceID = ids[2];
         const sql = buildSQLOf(
            sourcesIn(c)[sourceID],
            { entries: {} },
            targetsIn(c)?.[sourceID],
         );
         const duckdb = await pkg.getMalloyConnection!("duckdb");
         const rows = (
            await duckdb.runSQL(
               `SELECT id, east_big FROM (${sql}) t ORDER BY id`,
            )
         ).rows;
         expect(rows.map((r) => [Number(r.id), r.east_big])).toEqual([
            [1, true],
         ]);
      },
      { timeout: 30000 },
   );

   it(
      "builds a model in a subdirectory, importing it from beside the companion",
      async () => {
         const c = await compiled({
            "sub/m.malloy": `${BASE}
#(materialize) storage="lake"
source: orders is orders_raw
`,
         });
         const [sourceID] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         expect(modelPathsIn(c)?.[sourceID]).toBe("sub/m.malloy");
         expect(generatedPlans(c).map((p) => p.sourceName)).toEqual(["orders"]);
      },
      { timeout: 30000 },
   );

   it(
      "inlines a join_one: stores what reads through it and the joined source's dimensions, with a uniqueness check",
      async () => {
         const c = await compiled(`${JOINED}
#(materialize) inline=["regions"] storage="lake"
source: orders is orders_raw extend {
  join_one: regions with region
  dimension: region_label is regions.label
}
`);
         const [plan] = generatedPlans(c);
         expect(plan.inline).toEqual(["regions"]);
         expect(
            plan.columns.map((x) => [x.column, x.join ?? null, x.computed]),
         ).toEqual([
            ["id", null, false],
            ["region", null, false],
            ["amount", null, false],
            ["order_date", null, false],
            ["region_label", null, true],
            ["regions__code", "regions", true],
            ["regions__label", "regions", true],
            ["regions__loud_label", "regions", true],
         ]);
         expect(plan.uniquenessChecks).toHaveLength(1);
         const [check] = plan.uniquenessChecks ?? [];
         expect(check.join).toBe("regions");
         expect(check.keyColumn).toBe("code");
         expect(check.connectionName).toBe("duckdb");
         expect(check.sql).toContain("HAVING");
         // The check finds no duplicate here, and one when there is.
         const pkg = await packages.create("");
         const duckdb = await pkg.getMalloyConnection("duckdb");
         expect((await duckdb.runSQL(check.sql as string)).rows).toEqual([]);
         await assertInlinedJoinsUnique(plan, duckdb);
      },
      { timeout: 30000 },
   );

   it(
      "fails a build whose inlined join_one matches a key twice, naming the join",
      async () => {
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("""
  select * from (values ('east', 'East'), ('east', 'Also east'), ('west', 'West')) as t(code, label)
""") extend { primary_key: code }
#(materialize) inline=["regions"] storage="lake"
source: orders is orders_raw extend {
  join_one: regions with region
}
`);
         const [plan] = generatedPlans(c);
         const pkg = await packages.create("");
         const duckdb = await pkg.getMalloyConnection("duckdb");
         await expect(assertInlinedJoinsUnique(plan, duckdb)).rejects.toThrow(
            /inlines the join_one 'regions'/,
         );
         // A plan with no compiled check refuses, and says why.
         const unchecked = {
            ...plan,
            uniquenessChecks: (plan.uniquenessChecks ?? []).map((check) => ({
               ...check,
               sql: undefined,
            })),
         };
         await expect(
            assertInlinedJoinsUnique(unchecked, duckdb),
         ).rejects.toThrow(/carries no uniqueness check/);
      },
      { timeout: 30000 },
   );

   it(
      "inlines a join_many as a pair table: parent and child columns, and a parent-row flag written at build",
      async () => {
         const pkg = await packages.create(`${PAIRED}
#(materialize) storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  dimension:
    line_qty is lines.qty
    doubled is amount * 2
}
`);
         const c = await compilePackageBuildPlan(
            pkg,
            undefined,
            KEEP_INCREMENTAL,
         );
         const [plan] = generatedPlans(c);
         expect(plan.inline).toEqual(["lines"]);
         // A dimension through the join_many has a value per pair, so it is
         // computed at read, never stored.
         expect(plan.columns.map((x) => [x.column, x.join ?? null])).toEqual([
            ["id", null],
            ["region", null],
            ["amount", null],
            ["order_date", null],
            ["doubled", null],
            ["lines__line_id", "lines"],
            ["lines__order_id", "lines"],
            ["lines__qty", "lines"],
            ["lines__big_line", "lines"],
         ]);
         // No join_one to check; the parent key is checked, since the
         // parent-row flag marks one row per key, and the leaf's key, which
         // never fails the build (a count through the join needs it).
         expect(
            plan.uniquenessChecks?.map((x) => [
               x.pairLevel,
               x.keyColumn,
               x.recordOnly ?? false,
            ]),
         ).toEqual([
            [0, "id", false],
            [1, "line_id", true],
         ]);
         expect(plan.pair).toEqual({
            join: "lines",
            parentKey: "id",
            childKey: "line_id",
            childSourceID: expect.stringMatching(/^lines@/),
            parentRowColumn: "__parent_row",
         });
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(
            source,
            { entries: {} },
            targetsIn(c)?.[sourceID],
         );
         const duckdb = await pkg.getMalloyConnection("duckdb");
         const rows = (
            await duckdb.runSQL(
               `SELECT id, lines__line_id, __parent_row FROM (${sql}) t ORDER BY id, lines__line_id`,
            )
         ).rows.map((r) => [
            Number(r.id),
            r.lines__line_id === null ? null : Number(r.lines__line_id),
            r.__parent_row,
         ]);
         // Order 3 has no line and keeps one row; line 4 pairs with no order.
         expect(rows.map(([id, line]) => [id, line])).toEqual([
            [1, 1],
            [1, 2],
            [2, 3],
            [3, null],
         ]);
         // Exactly one parent row per order.
         for (const id of [1, 2, 3]) {
            expect(
               rows.filter(([o, , parent]) => o === id && parent === true),
            ).toHaveLength(1);
         }
      },
      { timeout: 30000 },
   );

   for (const [tier, route] of [
      ["database", ""],
      ["storage", 'storage="lake" '],
   ]) {
      it(
         `builds an extension over a pair table with its join put back, relying on its leaf check (${tier})`,
         async () => {
            const ext = route.trim();
            const pkg = await packages.create(`${PAIRED}
#(materialize) ${route}inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  measure: order_count is count()
}

#(materialize)${ext ? ` ${ext}` : ""}
source: orders_ext is orders extend {
  measure: twice is order_count * 2
}
`);
            const c = await compilePackageBuildPlan(
               pkg,
               undefined,
               KEEP_INCREMENTAL,
            );
            expect(
               generatedPlans(c).find((p) => p.sourceName === "orders")?.pair
                  ?.join,
            ).toBe("lines");
            // Linked over the pair table, read back one row per parent,
            // never built over the raw chain.
            expect(
               generatedPlans(c).find((p) => p.sourceName === "orders_ext")
                  ?.buildsOver?.mode,
            ).toEqual({
               tier: "same",
               pair: { leafKeys: ["leaf:lines.line_id"] },
            });
            expect(
               (c.annotationFindings ?? []).filter(
                  (f) => f.code === "materialize-not-built",
               ),
            ).toEqual([]);
         },
         { timeout: 30000 },
      );
   }

   it(
      "proves a middle level one parent per row only down joins keyed on each parent level's primary key",
      async () => {
         const pkg = await packages.create(`${PAIRED}
source: picks is duckdb.sql("""
  select * from (values (10, 1, 'a'), (11, 1, 'b')) as t(pick_id, line_id, bin)
""") extend { primary_key: pick_id }
source: bins is duckdb.sql("""
  select * from (values (100, 10, 'x')) as t(bin_id, pick_id, tag)
""") extend { primary_key: bin_id }
source: picks_binned is picks extend {
  join_many: bins on bins.pick_id = pick_id
}
source: lines_picked is lines extend {
  join_many: picks on picks.line_id = line_id
}
source: lines_by_qty is lines extend {
  dimension: qty_key is qty
  join_many: picks is picks_binned on picks.line_id = qty_key
}
#(materialize) storage="lake" inline=["lp.picks"]
source: by_key is orders_raw extend {
  primary_key: id
  dimension: id2 is id
  join_many: lp is lines_picked on lp.order_id = id2
}
#(materialize) storage="lake" inline=["lq.picks.bins"]
source: by_qty is orders_raw extend {
  primary_key: id
  join_many: lq is lines_by_qty on lq.order_id = id
}
`);
         const c = await compilePackageBuildPlan(
            pkg,
            undefined,
            KEEP_INCREMENTAL,
         );
         const plans = new Map(
            generatedPlans(c).map((p) => [p.sourceName, p] as const),
         );
         // Keyed on a dimension of the order, not its primary key.
         expect(plans.get("by_key")?.pair?.oneParentLevels).toBeUndefined();
         // The first level is keyed on the order's key; the second on a
         // dimension of the line, so it and every level below stop there.
         expect(plans.get("by_qty")?.pair?.oneParentLevels).toEqual([1]);
      },
      { timeout: 30000 },
   );

   it(
      "inlines a path of join_manys as one pair table, with a row flag per level above the last",
      async () => {
         const pkg = await packages.create(`${PAIRED}
source: picks is duckdb.sql("""
  select * from (values (10, 1, 'a'), (11, 1, 'b'), (12, 3, 'a')) as t(pick_id, line_id, bin)
""") extend { primary_key: pick_id }
source: lines_picked is lines extend {
  join_many: picks on picks.line_id = line_id
}
#(materialize) storage="lake" inline=["lp", "lp.picks"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
}
`);
         const c = await compilePackageBuildPlan(
            pkg,
            undefined,
            KEEP_INCREMENTAL,
         );
         const [plan] = generatedPlans(c);
         // `lp` is the start of `lp.picks`, which stores it: one table.
         expect(plan.pairBlocks).toBeUndefined();
         expect(plan.pair).toEqual({
            join: "lp",
            parentKey: "id",
            childKey: "line_id",
            childSourceID: expect.stringMatching(/^lines_picked@/),
            parentRowColumn: "__parent_row",
            // `lp` is keyed on the order's primary key: each line has one
            // order, so its flag marks one row per line under any order.
            oneParentLevels: [1],
            hops: [
               {
                  join: "picks",
                  key: "pick_id",
                  sourceID: expect.stringMatching(/^picks@/),
                  rowColumn: "__child_row",
               },
            ],
         });
         expect(
            plan.columns
               .filter((x) => x.join !== undefined)
               .map((x) => [x.column, x.path ?? null]),
         ).toEqual([
            ["lp__line_id", null],
            ["lp__order_id", null],
            ["lp__qty", null],
            ["lp__big_line", null],
            ["lp__picks__pick_id", ["lp", "picks"]],
            ["lp__picks__line_id", ["lp", "picks"]],
            ["lp__picks__bin", ["lp", "picks"]],
         ]);
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(
            source,
            { entries: {} },
            targetsIn(c)?.[sourceID],
         );
         const duckdb = await pkg.getMalloyConnection("duckdb");
         const rows = (
            await duckdb.runSQL(
               `SELECT id, lp__line_id AS l, lp__picks__pick_id AS p, __parent_row AS pr, __child_row AS cr FROM (${sql}) t ORDER BY id, l, p`,
            )
         ).rows.map((r) => [
            Number(r.id),
            r.l === null ? null : Number(r.l),
            r.p === null ? null : Number(r.p),
            r.pr,
            r.cr,
         ]);
         // Line 1 has two picks, line 2 none, line 3 one; order 3 no line.
         expect(rows.map(([id, l, p]) => [id, l, p])).toEqual([
            [1, 1, 10],
            [1, 1, 11],
            [1, 2, null],
            [2, 3, 12],
            [3, null, null],
         ]);
         for (const id of [1, 2, 3]) {
            expect(
               rows.filter((r) => r[0] === id && r[3] === true),
            ).toHaveLength(1);
         }
         // One child-row flag per order and line, a childless order's row
         // included.
         for (const [id, line] of [
            [1, 1],
            [1, 2],
            [2, 3],
            [3, null],
         ]) {
            expect(
               rows.filter(
                  (r) => r[0] === id && r[1] === line && r[4] === true,
               ),
            ).toHaveLength(1);
         }
      },
      { timeout: 30000 },
   );

   it(
      "stores two join_manys as two blocks of one table, each the rows of its own pair table",
      async () => {
         const pkg = await packages.create(`${PAIRED}
source: notes is duckdb.sql("""
  select * from (values (7, 1, 'late'), (8, 2, 'gift'), (9, 2, 'late')) as t(note_id, order_id, topic)
""") extend { primary_key: note_id }
#(materialize) storage="lake" inline=["lines", "notes"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  join_many: notes on notes.order_id = id
}
`);
         const c = await compilePackageBuildPlan(
            pkg,
            undefined,
            KEEP_INCREMENTAL,
         );
         const [plan] = generatedPlans(c);
         expect(plan.pair?.join).toBe("lines");
         expect(plan.pairBlocks).toEqual({
            blocks: [
               expect.objectContaining({ join: "lines" }),
               expect.objectContaining({ join: "notes" }),
            ],
            laneColumn: "__pair_lane",
            connection: "duckdb",
            columnTypes: {
               lines__line_id: "number",
               lines__order_id: "number",
               lines__qty: "number",
               lines__big_line: "boolean",
               notes__note_id: "number",
               notes__order_id: "number",
               notes__topic: "string",
            },
            levels: 1,
         });
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(
            source,
            { entries: {} },
            targetsIn(c)?.[sourceID],
         );
         const duckdb = await pkg.getMalloyConnection("duckdb");
         const rows = (
            await duckdb.runSQL(
               `SELECT __pair_lane AS b, id, lines__line_id AS l, notes__note_id AS n, __parent_row AS pr FROM (${sql}) t ORDER BY b, id, l, n`,
            )
         ).rows.map((r) =>
            [r.b, r.id, r.l, r.n].map((v) => (v === null ? null : Number(v))),
         );
         // Block 0 is the lines pair table, block 1 the notes one; neither
         // pairs a line with a note.
         expect(rows).toEqual([
            [0, 1, 1, null],
            [0, 1, 2, null],
            [0, 2, 3, null],
            [0, 3, null, null],
            [1, 1, null, 7],
            [1, 2, null, 8],
            [1, 2, null, 9],
            [1, 3, null, null],
         ]);
         const flagged = (
            await duckdb.runSQL(
               `SELECT __pair_lane AS b, count(*) AS n FROM (${sql}) t WHERE __parent_row GROUP BY 1 ORDER BY 1`,
            )
         ).rows.map((r) => [Number(r.b), Number(r.n)]);
         // One parent row per order in each block.
         expect(flagged).toEqual([
            [0, 3],
            [1, 3],
         ]);
      },
      { timeout: 30000 },
   );

   it(
      "checks a pair table's keys before each build (one hop, a path, blocks): unique and never null at every level above the leaf",
      async () => {
         const model = (lines: string, orders = "orders_raw") => `${BASE}
source: lines is duckdb.sql("""
  select * from (values ${lines}) as t(line_id, order_id, qty)
""") extend { primary_key: line_id }
source: picks is duckdb.sql("""
  select * from (values (10, 1, 'a'), (11, 1, 'b'), (12, 3, 'a')) as t(pick_id, line_id, bin)
""") extend { primary_key: pick_id }
source: lines_picked is lines extend {
  join_many: picks on picks.line_id = line_id
}
source: notes is duckdb.sql("""
  select * from (values (7, 1, 'late'), (8, 2, 'gift')) as t(note_id, order_id, topic)
""") extend { primary_key: note_id }
#(materialize) storage="lake" inline=["lp.picks"]
source: orders_hop is ${orders} extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
}
#(materialize) storage="lake" inline=["lines", "notes"]
source: orders_blocks is ${orders} extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  join_many: notes on notes.order_id = id
}
#(materialize) storage="lake" inline=["lines"]
source: orders_one is ${orders} extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
`;
         const plansOf = async (text: string) => {
            const pkg = await packages.create(text);
            const c = await compilePackageBuildPlan(
               pkg,
               undefined,
               KEEP_INCREMENTAL,
            );
            const byName = new Map(
               generatedPlans(c).map((p) => [p.sourceName, p]),
            );
            return { byName, duckdb: await pkg.getMalloyConnection("duckdb") };
         };
         const clean = await plansOf(
            model("(1, 1, 2.0), (2, 1, 3.0), (3, 2, 4.0)"),
         );
         // The path checks the source's key and the middle level's, which
         // fail the build; the leaf's is checked too, never failing it (a
         // count through the path is served only where it passed). The block
         // table checks the source's key, and each block's leaf. A one-hop
         // table checks the source's key too: its parent-row flag marks one
         // row per key.
         const checksOf = (name: string) =>
            clean.byName
               .get(name)
               ?.uniquenessChecks?.map((x) => [
                  x.pairLevel,
                  x.join,
                  x.keyColumn,
                  x.recordOnly ?? false,
               ]);
         expect(checksOf("orders_hop")).toEqual([
            [0, "", "id", false],
            [1, "lp", "line_id", false],
            [2, "lp.picks", "pick_id", true],
         ]);
         expect(checksOf("orders_blocks")).toEqual([
            [0, "", "id", false],
            [1, "lines", "line_id", true],
            [1, "notes", "note_id", true],
         ]);
         expect(checksOf("orders_one")).toEqual([
            [0, "", "id", false],
            [1, "lines", "line_id", true],
         ]);
         for (const name of ["orders_hop", "orders_blocks", "orders_one"]) {
            await assertInlinedJoinsUnique(
               clean.byName.get(name),
               clean.duckdb,
            );
         }
         // A middle key held twice, or null, fails the path's build.
         for (const lines of [
            "(1, 1, 2.0), (1, 1, 3.0), (3, 2, 4.0)",
            "(1, 1, 2.0), (NULL, 1, 3.0), (3, 2, 4.0)",
         ]) {
            const bad = await plansOf(model(lines));
            await expect(
               assertInlinedJoinsUnique(
                  bad.byName.get("orders_hop"),
                  bad.duckdb,
               ),
            ).rejects.toThrow(
               /keys one row flag on 'line_id' of the source 'lp'/,
            );
            // There `lines` is the leaf: its key check never fails the build,
            // and finds the key held twice, or null.
            for (const name of ["orders_blocks", "orders_one"]) {
               await assertInlinedJoinsUnique(bad.byName.get(name), bad.duckdb);
               const [leaf] = pairLeafKeyChecksOf(bad.byName.get(name));
               expect(leaf).toMatchObject({
                  keyColumn: "line_id",
                  sourceID: expect.stringMatching(/^lines@/),
               });
               expect((await runKeyCheck(leaf, bad.duckdb))?.unique).toBe(
                  false,
               );
            }
         }
         // A clean leaf passes its check.
         for (const name of ["orders_hop", "orders_blocks", "orders_one"]) {
            for (const leaf of pairLeafKeyChecksOf(clean.byName.get(name))) {
               expect((await runKeyCheck(leaf, clean.duckdb))?.unique).toBe(
                  true,
               );
            }
         }
         // A null source key fails all three.
         const nullParent = await plansOf(
            model(
               "(1, 1, 2.0), (2, 1, 3.0), (3, 2, 4.0)",
               "orders_raw extend { dimension: id2 is pick null when id = 3 else id } -> { select: id is id2, region, amount, order_date }",
            ),
         );
         for (const name of ["orders_hop", "orders_blocks", "orders_one"]) {
            await expect(
               assertInlinedJoinsUnique(
                  nullParent.byName.get(name),
                  nullParent.duckdb,
               ),
            ).rejects.toThrow(/keys one row flag on 'id' of '.*' itself/);
         }
      },
      { timeout: 60000 },
   );

   it(
      "checks a pair table's key over the rows its build stores: a given's term and an upper bound left out of the build are left out of the check",
      async () => {
         const model = (
            orders: string,
            where: string,
         ) => `##! experimental { givens }
given:
  ORG_ID :: number${where.includes("DEFAULT") ? " is 1" : ""}
source: raw is duckdb.sql("""
  select * from (values ${orders}) as t(id, org_id, amount, t)
""")
source: lines is duckdb.sql("""
  select * from (values (1, 1, 2.0), (2, 2, 3.0)) as t(line_id, order_id, qty)
""") extend { primary_key: line_id }
#(materialize) storage="lake" inline=["lines"]
source: orders is raw extend {
  primary_key: id
  where: ${where.replace(" DEFAULT", "")}
  join_many: lines on lines.order_id = id
}
`;
         const planOf = async (text: string) => {
            const pkg = await packages.create(text);
            const c = await compilePackageBuildPlan(
               pkg,
               undefined,
               KEEP_INCREMENTAL,
            );
            const plan = generatedPlans(c).find(
               (p) => p.sourceName === "orders",
            );
            return { plan, duckdb: await pkg.getMalloyConnection("duckdb") };
         };
         const old = "timestamp '2020-01-01'";
         const late = "timestamp '2999-01-01'";
         // A given with no default: the check over the author's source could
         // not compile, which failed the whole companion. Over the build copy
         // it compiles, and passes on a key unique across every tenant.
         const clean = await planOf(
            model(
               `(1, 1, 1.0, ${old}), (2, 2, 2.0, ${old})`,
               "org_id = $ORG_ID",
            ),
         );
         expect(clean.plan?.uniquenessChecks?.map((x) => x.pairLevel)).toEqual([
            0, 1,
         ]);
         await assertInlinedJoinsUnique(clean.plan, clean.duckdb);
         // A key unique within the default tenant but held by two tenants:
         // the table stores both rows, so the build is refused.
         const tenants = await planOf(
            model(
               `(1, 1, 1.0, ${old}), (1, 2, 2.0, ${old})`,
               "org_id = $ORG_ID DEFAULT",
            ),
         );
         await expect(
            assertInlinedJoinsUnique(tenants.plan, tenants.duckdb),
         ).rejects.toThrow(/keys one row flag on 'id' of 'orders' itself/);
         // A key held twice across an upper-bound window's cut-off: one row
         // live, two in the table.
         const window = await planOf(
            model(
               `(1, 1, 1.0, ${old}), (1, 1, 2.0, ${late})`,
               "t < now - 30 days",
            ),
         );
         expect(window.plan?.readTimeTerms?.map((t) => t.kind)).toEqual([
            "current_time",
         ]);
         await expect(
            assertInlinedJoinsUnique(window.plan, window.duckdb),
         ).rejects.toThrow(/keys one row flag on 'id' of 'orders' itself/);
      },
      { timeout: 60000 },
   );

   it(
      "stores a many-to-many pair once per pair, and flags each parent once",
      async () => {
         const pkg = await packages.create(`${BASE}
source: notes is duckdb.sql("""
  select * from (values (1, 'east'), (2, 'east'), (3, 'north')) as t(note_id, region)
""") extend { primary_key: note_id }
#(materialize) storage="lake" inline=["notes"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: notes on notes.region = region
}
`);
         const c = await compilePackageBuildPlan(
            pkg,
            undefined,
            KEEP_INCREMENTAL,
         );
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(
            source,
            { entries: {} },
            targetsIn(c)?.[sourceID],
         );
         const duckdb = await pkg.getMalloyConnection("duckdb");
         const rows = (
            await duckdb.runSQL(
               `SELECT id, notes__note_id AS n, __parent_row AS p FROM (${sql}) t ORDER BY id, n`,
            )
         ).rows.map((r) => [
            Number(r.id),
            r.n === null ? null : Number(r.n),
            r.p,
         ]);
         expect(rows.map(([id, n]) => [id, n])).toEqual([
            [1, 1],
            [1, 2],
            [2, null],
            [3, 1],
            [3, 2],
         ]);
         // A note shared by two orders is stored with each, as live's LEFT
         // JOIN pairs it with each; each order is flagged once.
         for (const id of [1, 2, 3]) {
            expect(
               rows.filter(([o, , flag]) => o === id && flag === true),
            ).toHaveLength(1);
         }
      },
      { timeout: 30000 },
   );

   it(
      "plans an incremental pair table: a per-parent watermark over both sides, replaced by the parent key",
      async () => {
         const c = await compiled(
            pairSource('watermark=["order_date", "lines.loaded_on"]'),
         );
         const [plan] = generatedPlans(c);
         expect(plan.pairFullRefresh).toBeUndefined();
         expect(plan.pair?.watermark).toEqual({
            column: "__pair_watermark",
            parts: ["order_date", "lines__loaded_on"],
         });
         // v1's machinery reads the generated watermark and merges by the
         // parent key, which the delta turns into whole-parent replacement.
         expect(plan.persist).toMatchObject({
            refresh: "incremental",
            watermark: "__pair_watermark",
            merge_key: "id",
         });
         // The parent key check runs before each build, a refresh included:
         // a delta that brings a repeated or null key fails as a full build
         // would, and the plan stays incremental. The leaf's never fails it.
         expect(
            plan.uniquenessChecks?.map((x) => [
               x.pairLevel,
               x.keyColumn,
               x.recordOnly ?? false,
            ]),
         ).toEqual([
            [0, "id", false],
            [1, "line_id", true],
         ]);
         expect(
            (c.annotationFindings ?? []).filter((f) =>
               f.code.startsWith("materialize-"),
            ),
         ).toEqual([]);
         // Each parent's rows carry one watermark: the greatest over the
         // parent and its children; a childless parent its own.
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(
            source,
            { entries: {} },
            targetsIn(c)?.[sourceID],
         );
         const pkg = await packages.create(DATED);
         const duckdb = await pkg.getMalloyConnection("duckdb");
         const rows = (
            await duckdb.runSQL(
               `SELECT id, count(*) AS n, count(DISTINCT __pair_watermark) AS w,
                       max(__pair_watermark) AS wm,
                       count(*) FILTER (WHERE __parent_row) AS flags
                  FROM (${sql}) t GROUP BY id ORDER BY id`,
            )
         ).rows.map((r) => [
            Number(r.id),
            Number(r.n),
            Number(r.w),
            new Date(String(r.wm)).toISOString().slice(0, 10),
            Number(r.flags),
         ]);
         expect(rows).toEqual([
            [1, 2, 1, "2026-01-05", 1],
            [2, 1, 1, "2026-01-02", 1],
            [3, 1, 1, "2026-01-03", 1],
         ]);
         // The two generated windows are partitioned by the parent key, so
         // the window gate does not apply to them; nothing else changes.
         const declaration = collectIncrementalDeclarations(c.tables)[
            plan.sourceID
         ];
         expect(declaration.calculateFields).toEqual([]);
         expect(declaration.pair).toEqual({
            parentKey: "id",
            parentRow: "__parent_row",
         });
         expect(declaration.strategy).toBe("merge");
         // The publish gate admits it on an allowlisted origin, the window
         // rule included.
         expect(
            incrementalPolicyRejections([
               { sourceName: "orders", dialect: "postgres", declaration },
            ]),
         ).toEqual([]);
         const lineage = (targetDialect: string) =>
            incrementalLineage({
               declaration,
               dialect: "postgres",
               targetDialect,
               physicalTableName: "orders_all",
               connectionName: "duckdb",
               sourceEntityId: "sid",
            });
         expect(lineage("duckdb")).toMatchObject({
            watermarkName: "__pair_watermark",
            mergeKeys: ["id"],
            pair: { parentKey: "id", parentRow: "__parent_row" },
         });
         // A pair delta is proven in DuckDB only: anything else seeds.
         expect(lineage("postgres")).toBeUndefined();
      },
      { timeout: 30000 },
   );

   it(
      "plans an incremental pair table of a path: a field at every level, each level's flag checked after the delta",
      async () => {
         const c = await compiled(
            pathSource(
               'watermark=["order_date", "lp.loaded_on", "lp.picks.picked_on"]',
            ),
         );
         const [plan] = generatedPlans(c);
         expect(plan.pairFullRefresh).toBeUndefined();
         expect(plan.pair?.watermark).toEqual({
            column: "__pair_watermark",
            parts: ["order_date", "lp__loaded_on", "lp__picks__picked_on"],
         });
         expect(plan.persist).toMatchObject({
            refresh: "incremental",
            watermark: "__pair_watermark",
            merge_key: "id",
         });
         const declaration = collectIncrementalDeclarations(c.tables)[
            plan.sourceID
         ];
         // Every generated window is partitioned within a parent.
         expect(declaration.calculateFields).toEqual([]);
         expect(declaration.pair).toEqual({
            parentKey: "id",
            parentRow: "__parent_row",
            checks: [
               { flag: "__parent_row", keys: ["id"] },
               { flag: "__child_row", keys: ["id", "lp__line_id"] },
            ],
         });
         expect(
            incrementalPolicyRejections([
               { sourceName: "orders", dialect: "postgres", declaration },
            ]),
         ).toEqual([]);
         expect(
            incrementalLineage({
               declaration,
               dialect: "postgres",
               targetDialect: "duckdb",
               physicalTableName: "orders_all",
               connectionName: "duckdb",
               sourceEntityId: "sid",
            }),
         ).toMatchObject({ pair: declaration.pair });
      },
      { timeout: 30000 },
   );

   it(
      "plans an incremental table of several blocks: one watermark per parent across every block, each block's flags checked",
      async () => {
         const pkg = await packages.create(`${PICKED}
source: pays is duckdb.sql("""
  select * from (values (50, 1, date '2026-01-04'), (51, 3, date '2026-01-09')) as t(pay_id, order_id, paid_on)
""") extend { primary_key: pay_id }
#(materialize) storage="lake" inline=["lp.picks", "pays"] refresh="incremental" watermark=["order_date", "lp.loaded_on", "lp.picks.picked_on", "pays.paid_on"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
  join_many: pays on pays.order_id = id
}
`);
         const c = await compilePackageBuildPlan(
            pkg,
            undefined,
            KEEP_INCREMENTAL,
         );
         const [plan] = generatedPlans(c);
         expect(plan.pairFullRefresh).toBeUndefined();
         expect(plan.pairBlocks?.watermark).toEqual({
            column: "__pair_watermark",
            parts: [
               "order_date",
               "lp__loaded_on",
               "lp__picks__picked_on",
               "pays__paid_on",
            ],
         });
         expect(plan.persist).toMatchObject({
            refresh: "incremental",
            watermark: "__pair_watermark",
            merge_key: "id",
         });
         const declaration = collectIncrementalDeclarations(c.tables)[
            plan.sourceID
         ];
         expect(declaration.calculateFields).toEqual([]);
         expect(declaration.pair).toEqual({
            parentKey: "id",
            parentRow: "__parent_row",
            laneColumn: "__pair_lane",
            lanes: 2,
            checks: [
               { flag: "__parent_row", keys: ["id"], lane: 0 },
               { flag: "__child_row", keys: ["id", "lp__line_id"], lane: 0 },
               { flag: "__parent_row", keys: ["id"], lane: 1 },
               // Past the payments block's leaf: every key it has.
               { flag: "__child_row", keys: ["id", "pays__pay_id"], lane: 1 },
            ],
         });
         expect(
            incrementalPolicyRejections([
               { sourceName: "orders", dialect: "postgres", declaration },
            ]),
         ).toEqual([]);
         // Built: each parent carries one watermark across both blocks, the
         // greatest over all of its rows.
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(
            source,
            { entries: {} },
            targetsIn(c)?.[sourceID],
         );
         const duckdb = await pkg.getMalloyConnection("duckdb");
         const rows = (
            await duckdb.runSQL(
               `SELECT id, count(DISTINCT __pair_watermark) AS w, max(__pair_watermark) AS wm,
                       count(DISTINCT __pair_lane) AS lanes
                  FROM (${sql}) t GROUP BY id ORDER BY id`,
            )
         ).rows.map((r) => [
            Number(r.id),
            Number(r.w),
            new Date(String(r.wm)).toISOString().slice(0, 10),
            Number(r.lanes),
         ]);
         expect(rows).toEqual([
            // Order 1: its line 1's pick; order 2: its line 3's pick; order
            // 3, with no line: its payment.
            [1, 1, "2026-01-07", 2],
            [2, 1, "2026-01-08", 2],
            [3, 1, "2026-01-09", 2],
         ]);
      },
      { timeout: 30000 },
   );

   it(
      "leaves the window gate in force on a pair table that is not incremental",
      async () => {
         const c = await compiled(`${DATED}
#(materialize) storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
`);
         const [plan] = generatedPlans(c);
         expect(plan.pair?.watermark).toBeUndefined();
         expect(plan.persist.refresh).toBeUndefined();
         const declaration = collectIncrementalDeclarations(c.tables)[
            plan.sourceID
         ];
         expect(declaration.pair).toBeUndefined();
         expect(declaration.calculateFields).toEqual(["__parent_row"]);
      },
      { timeout: 30000 },
   );

   it(
      "carries freshness onto the generated source, where the plan reads it",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) freshness.window="24h" freshness.fallback="stale_ok" storage="lake"
source: orders is orders_raw
`);
         const [p] = generatedPlans(c);
         expect(p.persist).toEqual({
            name: "orders",
            storage: "lake",
            "freshness.window": "24h",
            "freshness.fallback": "stale_ok",
         });
         const outcome = derivePackagePlan(c, null);
         const wire = Object.values(outcome.plan?.sources ?? {}).find(
            (s) => s.name === "orders",
         );
         expect(wire?.freshness?.window).toBe("24h");
      },
      { timeout: 30000 },
   );

   it(
      "presents the generated source under the author's name and sourceID, at the generated source's own address",
      async () => {
         const c = await compiled(`##! experimental.persistence
${BASE}
#@ persist name="by_region"
source: by_region is orders_raw -> { group_by: region }

#(materialize) storage="lake"
source: orders is orders_raw extend { dimension: big is amount > 10 }
`);
         const [p] = generatedPlans(c);
         // Keyed by the author's declaration, never by the generated name.
         const source = sourcesIn(c)[p.sourceID];
         expect(source.name).toBe("orders");
         expect(source.sourceID).toBe(p.sourceID);
         expect(p.sourceID.startsWith("orders@")).toBe(true);
         expect(materializePlansIn(c)?.[p.sourceID]).toBe(p);
         // The generated source it presents: the companion's, at the same
         // address and SQL, so no key the control plane holds moves.
         const generated = Object.getPrototypeOf(source) as typeof source;
         expect(generated.name).toBe("orders__mat");
         const target = targetsIn(c)?.[p.sourceID];
         expect(
            computeSourceEntityId(source, c.connectionDigests, target),
         ).toBe(computeSourceEntityId(generated, c.connectionDigests, target));
         expect(addressSQL(source, target)).toBe(addressSQL(generated, target));
         // The graph names the author's source too.
         expect(c.builds.flatMap((b) => b.order)).toContain(p.sourceID);
         // Every name in the plan is unique: a source that is both #@ persist
         // and #(materialize) is refused, so the author's name is free.
         const outcome = derivePackagePlan(c, null);
         const names = Object.values(outcome.plan?.sources ?? {}).map(
            (s) => s.name,
         );
         expect(names.sort()).toEqual(["by_region", "orders"]);
         expect(outcome.plan?.sources[p.sourceID]?.sourceEntityId).toBe(
            computeSourceEntityId(generated, c.connectionDigests, target),
         );
         expect(JSON.stringify(outcome.plan)).not.toContain("__mat");
      },
      { timeout: 30000 },
   );
});
