// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   addressSQL,
   buildSQLOf,
   computeSourceEntityId,
   deriveBuildPlanOutcome,
} from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";
import { collectIncrementalDeclarations } from "./build_plan";
import { incrementalLineage } from "../run/incremental_build";
import { assertInlinedJoinsUnique } from "../run/inline_uniqueness";
import { rollingWindowsOf } from "./rolling_window";
import {
   buildCopyText,
   buildSourceName,
   generateBuildFile,
   generateBuildFiles,
   materializeRefusalFinding,
   planModelMaterialize,
   type MaterializePlan,
} from "./build_file_generator";

/**
 * The `#(materialize)` build-file generator (materialization v2, phase 3,
 * PR 2b): the generated companion's text for each shape, that it compiles and
 * builds what the source computes, and that with PUBLISHER_MATERIALIZE off
 * nothing changes.
 */

const BASE = `source: orders_raw is duckdb.sql("""
  select * from (values
    (1, 'east', 10.5, date '2026-01-01'),
    (2, 'west', 20.0, date '2026-01-02'),
    (3, 'east', 5.25, date '2026-01-03')
  ) as t(id, region, amount, order_date)
""")
`;

const packages = tempBuildPlanPackages();
let restore: () => void;

function enable(on: boolean) {
   restore?.();
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: on ? "true" : "false" },
   }).restore;
}

beforeEach(() => enable(true));
afterEach(async () => {
   restore?.();
   await packages.cleanup();
});

async function contentsOf(
   pkg: Awaited<ReturnType<typeof packages.create>>,
): Promise<Record<string, unknown>> {
   const { Model } = await import("../../model");
   const { runtime, modelURL, importBaseURL } = await Model.getModelRuntime(
      pkg.getPackagePath(),
      "m.malloy",
      pkg.getMalloyConfig(),
   );
   const m = await runtime.loadModel(modelURL, { importBaseURL }).getModel();
   return m._modelDef.contents as Record<string, unknown>;
}

async function compiled(files: string | Record<string, string>) {
   const pkg = await packages.create(files);
   return compilePackageBuildPlan(pkg);
}

function generatedPlans(
   c: Awaited<ReturnType<typeof compiled>>,
): MaterializePlan[] {
   return Object.values(c.materializePlans ?? {});
}

describe("planModelMaterialize and generateBuildFile", () => {
   const plan = (overrides: Partial<MaterializePlan>): MaterializePlan => ({
      sourceName: "orders",
      sourceID: "orders@file:///pkg/m.malloy",
      buildSourceName: buildSourceName("orders"),
      columns: [
         { dimension: "id", column: "id", computed: false },
         { dimension: "big", column: "big", computed: true },
      ],
      persist: {},
      ...overrides,
   });

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
                  partition: "order_date",
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
         '#@ persist name="orders_all" storage="my-lake" refresh="incremental" watermark="order_date" partition="order_date"',
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
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "nested",
         )!;
         expect(
            buildSQLOf(source, { entries: {} }, c.targets?.[sourceID]),
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

#(materialize) name="recent_events" refresh="incremental" watermark="ts"
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
         const declaration = collectIncrementalDeclarations(
            c.sources,
            c.materializePlans,
         )[p.sourceID];
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
         const sql = addressSQL(c.sources[p.sourceID], c.targets?.[p.sourceID]);
         expect(sql).toContain("LOCALTIMESTAMP");
         expect(sql).not.toMatch(/TIMESTAMP '20\d\d-/);
      },
      { timeout: 30000 },
   );

   it(
      "plans orders__mat: public dimensions stored under their own names, measures, views, joins and private fields left out",
      async () => {
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("select 'east' as region, 'East' as label") extend {
  primary_key: region
}

#(materialize) name="orders_all"
source: orders is orders_raw extend {
  primary_key: id
  dimension: big is amount > 10
  private dimension: secret is amount * 2
  measure: total is amount.sum()
  view: by_region is { group_by: region; aggregate: total }
  join_one: regions with region
}
`);
         const [p] = generatedPlans(c);
         expect(p.buildSourceName).toBe("orders__mat");
         expect(p.columns).toEqual([
            { dimension: "id", column: "id", computed: false },
            { dimension: "region", column: "region", computed: false },
            { dimension: "amount", column: "amount", computed: false },
            { dimension: "order_date", column: "order_date", computed: false },
            { dimension: "big", column: "big", computed: true },
         ]);
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         expect(c.sourceModelPaths?.[sourceID]).toBe("m.malloy");
         // Addressed like any persist source.
         expect(
            computeSourceEntityId(
               source,
               c.connectionDigests,
               c.targets?.[sourceID],
            ),
         ).toEqual(expect.any(String));
         expect(addressSQL(source, c.targets?.[sourceID])).not.toContain(
            "secret",
         );
      },
      { timeout: 30000 },
   );

   it(
      "builds exactly what the source computes",
      async () => {
         const pkg = await packages.create(`${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw extend { dimension: big is amount > 10 }
`);
         const c = await compilePackageBuildPlan(pkg);
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(source, { entries: {} }, c.targets?.[sourceID]);
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
      "carries partition, incremental and storage keys onto the generated #@ persist",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) refresh="incremental" watermark="order_date" partition="order_date" storage="lake"
source: orders is orders_raw
`);
         const [p] = generatedPlans(c);
         expect(p.persist).toEqual({
            name: "orders",
            refresh: "incremental",
            watermark: "order_date",
            partition: "order_date",
            storage: "lake",
         });
         expect(Object.values(c.sources).some((s) => s.name === "orders")).toBe(
            true,
         );
      },
      { timeout: 30000 },
   );

   it(
      "builds a materialized extension from its parent's build source, after it (P10)",
      async () => {
         const pkg = await packages.create(`${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw extend {
  dimension: big is amount > 10
  measure: total is amount.sum()
}

#(materialize) name="orders_east"
source: orders_ext is orders extend {
  where: region = 'east'
  dimension: east_big is big and region = 'east'
}

#(materialize) name="orders_east_big"
source: orders_ext2 is orders_ext extend { where: east_big }
`);
         const c = await compilePackageBuildPlan(pkg);
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
            c.sources[sourceID],
            { entries: {} },
            c.targets?.[sourceID],
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
      "builds an extension on another tier than its parent, an incremental one, and one in another file, over the raw chain",
      async () => {
         const c = await compiled({
            "m.malloy": `${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw

#(materialize) storage="lake"
source: stored_ext is orders extend { dimension: east is region = 'east' }

#(materialize) name="orders_inc" refresh="incremental" watermark="order_date"
source: inc_ext is orders extend { dimension: west is region = 'west' }
`,
            "other.malloy": `import "m.malloy"
#(materialize) name="other_ext"
source: other_ext is orders extend { dimension: n is 1 }
`,
         });
         for (const p of generatedPlans(c)) {
            expect([p.sourceName, p.buildsOver]).toEqual([
               p.sourceName,
               undefined,
            ]);
         }
         expect(
            generatedPlans(c)
               .map((p) => p.sourceName)
               .sort(),
         ).toEqual(["inc_ext", "orders", "other_ext", "stored_ext"]);
      },
      { timeout: 30000 },
   );

   it(
      "builds a storage= extension over its parent's stored table when both are in one destination (P10)",
      async () => {
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("select 'east' as code") extend {
  primary_key: code
}

#(materialize) storage="lake"
source: orders is orders_raw extend {
  dimension: big is amount > 10
  measure: total is amount.sum()
}

source: mid is orders extend { measure: n is count() }

#(materialize) storage="lake"
source: east is mid extend {
  where: region = 'east'
  dimension: label is concat(region, '!')
}

#(materialize) storage="other"
source: elsewhere is orders extend { dimension: w is region = 'west' }

#(materialize) storage="lake"
source: joined is orders extend {
  join_one: regions on region = regions.code
  dimension: amount2 is amount * 2
}
`);
         const plan = (name: string) =>
            generatedPlans(c).find((p) => p.sourceName === name)!;
         // Over its parent's stored table, through the unmaterialized `mid`:
         // the parent re-rooted on its build source, less the dimension its
         // table stores, then each declaration down to the extension's.
         expect(plan("east").buildsOver?.overStored).toEqual({
            parentBuildSourceName: "orders__mat",
            flags: [],
            declarations: `source: orders is orders__mat extend {
  measure: total is amount.sum()
}
source: mid is orders extend { measure: n is count() }
source: east is mid extend {
  where: region = 'east'
  dimension: label is concat(region, '!')
}`,
         });
         // Another destination, and a chain reaching a warehouse source the
         // destination cannot read (a join), build over the raw chain.
         expect(plan("elsewhere").buildsOver).toBeUndefined();
         expect(plan("joined").buildsOver).toBeUndefined();
         // Planned in order, the extension depending on its parent: the
         // build reads the parent's table, and its address stacks on it.
         const deps = c.builds.flatMap((b) => Object.entries(b.dependsOn));
         expect(Object.fromEntries(deps)[plan("east").sourceID]).toEqual([
            plan("orders").sourceID,
         ]);
         expect(addressSQL(c.sources[plan("east").sourceID])).toContain("big");
      },
      { timeout: 30000 },
   );

   it(
      "builds an extension over the raw chain when its chained build does not compile",
      async () => {
         // `regions` is joined above the parent, so the parent's build source
         // does not carry it, and the extension's own dimension reads it.
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("select 'east' as code, 'East' as label") extend {
  primary_key: code
}
source: joined is orders_raw extend {
  join_one: regions on region = regions.code
}

#(materialize) name="orders_all"
source: orders is joined

#(materialize) name="orders_ext" inline=["regions"]
source: orders_ext is orders extend {
  dimension: label is regions.label
}
`);
         const ext = generatedPlans(c).find(
            (p) => p.sourceName === "orders_ext",
         )!;
         expect(ext.buildsOver).toBeUndefined();
         expect(ext.columns.map((col) => col.column)).toContain("label");
         expect(c.builds.flatMap((b) => b.order).length).toBe(2);
      },
      { timeout: 30000 },
   );

   it(
      "plans one build source for a source two models share, under its declared name",
      async () => {
         const c = await compiled({
            "base.malloy": `${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw
`,
            "index.malloy": `import { o is orders } from "base.malloy"
source: o2 is o
`,
         });
         // index.malloy re-exports `orders` as `o`; the contents of index hold
         // `o`, which carries orders' sourceID and its #(materialize).
         const names = Object.values(c.sources).map((s) => s.name);
         expect(names.filter((n) => n === "orders").length).toBe(1);
      },
      { timeout: 30000 },
   );

   it(
      "builds a model in a subdirectory, importing it from beside the companion",
      async () => {
         const c = await compiled({
            "sub/m.malloy": `${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw
`,
         });
         const [sourceID] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         expect(c.sourceModelPaths?.[sourceID]).toBe("sub/m.malloy");
         expect(generatedPlans(c).map((p) => p.sourceName)).toEqual(["orders"]);
      },
      { timeout: 30000 },
   );

   const JOINED = `${BASE}
source: regions is duckdb.sql("""
  select * from (values ('east', 'East coast'), ('west', 'West coast')) as t(code, label)
""") extend {
  primary_key: code
  dimension: loud_label is upper(label)
}
source: lines is duckdb.sql("""
  select * from (values (1, 1, 2.0), (2, 1, 3.0), (3, 2, 4.0)) as t(line_id, order_id, qty)
""")
`;

   it(
      "keeps a dimension through a reference join out of the table, and a join_many's fan-out out of the build",
      async () => {
         const c = await compiled(`${JOINED}
#(materialize) name="orders_all"
source: orders is orders_raw extend {
  join_one: regions with region
  join_many: lines on lines.order_id = id
  dimension:
    region_label is regions.label
    line_qty is lines.qty
    doubled is amount * 2
}
`);
         const [plan] = generatedPlans(c);
         expect(plan.columns.map((x) => x.column)).toEqual([
            "id",
            "region",
            "amount",
            "order_date",
            "doubled",
         ]);
         expect(plan.inline).toBeUndefined();
      },
      { timeout: 30000 },
   );

   it(
      "inlines a join_one: stores what reads through it and the joined source's dimensions, with a uniqueness check",
      async () => {
         const c = await compiled(`${JOINED}
#(materialize) name="orders_all" inline=["regions"]
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
#(materialize) name="orders_all" inline=["regions"]
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

   for (const [name, inline, why] of [
      ["a name that is not a join", '["nope"]', /not a join/],
      ["a join_many", '["lines"]', /join_many/],
      ["a join defined in place", '["r2"]', /defined in place/],
      ["a list of something else", '"regions"', /list of join names/],
      ["a join with no single key", '["r3"]', /not one equality/],
      // Keyed on a join_many's column: a build would store a row per line.
      [
         "a join keyed through a join_many",
         '["r4"]',
         /reads another join \(`lines`\)/,
      ],
      [
         "a join keyed on a dimension that reads a join_many",
         '["r5"]',
         /reads another join \(`lines`\)/,
      ],
   ] as const) {
      it(
         `refuses inline= naming ${name}, and plans nothing for it`,
         async () => {
            const pkg = await packages.create(`${JOINED}
#(materialize) name="orders_all" inline=${inline}
source: orders is orders_raw extend {
  join_one: regions with region
  join_one: r2 is duckdb.sql("""select 'east' as code""") on r2.code = region
  join_many: lines on lines.order_id = id
  join_one: r3 is regions on r3.code = region and r3.label != 'x'
  join_one: r4 is regions on r4.code = lines.line_id::string
  dimension: line_sku is lines.line_id::string
  join_one: r5 is regions on r5.code = line_sku
}
`);
            const c = await compilePackageBuildPlan(pkg);
            expect(generatedPlans(c)).toEqual([]);
            // Reported as a package warning, not only logged.
            const findings = (c.annotationFindings ?? []).filter(
               (f) => f.code === "materialize-not-built",
            );
            expect(findings.map((f) => f.subject)).toEqual(["orders"]);
            expect(findings[0].message).toMatch(why);
            const { plans, refused } = planModelMaterialize(
               await contentsOf(pkg),
            );
            expect(plans).toEqual([]);
            expect(refused[0]?.reason).toMatch(why);
         },
         { timeout: 30000 },
      );
   }

   it(
      "refuses an inlined column that clashes with one of the source's own, as a package warning",
      async () => {
         const c = await compiled(`${JOINED}
#(materialize) name="orders_all" inline=["regions"]
source: orders is orders_raw extend {
  join_one: regions with region
  dimension: regions__label is upper(region)
}
`);
         expect(generatedPlans(c)).toEqual([]);
         const findings = (c.annotationFindings ?? []).filter(
            (f) => f.code === "materialize-not-built",
         );
         expect(findings.map((f) => f.subject)).toEqual(["orders"]);
         expect(findings[0].message).toContain("`regions__label`");
      },
      { timeout: 30000 },
   );

   it(
      "with PUBLISHER_MATERIALIZE off, plans nothing for #(materialize) and leaves #@ persist sources as they were",
      async () => {
         const files = `##! experimental.persistence
${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw

#@ persist name="by_region"
source: by_region is orders_raw -> { group_by: region; aggregate: n is count() }
`;
         const on = await compiled(files);
         enable(false);
         const off = await compiled(files);
         expect(off.materializePlans).toEqual({});
         expect(Object.values(off.sources).map((s) => s.name)).toEqual([
            "by_region",
         ]);
         const address = (c: typeof on) => {
            const [id, s] = Object.entries(c.sources).find(
               ([, x]) => x.name === "by_region",
            )!;
            return computeSourceEntityId(
               s,
               c.connectionDigests,
               c.targets?.[id],
            );
         };
         // The authored persist source keeps its address either way.
         expect(address(on)).toBe(address(off));
      },
      { timeout: 30000 },
   );

   it(
      "names a storage= table after the author's source when name= is absent",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) storage="lake"
source: orders is orders_raw
`);
         const [p] = generatedPlans(c);
         expect(p.persist.name).toBe("orders");
         expect(p.buildSourceName).toBe("orders__mat");
      },
      { timeout: 30000 },
   );

   it(
      "reports a refused declaration as a package warning, and builds the rest",
      async () => {
         const c = await compiled(`${BASE}
#(materialize)
source: unnamed is orders_raw

#(materialize) nmae="typo" storage="lake"
source: misspelt is orders_raw

#(materialize) name="orders_all"
source: orders is orders_raw
`);
         expect(generatedPlans(c).map((p) => p.sourceName)).toEqual(["orders"]);
         const findings = (c.annotationFindings ?? []).filter(
            (f) =>
               f.code === "materialize-not-built" ||
               f.code === "materialize-unknown-key",
         );
         expect(
            findings.map((f) => [f.subject, f.code, f.tier, f.model]),
         ).toEqual([
            ["misspelt", "materialize-unknown-key", "malformed", "m.malloy"],
            ["unnamed", "materialize-not-built", "unservable", "m.malloy"],
         ]);
         // A misspelt name= is malformed (refused at publish), never defaulted
         // to the source's name.
         expect(findings[0].message).toContain("`nmae=`");
         expect(findings[0].message).toContain("did you mean `name=`?");
         expect(findings[1].message).toContain("needs name=");
      },
      { timeout: 30000 },
   );

   it(
      "carries freshness onto the generated source, where the plan reads it",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) name="orders_all" freshness.window="24h" freshness.fallback="serve_stale"
source: orders is orders_raw
`);
         const [p] = generatedPlans(c);
         expect(p.persist).toEqual({
            name: "orders_all",
            "freshness.window": "24h",
            "freshness.fallback": "serve_stale",
         });
         const outcome = deriveBuildPlanOutcome(c, null);
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

#(materialize) name="orders_all"
source: orders is orders_raw extend { dimension: big is amount > 10 }
`);
         const [p] = generatedPlans(c);
         // Keyed by the author's declaration, never by the generated name.
         const source = c.sources[p.sourceID];
         expect(source.name).toBe("orders");
         expect(source.sourceID).toBe(p.sourceID);
         expect(p.sourceID.startsWith("orders@")).toBe(true);
         expect(c.materializePlans?.[p.sourceID]).toBe(p);
         // The generated source it presents: the companion's, at the same
         // address and SQL, so no key the control plane holds moves.
         const generated = Object.getPrototypeOf(source) as typeof source;
         expect(generated.name).toBe("orders__mat");
         const target = c.targets?.[p.sourceID];
         expect(
            computeSourceEntityId(source, c.connectionDigests, target),
         ).toBe(computeSourceEntityId(generated, c.connectionDigests, target));
         expect(addressSQL(source, target)).toBe(addressSQL(generated, target));
         // The graph names the author's source too.
         expect(c.builds.flatMap((b) => b.order)).toContain(p.sourceID);
         // Every name in the plan is unique: a source that is both #@ persist
         // and #(materialize) is refused, so the author's name is free.
         const outcome = deriveBuildPlanOutcome(c, null);
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

   it(
      "refuses a source that also carries its own #@ persist, rather than storing it twice",
      async () => {
         const c = await compiled(`##! experimental.persistence
${BASE}
#@ persist name="by_hand"
#(materialize) name="orders_all"
source: orders is orders_raw -> { select: * }
`);
         expect(generatedPlans(c)).toEqual([]);
         // One source of that name: the author's own #@ persist, not a second
         // presented as the same name.
         expect(Object.values(c.sources).map((s) => s.name)).toEqual([
            "orders",
         ]);
         const wire = deriveBuildPlanOutcome(c, null).plan;
         expect(Object.values(wire?.sources ?? {}).map((s) => s.name)).toEqual([
            "orders",
         ]);
         expect(
            c.annotationFindings?.find(
               (f) => f.code === "materialize-not-built",
            )?.message,
         ).toContain("its own #@ persist");
      },
      { timeout: 30000 },
   );

   it("refuses a colocated source with no name=, since its default could be the table it reads", () => {
      const notes = (text: string) => ({
         notes: [
            {
               text,
               at: {
                  url: "file:///pkg/m.malloy",
                  range: {
                     start: { line: 0, character: 0 },
                     end: { line: 0, character: text.length },
                  },
               },
            },
         ],
      });
      const field = { name: "id", type: "number" };
      const { plans, refused } = planModelMaterialize({
         orders: {
            sourceID: "orders@file:///pkg/m.malloy",
            annotations: notes("#(materialize)\n"),
            fields: [field],
         },
         named: {
            sourceID: "named@file:///pkg/m.malloy",
            annotations: notes('#(materialize) name="orders_all"\n'),
            fields: [field],
         },
      });
      expect(plans.map((p) => p.persist.name)).toEqual(["orders_all"]);
      expect(refused).toHaveLength(1);
      expect(refused[0].sourceName).toBe("orders");
      expect(refused[0].reason).toContain("needs name=");
   });

   it("reads planModelMaterialize's refusals for a source with nothing to store", () => {
      const { plans, refused } = planModelMaterialize({
         empty: {
            sourceID: "empty@file:///pkg/m.malloy",
            annotations: {
               notes: [
                  {
                     text: "#(materialize)\n",
                     at: {
                        url: "file:///pkg/m.malloy",
                        range: {
                           start: { line: 0, character: 0 },
                           end: { line: 0, character: 14 },
                        },
                     },
                  },
               ],
            },
            fields: [
               {
                  name: "n",
                  type: "number",
                  expressionType: "aggregate",
                  e: {},
               },
            ],
         },
      });
      expect(plans).toEqual([]);
      expect(refused).toEqual([
         {
            sourceName: "empty",
            sourceID: "empty@file:///pkg/m.malloy",
            reason: "the source has no public dimension to store",
         },
      ]);
   });

   it("refuses a value of the wrong shape and a declaration that does not parse", () => {
      const declared = (text: string) => ({
         sourceID: "orders@file:///pkg/m.malloy",
         annotations: {
            notes: [
               {
                  text,
                  at: {
                     url: "file:///pkg/m.malloy",
                     range: {
                        start: { line: 0, character: 0 },
                        end: { line: 0, character: text.length },
                     },
                  },
               },
            ],
         },
         fields: [{ name: "id", type: "number" }],
      });
      const refusal = (text: string) =>
         planModelMaterialize({ orders: declared(text) }).refused[0];
      const reason = (text: string) => refusal(text)?.reason;
      expect(reason('#(materialize) name=["a", "b"]\n')).toContain(
         "`name=` must be a string",
      );
      expect(reason('#(materialize) name="a" freshness="24h"\n')).toContain(
         "`freshness` takes properties",
      );
      expect(reason('#(materialize) name="a\n')).toContain("does not parse");
      // A value of the wrong shape is unservable (the source serves live); a
      // line that does not parse is malformed, refused at publish.
      expect(
         refusal('#(materialize) name=["a", "b"]\n')?.malformedCode,
      ).toBeUndefined();
      const unparseable = refusal('#(materialize) name="a\n');
      expect(unparseable?.malformedCode).toBe("materialize-unparseable");
      expect(materializeRefusalFinding(unparseable!, "m.malloy")).toMatchObject(
         { tier: "malformed", code: "materialize-unparseable" },
      );
   });

   it(
      "plans only a source that declares #(materialize) itself, not an extension that inherits it",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw extend { dimension: big is amount > 10 }

source: east_orders is orders extend { where: region = 'east' }

source: renamed_orders is orders

#(materialize) name="west_all"
source: west_orders is orders extend { where: region = 'west' }
`);
         // Malloy copies orders' note onto east_orders and renamed_orders by
         // reference; neither declares one, so neither gets a build.
         expect(
            generatedPlans(c)
               .map((p) => [p.sourceName, p.buildSourceName])
               .sort(),
         ).toEqual([
            ["orders", "orders__mat"],
            ["west_orders", "west_orders__mat"],
         ]);
         expect(Object.values(c.sources).map((s) => s.name)).not.toContain(
            "east_orders__mat",
         );
         expect(
            (c.annotationFindings ?? []).filter(
               (f) => f.code === "materialize-not-built",
            ),
         ).toEqual([]);
      },
      { timeout: 30000 },
   );

   it(
      "reads a declaration by the statement that wrote it: across an import, past comments, through chained extensions, and on every definition of a block",
      async () => {
         const c = await compiled({
            "a.malloy": `${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw extend { dimension: big is amount > 10 }
`,
            "m.malloy": `import 'a.malloy'
// orders' declaration reaches these by reference, from another file.
source: east_orders is orders extend { where: region = 'east' }
source: renamed_orders is orders

#(materialize) name="west_all"

// A comment and blank lines between the annotation and its statement.

source: west_orders is orders extend { where: region = 'west' }
source: west_big is west_orders extend { where: big }
source: west_big_east is west_big extend { dimension: e is region = 'east' }

// A block annotation is every definition's own: both are read as declaring
// it, so both are refused for the same reason.
#(materialize)
source:
   first_copy is orders extend {}
   second_copy is orders extend {}
`,
         });
         expect(
            generatedPlans(c)
               .map((p) => [p.sourceName, p.buildSourceName])
               .sort(),
         ).toEqual([
            ["orders", "orders__mat"],
            ["west_orders", "west_orders__mat"],
         ]);
         expect(
            (c.annotationFindings ?? [])
               .filter((f) => f.code === "materialize-not-built")
               .map((f) => f.message.match(/`(\w+)`/)?.[1] ?? f.message)
               .sort(),
         ).toEqual(["first_copy", "second_copy"]);
      },
      { timeout: 30000 },
   );
});

describe("a gated #(materialize) source (R3, contracts at read)", () => {
   const GATED = `##! experimental.givens
given: GROUPS :: string[]
${BASE}
#(access_filter) region in $GROUPS
#(materialize) storage="lake"
source: orders is orders_raw
`;

   async function outcome(gatedSources: boolean, text = GATED) {
      restore?.();
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
         },
      }).restore;
      const c = await compiled(text);
      const [sourceID] = Object.keys(c.materializePlans ?? {});
      return { c, sourceID, outcome: deriveBuildPlanOutcome(c, null) };
   }

   it(
      "classifies the gate against the author's model, where its givens are declared",
      async () => {
         const { c, sourceID } = await outcome(true);
         expect(c.sourceGateOutcomes?.[sourceID]).toEqual({
            classification: "row_level",
            attributed: true,
         });
      },
      { timeout: 30000 },
   );

   it(
      "is built and served from storage only with materialize.gatedSources on",
      async () => {
         const on = await outcome(true);
         expect(on.outcome.plan?.refusedSources ?? {}).toEqual({});
         expect(on.outcome.sourceEligibility.eligible).toEqual([on.sourceID]);
         // Off, the managed tier never holds the gated source's columns.
         const off = await outcome(false);
         expect(off.outcome.plan?.refusedSources?.[off.sourceID]?.reason).toBe(
            "authorize",
         );
         expect(off.outcome.sourceEligibility.eligible).toEqual([]);
      },
      { timeout: 30000 },
   );

   it(
      "keeps refusing a gated #@ persist storage= source with the switch on",
      async () => {
         const { outcome: o } = await outcome(
            true,
            `##! experimental { persistence givens }
given: GROUPS :: string[]
${BASE}
#(access_filter) region in $GROUPS
#@ persist storage="lake" name="o_all"
source: orders is orders_raw -> { select: * }
`,
         );
         expect(
            Object.values(o.plan?.refusedSources ?? {}).map((r) => r.reason),
         ).toEqual(["authorize"]);
      },
      { timeout: 30000 },
   );
});
