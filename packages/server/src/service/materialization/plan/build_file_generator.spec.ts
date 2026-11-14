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
import { incrementalPolicyRejections } from "./incremental_policy";
import {
   assertInlinedJoinsUnique,
   pairLeafKeyChecksOf,
   runRollupKeyCheck,
} from "../run/inline_uniqueness";
import { rollingWindowsOf } from "./rolling_window";
import {
   buildCopyText,
   buildSourceName,
   colocatedNeedsName,
   generateBuildFile,
   generateBuildFiles,
   materializeRefusalFinding,
   planModelMaterialize,
   storedChainText,
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

   it("lifts a chained extension's own access modifiers over its parent's re-root, and drops a stored hidden parent dimension with its label (P3 with P10)", () => {
      const text = `source: orders is duckdb.table('ev')

source: stored is orders extend {
  internal dimension: big is amt > 10
  private dimension: secret is amt * 2
  measure: total is amt.sum()
}

source: ext is stored extend {
  private dimension: mine is amt + 1
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
         columns: [
            { dimension: "amt", column: "amt", computed: false },
            {
               dimension: "big",
               column: "big",
               computed: true,
               access: "internal",
            },
            {
               dimension: "secret",
               column: "secret",
               computed: true,
               access: "private",
            },
         ],
         declaration: at("stored", 6),
      });
      const ext = plan({
         sourceName: "ext",
         sourceID: "ext@file:///pkg/m.malloy",
         buildSourceName: buildSourceName("ext"),
         columns: [
            { dimension: "amt", column: "amt", computed: false },
            {
               dimension: "mine",
               column: "mine",
               computed: true,
               access: "private",
            },
         ],
         declaration: at("ext", 10),
         buildsOver: {
            sourceID: parent.sourceID,
            sourceName: "stored",
            declaration: at("stored", 6),
            path: [at("ext", 10)],
         },
      });
      const copy = buildCopyText(
         text,
         ext,
         { parent, importFrom: "m.malloy.0.materialize-level.malloy" },
         { liftAccess: true },
      );
      // The parent's stored hidden dimensions are gone, labels and all (a
      // label left behind would hide the next statement); the extension's
      // own label is lifted and its inherited internal fields lifted by the
      // wrap.
      expect(copy).toEqual({
         copy: `##! experimental.access_modifiers
import { stored__mat } from 'm.malloy.0.materialize-level.malloy'
source: orders is duckdb.table('ev')

source: stored is stored__mat extend {
  measure: total is amt.sum()
}

source: ext is stored extend {
  dimension: mine is amt + 1
} include { public: * }
`,
      });
      // The storage= build text carries the same lift, and its flag.
      const chain = storedChainText(text, ext, parent, { liftAccess: true });
      expect(chain?.flags).toEqual(["##! experimental.access_modifiers"]);
      expect(chain?.declarations).toContain(
         "dimension: mine is amt + 1\n} include { public: * }",
      );
      expect(chain?.declarations).not.toContain("private");
      expect(chain?.declarations).not.toContain("internal");
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

#(materialize) name="recent_events" refresh="incremental" watermark="ts" storage="lake"
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
      "plans orders__mat: dimensions stored under their own names, a private one with its modifier (P3), measures, views and joins left out",
      async () => {
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("select 'east' as region, 'East' as label") extend {
  primary_key: region
}

#(materialize) name="orders_all" storage="lake"
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
            // The probe compiler found `big` the same in every query time
            // zone (P9); a private field cannot be probed, so it is not marked.
            {
               dimension: "big",
               column: "big",
               computed: true,
               zoneFree: true,
            },
            {
               dimension: "secret",
               column: "secret",
               computed: true,
               access: "private",
            },
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
         // Built from a copy with the source's own `private` lifted, so the
         // build selects it (P3).
         expect(addressSQL(source, c.targets?.[sourceID])).toContain("secret");
      },
      { timeout: 30000 },
   );

   it(
      "builds exactly what the source computes",
      async () => {
         const pkg = await packages.create(`${BASE}
#(materialize) name="orders_all" storage="lake"
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
#(materialize) name="orders_all" storage="lake"
source: orders is orders_raw extend {
  dimension: big is amount > 10
  measure: total is amount.sum()
}

#(materialize) name="orders_east" storage="lake"
source: orders_ext is orders extend {
  where: region = 'east'
  dimension: east_big is big and region = 'east'
}

#(materialize) name="orders_east_big" storage="lake"
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
      "builds an extension in another destination than its parent, an incremental one, and one in another file, over the raw chain",
      async () => {
         const c = await compiled({
            "m.malloy": `${BASE}
#(materialize) name="orders_all" storage="lake"
source: orders is orders_raw

#(materialize) storage="other"
source: stored_ext is orders extend { dimension: east is region = 'east' }

#(materialize) name="orders_inc" refresh="incremental" watermark="order_date" storage="lake"
source: inc_ext is orders extend { dimension: west is region = 'west' }
`,
            "other.malloy": `import "m.malloy"
#(materialize) name="other_ext" storage="lake"
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

#(materialize) name="orders_all" storage="lake"
source: orders is joined

#(materialize) name="orders_ext" inline=["regions"] storage="lake"
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
#(materialize) name="orders_all" storage="lake"
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
#(materialize) name="orders_all" storage="lake"
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
#(materialize) name="orders_all" storage="lake"
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
#(materialize) name="orders_all" inline=["regions"] storage="lake"
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
#(materialize) name="orders_all" inline=["regions"] storage="lake"
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

   const PAIRED = `${BASE}
source: lines is duckdb.sql("""
  select * from (values (1, 1, 2.0), (2, 1, 3.0), (3, 2, 4.0), (4, 9, 1.0)) as t(line_id, order_id, qty)
""") extend {
  primary_key: line_id
  dimension: big_line is qty > 2
  measure: total_qty is qty.sum()
}
`;

   it(
      "inlines a join_many as a pair table: parent and child columns, and a parent-row flag written at build",
      async () => {
         const pkg = await packages.create(`${PAIRED}
#(materialize) name="orders_all" storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  dimension:
    line_qty is lines.qty
    doubled is amount * 2
}
`);
         const c = await compilePackageBuildPlan(pkg);
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
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(source, { entries: {} }, c.targets?.[sourceID]);
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

   for (const route of ['name="orders_all"', 'storage="lake"']) {
      it(
         `never builds an extension over a pair table, which holds each parent once per child (${route})`,
         async () => {
            const ext = route.startsWith("name")
               ? 'name="orders_ext"'
               : 'storage="lake"';
            const pkg = await packages.create(`${PAIRED}
#(materialize) ${route} inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  measure: order_count is count()
}

#(materialize) ${ext}
source: orders_ext is orders extend {
  measure: twice is order_count * 2
}
`);
            const c = await compilePackageBuildPlan(pkg);
            const plan = (name: string) =>
               generatedPlans(c).find((p) => p.sourceName === name)!;
            expect(plan("orders").pair?.join).toBe("lines");
            // Over the raw chain, one row per order, never the parent's
            // build source, whose rows are the pairs.
            expect(plan("orders_ext").buildsOver).toBeUndefined();
            const [sourceID, source] = Object.entries(c.sources).find(
               ([, s]) => s.name === "orders_ext",
            )!;
            const sql = buildSQLOf(
               source,
               { entries: {} },
               c.targets?.[sourceID],
            );
            const duckdb = await pkg.getMalloyConnection("duckdb");
            const rows = (
               await duckdb.runSQL(`SELECT id FROM (${sql}) t ORDER BY id`)
            ).rows.map((r) => Number(r.id));
            expect(rows).toEqual([1, 2, 3]);
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
#(materialize) name="by_key" storage="lake" inline=["lp.picks"]
source: by_key is orders_raw extend {
  primary_key: id
  dimension: id2 is id
  join_many: lp is lines_picked on lp.order_id = id2
}
#(materialize) name="by_qty" storage="lake" inline=["lq.picks.bins"]
source: by_qty is orders_raw extend {
  primary_key: id
  join_many: lq is lines_by_qty on lq.order_id = id
}
`);
         const c = await compilePackageBuildPlan(pkg);
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
#(materialize) name="orders_all" storage="lake" inline=["lp", "lp.picks"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
}
`);
         const c = await compilePackageBuildPlan(pkg);
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
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(source, { entries: {} }, c.targets?.[sourceID]);
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
#(materialize) name="orders_all" storage="lake" inline=["lines", "notes"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  join_many: notes on notes.order_id = id
}
`);
         const c = await compilePackageBuildPlan(pkg);
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
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(source, { entries: {} }, c.targets?.[sourceID]);
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
#(materialize) name="orders_hop" storage="lake" inline=["lp.picks"]
source: orders_hop is ${orders} extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
}
#(materialize) name="orders_blocks" storage="lake" inline=["lines", "notes"]
source: orders_blocks is ${orders} extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  join_many: notes on notes.order_id = id
}
#(materialize) name="orders_one" storage="lake" inline=["lines"]
source: orders_one is ${orders} extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
`;
         const plansOf = async (text: string) => {
            const pkg = await packages.create(text);
            const c = await compilePackageBuildPlan(pkg);
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
               expect(
                  (await runRollupKeyCheck(leaf, bad.duckdb, "t"))?.unique,
               ).toBe(false);
            }
         }
         // A clean leaf passes its check.
         for (const name of ["orders_hop", "orders_blocks", "orders_one"]) {
            for (const leaf of pairLeafKeyChecksOf(clean.byName.get(name))) {
               expect(
                  (await runRollupKeyCheck(leaf, clean.duckdb, "t"))?.unique,
               ).toBe(true);
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
#(materialize) name="orders_t" storage="lake" inline=["lines"]
source: orders is raw extend {
  primary_key: id
  where: ${where.replace(" DEFAULT", "")}
  join_many: lines on lines.order_id = id
}
`;
         const planOf = async (text: string) => {
            const pkg = await packages.create(text);
            const c = await compilePackageBuildPlan(pkg);
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
#(materialize) name="orders_all" storage="lake" inline=["notes"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: notes on notes.region = region
}
`);
         const c = await compilePackageBuildPlan(pkg);
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(source, { entries: {} }, c.targets?.[sourceID]);
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

   for (const [name, declaration, why] of [
      [
         "a join_many to a source with no primary key",
         `#(materialize) name="orders_all" storage="lake" inline=["raw_lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: raw_lines is lines_raw on raw_lines.order_id = id
}`,
         /the joined source's primary key/,
      ],
      [
         "a join_many whose joined source has joins of its own",
         `source: lines_joined is lines extend {
  join_one: o is orders_raw on o.id = order_id
}
#(materialize) name="orders_all" storage="lake" inline=["lj"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lj is lines_joined on lj.order_id = id
}`,
         /joins of its own/,
      ],
      [
         "two paths through one join that part",
         `source: picks is duckdb.sql("""select 1 as pick_id, 1 as line_id""") extend { primary_key: pick_id }
source: packs is duckdb.sql("""select 1 as pack_id, 1 as line_id""") extend { primary_key: pack_id }
source: lines_two is lines extend {
  join_many: picks on picks.line_id = line_id
  join_many: packs on packs.line_id = line_id
}
#(materialize) name="orders_all" storage="lake" inline=["lt.picks", "lt.packs"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lt is lines_two on lt.order_id = id
}`,
         /two paths through `lt` that part/,
      ],
      [
         "a path through a join_one",
         `source: lines_one is lines extend {
  join_one: o is orders_raw extend { primary_key: id } on o.id = order_id
}
#(materialize) name="orders_all" storage="lake" inline=["lo.o"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lo is lines_one on lo.order_id = id
}`,
         /a path through a join_one/,
      ],
      [
         "a path whose middle source has a join off the path",
         `source: picks is duckdb.sql("""select 1 as pick_id, 1 as line_id""") extend { primary_key: pick_id }
source: lines_off is lines extend {
  join_many: picks on picks.line_id = line_id
  join_one: o is orders_raw extend { primary_key: id } on o.id = order_id
}
#(materialize) name="orders_all" storage="lake" inline=["lf.picks"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lf is lines_off on lf.order_id = id
}`,
         /joins off the path \(`o`\)/,
      ],
      [
         "a path with no primary key at its second join",
         `source: picks_raw is duckdb.sql("""select 1 as pick_id, 1 as line_id""")
source: lines_unkeyed is lines extend {
  join_many: picks_raw on picks_raw.line_id = line_id
}
#(materialize) name="orders_all" storage="lake" inline=["lu.picks_raw"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lu is lines_unkeyed on lu.order_id = id
}`,
         /primary key of `picks_raw`'s joined source/,
      ],
      [
         "two join_manys with a level flag that clashes with a dimension",
         `source: picks is duckdb.sql("""select 1 as pick_id, 1 as line_id""") extend { primary_key: pick_id }
source: lines_picked is lines extend {
  join_many: picks on picks.line_id = line_id
}
#(materialize) name="orders_all" storage="lake" inline=["lp.picks", "l2"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
  join_many: l2 is lines on l2.order_id = id
  dimension: __pair_lane is amount * 2
}`,
         /`__pair_lane` column/,
      ],
      [
         "a join_many whose watermark column clashes with a dimension",
         `#(materialize) name="orders_all" storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  dimension: __pair_watermark is order_date
}`,
         /`__pair_watermark` column/,
      ],
      [
         "a watermark list on a source with no join_many",
         `#(materialize) name="orders_all" storage="lake" refresh="incremental" watermark=["order_date", "id"]
source: orders is orders_raw extend {
  primary_key: id
}`,
         /takes a list only on an incremental pair table/,
      ],
      [
         "a join_many whose flag clashes with a dimension",
         `#(materialize) name="orders_all" storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  dimension: __parent_row is amount > 0
}`,
         /`__parent_row` column/,
      ],
      [
         "a join_cross",
         `#(materialize) name="orders_all" storage="lake" inline=["lc"]
source: orders is orders_raw extend {
  primary_key: id
  join_cross: lc is lines on lc.order_id = id
}`,
         /join_cross/,
      ],
   ] as const) {
      it(
         `refuses inline= naming ${name}, as a package warning`,
         async () => {
            const c = await compiled(`${PAIRED}
source: lines_raw is duckdb.sql("""select 1 as order_id, 2.0 as qty""")
${declaration}
`);
            expect(generatedPlans(c)).toEqual([]);
            const findings = (c.annotationFindings ?? []).filter(
               (f) => f.code === "materialize-not-built",
            );
            expect(findings.map((f) => f.subject)).toEqual(["orders"]);
            expect(findings[0].message).toMatch(why);
         },
         { timeout: 30000 },
      );
   }

   const DATED = `${BASE}
source: lines is duckdb.sql("""
  select * from (values
    (1, 1, 2.0, date '2026-01-05'),
    (2, 1, 3.0, date '2026-01-02'),
    (3, 2, 4.0, date '2026-01-01'),
    (4, 9, 1.0, date '2026-01-09')
  ) as t(line_id, order_id, qty, loaded_on)
""") extend {
  primary_key: line_id
}
`;
   const pairSource = (keys: string, extra = "") => `${DATED}
#(materialize) name="orders_all" storage="lake" inline=["lines"] refresh="incremental" ${keys}
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id${extra}
}
`;

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
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(source, { entries: {} }, c.targets?.[sourceID]);
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
         const declaration = collectIncrementalDeclarations(
            c.sources,
            c.materializePlans,
         )[plan.sourceID];
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

   const PICKED = `${DATED}
source: picks is duckdb.sql("""
  select * from (values (10, 1, date '2026-01-07'), (11, 3, date '2026-01-08')) as t(pick_id, line_id, picked_on)
""") extend { primary_key: pick_id }
source: lines_picked is lines extend {
  join_many: picks on picks.line_id = line_id
}
`;
   const pathSource = (keys: string) => `${PICKED}
#(materialize) name="orders_all" storage="lake" inline=["lp.picks"] refresh="incremental" ${keys}
source: orders is orders_raw extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
}
`;

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
         const declaration = collectIncrementalDeclarations(
            c.sources,
            c.materializePlans,
         )[plan.sourceID];
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

   for (const [name, keys, why] of [
      [
         "names no field of the middle level, where a new line with no pick would not move it",
         'watermark=["order_date", "lp.picks.picked_on"]',
         /names no field of `lp`, so a row added there/,
      ],
      [
         "names no field of the leaf",
         'watermark=["order_date", "lp.loaded_on"]',
         /names no field of `lp.picks`/,
      ],
      [
         "names only the source's own field",
         'watermark="order_date"',
         /names only the source's own fields/,
      ],
   ] as const) {
      it(
         `rebuilds a path's pair table in full, with a coded warning, when its watermark ${name}`,
         async () => {
            const c = await compiled(pathSource(keys));
            const [plan] = generatedPlans(c);
            expect(plan.pair?.watermark).toBeUndefined();
            expect(plan.pairFullRefresh).toMatch(why);
            for (const key of ["refresh", "watermark", "merge_key"]) {
               expect(plan.persist[key]).toBeUndefined();
            }
         },
         { timeout: 30000 },
      );
   }

   it(
      "plans an incremental table of several blocks: one watermark per parent across every block, each block's flags checked",
      async () => {
         const pkg = await packages.create(`${PICKED}
source: pays is duckdb.sql("""
  select * from (values (50, 1, date '2026-01-04'), (51, 3, date '2026-01-09')) as t(pay_id, order_id, paid_on)
""") extend { primary_key: pay_id }
#(materialize) name="orders_all" storage="lake" inline=["lp.picks", "pays"] refresh="incremental" watermark=["order_date", "lp.loaded_on", "lp.picks.picked_on", "pays.paid_on"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
  join_many: pays on pays.order_id = id
}
`);
         const c = await compilePackageBuildPlan(pkg);
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
         const declaration = collectIncrementalDeclarations(
            c.sources,
            c.materializePlans,
         )[plan.sourceID];
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
         const [sourceID, source] = Object.entries(c.sources).find(
            ([, s]) => s.name === "orders",
         )!;
         const sql = buildSQLOf(source, { entries: {} }, c.targets?.[sourceID]);
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
#(materialize) name="orders_all" storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
`);
         const [plan] = generatedPlans(c);
         expect(plan.pair?.watermark).toBeUndefined();
         expect(plan.persist.refresh).toBeUndefined();
         const declaration = collectIncrementalDeclarations(
            c.sources,
            c.materializePlans,
         )[plan.sourceID];
         expect(declaration.pair).toBeUndefined();
         expect(declaration.calculateFields).toEqual(["__parent_row"]);
      },
      { timeout: 30000 },
   );

   for (const [name, keys, extra, why] of [
      [
         "names only the parent's field, so a child-only change is not seen",
         'watermark="order_date"',
         "",
         /names only the source's own fields, so a child added/,
      ],
      [
         "names only a child's field, so a new childless parent is not seen",
         'watermark=["lines.loaded_on"]',
         "",
         /names only fields of `lines`/,
      ],
      [
         "names a field that is not stored",
         'watermark=["order_date", "lines.nope"]',
         "",
         /`lines.nope`, which is not a stored dimension/,
      ],
      [
         "names fields of two types",
         'watermark=["order_date", "lines.qty"]',
         "",
         /must all be of one type.*date, number/,
      ],
      [
         "merges on a key other than the parent's",
         'watermark=["order_date", "lines.loaded_on"] merge_key="region"',
         "",
         /merge_key= names `region`/,
      ],
   ] as const) {
      it(
         `rebuilds a pair table in full, with a coded warning, when its watermark ${name}`,
         async () => {
            const c = await compiled(pairSource(keys, extra));
            const [plan] = generatedPlans(c);
            // Built and served; only the refresh is not incremental.
            expect(plan.pair?.watermark).toBeUndefined();
            expect(plan.pairFullRefresh).toMatch(why);
            for (const key of ["refresh", "watermark", "merge_key"]) {
               expect(plan.persist[key]).toBeUndefined();
            }
            const findings = (c.annotationFindings ?? []).filter((f) =>
               f.code.startsWith("materialize-"),
            );
            expect(findings.map((f) => [f.code, f.tier, f.subject])).toEqual([
               ["materialize-pair-full-refresh", "unservable", "orders"],
            ]);
            expect(findings[0].message).toMatch(why);
         },
         { timeout: 30000 },
      );
   }

   for (const [name, declaration, why] of [
      [
         "two join_manys",
         `#(materialize) name="orders_all" storage="lake" inline=["lines", "l2"] refresh="incremental" watermark="order_date"
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  join_many: l2 is lines on l2.order_id = id
}`,
         /names only the source's own fields/,
      ],
   ] as const) {
      it(
         `rebuilds in full, with a coded warning, an incremental pair table of ${name}`,
         async () => {
            const c = await compiled(`${PAIRED}
${declaration}
`);
            const [plan] = generatedPlans(c);
            expect(plan.pair).toBeDefined();
            expect(plan.pair?.watermark).toBeUndefined();
            expect(plan.pairFullRefresh).toMatch(why);
            for (const key of ["refresh", "watermark", "merge_key"]) {
               expect(plan.persist[key]).toBeUndefined();
            }
            const findings = (c.annotationFindings ?? []).filter((f) =>
               f.code.startsWith("materialize-"),
            );
            expect(findings.map((f) => [f.code, f.subject])).toEqual([
               ["materialize-pair-full-refresh", "orders"],
            ]);
         },
         { timeout: 30000 },
      );
   }

   for (const [name, inline, why] of [
      ["a name that is not a join", '["nope"]', /not a join/],
      [
         "a join_many from a source with no primary key",
         '["lines"]',
         /join_many, and a pair table needs the source's own primary key/,
      ],
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
#(materialize) name="orders_all" inline=${inline} storage="lake"
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
#(materialize) name="orders_all" inline=["regions"] storage="lake"
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
#(materialize) name="orders_all" storage="lake"
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
      "chains a colocated extension over its colocated parent, and builds a storage= extension of a colocated parent over the raw chain (P10, phase 3 pull request 6)",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw extend { dimension: big is amount > 10 }

#(materialize) name="orders_east"
source: orders_ext is orders extend { where: region = 'east' }

#(materialize) storage="lake"
source: stored_ext is orders extend { dimension: east is region = 'east' }
`);
         expect(
            generatedPlans(c).map((p) => [
               p.sourceName,
               p.buildsOver?.sourceName,
               p.buildsOver?.overStored === undefined,
            ]),
         ).toEqual([
            ["orders", undefined, true],
            ["stored_ext", undefined, true],
            // Colocated: its build reads the parent's build source in the
            // warehouse, so nothing is compiled over a stored table.
            ["orders_ext", "orders", true],
         ]);
         expect(
            (c.annotationFindings ?? []).filter(
               (f) => f.code === "materialize-not-built",
            ),
         ).toEqual([]);
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

#(materialize) name="orders_all" storage="lake"
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
         expect(findings[1].message).toContain(
            "a #(materialize) source built into its own warehouse needs name=",
         );
      },
      { timeout: 30000 },
   );

   it(
      "carries freshness onto the generated source, where the plan reads it",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) name="orders_all" freshness.window="24h" freshness.fallback="serve_stale" storage="lake"
source: orders is orders_raw
`);
         const [p] = generatedPlans(c);
         expect(p.persist).toEqual({
            name: "orders_all",
            storage: "lake",
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

#(materialize) name="orders_all" storage="lake"
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
#(materialize) name="orders_all" storage="lake"
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

   it("builds a named colocated source, and refuses one with no name=, since its default could be the table it reads", () => {
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
         stored: {
            sourceID: "stored@file:///pkg/m.malloy",
            annotations: notes('#(materialize) storage="lake"\n'),
            fields: [field],
         },
      });
      // A named colocated source is built (and served by the served copy on
      // its own connection, phase 3 pull request 6); an unnamed one is not,
      // since its default table could be the one it reads.
      expect(plans.map((p) => p.persist)).toEqual([
         { name: "orders_all" },
         { name: "stored", storage: "lake" },
      ]);
      expect(refused.map((r) => [r.sourceName, r.reason])).toEqual([
         ["orders", colocatedNeedsName("orders")],
      ]);
      expect(materializeRefusalFinding(refused[0], "m.malloy")).toMatchObject({
         code: "materialize-not-built",
         tier: "unservable",
         message: expect.stringContaining(
            "a #(materialize) source built into its own warehouse needs name=: without one its table would be `orders`",
         ),
      });
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
            reason: "the source has no dimension to store",
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
#(materialize) name="orders_all" storage="lake"
source: orders is orders_raw extend { dimension: big is amount > 10 }

source: east_orders is orders extend { where: region = 'east' }

source: renamed_orders is orders

#(materialize) name="west_all" storage="lake"
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
#(materialize) name="orders_all" storage="lake"
source: orders is orders_raw extend { dimension: big is amount > 10 }
`,
            "m.malloy": `import 'a.malloy'
// orders' declaration reaches these by reference, from another file.
source: east_orders is orders extend { where: region = 'east' }
source: renamed_orders is orders

#(materialize) name="west_all" storage="lake"

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
               .map((f) => f.subject)
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
