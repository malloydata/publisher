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
import {
   buildSourceName,
   generateBuildFile,
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
      "builds a materialized extension from its own source, beside its parent's",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) name="orders_all"
source: orders is orders_raw

#(materialize) name="orders_east"
source: orders_ext is orders extend { dimension: east is region = 'east' }
`);
         expect(
            generatedPlans(c)
               .map((p) => p.buildSourceName)
               .sort(),
         ).toEqual(["orders__mat", "orders_ext__mat"]);
         const ext = generatedPlans(c).find(
            (p) => p.buildSourceName === "orders_ext__mat",
         )!;
         expect(ext.columns.map((col) => col.column)).toContain("east");
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

   it(
      "refuses inline= until the joins pull request, and plans nothing for it",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) name="orders_all" inline=["regions"]
source: orders is orders_raw
`);
         expect(generatedPlans(c)).toEqual([]);
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
            (f) => f.code === "materialize-not-built",
         );
         expect(findings.map((f) => [f.subject, f.tier, f.model])).toEqual([
            ["misspelt", "unservable", "m.malloy"],
            ["unnamed", "unservable", "m.malloy"],
         ]);
         // A misspelt name= is refused, never defaulted to the source's name.
         expect(findings[0].message).toContain("`nmae=`");
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
      const reason = (text: string) =>
         planModelMaterialize({ orders: declared(text) }).refused[0]?.reason;
      expect(reason('#(materialize) name=["a", "b"]\n')).toContain(
         "`name=` must be a string",
      );
      expect(reason('#(materialize) name="a" freshness="24h"\n')).toContain(
         "`freshness` takes properties",
      );
      expect(reason('#(materialize) name="a\n')).toContain("does not parse");
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
});
