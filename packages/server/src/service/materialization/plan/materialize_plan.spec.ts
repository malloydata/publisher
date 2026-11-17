// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   gatesIn,
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
   computeSourceEntityId,
   deriveBuildPlanOutcome,
} from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";
import {
   materializeRefusalFinding,
   materializeTableName,
   planModelMaterialize,
} from "./materialize_plan";
import {
   BASE,
   contentsOf,
   generatedPlans,
   JOINED,
   PAIRED,
   pairSource,
   pathSource,
} from "../../../test_helpers/materialize_spec_models";

/**
 * Planning a model's `#(materialize)` sources (`materialize_plan.ts`): which
 * source is planned and under what name, what it stores, the parent an
 * extension builds over, and every refusal and package warning, read through
 * the companion compiled into the build plan or from `planModelMaterialize`
 * directly. With PUBLISHER_MATERIALIZE off nothing is planned. The generated
 * text is `build_file_generator.spec.ts`'s.
 */

const packages = planPackages({}, { keepIncrementalUnbuildable: true });
const enable = (on: boolean) =>
   packages.settings({ PUBLISHER_MATERIALIZE: on ? "true" : "false" });

const compiled = async (files: ModelFiles) => (await packages.plan(files)).c;

describe("the generated companion, compiled into the build plan", () => {
   it(
      "plans orders__mat: dimensions stored under their own names, a private one with its modifier (P3), measures, views and joins left out",
      async () => {
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("select 'east' as region, 'East' as label") extend {
  primary_key: region
}

#(materialize) storage="lake"
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
         const [sourceID, source] = Object.entries(sourcesIn(c)).find(
            ([, s]) => s.name === "orders",
         )!;
         expect(modelPathsIn(c)?.[sourceID]).toBe("m.malloy");
         // Addressed like any persist source.
         expect(
            computeSourceEntityId(
               source,
               c.connectionDigests,
               targetsIn(c)?.[sourceID],
            ),
         ).toEqual(expect.any(String));
         // Built from a copy with the source's own `private` lifted, so the
         // build selects it (P3).
         expect(addressSQL(source, targetsIn(c)?.[sourceID])).toContain(
            "secret",
         );
      },
      { timeout: 30000 },
   );

   it(
      "builds an extension in another destination than its parent, an incremental one, and one in another file, over the raw chain",
      async () => {
         const c = await compiled({
            "m.malloy": `${BASE}
#(materialize) storage="lake"
source: orders is orders_raw

#(materialize) storage="other"
source: stored_ext is orders extend { dimension: east is region = 'east' }

#(materialize) refresh="incremental" watermark="order_date" storage="lake"
source: inc_ext is orders extend { dimension: west is region = 'west' }
`,
            "other.malloy": `import "m.malloy"
#(materialize) storage="lake"
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
      "plans one build source for a source two models share, under its declared name",
      async () => {
         const c = await compiled({
            "base.malloy": `${BASE}
#(materialize) storage="lake"
source: orders is orders_raw
`,
            "index.malloy": `import { o is orders } from "base.malloy"
source: o2 is o
`,
         });
         // index.malloy re-exports `orders` as `o`; the contents of index hold
         // `o`, which carries orders' sourceID and its #(materialize).
         const names = Object.values(sourcesIn(c)).map((s) => s.name);
         expect(names.filter((n) => n === "orders").length).toBe(1);
      },
      { timeout: 30000 },
   );

   it(
      "keeps a dimension through a reference join out of the table, and a join_many's fan-out out of the build",
      async () => {
         const c = await compiled(`${JOINED}
#(materialize) storage="lake"
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

   for (const [name, declaration, why] of [
      [
         "a join_many to a source with no primary key",
         `#(materialize) storage="lake" inline=["raw_lines"]
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
#(materialize) storage="lake" inline=["lj"]
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
#(materialize) storage="lake" inline=["lt.picks", "lt.packs"]
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
#(materialize) storage="lake" inline=["lo.o"]
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
#(materialize) storage="lake" inline=["lf.picks"]
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
#(materialize) storage="lake" inline=["lu.picks_raw"]
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
#(materialize) storage="lake" inline=["lp.picks", "l2"]
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
         `#(materialize) storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  dimension: __pair_watermark is order_date
}`,
         /`__pair_watermark` column/,
      ],
      [
         "a watermark list on a source with no join_many",
         `#(materialize) storage="lake" refresh="incremental" watermark=["order_date", "id"]
source: orders is orders_raw extend {
  primary_key: id
}`,
         /takes a list only on an incremental pair table/,
      ],
      [
         "a join_many whose flag clashes with a dimension",
         `#(materialize) storage="lake" inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  dimension: __parent_row is amount > 0
}`,
         /`__parent_row` column/,
      ],
      [
         "a join_cross",
         `#(materialize) storage="lake" inline=["lc"]
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
            // A watermark list off a pair table is invalid (it fails the
            // load); every other case is a valid layout Publisher refuses.
            const code = /watermark list/.test(name)
               ? "materialize-invalid"
               : "materialize-not-built";
            const findings = (c.annotationFindings ?? []).filter(
               (f) => f.code === code,
            );
            expect(findings.map((f) => f.subject)).toEqual(["orders"]);
            expect(findings[0].message).toMatch(why);
         },
         { timeout: 30000 },
      );
   }

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
         `#(materialize) storage="lake" inline=["lines", "l2"] refresh="incremental" watermark="order_date"
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
#(materialize) inline=${inline} storage="lake"
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
            // Reported as a finding, not only logged: naming no join, or not
            // a list, is invalid (it fails the load); the rest are refused.
            const code = /not a join|something else/.test(name)
               ? "materialize-invalid"
               : "materialize-not-built";
            const findings = (c.annotationFindings ?? []).filter(
               (f) => f.code === code,
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
#(materialize) inline=["regions"] storage="lake"
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
#(materialize) storage="lake"
source: orders is orders_raw

#@ persist name="by_region"
source: by_region is orders_raw -> { group_by: region; aggregate: n is count() }
`;
         const on = await compiled(files);
         enable(false);
         const off = await compiled(files);
         expect(materializePlansIn(off)).toEqual({});
         expect(Object.values(sourcesIn(off)).map((s) => s.name)).toEqual([
            "by_region",
         ]);
         const address = (c: typeof on) => {
            const [id, s] = Object.entries(sourcesIn(c)).find(
               ([, x]) => x.name === "by_region",
            )!;
            return computeSourceEntityId(
               s,
               c.connectionDigests,
               targetsIn(c)?.[id],
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
#(materialize)
source: orders is orders_raw extend { dimension: big is amount > 10 }

#(materialize)
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
#(materialize) name="orders_named"
source: named is orders_raw

#(materialize) nmae="typo" storage="lake"
source: misspelt is orders_raw

#(materialize) storage="lake"
source: orders is orders_raw
`);
         expect(
            generatedPlans(c)
               .map((p) => [p.sourceName, p.persist.name])
               .sort(),
         ).toEqual([
            ["named", "orders_named"],
            ["orders", "orders"],
         ]);
         const findings = (c.annotationFindings ?? []).filter(
            (f) =>
               f.code === "materialize-not-built" ||
               f.code === "materialize-unknown-key",
         );
         expect(
            findings.map((f) => [f.subject, f.code, f.tier, f.model]),
         ).toEqual([
            ["misspelt", "materialize-unknown-key", "malformed", "m.malloy"],
         ]);
         // A misspelt name= is malformed (refused at publish), never defaulted
         // to the source's name.
         expect(findings[0].message).toContain("`nmae=`");
         expect(findings[0].message).toContain("did you mean `name=`?");
      },
      { timeout: 30000 },
   );

   it(
      "refuses a #(materialize) table only when the run writes one table for two sources: by the names it writes, case-folded where the destination folds case",
      async () => {
         const notBuilt = (c: Awaited<ReturnType<typeof compiled>>) =>
            (c.annotationFindings ?? [])
               .filter((f) => f.code === "materialization-table-collision")
               .map((f) => f.subject)
               .sort();
         // A defaulted name takes the package's suffix, which an author's
         // name= never carries: a v1 table at name="orders" and a
         // #(materialize) source called orders are two tables.
         const apart = await compiled(`##! experimental.persistence
${BASE}
#@ persist name="orders"
source: placed is orders_raw -> { select: * }

#(materialize)
source: orders is orders_raw extend { dimension: big is amount > 10 }
`);
         expect(notBuilt(apart)).toEqual([]);
         expect(generatedPlans(apart).map((p) => p.sourceName)).toEqual([
            "orders",
         ]);
         // Without name=, Orders and orders take readable names hashed over
         // their own names, so even a destination that folds case holds two
         // tables.
         const folded = await compiled(`${BASE}
#(materialize) storage="lake"
source: Orders is orders_raw extend { where: region = 'east' }

#(materialize) storage="lake"
source: orders is orders_raw extend { where: region = 'west' }
`);
         expect(notBuilt(folded)).toEqual([]);
         // Two author names that differ only by case are one table there.
         const named = await compiled(`${BASE}
#(materialize) name="T_east" storage="lake"
source: east is orders_raw extend { where: region = 'east' }

#(materialize) name="t_east" storage="lake"
source: west is orders_raw extend { where: region = 'west' }
`);
         expect(notBuilt(named)).toEqual(["east", "west"]);
      },
      { timeout: 30000 },
   );

   it(
      "refuses a retired partition= as malformed, and builds nothing for it",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) name="orders_all" storage="lake" partition="order_date"
source: orders is orders_raw
`);
         expect(generatedPlans(c)).toEqual([]);
         const findings = (c.annotationFindings ?? []).filter(
            (f) => f.subject === "orders",
         );
         expect(findings.map((f) => [f.code, f.tier])).toEqual([
            ["materialize-retired-key", "malformed"],
         ]);
         expect(findings[0].message).toContain(
            "#(materialize) no longer reads `partition=`; remove it",
         );
      },
      { timeout: 30000 },
   );

   it(
      "refuses constructor= and toString= as unknown keys, not retired ones",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) name="orders_all" storage="lake" constructor="x" toString="y"
source: orders is orders_raw
`);
         expect(generatedPlans(c)).toEqual([]);
         const findings = (c.annotationFindings ?? []).filter(
            (f) => f.subject === "orders",
         );
         expect(findings.map((f) => f.code)).not.toContain(
            "materialize-retired-key",
         );
         expect(findings.map((f) => [f.code, f.tier])).toContainEqual([
            "materialize-unknown-key",
            "malformed",
         ]);
         expect(
            findings.some((f) => f.message.includes("no longer reads")),
         ).toBe(false);
      },
      { timeout: 30000 },
   );

   it(
      "refuses a source that also carries its own #@ persist, rather than storing it twice",
      async () => {
         const c = await compiled(`##! experimental.persistence
${BASE}
#@ persist name="by_hand"
#(materialize) storage="lake"
source: orders is orders_raw -> { select: * }
`);
         expect(generatedPlans(c)).toEqual([]);
         // One source of that name: the author's own #@ persist, not a second
         // presented as the same name.
         expect(Object.values(sourcesIn(c)).map((s) => s.name)).toEqual([
            "orders",
         ]);
         const wire = deriveBuildPlanOutcome(c, null).plan;
         expect(Object.values(wire?.sources ?? {}).map((s) => s.name)).toEqual([
            "orders",
         ]);
         expect(
            c.annotationFindings?.find((f) => f.code === "materialize-invalid")
               ?.message,
         ).toContain("its own #@ persist");
      },
      { timeout: 30000 },
   );

   it("builds a colocated source at its name=, or under its own name without one", () => {
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
      // A named colocated source is built at its name=; one without is
      // built under its own name (the run adds its package's or version's
      // suffix, or a host its own name), colocated or stored.
      expect(
         plans.map((p) => [p.sourceName, p.persist, p.nameDefaulted]),
      ).toEqual([
         ["named", { name: "orders_all" }, undefined],
         ["orders", { name: "orders" }, true],
         ["stored", { name: "stored", storage: "lake" }, true],
      ]);
      expect(refused).toEqual([]);
   });

   it("refuses a colocated BigQuery source with no dataset-qualified name=, which alone places its table", () => {
      const declared = (text: string) => ({
         sourceID: "orders@file:///pkg/m.malloy",
         dialect: "standardsql",
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
      const outcome = (text: string) =>
         planModelMaterialize({ orders: declared(text) });
      for (const text of ["#(materialize)\n", '#(materialize) name="t"\n']) {
         const { plans, refused } = outcome(text);
         expect(plans).toEqual([]);
         expect(
            materializeRefusalFinding(refused[0], "m.malloy"),
         ).toMatchObject({
            code: "materialize-not-built",
            message: expect.stringContaining(
               "needs a dataset-qualified name= (`dataset.table`)",
            ),
         });
      }
      expect(
         outcome('#(materialize) name="analytics.t"\n').plans.map(
            (p) => p.persist,
         ),
      ).toEqual([{ name: "analytics.t" }]);
      // In a storage destination, the destination places it.
      expect(
         outcome('#(materialize) storage="lake"\n').plans.map((p) => p.persist),
      ).toEqual([{ name: "orders", storage: "lake" }]);
   });

   it("names a source's table after it, made plain when it is not", () => {
      expect(materializeTableName("orders")).toBe("orders");
      expect(materializeTableName("orders-2")).toBe("orders-2");
      const cjk = materializeTableName("注文");
      expect(cjk).toMatch(/^source_[0-9a-f]{8}$/);
      expect(materializeTableName("order lines")).toMatch(
         /^order_lines_[0-9a-f]{8}$/,
      );
      // Two names that fold alike keep apart, and a name is stable.
      expect(materializeTableName("a b")).not.toBe(materializeTableName("a.b"));
      expect(materializeTableName("注文")).toBe(cjk);
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
            outcome: {
               refused: { reason: "no_stored_columns", tier: "colocated" },
            },
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
      expect(reason('#(materialize) storage=["a", "b"]\n')).toContain(
         "`storage=` must be a string",
      );
      expect(reason('#(materialize) storage="a" freshness="24h"\n')).toContain(
         "`freshness` takes properties",
      );
      expect(reason('#(materialize) storage="a\n')).toContain("does not parse");
      // Both are malformed, which fails the load: a value of the wrong shape
      // (materialize-invalid) and a line that does not parse.
      expect(refusal('#(materialize) storage=["a", "b"]\n')?.outcome).toEqual({
         malformed: "materialize-invalid",
      });
      const unparseable = refusal('#(materialize) storage="a\n');
      expect(unparseable?.outcome).toEqual({
         malformed: "materialize-unparseable",
      });
      expect(materializeRefusalFinding(unparseable!, "m.malloy")).toMatchObject(
         { tier: "malformed", code: "materialize-unparseable" },
      );
   });

   it(
      "plans only a source that declares #(materialize) itself, not an extension that inherits it",
      async () => {
         const c = await compiled(`${BASE}
#(materialize) storage="lake"
source: orders is orders_raw extend { dimension: big is amount > 10 }

source: east_orders is orders extend { where: region = 'east' }

source: renamed_orders is orders

#(materialize) storage="lake"
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
         expect(Object.values(sourcesIn(c)).map((s) => s.name)).not.toContain(
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
#(materialize) storage="lake"
source: orders is orders_raw extend { dimension: big is amount > 10 }
`,
            "m.malloy": `import 'a.malloy'
// orders' declaration reaches these by reference, from another file.
source: east_orders is orders extend { where: region = 'east' }
source: renamed_orders is orders

#(materialize) storage="lake"

// A comment and blank lines between the annotation and its statement.

source: west_orders is orders extend { where: region = 'west' }
source: west_big is west_orders extend { where: big }
source: west_big_east is west_big extend { dimension: e is region = 'east' }

// A block annotation is every definition's own: both are read as declaring
// it, so both are planned.
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
            ["first_copy", "first_copy__mat"],
            ["orders", "orders__mat"],
            ["second_copy", "second_copy__mat"],
            ["west_orders", "west_orders__mat"],
         ]);
         expect(
            (c.annotationFindings ?? []).filter(
               (f) => f.code === "materialize-not-built",
            ),
         ).toEqual([]);
      },
      { timeout: 30000 },
   );
});

describe("a colocated incremental pair table", () => {
   it(
      "plans a colocated incremental pair table outside DuckDB as a full rebuild, saying why",
      async () => {
         const colocated = pairSource(
            'watermark=["order_date", "lines.loaded_on"]',
         ).replace(' storage="lake"', ' name="orders_pairs"');
         const pkg = await packages.create(colocated);
         const contents = await contentsOf(pkg);
         // In a DuckDB warehouse its delta applies in place.
         const [inDuckDB] = planModelMaterialize(contents).plans;
         expect(inDuckDB.pairFullRefresh).toBeUndefined();
         expect(inDuckDB.persist.refresh).toBe("incremental");
         // The same source in a Postgres warehouse: rebuilt in full, which
         // the plan says, carrying no incremental key to its build.
         const inPostgres = Object.fromEntries(
            Object.entries(contents).map(([name, def]) => [
               name,
               name === "orders"
                  ? { ...(def as object), dialect: "postgres" }
                  : def,
            ]),
         );
         const [plan] = planModelMaterialize(inPostgres).plans;
         expect(plan.pairFullRefresh).toMatch(
            /colocated in its source's warehouse \(dialect postgres\).*declare storage=/,
         );
         expect(plan.persist).not.toHaveProperty("refresh");
         expect(plan.persist).not.toHaveProperty("watermark");
         expect(plan.persist).not.toHaveProperty("merge_key");
         // A `storage=` one keeps its delta, its origin whatever it is.
         const stored = planModelMaterialize(
            Object.fromEntries(
               Object.entries(
                  await contentsOf(
                     await packages.create(
                        pairSource(
                           'watermark=["order_date", "lines.loaded_on"]',
                        ),
                     ),
                  ),
               ).map(([name, def]) => [
                  name,
                  name === "orders"
                     ? { ...(def as object), dialect: "postgres" }
                     : def,
               ]),
            ),
         ).plans[0];
         expect(stored.pairFullRefresh).toBeUndefined();
         expect(stored.persist.refresh).toBe("incremental");
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
      packages.settings({
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
      });
      const c = await compiled(text);
      const [sourceID] = Object.keys(materializePlansIn(c) ?? {});
      return { c, sourceID, outcome: deriveBuildPlanOutcome(c, null) };
   }

   it(
      "classifies the gate against the author's model, where its givens are declared",
      async () => {
         const { c, sourceID } = await outcome(true);
         expect(gatesIn(c)?.[sourceID]).toEqual({
            classification: "row_level",
            attributed: true,
         });
      },
      { timeout: 30000 },
   );

   it(
      "is built and served from storage with materialize.gatedSources on or off (G3, its own gate)",
      async () => {
         for (const gatedSources of [true, false]) {
            const o = await outcome(gatedSources);
            expect(o.outcome.plan?.refusedSources ?? {}).toEqual({});
            expect(o.outcome.sourceEligibility.eligible).toEqual([o.sourceID]);
            expect(o.outcome.sourceEligibility.gateKept).toEqual([o.sourceID]);
         }
      },
      { timeout: 30000 },
   );

   it(
      "builds a gated #@ persist storage= source under its own gate, switch on or off (G3), as a colocated one",
      async () => {
         for (const gatedSources of [true, false]) {
            const { outcome: o } = await outcome(
               gatedSources,
               `##! experimental { persistence givens }
given: GROUPS :: string[]
${BASE}
#(access_filter) region in $GROUPS
#@ persist storage="lake" name="o_all"
source: orders is orders_raw -> { select: * }
`,
            );
            expect(o.plan?.refusedSources ?? {}).toEqual({});
            expect(o.sourceEligibility.eligible).toHaveLength(1);
            expect(o.sourceEligibility.gateKept).toEqual(
               o.sourceEligibility.eligible,
            );
         }
      },
      { timeout: 30000 },
   );
});
