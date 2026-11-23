// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { planPackages } from "../../../test_helpers/materialization_fixture";
import {
   BASE,
   examplePlan,
   generatedPlans,
   PAIRED,
} from "../../../test_helpers/materialize_spec_models";
import { chainVerdict } from "./materialize_plan";
import type { MaterializePlan } from "../domain/table_plans";
import { buildCopyText } from "./materialize_build_copy";

/**
 * An extension table of a `#(materialize)` parent is built over the parent's
 * table or refused at plan (`upstream_table_unusable`): `chainVerdict` has
 * no "build over the raw chain" outcome, and no planned extension is left
 * with neither a `buildsOver` nor a refusal.
 */

const packages = planPackages({}, { keepIncrementalUnbuildable: true });

const NOTES = `source: notes is duckdb.sql("""
  select * from (values (7, 1, 'late'), (8, 2, 'gift')) as t(note_id, order_id, topic)
""") extend { primary_key: note_id }`;

const DATED = `source: ev is duckdb.sql("select 1 as id, 'east' as region, timestamp '2026-01-01' as ts, 2.0 as amount")
`;

/** Each shape, and the extensions in it. */
const SHAPES: { label: string; files: string; extensions: string[] }[] = [
   {
      label: "one tier, database and storage",
      files: `${BASE}
#(materialize)
source: orders is orders_raw
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }
#(materialize) storage="lake"
source: sorders is orders_raw
#(materialize) storage="lake"
source: sext is sorders extend { dimension: big is amount > 10 }
`,
      extensions: ["ext", "sext"],
   },
   {
      label: "across tiers and destinations",
      files: `${BASE}
#(materialize)
source: orders is orders_raw
#(materialize) storage="lake"
source: s_over_db is orders extend { dimension: big is amount > 10 }
#(materialize) storage="lake"
source: sorders is orders_raw
#(materialize)
source: db_over_s is sorders extend { dimension: big is amount > 10 }
#(materialize) storage="other"
source: other_over_s is sorders extend { dimension: big is amount > 10 }
`,
      extensions: ["s_over_db", "db_over_s", "other_over_s"],
   },
   {
      label: "incremental",
      files: `${BASE}
#(materialize) refresh="incremental" watermark="order_date"
source: orders is orders_raw
#(materialize)
source: full_over_inc is orders extend { dimension: big is amount > 10 }
#(materialize) refresh="incremental" watermark="order_date"
source: inc_over_inc is orders extend { dimension: big is amount > 10 }
#(materialize)
source: o2 is orders_raw
#(materialize) refresh="incremental" watermark="order_date"
source: inc_over_full is o2 extend { dimension: big is amount > 10 }
`,
      extensions: ["full_over_inc", "inc_over_inc", "inc_over_full"],
   },
   {
      label: "incremental on different or renamed watermarks",
      files: `${DATED}
#(materialize) refresh="incremental" watermark="ts"
source: p is ev
#(materialize) refresh="incremental" watermark="id"
source: other_wm is p extend { dimension: big is amount > 1 }
#(materialize) refresh="incremental" watermark="t2"
source: renamed_wm is p extend { rename: t2 is ts }
`,
      extensions: ["other_wm", "renamed_wm"],
   },
   {
      label: "incremental, stacked in one storage destination",
      files: `${BASE}
#(materialize) storage="lake" refresh="incremental" watermark="order_date"
source: orders is orders_raw
#(materialize) storage="lake" refresh="incremental" watermark="order_date"
source: sinc is orders extend { dimension: big is amount > 10 }
#(materialize) storage="lake"
source: sfull is orders extend { dimension: big is amount > 10 }
`,
      extensions: ["sinc", "sfull"],
   },
   {
      label: "incremental across tiers",
      files: `${BASE}
#(materialize) refresh="incremental" watermark="order_date"
source: orders is orders_raw
#(materialize) storage="lake" refresh="incremental" watermark="order_date"
source: s_inc is orders extend { dimension: big is amount > 10 }
`,
      extensions: ["s_inc"],
   },
   {
      label: "incremental across destinations",
      files: `${BASE}
#(materialize) storage="lake" refresh="incremental" watermark="order_date"
source: orders is orders_raw
#(materialize) storage="other" refresh="incremental" watermark="order_date"
source: o_inc is orders extend { dimension: big is amount > 10 }
#(materialize) storage="other"
source: o_full is orders extend { dimension: big is amount > 10 }
`,
      extensions: ["o_inc", "o_full"],
   },
   {
      label: "incremental over a pair-table parent",
      files: `${PAIRED}
#(materialize) inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
#(materialize) refresh="incremental" watermark="order_date"
source: inc_ext is orders extend { dimension: big is amount > 10 }
`,
      extensions: ["inc_ext"],
   },
   {
      label: "pair-table parent",
      files: `${PAIRED}
#(materialize) inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }
`,
      extensions: ["ext"],
   },
   {
      label: "pair-table parent through a path of two joins",
      files: `${PAIRED}
source: picks is duckdb.sql("""
  select * from (values (10, 1), (11, 3)) as t(pick_id, line_id)
""") extend { primary_key: pick_id }
source: lines_picked is lines extend {
  join_many: picks on picks.line_id = line_id
}
#(materialize) inline=["lp.picks"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
}
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }
`,
      extensions: ["ext"],
   },
   {
      label: "parent and extension of several inlined paths (blocks)",
      files: `${PAIRED}
${NOTES}
#(materialize) inline=["lines", "notes"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  join_many: notes on notes.order_id = id
}
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }
#(materialize)
source: plain is orders_raw extend { primary_key: id }
#(materialize) inline=["lines", "notes"]
source: blocks_ext is plain extend {
  join_many: lines on lines.order_id = id
  join_many: notes on notes.order_id = id
}
#(materialize) storage="lake"
source: splain is orders_raw extend { primary_key: id }
#(materialize) storage="lake" inline=["lines", "notes"]
source: sblocks_ext is splain extend {
  join_many: lines on lines.order_id = id
  join_many: notes on notes.order_id = id
}
`,
      extensions: ["ext", "blocks_ext", "sblocks_ext"],
   },
   {
      label: "pair-table parent and an incremental side",
      files: `${PAIRED}
#(materialize) inline=["lines"] refresh="incremental" watermark="order_date"
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }
#(materialize) inline=["lines"]
source: o2 is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
}
#(materialize) refresh="incremental" watermark="order_date"
source: inc_ext is o2 extend { dimension: big is amount > 10 }
`,
      extensions: ["ext", "inc_ext"],
   },
   {
      label: "upper-bound window on the parent",
      files: `${DATED}
#(materialize)
source: older is ev extend { where: ts < now - 30 days }
#(materialize)
source: ext is older extend { dimension: big is amount > 1 }
#(materialize) storage="lake"
source: solder is ev extend { where: ts < now - 30 days }
#(materialize) storage="lake"
source: sext is solder extend { dimension: big is amount > 1 }
source: windowed is ev extend { where: ts < now - 30 days }
#(materialize)
source: on_base is windowed extend { dimension: tag is 'x' }
#(materialize)
source: ext_of_base is on_base extend { dimension: big is amount > 1 }
`,
      extensions: ["ext", "sext", "ext_of_base"],
   },
   {
      label: "given-reading parent",
      files: `##! experimental { persistence givens }
given: REGION :: string
${BASE}
#(materialize)
source: orders is orders_raw extend { where: region = $REGION }
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }
`,
      extensions: ["ext"],
   },
];

describe("every extension table links to its parent's table or is refused", () => {
   for (const shape of SHAPES) {
      it(
         `leaves no extension over the raw chain (${shape.label})`,
         async () => {
            const { c } = await packages.plan(shape.files);
            const plans = new Map(
               generatedPlans(c).map((p) => [p.sourceName, p]),
            );
            const refused = new Set(
               (c.annotationFindings ?? [])
                  .filter((f) => f.code === "materialize-not-built")
                  .map((f) => f.subject),
            );
            for (const name of shape.extensions) {
               const plan = plans.get(name);
               expect([
                  name,
                  plan?.buildsOver !== undefined || refused.has(name),
               ]).toEqual([name, true]);
               // Not both: a refused extension is not planned.
               expect([name, plan !== undefined && refused.has(name)]).toEqual([
                  name,
                  false,
               ]);
            }
         },
         { timeout: 60000 },
      );
   }
});

describe("an extension's build copy over a pair-table parent", () => {
   it(
      "re-roots the parent on its join put back, one row per parent, its join_many's own definition taken out",
      async () => {
         const files = `${PAIRED}
#(materialize) inline=["lines"]
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  measure: total is amount.sum()
}
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }
`;
         const { c } = await packages.plan(files);
         const plans = generatedPlans(c);
         const ext = plans.find((p) => p.sourceName === "ext")!;
         const parent = plans.find((p) => p.sourceName === "orders")!;
         const copy = buildCopyText(files, ext, {
            parent,
            importFrom: "lvl.malloy",
         });
         expect("copy" in copy && copy.copy).toContain(
            `source: orders is orders__mat -> {
  where: __parent_row
  select: id, region, amount, order_date
} extend {
  primary_key: id
  join_many: lines is orders__mat -> {
    where: lines__line_id is not null
    select: __pair_parent_key is id, line_id is lines__line_id, order_id is lines__order_id, qty is lines__qty, big_line is lines__big_line
  } extend {
    primary_key: line_id
  } on lines.__pair_parent_key = id
} extend {
  primary_key: id

  measure: total is amount.sum()
}
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }`,
         );
      },
      { timeout: 60000 },
   );
});

describe("an extension's build copy over a parent whose build leaves a term out", () => {
   it(
      "re-roots the parent with its upper-bound window left out, and keeps the extension's own text",
      async () => {
         const files = `${DATED}
#(materialize)
source: older is ev extend { where: ts < now - 30 days }
#(materialize)
source: ext is older extend { dimension: big is amount > 1 }
`;
         const { c } = await packages.plan(files);
         const plans = generatedPlans(c);
         const ext = plans.find((p) => p.sourceName === "ext")!;
         const parent = plans.find((p) => p.sourceName === "older")!;
         expect(ext.buildsOver?.mode).toEqual({
            tier: "same",
            leftOutParentTerms: [
               { code: "ts < now - 30 days", at: expect.any(Array) },
            ],
         });
         const copy = buildCopyText(files, ext, {
            parent,
            importFrom: "lvl.malloy",
         });
         expect("copy" in copy && copy.copy).toContain(
            `source: older is older__mat extend { where: true }
#(materialize)
source: ext is older extend { dimension: big is amount > 1 }`,
         );
      },
      { timeout: 60000 },
   );
});

describe("an extension of a given-reading parent", () => {
   for (const [label, text] of [
      [
         "the parent's own term",
         `#(materialize)
source: orders is orders_raw extend { where: region = $REGION }
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }`,
      ],
      [
         "a term on a source between them",
         `#(materialize)
source: orders is orders_raw extend { dimension: x is 1 }
source: mid is orders extend { where: region = $REGION }
#(materialize)
source: ext is mid extend { dimension: big is amount > 10 }`,
      ],
   ] as const) {
      it(
         `is refused at plan as upstream_table_unusable: ${label}`,
         async () => {
            const { c } =
               await packages.plan(`##! experimental { persistence givens }
given: REGION :: string
${BASE}
${text}
`);
            expect(generatedPlans(c).map((p) => p.sourceName)).toEqual([
               "orders",
            ]);
            const refusal = (c.annotationFindings ?? []).find(
               (f) => f.code === "materialize-not-built" && f.subject === "ext",
            );
            expect(refusal?.refused?.reason).toBe("upstream_table_unusable");
            expect(refusal?.message).toContain(
               "one shared table cannot serve every caller",
            );
         },
         { timeout: 60000 },
      );
   }

   it(
      "keeps inherited_where_with_given for a source with no materialized parent",
      async () => {
         const { c } =
            await packages.plan(`##! experimental { persistence givens }
given: REGION :: string
${BASE}
source: b is orders_raw extend { where: region = $REGION }
#(materialize)
source: ext is b extend { dimension: big is amount > 10 }
`);
         const refusal = (c.annotationFindings ?? []).find(
            (f) => f.code === "materialize-not-built" && f.subject === "ext",
         );
         expect(refusal?.refused?.reason).toBe("inherited_where_with_given");
      },
      { timeout: 60000 },
   );
});

describe("an incremental extension's plan", () => {
   it(
      "records its coordination, and warns when it rebuilds on a parent change",
      async () => {
         const byLabel = (label: string) =>
            SHAPES.find((s) => s.label === label)!.files;
         const wm = await packages.plan(
            byLabel("incremental on different or renamed watermarks"),
         );
         const plans = new Map(
            generatedPlans(wm.c).map((p) => [p.sourceName, p]),
         );
         for (const name of ["other_wm", "renamed_wm"]) {
            expect([name, plans.get(name)?.buildsOver?.mode]).toEqual([
               name,
               {
                  tier: "same",
                  incremental: {
                     coordinate: "reseed_on_parent_change",
                     parentWatermark: "ts",
                  },
               },
            ]);
         }
         const warned = (wm.c.annotationFindings ?? []).filter(
            (f) => f.code === "materialize-extension-full-refresh",
         );
         expect(warned.map((f) => f.subject).sort()).toEqual([
            "other_wm",
            "renamed_wm",
         ]);
         expect(warned[0].message).toContain('"ts"');
         const stacked = await packages.plan(
            byLabel("incremental, stacked in one storage destination"),
         );
         const sinc = generatedPlans(stacked.c).find(
            (p) => p.sourceName === "sinc",
         );
         expect(sinc?.buildsOver?.mode).toEqual({
            tier: "same",
            incremental: {
               coordinate: "delta",
               parentWatermark: "order_date",
            },
         });
         expect(sinc?.buildsOver?.overStored).toBeDefined();
      },
      { timeout: 60000 },
   );
});

describe("an extension of a parent whose incremental refresh is not built here", () => {
   const strict = planPackages({});
   it(
      "is refused with it, never built over a parent with no table",
      async () => {
         const { c } = await strict.plan(`${BASE}
#(materialize) refresh="incremental" watermark="order_date"
source: orders is orders_raw
#(materialize)
source: ext is orders extend { dimension: big is amount > 10 }
`);
         const refused = (c.annotationFindings ?? [])
            .filter((f) => f.code === "materialize-not-built")
            .map((f) => [f.subject, f.refused?.reason]);
         expect(refused.sort()).toEqual([
            ["ext", "upstream_table_unusable"],
            ["orders", "incremental_unsupported"],
         ]);
      },
      { timeout: 60000 },
   );
});

describe("every persisted downstream of a given-reading #(materialize) parent is refused at plan", () => {
   const head = `##! experimental { persistence givens }
given: REGION :: string
${BASE}
#(materialize)
source: orders is orders_raw extend { where: region = $REGION }
`;
   for (const [label, text, subject, code, reason] of [
      [
         "a persisted view",
         `source: s is orders extend {
  #@ persist name="v"
  view: by_region is { group_by: region; aggregate: n is count() }
}`,
         "s -> by_region",
         "persist-view-not-built",
         "upstream_table_unusable",
      ],
      [
         "a persisted query",
         `#@ persist name="q"
query: q is orders -> { group_by: region; aggregate: n is count() }`,
         "q",
         "persist-query-not-built",
         "reads_given",
      ],
      [
         "a rollup",
         `source: rolled is orders extend {
  #(rollup) grain=["region"]
  measure: n is count()
}`,
         "rolled.n",
         "rollup-not-built",
         "inherited_where_with_given",
      ],
   ] as const) {
      it(
         `${label}, with no companion failure and the parent still planned`,
         async () => {
            const { c } = await packages.plan(`${head}\n${text}\n`);
            expect(c.omittedSources).toEqual([]);
            expect(generatedPlans(c).map((p) => p.sourceName)).toEqual([
               "orders",
            ]);
            const finding = (c.annotationFindings ?? []).find(
               (f) => f.code === code && f.subject === subject,
            );
            expect(finding?.refused?.reason).toBe(reason);
         },
         { timeout: 60000 },
      );
   }
});

describe("chainVerdict", () => {
   const parent = examplePlan({ sourceID: "p@file:///pkg/m.malloy" });
   const ext = examplePlan({
      sourceName: "ext",
      sourceID: "ext@file:///pkg/m.malloy",
   });

   it("links a pair on one tier, recording the mode", () => {
      expect(chainVerdict(ext, parent)).toEqual({ link: { tier: "same" } });
      expect(
         chainVerdict(
            { ...ext, persist: { storage: "lake" } },
            { ...parent, persist: { storage: "lake" } },
         ),
      ).toEqual({ link: { tier: "same" } });
   });

   it("links a storage= extension of a database parent, or of a parent in another destination", () => {
      expect(
         chainVerdict({ ...ext, persist: { storage: "lake" } }, parent),
      ).toEqual({ link: { tier: "storage_over_database" } });
      expect(
         chainVerdict(
            { ...ext, persist: { storage: "other" } },
            { ...parent, persist: { storage: "lake" } },
         ),
      ).toEqual({
         link: { tier: "across_destinations", parentDestination: "lake" },
      });
   });

   const pair = {
      join: "lines",
      childKey: "line_id",
      parentKey: "id",
      parentRowColumn: "__parent_row",
   } as unknown as NonNullable<MaterializePlan["pair"]>;

   it("links an extension of a pair table, naming the leaf checks its build relies on", () => {
      expect(chainVerdict(ext, { ...parent, pair })).toEqual({
         link: { tier: "same", pair: { leafKeys: ["lines.line_id"] } },
      });
   });

   it("refuses an extension of a pair table when either side is incremental", () => {
      for (const [e, p] of [
         [
            { ...ext, persist: { refresh: "incremental" } },
            { ...parent, pair },
         ],
         [ext, { ...parent, pair, persist: { refresh: "incremental" } }],
      ] as const) {
         const verdict = chainVerdict(e, p);
         expect("refuse" in verdict && verdict.refuse).toContain(
            "neither may be incremental",
         );
      }
   });

   it("refuses a storage= extension of several inlined paths", () => {
      const verdict = chainVerdict(
         {
            ...ext,
            persist: { storage: "lake" },
            pairBlocks: {} as NonNullable<MaterializePlan["pairBlocks"]>,
         },
         { ...parent, persist: { storage: "lake" } },
      );
      expect("refuse" in verdict && verdict.refuse).toContain(
         "several inlined paths",
      );
   });

   const window = {
      kind: "current_time",
      code: "ts < now - 30 days",
      columns: ["ts"],
      window: {
         field: "ts",
         op: "<",
         offset: { sign: -1, amount: 30, unit: "day" },
      },
      leftOut: [
         {
            url: "file:///pkg/m.malloy",
            range: {
               start: { line: 3, character: 30 },
               end: { line: 3, character: 32 },
            },
         },
      ],
   } as const;

   it("links an extension of a parent whose build leaves a window out, naming the term its re-root leaves out", () => {
      expect(
         chainVerdict(ext, {
            ...parent,
            readTimeTerms: [
               { ...window, window: { ...window.window } } as never,
            ],
         }),
      ).toEqual({
         link: {
            tier: "same",
            leftOutParentTerms: [
               { code: window.code, at: window.leftOut as never },
            ],
         },
      });
   });

   it("refuses an extension of a parent whose where: reads a given", () => {
      const verdict = chainVerdict(ext, {
         ...parent,
         readTimeTerms: [
            {
               kind: "given",
               code: "region = $R",
               givens: ["R"],
               columns: ["region"],
               at: [],
            },
         ],
      });
      expect("refuse" in verdict && verdict.refuse).toContain(
         "one shared table cannot serve every caller",
      );
   });

   it("records how an incremental extension follows its parent", () => {
      const wm = (name: string) => ({
         columns: [
            { dimension: "id", column: "id", computed: false },
            { dimension: name, column: name, computed: false },
         ],
      });
      const inc = (watermark: string) => ({
         persist: { refresh: "incremental", watermark },
      });
      // Full parent: the extension deltas over its table.
      expect(
         chainVerdict({ ...ext, ...wm("ts"), ...inc("ts") }, parent),
      ).toEqual({
         link: { tier: "same", incremental: { coordinate: "delta" } },
      });
      // Both incremental on one stored column: a coordinated delta.
      expect(
         chainVerdict(
            { ...ext, ...wm("ts"), ...inc("ts") },
            { ...parent, ...wm("ts"), ...inc("ts") },
         ),
      ).toEqual({
         link: {
            tier: "same",
            incremental: { coordinate: "delta", parentWatermark: "ts" },
         },
      });
      // Different watermarks: reseeded whenever the parent changed.
      expect(
         chainVerdict(
            { ...ext, ...wm("id"), ...inc("id") },
            { ...parent, ...wm("ts"), ...inc("ts") },
         ),
      ).toEqual({
         link: {
            tier: "same",
            incremental: {
               coordinate: "reseed_on_parent_change",
               parentWatermark: "ts",
            },
         },
      });
      // A full extension of an incremental parent records nothing: the run
      // rebuilds a reader whenever its parent's table moves.
      expect(
         chainVerdict(ext, { ...parent, ...wm("ts"), ...inc("ts") }),
      ).toEqual({ link: { tier: "same" } });
   });

   it("refuses an incremental side with a pair table", () => {
      const inc = { persist: { refresh: "incremental", watermark: "id" } };
      const pairParent = chainVerdict(
         { ...ext, ...inc },
         {
            ...parent,
            pair: {} as NonNullable<typeof parent.pair>,
         },
      );
      expect("refuse" in pairParent && pairParent.refuse).toContain(
         "pair table",
      );
   });

   it("links an incremental side across tiers and destinations, coordinated as on one", () => {
      const wm = {
         columns: [
            { dimension: "id", column: "id", computed: false },
            { dimension: "ts", column: "ts", computed: false },
         ],
      };
      const inc = (storage?: string) => ({
         ...wm,
         persist: {
            refresh: "incremental",
            watermark: "ts",
            ...(storage ? { storage } : {}),
         },
      });
      expect(
         chainVerdict({ ...ext, ...inc("lake") }, { ...parent, ...inc() }),
      ).toEqual({
         link: {
            tier: "storage_over_database",
            incremental: { coordinate: "delta", parentWatermark: "ts" },
         },
      });
      expect(
         chainVerdict(
            { ...ext, ...inc("other") },
            { ...parent, ...inc("lake") },
         ),
      ).toEqual({
         link: {
            tier: "across_destinations",
            parentDestination: "lake",
            incremental: { coordinate: "delta", parentWatermark: "ts" },
         },
      });
   });

   it("refuses, with why, a database extension of a storage= parent", () => {
      const verdict = chainVerdict(ext, {
         ...parent,
         persist: { storage: "lake" },
      });
      expect("refuse" in verdict && verdict.refuse).toContain(
         "a database build cannot read",
      );
   });
});

describe("an extension whose parent is in another file", () => {
   const findings = (c: Awaited<ReturnType<typeof packages.plan>>["c"]) =>
      (c.annotationFindings ?? [])
         .filter((f) => f.code === "materialize-not-built")
         .map((f) => [f.subject, f.refused?.reason, f.message]);

   it(
      "links through a file that declares nothing on the chain",
      async () => {
         const { c } = await packages.plan({
            "a.malloy": `${BASE}
#(materialize)
source: orders is orders_raw
`,
            "b.malloy": `import "a.malloy"
source: mid is orders extend { dimension: x is 1 }
`,
            "m.malloy": `import "b.malloy"
#(materialize)
source: ext is mid extend { dimension: big is amount > 10 }
`,
         });
         const ext = generatedPlans(c).find((p) => p.sourceName === "ext");
         expect(ext?.buildsOver?.sourceName).toBe("orders");
         expect(ext?.buildsOver?.crossFile?.via).toEqual([
            expect.stringMatching(/b\.malloy$/),
         ]);
         expect(findings(c)).toEqual([]);
      },
      { timeout: 60000 },
   );

   it(
      "passes through a source its own file refuses, to a parent beyond it",
      async () => {
         const { c } = await packages.plan({
            "a.malloy": `${BASE}
#(materialize)
source: orders is orders_raw
`,
            // Refused in its own file: its inlined join reads the current
            // time, which a stored copy would freeze.
            "b.malloy": `import "a.malloy"
source: regions is orders_raw -> { group_by: region } extend {
  primary_key: region
  where: year(now()) > 2000
}
#(materialize) inline=["k"]
source: mid is orders extend {
  join_one: k is regions on region = k.region
  dimension: x is 1
}
`,
            "m.malloy": `import "b.malloy"
#(materialize)
source: ext is mid extend { dimension: big is amount > 10 }
`,
         });
         const ext = generatedPlans(c).find((p) => p.sourceName === "ext");
         expect(findings(c).map(([subject]) => subject)).toEqual(["mid"]);
         expect(ext?.buildsOver?.sourceName).toBe("orders");
      },
      { timeout: 60000 },
   );

   it(
      "builds, with no parent, an extension whose chain into another file reaches no persisted source",
      async () => {
         const { c } = await packages.plan({
            "b.malloy": `${BASE}
source: mid is orders_raw extend { dimension: x is 1 }
`,
            "m.malloy": `import "b.malloy"
#(materialize)
source: ext is mid extend { dimension: big is amount > 10 }
`,
         });
         const ext = generatedPlans(c).find((p) => p.sourceName === "ext");
         expect(ext).toBeDefined();
         expect(ext?.buildsOver).toBeUndefined();
         expect(findings(c)).toEqual([]);
      },
      { timeout: 60000 },
   );
});
