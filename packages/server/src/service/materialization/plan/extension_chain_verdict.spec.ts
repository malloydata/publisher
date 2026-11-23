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

/**
 * An extension table of a `#(materialize)` parent is built over the parent's
 * table or refused at plan (`upstream_table_unusable`): `chainVerdict` has
 * no "build over the raw chain" outcome, and no planned extension is left
 * with neither a `buildsOver` nor a refusal.
 */

const packages = planPackages({}, { keepIncrementalUnbuildable: true });

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
      label: "upper-bound window on the parent",
      files: `${DATED}
#(materialize)
source: older is ev extend { where: ts < now - 30 days }
#(materialize)
source: ext is older extend { dimension: big is amount > 1 }
`,
      extensions: ["ext"],
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
