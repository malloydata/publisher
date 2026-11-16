// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// v1 `#@ preaggregate` lines read by the rollup leg (G4)
// beside `#(preaggregate)` lines on the same measure, and inherited v1 lines.
import { describe, expect, it } from "bun:test";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import { planModelRollups } from "./rollup_plan";
import { SourceMap } from "../text/source_map";

const mz = materializationFixture();

const ROWS = `duckdb.sql("""
  SELECT * FROM (VALUES (1, 10, 'A'), (2, 20, 'B')) AS t(id, amount, category)
""")`;

async function plan(text: string) {
   return planModelRollups(SourceMap.of(await mz.compile(text)), {
      packageRelativeSourceID: (id) => id.replace("file:///pkg/", ""),
      readsPreaggregateLines: true,
   });
}

describe("v1 `#@ preaggregate` lines on the rollup leg", () => {
   it("plans one rollup when a measure declares the same grain both ways", async () => {
      const { plans } = await plan(`source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}`);
      expect(plans.map((p) => p.rollupSourceName)).toHaveLength(
         new Set(plans.map((p) => p.rollupSourceName)).size,
      );
      expect(plans).toHaveLength(1);
   });

   it("plans an extension's own rollup once, not once per inherited v1 line", async () => {
      const { plans } = await plan(`source: orders is ${ROWS} extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
source: more is orders extend { dimension: c2 is category }`);
      const names = plans.map((p) => p.rollupSourceName);
      expect(names).toHaveLength(new Set(names).size);
   });
});
