// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(rollup)` is the v2 rollup annotation; `#(preaggregate)` is its
 * deprecated spelling, read the same for one release. Through the real
 * package path: the alias loads, warns `deprecated-preaggregate-annotation`
 * naming the spelling it found, and plans, builds and serves exactly what
 * `#(rollup)` does, equal to live; both spellings on one measure fail the
 * load (`rollup-invalid`). The wire says `rollup` for either.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const model = (route: string) => `##! experimental { persistence }
import "raw.malloy"

source: orders is orders_raw extend {
  ${route} grain=["region"] storage="lake"
  measure: total is amount.sum()
  ${route} grain=["region"]
  measure: n is count()
}
`;

const QUERIES = [
   "run: orders -> { group_by: region; aggregate: total; order_by: region }",
   "run: orders -> { group_by: region; aggregate: n; order_by: region }",
];

describe("#(preaggregate), #(rollup)'s deprecated spelling, as a package serves it", () => {
   let restore: () => void = () => {};
   beforeAll(async () => {
      restore = withSwitch("cutover");
      await freshLoadPool();
   });
   afterAll(async () => {
      restore();
      await freshLoadPool();
   });

   const served = async (route: string) => {
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "m.malloy": model(route),
      });
      try {
         const live = [];
         for (const q of QUERIES) live.push(await loader.run("m.malloy", q));
         const fixture = await loader.bind({ includeRollups: true });
         const answers = [];
         for (const q of QUERIES)
            answers.push(await fixture.run("m.malloy", q));
         const plan = loader.pkg.getBuildPlan();
         return {
            live,
            answers,
            warnings: fixture.warnings,
            origins: Object.values(plan?.sources ?? {}).map((s) => s.origin),
            // The address folds in the fixture's own temporary path, which
            // each load draws afresh: compare what is built instead.
            tables: Object.values(plan?.sources ?? {})
               .map((s) => `${s.name}\n${s.sql}`)
               .sort(),
         };
      } finally {
         await loader.dispose();
      }
   };

   it("warns, and plans, builds and serves what #(rollup) does, equal to live", async () => {
      const current = await served("#(rollup)");
      const alias = await served("#(preaggregate)");
      expect(
         current.warnings.filter(
            (w) => w.code === "deprecated-preaggregate-annotation",
         ),
      ).toEqual([]);
      const deprecations = alias.warnings.filter(
         (w) => w.code === "deprecated-preaggregate-annotation",
      );
      expect(deprecations.length).toBe(2);
      for (const w of deprecations) {
         expect(w.message).toContain("#(preaggregate)");
         expect(w.message).toContain("#(rollup)");
      }
      // One table per grain and placement, the same for either spelling.
      expect(alias.tables).toEqual(current.tables);
      expect(new Set(alias.origins)).toEqual(new Set(["rollup"]));
      for (const run of [current, alias]) {
         for (const [i, answer] of run.answers.entries()) {
            const live = run.live[i];
            if ("error" in answer || "error" in live) {
               throw new Error(JSON.stringify({ answer, live }));
            }
            expect(answer.answer.rows).toEqual(live.answer.rows);
            expect(answer.answer.servedBy).toBe("rollup");
         }
      }
   });

   it("fails the load when one measure carries both spellings (rollup-invalid)", async () => {
      const both = `##! experimental { persistence }
import "raw.malloy"

source: orders is orders_raw extend {
  #(rollup) grain=["region"]
  #(preaggregate) grain=["status"]
  measure: total is amount.sum()
}
`;
      let failure: unknown;
      try {
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": both,
         });
         await loader.dispose();
      } catch (error) {
         failure = error;
      }
      expect(String(failure)).toContain("[rollup-invalid]");
   });
});
