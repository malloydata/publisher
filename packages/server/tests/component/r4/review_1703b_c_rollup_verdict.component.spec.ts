// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Second review of #1703, 6e: the dry run names the rollup leg for every v1
 * `#@ preaggregate` rollup. A storage= rollup over an access-filtered source,
 * without PUBLISHER_MATERIALIZE_GATED_SOURCES, is never built or read under
 * the cutover; the dry run must not say `rollup` for it.
 */

import { expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "./conformance/package_harness";

const MODEL = `##! experimental { persistence givens }
import "raw.malloy"

given: REGIONS :: string[]

#(access_filter) region in $REGIONS
source: orders is orders_raw extend {
  #@ preaggregate grain="region" storage="lake"
  measure: total is amount.sum()
}
`;

it("a gated storage= v1 rollup: the dry run's path matches what serves", async () => {
   const restore = withSwitch("cutover");
   await freshLoadPool();
   const loader = await loadFixture({
      ...DATA_FILES,
      "raw.malloy": RAW_MODEL,
      "m.malloy": MODEL,
   });
   try {
      const fixture = await loader.bind({ includeRollups: true });
      const verdicts = await fixture.pkg.previewServeVerdicts();
      const out = await fixture.run(
         "m.malloy",
         "run: orders -> { group_by: region; aggregate: total }",
         { REGIONS: ["north"] },
      );
      const by = "error" in out ? out.error : out.answer.servedBy;
      const said = verdicts
         .filter((v) => v.origin === "preaggregate")
         .map((v) => v.cutoverPath);
      if (by !== "rollup") expect(said).not.toContain("rollup");
   } finally {
      restore();
      await loader.dispose();
   }
}, 120_000);
