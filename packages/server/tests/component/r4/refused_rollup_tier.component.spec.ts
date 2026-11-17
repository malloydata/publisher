// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A refused rollup's serve verdict says where its table would live. Its
 * refusal record says `tier: preaggregate` (a mechanism, kept there for one
 * more release, read by Credible's control plane) and `origin:
 * preaggregate`, and the verdict's `tier` is the rollup's own `storage=`
 * placement: a refused `storage=` rollup used to read `tier: colocated`
 * (serve_verdict.ts), because the verdict mapped every record tier that was
 * not `storage` to the database.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";
import { RAW_MODEL } from "./conformance/data";

/** A rollup refused for its hidden measure, placed as `placement` says. */
const model = (
   placement: string,
) => `##! experimental { persistence access_modifiers }
import "raw.malloy"

source: orders is orders_raw extend {
  #(preaggregate) grain=["region"]${placement}
  private measure: secret_total is amount.sum()
}
`;

describe("a refused rollup's serve verdict", () => {
   let restore: () => void = () => {};
   beforeAll(async () => {
      restore = withSwitch("cutover");
      await freshLoadPool();
   });
   afterAll(async () => {
      restore();
      await freshLoadPool();
   });

   for (const [label, placement, tier] of [
      ["a storage= rollup", ' storage="lake"', "storage"],
      ["a database rollup", "", "database"],
   ] as const) {
      it(`${label}: the verdict's tier is ${tier}, the record's preaggregate`, async () => {
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": model(placement),
         });
         try {
            const refused = Object.entries(
               loader.pkg.getBuildPlan()?.refusedSources ?? {},
            ).filter(([, r]) => r.tier === "preaggregate");
            expect(refused.length).toBe(1);
            const [sourceID, record] = refused[0];
            expect(record.reason).toBe("hidden_field");
            const verdict = loader.pkg
               .materialization()
               .serveVerdicts()
               .find((v) => v.sourceID === sourceID);
            expect(verdict).toMatchObject({
               origin: "preaggregate",
               tier,
               verdict: "live",
               reason: "refused",
            });
         } finally {
            await loader.dispose();
         }
      });
   }
});
