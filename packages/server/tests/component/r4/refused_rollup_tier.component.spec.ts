// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A refused rollup's refusal record and serve verdict say where its table
 * would live, the tier its own `storage=` puts it in, and `origin: rollup`
 * says it is a rollup. A refused `storage=` rollup used to read
 * `tier: colocated` on its verdict (serve_verdict.ts), and its record said
 * `tier: preaggregate`, a mechanism and not a place.
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
  #(rollup) grain=["region"]${placement}
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
      it(`${label}: the record and the verdict both say ${tier}, origin rollup`, async () => {
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": model(placement),
         });
         try {
            const refused = Object.entries(
               loader.pkg.getBuildPlan()?.refusedSources ?? {},
            ).filter(([, r]) => r.origin === "rollup");
            expect(refused.length).toBe(1);
            const [sourceID, record] = refused[0];
            expect(record.reason).toBe("hidden_field");
            // The record's tier is the rollup's own placement too.
            expect(record.tier).toBe(tier);
            const verdict = loader.pkg
               .materialization()
               .serveVerdicts()
               .find((v) => v.sourceID === sourceID);
            expect(verdict).toMatchObject({
               origin: "rollup",
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
