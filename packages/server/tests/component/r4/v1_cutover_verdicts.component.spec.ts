// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's dry-run verdict (6e, plan question 15):
 * `POST …/packages?dryRun=true` names the path that will serve each
 * materialized source (`cutoverPath`: `served_copy`, `rollup`, or `live` with
 * `cutoverReason`), judged by compiling the served copy over bindings
 * declared from the plan, before any table is built.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "./conformance/package_harness";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";

async function preview(
   shapeName: string,
   variant: Variant,
   on: "on" | "cutover",
): Promise<Record<string, [string | undefined, string | undefined]>> {
   const restore = withSwitch(on);
   await freshLoadPool();
   const shape = SHAPES.find((s) => s.name === shapeName)!;
   const loader = await loadFixture({
      ...DATA_FILES,
      ...renderFiles(shape, variant),
   });
   try {
      const pkg = (await loader.bind({ includeRollups: true })).pkg;
      const verdicts = await pkg.previewServeVerdicts();
      return Object.fromEntries(
         verdicts.map((v) => [v.sourceName, [v.cutoverPath, v.cutoverReason]]),
      );
   } finally {
      restore();
      await loader.dispose();
   }
}

describe("v1 cutover: the dry run's serve path", () => {
   afterEach(freshLoadPool);

   it("names the served copy for v1 storage= and colocated sources", async () => {
      for (const variant of ["storage", "colocated"] as const) {
         const paths = await preview("baseline", variant, "cutover");
         expect(paths.daily).toEqual(["served_copy", undefined]);
      }
   }, 120_000);

   it("names the served copy for a source with hidden columns, colocated or storage= (G2)", async () => {
      for (const variant of ["colocated", "storage"] as const) {
         const paths = await preview(
            "joined persisted source with hidden fields, from an index",
            variant,
            "cutover",
         );
         expect(paths.customer_dim?.[0]).toBe("served_copy");
         expect(paths.order_facts?.[0]).toBe("served_copy");
      }
   }, 120_000);

   it("says live, with the reason, for a source it leaves as written", async () => {
      // A gated source is refused for storage= by design.
      const paths = await preview(
         "givens and access filters",
         "storage",
         "cutover",
      );
      expect(paths.gated?.[0]).toBe("live");
      expect(paths.gated?.[1]).toBeDefined();
      expect(paths.gated?.[1]).not.toBe("not_rerooted");
   }, 120_000);

   it("names the rollup leg for a v1 rollup", async () => {
      const paths = await preview(
         "v1 rollup on annotated measures",
         "storage",
         "cutover",
      );
      expect(Object.values(paths).map((p) => p[0])).toContain("rollup");
   }, 120_000);
});
