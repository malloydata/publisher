// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `PUBLISHER_V1_SERVED_COPY=shadow` (the v1 cutover, materialization v2 phase
 * 3 pull request 6): each query a retiring v1 path answers (the storage serve
 * shape, the `#@ preaggregate` composite, the colocated build manifest) is
 * answered exactly as with the switch `off`, and compared off the request path
 * with the answer the cutover gives, once per query text, the outcome counted
 * in `publisher_v1_served_copy_shadow_total`.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetMaterializationTelemetryForTesting } from "../../../src/materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../src/test_helpers/metrics_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   type Outcome,
} from "./conformance/package_harness";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";

const COUNTER = "publisher_v1_served_copy_shadow_total";

function settings(mode: "off" | "shadow") {
   return installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE: "false",
         PUBLISHER_V1_SERVED_COPY: mode,
      },
   }).restore;
}

const answerOf = (o: Outcome) => {
   if ("error" in o) throw new Error(o.error);
   return o.answer;
};

async function settled(
   harness: MetricsHarness,
   want: number,
): Promise<Record<string, number>> {
   const outcomes = [
      "equal",
      "live",
      "rows_differ",
      "metadata_differ",
      "error",
   ];
   const deadline = Date.now() + 30_000;
   for (;;) {
      const counts: Record<string, number> = {};
      let total = 0;
      for (const outcome of outcomes) {
         counts[outcome] = await harness.collectCounter(COUNTER, { outcome });
         total += counts[outcome];
      }
      if (total >= want || Date.now() > deadline) return counts;
      await new Promise((r) => setTimeout(r, 50));
   }
}

describe("v1 served copy: shadow", () => {
   let harness: MetricsHarness;
   let restore: () => void = () => {};
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
   });
   afterEach(async () => {
      restore();
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
      await freshLoadPool();
   });

   it("reports a difference: today's serve shape drops a stored timeframe the cutover keeps", async () => {
      const shape = SHAPES.find(
         (s) => s.name === "doc, percent and timeframes carried",
      )!;
      restore = settings("shadow");
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         ...renderFiles(shape, "storage"),
      });
      try {
         const fixture = await loader.bind();
         const q = shape.queries[0];
         const today = answerOf(
            await fixture.run(Object.keys(shape.files)[0], q.malloy),
         );
         expect(today.via).toBeUndefined();
         const counts = await settled(harness, 1);
         expect(counts.metadata_differ).toBe(1);
      } finally {
         await loader.dispose();
      }
   }, 120_000);

   for (const [shapeName, variant, outcome] of [
      ["baseline", "storage", "equal"],
      ["baseline", "colocated", "equal"],
      // The cutover reads a v1 rollup as the rollup leg's, under its v2 name
      // (decision 3), which a package built before it has not built: it
      // answers live until its next build, and the shadow says so.
      ["v1 rollup on annotated measures", "colocated", "live"],
   ] as [string, Variant, "equal" | "live"][]) {
      it(`serves ${shapeName} [${variant}] as today and finds the cutover's answer ${outcome}`, async () => {
         const shape = SHAPES.find((s) => s.name === shapeName)!;
         // Those today's paths answer (no gap for this variant).
         const queries = shape.queries.filter(
            (q) =>
               q.expect !== "live" &&
               !Object.keys(q.gaps ?? {}).some((k) => k.startsWith(variant)),
         );
         expect(queries.length).toBeGreaterThan(0);
         restore = settings("off");
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            ...renderFiles(shape, variant),
         });
         try {
            const fixture = await loader.bind({ includeRollups: true });
            const entry = Object.keys(shape.files)[0];
            const today = [];
            for (const q of queries) {
               today.push(answerOf(await fixture.run(entry, q.malloy)));
            }
            restore();
            restore = settings("shadow");
            await fixture.rebind();
            for (const [i, q] of queries.entries()) {
               const shadowed = answerOf(await fixture.run(entry, q.malloy));
               // Served exactly as with the switch off.
               expect(shadowed).toEqual(today[i]);
               expect(shadowed.servedBy).not.toBe("live");
               expect(shadowed.via).toBeUndefined();
            }
            const counts = await settled(harness, queries.length);
            expect(counts).toEqual({
               equal: outcome === "equal" ? queries.length : 0,
               live: outcome === "live" ? queries.length : 0,
               rows_differ: 0,
               metadata_differ: 0,
               error: 0,
            });
            // Once per query text: asking again compares nothing new.
            await fixture.run(entry, queries[0].malloy);
            await new Promise((r) => setTimeout(r, 200));
            expect((await settled(harness, queries.length))[outcome]).toBe(
               queries.length,
            );
         } finally {
            await loader.dispose();
         }
      }, 120_000);
   }
});
