// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `Model.prepareServedCopyOver` counts the served copy's sources once per
 * compile, through `servedCopyOutcomes`: a source still re-rooted with a
 * join read as written is `partial`, and one dropped after that is `declined`
 * only. Driven on the method itself with its collaborators faked.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   type MetricsHarness,
   startMetricsHarness,
} from "../test_helpers/metrics_harness";
import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import { Model } from "./model";

type Problem = { sourceID?: string; reason: string };

/** Runs the real `prepareServedCopyOver` over a fake served copy. */
async function prepare(problems: Problem[], rerooted: string[]): Promise<void> {
   const fake = {
      storageTables: () => undefined,
      servedCopyEnabled: () => true,
      servedCopy: {
         prepare: async () => {},
         problems: () => problems,
         rerootedSourceIDs: () => new Set(rerooted),
      },
   };
   await (
      Model.prototype as unknown as {
         prepareServedCopyOver(this: unknown, snapshot: unknown): Promise<void>;
      }
   ).prepareServedCopyOver.call(fake, { bindings: [], entries: {} });
}

describe("prepareServedCopy's bind counts", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
   });
   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });
   const count = (labels: Record<string, string>) =>
      harness.collectCounter("publisher_served_copy_bind_total", labels);

   it("counts a re-rooted source with two kept inline joins once, as partial", async () => {
      await prepare(
         [
            { sourceID: "s", reason: "inline_join_kept" },
            { sourceID: "s", reason: "inline_join_kept" },
         ],
         ["s"],
      );
      expect(await count({ outcome: "partial" })).toBe(1);
      expect(await count({ outcome: "declined" })).toBe(0);
   });

   it("counts a source with a kept inline join that is then dropped as declined only", async () => {
      await prepare(
         [
            { sourceID: "s", reason: "inline_join_kept" },
            { sourceID: "s", reason: "reroot_not_compiled" },
         ],
         [],
      );
      expect(await count({ outcome: "partial" })).toBe(0);
      expect(
         await count({
            scope: "source",
            outcome: "declined",
            reason: "reroot_not_compiled",
         }),
      ).toBe(1);
      expect(await count({ outcome: "declined" })).toBe(1);
   });

   it("counts a kept inline join on a source no longer re-rooted as declined", async () => {
      await prepare([{ sourceID: "s", reason: "inline_join_kept" }], []);
      expect(await count({ outcome: "partial" })).toBe(0);
      expect(
         await count({ outcome: "declined", reason: "inline_join_kept" }),
      ).toBe(1);
   });
});
