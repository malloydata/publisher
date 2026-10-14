// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import {
   recordPackageLoadPhaseDurations,
   resetPackageLoadMetricsForTesting,
   type PackageLoadPhase,
} from "./package_load_metrics";
import { Environment } from "./service/environment";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

const METRIC = "publisher_package_load_phase_duration_ms";
const PHASES: PackageLoadPhase[] = [
   "worker",
   "hydrate",
   "render_validation",
   "discovery",
   "lint",
   "build_plan",
   "total",
];

describe("publisher_package_load_phase_duration_ms", () => {
   let harness: MetricsHarness;
   let rootDir: string;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetPackageLoadMetricsForTesting();
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-phases-"));
   });

   afterEach(async () => {
      resetPackageLoadMetricsForTesting();
      await harness.shutdown();
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("records each phase under its own label, skipping one that did not run", async () => {
      recordPackageLoadPhaseDurations({ worker: 40, hydrate: 2, total: 90 });
      recordPackageLoadPhaseDurations({
         worker: 60,
         build_plan: undefined,
         total: 110,
      });
      const worker = await harness.collectHistogram(METRIC, {
         phase: "worker",
      });
      expect(worker.count).toBe(2);
      expect(worker.sum).toBe(100);
      expect(
         (await harness.collectHistogram(METRIC, { phase: "total" })).sum,
      ).toBe(200);
      expect(
         (await harness.collectHistogram(METRIC, { phase: "build_plan" }))
            .count,
      ).toBe(0);
      // The slow-load tail resolves, as on the other load histograms.
      expect(worker.boundaries[worker.boundaries.length - 1]).toBe(300_000);
   });

   it(
      "records every phase once per successful load, inside the total",
      async () => {
         const envPath = path.join(rootDir, "env");
         const dir = path.join(envPath, "pkg");
         await fs.mkdir(dir, { recursive: true });
         await fs.writeFile(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name: "pkg", description: "fixture" }),
         );
         await fs.writeFile(
            path.join(dir, "m.malloy"),
            `source: nums is duckdb.sql("SELECT 1 as one") extend {
  measure: n is count()
  # bar_chart
  view: by_one is { group_by: one; aggregate: n }
}
`,
         );
         const env = await Environment.create("testEnv", envPath, []);
         await env.addPackage("pkg");

         const sums: Partial<Record<PackageLoadPhase, number>> = {};
         for (const phase of PHASES) {
            const h = await harness.collectHistogram(METRIC, { phase });
            expect(h.count).toBe(1);
            expect(h.sum).toBeGreaterThanOrEqual(0);
            sums[phase] = h.sum;
         }
         // The stages run one after another, so none of them, nor all of them
         // together, can exceed the whole load.
         const stages = PHASES.filter((p) => p !== "total").reduce(
            (acc, p) => acc + (sums[p] ?? 0),
            0,
         );
         expect(stages).toBeLessThanOrEqual(sums.total!);
      },
      { timeout: 60000 },
   );
});
