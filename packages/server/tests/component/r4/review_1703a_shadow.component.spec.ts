// Review probes (PR #1703, first half): PUBLISHER_V1_SERVED_COPY=shadow must
// serve exactly as `off` and add nothing a caller or an operator sees but the
// shadow counter and its log line. Not for merge.

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { resetMaterializationTelemetryForTesting } from "../../../src/materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../src/test_helpers/metrics_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import { freshLoadPool, loadFixture } from "./conformance/package_harness";

const GATED = `##! experimental { persistence givens }
import "raw.malloy"

given: ROLE :: string
given: REGIONS :: string[]

#(authorize) 'analyst' = $ROLE
#(access_filter) region in $REGIONS
#@ persist name="locked_orders"
source: locked_orders is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}
`;

const SHADOW = "publisher_v1_served_copy_shadow_total";

async function shadowSettled(h: MetricsHarness): Promise<number> {
   const deadline = Date.now() + 20_000;
   for (;;) {
      let n = 0;
      for (const outcome of [
         "equal",
         "live",
         "rows_differ",
         "metadata_differ",
         "error",
      ])
         n += await h.collectCounter(SHADOW, { outcome });
      if (n > 0 || Date.now() > deadline) return n;
      await new Promise((r) => setTimeout(r, 50));
   }
}

describe("v1 shadow: a shadow run is not counted as a caller's query", () => {
   let harness: MetricsHarness;
   let restore = () => {};
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
   });
   afterEach(async () => {
      restore();
      await harness.shutdown();
      await freshLoadPool();
   });

   it("counts one caller's gated query once in the authorize counters", async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "false",
            PUBLISHER_V1_SERVED_COPY: "shadow",
         },
      }).restore;
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "g.malloy": GATED,
      });
      try {
         const f = await loader.bind();
         const lock = () =>
            harness.collectCounter("publisher_authorize_lock_total", {
               decision: "admitted",
            });
         const givens = { ROLE: "analyst", REGIONS: ["north"] } as never;
         // `off`: what one caller's query counts.
         restore();
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "false",
               PUBLISHER_V1_SERVED_COPY: "off",
            },
         }).restore;
         let before = await lock();
         let out = await f.run(
            "g.malloy",
            "run: locked_orders -> { group_by: region; aggregate: total }",
            givens,
         );
         if ("error" in out) throw new Error(out.error);
         const perQueryOff = (await lock()) - before;
         expect(perQueryOff).toBeGreaterThan(0);
         // `shadow`, another text of the same query (so it is compared).
         restore();
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "false",
               PUBLISHER_V1_SERVED_COPY: "shadow",
            },
         }).restore;
         await f.rebind();
         before = await lock();
         out = await f.run(
            "g.malloy",
            "run: locked_orders -> { group_by: region;  aggregate: total }",
            givens,
         );
         if ("error" in out) throw new Error(out.error);
         expect(await shadowSettled(harness)).toBe(1);
         await new Promise((r) => setTimeout(r, 200));
         const perQueryShadow = (await lock()) - before;
         // Serving as `off` includes what an operator's counters say.
         expect(perQueryShadow).toBe(perQueryOff);
      } finally {
         await loader.dispose();
      }
   }, 120_000);
});
