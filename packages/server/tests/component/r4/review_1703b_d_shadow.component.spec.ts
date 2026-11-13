// Second review of PR #1703 (F1, F4): under PUBLISHER_V1_SERVED_COPY=shadow,
// every metric a caller's query moves must move exactly as under `off`, on
// the shadow's success and error paths alike; and a burst of distinct queries
// must not leak the shadow's slots or hold up callers.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { metrics } from "@opentelemetry/api";
import { MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { ShadowScheduler } from "../../../src/service/materialization/serve/v1_shadow";
import {
   metricsAreSuppressed,
   resetInstrumentsForTesting,
} from "../../../src/telemetry";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import { freshLoadPool, loadFixture } from "./conformance/package_harness";

class Reader extends MetricReader {
   protected override async onForceFlush(): Promise<void> {}
   protected override async onShutdown(): Promise<void> {}
}

const MODEL = `##! experimental { persistence givens }
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

#@ persist name="open_orders"
source: open_orders is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
  measure: n is count()
}
`;

const SHADOW = "publisher_v1_served_copy_shadow_total";
const GIVENS = { ROLE: "analyst", REGIONS: ["north", "south"] } as never;

type Snap = Map<string, number>;

async function snapshot(reader: Reader): Promise<Snap> {
   const out: Snap = new Map();
   const { resourceMetrics } = await reader.collect();
   for (const scope of resourceMetrics.scopeMetrics) {
      for (const m of scope.metrics) {
         // Gauges are sampled state, not counts of the caller's query.
         if (m.dataPointType === 2 /* GAUGE */) continue;
         for (const dp of m.dataPoints) {
            const key = `${m.descriptor.name} ${JSON.stringify(dp.attributes)}`;
            const v = dp.value as number | { count: number };
            out.set(key, typeof v === "number" ? v : v.count);
         }
      }
   }
   return out;
}

function delta(before: Snap, after: Snap): Record<string, number> {
   const out: Record<string, number> = {};
   for (const [k, v] of after) {
      if (k.startsWith(SHADOW)) continue;
      const d = v - (before.get(k) ?? 0);
      if (d !== 0) out[k.replace(/"(executionTime|duration)[^,}]*/g, "")] = d;
   }
   return out;
}

async function shadowTotal(reader: Reader, outcome?: string): Promise<number> {
   let n = 0;
   for (const [k, v] of await snapshot(reader)) {
      if (!k.startsWith(SHADOW + " ")) continue;
      if (outcome && !k.includes(`"outcome":"${outcome}"`)) continue;
      n += v;
   }
   return n;
}

async function until(fn: () => Promise<boolean>, ms = 20_000) {
   const deadline = Date.now() + ms;
   while (!(await fn())) {
      if (Date.now() > deadline) throw new Error("timed out waiting");
      await new Promise((r) => setTimeout(r, 25));
   }
}

describe("review 1703b: shadow metrics and resources", () => {
   let reader: Reader;
   let restore = () => {};
   const env = (mode: string, extra: Record<string, string> = {}) => {
      restore();
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "false",
            PUBLISHER_V1_SERVED_COPY: mode,
            ...extra,
         },
      }).restore;
   };
   beforeEach(() => {
      reader = new Reader();
      metrics.disable();
      metrics.setGlobalMeterProvider(new MeterProvider({ readers: [reader] }));
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      sinon.restore();
      restore();
      restore = () => {};
      await reader.shutdown();
      metrics.disable();
      await freshLoadPool();
   });

   it("F1: shadow moves every metric exactly as off, on success and error paths", async () => {
      env("shadow");
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "g.malloy": MODEL,
      });
      try {
         const f = await loader.bind();
         const queries = (pad: string) => [
            `run: locked_orders -> {${pad}group_by: region; aggregate: total }`,
            `run: open_orders -> {${pad}group_by: status; aggregate: total, n }`,
         ];
         const runAll = async (pad: string) => {
            for (const q of queries(pad)) {
               const out = await f.run("g.malloy", q, GIVENS);
               if ("error" in out) throw new Error(out.error);
            }
         };
         env("off");
         await f.rebind();
         let before = await snapshot(reader);
         await runAll(" ");
         const offDelta = delta(before, await snapshot(reader));
         expect(Object.keys(offDelta).length).toBeGreaterThan(0);

         env("shadow");
         await f.rebind();
         before = await snapshot(reader);
         await runAll("  ");
         await until(async () => (await shadowTotal(reader)) >= 2);
         await until(async () => ShadowScheduler.inFlightForTests() === 0);
         expect(await shadowTotal(reader, "equal")).toBe(2);
         const shadowDelta = delta(before, await snapshot(reader));
         expect(shadowDelta).toEqual(offDelta);

         // Error path: the shadow run's statement fails at the driver.
         const real = DuckDBConnection.prototype.runSQL;
         sinon
            .stub(DuckDBConnection.prototype, "runSQL")
            .callsFake(async function (this: DuckDBConnection, ...args) {
               if (metricsAreSuppressed()) throw new Error("boom in shadow");
               return real.apply(this, args as never);
            });
         before = await snapshot(reader);
         await runAll("   ");
         await until(async () => (await shadowTotal(reader, "error")) >= 2);
         await until(async () => ShadowScheduler.inFlightForTests() === 0);
         const errorDelta = delta(before, await snapshot(reader));
         expect(errorDelta).toEqual(offDelta);
      } finally {
         await loader.dispose();
      }
   }, 180_000);

   it("F4: a budget-expired shadow run releases its slot (driver honouring the signal)", async () => {
      env("shadow", { PUBLISHER_QUERY_TIMEOUT_MS: "300" });
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "g.malloy": MODEL,
      });
      try {
         const f = await loader.bind();
         const real = DuckDBConnection.prototype.runSQL;
         sinon
            .stub(DuckDBConnection.prototype, "runSQL")
            .callsFake(async function (this: DuckDBConnection, ...args) {
               const opts = args[1] as
                  | { abortSignal?: AbortSignal }
                  | undefined;
               if (metricsAreSuppressed()) {
                  await new Promise((_, reject) => {
                     const s = opts?.abortSignal;
                     if (!s) return; // no signal: hangs forever
                     s.addEventListener("abort", () =>
                        reject(new Error("aborted")),
                     );
                  });
               }
               return real.apply(this, args as never);
            });
         const t0 = Date.now();
         const out = await f.run(
            "g.malloy",
            "run: open_orders -> { group_by: region; aggregate: n }",
            GIVENS,
         );
         if ("error" in out) throw new Error(out.error);
         expect(Date.now() - t0).toBeLessThan(5_000);
         await until(
            async () => (await shadowTotal(reader, "error")) >= 1,
            10_000,
         );
         expect(ShadowScheduler.inFlightForTests()).toBe(0);
      } finally {
         await loader.dispose();
      }
   }, 120_000);

   it("F4: a burst of 100 distinct queries keeps at most two shadows, answers every caller, and leaks no slot", async () => {
      env("shadow");
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "g.malloy": MODEL,
      });
      try {
         const f = await loader.bind();
         let peak = 0;
         const sampler = setInterval(() => {
            peak = Math.max(peak, ShadowScheduler.inFlightForTests());
         }, 1);
         const texts = Array.from(
            { length: 100 },
            (_, i) =>
               `run: open_orders -> { where: id > ${i}; group_by: region; aggregate: n }`,
         );
         const outs = await Promise.all(
            texts.map((q) => f.run("g.malloy", q, GIVENS)),
         );
         for (const o of outs) {
            if ("error" in o) throw new Error(o.error);
         }
         await until(async () => ShadowScheduler.inFlightForTests() === 0);
         clearInterval(sampler);
         expect(peak).toBeLessThanOrEqual(2);
         const total = await shadowTotal(reader);
         // Every text is either compared or counted skipped.
         expect(total).toBe(100);
         // A skipped text is not remembered: re-running them compares them.
         for (const q of texts) await f.run("g.malloy", q, GIVENS);
         await until(async () => ShadowScheduler.inFlightForTests() === 0);
         const after = await shadowTotal(reader);
         expect(after).toBeGreaterThan(total);
      } finally {
         await loader.dispose();
      }
   }, 180_000);
});
