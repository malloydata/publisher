// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { describe, expect, it } from "bun:test";
import type { StormResult } from "./storm";
import { DEFAULT_LIMITS, judge, median, summarize } from "./verdict";

function run(over: Partial<StormResult> = {}): StormResult {
   return {
      label: "r",
      dist: "/d",
      version: "0.0.0",
      options: {
         dist: "/d",
         fixtures: "/f",
         preflight: false,
         ops: 100,
         concurrency: 8,
         envs: 4,
         seed: 1,
         port: 4941,
         label: "r",
      },
      wallMs: 20_000,
      ops: { load: { ok: 40, failed: {} } },
      completedOps: 100,
      mainThreadCpuMs: 6000,
      processCpuMs: 60_000,
      cpuPerOpMs: 60,
      status: { n: 100, p50: 40, p90: 200, max: 600, timeouts: 0 },
      maxBlockMs: 400,
      blocksOver: { "250": 2 },
      eventLoopDelay: { maxMs: 400, p99Ms: 100 },
      loadDispatches: 40,
      rebind: {
         rebinds: 3,
         ok: true,
         compiles: 0,
         compilesByCaller: {},
         dispatches: {},
      },
      statusLoads: {
         polls: 20,
         unloaded: 3,
         loadDispatches: 0,
         dispatches: {},
      },
      hooks: {
         postMessage: true,
         compile: true,
         getModel: true,
         queryModel: true,
      },
      quietMs: [2000, 2000, 2000],
      control: { queryStatus: 200, queryCompiles: 1 },
      queryModel: { builds: 20, ms: 5, byCaller: {} },
      indexedLoad: {
         ok: true,
         indexedDimensions: 120,
         builds: 4,
         ms: 1,
         byCaller: {},
         entitiesOk: true,
         entitiesBuilds: 0,
         entitiesMs: 0,
      },
      maxRssBytes: 1e9,
      ...over,
   };
}

const outcomeOf = (v: ReturnType<typeof judge>, name: string) =>
   v.checks.find((c) => c.name === name)?.outcome;

describe("judge", () => {
   it("passes a candidate that matches its baseline", () => {
      const v = judge([run(), run()], [run(), run()]);
      expect(v.outcome).toBe("pass");
   });

   it("fails main-thread CPU per op over 1.5x the baseline", () => {
      const v = judge(
         [run(), run()],
         [run({ cpuPerOpMs: 95 }), run({ cpuPerOpMs: 95 })],
      );
      expect(outcomeOf(v, "main-thread CPU per op vs baseline")).toBe("fail");
      expect(v.outcome).toBe("fail");
   });

   it("judges each side by its median round, so one slow round is not a failure", () => {
      const v = judge(
         [run(), run(), run()],
         [run(), run({ cpuPerOpMs: 200 }), run()],
      );
      expect(outcomeOf(v, "main-thread CPU per op vs baseline")).toBe("pass");
   });

   it("fails /status p90 over 2x the baseline, but not below the floor", () => {
      const slow = run({
         status: { n: 100, p50: 50, p90: 450, max: 900, timeouts: 0 },
      });
      expect(outcomeOf(judge([run()], [slow]), "/status p90 vs baseline")).toBe(
         "fail",
      );
      const fastBase = run({
         status: { n: 100, p50: 5, p90: 30, max: 90, timeouts: 0 },
      });
      const fastCand = run({
         status: { n: 100, p50: 9, p90: 90, max: 120, timeouts: 0 },
      });
      expect(
         outcomeOf(judge([fastBase], [fastCand]), "/status p90 vs baseline"),
      ).toBe("pass");
   });

   it("holds the absolute budgets even when the baseline is as bad", () => {
      const bad = run({
         status: { n: 100, p50: 300, p90: 2000, max: 5000, timeouts: 0 },
         maxBlockMs: 4000,
      });
      const v = judge([bad], [bad]);
      expect(outcomeOf(v, "/status p90 vs baseline")).toBe("pass");
      expect(outcomeOf(v, "/status p90 under the storm")).toBe("fail");
      expect(outcomeOf(v, "longest event-loop block")).toBe("fail");
   });

   it("fails any round with a compile on an identical re-bind or a load from /status", () => {
      const compiled = run({
         rebind: {
            rebinds: 3,
            ok: true,
            compiles: 3,
            compilesByCaller: { compile: 3 },
            dispatches: {},
         },
      });
      const v = judge([run(), run()], [run(), compiled]);
      const check = v.checks.find(
         (c) =>
            c.name === "main-thread Malloy compiles on an identical re-bind",
      );
      expect(check?.outcome).toBe("fail");
      expect(check?.detail).toContain("compile 3");
      const loaded = run({
         statusLoads: {
            polls: 20,
            unloaded: 3,
            loadDispatches: 1,
            dispatches: {},
         },
      });
      expect(
         outcomeOf(judge([run()], [loaded]), "loads started by /status"),
      ).toBe("fail");
   });

   it("is invalid, not a pass, when a run did not do the work", () => {
      expect(judge([run()], [run({ completedOps: 50 })]).outcome).toBe(
         "invalid",
      );
      expect(judge([run()], [run({ loadDispatches: 0 })]).outcome).toBe(
         "invalid",
      );
      expect(
         judge(
            [run()],
            [run({ ops: { load: { ok: 20, failed: { "503": 20 } } } })],
         ).outcome,
      ).toBe("invalid");
   });

   it("takes its limits from the caller", () => {
      const v = judge([run()], [run({ cpuPerOpMs: 95 })], {
         ...DEFAULT_LIMITS,
         cpuPerOpRatio: 2,
      });
      expect(outcomeOf(v, "main-thread CPU per op vs baseline")).toBe("pass");
   });
});

describe("judge at the limits", () => {
   const p90 = (n: number) => ({
      n: 100,
      p50: 40,
      p90: n,
      max: n,
      timeouts: 0,
   });

   it("keeps the documented limits", () => {
      expect(DEFAULT_LIMITS).toEqual({
         cpuPerOpRatio: 1.5,
         statusP90Ratio: 2,
         statusP90RatioFloorMs: 150,
         statusP90BudgetMs: 1600,
         maxBlockBudgetMs: 3000,
         rebindCompiles: 0,
         statusLoads: 0,
         indexedLoadQueryModelBuilds: 20,
      });
   });

   it("passes exactly 1.5x CPU and fails just over", () => {
      const at = judge([run()], [run({ cpuPerOpMs: 90 })]);
      expect(outcomeOf(at, "main-thread CPU per op vs baseline")).toBe("pass");
      const over = judge([run()], [run({ cpuPerOpMs: 90.1 })]);
      expect(outcomeOf(over, "main-thread CPU per op vs baseline")).toBe(
         "fail",
      );
   });

   it("passes exactly 1600 ms p90 and 3000 ms block, fails just over", () => {
      const at = judge([run()], [run({ status: p90(1600), maxBlockMs: 3000 })]);
      expect(outcomeOf(at, "/status p90 under the storm")).toBe("pass");
      expect(outcomeOf(at, "longest event-loop block")).toBe("pass");
      const over = judge(
         [run()],
         [run({ status: p90(1601), maxBlockMs: 3001 })],
      );
      expect(outcomeOf(over, "/status p90 under the storm")).toBe("fail");
      expect(outcomeOf(over, "longest event-loop block")).toBe("fail");
   });

   it("applies the p90 ratio only above the 150 ms floor", () => {
      const base = run({ status: p90(50) });
      expect(
         outcomeOf(
            judge([base], [run({ status: p90(150) })]),
            "/status p90 vs baseline",
         ),
      ).toBe("pass");
      expect(
         outcomeOf(
            judge([base], [run({ status: p90(151) })]),
            "/status p90 vs baseline",
         ),
      ).toBe("fail");
   });

   it("fails an indexed load over 20 QueryModel builds, naming the callers", () => {
      const bad = run({
         indexedLoad: {
            ...run().indexedLoad,
            builds: 256,
            byCaller: {
               buildDimIndexFingerprintLookup: 252,
               getDatabaseInfo: 4,
            },
         },
      });
      const v = judge([run()], [bad]);
      const c = v.checks.find(
         (k) => k.name === "main-thread QueryModel builds on an indexed load",
      );
      expect(c?.outcome).toBe("fail");
      expect(c?.detail).toContain("buildDimIndexFingerprintLookup 252");
      const at = run({ indexedLoad: { ...run().indexedLoad, builds: 20 } });
      expect(
         outcomeOf(
            judge([run()], [at]),
            "main-thread QueryModel builds on an indexed load",
         ),
      ).toBe("pass");
   });

   it("is invalid on a failed re-bind, few samples, empty sides or a counter that cannot count", () => {
      expect(
         judge([run()], [run({ rebind: { ...run().rebind, ok: false } })])
            .outcome,
      ).toBe("invalid");
      expect(
         judge([run()], [run({ status: { ...run().status, n: 9 } })]).outcome,
      ).toBe("invalid");
      expect(judge([], [run()]).outcome).toBe("invalid");
      expect(judge([run()], []).outcome).toBe("invalid");
      expect(
         judge(
            [run()],
            [run({ control: { queryStatus: 200, queryCompiles: 0 } })],
         ).outcome,
      ).toBe("invalid");
      expect(
         judge([run()], [run({ hooks: { ...run().hooks, compile: false } })])
            .outcome,
      ).toBe("invalid");
      expect(
         judge(
            [run()],
            [run({ queryModel: { builds: 0, ms: 0, byCaller: {} } })],
         ).outcome,
      ).toBe("invalid");
      expect(
         judge(
            [run()],
            [run({ indexedLoad: { ...run().indexedLoad, ok: false } })],
         ).outcome,
      ).toBe("invalid");
   });

   it("is invalid when a quiet check never got quiet", () => {
      expect(judge([run()], [run({ quietMs: [2000, -1, 2000] })]).outcome).toBe(
         "invalid",
      );
   });

   it("is invalid on the wrong runner, or a baseline far over budget", () => {
      expect(
         judge([run()], [run()], DEFAULT_LIMITS, { cpus: 2, expectCpus: 4 })
            .outcome,
      ).toBe("invalid");
      expect(
         judge([run()], [run()], DEFAULT_LIMITS, { cpus: 4, expectCpus: 4 })
            .outcome,
      ).toBe("pass");
      expect(judge([run({ status: p90(5000) })], [run()]).outcome).toBe(
         "invalid",
      );
      expect(judge([run({ maxBlockMs: 3500 })], [run()]).outcome).toBe(
         "invalid",
      );
   });

   it("compares every op type, but only notes one a side never serves", () => {
      const fewer = run({
         ops: {
            load: { ok: 40, failed: {} },
            unload: { ok: 5, failed: { "503": 15 } },
         },
      });
      const more = run({
         ops: { load: { ok: 40, failed: {} }, unload: { ok: 20, failed: {} } },
      });
      expect(judge([more], [fewer]).outcome).toBe("invalid");
      const noRoute = run({
         ops: {
            load: { ok: 40, failed: {} },
            entities: { ok: 0, failed: { "404": 9 } },
         },
      });
      const route = run({
         ops: { load: { ok: 40, failed: {} }, entities: { ok: 9, failed: {} } },
      });
      const v = judge([noRoute], [route]);
      expect(v.outcome).toBe("pass");
      expect(
         v.checks.find((c) => c.name === "ops only one side serves")?.value,
      ).toContain("entities");
   });
});

describe("median", () => {
   it("averages the middle two of an even count", () => {
      expect(median([1, 9, 3, 5])).toBe(4);
      expect(median([7])).toBe(7);
   });
});

describe("summarize", () => {
   it("tables every check and both sides' rounds", () => {
      const v = judge([run(), run()], [run(), run({ cpuPerOpMs: 120 })]);
      const md = summarize(
         v,
         [run(), run()],
         [run(), run({ cpuPerOpMs: 120 })],
         {
            baseline: "git:base",
            candidate: "git:head",
         },
      );
      expect(md).toContain("## Main-thread regression:");
      expect(md).toContain(
         "| main-thread CPU per op | 60 ms / 60 ms | 60 ms / 120 ms |",
      );
      expect(md).toContain(
         "main-thread Malloy compiles on an identical re-bind",
      );
      expect(md).toContain("indexed-load QueryModel builds");
   });
});
