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
         (c) => c.name === "served-copy compiles on an identical re-bind",
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
      expect(md).toContain("served-copy compiles on an identical re-bind");
   });
});
