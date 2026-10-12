// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `mixed` scenario: query latency while packages load, against an isolated
// baseline measured in the same run.
//
// Timeline (D = PERF_DURATION):
//
//   0 ─────────── D ─────────── 2D
//   queries  ════════════════════════   constant-arrival-rate, PERF_RATE req/s
//            window=baseline │ window=under_load
//   reload_same          ════════════   PERF_LOADER_VUS VUs: reload the package
//                                       being queried, back to back
//   load_other           ════════════   PERF_LOADER_VUS VUs: register a copy of
//                                       ANOTHER package, reload it, delete it
//
// Queries run at a fixed arrival rate (open model) so a slow window cannot
// hide by issuing fewer requests: latency is measured at the same offered load
// in both windows, and any requests k6 could not start in time show up as
// dropped_iterations. A query's window is decided by when it STARTED.
//
// The runner (perf/run.ts) sets every env var; run it, not this file.

import exec from "k6/execution";
import {
   deletePackage,
   intEnv,
   MEASURE,
   measureWindowMs,
   perOpThresholds,
   registerPackage,
   reloadPackage,
   strEnv,
   workloadFromEnv,
   type SetupData,
} from "./lib/k6.ts";
import { ACTIVE_MIX_OPS, runSlot, warmUp } from "./lib/mix.ts";

const W = workloadFromEnv();
const OTHER = workloadFromEnv("PERF_OTHER_WORKLOAD");
const DURATION_S = intEnv("PERF_DURATION_S", 20);
const RATE = intEnv("PERF_RATE", 10);
const MAX_VUS = intEnv("PERF_MAX_VUS", 64);
const LOADER_VUS = intEnv("PERF_LOADER_VUS", 1);
const WARMUP_ROUNDS = intEnv("PERF_WARMUP_ROUNDS", 1);
const RUN_ID = strEnv("PERF_RUN_ID", String(Date.now()));
const prefix = `perf_${RUN_ID}_${OTHER.package}`.replace(
   /[^a-zA-Z0-9_-]/g,
   "_",
);

const loaderScenario = (exec_: string, loader: string) => ({
   executor: "constant-vus",
   exec: exec_,
   vus: LOADER_VUS,
   startTime: `${DURATION_S}s`,
   duration: `${DURATION_S}s`,
   gracefulStop: "120s",
   tags: { ...MEASURE, window: "under_load", loader },
});

export const options = {
   setupTimeout: strEnv("PERF_SETUP_TIMEOUT", "600s"),
   scenarios: {
      queries: {
         executor: "constant-arrival-rate",
         exec: "queries",
         rate: RATE,
         timeUnit: "1s",
         duration: `${DURATION_S * 2}s`,
         preAllocatedVUs: Math.min(MAX_VUS, Math.max(4, RATE)),
         maxVUs: MAX_VUS,
         gracefulStop: "60s",
         tags: { ...MEASURE },
      },
      reload_same: loaderScenario("reloadSame", "same"),
      load_other: loaderScenario("loadOther", "other"),
   },
   thresholds: {
      ...perOpThresholds(ACTIVE_MIX_OPS, [
         { window: "baseline" },
         { window: "under_load" },
      ]),
      ...perOpThresholds(
         ["reload"],
         [{ window: "under_load", loader: "same" }],
      ),
      ...perOpThresholds(
         ["register", "reload", "delete"],
         [{ window: "under_load", loader: "other" }],
      ),
      // An open model that cannot keep up drops iterations rather than
      // queueing them; surface the count in the summary.
      dropped_iterations: ["count>=0"],
   },
};

export function setup(): SetupData {
   warmUp(W, WARMUP_ROUNDS * Math.max(4, RATE));
   return { measureStartMs: Date.now() };
}

export function queries(): void {
   const elapsedMs = Date.now() - exec.scenario.startTime;
   const window = elapsedMs < DURATION_S * 1000 ? "baseline" : "under_load";
   runSlot(W, exec.scenario.iterationInTest, { ...MEASURE, window });
}

const LOADER_TAGS = (loader: string) => ({
   ...MEASURE,
   window: "under_load",
   loader,
});

export function reloadSame(): void {
   reloadPackage(W.package, LOADER_TAGS("same"));
}

export function loadOther(): void {
   const tags = LOADER_TAGS("other");
   const name = `${prefix}_${exec.vu.idInTest}_${exec.vu.iterationInInstance}`;
   const res = registerPackage(name, OTHER.location, tags);
   if (res.status === 200) reloadPackage(name, tags);
   deletePackage(name, res.status === 200 ? tags : { phase: "cleanup" });
}

export function teardown(data: SetupData): void {
   measureWindowMs.add(Date.now() - data.measureStartMs);
}
