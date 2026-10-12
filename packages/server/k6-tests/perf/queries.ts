// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `queries` scenario: query latency and throughput at one concurrency.
//
// Each iteration runs ONE request from the weighted mix in lib/mix.ts
// (query_cheap / query_medium / query_heavy / report_list / report_get), each
// tagged with its `op` so the report breaks results out per class.
//
// Executor: constant-vus (PERF_VUS VUs for PERF_DURATION), a closed loop: the
// result is saturation throughput at that concurrency and the latency it costs.
// Each VU holds at most one request in flight, so PERF_VUS is also the
// server-side concurrency. Warm-up runs in setup() (every query text and report
// once, then PERF_WARMUP_ROUNDS × PERF_VUS requests of the mix), tagged
// phase=warmup and excluded from the stats.
//
// The runner (perf/run.ts) sets every env var; run it, not this file.

import exec from "k6/execution";
import {
   intEnv,
   MEASURE,
   measureWindowMs,
   perOpThresholds,
   strEnv,
   workloadFromEnv,
   type SetupData,
} from "./lib/k6.ts";
import { ACTIVE_MIX_OPS, runSlot, warmUp } from "./lib/mix.ts";

const W = workloadFromEnv();
const VUS = intEnv("PERF_VUS", 1);
const DURATION = strEnv("PERF_DURATION", "20s");
const WARMUP_ROUNDS = intEnv("PERF_WARMUP_ROUNDS", 1);

export const options = {
   setupTimeout: strEnv("PERF_SETUP_TIMEOUT", "600s"),
   scenarios: {
      queries: {
         executor: "constant-vus",
         vus: VUS,
         duration: DURATION,
         gracefulStop: "60s",
         tags: { ...MEASURE },
      },
   },
   thresholds: perOpThresholds(ACTIVE_MIX_OPS),
};

export function setup(): SetupData {
   warmUp(W, WARMUP_ROUNDS * VUS);
   return { measureStartMs: Date.now() };
}

export default function (): void {
   // Offset each VU into the schedule so concurrent VUs run different ops.
   runSlot(W, exec.vu.iterationInInstance + exec.vu.idInTest * 37, MEASURE);
}

export function teardown(data: SetupData): void {
   measureWindowMs.add(Date.now() - data.measureStartMs);
}
