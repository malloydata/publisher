// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The query mix shared by the `queries` and `mixed` scenarios (k6 side).
//
//   query_cheap   a single aggregate, no joins
//   query_medium  a group-by over the fact table
//   query_heavy   every join plus nested views
//   report_list   GET …/reports
//   report_get    GET …/reports/{path}
//
// Weights default to 40/35/20/2.5/2.5 (PERF_QUERY_WEIGHTS="40,35,20,2.5,2.5").
// The schedule is deterministic (smooth weighted round robin), so two runs
// issue the same mix in the same order.

import {
   getReport,
   listReports,
   nth,
   QUERY_OPS,
   runQuery,
   strEnv,
   WARMUP,
   type Op,
   type Workload,
} from "./k6.ts";

export const MIX_OPS: Op[] = [...QUERY_OPS, "report_list", "report_get"];

/** A 200-slot interleaved schedule honouring the weights. */
export function buildSchedule(weightsCsv: string): Op[] {
   const weights = weightsCsv.split(",").map(Number);
   if (weights.length !== MIX_OPS.length || weights.some((w) => !(w >= 0))) {
      throw new Error(
         `PERF_QUERY_WEIGHTS needs ${MIX_OPS.length} non-negative numbers (${MIX_OPS.join(",")})`,
      );
   }
   const total = weights.reduce((a, b) => a + b, 0);
   const current = weights.map(() => 0);
   const out: Op[] = [];
   for (let i = 0; i < 200; i++) {
      let best = 0;
      for (let j = 0; j < weights.length; j++) {
         current[j]! += weights[j]!;
         if (current[j]! > current[best]!) best = j;
      }
      current[best]! -= total;
      out.push(MIX_OPS[best]!);
   }
   return out;
}

export const SCHEDULE = buildSchedule(
   strEnv("PERF_QUERY_WEIGHTS", "40,35,20,2.5,2.5"),
);

/** The ops the schedule actually issues (a zero weight drops one). */
export const ACTIVE_MIX_OPS = MIX_OPS.filter((op) => SCHEDULE.includes(op));

/** Run `op` once; `n` picks which query text / report, round-robin. */
export function runOp(
   w: Workload,
   op: Op,
   n: number,
   tags: Record<string, string>,
): void {
   switch (op) {
      case "query_cheap":
      case "query_medium":
      case "query_heavy":
         runQuery(w, op, nth(w.queries[op], n), tags);
         return;
      case "report_list":
         listReports(w, tags);
         return;
      case "report_get":
         getReport(w, nth(w.reports, n), tags);
         return;
      default:
         throw new Error(`not a query op: ${op}`);
   }
}

/** Run slot `slot` of the schedule. */
export const runSlot = (
   w: Workload,
   slot: number,
   tags: Record<string, string>,
): void => runOp(w, nth(SCHEDULE, slot), slot, tags);

/**
 * Warm caches before measuring: every query text and report once, then
 * `requests` requests of the real mix. setup() is a single VU, so this is
 * sequential; the point is warm caches (compiled plans, DuckDB's file cache,
 * JIT), not concurrency.
 */
export function warmUp(w: Workload, requests: number): void {
   for (const op of QUERY_OPS) {
      w.queries[op].forEach((q) => runQuery(w, op, q, WARMUP));
   }
   if (w.reports.length > 0) {
      listReports(w, WARMUP);
      w.reports.forEach((r) => getReport(w, r, WARMUP));
   }
   for (let i = 0; i < requests; i++) runSlot(w, i, WARMUP);
}
