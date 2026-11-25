// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Shared k6-side helpers for the perf scenarios: configuration from the
// environment the runner sets, tagged request wrappers for the current
// `environments` API, and the threshold set that makes per-op submetrics show
// up in `--summary-export`.
//
// This file runs inside k6 (goja), not Bun: only k6 modules and plain JS here.

import { check } from "k6";
import http from "k6/http";
import type { RefinedResponse, ResponseType } from "k6/http";
import { Gauge } from "k6/metrics";
import { logger } from "../../utils/logger.ts";

logger.setLogLevel(__ENV.K6_LOG_LEVEL || "warn");

export const PUBLISHER_URL = __ENV.K6_PUBLISHER_URL || "http://127.0.0.1:4200";
export const API = `${PUBLISHER_URL}/api/v0`;
export const ENV_NAME = __ENV.PERF_ENV || "perf";

/** What one target package offers the scenarios (written by the runner). */
export interface Workload {
   /** Package name as served. */
   package: string;
   /** Absolute path a copy of it can be registered from. */
   location: string;
   /** Model path every query runs against. */
   modelPath: string;
   /** Query text per class; one is picked per iteration, round-robin. */
   queries: {
      query_cheap: string[];
      query_medium: string[];
      query_heavy: string[];
   };
   /** Report paths `GET …/reports/{path}` reads. */
   reports: string[];
}

export function workloadFromEnv(name = "PERF_WORKLOAD"): Workload {
   const raw = __ENV[name];
   if (!raw) throw new Error(`${name} is not set (the runner sets it)`);
   return JSON.parse(raw) as Workload;
}

export const intEnv = (name: string, fallback: number): number => {
   const raw = __ENV[name];
   if (raw === undefined || raw === "") return fallback;
   const n = Number(raw);
   if (!Number.isFinite(n)) throw new Error(`${name} must be a number`);
   return n;
};

export const strEnv = (name: string, fallback: string): string =>
   __ENV[name] || fallback;

export type Op =
   | "register"
   | "delete"
   | "reload"
   | "query_cheap"
   | "query_medium"
   | "query_heavy"
   | "report_list"
   | "report_get";

export type QueryOp = "query_cheap" | "query_medium" | "query_heavy";
export const QUERY_OPS: QueryOp[] = [
   "query_cheap",
   "query_medium",
   "query_heavy",
];

type Res = RefinedResponse<ResponseType | undefined>;

const JSON_HEADERS = { "Content-Type": "application/json" };
const TIMEOUT = strEnv("PERF_HTTP_TIMEOUT", "300s");

function record(op: Op, res: Res, tags: Record<string, string>): Res {
   const ok = check(
      res,
      { [`${op} 2xx`]: (r) => r.status >= 200 && r.status < 300 },
      tags,
   );
   if (!ok) {
      logger.warn(
         `${op} -> ${res.status} ${String(res.body ?? "").slice(0, 300)}`,
      );
   }
   return res;
}

const pkgUrl = (pkg: string): string =>
   `${API}/environments/${ENV_NAME}/packages/${encodeURIComponent(pkg)}`;

export function registerPackage(
   name: string,
   location: string,
   tags: Record<string, string>,
): Res {
   const t = { ...tags, op: "register" };
   return record(
      "register",
      http.post(
         `${API}/environments/${ENV_NAME}/packages`,
         JSON.stringify({ name, location }),
         { headers: JSON_HEADERS, tags: t, timeout: TIMEOUT },
      ),
      t,
   );
}

export function deletePackage(name: string, tags: Record<string, string>): Res {
   const t = { ...tags, op: "delete" };
   return record(
      "delete",
      http.del(pkgUrl(name), null, { tags: t, timeout: TIMEOUT }),
      t,
   );
}

export function reloadPackage(name: string, tags: Record<string, string>): Res {
   const t = { ...tags, op: "reload" };
   return record(
      "reload",
      http.get(`${pkgUrl(name)}?reload=true`, { tags: t, timeout: TIMEOUT }),
      t,
   );
}

export function runQuery(
   w: Workload,
   op: QueryOp,
   query: string,
   tags: Record<string, string>,
): Res {
   const t = { ...tags, op };
   return record(
      op,
      http.post(
         `${pkgUrl(w.package)}/models/${w.modelPath}/query`,
         JSON.stringify({ query, compactJson: true }),
         { headers: JSON_HEADERS, tags: t, timeout: TIMEOUT },
      ),
      t,
   );
}

export function listReports(w: Workload, tags: Record<string, string>): Res {
   const t = { ...tags, op: "report_list" };
   return record(
      "report_list",
      http.get(`${pkgUrl(w.package)}/reports`, { tags: t, timeout: TIMEOUT }),
      t,
   );
}

export function getReport(
   w: Workload,
   reportPath: string,
   tags: Record<string, string>,
): Res {
   const t = { ...tags, op: "report_get" };
   return record(
      "report_get",
      http.get(`${pkgUrl(w.package)}/reports/${reportPath}`, {
         tags: t,
         timeout: TIMEOUT,
      }),
      t,
   );
}

/** Pick element `n` of a list, cycling. */
export const nth = <T>(list: T[], n: number): T => {
   if (list.length === 0) throw new Error("empty list");
   return list[((n % list.length) + list.length) % list.length]!;
};

/**
 * Thresholds that (a) create a submetric per op so `--summary-export` reports
 * it, and (b) sanity-check the error rate. Latency is never a pass/fail
 * criterion here: the `max>=0` entries always pass and exist only to make k6
 * keep the submetric.
 */
export function perOpThresholds(
   ops: Op[],
   extraTags: Record<string, string>[] = [{}],
): Record<string, string[]> {
   const maxErrorRate = strEnv("PERF_MAX_ERROR_RATE", "0.05");
   const out: Record<string, string[]> = {
      [`http_req_failed{phase:measure}`]: [`rate<=${maxErrorRate}`],
   };
   for (const extra of extraTags) {
      for (const op of ops) {
         const tagStr = Object.entries({ phase: "measure", op, ...extra })
            .map(([k, v]) => `${k}:${v}`)
            .join(",");
         out[`http_req_duration{${tagStr}}`] = ["max>=0"];
         out[`http_reqs{${tagStr}}`] = ["count>=0"];
         out[`http_req_failed{${tagStr}}`] = ["rate>=0"];
      }
   }
   return out;
}

/**
 * Wall-clock length of the measured phase, from the end of setup to the start
 * of teardown, so the report can turn request counts into throughput without
 * counting setup's warm-up.
 */
export const measureWindowMs = new Gauge("perf_measure_window_ms");

export interface SetupData {
   measureStartMs: number;
   [key: string]: unknown;
}

export const MEASURE = { phase: "measure" } as const;
export const WARMUP = { phase: "warmup" } as const;
