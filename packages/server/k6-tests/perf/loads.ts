// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `loads` scenario: package load latency and throughput at one concurrency.
//
//   PERF_LOAD_OP=register  each VU loops: POST a new package (a copy of the
//                          target, unique name) and DELETE it. Measures a cold
//                          install: copy + compile + register.
//   PERF_LOAD_OP=reload    setup registers PERF_VUS copies of the target; each
//                          VU loops `GET …/packages/{its copy}?reload=true`.
//                          Measures recompiling a package already resident.
//
// Executor: constant-vus (PERF_VUS VUs for PERF_DURATION) — a closed loop, so
// the result is saturation throughput at that concurrency and the latency it
// costs. Warm-up runs in setup(), PERF_WARMUP_ROUNDS rounds of PERF_VUS
// operations in parallel, tagged phase=warmup and excluded from the stats.
//
// The runner (perf/run.ts) sets every env var; run it, not this file.

import exec from "k6/execution";
import http from "k6/http";
import {
   API,
   deletePackage,
   ENV_NAME,
   intEnv,
   MEASURE,
   measureWindowMs,
   perOpThresholds,
   registerPackage,
   reloadPackage,
   strEnv,
   WARMUP,
   workloadFromEnv,
   type SetupData,
} from "./lib/k6.ts";

const W = workloadFromEnv();
const OP = strEnv("PERF_LOAD_OP", "register");
if (OP !== "register" && OP !== "reload") {
   throw new Error(`PERF_LOAD_OP must be register or reload, got ${OP}`);
}
const VUS = intEnv("PERF_VUS", 1);
const DURATION = strEnv("PERF_DURATION", "20s");
const WARMUP_ROUNDS = intEnv("PERF_WARMUP_ROUNDS", 1);
// Unique per k6 run, so names never collide with a previous run's leftovers.
const RUN_ID = strEnv("PERF_RUN_ID", String(Date.now()));
const prefix = `perf_${RUN_ID}_${W.package}`.replace(/[^a-zA-Z0-9_-]/g, "_");

export const options = {
   setupTimeout: strEnv("PERF_SETUP_TIMEOUT", "600s"),
   teardownTimeout: strEnv("PERF_SETUP_TIMEOUT", "600s"),
   scenarios: {
      [OP]: {
         executor: "constant-vus",
         vus: VUS,
         duration: DURATION,
         gracefulStop: "120s",
         tags: { ...MEASURE },
      },
   },
   thresholds: perOpThresholds(
      OP === "register" ? ["register", "delete"] : ["reload"],
   ),
};

const reloadCopy = (i: number): string => `${prefix}_r${i}`;

/** POST one batch of requests in parallel; returns how many were 2xx. */
function parallel(
   reqs: Array<{ method: string; url: string; body?: string; op: string }>,
   tags: Record<string, string>,
): number {
   const res = http.batch(
      reqs.map((r) => ({
         method: r.method,
         url: r.url,
         body: r.body ?? null,
         params: {
            headers: { "Content-Type": "application/json" },
            tags: { ...tags, op: r.op },
            timeout: "300s",
         },
      })),
   );
   return res.filter((r) => r.status >= 200 && r.status < 300).length;
}

const pkgsUrl = `${API}/environments/${ENV_NAME}/packages`;

export function setup(): SetupData {
   if (OP === "reload") {
      // The copies each VU reloads, registered in parallel (setup phase).
      const created = parallel(
         Array.from({ length: VUS }, (_, i) => ({
            method: "POST",
            url: pkgsUrl,
            body: JSON.stringify({ name: reloadCopy(i), location: W.location }),
            op: "register",
         })),
         { phase: "setup" },
      );
      if (created !== VUS) {
         exec.test.abort(`setup registered ${created}/${VUS} reload copies`);
      }
   }
   for (let round = 0; round < WARMUP_ROUNDS; round++) {
      if (OP === "reload") {
         parallel(
            Array.from({ length: VUS }, (_, i) => ({
               method: "GET",
               url: `${pkgsUrl}/${reloadCopy(i)}?reload=true`,
               op: "reload",
            })),
            WARMUP,
         );
      } else {
         const names = Array.from(
            { length: VUS },
            (_, i) => `${prefix}_w${round}_${i}`,
         );
         parallel(
            names.map((name) => ({
               method: "POST",
               url: pkgsUrl,
               body: JSON.stringify({ name, location: W.location }),
               op: "register",
            })),
            WARMUP,
         );
         parallel(
            names.map((name) => ({
               method: "DELETE",
               url: `${pkgsUrl}/${name}`,
               op: "delete",
            })),
            WARMUP,
         );
      }
   }
   return { measureStartMs: Date.now() };
}

export default function (): void {
   const vu = exec.vu.idInTest;
   if (OP === "reload") {
      reloadPackage(reloadCopy((vu - 1) % VUS), MEASURE);
      return;
   }
   const name = `${prefix}_${vu}_${exec.vu.iterationInInstance}`;
   const res = registerPackage(name, W.location, MEASURE);
   // Delete even after a failed register: a 5xx can leave a half-made package.
   deletePackage(name, res.status === 200 ? MEASURE : { phase: "cleanup" });
}

export function teardown(data: SetupData): void {
   measureWindowMs.add(Date.now() - data.measureStartMs);
   if (OP === "reload") {
      parallel(
         Array.from({ length: VUS }, (_, i) => ({
            method: "DELETE",
            url: `${pkgsUrl}/${reloadCopy(i)}`,
            op: "delete",
         })),
         { phase: "teardown" },
      );
   }
}
