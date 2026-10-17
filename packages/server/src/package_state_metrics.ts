// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ObservableGauge } from "@opentelemetry/api";
import type { EnvironmentStore } from "./service/environment_store";
import { lazyCounter, publisherMeter } from "./telemetry";

export type ReloadTrigger = "rest" | "mcp" | "watch";
export type ReloadOutcome = "success" | "failure";

const reloads = lazyCounter(
   "publisher_package_reloads_total",
   "Package reloads. Labels: trigger ('rest'|'mcp'|'watch'), outcome ('success'|'failure'). A failed reload leaves the previous model serving, marked stale in /status.",
);

export function recordPackageReload(
   trigger: ReloadTrigger,
   outcome: ReloadOutcome,
): void {
   reloads().add(1, { trigger, outcome });
}

let packagesGauge: ObservableGauge | null = null;

/**
 * `publisher_packages{state}`: how many packages are serving, failed to load,
 * or serving a stale model after a failed reload, read from the store's
 * in-memory state at each scrape. A package that is both loaded and stale
 * counts under both, matching how /status lists it. Registered once.
 */
export function observePackageStates(store: EnvironmentStore): void {
   if (packagesGauge) return;
   packagesGauge = publisherMeter().createObservableGauge(
      "publisher_packages",
      {
         description:
            "Packages by state, summed over environments. Label: state ('serving'|'failed'|'stale').",
      },
   );
   packagesGauge.addCallback((result) => {
      let serving = 0;
      let failed = 0;
      let stale = 0;
      for (const environment of store.getLoadedEnvironments()) {
         serving += environment.getLoadedPackages().length;
         failed += environment.getFailedPackages().size;
         stale += environment.getStaleCompileErrors().size;
      }
      result.observe(serving, { state: "serving" });
      result.observe(failed, { state: "failed" });
      result.observe(stale, { state: "stale" });
   });
}

/** Visible for tests: lets a fresh MeterProvider register the gauge again. */
export function resetPackageStateGaugeForTesting(): void {
   packagesGauge = null;
}
