// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { LoadedEnvironmentsReader } from "./service/environment_ports";
import { lazyCounter, lazyInstrument, publisherMeter } from "./telemetry";

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

let observedStore: LoadedEnvironmentsReader | null = null;

const packagesGauge = lazyInstrument(() => {
   const gauge = publisherMeter().createObservableGauge("publisher_packages", {
      description:
         "Packages by state, summed over environments. Label: state ('serving'|'failed'|'stale').",
   });
   gauge.addCallback((result) => {
      if (!observedStore) return;
      let serving = 0;
      let failed = 0;
      let stale = 0;
      for (const environment of observedStore.getLoadedEnvironments()) {
         serving += environment.getLoadedPackages().length;
         failed += environment.getFailedPackages().size;
         stale += environment.getStaleCompileErrors().size;
      }
      result.observe(serving, { state: "serving" });
      result.observe(failed, { state: "failed" });
      result.observe(stale, { state: "stale" });
   });
   return gauge;
});

/**
 * `publisher_packages{state}`: how many packages are serving, failed to load,
 * or serving a stale model after a failed reload, read from the store's
 * in-memory state at each scrape. A package that is both loaded and stale
 * counts under both, matching how /status lists it.
 */
export function observePackageStates(store: LoadedEnvironmentsReader): void {
   observedStore = store;
   packagesGauge();
}
