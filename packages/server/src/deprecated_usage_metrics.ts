// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How much each deprecated feature is still used: the evidence for deleting
 * it, or for not deleting it yet.
 *
 * Each feature is counted inside the seam that keeps it alive, so deleting the
 * seam deletes its counting, and its value here goes with it:
 *
 * - A request: the legacy report routes, one per route family, counted in
 *   their controller (`controller/legacy_report.controller.ts`) so the
 *   environment and project paths count alike; a Console page load on a `dashboards/<name>` or
 *   `notebooks/<name>` alias of a report, counted by the server's app
 *   fallback (`service/legacy_report_views.ts`; not under a development
 *   server, where Vite serves the app); a query carrying
 *   `filterParams` / `filter_params` or `bypassFilters` / `bypass_filters`
 *   (`filter_deprecation.ts`), on REST and MCP alike; and a
 *   `PATCH …/packages/{pkg}`, counted in its controller
 *   (`PackageController.updatePackage`) so the environment and project paths
 *   count alike.
 * - A package load or reload: per package that sets `explores` or
 *   `queryableSources` in publisher.json, per `.malloynb` file, and per source
 *   that declares `#(filter)` annotations (`service/deprecated/*`).
 *
 * Lazily created for the same reason as {@link ./query_cap_metrics}.
 */

import { lazyCounter, resetInstrumentsForTesting } from "./telemetry";

export type DeprecatedFeature =
   | "dashboards_list"
   | "dashboard_get"
   | "notebooks_list"
   | "notebook_get"
   | "notebook_cell"
   | "console_alias"
   | "malloynb"
   | "explores"
   | "queryable_sources"
   | "filter_annotation"
   | "filter_params"
   | "bypass_filters"
   | "package_patch";

const counter = lazyCounter(
   "publisher_deprecated_usage_total",
   "Uses of a deprecated feature: a request to a legacy route or carrying a legacy parameter, or a package load or reload that uses one. Labels: feature.",
);

/** `count` uses of a deprecated feature. */
export function recordDeprecatedUsage(
   feature: DeprecatedFeature,
   count = 1,
): void {
   if (count <= 0) return;
   counter().add(count, { feature });
}

/** Drop the memoized instrument so a test can install a fresh MeterProvider. */
export function resetDeprecatedUsageMetricsForTest(): void {
   resetInstrumentsForTesting();
}
