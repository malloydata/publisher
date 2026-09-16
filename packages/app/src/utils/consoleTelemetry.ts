// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardEvent } from "@malloy-publisher/sdk";

/** Which dashboard an event is about; the surface itself does not know. */
export interface DashboardContext {
   environmentName: string;
   packageName: string;
   dashboardName: string;
}

const LOG_PREFIX = "[publisher.dashboard]";

/**
 * The Console's telemetry sink for the dashboard surfaces: one structured
 * line per event, at `warn` when something was refused or failed and `info`
 * otherwise, so a browser console (or whatever a deployment forwards it to)
 * can be filtered on the prefix and the `type`. Durations ride on the event.
 */
export function logDashboardEvent(
   context: DashboardContext,
): (event: DashboardEvent) => void {
   return (event) => {
      const failed =
         event.type.endsWith("_refused") ||
         (event.type === "dashboard.rows_shown" && !event.ok);
      const line = { ...context, ...event, at: new Date().toISOString() };
      if (failed) console.warn(LOG_PREFIX, line);
      else console.info(LOG_PREFIX, line);
   };
}
