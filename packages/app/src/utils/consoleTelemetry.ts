// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ConsoleEvent, DashboardEvent } from "@malloy-publisher/sdk";

/** Which dashboard an event is about; the surface itself does not know. */
export interface DashboardContext {
   environmentName: string;
   packageName: string;
   dashboardName: string;
}

const DASHBOARD_PREFIX = "[publisher.dashboard]";
const CONSOLE_PREFIX = "[publisher.console]";

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
      if (failed) console.warn(DASHBOARD_PREFIX, line);
      else console.info(DASHBOARD_PREFIX, line);
   };
}

/**
 * The Console's sink for its own writes, installed once at boot.
 *
 * Same shape as the dashboard sink: one structured line, `warn` when the write
 * failed and `info` when it landed, filterable on the prefix. A failed write
 * is the line worth keeping — before this, a delete that failed left no trace
 * once its six-second snackbar had gone.
 */
export function logConsoleEvent(event: ConsoleEvent): void {
   const line = { ...event, at: new Date().toISOString() };
   if (event.ok) console.info(CONSOLE_PREFIX, line);
   else console.warn(CONSOLE_PREFIX, line);
}
