// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ConsoleEvent, DashboardEvent } from "@malloy-publisher/sdk";
import type { NotebookEvent } from "@malloy-publisher/sdk/builder";

/** Which dashboard an event is about; the surface itself does not know. */
export interface DashboardContext {
   environmentName: string;
   packageName: string;
   dashboardName: string;
}

/** Which notebook an event is about; the editor itself does not know. */
export interface NotebookContext {
   environmentName: string;
   packageName: string;
   notebookName: string;
}

const DASHBOARD_PREFIX = "[publisher.dashboard]";
const NOTEBOOK_PREFIX = "[publisher.notebook]";
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

/** The notebook editor's sink: the dashboard sink's shape, under its own prefix. */
export function logNotebookEvent(
   context: NotebookContext,
): (event: NotebookEvent) => void {
   return (event) => {
      const line = { ...context, ...event, at: new Date().toISOString() };
      if (event.type.endsWith("_refused")) console.warn(NOTEBOOK_PREFIX, line);
      else console.info(NOTEBOOK_PREFIX, line);
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
