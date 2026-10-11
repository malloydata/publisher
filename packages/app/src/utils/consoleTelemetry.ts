// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ConsoleEvent, ReportEvent } from "@malloy-publisher/sdk";

/**
 * Where a report event happened. The event names the report by its package
 * path; the environment and package are the host's to add.
 */
export interface ReportSinkContext {
   environmentName: string;
   packageName: string;
}

const REPORT_PREFIX = "[publisher.report]";
const CONSOLE_PREFIX = "[publisher.console]";

/** Whether an event is an outcome someone should look at. */
export function reportEventFailed(event: ReportEvent): boolean {
   switch (event.type) {
      case "report.load_failed":
      case "report.query_failed":
      case "report.save_refused":
      case "report.save_undo_refused":
      case "report.create_refused":
         return true;
      case "report.rows_shown":
         return !event.ok;
      case "report.drill":
         return event.outcome !== "resolved";
      default:
         return false;
   }
}

/**
 * The Console's one sink for every `report.*` event, from the reader's view,
 * the builder and the New report dialog alike: one structured line per event,
 * at `warn` when something was refused or failed and `info` otherwise, so a
 * browser console (or whatever a deployment forwards it to) can be filtered
 * on the prefix and the `type`. Durations ride on the event.
 */
export function logReportEvent(
   context: ReportSinkContext,
): (event: ReportEvent) => void {
   return (event) => {
      const line = { ...context, ...event, at: new Date().toISOString() };
      if (reportEventFailed(event)) console.warn(REPORT_PREFIX, line);
      else console.info(REPORT_PREFIX, line);
   };
}

/**
 * The Console's sink for its own writes, installed once at boot.
 *
 * Same shape as the report sink: one structured line, `warn` when the write
 * failed and `info` when it landed, filterable on the prefix. A failed write
 * is the line worth keeping — before this, a delete that failed left no trace
 * once its six-second snackbar had gone.
 */
export function logConsoleEvent(event: ConsoleEvent): void {
   const line = { ...event, at: new Date().toISOString() };
   if (event.ok) console.info(CONSOLE_PREFIX, line);
   else console.warn(CONSOLE_PREFIX, line);
}
