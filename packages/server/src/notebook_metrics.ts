// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: outcome and duration of the `.malloynb` cell route
 * (`GET …/notebooks/{path}/cells/{i}`, kept by `routes/deprecated.ts`). A
 * `.malloy` report's cells are measured as queries. This module goes with the
 * route.
 *
 * Lazy: an instrument created before `setGlobalMeterProvider` binds to a NoOp
 * meter (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import {
   lazyCounter,
   lazyHistogram,
   REQUEST_DURATION_BUCKETS_MS,
   resetInstrumentsForTesting,
} from "./telemetry";

/** `malloynb` is the legacy cell format; `malloy` is a linear `.malloy` report the route also runs. */
export type NotebookFormat = "malloynb" | "malloy";

/**
 * `code` is a `.malloynb` code cell, which carries no finer kind; `none` is a
 * served notebook's run that names no cell (out of range, or refused cells).
 */
export type NotebookCellExecutionKind =
   | "markdown"
   | "query"
   | "definition"
   | "code"
   | "none";

/**
 * - `ok` — answered, with a result for a query cell.
 * - `denied` — an authorize gate refused it (403).
 * - `not_queryable` — the surface refused it (404).
 * - `bad_request` — the request or the cell was invalid (400).
 * - `error` — anything else.
 */
export type NotebookCellExecutionOutcome =
   | "ok"
   | "denied"
   | "not_queryable"
   | "bad_request"
   | "error";

const cellExecutionCounter = lazyCounter(
   "publisher_notebook_cell_executions_total",
   "Notebook cell runs. Labels: format ('malloynb'|'malloy'), kind ('markdown'|'query'|'definition'|'code'|'none'), outcome ('ok'|'denied'|'not_queryable'|'bad_request'|'error').",
);

const cellExecutionDuration = lazyHistogram(
   "publisher_notebook_cell_execution_duration_ms",
   "Wall-clock duration of a notebook cell run. Labels: format, outcome.",
   // The HTTP request histogram's set, since a cell run is one request.
   { buckets: REQUEST_DURATION_BUCKETS_MS },
);

/** One cell run finished, however it finished. */
export function recordNotebookCellExecution(
   format: NotebookFormat,
   kind: NotebookCellExecutionKind,
   outcome: NotebookCellExecutionOutcome,
   durationMs: number,
): void {
   cellExecutionCounter().add(1, { format, kind, outcome });
   cellExecutionDuration().record(durationMs, { format, outcome });
}

/** Drop memoized instruments so a test can install a fresh MeterProvider. */
export function resetNotebookMetricsForTest(): void {
   resetInstrumentsForTesting();
}
