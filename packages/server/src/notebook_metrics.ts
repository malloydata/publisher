// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: telemetry for the notebook cell route
 * (`GET …/notebooks/{path}/cells/{i}`), which `routes/deprecated.ts` keeps for
 * `.malloynb` notebooks: how running a cell ends, and how long it takes. A
 * `.malloy` report's cells run through `POST …/models/{path}/query` and are
 * measured as queries. Whether a report's cells could be read at all is the
 * `notebook-cells-unreadable` finding, counted with the other report findings
 * (`report_metrics.ts`). This module goes with the route.
 *
 * Instruments are created lazily for the same reason as
 * {@link ./query_cap_metrics}: one created before `setGlobalMeterProvider`
 * binds to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { type Counter, type Histogram } from "@opentelemetry/api";
import { publisherMeter, REQUEST_DURATION_BUCKETS_MS } from "./telemetry";

const resetHooks: (() => void)[] = [];

function lazyCounter(name: string, description: string): () => Counter {
   let instrument: Counter | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= publisherMeter().createCounter(name, { description }));
}

function lazyHistogram(
   name: string,
   description: string,
   unit: string,
   buckets: number[],
): () => Histogram {
   let instrument: Histogram | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= publisherMeter().createHistogram(name, {
         description,
         unit,
         advice: { explicitBucketBoundaries: buckets },
      }));
}

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
   "ms",
   // The HTTP request histogram's set, since a cell run is one request.
   REQUEST_DURATION_BUCKETS_MS,
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
   for (const reset of resetHooks) reset();
}
