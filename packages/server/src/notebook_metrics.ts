// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Telemetry for notebooks, both formats: whether discovery could read a
 * notebook's cells, and how running a cell ends.
 *
 * A served notebook the reader refuses still compiles and serves as a model, so
 * nothing else says it stopped opening; the discovery counter is what shows a
 * Malloy upgrade or an authoring pattern breaking the reader.
 *
 * Instruments are created lazily for the same reason as
 * {@link ./query_cap_metrics}: one created before `setGlobalMeterProvider`
 * binds to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { type Counter, type Histogram } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

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
): () => Histogram {
   let instrument: Histogram | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= publisherMeter().createHistogram(name, {
         description,
         unit,
      }));
}

/** `malloynb` is the legacy cell format; `malloy` is a served `notebooks/*.malloy`. */
export type NotebookFormat = "malloynb" | "malloy";

/**
 * - `ok` — its cells were read.
 * - `refused` — it compiled, and the reader refused its cells.
 * - `broken` — it did not compile, so it has no cells to read.
 */
export type NotebookDiscoveryOutcome = "ok" | "refused" | "broken";

/** `code` is a `.malloynb` code cell, which carries no finer kind. */
export type NotebookCellExecutionKind =
   | "markdown"
   | "query"
   | "definition"
   | "code";

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

const discoveryCounter = lazyCounter(
   "publisher_notebook_discovery_total",
   "Notebooks read at package discovery. Labels: format ('malloynb'|'malloy'), outcome ('ok'|'refused'|'broken').",
);

const cellExecutionCounter = lazyCounter(
   "publisher_notebook_cell_executions_total",
   "Notebook cell runs. Labels: format ('malloynb'|'malloy'), kind ('markdown'|'query'|'definition'|'code'), outcome ('ok'|'denied'|'not_queryable'|'bad_request'|'error').",
);

const cellExecutionDuration = lazyHistogram(
   "publisher_notebook_cell_execution_duration_ms",
   "Wall-clock duration of a notebook cell run. Labels: format, outcome.",
   "ms",
);

/** One notebook was read (or not) by a discovery pass. */
export function recordNotebookDiscovery(
   format: NotebookFormat,
   outcome: NotebookDiscoveryOutcome,
): void {
   discoveryCounter().add(1, { format, outcome });
}

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
