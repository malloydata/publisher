// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Telemetry for the dashboard write path (see
 * `controller/dashboard.controller.ts`).
 *
 * This is the only endpoint in Publisher that MUTATES a package's model files,
 * and until now it was the only one that emitted nothing. The failures it has
 * are exactly the ones a log line does not answer: how often two editors
 * collide on one file, whether the compile gate is doing its job or turning
 * away every save, and — the one that matters — how often a write compiles,
 * lands, and then fails to reload, which rolls the file back and leaves the
 * author certain they saved. That last one is rare by design, so "rare" versus
 * "started happening after the upgrade" is a question only a counter answers.
 *
 * Duration is measured around the whole endpoint rather than the disk write:
 * the compile and the package reload are what a caller waits on, and a
 * dashboard that has grown expensive to compile shows up here first.
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

/** Which kind of file was written: the `kind` label on every write metric. */
export type DashboardWriteKind = "dashboard" | "notebook";

/**
 * How a write ended.
 *
 * - `created` / `replaced` — it landed, and the package is serving it.
 * - `conflict` — the precondition refused it: the file exists and no hash was
 *   sent, or the hash no longer matches. Nothing was written.
 * - `compile_failed` — the text does not compile. Nothing was written.
 * - `refused` — the request was refused before anything was written: a frozen
 *   config, a path that is not a dashboard or notebook, a body with no source,
 *   or a notebook write over a file the package does not serve as one.
 * - `rolled_back` — it compiled and was written, the package would not reload
 *   with it, and the previous text was put back.
 */
export type DashboardWriteOutcome =
   | "created"
   | "replaced"
   | "conflict"
   | "compile_failed"
   | "refused"
   | "rolled_back";

const writeCounter = lazyCounter(
   "publisher_dashboard_writes_total",
   "Dashboard and notebook write attempts. Labels: outcome ('created'|'replaced'|'conflict'|'compile_failed'|'refused'|'rolled_back'), kind ('dashboard'|'notebook').",
);

const writeDuration = lazyHistogram(
   "publisher_dashboard_write_duration_ms",
   "Wall-clock duration of a dashboard or notebook write, compile and package reload included. Labels: outcome, kind.",
   "ms",
);

/** One write attempt finished, however it finished. */
export function recordDashboardWrite(
   outcome: DashboardWriteOutcome,
   durationMs: number,
   kind: DashboardWriteKind,
): void {
   writeCounter().add(1, { outcome, kind });
   writeDuration().record(durationMs, { outcome, kind });
}

/** Drop memoized instruments so a test can install a fresh MeterProvider. */
export function resetDashboardWriteMetricsForTest(): void {
   for (const reset of resetHooks) reset();
}
