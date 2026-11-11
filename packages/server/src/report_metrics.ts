// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Telemetry for reports: what discovery found in a package, what the lint
 * said about it, how the two read routes answer, and how the write path ends.
 *
 * A dropped or tileless report is still a model that compiles and serves, so
 * only these counters show that an upgrade stopped a file reading as a report,
 * or that one lint finding started firing everywhere. Writes are counted by
 * outcome (collisions, compile refusals, rollbacks).
 *
 * Labels are bounded enumerations only; never a package name or a path.
 * Instruments are lazy: one created before `setGlobalMeterProvider` binds to
 * a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import type express from "express";
import { LOAD_DURATION_BUCKETS_MS } from "./package_load_metrics";
import {
   lazyCounter,
   lazyHistogram,
   REQUEST_DURATION_BUCKETS_MS,
   resetInstrumentsForTesting,
} from "./telemetry";

/** A report's layout: the `layout` label wherever one is recorded. */
export type ReportLayout = "grid" | "linear";

// ---------------------------------------------------------------------------
// Discovery and lint
// ---------------------------------------------------------------------------

/**
 * Which pass discovered the package's reports: its first load, a reload, or
 * the dry run a package-scope compile makes on a throwaway package.
 */
export type ReportDiscoveryDuring = "load" | "reload" | "compile";

/**
 * What discovery made of a file that carries an artifact tag:
 *
 * - `served` — a report, listed and served.
 * - `tileless` — a grid report with no tiles yet: not served until it has one.
 * - `invalid` — a tag that makes no report (the lint says why).
 * - `dropped` — a report that could not be read, so it is not served.
 */
export type ReportFileStatus = "served" | "tileless" | "invalid" | "dropped";

/** The `code` a finding with none of its own is counted and logged under. */
export const UNCODED_REPORT_FINDING = "dashboard-lint";

/** What one discovery found: the info log line and the counters both read it. */
export interface ReportDiscoverySummary {
   /** Served reports, by layout. */
   reports: Record<ReportLayout, number>;
   /** Tagged files, by what discovery made of them. */
   files: Record<ReportFileStatus, number>;
   /** Lint findings by code, then by severity. */
   findings: Record<string, Record<string, number>>;
}

/** Count `files` by status and layout, and `findings` by code and severity. */
export function summarizeReportDiscovery(
   files: Iterable<{ status: ReportFileStatus; layout?: ReportLayout }>,
   findings: Iterable<{ code?: string; severity?: string }>,
): ReportDiscoverySummary {
   const summary: ReportDiscoverySummary = {
      reports: { grid: 0, linear: 0 },
      files: { served: 0, tileless: 0, invalid: 0, dropped: 0 },
      findings: {},
   };
   for (const { status, layout } of files) {
      summary.files[status] += 1;
      if (status === "served" && layout) summary.reports[layout] += 1;
   }
   for (const {
      code = UNCODED_REPORT_FINDING,
      severity = "warn",
   } of findings) {
      const bySeverity = (summary.findings[code] ??= {});
      bySeverity[severity] = (bySeverity[severity] ?? 0) + 1;
   }
   return summary;
}

const discoveryDuration = lazyHistogram(
   "publisher_report_discovery_duration_ms",
   "Wall-clock duration of reading a package's reports and linting them. Labels: during ('load'|'reload'|'compile').",
   { buckets: LOAD_DURATION_BUCKETS_MS },
);

const reportsDiscovered = lazyCounter(
   "publisher_reports_discovered_total",
   "Reports served after a package load or reload, once per pass. Labels: layout ('grid'|'linear').",
);

const reportFiles = lazyCounter(
   "publisher_report_files_total",
   "Files carrying an artifact tag at a package load or reload, once per pass. Labels: status ('served'|'tileless'|'invalid'|'dropped').",
);

const lintFindings = lazyCounter(
   "publisher_report_lint_findings_total",
   "Report lint findings at a package load or reload, once per pass. Labels: code, severity ('warn'|'error').",
);

/** One discovery pass took `durationMs`, whichever pass it was. */
export function recordReportDiscoveryDuration(
   during: ReportDiscoveryDuring,
   durationMs: number,
): void {
   discoveryDuration().record(durationMs, { during });
}

/**
 * What a load or reload found. Not called for a compile's dry run: a what-if
 * counted beside the served state would make neither readable.
 */
export function recordReportDiscovery(summary: ReportDiscoverySummary): void {
   for (const [layout, count] of Object.entries(summary.reports)) {
      if (count > 0) reportsDiscovered().add(count, { layout });
   }
   for (const [status, count] of Object.entries(summary.files)) {
      if (count > 0) reportFiles().add(count, { status });
   }
   for (const [code, bySeverity] of Object.entries(summary.findings)) {
      for (const [severity, count] of Object.entries(bySeverity)) {
         lintFindings().add(count, { code, severity });
      }
   }
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/** `list` is `GET …/reports`; `get` is `GET …/reports/{path}`. */
export type ReportReadRoute = "list" | "get";

const readDuration = lazyHistogram(
   "publisher_report_read_duration_ms",
   "Wall-clock duration of a report read route. Labels: route ('list'|'get'), status_class ('2xx'|'4xx'|'5xx').",
   { buckets: REQUEST_DURATION_BUCKETS_MS },
);

function statusClass(status: number): string {
   return `${Math.floor(status / 100)}xx`;
}

/** Times a report read route until the response finishes, whatever it answered. */
export function timeReportRead(route: ReportReadRoute): express.RequestHandler {
   return (_req, res, next) => {
      const startedAt = performance.now();
      res.once("finish", () =>
         readDuration().record(performance.now() - startedAt, {
            route,
            status_class: statusClass(res.statusCode),
         }),
      );
      next();
   };
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/**
 * How a write ended.
 *
 * - `created` / `replaced` — it landed, and the package is serving it.
 * - `conflict` — the precondition refused it: the file exists and no hash was
 *   sent, or the hash no longer matches. Nothing was written.
 * - `compile_failed` — the text does not compile. Nothing was written.
 * - `refused` — the request was refused before anything was written; `reason`
 *   says why.
 * - `rolled_back` — it compiled and was written, the package would not reload
 *   with it, and the previous text was put back (even when reloading that
 *   failed: the file is restored, and what is left is a reload).
 * - `rollback_failed` — as `rolled_back`, but putting the previous text back
 *   failed too, so the package may serve the new text or none.
 */
export type ReportWriteOutcome =
   | "created"
   | "replaced"
   | "conflict"
   | "compile_failed"
   | "refused"
   | "rolled_back"
   | "rollback_failed";

/**
 * Why a write did not land; `none` when it did.
 *
 * - `frozen_config` — the server's config is frozen.
 * - `invalid_path` — not a path a report may be written at.
 * - `missing_source` — the body carries no `source`.
 * - `untagged` — the text carries no artifact tag, so it is no report.
 * - `not_a_report` — the file there is a model, which a report write never
 *   replaces.
 * - `conflict` / `compile_failed` — as the outcomes of the same name.
 * - `not_found` — the environment or package does not exist.
 * - `reload_failed` — it was written and the package would not serve it
 *   (`rolled_back` and `rollback_failed`).
 * - `bad_request` — any other 400.
 * - `other` — anything else, a server fault included.
 */
export type ReportWriteReason =
   | "none"
   | "frozen_config"
   | "invalid_path"
   | "missing_source"
   | "untagged"
   | "not_a_report"
   | "conflict"
   | "compile_failed"
   | "not_found"
   | "reload_failed"
   | "bad_request"
   | "other";

const writeCounter = lazyCounter(
   "publisher_report_writes_total",
   "Report write attempts. Labels: outcome ('created'|'replaced'|'conflict'|'compile_failed'|'refused'|'rolled_back'|'rollback_failed'), layout ('grid'|'linear'), reason ('none'|'frozen_config'|'invalid_path'|'missing_source'|'untagged'|'not_a_report'|'conflict'|'compile_failed'|'not_found'|'reload_failed'|'bad_request'|'other').",
);

const writeDuration = lazyHistogram(
   "publisher_report_write_duration_ms",
   "Wall-clock duration of a report write, compile and package reload included. Labels: outcome, layout.",
   { buckets: LOAD_DURATION_BUCKETS_MS },
);

/** One write attempt finished, however it finished. */
export function recordReportWrite(
   outcome: ReportWriteOutcome,
   reason: ReportWriteReason,
   durationMs: number,
   layout: ReportLayout,
): void {
   writeCounter().add(1, { outcome, layout, reason });
   writeDuration().record(durationMs, { outcome, layout });
}

/** Drop memoized instruments so a test can install a fresh MeterProvider. */
export function resetReportMetricsForTest(): void {
   resetInstrumentsForTesting();
}
