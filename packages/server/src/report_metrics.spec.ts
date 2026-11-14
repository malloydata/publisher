// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import type express from "express";

import {
   recordReportDiscovery,
   recordReportDiscoveryDuration,
   recordReportWrite,
   resetReportMetricsForTest,
   summarizeReportDiscovery,
   timeReportRead,
   type ReportWriteOutcome,
} from "./report_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("report_metrics", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      // Drop cached instruments so they re-init against the new provider;
      // otherwise this test's records go to instruments bound to the previous
      // provider's reader.
      resetReportMetricsForTest();
   });

   afterEach(async () => {
      resetReportMetricsForTest();
      await harness.shutdown();
   });

   describe("discovery", () => {
      const summary = () =>
         summarizeReportDiscovery(
            [
               { status: "served", layout: "grid" },
               { status: "served", layout: "grid" },
               { status: "served", layout: "linear" },
               { status: "tileless" },
               { status: "invalid" },
               { status: "dropped" },
            ],
            [
               { code: "report-kind-removed", severity: "error" },
               { code: "report-cells-removed", severity: "error" },
               { code: "report-cells-removed", severity: "error" },
               // The manifest lint's findings carry no code of their own.
               { severity: "error" },
            ],
         );

      it("summarizes reports by layout, files by status, and findings by code and severity", () => {
         expect(summary()).toEqual({
            reports: { grid: 2, linear: 1 },
            files: { served: 3, tileless: 1, invalid: 1, dropped: 1 },
            findings: {
               "report-kind-removed": { error: 1 },
               "report-cells-removed": { error: 2 },
               "dashboard-lint": { error: 1 },
            },
         });
      });

      it("counts one pass's reports, files and findings under their labels", async () => {
         recordReportDiscovery(summary());

         const reports = (layout: string) =>
            harness.collectCounter("publisher_reports_discovered_total", {
               layout,
            });
         expect(await reports("grid")).toBe(2);
         expect(await reports("linear")).toBe(1);

         const files = (status: string) =>
            harness.collectCounter("publisher_report_files_total", { status });
         expect(await files("served")).toBe(3);
         expect(await files("tileless")).toBe(1);
         expect(await files("invalid")).toBe(1);
         expect(await files("dropped")).toBe(1);

         const findings = (code: string, severity: string) =>
            harness.collectCounter("publisher_report_lint_findings_total", {
               code,
               severity,
            });
         expect(await findings("report-cells-removed", "error")).toBe(2);
         expect(await findings("report-kind-removed", "error")).toBe(1);
         expect(await findings("dashboard-lint", "error")).toBe(1);
      });

      it("counts a second pass again, since each load or reload is one", async () => {
         recordReportDiscovery(summary());
         recordReportDiscovery(summary());
         expect(
            await harness.collectCounter("publisher_reports_discovered_total", {
               layout: "grid",
            }),
         ).toBe(4);
      });

      it("times each pass under its `during`", async () => {
         recordReportDiscoveryDuration("load", 40);
         recordReportDiscoveryDuration("reload", 10);
         recordReportDiscoveryDuration("reload", 20);
         recordReportDiscoveryDuration("compile", 5);

         const reload = await harness.collectHistogram(
            "publisher_report_discovery_duration_ms",
            { during: "reload" },
         );
         expect(reload.count).toBe(2);
         expect(reload.sum).toBe(30);
         const compile = await harness.collectHistogram(
            "publisher_report_discovery_duration_ms",
            { during: "compile" },
         );
         expect(compile.count).toBe(1);
         // A large package's pass must land in a bucket, not in +Inf past 10s.
         expect(reload.boundaries).toContain(60_000);
      });
   });

   describe("reads", () => {
      /** Run the middleware over a response that finishes with `status`. */
      const read = (route: "list" | "get", status: number) => {
         const res = Object.assign(new EventEmitter(), { statusCode: 200 });
         let nextCalled = false;
         timeReportRead(route)(
            {} as express.Request,
            res as unknown as express.Response,
            () => (nextCalled = true),
         );
         expect(nextCalled).toBe(true);
         res.statusCode = status;
         res.emit("finish");
      };

      it("times each read by route and status class, once it has answered", async () => {
         read("list", 200);
         read("get", 200);
         read("get", 404);
         read("get", 409);
         read("get", 500);

         const timed = async (route: string, status_class: string) =>
            (
               await harness.collectHistogram(
                  "publisher_report_read_duration_ms",
                  { route, status_class },
               )
            ).count;
         expect(await timed("list", "2xx")).toBe(1);
         expect(await timed("get", "2xx")).toBe(1);
         expect(await timed("get", "4xx")).toBe(2);
         expect(await timed("get", "5xx")).toBe(1);
      });
   });

   describe("writes", () => {
      it("counts each attempt once, labelled by outcome, layout and reason", async () => {
         recordReportWrite("created", "none", 120, "grid");
         recordReportWrite("replaced", "none", 95, "linear");
         recordReportWrite("replaced", "none", 80, "grid");
         recordReportWrite("refused", "untagged", 4, "grid");

         expect(
            await harness.collectCounter("publisher_report_writes_total", {
               outcome: "replaced",
            }),
         ).toBe(2);
         expect(
            await harness.collectCounter("publisher_report_writes_total", {
               outcome: "replaced",
               layout: "linear",
            }),
         ).toBe(1);
         expect(
            await harness.collectCounter("publisher_report_writes_total", {
               outcome: "refused",
               reason: "untagged",
            }),
         ).toBe(1);
      });

      it("records the duration against the same outcome and layout", async () => {
         recordReportWrite("created", "none", 100, "grid");
         recordReportWrite("created", "none", 300, "grid");

         const created = await harness.collectHistogram(
            "publisher_report_write_duration_ms",
            { outcome: "created", layout: "grid" },
         );
         expect(created.count).toBe(2);
         expect(created.sum).toBe(400);
      });

      /**
       * The rollback is the reason the write path is counted at all: a write
       * that compiled, landed, and then failed to reload leaves the author
       * certain they saved something the package is not serving. One whose
       * restore failed too may leave the package serving either text, so it is
       * the one an operator must be paged for, and it has its own outcome.
       */
      it("counts a rollback, and a failed one, apart from the refusals", async () => {
         recordReportWrite("refused", "frozen_config", 1, "grid");
         recordReportWrite("rolled_back", "reload_failed", 900, "grid");
         recordReportWrite("rollback_failed", "reload_failed", 950, "grid");

         for (const outcome of ["refused", "rolled_back", "rollback_failed"]) {
            expect(
               await harness.collectCounter("publisher_report_writes_total", {
                  outcome,
               }),
            ).toBe(1);
         }
      });

      it("emits every outcome the type allows, so none is unreachable by a dashboard", async () => {
         // A label an operator cannot chart is worse than no label: the panel
         // reads as zero rather than as missing.
         const outcomes: ReportWriteOutcome[] = [
            "created",
            "replaced",
            "conflict",
            "compile_failed",
            "refused",
            "rolled_back",
            "rollback_failed",
         ];
         for (const outcome of outcomes)
            recordReportWrite(outcome, "none", 10, "grid");
         for (const outcome of outcomes) {
            expect(
               await harness.collectCounter("publisher_report_writes_total", {
                  outcome,
               }),
            ).toBe(1);
         }
      });
   });
});
