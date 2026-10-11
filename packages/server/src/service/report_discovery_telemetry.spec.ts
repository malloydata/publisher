// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a package load and reload say about their reports, on a real package:
 * one `info` summary per discovery and one `debug` line per finding (never a
 * `warn` per finding, which every load and reload used to repeat), the
 * discovery counters, and the deprecated features the package uses.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { resetDeprecatedUsageMetricsForTest } from "../deprecated_usage_metrics";
import { logger } from "../logger";
import { resetReportMetricsForTest } from "../report_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { Environment } from "./environment";
import type { Package } from "./package";

const PACKAGE = "tele";

const FILES: Record<string, string> = {
   // Both keys deprecated; `"all"` keeps every source queryable, so the
   // surface does not get in the way of the reports below.
   "publisher.json": JSON.stringify({
      name: PACKAGE,
      version: "1.0.0",
      explores: ["base.malloy"],
      queryableSources: "all",
   }),
   "base.malloy":
      `#(filter) dimension=x type=equal\n` +
      `source: s is duckdb.sql("select 1 as x") extend {\n` +
      `   view: v is { group_by: x }\n` +
      `}\n`,
   "grid.malloy": `## artifact { layout=grid tiles=["s -> v"] }\nimport "base.malloy"\n`,
   "story.malloy": `## artifact { layout=linear }\nimport "base.malloy"\nrun: s -> v\n`,
   // `kind=` is no longer read: a tag that makes no report.
   "old.malloy": `## artifact { kind=dashboard tiles=["s -> v"] }\nimport "base.malloy"\n`,
   "legacy.malloynb": `>>>markdown\n# A legacy notebook\n`,
};

type Calls = { mock: { calls: unknown[][] } };
const callsOf = (spy: Calls, message: string) =>
   spy.mock.calls
      .filter(([text]) => text === message)
      .map(([, meta]) => meta as Record<string, unknown>);

describe("report discovery telemetry", () => {
   let rootDir: string;
   let env: Environment;
   let harness: MetricsHarness;
   const spies: { mockRestore(): void }[] = [];
   let info: Calls;
   let debug: Calls;
   let warn: Calls;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetReportMetricsForTest();
      resetDeprecatedUsageMetricsForTest();
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-tele-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      const spy = <T extends "info" | "debug" | "warn">(level: T) => {
         const s = spyOn(logger, level);
         spies.push(s);
         return s as unknown as Calls;
      };
      info = spy("info");
      debug = spy("debug");
      warn = spy("warn");
   });

   afterEach(async () => {
      for (const spy of spies.splice(0)) spy.mockRestore();
      resetReportMetricsForTest();
      resetDeprecatedUsageMetricsForTest();
      await harness.shutdown();
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const install = async (): Promise<Package> => {
      await env.installPackage(PACKAGE, async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         for (const [name, text] of Object.entries(FILES))
            await fs.writeFile(path.join(stagingPath, name), text);
      });
      return env.getPackage(PACKAGE, false);
   };

   it("logs one info summary per load, and each finding once at debug", async () => {
      const pkg = await install();

      const summaries = callsOf(info, "Report discovery");
      expect(summaries).toHaveLength(1);
      const [summary] = summaries;
      expect(summary).toMatchObject({
         packageName: PACKAGE,
         during: "load",
         reports: { grid: 1, linear: 2 },
         files: { served: 3, tileless: 0, invalid: 1, dropped: 0 },
      });
      expect(typeof summary.durationMs).toBe("number");
      const findings = summary.findings as Record<
         string,
         Record<string, number>
      >;
      expect(findings["report-kind-removed"]).toEqual({ error: 1 });

      // One debug line per finding the package carries, with the shared
      // field vocabulary, and the summary's counts add up to them.
      const lines = callsOf(debug, "Report lint finding");
      const total = Object.values(findings)
         .flatMap((bySeverity) => Object.values(bySeverity))
         .reduce((sum, n) => sum + n, 0);
      expect(lines).toHaveLength(total);
      expect(lines).toContainEqual(
         expect.objectContaining({
            packageName: PACKAGE,
            model: "old.malloy",
            code: "report-kind-removed",
            severity: "error",
         }),
      );
      // The per-finding `warn` is gone.
      expect(
         warn.mock.calls.filter(([text]) =>
            String(text).startsWith("Report lint"),
         ),
      ).toEqual([]);
      expect(pkg.listReports().map((report) => report.path)).toEqual([
         "grid.malloy",
         "legacy.malloynb",
         "story.malloy",
      ]);
   });

   it("logs a reload as its own discovery", async () => {
      const pkg = await install();
      await pkg.reloadAllModels({});
      expect(
         callsOf(info, "Report discovery").map((meta) => meta.during),
      ).toEqual(["load", "reload"]);
   });

   it("counts what a load discovered, and times it", async () => {
      await install();
      const count = (name: string, labels: Record<string, string>) =>
         harness.collectCounter(name, labels);

      expect(
         await count("publisher_reports_discovered_total", { layout: "grid" }),
      ).toBe(1);
      expect(
         await count("publisher_reports_discovered_total", {
            layout: "linear",
         }),
      ).toBe(2);
      expect(
         await count("publisher_report_files_total", { status: "invalid" }),
      ).toBe(1);
      expect(
         await count("publisher_report_lint_findings_total", {
            code: "report-kind-removed",
            severity: "error",
         }),
      ).toBe(1);
      expect(
         (
            await harness.collectHistogram(
               "publisher_report_discovery_duration_ms",
               { during: "load" },
            )
         ).count,
      ).toBe(1);
   });

   it("counts each deprecated feature the package uses, once per load", async () => {
      const pkg = await install();
      const used = (feature: string) =>
         harness.collectCounter("publisher_deprecated_usage_total", {
            feature,
         });
      expect(await used("malloynb")).toBe(1);
      expect(await used("filter_annotation")).toBe(1);
      expect(await used("explores")).toBe(1);
      expect(await used("queryable_sources")).toBe(1);

      await pkg.reloadAllModels({});
      expect(await used("explores")).toBe(2);
      expect(await used("malloynb")).toBe(2);
   });
});
