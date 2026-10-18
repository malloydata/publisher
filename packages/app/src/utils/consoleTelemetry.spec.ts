// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ReportEvent } from "@malloy-publisher/sdk";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { logReportEvent, reportEventFailed } from "./consoleTelemetry";

const at = { path: "sales/q3.malloy", layout: "grid" } as const;

describe("logReportEvent", () => {
   let info: ReturnType<typeof spyOn>;
   let warn: ReturnType<typeof spyOn>;
   beforeEach(() => {
      info = spyOn(console, "info").mockImplementation(() => {});
      warn = spyOn(console, "warn").mockImplementation(() => {});
   });
   afterEach(() => {
      info.mockRestore();
      warn.mockRestore();
   });

   it("writes one structured line under the report prefix, with where it happened", () => {
      logReportEvent({ environmentName: "env", packageName: "pkg" })({
         ...at,
         type: "report.opened",
         surface: "view",
         from: "package",
         tiles: 3,
         durationMs: 12,
      });
      expect(warn).not.toHaveBeenCalled();
      expect(info).toHaveBeenCalledTimes(1);
      const [prefix, line] = info.mock.calls[0] as [string, object];
      expect(prefix).toBe("[publisher.report]");
      expect(line).toMatchObject({
         environmentName: "env",
         packageName: "pkg",
         path: "sales/q3.malloy",
         layout: "grid",
         type: "report.opened",
         tiles: 3,
         durationMs: 12,
      });
      expect(typeof (line as { at: unknown }).at).toBe("string");
   });

   it("warns on a failure and informs on everything else", () => {
      const log = logReportEvent({
         environmentName: "env",
         packageName: "pkg",
      });
      log({ ...at, type: "report.save_refused", reason: "stale" });
      log({
         ...at,
         type: "report.opened",
         surface: "view",
         from: "package",
         tiles: 3,
         durationMs: 12,
      });
      expect(warn).toHaveBeenCalledTimes(1);
      expect(info).toHaveBeenCalledTimes(1);
   });
});

describe("reportEventFailed", () => {
   const failed: ReportEvent[] = [
      {
         path: "x.malloy",
         type: "report.load_failed",
         surface: "view",
         category: "not_found",
      },
      { ...at, type: "report.query_failed", tile: 0, category: "server" },
      { ...at, type: "report.save_refused", reason: "x" },
      { ...at, type: "report.save_undo_refused", reason: "x" },
      { ...at, type: "report.create_refused", reason: "x" },
      {
         ...at,
         type: "report.rows_shown",
         source: "s",
         view: "v",
         field: "f",
         ok: false,
         durationMs: 1,
      },
      { ...at, type: "report.drill", to: "report", outcome: "unresolved" },
      { ...at, type: "report.drill", to: "self", outcome: "malformed" },
   ];
   const fine: ReportEvent[] = [
      {
         ...at,
         type: "report.saved",
         tiles: 1,
         structural: false,
         where: "package",
         durationMs: 1,
      },
      { ...at, type: "report.created", where: "host", durationMs: 1 },
      {
         ...at,
         type: "report.rows_shown",
         source: "s",
         view: "v",
         field: "f",
         ok: true,
         durationMs: 1,
      },
      { ...at, type: "report.drill", to: "report", outcome: "resolved" },
      // Clicked before the listing could say: the page it lands on says.
      { ...at, type: "report.drill", to: "report", outcome: "unverified" },
      { ...at, type: "report.explored", tile: "t" },
   ];

   it.each(failed.map((event) => [event.type, event]))(
      "counts %s as a failure",
      (_type, event) =>
         expect(reportEventFailed(event as ReportEvent)).toBe(true),
   );
   it.each(fine.map((event) => [event.type, event]))(
      "counts %s as fine",
      (_type, event) =>
         expect(reportEventFailed(event as ReportEvent)).toBe(false),
   );
});
