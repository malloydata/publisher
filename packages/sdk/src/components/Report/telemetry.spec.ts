// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, mock } from "bun:test";
import {
   failureCategory,
   reportEmitter,
   withReportContext,
   type ReportEvent,
   type ReportFailure,
} from "./telemetry";

describe("failureCategory", () => {
   it.each([
      [{ status: 404 }, "not_found"],
      [{ status: 401 }, "forbidden"],
      [{ response: { status: 403 } }, "forbidden"],
      [{ status: 400 }, "invalid"],
      [{ response: { status: 424 } }, "invalid"],
      [{ status: 501 }, "unsupported"],
      [{ response: { status: 503 } }, "server"],
      [{ code: "ERR_NETWORK" }, "network"],
      [new Error("anything"), "unknown"],
      [undefined, "unknown"],
      ["a string", "unknown"],
   ] as [unknown, ReportFailure][])("reads %j as %s", (error, category) => {
      expect(failureCategory(error)).toBe(category);
   });
});

describe("reportEmitter", () => {
   it("stamps each event with the report's path and layout", () => {
      const onEvent = mock((_event: ReportEvent) => {});
      const { emit, listening } = reportEmitter(
         { path: "sales/q3.malloy", layout: "grid" },
         onEvent,
      );
      expect(listening).toBe(true);
      emit({ type: "report.save_refused", reason: "x" });
      expect(onEvent.mock.calls[0][0]).toEqual({
         type: "report.save_refused",
         reason: "x",
         path: "sales/q3.malloy",
         layout: "grid",
      });
   });

   it("is silent, and says so, when no host listens", () => {
      const { emit, listening } = reportEmitter(
         { path: "x.malloy" },
         undefined,
      );
      expect(listening).toBe(false);
      expect(() =>
         emit({
            type: "report.load_failed",
            surface: "view",
            category: "unknown",
         }),
      ).not.toThrow();
   });

   it("never lets a sink's own failure reach the surface", () => {
      const { emit } = reportEmitter(
         { path: "x.malloy", layout: "linear" },
         () => {
            throw new Error("sink down");
         },
      );
      expect(() =>
         emit({ type: "report.explored", tile: "orders -> by_month" }),
      ).not.toThrow();
   });
});

describe("withReportContext", () => {
   it("leaves layout off an event whose report never said", () => {
      expect(
         withReportContext(
            { type: "report.load_failed", surface: "view", category: "server" },
            { path: "x.malloy" },
         ),
      ).toEqual({
         type: "report.load_failed",
         surface: "view",
         category: "server",
         path: "x.malloy",
      });
   });
});
