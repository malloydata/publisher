// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED FEATURE COVERAGE — delete together with the old report
 * vocabulary: `kind` on a compiled document and the Console alias.
 */
import { describe, expect, it } from "bun:test";
import {
   isLegacyReportAlias,
   legacyKindOf,
   withLegacyDocumentKind,
} from "./legacy_report_views";
import type { Package } from "./package";

type Listed = { path: string; name: string; layout: "grid" | "linear" };

/** A package that serves `reports`. */
const fakePackage = (reports: Listed[]) =>
   ({
      servedReportPaths: () => reports.map((r) => r.path).values(),
      isServedReport: (path: string) => reports.some((r) => r.path === path),
   }) as unknown as Package;

describe("legacy report views", () => {
   it("names a layout in the old vocabulary", () => {
      expect(legacyKindOf("grid")).toBe("dashboard");
      expect(legacyKindOf("linear")).toBe("notebook");
   });

   it("reads a dashboards/<name> or notebooks/<name> route as an alias only when a report has the name", () => {
      const pkg = fakePackage([
         { path: "deep/overview.malloy", name: "overview", layout: "grid" },
      ]);
      expect(isLegacyReportAlias(pkg, "dashboards/overview")).toBe(true);
      expect(isLegacyReportAlias(pkg, "notebooks/overview")).toBe(true);
      expect(isLegacyReportAlias(pkg, "dashboards/absent")).toBe(false);
      expect(isLegacyReportAlias(pkg, "dashboards/a/overview")).toBe(false);
      expect(isLegacyReportAlias(pkg, "overview")).toBe(false);
   });

   // The SPA fallback asks this on every candidate request, so it reads the
   // report paths rather than building the listing.
   it("answers the alias without building the report listing", () => {
      const pkg = {
         ...(fakePackage([
            { path: "deep/overview.malloy", name: "overview", layout: "grid" },
         ]) as object),
         listReports: () => {
            throw new Error("listReports called");
         },
      } as unknown as Package;
      expect(isLegacyReportAlias(pkg, "dashboards/overview")).toBe(true);
      expect(isLegacyReportAlias(pkg, "dashboards/absent")).toBe(false);
   });

   it("adds kind to a compiled document and its manifest", () => {
      expect(
         withLegacyDocumentKind({
            layout: "linear",
            cells: [],
            manifest: { name: "x", layout: "linear" } as never,
         }),
      ).toMatchObject({ kind: "notebook", manifest: { kind: "notebook" } });
      expect(withLegacyDocumentKind({ layout: "grid", cells: [] })).toEqual({
         layout: "grid",
         cells: [],
         kind: "dashboard",
      });
   });
});
