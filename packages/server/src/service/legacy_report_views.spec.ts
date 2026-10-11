// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED FEATURE COVERAGE — delete together with the `…/dashboards*` and
 * `…/notebooks*` routes: the old shapes, projected from the report API.
 */
import { describe, expect, it } from "bun:test";
import {
   AmbiguousReportNameError,
   getDashboard,
   isLegacyReportAlias,
   legacyKindOf,
   listDashboards,
   withLegacyDocumentKind,
   withLegacyNotebookKind,
} from "./legacy_report_views";
import type { Package } from "./package";

type Listed = { path: string; name: string; layout: "grid" | "linear" };

/** A package that serves `reports`, each a manifest whose title is its name. */
const fakePackage = (reports: Listed[]) =>
   ({
      getPackageName: () => "pkg",
      listReports: () =>
         reports.map((r) => ({
            ...r,
            resource: `/api/v0/environments/env/packages/pkg/reports/${r.path}`,
            packageName: "pkg",
            title: r.name,
            editable: true,
         })),
      getReport: async (path: string) => {
         const r = reports.find((report) => report.path === path);
         return (
            r && {
               ...r,
               resource: "",
               packageName: "pkg",
               title: r.name,
               editable: true,
               tiles: [{ kind: "query", query: "s -> v" }],
               autorun: true,
               givens: [],
            }
         );
      },
   }) as unknown as Package;

describe("legacy report views", () => {
   it("names a layout in the old vocabulary", () => {
      expect(legacyKindOf("grid")).toBe("dashboard");
      expect(legacyKindOf("linear")).toBe("notebook");
   });

   it("lists the grid reports, with the old basename URL, encoded", () => {
      const pkg = fakePackage([
         { path: "a/x#1.malloy", name: "x#1", layout: "grid" },
         { path: "story.malloy", name: "story", layout: "linear" },
      ]);
      expect(listDashboards(pkg, "env")).toEqual([
         {
            resource: "/api/v0/environments/env/packages/pkg/dashboards/x%231",
            packageName: "pkg",
            name: "x#1",
            path: "a/x#1.malloy",
            title: "x#1",
            description: undefined,
            error: undefined,
         },
      ]);
   });

   it("resolves a basename one grid report has, with kind, and answers 409 for two", async () => {
      const pkg = fakePackage([
         { path: "a/overview.malloy", name: "overview", layout: "grid" },
         { path: "b/overview.malloy", name: "overview", layout: "grid" },
         { path: "q3.malloy", name: "q3", layout: "grid" },
         { path: "notes/q3.malloy", name: "q3", layout: "linear" },
      ]);
      expect(await getDashboard(pkg, "env", "q3")).toMatchObject({
         path: "q3.malloy",
         kind: "dashboard",
         layout: "grid",
      });
      await expect(getDashboard(pkg, "env", "overview")).rejects.toBeInstanceOf(
         AmbiguousReportNameError,
      );
      expect(await getDashboard(pkg, "env", "absent")).toBeUndefined();
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

   it("adds kind to a compiled document and its manifest, and to a notebook's tile layout", () => {
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
      expect(
         withLegacyNotebookKind({
            dashboard: { layout: "linear" },
         } as never).dashboard,
      ).toMatchObject({ kind: "notebook" });
      expect(withLegacyNotebookKind({} as never)).toEqual({});
   });
});
