// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   documentRoute,
   documentSlug,
   locateDocument,
   locateReport,
   locateReportByName,
   reportEditRoute,
   reportRoute,
   reportRoutePath,
   resolveDrillTarget,
} from "./documentLocation";

describe("documentSlug", () => {
   it("is the basename of any path, at any depth", () => {
      expect(documentSlug("tour.malloy")).toBe("tour");
      expect(documentSlug("notebooks/tour.malloy")).toBe("tour");
      expect(documentSlug("dashboards/tour.malloy")).toBe("tour");
      expect(documentSlug("sales/2024/q3.malloy")).toBe("q3");
   });

   it("drops a .malloynb suffix too", () => {
      expect(documentSlug("README.malloynb")).toBe("README");
      expect(documentSlug("old/orders.malloynb")).toBe("orders");
   });

   it("is undefined for no path", () => {
      expect(documentSlug(undefined)).toBeUndefined();
      expect(documentSlug("")).toBeUndefined();
   });
});

describe("reportRoute", () => {
   it("is the package path without .malloy, each segment encoded", () => {
      expect(reportRoute("env", "pkg", "sales/q3.malloy")).toBe(
         "/env/pkg/sales/q3",
      );
      expect(reportRoutePath("a#b/c d.malloy")).toBe("a%23b/c%20d");
   });

   it("edits with ?edit, so a report named edit keeps its own route", () => {
      expect(reportEditRoute("env", "pkg", "sales/q3.malloy")).toBe(
         "/env/pkg/sales/q3?edit",
      );
      expect(reportRoute("env", "pkg", "sales/edit.malloy")).toBe(
         "/env/pkg/sales/edit",
      );
      expect(reportEditRoute("env", "pkg", "sales/edit.malloy")).toBe(
         "/env/pkg/sales/edit?edit",
      );
   });

   it("keeps a .malloynb suffix, which opens by path", () => {
      expect(reportRoutePath("old.malloynb")).toBe("old.malloynb");
   });

   it("leaves the deprecated folder route as it was", () => {
      expect(documentRoute("env", "pkg", "notebook", "a#b")).toBe(
         "/env/pkg/notebooks/a%23b",
      );
   });
});

const REPORTS = [
   { path: "overview.malloy", layout: "grid" },
   { path: "dashboards/overview.malloy", layout: "grid" },
   { path: "notebooks/overview.malloy", layout: "linear" },
   { path: "sales/q3.malloy", layout: "linear", editable: false },
];

describe("locateReport", () => {
   it("matches the exact path only", () => {
      expect(locateReport("sales/q3.malloy", REPORTS)).toEqual({
         kind: "notebook",
         layout: "linear",
         path: "sales/q3.malloy",
         editable: false,
      });
      expect(locateReport("q3.malloy", REPORTS)).toBeUndefined();
      expect(locateReport("sales/q3", REPORTS)).toBeUndefined();
   });

   it("reads the layout from the listing, never the directory", () => {
      expect(
         locateReport("notebooks/x.malloy", [
            { path: "notebooks/x.malloy", layout: "grid" },
         ])?.layout,
      ).toBe("grid");
   });
});

describe("locateReportByName", () => {
   const pair = [
      { path: "dashboards/overview.malloy", layout: "grid" },
      { path: "notebooks/overview.malloy", layout: "linear" },
   ];

   it("prefers the one grid report a dashboard and a notebook share the name with", () => {
      expect(locateReportByName("overview", pair)).toEqual({
         found: {
            kind: "dashboard",
            layout: "grid",
            path: "dashboards/overview.malloy",
            editable: true,
         },
      });
   });

   it("prefers linear for the notebooks alias", () => {
      const found = locateReportByName("overview", pair, "linear");
      expect("found" in found && found.found.path).toBe(
         "notebooks/overview.malloy",
      );
   });

   it("takes a unique report of the other layout when none of the preferred one has it", () => {
      const found = locateReportByName("q3", REPORTS, "grid");
      expect("found" in found && found.found.path).toBe("sales/q3.malloy");
   });

   it("reports ambiguity, naming the paths sorted, rather than taking the first", () => {
      expect(
         locateReportByName("overview", [
            { path: "z/overview.malloy", layout: "grid" },
            { path: "a/overview.malloy", layout: "grid" },
         ]),
      ).toEqual({ ambiguous: ["a/overview.malloy", "z/overview.malloy"] });
   });

   it("says when nothing has the name", () => {
      expect(locateReportByName("absent", REPORTS)).toEqual({ missing: true });
   });
});

describe("resolveDrillTarget", () => {
   it("reads a target as a package path from the root", () => {
      const target = resolveDrillTarget("sales/q3", REPORTS);
      expect(target.kind).toBe("path");
      expect("report" in target && target.report.path).toBe("sales/q3.malloy");
   });

   it("refuses a target that is not a plain package path: . or .. segments, empty segments, a leading /", () => {
      for (const to of [
         "../../x",
         "../sales/q3",
         "sales/../q3",
         "./sales/q3",
         "/sales/q3",
         "sales//q3",
         "sales/q3/",
         ".",
         "..",
         "",
      ]) {
         expect(resolveDrillTarget(to, REPORTS)).toEqual({
            kind: "unresolved",
            candidates: [],
         });
      }
   });

   it("reads a bare name at the root path first, with no fallback", () => {
      const target = resolveDrillTarget("overview", REPORTS);
      expect(target.kind).toBe("path");
      expect("report" in target && target.report.path).toBe("overview.malloy");
   });

   it("falls back to a basename only when nothing is at the root path, preferring grid", () => {
      const target = resolveDrillTarget("overview", REPORTS.slice(1));
      expect(target.kind).toBe("name");
      expect("report" in target && target.report.path).toBe(
         "dashboards/overview.malloy",
      );
   });

   it("never falls back for a target with a /", () => {
      expect(resolveDrillTarget("x/q3", REPORTS)).toEqual({
         kind: "unresolved",
         candidates: [],
      });
   });

   it("names the candidates when the basename is ambiguous", () => {
      expect(
         resolveDrillTarget("overview", [
            { path: "a/overview.malloy", layout: "linear" },
            { path: "b/overview.malloy", layout: "linear" },
         ]),
      ).toEqual({
         kind: "unresolved",
         candidates: ["a/overview.malloy", "b/overview.malloy"],
      });
   });

   it("is unresolved with no candidates when nothing has the name", () => {
      expect(resolveDrillTarget("absent", REPORTS)).toEqual({
         kind: "unresolved",
         candidates: [],
      });
   });
});

describe("locateDocument (deprecated)", () => {
   it("finds a report by the name a folder route carries, preferring the route's layout", () => {
      const pair = [
         { path: "dashboards/story.malloy", layout: "grid" },
         { path: "notebooks/story.malloy", layout: "linear" },
      ];
      expect(
         locateDocument({ kind: "notebook", slug: "story", reports: pair })
            ?.path,
      ).toBe("notebooks/story.malloy");
      expect(
         locateDocument({ kind: "dashboard", slug: "story", reports: pair })
            ?.path,
      ).toBe("dashboards/story.malloy");
   });

   it("is undefined for a name no listing has, or one it cannot tell apart", () => {
      expect(
         locateDocument({ kind: "dashboard", slug: "x", reports: REPORTS }),
      ).toBeUndefined();
      expect(
         locateDocument({
            kind: "dashboard",
            slug: "x",
            reports: [
               { path: "a/x.malloy", layout: "linear" },
               { path: "b/x.malloy", layout: "linear" },
            ],
         }),
      ).toBeUndefined();
   });
});
