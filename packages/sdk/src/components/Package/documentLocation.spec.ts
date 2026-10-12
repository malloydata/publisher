// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   documentSlug,
   locateReport,
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

   it("encodes the environment and package too, so no caller encodes again", () => {
      expect(reportRoute("my env", "a#b", "q3.malloy")).toBe(
         "/my%20env/a%23b/q3",
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
         });
      }
   });

   it("reads a bare name at the root path first, with no fallback", () => {
      const target = resolveDrillTarget("overview", REPORTS);
      expect(target.kind).toBe("path");
      expect("report" in target && target.report.path).toBe("overview.malloy");
   });

   it("reads a bare name as a root file only: a report elsewhere with that basename is not looked up", () => {
      expect(resolveDrillTarget("overview", REPORTS.slice(1))).toEqual({
         kind: "unresolved",
      });
   });

   it("is unresolved for a path nothing is at", () => {
      expect(resolveDrillTarget("x/q3", REPORTS)).toEqual({
         kind: "unresolved",
      });
   });
});

/** The SDK's column of the drill case table the server's `resolveDrillDestination` shares. */
describe("resolveDrillTarget: the shared drill case table", () => {
   type Answer = { kind: string; path?: string; malformed?: boolean };
   const { cases } = JSON.parse(
      fs.readFileSync(
         path.resolve(
            import.meta.dir,
            "../ReportBuilder/testing/drillResolution.json",
         ),
         "utf8",
      ),
   ) as {
      cases: {
         name: string;
         to: string;
         reports: { path: string; layout: string; dropped?: boolean }[];
         server: Answer;
         /** What the SDK answers where it differs from the server by design; `knownDifference` says why. */
         sdk?: Answer;
         knownDifference?: string;
      }[];
   };
   for (const testCase of cases) {
      it(testCase.name, () => {
         // The listing: a report dropped at discovery is not in it.
         const listed = testCase.reports.filter((r) => !r.dropped);
         const target = resolveDrillTarget(testCase.to, listed);
         const answer: Answer =
            target.kind === "unresolved"
               ? { kind: target.kind }
               : { kind: target.kind, path: target.report.path };
         const { malformed: _malformed, ...server } = testCase.server;
         expect(testCase.sdk === undefined).toBe(
            testCase.knownDifference === undefined,
         );
         expect(answer).toEqual(testCase.sdk ?? server);
      });
   }
});
