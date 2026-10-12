// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   documentRoute,
   legacyAliasOf,
   locateDocument,
   locateReportByName,
} from "./nameAlias";

const REPORTS = [
   { path: "overview.malloy", layout: "grid" },
   { path: "dashboards/overview.malloy", layout: "grid" },
   { path: "notebooks/overview.malloy", layout: "linear" },
   { path: "sales/q3.malloy", layout: "linear", editable: false },
];

describe("locateReportByName", () => {
   const pair = [
      { path: "dashboards/overview.malloy", layout: "grid" },
      { path: "notebooks/overview.malloy", layout: "linear" },
   ];

   it("prefers the one grid report a dashboard and a notebook share the name with", () => {
      expect(locateReportByName("overview", pair)).toEqual({
         found: {
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

describe("legacyAliasOf", () => {
   it("reads an old by-name route, with the layout its folder preferred", () => {
      expect(legacyAliasOf("dashboards/overview")).toEqual({
         name: "overview",
         prefer: "grid",
      });
      expect(legacyAliasOf("notebooks/story")).toEqual({
         name: "story",
         prefer: "linear",
      });
   });

   it("is undefined for any other route", () => {
      for (const route of [
         "overview",
         "sales/q3",
         "dashboards/a/b",
         "dashboards/",
         "reports/overview",
      ])
         expect(legacyAliasOf(route)).toBeUndefined();
   });
});

describe("documentRoute (deprecated)", () => {
   it("leaves the deprecated folder route as it was", () => {
      expect(documentRoute("env", "pkg", "notebook", "a#b")).toBe(
         "/env/pkg/notebooks/a%23b",
      );
   });
});
