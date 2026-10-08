// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   documentRoute,
   documentSlug,
   locateDocument,
} from "./documentLocation";

describe("documentSlug", () => {
   it("reads the slug from either folder", () => {
      expect(documentSlug("notebooks/tour.malloy")).toBe("tour");
      expect(documentSlug("dashboards/tour.malloy")).toBe("tour");
   });

   it("is undefined for anything that is not a document path", () => {
      expect(documentSlug("models/tour.malloy")).toBeUndefined();
      expect(documentSlug("notebooks/a/b.malloy")).toBeUndefined();
      expect(documentSlug("notebooks/tour.malloynb")).toBeUndefined();
      expect(documentSlug(undefined)).toBeUndefined();
   });
});

describe("documentRoute", () => {
   it("encodes the slug and leaves the rest alone", () => {
      expect(documentRoute("env", "pkg", "notebook", "a#b")).toBe(
         "/env/pkg/notebooks/a%23b",
      );
   });
});

describe("locateDocument", () => {
   const dashboards = [
      { name: "overview", path: "dashboards/overview.malloy" },
      { name: "tour", path: "notebooks/tour.malloy" },
   ];
   const notebooks = [
      { path: "notebooks/review.malloy" },
      { path: "dashboards/story.malloy" },
   ];
   const find = (
      kind: "dashboard" | "notebook",
      slug: string,
      withNotebooks = notebooks,
   ) => locateDocument({ kind, slug, dashboards, notebooks: withNotebooks });

   it("finds a document in its own folder", () => {
      expect(find("dashboard", "overview")).toEqual({
         kind: "dashboard",
         path: "dashboards/overview.malloy",
      });
   });

   it("takes the kind from the listing, not the folder or the route", () => {
      expect(find("notebook", "story")).toEqual({
         kind: "notebook",
         path: "dashboards/story.malloy",
      });
      expect(find("notebook", "tour")).toEqual({
         kind: "dashboard",
         path: "notebooks/tour.malloy",
      });
   });

   it("prefers the route's kind when both folders hold the slug", () => {
      const both = [{ path: "notebooks/overview.malloy" }];
      expect(find("notebook", "overview", both)).toEqual({
         kind: "notebook",
         path: "notebooks/overview.malloy",
      });
      expect(find("dashboard", "overview", both)).toEqual({
         kind: "dashboard",
         path: "dashboards/overview.malloy",
      });
   });

   it("is undefined for a document no listing has", () => {
      expect(find("dashboard", "ghost")).toBeUndefined();
   });
});
