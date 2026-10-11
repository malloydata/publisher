// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** A report's Console routes: to read it, to edit it with `?edit`, and the legacy `/edit` segment. */
import { describe, expect, it } from "bun:test";
import {
   documentPath,
   isEditSearch,
   legacyEditBase,
   withEdit,
   withoutEdit,
} from "./documentRoutes";

describe("documentPath", () => {
   it("is the report's package path without .malloy, plus ?edit to edit", () => {
      expect(documentPath("env", "pkg", "sales/q3.malloy", "view")).toBe(
         "/env/pkg/sales/q3",
      );
      expect(documentPath("env", "pkg", "sales/q3.malloy", "edit")).toBe(
         "/env/pkg/sales/q3?edit",
      );
   });

   it("keeps a .malloynb's suffix, since it opens by path", () => {
      expect(documentPath("env", "pkg", "notes/x.malloynb", "view")).toBe(
         "/env/pkg/notes/x.malloynb",
      );
   });

   it("encodes each path segment, but not the environment or package", () => {
      expect(documentPath("my env", "my pkg", "a b/c#d.malloy", "view")).toBe(
         "/my env/my pkg/a%20b/c%23d",
      );
   });
});

describe("the ?edit parameter", () => {
   it("is read whatever its value, and only by name", () => {
      expect(isEditSearch("?edit")).toBe(true);
      expect(isEditSearch("?region=west&edit=")).toBe(true);
      expect(isEditSearch("?editor=1")).toBe(false);
      expect(isEditSearch("")).toBe(false);
   });

   it("is put first and taken off without touching the rest of the query", () => {
      expect(withEdit("")).toBe("?edit");
      expect(withEdit("?region=west")).toBe("?edit&region=west");
      expect(withEdit("?region=west&edit")).toBe("?edit&region=west");
      expect(withoutEdit("?edit&region=west")).toBe("?region=west");
      expect(withoutEdit("?edit")).toBe("");
   });
});

describe("legacyEditBase", () => {
   it("is the route a trailing /edit segment edits", () => {
      expect(legacyEditBase("sales/q3/edit")).toBe("sales/q3");
      expect(legacyEditBase("sales/q3/edit/")).toBe("sales/q3");
   });

   it("is undefined for a route with no /edit segment, or only one", () => {
      expect(legacyEditBase("sales/q3")).toBeUndefined();
      expect(legacyEditBase("sales/editor")).toBeUndefined();
      expect(legacyEditBase("/edit")).toBeUndefined();
      expect(legacyEditBase("edit")).toBeUndefined();
   });
});
