// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { documentPath, editPathOf, splitEdit } from "./documentRoutes";

describe("splitEdit", () => {
   it("takes a trailing /edit segment off", () => {
      expect(splitEdit("sales/q3/edit")).toEqual({
         path: "sales/q3",
         edit: true,
      });
      expect(splitEdit("sales/q3/edit/")).toEqual({
         path: "sales/q3",
         edit: true,
      });
   });

   it("leaves a path with no edit segment, minus a trailing slash", () => {
      expect(splitEdit("sales/q3")).toEqual({ path: "sales/q3", edit: false });
      expect(splitEdit("sales/q3/")).toEqual({ path: "sales/q3", edit: false });
      // A segment that only starts with `edit` is not one.
      expect(splitEdit("sales/editor")).toEqual({
         path: "sales/editor",
         edit: false,
      });
   });

   it("reads a report named `edit` at the root as the edit route of nothing", () => {
      // Pinned as it stands: `/edit` alone is the segment, so the path is "".
      expect(splitEdit("/edit")).toEqual({ path: "", edit: true });
      expect(splitEdit("edit")).toEqual({ path: "edit", edit: false });
   });
});

describe("documentPath", () => {
   it("is the report's package path without .malloy, plus /edit to edit", () => {
      expect(documentPath("env", "pkg", "sales/q3.malloy", "view")).toBe(
         "/env/pkg/sales/q3",
      );
      expect(documentPath("env", "pkg", "sales/q3.malloy", "edit")).toBe(
         "/env/pkg/sales/q3/edit",
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

describe("editPathOf", () => {
   it("appends the edit segment to a read route", () => {
      expect(editPathOf("/env/pkg/sales/q3")).toBe("/env/pkg/sales/q3/edit");
   });
});
