// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as path from "path";
import { pathToFileURL } from "url";
import { declaringFileOf } from "./declaring_file_variant";

describe("declaringFileOf", () => {
   it("redeclares in the source's declaring file, beside its copy", () => {
      // An absolute path on this platform: `file:///pkg/…` has no drive, so
      // Windows reads it as no local file at all.
      const file = path.resolve("/pkg/models/orders.malloy");
      const at = declaringFileOf(
         `orders@${pathToFileURL(file).href}`,
         ".view-author.malloy",
      );
      expect(at).toEqual({
         name: "orders",
         declaringURL: pathToFileURL(file),
         copyURL: pathToFileURL(`${file}.view-author.malloy`),
         copyName: "orders.malloy.view-author.malloy",
      });
   });

   it("has no declaring file for a sourceID without a URL", () => {
      expect(declaringFileOf("orders", ".copy")).toEqual({
         error: "no declaring file",
      });
   });

   it("has none for a declaring file that is not local", () => {
      expect(
         declaringFileOf("orders@https://example.com/orders.malloy", ".copy"),
      ).toEqual({ error: "not a local file" });
   });

   it("keeps a declaring file's spaces through its URL and the copy's name", () => {
      const file = path.resolve("/pkg/my models/the orders.malloy");
      const at = declaringFileOf(
         `orders@${pathToFileURL(file).href}`,
         ".rollup-author.malloy",
      );
      expect(at).toEqual({
         name: "orders",
         declaringURL: pathToFileURL(file),
         copyURL: pathToFileURL(`${file}.rollup-author.malloy`),
         copyName: "the orders.malloy.rollup-author.malloy",
      });
   });

   it("reads the name before the URL's scheme, a quoted name with a dot included", () => {
      const file = path.resolve("/pkg/m.malloy");
      const at = declaringFileOf(`a.b@${pathToFileURL(file).href}`, ".copy");
      expect("name" in at && at.name).toBe("a.b");
   });
});
