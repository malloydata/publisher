// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as path from "path";
import { pathToFileURL } from "url";
import { declaringFileOf } from "./declaring_file_variant";

describe("declaringFileOf", () => {
   it("redeclares in the source's declaring file, beside its copy", () => {
      // An absolute path on this platform, so on Windows the URL carries a
      // drive: file:///pkg/... names no local file there.
      const declaringURL = pathToFileURL(
         path.resolve("/pkg/models/orders.malloy"),
      );
      const at = declaringFileOf(
         `orders@${declaringURL.href}`,
         ".view-author.malloy",
      );
      expect(at).toEqual({
         name: "orders",
         declaringURL,
         copyURL: new URL(`${declaringURL.href}.view-author.malloy`),
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
});
