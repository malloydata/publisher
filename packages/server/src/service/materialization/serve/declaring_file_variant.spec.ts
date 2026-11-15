// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { declaringFileOf } from "./declaring_file_variant";

describe("declaringFileOf", () => {
   it("redeclares in the source's declaring file, beside its copy", () => {
      const at = declaringFileOf(
         "orders@file:///pkg/models/orders.malloy",
         ".view-author.malloy",
      );
      expect(at).toEqual({
         name: "orders",
         declaringURL: new URL("file:///pkg/models/orders.malloy"),
         copyURL: new URL(
            "file:///pkg/models/orders.malloy.view-author.malloy",
         ),
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
