// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as path from "path";
import { pathToFileURL } from "url";
import { overlayKey, overlayKeyOfPath } from "./overlay_key";

describe("overlayKey", () => {
   const root = path.resolve("/tmp/pkg");

   it("gives a file the same key from its path and from the URL the compiler asks for", () => {
      for (const name of [
         "model.malloy",
         "a b.malloy",
         "a%b.malloy",
         "q#1.malloy",
         "x?y.malloy",
      ]) {
         const file = path.join(root, name);
         expect(overlayKey(pathToFileURL(file))).toBe(overlayKeyOfPath(file));
      }
   });

   it("leaves a URL that is not a file's alone", () => {
      expect(overlayKey("https://example.com/m.malloy")).toBe(
         "https://example.com/m.malloy",
      );
   });
});
