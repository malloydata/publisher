// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   malloyIdentifier,
   namedQueryRunQuery,
   viewRunQuery,
} from "./malloyRunText";

describe("malloyIdentifier", () => {
   it("backtick-quotes every identifier, so a reserved word is as safe as any other name", () => {
      expect(malloyIdentifier("top_carriers")).toBe("`top_carriers`");
      expect(malloyIdentifier("year")).toBe("`year`");
      expect(malloyIdentifier("source")).toBe("`source`");
   });

   it("quotes names no bare identifier could spell", () => {
      expect(malloyIdentifier("order-items")).toBe("`order-items`");
      expect(malloyIdentifier("1st")).toBe("`1st`");
      expect(malloyIdentifier("with space")).toBe("`with space`");
   });

   it("escapes backslashes and backticks inside the quotes, backslashes first", () => {
      expect(malloyIdentifier("a`b")).toBe("`a\\`b`");
      expect(malloyIdentifier("a\\b")).toBe("`a\\\\b`");
      // A trailing backslash must not escape the closing backtick.
      expect(malloyIdentifier("a\\")).toBe("`a\\\\`");
      expect(malloyIdentifier("a\\`")).toBe("`a\\\\\\``");
   });
});

describe("run text", () => {
   it("runs a view by source and name", () => {
      expect(viewRunQuery("flights", "top_carriers")).toBe(
         "run: `flights` -> `top_carriers`",
      );
      expect(viewRunQuery("order-items", "by carrier")).toBe(
         "run: `order-items` -> `by carrier`",
      );
   });

   it("runs a named query by name", () => {
      expect(namedQueryRunQuery("genre_movie_map")).toBe(
         "run: `genre_movie_map`",
      );
   });
});
