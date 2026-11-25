// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The files a model imports whole: what a chain's declarations may be
// carried across into one text.

import { describe, expect, it } from "bun:test";

import { wholeImportsOf } from "./declaration_text";

const URL_ = "file:///pkg/dir/m.malloy";

describe("wholeImportsOf", () => {
   it("lists each whole import, resolved against the file, and no selective one", () => {
      const text = [
         "##! experimental.persistence",
         'import "a.malloy"',
         'import { b } from "b.malloy"',
         "import '../c.malloy'",
         'source: x is duckdb.table("t") extend { dimension: s is "import \\"d.malloy\\"" }',
      ].join("\n");
      expect(wholeImportsOf(text, URL_)).toEqual([
         "file:///pkg/dir/a.malloy",
         "file:///pkg/c.malloy",
      ]);
   });

   it("is undefined for a path with an escape, which it cannot read", () => {
      expect(wholeImportsOf('import "a\\\\b.malloy"', URL_)).toBeUndefined();
   });
});
