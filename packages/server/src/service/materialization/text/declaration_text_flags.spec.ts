// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A file's `##!` flags, carried into generated text.

import { describe, expect, it } from "bun:test";

import { documentFlagLines } from "./declaration_text";

describe("documentFlagLines", () => {
   it("returns the ##! lines in order and nothing else", () => {
      const text =
         "// header\n##! experimental.persistence\n\n##! experimental { givens, access_modifiers }\n## materialization.freshness.window = 1h\nsource: a is b\n";
      expect(documentFlagLines(text)).toEqual([
         "##! experimental.persistence",
         "##! experimental { givens, access_modifiers }",
      ]);
   });
});
