// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).
import { describe, expect, it } from "bun:test";
import { cellRuns, stripProse } from "./cellKind";

describe("cellRuns", () => {
   it("runs a .malloynb code cell and skips its markdown", () => {
      expect(cellRuns({ type: "code" })).toBe(true);
      expect(cellRuns({ type: "markdown" })).toBe(false);
   });
});

describe("stripProse", () => {
   it("drops the ## lines, as the released viewer did", () => {
      expect(
         stripProse({ text: "## title\n  ## indented\nrun: q\n#(markdown) x" }),
      ).toBe("run: q\n#(markdown) x");
   });

   it("drops a .malloynb ##! line", () => {
      expect(stripProse({ text: "##! experimental.parameters\nrun: q" })).toBe(
         "run: q",
      );
   });

   it("leaves (markdown) lines alone", () => {
      const cell = '#(markdown) note\n#|(markdown)\nbody\n|#\n#" c\nrun: q';
      expect(stripProse({ text: cell })).toBe(cell);
   });
});
