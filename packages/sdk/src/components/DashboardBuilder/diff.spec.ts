// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { foldUnchanged, lineDiff } from "./diff";

describe("lineDiff", () => {
   it("reports an inserted and a removed line, and nothing else", () => {
      const before = "a\nb\nc\nd";
      const after = "a\nc\nd\ne";
      expect(lineDiff(before, after)).toEqual([
         { kind: "same", text: "a" },
         { kind: "del", text: "b" },
         { kind: "same", text: "c" },
         { kind: "same", text: "d" },
         { kind: "add", text: "e" },
      ]);
   });

   it("is empty-handed when nothing changed", () => {
      expect(lineDiff("x\ny", "x\ny").every((l) => l.kind === "same")).toBe(
         true,
      );
   });
});

describe("foldUnchanged", () => {
   it("keeps context around each change and folds the rest", () => {
      const lines = lineDiff(
         Array.from({ length: 20 }, (_, i) => `l${i}`).join("\n"),
         Array.from({ length: 20 }, (_, i) =>
            i === 10 ? "changed" : `l${i}`,
         ).join("\n"),
      );
      const folded = foldUnchanged(lines, 2);
      expect(folded[0]).toEqual({ kind: "fold", count: 8 });
      expect(
         folded
            .filter((l) => l.kind !== "fold")
            .map((l) => (l as { text: string }).text),
      ).toEqual(["l8", "l9", "l10", "changed", "l11", "l12"]);
      expect(folded.at(-1)).toEqual({ kind: "fold", count: 7 });
   });
});
