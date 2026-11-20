// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE: delete with `.malloynb` notebooks.

import { describe, expect, it } from "bun:test";
import {
   listMalloynbCellFindings,
   MALLOYNB_CELL_REFUSED,
   MALLOYNB_CELLS_UNLISTED,
} from "./malloynb";

describe("listMalloynbCellFindings: bounded, nothing lost", () => {
   /** `n` refused cells of `notebook`, numbered from 2, alternating reasons. */
   const refused = (notebook: string, n: number) =>
      Array.from({ length: n }, (_, k) => ({
         cell: k + 2,
         finding: {
            model: notebook,
            code: MALLOYNB_CELL_REFUSED,
            reason: k % 2 === 0 ? "restricted_mode" : "off_surface",
            severity: "warn" as const,
            message: `Cell ${k + 2} of ${notebook} won't run`,
         },
      }));

   it("lists a notebook's cells in full up to the per-notebook limit, and the rest in one warning", () => {
      const { warnings, unlisted } = listMalloynbCellFindings(
         [{ model: "a.malloynb", refusals: refused("a.malloynb", 8) }],
         { perNotebook: 5, perPackage: 50 },
      );
      expect(warnings.map((w) => w.code)).toEqual([
         ...Array(5).fill(MALLOYNB_CELL_REFUSED),
         MALLOYNB_CELLS_UNLISTED,
      ]);
      expect(unlisted).toBe(3);
      expect(warnings[5]).toEqual({
         model: "a.malloynb",
         code: MALLOYNB_CELLS_UNLISTED,
         severity: "warn",
         message:
            "3 more cells of a.malloynb won't run for any reader in the " +
            "Console, not listed one by one: cells 7 and 9 (off_surface); " +
            "cell 8 (restricted_mode). The legacy .malloynb section of " +
            "docs/reports.md gives the fix for each reason, and a reload " +
            "after fixing lists what remains.",
      });
   });

   it("stops listing in full at the per-package limit, and every notebook past it gets its summary", () => {
      const { warnings, unlisted } = listMalloynbCellFindings(
         [
            { model: "a.malloynb", refusals: refused("a.malloynb", 4) },
            { model: "b.malloynb", refusals: refused("b.malloynb", 4) },
            { model: "c.malloynb", refusals: refused("c.malloynb", 2) },
            { model: "d.malloynb", refusals: [] },
         ],
         { perNotebook: 3, perPackage: 5 },
      );
      expect(warnings.map((w) => [w.model, w.code])).toEqual([
         ["a.malloynb", MALLOYNB_CELL_REFUSED],
         ["a.malloynb", MALLOYNB_CELL_REFUSED],
         ["a.malloynb", MALLOYNB_CELL_REFUSED],
         ["a.malloynb", MALLOYNB_CELLS_UNLISTED],
         ["b.malloynb", MALLOYNB_CELL_REFUSED],
         ["b.malloynb", MALLOYNB_CELL_REFUSED],
         ["b.malloynb", MALLOYNB_CELLS_UNLISTED],
         ["c.malloynb", MALLOYNB_CELLS_UNLISTED],
      ]);
      // Every refused cell is either listed or named in a summary.
      expect(unlisted).toBe(1 + 2 + 2);
   });

   it("lists everything when nothing passes a limit", () => {
      const { warnings, unlisted } = listMalloynbCellFindings(
         [{ model: "a.malloynb", refusals: refused("a.malloynb", 2) }],
         { perNotebook: 5, perPackage: 50 },
      );
      expect(warnings).toHaveLength(2);
      expect(unlisted).toBe(0);
   });
});
