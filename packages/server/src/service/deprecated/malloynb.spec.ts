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
         { perNotebook: 5, perPackage: 50, notebooksNamed: 10 },
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

   it("stops listing in full at the per-package limit, and folds the notebooks past it into one package summary", () => {
      const { warnings, unlisted } = listMalloynbCellFindings(
         [
            { model: "a.malloynb", refusals: refused("a.malloynb", 4) },
            { model: "b.malloynb", refusals: refused("b.malloynb", 4) },
            { model: "c.malloynb", refusals: refused("c.malloynb", 2) },
            { model: "d.malloynb", refusals: [] },
            { model: "e.malloynb", refusals: refused("e.malloynb", 3) },
         ],
         { perNotebook: 3, perPackage: 5, notebooksNamed: 10 },
      );
      expect(warnings.map((w) => [w.model, w.code])).toEqual([
         ["a.malloynb", MALLOYNB_CELL_REFUSED],
         ["a.malloynb", MALLOYNB_CELL_REFUSED],
         ["a.malloynb", MALLOYNB_CELL_REFUSED],
         ["a.malloynb", MALLOYNB_CELLS_UNLISTED],
         ["b.malloynb", MALLOYNB_CELL_REFUSED],
         ["b.malloynb", MALLOYNB_CELL_REFUSED],
         ["b.malloynb", MALLOYNB_CELLS_UNLISTED],
         [undefined, MALLOYNB_CELLS_UNLISTED],
      ]);
      expect(warnings[7]).toEqual({
         code: MALLOYNB_CELLS_UNLISTED,
         severity: "warn",
         message:
            "5 more cells, in 2 more notebooks, won't run for any reader in " +
            "the Console, not listed because the package lists at most 5 " +
            "one by one: 2 in c.malloynb and 3 in e.malloynb. By reason: 3 " +
            "restricted_mode and 2 off_surface. The legacy .malloynb section " +
            "of docs/reports.md gives the fix for each reason, and a reload " +
            "after fixing lists what remains.",
      });
      // Every refused cell is either listed or named in a summary.
      expect(unlisted).toBe(1 + 2 + 2 + 3);
   });

   it("keeps one package summary however many notebooks pass the per-package limit", () => {
      const notebooks = Array.from({ length: 300 }, (_, n) => ({
         model: `n${n}.malloynb`,
         refusals: refused(`n${n}.malloynb`, 1),
      }));
      const { warnings, unlisted } = listMalloynbCellFindings(notebooks, {
         perNotebook: 10,
         perPackage: 50,
         notebooksNamed: 3,
      });
      expect(warnings).toHaveLength(51);
      expect(
         warnings.filter((w) => w.code === MALLOYNB_CELL_REFUSED),
      ).toHaveLength(50);
      expect(warnings[50].model).toBeUndefined();
      expect(warnings[50].message).toStartWith(
         "250 more cells, in 250 more notebooks, won't run for any reader " +
            "in the Console, not listed because the package lists at most " +
            "50 one by one: 1 in n50.malloynb, 1 in n51.malloynb, 1 in " +
            "n52.malloynb and 247 in 247 other notebooks. By reason: 250 " +
            "restricted_mode.",
      );
      expect(unlisted).toBe(250);
   });

   it("lists everything when nothing passes a limit", () => {
      const { warnings, unlisted } = listMalloynbCellFindings(
         [{ model: "a.malloynb", refusals: refused("a.malloynb", 2) }],
         { perNotebook: 5, perPackage: 50, notebooksNamed: 10 },
      );
      expect(warnings).toHaveLength(2);
      expect(unlisted).toBe(0);
   });
});
