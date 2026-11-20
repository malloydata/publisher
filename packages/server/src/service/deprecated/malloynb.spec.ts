// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE: delete with `.malloynb` notebooks.

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   listMalloynbCellFindings,
   MALLOYNB_CELL_REFUSED,
   MALLOYNB_CELLS_UNLISTED,
   malloynbCellQuery,
} from "./malloynb";

// One case table for this copy and the SDK's `malloynbCellQuery`, whose spec
// reads the same file: the load-time lint must check the text Report sends.
const TABLE = path.resolve(
   __dirname,
   "../../../../sdk/src/components/Report/malloynb/cellQueryParity.json",
);
const SDK_SOURCE = path.resolve(
   __dirname,
   "../../../../sdk/src/components/Report/malloynb/cellKind.ts",
);

/** A file's text from the function's first statement to its end: the body and the tokenizer it calls. */
const bodyOf = (file: string) => {
   const text = fs.readFileSync(file, "utf8");
   const start = text.indexOf("   const tokens = topLevelTokens(text);");
   expect(start).toBeGreaterThan(0);
   return text.slice(start);
};
const { cases } = JSON.parse(fs.readFileSync(TABLE, "utf8")) as {
   cases: { name: string; text: string; query: string | null }[];
};

describe("malloynbCellQuery, as the SDK's Report sends a cell", () => {
   it("is the SDK's function, statement for statement", () => {
      // Only the signature differs: the SDK takes a cell, this takes its text.
      expect(bodyOf(path.resolve(__dirname, "malloynb.ts"))).toBe(
         bodyOf(SDK_SOURCE),
      );
   });

   it("differs from it only in the signature: the SDK reads the cell's text and nothing else first", () => {
      const preamble = (file: string) => {
         const text = fs.readFileSync(file, "utf8");
         return text.slice(
            text.indexOf("export function malloynbCellQuery("),
            text.indexOf("   const tokens = topLevelTokens(text);"),
         );
      };
      expect(preamble(SDK_SOURCE)).toBe(
         "export function malloynbCellQuery(\n" +
            '   cell: Pick<NotebookCell, "text">,\n' +
            "): string | undefined {\n" +
            '   const text = cell.text ?? "";\n',
      );
      expect(preamble(path.resolve(__dirname, "malloynb.ts"))).toBe(
         "export function malloynbCellQuery(text: string): string | undefined {\n",
      );
   });

   it("reads a table with cases in it", () => {
      expect(cases.length).toBeGreaterThan(10);
   });
   for (const { name, text, query } of cases) {
      it(name, () => {
         expect(malloynbCellQuery(text) ?? null).toBe(query);
      });
   }
});

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
            "cell 8 (restricted_mode). Each rule's fix is in the legacy .malloynb " +
            "section of docs/reports.md, and the warnings listed for this " +
            "package show it for cells like these; a reload after fixing " +
            "lists what remains.",
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
