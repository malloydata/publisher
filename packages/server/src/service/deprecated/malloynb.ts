// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the `.malloynb` notebook format (cells delimited by
 * `>>>markdown` and `>>>malloy`), read but never authored. A `.malloy` report
 * replaces it. The format's own parsing lives here so its removal starts here.
 */

import { MalloySQLParser } from "@malloydata/malloy-sql";
import { NOTEBOOK_FILE_SUFFIX, REPORTS_DOCS_URL } from "../../constants";

/**
 * `PackageWarning.code` of a `.malloynb` code cell the query route refuses for
 * every reader, one per cell, with `reason` naming the rule.
 */
export const MALLOYNB_CELL_REFUSED = "malloynb-cell-refused";

/**
 * `PackageWarning.code` of a warning that names refused cells
 * {@link listMalloynbCellFindings} does not list one by one: one per notebook
 * that lists some of its cells, `model` naming it, and one for the package,
 * with no `model`, for the notebooks past the package's limit.
 */
export const MALLOYNB_CELLS_UNLISTED = "malloynb-cells-unlisted";

/**
 * `PackageWarning.code` of a notebook whose cell check could not run, so a
 * refused cell of it may have no warning.
 */
export const MALLOYNB_CELL_CHECK_FAILED = "malloynb-cell-check-failed";

/**
 * How many refused cells are listed one by one, in one notebook and in the
 * package, and how many notebooks the package summary names.
 */
export const MALLOYNB_CELLS_LISTED = {
   perNotebook: 10,
   perPackage: 50,
   notebooksNamed: 10,
};

/** A refused cell's warning, as the package lint builds it. */
interface CellFinding {
   model?: string;
   code?: string;
   reason?: string;
   severity?: "warn" | "error";
   message?: string;
}

/**
 * The refused-cell warnings to put on the package, bounded so a package with
 * many legacy notebooks does not fill every read of its warnings (an MCP
 * `reload_package` answer carries them all). Each notebook lists its first
 * `perNotebook` refused cells in full, and the package lists at most
 * `perPackage` in all, in notebook order. A notebook that lists some of its
 * cells gets one {@link MALLOYNB_CELLS_UNLISTED} warning naming the rest by
 * number and reason. The notebooks after the package's limit is reached
 * share one such warning for the package, which counts their cells by reason
 * and names the first `notebooksNamed` of them, so the warning count stays
 * bounded however many notebooks there are. `unlisted` counts the cells only
 * a summary names, so a metric can still count every refused cell.
 */
export function listMalloynbCellFindings<F extends CellFinding>(
   notebooks: readonly {
      model: string;
      refusals: readonly { cell: number; finding: F }[];
   }[],
   limits: { perNotebook: number; perPackage: number; notebooksNamed: number },
): { warnings: (F | CellFinding)[]; unlisted: number } {
   const warnings: (F | CellFinding)[] = [];
   let listed = 0;
   let unlisted = 0;
   const past: { model: string; cells: number }[] = [];
   const pastByReason = new Map<string, number>();
   for (const { model, refusals } of notebooks) {
      const shown = Math.max(
         0,
         Math.min(limits.perNotebook, limits.perPackage - listed),
      );
      const kept = refusals.slice(0, shown);
      const rest = refusals.slice(shown);
      warnings.push(...kept.map((r) => r.finding));
      listed += kept.length;
      if (rest.length === 0) continue;
      unlisted += rest.length;
      if (kept.length === 0) {
         past.push({ model, cells: rest.length });
         for (const { finding } of rest) {
            const reason = finding.reason ?? "unknown";
            pastByReason.set(reason, (pastByReason.get(reason) ?? 0) + 1);
         }
         continue;
      }
      const byReason = new Map<string, number[]>();
      for (const { cell, finding } of rest) {
         const reason = finding.reason ?? "unknown";
         const cells = byReason.get(reason);
         if (cells) cells.push(cell);
         else byReason.set(reason, [cell]);
      }
      const groups = Array.from(
         byReason,
         ([reason, cells]) =>
            `${cells.length === 1 ? "cell" : "cells"} ${numberList(cells)} (${reason})`,
      ).join("; ");
      warnings.push({
         model,
         code: MALLOYNB_CELLS_UNLISTED,
         severity: "warn",
         message:
            `${rest.length} more ${rest.length === 1 ? "cell" : "cells"} of ` +
            `${model} won't run for any reader in the Console, not listed ` +
            `one by one: ${groups}. ${FIX_AND_RELOAD}`,
      });
   }
   if (past.length > 0) {
      const cells = past.reduce((sum, notebook) => sum + notebook.cells, 0);
      const named = past
         .slice(0, limits.notebooksNamed)
         .map((notebook) => `${notebook.cells} in ${notebook.model}`);
      const others = past.slice(limits.notebooksNamed);
      if (others.length > 0) {
         const otherCells = others.reduce((sum, n) => sum + n.cells, 0);
         named.push(
            `${otherCells} in ${others.length} other ` +
               `${others.length === 1 ? "notebook" : "notebooks"}`,
         );
      }
      const reasons = Array.from(
         pastByReason,
         ([reason, count]) => `${count} ${reason}`,
      );
      warnings.push({
         code: MALLOYNB_CELLS_UNLISTED,
         severity: "warn",
         message:
            `${cells} more ${cells === 1 ? "cell" : "cells"}, in ` +
            `${past.length} more ` +
            `${past.length === 1 ? "notebook" : "notebooks"}, won't run for ` +
            `any reader in the Console, not listed because the package lists ` +
            `at most ${limits.perPackage} one by one: ${wordList(named)}. ` +
            `By reason: ${wordList(reasons)}. ${FIX_AND_RELOAD}`,
      });
   }
   return { warnings, unlisted };
}

const FIX_AND_RELOAD =
   `The legacy .malloynb section of ${REPORTS_DOCS_URL}#legacy-malloynb-files gives the fix for each ` +
   "reason, and a reload after fixing lists what remains.";

/** `2`, `2 and 3`, `2, 3 and 4`. */
function numberList(numbers: readonly number[]): string {
   return wordList(numbers.map(String));
}

/** `a`, `a and b`, `a, b and c`. */
function wordList(words: readonly string[]): string {
   if (words.length === 1) return words[0];
   return `${words.slice(0, -1).join(", ")} and ${words[words.length - 1]}`;
}

/** Whether a package path is a `.malloynb`. */
export function isMalloynbPath(modelPath: string): boolean {
   return modelPath.endsWith(NOTEBOOK_FILE_SUFFIX);
}

/**
 * Split a `.malloynb` into its statements. The cell parser splits on "\n"
 * alone, so a CRLF file (any Windows checkout) is normalized first; otherwise
 * each cell's text would start with a stray line break.
 */
export function parseMalloynb(
   fileContents: string,
   modelPath: string,
): ReturnType<typeof MalloySQLParser.parse> {
   return MalloySQLParser.parse(fileContents.replace(/\r\n/g, "\n"), modelPath);
}
