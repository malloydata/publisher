// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the `.malloynb` notebook format (cells delimited by
 * `>>>markdown` and `>>>malloy`), read but never authored. A `.malloy` report
 * replaces it. The format's own parsing lives here so its removal starts here.
 */

import { MalloySQLParser } from "@malloydata/malloy-sql";
import { NOTEBOOK_FILE_SUFFIX } from "../../constants";

/**
 * `PackageWarning.code` of a `.malloynb` code cell the query route refuses for
 * every reader, one per cell, with `reason` naming the rule.
 */
export const MALLOYNB_CELL_REFUSED = "malloynb-cell-refused";

/**
 * `PackageWarning.code` of the one warning that names, for a notebook, the
 * refused cells {@link listMalloynbCellFindings} does not list one by one.
 */
export const MALLOYNB_CELLS_UNLISTED = "malloynb-cells-unlisted";

/**
 * `PackageWarning.code` of a notebook whose cell check could not run, so a
 * refused cell of it may have no warning.
 */
export const MALLOYNB_CELL_CHECK_FAILED = "malloynb-cell-check-failed";

/** How many refused cells are listed one by one, in one notebook and in the package. */
export const MALLOYNB_CELLS_LISTED = { perNotebook: 10, perPackage: 50 };

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
 * `perPackage` in all, in notebook order; every notebook with cells past
 * either limit gets one {@link MALLOYNB_CELLS_UNLISTED} warning naming them by
 * number and reason. `unlisted` counts the cells only that warning names, so
 * a metric can still count every refused cell.
 */
export function listMalloynbCellFindings<F extends CellFinding>(
   notebooks: readonly {
      model: string;
      refusals: readonly { cell: number; finding: F }[];
   }[],
   limits: { perNotebook: number; perPackage: number },
): { warnings: (F | CellFinding)[]; unlisted: number } {
   const warnings: (F | CellFinding)[] = [];
   let listed = 0;
   let unlisted = 0;
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
      const byReason = new Map<string, number[]>();
      for (const { cell, finding } of rest) {
         const reason = finding.reason ?? "unknown";
         byReason.set(reason, [...(byReason.get(reason) ?? []), cell]);
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
            `${rest.length} ${kept.length > 0 ? "more " : ""}` +
            `${rest.length === 1 ? "cell" : "cells"} of ${model} won't run ` +
            `for any reader in the Console, not listed one by one: ${groups}. ` +
            `The legacy .malloynb section of docs/reports.md gives the fix ` +
            `for each reason, and a reload after fixing lists what remains.`,
      });
   }
   return { warnings, unlisted };
}

/** `2`, `2 and 3`, `2, 3 and 4`. */
function numberList(numbers: readonly number[]): string {
   if (numbers.length === 1) return String(numbers[0]);
   return `${numbers.slice(0, -1).join(", ")} and ${numbers[numbers.length - 1]}`;
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
