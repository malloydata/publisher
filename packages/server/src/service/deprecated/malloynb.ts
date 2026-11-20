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

/**
 * What the SDK's `Report` sends as `query` for a `.malloynb` code cell, so the
 * load-time cell lint checks the text a reader's request carries: the cell's
 * last `run:` statement, up to the statement after it, with the `#` render
 * tags above it. A copy of the SDK's `malloynbCellQuery`
 * (`packages/sdk/src/components/Report/malloynb/cellKind.ts`): this file's
 * spec fails unless everything from the function's first statement to the
 * end of the file is the same text as the SDK's, and both specs read one
 * case table, `cellQueryParity.json` beside the SDK's. Undefined for a cell
 * with nothing to run, which is never sent.
 */
export function malloynbCellQuery(text: string): string | undefined {
   const tokens = topLevelTokens(text);
   let runIndex = -1;
   tokens.forEach((token, i) => {
      if (token.kind === "statement" && token.run) runIndex = i;
   });
   if (runIndex < 0) return undefined;
   const run = tokens[runIndex];
   let end = run.end;
   for (const token of tokens.slice(runIndex + 1)) {
      if (token.kind !== "code") break;
      end = token.end;
   }
   let start = run.at;
   for (let i = runIndex - 1; i >= 0; i--) {
      const token = tokens[i];
      if (token.kind !== "tag" || token.notebook) break;
      start = token.at;
   }
   return text.slice(start, end).trim();
}

/**
 * A cell's text as the pieces {@link malloynbCellQuery} reads, in order, with
 * whitespace and comments left out: the start of a top-level statement, such
 * as `run:`, `source:` or `import`; a `;` between statements; a top-level `#`
 * line, marked `notebook` when it is a `##` line; and code. Strings, and
 * anything inside brackets, are code.
 */
type CellToken =
   | { kind: "statement"; at: number; end: number; run: boolean }
   | { kind: "tag"; at: number; end: number; notebook: boolean }
   | { kind: "separator"; at: number; end: number }
   | { kind: "code"; at: number; end: number };

const STATEMENT =
   /(?:(run|source|query|given|sql|type)\s*:|(?:import|export)(?![A-Za-z0-9_]))/iy;
const IDENTIFIER_CHAR = /[A-Za-z0-9_]/;

function topLevelTokens(text: string): CellToken[] {
   const tokens: CellToken[] = [];
   const lineEnd = (from: number) => {
      const newline = text.indexOf("\n", from);
      return newline < 0 ? text.length : newline;
   };
   let depth = 0;
   let i = 0;
   while (i < text.length) {
      const c = text[i];
      const pair = text.slice(i, i + 2);
      if (/\s/.test(c)) {
         i++;
      } else if (pair === "//" || pair === "--") {
         i = lineEnd(i);
      } else if (pair === "/*") {
         const close = text.indexOf("*/", i + 2);
         i = close < 0 ? text.length : close + 2;
      } else if (c === "#") {
         const end = lineEnd(i);
         tokens.push(
            depth === 0
               ? { kind: "tag", at: i, end, notebook: text[i + 1] === "#" }
               : { kind: "code", at: i, end },
         );
         i = end;
      } else if (c === "'" || c === '"' || c === "`") {
         const end = stringEnd(text, i);
         tokens.push({ kind: "code", at: i, end });
         i = end;
      } else if (c === ";" && depth === 0) {
         tokens.push({ kind: "separator", at: i, end: i + 1 });
         i++;
      } else {
         if (depth === 0 && (i === 0 || !IDENTIFIER_CHAR.test(text[i - 1]))) {
            STATEMENT.lastIndex = i;
            const match = STATEMENT.exec(text);
            if (match) {
               const end = i + match[0].length;
               tokens.push({
                  kind: "statement",
                  at: i,
                  end,
                  run: match[1]?.toLowerCase() === "run",
               });
               i = end;
               continue;
            }
         }
         if ("{([".includes(c)) depth++;
         else if ("})]".includes(c)) depth = Math.max(0, depth - 1);
         tokens.push({ kind: "code", at: i, end: i + 1 });
         i++;
      }
   }
   return tokens;
}

/** The index just past the string that opens at `from`; an unclosed one ends with its line. */
function stringEnd(text: string, from: number): number {
   if (text.startsWith('"""', from)) {
      const close = text.indexOf('"""', from + 3);
      return close < 0 ? text.length : close + 3;
   }
   const quote = text[from];
   for (let i = from + 1; i < text.length; i++) {
      if (text[i] === "\\") i++;
      else if (text[i] === quote) return i + 1;
      else if (text[i] === "\n") return i;
   }
   return text.length;
}
