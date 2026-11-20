// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @deprecated `.malloynb` only; deleted with the format.
 *
 * How a `.malloynb` cell is read: every code cell runs, and a cell's code is
 * shown without its `##` lines (the notebook's own annotations), which are not
 * part of the query. A `.malloy` report never reaches here: it is tiles.
 */
import type { NotebookCell } from "../../../client";

/** Whether the viewer hands a cell to its runner: every code cell. */
export function cellRuns(cell: Pick<NotebookCell, "type">): boolean {
   return cell.type === "code";
}

/** A cell's code without its `##` lines. */
export function stripProse(cell: Pick<NotebookCell, "text">): string {
   return (cell.text ?? "")
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("##"))
      .join("\n");
}

/**
 * What `Report` sends as `query` for a `.malloynb` code cell: the cell's last
 * `run:` statement, up to the statement after it, with the `#` render tags
 * above it. A blank line or a comment between a tag and the `run:` is kept
 * with them. A `run:` in a comment or a string does not count. The cell's imports and declarations are
 * left out, because the notebook's own path, which the query runs against,
 * already compiles every cell. Undefined for a cell with nothing to run,
 * which is never sent.
 */
export function malloynbCellQuery(
   cell: Pick<NotebookCell, "text">,
): string | undefined {
   const text = cell.text ?? "";
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
