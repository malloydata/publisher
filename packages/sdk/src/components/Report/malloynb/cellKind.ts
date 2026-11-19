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
 * What `Report` sends as `query` for a `.malloynb` code cell: from the cell's
 * last `run:` line to its end, with the `#` render tags directly above it.
 * The cell's imports and declarations are left out, because the notebook's own
 * path, which the query runs against, already compiles every cell. Undefined
 * for a cell with nothing to run, which is never sent.
 */
export function malloynbCellQuery(
   cell: Pick<NotebookCell, "text">,
): string | undefined {
   const lines = (cell.text ?? "").split("\n");
   let run = -1;
   lines.forEach((line, i) => {
      if (/^\s*run\s*:/.test(line)) run = i;
   });
   if (run < 0) return undefined;
   let start = run;
   while (start > 0 && /^\s*#(?!#)/.test(lines[start - 1])) start--;
   return lines.slice(start).join("\n").trim();
}
