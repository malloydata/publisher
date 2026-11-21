// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @deprecated `.malloynb` only; deleted with the format.
 *
 * How a `.malloynb` cell is read: every code cell is handed to the runner,
 * which sends the cell's served `runQuery`, and a cell's code is shown without
 * its `##` lines (the notebook's own annotations), which are not part of the
 * query. A `.malloy` report never reaches here: it is tiles.
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
