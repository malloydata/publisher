// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NotebookCell } from "../../client";

/**
 * Whether the viewer sends a cell to the server to run. A served notebook's
 * definition cell is `type: code` too, so `type` alone would execute it.
 */
export function cellRuns(cell: Pick<NotebookCell, "type" | "kind">): boolean {
   return cell.kind ? cell.kind === "query" : cell.type === "code";
}

type CellLines = Pick<NotebookCell, "text" | "proseLines">;

/** The lines of a cell the server did not name as prose; all of them when it named none. */
function codeLines({ text, proseLines }: CellLines): string[] {
   return (text ?? "")
      .split("\n")
      .filter(
         (_, index) =>
            !proseLines?.some(([start, end]) => index >= start && index <= end),
      );
}

/**
 * A query cell's caption: the server's, or on a cell without one (a `.malloynb`, an older
 * server) the `#"` lines in the tag block above its `run:`.
 */
export function cellCaption(
   cell: Pick<NotebookCell, "text" | "caption" | "proseLines">,
): string | undefined {
   if (cell.caption !== undefined) return cell.caption;
   const lines: string[] = [];
   for (const raw of codeLines(cell)) {
      const line = raw.trim();
      if (line.startsWith("//")) continue;
      if (!line.startsWith("#")) break;
      if (line.startsWith('#"')) lines.push(line.slice(2).trim());
   }
   return lines.length > 0 ? lines.join(" ") : undefined;
}

/**
 * A cell's code without its prose, which renders separately. The server names the prose lines
 * of a served cell; without them (a `.malloynb`, an older server) it drops `##` lines.
 */
export function stripProse(cell: CellLines): string {
   if (cell.proseLines) return codeLines(cell).join("\n");
   return cell.text
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("##"))
      .join("\n");
}

/** One-line label for a folded definition cell: its statement kind and name. */
export function definitionSummary(cell: CellLines): string {
   const line =
      codeLines(cell)
         .map((raw) => raw.trim())
         .find((l) => l && !l.startsWith("#") && !l.startsWith("//")) ?? "";
   const named = /^(source|query|given|type):?\s+([A-Za-z_]\w*)/.exec(line);
   if (named) return `${named[1]}: ${named[2]}`;
   const keyword = /^(import|export)\b/.exec(line);
   return keyword ? keyword[1] : line;
}
