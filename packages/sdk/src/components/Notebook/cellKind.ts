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

const MARKDOWN_BLOCK_OPEN = "#|(markdown)";
const MARKDOWN_LINE = "#(markdown)";

/**
 * Calls `visit` on each line outside prose: a `#(markdown)` line, a
 * `#|(markdown)` ... `|#` block, and `##` lines and `##|` ... `|##` blocks are
 * skipped, so a block's body lines are never mistaken for tags or code.
 * `visit` returns false to stop.
 */
function forEachNonProseLine(
   text: string,
   visit: (raw: string, trimmed: string) => boolean | void,
): void {
   const lines = text.split("\n");
   // A closer is a line that starts with it; an opener with none is not a block.
   const closerAfter = (from: number, closer: string) => {
      for (let j = from + 1; j < lines.length; j++)
         if (lines[j].trim().startsWith(closer)) return j;
      return -1;
   };
   for (let i = 0; i < lines.length; i++) {
      const raw = lines[i];
      const line = raw.trim();
      if (line.startsWith("##|")) {
         const end = closerAfter(i, "|##");
         if (end >= 0) i = end;
         continue;
      }
      if (line.startsWith("##")) continue;
      if (line.startsWith(MARKDOWN_BLOCK_OPEN)) {
         const end = closerAfter(i, "|#");
         if (end >= 0) {
            i = end;
            continue;
         }
      }
      if (line.startsWith(MARKDOWN_LINE)) continue;
      if (visit(raw, line) === false) return;
   }
}

/**
 * A query cell's caption: the `#"` lines in the tag block above its `run:`.
 * The API carries no caption field, so the cell text is the source of truth.
 */
export function cellCaption(text: string | undefined): string | undefined {
   const lines: string[] = [];
   forEachNonProseLine(text ?? "", (_raw, line) => {
      if (line.startsWith("//")) return;
      if (!line.startsWith("#")) return false;
      if (line.startsWith('#"')) lines.push(line.slice(2).trim());
   });
   return lines.length > 0 ? lines.join(" ") : undefined;
}

/** A cell's code without its prose (`##` and `(markdown)` annotations), which renders separately. */
export function stripProse(code: string): string {
   const kept: string[] = [];
   forEachNonProseLine(code, (raw) => {
      kept.push(raw);
   });
   return kept.join("\n");
}

/** One-line label for a folded definition cell: its statement kind and name. */
export function definitionSummary(text: string | undefined): string {
   let line = "";
   forEachNonProseLine(text ?? "", (_raw, l) => {
      if (!l || l.startsWith("#") || l.startsWith("//")) return;
      line = l;
      return false;
   });
   const named = /^(source|query|given|type):?\s+([A-Za-z_]\w*)/.exec(line);
   if (named) return `${named[1]}: ${named[2]}`;
   const keyword = /^(import|export)\b/.exec(line);
   return keyword ? keyword[1] : line;
}
