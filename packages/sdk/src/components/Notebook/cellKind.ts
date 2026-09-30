// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NotebookCell } from "../../client";
import { closesBlock, markdownNote } from "../DashboardBuilder/malloyText";

/**
 * Whether the viewer sends a cell to the server to run. A served notebook's
 * definition cell is `type: code` too, so `type` alone would execute it.
 */
export function cellRuns(cell: Pick<NotebookCell, "type" | "kind">): boolean {
   return cell.kind ? cell.kind === "query" : cell.type === "code";
}

const indentOf = (line: string) => /^[ \t]*/.exec(line)[0].length;

/** The line closing the block opened on line `from`, by Malloy's column rule; -1 when there is none. */
function closerAfter(lines: string[], from: number, closer: "|#" | "|##") {
   if (from > 0) {
      const column = indentOf(lines[from]);
      for (let j = from + 1; j < lines.length; j++)
         if (closesBlock(lines[j], column, closer)) return j;
      return -1;
   }
   // A cell's first line has lost its indentation, so the opener's column is unknown. The real
   // closer is followed by code at its own column; failing that, the nearest closer wins.
   let nearest = -1;
   for (let j = from + 1; j < lines.length; j++) {
      if (!closesBlock(lines[j], undefined, closer)) continue;
      if (nearest < 0) nearest = j;
      const next = lines.slice(j + 1).find((l) => l.trim() !== "");
      if (next === undefined || indentOf(next) === indentOf(lines[j])) return j;
   }
   return nearest;
}

/**
 * Calls `visit` on each line of a cell that is not prose. Prose is what the server reads off the
 * statement's LEADING tag block: `##` lines and `##|` blocks, and `(markdown)` lines and
 * `#|(markdown)` blocks. It stops at the first line of code, so a note nested in the statement
 * stays visible. A comment line, and the body of a non-markdown `#|` block, reach `visit` with
 * `opaque` set: they are neither tags nor code. `visit` returns false to stop.
 */
function forEachNonProseLine(
   text: string,
   visit: (raw: string, trimmed: string, opaque: boolean) => boolean | void,
): void {
   const lines = text.split("\n");
   let leading = true;
   let inComment = false;
   for (let i = 0; i < lines.length; i++) {
      const raw = lines[i];
      const line = raw.trim();
      if (inComment || /^(\/\/|--|\/\*)/.test(line)) {
         inComment = inComment
            ? !line.includes("*/")
            : line.startsWith("/*") && !line.includes("*/", 2);
         if (visit(raw, line, true) === false) return;
         continue;
      }
      if (leading && line.startsWith("#")) {
         const note = markdownNote(line);
         if (line.startsWith("##")) {
            // An unterminated `##|` opener is dropped alone.
            if (line.startsWith("##|")) {
               const end = closerAfter(lines, i, "|##");
               if (end >= 0) i = end;
            }
            continue;
         }
         if (line.startsWith("#|")) {
            const end = closerAfter(lines, i, "|#");
            if (end >= 0) {
               if (note?.level === 1) {
                  i = end;
                  continue;
               }
               if (visit(raw, line, false) === false) return;
               for (i++; i <= end; i++)
                  if (visit(lines[i], lines[i].trim(), true) === false) return;
               i = end;
               continue;
            }
         } else if (note) continue;
      } else if (line !== "") leading = false;
      if (visit(raw, line, false) === false) return;
   }
}

/**
 * A query cell's caption: the `#"` lines in the tag block above its `run:`.
 * The API carries no caption field, so the cell text is the source of truth.
 */
export function cellCaption(text: string | undefined): string | undefined {
   const lines: string[] = [];
   forEachNonProseLine(text ?? "", (_raw, line, opaque) => {
      if (opaque || line === "") return;
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
   forEachNonProseLine(text ?? "", (_raw, l, opaque) => {
      if (opaque || !l || l.startsWith("#")) return;
      line = l;
      return false;
   });
   const named = /^(source|query|given|type):?\s+([A-Za-z_]\w*)/.exec(line);
   if (named) return `${named[1]}: ${named[2]}`;
   const keyword = /^(import|export)\b/.exec(line);
   return keyword ? keyword[1] : line;
}
