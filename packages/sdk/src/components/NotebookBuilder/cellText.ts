// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NotebookSource } from "./readNotebookSource";

/** Each read cell's exact text by id: what a query cell sends to run, byte for byte. */
export function cellSlices(source: NotebookSource): Map<string, string> {
   return new Map(
      source.cells.map((cell) => [
         cell.id,
         source.text.slice(cell.span.start, cell.span.end),
      ]),
   );
}

/** Each cell's text less its prose notes and captions: what a preview runs, since prose naming a gated source or `#(authorize)` would refuse it. */
export function cellQueries(source: NotebookSource): Map<string, string> {
   return new Map(
      source.cells.map((cell) => {
         let text = "";
         let at = cell.span.start;
         for (const prose of cell.prose ?? []) {
            text += source.text.slice(at, prose.start);
            at = prose.end;
         }
         return [cell.id, text + source.text.slice(at, cell.span.end)];
      }),
   );
}

/** A query cell's code with its prose and caption lines taken out, since those render as prose. */
export function queryCode(slice: string): string {
   const out: string[] = [];
   let inBlock = false;
   for (const line of slice.replace(/\r\n/g, "\n").split("\n")) {
      const trimmed = line.trim();
      if (inBlock) {
         if (trimmed.startsWith("|#")) inBlock = false;
         continue;
      }
      if (/^#\|\(/.test(trimmed)) {
         inBlock = true;
         continue;
      }
      if (trimmed.startsWith('#"') || /^#\((markdown|text)\)/.test(trimmed))
         continue;
      out.push(line);
   }
   return out.join("\n").trim();
}

/** The `#"` caption lines in a query cell's tag block, joined; prose blocks and comments inside the block are skipped. */
export function captionOf(slice: string): string | undefined {
   const lines: string[] = [];
   let inBlock = false;
   for (const line of slice.replace(/\r\n/g, "\n").split("\n")) {
      const trimmed = line.trim();
      if (inBlock) {
         if (trimmed.startsWith("|#")) inBlock = false;
         continue;
      }
      if (/^#\|\(/.test(trimmed)) {
         inBlock = true;
         continue;
      }
      if (
         trimmed === "" ||
         trimmed.startsWith("//") ||
         trimmed.startsWith("--")
      )
         continue;
      if (!trimmed.startsWith("#")) break;
      if (trimmed.startsWith('#"')) lines.push(trimmed.slice(2).trim());
   }
   return lines.length > 0 ? lines.join(" ") : undefined;
}
