// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { chartLineText, type ChartState } from "../DashboardBuilder/chartLine";
import { closesBlock } from "../DashboardBuilder/malloyText";
import type { NotebookSource } from "../DashboardBuilder/legacyNotebook";

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

/**
 * The lines of a cell's `#|(…)` prose blocks, opener to closer. The closer is Malloy's: at the
 * opener's column, and not `|##`. An opener with no closer is not a block.
 */
function proseBlockLines(lines: string[]): Set<number> {
   const inside = new Set<number>();
   for (let i = 0; i < lines.length; i++) {
      if (!/^[ \t]*#\|\(/.test(lines[i])) continue;
      const column = /^[ \t]*/.exec(lines[i])![0].length;
      for (let j = i + 1; j < lines.length; j++) {
         if (!closesBlock(lines[j], column, "|#")) continue;
         for (let k = i; k <= j; k++) inside.add(k);
         i = j;
         break;
      }
   }
   return inside;
}

/** A query cell's code with its prose and caption lines taken out, since those render as prose. */
export function queryCode(slice: string): string {
   const lines = slice.replace(/\r\n/g, "\n").split("\n");
   const blocks = proseBlockLines(lines);
   return lines
      .filter((line, i) => {
         if (blocks.has(i)) return false;
         const trimmed = line.trim();
         return !(
            trimmed.startsWith('#"') || /^#\((markdown|text)\)/.test(trimmed)
         );
      })
      .join("\n")
      .trim();
}

/** The `#"` caption lines in a query cell's tag block, joined; prose blocks and comments inside the block are skipped. */
export function captionOf(slice: string): string | undefined {
   const lines: string[] = [];
   const all = slice.replace(/\r\n/g, "\n").split("\n");
   const blocks = proseBlockLines(all);
   for (const [i, line] of all.entries()) {
      if (blocks.has(i)) continue;
      const trimmed = line.trim();
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

/** Whether a line can sit above a statement's code: blank, a tag or a comment. */
const isHeaderLine = (trimmed: string) =>
   trimmed === "" ||
   trimmed.startsWith("#") ||
   trimmed.startsWith("//") ||
   trimmed.startsWith("--");

/**
 * A query cell's text with its chart line set to `chart`, as the writer leaves it: the recognized line replaced or removed, or a new one put directly above the code.
 * `existing` is the recognized chart lines the cell was read with; more than one leaves the text alone, as the writer refuses that.
 */
export function withChart(
   text: string,
   chart: ChartState | undefined,
   existing: readonly string[],
): string {
   if (chart === undefined || chart === "custom" || existing.length > 1)
      return text;
   const nl = text.includes("\r\n") ? "\r\n" : "\n";
   const lines: string[] = text.match(/[^\n]*\n|[^\n]+$/g) ?? [];
   const fresh = chart === "default" ? [] : [chartLineText(chart) + nl];
   if (existing.length === 1) {
      const found = lines.findIndex(
         (line) => line.trim() === existing[0].trim(),
      );
      if (found < 0) return text;
      lines.splice(found, 1, ...fresh);
      return lines.join("");
   }
   const blocks = proseBlockLines(
      lines.map((line) => line.replace(/\r?\n$/, "")),
   );
   let at = lines.length;
   for (const [i, line] of lines.entries()) {
      if (blocks.has(i)) continue;
      const trimmed = line.trim();
      if (!isHeaderLine(trimmed)) {
         at = i;
         break;
      }
   }
   lines.splice(at, 0, ...fresh);
   return lines.join("");
}

const NAME = "(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)";
const RUN_TARGET = new RegExp(
   `(?:^|\\n)\\s*run:\\s*${NAME}\\s*->\\s*${NAME}\\s*$`,
);

/** The `source -> view` a query cell runs, when its code is a plain `run:` of one. */
export function runTargetOf(
   text: string,
): { source: string; view: string } | undefined {
   const match = RUN_TARGET.exec(queryCode(text));
   if (!match) return undefined;
   const bare = (name: string) => name.replace(/^`|`$/g, "");
   return { source: bare(match[1]), view: bare(match[2]) };
}
