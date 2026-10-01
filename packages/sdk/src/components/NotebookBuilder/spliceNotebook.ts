// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** Rebuilds a notebook from the locator's spans, copying every cell it does not rewrite byte for byte. */

import {
   chartLineText,
   chartStateOf,
   isChartPick,
   type ChartPick,
   type ChartState,
} from "../DashboardBuilder/chartLine";
import { syntaxErrors } from "../DashboardBuilder/spliceDocument";
import type { SpliceResult } from "../DashboardBuilder/spliceResult";
import { captionOf } from "./cellText";
import { queryCellText, queryRunProblem, type QueryRun } from "./queryCell";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
   type NotebookSourceCell,
} from "./readNotebookSource";

export type NotebookCellKind = NotebookSourceCell["kind"];

export interface NotebookDocumentCell {
   /** A read cell's `id`, or a fresh one for an added cell. */
   id: string;
   kind: NotebookCellKind;
   /** Markdown cells only: the prose as it should read. */
   markdown?: string;
   /** Query cells: the chart as the picker holds it. Absent on a read cell means as the file has it, and on an added cell the view's own. */
   chart?: ChartState;
   /** An added query cell: what it runs. Ignored once the cell is in the file. */
   run?: QueryRun;
   added?: boolean;
}

/** An ordered list of cells, referring to the read by `id`. */
export interface NotebookDocument {
   cells: NotebookDocumentCell[];
}

/** Whether the lines are exactly what the writer would emit for their state, which a re-pick then leaves alone. */
const isCanonical = (lines: string[], state: ChartState) =>
   state === "default"
      ? lines.length === 0
      : state === "custom"
        ? true
        : lines.length === 1 && lines[0].trim() === chartLineText(state);

/** A cell's chart in the document; absent for a line the writer would spell differently, so only a re-pick rewrites it. */
const chartOfLines = (lines: string[]) => {
   const state = chartStateOf(lines);
   return isCanonical(lines, state) ? { chart: state } : {};
};

/** The document a freshly read notebook opens as. */
export function notebookDocumentOf(source: NotebookSource): NotebookDocument {
   return {
      cells: source.cells.map((cell) => ({
         id: cell.id,
         kind: cell.kind,
         ...(cell.kind === "markdown" && { markdown: cell.markdown }),
         ...(cell.chart && chartOfLines(cell.chart.lines.map((l) => l.text))),
      })),
   };
}

/** Definitions above each query, by cell id, in the file as opened: a query compiled there reads none below it. */
export type DefinitionsAbove = ReadonlyMap<string, number>;

/** Whether `cells[from]` may land at index `to`: the compiler refuses a forward reference, so no query rises above a definition it had above it when read. */
export function canMove(
   doc: NotebookDocument,
   from: number,
   to: number,
   opened?: DefinitionsAbove,
): boolean {
   const { cells } = doc;
   const inRange = (i: number) =>
      Number.isInteger(i) && i >= 0 && i < cells.length;
   if (!inRange(from) || !inRange(to)) return false;
   if (from === to) return true;
   const { kind } = cells[from];
   if (kind === "definition") return false;
   if (kind === "markdown" || to > from) return true;
   const rest = cells.filter((_, i) => i !== from);
   const above = rest
      .slice(0, to)
      .filter((c) => c.kind === "definition").length;
   const readAt = Number(cells[from].id);
   // Ids are read indices, so a query moved down past a definition may move back up to where it was read.
   if (!Number.isInteger(readAt))
      return !cells.slice(to, from).some((c) => c.kind === "definition");
   const wasAbove = rest.filter(
      (c) => c.kind === "definition" && Number(c.id) < readAt,
   ).length;
   return above >= Math.min(wasAbove, opened?.get(cells[from].id) ?? wasAbove);
}

/** Whether a query cell may be added at index `at`: every definition stays above it, since Malloy reads nothing below. */
export function canInsertQuery(doc: NotebookDocument, at: number): boolean {
   return (
      Number.isInteger(at) &&
      at >= 0 &&
      at <= doc.cells.length &&
      !doc.cells.slice(at).some((cell) => cell.kind === "definition")
   );
}

/** Whether saving `next` over `saved` removes a query cell the file already had, which the catalog writer cannot write back. */
export function removesReadQuery(
   saved: NotebookDocument,
   next: NotebookDocument,
): boolean {
   const after = new Set(next.cells.map((cell) => cell.id));
   return saved.cells.some(
      (cell) => cell.kind === "query" && !cell.added && !after.has(cell.id),
   );
}

/** Whether stepping back after saving `next` over `saved` could land on a state the writer cannot write: a removed read query, or a `custom` chart line that was replaced. */
export function undoUnsafeAfter(
   saved: NotebookDocument,
   next: NotebookDocument,
): boolean {
   if (removesReadQuery(saved, next)) return true;
   const now = new Map(next.cells.map((cell) => [cell.id, cell]));
   return saved.cells.some(
      (cell) =>
         cell.kind === "query" &&
         cell.chart === "custom" &&
         now.get(cell.id)?.chart !== "custom",
   );
}

const normalizeNewlines = (text: string) => text.replace(/\r\n?/g, "\n");

/** A line with nothing on it, anywhere in a gap that starts at a line start. */
const BLANK_LINE = /(^|\n)[ \t\r]*\n/;

/** A gap whose last line is blank. */
const ENDS_BLANK = /(^|\n)[ \t\r]*\n$/;

const refuse = (reason: string): SpliceResult => ({ ok: false, reason });

const KEPT = "Your changes are still here.";

/** The comment lines a markdown cell's span opens with, or undefined when a comment shares a line with its prose. */
export function leadingComments(span: string): string | undefined {
   let at = 0;
   let inBlock = false;
   while (at < span.length) {
      const newline = span.indexOf("\n", at);
      const end = newline < 0 ? span.length : newline + 1;
      const line = span.slice(at, end).trim();
      if (inBlock) {
         const close = line.indexOf("*/");
         if (close >= 0) {
            if (line.slice(close + 2).trim()) return undefined;
            inBlock = false;
         }
      } else if (line.startsWith("//") || line.startsWith("--")) {
         at = end;
         continue;
      } else if (line.startsWith("/*")) {
         const close = line.indexOf("*/", 2);
         if (close < 0) inBlock = true;
         else if (line.slice(close + 2).trim()) return undefined;
      } else {
         return line.startsWith("#") ? span.slice(0, at) : undefined;
      }
      at = end;
   }
   return undefined;
}

/** A markdown cell in the canonical spelling: one line as `##(markdown)`, more as a column-0 block. */
function emitMarkdown(markdown: string, nl: string): string {
   if (!markdown.includes("\n")) return `##(markdown) ${markdown}${nl}`;
   const body = markdown
      .split("\n")
      .map((line) => `${line}${nl}`)
      .join("");
   return `##|(markdown)${nl}${body}|##${nl}`;
}

/** The gaps a removal runs together: whatever they hold is kept, and whitespace collapses to one piece. */
function mergeGaps(pieces: string[], keepLast: boolean): string {
   if (pieces.length === 1) return pieces[0];
   const content = pieces.filter((piece) => piece.trim() !== "");
   if (content.length > 0) return content.join("");
   return keepLast ? pieces[pieces.length - 1] : pieces[0];
}

interface Emitted {
   cell: NotebookDocumentCell;
   original?: NotebookSourceCell;
   /** Original index in the read, for cells that were read. */
   index?: number;
   /** Written fresh from `markdown` rather than copied from the span. */
   fresh: boolean;
   /** Markdown cells: the prose the read-back must yield. */
   markdown?: string;
   /** A query cell the writer wrote or rewrote: its exact text, and the chart and caption the read-back must find in it. */
   text?: string;
   chart?: ChartState;
   caption?: string;
}

/** The cell's text with its chart line set to `chart`, or why that cannot be done. */
function withChart(
   text: string,
   cell: NotebookSourceCell,
   chart: ChartPick | "none" | "default",
   nl: string,
): string | { refuse: string } {
   const found = cell.chart;
   if (!found) return { refuse: "has no place for a chart line" };
   if (found.unmodelled !== undefined)
      return {
         refuse: `carries \`${found.unmodelled}\`, which this editor does not model, so its chart cannot be changed here`,
      };
   if (found.lines.length > 1)
      return {
         refuse:
            "has more than one chart line, so its chart cannot be changed here",
      };
   const line = chart === "default" ? "" : chartLineText(chart) + nl;
   const { span } = cell;
   const [only] = found.lines;
   if (!only)
      return (
         text.slice(span.start, found.insertAt) +
         line +
         text.slice(found.insertAt, span.end)
      );
   const alone =
      (only.span.start === 0 || text[only.span.start - 1] === "\n") &&
      text.slice(only.span.start, only.span.end).endsWith("\n");
   if (!alone)
      return {
         refuse:
            "has its chart tag sharing a line with something else, so its chart cannot be changed here",
      };
   return (
      text.slice(span.start, only.span.start) +
      line +
      text.slice(only.span.end, span.end)
   );
}

/** Rebuild `sourceText` as `doc` asks, or say why not. */
export async function spliceNotebookDocument(
   sourceText: string,
   doc: NotebookDocument,
   opened?: DefinitionsAbove,
   /** The sources the notebook's own compiled model offers, which an added query cell must pick from. */
   reachable?: readonly string[],
): Promise<SpliceResult> {
   const read = await readNotebookSource(sourceText);
   if (notebookSourceRefused(read))
      return refuse(`This notebook cannot be edited here. ${read.refused}`);
   const { text, header, cells: original } = read.source;
   const crlf = text.split("\r\n").length - 1;
   const nl = crlf > text.split("\n").length - 1 - crlf ? "\r\n" : "\n";
   const indexOf = new Map(original.map((cell, i) => [cell.id, i]));

   const emitted: Emitted[] = [];
   const seen = new Set<string>();
   for (const [position, cell] of doc.cells.entries()) {
      const where = `Cell ${position + 1}`;
      if (seen.has(cell.id))
         return refuse(
            `${where} repeats the id "${cell.id}", so the edit cannot be placed. ${KEPT}`,
         );
      seen.add(cell.id);
      const index = cell.added ? undefined : indexOf.get(cell.id);
      if (cell.added && indexOf.has(cell.id))
         return refuse(
            `${where} is marked added but reuses the id of a cell already in the file. ${KEPT}`,
         );
      if (cell.added && cell.kind === "definition")
         return refuse(
            `${where} is a new definition; only markdown and query cells can be added here. ${KEPT}`,
         );
      const was = index === undefined ? undefined : original[index];
      if (!cell.added && (!was || was.kind !== cell.kind))
         return refuse(
            `${where} does not match any ${cell.kind} cell in the file as it was opened, so the edit cannot be placed. Reopen the notebook. ${KEPT}`,
         );
      if (cell.kind === "query") {
         if (cell.added) {
            if (!cell.run)
               return refuse(
                  `${where} is a new query with nothing to run. ${KEPT}`,
               );
            const problem = queryRunProblem(cell.run, cell.chart, reachable);
            if (problem) return refuse(`${where}: ${problem} ${KEPT}`);
            if (doc.cells.slice(position).some((c) => c.kind === "definition"))
               return refuse(
                  `${where} is a new query above a definition, which Malloy refuses to let it read. Move it below every definition. ${KEPT}`,
               );
            const chart = cell.chart ?? "default";
            emitted.push({
               cell,
               fresh: true,
               text: queryCellText(cell.run, chart, nl),
               chart,
               caption: cell.run.caption?.trim(),
            });
            continue;
         }
         const wanted = cell.chart;
         const lines = (was?.chart?.lines ?? []).map((line) => line.text);
         const have = chartStateOf(lines);
         if (
            wanted === undefined ||
            (wanted === have && isCanonical(lines, have))
         ) {
            emitted.push({ cell, original: was, index, fresh: false });
            continue;
         }
         if (wanted !== "default" && wanted !== "none" && !isChartPick(wanted))
            return refuse(
               `${where} asks for a chart this editor cannot write ("${wanted}"). ${KEPT}`,
            );
         const rewritten = withChart(text, was!, wanted, nl);
         if (typeof rewritten !== "string")
            return refuse(`${where} ${rewritten.refuse}. ${KEPT}`);
         emitted.push({
            cell,
            original: was,
            index,
            fresh: false,
            text: rewritten,
            chart: wanted,
         });
         continue;
      }
      if (cell.kind !== "markdown") {
         emitted.push({ cell, original: was, index, fresh: false });
         continue;
      }
      const markdown = normalizeNewlines(cell.markdown ?? "");
      const fresh = cell.added === true || markdown !== was?.markdown;
      // An untouched empty cell is copied as it was, which the read-back already accepts.
      if (fresh && markdown.trim() === "")
         return refuse(
            `${where} is an empty markdown cell; remove the cell instead. ${KEPT}`,
         );
      if (/^\|##/m.test(markdown))
         return refuse(
            `${where} has a line starting with \`|##\`, which would close its prose block early. Indent that line or reword it. ${KEPT}`,
         );
      emitted.push({ cell, original: was, index, fresh, markdown });
   }

   const missing = original.find(
      (cell) => cell.kind === "definition" && !seen.has(cell.id),
   );
   if (missing)
      return refuse(
         `A definition is missing from the edit; only markdown and query cells can be removed here. ${KEPT}`,
      );

   const definitions = (cells: { id: string; kind: NotebookCellKind }[]) =>
      cells
         .filter((c) => c.kind === "definition")
         .map((c) => c.id)
         .join(",");
   if (definitions(doc.cells) !== definitions(original))
      return refuse(
         `A definition moved; definitions keep their order in this editor. ${KEPT}`,
      );
   // With definitions in order, the definitions above a cell are a prefix, so a count compares the sets.
   const definitionsAbove = (kinds: NotebookCellKind[], upTo: number) =>
      kinds.slice(0, upTo).filter((kind) => kind === "definition").length;
   const originalKinds = original.map((c) => c.kind);
   const docKinds = doc.cells.map((c) => c.kind);
   for (const [position, { cell, index }] of emitted.entries()) {
      if (
         cell.kind === "query" &&
         index !== undefined &&
         definitionsAbove(docKinds, position) <
            Math.min(
               definitionsAbove(originalKinds, index),
               opened?.get(cell.id) ?? Infinity,
            )
      )
         return refuse(
            `Cell ${position + 1}, a query, would sit above a definition it may read, which Malloy refuses. Move it back below that definition. ${KEPT}`,
         );
   }

   // gaps[i] is the bytes before read cell i; the last entry is the trailing bytes.
   const gaps: string[] = [];
   let at = header.end;
   for (const cell of original) {
      gaps.push(text.slice(at, cell.span.start));
      at = cell.span.end;
   }
   gaps.push(text.slice(at));
   // A removed cell takes its whole span, so the comment block directly above it, which documents it, goes too.
   const survivors = original
      .map((cell, i) => (seen.has(cell.id) ? i : -1))
      .filter((i) => i >= 0);
   // slotGaps[j] precedes the j-th surviving cell; the last entry trails them all.
   const slotGaps: string[] = [];
   let from = 0;
   for (const s of survivors) {
      slotGaps.push(mergeGaps(gaps.slice(from, s + 1), false));
      from = s + 1;
   }
   if (survivors.length > 0) slotGaps.push(mergeGaps(gaps.slice(from), true));
   else slotGaps.push(mergeGaps(gaps, false));

   let out = text.slice(header.start, header.end);
   const startLine = () => {
      if (out !== "" && !out.endsWith("\n")) out += nl;
   };
   let slot = 0;
   let prev: Emitted | undefined;
   for (const [position, item] of emitted.entries()) {
      let gap: string;
      if (!prev) gap = slotGaps[0];
      else if (prev.cell.added) gap = nl;
      else gap = slotGaps[slot];
      const untouchedNeighbours =
         prev !== undefined &&
         !prev.fresh &&
         !item.fresh &&
         prev.index !== undefined &&
         item.index === prev.index + 1 &&
         survivors[slot] === item.index;
      // A prose cell's last line and the next cell's first would merge on read-back without a blank between them.
      const merges = prev?.cell.kind === "markdown" && !BLANK_LINE.test(gap);
      // An added cell would claim a comment ending the gap into its span, so the blank goes after the gap.
      const claims = item.cell.added === true && !ENDS_BLANK.test(gap);
      if (!untouchedNeighbours && (merges || claims))
         gap = (gap === "" || gap.endsWith("\n") ? gap : gap + nl) + nl;
      startLine();
      out += gap;
      startLine();
      if (item.text !== undefined) {
         out += item.text;
      } else if (!item.fresh && item.original) {
         out += text.slice(item.original.span.start, item.original.span.end);
      } else {
         let prefix = "";
         if (item.original) {
            const comments = leadingComments(
               text.slice(item.original.span.start, item.original.span.end),
            );
            if (comments === undefined)
               return refuse(
                  `A comment shares a line with the prose of cell ${position + 1}, so rewriting it would lose the comment. Put the comment on its own line first. ${KEPT}`,
               );
            prefix = comments;
         }
         out += prefix + emitMarkdown(item.markdown ?? "", nl);
      }
      startLine();
      if (!item.cell.added) slot++;
      prev = item;
   }
   // After a trailing added cell the last slot's gap was already spent before it.
   const trailing = !prev ? slotGaps[0] : prev.cell.added ? "" : slotGaps[slot];
   out += trailing;
   // A file that ended without a newline still does.
   if (
      trailing === "" &&
      prev &&
      text !== "" &&
      !text.endsWith("\n") &&
      out.endsWith(nl)
   )
      out = out.slice(0, -nl.length);

   const broke = await syntaxErrors(out);
   if (broke.length > 0)
      return refuse(
         `The edit would have produced a notebook Malloy cannot parse, so it was not written (${broke[0]}). ${KEPT}`,
      );
   const back = await readNotebookSource(out);
   if (notebookSourceRefused(back))
      return refuse(
         `The edit produced a notebook that cannot be read back, so it was not written. ${back.refused} ${KEPT}`,
      );
   const withoutNewline = (s: string) => s.replace(/\r?\n$/, "");
   const matches =
      back.source.cells.length === emitted.length &&
      back.source.cells.every((cell, i) => {
         const want = emitted[i];
         if (cell.kind !== want.cell.kind) return false;
         const markdown =
            want.cell.kind === "markdown"
               ? want.markdown
               : want.original?.markdown;
         if (cell.markdown !== markdown) return false;
         if (want.text !== undefined) {
            const written = withoutNewline(
               out.slice(cell.span.start, cell.span.end),
            );
            return (
               written === withoutNewline(want.text) &&
               chartStateOf((cell.chart?.lines ?? []).map((l) => l.text)) ===
                  want.chart &&
               (!want.cell.added || captionOf(written) === want.caption)
            );
         }
         if (want.fresh || !want.original) return true;
         const { span } = want.original;
         return (
            withoutNewline(out.slice(cell.span.start, cell.span.end)) ===
            withoutNewline(text.slice(span.start, span.end))
         );
      });
   if (!matches)
      return refuse(
         `What was written did not read back as the cells asked for, so it was not written. ${KEPT}`,
      );
   return { ok: true, source: out };
}
