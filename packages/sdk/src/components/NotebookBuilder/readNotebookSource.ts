// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where everything in a `.malloy` notebook IS, as exact offsets into its text.
 *
 * The browser-side twin of the server's `readNotebookCells`, which cannot ship
 * here: it statically imports `@malloydata/malloy` and carries only line
 * numbers. This one reads the same parse the same way and adds what a writer
 * needs, a span per cell, so everything between the spans can be kept byte for
 * byte. Like the server it is all-or-nothing: anything it cannot place refuses
 * the whole notebook rather than yielding a partial read to edit.
 */

import {
   commentIndex,
   isRule,
   Reader,
   translate,
   type Ctx,
   type Span,
   type TokenStream,
} from "../DashboardBuilder/malloyTree";
import {
   mentionsChartTag,
   parseChartLine,
} from "../DashboardBuilder/chartLine";

export type { Span };

/** A `# <chart>` line the chart rule recognizes, whole line including its newline. */
export interface ChartLineSpan {
   span: Span;
   /** The line less its newline. */
   text: string;
}

/** The chart tag lines of a query cell. */
export interface QueryChart {
   lines: ChartLineSpan[];
   /** A `#` line that names a chart tag but is not one the rule recognizes, kept byte for byte. */
   unmodelled?: string;
   /** Where a new chart line goes: the start of the line holding the statement's code. */
   insertAt: number;
}

export interface NotebookSourceCell {
   kind: "markdown" | "query" | "definition";
   /** The movable chunk: from the first line of its tag/prose/caption/comment block to the end of the statement's last line (incl. newline). */
   span: Span;
   /** markdown cells: the prose; query cells: the attached header prose, if any. */
   markdown?: string;
   /** Statement cells: the attached `(markdown)`/`(text)` notes and `"` captions, each a whole note token run. */
   prose?: Span[];
   /** Query cells only. */
   chart?: QueryChart;
   /** Stable id for React keys and history; its index in the read. */
   id: string;
}

/**
 * A notebook's layout. `header` starts at 0; the cells follow in file order
 * without overlapping it or each other, and every byte between two spans (or
 * after the last) is a gap the writer keeps as it found it.
 */
export interface NotebookSource {
   text: string;
   header: Span;
   cells: NotebookSourceCell[];
}

export type NotebookSourceResult =
   | { ok: true; source: NotebookSource }
   | { ok: false; refused: string; line?: number };

export const notebookSourceRefused = (
   r: NotebookSourceResult,
): r is Extract<NotebookSourceResult, { ok: false }> => r.ok === false;

interface Token {
   type: number;
   channel: number;
   startIndex: number;
   stopIndex: number;
   line: number;
}

type Node = Ctx & { symbol?: Token };

type Malloy = typeof import("@malloydata/malloy");

// By accessor, never by class name: the host bundler minifies the peer dependency.
const callAccessor = (node: Node, name: string): unknown => {
   const accessor = node[name];
   return typeof accessor === "function" ? accessor.call(node) : undefined;
};

/** `malloyStatement` accessors, in step with the server reader's `STATEMENT_ACCESSORS`. */
const STATEMENT_ACCESSORS: readonly [string, "run" | "notes" | "definition"][] =
   [
      ["runStatement", "run"],
      ["docAnnotations", "notes"],
      ["importStatement", "definition"],
      ["defineSourceStatement", "definition"],
      ["defineQuery", "definition"],
      ["defineGivenStatement", "definition"],
      ["defineUserTypeStatement", "definition"],
      ["exportStatement", "definition"],
   ];

const ARTIFACT_NOTE = /^##[ \t]*artifact\b/;
const TEXT_BLOCK_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const PARSE_URL = "file:///publisher-notebook-builder/notebook.malloy";

const normalizeNewlines = (text: string) => text.replace(/\r\n?/g, "\n");

interface Note {
   text: string;
   route: string | undefined;
   /** Set only on a prose note: `(markdown)`, `"` or `(text)`. */
   body?: string;
   block: boolean;
   startLine: number;
   endLine: number;
}

type Item =
   | {
        kind: "statement";
        run: boolean;
        markdown?: string;
        prose: Span[];
        chart: QueryChart;
        startLine: number;
        endLine: number;
     }
   | { kind: "note"; note: Note };

/** A notebook's layout, or why it cannot be edited; a refusal's `line` is 1-based, while lines inside are 0-based. */
export async function readNotebookSource(
   text: string,
): Promise<NotebookSourceResult> {
   const refuse = (line: number, why: string): NotebookSourceResult => ({
      ok: false,
      refused: `Line ${line + 1}: ${why}`,
      line: line + 1,
   });
   // Lines here end only at LF, so a lone CR would hide a line boundary from every span.
   const loneCr = /\r(?!\n)/.exec(text);
   if (loneCr)
      return refuse(
         text.slice(0, loneCr.index).split("\n").length - 1,
         "a carriage return that is not part of a CRLF line ending, so the cells cannot be located. Fix: save the file with LF or CRLF line endings.",
      );

   let translation: Awaited<ReturnType<typeof translate>>;
   let malloy: Malloy;
   try {
      translation = await translate(text, PARSE_URL);
      // The same dynamic import as `translate`, for Malloy's own note routing.
      malloy = await import("@malloydata/malloy");
   } catch (error) {
      return refuse(0, `Malloy could not read this notebook: ${error}`);
   }
   const syntax = translation.problems.find((p) => p.code === "syntax-error");
   if (syntax)
      return refuse(
         syntax.at?.range?.start?.line ?? 0,
         `Malloy could not parse this notebook (${syntax.message ?? "unknown"}), so it cannot be edited here. Fix: correct the syntax on that line.`,
      );

   const root = translation.parse?.root as Node | undefined;
   const stream = translation.parse?.tokenStream as TokenStream | undefined;
   const vocabulary = stream?.tokenSource?.vocabulary;
   const tokens =
      typeof stream?.getTokens === "function"
         ? (stream.getTokens() as Token[])
         : undefined;
   if (
      !root ||
      typeof root.getChild !== "function" ||
      !stream ||
      !tokens ||
      typeof vocabulary?.getSymbolicName !== "function" ||
      (tokens.length === 0 && text.trim() !== "")
   )
      return {
         ok: false,
         refused:
            "This build of Malloy does not expose a parse tree and token stream the notebook builder can read, so editing is off.",
      };
   const symbolOf = (token: Token) => vocabulary.getSymbolicName(token.type);

   const routeOf = (note: string): string | undefined =>
      malloy.routeOf({ value: note.trimStart() } as Parameters<
         Malloy["routeOf"]
      >[0]);
   const payloadOf = (note: string): string =>
      malloy.payloadOf({ value: note } as Parameters<Malloy["payloadOf"]>[0]);
   const isProseRoute = (route: string | undefined) =>
      route === "markdown" || route === '"' || route === "text";
   const lineBody = (note: string) =>
      normalizeNewlines(payloadOf(note)).replace(/\n$/, "");
   // Text on a block's opener line is prose too, except a lone bare word on a `(markdown)`/`(text)` opener, which names it.
   const blockBody = (opener: string, bodyLines: string[]) => {
      const rest = payloadOf(opener).trim();
      const onOpener =
         routeOf(opener) !== '"' && TEXT_BLOCK_NAME.test(rest) ? "" : rest;
      return normalizeNewlines(
         [onOpener && `${onOpener}\n`, ...bodyLines].join(""),
      ).replace(/\n$/, "");
   };

   const r = new Reader(text);
   const comments = commentIndex(r, stream);
   const tokenText = (token: Token) =>
      text.slice(r.utf16(token.startIndex), r.utf16(token.stopIndex + 1));
   const spanOf = (node: Node) => {
      const startCp = node.start?.startIndex;
      const stopCp = node.stop?.stopIndex;
      const span = r.span(node);
      if (startCp === undefined || stopCp === undefined || !span)
         return undefined;
      // A note's token carries its newline; its last line is the one before it.
      let last = span.end;
      while (
         last > span.start &&
         (text[last - 1] === "\n" || text[last - 1] === "\r")
      )
         last--;
      return {
         startCp,
         stopCp,
         ...span,
         startLine: r.line(span.start),
         endLine: r.line(Math.max(span.start, last - 1)),
      };
   };
   const firstTokenAt = (cp: number): number => {
      let lo = 0;
      let hi = tokens.length;
      while (lo < hi) {
         const mid = (lo + hi) >> 1;
         if (tokens[mid].startIndex < cp) lo = mid + 1;
         else hi = mid;
      }
      return lo;
   };
   // A statement's own `#(markdown)` prose, read from the notes it opens with; blocks and runs join with a blank line.
   const attachedProse = (
      startCp: number,
      stopCp: number,
   ): { markdown: string | undefined; prose: Span[]; chart: QueryChart } => {
      const segments: string[] = [];
      const prose: Span[] = [];
      const chart: QueryChart = { lines: [], insertAt: r.utf16(startCp) };
      let lineEnd = -2;
      for (let i = firstTokenAt(startCp); i < tokens.length; i++) {
         const token = tokens[i];
         if (token.startIndex > stopCp) break;
         if (token.channel !== 0) continue;
         const name = symbolOf(token);
         if (name !== "ANNOTATION" && name !== "BLOCK_ANNOTATION_BEGIN") {
            chart.insertAt = r.lineStarts[r.line(r.utf16(token.startIndex))];
            break;
         }
         const note = normalizeNewlines(tokenText(token));
         const bodyTexts: string[] = [];
         if (name === "BLOCK_ANNOTATION_BEGIN") {
            while (symbolOf(tokens[i + 1] ?? token) === "BLOCK_ANNOTATION_TEXT")
               bodyTexts.push(tokenText(tokens[++i]));
            if (symbolOf(tokens[i + 1] ?? token) === "BLOCK_ANNOTATION_END")
               i++;
         }
         // The lexer's own extent, since a `|#` at another column than the opener's is still body.
         if (isProseRoute(routeOf(note))) {
            const start = r.utf16(token.startIndex);
            const lineStart = r.lineStarts[r.line(start)];
            // Indentation left behind joins the next line, and a block opened there then never closes at its column.
            const indentOnly = /^[ \t]*$/.test(text.slice(lineStart, start));
            prose.push({
               start: indentOnly ? lineStart : start,
               end: r.utf16(tokens[i].stopIndex + 1),
            });
         }
         if (name === "ANNOTATION") {
            const line = note.replace(/\n$/, "");
            if (parseChartLine(line)) {
               const start = r.utf16(token.startIndex);
               const lineStart = r.lineStarts[r.line(start)];
               chart.lines.push({
                  span: {
                     start: /^[ \t]*$/.test(text.slice(lineStart, start))
                        ? lineStart
                        : start,
                     end: r.utf16(token.stopIndex + 1),
                  },
                  text: line,
               });
            } else if (mentionsChartTag(line) && chart.unmodelled === undefined)
               chart.unmodelled = line.trim();
         }
         if (routeOf(note) !== "markdown") {
            lineEnd = -2;
         } else if (name === "BLOCK_ANNOTATION_BEGIN") {
            segments.push(blockBody(note, bodyTexts));
            lineEnd = -2;
         } else {
            const body = lineBody(note);
            if (token.line === lineEnd + 1)
               segments[segments.length - 1] += `\n${body}`;
            else segments.push(body);
            lineEnd = token.line;
         }
      }
      return {
         markdown: segments.length > 0 ? segments.join("\n\n") : undefined,
         prose,
         chart,
      };
   };

   const covered: [number, number][] = [];
   const items: Item[] = [];
   const cannotHold = (
      line: number,
      what: string,
      fix = "remove it, or rewrite it as an import, source:, query:, given:, type: or export statement, a run:, or a `##(markdown)` / `##|(markdown)` prose note.",
   ) =>
      refuse(
         line,
         `${what}, which no notebook cell can hold, so the notebook cannot be edited here. Fix: ${fix}`,
      );

   for (let i = 0; i < (root.childCount ?? 0); i++) {
      const child = root.getChild(i) as Node;
      if (!isRule(child)) {
         const token = child?.symbol;
         const name = token ? symbolOf(token) : undefined;
         if (token && name === "EOF") continue;
         if (token && name === "SEMI") {
            covered.push([token.startIndex, token.stopIndex]);
            continue;
         }
         return cannotHold(
            (token?.line ?? 1) - 1,
            "a token outside any statement",
         );
      }
      const span = spanOf(child);
      if (!span) return cannotHold(0, "a statement with no readable range");
      const match = STATEMENT_ACCESSORS.find(
         ([accessor]) => callAccessor(child, accessor) !== undefined,
      );
      if (!match && callAccessor(child, "ignoredObjectAnnotations"))
         return cannotHold(
            span.startLine,
            "a # tag that annotates no statement",
            "move the tag directly above its run:, or write trailing prose as a `##(markdown)` note.",
         );
      if (!match) {
         const firstLine = text.slice(span.start, span.end).split("\n")[0];
         return cannotHold(
            span.startLine,
            `\`${firstLine.trim()}\` is a statement the notebook reader does not recognize`,
         );
      }
      covered.push([span.startCp, span.stopCp]);
      const [accessor, kind] = match;
      if (kind !== "notes") {
         const { markdown, prose, chart } = attachedProse(
            span.startCp,
            span.stopCp,
         );
         items.push({
            kind: "statement",
            run: kind === "run",
            ...(markdown !== undefined && { markdown }),
            prose,
            chart,
            startLine: span.startLine,
            endLine: span.endLine,
         });
         continue;
      }
      const group = callAccessor(child, accessor) as Node;
      for (const noteNode of (callAccessor(group, "docAnnotation") ??
         []) as Node[]) {
         const noteSpan = spanOf(noteNode);
         if (!noteSpan)
            return cannotHold(span.startLine, "a note with no readable range");
         const block =
            callAccessor(noteNode, "docBlockAnnotation") !== undefined;
         const own = tokens.filter(
            (t) =>
               t.startIndex >= noteSpan.startCp &&
               t.stopIndex <= noteSpan.stopCp,
         );
         const bodyTokens = own.filter(
            (t) => symbolOf(t) === "BLOCK_ANNOTATION_TEXT",
         );
         // A block's note text ends with its body, as the server reads it: with the closer, MOTLY drops the tag.
         const lastBody = bodyTokens[bodyTokens.length - 1] ?? own[0];
         const noteText = normalizeNewlines(
            block && lastBody
               ? text
                    .slice(noteSpan.start, r.utf16(lastBody.stopIndex + 1))
                    .replace(/\r?\n$/, "")
               : text.slice(noteSpan.start, noteSpan.end),
         );
         const route = routeOf(noteText);
         const body = !isProseRoute(route)
            ? undefined
            : block
              ? blockBody(noteText.split("\n", 1)[0], bodyTokens.map(tokenText))
              : lineBody(noteText);
         items.push({
            kind: "note",
            note: {
               text: noteText,
               route,
               ...(body !== undefined && { body }),
               block,
               startLine: noteSpan.startLine,
               endLine: noteSpan.endLine,
            },
         });
      }
   }

   // The lexer folds the rest of a closer's line into the closer, and the writer, rebuilding from `markdown`, would drop it.
   for (const token of tokens) {
      if (symbolOf(token) !== "BLOCK_ANNOTATION_END") continue;
      const trailing = tokenText(token)
         .trimStart()
         .replace(/^\|#{1,2}/, "")
         .trim();
      if (trailing)
         return refuse(
            token.line - 1,
            `the text after a block's closer (\`${trailing}\`) is not part of any cell, so the notebook cannot be edited here. Fix: put it on its own line, or inside the block.`,
         );
   }

   // Both lists are in file order, so one forward sweep checks every token.
   let range = 0;
   for (const token of tokens) {
      if (token.channel !== 0 || symbolOf(token) === "EOF") continue;
      while (range < covered.length && covered[range][1] < token.startIndex)
         range++;
      const [from, to] = covered[range] ?? [Infinity, -Infinity];
      if (token.startIndex < from || token.stopIndex > to)
         return cannotHold(token.line - 1, "a token outside any statement");
   }

   const artifactAt = items.findIndex(
      (item) => item.kind === "note" && ARTIFACT_NOTE.test(item.note.text),
   );
   if (artifactAt < 0)
      return refuse(
         0,
         "this file has no ## artifact note, so it is not a notebook the builder can edit. Fix: add `## artifact { kind=notebook }` after the ##! line.",
      );
   const above = items.findIndex(
      (item, index) => index < artifactAt && item.kind === "statement",
   );
   if (above >= 0)
      return refuse(
         (items[above] as { startLine: number }).startLine,
         'a statement sits above the `## artifact` tag, where only `##!` flags, comments and `"` notes may. Fix: move it below the artifact tag.',
      );

   // Cells span whole lines, so two items on one line cannot be moved apart.
   for (let i = 1; i < items.length; i++) {
      const prev = items[i - 1];
      const item = items[i];
      const prevEnd = prev.kind === "note" ? prev.note.endLine : prev.endLine;
      const start = item.kind === "note" ? item.note.startLine : item.startLine;
      if (start <= prevEnd)
         return cannotHold(
            start,
            "a line holding two statements or notes",
            "put each statement and note on its own line.",
         );
   }

   const lineEnd = (line: number) => r.lineStarts[line + 1] ?? text.length;
   interface Building {
      cell: Omit<NotebookSourceCell, "span">;
      startLine: number;
      endLine: number;
   }
   const building: Building[] = [];
   let headerEnd = -1;
   let inHeader = true;
   // The markdown cell a contiguous `##(markdown)` line run is building, which the next adjacent line joins.
   let lineRun: Building | undefined;
   items.forEach((item, index) => {
      if (item.kind === "statement") {
         inHeader = false;
         lineRun = undefined;
         building.push({
            cell: {
               kind: item.run ? "query" : "definition",
               ...(item.markdown !== undefined && { markdown: item.markdown }),
               ...(item.prose.length > 0 && { prose: item.prose }),
               ...(item.run && { chart: item.chart }),
               id: "",
            },
            startLine: item.startLine,
            endLine: item.endLine,
         });
         return;
      }
      const { note } = item;
      const floating = index > artifactAt && note.body !== undefined;
      if (!floating) {
         // A non-prose note is header until the first cell, and gap content after it.
         if (inHeader) headerEnd = note.endLine;
         lineRun = undefined;
         return;
      }
      inHeader = false;
      if (!note.block && lineRun && lineRun.endLine + 1 === note.startLine) {
         lineRun.cell.markdown += `\n${note.body}`;
         lineRun.endLine = note.endLine;
         return;
      }
      const cell: Building = {
         cell: { kind: "markdown", markdown: note.body, id: "" },
         startLine: note.startLine,
         endLine: note.endLine,
      };
      building.push(cell);
      lineRun = note.block ? undefined : cell;
   });

   const header: Span = { start: 0, end: lineEnd(headerEnd) };
   let boundary = headerEnd;
   const cells: NotebookSourceCell[] = building.map(
      ({ cell, startLine, endLine }, index) => {
         // A comment directly above a cell, with no blank line between, travels with it.
         let first = startLine;
         while (first - 1 > boundary && comments.lines.has(first - 1)) first--;
         boundary = endLine;
         return {
            ...cell,
            span: { start: r.lineStarts[first], end: lineEnd(endLine) },
            id: String(index),
         };
      },
   );

   // A comment cut by a span boundary would be split by a move.
   for (const span of [header, ...cells.map((c) => c.span)]) {
      const cut = comments.all.find(
         (c) =>
            (c.start < span.start && c.end > span.start) ||
            (c.start < span.end && c.end > span.end),
      );
      if (cut)
         return cannotHold(
            r.line(cut.start),
            "a comment that straddles a cell",
            "start the comment on its own line.",
         );
   }
   return { ok: true, source: { text, header, cells } };
}
