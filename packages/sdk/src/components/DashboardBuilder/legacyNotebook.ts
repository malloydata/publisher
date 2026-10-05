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
 *
 * {@link convertLegacyNotebook} builds on that read to rewrite such a notebook
 * as the layout notebook the dashboard builder edits.
 */

import {
   commentIndex,
   isRule,
   Reader,
   readTileList,
   translate,
   type Ctx,
   type Span,
   type TokenStream,
} from "./malloyTree";
import { mentionsChartTag, parseChartLine } from "./chartLine";
import { annotationTextProblem } from "./annotationText";
import {
   ARTIFACT_NOT_FIRST,
   artifactLeads,
   artifactTag,
   isBareName,
   setsArtifactProperty,
} from "./malloyText";
import { parseTagLines } from "./tagParse";
import { loadMalloy, loadMalloyTag } from "./loadMalloy";

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
      malloy = await loadMalloy();
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
      (item) => item.kind === "note" && setsArtifactProperty(item.note.text),
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

/* ------------------------------------------------------------------ */
/* Converting the cell format to the layout format                     */
/* ------------------------------------------------------------------ */

export type LegacyConversion =
   | { ok: true; text: string; tiles: number }
   | { ok: false; refused: string; line?: number };

export const conversionRefused = (
   r: LegacyConversion,
): r is Extract<LegacyConversion, { ok: false }> => r.ok === false;

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const QUERY_DEFINITION = /^query\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s+is\s+/;

/** A tag string value, with the characters the tag parser unescapes escaped. */
const tagString = (text: string) =>
   `"${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;

/** `text` with comments and string contents blanked to spaces, newlines kept, so a scan for syntax lands only on code. */
function blankNonCode(text: string): string {
   let out = "";
   let i = 0;
   while (i < text.length) {
      const two = text.slice(i, i + 2);
      if (two === "//" || two === "--") {
         const end = text.indexOf("\n", i);
         const stop = end < 0 ? text.length : end;
         out += " ".repeat(stop - i);
         i = stop;
      } else if (two === "/*") {
         const end = text.indexOf("*/", i + 2);
         const stop = end < 0 ? text.length : end + 2;
         out += text.slice(i, stop).replace(/[^\n]/g, " ");
         i = stop;
      } else if (text[i] === "'" || text[i] === '"' || text[i] === "`") {
         const quote = text[i];
         let j = i + 1;
         while (j < text.length && text[j] !== quote)
            j += text[j] === "\\" ? 2 : 1;
         const stop = Math.min(j, text.length);
         out += quote + text.slice(i + 1, stop).replace(/[^\n]/g, " ");
         if (stop < text.length) out += quote;
         i = stop + 1;
      } else {
         out += text[i];
         i++;
      }
   }
   return out;
}

/** Every `->` outside brackets in already-blanked code. */
function topLevelArrows(blank: string): number[] {
   const at: number[] = [];
   let depth = 0;
   for (let i = 0; i < blank.length; i++) {
      const c = blank[i];
      if (c === "{" || c === "(" || c === "[") depth++;
      else if (c === "}" || c === ")" || c === "]") depth--;
      else if (depth === 0 && c === "-" && blank[i + 1] === ">") at.push(i);
   }
   return at;
}

/** Removals applied last-first to `text`, which starts at `base` in the file the spans address. */
function withoutSpans(text: string, base: number, spans: Span[]): string {
   let out = text;
   for (const span of [...spans].sort((a, b) => b.start - a.start))
      out = out.slice(0, span.start - base) + out.slice(span.end - base);
   return out;
}

/** The `##|(markdown) name` block for a text tile; a body line that would close it early gets one space of indent. */
function textBlock(name: string, markdown: string): string {
   const body = markdown
      .split("\n")
      .map((line) => (line.startsWith("|##") ? ` ${line}` : line))
      .join("\n");
   return `##|(markdown) ${name}\n${body === "" ? "" : `${body}\n`}|##\n`;
}

/** An extension-body line: tags and comments lose their old indent, other lines (inside a block comment) keep theirs. */
function indented(line: string): string {
   if (line.trim() === "") return "";
   return /^\s*(#|\/\/|--|\/\*)/.test(line)
      ? `  ${line.trimStart()}`
      : `  ${line}`;
}

interface LegacyStatement {
   cell: NotebookSourceCell;
   /** Text of the lines above the statement, less the prose that becomes text tiles. */
   above: string;
   /** From the statement's keyword to the end of the cell. */
   statement: string;
   /** The cell's whole text with only attached markdown removed: a definition is kept as it was. */
   kept: string;
   /** `"` notes above a `run:`. */
   caption?: string;
   line: number;
}

/** A view name from a tile's label or caption: snake_case words, or undefined when nothing usable is left. */
function viewNameFor(label: string | undefined): string | undefined {
   const words = (label ?? "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "");
   // Long labels are cut at a word, not in the middle of one.
   const cut = words.length > 40 ? words.lastIndexOf("_", 40) : words.length;
   const slug = words.slice(0, cut > 0 ? cut : 40);
   return /^[a-z]/.test(slug) && isBareName(slug) ? slug : undefined;
}

/**
 * Rewrite a cell-format notebook (`run:` cells and prose cells) as a layout
 * notebook: the same file with a `tiles=[…]` list, each prose cell a
 * `##|(markdown)` block, and each `run:` a view in a `<source>_tiles`
 * extension. Text outside the cells is carried over as written.
 *
 * PURE: text in, text out. The caller keeps the original to hand back on Undo.
 */
export async function convertLegacyNotebook(
   text: string,
): Promise<LegacyConversion> {
   const read = await readNotebookSource(text);
   if (notebookSourceRefused(read))
      return { ok: false, refused: read.refused, line: read.line };
   const { header, cells } = read.source;
   const refuse = (line: number, why: string): LegacyConversion => ({
      ok: false,
      refused: `Line ${line + 1}: ${why}`,
      line: line + 1,
   });

   const headerLines = text.slice(header.start, header.end).split("\n");
   const artifact = artifactTag(headerLines);
   if (artifact && !artifactLeads(artifact.text))
      return refuse(artifact.from, ARTIFACT_NOT_FIRST);
   if (!artifact || readTileList(artifact.text))
      return refuse(
         Math.max(artifact?.from ?? 0, 0),
         "this notebook already lists its tiles, so there is nothing to convert.",
      );

   const translation = await translate(
      text,
      "file:///publisher-notebook-builder/convert.malloy",
   );
   const stream = translation.parse?.tokenStream as TokenStream | undefined;
   const vocabulary = stream?.tokenSource?.vocabulary;
   const tokens = (stream?.getTokens?.() ?? []) as Token[];
   if (typeof vocabulary?.getSymbolicName !== "function")
      return refuse(0, "Malloy's token stream is not available.");
   const malloy = await loadMalloy();
   const { parseAnnotation } = await loadMalloyTag();
   const symbolOf = (token: Token) => vocabulary.getSymbolicName(token.type);
   const r = new Reader(text);
   const comments = commentIndex(r, stream as TokenStream);
   const tokenText = (token: Token) =>
      text.slice(r.utf16(token.startIndex), r.utf16(token.stopIndex + 1));
   const routeOf = (note: string) =>
      malloy.routeOf({ value: note.trimStart() } as Parameters<
         typeof malloy.routeOf
      >[0]);
   const payloadOf = (note: string) =>
      malloy.payloadOf({ value: note } as Parameters<
         typeof malloy.payloadOf
      >[0]);
   const lf = (value: string) => value.replace(/\r\n?/g, "\n");

   // Names the new text and views must not take.
   const taken = new Set(text.match(/[A-Za-z_][A-Za-z0-9_]*/g));
   const unique = (base: string) => {
      let name = base;
      for (let n = 2; taken.has(name); n++) name = `${base}_${n}`;
      taken.add(name);
      return name;
   };

   const analyse = (cell: NotebookSourceCell): LegacyStatement | undefined => {
      const inside = tokens.filter(
         (t) =>
            t.channel === 0 &&
            r.utf16(t.startIndex) >= cell.span.start &&
            r.utf16(t.stopIndex + 1) <= cell.span.end,
      );
      const markdownRemovals: Span[] = [];
      const captionRemovals: Span[] = [];
      const captions: string[] = [];
      let statement: Token | undefined;
      for (let i = 0; i < inside.length; i++) {
         const name = symbolOf(inside[i]);
         if (name !== "ANNOTATION" && name !== "BLOCK_ANNOTATION_BEGIN") {
            statement = inside[i];
            break;
         }
         let last = i;
         const bodies: string[] = [];
         if (name === "BLOCK_ANNOTATION_BEGIN") {
            while (
               symbolOf(inside[last + 1] ?? inside[i]) ===
               "BLOCK_ANNOTATION_TEXT"
            )
               bodies.push(tokenText(inside[++last]));
            if (
               symbolOf(inside[last + 1] ?? inside[i]) ===
               "BLOCK_ANNOTATION_END"
            )
               last++;
         }
         const note = lf(tokenText(inside[i]));
         const route = routeOf(note);
         const from = r.utf16(inside[i].startIndex);
         const lineStart = r.lineStarts[r.line(from)];
         const span = {
            start: /^[ \t]*$/.test(text.slice(lineStart, from))
               ? lineStart
               : from,
            end: r.utf16(inside[last].stopIndex + 1),
         };
         if (route === "markdown") markdownRemovals.push(span);
         else if (route === '"') {
            captionRemovals.push(span);
            captions.push(
               name === "ANNOTATION"
                  ? lf(payloadOf(note)).replace(/\n$/, "").trim()
                  : [lf(payloadOf(note.split("\n", 1)[0])), ...bodies.map(lf)]
                       .map((l) => l.trim())
                       .filter(Boolean)
                       .join(" "),
            );
         }
         i = last;
      }
      if (!statement) return undefined;
      const statementAt = r.utf16(statement.startIndex);
      const cellText = text.slice(cell.span.start, cell.span.end);
      return {
         cell,
         above: withoutSpans(
            text.slice(cell.span.start, statementAt),
            cell.span.start,
            (cell.kind === "query"
               ? [...markdownRemovals, ...captionRemovals]
               : markdownRemovals
            ).filter((s) => s.end <= statementAt),
         ),
         statement: text.slice(statementAt, cell.span.end).trimEnd(),
         kept: withoutSpans(cellText, cell.span.start, markdownRemovals),
         ...(cell.kind === "query" &&
            captions.length > 0 && { caption: captions.join(" ") }),
         line: r.line(statementAt),
      };
   };

   // Pass one: what each cell is, and the named queries a `run:` can name.
   const named = new Map<string, { expression: string; cell: number }>();
   const analysed = new Map<number, LegacyStatement>();
   for (const [index, cell] of cells.entries()) {
      if (cell.kind === "markdown") continue;
      const found = analyse(cell);
      if (!found)
         return refuse(
            r.line(cell.span.start),
            "a cell with no statement the converter can place.",
         );
      analysed.set(index, found);
      const query = QUERY_DEFINITION.exec(found.statement);
      if (cell.kind === "definition" && query)
         named.set(query[1], {
            expression: found.statement.slice(query[0].length).trim(),
            cell: index,
         });
   }

   interface Resolved {
      source: string;
      body: string;
      used: string[];
   }
   const resolve = (
      statement: LegacyStatement,
   ): Resolved | LegacyConversion => {
      const keyword = /^run\s*:\s*/.exec(statement.statement);
      if (!keyword)
         return refuse(statement.line, "a run that is not `run: <query>`.");
      let expression = statement.statement.slice(keyword[0].length).trim();
      const used: string[] = [];
      for (let hops = 0; hops < 10; hops++) {
         const blank = blankNonCode(expression);
         const lead = /^[A-Za-z_][A-Za-z0-9_]*/.exec(blank)?.[0];
         const definition = lead ? named.get(lead) : undefined;
         if (!lead || !definition) break;
         const rest = expression.slice(lead.length).trim();
         const restBlank = blank.slice(lead.length).trim();
         if (
            restBlank !== "" &&
            !restBlank.startsWith("->") &&
            !restBlank.startsWith("+")
         )
            break;
         if (restBlank.startsWith("+")) {
            const arrows = topLevelArrows(blankNonCode(definition.expression));
            if (arrows.length !== 1)
               return refuse(
                  statement.line,
                  `\`${lead} + { … }\` refines a query with more than one stage, which cannot become one view.`,
               );
         }
         used.push(lead);
         // A trailing `//` or `--` comment on the definition would swallow a refinement on its line.
         const joiner = /(\/\/|--)[^\n]*$/.test(definition.expression)
            ? "\n"
            : " ";
         expression = `${definition.expression}${rest === "" ? "" : `${joiner}${rest}`}`;
      }
      const blank = blankNonCode(expression);
      // The hop limit stopped on a query name; `q -> …` would otherwise convert as a source called `q`.
      if (named.has(/^[A-Za-z_][A-Za-z0-9_]*/.exec(blank)?.[0] ?? ""))
         return refuse(
            statement.line,
            "this run names a query defined through more than 10 other queries, which the converter does not follow.",
         );
      const arrows = topLevelArrows(blank);
      if (arrows.length === 0)
         return refuse(
            statement.line,
            "this run is not `source -> view`, so it cannot become a tile. Fix: write it as `run: <source> -> <view>`.",
         );
      const head = expression.slice(0, arrows[0]).trim();
      if (/\bextend\b/.test(blank.slice(0, arrows[0])))
         return refuse(
            statement.line,
            "this run extends its source inline before `->`, so it cannot become a tile. Fix: declare the extension with a `source:` statement and run that.",
         );
      if (!IDENTIFIER.test(head) && !/^`[^`]+`$/.test(head))
         return refuse(
            statement.line,
            "the source of this run is not a name, so it cannot become a tile. Fix: name it with a `source:` statement and run that.",
         );
      const body = expression.slice(arrows[0] + 2).trim();
      if (body === "")
         return refuse(statement.line, "this run has nothing after `->`.");
      return { source: head, body, used };
   };

   // Pass two, in order: tiles, the views they run, and the cells left in place.
   interface Extension {
      name: string;
      base: string;
      views: string[];
   }
   const extensions = new Map<string, Extension>();
   const entries: string[] = [];
   const dropped = new Set<number>();
   const placed = new Map<number, string>();
   let textCount = 0;
   let tileCount = 0;

   const consumers = new Map<string, number>();
   for (const statement of analysed.values()) {
      if (statement.cell.kind !== "query") continue;
      const resolved = resolve(statement);
      if ("ok" in resolved) return resolved;
      for (const name of resolved.used)
         consumers.set(name, (consumers.get(name) ?? 0) + 1);
   }
   // A named query used by exactly one run, and by nothing else, is that run's view.
   const consumable = (name: string) =>
      consumers.get(name) === 1 &&
      (text.match(new RegExp(`\\b${name}\\b`, "g")) ?? []).length === 2;
   const carried = new Map<string, string[]>();

   for (const [index, cell] of cells.entries()) {
      if (cell.kind === "markdown") {
         const name = unique(`text_${++textCount}`);
         entries.push(`${name} { kind=text }`);
         const aside = comments.all
            .filter((c) => c.start >= cell.span.start && c.end <= cell.span.end)
            .map((c) => `${text.slice(c.start, c.end)}\n`)
            .join("");
         placed.set(index, aside + textBlock(name, cell.markdown ?? ""));
         continue;
      }
      const statement = analysed.get(index) as LegacyStatement;
      let lead = "";
      if (cell.markdown !== undefined) {
         const name = unique(`text_${++textCount}`);
         entries.push(`${name} { kind=text }`);
         lead = `${textBlock(name, cell.markdown)}\n`;
      }
      if (cell.kind === "definition") {
         const query = QUERY_DEFINITION.exec(statement.statement)?.[1];
         if (query && consumable(query)) {
            dropped.add(index);
            carried.set(
               query,
               statement.above.split("\n").filter((l) => l.trim() !== ""),
            );
            if (lead) placed.set(index, lead.replace(/\n$/, ""));
            continue;
         }
         placed.set(index, `${lead}${statement.kept}`);
         continue;
      }

      const resolved = resolve(statement) as Resolved;
      // The generated name is a bare identifier; the quoted base stays verbatim on the `extend`.
      const base = resolved.source
         .replace(/`/g, "")
         .replace(/[^A-Za-z0-9_]+/g, "_")
         .replace(/^(?=[0-9])/, "_");
      let extension = extensions.get(resolved.source);
      if (!extension) {
         extension = {
            name: unique(`${base}_tiles`),
            base: resolved.source,
            views: [],
         };
         extensions.set(resolved.source, extension);
      }

      const above = [
         ...resolved.used.flatMap((name) => carried.get(name) ?? []),
         ...statement.above.split("\n"),
      ].filter((l) => l.trim() !== "");
      const tagLines = above.filter((l) => /^\s*#(?!["(|])/.test(l));
      const tag =
         tagLines.length > 0
            ? parseTagLines(parseAnnotation, tagLines).tag
            : undefined;
      // A view is named for what the tile says about itself, else by its place.
      const view = unique(
         viewNameFor(tag?.text("label") ?? statement.caption) ??
            `tile_${tileCount + 1}`,
      );
      tileCount++;
      entries.push(`"${extension.name} -> ${view}"`);
      const lines = above.map(indented);
      if (statement.caption !== undefined) {
         const tagged =
            annotationTextProblem("caption", statement.caption) === undefined;
         if (tagged && !tag?.has("label"))
            lines.push(`  # label=${tagString(statement.caption)}`);
         else if (tagged && !tag?.has("subtitle"))
            lines.push(`  # subtitle=${tagString(statement.caption)}`);
         else lines.push(`  #" ${statement.caption}`);
      }
      const body = resolved.body
         .split("\n")
         .map((l, i) => (i === 0 || l.trim() === "" ? l : `  ${l}`))
         .join("\n");
      extension.views.push([...lines, `  view: ${view} is ${body}`].join("\n"));
      dropped.add(index);
      if (lead) placed.set(index, lead.replace(/\n$/, ""));
   }

   // Stitch: the header with its list, each cell's replacement, and the bytes between them as found.
   const close = (() => {
      let depth = 0;
      const tagText = artifact.text;
      for (let i = tagText.indexOf("{"); i < tagText.length; i++) {
         if (tagText[i] === '"') {
            for (i++; i < tagText.length && tagText[i] !== '"'; i++)
               if (tagText[i] === "\\") i++;
         } else if (tagText[i] === "{") depth++;
         else if (tagText[i] === "}" && --depth === 0) return i;
      }
      return -1;
   })();
   if (close < 0)
      return refuse(artifact.from, "the `## artifact { … }` tag never closes.");
   // The tag is written as a `##|` block with a tile on each line, which is how a layout is read and edited.
   const entryList =
      entries.length === 0
         ? "\n  tiles=[]\n"
         : `\n  tiles=[\n${entries.map((e) => `    ${e}`).join(",\n")}\n  ]\n`;
   const rewritten = `${artifact.text
      .slice(0, close)
      .trimEnd()
      .replace(/^##(?!\|)/, "##|")}${entryList}${artifact.text.slice(close)}`;
   headerLines.splice(
      artifact.from,
      artifact.block ? artifact.to - artifact.from : 1,
      ...(artifact.block ? rewritten : `${rewritten}\n|##`).split("\n"),
   );

   // Cells that left their place leave their gaps side by side; a run of blank lines is one, except inside a comment.
   const squeeze = (gap: { text: string; at: number[] }) => {
      const lines = gap.text.split("\n");
      const tail = lines.pop();
      const kept: string[] = [];
      let offset = 0;
      for (const line of lines) {
         const from = gap.at[offset] ?? -1;
         const inComment = comments.all.some(
            (c) => from >= c.start && from < c.end,
         );
         if (line.trim() !== "" || kept.at(-1)?.trim() !== "" || inComment)
            kept.push(line);
         offset += line.length + 1;
      }
      return kept.map((line) => `${line}\n`).join("") + tail;
   };
   const gap = { text: "", at: [] as number[] };
   const addGap = (from: number, to: number) => {
      gap.text += text.slice(from, to);
      for (let k = from; k < to; k++) gap.at.push(k);
   };
   let out = headerLines.join("\n");
   let cursor = header.end;
   for (const [index, cell] of cells.entries()) {
      addGap(cursor, cell.span.start);
      cursor = cell.span.end;
      const replacement = placed.get(index);
      if (replacement === undefined) continue;
      out +=
         squeeze(gap) +
         (replacement.endsWith("\n") ? replacement : `${replacement}\n`);
      gap.text = "";
      gap.at = [];
   }
   addGap(cursor, text.length);
   out = `${out.trimEnd()}\n` + (gap.text.trim() === "" ? "" : squeeze(gap));
   out = `${out.trimEnd()}\n`;

   const blocks = [...extensions.values()].map(
      (extension) =>
         `source: ${extension.name} is ${extension.base} extend {\n${extension.views.join("\n\n")}\n}\n`,
   );
   if (blocks.length > 0) out += `\n${blocks.join("\n")}`;
   // The generated text is LF; a CRLF file gets CRLF throughout rather than a mix.
   if (text.includes("\r\n")) out = out.replace(/\r?\n/g, "\r\n");
   return { ok: true, text: out, tiles: entries.length };
}
