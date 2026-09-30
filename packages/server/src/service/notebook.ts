// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   MalloyTranslator,
   payloadOf,
   routeOf,
   type LogMessage,
   type ModelDef,
} from "@malloydata/malloy";
import { MODEL_FILE_SUFFIX } from "../constants";
import { ownModelNoteObjects, type AnnotationNote } from "./annotations";
import { docCommentText } from "./motly";

/** The package-relative directory a served notebook must live in. */
export const NOTEBOOKS_DIR = "notebooks";

/**
 * True for a package-relative path that notebook discovery considers: a
 * `.malloy` directly inside the top-level `notebooks/` directory. Only the
 * candidate filter; the file is a notebook only if it carries a model-level
 * `## artifact` note.
 */
export function isNotebookModelPath(modelPath: string): boolean {
   if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) return false;
   const segments = modelPath.split("/");
   return segments.length === 2 && segments[0] === NOTEBOOKS_DIR;
}

const ARTIFACT_NOTE = /^##[ \t]*artifact\b/;

/** The note route whose payload is markdown: a floating cell at `##`, a cell's own prose at `#`. */
export const MARKDOWN_ROUTE = "markdown";

/** Malloy's own routing for one note: its route, empty for a plain tag, `undefined` when the prefix is malformed. */
export function routeOfNote(text: string): string | undefined {
   return routeOf({ value: text.trimStart() } as Parameters<typeof routeOf>[0]);
}

/** Whether a note is on the `(markdown)` route, at either level and in either form. */
export function isMarkdownNote(text: string): boolean {
   return routeOfNote(text) === MARKDOWN_ROUTE;
}

/** The fix for a `(markdown)` note that annotates no statement: it stands alone, or moves above one. */
export const attachedNowhereFix = (block: boolean) =>
   block
      ? "write it as a floating `##|(markdown)` block closed by `|##` for prose that stands on its own, or move it directly above the statement it describes."
      : "write it as a floating `##(markdown)` line for prose that stands on its own, or move it directly above the statement it describes.";

/** Whether a floating note is prose: `(markdown)`, or the `"` and `(text)` spellings it replaced. */
export function isProseRoute(route: string | undefined): boolean {
   return route === MARKDOWN_ROUTE || route === '"' || route === "text";
}

export function isArtifactNoteText(text: string): boolean {
   return ARTIFACT_NOTE.test(text);
}

/** The 0-based line of the first own `## artifact` note, if the file has one. */
export function artifactNoteLine(
   notes: readonly AnnotationNote[],
): number | undefined {
   return notes.find((n) => isArtifactNoteText(n.text))?.at?.range.start.line;
}

/**
 * The texts of the own notes above the artifact line, of every route, in the
 * order `ownLevelNotes` yields them (block notes first); all of them when the
 * file has no artifact note. The doc-comment reader that consumes them keeps
 * only the `"` route.
 */
export function docNotesAboveArtifact(
   notes: readonly AnnotationNote[],
): string[] {
   const line = artifactNoteLine(notes);
   return notes
      .filter((n) => line === undefined || (n.at?.range.start.line ?? 0) < line)
      .map((n) => n.text);
}

/**
 * A dashboard's description notes: those above the artifact line, or, when they
 * carry no prose, those below it, so a `"` note below the tag (where a
 * dashboard's description used to be read from) still describes the page.
 */
export function dashboardDescriptionNotes(
   notes: readonly AnnotationNote[],
): string[] {
   const above = docNotesAboveArtifact(notes);
   if (docCommentText(above) !== undefined) return above;
   const line = artifactNoteLine(notes);
   return notes
      .filter((n) => line !== undefined && (n.at?.range.start.line ?? 0) > line)
      .map((n) => n.text);
}

/**
 * Whether raw file text has a line matching `artifact` outside a `##|"` (or
 * `#|`) block body, whose prose could otherwise pass for a tag. For a file that
 * did not compile, where no note can be read.
 */
export function hasArtifactLineOutsideBlocks(
   source: string,
   artifactLine: RegExp,
): boolean {
   const lines = source.split(/\r\n|\r|\n/);
   for (let i = 0; i < lines.length; i++) {
      const trimmed = lines[i].trimStart();
      const opener = /^(#{1,2})\|/.exec(trimmed);
      if (opener) {
         const wanted = `|${opener[1]}`;
         if (trimmed.includes(wanted, opener[0].length)) continue;
         let end = -1;
         for (let at = i + 1; at < lines.length; at++)
            if (lines[at].trimStart().startsWith(wanted)) {
               end = at;
               break;
            }
         // An unclosed opener holds no block, so it must not hide the rest of the file.
         if (end !== -1) {
            i = end;
            continue;
         }
      }
      if (artifactLine.test(trimmed)) return true;
   }
   return false;
}

export function claimsToBeANotebook(source: string): boolean {
   return hasArtifactLineOutsideBlocks(source, ARTIFACT_NOTE);
}

/* ------------------------------------------------------------------ */
/* The cell reader                                                     */
/* ------------------------------------------------------------------ */

export type NotebookCellKind = "markdown" | "query" | "definition";

/** One cell of a served notebook, as the reader sliced it from the file. */
export interface NotebookCellSpan {
   kind: NotebookCellKind;
   type: "markdown" | "code";
   /** Markdown: the prose without sigils. Code: the statement verbatim, from its first tag line, `(markdown)` annotations included. */
   text: string;
   /** A code cell's own `#(markdown)` prose, read out of its tag lines; blocks join with a blank line. */
   markdown?: string;
   /** The 0-based inclusive `[start, end]` lines of `text` (split on `\n`) that hold that prose; absent with no `markdown`. */
   proseLines?: [number, number][];
   /** The joined text of the statement's leading `#"` lines; absent when it has none. */
   caption?: string;
   /** 1-based and inclusive. */
   startLine: number;
   endLine: number;
   /** A query cell's index into `modelDef.queryList` and `modelInfo.anonymous_queries`. */
   queryIndex?: number;
}

/** Why the reader refused a notebook: the 1-based line, and a message that names the fix. */
export interface NotebookReaderError {
   line: number;
   message: string;
}

export interface NotebookReadResult {
   /** Empty whenever `error` is set: a notebook is never served with part of its cells. */
   cells: NotebookCellSpan[];
   /**
    * Every own note that is not a floating markdown cell, each once, in file
    * order. A `"` or `(text)` note below the artifact tag is a cell, so it is
    * never listed and never reads as a description.
    */
   annotations: string[];
   error?: NotebookReaderError;
}

/** Malloy's parse of one file: its tree and the lexer's token stream. */
export interface NotebookParse {
   root: unknown;
   tokenStream: unknown;
}

export interface ParseToken {
   type: number;
   channel: number;
   startIndex: number;
   stopIndex: number;
   line: number;
}

export interface TokenStreamShape {
   tokenSource?: {
      vocabulary?: { getSymbolicName(type: number): string | undefined };
   };
   getTokens?(): ParseToken[];
}

export type ParseNode = Record<string, unknown> & {
   ruleIndex?: number;
   childCount?: number;
   getChild(i: number): ParseNode;
   start?: { startIndex: number };
   stop?: { stopIndex: number };
   symbol?: ParseToken;
};

export const isRuleNode = (node: ParseNode | undefined): boolean =>
   node !== undefined &&
   node.ruleIndex !== undefined &&
   (node.childCount ?? 0) > 0;

// By accessor, never by class name: a minifying bundler renames classes.
export const callAccessor = (node: ParseNode, name: string): unknown => {
   const accessor = node[name];
   return typeof accessor === "function" ? accessor.call(node) : undefined;
};

/** `malloyStatement` accessors the reader recognizes, and what each one is. */
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

const NOTEBOOK_PARSE_URL = "file:///publisher-notebook-reader/notebook.malloy";

/**
 * Parse `text` with Malloy's own translator, stopping at the parse step, or
 * refuse. Imports are never fetched: the parse is all the reader needs.
 */
export function parseNotebookText(
   text: string,
): NotebookParse | NotebookReaderError {
   try {
      const { problems, parse } = translateToParse(text);
      const syntax = problems.find(
         (problem) =>
            problem.code === "syntax-error" && problem.severity === "error",
      );
      if (syntax) {
         const line = (syntax.at?.range.start.line ?? 0) + 1;
         return {
            line,
            message: `Line ${line}: Malloy could not parse this notebook (${syntax.message}). Fix: correct the syntax on that line.`,
         };
      }
      if (!parse) return unreadableParse();
      return parse;
   } catch (error) {
      return {
         line: 1,
         message: `Line 1: Malloy could not parse this notebook (${error instanceof Error ? error.message : String(error)}). Fix: correct the file so it compiles.`,
      };
   }
}

/** The parse step's tree and tokens, kept even when the text has syntax errors, so a lint can still read a broken file. */
export function translateToParse(text: string): {
   problems: LogMessage[];
   parse?: NotebookParse;
} {
   const translator = new MalloyTranslator(NOTEBOOK_PARSE_URL, null, {
      urls: { [NOTEBOOK_PARSE_URL]: text },
   });
   const problems = translator.translate().problems ?? [];
   const parse = translator.parseStep.response?.parse;
   return {
      problems,
      parse: parse && { root: parse.root, tokenStream: parse.tokenStream },
   };
}

export function isNotebookReaderError(
   value: NotebookParse | NotebookReaderError,
): value is NotebookReaderError {
   return "message" in value;
}

function unreadableParse(): NotebookReaderError {
   return {
      line: 1,
      message:
         "Line 1: this build of Malloy does not expose a parse tree and token stream the notebook reader can read, so no cell can be shown. Fix: serve it from a Publisher built against a supported Malloy version.",
   };
}

/** Served prose must not depend on the line endings the package was checked out with. */
function normalizeNewlines(text: string): string {
   return text.replace(/\r\n?/g, "\n");
}

/** ANTLR indexes code points; JavaScript strings index UTF-16 units. */
export function codePointMap(text: string): Int32Array {
   const map = new Int32Array([...text].length + 1);
   let cp = 0;
   for (let i = 0; i < text.length; ) {
      map[cp++] = i;
      i += (text.codePointAt(i) as number) > 0xffff ? 2 : 1;
   }
   map[cp] = text.length;
   return map;
}

interface ReaderNote {
   text: string;
   route: string | undefined;
   /** The prose without sigils; set only on a `(markdown)` note. */
   body?: string;
   block: boolean;
   startLine: number;
   endLine: number;
}

type ReaderItem =
   | {
        kind: "statement";
        run: boolean;
        text: string;
        markdown?: string;
        proseLines?: [number, number][];
        caption?: string;
        startLine: number;
        endLine: number;
     }
   | { kind: "note"; note: ReaderNote };

/** A `(markdown)` block's body: text on its opener line is prose too, except a lone bare word, which names it. */
function markdownBlockBody(opener: string, bodyLines: string[]): string {
   const rest = payloadOf({ value: opener } as Parameters<
      typeof payloadOf
   >[0]).trim();
   // Only a `(markdown)` or `(text)` opener names its block; a `"` opener's text is always prose.
   const onOpener =
      routeOfNote(opener) !== '"' && TEXT_BLOCK_NAME.test(rest) ? "" : rest;
   return normalizeNewlines(
      [onOpener && `${onOpener}\n`, ...bodyLines].join(""),
   ).replace(/\n$/, "");
}

/** A `(markdown)` line note's prose: what follows the route and its one separator. */
function markdownLineBody(noteText: string): string {
   return normalizeNewlines(
      payloadOf({ value: noteText } as Parameters<typeof payloadOf>[0]),
   ).replace(/\n$/, "");
}

/**
 * A served notebook's cells, read off Malloy's parse tree and token stream.
 * Pure over its inputs. `modelDef` is the compile of the same `text`, used only
 * to check that every `run:` the reader found is one Malloy compiled, so the
 * k-th query cell is `queryList[k]`.
 *
 * All-or-nothing: a top-level construct the reader cannot classify, or a
 * default-channel token outside every recognized statement and note, refuses
 * the whole notebook with the line and the fix. Comments are hidden-channel
 * tokens and never structure.
 *
 * Markdown and annotation text is LF-normalized; code cell text is the exact
 * compiled slice. The cells are a view, never a source to write the file back from.
 */
export function readNotebookCells(
   parse: NotebookParse,
   modelDef: Pick<ModelDef, "queryList">,
   text: string,
): NotebookReadResult {
   const refuse = (error: NotebookReaderError): NotebookReadResult => ({
      cells: [],
      annotations: [],
      error,
   });
   const root = parse.root as ParseNode | undefined;
   const stream = parse.tokenStream as TokenStreamShape | undefined;
   const vocabulary = stream?.tokenSource?.vocabulary;
   const tokens =
      typeof stream?.getTokens === "function" ? stream.getTokens() : undefined;
   if (
      !root ||
      typeof root.getChild !== "function" ||
      !tokens ||
      !vocabulary ||
      typeof vocabulary.getSymbolicName !== "function" ||
      (tokens.length === 0 && text.trim() !== "")
   ) {
      return refuse(unreadableParse());
   }
   const symbolOf = (token: ParseToken) =>
      vocabulary.getSymbolicName(token.type);

   const map = codePointMap(text);
   const lineStarts = [0];
   for (let i = 0; i < text.length; i++)
      if (text[i] === "\n") lineStarts.push(i + 1);
   const lineOf = (offset: number): number => {
      let lo = 0;
      let hi = lineStarts.length - 1;
      while (lo < hi) {
         const mid = (lo + hi + 1) >> 1;
         if (lineStarts[mid] <= offset) lo = mid;
         else hi = mid - 1;
      }
      return lo + 1;
   };
   const spanOf = (node: ParseNode) => {
      const startCp = node.start?.startIndex;
      const stopCp = node.stop?.stopIndex;
      if (startCp === undefined || stopCp === undefined) return undefined;
      const start = map[startCp];
      const end = map[stopCp + 1];
      if (start === undefined || end === undefined || end < start)
         return undefined;
      // A note's token carries its newline; its last line is the one before it.
      let last = end;
      while (
         last > start &&
         (text[last - 1] === "\n" || text[last - 1] === "\r")
      )
         last--;
      return {
         startCp,
         stopCp,
         start,
         end,
         startLine: lineOf(start),
         endLine: lineOf(Math.max(start, last - 1)),
      };
   };
   const tokenText = (token: ParseToken) =>
      text.slice(map[token.startIndex], map[token.stopIndex + 1]);
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
   // The `#` notes a statement opens with, up to its first token that is not one.
   const leadingObjectNotes = (startCp: number, stopCp: number) => {
      const notes: {
         text: string;
         block: boolean;
         bodyTexts: string[];
         line: number;
         endLine: number;
      }[] = [];
      for (let i = firstTokenAt(startCp); i < tokens.length; i++) {
         const token = tokens[i];
         if (token.startIndex > stopCp) break;
         if (token.channel !== 0) continue;
         const name = symbolOf(token);
         if (name === "ANNOTATION") {
            notes.push({
               text: normalizeNewlines(tokenText(token)),
               block: false,
               bodyTexts: [],
               line: token.line,
               endLine: token.line,
            });
         } else if (name === "BLOCK_ANNOTATION_BEGIN") {
            const bodyTexts: string[] = [];
            let end = i + 1;
            while (
               end < tokens.length &&
               symbolOf(tokens[end]) === "BLOCK_ANNOTATION_TEXT"
            )
               bodyTexts.push(tokenText(tokens[end++]));
            if (
               end < tokens.length &&
               symbolOf(tokens[end]) === "BLOCK_ANNOTATION_END"
            )
               end++;
            notes.push({
               text: normalizeNewlines(tokenText(token)),
               block: true,
               bodyTexts,
               line: token.line,
               endLine: lineOf(map[tokens[end - 1].stopIndex]),
            });
            i = end - 1;
         } else break;
      }
      return notes;
   };
   // One pass over the markdown notes builds both fields, so `proseLines` can never disagree with `markdown`.
   const attachedProse = (
      notes: ReturnType<typeof leadingObjectNotes>,
      firstLine: number,
   ): { markdown?: string; proseLines?: [number, number][] } => {
      const segments: string[] = [];
      const proseLines: [number, number][] = [];
      let lineEnd = -2;
      for (const note of notes) {
         if (!isMarkdownNote(note.text)) {
            lineEnd = -2;
            continue;
         }
         proseLines.push([note.line - firstLine, note.endLine - firstLine]);
         if (note.block) {
            segments.push(markdownBlockBody(note.text, note.bodyTexts));
            lineEnd = -2;
         } else {
            const body = markdownLineBody(note.text);
            if (note.line === lineEnd + 1)
               segments[segments.length - 1] += `\n${body}`;
            else segments.push(body);
            lineEnd = note.line;
         }
      }
      return segments.length > 0
         ? { markdown: segments.join("\n\n"), proseLines }
         : {};
   };
   const attachedCaption = (
      notes: ReturnType<typeof leadingObjectNotes>,
   ): string | undefined => {
      const lines = notes
         .filter((note) => !note.block && routeOfNote(note.text) === '"')
         .map((note) => markdownLineBody(note.text).trim())
         .filter((line) => line !== "");
      return lines.length > 0 ? lines.join(" ") : undefined;
   };

   const covered: [number, number][] = [];
   const items: ReaderItem[] = [];
   const unclassifiable = (line: number, what: string): NotebookReadResult =>
      refuse({
         line,
         message: `Line ${line}: ${what}, which no notebook cell can hold, so the notebook is not shown. Fix: remove it, or rewrite it as an import, source:, query:, given:, type: or export statement, a run:, or a \`##(markdown)\` / \`##|(markdown)\` prose note.`,
      });

   for (let i = 0; i < (root.childCount ?? 0); i++) {
      const child = root.getChild(i);
      if (!isRuleNode(child)) {
         const token = child?.symbol;
         const name = token ? symbolOf(token) : undefined;
         if (token && name === "EOF") continue;
         if (token && name === "SEMI") {
            covered.push([token.startIndex, token.stopIndex]);
            continue;
         }
         return unclassifiable(
            token?.line ?? 1,
            "a token outside any statement",
         );
      }
      const span = spanOf(child);
      if (!span) return unclassifiable(1, "a statement with no readable range");
      const match = STATEMENT_ACCESSORS.find(
         ([accessor]) => callAccessor(child, accessor) !== undefined,
      );
      if (!match && callAccessor(child, "ignoredObjectAnnotations")) {
         const prose = leadingObjectNotes(span.startCp, span.stopCp).find(
            (note) => isMarkdownNote(note.text),
         );
         return refuse({
            line: span.startLine,
            message: prose
               ? `Line ${span.startLine}: a \`#(markdown)\` annotation that annotates no statement (an import and an export take none), so the notebook is not shown. Fix: ${attachedNowhereFix(prose.block)}`
               : `Line ${span.startLine}: a # tag that annotates no statement, so the notebook is not shown. Fix: move the tag directly above its run:, or write trailing prose as a \`##(markdown)\` note.`,
         });
      }
      if (!match) {
         const firstLine = text
            .slice(span.start, span.end)
            .split("\n")[0]
            .trim();
         return unclassifiable(
            span.startLine,
            `\`${firstLine}\` is a statement the notebook reader does not recognize`,
         );
      }
      covered.push([span.startCp, span.stopCp]);
      const [accessor, kind] = match;
      if (kind !== "notes") {
         const leading = leadingObjectNotes(span.startCp, span.stopCp);
         const caption = attachedCaption(leading);
         items.push({
            kind: "statement",
            run: kind === "run",
            text: text.slice(span.start, span.end),
            ...attachedProse(leading, span.startLine),
            ...(caption !== undefined && { caption }),
            startLine: span.startLine,
            endLine: span.endLine,
         });
         continue;
      }
      const group = callAccessor(child, accessor) as ParseNode;
      const notes = (callAccessor(group, "docAnnotation") ?? []) as ParseNode[];
      for (const noteNode of notes) {
         const noteSpan = spanOf(noteNode);
         if (!noteSpan) {
            return unclassifiable(
               span.startLine,
               "a note with no readable range",
            );
         }
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
         // A block's text ends with its body, as Malloy's own note text does: with the closer, MOTLY drops the tag.
         const lastBody = bodyTokens[bodyTokens.length - 1] ?? own[0];
         const noteText = normalizeNewlines(
            block && lastBody
               ? text
                    .slice(noteSpan.start, map[lastBody.stopIndex + 1])
                    .replace(/\r?\n$/, "")
               : text.slice(noteSpan.start, noteSpan.end),
         );
         const route = routeOfNote(noteText);
         let body: string | undefined;
         if (isProseRoute(route)) {
            body = block
               ? markdownBlockBody(
                    noteText.split("\n", 1)[0],
                    bodyTokens.map(tokenText),
                 )
               : markdownLineBody(noteText);
         }
         items.push({
            kind: "note",
            note: {
               text: noteText,
               route,
               body,
               block,
               startLine: noteSpan.startLine,
               endLine: noteSpan.endLine,
            },
         });
      }
   }

   // Both lists are in file order, so one forward sweep checks every token.
   let range = 0;
   for (const token of tokens) {
      // EOF spans nothing (its stop precedes its start), so no range can hold it.
      if (token.channel !== 0 || symbolOf(token) === "EOF") continue;
      while (range < covered.length && covered[range][1] < token.startIndex)
         range++;
      const [from, to] = covered[range] ?? [Infinity, -Infinity];
      if (token.startIndex < from || token.stopIndex > to) {
         return unclassifiable(token.line, "a token outside any statement");
      }
   }

   const artifactAt = items.findIndex(
      (item) => item.kind === "note" && isArtifactNoteText(item.note.text),
   );
   if (artifactAt < 0) {
      return refuse({
         line: 1,
         message:
            "Line 1: this notebook has no ## artifact note, so its header cannot be told from its cells. Fix: add `## artifact { kind=notebook }` after the ##! line.",
      });
   }

   const runCount = items.filter(
      (item) => item.kind === "statement" && item.run,
   ).length;
   if (runCount !== modelDef.queryList.length) {
      return refuse({
         line: 1,
         message: `Line 1: the notebook reader found ${runCount} run: statements where Malloy compiled ${modelDef.queryList.length}, so its query cells cannot be matched to their results. Fix: none in the file; report it, since the reader and the compiler disagree.`,
      });
   }

   const cells: NotebookCellSpan[] = [];
   const annotations: string[] = [];
   // The markdown cell a contiguous `##(markdown)` run is building, which the next adjacent line joins.
   let lineRun: NotebookCellSpan | undefined;
   let runsSeen = 0;
   items.forEach((item, index) => {
      const belowTag = index > artifactAt;
      if (item.kind === "statement") {
         lineRun = undefined;
         const queryIndex = item.run ? runsSeen++ : undefined;
         // A `run:` above the tag is header, so a definition cell, but it still holds its queryList slot.
         cells.push(
            item.run && belowTag
               ? {
                    kind: "query",
                    type: "code",
                    text: item.text,
                    ...(item.markdown !== undefined && {
                       markdown: item.markdown,
                       proseLines: item.proseLines,
                    }),
                    ...(item.caption !== undefined && {
                       caption: item.caption,
                    }),
                    startLine: item.startLine,
                    endLine: item.endLine,
                    queryIndex,
                 }
               : {
                    kind: "definition",
                    type: "code",
                    text: item.text,
                    ...(item.markdown !== undefined && {
                       markdown: item.markdown,
                       proseLines: item.proseLines,
                    }),
                    ...(item.caption !== undefined && {
                       caption: item.caption,
                    }),
                    startLine: item.startLine,
                    endLine: item.endLine,
                 },
         );
         return;
      }
      const { note } = item;
      const floating = belowTag && note.body !== undefined;
      if (!floating) annotations.push(note.text);
      if (!floating || note.body === undefined) {
         lineRun = undefined;
         return;
      }
      if (!note.block && lineRun && lineRun.endLine + 1 === note.startLine) {
         lineRun.text += `\n${note.body}`;
         lineRun.endLine = note.endLine;
         return;
      }
      const cell: NotebookCellSpan = {
         kind: "markdown",
         type: "markdown",
         text: note.body,
         startLine: note.startLine,
         endLine: note.endLine,
      };
      cells.push(cell);
      lineRun = note.block ? undefined : cell;
   });
   return { cells, annotations };
}

/** A dashboard text tile's name: a MOTLY bare word, as `tiles=[…]` spells its entries. */
export const TEXT_BLOCK_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * What follows `##|(markdown)` or `#|(markdown)` on a block's opener, and the
 * name when it is a lone bare word; undefined when the opener is not a
 * `(markdown)` block opener.
 */
export function parseMarkdownOpener(
   opener: string,
): { level: 1 | 2; rest: string; name?: string } | undefined {
   const line = opener.replace(/\r?\n$/, "");
   const sigil = /^(#{1,2})\|/.exec(line);
   if (!sigil) return undefined;
   // A `"` block is a description above a tag, so only `(text)` is read here as a tile.
   const route = routeOfNote(line);
   if (route !== MARKDOWN_ROUTE && !(sigil[1] === "##" && route === "text"))
      return undefined;
   const rest = payloadOf({ value: line } as Parameters<
      typeof payloadOf
   >[0]).trim();
   return {
      level: sigil[1].length as 1 | 2,
      rest,
      name: TEXT_BLOCK_NAME.test(rest) ? rest : undefined,
   };
}

/** A floating `##|(markdown) [name]` … `|##` block; in a dashboard, a text tile. */
export interface NotebookMarkdownBlock {
   /** Undefined when the opener has no name or more than one bare word. */
   name?: string;
   /** 1-based opener line and closer line (the last body line when unclosed). */
   line: number;
   endLine: number;
}

/** The floating `(markdown)` blocks of a parsed file, in file order. */
export function readMarkdownBlocks(
   parse: NotebookParse,
   text: string,
): NotebookMarkdownBlock[] {
   const stream = parse.tokenStream as TokenStreamShape | undefined;
   const vocabulary = stream?.tokenSource?.vocabulary;
   const tokens =
      typeof stream?.getTokens === "function" ? stream.getTokens() : undefined;
   if (!tokens || !vocabulary) return [];
   const map = codePointMap(text);
   const tokenText = (token: ParseToken) =>
      text.slice(map[token.startIndex], map[token.stopIndex + 1]);
   const blocks: NotebookMarkdownBlock[] = [];
   for (let i = 0; i < tokens.length; i++) {
      if (
         vocabulary.getSymbolicName(tokens[i].type) !==
         "DOC_BLOCK_ANNOTATION_BEGIN"
      )
         continue;
      const opener = parseMarkdownOpener(tokenText(tokens[i]));
      if (!opener) continue;
      const body: string[] = [];
      let endLine = tokens[i].line;
      for (
         let j = i + 1;
         j < tokens.length &&
         vocabulary.getSymbolicName(tokens[j].type) === "BLOCK_ANNOTATION_TEXT";
         j++
      ) {
         body.push(tokenText(tokens[j]));
         endLine = tokens[j].line;
      }
      const closer = tokens[i + 1 + body.length];
      if (
         closer &&
         vocabulary.getSymbolicName(closer.type) === "BLOCK_ANNOTATION_END"
      )
         endLine = closer.line;
      blocks.push({
         name: opener.name,
         line: tokens[i].line,
         endLine,
      });
   }
   return blocks;
}

/**
 * The compile problem for a served notebook the reader would refuse, so a
 * compile check (an agent's, or the builder's save gate) fails on a notebook
 * that would not open. Undefined for anything that is not a served notebook.
 */
export function notebookReaderProblem(
   modelPath: string,
   text: string,
   modelDef: ModelDef,
   url: string,
): LogMessage | undefined {
   if (!isNotebookModelPath(modelPath)) return undefined;
   if (artifactNoteLine(ownModelNoteObjects(modelDef)) === undefined)
      return undefined;
   const parse = parseNotebookText(text);
   const error = isNotebookReaderError(parse)
      ? parse
      : readNotebookCells(parse, modelDef, text).error;
   if (!error) return undefined;
   const line = error.line - 1;
   return {
      code: "notebook-cells-unreadable",
      severity: "error",
      message: error.message,
      at: {
         url,
         range: {
            start: { line, character: 0 },
            end: { line, character: 0 },
         },
      },
   };
}
