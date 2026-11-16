// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A Malloy file with only some of its top-level `source:` declarations left:
 * every other `source:`, `query:` and `run:` statement, with the annotation
 * lines above it, is blanked, so the file compiles without the connections
 * those declarations read.
 *
 * Read by the chained `storage=` build's text (the served copy's
 * `chainBuildFiles`), which compiles a downstream over its stored upstreams
 * in their destination alone. Blanking replaces each character with a space
 * and keeps every line break, so a position the compiler reported in the
 * author's file (a declaration's range, which the re-root rewriter edits by)
 * holds in the result. Imports, `##!` flags, `type:` and `given:` statements
 * are kept.
 *
 * Pure: undefined when the file does not lex, never a guess.
 */

import { definitionCutAt } from "./definition_cut";
import {
   lexAll,
   offsetOf,
   statementHead,
   statementStarts,
} from "./malloy_lexer";

/** The top-level statements a kept file may lose. */
const BLANKABLE = new Set(["source", "query", "run"]);

/**
 * `text` with every top-level `source:` statement that holds none of `keep`
 * (positions in `text`, each inside a declaration to keep), and every `query:`
 * and `run:` statement, blanked along with their annotation lines.
 */
export function keepDeclarations(
   text: string,
   keep: readonly { line: number; character: number }[],
): string | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const keepAt = keep.map((p) => offsetOf(text, p)).filter((o) => o >= 0);
   // Every top-level boundary: a statement's head, or an `import`.
   const heads: { start: number; keyword: string }[] = [];
   for (const k of statementStarts(tokens)) {
      if (tokens[k].depth !== 0) continue;
      heads.push({
         start: statementHead(tokens, k).start,
         keyword: tokens[k].text,
      });
   }
   for (const t of tokens) {
      if (t.depth === 0 && t.kind === "word" && t.text === "import") {
         heads.push({ start: t.start, keyword: "import" });
      }
   }
   heads.sort((a, b) => a.start - b.start);
   const chars = text.split("");
   let floor = 0;
   for (let i = 0; i < heads.length; i++) {
      const head = heads[i];
      const from = annotationsStart(text, floor, head.start);
      const nextHead = heads[i + 1]?.start ?? text.length;
      const to =
         i + 1 < heads.length
            ? annotationsStart(text, head.start, nextHead)
            : text.length;
      floor = to;
      if (!BLANKABLE.has(head.keyword)) continue;
      const kept =
         head.keyword === "source" &&
         keepAt.some((o) => o >= head.start && o < to);
      if (kept) continue;
      for (let c = from; c < to; c++) {
         if (chars[c] !== "\n") chars[c] = " ";
      }
   }
   return chars.join("");
}

/**
 * Where the annotation lines above the statement at `keyword` start: the
 * first of the `#` lines (not `##`) and blank lines directly above it, no
 * earlier than `floor`.
 */
function annotationsStart(
   text: string,
   floor: number,
   keyword: number,
): number {
   const lineStart = text.lastIndexOf("\n", keyword - 1) + 1;
   if (lineStart < floor || text.slice(lineStart, keyword).trim() !== "") {
      return keyword;
   }
   let start = lineStart;
   while (start > floor) {
      const prevStart = text.lastIndexOf("\n", start - 2) + 1;
      if (prevStart < floor) break;
      const line = text.slice(prevStart, start - 1).trim();
      if (line !== "" && !(line.startsWith("#") && !line.startsWith("##"))) {
         break;
      }
      start = prevStart;
   }
   return start;
}

/**
 * `text` with the one definition (a dimension, measure, join or view) that
 * holds `at` blanked, annotations and all, when it lies inside one of
 * `within` (character ranges of `text`); undefined when none does, or the
 * definition there is not one that can be left out (a `where:` never is).
 * Positions hold, as for {@link keepDeclarations}.
 */
export function leaveOutDefinitionAt(
   text: string,
   at: { line: number; character: number },
   within: readonly { start: number; end: number }[],
): string | undefined {
   const offset = offsetOf(text, at);
   if (offset < 0) return undefined;
   if (!within.some((w) => offset >= w.start && offset < w.end)) {
      return undefined;
   }
   const cut = definitionCutAt(text, offset);
   if (
      !cut ||
      cut.left.keyword === "source" ||
      cut.left.keyword === "query" ||
      !within.some((w) => cut.start >= w.start && cut.end <= w.end)
   ) {
      return undefined;
   }
   const chars = text.split("");
   for (let c = cut.start; c < cut.end; c++) {
      if (chars[c] !== "\n") chars[c] = " ";
   }
   return chars.join("");
}
