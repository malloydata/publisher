// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A Malloy file with only some of its top-level `source:` and `query:`
 * declarations left: every other `source:`, `query:` and `run:` statement, with the annotations
 * on it (`#|` blocks included), is blanked, so the file compiles without the connections
 * those declarations read.
 *
 * Read by the chained `storage=` build's text (the served copy's
 * `chainBuildFiles`), which compiles a downstream over its stored upstreams
 * in their destination alone. Blanking replaces each code point with a space
 * and keeps every line break, so a position the compiler reported in the
 * author's file (a declaration's range, which the re-root rewriter edits by)
 * holds in the result. Imports, `##!` flags, `type:` and `given:` statements
 * are kept.
 *
 * Pure: undefined when the file does not lex, never a guess.
 */

import {
   lexAll,
   statementAnnotationsFrom,
   statementHead,
   statementStarts,
} from "../../malloy_lexer_core";
import { definitionCutAt } from "./definition_cut";
import { offsetOf } from "./malloy_lexer";

/** The top-level statements a kept file may lose. */
const BLANKABLE = new Set(["source", "query", "run"]);

/**
 * `text` with every top-level `source:` or `query:` statement that holds none
 * of `keep` (positions in `text`, each inside a declaration to keep), and
 * every `run:` statement, blanked along with their annotations.
 */
export function keepDeclarations(
   text: string,
   keep: readonly { line: number; character: number }[],
): string | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const keepAt = keep.map((p) => offsetOf(text, p)).filter((o) => o >= 0);
   // Every top-level boundary, by the index of its first token: a
   // statement's head (its access label when it has one), or an `import`.
   const heads: { index: number; keyword: string }[] = [];
   for (const k of statementStarts(tokens)) {
      if (tokens[k].depth !== 0) continue;
      heads.push({
         index: statementHead(tokens, k) === tokens[k] ? k : k - 1,
         keyword: tokens[k].text,
      });
   }
   tokens.forEach((t, index) => {
      if (t.depth === 0 && t.kind === "word" && t.text === "import") {
         heads.push({ index, keyword: "import" });
      }
   });
   heads.sort((a, b) => a.index - b.index);
   const chars = text.split("");
   for (let i = 0; i < heads.length; i++) {
      const { index, keyword } = heads[i];
      if (!BLANKABLE.has(keyword)) continue;
      const start = tokens[index].start;
      // Through its own last token: the trivia after it is no statement's.
      const to = tokens[(heads[i + 1]?.index ?? tokens.length) - 1].end;
      const kept =
         keyword !== "run" && keepAt.some((o) => o >= start && o < to);
      if (kept) continue;
      const from = statementAnnotationsFrom(
         text,
         tokens[index - 1]?.end ?? 0,
         start,
      );
      blank(chars, from, to);
   }
   return chars.join("");
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
   blank(chars, cut.start, cut.end);
   return chars.join("");
}

/**
 * Blanks `chars[from..to)` in place, one space per code point (Malloy counts
 * columns in code points, so a `#|` opener later on the line stays where its
 * closer expects it), keeping every line break.
 */
function blank(chars: string[], from: number, to: number): void {
   for (let c = from; c < to; c++) {
      if (chars[c] === "\n") continue;
      const high = chars[c]!.charCodeAt(0);
      const low = c + 1 < to ? chars[c + 1]!.charCodeAt(0) : 0;
      const pair =
         high >= 0xd800 && high <= 0xdbff && low >= 0xdc00 && low <= 0xdfff;
      chars[c] = " ";
      if (pair) chars[++c] = "";
   }
}
