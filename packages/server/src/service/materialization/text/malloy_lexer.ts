// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The statement lexer the materialization rewriters read the author's files
 * with: the re-root rewriter (serve/served_copy.ts), the served copy's
 * leaving out of joins it moves across connections (cross_connection_text.ts)
 * and the text readers here. A thin layer over the one Malloy lexer core
 * (`service/malloy_lexer_core.ts`), which the caller-text reader shares: it
 * adds only brackets and their depth, which is all a statement-level edit
 * needs; it never parses Malloy.
 *
 * Its token API is internal to text/ and the module's text readers; a lexer
 * that cannot read the text returns undefined.
 *
 * Pure.
 */

import {
   isNameCode,
   isSpaceCode,
   StringScan,
   stripMalloyCommentsAndLiterals,
   unitsAt,
} from "../../malloy_lexer_core";

/** One lexical token, at the bracket depth it sits at. */
export interface Token {
   kind: "word" | "open" | "close" | "other";
   text: string;
   start: number;
   end: number;
   /** The brackets enclosing it; a bracket itself has the depth outside it. */
   depth: number;
}

/**
 * Every token of `s` with its depth, read as Malloy's own lexer reads it
 * (`service/malloy_lexer_core.ts`): comments and annotations (a `#|` block
 * one included) are skipped; a string or quoted identifier is one "other"
 * token, and so is a `"""` block, Malloy inside its `%{ … }` and all; a word
 * starts with a letter or `_` and continues with letters, digits and `_`, a
 * letter being any Unicode letter. Undefined when brackets do not balance or
 * a string or quoted identifier does not close where Malloy closes it.
 */
export function lexAll(s: string): Token[] | undefined {
   // Comments, annotations and string bodies blanked in place: every offset
   // and every quote, bracket and name is where it is in `s`.
   const code = stripMalloyCommentsAndLiterals(s);
   const scan = new StringScan(s);
   const out: Token[] = [];
   let depth = 0;
   let i = 0;
   const n = code.length;
   const push = (kind: Token["kind"], start: number, end: number, d = depth) =>
      out.push({ kind, text: s.slice(start, end), start, end, depth: d });
   while (i < n) {
      const c = code.charCodeAt(i);
      if (isSpaceCode(c, code, i)) {
         i += unitsAt(code, i);
         continue;
      }
      // A SQL block, Malloy in its `%{ … }` included, as one token.
      if (code.startsWith('"""', i)) {
         const end = sqlBlockEnd(code, i);
         if (end < 0) return undefined;
         push("other", i, end);
         i = end;
         continue;
      }
      // Strings and quoted identifiers, closed where Malloy closes them.
      if (c === 39 || c === 34 || c === 96) {
         const close = scan.stringClose(i);
         if (close < 0) return undefined;
         push("other", i, close + 1);
         i = close + 1;
         continue;
      }
      if (c === 123 || c === 40 || c === 91) {
         push("open", i, i + 1);
         depth++;
         i++;
         continue;
      }
      if (c === 125 || c === 41 || c === 93) {
         depth--;
         if (depth < 0) return undefined;
         push("close", i, i + 1);
         i++;
         continue;
      }
      if (startsWord(c, code, i)) {
         let j = i + unitsAt(code, i);
         while (j < n && isNameCode(code.charCodeAt(j), code, j)) {
            j += unitsAt(code, j);
         }
         push("word", i, j);
         i = j;
         continue;
      }
      const units = unitsAt(code, i);
      push("other", i, i + units);
      i += units;
   }
   return depth === 0 ? out : undefined;
}

/** Whether a word starts at `at`: a letter or `_`, never a digit. */
function startsWord(c: number, text: string, at: number): boolean {
   return c === 95 || (!(c >= 48 && c <= 57) && isNameCode(c, text, at));
}

/**
 * The index just past the `"""` closing the SQL block opening at `i` in
 * blanked text, stepping over the Malloy in each `%{ … }`; -1 when it never
 * closes. The block's SQL is blank there, so only its delimiters, its
 * interpolations and their brackets remain.
 */
function sqlBlockEnd(code: string, i: number): number {
   let at = i + 3;
   while (at < code.length) {
      if (code.startsWith('"""', at)) return at + 3;
      if (code.startsWith("%{", at)) {
         let depth = 1;
         at += 2;
         while (at < code.length && depth > 0) {
            const c = code.charCodeAt(at);
            if (c === 123) depth++;
            else if (c === 125) depth--;
            at++;
         }
         continue;
      }
      at++;
   }
   return -1;
}

/** Indexes in `tokens` of a word followed by `:` (not `::`) at its own depth. */
export function statementStarts(tokens: readonly Token[]): number[] {
   const starts: number[] = [];
   for (let k = 0; k + 1 < tokens.length; k++) {
      const t = tokens[k];
      if (
         t.kind === "word" &&
         tokens[k + 1].text === ":" &&
         tokens[k + 1].start === t.end &&
         tokens[k + 2]?.text !== ":"
      ) {
         starts.push(k);
      }
   }
   return starts;
}

/** The access modifiers a statement can be labelled with. */
const ACCESS_LABELS: ReadonlySet<string> = new Set([
   "public",
   "private",
   "internal",
]);

/**
 * Where the statement whose keyword is `tokens[k]` begins: its access label
 * (`internal dimension: …`) when it has one, else its keyword. The end of the
 * statement before it is here, so a cut or a replacement of that statement
 * never takes this one's label with it (which would expose or hide this one).
 */
export function statementHead(tokens: readonly Token[], k: number): Token {
   const label = tokens[k - 1];
   return label?.kind === "word" &&
      label.depth === tokens[k].depth &&
      ACCESS_LABELS.has(label.text)
      ? label
      : tokens[k];
}

/** Whether `t` can name a definition: a word or a quoted identifier. */
export function isName(t: Token | undefined): t is Token {
   return !!t && (t.kind === "word" || t.text.startsWith("`"));
}

/** The character offset of a zero-based line/character position, or -1. */
export function offsetOf(
   text: string,
   pos: { line: number; character: number },
): number {
   let offset = 0;
   let line = 0;
   while (line < pos.line) {
      const nl = text.indexOf("\n", offset);
      if (nl < 0) return -1;
      offset = nl + 1;
      line++;
   }
   const lineEnd = text.indexOf("\n", offset);
   const length = (lineEnd < 0 ? text.length : lineEnd) - offset;
   if (pos.character > length) return -1;
   return offset + pos.character;
}
