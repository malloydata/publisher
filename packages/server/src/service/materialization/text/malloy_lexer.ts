// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A small lexer over Malloy text for the materialization rewriters: the
 * re-root rewriter (serve/served_copy.ts) and the build view that leaves out
 * joins across connections (plan/cross_connection_joins.ts). It knows only
 * brackets, words, strings and comments, which is all a statement-level edit
 * needs; it never parses Malloy.
 *
 * Its token API is internal to text/ and the module's text readers; a lexer
 * that cannot read the text returns undefined.
 *
 * Pure.
 */

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
 * Every token of `s` with its depth. Comments and `#` annotation lines are
 * skipped; a string or quoted identifier is one "other" token. Undefined when
 * brackets do not balance or a string or comment does not close.
 */
export function lexAll(s: string): Token[] | undefined {
   const out: Token[] = [];
   let depth = 0;
   let i = 0;
   const n = s.length;
   const push = (kind: Token["kind"], start: number, end: number, d = depth) =>
      out.push({ kind, text: s.slice(start, end), start, end, depth: d });
   while (i < n) {
      const c = s[i];
      const next = s[i + 1];
      // Comments, and annotations (`#` to the end of the line).
      if (
         (c === "-" && next === "-") ||
         (c === "/" && next === "/") ||
         c === "#"
      ) {
         const nl = s.indexOf("\n", i);
         i = nl < 0 ? n : nl + 1;
         continue;
      }
      if (c === "/" && next === "*") {
         const close = s.indexOf("*/", i + 2);
         if (close < 0) return undefined;
         i = close + 2;
         continue;
      }
      // Strings and quoted identifiers.
      if (c === '"' && s.startsWith('"""', i)) {
         const close = s.indexOf('"""', i + 3);
         if (close < 0) return undefined;
         push("other", i, close + 3);
         i = close + 3;
         continue;
      }
      if (c === "'" || c === '"' || c === "`") {
         const end = quotedEnd(s, i, c);
         if (end < 0) return undefined;
         push("other", i, end);
         i = end;
         continue;
      }
      if (c === "{" || c === "(" || c === "[") {
         push("open", i, i + 1);
         depth++;
         i++;
         continue;
      }
      if (c === "}" || c === ")" || c === "]") {
         depth--;
         if (depth < 0) return undefined;
         push("close", i, i + 1);
         i++;
         continue;
      }
      if (/[A-Za-z_]/.test(c)) {
         let j = i + 1;
         while (j < n && /[A-Za-z0-9_]/.test(s[j])) j++;
         push("word", i, j);
         i = j;
         continue;
      }
      if (!/\s/.test(c)) push("other", i, i + 1);
      i++;
   }
   return depth === 0 ? out : undefined;
}

/** The index just past the closing `quote` of a string starting at `start`. */
function quotedEnd(s: string, start: number, quote: string): number {
   let i = start + 1;
   while (i < s.length) {
      const c = s[i];
      if (c === "\\") {
         i += 2;
         continue;
      }
      if (c === quote) return i + 1;
      i++;
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
