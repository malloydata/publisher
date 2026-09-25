// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Pure parsing of caller-authored Malloy query text.
 *
 * The authorization gate, filter inheritance, and the query boundary all need
 * to identify the run target and follow `source: NAME is BASE` derivation
 * chains *before* the query compiles — a denied caller must never reach
 * compilation. These helpers are deliberately side-effect free (no model
 * state) so the readers behind those security checks can be unit tested in
 * isolation from the stateful `Model`.
 *
 * Every reader first blanks comments, annotations and string-literal bodies
 * ({@link stripMalloyCommentsAndLiterals}), then walks one linear tokenization
 * with paired brackets. Both follow `MalloyLexer.g4`: keywords match in any
 * case, ASCII letters only (a regex `iu` flag would also fold `ſ` and `K`,
 * which the lexer does not); a bare name is `ID_CHAR` (`\p{Alphabetic}` or
 * `_`) followed by those or ASCII digits; and a backtick-quoted name is
 * decoded with the same `parseString` Malloy uses, so `` `\locked` `` is
 * `locked`. Names are keyed the same way sources are.
 */

import { ParseUtil } from "@malloydata/malloy-tag";
import type { LogMessage } from "@malloydata/malloy";

/**
 * The base recorded for `NAME is …` when what follows cannot be read, so the
 * name stays unproven rather than proven by some other edge. A backtick cannot
 * occur inside a Malloy name, so no model source can be called this.
 */
export const UNREADABLE_BASE = "`";

/**
 * The top-level source a `run:` / `->` query targets, or undefined when the
 * text has no recognizable run target.
 *
 * The LAST `run:`, because Malloy executes `queryList[length - 1]`. A reader
 * that must refuse on any statement the text compiles, not only the one it
 * runs, wants {@link extractRunTargetSourceNames}. Strips its own input, for
 * the reason {@link buildSourceAliasMap} gives.
 */
export function extractRunTargetSourceName(query?: string): string | undefined {
   return extractRunTargetSourceNames(query).at(-1);
}

/**
 * Every `run:` statement's target, in order; with no `run:`, the name opening a
 * bare leading `NAME ->` line, if any.
 *
 * The `run:` form does NOT require a following `->`: a run target can be an
 * expression over the name (`run: locked extend { … } -> { … }`, `run:
 * locked_q + { … }`), and requiring the arrow let those skip every pre-compile
 * check. The name after `run:` (behind any `(`) is the run target or nothing.
 * The bare form still requires the arrow, or it would read the first word of
 * any statement (`source`, `query`, …) as a run target.
 */
export function extractRunTargetSourceNames(query?: string): string[] {
   if (!query) return [];
   const tokens = tokenize(stripMalloyCommentsAndLiterals(query));
   const targets: string[] = [];
   for (let k = 0; k + 1 < tokens.count; k++) {
      if (!isWord(tokens, k, "run") || !textIs(tokens, k + 1, ":")) continue;
      let j = k + 2;
      while (textIs(tokens, j, "(") && tokens.kind[j] === PUNCT) j++;
      if (isName(tokens, j)) targets.push(textOf(tokens, j));
   }
   if (targets.length > 0) return targets;
   for (let k = 0; k + 2 < tokens.count; k++) {
      if (
         isName(tokens, k) &&
         textIs(tokens, k + 1, "-") &&
         textIs(tokens, k + 2, ">") &&
         tokens.start[k + 2] === tokens.start[k + 1] + 1 &&
         opensLine(tokens, k)
      ) {
         return [textOf(tokens, k)];
      }
   }
   return [];
}

/** Whether token `k` is the first on its line. */
function opensLine(tokens: Tokens, k: number): boolean {
   if (k === 0) return true;
   const from = tokens.end[k - 1];
   const to = tokens.start[k];
   for (let at = from; at < to; at++) {
      const code = tokens.source.charCodeAt(at);
      if (code === 10 || code === 13) return true;
   }
   return false;
}

/**
 * Every identifier in `text` outside comments and string literals, with a
 * backticked name read whole (`` `orders-staging` `` is one name, not two).
 */
export function malloyIdentifiers(text: string): string[] {
   return [...scanIdentifiers(stripMalloyCommentsAndLiterals(text))];
}

/**
 * Map each ad-hoc source alias to the base it derives from
 * (`source: NAME is BASE …` → NAME → BASE). Used to walk derivation chains in
 * caller-authored text for filter inheritance -- a filter-protected source
 * carries its filter requirements when read under a derived name.
 *
 * `source:`-only and last-declaration-wins. It reads STRIPPED text, so the two
 * misreads this map used to carry are closed: a declaration spelled inside a
 * string literal is no longer an edge that last-wins could use to REPLACE a
 * real base, and a comment between `is` and the base no longer ERASES one.
 *
 * Those misreads are why the query boundary stopped reading this map: a replaced
 * edge re-pointed a name from the hidden base it really derives from to a
 * curated one and bought admission. That boundary still uses
 * {@link buildDerivationBaseMap}, which is set-valued and refuses on ambiguity;
 * this map stays last-wins and single-valued because
 * {@link Model.resolveFilterSource} needs ONE source name to inject filters
 * from. Prefer that one on any path where an edge grants access.
 *
 * Strips its own input rather than trusting the caller to have done it. A
 * documented "pass me stripped text" precondition would hold only until the
 * next caller, and the failure is silent in the unsafe direction -- a missed
 * edge means no filter is injected, on a path with no post-compile backstop.
 * {@link stripMalloyCommentsAndLiterals} blanks to spaces, so it is idempotent
 * and a caller that already stripped pays a second scan and nothing else.
 */
export function buildSourceAliasMap(query: string): Map<string, string> {
   const aliasOf = new Map<string, string>();
   // The same statement reader {@link buildDerivationBaseMap} uses, for the
   // same reason: a missed edge here is silent in the unsafe direction -- no
   // filter is injected and the caller sees unfiltered rows with no error.
   // Deliberately still `source:`-only and single-valued: this feeds
   // `resolveFilterSource`, which needs exactly one base to inject from.
   readDefinitions(query, (keyword, name, base) => {
      if (keyword === "source" && base !== UNREADABLE_BASE) {
         aliasOf.set(name, base);
      }
   });
   return aliasOf;
}

/**
 * `text` with every comment and every STRING-LITERAL BODY blanked out, so a
 * pattern scan over caller-authored Malloy sees only real syntax.
 *
 * Both halves are security-relevant, in opposite directions:
 *  - **Comments** can HIDE a declaration from a scan that a compiler still
 *    reads around (`source: mine is -- c\n X extend { … }`), or plant a decoy
 *    one that never compiles at all (`-- run: bogus`).
 *  - **String literals** can FORGE a declaration. A scan that reads inside
 *    them lets `where: note = 'source: mine is open_src'` inject an alias
 *    edge, which — if a later edge for a name could overwrite an earlier one —
 *    would let a caller relabel its own derivation's base. (The alias map
 *    below keeps EVERY base per name rather than the last, so forging is
 *    additive and cannot erase a real edge; blanking literals closes it at
 *    the source as well.)
 *
 * One left-to-right pass that follows `MalloyLexer.g4`, so a `--` inside a
 * string is not a comment and a `'` inside a comment does not open a string:
 *  - `--` / `//` comments and `#` / `##` annotations run to the line's end;
 *    a block comment (slash-star) to its close, or to end of input when
 *    unterminated.
 *  - A `#|` / `##|` block annotation consumes its opener's line and every line
 *    after it until one whose closer (`|#` / `|##`) sits in the opener's
 *    column with only spaces or tabs before it; `|##` does not close `#|`.
 *  - A `'`, `"` or backtick string cannot contain a raw newline or control
 *    character; one that does not close is a single stray character to the
 *    lexer, and what follows it is syntax. A backslash escapes the next
 *    character.
 *  - `f` / `s` / `r` prefixed literals (`f'…'`, `f'''…'''`, `f"""…"""`,
 *    `` f`…` ``, `s'…'`, `r'…'`, `/'…'`) are the lexer's filter, raw and regex
 *    strings; the body is blanked and the prefix letter kept, so it can read
 *    as a stray name. A plain `'''` is `''` followed by an ordinary string.
 *  - A triple-quoted `"""` SQL block runs to its own closing `"""`, and the
 *    Malloy inside a `%{ … }` in it is read as Malloy.
 *
 * Backtick-quoted identifiers are PRESERVED and SKIPPED WHOLE: unlike
 * `'…'`/`"…"` they carry real names (`` source: `my-src` is X ``) that the scan
 * must see, and Malloy lexes the span as a single token, so nothing inside it
 * can open a comment or a literal. Scanning into one was a bypass in its own
 * right — a legal `` dimension: `q'` is 1 `` opened a phantom literal that
 * blanked every declaration after it.
 *
 * Replacement is character-for-character (a space, or `~` where a space would
 * let a second pass read a delimiter differently), so every offset, line, and
 * token boundary in the result matches the input, and a second pass is a no-op.
 */
export function stripMalloyCommentsAndLiterals(text: string): string {
   // Request bodies are JSON, so the type annotation alone doesn't bound the loops below.
   if (typeof text !== "string")
      throw new TypeError("query text must be a string");
   // Unblanked text is copied in slices between blanked spans, never per char.
   const parts: string[] = [];
   let copied = 0;
   // Control characters are kept, with the backslashes that escape them: they
   // end a string, so blanking one would let a later reader close a string or
   // backtick name the lexer never closed, or leave open one it did close.
   const blank = (from: number, to: number): void => {
      const end = Math.min(to, text.length);
      if (from >= end) return;
      parts.push(text.slice(copied, from));
      let run = from;
      for (let k = from; k < end; k++) {
         if (text.charCodeAt(k) < 32) {
            let slashes = k;
            while (slashes > run && text.charCodeAt(slashes - 1) === 92)
               slashes--;
            parts.push(" ".repeat(slashes - run), text.slice(slashes, k + 1));
            run = k + 1;
         }
      }
      let tail = end;
      if (end < text.length && text.charCodeAt(end) < 32) {
         while (tail > run && text.charCodeAt(tail - 1) === 92) tail--;
      }
      parts.push(" ".repeat(tail - run), text.slice(tail, end));
      copied = end;
   };
   const inert = (from: number, to: number): void => {
      parts.push(text.slice(copied, from), "~".repeat(to - from));
      copied = to;
   };
   const scan = new StringScan(text);
   // Brace depths at which a `%{ … }` returns to its enclosing SQL block.
   const sqlResume: number[] = [];
   let depth = 0;
   // Blanks SQL from `from`; returns where code resumes.
   const sql = (from: number): number => {
      const stop = sqlStop(text, from);
      blank(from, stop.bodyEnd);
      if (stop.opensCode) {
         sqlResume.push(depth);
         depth++;
      }
      return stop.end;
   };
   // An `f` / `s` that opened no literal, and the last character blanked since.
   // Blanked trivia becomes spaces a second pass would read as the prefix's
   // own, so a quote after it gets an inert mark in the last blanked position.
   let prefixAt = -1;
   let blankedSince = -1;
   const marks: number[] = [];
   const trivia = (from: number, to: number): void => {
      blank(from, to);
      if (prefixAt < 0) return;
      for (let k = Math.min(to, text.length) - 1; k >= from; k--) {
         if (text.charCodeAt(k) >= 32) {
            blankedSince = k;
            break;
         }
      }
   };
   // startsWith rather than a per-char slice: a 1MB body must not allocate a string per character.
   let i = 0;
   while (i < text.length) {
      const code = text.charCodeAt(i);
      if (text.startsWith("--", i) || text.startsWith("//", i) || code === 35) {
         const end =
            code === 35
               ? skipAnnotation(text, i, trivia)
               : lineTrivia(text, i, trivia);
         if (end >= 0) {
            i = end;
            continue;
         }
      } else if (text.startsWith("/*", i)) {
         const close = text.indexOf("*/", i + 2);
         const end = close === -1 ? text.length : close + 2;
         trivia(i, end);
         i = end;
         continue;
      }
      if (prefixAt >= 0 && !isLexerSpace(code)) {
         if ((code === 39 || code === 34 || code === 96) && blankedSince >= 0) {
            marks.push(blankedSince);
         }
         prefixAt = -1;
         blankedSince = -1;
      }
      if (code === 123) {
         depth++;
         i++;
         continue;
      }
      if (code === 125) {
         depth--;
         i++;
         if (
            sqlResume.length > 0 &&
            sqlResume[sqlResume.length - 1] === depth
         ) {
            sqlResume.pop();
            i = sql(i);
         }
         continue;
      }
      if (text.startsWith('"""', i)) {
         i = sql(i + 3);
         continue;
      }
      if (code === 39 || code === 34 || code === 96) {
         const close = scan.stringClose(i);
         if (close < 0) {
            i++;
            continue;
         }
         // A backtick span is a name, kept whole; a quote's body is blanked.
         if (code !== 96) blank(i + 1, close);
         i = close + 1;
         continue;
      }
      const prefixed = scan.prefixedLiteral(i);
      if (prefixed) {
         const { open, quote, bodyEnd, end } = prefixed;
         // The prefix stays: without it a second pass could fuse the quotes
         // with a neighbor's (`s""` + `"` reading as `"""`).
         // A backtick delimiter left in place would pair with a later one and
         // read as a name, so it is written as a character nothing reads.
         const tick = text.charCodeAt(open) === 96;
         if (tick) inert(open, open + quote);
         blank(open + quote, bodyEnd);
         if (tick && text.charCodeAt(bodyEnd) === 96) inert(bodyEnd, end);
         i = end;
         continue;
      }
      const letter = code | 32;
      if ((letter === 102 || letter === 115) && !continuesToken(text, i)) {
         prefixAt = i;
      }
      i++;
   }
   parts.push(text.slice(copied));
   let out = parts.join("");
   for (const at of marks) out = `${out.slice(0, at)}~${out.slice(at + 1)}`;
   return out;
}

/** Blanks the `--` / `//` comment at `i`, returning the index after it, or -1. */
function lineTrivia(
   text: string,
   i: number,
   blank: (from: number, to: number) => void,
): number {
   const end = toEndOfLine(text, i);
   if (end >= 0) blank(i, end);
   return end;
}

/** The index of the `\n` or `\r` ending the line `i` is on, or the text's end. */
function lineEnd(text: string, i: number): number {
   let at = i;
   while (at < text.length) {
      const code = text.charCodeAt(at);
      if (code === 10 || code === 13) return at;
      at++;
   }
   return at;
}

/**
 * The lexer's `F_TO_EOL` from `i`: where the line's terminator starts, or -1
 * when a lone `\r` ends it, which no to-end-of-line token can consume — the
 * lexer then reads the opening character on its own and the rest as syntax.
 */
function toEndOfLine(text: string, i: number): number {
   const end = lineEnd(text, i);
   if (text.charCodeAt(end) === 13 && text.charCodeAt(end + 1) !== 10) {
      return -1;
   }
   return end;
}

/** The index after the line terminator of the line `i` is on. */
function nextLine(text: string, i: number): number {
   const end = lineEnd(text, i);
   if (text.charCodeAt(end) === 13 && text.charCodeAt(end + 1) === 10) {
      return end + 2;
   }
   return Math.min(end + 1, text.length);
}

/** Blanks the annotation at `i` (a `#`), returning the index after it, or -1. */
function skipAnnotation(
   text: string,
   i: number,
   blank: (from: number, to: number) => void,
): number {
   const end = toEndOfLine(text, i);
   if (end < 0) return -1;
   const closer = text.startsWith("##|", i)
      ? "|##"
      : text.startsWith("#|", i)
        ? "|#"
        : undefined;
   if (!closer) {
      blank(i, end);
      return end;
   }
   // The lexer counts the opener's column in code points from the last `\n`.
   let column = 0;
   for (let at = text.lastIndexOf("\n", i - 1) + 1; at < i; at++) {
      if ((text.charCodeAt(at) & 0xfc00) !== 0xdc00) column++;
   }
   let at = nextLine(text, i);
   blank(i, at);
   while (at < text.length) {
      const closes = closesBlock(text, at, column, closer);
      const after = nextLine(text, at);
      blank(at, after);
      at = after;
      if (closes) break;
   }
   return at;
}

/** Whether the line starting at `at` closes a block annotation opened in `column`. */
function closesBlock(
   text: string,
   at: number,
   column: number,
   closer: string,
): boolean {
   for (let c = 0; c < column; c++) {
      const code = text.charCodeAt(at + c);
      if (code !== 32 && code !== 9) return false;
   }
   const from = at + column;
   if (!text.startsWith(closer, from)) return false;
   return closer !== "|#" || text.charCodeAt(from + 2) !== 35;
}

/**
 * Where a SQL block's body starting at `from` stops: before its closing `"""`
 * (`end` after it) or a `%{` that opens Malloy (`end` after it), else the end
 * of text. `SQL_CHAR` is ambiguous — a backslash is read alone or with the
 * next character — so every reading is followed at once, as the lexer's ATN
 * does, and the terminator furthest along any reading wins, as the lexer's
 * longest match does. A reading continues past a terminator only through a
 * pair that swallows its first character, so the scan ends where the block
 * does.
 */
function sqlStop(
   text: string,
   from: number,
): { bodyEnd: number; end: number; opensCode: boolean } {
   // Reachable offsets; a reading advances at most three, so four slots do.
   const reach = [false, false, false, false];
   let furthest = from;
   const mark = (at: number): void => {
      reach[at & 3] = true;
      furthest = Math.max(furthest, at);
   };
   mark(from);
   let stop: { bodyEnd: number; end: number; opensCode: boolean } = {
      bodyEnd: text.length,
      end: text.length,
      opensCode: false,
   };
   let found = false;
   for (let j = from; j < text.length && j <= furthest; j++) {
      if (!reach[j & 3]) continue;
      reach[j & 3] = false;
      const code = text.charCodeAt(j);
      const next = text.charCodeAt(j + 1);
      if (code === 34 && next === 34 && text.charCodeAt(j + 2) === 34) {
         if (!found || j + 3 > stop.end) {
            stop = { bodyEnd: j, end: j + 3, opensCode: false };
         }
         found = true;
         continue;
      }
      if (code === 37 && next === 123) {
         if (!found || j + 2 > stop.end) {
            stop = { bodyEnd: j, end: j + 2, opensCode: true };
         }
         found = true;
         continue;
      }
      if (code === 92) {
         mark(j + 1);
         mark(j + 2);
      } else if (code === 34) {
         mark(next === 34 ? j + 3 : j + 2);
      } else if (code === 37) {
         mark(j + 2);
      } else {
         mark(j + 1);
      }
   }
   return stop;
}

/** The lexer's `SPACE_CHAR`, which may sit between a literal's prefix and its quote. */
function isLexerSpace(code: number): boolean {
   return (
      code === 32 ||
      code === 9 ||
      code === 10 ||
      code === 11 ||
      code === 13 ||
      code === 160
   );
}

/**
 * String scans over one text. A scan that fails records where it failed, so a
 * later opener of the same kind inside that span fails without rescanning:
 * any such opener was an escaped character of the failed scan, which leaves
 * both scans in step from there. That keeps a 1MB body linear.
 */
class StringScan {
   private stringFailedAt = [-1, -1, -1];
   private tripleUnclosedFrom = [Infinity, Infinity, Infinity];

   constructor(private readonly text: string) {}

   /**
    * The closing quote of the `'`, `"` or backtick string opening at `i`, as
    * the lexer's `SQ_STRING` / `DQ_STRING` / `BQ_STRING`, or -1.
    */
   stringClose(i: number): number {
      const text = this.text;
      const quote = text.charCodeAt(i);
      const slot = quoteSlot(quote);
      if (i < this.stringFailedAt[slot]) return -1;
      let j = i + 1;
      while (j < text.length) {
         const code = text.charCodeAt(j);
         if (code === quote) return j;
         if (code === 92) {
            if (j + 1 >= text.length || text.charCodeAt(j + 1) === 10) break;
            j += 2;
            continue;
         }
         if (code < 32 && code !== 9) break;
         j++;
      }
      this.stringFailedAt[slot] = j;
      return -1;
   }

   /**
    * A prefixed literal starting at `i`: an `f` / `s` / `r` letter (or a `/`
    * before `'`) that opens a token, then its quote. `open` is the first quote,
    * `quote` its length, `bodyEnd` where the body stops, `end` after the token.
    */
   prefixedLiteral(
      i: number,
   ):
      | { open: number; quote: number; bodyEnd: number; end: number }
      | undefined {
      const text = this.text;
      const code = text.charCodeAt(i);
      if (code === 47) {
         if (text.charCodeAt(i + 1) !== 39) return undefined;
         return this.raw(i + 1, 39, false);
      }
      const letter = code | 32;
      if (letter !== 102 && letter !== 114 && letter !== 115) return undefined;
      if (continuesToken(text, i)) return undefined;
      if (letter === 114) {
         return text.charCodeAt(i + 1) === 39
            ? this.raw(i + 1, 39, false)
            : undefined;
      }
      let open = i + 1;
      while (open < text.length && isLexerSpace(text.charCodeAt(open))) open++;
      const quote = text.charCodeAt(open);
      if (letter === 115) {
         return quote === 39 || quote === 34
            ? this.raw(open, quote, true)
            : undefined;
      }
      if (quote !== 39 && quote !== 34 && quote !== 96) return undefined;
      if (
         text.charCodeAt(open + 1) === quote &&
         text.charCodeAt(open + 2) === quote
      ) {
         const close = this.tripleClose(open + 3, quote);
         if (close >= 0) {
            return { open, quote: 3, bodyEnd: close, end: close + 3 };
         }
      }
      return this.raw(open, quote, true);
   }

   /** `RAW_CHAR*?` then the quote, or (with `endsAtNewline`) a consumed `\n`. */
   private raw(
      open: number,
      quote: number,
      endsAtNewline: boolean,
   ):
      | { open: number; quote: number; bodyEnd: number; end: number }
      | undefined {
      const text = this.text;
      let j = open + 1;
      while (j < text.length) {
         const code = text.charCodeAt(j);
         if (code === 10) {
            return endsAtNewline
               ? { open, quote: 1, bodyEnd: j, end: j + 1 }
               : undefined;
         }
         if (code === 92) {
            if (j + 1 >= text.length || text.charCodeAt(j + 1) === 10) {
               return undefined;
            }
            j += 2;
            continue;
         }
         if (code === quote) return { open, quote: 1, bodyEnd: j, end: j + 1 };
         j++;
      }
      return undefined;
   }

   /** The first index of the closing triple for a body starting at `body`, or -1. */
   private tripleClose(body: number, quote: number): number {
      const text = this.text;
      const slot = quoteSlot(quote);
      if (body >= this.tripleUnclosedFrom[slot]) return -1;
      let j = body;
      while (j < text.length) {
         const code = text.charCodeAt(j);
         if (code === 92) {
            j += 2;
            continue;
         }
         if (
            code === quote &&
            text.charCodeAt(j + 1) === quote &&
            text.charCodeAt(j + 2) === quote
         ) {
            return j;
         }
         j++;
      }
      this.tripleUnclosedFrom[slot] = body;
      return -1;
   }
}

function quoteSlot(quote: number): number {
   return quote === 39 ? 0 : quote === 34 ? 1 : 2;
}

/** Whether the character before `i` is part of a token that `i` would continue. */
function continuesToken(text: string, i: number): boolean {
   if (i === 0) return false;
   let at = i - 1;
   if ((text.charCodeAt(at) & 0xfc00) === 0xdc00 && at > 0) at--;
   const code = text.charCodeAt(at);
   // `$name` is one given reference.
   return code === 36 || isNameCode(code, text, at);
}

/**
 * Every base each ad-hoc alias in `text` may derive from — `source: NAME is
 * BASE` and `query: NAME is BASE` — as NAME → set of BASEs.
 *
 * Built to over-collect on purpose:
 *  - `query:` declarations are included, so a `query:` hop between a
 *    derivation and the `run:` cannot break the chain;
 *  - a name maps to a SET, keeping every base declared for it rather than the
 *    last, so a second (forged or shadowing) declaration can only add a base
 *    to check, never replace the real one.
 *
 * Read by BOTH the authorize gate and the query boundary, which want opposite
 * things from it, so the quantifier -- not the map -- is what carries the
 * direction. The authorize gate denies if ANY branch reaches a gated source,
 * so an extra edge widens DENIAL and over-collection is trivially safe. The
 * boundary admits only if EVERY base proves curated, so an extra edge adds an
 * obligation rather than discharging one, and over-collection is safe there
 * too. A future caller that reads this map with an ANY-branch ADMISSION
 * quantifier would invert that and turn a forged edge into a bypass.
 *
 * Does NOT replace {@link buildSourceAliasMap}, which survives for
 * `resolveFilterSource`'s filter-inheritance walk.
 *
 * Every item of a statement is read (`source: a is x extend {} b is y`), with
 * an optional parameter list after the name and any number of `(` before the
 * base; a name whose base cannot be read gets {@link UNREADABLE_BASE}, so a
 * decoy elsewhere cannot stand in for the declaration it missed.
 *
 * Strips its own input, for the reason {@link buildSourceAliasMap} gives: a
 * documented precondition holds only until the next caller, and the strip is
 * idempotent, so a caller that already stripped pays a second scan and nothing
 * else. The stakes are lower here than there -- this map is set-valued and its
 * callers refuse on ambiguity rather than picking one -- but the argument for
 * owning the guarantee rather than documenting it is the same.
 */
export function buildDerivationBaseMap(
   query: string,
): Map<string, Set<string>> {
   const out = new Map<string, Set<string>>();
   readDefinitions(query, (_keyword, name, base) => addEdge(out, name, base));
   return out;
}

/**
 * Every `NAME is BASE` item of every `source:` / `query:` statement outside a
 * brace block, in order: a `query:` spelled inside `extend { … }` names a
 * view, not a model-namespace name. Strips its own input.
 */
function readDefinitions(
   query: string,
   onItem: (keyword: "source" | "query", name: string, base: string) => void,
): void {
   const tokens = tokenize(stripMalloyCommentsAndLiterals(query));
   let depth = 0;
   for (let i = 0; i < tokens.count; i++) {
      if (isPunct(tokens, i, "{")) depth++;
      else if (isPunct(tokens, i, "}")) depth--;
      if (depth !== 0 || !isStatementKeyword(tokens, i)) continue;
      const keyword = isWord(tokens, i, "source")
         ? "source"
         : isWord(tokens, i, "query")
           ? "query"
           : undefined;
      if (!keyword) continue;
      readStatementItems(tokens, i + 2, false, (name, base) =>
         onItem(keyword, name, base),
      );
   }
}

const PUNCT = 0;
const NAME = 1;
const QUOTED = 2;

/**
 * Stripped Malloy text as names and single punctuation characters, held in
 * flat arrays rather than one object per token. A token's text is
 * `source.slice(start, end)`: a name without its backticks, or the character;
 * a backtick name with an escape in it is read from `decoded` instead.
 */
interface Tokens {
   source: string;
   count: number;
   kind: Uint8Array;
   start: Int32Array;
   end: Int32Array;
   /** A bracket's partner index; -1 for anything else or an unbalanced one. */
   partner: Int32Array;
   decoded: Map<number, string>;
}

/** `ID_CHAR` beyond ASCII; a name continues with it, `_` or an ASCII digit. */
const NAME_CHAR = /\p{Alphabetic}/u;
const SPACE_CHAR = /\s/u;

/** ASCII is decided by code; only a non-ASCII character pays for the regex. */
function isNameCode(code: number, text: string, at: number): boolean {
   if (code < 128) {
      return (
         (code >= 48 && code <= 57) ||
         (code >= 65 && code <= 90) ||
         (code >= 97 && code <= 122) ||
         code === 95
      );
   }
   return NAME_CHAR.test(String.fromCodePoint(text.codePointAt(at)!));
}

function isSpaceCode(code: number, text: string, at: number): boolean {
   if (code < 128) return code === 32 || (code >= 9 && code <= 13);
   return SPACE_CHAR.test(String.fromCodePoint(text.codePointAt(at)!));
}

/** UTF-16 units in the character at `at`. */
function unitsAt(text: string, at: number): number {
   return text.codePointAt(at)! > 0xffff ? 2 : 1;
}

/**
 * Split stripped text into names and punctuation in one pass, pairing
 * brackets, so every reader below walks tokens instead of retrying a pattern.
 * A backtick span is one name token, so nothing inside it can read as syntax.
 */
function tokenize(text: string): Tokens {
   const size = text.length + 1;
   const tokens: Tokens = {
      source: text,
      count: 0,
      kind: new Uint8Array(size),
      start: new Int32Array(size),
      end: new Int32Array(size),
      partner: new Int32Array(size).fill(-1),
      decoded: new Map(),
   };
   const push = (kind: number, start: number, end: number): number => {
      const index = tokens.count++;
      tokens.kind[index] = kind;
      tokens.start[index] = start;
      tokens.end[index] = end;
      return index;
   };
   const open: number[] = [];
   const scan = new StringScan(text);
   let i = 0;
   while (i < text.length) {
      const code = text.charCodeAt(i);
      if (isSpaceCode(code, text, i)) {
         i += unitsAt(text, i);
         continue;
      }
      // A backtick that does not close on its line is one stray character.
      const close = code === 96 ? scan.stringClose(i) : -1;
      if (close >= 0) {
         const index = push(QUOTED, i + 1, close);
         const inner = text.slice(i + 1, close);
         if (inner.includes("\\")) {
            tokens.decoded.set(index, ParseUtil.parseString(inner));
         }
         i = close + 1;
         continue;
      }
      if (isNameCode(code, text, i)) {
         let j = i + unitsAt(text, i);
         while (j < text.length && isNameCode(text.charCodeAt(j), text, j)) {
            j += unitsAt(text, j);
         }
         push(NAME, i, j);
         i = j;
         continue;
      }
      const units = unitsAt(text, i);
      const index = push(PUNCT, i, i + units);
      if (code === 40 || code === 91 || code === 123) {
         open.push(index);
      } else if (code === 41 || code === 93 || code === 125) {
         const at = open.pop();
         if (at !== undefined) {
            tokens.partner[at] = index;
            tokens.partner[index] = at;
         }
      }
      i += units;
   }
   return tokens;
}

function textOf(tokens: Tokens, i: number): string {
   return (
      tokens.decoded.get(i) ??
      tokens.source.slice(tokens.start[i], tokens.end[i])
   );
}

/** Whether token `i` exists and its text is the one character `char`. */
function textIs(tokens: Tokens, i: number, char: string): boolean {
   return (
      i >= 0 &&
      i < tokens.count &&
      tokens.end[i] - tokens.start[i] === 1 &&
      tokens.source.charCodeAt(tokens.start[i]) === char.charCodeAt(0)
   );
}

function isPunct(tokens: Tokens, i: number, char: string): boolean {
   return textIs(tokens, i, char) && tokens.kind[i] === PUNCT;
}

function isName(tokens: Tokens, i: number): boolean {
   return i >= 0 && i < tokens.count && tokens.kind[i] !== PUNCT;
}

/** An unquoted name that is the lowercase `word` in any ASCII case. */
function isWord(tokens: Tokens, i: number, word: string): boolean {
   if (
      i < 0 ||
      i >= tokens.count ||
      tokens.kind[i] !== NAME ||
      tokens.end[i] - tokens.start[i] !== word.length
   ) {
      return false;
   }
   const from = tokens.start[i];
   for (let k = 0; k < word.length; k++) {
      const code = tokens.source.charCodeAt(from + k);
      const lower = code >= 65 && code <= 90 ? code + 32 : code;
      if (lower !== word.charCodeAt(k)) return false;
   }
   return true;
}

/** `name:` opening a statement, and not a `::` type or a `.` path. */
function isStatementKeyword(tokens: Tokens, i: number): boolean {
   return (
      i < tokens.count &&
      tokens.kind[i] === NAME &&
      textIs(tokens, i + 1, ":") &&
      !textIs(tokens, i + 2, ":") &&
      !textIs(tokens, i - 1, ":") &&
      !textIs(tokens, i - 1, ".")
   );
}

/** The base after an `is` at `i - 1`: behind any `(`, else unreadable. */
function readBase(tokens: Tokens, i: number): string {
   let j = i;
   while (textIs(tokens, j, "(") && tokens.kind[j] === PUNCT) j++;
   if (!isName(tokens, j) || isLiteralPrefix(tokens, j)) return UNREADABLE_BASE;
   return textOf(tokens, j);
}

/** A kept `f` / `s` / `r` whose literal body was blanked after it. */
function isLiteralPrefix(tokens: Tokens, i: number): boolean {
   if (
      !(
         isWord(tokens, i, "f") ||
         isWord(tokens, i, "s") ||
         isWord(tokens, i, "r")
      )
   ) {
      return false;
   }
   return ["'", '"', "`", "~"].some((quote) => isPunct(tokens, i + 1, quote));
}

/**
 * Walk one statement's items at its own bracket level, from `start` until a
 * `;`, the closing bracket around it, or the next statement keyword, jumping
 * over every bracketed group. Reports `NAME is BASE` (and `NAME(params) is
 * BASE`) items, and with `shorthand` a bare name that opens an item.
 */
function readStatementItems(
   tokens: Tokens,
   start: number,
   shorthand: boolean,
   onItem: (name: string, base: string) => void,
): void {
   let atItemStart = true;
   let i = start;
   while (i < tokens.count) {
      if (tokens.kind[i] === PUNCT) {
         if (textIs(tokens, i, ";")) return;
         if (tokens.partner[i] > i) {
            i = tokens.partner[i] + 1;
            atItemStart = false;
            continue;
         }
         if (
            textIs(tokens, i, ")") ||
            textIs(tokens, i, "]") ||
            textIs(tokens, i, "}")
         ) {
            return;
         }
         atItemStart = textIs(tokens, i, ",");
         i++;
         continue;
      }
      if (isStatementKeyword(tokens, i)) return;
      let k = i + 1;
      if (textIs(tokens, k, "(") && tokens.partner[k] > k) {
         k = tokens.partner[k] + 1;
      }
      if (isWord(tokens, k, "is")) {
         onItem(textOf(tokens, i), readBase(tokens, k + 1));
         i = k + 1;
      } else {
         if (shorthand && atItemStart) {
            const name = textOf(tokens, i);
            onItem(name, name);
         }
         i++;
      }
      atItemStart = false;
   }
}

function addEdge(
   map: Map<string, Set<string>>,
   name: string,
   base: string,
): void {
   const bases = map.get(name) ?? new Set<string>();
   bases.add(base);
   map.set(name, bases);
}

/**
 * Every base each `join_one:` / `join_many:` / `join_cross:` alias in `query`
 * is declared over, as ALIAS → set of BASEs; a shorthand item (`join_one:
 * gated on …`) maps the name to itself.
 *
 * Kept apart from {@link buildDerivationBaseMap}: a join alias is field-scoped,
 * not a model-namespace name, so merged it would add edges to any caller
 * source that happens to share the alias and over-deny a query that never
 * reads through them.
 *
 * Reads each join statement's items at its own bracket level until the
 * statement ends (`;`, a closing bracket, or the next `keyword:`), so a
 * comma-separated or whitespace-separated later item is read too, and an
 * `extend { … }` or argument list inside one is not. A shorthand item is
 * recognized only first or after a comma, where it cannot be an `on`
 * expression's identifier. Best effort, and unanchored to what runs: a join in
 * a declaration the query never uses is read too, which over-denies in the
 * pre-compile pass it feeds and admits nothing. Strips its own input, for the
 * reason {@link buildSourceAliasMap} gives.
 */
export function buildJoinBaseMap(query: string): Map<string, Set<string>> {
   const out = new Map<string, Set<string>>();
   const tokens = tokenize(stripMalloyCommentsAndLiterals(query));
   for (let i = 0; i < tokens.count; i++) {
      if (!isStatementKeyword(tokens, i)) continue;
      if (!/^join_(?:one|many|cross)$/i.test(textOf(tokens, i))) continue;
      readStatementItems(tokens, i + 2, true, (alias, base) =>
         addEdge(out, alias, base),
      );
   }
   return out;
}

/**
 * Every `NAME is BASE` edge anywhere in `query`, whatever statement it sits in,
 * as NAME → set of BASEs.
 *
 * For the one decision that has only text to go on: the base of a caller join
 * the compiler left no `sourceID` on (an inline `x extend { … }`). A join alias
 * may be declared again in another scope, so a scan that missed the real
 * declaration would let a decoy's base stand in for it; this one has no
 * statement structure to misread. Over-collects on purpose: its reader
 * requires EVERY base to prove out, so an extra edge adds an obligation and
 * never discharges one.
 */
export function buildIsEdgeMap(query: string): Map<string, Set<string>> {
   const out = new Map<string, Set<string>>();
   const tokens = tokenize(stripMalloyCommentsAndLiterals(query));
   for (let k = 1; k < tokens.count; k++) {
      if (!isWord(tokens, k, "is")) continue;
      let n = k - 1;
      if (
         textIs(tokens, n, ")") &&
         tokens.kind[n] === PUNCT &&
         tokens.partner[n] >= 0
      ) {
         n = tokens.partner[n] - 1;
      }
      if (!isName(tokens, n)) continue;
      addEdge(out, textOf(tokens, n), readBase(tokens, k + 1));
   }
   return out;
}

/** Every bare and decoded backtick name in `text`. Does not itself skip strings. */
export function scanIdentifiers(text: string): Set<string> {
   const tokens = tokenize(text);
   const names = new Set<string>();
   for (let i = 0; i < tokens.count; i++) {
      if (isName(tokens, i)) names.add(textOf(tokens, i));
   }
   return names;
}

/**
 * Every name the pre-compile lock should decide: the union of a scan of the
 * raw text and of the stripped text. Either view can hide a name the other
 * still shows (a string the stripper blanks, a span the raw scan reads as one
 * name), and a missed locked name is a schema oracle. Over-collection refuses
 * only a caller the lock already refuses on that name.
 */
export function collectIdentifierNames(text: string): Set<string> {
   const names = scanIdentifiers(text);
   for (const name of scanIdentifiers(stripMalloyCommentsAndLiterals(text))) {
      names.add(name);
   }
   return names;
}

/**
 * One compile problem as a caller can act on it: `line L:C message` when it is
 * located (1-based, the compiler's own columns), the bare message otherwise.
 * Shared so the query 400 and the dashboard-write `Error` body format a problem
 * the same way rather than drifting apart.
 */
export function formatProblem(problem: {
   message: string;
   at?: { range?: { start?: { line?: number; character?: number } } };
}): string {
   const start = problem.at?.range?.start;
   return start?.line === undefined
      ? problem.message
      : `line ${start.line + 1}:${(start.character ?? 0) + 1} ${problem.message}`;
}

/**
 * Re-express compile problems for query text in the coordinates of the text the
 * caller sent.
 *
 * The server compiles `prefix + callerText (+ appended refinement)`, so the
 * compiler's line numbers are shifted by the prefix's line count. A problem in
 * the caller's own lines is moved back by that many lines; one that falls
 * outside them (in the prefix, or in a refinement the server appended, such as
 * an injected source filter) keeps its message and loses its location, since no
 * span of the caller's payload produced it. A problem in any other document
 * keeps no location either.
 *
 * The compiled document is recognized by the `internal://` URL the compiler
 * gives text that has none of its own; restricted mode forbids `import`, so no
 * other document can contribute a problem located in caller-written text.
 */
export function locateProblemsInCallerText(
   problems: readonly LogMessage[],
   callerText: string,
   prefixLines: number,
): LogMessage[] {
   const callerLines = callerText.split("\n").length;
   return problems.map((problem) => {
      const at = problem.at;
      if (!at) return problem;
      const start = at.range.start.line - prefixLines;
      const end = at.range.end.line - prefixLines;
      const inCallerText =
         at.url.startsWith("internal://") && start >= 0 && end < callerLines;
      if (!inCallerText) {
         const { at: _dropped, ...unlocated } = problem;
         return unlocated;
      }
      return {
         ...problem,
         at: {
            ...at,
            range: {
               start: { ...at.range.start, line: start },
               end: { ...at.range.end, line: end },
            },
         },
      };
   });
}
