// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one Malloy lexer core: how Malloy's own lexer reads a text's names,
 * spaces, strings, comments, annotations and SQL blocks, character by
 * character, and the statement tokens built on that (`lexAll`). The readers
 * of Malloy text build on it: the caller-text reader (`query_text.ts`), which
 * reads untrusted text before it is compiled, the rewriters' statement lexer
 * (`materialization/text/malloy_lexer.ts`), which reads the author's files to
 * rewrite them, and discovery's reading of the annotations a source declares
 * (`entities/field_helpers.ts`). None is looser than
 * Malloy: a string ends at its line, a name may be any letter, `#|` opens a
 * block annotation, and `%{ … }` in a `"""` block is Malloy.
 *
 * Pure.
 */

/** `ID_CHAR` beyond ASCII; a name continues with it, `_` or an ASCII digit. */
const NAME_CHAR = /\p{Alphabetic}/u;
const SPACE_CHAR = /\s/u;

/** ASCII is decided by code; only a non-ASCII character pays for the regex. */
export function isNameCode(code: number, text: string, at: number): boolean {
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

export function isSpaceCode(code: number, text: string, at: number): boolean {
   if (code < 128) return code === 32 || (code >= 9 && code <= 13);
   return SPACE_CHAR.test(String.fromCodePoint(text.codePointAt(at)!));
}

/** UTF-16 units in the character at `at`. */
export function unitsAt(text: string, at: number): number {
   return text.codePointAt(at)! > 0xffff ? 2 : 1;
}

/**
 * `text` with every comment and every STRING-LITERAL BODY blanked out, so a
 * pattern scan over caller-authored Malloy sees only real syntax.
 *
 * Both halves are security-relevant, in opposite directions:
 *  - **Comments** can HIDE a declaration from a scan that a compiler still
 *    reads around (`source: mine is -- c\n X extend { … }`), or plant a decoy
 *    one that never compiles at all (`-- run: bogus`).
 *  - **String literals** can FORGE a declaration: a scan reading inside them
 *    lets `where: note = 'source: mine is open_src'` inject an alias edge
 *    that could relabel a derivation's base in a last-wins map.
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
 * can open a comment or a literal (scanning into `` `q'` `` would open a
 * phantom literal blanking every declaration after it).
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

/** An annotation as the lexer reads it: where it starts and ends, and whether it is a `##` one. */
interface AnnotationSpan {
   from: number;
   to: number;
   /** A `##` or `##|` model annotation, which tags no statement. */
   model: boolean;
}

/**
 * The annotations from `from` to `to` that tag what follows `to`: the `#` and
 * `##` lines and the `#|` / `##|` blocks, read as {@link skipAnnotation} reads
 * them, past spaces and comments. Anything else in the span ends the run of
 * annotations before it, so only those after the last such character count.
 */
function annotationsBefore(
   text: string,
   from: number,
   to: number,
): AnnotationSpan[] {
   let found: AnnotationSpan[] = [];
   const keep = (): void => undefined;
   let i = from;
   while (i < to) {
      const code = text.charCodeAt(i);
      if (isSpaceCode(code, text, i)) {
         i += unitsAt(text, i);
         continue;
      }
      let end = -1;
      if (code === 35) {
         end = skipAnnotation(text, i, keep);
         if (end >= 0) {
            found.push({ from: i, to: end, model: text.startsWith("##", i) });
         }
      } else if (text.startsWith("--", i) || text.startsWith("//", i)) {
         end = toEndOfLine(text, i);
      } else if (text.startsWith("/*", i)) {
         const close = text.indexOf("*/", i + 2);
         end = close === -1 ? text.length : close + 2;
      }
      if (end < 0) {
         found = [];
         i += unitsAt(text, i);
         continue;
      }
      i = end;
   }
   return found;
}

/**
 * Where the annotations tagging the statement at `at` start, `#|` blocks
 * included: `at` when it has none. `floor` is where the token before it ends,
 * so only trivia lies between; a `##` annotation is the file's, so the run
 * stops at one.
 */
export function statementAnnotationsFrom(
   text: string,
   floor: number,
   at: number,
): number {
   let from = at;
   for (const tag of annotationsBefore(text, floor, at).reverse()) {
      if (tag.model) break;
      from = tag.from;
   }
   return from;
}

/** Whether the line starting at `at` closes a block annotation opened in `column`. */
export function closesBlock(
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
export class StringScan {
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

/**
 * Indexes in `tokens` of a word followed by `:` (not `::`) at its own depth.
 * Malloy lets whitespace, a line break included, sit between a keyword and
 * its colon, and uses a lone `:` nowhere else.
 */
export function statementStarts(tokens: readonly Token[]): number[] {
   const starts: number[] = [];
   for (let k = 0; k + 1 < tokens.length; k++) {
      const t = tokens[k];
      if (
         t.kind === "word" &&
         tokens[k + 1].text === ":" &&
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
      ACCESS_LABELS.has(label.text.toLowerCase())
      ? label
      : tokens[k];
}
