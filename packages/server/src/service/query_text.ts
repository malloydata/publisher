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
 * Every reader first blanks comments, `#` annotations and string-literal
 * bodies ({@link stripMalloyCommentsAndLiterals}). The run-target and alias
 * readers then match a pattern; the derivation, join and `is`-edge readers walk
 * one linear tokenization with paired brackets. All of them read a bare
 * Unicode name and a backtick-quoted one (`customer-orders`) and return the
 * name without backticks, keyed the same way sources are.
 */

/**
 * A bare or backtick-quoted Malloy identifier, capturing the bare form and the
 * quoted form's inner name in two consecutive groups. `\w` is ASCII-only, so
 * the bare form uses Unicode property classes (`café` is a legal name).
 */
const IDENT = String.raw`(?:\x60([^\x60]+)\x60|([\p{L}\p{N}_]+))`;

/**
 * A regex over caller text whose keywords match in any case, as Malloy's lexer
 * reads them: `RUN:` compiles, and a lowercase-only reader let it skip every
 * pre-compile check keyed on the run target. Identifiers are captured verbatim.
 */
function keywordPattern(pattern: string, flags = ""): RegExp {
   return new RegExp(pattern, `iu${flags}`);
}

/**
 * The base recorded for `NAME is …` when what follows cannot be read, so the
 * name stays unproven rather than proven by some other edge. A backtick cannot
 * occur inside a Malloy name, so no model source can be called this.
 */
export const UNREADABLE_BASE = "`";

/**
 * The top-level source a `run:` / `->` query targets, or undefined when the
 * text has no recognizable run target.
 */
export function extractRunTargetSourceName(query?: string): string | undefined {
   if (!query) return undefined;
   // The `run:` form does NOT require a following `->`. A run target can be an
   // expression over the name — `run: locked extend { … } -> { … }`, or a
   // refinement of a named query, `run: locked_q + { … }` — and requiring `->`
   // right after the identifier missed both. Those were the shapes that skipped
   // the pre-compile gate and got their compile errors back (a column-name and
   // column-type oracle on a source the caller is denied on) while the compiled
   // backstop denied them a moment later. Anchoring on `run:` is what keeps this
   // safe to widen: the identifier after it is the run target or nothing.
   const runMatch = query.match(keywordPattern(String.raw`run\s*:\s*${IDENT}`));
   // The bare leading-`->` form still requires the arrow. Without it this would
   // match the first word of any statement (`source`, `query`, …) and resolve a
   // keyword as the run target.
   const arrowMatch = query.match(
      keywordPattern(String.raw`^\s*${IDENT}\s*->`, "m"),
   );
   return runMatch?.[1] ?? runMatch?.[2] ?? arrowMatch?.[1] ?? arrowMatch?.[2];
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
   const text = stripMalloyCommentsAndLiterals(query);
   // The same identifier and declaration shapes {@link buildDerivationBaseMap}
   // reads, for the same reason: each is legal grammar the compiler links and a
   // narrower pattern silently declined to. `\w` is ASCII-only, so `café` went
   // unmatched; a parameter list after the name and a parenthesised base are
   // both ordinary. Missing an edge here is silent in the unsafe direction --
   // no filter is injected and the caller sees unfiltered rows with no error --
   // so the pattern matching the compiler's reading is what the guarantee rests
   // on. Deliberately still `source:`-only and single-valued: this feeds
   // `resolveFilterSource`, which needs exactly one base to inject from.
   const declRe = keywordPattern(
      String.raw`source\s*:\s*${IDENT}(?:\s*\([^)]*\))?\s+is\s*\(?\s*${IDENT}`,
      "g",
   );
   let match: RegExpExecArray | null;
   while ((match = declRe.exec(text)) !== null) {
      aliasOf.set(match[1] ?? match[2], match[3] ?? match[4]);
   }
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
 * Comments are recognized outside literals only, and literals outside
 * comments only, in ONE left-to-right pass — a `--` inside a string is not a
 * comment, and a `'` inside a comment does not open a string. Line comments
 * (`--`, `//`) and `#` annotations run to the newline; block comments
 * (slash-star) to their close,
 * or to end of input when unterminated.
 *
 * Backtick-quoted identifiers are PRESERVED and SKIPPED WHOLE: unlike
 * `'…'`/`"…"` they carry real names (`` source: `my-src` is X ``) that the scan
 * must see, and Malloy lexes the span as a single token, so nothing inside it
 * can open a comment or a literal. Scanning into one was a bypass in its own
 * right — a legal `` dimension: `q'` is 1 `` opened a phantom literal that
 * blanked every declaration after it.
 *
 * Replacement is space-for-character, so every offset, line, and token
 * boundary in the result matches the input.
 */
export function stripMalloyCommentsAndLiterals(text: string): string {
   // Unblanked text is copied in slices between blanked spans, never per char.
   const parts: string[] = [];
   let copied = 0;
   const blank = (from: number, to: number): void => {
      const end = Math.min(to, text.length);
      if (from >= end) return;
      parts.push(text.slice(copied, from));
      let run = from;
      for (let k = from; k < end; k++) {
         if (text.charCodeAt(k) === 10) {
            parts.push(" ".repeat(k - run), "\n");
            run = k + 1;
         }
      }
      parts.push(" ".repeat(end - run));
      copied = end;
   };
   // startsWith rather than a per-char slice: a 1MB body must not allocate a string per character.
   for (let i = 0; i < text.length; i++) {
      // A `#` annotation is read around like a comment, so text in one can
      // neither hide a declaration nor plant a decoy.
      if (
         text.startsWith("--", i) ||
         text.startsWith("//", i) ||
         text[i] === "#"
      ) {
         const nl = text.indexOf("\n", i);
         const end = nl === -1 ? text.length : nl;
         blank(i, end);
         i = end;
         continue;
      }
      if (text.startsWith("/*", i)) {
         const close = text.indexOf("*/", i + 2);
         const end = close === -1 ? text.length : close + 2;
         blank(i, end);
         i = end - 1;
         continue;
      }
      const ch = text[i];
      if (ch === "`") {
         // A backtick-quoted IDENTIFIER is one token to Malloy's lexer, so
         // nothing inside it can open a comment or a string literal. Skipped
         // WHOLESALE rather than scanned: reading into it let a `'`, `--`,
         // `//` or a block-comment opener in a perfectly legal name
         // (`` source: `a'` is … ``, `` dimension: `q'` is 1 ``) blank real
         // syntax that the compiler went on to read — including every
         // declaration AFTER it, which turned an innocuous field name into a
         // full laundering bypass. Contents are preserved for the same reason
         // they were before: a backticked span carries a real name the
         // declaration scan must see.
         const close = text.indexOf("`", i + 1);
         // Unterminated: the compiler will not accept this text either, so
         // there is nothing further worth scanning.
         i = close === -1 ? text.length : close;
         continue;
      }
      if (ch === "'" || ch === '"') {
         // Blank the BODY, keep both delimiters, so the result still parses as
         // a string where one was and no adjacent tokens are glued together.
         let j = i + 1;
         while (j < text.length && text[j] !== ch) {
            // Malloy escapes a quote inside a literal with a backslash; skip
            // the escaped character so it cannot close the literal early.
            if (text[j] === "\\") j++;
            j++;
         }
         blank(i + 1, j);
         i = j;
         continue;
      }
   }
   parts.push(text.slice(copied));
   return parts.join("");
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
   const tokens = tokenize(stripMalloyCommentsAndLiterals(query));
   for (let i = 0; i < tokens.count; i++) {
      if (!isStatementKeyword(tokens, i)) continue;
      const keyword = textOf(tokens, i).toLowerCase();
      if (keyword !== "source" && keyword !== "query") continue;
      readStatementItems(tokens, i + 2, false, (name, base) =>
         addEdge(out, name, base),
      );
   }
   return out;
}

const PUNCT = 0;
const NAME = 1;
const QUOTED = 2;

/**
 * Stripped Malloy text as names and single punctuation characters, held in
 * flat arrays rather than one object per token. A token's text is
 * `source.slice(start, end)`: a name without its backticks, or the character.
 */
interface Tokens {
   source: string;
   count: number;
   kind: Uint8Array;
   start: Int32Array;
   end: Int32Array;
   /** A bracket's partner index; -1 for anything else or an unbalanced one. */
   partner: Int32Array;
}

const NAME_CHAR = /[\p{L}\p{N}_]/u;
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
   };
   const push = (kind: number, start: number, end: number): number => {
      const index = tokens.count++;
      tokens.kind[index] = kind;
      tokens.start[index] = start;
      tokens.end[index] = end;
      return index;
   };
   const open: number[] = [];
   let i = 0;
   while (i < text.length) {
      const code = text.charCodeAt(i);
      if (isSpaceCode(code, text, i)) {
         i += unitsAt(text, i);
         continue;
      }
      if (code === 96) {
         const close = text.indexOf("`", i + 1);
         const end = close === -1 ? text.length : close;
         push(QUOTED, i + 1, end);
         i = end + 1;
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
   return tokens.source.slice(tokens.start[i], tokens.end[i]);
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

function isName(tokens: Tokens, i: number): boolean {
   return i >= 0 && i < tokens.count && tokens.kind[i] !== PUNCT;
}

/** An unquoted name whose lower-cased text is `word`. */
function isWord(tokens: Tokens, i: number, word: string): boolean {
   return (
      i >= 0 &&
      i < tokens.count &&
      tokens.kind[i] === NAME &&
      tokens.end[i] - tokens.start[i] === word.length &&
      textOf(tokens, i).toLowerCase() === word
   );
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
   return isName(tokens, j) ? textOf(tokens, j) : UNREADABLE_BASE;
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
