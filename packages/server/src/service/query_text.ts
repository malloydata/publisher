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
import {
   isNameCode,
   isSpaceCode,
   StringScan,
   stripMalloyCommentsAndLiterals,
   unitsAt,
} from "./malloy_lexer_core";

export {
   closesBlock,
   stripMalloyCommentsAndLiterals,
} from "./malloy_lexer_core";

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

/**
 * A query that starts from a named view, read from its text: `run: ops ->
 * by_carrier`, refined (`ops -> by_carrier + { limit: 3 }`), or with later
 * stages (`ops -> by_carrier -> { … }`). The names are as written; whether
 * `source` is a source and `view` one of its views is the model's to say.
 */
export interface RunViewCall {
   source: string;
   view: string;
   /** A `+ { … }` refinement follows the view. */
   refined: boolean;
   /** How many `-> …` stages follow the view's own. */
   laterStages: number;
   /**
    * Every name the text spells outside the call's head, backticks removed
    * (a preamble's, a refinement's, a later stage's).
    */
   otherNames: ReadonlySet<string>;
}

/**
 * Every name the caller's text spells outside the call's own head (the
 * source and view of `src -> v`, or the query of `run: q`), backticks
 * removed: a preamble statement's, a refinement's and a later stage's. A
 * stored result's variant redeclares the run target's name over the stored
 * result, so the leg reads this to decline a text that names it again (a
 * join back to the source, a preamble source built over it).
 */
function namesOutside(
   tokens: Tokens,
   headFrom: number,
   headTo: number,
): ReadonlySet<string> {
   const names = new Set<string>();
   for (let k = 0; k < tokens.count; k++) {
      if (k >= headFrom && k <= headTo) continue;
      if (isName(tokens, k)) names.add(textOf(tokens, k));
   }
   return names;
}

/**
 * The view the LAST run statement starts from (Malloy runs the last), or
 * undefined when its first stage is not a bare view name: a hand-written
 * stage (`-> { … }`), a source with arguments or a refinement of its own
 * (`ops extend { … } -> …`), or anything this reader does not follow.
 *
 * An exact call (`ops -> by_carrier`, nothing after it) is also recognized
 * from the compiled result schema (`resultExplore.name`); this reader is the
 * half the schema cannot give, a query that only starts from the view
 * (materialization v2 plan, Ownership, P14). Strips its own input.
 */
export function extractRunViewCall(query?: string): RunViewCall | undefined {
   if (!query) return undefined;
   const tokens = tokenize(stripMalloyCommentsAndLiterals(query));
   let start = -1;
   for (let k = 0; k + 1 < tokens.count; k++) {
      if (isWord(tokens, k, "run") && textIs(tokens, k + 1, ":")) start = k + 2;
   }
   if (start < 0) {
      // The bare form, as extractRunTargetSourceNames reads it.
      for (let k = 0; k + 2 < tokens.count; k++) {
         if (
            isName(tokens, k) &&
            arrowAt(tokens, k + 1) &&
            opensLine(tokens, k)
         ) {
            start = k;
            break;
         }
      }
   }
   if (start < 0 || !isName(tokens, start) || !arrowAt(tokens, start + 1)) {
      return undefined;
   }
   const viewAt = start + 3;
   if (!isName(tokens, viewAt)) return undefined;
   let k = viewAt + 1;
   let refined = false;
   if (textIs(tokens, k, "+") && tokens.kind[k] === PUNCT) {
      if (!textIs(tokens, k + 1, "{") || tokens.partner[k + 1] < 0) {
         return undefined;
      }
      refined = true;
      k = tokens.partner[k + 1] + 1;
   }
   let laterStages = 0;
   while (arrowAt(tokens, k)) {
      const stage = k + 2;
      if (textIs(tokens, stage, "{") && tokens.partner[stage] >= 0) {
         k = tokens.partner[stage] + 1;
      } else if (isName(tokens, stage)) {
         k = stage + 1;
      } else {
         return undefined;
      }
      laterStages++;
   }
   // The statement must end here: the next statement, or the text's end.
   if (k < tokens.count && !(isName(tokens, k) && textIs(tokens, k + 1, ":"))) {
      return undefined;
   }
   return {
      source: textOf(tokens, start),
      view: textOf(tokens, viewAt),
      refined,
      laterStages,
      otherNames: namesOutside(tokens, start, viewAt),
   };
}

/**
 * A query that starts from a named query, read from its text: `run: q`,
 * refined (`run: q + { limit: 3 }`), or with later stages (`run: q -> { … }`).
 * The name is as written; whether it names a query is the model's to say.
 */
export interface RunQueryCall {
   query: string;
   /** A `+ { … }` refinement follows the name. */
   refined: boolean;
   /** How many `-> …` stages follow the query's own. */
   laterStages: number;
   /**
    * Every name the text spells outside the call's head, backticks removed
    * (a preamble's, a refinement's, a later stage's).
    */
   otherNames: ReadonlySet<string>;
}

/**
 * The named query the LAST run statement starts from (Malloy runs the last),
 * or undefined when it does not read as one: `run: NAME` (backticked or not),
 * `run: NAME + { … }`, `run: NAME -> …` with any number of later stages, or a
 * bare `NAME -> …` line with no `run:`. Anything else this reader does not
 * follow (a parenthesized name, `NAME + other`, an `extend`) is undefined, so
 * the caller answers it as written. Strips its own input.
 *
 * The persisted-query leg's half of an exact call's recognition: the compiled
 * result schema cannot tell `run: q` from `run: q + { … }` (both are named
 * `q`), so the text decides, and the compiled query's name and declaring file
 * confirm it (materialization, persisted named queries).
 */
export function extractRunQueryCall(query?: string): RunQueryCall | undefined {
   if (!query) return undefined;
   const tokens = tokenize(stripMalloyCommentsAndLiterals(query));
   let start = -1;
   for (let k = 0; k + 1 < tokens.count; k++) {
      if (isWord(tokens, k, "run") && textIs(tokens, k + 1, ":")) start = k + 2;
   }
   if (start < 0) {
      // The bare form, as extractRunTargetSourceNames reads it.
      for (let k = 0; k + 2 < tokens.count; k++) {
         if (
            isName(tokens, k) &&
            arrowAt(tokens, k + 1) &&
            opensLine(tokens, k)
         ) {
            start = k;
            break;
         }
      }
   }
   if (start < 0 || !isName(tokens, start)) return undefined;
   let k = start + 1;
   let refined = false;
   if (textIs(tokens, k, "+") && tokens.kind[k] === PUNCT) {
      if (!textIs(tokens, k + 1, "{") || tokens.partner[k + 1] < 0) {
         return undefined;
      }
      refined = true;
      k = tokens.partner[k + 1] + 1;
   }
   let laterStages = 0;
   while (arrowAt(tokens, k)) {
      const stage = k + 2;
      if (textIs(tokens, stage, "{") && tokens.partner[stage] >= 0) {
         k = tokens.partner[stage] + 1;
      } else if (isName(tokens, stage)) {
         k = stage + 1;
      } else {
         return undefined;
      }
      laterStages++;
   }
   // The statement must end here: the next statement, or the text's end.
   if (k < tokens.count && !(isName(tokens, k) && textIs(tokens, k + 1, ":"))) {
      return undefined;
   }
   return {
      query: textOf(tokens, start),
      refined,
      laterStages,
      otherNames: namesOutside(tokens, start, start),
   };
}

/** A compiled object's location in its file: zero-based lines and UTF-16 columns. */
export interface TextRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/**
 * The text of the definition `name is …` that `range` spans in `fileText`, as
 * written, or undefined unless it reads as exactly that: the name (backticked
 * or not), `is`, then a body whose every bracket pairs inside the span. For a
 * compiled field whose location is its definition (`view: v is { … }` spans
 * `v is { … }`), so the definition can be compiled again elsewhere.
 */
export function definitionTextAt(
   fileText: string,
   range: TextRange,
   name: string,
): string | undefined {
   const offsetOf = (at: { line: number; character: number }) => {
      let offset = 0;
      for (let line = 0; line < at.line; line++) {
         const next = fileText.indexOf("\n", offset);
         if (next < 0) return -1;
         offset = next + 1;
      }
      return offset + at.character;
   };
   const from = offsetOf(range.start);
   const to = offsetOf(range.end);
   if (from < 0 || to <= from || to > fileText.length) return undefined;
   const text = fileText.slice(from, to);
   const tokens = tokenize(stripMalloyCommentsAndLiterals(text));
   if (
      tokens.count < 3 ||
      !isName(tokens, 0) ||
      textOf(tokens, 0) !== name ||
      !isWord(tokens, 1, "is")
   ) {
      return undefined;
   }
   for (let k = 0; k < tokens.count; k++) {
      if (tokens.kind[k] !== PUNCT) continue;
      const code = tokens.source.charCodeAt(tokens.start[k]);
      const bracket =
         code === 40 ||
         code === 41 ||
         code === 91 ||
         code === 93 ||
         code === 123 ||
         code === 125;
      if (bracket && tokens.partner[k] < 0) return undefined;
   }
   return text;
}

/** Whether tokens `k` and `k + 1` are an adjacent `->`. */
function arrowAt(tokens: Tokens, k: number): boolean {
   return (
      textIs(tokens, k, "-") &&
      tokens.kind[k] === PUNCT &&
      textIs(tokens, k + 1, ">") &&
      tokens.kind[k + 1] === PUNCT &&
      tokens.start[k + 1] === tokens.start[k] + 1
   );
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
 * `source:`-only and last-declaration-wins, because
 * {@link Model.resolveFilterSource} needs ONE source name to inject filters
 * from. Last-wins means a later edge REPLACES an earlier one, so never use this
 * on a path where an edge grants access: use the set-valued
 * {@link buildDerivationBaseMap}, which refuses on ambiguity.
 *
 * Strips its own input rather than trusting callers: a missed edge is silent in
 * the unsafe direction (no filter injected, no post-compile backstop), and the
 * strip is idempotent, so a pre-stripped caller pays only a second scan.
 */
export function buildSourceAliasMap(query: string): Map<string, string> {
   const aliasOf = new Map<string, string>();
   readDefinitions(query, (keyword, name, base) => {
      if (keyword === "source" && base !== UNREADABLE_BASE) {
         aliasOf.set(name, base);
      }
   });
   return aliasOf;
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
 * Every item of a statement is read (`source: a is x extend {} b is y`), with
 * an optional parameter list after the name and any number of `(` before the
 * base; a name whose base cannot be read gets {@link UNREADABLE_BASE}, so a
 * decoy elsewhere cannot stand in for the declaration it missed.
 *
 * Strips its own input, for the reason {@link buildSourceAliasMap} gives.
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
