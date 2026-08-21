// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Encoding between control widgets and `filter<…>` given values.
 *
 * A `filter<T>` given takes Malloy filter syntax as a string: `"us-east,
 * us-west"` for a string filter, `">= 100"` for a number one (see
 * `docs/givens.md` §Accepted JS Shapes). A select or a slider works in plain
 * values, so something has to translate, and it is worth keeping that
 * translation here rather than inline in a widget: it is the one part of the
 * control layer that can silently produce a filter meaning something other than
 * what the user picked.
 *
 * Which is why the translation is not hand-written. Malloy's filter languages
 * are parsed by `@malloydata/malloy-filter`, and that package can also print a
 * clause back out, escaping exactly what its own grammar treats as special. So
 * a value goes out through `unparse` and comes back through `parse`, and the
 * two agree by construction rather than by our reading of the grammar. An
 * earlier version of this module quoted values with `"…"`, which the string
 * grammar has no notion of: it escaped nothing, and a picked `-Outerwear`
 * silently ran as a negation.
 */

import {
   isStringCondition,
   NumberFilterExpression,
   StringFilterExpression,
   type StringCondition,
   type StringFilter,
} from "@malloydata/malloy-filter";

/** True for the `filter<…>` family, whose values are filter syntax, not plain. */
export function isFilterType(type: string | undefined): boolean {
   return (type ?? "").startsWith("filter<");
}

/** The `T` of a `filter<T>`, or undefined for any other type. */
export function filterInnerType(type: string | undefined): string | undefined {
   const match = /^filter<(.+)>$/.exec(type ?? "");
   return match ? match[1] : undefined;
}

/**
 * Join picked values into one string filter (`"Nike, Levi's"`), which Malloy
 * reads as "any of these".
 *
 * Every value is escaped by the filter package's own printer, so a value that
 * would otherwise read as syntax survives as a literal. A comma does not split
 * it in two, a leading `-` is not a negation, `%` and `_` are not wildcards, and
 * a bare `null` or `empty` is the word rather than the operator.
 *
 * An empty value cannot be carried and is dropped. Malloy's `empty` operator
 * means "null or the empty string", which is a wider question than "the rows
 * whose value is exactly `''`", so there is no faithful spelling for a picked
 * empty option. Dropping it means an all-empty selection encodes to `""`, the
 * empty filter, which is the "All" state.
 *
 * Three kinds of value do not survive, all involving whitespace the grammar
 * treats as structure. None is fixable here without re-introducing the drift
 * this module delegates to avoid, and none can WIDEN a filter, which is the
 * failure that would matter:
 *
 * - **A leading or trailing tab is dropped.** The escaper covers the space
 *   character but not the tab, and the parser trims surrounding whitespace, so
 *   `"a\t"` comes back as `"a"`: a filter for a different single value, not a
 *   broader one. A space in the same position is escaped and does round-trip,
 *   and a tab INSIDE a value is untouched.
 * - **An embedded newline makes the whole filter unparseable.** The grammar
 *   excludes a bare newline from a match string, so `["a\nb", "c"]` emits
 *   `"a\nb, c"` and the parser refuses it outright: `parsed` is null and the
 *   log reads `Expected "," … but "\n" found`. The query then errors where the
 *   reader can see it, which is the good failure. `decodeFilterList` hands that
 *   string back as one opaque value, but that is its display fallback for a
 *   filter it cannot represent, NOT a filter that runs and matches the wrong
 *   thing. Escaping is not the fix: the library's `unescape` is `/\\(.)/g` and
 *   `.` does not match a newline, so the value would return carrying a stray
 *   backslash.
 * - **A value that is ENTIRELY whitespace is refused**, rather than encoded.
 *   For a tab or an NBSP it HAS to be: the escaper does not cover them, `parse`
 *   returns a null clause with an empty log rather than an error, and Malloy
 *   compiles a null clause to the SQL constant `true`, so encoding one would
 *   match every row. That is the silent widening this module exists to prevent.
 *
 *   For a run of ASCII SPACES it is a choice, not a necessity, and the earlier
 *   wording here claimed otherwise. Measured: `unparse({operator:"=", values:
 *   [" "]})` gives `"\ "` and parses straight back to `[" "]`, so spaces do
 *   round-trip. They are dropped anyway to keep ONE rule. The alternative
 *   carries spaces and drops tabs, and nothing on screen distinguishes them,
 *   so a reader could not predict which of two identical-looking values
 *   filtered and which quietly matched everything.
 *
 *   The test is JavaScript's `trim()`, which is where this stops: it treats
 *   NBSP and the ideographic space as whitespace but NOT U+200B, so a
 *   zero-width space survives as an ordinary value. Harmless, since it filters
 *   for something useless rather than for everything, but worth the boundary.
 *
 * All three are pinned in the spec so a future change to either side is visible.
 */
export function encodeFilterList(values: readonly string[]): string {
   // Dropped alongside `""`: a value with no non-whitespace character cannot be
   // carried by this grammar, and carrying it anyway WIDENS the filter to match
   // everything. `unparse` emits the whitespace as-is, `parse` returns a null
   // clause with an EMPTY log rather than an error, and Malloy compiles a null
   // clause to the SQL constant `true`. So a picked or drilled `\u00a0`, which
   // is ordinary in pasted and scraped data, silently selected every row.
   //
   // Counted rather than guessed: 24 codepoints up to U+3000 satisfy `trim()`,
   // 23 of them parse to a null clause, and the single exception is U+0020,
   // the ASCII space the escaper does cover. All 24 are dropped here anyway,
   // for the uniformity reason in the docstring above. The spec asserts those
   // three numbers, so a change on either side of the boundary fails a test
   // rather than quietly making this comment wrong, which is what happened to
   // the number that used to sit here.
   const present = values.filter((value) => value.trim() !== "");
   if (present.length === 0) return "";
   return StringFilterExpression.unparse({
      operator: "=",
      values: [...present],
   });
}

/**
 * Split a string filter back into the values a multiselect should show
 * selected, undoing {@link encodeFilterList}.
 *
 * Only the list form round-trips. Anything else a filter can express (`-Nike`,
 * `%foo%`, `null`, a `;` conjunction) comes back as a single opaque entry rather
 * than being reinterpreted, so a hand-written filter is preserved rather than
 * mangled by a control that cannot represent it.
 */
export function decodeFilterList(value: string): string[] {
   if (value.trim() === "") return [];
   const { parsed } = StringFilterExpression.parse(value);
   if (!isPlainEquality(parsed)) return [value];
   return parsed.values;
}

/**
 * Whether a filter is one a picker can represent, and therefore one it may
 * safely re-encode.
 *
 * The picker's selection round-trips through {@link decodeFilterList} and
 * {@link encodeFilterList}, and that is only faithful for the list form. A
 * negation, a wildcard, or a conjunction comes back as one opaque entry so the
 * control can show *something*, but the next edit would re-encode that entry as
 * a literal: turning `-Nike` from "everything except Nike" into a search for
 * the five-character string `-Nike`. A caller that can fall back to a plain text
 * box should ask this first and do so, which keeps the author's filter both
 * visible and intact.
 */
export function isPlainFilterList(value: string | undefined): boolean {
   if (value === undefined || value.trim() === "") return true;
   return isPlainEquality(StringFilterExpression.parse(value).parsed);
}

/**
 * A clause the multiselect can represent: "the value is one of these", with no
 * negation. `parse` already unescaped the values, so they are the literals the
 * user picked.
 */
function isPlainEquality(
   clause: StringFilter | null,
): clause is StringCondition {
   return (
      clause !== null &&
      isStringCondition(clause) &&
      clause.operator === "=" &&
      clause.not !== true
   );
}

/** A lower-bound number filter, which is what a one-handled slider means. */
export function encodeAtLeast(value: number): string {
   return NumberFilterExpression.unparse({
      operator: ">=",
      values: [String(value)],
   });
}

/**
 * The bound back out of {@link encodeAtLeast}, or undefined for any other filter.
 *
 * Through the library for the same reason the string side is: a hand-rolled
 * `/^>=\s*(-?\d+(?:\.\d+)?)$/` was narrower than the grammar it was standing
 * in for, so `>= 1e3` and `>= .5`, both of which the parser accepts and
 * `unparse` round-trips, read back as undefined and the slider silently showed
 * no lower bound at all.
 */
export function decodeAtLeast(value: string): number | undefined {
   // Trimmed, and `parsed` null-checked: the number parser rejects surrounding
   // whitespace where the string one absorbs it, and it reports anything it
   // cannot read as a null `parsed` rather than by throwing.
   const text = value.trim();
   if (text === "") return undefined;
   const { parsed } = NumberFilterExpression.parse(text);
   if (
      parsed === null ||
      parsed.operator !== ">=" ||
      parsed.not === true ||
      parsed.values?.length !== 1
   ) {
      return undefined;
   }
   const bound = Number(parsed.values[0]);
   return Number.isFinite(bound) ? bound : undefined;
}
