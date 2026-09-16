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
   TemporalFilterExpression,
   type Moment,
   type StringCondition,
   type StringFilter,
   type TemporalFilter,
} from "@malloydata/malloy-filter";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";

dayjs.extend(utc);

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

/**
 * An inclusive number range, which is what a two-handled slider means:
 * `encodeBetween(10, 20)` is `[10 to 20]`, Malloy's `>= 10 and <= 20`. The same
 * spelling Malloyyo's `filters.between` writes, so a value set on either host
 * reads back on the other.
 */
export function encodeBetween(lo: number, hi: number): string {
   return NumberFilterExpression.unparse({
      operator: "range",
      startOperator: ">=",
      startValue: String(lo),
      endOperator: "<=",
      endValue: String(hi),
   });
}

/**
 * The bounds back out of {@link encodeBetween}, or undefined for any other
 * filter. Only the closed range round-trips: `(10 to 20]` and its kin exclude an
 * end the slider's thumbs cannot express, and a negated range is not a range at
 * all, so both fall through to the text box rather than being redrawn as
 * something else.
 */
export function decodeBetween(value: string): [number, number] | undefined {
   const text = value.trim();
   if (text === "") return undefined;
   const { parsed } = NumberFilterExpression.parse(text);
   if (
      parsed === null ||
      parsed.operator !== "range" ||
      parsed.not === true ||
      parsed.startOperator !== ">=" ||
      parsed.endOperator !== "<="
   ) {
      return undefined;
   }
   const lo = Number(parsed.startValue);
   const hi = Number(parsed.endValue);
   return Number.isFinite(lo) && Number.isFinite(hi) && lo <= hi
      ? [lo, hi]
      : undefined;
}

/**
 * The preset windows a time-range control offers for a `filter<date>` or
 * `filter<timestamp>` given. The same six Malloyyo's `TimeRange` widget ships,
 * spelled in Malloy's filter grammar: `today` is the current day, `7 days` is
 * Malloy's "in the last 7 days", a rolling window ending now.
 *
 * Structured clauses rather than strings so the spelling on the wire is the
 * filter package's own, and {@link decodeTimePreset} compares against the same
 * structure the parser returns rather than against text that may be spelled
 * `7 day`, `7 days` or `7  days` by whoever wrote the URL.
 */
export const TIME_PRESETS: readonly {
   key: string;
   label: string;
   clause: TemporalFilter;
}[] = [
   {
      key: "today",
      label: "Today",
      clause: { operator: "in", in: { moment: "today" } },
   },
   {
      key: "7d",
      label: "Last 7 days",
      clause: { operator: "in_last", units: "day", n: "7" },
   },
   {
      key: "30d",
      label: "Last 30 days",
      clause: { operator: "in_last", units: "day", n: "30" },
   },
   {
      key: "90d",
      label: "Last 90 days",
      clause: { operator: "in_last", units: "day", n: "90" },
   },
   {
      key: "12m",
      label: "Last 12 months",
      clause: { operator: "in_last", units: "month", n: "12" },
   },
];

/** The filter text for one of {@link TIME_PRESETS}, by key. */
export function encodeTimePreset(key: string): string | undefined {
   const preset = TIME_PRESETS.find((p) => p.key === key);
   return preset ? TemporalFilterExpression.unparse(preset.clause) : undefined;
}

/**
 * Which preset a filter is, or undefined when it is none of them. Matched on the
 * parsed clause, so `7 day` and `7 days` are the same window, and a negated or
 * compound filter is not a preset however it is spelled.
 */
export function decodeTimePreset(value: string): string | undefined {
   const text = value.trim();
   if (text === "") return undefined;
   const { parsed } = TemporalFilterExpression.parse(text);
   if (parsed === null) return undefined;
   for (const preset of TIME_PRESETS) {
      const want = preset.clause;
      if (want.operator !== parsed.operator) continue;
      if (want.operator === "in" && parsed.operator === "in") {
         if (parsed.not !== true && parsed.in.moment === want.in.moment) {
            return preset.key;
         }
      }
      if (want.operator === "in_last" && parsed.operator === "in_last") {
         if (
            parsed.not !== true &&
            parsed.units === want.units &&
            Number(parsed.n) === Number(want.n)
         ) {
            return preset.key;
         }
      }
   }
   return undefined;
}

const DAY = /^\d{4}-\d{2}-\d{2}$/;

/**
 * An inclusive range of whole days, as two `YYYY-MM-DD` strings, in Malloy's
 * filter grammar.
 *
 * Malloy's `A to B` runs from the start of A up to but NOT including the start
 * of B (`filter_compilers.ts`, `case 'to'`), while a reader picking "January
 * 1st to January 31st" means both days. So the end is encoded as the day AFTER
 * the last day picked: `encodeDayRange("2024-01-01", "2024-01-31")` is
 * `2024-01-01 to 2024-02-01`. {@link decodeDayRange} undoes exactly that, so a
 * range written by Malloyyo (which encodes the picked day verbatim) displays
 * here as the days it actually selects.
 */
export function encodeDayRange(firstDay: string, lastDay: string): string {
   const end = dayjs.utc(lastDay, "YYYY-MM-DD").add(1, "day");
   return TemporalFilterExpression.unparse({
      operator: "to",
      fromMoment: { moment: "literal", literal: firstDay, units: "day" },
      toMoment: {
         moment: "literal",
         literal: end.format("YYYY-MM-DD"),
         units: "day",
      },
   });
}

/**
 * The inclusive days back out of {@link encodeDayRange}, or undefined for any
 * other filter. Both ends must be whole-day literals: a range with a time of
 * day, a relative end (`2024-01-01 to now`), or a negation is not something two
 * day pickers can show, and falls through to the text box.
 */
export function decodeDayRange(
   value: string,
): { firstDay: string; lastDay: string } | undefined {
   const text = value.trim();
   if (text === "") return undefined;
   const { parsed } = TemporalFilterExpression.parse(text);
   if (parsed === null || parsed.operator !== "to" || parsed.not === true) {
      return undefined;
   }
   const from = dayLiteral(parsed.fromMoment);
   const to = dayLiteral(parsed.toMoment);
   if (from === undefined || to === undefined) return undefined;
   const lastDay = dayjs.utc(to, "YYYY-MM-DD").subtract(1, "day");
   if (!lastDay.isValid() || lastDay.isBefore(dayjs.utc(from, "YYYY-MM-DD"))) {
      return undefined;
   }
   return { firstDay: from, lastDay: lastDay.format("YYYY-MM-DD") };
}

function dayLiteral(moment: Moment): string | undefined {
   return moment.moment === "literal" && DAY.test(moment.literal)
      ? moment.literal
      : undefined;
}
