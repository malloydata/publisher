// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Pure functions: no DOM, no globals, no Publisher. Everything here is a value
// in and a value out, which is what makes it the one file `node --test` covers
// directly (../../tests/format.test.mjs).

import {
   StringFilterExpression,
   NumberFilterExpression,
} from "../vendor/malloy-filter.js";

const CURRENCY = new Intl.NumberFormat(undefined, {
   style: "currency",
   currency: "USD",
   maximumFractionDigits: 0,
});
const CURRENCY_CENTS = new Intl.NumberFormat(undefined, {
   style: "currency",
   currency: "USD",
   minimumFractionDigits: 2,
   maximumFractionDigits: 2,
});
const PERCENT = new Intl.NumberFormat(undefined, {
   style: "percent",
   minimumFractionDigits: 1,
   maximumFractionDigits: 1,
});
// Minimum as well as maximum: a field tagged `# number="#,##0.0"` asked for a
// decimal place, so a value that lands on a whole number keeps it. That is the
// only case the minimum changes, measured: without it an orders-per-customer of
// exactly 11 renders as "11" and reads like a count rather than a rate, while
// today's 11.2936 gives "11.3" either way. The tag asked for one decimal, so the
// field shows one whatever the data happens to be that day.
const DECIMAL = new Intl.NumberFormat(undefined, {
   minimumFractionDigits: 1,
   maximumFractionDigits: 1,
});
const INTEGER = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

/** Malloy hands large integers back as strings, so coerce before formatting. */
const toNumber = (value) => {
   const n = typeof value === "number" ? value : Number(value);
   return Number.isFinite(n) ? n : null;
};

export const usd = (value, cents = false) => {
   const n = toNumber(value);
   return n === null ? "—" : (cents ? CURRENCY_CENTS : CURRENCY).format(n);
};

export const percent = (value) => {
   const n = toNumber(value);
   return n === null ? "—" : PERCENT.format(n);
};

export const decimal = (value) => {
   const n = toNumber(value);
   return n === null ? "—" : DECIMAL.format(n);
};

export const integer = (value) => {
   const n = toNumber(value);
   return n === null ? "—" : INTEGER.format(n);
};

/**
 * Format a cell the way the field's own tags ask for, so the page never carries
 * a second opinion about how a number reads. `# currency` and `# percent` come
 * from the model; `id` is Malloy's "this is an identifier, do not group the
 * digits" format, which is why a customer id renders as 88884 rather than
 * with digit grouping. Five digits, and no separator named: four is ungrouped
 * in several locales and the separator itself differs between them, so a
 * smaller example is only true where the author happened to run it.
 */
export function formatValue(value, format) {
   if (value === null || value === undefined) return "—";
   switch (format) {
      case "currency":
         return usd(value);
      case "percent":
         return percent(value);
      case "id":
         return String(value);
      case "decimal":
         return decimal(value);
      case "month":
         return monthLabel(value);
      case "integer":
         return integer(value);
      default: {
         // An untagged number still reads better grouped, and Malloy hands a
         // large integer back as a string, so a numeric-looking string counts.
         // A boolean and a blank cell must NOT: both are excluded by the two
         // tests below rather than by a branch of their own. `Number("true")`
         // is NaN, and a blank fails the trim, so each falls through to its own
         // text. Measured, because the obvious guard is against `Number(value)`,
         // which would indeed make `true` into 1, and that is not what runs.
         const text = String(value);
         return text.trim() !== "" && !Number.isNaN(Number(text))
            ? integer(text)
            : text;
      }
   }
}

/** A month-truncated timestamp comes back as an ISO string: keep YYYY-MM. */
export const monthKey = (value) => String(value ?? "").slice(0, 7);

const MONTHS_SHORT = [
   "Jan",
   "Feb",
   "Mar",
   "Apr",
   "May",
   "Jun",
   "Jul",
   "Aug",
   "Sep",
   "Oct",
   "Nov",
   "Dec",
];

export function monthLabel(value) {
   const key = monthKey(value);
   if (!/^\d{4}-\d{2}$/.test(key)) return String(value ?? "—");
   const [year, month] = key.split("-");
   return `${MONTHS_SHORT[Number(month) - 1]} ${year}`;
}

/** `month(created_at)` is a number 1-12, not a date. */
export const monthOfYearLabel = (n) => MONTHS_SHORT[Number(n) - 1] ?? String(n);

/** Enough pluralizing for a control's "All categories" / "All brands" option. */
export const plural = (word) => {
   const text = String(word ?? "");
   if (/[^aeiou]y$/i.test(text)) return `${text.slice(0, -1)}ies`;
   if (/(s|x|z|ch|sh)$/i.test(text)) return `${text}es`;
   return `${text}s`;
};

// ---- filter values -----------------------------------------------------------
//
// A `filter<T>` given takes Malloy filter syntax as a string, not a plain value:
// `"us-east, us-west"` for a string filter, `">= 100"` for a number one (see
// docs/givens.md). A control works in plain values, so something has to
// translate, and this is the one part of the page that can silently produce a
// filter meaning something other than what the reader picked.
//
// Which is why none of it is hand-written. Malloy's filter languages are parsed
// AND printed by `@malloydata/malloy-filter`, vendored next door as a browser
// module (see scripts/vendor-malloy-filter.mjs), so a value goes out through
// `unparse` and comes back through `parse` and the two agree by construction.
//
// The page this replaced did not write filter syntax at all: it pasted the
// picked value into the query text as a Malloy string literal, escaping `'` by
// doubling it, which is not Malloy's escape (Malloy escapes with a backslash).
// Binding the value as a given instead is the fix for that, and it is what
// data_app.malloy does. It also moves the escaping problem here, because a
// `filter<string>` given takes filter syntax rather than a plain value.
//
// That is the part worth being careful about, because a naive encoder fails
// silently. Measured against the parser below, a value wrapped in double quotes
// (which the string grammar has no notion of) matches the quote characters too,
// and an unescaped value is worse: `-Outerwear` is a NEGATION, `%` is a
// match-anything pattern that returns every row, `null` and `empty` are
// operators, `a;b` is a conjunction, and a comma splits one brand into two. The
// page still renders a table for each of those. It is just the wrong table.

/**
 * Whether one value survives a round trip through the grammar unchanged.
 *
 * The page asks rather than predicts. A hand-written predicate here would be a
 * second copy of a rule the library already owns, and the interesting cases are
 * exactly the ones a predicate gets wrong: a non-breaking space and a lone tab
 * both `unparse` to themselves and then `parse` to a NULL clause with an empty
 * log, and Malloy compiles a null clause to the SQL constant `true`, so
 * encoding one would have matched every row. An ordinary space, which a
 * `trim()`-based guard would also have dropped, escapes to `\ ` and round-trips
 * fine. A value with a newline in it fails to parse at all, and a trailing tab
 * comes back without the tab. All four are decided here by running the round
 * trip, so none of them needs to be remembered.
 */
function survivesRoundTrip(value) {
   const expr = StringFilterExpression.unparse({
      operator: "=",
      values: [value],
   });
   const { parsed, log } = StringFilterExpression.parse(expr);
   return (
      (log?.length ?? 0) === 0 &&
      parsed !== null &&
      parsed.operator === "=" &&
      parsed.not !== true &&
      parsed.values?.length === 1 &&
      parsed.values[0] === value
   );
}

/**
 * Join picked values into one string filter (`"Nike, Levi's"`), which Malloy
 * reads as "any of these".
 *
 * A value that cannot be carried is dropped rather than approximated. Dropping
 * every value leaves `""`, the empty filter, which is the "All" state: that is
 * a WIDENING, so it is deliberate and visible rather than incidental. The
 * control shows nothing selected, which is what "All" looks like.
 */
export function encodeFilterList(values) {
   const present = values.map(String).filter(survivesRoundTrip);
   if (present.length === 0) return "";
   return StringFilterExpression.unparse({ operator: "=", values: present });
}

/**
 * One value as a string filter, or `""` when it cannot be one.
 *
 * Callers decide whether a value is filterable by looking at what this RETURNS,
 * never by testing the value themselves. Two places predicting the same rule is
 * how the rule drifts, and the prediction is the copy that goes stale.
 *
 * Every kind of value lands on the same encoder for the same reason. A number
 * is only "obviously safe" until it is negative: `-5` printed as itself is a
 * NEGATION of 5, not a match for it, which is the failure this module exists to
 * prevent. The non-string branches only reduce a value to the text it stands
 * for; the escaping is still the grammar's.
 */
export function encodeFilterValue(value) {
   if (value === null || value === undefined) return "";
   if (typeof value === "boolean") return encodeFilterList([String(value)]);
   if (typeof value === "number")
      return Number.isFinite(value) ? encodeFilterList([String(value)]) : "";
   if (value instanceof Date)
      return Number.isNaN(value.getTime())
         ? ""
         : encodeFilterList([value.toISOString().slice(0, 10)]);
   if (typeof value !== "string") return "";
   return encodeFilterList([value]);
}

/**
 * Split a string filter back into the values a picker should show selected,
 * undoing {@link encodeFilterList}.
 *
 * Only the list form round-trips. Anything else a filter can express (`-Nike`,
 * `%foo%`, `null`, a `;` conjunction) comes back as a single opaque entry
 * rather than being reinterpreted, so a hand-written filter someone put in the
 * URL is preserved rather than mangled by a control that cannot represent it.
 */
export function decodeFilterList(filter) {
   const text = String(filter ?? "");
   if (text.trim() === "") return [];
   const { parsed } = StringFilterExpression.parse(text);
   return isPlainEquality(parsed) ? parsed.values : [text];
}

/**
 * Whether a filter is one a picker can represent, and therefore one it may
 * safely re-encode.
 *
 * A picker's selection round-trips through {@link decodeFilterList} and
 * {@link encodeFilterList}, and that is only faithful for the list form. A
 * negation or a wildcard comes back as one opaque entry so the control can show
 * something, but the next edit would re-encode that entry as a literal, turning
 * `-Nike` from "everything except Nike" into a search for the five characters
 * `-Nike`. A caller with a plain text box to fall back to should ask this first.
 */
export function isPlainFilterList(filter) {
   const text = String(filter ?? "");
   if (text.trim() === "") return true;
   return isPlainEquality(StringFilterExpression.parse(text).parsed);
}

/** "the value is one of these", with no negation: what a picker can show. */
function isPlainEquality(clause) {
   return (
      clause !== null &&
      clause !== undefined &&
      clause.operator === "=" &&
      clause.not !== true &&
      Array.isArray(clause.values)
   );
}

/**
 * What a picker needs to know about a filter, in ONE call.
 *
 * The values and "can this control represent it" are two answers to the same
 * question, and asking for one without the other is how the hazard below gets
 * shipped: a caller that decodes and forgets to check re-encodes `-Nike`, which
 * means everything EXCEPT Nike, as a literal search for those five characters.
 * That has now happened twice in this file's callers, once in each picker, so
 * the two answers are returned together and there is nothing to forget.
 *
 * `opaque` means the control cannot show this filter faithfully. Show the
 * filter as written, say so, and let the reader replace it deliberately.
 */
export function readFilterForPicker(filter) {
   return {
      values: decodeFilterList(filter),
      opaque: !isPlainFilterList(filter),
   };
}

/** A lower-bound number filter, which is what a one-handled slider means. */
export function encodeAtLeast(value) {
   const n = Number(value);
   if (!Number.isFinite(n) || n <= 0) return "";
   return NumberFilterExpression.unparse({
      operator: ">=",
      values: [String(n)],
   });
}

/**
 * The bound back out of {@link encodeAtLeast}, or 0 for any other filter.
 *
 * Through the library for the same reason the string side is. A hand-rolled
 * `/(-?\d+(?:\.\d+)?)/` grabbed the first number it saw wherever it sat, which
 * is wrong in both directions: `<= 2` and `!= 9` came back as lower bounds the
 * filter never expressed, and `>= .5` came back as 5, ten times the bound.
 */
export function decodeAtLeast(filter) {
   const text = String(filter ?? "").trim();
   if (text === "") return 0;
   const { parsed } = NumberFilterExpression.parse(text);
   if (
      parsed === null ||
      parsed === undefined ||
      parsed.operator !== ">=" ||
      parsed.not === true ||
      parsed.values?.length !== 1
   ) {
      return 0;
   }
   const bound = Number(parsed.values[0]);
   return Number.isFinite(bound) ? bound : 0;
}

/** `@2023-01-01` and `f''` are Malloy literals; a control needs the value inside. */
export function readGivenDefault(literal) {
   const text = String(literal ?? "");
   // No special case for `f''`: the pattern below already captures the empty
   // string and returns it.
   const filter = /^f'(.*)'$/s.exec(text);
   if (filter) return filter[1];
   return text.replace(/^@/, "");
}
