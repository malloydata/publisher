// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Malloy text emitters: the one place Publisher turns a name or a value into
 * Malloy source. Every generated model, query and probe (serve shapes, rollup
 * companions, authorize probes, value scans, schema snippets) goes through
 * these, so a quoting rule is fixed once and an injection or round-trip bug
 * cannot hide in a private copy.
 *
 * Generated Malloy is text, never a graft onto compiled internals: emit the
 * text, compile it, and let the compiler judge it.
 */

/** A name Malloy reads as one identifier without quoting. */
const PLAIN_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * The Malloy keywords spelled as a bare word (statement keywords end in `:` and
 * cannot collide with a name). A bare keyword where a name is expected does not
 * compile, or compiles as the keyword: `date` is a type, `count` an aggregate,
 * `source` a declaration. A spec pins this list against Malloy's own lexer, so
 * a keyword Malloy adds fails the build here rather than a generated model.
 */
export const MALLOY_BARE_KEYWORDS: ReadonlySet<string> = new Set([
   "all",
   "and",
   "as",
   "asc",
   "avg",
   "boolean",
   "by",
   "case",
   "cast",
   "compose",
   "count",
   "date",
   "day",
   "desc",
   "distinct",
   "else",
   "end",
   "exclude",
   "export",
   "extend",
   "false",
   "filter",
   "for",
   "from",
   "full",
   "has",
   "hour",
   "import",
   "in",
   "include",
   "inner",
   "internal",
   "is",
   "json",
   "left",
   "like",
   "max",
   "min",
   "minute",
   "month",
   "not",
   "now",
   "null",
   "number",
   "on",
   "or",
   "pick",
   "private",
   "public",
   "quarter",
   "right",
   "second",
   "source",
   "sql",
   "string",
   "sum",
   "table",
   "then",
   "this",
   "timestamp",
   "timestamptz",
   "to",
   "true",
   "virtual",
   "week",
   "when",
   "with",
   "year",
]);

/**
 * Escape a name's text for the inside of backticks: backslash first, so the
 * backtick's own escape is not doubled and a trailing backslash cannot swallow
 * the closing quote. Malloy escapes with a backslash, never by doubling.
 * Mirrors Malloy's unexported `escapeIdentifier` (to_stable.ts).
 */
function escapeIdentifierText(name: string): string {
   return name.replace(/\\/g, "\\\\").replace(/`/g, "\\`");
}

/**
 * A name as one Malloy identifier, always backtick-quoted. Safe for any name:
 * a hyphen, a space, a reserved word, a leading digit or an embedded backtick
 * cannot break out. Use it wherever the text is not meant to be read by a
 * person (probes, `run:` strings).
 */
export function quotedIdentifier(name: string): string {
   return "`" + escapeIdentifierText(name) + "`";
}

/**
 * A name as one Malloy identifier, bare when Malloy reads it bare (a plain
 * name that is no keyword), otherwise quoted. Use it for generated models a
 * person may read (serve shapes, rollup companions).
 */
export function identifier(name: string): string {
   return PLAIN_NAME.test(name) && !MALLOY_BARE_KEYWORDS.has(name.toLowerCase())
      ? name
      : quotedIdentifier(name);
}

/**
 * A dotted field path (`order_items.label`) with each segment quoted, since
 * Malloy traverses joins with `.` but quotes identifiers one at a time: one
 * pair of backticks around the whole path would read as a single bogus name.
 */
export function quotedPath(path: string | readonly string[]): string {
   const segments = typeof path === "string" ? path.split(".") : path;
   return segments.map(quotedIdentifier).join(".");
}

/**
 * Escape a value for the inside of a single-quoted Malloy string. Malloy
 * escapes with a backslash, not by doubling the quote as SQL does, so
 * `o'brien` needs `\'`.
 */
export function escapeString(value: string): string {
   return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/** A value as a single-quoted Malloy string literal. */
export function stringLiteral(value: string): string {
   return `'${escapeString(value)}'`;
}

/**
 * A value as a double-quoted annotation (MOTLY) property value, e.g.
 * `storage="my-lake"`. Quoted always: unquoted, the tag parser splits at the
 * first non-identifier character (`storage=my-lake` reads as `storage="my"`
 * plus a stray `lake` tag).
 */
export function annotationValue(value: string): string {
   return JSON.stringify(value);
}
