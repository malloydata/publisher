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
 * The Malloy keywords spelled as a bare word (statement keywords, which end in
 * `:`, are {@link MALLOY_STATEMENT_KEYWORDS}). A bare keyword where a name is expected does not
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
 * The Malloy statement keywords, spelled without their colon. The lexer reads
 * a statement keyword together with the `:` after it, so a bare name of one of
 * these words is fine in an expression but not where a `:` follows it: in a
 * `type:` field (`select::string` lexes as `select:` and then `:string`) or a
 * named argument. A spec pins this list against Malloy's own lexer too.
 */
export const MALLOY_STATEMENT_KEYWORDS: ReadonlySet<string> = new Set([
   "accept",
   "aggregate",
   "calculate",
   "calculation",
   "connection",
   "declare",
   "dimension",
   "drill",
   "except",
   "extend",
   "given",
   "group_by",
   "grouped_by",
   "having",
   "index",
   "internal",
   "join_cross",
   "join_many",
   "join_one",
   "limit",
   "measure",
   "nest",
   "order_by",
   "partition_by",
   "primary_key",
   "private",
   "public",
   "query",
   "rename",
   "run",
   "sample",
   "select",
   "source",
   "timezone",
   "top",
   "type",
   "view",
   "where",
]);

/**
 * Escape a name's text for the inside of backticks: backslash first, so the
 * backtick's own escape is not doubled and a trailing backslash cannot swallow
 * the closing quote. Malloy escapes with a backslash, never by doubling.
 * Mirrors Malloy's unexported `escapeIdentifier` (to_stable.ts).
 */
function escapeIdentifierText(name: string): string {
   return escapeControl(name.replace(/\\/g, "\\\\").replace(/`/g, "\\`"));
}

/**
 * Escape the characters Malloy's lexer refuses raw inside a quoted string or
 * identifier: a line break ends the token, and other control characters are
 * not accepted. Malloy reads `\n`, `\r` and `\uXXXX` back (malloy-tag's
 * `parseString`). A tab is accepted raw, so it is left alone and text that
 * already compiled is unchanged.
 */
function escapeControl(text: string): string {
   // eslint-disable-next-line no-control-regex
   return text.replace(/[\u0000-\u0008\u000a-\u001f\u007f]/g, (c) =>
      c === "\n"
         ? "\\n"
         : c === "\r"
           ? "\\r"
           : "\\u" + c.charCodeAt(0).toString(16).padStart(4, "0"),
   );
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
   const word = name.toLowerCase();
   return PLAIN_NAME.test(name) &&
      !MALLOY_BARE_KEYWORDS.has(word) &&
      !MALLOY_STATEMENT_KEYWORDS.has(word)
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
 * `o'brien` needs `\'`; a line break or other control character is written
 * as an escape, since Malloy's lexer refuses it raw.
 */
export function escapeString(value: string): string {
   return escapeControl(value.replace(/\\/g, "\\\\").replace(/'/g, "\\'"));
}

/** A value as a single-quoted Malloy string literal. */
export function stringLiteral(value: string): string {
   return `'${escapeString(value)}'`;
}

/**
 * A value as a double-quoted Malloy string literal, for the places Malloy's
 * grammar writes one in double quotes (an `import` path). Same escaping as
 * {@link stringLiteral}, with the double quote escaped instead of the single.
 * An ordinary path reads exactly as `JSON.stringify` wrote it.
 */
export function doubleQuotedString(value: string): string {
   return `"${escapeControl(value.replace(/\\/g, "\\\\").replace(/"/g, '\\"'))}"`;
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

/** The current-time offset units a rolling-window term may use. */
export const NOW_OFFSET_UNITS = [
   "second",
   "minute",
   "hour",
   "day",
   "week",
   "month",
   "quarter",
   "year",
] as const;
export type NowOffsetUnit = (typeof NOW_OFFSET_UNITS)[number];

/** The comparisons a rolling-window term may use. */
export type NowComparisonOp = ">=" | ">" | "<" | "<=";

/**
 * A comparison of a field with the current time, `<field> <op> now [± <n>
 * <unit>s]`, as a `where:` term re-applies a rolling window at read. Every part
 * is checked against a closed set (the field is quoted as an identifier), so
 * the text cannot carry anything but the comparison.
 */
export function nowComparison(
   field: string,
   op: NowComparisonOp,
   offset?: { sign: 1 | -1; amount: number; unit: NowOffsetUnit },
): string {
   if (![">=", ">", "<", "<="].includes(op)) {
      throw new Error(`not a comparison: ${op}`);
   }
   let now = "now";
   if (offset) {
      if (!Number.isSafeInteger(offset.amount) || offset.amount < 0) {
         throw new Error(`not a whole offset: ${offset.amount}`);
      }
      if (!(NOW_OFFSET_UNITS as readonly string[]).includes(offset.unit)) {
         throw new Error(`not a time unit: ${offset.unit}`);
      }
      now = `now ${offset.sign < 0 ? "-" : "+"} ${offset.amount} ${offset.unit}s`;
   }
   return `${identifier(field)} ${op} ${now}`;
}
