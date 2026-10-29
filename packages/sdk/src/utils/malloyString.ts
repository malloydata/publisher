// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Escape a value for use inside a single-quoted Malloy string literal.
 *
 * Backslashes are escaped first, then quotes: doing it the other way round
 * would re-escape the backslash that the quote escape introduced. A raw line
 * break ends the literal, so control characters are written as escapes too.
 */
export function escapeMalloyString(value: string): string {
   return value
      .replace(/\\/g, "\\\\")
      .replace(/'/g, "\\'")
      .replace(
         // eslint-disable-next-line no-control-regex -- matching them is the point
         /[\u0000-\u001f]/g,
         (c) => CONTROL_ESCAPES[c] ?? unicodeEscape(c),
      );
}

const CONTROL_ESCAPES: Record<string, string> = {
   "\n": "\\n",
   "\r": "\\r",
   "\t": "\\t",
   "\b": "\\b",
   "\f": "\\f",
};

const unicodeEscape = (c: string) =>
   `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`;
