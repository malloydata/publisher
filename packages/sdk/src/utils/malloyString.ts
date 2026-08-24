// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Escape a value for use inside a single-quoted Malloy string literal.
 *
 * Backslashes are escaped first, then quotes: doing it the other way round
 * would re-escape the backslash that the quote escape introduced.
 */
export function escapeMalloyString(value: string): string {
   return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}
