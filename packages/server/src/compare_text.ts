// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stable order for two strings, by UTF-16 code unit.
 *
 * Deliberately not `localeCompare`: called with no locale it uses the
 * runtime's, so the same bytes sort differently under a different LANG. Every
 * caller wants "the same order everywhere", never a human-friendly one.
 */
export function compareText(a: string, b: string): number {
   return a < b ? -1 : a > b ? 1 : 0;
}
