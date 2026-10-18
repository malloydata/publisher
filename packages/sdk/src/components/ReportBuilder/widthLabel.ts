// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The common fractions a grid width reads as, by numerator/denominator in lowest terms. */
const FRACTION_NAMES: Record<string, string> = {
   "1/2": "Half",
   "1/3": "A third",
   "2/3": "Two-thirds",
   "1/4": "A quarter",
   "3/4": "Three-quarters",
};

const gcd = (a: number, b: number): number => (b === 0 ? a : gcd(b, a % b));

/**
 * What a tile's width reads as while it is dragged: `8 of 12 · Two-thirds`,
 * or `5 of 12` where the width is no common fraction, and `12 of 12 · Full
 * width` at the grid's edge.
 */
export function widthLabel(span: number, columns: number): string {
   const count = `${span} of ${columns}`;
   if (span >= columns) return `${count} · Full width`;
   const divisor = gcd(span, columns);
   const name = FRACTION_NAMES[`${span / divisor}/${columns / divisor}`];
   return name ? `${count} · ${name}` : count;
}
