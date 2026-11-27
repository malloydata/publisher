// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A rollup's grain entries and the columns its table stores them as. An
 * entry is a dimension of the base (`region`), stored under its name, or a
 * one-hop join path (`account.region`), stored under the `inline=` naming
 * (`account__region`), which no field of the base may already have.
 *
 * Pure.
 */

/** The stored column of a path through `join` to `field`. */
function pathColumnName(join: string, field: string): string {
   return `${join}__${field}`;
}

/** The join and field of a path grain entry, or undefined for a dimension. */
export function grainPathOf(
   grain: string,
): { join: string; field: string } | undefined {
   const at = grain.indexOf(".");
   if (at < 0) return undefined;
   return { join: grain.slice(0, at), field: grain.slice(at + 1) };
}

/** The column a grain entry is stored as. */
export function grainColumnOf(grain: string): string {
   const path = grainPathOf(grain);
   return path ? pathColumnName(path.join, path.field) : grain;
}
