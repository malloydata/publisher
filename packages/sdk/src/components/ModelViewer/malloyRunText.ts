// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Malloy text the viewer runs. Pure string building, kept apart from the
 * hooks that send it so it can be tested without a DOM. Every name is quoted
 * by {@link malloyIdentifier}, and no value is ever written into the text.
 */

/**
 * A Malloy identifier, always backtick-quoted. Quoting unconditionally is what
 * makes this safe: a name that LOOKS plain can still be a reserved word (`year`,
 * `source`, `count`), and the quotes are the only spelling the grammar accepts
 * for one of those. Inside the quotes, backslashes are escaped before backticks
 * so neither can end the quoted name early.
 */
export function malloyIdentifier(name: string): string {
   const escaped = name.replace(/\\/g, "\\\\").replace(/`/g, "\\`");
   return `\`${escaped}\``;
}

/** The Malloy that runs a source's view by name. */
export function viewRunQuery(sourceName: string, viewName: string): string {
   return `run: ${malloyIdentifier(sourceName)} -> ${malloyIdentifier(viewName)}`;
}

/** The Malloy that runs a model-level named query. */
export function namedQueryRunQuery(queryName: string): string {
   return `run: ${malloyIdentifier(queryName)}`;
}

/**
 * The Malloy that reads a dimension's most common values, most common first:
 * what a field's Values card shows when its host has no index to ask.
 */
export function sampleValuesQuery(
   sourceName: string,
   fieldName: string,
   limit: number,
): string {
   const field = malloyIdentifier(fieldName);
   return [
      `run: ${malloyIdentifier(sourceName)} -> {`,
      `   where: ${field} is not null`,
      `   group_by: ${field}`,
      `   aggregate: \`__sample_rows\` is count()`,
      `   order_by: \`__sample_rows\` desc`,
      `   limit: ${limit}`,
      `}`,
   ].join("\n");
}
