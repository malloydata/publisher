// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The text of every key check: one Malloy writer for the four checks that
 * compile through the model, and the SQL of a stored table's check, which
 * runs on a destination's own session. Each writes the predicate its kind's
 * NULL rule (`domain/key_check.ts`) says: a query returns a row exactly when
 * the check fails.
 *
 * Pure.
 */

import { identifier } from "../../malloy_text";
import { KEY_RULES, type KeyCheckKind } from "../domain/key_check";

/** The names a check's query writes, per kind. */
const SPELLING = {
   inline_join_one: { count: "matches", key: undefined, notNull: "is not" },
   pair_level: { count: "matches", key: undefined, isNull: "is" },
   rollup_key: { count: "key_rows__check", key: undefined, notNull: "!=" },
   parent_key: { count: "key_rows__check", key: "key__check", isNull: "=" },
} as const;

/**
 * The pipeline `<source> -> { … }` that returns one row when `keyColumn`
 * of `source` (a Malloy name) fails a `kind` check: a repeat, or, where the
 * kind's NULL rule says so, a NULL.
 */
export function keyCheckPipeline(
   kind: Exclude<KeyCheckKind, "stored_key">,
   source: string,
   keyColumn: string,
): string {
   const k = identifier(keyColumn);
   const s = identifier(source);
   if (KEY_RULES[kind].nullKey === "ignored") {
      const spelled = SPELLING[kind as "inline_join_one" | "rollup_key"];
      // malloy-text: a count name from the fixed SPELLING table
      const count = spelled.count;
      // malloy-text: an operator from the fixed SPELLING table
      const notNull = spelled.notNull;
      return `${s} -> {
  where: ${k} ${notNull} null
  group_by: ${k}
  aggregate: ${count} is count()
  having: ${count} > 1
  limit: 1
}`;
   }
   const spelled = SPELLING[kind as "pair_level" | "parent_key"];
   // malloy-text: a count name from the fixed SPELLING table
   const count = spelled.count;
   const key = spelled.key;
   // malloy-text: an operator from the fixed SPELLING table
   const isNull = spelled.isNull;
   // malloy-text: the key through identifier, or a fixed alias for it
   const grouped = key === undefined ? k : `${key} is ${k}`;
   // malloy-text: the key through identifier, or a fixed alias
   const ref = key ?? k;
   return `${s} -> {
  group_by: ${grouped}
  aggregate: ${count} is count()
} -> {
  where: ${count} > 1 or ${ref} ${isNull} null
  select: ${ref}
  limit: 1
}`;
}

/**
 * The SQL of a stored table's key check, over `quotedTable` (a DuckDB path,
 * as the destination's session reads it): one row when a value of
 * `keyColumn` repeats or, by the `stored_key` rule, is NULL.
 */
export function storedKeyCheckSql(
   quotedTable: string,
   keyColumn: string,
): string {
   const k = `"${keyColumn.replace(/"/g, '""')}"`;
   const nulls =
      KEY_RULES.stored_key.nullKey === "fails" ? ` OR ${k} IS NULL` : "";
   return `SELECT ${k} FROM ${quotedTable} GROUP BY ${k} HAVING COUNT(*) > 1${nulls} LIMIT 1`;
}
