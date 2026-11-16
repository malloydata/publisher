// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "./DuckDBConnection";

export type Row = Record<string, unknown>;

/** A row id: creation millis plus up to nine random base-36 characters. */
export function newId(): string {
   return `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
}

export function rowTimestamps(row: Row): { createdAt: Date; updatedAt: Date } {
   return {
      createdAt: new Date(row.created_at as string),
      updatedAt: new Date(row.updated_at as string),
   };
}

/**
 * `UPDATE table SET … WHERE id = ?` for every column whose value is defined
 * (null is written; undefined means "leave alone"), plus any raw `col = expr`
 * assignments, always stamping `updated_at`.
 */
export async function updateRow(
   db: DuckDBConnection,
   table: string,
   id: string,
   values: Record<string, unknown>,
   rawAssignments: string[] = [],
): Promise<void> {
   const { sql, params } = updateRowSql(table, id, values, rawAssignments);
   await db.run(sql, params);
}

/**
 * {@link updateRow}'s statement, its WHERE narrowed by `where` (a condition
 * on the row with its parameters) when given.
 */
export function updateRowSql(
   table: string,
   id: string,
   values: Record<string, unknown>,
   rawAssignments: string[] = [],
   where?: { condition: string; params: unknown[] },
): { sql: string; params: unknown[] } {
   const assigned = Object.entries(values).filter(([, v]) => v !== undefined);
   const setClauses = [
      ...assigned.map(([column]) => `${column} = ?`),
      ...rawAssignments,
      "updated_at = ?",
   ];
   return {
      sql:
         `UPDATE ${table} SET ${setClauses.join(", ")} WHERE id = ?` +
         (where ? ` AND ${where.condition}` : ""),
      params: [
         ...assigned.map(([, v]) => v),
         new Date().toISOString(),
         id,
         ...(where?.params ?? []),
      ],
   };
}
