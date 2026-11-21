// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The collation readers, keyed by Malloy's dialect name (so BigQuery is
 * `standardsql`). A dialect with no reader has no readable collation: a
 * comparison of its strings on a stored copy fails closed, and a database
 * table in it compares as its source does. Looked up with `Object.hasOwn`
 * after the dialect is narrowed, so no other string finds a reader.
 */

import { type DialectName, registeredDialect } from "../../domain/dialect";
import { BIGQUERY_COLLATION } from "./bigquery";
import { DUCKDB_COLLATION } from "./duckdb";
import { POSTGRES_COLLATION } from "./postgres";

/** A dialect's reader, told apart by how it is read (its `scope`). */
type DialectCollationReader =
   | typeof DUCKDB_COLLATION
   | typeof POSTGRES_COLLATION
   | typeof BIGQUERY_COLLATION;

/** Each dialect's reader, where its collation can be read. */
export const COLLATION_READERS: Readonly<
   Partial<Record<DialectName, DialectCollationReader>>
> = Object.freeze({
   duckdb: DUCKDB_COLLATION,
   postgres: POSTGRES_COLLATION,
   standardsql: BIGQUERY_COLLATION,
});

/** The reader for `dialect`, or undefined when its collation is not read. */
export function collationReaderOf(
   dialect: unknown,
): DialectCollationReader | undefined {
   const name = registeredDialect(dialect);
   return name !== undefined && Object.hasOwn(COLLATION_READERS, name)
      ? COLLATION_READERS[name]
      : undefined;
}
