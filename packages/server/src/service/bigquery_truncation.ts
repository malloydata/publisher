// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   Connection,
   MalloyQueryData,
   RunSQLOptions,
} from "@malloydata/malloy";
import { ResultTruncatedError } from "../errors";

/**
 * Refuse a BigQuery answer that came back short.
 *
 * `BigQueryConnection.runSQL` asks for `rowLimit` rows and reads one results
 * page. BigQuery ends a page at about 10 MB whatever `maxResults` asks for, and
 * the connector never reads the next page (malloydata/malloy#3243), so a wide
 * answer loses rows with no error. The page's `totalRows` still reports the
 * whole answer, which is how the cut shows: fewer rows than both the row
 * limit and the rows reported past `rowIndex`.
 *
 * A run with no `rowLimit` reads the connector's default page size, a row cap
 * of its own, so it is not checked.
 */
export function assertCompleteBigQueryPage(
   data: MalloyQueryData,
   rowLimit: number | undefined,
   rowIndex: number = 0,
): void {
   if (rowLimit === undefined || rowLimit <= 0) return;
   const reported = Number(data.totalRows) || 0;
   const expected = Math.min(rowLimit, Math.max(0, reported - rowIndex));
   if (data.rows.length < expected) {
      throw new ResultTruncatedError(data.rows.length, reported);
   }
}

type RunSQL = (
   sql: string,
   options?: RunSQLOptions,
   rowIndex?: number,
) => Promise<MalloyQueryData>;

const guarded = new WeakSet<Connection>();

/**
 * Make `connection.runSQL` refuse a short page (see
 * {@link assertCompleteBigQueryPage}). Installed once per connection object,
 * so a cached connection looked up again is not wrapped twice.
 */
export function guardBigQueryTruncation(connection: Connection): void {
   if (guarded.has(connection)) return;
   guarded.add(connection);
   const runSQL = (connection.runSQL as RunSQL).bind(connection);
   const checked: RunSQL = async (sql, options, rowIndex = 0) => {
      const data = await runSQL(sql, options, rowIndex);
      assertCompleteBigQueryPage(data, options?.rowLimit, rowIndex);
      return data;
   };
   connection.runSQL = checked;
}
