// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The database tier's incremental path ({@link buildDatabaseDelta}): a
 * bounded delta applied by the source's own warehouse to the table it serves,
 * or `undefined` when the table must be rebuilt in full. Called by the
 * database tier (database_build_strategy.ts) before its staging CTAS.
 *
 * The decision (seed, delta or skip) is the shared planner's in
 * incremental_build.ts; the delta's SQL is incremental_apply.ts's. This module
 * only joins them for one warehouse table.
 */

import { quoteTablePath } from "../../quoting";
import { ManifestEntry } from "../domain/types";
import type { DatabaseBuildRequest } from "./tier";
import { warehouseDeltaTarget } from "./incremental_apply";
import { refreshOneSourceIncrementally } from "./incremental_build";

/**
 * Incremental refresh of a database table: a source with
 * `refresh="incremental"` and a usable watermark advances by a bounded delta
 * the warehouse applies to its own table. Answers `undefined` — rebuild it in
 * full — for any other source, and for one whose ledger says a delta cannot be
 * trusted (the shared planner decides seed, delta or skip).
 */
export async function buildDatabaseDelta(
   request: DatabaseBuildRequest,
): Promise<ManifestEntry | undefined> {
   const {
      persistSource,
      instruction,
      connection,
      manifest,
      buildSQL,
      runOptions,
      incrementalRefresh,
   } = request;
   if (!incrementalRefresh) return undefined;
   const dialect = persistSource.dialectName;
   return refreshOneSourceIncrementally({
      ...incrementalRefresh,
      persistSource,
      instruction,
      target: warehouseDeltaTarget({
         dialect,
         runner: (sql, options) =>
            connection.runSQL(sql, { ...runOptions, ...options }),
         quotedTablePath: quoteTablePath(
            instruction.physicalTableName,
            dialect,
         ),
         lineage: incrementalRefresh.lineage,
         // The CTAS's own SQL, so a delta computes what a rebuild would
         // (see deltaSelect).
         sourceSQL: buildSQL,
      }),
      manifest,
      upstreamReuse: request.upstreamReuse,
   });
}
