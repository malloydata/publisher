// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The colocated tier's incremental path ({@link buildColocatedDelta}): a
 * bounded delta applied by the source's own warehouse to the table it serves,
 * or `undefined` when the table must be rebuilt in full. Called by the
 * colocated tier (colocated_build_strategy.ts) before its staging CTAS.
 *
 * The decision (seed, delta or skip) is the shared planner's in
 * incremental_build.ts; the delta's SQL is incremental_apply.ts's. This module
 * only joins them for one warehouse table.
 */

import { transactsInScriptingBlock } from "./warehouse_sql";
import { quoteTablePath } from "../../quoting";
import { ManifestEntry } from "../domain/types";
import type { ColocatedBuildRequest } from "./tier";
import { warehouseDeltaTarget } from "./incremental_apply";
import { refreshOneSourceIncrementally } from "./incremental_build";

/**
 * Incremental refresh of a colocated table: a source with
 * `refresh="incremental"` and a usable watermark advances by a bounded delta
 * the warehouse applies to its own table. Answers `undefined` — rebuild it in
 * full — for any other source, and for one whose ledger says a delta cannot be
 * trusted (the shared planner decides seed, delta or skip).
 */
export async function buildColocatedDelta(
   request: ColocatedBuildRequest,
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
         // The run's cancel reaches the delta's script, which runs in a
         // transaction its failure rolls back. Not on Snowflake: there the
         // script is one EXECUTE IMMEDIATE block whose rollback is its own
         // EXCEPTION handler, which a cancelled statement is not known to run,
         // so the delta finishes and the build stops after it.
         ...(request.signal && !transactsInScriptingBlock(dialect)
            ? { abortSignal: request.signal }
            : {}),
      }),
      manifest,
      upstreamReuse: request.upstreamReuse,
   });
}
