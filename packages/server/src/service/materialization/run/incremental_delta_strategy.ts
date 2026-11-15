// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
      }),
      manifest,
      upstreamReuse: request.upstreamReuse,
   });
}
