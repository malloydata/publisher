// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../../../logger";
import { recordSourceBuildDuration } from "../../../materialization_metrics";
import { errMessage } from "../../../utils";
import { quoteRenameTarget, quoteTablePath } from "../../quoting";
import { ManifestEntry } from "../types";
import type { BuildStrategy, ColocatedBuildRequest } from "./build_strategy";
import {
   advanceLedgerAfterSeed,
   ledgerFields,
   refreshFields,
   resetLedger,
} from "./incremental_build";
import { stagingSuffix } from "./materialization_run_support";

/**
 * A full rebuild of a colocated table in the source's own warehouse: COPY into
 * a staging table, then an atomic swap onto the physical name, so a crash
 * mid-build never leaves the served table half-written. The staging name
 * derives from the sourceEntityId. For an incremental source it also clears
 * the ledger before the rebuild and records the new boundary after it.
 */
export class ColocatedCtasStrategy
   implements BuildStrategy<ColocatedBuildRequest>
{
   async build(request: ColocatedBuildRequest): Promise<ManifestEntry> {
      const {
         persistSource,
         instruction,
         connection,
         manifest,
         buildSQL,
         runOptions,
         incrementalRefresh,
      } = request;
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const dialect = persistSource.dialectName;

      const stagingTableName = `${physicalTableName}${stagingSuffix(sourceEntityId)}`;
      // Names arrive logical (unquoted); quote for valid DDL (e.g. a hyphenated
      // BigQuery project id). The manifest echoes the logical name.
      const quotedStaging = quoteTablePath(stagingTableName, dialect);
      const quotedPhysical = quoteTablePath(physicalTableName, dialect);
      // Not always the bare name: on some dialects a bare rename target moves
      // the table into the session's default container.
      const quotedRenameTarget = quoteRenameTarget(physicalTableName, dialect);

      // Drop the boundary BEFORE the rebuild: a crash mid-rebuild must not leave
      // a boundary for data that no longer exists for the next delta to trust.
      if (incrementalRefresh) {
         await resetLedger(
            incrementalRefresh.context,
            incrementalRefresh.lineage,
         );
      }

      const startTime = performance.now();
      await connection.runSQL(
         `DROP TABLE IF EXISTS ${quotedStaging}`,
         runOptions,
      );
      // Only the CTAS reads source data, so only its cost is reported.
      // Undefined on backends that do not report one.
      let buildCostBytes: number | undefined;
      try {
         const ctas = await connection.runSQL(
            `CREATE TABLE ${quotedStaging} AS (${buildSQL})`,
            runOptions,
         );
         buildCostBytes = ctas?.runStats?.queryCostBytes;
         await connection.runSQL(
            `DROP TABLE IF EXISTS ${quotedPhysical}`,
            runOptions,
         );
         await connection.runSQL(
            `ALTER TABLE ${quotedStaging} RENAME TO ${quotedRenameTarget}`,
            runOptions,
         );
      } catch (err) {
         try {
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quotedStaging}`,
               runOptions,
            );
         } catch (cleanupErr) {
            logger.error(
               "Failed to clean up staging table after a failed build; physical leak",
               {
                  stagingTableName,
                  connectionName: persistSource.connectionName,
                  cleanupError: errMessage(cleanupErr),
               },
            );
         }
         throw err;
      }

      // Visible to later downstreams in this run, under the SAME quoted path
      // the CREATE used (pasted into a downstream `FROM` verbatim). The returned
      // entry keeps the logical name.
      manifest.update(sourceEntityId, { tableName: quotedPhysical });

      const durationMs = Math.round(performance.now() - startTime);
      recordSourceBuildDuration(durationMs, "in_warehouse");
      logger.info(`Built materialized source ${persistSource.name}`, {
         physicalTableName,
         durationMs,
      });

      // The table now holds a full snapshot; its boundary turns the NEXT
      // refresh into a delta.
      const seededThrough = incrementalRefresh
         ? await advanceLedgerAfterSeed({
              context: incrementalRefresh.context,
              lineage: incrementalRefresh.lineage,
              quotedTablePath: quotedPhysical,
              dialect,
              runner: (sql) => connection.runSQL(sql, runOptions),
           })
         : undefined;

      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         realization: instruction.realization,
         ...ledgerFields(incrementalRefresh?.lineage, seededThrough),
         // Reported for any incremental source that rebuilt, so a full rebuild
         // is distinguishable from a delta.
         ...refreshFields(incrementalRefresh ? "full" : undefined),
         rowCount: null,
         buildDurationMs: durationMs,
         // The recurring warehouse cost of keeping this source materialized.
         queryCostBytes: buildCostBytes ?? null,
         ...request.upstreamReuse,
      };
   }
}
