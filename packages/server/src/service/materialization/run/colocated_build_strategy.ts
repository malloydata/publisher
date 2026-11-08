// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection as MalloyConnection } from "@malloydata/malloy";
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
import { backupSuffix, stagingSuffix } from "./materialization_run_support";

/**
 * A full rebuild of a colocated table in the source's own warehouse: COPY into
 * a staging table, then swap it onto the physical name (see
 * {@link swapIntoPlace}). The served table is never written in place, so it is
 * never half-written, and it is never dropped before its replacement holds its
 * name. The swap is two renames, not one atomic statement: between them a
 * reader finds no table under the physical name (a not-found error, never a
 * partial one), and a crash there leaves the previous table under its backup
 * name, which the next build of the source restores or replaces. The staging
 * and backup names derive from the sourceEntityId. For an incremental source
 * it also clears the ledger before the rebuild and records the new boundary
 * after it.
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
      await swapIntoPlace({
         connection,
         runOptions,
         dialect,
         physicalTableName,
         stagingTableName,
         backupTableName: `${physicalTableName}${backupSuffix(sourceEntityId)}`,
         connectionName: persistSource.connectionName,
      });

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

/**
 * Put a built staging table in place of the served one, without ever leaving
 * neither: the served table is moved aside to `backupTableName`, the staging
 * table renamed onto the physical name, and only then is the backup dropped.
 * Portable: plain `ALTER TABLE ... RENAME TO` and `DROP TABLE IF EXISTS`, and
 * a `SELECT ... WHERE 1 = 0` probe for whether a table exists, which reads no
 * rows on any dialect here.
 *
 * What survives each failure:
 * - moving the served table aside fails: it is untouched; staging is dropped.
 * - renaming staging into place fails: the backup is renamed back, then
 *   staging is dropped; when that restore fails too, both are kept (an error
 *   names them) and nothing is dropped.
 * - dropping the backup fails: the new table is already served; the backup is
 *   left (a warning names it) and the next build drops it.
 *
 * A backup with no table under the physical name is what a crash between the
 * two renames leaves: it is the last served copy, so it is treated as already
 * moved aside, and restored if this swap fails. A backup beside a served table
 * is stale and is dropped before the move. A probe that wrongly reports a
 * table missing only ever makes a rename fail, never a table be dropped.
 */
export async function swapIntoPlace({
   connection,
   runOptions,
   dialect,
   physicalTableName,
   stagingTableName,
   backupTableName,
   connectionName,
}: {
   connection: Pick<MalloyConnection, "runSQL">;
   runOptions: ColocatedBuildRequest["runOptions"];
   dialect: string;
   physicalTableName: string;
   stagingTableName: string;
   backupTableName: string;
   connectionName: string;
}): Promise<void> {
   const run = (sql: string) => connection.runSQL(sql, runOptions);
   const quotedPhysical = quoteTablePath(physicalTableName, dialect);
   const quotedStaging = quoteTablePath(stagingTableName, dialect);
   const quotedBackup = quoteTablePath(backupTableName, dialect);
   // Not always the bare name: on some dialects a bare rename target moves
   // the table into the session's default container.
   const renameToPhysical = quoteRenameTarget(physicalTableName, dialect);
   const exists = async (quoted: string): Promise<boolean> => {
      try {
         await run(`SELECT 1 FROM ${quoted} WHERE 1 = 0`);
         return true;
      } catch {
         return false;
      }
   };
   const dropStaging = async () => {
      try {
         await run(`DROP TABLE IF EXISTS ${quotedStaging}`);
      } catch (cleanupErr) {
         logger.error(
            "Failed to clean up staging table after a failed build; physical leak",
            {
               stagingTableName,
               connectionName,
               cleanupError: errMessage(cleanupErr),
            },
         );
      }
   };

   // Whether the previous table now sits under the backup name.
   let movedAside: boolean;
   try {
      if (await exists(quotedPhysical)) {
         await run(`DROP TABLE IF EXISTS ${quotedBackup}`);
         await run(
            `ALTER TABLE ${quotedPhysical} RENAME TO ${quoteRenameTarget(backupTableName, dialect)}`,
         );
         movedAside = true;
      } else {
         movedAside = await exists(quotedBackup);
      }
   } catch (err) {
      // The served table was not moved: it is as it was.
      await dropStaging();
      throw err;
   }

   try {
      await run(`ALTER TABLE ${quotedStaging} RENAME TO ${renameToPhysical}`);
   } catch (err) {
      if (movedAside) {
         try {
            await run(
               `ALTER TABLE ${quotedBackup} RENAME TO ${renameToPhysical}`,
            );
         } catch (restoreErr) {
            // Nothing is dropped: the previous table is under the backup name
            // and the new one under the staging name.
            logger.error(
               "Failed to put a materialized table in place or restore the previous one; both are kept",
               {
                  physicalTableName,
                  backupTableName,
                  stagingTableName,
                  connectionName,
                  error: errMessage(err),
                  restoreError: errMessage(restoreErr),
               },
            );
            throw err;
         }
      }
      await dropStaging();
      throw err;
   }

   if (!movedAside) return;
   try {
      await run(`DROP TABLE IF EXISTS ${quotedBackup}`);
   } catch (dropErr) {
      logger.warn(
         "Failed to drop the previous copy of a rebuilt materialized table; the next build drops it",
         {
            physicalTableName,
            backupTableName,
            connectionName,
            error: errMessage(dropErr),
         },
      );
   }
}
