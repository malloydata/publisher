// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The database tier: a table built in its source's own warehouse, by a
 * delta or by a staging table swapped onto the served name
 * ({@link DatabaseTier}, {@link swapIntoPlace}); its schema capture, its
 * drop and its key check on the source's connection. The swap's transaction
 * text, which differs by dialect, is warehouse_sql.ts's.
 */

import { BuildCancelledError } from "../domain/build_cancelled";
import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { logger } from "../../../logger";
import { errorCodeOf } from "./error_code";
import { errMessage } from "../../../utils";
import { quoteRenameTarget, quoteTablePath } from "../../quoting";
import { storedColumnsOfOutput } from "../stored_column_types";
import { capturedNativeSchema } from "./database_native_types";
import { ManifestEntry } from "../domain/types";
import { capabilitiesOf } from "../domain/dialect";
import type {
   DatabaseBuildRequest,
   DatabaseSite,
   DroppedTable,
   KeyCheck,
   KeyCheckRecord,
   TierAdapter,
} from "./tier";
import { runRollupKeyCheck } from "./inline_uniqueness";
import { buildDatabaseDelta } from "./incremental_delta_strategy";
import {
   advanceLedgerAfterSeed,
   ledgerFields,
   refreshFields,
   resetLedger,
} from "./incremental_build";
import { backupSuffix, stagingSuffix } from "./build_table_names";
import { transactionalSwapSql } from "./warehouse_sql";
import { runAsInternalStatement } from "../../duckdb_lockdown";

/**
 * A full rebuild of a database table in the source's own warehouse: COPY into
 * a staging table, then swap it onto the physical name (see
 * {@link swapIntoPlace}). The served table is never written in place, so it is
 * never half-written. What the swap guarantees depends on the dialect:
 * - Postgres and DuckDB (transactional DDL): the drop of the served table and
 *   the rename onto its name are one transaction. A reader never finds the
 *   name missing, and a failure, including a Postgres view that depends on
 *   the served table, leaves it exactly as it was.
 * - every other dialect: two renames, the served table moved aside to a
 *   backup name and dropped only once the new one holds its name. Between
 *   them a reader finds no table under the physical name (a not-found error,
 *   never a partial table), and a crash there leaves the previous table under
 *   the backup name, which the next build of the source restores or replaces.
 * The staging and backup names derive from the sourceEntityId. For an
 * incremental source it also clears the ledger before the rebuild and records
 * the new boundary after it.
 */
export class DatabaseTier
   implements TierAdapter<DatabaseBuildRequest, DatabaseSite>
{
   readonly tier = "database" as const;

   /** A delta for an incremental table the run can advance, else a rebuild in full. */
   async build(request: DatabaseBuildRequest): Promise<ManifestEntry> {
      return (await buildDatabaseDelta(request)) ?? this.buildInFull(request);
   }

   /**
    * Drop the table, quoted as the build quoted it so what built also drops,
    * then the staging table a crash between its create and the swap can
    * leave, and the previous copy a crash between the swap's two renames
    * can leave.
    */
   async drop(site: DatabaseSite, table: DroppedTable): Promise<void> {
      const { connection, runOptions } = site;
      const dialect = connection.dialectName;
      try {
         await connection.runSQL(
            `DROP TABLE IF EXISTS ${quoteTablePath(
               table.physicalTableName,
               dialect,
            )}`,
            runOptions,
         );
      } catch (err) {
         if (!table.onDropFailed) throw err;
         await table.onDropFailed(err);
      }
      if (table.leftoversOf === undefined) return;
      await connection.runSQL(
         `DROP TABLE IF EXISTS ${quoteTablePath(
            `${table.physicalTableName}${stagingSuffix(table.leftoversOf)}`,
            dialect,
         )}`,
         runOptions,
      );
      await connection.runSQL(
         `DROP TABLE IF EXISTS ${quoteTablePath(
            `${table.physicalTableName}${backupSuffix(table.leftoversOf)}`,
            dialect,
         )}`,
         runOptions,
      );
   }

   /** On the warehouse connection itself. */
   checkKey(
      site: DatabaseSite,
      check: KeyCheck,
      table: string,
   ): Promise<KeyCheckRecord | undefined> {
      return runRollupKeyCheck(check, site.connection, table);
   }

   /** The staging CTAS and the swap onto the served name. */
   private async buildInFull(
      request: DatabaseBuildRequest,
   ): Promise<ManifestEntry> {
      const {
         persistSource,
         instruction,
         connection,
         manifest,
         buildSQL,
         runOptions,
         incrementalRefresh,
         replacesView,
         signal,
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
         // A run cancelled before its read starts reads nothing; a driver
         // that honours the signal cancels the read in flight.
         if (signal?.aborted) throw new BuildCancelledError();
         const ctas = await connection.runSQL(
            `CREATE TABLE ${quotedStaging} AS (${buildSQL})`,
            signal ? { ...runOptions, abortSignal: signal } : runOptions,
         );
         buildCostBytes = ctas?.runStats?.queryCostBytes;
         // Cancelled while it read (a driver that ignores the signal): the
         // staging table is dropped and never swapped into place.
         if (signal?.aborted) throw new BuildCancelledError();
      } catch (err) {
         await dropStagingAfterFailure({
            connection,
            runOptions,
            dialect,
            stagingTableName,
            connectionName: persistSource.connectionName,
         });
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
         ...(replacesView ? { replacesView } : {}),
      });

      // Visible to later downstreams in this run, under the SAME quoted path
      // the CREATE used (pasted into a downstream `FROM` verbatim). The returned
      // entry keeps the logical name.
      manifest.update(sourceEntityId, { tableName: quotedPhysical });

      const durationMs = Math.round(performance.now() - startTime);
      // A database `#@ persist` table is bound on the served copy with the
      // columns its build wrote, captured here as a `storage=` build captures
      // them (the v1 cutover's G1).
      const schema = await capturedDatabaseSchema({
         connection,
         physicalTableName,
         dialect,
         sourceName: persistSource.name,
      });

      // Each column's type as the warehouse names it, so the served copy
      // reads a big-integer column through a cast to exactly that type and
      // never narrows one (known limitation 12). Captured on every build: a
      // `#(materialize)` table records no other schema.
      const nativeSchema = await capturedNativeSchema({
         run: (sql, options) =>
            connection.runSQL(sql, { ...runOptions, ...options }),
         physicalTableName,
         dialect,
         sourceName: persistSource.name,
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
         realization: "COPY",
         ...ledgerFields(incrementalRefresh?.lineage, seededThrough),
         // Reported for any incremental source that rebuilt, so a full rebuild
         // is distinguishable from a delta.
         ...refreshFields(incrementalRefresh ? "full" : undefined),
         rowCount: null,
         buildDurationMs: durationMs,
         // The recurring warehouse cost of keeping this source materialized.
         queryCostBytes: buildCostBytes ?? null,
         ...(schema?.length ? { schema } : {}),
         ...(nativeSchema?.length ? { nativeSchema } : {}),
         ...request.upstreamReuse,
      };
   }
}

/**
 * The columns of a database table just built, read back from its warehouse
 * through the connection's own schema reader and spelled as a `storage=`
 * entry's are (DuckDB's names; `storedColumnsOfOutput`). Undefined when the
 * read fails or yields nothing: the build stands, and the served copy derives
 * the columns from the compiled source instead (database_bindings.ts).
 */
export async function capturedDatabaseSchema(args: {
   connection: Pick<MalloyConnection, "fetchSchemaForTables">;
   physicalTableName: string;
   dialect: string;
   sourceName: string;
}): Promise<{ name: string; type: string }[] | undefined> {
   const key = args.physicalTableName;
   // Read as the build CREATEd it: quoted for the dialect, so a name a
   // case-folding warehouse (Snowflake) keeps in its case is found.
   const tablePath = quoteTablePath(key, args.dialect);
   try {
      const read = await args.connection.fetchSchemaForTables(
         { [key]: tablePath },
         { refreshTimestamp: Date.now() },
      );
      const fields = read.schemas[key]?.fields;
      if (!fields?.length) {
         logger.warn(
            "Could not read a database table's columns after its build; the served copy derives them",
            {
               sourceName: args.sourceName,
               physicalTableName: key,
               error: read.errors[key],
            },
         );
         return undefined;
      }
      const columns = storedColumnsOfOutput(fields, {
         nested: capabilitiesOf(args.dialect).nestedColumns,
         bigint: true,
         timestamptz: true,
      });
      return columns.length > 0 ? columns : undefined;
   } catch (err) {
      logger.warn(
         "Could not read a database table's columns after its build; the served copy derives them",
         {
            sourceName: args.sourceName,
            physicalTableName: key,
            error: errMessage(err),
         },
      );
      return undefined;
   }
}

/**
 * Drop the staging table of a build that failed. Never throws: the build's own
 * failure is what the caller reports, so a failed drop is only logged, as the
 * table it leaves behind in the warehouse.
 */
async function dropStagingAfterFailure({
   connection,
   runOptions,
   dialect,
   stagingTableName,
   connectionName,
}: {
   connection: Pick<MalloyConnection, "runSQL">;
   runOptions: DatabaseBuildRequest["runOptions"];
   dialect: string;
   stagingTableName: string;
   connectionName: string;
}): Promise<void> {
   try {
      await connection.runSQL(
         `DROP TABLE IF EXISTS ${quoteTablePath(stagingTableName, dialect)}`,
         runOptions,
      );
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
}

interface SwapArgs {
   connection: Pick<MalloyConnection, "runSQL">;
   runOptions: DatabaseBuildRequest["runOptions"];
   dialect: string;
   physicalTableName: string;
   stagingTableName: string;
   backupTableName: string;
   connectionName: string;
   /** Run first in the swap's transaction: see DatabaseBuildRequest. */
   replacesView?: string[];
}

/**
 * Put a built staging table in place of the served one. On a dialect with
 * transactional DDL the swap is one atomic transaction
 * ({@link swapInTransaction}); elsewhere the served table is renamed aside
 * first and dropped only once the new one holds its name
 * ({@link swapByRenamingAside}). Either way a failure leaves the physical name
 * holding the previous table or the new one, never neither for good, and gives
 * back the staging table once the served one is intact.
 */
export async function swapIntoPlace(args: SwapArgs): Promise<void> {
   if (capabilitiesOf(args.dialect).transactionalSwap) {
      await swapInTransaction(args);
      return;
   }
   if (args.replacesView?.length) {
      // Only DuckDB and Postgres are given these, whose swap is one
      // transaction; anywhere else they run just before it.
      for (const sql of args.replacesView) {
         await args.connection.runSQL(sql, args.runOptions);
      }
   }
   await swapByRenamingAside(args);
}

/**
 * `DROP TABLE IF EXISTS <physical>` then `ALTER TABLE <staging> RENAME TO
 * <physical>`, as one transaction sent as ONE driver call. A reader sees the
 * previous table or the new one, never no table, and a failure of either
 * statement rolls both back, leaving the served table as it was. That covers
 * a Postgres view over the served table: it makes the DROP fail, so the build
 * fails with the table intact (rather than the view following a rename to a
 * name that is then dropped). No existence probe is needed.
 *
 * One call, because the transaction must not span calls: Malloy's Postgres
 * connections take a session per call (a pooled one from `pg.Pool`, or a new
 * client each time), so a BEGIN sent on its own would bracket nothing.
 * - Postgres: no BEGIN/COMMIT in the text. The simple query protocol runs a
 *   multi-statement string as one implicit transaction, which a failure rolls
 *   back and ends, so no session is left in an aborted transaction.
 * - DuckDB: a multi-statement string autocommits each statement, so it is
 *   bracketed by BEGIN TRANSACTION/COMMIT, and a ROLLBACK follows a failure as
 *   its own call. That reaches the same session: a DuckDB connection holds one.
 */
async function swapInTransaction({
   connection,
   runOptions,
   dialect,
   physicalTableName,
   stagingTableName,
   connectionName,
   replacesView,
}: SwapArgs): Promise<void> {
   const run = (sql: string) => connection.runSQL(sql, runOptions);
   const swap = transactionalSwapSql(dialect, {
      before: replacesView ?? [],
      quotedPhysical: quoteTablePath(physicalTableName, dialect),
      quotedStaging: quoteTablePath(stagingTableName, dialect),
      renameTarget: quoteRenameTarget(physicalTableName, dialect),
   });
   try {
      // Publisher's own transaction, several statements in one call: admitted
      // past a locked instance's one-statement rule for this exact text only
      // (duckdb_lockdown.ts). It embeds no author SQL.
      await runAsInternalStatement(swap.sql, () => run(swap.sql));
      return;
   } catch (failure) {
      if (swap.rollback !== undefined) {
         try {
            await run(swap.rollback);
         } catch (rollbackErr) {
            // Usually no transaction was left open, which is harmless.
            logger.warn("Failed to roll back a materialized table swap", {
               physicalTableName,
               connectionName,
               error: errMessage(rollbackErr),
               // The swap's error can quote its SQL; it reaches the run's
               // record.
               causeCode: errorCodeOf(failure),
            });
         }
      }
      // Rolled back, so the served table is as it was.
      await dropStagingAfterFailure({
         connection,
         runOptions,
         dialect,
         stagingTableName,
         connectionName,
      });
      if (replacesView?.length && dependedOn(failure)) {
         throw new Error(
            `the table cannot take the name '${physicalTableName}': the name= ` +
               `view this package left there in an earlier release has views of ` +
               `its own over it, so dropping it would drop them too. Nothing was ` +
               `changed. Drop or repoint those views, then build again.`,
         );
      }
      throw failure;
   }
}

/** Whether a statement failed because other objects depend on what it drops. */
export function dependedOn(err: unknown): boolean {
   const code = (err as { code?: unknown } | null)?.code;
   return (
      code === "2BP01" ||
      /depend on it|dependent objects|has dependent/i.test(errMessage(err))
   );
}

/**
 * Whether a statement failed because the table it names does not exist.
 * Liberal by design: the swap reads a not-found as "nothing there" only where
 * being wrong makes a later rename onto an occupied name fail, never a table
 * be dropped (see {@link swapByRenamingAside}). Snowflake's "does not exist or
 * not authorized" is one such case.
 */
export function isTableNotFound(err: unknown): boolean {
   const code = (err as { code?: unknown } | null)?.code;
   // Postgres's undefined_table; MySQL's ER_NO_SUCH_TABLE.
   if (code === "42P01" || code === "ER_NO_SUCH_TABLE" || code === 1146) {
      return true;
   }
   return /does not exist|doesn't exist|not found|cannot be found|no such table|invalid object name|TABLE_OR_VIEW_NOT_FOUND/i.test(
      errMessage(err),
   );
}

/**
 * The swap where DDL is not transactional: the served table is renamed aside
 * to `backupTableName`, the staging table renamed onto the physical name, and
 * only then is the backup dropped. Plain `ALTER TABLE ... RENAME TO` and
 * `DROP TABLE IF EXISTS`: three statements on a rebuild and no existence
 * probe, since a not-found from moving the served table aside is what says
 * there is none. Between the two renames a reader finds no table under the
 * physical name (a not-found error, never a partial table).
 *
 * What survives each failure:
 * - moving the served table aside fails, other than as not-found: when a
 *   probe (`SELECT 1 ... WHERE 1 = 0`, reading no rows) shows the served table
 *   is there, the backup name is stale, so it is dropped and the move retried
 *   once. Otherwise, or when the retry fails, the served table is untouched,
 *   staging is dropped and the error raised. This is the only probe, and only
 *   its success counts: a probe that fails for any reason aborts the swap.
 * - renaming staging into place fails: the backup is renamed back, then
 *   staging is dropped; when that restore fails too, both are kept (an error
 *   names them) and nothing is dropped.
 * - dropping the backup fails: the new table is already served; the backup is
 *   left (a warning names it) and the next build drops it.
 *
 * A backup with no table under the physical name is what a crash between the
 * two renames leaves: it is the last served copy, so it is restored if this
 * swap fails, and dropped only once the new table holds the name.
 */
async function swapByRenamingAside({
   connection,
   runOptions,
   dialect,
   physicalTableName,
   stagingTableName,
   backupTableName,
   connectionName,
}: SwapArgs): Promise<void> {
   const run = (sql: string) => connection.runSQL(sql, runOptions);
   const quotedPhysical = quoteTablePath(physicalTableName, dialect);
   const quotedStaging = quoteTablePath(stagingTableName, dialect);
   const quotedBackup = quoteTablePath(backupTableName, dialect);
   // Not always the bare name: on some dialects a bare rename target moves
   // the table into the session's default container.
   const renameToPhysical = quoteRenameTarget(physicalTableName, dialect);
   const moveAside = `ALTER TABLE ${quotedPhysical} RENAME TO ${quoteRenameTarget(backupTableName, dialect)}`;
   const dropStaging = () =>
      dropStagingAfterFailure({
         connection,
         runOptions,
         dialect,
         stagingTableName,
         connectionName,
      });

   // Whether the previous table is known to sit under the backup name. False
   // also when a crash stranded it there: that copy is restored or dropped
   // below by trying, not by knowing.
   let movedAside = true;
   try {
      await run(moveAside);
   } catch (moveErr) {
      if (isTableNotFound(moveErr)) {
         movedAside = false;
      } else {
         try {
            await run(`SELECT 1 FROM ${quotedPhysical} WHERE 1 = 0`);
         } catch {
            // Whether the served table is there is unknown, so the backup may
            // be the last served copy: touch nothing.
            await dropStaging();
            throw moveErr;
         }
         // The served table is there, so a table under the backup name is
         // stale, and may be what the move collided with.
         try {
            await run(`DROP TABLE IF EXISTS ${quotedBackup}`);
            await run(moveAside);
         } catch (retryErr) {
            // The served table was not moved: it is as it was.
            await dropStaging();
            throw retryErr;
         }
      }
   }

   try {
      await run(`ALTER TABLE ${quotedStaging} RENAME TO ${renameToPhysical}`);
   } catch (err) {
      // Put the previous table back: the one just moved aside, or one a crash
      // stranded under the backup name.
      try {
         await run(`ALTER TABLE ${quotedBackup} RENAME TO ${renameToPhysical}`);
      } catch (restoreErr) {
         if (movedAside || !isTableNotFound(restoreErr)) {
            // Nothing is dropped: the previous table is (or may be) under the
            // backup name and the new one under the staging name.
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
         // No backup: there was nothing to restore.
      }
      await dropStaging();
      throw err;
   }

   // Also drops a copy a crash stranded under the backup name, now replaced.
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
