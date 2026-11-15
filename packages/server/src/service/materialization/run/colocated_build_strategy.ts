// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { logger } from "../../../logger";
import { errorCodeOf } from "./error_code";
import { errMessage } from "../../../utils";
import { quoteRenameTarget, quoteTablePath } from "../../quoting";
import { storedColumnsOfOutput } from "../stored_column_types";
import { capturedNativeSchema } from "./colocated_native_types";
import { ManifestEntry } from "../domain/types";
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
         swapDependents,
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
         swapDependents,
      });

      // Visible to later downstreams in this run, under the SAME quoted path
      // the CREATE used (pasted into a downstream `FROM` verbatim). The returned
      // entry keeps the logical name.
      manifest.update(sourceEntityId, { tableName: quotedPhysical });

      const durationMs = Math.round(performance.now() - startTime);
      // Debug: the run's `materialization.build.finished` is the build's event.
      logger.debug(`Built materialized source ${persistSource.name}`, {
         physicalTableName,
         durationMs,
      });

      // A colocated `#@ persist` table is bound on the served copy with the
      // columns its build wrote, captured here as a `storage=` build captures
      // them (the v1 cutover's G1).
      const schema = await capturedColocatedSchema({
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
         realization: instruction.realization,
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
 * The columns of a colocated table just built, read back from its warehouse
 * through the connection's own schema reader and spelled as a `storage=`
 * entry's are (DuckDB's names; `storedColumnsOfOutput`). Undefined when the
 * read fails or yields nothing: the build stands, and the served copy derives
 * the columns from the compiled source instead (colocated_bindings.ts).
 */
export async function capturedColocatedSchema(args: {
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
            "Could not read a colocated table's columns after its build; the served copy derives them",
            {
               sourceName: args.sourceName,
               physicalTableName: key,
               error: read.errors[key],
            },
         );
         return undefined;
      }
      const columns = storedColumnsOfOutput(fields, {
         nested: args.dialect === "duckdb",
         bigint: true,
         timestamptz: true,
      });
      return columns.length > 0 ? columns : undefined;
   } catch (err) {
      logger.warn(
         "Could not read a colocated table's columns after its build; the served copy derives them",
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
   runOptions: ColocatedBuildRequest["runOptions"];
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

/**
 * Dialects whose swap is one transaction: DDL is transactional there, and the
 * swap reaches one database session (see {@link swapInTransaction}).
 *
 * Not the others: Snowflake, MySQL, Databricks and Trino commit DDL as it
 * runs, and BigQuery refuses DROP and RENAME inside a transaction. SQL Server
 * has transactional DDL, but not this rename, so it gains nothing here.
 */
const TRANSACTIONAL_SWAP_DIALECTS: ReadonlySet<string> = new Set([
   "postgres",
   "duckdb",
]);

interface SwapArgs {
   connection: Pick<MalloyConnection, "runSQL">;
   runOptions: ColocatedBuildRequest["runOptions"];
   dialect: string;
   physicalTableName: string;
   stagingTableName: string;
   backupTableName: string;
   connectionName: string;
   /**
    * A `name=` view the swap must carry: the transactional swap runs each of
    * its attempts' statements before its DROP and after its RENAME, trying
    * the next attempt when one fails (a view over the served table, which
    * only Postgres asks for; or this package's own view AT the name, which
    * the new table replaces). A non-transactional swap runs only its
    * `aside` statements: `before` first, `restore` when the swap fails.
    */
   swapDependents?: ColocatedBuildRequest["swapDependents"];
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
   if (TRANSACTIONAL_SWAP_DIALECTS.has(args.dialect)) {
      await swapInTransaction(args);
      return;
   }
   const aside = args.swapDependents?.aside;
   if (!aside) {
      await swapByRenamingAside(args);
      return;
   }
   const run = (sql: string) => args.connection.runSQL(sql, args.runOptions);
   try {
      for (const sql of aside.before) await run(sql);
   } catch (err) {
      await dropStagingAfterFailure(args);
      throw err;
   }
   try {
      await swapByRenamingAside(args);
   } catch (err) {
      // The swap left the name as it found it (empty): put back what
      // `before` removed.
      try {
         for (const sql of aside.restore) await run(sql);
      } catch (restoreErr) {
         logger.error(
            "Failed to put back a name= view after a failed materialized table swap",
            {
               physicalTableName: args.physicalTableName,
               connectionName: args.connectionName,
               error: errMessage(err),
               restoreError: errMessage(restoreErr),
            },
         );
      }
      throw err;
   }
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
   swapDependents,
}: SwapArgs): Promise<void> {
   const run = (sql: string) => connection.runSQL(sql, runOptions);
   const attempts = swapDependents?.attempts(stagingTableName) ?? [
      { before: [], after: [] },
   ];
   const explicit = dialect !== "postgres";
   let failure: unknown;
   for (const { before, after } of attempts) {
      const body = [
         ...before,
         `DROP TABLE IF EXISTS ${quoteTablePath(physicalTableName, dialect)}`,
         `ALTER TABLE ${quoteTablePath(stagingTableName, dialect)} RENAME TO ${quoteRenameTarget(physicalTableName, dialect)}`,
         ...after,
      ];
      const statements = explicit
         ? ["BEGIN TRANSACTION", ...body, "COMMIT"]
         : body;
      try {
         await run(statements.map((s) => `${s};`).join("\n"));
         return;
      } catch (err) {
         failure = err;
         if (explicit) {
            try {
               await run("ROLLBACK");
            } catch (rollbackErr) {
               // Usually no transaction was left open, which is harmless.
               logger.warn("Failed to roll back a materialized table swap", {
                  physicalTableName,
                  connectionName,
                  error: errMessage(rollbackErr),
                  // The swap's error can quote its SQL; it reaches the run's
                  // record.
                  causeCode: errorCodeOf(err),
               });
            }
         }
      }
   }
   // Every attempt rolled back, so the served table is as it was.
   await dropStagingAfterFailure({
      connection,
      runOptions,
      dialect,
      stagingTableName,
      connectionName,
   });
   throw failure;
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
