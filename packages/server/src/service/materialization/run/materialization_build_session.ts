// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Build-scoped DuckDB sessions on a `storage=` destination: the isolated
 * session a build runs in, the native passthrough read that lets the source
 * warehouse compute the rows ({@link wrapPassthrough},
 * {@link issuePassthroughRead}), the CTAS that writes a stored table with its
 * declared types ({@link buildSourceIntoStorage},
 * {@link createTableWithDeclaredTypes}), the chained build that stacks a
 * downstream on stored parents ({@link planChainedDownstream},
 * {@link buildDownstreamIntoStorage}), and the destination sessions the
 * destination's admin operations (storage_admin.ts) run on. Each source
 * type's part in the read is its passthrough build's (passthrough_build.ts).
 * Read by the `storage=` tier, incremental storage, table sites, storage
 * admin, table loads and catalog_admin.ts.
 *
 * Effects: SQL in the source warehouse (through the passthrough) and in the
 * destination; logs and counts.
 */

import { BuildCancelledError } from "../domain/build_cancelled";
import { sourceDefOf } from "../../governance/ir/compiled_model";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   type Runtime,
   type PersistSource,
   type QueryRecord,
   type RunSQLOptions,
   MalloyError,
} from "@malloydata/malloy";
import { leaveOutDefinitionAt } from "../text/declarations_kept";
import { inMemoryRuntime } from "../text/generated_files";
import {
   existsSync,
   mkdirSync,
   mkdtempSync,
   readdirSync,
   rmSync,
   statSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import type { components } from "../../../api";
import { duckDBByteSize, getDuckDBMemoryLimit } from "../../../config";
import { settings } from "../../../settings";
import {
   BadRequestError,
   MaterializationEligibilityError,
} from "../../../errors";
import { logger } from "../../../logger";
import { errorCodeOf } from "../../../error_code";
import { errMessage } from "../../../utils";
import { tokenizeDuckDBSQL } from "../../duckdb_lockdown";
import {
   quoteIdentifier,
   quoteManifestTablePath,
   tablePathParts,
} from "../../quoting";
import type { TableCollations } from "../domain/collation";
import { collationCatalogBatches } from "./warehouse_sql";
import { ContractCaptureFailedError } from "../../../errors";
import {
   classifySelect,
   containsStatementSeparator,
} from "../../single_select";
import { fakedCollations } from "./column_contract_capture";
import { fromSubselect } from "../subselect";
import {
   type BuildCastOptions,
   canonicalRenames,
   castToDeclaredTypes,
   wideIntegerColumns,
   isCastConversionError,
   keepsWarehouseType,
   STORED_COLUMN_TYPE_MISMATCH_CODE,
   storageBuildCasts,
   type DeclaredColumn,
} from "../stored_column_types";
import { recordStoredColumnTypeMismatch } from "../../../materialization_metrics";
import {
   buildSQLOf,
   plannedSources,
   projectToStoredColumns,
   storedColumnNames,
} from "./run_plan";
import { resultRows, type BuildReadCost } from "./build_read_cost";
import {
   passthroughBuildOf,
   type PassthroughDeps,
   type PassthroughRead,
} from "./passthrough_build";
import type { QueryMetadata } from "../../query_metadata";
import {
   attachDuckLakeReadOnlyIfPresent,
   attachDuckLakeReadWrite,
} from "../../connection";
import { hardCloseWhenIdle } from "../../duckdb_hard_close";
import { withStatementGate } from "../../duckdb_statement_gate";
import { managedSearchPathSql } from "../../dialects/ducklake";
import { serializeSQL } from "../../single_select";
import { fileOutsideScope } from "../../same_catalog_tables";
import type { PartitionKey } from "../../dialects/ducklake_catalog";
import {
   connectionDialectOf,
   federateSourceForPassthrough,
   type FederatedSourceType,
} from "../../dialects";
import type { DestinationDialect } from "../../dialects/types";
import {
   applySessionResourceLimits,
   escapeSQL,
} from "../../duckdb_session_setup";
import { NO_TRANSACTION, type WatermarkBound } from "./incremental_apply";
import type { LateRows } from "../domain/types";
import {
   storageDestinationFile,
   storageDestinationRoot,
} from "../../connection_config";
import {
   allowedForLockdown,
   hasUrlScheme,
   lockDownDuckDBSession,
   runInternalSQL,
} from "../../duckdb_lockdown";
import {
   DEFAULT_DESTINATION_SCHEMA,
   destinationSchemaOf,
   storageTablePath,
} from "../domain/storage_table_path";
import { authorSqlCause, isSameCatalog } from "../domain/same_catalog_build";
import { PASSTHROUGH_SOURCE_TYPES } from "../domain/source_connection_type";
import { capabilitiesOf, STORAGE_DIALECT } from "../domain/dialect";

type ApiConnection = components["schemas"]["Connection"];

type WireColumn = components["schemas"]["Column"];

/**
 * Warehouse types the build can materialize INTO. Wider than
 * `DECLARABLE_STORAGE_DESTINATION_TYPES`, which is what a storage destination may
 * be declared as and admits `ducklake` only — so the `duckdb` branch here is not
 * reachable from configuration.
 */
const STORAGE_DESTINATION_TYPES = ["ducklake", "duckdb"] as const;

/**
 * Wrap a source-dialect SELECT as a native per-engine query-passthrough call, so
 * the warehouse computes and only result rows enter DuckDB. `handle` is the
 * per-engine reference {@link federateSourceForPassthrough} established (a
 * projectId, a secret name, or an ATTACH alias). Argument order differs per
 * engine, which is why this is centralized.
 */
export function wrapPassthrough(
   sourceType: FederatedSourceType,
   handle: string,
   innerSQL: string,
): string {
   const passthrough = connectionDialectOf(sourceType)?.passthrough;
   if (passthrough === undefined) {
      throw new BadRequestError(
         `Unsupported passthrough source type: ${String(sourceType)}`,
      );
   }
   return passthrough.wrap(handle, innerSQL);
}

/**
 * The SELECT a partitioned insert reads, ordered by `expressions`.
 *
 * DuckDB's partitioned COPY (what a DuckLake insert into a laid-out table
 * plans) charges each appender one vector per column for every partition it
 * has met, before flushing any. Rows in partition order keep that set at one or
 * two partitions; interleaved rows bring in every partition within a few
 * chunks, and a wide, many-partition insert then exceeds `memory_limit` at any
 * flush threshold. Wrapped as a subselect so the SELECT is not parsed.
 */
function orderByExpressions(
   selectSQL: string,
   expressions: readonly string[],
): string {
   if (expressions.length === 0) {
      return selectSQL;
   }
   return `SELECT * ${fromSubselect(selectSQL, "partitioned_build")} ORDER BY ${expressions.join(", ")}`;
}

/**
 * Issue the source-warehouse read for a `storage=` build and return what the
 * CTAS should select from. Two shapes, chosen by whether there is a BigQuery
 * label to apply:
 *
 * <ul>
 *   <li><b>Direct</b> — `wrapPassthrough`'s single rows-returning call, for every
 *       engine and for unlabelled BigQuery. Yields no job id.
 *   <li><b>Split (BigQuery, labelled)</b> — `bigquery_execute` runs the SELECT in
 *       a script that sets `@@query_label` first, then `bigquery_scan` reads the
 *       anonymous result table. `bigquery_query()` takes no labels and cannot run
 *       a script, so labelling requires the split.
 * </ul>
 *
 * The split is not a second scan: reading the result table goes through the
 * Storage Read API and creates no new QUERY job. Gating on the label keeps the
 * unlabelled path unchanged, so only builds that asked to be attributed split.
 */
export async function issuePassthroughRead(
   session: DuckDBConnection,
   sourceType: FederatedSourceType,
   handle: string,
   buildSQL: string,
   queryMetadata: QueryMetadata | undefined,
): Promise<PassthroughRead> {
   // The engine's own preparation: BigQuery marks the read so a cancelled
   // build can find its job, and labels it when it can split
   // (passthrough/bigquery.ts); any other engine reads it as written.
   const build = passthroughBuildOf(sourceType);
   const prepared = build
      ? await build.prepareRead(session, handle, buildSQL, queryMetadata)
      : { sql: buildSQL };
   if (prepared.label === undefined || build?.splitRead === undefined) {
      return {
         selectSQL: wrapPassthrough(sourceType, handle, prepared.sql),
         jobId: null,
         cost: null,
      };
   }
   return build.splitRead(session, handle, prepared.sql, prepared.label);
}

/** Narrow a connection's declared type to a supported passthrough source type. */
export function passthroughSourceType(
   sourceConnection: ApiConnection,
): FederatedSourceType {
   const type = sourceConnection.type;
   if ((PASSTHROUGH_SOURCE_TYPES as readonly string[]).includes(type ?? "")) {
      return type as FederatedSourceType;
   }
   if (isCatalogDestination(sourceConnection)) {
      // Reached only when isSameCatalog said no: say why, not just "unsupported".
      throw new BadRequestError(
         `Cannot materialize a '${type}' source into a storage destination ` +
            `that is a different catalog. A DuckLake source builds only into its ` +
            `own catalog, matched by the catalog database's host, port and ` +
            `databaseName (a connectionString is never matched), its ` +
            `metadataSchema, and its data path when both configs state one.`,
      );
   }
   throw new BadRequestError(
      `Cannot materialize a '${type}' source into a storage destination: the ` +
         `native query-passthrough build supports source connections of type ` +
         `${PASSTHROUGH_SOURCE_TYPES.join(", ")} only.`,
   );
}

const BUILD_DIR_PREFIX = "malloy-build-";

/** Whether a process with `pid` exists; EPERM is one this process may not signal. */
function processAlive(pid: number): boolean {
   try {
      process.kill(pid, 0);
      return true;
   } catch (err) {
      return (err as NodeJS.ErrnoException).code === "EPERM";
   }
}

/**
 * Removes the working directories of sessions an earlier process never
 * disposed: an OOM kill restarts the container with its emptyDir, so their
 * spill still counts against the ephemeral-storage limit. A directory is named
 * for its process (`malloy-build-<pid>-…`), and goes when that process is gone.
 * One named for this process's own pid goes only if it predates this process,
 * since a restarted container reuses its pids (pid 1); a directory of another
 * live process is never touched, nor a live session's. One with no pid in its
 * name (an earlier release's) is left alone. Liveness is by pid, so the temp
 * root must not be shared across PID namespaces. Returns how many it removed.
 */
export function removeStaleBuildDirs(
   root: string = os.tmpdir(),
   startedAtMs: number = Date.now() - process.uptime() * 1000,
   ownPid: number = process.pid,
): number {
   let removed = 0;
   try {
      for (const entry of readdirSync(root, { withFileTypes: true })) {
         if (!entry.isDirectory() || !entry.name.startsWith(BUILD_DIR_PREFIX)) {
            continue;
         }
         const dir = path.join(root, entry.name);
         const owner = /^malloy-build-(\d+)-/.exec(entry.name)?.[1];
         // With no pid there is no telling whether an older publisher sharing this temp root still uses it.
         if (owner === undefined) continue;
         const pid = Number(owner);
         try {
            if (
               pid !== ownPid
                  ? processAlive(pid)
                  : statSync(dir).mtimeMs >= startedAtMs
            ) {
               continue;
            }
            rmSync(dir, { recursive: true, force: true });
            removed += 1;
         } catch (err) {
            logger.warn("Failed to remove a stale build working directory", {
               dir,
               code: errorCodeOf(err),
            });
         }
      }
   } catch (err) {
      logger.warn("Failed to list stale build working directories", {
         root,
         code: errorCodeOf(err),
      });
   }
   if (removed > 0) {
      logger.info("Removed stale build working directories", { removed });
   }
   return removed;
}

/**
 * The error types DuckDB does not abort a transaction on
 * (`Exception::InvalidatesTransaction`), matched by message prefix because
 * node-api exposes no error type.
 */
const TRANSACTION_SAFE_ERROR =
   /^(Binder|Catalog|Connection|Parameter Not Allowed|Parser|Permission) Error:/;

/** The two error types that invalidate a DuckDB instance; DuckDB puts the type first, so a quoted value never matches. */
const FATAL_ERROR = /^(INTERNAL|FATAL) Error:/;

/** Whether `sql` is a ROLLBACK (or its alias ABORT), the one statement a refusing session still runs. */
function isRollback(sql: string): boolean {
   const [verb, ...rest] = tokenizeDuckDBSQL(sql).filter(
      (token) => token.kind !== "comment",
   );
   if (
      verb?.kind !== "word" ||
      (verb.value !== "rollback" && verb.value !== "abort")
   ) {
      return false;
   }
   // At most one modifier: DuckDB's parser refuses `ROLLBACK WORK WORK`.
   const [modifier, ...tail] = rest;
   const after =
      modifier?.kind === "word"
         ? modifier.value === "transaction" || modifier.value === "work"
            ? tail
            : undefined
         : rest;
   return (
      after !== undefined &&
      after.every((token) => token.kind === "punct" && token.value === ";")
   );
}

/** A statement DuckDB refused to parse, so nothing of it ran. */
const PARSER_ERROR = /^Parser Error:/;

/**
 * The connection of a build-scoped session. It opens its instance with
 * `disable_database_invalidation`, so an INTERNAL or FATAL error fails its
 * statement and the instance lives. Otherwise DuckDB 1.5 refuses the
 * ROLLBACK, and the ClientContext destructor's DuckLake rollback throws and
 * aborts the process (DuckDB issue 26132, fixed in 2.0). The setting also lifts
 * DuckDB's refusal of statements after an error, which this class restores for
 * every transaction on the session: after a fatal error, an error that aborts
 * the open transaction, or a cancel, every statement but a rollback is refused.
 * The check, the statement and the latch run under one hold of the statement
 * gate, so no other statement interleaves. The gaps: DuckDB's own default, a
 * Binder or Catalog error mid-script leaves the earlier statements committable;
 * and a DuckLake catalog kept in a local DuckDB file is still checkpointed on
 * close after a fatal error (production catalogs keep their metadata in
 * Postgres). Revisit both on DuckDB >= 2.0.1.
 */
export class IsolatedSessionConnection extends DuckDBConnection {
   /** The first INTERNAL or FATAL error: permanent. */
   private fatalError: Error | undefined;
   /** The error that aborted the open transaction: cleared by a rollback. */
   private abortedBy: Error | undefined;
   private cancelled = false;
   /** The transaction probe is running: a statement-scoped interrupt must not kill it. */
   private probing = false;

   /** The first fatal error the session latched. */
   get fatal(): Error | undefined {
      return this.fatalError;
   }

   /** Refuse everything but a rollback from now on, and interrupt the statement in flight ({@link interruptOnAbort}). */
   cancel(): void {
      this.cancelled = true;
      this.interrupt();
   }

   /** Interrupt the statement in flight; DuckDB stops it at its next poll. A no-op while the probe runs, whose statement is not the one being given up. */
   interrupt(): void {
      if (!this.probing) this.connection?.interrupt();
   }

   protected override runDuckDBQuery(
      sql: string,
   ): Promise<{ rows: QueryRecord[]; totalRows: number }> {
      const rollback = isRollback(sql);
      return withStatementGate(this, async () => {
         if (!rollback) this.refuseIfEnded();
         let ran = true;
         try {
            return await super.runDuckDBQuery(sql);
         } catch (err) {
            ran = !PARSER_ERROR.test(errMessage(err));
            await this.latch(err, rollback);
            throw err;
         } finally {
            // DuckDB clears the transaction before it rolls back, so even a failed rollback leaves none open; a parse error ran nothing.
            if (rollback && ran) this.abortedBy = undefined;
         }
      });
   }

   override async *runSQLStream(
      sql: string,
      options?: RunSQLOptions,
   ): AsyncIterableIterator<QueryRecord> {
      this.refuseIfEnded();
      yield* super.runSQLStream(sql, options);
   }

   private refuseIfEnded(): void {
      if (this.cancelled) throw new BuildCancelledError();
      if (this.fatalError) {
         throw new Error(
            "The build session refuses statements after a fatal DuckDB error",
            { cause: this.fatalError },
         );
      }
      if (this.abortedBy) {
         throw new Error(
            "The build session's transaction was aborted by an earlier error and can only be rolled back",
            { cause: this.abortedBy },
         );
      }
   }

   private async latch(err: unknown, rollback: boolean): Promise<void> {
      const error = err instanceof Error ? err : new Error(String(err));
      if (FATAL_ERROR.test(error.message)) {
         this.fatalError ??= error;
      } else if (
         !rollback &&
         !this.cancelled &&
         !TRANSACTION_SAFE_ERROR.test(error.message)
      ) {
         if (await this.transactionIsOpen()) this.abortedBy ??= error;
      }
   }

   /**
    * Whether a transaction is open, asked of DuckDB: a BEGIN inside one fails
    * and leaves it untouched, and with none open the pair leaves none. Any
    * other failure counts as open, so the next rollback clears it.
    */
   private async transactionIsOpen(): Promise<boolean> {
      this.probing = true;
      try {
         await super.runDuckDBQuery("BEGIN; ROLLBACK");
         return false;
      } catch {
         return true;
      } finally {
         this.probing = false;
      }
   }
}

// db-duckdb's option builder is private and has no hook for an instance setting.
// It is looked up when called, never captured: Publisher patches it (thread cap, open-time lockdown) and that must still apply.
// It runs inside the base constructor, so it reads no field of the subclass.
type InstanceOptionsBuilder = (opts?: unknown) => Record<string, string>;
(
   IsolatedSessionConnection.prototype as unknown as {
      buildInstanceOptions: InstanceOptionsBuilder;
   }
).buildInstanceOptions = function (this: DuckDBConnection, opts?: unknown) {
   const base = (
      DuckDBConnection.prototype as unknown as {
         buildInstanceOptions: InstanceOptionsBuilder;
      }
   ).buildInstanceOptions;
   return {
      ...base.call(this, opts),
      disable_database_invalidation: "true",
   };
};

/**
 * A build-scoped DuckDB session on its OWN in-memory instance, plus a disposer
 * that closes it and removes its throwaway working directory.
 *
 * Isolation is load-bearing for multi-tenant safety. `@malloydata/db-duckdb`
 * pools instances in a process-global cache keyed by a share key that EXCLUDES
 * the connection name, so sessions with identical config share one in-memory
 * instance while their lifetimes overlap. Shared, they collide on transient
 * ATTACH aliases: two builds (or two environments with same-named connections
 * holding different credentials) clobber each other's `ATTACH … AS lake`, a
 * cross-tenant read or WRITE.
 *
 * A UNIQUE `workingDirectory` is part of the share key, so each session gets its
 * own instance (catalog, secrets, attaches). `databasePath` stays `:memory:`,
 * not a temp file, because only a `:memory:` primary lets a DuckLake attach
 * auto-initialize a fresh catalog. The working directory also holds the build's
 * spill (`temp_directory`) and nothing else; removal on dispose is best-effort.
 */
export function createIsolatedBuildSession(sessionName: string): {
   session: IsolatedSessionConnection;
   dispose: () => Promise<void>;
   /**
    * The session's private working directory. Returned so the caller can point
    * DuckDB's `temp_directory` at it: it is unique per build and removed by
    * `dispose`, so spill from one build can neither outlive it nor collide with
    * another build's.
    */
   workDir: string;
} {
   const workDir = mkdtempSync(
      path.join(os.tmpdir(), `${BUILD_DIR_PREFIX}${process.pid}-`),
   );
   // The disposer that owns removing workDir does not exist until this function
   // returns, so a throw from the constructor would strand the directory with
   // nothing left holding a reference to it.
   let session: IsolatedSessionConnection;
   try {
      // Lifts db-duckdb's default `rowLimit` of 10, which `runSQL` enforces by
      // slicing. `DESCRIBE` and `duckdb_columns()` return one row per COLUMN, so
      // the default would truncate a wide table's schema: the delta path would
      // see a shape mismatch and rebuild forever, and the manifest would declare
      // a short schema.
      session = new IsolatedSessionConnection(
         sessionName,
         ":memory:",
         workDir,
         {
            rowLimit: Number.MAX_SAFE_INTEGER,
         },
      );
   } catch (err) {
      rmSync(workDir, { recursive: true, force: true });
      throw err;
   }
   const dispose = async () => {
      await rollbackBeforeDisconnect(session, sessionName);
      // close() leaves the native ClientContext, with its attaches and credentials, alive until disconnected.
      try {
         await session.close();
      } catch (err) {
         logger.warn("Failed to close build session (leaked session)", {
            sessionName,
            code: errorCodeOf(err),
         });
      }
      hardCloseWhenIdle(session);
      try {
         rmSync(workDir, { recursive: true, force: true });
      } catch (err) {
         logger.warn("Failed to remove build session working directory", {
            sessionName,
            workDir,
            code: errorCodeOf(err),
         });
      }
   };
   return { session, dispose, workDir };
}

/** A session already closed, or not yet open: a second dispose, or one before any statement. */
const SESSION_CLOSED =
   /^Connection not open$|^DuckDB connection ".*" is closed$/;

/**
 * Roll back whatever `session` left open before it is disconnected: the
 * disconnect's destructor rolls an open DuckLake transaction back itself, and
 * a failure there throws past the destructor and aborts the process. DuckDB
 * clears the transaction before it rolls back, so this protects even when the
 * ROLLBACK fails. Never throws.
 */
async function rollbackBeforeDisconnect(
   session: IsolatedSessionConnection,
   sessionName: string,
): Promise<void> {
   if (session.fatal) {
      // Only the matched type, never the message, which can quote a value.
      const fatalType = FATAL_ERROR.exec(session.fatal.message)?.[1];
      logger.error("A fatal DuckDB error ended a build session", {
         sessionName,
         fatalType,
      });
   }
   try {
      // Queues behind a statement still in flight; no isolated session streams, which is what would hold the gate.
      await session.runRawSQL("ROLLBACK");
   } catch (err) {
      const message = errMessage(err);
      if (NO_TRANSACTION.test(message)) {
         logger.debug("No transaction was open when a build session closed", {
            sessionName,
            code: errorCodeOf(err),
         });
      } else if (!SESSION_CLOSED.test(message)) {
         logger.warn("Could not roll back a build session before closing it", {
            sessionName,
            code: errorCodeOf(err),
         });
      }
   }
}

/**
 * Pin a build session's timezone before any predicate is rendered against it.
 *
 * A `TIMESTAMPTZ` column compared with a naive `TIMESTAMP` literal resolves in
 * the session's zone, and the incremental path renders bounds as UTC text; on a
 * host-local zone the window silently shifts, and a boundary could LEAD the
 * table. Malloy's DuckDB connector already sets UTC; this keeps the property
 * from depending on another package's setup.
 */
export async function pinSessionToUTC(
   session: DuckDBConnection,
): Promise<void> {
   await session.runSQL("SET TimeZone='UTC'");
}

/**
 * Run `fn` on an isolated session ({@link createIsolatedBuildSession}) with
 * its resource limits applied, then dispose it. The limits come first, before
 * any attach: a DuckLake attach applies (and latches) them without the
 * session's own `tempDirectory`, so spill would go to the shared configured
 * path, outliving `dispose` and colliding between builds. They also bound
 * memory during the attach itself, and even a DROP is bounded: the sum of
 * caps across all live instances is what has to fit the container.
 */
export async function withIsolatedSession<T>(
   sessionName: string,
   fn: (session: IsolatedSessionConnection) => Promise<T>,
): Promise<T> {
   const { session, dispose, workDir } =
      createIsolatedBuildSession(sessionName);
   try {
      await applySessionResourceLimits(session, { tempDirectory: workDir });
      return await fn(session);
   } finally {
      await dispose();
   }
}

/**
 * Run `fn` on an isolated session named `<purpose>_<destination>`, with a
 * `storage=` destination attached read-write (as `attachAs`, its own name by
 * default): the serve attach is read-only, and isolation keeps the attach
 * from colliding with another tenant's ({@link createIsolatedBuildSession}).
 *
 * `build`: a build writes a table here, so the destination's schema must
 * exist, DuckLake row inlining is off (rows land as Parquet in the data
 * store, never in a shared catalog's Postgres; the catalog's persisted
 * options are untouched), and the session is pinned to UTC
 * ({@link pinSessionToUTC}).
 */
export async function withDestinationSession<T>(
   destination: {
      destinationName: string;
      destinationConnection: ApiConnection;
      environmentPath: string;
   },
   options: {
      purpose: string;
      attachAs?: string;
      build?: boolean;
      /**
       * The run's cancel signal: on abort the session is interrupted
       * ({@link interruptOnAbort}), so its statement in flight stops at
       * DuckDB's next poll and nothing after it runs.
       */
      signal?: AbortSignal;
      /**
       * `"caller"`: the caller confines the session itself, after setup of
       * its own (a build that federates its source first). By default the
       * session is confined to the destination before `fn` runs.
       */
      lockdown?: "caller";
      /**
       * Attach the destination read-only, for a read that only looks (a
       * bind's read of its tables). Never with `build`.
       */
      readOnly?: boolean;
      /**
       * Other destinations attached read-only beside this one, before the
       * session is confined, each under its own name: a build that reads a
       * table in another destination. Never written: their attach is
       * read-only, and the confinement allows only their own paths besides
       * this destination's.
       */
      alsoRead?: readonly {
         destinationName: string;
         destinationConnection: ApiConnection;
      }[];
   },
   fn: (session: IsolatedSessionConnection) => Promise<T>,
): Promise<T> {
   const { destinationName, destinationConnection, environmentPath } =
      destination;
   assertSupportedDestination(destinationName, destinationConnection);
   const alias = options.attachAs ?? destinationName;
   return withIsolatedSession(
      `${options.purpose}_${destinationName}`,
      async (session) => {
         const stopInterrupting = interruptOnAbort(session, options.signal);
         try {
            return await onDestinationSession(session);
         } finally {
            stopInterrupting();
         }
      },
   );

   async function onDestinationSession(
      session: IsolatedSessionConnection,
   ): Promise<T> {
      if (options.readOnly) {
         await attachDestinationReadOnly(
            session,
            { alias, destinationName },
            destinationConnection,
            environmentPath,
         );
      } else {
         await attachDestinationReadWrite(
            session,
            { alias, destinationName },
            destinationConnection,
            environmentPath,
         );
      }
      if (options.build) {
         await assertDestinationSchemaExists(
            session,
            alias,
            destinationName,
            destinationConnection,
         );
         for (const setting of catalogDestinationOf(destinationConnection)
            ?.buildSettings ?? []) {
            await session.runSQL(setting);
         }
         await pinSessionToUTC(session);
      }
      if (options.build) {
         // A build writes table files here: its local data path must exist
         // before the session is locked, whoever locks it.
         createLocalDataPaths(session, destinationAllowlist(destination));
      }
      const others = options.alsoRead ?? [];
      for (const other of others) {
         assertSupportedDestination(
            other.destinationName,
            other.destinationConnection,
         );
         await attachDestinationReadOnly(
            session,
            {
               alias: other.destinationName,
               destinationName: other.destinationName,
            },
            other.destinationConnection,
            environmentPath,
         );
      }
      if (options.lockdown !== "caller") {
         // Setup done (the destinations attached): confine the session to
         // them (duckdb_lockdown.ts) before any of its work runs. Each
         // `alsoRead` destination's data path is allowed too, so the build
         // can read its files: DuckDB's `allowed_directories` cannot allow a
         // path for reading alone, so writes there are kept out by the
         // catalog's READ_ONLY attach and by the one statement such a build
         // runs that a model shapes, which must be a single SELECT
         // ({@link assertSingleSelect}): a SELECT cannot `COPY ... TO`. That
         // SELECT is compiled in a session without them
         // (buildDownstreamIntoStorage), so the compile's reads never run
         // with them attached either.
         await lockDownDestinationSession(session, [
            ...destinationAllowlist(destination),
            ...others.flatMap((other) =>
               destinationAllowlist({
                  destinationConnection: other.destinationConnection,
                  environmentPath,
               }),
            ),
         ]);
      }
      return fn(session);
   }
}

/**
 * The bounds for an admin table load or copy, which runs beside the server's
 * other work in a pod sized for it: a memory cap below the pod's (the
 * operator's own `duckdb.memoryLimit` when that is lower), a spill cap under
 * its ephemeral storage, unordered output (so the writer streams), and
 * the HTTP metadata cache for the many small reads of a remote scan.
 */
export async function applyLoadSessionSettings(
   session: DuckDBConnection,
): Promise<void> {
   const cap = settings.get("admin.memoryLimit");
   const configured = getDuckDBMemoryLimit();
   const memoryLimit =
      configured !== undefined &&
      duckDBByteSize(configured) < duckDBByteSize(cap)
         ? configured
         : cap;
   for (const setting of [
      `memory_limit='${escapeSQL(memoryLimit)}'`,
      `max_temp_directory_size='${escapeSQL(settings.get("admin.maxTempDirectorySize"))}'`,
      "preserve_insertion_order=false",
      // No parquet_metadata_cache: DuckDB < 2.0 fails the second read of an encrypted file it cached (DuckDB issue 24311).
      "enable_http_metadata_cache=true",
   ]) {
      await session.runSQL(`SET ${setting}`);
   }
}

/**
 * Throws unless `sql` is exactly one SELECT, by DuckDB's own parser and as
 * the connector would split it: what a build in a session that also reads
 * another destination may run from a model. Its data path is in the
 * session's allowlist, which DuckDB cannot make read-only, so a statement
 * that writes files (`COPY ... TO`, `EXPORT DATABASE`) must never reach the
 * session; one SELECT writes nothing.
 */
export async function assertSingleSelect(sql: string): Promise<void> {
   const verdict = containsStatementSeparator(sql)
      ? { refused: "it holds the connector's statement separator" }
      : await classifySelect(sql);
   if ("refused" in verdict || verdict.selects !== 1) {
      throw new Error(
         `A build that reads another storage destination runs one SELECT ` +
            `from its model, and this SQL is not one (${
               "refused" in verdict
                  ? verdict.refused
                  : `${verdict.selects} statements`
            }), so it is not run`,
      );
   }
}

/**
 * Confine a destination session to `allow` and the paths its setup recorded
 * (duckdb_lockdown.ts). The configuration is left unlocked: a build sets
 * per-step options (threads around a partitioned write, a search path).
 */
export async function lockDownDestinationSession(
   session: DuckDBConnection,
   allow: readonly string[],
): Promise<void> {
   await lockDownDuckDBSession(session, {
      allow: [...allow],
      lockConfiguration: false,
   });
}

/**
 * Create each local directory among `allow` and the paths the session's
 * attaches have recorded so far, for a session that writes table files into
 * its destination: once locked, a session may create directories only inside
 * an allowed path, never the path itself, so the first write into a fresh
 * local DuckLake data path would fail. Called after the destination's attach
 * and before any other (a copy's source, a build's federation), so only the
 * destination's own paths are created. An object-store prefix is left alone.
 *
 * Best effort: a path that cannot be created here (a read-only or foreign
 * mount, a path recorded on another host) is logged and skipped, and the
 * lock proceeds; a write that needs it is still refused by DuckDB.
 */
export function createLocalDataPaths(
   session: DuckDBConnection,
   allow: readonly string[],
): void {
   for (const dir of new Set([...allow, ...allowedForLockdown(session)])) {
      if (dir === "" || hasUrlScheme(dir)) continue;
      try {
         mkdirSync(dir, { recursive: true });
      } catch (err) {
         logger.debug(
            "Could not create a destination's local data path; locking without it",
            {
               path: dir,
               code: (err as NodeJS.ErrnoException).code,
            },
         );
      }
   }
}

/**
 * Stop `session`'s work when `signal` aborts (now, if it already has), until
 * the returned function is called. A statement in flight is interrupted,
 * and DuckDB stops it at its next poll; every statement after the abort is
 * refused with {@link BuildCancelledError} before it starts (a rollback is
 * not, so a transaction ends in place), so an abort that lands while the
 * session is idle (between the federation and the CTAS, say) is not lost. A
 * read DuckDB makes through a warehouse extension (BigQuery's) is not
 * cancelled in the warehouse by the interrupt: a BigQuery build cancels its
 * jobs itself (passthrough/bigquery.ts).
 */
export function interruptOnAbort(
   session: IsolatedSessionConnection,
   signal: AbortSignal | undefined,
): () => void {
   if (!signal) return () => {};
   const interrupt = () => {
      // Single-use: the session stays cancelled after the release.
      try {
         session.cancel();
      } catch (err) {
         logger.warn("Could not interrupt a cancelled build's session", {
            code: errorCodeOf(err),
         });
      }
   };
   const release = () => signal.removeEventListener("abort", interrupt);
   if (signal.aborted) {
      interrupt();
      return release;
   }
   signal.addEventListener("abort", interrupt, { once: true });
   return release;
}

/** Result of building one source into a storage destination. */
export interface StorageBuildResult {
   /** The connection the physical table now lives in (the destination). */
   storageDestinationName: string;
   /** Authoritative DuckDB column schema, captured post-build via DESCRIBE. */
   schema: WireColumn[];
   /**
    * The table's column collations, read from its definition in the same
    * session ({@link storedCollations}), for its column contract. Undefined
    * when the read failed.
    */
   collations?: TableCollations;
   /**
    * The addresses of the stored tables a build stacked on read in place of
    * its source: what its entry's `builtFrom` names, and whose contracts
    * bound how its strings compare. Absent for a build that read its
    * source's warehouse.
    */
   readFrom?: string[];
   /**
    * Rows in the table after the build, counted in the destination (cheap in
    * DuckDB and DuckLake, which keep row counts with their files), and
    * recorded as the manifest entry's `rowCount`. Null when the count could
    * not be read; the build stands either way.
    */
   rowCount: number | null;
   /**
    * What the source warehouse charged for the read, when its shape reported it.
    * Null means unknown, not free (except on a chained build, which reads no
    * warehouse).
    */
   readCost: BuildReadCost | null;
   /** Present only when the table was advanced IN PLACE rather than rebuilt. */
   refresh?: StorageRefreshOutcome;
   /**
    * For an incremental source that was rebuilt: where the new table reaches,
    * the boundary that lets the NEXT refresh be a delta.
    */
   seededThrough?: WatermarkBound;
   /**
    * For a source that reads a persisted upstream: its rows were computed
    * from the upstream's STORED table in the destination (`reused`). Absent
    * for a source with no persisted upstream, whose only build reads the
    * warehouse.
    */
   upstreamReuse?: "reused";
}

/** What an in-place refresh did, for the manifest entry to report. */
interface StorageRefreshOutcome {
   /** Absent for a refresh that applied nothing (the watermark had not moved). */
   durationMs?: number;
   /** The boundary now in force — where the next refresh starts. */
   coveredThrough?: WatermarkBound;
   /** `delta` advanced the table; `none` found nothing to apply. */
   refresh: "delta" | "none";
   /** What the delta's own warehouse read cost, when its shape reported it. */
   readCost: BuildReadCost | null;
   /** What the late-row check found below the delta (domain/types.ts). */
   lateRows?: LateRows;
}

/**
 * A caller's chance to advance the table IN PLACE instead of rebuilding it,
 * handed a session that already holds the destination read-write and the source
 * federated. A callback because the decision is the caller's but the session
 * (whose credentials must not outlive this build) is owned here. Returning
 * undefined means "rebuild", which then runs on the same session.
 */
export interface StorageIncrementalRefresh {
   /**
    * Advance the table in place, or return undefined to have it rebuilt. Runs
    * after the attach and the federation, before the CTAS.
    */
   plan: (deps: {
      session: IsolatedSessionConnection;
      sourceType: FederatedSourceType;
      handle: string;
      /** The destination-qualified, quoted path the DML must name. */
      quotedTablePath: string;
   }) => Promise<StorageRefreshOutcome | undefined>;
   /**
    * Record where the table a REBUILD just wrote reaches, so the next refresh can
    * be a delta, and return that boundary. Runs on the same session after the
    * CTAS, since the boundary is probed from the table. Undefined when there is
    * no boundary (an empty source), so the next refresh rebuilds again.
    */
   afterSeed: (deps: {
      session: DuckDBConnection;
      quotedTablePath: string;
   }) => Promise<WatermarkBound | undefined>;
}

/**
 * {@link StorageIncrementalRefresh} for a table stacked on stored upstreams
 * in its own destination (an incremental extension table over its parent's
 * table): the session holds the destination alone, and the rows a delta
 * reads are the stacked build's own SQL (`storedSQL`) run there.
 */
export interface StackedIncrementalRefresh {
   plan: (deps: {
      session: IsolatedSessionConnection;
      quotedTablePath: string;
      /** The stacked build's SQL, projected to the stored columns. */
      storedSQL: string;
   }) => Promise<StorageRefreshOutcome | undefined>;
   afterSeed: StorageIncrementalRefresh["afterSeed"];
}

/** See `buildSourceIntoStorage`'s `deps`. */
export interface BuildSessionDeps {
   federate?: typeof federateSourceForPassthrough;
   read?: typeof issuePassthroughRead;
   /** The Postgres column-type read a build stores exact numerics by. */
   numericColumns?: PassthroughDeps["numericColumns"];
   /** The BigQuery jobs API a cancelled build cancels its reads through. */
   bigQueryJobs?: PassthroughDeps["bigQueryJobs"];
}

/**
 * Advance an incremental table in place when its plan can: the result, or
 * undefined for a full build.
 */
async function refreshInPlace(
   incremental: StorageIncrementalRefresh,
   session: IsolatedSessionConnection,
   sourceType: ReturnType<typeof passthroughSourceType>,
   handle: string,
   target: string,
   destinationName: string,
): Promise<StorageBuildResult | undefined> {
   const refreshed = await incremental.plan({
      session,
      sourceType,
      handle,
      quotedTablePath: target,
   });
   if (refreshed) {
      return {
         storageDestinationName: destinationName,
         // A delta cannot change the shape, but the manifest entry
         // declares the schema on every publication; omitting it would
         // leave the source unservable until its next full build.
         schema: await describeTable(session, target),
         rowCount: await countStoredRows(session, target),
         // A delta wrote in place, outside any transaction, and never
         // changes the column set: read again, a failure fails the source
         // after the write (its previous entry understates the table's
         // freshness, never its rows).
         collations: await readCollationsOrFail(session, target),
         readCost: refreshed.readCost,
         refresh: refreshed,
      };
   }
   return undefined;
}

/**
 * Build one persist source into a `storage=` destination via native
 * query-passthrough CTAS, on a build-scoped DuckDB session created and disposed
 * here — so the read-write destination attach and the source credentials exist
 * only for the build and NEVER on the serve connection (least privilege).
 *
 * Attach destination read-write → federate source → `CREATE OR REPLACE TABLE`
 * from the passthrough → DESCRIBE the result → dispose. DuckLake's catalog swap
 * is transactional, so the replace is atomic.
 *
 * @returns the destination name and the captured schema, recorded on the
 *   manifest entry for the serve transform.
 */
export async function buildSourceIntoStorage(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   sourceConnection: ApiConnection;
   /** The source-dialect compiled SELECT (the target's `buildSQL` output), verbatim. */
   buildSQL: string;
   /**
    * Whether the source's compiled definition carries SQL its author wrote
    * ({@link authorSqlCause}). Read only for a same-catalog build, which runs
    * on a read-write session and must refuse author SQL; absent counts as true.
    */
   sourceCarriesAuthorSql?: boolean;
   /**
    * The scope a `managed` source connection's storage secret was built with
    * when it attached (`managedScopeOf`), which a same-catalog build holds
    * the files it reads to. Absent, or resolving undefined, a managed source
    * is refused.
    */
   managedScope?: () => Promise<readonly string[] | undefined>;
   /** Logical, unquoted physical table path (may carry a container path). */
   physicalTableName: string;
   /**
    * The source's columns as Malloy declares them; the CTAS casts the
    * ones with a single faithful DuckDB type to it ({@link castToDeclaredTypes}).
    * Absent or empty stores whatever the warehouse returned.
    */
   declaredColumns?: readonly DeclaredColumn[];
   /** What decides a build cast beyond the declared type (see BuildCastOptions). */
   castOptions?: BuildCastOptions;
   environmentPath: string;
   /**
    * Per-query metadata for the warehouse read, already resolved. Applied here
    * because the read runs inside DuckDB's passthrough, where no Malloy
    * connector is in the call path to attach it.
    */
   queryMetadata?: QueryMetadata;
   /**
    * Offered the chance to advance this table in place before the CTAS runs.
    * Absent for a non-incremental source, which always builds in full.
    */
   incremental?: StorageIncrementalRefresh;
   /** See StorageBuildRequest.replacesView. */
   replacesView?: string[];
   /** The run's cancel signal: interrupts the build's session on abort. */
   signal?: AbortSignal;
   /**
    * Test seam for federation and the passthrough read; production passes
    * nothing. Lets a spec prove the `finally` closes a proxied source's tunnel
    * without a live warehouse.
    */
   deps?: BuildSessionDeps;
}): Promise<StorageBuildResult> {
   const { destinationName, destinationConnection, sourceConnection } = params;
   const federate = params.deps?.federate ?? federateSourceForPassthrough;

   assertSupportedDestination(destinationName, destinationConnection);
   if (isSameCatalog(sourceConnection, destinationConnection)) {
      return buildSameCatalogIntoStorage(params);
   }
   const sourceType = passthroughSourceType(sourceConnection);

   // A proxied source's tunnel, if any: closed once the session is disposed,
   // which does not close a listener opened outside it.
   let federatedClose: (() => Promise<void>) | undefined;
   try {
      return await withDestinationSession(
         params,
         {
            purpose: "build",
            build: true,
            signal: params.signal,
            lockdown: "caller",
         },
         async (session) => {
            const federated = await federate(
               session,
               sourceType,
               sourceFederationConfig(sourceConnection),
            );
            federatedClose = federated.close;
            // A BigQuery read outlives DuckDB's interrupt, so a cancel also
            // cancels the build's jobs in BigQuery, under the read's own key.
            const stopTrackingJobs =
               passthroughBuildOf(sourceType)?.trackJobs?.(
                  session,
                  params.signal,
                  federated.handle,
                  sourceConnection,
                  params.deps ?? {},
               ) ?? (async () => {});
            // Setup done (the destination attached, the source federated):
            // confine the session to them (duckdb_lockdown.ts). The
            // configuration is left unlocked because the build itself sets
            // per-step options (threads around a partitioned write); no caller
            // or author SQL runs in DuckDB here, as the build's SQL runs in the
            // source warehouse through the passthrough.
            await lockDownDestinationSession(
               session,
               destinationAllowlist(params),
            );
            try {
               return await buildOnFederatedSession(
                  params,
                  session,
                  sourceType,
                  federated.handle,
               );
            } finally {
               await stopTrackingJobs();
               await passthroughBuildOf(sourceType)?.release?.(
                  session,
                  federated.handle,
               );
            }
         },
      );
   } finally {
      // The tunnel, once the attach using it is gone. Best-effort.
      if (federatedClose) {
         await federatedClose().catch((e) =>
            logger.warn(
               "Failed to close the SSH proxy a storage build federated through",
               { code: errorCodeOf(e) },
            ),
         );
      }
   }
}

/**
 * {@link buildSourceIntoStorage}'s build on its session, once the source is
 * federated under `handle`: an in-place refresh when the plan can, else the
 * passthrough CTAS.
 */
async function buildOnFederatedSession(
   params: Parameters<typeof buildSourceIntoStorage>[0],
   session: IsolatedSessionConnection,
   sourceType: FederatedSourceType,
   handle: string,
): Promise<StorageBuildResult> {
   const {
      destinationName,
      destinationConnection,
      sourceConnection,
      buildSQL,
      physicalTableName,
      queryMetadata,
   } = params;
   const read = params.deps?.read ?? issuePassthroughRead;
   const build = passthroughBuildOf(sourceType);
   await build?.beforeRead?.(session, handle, queryMetadata);

   // Qualified with the destination catalog: an unqualified name would land
   // in the session's throwaway in-memory catalog and vanish with it.
   // quoteManifestTablePath, NOT quoteTablePath, to match how the serve side
   // renders the path (an already-quoted name passes through). A mismatch
   // surfaces only at run time, past the live fallback, and 400s every query.
   const target = quoteManifestTablePath(
      storageTablePath(
         destinationName,
         physicalTableName,
         destinationSchemaOf(destinationConnection),
      ),
      STORAGE_DIALECT,
   );

   // After the attach and federation, because the plan probes this
   // destination to decide; a seed then continues on the same session.
   const refreshed = params.incremental
      ? await refreshInPlace(
           params.incremental,
           session,
           sourceType,
           handle,
           target,
           destinationName,
        )
      : undefined;
   if (refreshed) return refreshed;

   // A Postgres numeric DuckDB would read as DOUBLE is read as its exact
   // text instead, staged, and stored as the DECIMAL that holds it
   // (passthrough/postgres.ts).
   const recast = capabilitiesOf(sourceType).passthroughNumericAsDouble
      ? await build?.recastColumns?.(
           session,
           handle,
           buildSQL,
           sourceType,
           params.deps ?? {},
        )
      : undefined;
   const passthrough = await read(
      session,
      sourceType,
      handle,
      recast?.readSQL ?? buildSQL,
      queryMetadata,
   );
   const staged = await recast?.stage?.(passthrough.selectSQL, target);

   // The serve transform declares exactly this schema, and the compiler does
   // not type-check a virtual source's declared columns.
   // Cast in DuckDB, around the passthrough: the warehouse runs the build
   // SQL (at most reading a numeric as its text), and only the stored types
   // change.
   let schema: WireColumn[];
   const captured: { collations?: TableCollations } = {};
   try {
      schema = await createTableWithDeclaredTypes(
         session,
         target,
         staged?.selectSQL ?? passthrough.selectSQL,
         { columns: params.declaredColumns ?? [], ...params.castOptions },
         {
            captured,
            sourceType,
            ...(recast?.described ? { described: recast.described } : {}),
            ...(params.replacesView
               ? { replacesView: params.replacesView }
               : {}),
         },
      );
   } finally {
      await staged?.drop();
   }
   // Record where the new snapshot reaches, so the next refresh can be a
   // delta. On this session: the attach does not outlive this function.
   const seededThrough = await params.incremental?.afterSeed({
      session,
      quotedTablePath: target,
   });

   return {
      seededThrough,
      storageDestinationName: destinationName,
      schema,
      rowCount: await countStoredRows(session, target),
      ...(captured.collations ? { collations: captured.collations } : {}),
      // BigQuery's labelled read reports its cost; Snowflake's must be asked
      // after the read, while the session still holds the credentials.
      readCost:
         passthrough.cost ??
         (await build?.readCost?.(
            session,
            handle,
            buildSQL,
            queryMetadata,
            sourceConnection,
         )) ??
         null,
   };
}

/**
 * A source that reads the catalog it is built into (a file loaded into a data
 * store, then persisted there): a plain CTAS on one read-write attach, with no
 * federation, no passthrough and no egress.
 *
 * The session is read-write on the catalog, so it runs only SQL Malloy
 * generated: a source whose compiled definition carries author SQL (an `.sql()`
 * source, a `sql_number()` and its kind) is refused, which keeps the build
 * path's invariant: no user SQL runs on a read-write lake session.
 *
 * The catalog is attached under the SOURCE connection's name, not the
 * destination's: the two may differ (sameness is decided by catalog identity),
 * and the compiled SQL names tables as that connection resolves them, either
 * bare (a managed connection, so the session's search path is its `main`) or
 * qualified with the connection's own name. The target is written under the
 * same alias, so it lands in the one attached catalog either way.
 *
 * Always a full build: an incremental refresh is offered only to a passthrough
 * source, so `incremental` is not consulted here.
 */
async function buildSameCatalogIntoStorage(params: {
   /** The run's cancel signal: interrupts the build's session on abort. */
   signal?: AbortSignal;
   destinationName: string;
   destinationConnection: ApiConnection;
   sourceConnection: ApiConnection;
   buildSQL: string;
   sourceCarriesAuthorSql?: boolean;
   /**
    * The scope a `managed` source connection's storage secret was built with
    * when it attached (`managedScopeOf`), which a same-catalog build holds
    * the files it reads to. Absent, or resolving undefined, a managed source
    * is refused.
    */
   managedScope?: () => Promise<readonly string[] | undefined>;
   physicalTableName: string;
   declaredColumns?: readonly DeclaredColumn[];
   castOptions?: BuildCastOptions;
   environmentPath: string;
   replacesView?: string[];
}): Promise<StorageBuildResult> {
   const { destinationName, destinationConnection, physicalTableName } = params;
   const alias = params.sourceConnection.name || destinationName;
   if (params.sourceCarriesAuthorSql !== false) {
      throw new BadRequestError(
         `Cannot materialize a source that reads storage destination ` +
            `'${destinationName}' and carries SQL its author wrote (an .sql() source ` +
            `or a sql_* expression) or reads a file or URL as a table: a build on the catalog ` +
            `runs only Malloy's own SQL over the catalog's tables. Express it that way. ` +
            `Dropping storage= does not help when the source's own connection is the catalog.`,
      );
   }
   return withDestinationSession(
      params,
      { purpose: "build", attachAs: alias, build: true, signal: params.signal },
      async (session) => {
         // Confined to the catalog before the build's SQL runs (by
         // withDestinationSession), the configuration unlocked for this.
         await session.runSQL(managedSearchPathSql(alias));
         const target = quoteManifestTablePath(
            storageTablePath(
               alias,
               physicalTableName,
               destinationSchemaOf(destinationConnection),
            ),
            STORAGE_DIALECT,
         );
         const captured: { collations?: TableCollations } = {};
         const schema = await createTableWithDeclaredTypes(
            session,
            target,
            params.buildSQL,
            { columns: params.declaredColumns ?? [], ...params.castOptions },
            {
               captured,
               ...(params.replacesView
                  ? { replacesView: params.replacesView }
                  : {}),
               // A managed connection reads only its scope; the destination's
               // credential reads the whole catalog. An unmanaged one reads the
               // whole catalog itself, so its views and files have no scope to
               // leave.
               ...(params.sourceConnection.managed === true
                  ? {
                       beforeCreate: () =>
                          refuseOutOfScopeReads(session, params.buildSQL, {
                             alias,
                             destinationName,
                             sourceConnection: params.sourceConnection,
                             managedScope: params.managedScope,
                          }),
                    }
                  : {}),
            },
         );
         // Reads no warehouse, as a chained build does: nothing was charged.
         return {
            storageDestinationName: destinationName,
            schema,
            rowCount: await countStoredRows(session, target),
            ...(captured.collations ? { collations: captured.collations } : {}),
            readCost: null,
         };
      },
   );
}

/**
 * Each `[schema, table]` (lower case) a reference in DuckDB's parse of
 * `selectSQL` may resolve to in catalog `alias`, whose search path is its
 * `main`: a bare name is `main`'s; `s.t` is schema `s`'s, or `main`'s when
 * `s` is the catalog; `<alias>.s.t` is `s`'s. A name a CTE gives is listed
 * too, which only widens the check. Undefined when DuckDB does not parse it
 * as SELECT statements.
 */
async function namesReadIn(
   selectSQL: string,
   alias: string,
): Promise<[string, string][] | undefined> {
   const parsed = await serializeSQL(selectSQL);
   if (parsed.error || !Array.isArray(parsed.statements)) return undefined;
   const catalog = alias.toLowerCase();
   const names: [string, string][] = [];
   const str = (v: unknown) => (typeof v === "string" ? v.toLowerCase() : "");
   const walk = (node: unknown): void => {
      if (node === null || typeof node !== "object") return;
      if (Array.isArray(node)) {
         node.forEach(walk);
         return;
      }
      const record = node as Record<string, unknown>;
      if (record.type === "BASE_TABLE") {
         const [c, s, t] = [
            str(record.catalog_name),
            str(record.schema_name),
            str(record.table_name),
         ];
         if (c) {
            if (c === catalog) names.push([s || "main", t]);
         } else if (s) {
            names.push([s, t]);
            if (s === catalog) names.push(["main", t]);
         } else {
            names.push(["main", t]);
         }
      }
      Object.values(record).forEach(walk);
   };
   walk(parsed.statements);
   return names;
}

/**
 * Refuse a same-catalog build from a `managed` connection whose SQL reads
 * what that connection could not: a view of the catalog, or a base table
 * holding a file outside the connection's storage scope
 * (`fileOutsideScope`). The build's session reads with the destination's
 * credential, which reaches the whole catalog; a view's body is not held to
 * the managed connection's scope, and a catalog writer can register any file
 * into a table. A served copy holds to the same rule (same_catalog_tables.ts).
 * Run in the build's transaction (`beforeCreate`), so the CREATE reads the
 * snapshot this read. SQL it cannot parse, or a catalog it cannot read, is
 * refused.
 */
async function refuseOutOfScopeReads(
   session: DuckDBConnection,
   selectSQL: string,
   params: {
      alias: string;
      destinationName: string;
      sourceConnection: ApiConnection;
      managedScope?: () => Promise<readonly string[] | undefined>;
   },
): Promise<void> {
   const { alias, destinationName, sourceConnection } = params;
   const names = await namesReadIn(selectSQL, alias);
   const refuse = (why: string) =>
      new BadRequestError(
         `Cannot materialize a source from managed connection '${sourceConnection.name}' ` +
            `into storage destination '${destinationName}' that ${why}: the build ` +
            `reads the catalog with the destination's credential, so it reads only ` +
            `base tables whose files the connection's own storage scope reaches.`,
      );
   if (!names) throw refuse("reads SQL it cannot parse");
   if (names.length === 0) return;
   const run = async (sql: string) =>
      (await runInternalSQL(session, sql)).rows as Record<string, unknown>[];
   let outside: string | undefined;
   try {
      const types = await run(
         "SELECT lower(table_schema) AS s, lower(table_name) AS t, table_type " +
            "FROM information_schema.tables " +
            `WHERE lower(table_catalog) = '${alias.toLowerCase().replace(/'/g, "''")}'`,
      );
      const typeOf = new Map(
         types.map((r) => [`${String(r.s)}.${String(r.t)}`, r.table_type]),
      );
      const read = names.filter(([s, t]) => typeOf.has(`${s}.${t}`));
      if (read.some(([s, t]) => typeOf.get(`${s}.${t}`) !== "BASE TABLE")) {
         throw refuse("reads a view");
      }
      if (read.length === 0) return;
      const [snapshot] = await run(
         `SELECT id FROM ${quoteIdentifier(alias, STORAGE_DIALECT)}.current_snapshot()`,
      );
      const distinct = [...new Map(read.map((r) => [r.join("."), r])).values()];
      const prefixes = await params.managedScope?.();
      if (!prefixes) {
         throw refuse(
            "reads tables while its storage scope is not known (the connection is not attached)",
         );
      }
      outside = await fileOutsideScope(
         run,
         alias,
         String(snapshot?.id),
         distinct,
         prefixes,
      );
   } catch (error) {
      if (error instanceof BadRequestError) throw error;
      throw refuse(
         "reads tables whose files could not be checked against its storage scope",
      );
   }
   if (outside) throw refuse(`reads ${outside}`);
}

/**
 * Compile a chained `storage=` build's model over `connections` (the
 * destination alone) and plan its downstream under `virtualMap`: the SQL to
 * CTAS, projected to the columns it declares, and the downstream it built. The
 * model is `files` by URL, loaded from `entry`: a one-file model of the build's
 * own, or the author's files with the upstreams re-rooted (the host port's
 * `chainBuildText`).
 * The only SHAPE step of a chained build: every failure is a
 * {@link MaterializationEligibilityError}, so the caller may fall back.
 */
/** A chained build's model: its files by URL, and the one it loads. */
interface ChainedBuildModel {
   files: ReadonlyMap<string, string>;
   entry: string;
   /**
    * Ranges of `files` whose definitions may be left out when they do not
    * compile on the destination (see ChainBuildText.rerootedSpans). None: a
    * failure is the build's.
    */
   leaveOutWithin?: readonly { url: string; start: number; end: number }[];
}

/** How many definitions a chained build's compile may leave out, at most. */
const MAX_LEFT_OUT = 32;

/**
 * Leave out of `files` each definition a compile error points at, when every
 * error points inside a range `build` allows; false (nothing changed) when
 * one does not.
 */
function leaveOutFailing(
   files: Map<string, string>,
   err: unknown,
   build: ChainedBuildModel,
): boolean {
   if (!(err instanceof MalloyError) || !build.leaveOutWithin?.length) {
      return false;
   }
   const problems = (
      err.problems as {
         severity?: string;
         at?: {
            url?: string;
            range?: { start: { line: number; character: number } };
         };
      }[]
   ).filter((p) => (p.severity ?? "error") === "error");
   const next = new Map(files);
   for (const p of problems) {
      const url = p.at?.url;
      const start = p.at?.range?.start;
      const text = url === undefined ? undefined : next.get(url);
      if (url === undefined || !start || text === undefined) return false;
      const within = build.leaveOutWithin.filter((w) => w.url === url);
      const left = leaveOutDefinitionAt(text, start, within);
      // A second error in a definition already left out needs nothing more.
      if (left === undefined) {
         const before = files.get(url);
         if (before === text || before === undefined) return false;
         continue;
      }
      next.set(url, left);
   }
   for (const [url, text] of next) files.set(url, text);
   return true;
}

/** A one-file model of the build's own, as a {@link ChainedBuildModel}. */
export function oneFileModel(text: string): ChainedBuildModel {
   const entry = "file:///chained-build/m.malloy";
   return { files: new Map([[entry, text]]), entry };
}

export async function planChainedDownstream(params: {
   model: ChainedBuildModel;
   downstreamName: string;
   virtualMap: ReadonlyMap<string, ReadonlyMap<string, string>>;
   connections: FixedConnectionMap;
   /**
    * The planned source, as a build from its definition would write it. A chained
    * build whose downstream lacks one of its stored columns (a column read
    * through a definition left out of a stored upstream, as a `select: *`
    * does) is a shape it could not carry: stored, the table would answer
    * with fewer columns than live. Absent: not checked.
    */
   plannedSource?: PersistSource;
   /**
    * With `plannedSource`: every column must also be declared as the type the
    * planned source declares it, or the build is a shape it could not carry (a
    * rollup over a stored base, whose aggregates could type otherwise).
    */
   sameDeclaredTypes?: boolean;
   /**
    * The downstream's build must read no given: a table holds one value's
    * rows, which no caller's query may read as its own.
    */
   givenFree?: boolean;
}): Promise<{ sql: string; downstream: PersistSource }> {
   const { model: build, downstreamName } = params;
   const virtualMap = new Map(
      [...params.virtualMap].map(([k, v]) => [k, new Map(v)] as const),
   );
   // The upstream virtual sources declare their captured schema, so compiling
   // reads no tables; the CTAS below runs the real SQL.
   const files = new Map(build.files);
   let runtime!: Runtime;
   // The only SHAPE step here: a compile failure means the downstream cannot
   // be expressed over its rebound parents, so the caller may fall back to
   // recomputing. Infrastructure failures (attach, CTAS, DESCRIBE) would fail
   // the same way on fallback, so only shape failures are marked eligible.
   // A definition of a re-rooted upstream that does not compile here (a join
   // to a source the build cannot read, and what reads it) is left out, as
   // the served copy leaves out a join across connections; one the downstream
   // reads then fails in the downstream's own text, which is never left out.
   let model;
   for (let pass = 0; ; pass++) {
      runtime = inMemoryRuntime(files, params.connections);
      try {
         model = await runtime
            .loadModel(new URL(build.entry), {
               importBaseURL: new URL(".", build.entry),
            })
            .getModel();
         break;
      } catch (err) {
         const left =
            pass < MAX_LEFT_OUT ? leaveOutFailing(files, err, build) : false;
         if (!left) {
            throw new MaterializationEligibilityError({
               message: `Chained build model did not compile over the rebound parents: ${errMessage(err)}`,
            });
         }
      }
   }
   // Planning renders every source's SQL, and the rebound virtual parents
   // render only through the `virtualMap`.
   const targets = await runtime.getBuildTargets(model, { virtualMap });
   const unbuildable = targets.unbuildable.find(
      (u) => u.source.name === downstreamName,
   );
   if (unbuildable) {
      throw new MaterializationEligibilityError({
         message:
            `Chained build model could not plan '${downstreamName}' over ` +
            `the rebound parents: ${unbuildable.problem.message}`,
      });
   }
   const downstream = plannedSources(targets).find(
      (ps) => ps.name === downstreamName,
   );
   if (!downstream) {
      // The downstream references something the rebind model lacks (an
      // uncarried parent refinement, a live leaf). The caller falls back.
      throw new MaterializationEligibilityError({
         message:
            `Chained build model did not yield a persist source named ` +
            `'${downstreamName}' (the downstream references something the ` +
            `rebound parents don't provide).`,
      });
   }
   if (params.plannedSource) {
      let expected: string[] = [];
      let built: string[] = [];
      try {
         expected = storedColumnNames(params.plannedSource);
         built = storedColumnNames(downstream);
      } catch {
         // An unknown surface is refused where the table is projected to it.
      }
      const has = new Set(built);
      const lost = expected.filter((c) => !has.has(c));
      if (lost.length > 0) {
         throw new MaterializationEligibilityError({
            message:
               `Chained build model of '${downstreamName}' over the rebound ` +
               `parents lacks ${lost.length === 1 ? "a column" : "columns"} ` +
               `a recompute writes (${lost.map((c) => `'${c}'`).join(", ")}), ` +
               `read through a definition no stored upstream carries`,
         });
      }
      if (params.sameDeclaredTypes) {
         const typed = (source: PersistSource) =>
            JSON.stringify(
               storageBuildCasts(source).declaredColumns.map((c) => [
                  c.name,
                  c.type,
               ]),
            );
         let same = false;
         try {
            same = typed(params.plannedSource) === typed(downstream);
         } catch {
            // An unreadable surface is not proven the same.
         }
         if (!same) {
            throw new MaterializationEligibilityError({
               message:
                  `Chained build model of '${downstreamName}' over the rebound ` +
                  `parents does not declare its columns as the types a ` +
                  `recompute declares them`,
            });
         }
      }
   }
   if (
      params.givenFree &&
      targets.connections.some((c) =>
         c.targets.some(
            (t) => t.readsGivens && t.sources.some((s) => s === downstream),
         ),
      )
   ) {
      throw new MaterializationEligibilityError({
         message: `Chained build model of '${downstreamName}' reads a given`,
      });
   }
   // The CTAS runs on a read-write session on the destination, as a
   // same-catalog build does, so the same rule holds: only SQL Malloy
   // generated. The rebound parents are virtual, so this reads the
   // downstream's own definition. Refused as ineligible.
   const cause = authorSqlCause(sourceDefOf(downstream));
   if (cause !== undefined) {
      throw new MaterializationEligibilityError({
         message:
            `Chained source '${downstreamName}' carries SQL its author wrote ` +
            `(${cause}), which does not run on the destination's read-write ` +
            `session.`,
      });
   }
   // Projected to the columns the source declares, hidden ones included, as
   // a build from the definition writes them; a column `except:` drops is left out.
   const sql = projectToStoredColumns(
      downstream,
      buildSQLOf(downstream, { entries: {} }),
   );
   return { sql, downstream };
}

/**
 * Build a CHAINED `storage=` source from its upstreams' already-materialized
 * tables in the destination, instead of recomputing them from the warehouse.
 * The downstream is then a pure function of the parents' STORED rows.
 *
 * `model` reads every upstream as a virtual source on this destination (the
 * author's files with the upstreams re-rooted, or an extension's chain over
 * its parent's table) and declares the downstream as a persist source. It is compiled against the build
 * session, and the downstream's `buildSQL` (planned under the `virtualMap`) is
 * CTAS'd into the destination; nothing is federated and nothing leaves DuckDB.
 * Session lifecycle mirrors {@link buildSourceIntoStorage}.
 *
 * @returns the destination name and the captured schema.
 */
export async function buildDownstreamIntoStorage(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   /** The model the downstream compiles from (see planChainedDownstream). */
   model: ChainedBuildModel;
   /** The connection the model reads the destination by; the destination's name when absent. */
   connectionName?: string;
   /** The downstream source's Malloy name, to locate it in the transient plan. */
   downstreamName: string;
   /** See planChainedDownstream's `plannedSource`. */
   plannedSource?: PersistSource;
   /** See planChainedDownstream's `sameDeclaredTypes`. */
   sameDeclaredTypes?: boolean;
   /** See planChainedDownstream's `givenFree`. */
   givenFree?: boolean;
   /** connection → handle → quoted physical path for the upstream virtuals. */
   virtualMap: ReadonlyMap<string, ReadonlyMap<string, string>>;
   /** Logical, unquoted physical table path for the downstream's own table. */
   physicalTableName: string;
   environmentPath: string;
   /** See StorageBuildRequest.replacesView. */
   replacesView?: string[];
   /**
    * Other destinations the build reads (an extension's parent in another
    * destination), each attached read-only under its own name.
    */
   readOnly?: readonly {
      destinationName: string;
      destinationConnection: ApiConnection;
   }[];
   /**
    * An incremental table's in-place refresh: a delta of the stacked SQL in
    * the destination, or undefined to have it rebuilt.
    */
   incremental?: StackedIncrementalRefresh;
}): Promise<StorageBuildResult> {
   const {
      destinationName,
      destinationConnection,
      model,
      downstreamName,
      virtualMap,
      environmentPath,
   } = params;

   const connectionName = params.connectionName ?? destinationName;
   const compile = (session: DuckDBConnection) =>
      planChainedDownstream({
         model,
         downstreamName,
         virtualMap,
         ...(params.plannedSource
            ? { plannedSource: params.plannedSource }
            : {}),
         ...(params.sameDeclaredTypes ? { sameDeclaredTypes: true } : {}),
         ...(params.givenFree ? { givenFree: true } : {}),
         connections: new FixedConnectionMap(
            new Map([[connectionName, session]]),
            connectionName,
         ),
      });
   // A build that reads another destination compiles first, in a session of
   // its own destination alone (confined to it), so
   // nothing the compile reads ever runs with the other destination
   // attached; the session that attaches it runs only the checked SELECT.
   const precompiled = params.readOnly?.length
      ? await withDestinationSession(
           { destinationName, destinationConnection, environmentPath },
           { purpose: "build_compile" },
           compile,
        )
      : undefined;
   if (precompiled) await assertSingleSelect(precompiled.sql);

   return withDestinationSession(
      { destinationName, destinationConnection, environmentPath },
      {
         purpose: "build",
         build: true,
         ...(params.readOnly ? { alsoRead: params.readOnly } : {}),
      },
      async (session) => {
         const { sql, downstream } = precompiled ?? (await compile(session));
         const incremental = params.incremental;
         if (!incremental) {
            return writeStackedTable(session, params, sql, downstream);
         }
         const target = quoteManifestTablePath(
            storageTablePath(
               destinationName,
               params.physicalTableName,
               destinationSchemaOf(destinationConnection),
            ),
            STORAGE_DIALECT,
         );
         const refreshed = await incremental.plan({
            session,
            quotedTablePath: target,
            storedSQL: sql,
         });
         if (refreshed) {
            return {
               storageDestinationName: destinationName,
               schema: await describeTable(session, target),
               rowCount: await countStoredRows(session, target),
               collations: await readCollationsOrFail(session, target),
               readCost: null,
               upstreamReuse: "reused",
               refresh: refreshed,
            };
         }
         const written = await writeStackedTable(
            session,
            params,
            sql,
            downstream,
         );
         const seededThrough = await incremental.afterSeed({
            session,
            quotedTablePath: target,
         });
         return { ...written, seededThrough };
      },
   );
}

/**
 * Build a `storage=` source whose definition runs the same query as a stored
 * upstream's (one compiler key: an `extend` of it that adds no query of its
 * own, folded onto a table of its own by a `name=` or a projection of its
 * own) as a projection of that upstream's table: the same rows a build from the definition
 * writes, the columns the source stores, cast to its declared types. The
 * caller proves the two builds are one; nothing is compiled here.
 */
export async function buildProjectionIntoStorage(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   /** The source being built: its declared types. */
   downstream: PersistSource;
   /** The columns it stores (storedColumnNames), each one the upstream's table holds. */
   columns: readonly string[];
   /** The upstream's stored table, quoted, as the build session reads it. */
   upstreamTable: string;
   /** Logical, unquoted physical table path for the downstream's own table. */
   physicalTableName: string;
   environmentPath: string;
   /** See StorageBuildRequest.replacesView. */
   replacesView?: string[];
}): Promise<StorageBuildResult> {
   const { destinationName, destinationConnection, environmentPath } = params;
   return withDestinationSession(
      { destinationName, destinationConnection, environmentPath },
      { purpose: "build", build: true },
      async (session) =>
         writeStackedTable(
            session,
            params,
            `SELECT ${params.columns
               .map((c) => quoteIdentifier(c, STORAGE_DIALECT))
               .join(", ")} FROM ${params.upstreamTable}`,
            params.downstream,
         ),
   );
}

/** CTAS a stacked build's SQL into the destination, typed as a build from the definition types it. */
async function writeStackedTable(
   session: IsolatedSessionConnection,
   params: {
      destinationName: string;
      destinationConnection: ApiConnection;
      physicalTableName: string;
      replacesView?: string[];
   },
   sql: string,
   downstream: PersistSource,
): Promise<StorageBuildResult> {
   const { destinationName, destinationConnection, physicalTableName } = params;
   // Quoted as in buildSourceIntoStorage, to match the serve side.
   const target = quoteManifestTablePath(
      storageTablePath(
         destinationName,
         physicalTableName,
         destinationSchemaOf(destinationConnection),
      ),
      STORAGE_DIALECT,
   );
   // The downstream's own declared types, as for a build from the definition.
   const casts = storageBuildCasts(downstream);
   const captured: { collations?: TableCollations } = {};
   const schema = await createTableWithDeclaredTypes(
      session,
      target,
      sql,
      { columns: casts.declaredColumns, ...casts.castOptions },
      {
         captured,
         ...(params.replacesView ? { replacesView: params.replacesView } : {}),
      },
   );
   return {
      storageDestinationName: destinationName,
      schema,
      rowCount: await countStoredRows(session, target),
      ...(captured.collations ? { collations: captured.collations } : {}),
      // Reads no source warehouse: here, uniquely, null means "spent nothing".
      readCost: null,
      upstreamReuse: "reused",
   };
}

/**
 * How a catalog of `connection`'s type is attached and set up for a build
 * (its type's registry entry: DuckLake's), or undefined for a type that is
 * no catalog, which a plain DuckDB file destination is not.
 */
function catalogDestinationOf(
   connection: ApiConnection | undefined,
): DestinationDialect | undefined {
   return connectionDialectOf(connection?.type)?.destination;
}

/**
 * Whether `connection` is a catalog a build writes into and a serve reads
 * from (DuckLake's), rather than a plain DuckDB file destination or no
 * destination at all.
 */
export function isCatalogDestination(
   connection: ApiConnection | undefined,
): boolean {
   return catalogDestinationOf(connection) !== undefined;
}

/** A DuckLake destination's catalog config, or a clean 400 when it has none. */
export function requireDuckLakeConfig(
   destinationName: string,
   destinationConnection: ApiConnection,
): components["schemas"]["DucklakeConnection"] {
   const cfg = destinationConnection.ducklakeConnection;
   if (!cfg) {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is type 'ducklake' but ` +
            `has no ducklakeConnection config.`,
      );
   }
   return cfg;
}

/**
 * {@link attachDestinationReadWrite}, read-only: a DuckLake catalog through
 * its read-only attach, a plain DuckDB destination's file `READ_ONLY`. A
 * destination nothing has written yet attaches nothing (its tables then
 * read as not found).
 */
async function attachDestinationReadOnly(
   session: DuckDBConnection,
   /**
    * What it is attached as, and the configured destination whose file or
    * catalog it is: the file's path is derived from the destination's own
    * name (storageDestinationFile), never from the alias.
    */
   names: { alias: string; destinationName: string },
   destinationConnection: ApiConnection,
   environmentPath: string,
): Promise<void> {
   const { alias, destinationName } = names;
   if (isCatalogDestination(destinationConnection)) {
      const cfg = requireDuckLakeConfig(destinationName, destinationConnection);
      await attachDuckLakeReadOnlyIfPresent(session, alias, cfg);
      return;
   }
   const dbPath = storageDestinationFile(environmentPath, destinationName);
   if (!existsSync(dbPath)) return;
   await session.runSQL(
      `ATTACH '${escapeSQL(dbPath)}' AS ${quoteIdentifier(alias, STORAGE_DIALECT)} (READ_ONLY)`,
   );
}

/** Attach the destination connection read-write on the build session. */
async function attachDestinationReadWrite(
   session: DuckDBConnection,
   /**
    * What it is attached as, and the configured destination whose file or
    * catalog it is: the file's path is derived from the destination's own
    * name (storageDestinationFile), never from the alias.
    */
   names: { alias: string; destinationName: string },
   destinationConnection: ApiConnection,
   environmentPath: string,
): Promise<void> {
   const { alias, destinationName } = names;
   if (isCatalogDestination(destinationConnection)) {
      const cfg = requireDuckLakeConfig(destinationName, destinationConnection);
      await attachDuckLakeReadWrite(session, alias, cfg);
      return;
   }
   // Plain DuckDB: the file lives under the destinations root (as connection
   // assembly derives it) so it cannot collide with a same-named connection.
   // Created here because a build may be the first thing to touch it.
   mkdirSync(storageDestinationRoot(environmentPath), { recursive: true });
   const dbPath = storageDestinationFile(environmentPath, destinationName);
   await session.runSQL(
      `ATTACH '${escapeSQL(dbPath)}' AS ${quoteIdentifier(alias, STORAGE_DIALECT)}`,
   );
}

/**
 * What a build session on a destination reads and writes, beyond what its
 * attach recorded (duckdb_lockdown.ts): the plain-DuckDB destinations' root,
 * and the DuckLake destination's configured data path.
 */
export function destinationAllowlist(params: {
   destinationConnection: ApiConnection;
   environmentPath: string;
}): string[] {
   const dataPath =
      params.destinationConnection.ducklakeConnection?.storage?.bucketUrl;
   return [
      storageDestinationRoot(params.environmentPath),
      ...(dataPath ? [dataPath] : []),
   ];
}

/** Throw a clean 422 unless the destination is a supported storage type. */
export function assertSupportedDestination(
   destinationName: string,
   destinationConnection: ApiConnection,
): void {
   const type = destinationConnection.type ?? "";
   if (!(STORAGE_DESTINATION_TYPES as readonly string[]).includes(type)) {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is type '${type}', but ` +
            `'storage=' destinations must be one of ` +
            `${STORAGE_DESTINATION_TYPES.join(", ")}.`,
      );
   }
}

/**
 * Throw a clean 400 when the destination's `destinationSchema` is not a schema
 * in the attached catalog. No build creates a schema (the catalog's owner does),
 * so without this a typo fails every build into the destination with DuckDB's
 * bare "schema does not exist". `main` always exists and is not checked.
 */
async function assertDestinationSchemaExists(
   session: DuckDBConnection,
   attachName: string,
   destinationName: string,
   destinationConnection: ApiConnection,
): Promise<void> {
   const schema = destinationSchemaOf(destinationConnection);
   if (schema === DEFAULT_DESTINATION_SCHEMA) return;
   const result = await session.runSQL(
      `SELECT 1 FROM duckdb_schemas() WHERE database_name = '${escapeSQL(attachName)}' ` +
         // DuckDB resolves identifiers case-insensitively, as the CTAS would.
         `AND lower(schema_name) = lower('${escapeSQL(schema)}')`,
   );
   const rows = Array.isArray(result) ? result : (result.rows ?? []);
   if (rows.length === 0) {
      throw new BadRequestError(
         `Storage destination '${destinationName}' declares destinationSchema ` +
            `'${schema}', but its catalog has no schema by that name. Create it ` +
            `in the catalog, or correct destinationSchema.`,
      );
   }
}

/**
 * The subset of a source connection's config the passthrough federation needs,
 * plus a sanitized `name` base for the secret/alias it creates on the session.
 */
function sourceFederationConfig(sourceConnection: ApiConnection): {
   name: string;
   bigqueryConnection?: components["schemas"]["BigqueryConnection"];
   snowflakeConnection?: components["schemas"]["SnowflakeConnection"];
   postgresConnection?: components["schemas"]["PostgresConnection"];
   proxy?: components["schemas"]["ConnectionProxy"];
} {
   return {
      name: sourceConnection.name ?? "src",
      bigqueryConnection: sourceConnection.bigqueryConnection,
      snowflakeConnection: sourceConnection.snowflakeConnection,
      postgresConnection: sourceConnection.postgresConnection,
      // federatePostgres opens the tunnel; buildSourceIntoStorage closes it.
      proxy: sourceConnection.proxy,
   };
}

/**
 * {@link createTableAndDescribe} over `selectSQL` with its columns cast to their
 * declared types (stored_types_ducklake.spec.ts). A value a cast cannot
 * convert (a boolean the warehouse returned as 'Y'/'N') retries the CTAS
 * without the casts, storing what the warehouse returned, rather than failing
 * the build; the serve side then decides that column. Never TRY_CAST,
 * whose NULLs would be wrong answers. The retry re-reads the source, so it is
 * paid only by a source whose data the cast rejects.
 */
export async function createTableWithDeclaredTypes(
   session: IsolatedSessionConnection,
   quotedTablePath: string,
   selectSQL: string,
   declared: { columns: readonly DeclaredColumn[] } & BuildCastOptions,
   options: {
      /**
       * The passthrough engine the SELECT reads from; absent on a chained
       * build. Decides which declared columns are cast ({@link columnsToCast}).
       */
      sourceType?: FederatedSourceType;
      /**
       * The source's columns as the session reads them, when the caller has
       * already described the passthrough: {@link columnsToCast} reads them
       * rather than describing it again.
       */
      described?: readonly { name: string; type: string }[];
   } & Parameters<typeof createTableAndDescribe>[3] = {},
): Promise<WireColumn[]> {
   const columns = await columnsToCast(
      session,
      selectSQL,
      declared.columns,
      options.sourceType,
      options.described,
   );
   options = {
      ...options,
      declaredNames: declared.columns.map((c) => c.name),
   };
   const exactIntegers = await computedWideIntegers(
      session,
      selectSQL,
      options.sourceType,
      options.described,
   );
   // Each class of cast is tried alone after the whole set fails, so one
   // value past 38 digits does not leave every other column uncast: the
   // declared-type casts and the exact-integer casts, then the computed
   // types as they are.
   const attempts = [
      castToDeclaredTypes(selectSQL, columns, { ...declared, exactIntegers }),
      castToDeclaredTypes(selectSQL, columns, { ...declared }),
      castToDeclaredTypes(selectSQL, [], { exactIntegers }),
      selectSQL,
   ].filter((sql, i, all) => all.indexOf(sql) === i);
   for (const [i, sql] of attempts.entries()) {
      try {
         return await createTableAndDescribe(
            session,
            quotedTablePath,
            sql,
            options,
         );
      } catch (err) {
         if (!isCastConversionError(err) || i === attempts.length - 1) {
            throw err;
         }
         // Once per build, as before: one cast failed.
         if (i === 0) recordStoredColumnTypeMismatch("build", "cast_failed");
         logger.warn(
            "A declared-type cast could not convert a stored value; storing that class of column as the warehouse computed it",
            {
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               table: quotedTablePath,
               // Not the message: it quotes the value the cast refused.
               errorCode: errorCodeOf(err),
            },
         );
      }
   }
   throw new Error("unreachable: the last attempt returns or throws");
}

/**
 * The output columns a DuckDB-computed build (same-catalog or chained: no
 * passthrough `sourceType`) produces as a 128-bit integer, which DuckLake
 * would store as a DOUBLE: stored as `DECIMAL(38,0)` instead
 * ({@link wideIntegerColumns}). Read by describing the SELECT in the
 * destination's session, which prepares it and runs nothing: the SELECT
 * reads only the destination's own tables. A passthrough's columns arrive
 * in the warehouse's types (a BigQuery INT64 as BIGINT, a Snowflake or
 * Postgres number as DECIMAL), so it is not described, though a description
 * the caller already holds is read. A describe that fails casts nothing; the
 * build itself reports the error.
 */
async function computedWideIntegers(
   session: DuckDBConnection,
   selectSQL: string,
   sourceType: FederatedSourceType | undefined,
   described: readonly { name: string; type: string }[] | undefined,
): Promise<string[]> {
   if (described) return wideIntegerColumns(described);
   if (sourceType !== undefined) return [];
   try {
      return wideIntegerColumns(await describeTable(session, `(${selectSQL})`));
   } catch (err) {
      logger.warn(
         "Could not describe a storage build's computed columns; storing its integers as computed",
         { errorCode: errorCodeOf(err) },
      );
      return [];
   }
}

/**
 * The declared columns a build casts: all of them, less the ones whose
 * warehouse type the build keeps ({@link keepsWarehouseType}: a Postgres
 * `bytea` or `interval`, which the dialect declares `string`).
 *
 * The warehouse's types are read by describing the SELECT in the destination's
 * session, for a source whose passthrough build keeps them (Postgres's) only:
 * there the describe prepares the statement and runs nothing, and only
 * Postgres's dialect declares such a column `string`. No other passthrough is described: whether it runs (and
 * bills) the query is not known. A describe that fails changes nothing: every
 * column is cast, and the build itself reports the error.
 */
async function columnsToCast(
   session: DuckDBConnection,
   selectSQL: string,
   declared: readonly DeclaredColumn[],
   sourceType: FederatedSourceType | undefined,
   alreadyDescribed?: readonly { name: string; type: string }[],
): Promise<readonly DeclaredColumn[]> {
   if (
      sourceType === undefined ||
      passthroughBuildOf(sourceType)?.keepsWarehouseTypes !== true
   ) {
      return declared;
   }
   if (!declared.some((d) => d.type === "string")) return declared;
   let described: readonly WireColumn[];
   try {
      described =
         alreadyDescribed ?? (await describeTable(session, `(${selectSQL})`));
   } catch (err) {
      logger.warn(
         "Could not describe a storage build's source columns; casting every declared column",
         { errorCode: errorCodeOf(err) },
      );
      return declared;
   }
   const kept = new Set(
      described.flatMap((c) => {
         const d = declared.find(
            (x) => x.name.toLowerCase() === c.name?.toLowerCase(),
         );
         return d && c.type && keepsWarehouseType(c.type, d.type)
            ? [d.name]
            : [];
      }),
   );
   return kept.size === 0
      ? declared
      : declared.filter((d) => !kept.has(d.name));
}

/**
 * Build the table and read back its schema, as a CTAS created and read back in
 * ONE transaction: the physical name is stable across generations
 * (`selfAssignTableName`), so a rebuild replaces the table being SERVED, and a
 * failure anywhere, the read-back included, rolls back to the previous
 * generation (or, on a first build, to no table, which no reclaim would
 * otherwise find: the caller records nothing until this returns).
 */
export async function createTableAndDescribe(
   session: IsolatedSessionConnection,
   quotedTablePath: string,
   selectSQL: string,
   options: {
      /**
       * Statements that drop this package's own leftover `name=` view at the
       * name, run first in the same transaction, so a failure leaves the view
       * (leftover_name_views.ts).
       */
      replacesView?: string[];
      /**
       * Run first in the same transaction, after `replacesView`: what it
       * reads of the catalog is the snapshot the CREATE reads. It throws to
       * refuse the build, which rolls back.
       */
      beforeCreate?: () => Promise<void>;
      /**
       * The column names the build source declares: a stored column that
       * matches one only by a unique case fold is renamed to it, in the same
       * transaction ({@link canonicalRenames}).
       */
      declaredNames?: readonly string[];
      /**
       * Filled with the table's column collations, read in the same
       * transaction: one that does not read rolls the build back
       * ({@link readCollationsOrFail}), so the previous generation keeps
       * serving.
       */
      captured?: { collations?: TableCollations };
   } = {},
): Promise<WireColumn[]> {
   // In the transaction, not create-then-drop-on-failure: a DROP after the
   // CREATE OR REPLACE committed would delete the name the previous manifest
   // serves from.
   return await inTransaction(session, quotedTablePath, async () => {
      for (const sql of options.replacesView ?? []) {
         await session.runSQL(sql);
      }
      await options.beforeCreate?.();
      await session.runSQL(
         `CREATE OR REPLACE TABLE ${quotedTablePath} AS (${selectSQL})`,
      );
      let described = await describeTable(session, quotedTablePath);
      const renames = canonicalRenames(described, options.declaredNames ?? []);
      for (const { from, to } of renames) {
         await session.runSQL(
            `ALTER TABLE ${quotedTablePath} RENAME COLUMN ${quoteIdentifier(from, STORAGE_DIALECT)} TO ${quoteIdentifier(to, STORAGE_DIALECT)}`,
         );
      }
      if (renames.length > 0) {
         described = await describeTable(session, quotedTablePath);
      }
      if (options.captured) {
         options.captured.collations = await readCollationsOrFail(
            session,
            quotedTablePath,
         );
      }
      return described;
   });
}

const TIME_TRANSFORMS = new Set(["year", "month", "day", "hour"]);
const BUCKET_TRANSFORM = /^bucket\((\d+)\)$/;

/**
 * A partition key as `ALTER TABLE … SET PARTITIONED BY` takes it, from the
 * transform DuckLake records for it. A transform outside the ones DuckLake
 * defines is refused rather than written as some other layout.
 */
export function partitionKeySql(key: PartitionKey): string {
   const column = quoteIdentifier(key.column, STORAGE_DIALECT);
   if (key.transform === "identity") return column;
   if (TIME_TRANSFORMS.has(key.transform)) return `${key.transform}(${column})`;
   const bucket = BUCKET_TRANSFORM.exec(key.transform);
   if (bucket) return `bucket(${Number(bucket[1])}, ${column})`;
   throw new BadRequestError(
      `Partition transform '${key.transform}' on column '${key.column}' is not one ` +
         "DuckLake defines (identity, year, month, day, hour, bucket(N)), so the " +
         "table cannot be written with that layout.",
   );
}

/**
 * The ORDER BY terms that bring a partitioned insert's rows in partition order:
 * each key as DuckLake partitions on it (`year("day")`, not `"day"`, so a year's
 * categories are not interleaved across its days), then each transformed key's
 * own column, so a time partition's rows also arrive in time order. Each term
 * once, in key order.
 *
 * A `bucket(N)` key cannot be ordered by: DuckDB has no scalar `bucket`, only
 * the partition transform. It orders by its column, which does not group a
 * bucket's rows, so on a bucketed key the appender can meet every bucket at
 * once and the memory bound the order exists for does not hold for that key.
 */
export function partitionOrderSql(keys: readonly PartitionKey[]): string[] {
   const terms: string[] = [];
   const add = (term: string) => {
      if (!terms.includes(term)) terms.push(term);
   };
   for (const key of keys) {
      if (!BUCKET_TRANSFORM.test(key.transform)) add(partitionKeySql(key));
   }
   for (const key of keys) {
      if (key.transform !== "identity") {
         add(quoteIdentifier(key.column, STORAGE_DIALECT));
      }
   }
   return terms;
}

/**
 * Rows the appender buffers before a partitioned copy flushes them to the
 * partition files (DuckDB's `partitioned_write_flush_threshold`, whose own
 * default is 524,288). Measured at a `768MB` memory limit on 122 columns, 616k
 * rows, 308 partitions: ordered rows on one thread complete at this value and
 * fail at DuckDB's default.
 */
const PARTITIONED_WRITE_FLUSH_THRESHOLD = 8192;

/**
 * Create `quotedTablePath` empty, lay it out by `keys`, and fill it from
 * `selectSQL`, all in ONE transaction. Used only by the admin API's copy of a
 * partitioned table (`copyStorageTable`), which keeps the source table's
 * layout; a build writes every table unpartitioned.
 *
 * Create-empty / lay out / insert because DuckLake applies a layout only to files
 * written after it is set. Run bare, `WITH NO DATA` would hold zero rows until
 * the INSERT lands, and a failed INSERT would lose the previous table; a
 * ROLLBACK keeps it.
 *
 * The layout only means something because the session disabled inlining
 * (`ducklake_default_data_inlining_row_limit=0`); inlined rows have no files.
 *
 * Memory is bounded three ways, which only work together: the insert reads its
 * rows in partition order ({@link orderByExpressions}), on one thread, at a
 * low flush threshold ({@link PARTITIONED_WRITE_FLUSH_THRESHOLD}).
 */
export async function writePartitionedTable(
   session: IsolatedSessionConnection,
   params: {
      quotedTablePath: string;
      selectSQL: string;
      keys: readonly PartitionKey[];
      createStatement: "CREATE TABLE" | "CREATE OR REPLACE TABLE";
   },
): Promise<void> {
   const { quotedTablePath, selectSQL, keys } = params;
   const layout = keys.map(partitionKeySql).join(", ");
   const order = partitionOrderSql(keys);
   // The session is this copy's own instance, so the setting reaches nothing
   // else.
   await session.runSQL(
      `SET partitioned_write_flush_threshold = ${PARTITIONED_WRITE_FLUSH_THRESHOLD}`,
   );
   // One appender: a sorted result is read in parallel, so with several
   // threads each meets every partition again and the insert runs out of
   // memory. `SET threads` is NOT transactional, so the RESET in the finally is
   // what restores it after a rollback.
   await session.runSQL("SET threads = 1");
   try {
      await inTransaction(session, quotedTablePath, async () => {
         await session.runSQL(
            `${params.createStatement} ${quotedTablePath} AS (${selectSQL}) WITH NO DATA`,
         );
         await session.runSQL(
            `ALTER TABLE ${quotedTablePath} SET PARTITIONED BY (${layout})`,
         );
         await session.runSQL(
            `INSERT INTO ${quotedTablePath} (${orderByExpressions(selectSQL, order)})`,
         );
      });
   } finally {
      // Best-effort: the session is disposed with the copy, so a stuck setting
      // dies with it.
      try {
         await session.runSQL("RESET threads");
      } catch {
         // see above
      }
   }
}

/**
 * Run a build's statements as one transaction: COMMIT when `body` returns,
 * ROLLBACK when it throws, which restores the previous generation of the
 * table. A failed rollback is logged as `rollbackFailure`, never raised over
 * the error that caused it.
 */
export async function inTransaction<T>(
   session: IsolatedSessionConnection,
   quotedTablePath: string,
   body: () => Promise<T>,
   rollbackFailure = "Failed to roll back a storage build; the served generation may have been replaced",
): Promise<T> {
   await session.runSQL("BEGIN TRANSACTION");
   let result: T;
   try {
      result = await body();
      await session.runSQL("COMMIT");
   } catch (buildErr) {
      try {
         await session.runSQL("ROLLBACK");
      } catch (rollbackErr) {
         // A failed COMMIT has already ended the transaction.
         if (NO_TRANSACTION.test(errMessage(rollbackErr))) {
            logger.debug("No transaction was open to roll back", {
               table: quotedTablePath,
            });
         } else {
            logger.warn(rollbackFailure, {
               table: quotedTablePath,
               code: errorCodeOf(rollbackErr),
               // The build's error can quote its SQL or a value; it is
               // rethrown to the run's record.
               causeCode: errorCodeOf(buildErr),
            });
         }
      }
      throw buildErr;
   }
   return result;
}

/**
 * The column collations of the table at `target` (quoted), as the bind's
 * read finds them (`collationCatalogBatches`, from its definition), in the
 * session that wrote it: so a stored table's contract records what a bind
 * will read. Throws {@link ContractCaptureFailedError} when they do not read.
 */
async function readCollationsOrFail(
   session: DuckDBConnection,
   target: string,
): Promise<TableCollations> {
   const faked = fakedCollations();
   if (faked) return faked;
   const parts = tablePathParts(target, STORAGE_DIALECT);
   const [batch] = parts
      ? collationCatalogBatches([{ key: "t", parts }], STORAGE_DIALECT)
      : [];
   let read: TableCollations | undefined;
   if (batch) {
      try {
         const result = await session.runSQL(batch.sql);
         const rows = Array.isArray(result) ? result : (result.rows ?? []);
         read = batch.collationsOf(rows as Record<string, unknown>[]).get("t");
      } catch {
         read = undefined;
      }
   }
   if (!read) {
      throw new ContractCaptureFailedError(
         target,
         "its column collations did not read",
      );
   }
   return read;
}

/**
 * The rows a built table holds, or null when the count could not be read
 * (logged at debug): the manifest entry records it, and a build never fails
 * on it.
 */
async function countStoredRows(
   session: DuckDBConnection,
   quotedTablePath: string,
): Promise<number | null> {
   try {
      const result = await session.runSQL(
         `SELECT count(*) AS n FROM ${quotedTablePath}`,
      );
      const n = Number(resultRows(result)[0]?.n);
      return Number.isFinite(n) ? n : null;
   } catch (error) {
      logger.debug("Could not count a built table's rows", {
         table: quotedTablePath,
         code: errorCodeOf(error),
      });
      return null;
   }
}

async function describeTable(
   session: DuckDBConnection,
   quotedTablePath: string,
): Promise<WireColumn[]> {
   const result = await session.runSQL(`DESCRIBE ${quotedTablePath}`);
   const rows = Array.isArray(result) ? result : (result.rows ?? []);
   const columns: WireColumn[] = [];
   for (const row of rows as Record<string, unknown>[]) {
      const name = row.column_name;
      const type = row.column_type;
      if (typeof name === "string" && typeof type === "string") {
         columns.push({ name, type });
      }
   }
   return columns;
}
