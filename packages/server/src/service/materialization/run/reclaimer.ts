// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Giving back what materializations built ({@link Reclaimer}): the tables of a
 * deleted run, of a failed run whose manifest names no table, of an archived or
 * deleted version, the tables a readable name replaced, and at startup a
 * version reclaim a crash cut short. Each table is dropped through its own
 * tier's adapter, with any `name=` view an earlier release left beside it.
 * Reached as the materialization service's `reclaimer`, by the service and
 * main_server.ts.
 *
 * Effects: drops in warehouses and destinations, writes the run store, logs
 * and counts each drop. Best-effort, and never drops a table another committed
 * run of the package still references, or one another package has since
 * marked as its own.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import {
   databaseAccessFailure,
   InvalidArgumentError,
   MaterializationConflictError,
   MaterializationNotFoundError,
   MaterializedTableDropRefusedError,
   TableNotFoundError,
} from "../../../errors";
import { logger } from "../../../logger";
import { UnquotableIdentifierError } from "../../quoting";
import {
   type DropOccasion,
   recordDropTables,
} from "../../../materialization_metrics";
import {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import { parseEnvironmentName, parsePackageName } from "../../identity";
import type { ApiConnection } from "../../model";
import type { QueryMetadata } from "../../query_metadata";
import { resolveEnvironmentId } from "../../resolve_environment";
import {
   isVersionOwnedRun,
   newestServingEntries,
   readsRun,
   type ServingReader,
   tableNamesIn,
   WRITES_TABLES_KEY,
   writeTargetsIn,
} from "../../versions/materialization_scope";
import type { MaterializationEnvironment, MaterializationHost } from "../host";
import {
   isLegacyFailedEntry,
   type KeptTable,
   type KeptTableReason,
   ManifestEntry,
} from "../domain/types";
import { DatabaseTier } from "./database_build_strategy";
import { StorageBuildStrategy } from "./storage_build_strategy";
import {
   backupSuffix,
   BuildEnvironment,
   stagingSuffix,
   statementRunOptions,
   isActiveRun,
   redactConnectionSecrets,
} from "./materialization_run_support";
import {
   dropLeftoverNameViews,
   heldByOwnNameView,
   nameViewsIn,
   type RecordedNameView,
} from "./leftover_name_views";
import { packageOwner } from "./table_mark_sql";
import {
   ownedElsewhere,
   markedFor,
   tableExistsAt,
   type TableHolder,
   withOwnedNameLock,
} from "./table_ownership";
import { ownedTableProbeSql } from "./warehouse_sql";
import { dependedOn } from "./database_build_strategy";
import {
   atTableSite,
   databaseTableSite,
   type TableSiteContext,
} from "./table_site";
import { capabilitiesOf } from "../domain/dialect";
import type { RunLifecycle } from "./run_lifecycle";
import {
   entryTable,
   leafKey,
   mayBeSameTable,
   tableKey,
} from "../domain/physical_table";
import { previousDestinationOf, type RenamedTable } from "./run_instructions";
import { hasReservedTableSuffix } from "../domain/table_name";

/**
 * The one occasion a reclaim drops on: a run's delete is record-only, so
 * `delete` is left to a caller dropping a storage destination's table.
 */
type ReclaimOccasion = Extract<DropOccasion, "reclaim">;

/** The tiers a drop reaches a table through. */
const DATABASE = new DatabaseTier();
// Only its drop. A build goes through the service's adapter, which routes a
// chained build through the run's collaborators; this one would not.
const STORAGE: Pick<StorageBuildStrategy, "drop"> = new StorageBuildStrategy();

/**
 * Where a run records the tables it built under new names or in new places
 * in place of tables earlier runs built for the same sources (see
 * Reclaimer.reclaimRenamedTables), until each is dropped or kept.
 */
export const RENAMED_TABLES_KEY = "renamedTables";

/**
 * Where a run lists the tables it replaced and keeps ({@link KeptTable}):
 * free-form run metadata, carried forward by each later run of the serving
 * line until the table is dropped or its name is reused.
 */
export const KEPT_TABLES_KEY = "keptTables";

/** The {@link KeptTable}s a run's metadata lists, or empty. */
function keptTablesIn(metadata: Record<string, unknown> | null): KeptTable[] {
   const tables = metadata?.[KEPT_TABLES_KEY];
   if (!Array.isArray(tables)) return [];
   return tables.filter(
      (t): t is KeptTable =>
         typeof t === "object" &&
         t !== null &&
         typeof (t as KeptTable).sourceEntityId === "string" &&
         typeof (t as KeptTable).physicalTableName === "string" &&
         typeof (t as KeptTable).connectionName === "string" &&
         ((t as KeptTable).reason === "author_name" ||
            (t as KeptTable).reason === "destination_missing" ||
            (t as KeptTable).reason === "may_be_served"),
   );
}

/**
 * The newest committed run `reader` reads that lists kept tables (an empty
 * list included), among `runs`, committed no later than `until` and other
 * than `except`, if given.
 */
function newestListing(
   runs: Materialization[],
   reader: ServingReader,
   options: { until?: Materialization; except?: string } = {},
): Materialization | undefined {
   let newest: Materialization | undefined;
   for (const m of runs) {
      if (m.id === options.except || m.status !== "MANIFEST_FILE_READY") {
         continue;
      }
      if (!readsRun(m, reader) || !Array.isArray(m.metadata?.[KEPT_TABLES_KEY]))
         continue;
      if (options.until && committedAt(m) > committedAt(options.until)) {
         continue;
      }
      if (!newest || committedAt(m) > committedAt(newest)) newest = m;
   }
   return newest;
}

/**
 * How a loaded package finds, in the store, the tables its serving line's
 * newest auto-run lists as kept after a rename, for its warnings: read again
 * whenever the package (re)loads, as its served tables are.
 */
export function keptTablesResolverFor(
   repository: Pick<ResourceRepository, "listMaterializations">,
   environmentId: string,
): (packageName: string, reader: ServingReader) => Promise<KeptTable[]> {
   return async (packageName, reader) => {
      const runs = await repository.listMaterializations(
         environmentId,
         packageName,
         reader.owned !== undefined ? { version: reader.owned } : undefined,
      );
      return keptTablesIn(newestListing(runs ?? [], reader)?.metadata ?? null);
   };
}

/** One kept table, wherever it is listed. */
function keptKey(t: KeptTable): string {
   return tableKey(entryTable(t));
}

/** The {@link RenamedTable}s a run's metadata records, or empty. */
function renamedTablesIn(
   metadata: Record<string, unknown> | null,
): RenamedTable[] {
   const tables = metadata?.[RENAMED_TABLES_KEY];
   if (!Array.isArray(tables)) return [];
   return tables.filter(
      (t): t is RenamedTable =>
         typeof t === "object" &&
         t !== null &&
         typeof (t as RenamedTable).sourceEntityId === "string" &&
         typeof (t as RenamedTable).previousName === "string" &&
         typeof (t as RenamedTable).physicalTableName === "string" &&
         typeof (t as RenamedTable).connectionName === "string",
   );
}

/**
 * The guards of Reclaimer.reclaimRenamedTables a later run may find passing
 * (its replacement commits, it becomes what is served, the other run naming
 * the table goes): a table kept by one stays recorded, to be tried again.
 */
const TRANSIENT_GUARDS: ReadonlySet<string> = new Set([
   "replacement-not-committed",
   "not-serving-replacement",
   "still-referenced",
]);

/** How many kept tables' existence is probed at once. */
const KEPT_PROBES_AT_ONCE = 4;

/** One old table, wherever it is recorded: where it is, and its name. */
function renamedKey(
   t: Pick<
      RenamedTable,
      | "previousName"
      | "connectionName"
      | "storageDestinationName"
      | "previousPlace"
   >,
): string {
   return tableKey(
      entryTable({
         connectionName: t.connectionName,
         storageDestinationName: previousDestinationOf(t),
         physicalTableName: t.previousName,
      }),
   );
}

/** When a committed run committed; the serving line is ordered by it. */
function committedAt(m: Materialization): number {
   return (m.completedAt ?? m.createdAt).getTime();
}

/**
 * The tables an archived version's run owes back: its manifest's entries, and
 * every table it recorded writing (WRITES_TARGETS_KEY) that the
 * manifest does not name. For a run that failed or was cancelled before it
 * committed, the manifest names none, so the recorded tables are all of them;
 * without them the reclaim would delete the run's record and leave its tables
 * with nothing that names them.
 */
function reclaimableEntries(m: Materialization): ManifestEntry[] {
   const entries = Object.values(m.manifest?.entries ?? {});
   const named = new Set(entries.map((e) => tableKey(entryTable(e))));
   for (const target of writeTargetsIn(m.metadata ?? null)) {
      const key = tableKey(entryTable(target));
      if (named.has(key)) continue;
      named.add(key);
      entries.push({
         sourceEntityId: target.sourceEntityId,
         physicalTableName: target.physicalTableName,
         connectionName: target.connectionName,
         ...(target.storageDestinationName
            ? { storageDestinationName: target.storageDestinationName }
            : {}),
      });
   }
   return entries;
}

/**
 * The destination's config for a best-effort table drop, or undefined when it
 * is no longer configured, so a cleanup sweep skips that table instead of
 * aborting and leaving every later table behind. The build path does NOT use
 * this: there an unresolvable destination must fail the run.
 */
function destinationForCleanup(
   environment: BuildEnvironment,
   destinationName: string,
): ApiConnection | undefined {
   try {
      return environment.getStorageDestination(destinationName);
   } catch (error) {
      logger.warn(
         "Skipping a table drop: its storage destination is no longer configured",
         { destinationName, error: errMessage(error) },
      );
      return undefined;
   }
}

/**
 * The `RunSQLOptions` for the drops that retire a materialization's tables.
 * No model layer: the source's declaration described how to BUILD it, and the
 * source may no longer exist by the time its table is retired.
 */
function dropRunSQLOptions(
   environment: BuildEnvironment,
   connectionName: string,
   environmentName: string,
   packageName: string,
   materializationId: string,
): { queryMetadata?: QueryMetadata } {
   return statementRunOptions(environment, connectionName, {
      queryClass: "ops",
      environment: environmentName,
      package: packageName,
      runId: materializationId,
   });
}

/**
 * How one drop of a run's entries reaches their tables: each connection
 * resolved once, the sites a leftover `name=` view is reached at, the
 * package's ownership mark, and what a failed table drop does.
 */
function dropReach(
   environment: MaterializationEnvironment,
   environmentName: string,
   packageName: string,
   materializationId: string,
   occasion: ReclaimOccasion,
   connectionFor: (name: string) => Promise<MalloyConnection | undefined>,
) {
   const connectionCache = new Map<string, MalloyConnection>();
   const cachedConnection = async (name: string) => {
      const cached = connectionCache.get(name);
      if (cached) return cached;
      const resolved = await connectionFor(name);
      if (resolved) connectionCache.set(name, resolved);
      return resolved;
   };
   const viewContext = {
      connectionFor: cachedConnection,
      destinationFor: (name: string) =>
         destinationForCleanup(environment, name),
      // Read only when a view's site is reached: a drop that never
      // reaches one never asks.
      get environmentPath() {
         return environment.getEnvironmentPath();
      },
      runOptions: (name: string) =>
         dropRunSQLOptions(
            environment,
            name,
            environmentName,
            packageName,
            materializationId,
         ),
   };
   // A table's DROP that fails because this package's own `name=` view
   // holds its name: an earlier release replaced the table with the view,
   // so the table is gone and its entry is retired, not owed. Probed only
   // after a failed drop.
   const owner = packageOwner(environmentName, packageName);
   const replacedByOwnView = (entry: ManifestEntry) =>
      heldByOwnNameView(
         {
            connectionName: entry.connectionName ?? "",
            ...(entry.storageDestinationName
               ? { storageDestinationName: entry.storageDestinationName }
               : {}),
         },
         entry.physicalTableName,
         owner,
         viewContext,
      );
   // A failed drop of the table itself: retired when that view holds the
   // name, else the failure stands.
   const retireIfReplacedByView =
      (entry: ManifestEntry) =>
      async (err: unknown): Promise<void> => {
         if (!(await replacedByOwnView(entry))) throw err;
         logRetiredByView(materializationId, entry.physicalTableName);
      };
   return {
      cachedConnection,
      viewContext,
      owner,
      retireIfReplacedByView,
      occasion,
   };
}

/**
 * Drop one `storage=` table in its destination, through a READ-WRITE attach
 * (the serve attach is read-only). Best-effort: false when the destination is
 * no longer configured or the drop failed, which is logged.
 */
async function dropStorageTable(
   environment: BuildEnvironment,
   destinationName: string,
   table: Parameters<typeof STORAGE.drop>[1],
   occasion: ReclaimOccasion,
   materializationId?: string,
): Promise<boolean> {
   const destinationConnection = destinationForCleanup(
      environment,
      destinationName,
   );
   if (!destinationConnection) return false;
   const context = {
      ...(materializationId ? { runId: materializationId } : {}),
      physicalTableName: table.physicalTableName,
      destinationName,
   };
   try {
      await STORAGE.drop(
         {
            destinationName,
            destinationConnection,
            environmentPath: environment.getEnvironmentPath(),
         },
         table,
      );
      recordDropTables("success", "storage", occasion);
      logger.info("Dropped a materialized storage table", context);
      return true;
   } catch (err) {
      recordDropTables("failure", "storage", occasion);
      logger.warn("Failed to drop a materialized storage table", {
         ...context,
         // The drop attaches read-write, so a failure can echo the catalog
         // connstring.
         error: redactConnectionSecrets(errMessage(err), destinationConnection),
      });
      return false;
   }
}

/**
 * The `name=` views an earlier release recorded making, on `runs` and on every
 * other run `listed` (a run that carried a table forward did not record its
 * view again), for the drop to clean up (leftover_name_views.ts); undefined
 * when none did.
 */
function recordedNameViews(
   listed: Materialization[],
   runs: Materialization[],
): RecordedNameView[] | undefined {
   const ids = new Set(runs.map((m) => m.id));
   const recorded = [...runs, ...listed.filter((m) => !ids.has(m.id))].flatMap(
      (m) => nameViewsIn(m.metadata),
   );
   return recorded.length > 0 ? recorded : undefined;
}

/**
 * The key {@link tablesStillReferenced} holds a table segment under, so a
 * reclaim keeps any table a committed run names anywhere under that segment.
 */
function leafReferenceKey(physicalTableName: string): string {
   return `\u0000leaf:${leafKey(physicalTableName)}`;
}

/**
 * The destination-and-name keys of every table a MANIFEST_FILE_READY run
 * references, among the `listed` runs `include` admits, and the key of each
 * table segment they name ({@link leafReferenceKey}): a table's name does
 * not say where it is, so one in another place or container may be the
 * table a run references (two names for one DuckLake, a DuckDB catalog
 * name), and a reclaim keeps it.
 */
function tablesStillReferenced(
   listed: Materialization[],
   include: (other: Materialization) => boolean,
): Set<string> {
   const stillReferenced = new Set<string>();
   for (const other of listed) {
      if (!include(other)) continue;
      if (other.status !== "MANIFEST_FILE_READY") continue;
      for (const e of Object.values(other.manifest?.entries ?? {})) {
         if (e.physicalTableName) {
            stillReferenced.add(tableKey(entryTable(e)));
            stillReferenced.add(leafReferenceKey(e.physicalTableName));
         }
      }
   }
   return stillReferenced;
}

/**
 * Whether a run's entry is left in place without a drop: another committed
 * run still references its table, or another package has marked the table as
 * its own since this run built it (it holds that package's `name=` now), so
 * it is no longer this package's to drop: retired, not owed
 * (table_ownership.ts). Logged either way.
 */
async function keptInPlace(
   entry: ManifestEntry,
   connectionName: string,
   physicalTableName: string,
   stillReferenced: Set<string>,
   { viewContext, owner }: ReturnType<typeof dropReach>,
   materializationId: string,
): Promise<boolean> {
   const destForKey = entry.storageDestinationName ?? connectionName;
   if (
      stillReferenced.has(
         tableKey(entryTable({ ...entry, connectionName, physicalTableName })),
      )
   ) {
      logger.info(
         "Skipping drop: table still referenced by another materialization",
         {
            runId: materializationId,
            physicalTableName,
            destinationName: destForKey,
         },
      );
      return true;
   }
   if (stillReferenced.has(leafReferenceKey(physicalTableName))) {
      logger.info("Keeping a table a reclaim would drop: it may be served", {
         runId: materializationId,
         physicalTableName,
         destinationName: destForKey,
         guard: "may_be_served",
      });
      return true;
   }
   const markedBy = await atTableSite(
      {
         connectionName,
         ...(entry.storageDestinationName
            ? { storageDestinationName: entry.storageDestinationName }
            : {}),
      },
      viewContext,
      (site) => ownedElsewhere(site, physicalTableName, owner),
   ).catch(() => undefined);
   if (markedBy !== undefined) {
      logger.warn(
         "Skipping drop: another package has built and marked this table since",
         {
            runId: materializationId,
            physicalTableName,
            destinationName: destForKey,
            markedBy,
         },
      );
      return true;
   }
   return false;
}

/**
 * Drop one database table, with its staging and backup names, on its own
 * connection. Best-effort: `owed` when the drop failed, which is logged;
 * `skipped` when the table lived in an unloaded version's in-memory duckdb;
 * `unquotable` when its name cannot be written as SQL in the connection's
 * dialect, so no drop can name it and none is owed (a retry fails alike).
 */
async function dropDatabaseTable(
   environment: MaterializationEnvironment,
   environmentName: string,
   packageName: string,
   materializationId: string,
   entry: ManifestEntry,
   connectionName: string,
   {
      cachedConnection,
      retireIfReplacedByView,
      occasion,
   }: ReturnType<typeof dropReach>,
): Promise<"dropped" | "owed" | "skipped" | "unquotable"> {
   const physicalTableName = entry.physicalTableName;
   try {
      const connection = await cachedConnection(connectionName);
      if (!connection) {
         logger.info(
            "Skipping drop: the table lived in an unloaded version's in-memory duckdb",
            { runId: materializationId, physicalTableName },
         );
         return "skipped";
      }
      await DATABASE.drop(
         {
            connection,
            // Tagged for attribution like a build, but as `ops`.
            runOptions: dropRunSQLOptions(
               environment,
               connectionName,
               environmentName,
               packageName,
               materializationId,
            ),
         },
         {
            physicalTableName,
            // Its staging and backup names, which a crash part-way through a
            // build can leave.
            leftoversOf: entry.sourceEntityId,
            onDropFailed: retireIfReplacedByView(entry),
         },
      );
      recordDropTables("success", "database", occasion);
      logger.info("Dropped materialized table on delete", {
         runId: materializationId,
         physicalTableName,
         connectionName,
      });
      return "dropped";
   } catch (err) {
      if (err instanceof UnquotableIdentifierError) {
         logger.warn(
            "Not dropping a materialized table whose name cannot be written as SQL",
            {
               runId: materializationId,
               physicalTableName,
               connectionName,
               error: err.message,
            },
         );
         return "unquotable";
      }
      recordDropTables("failure", "database", occasion);
      logger.warn("Failed to drop materialized table on delete", {
         runId: materializationId,
         physicalTableName,
         connectionName,
         error: errMessage(err),
      });
      return "owed";
   }
}

/** The {@link renamedKey} of the table a reclaimable entry names. */
function entryRenamedKey(entry: ManifestEntry): string {
   return renamedKey({
      previousName: entry.physicalTableName,
      connectionName: entry.connectionName ?? "",
      storageDestinationName: entry.storageDestinationName,
   });
}

/** Whether a storage destination is configured, so a drop can reach it. */
function destinationConfigured(
   environment: MaterializationEnvironment,
   destinationName: string,
): boolean {
   try {
      environment.getStorageDestination(destinationName);
      return true;
   } catch {
      return false;
   }
}

/** The {@link KeptTable} a reclaimable entry is, kept for `reason`. */
function keptOf(entry: ManifestEntry, reason: KeptTableReason): KeptTable {
   return {
      sourceEntityId: entry.sourceEntityId,
      physicalTableName: entry.physicalTableName,
      connectionName: entry.connectionName ?? "",
      ...(entry.storageDestinationName
         ? { storageDestinationName: entry.storageDestinationName }
         : {}),
      reason,
   };
}

/** The reclaim's "kept" log line, for a table a guard after the decision kept. */
function logKept(entry: ManifestEntry, guard: string): void {
   logger.info("Keeping a table a readable name replaced", {
      physicalTableName: entry.physicalTableName,
      guard,
   });
}

/** Where a table is: its connection, and its storage destination if any. */
type Place = {
   connectionName?: string | null;
   storageDestinationName?: string | null;
};

/** A place as one key: two places are one only when their keys match. */
function placeKey(p: Place): string {
   return `${p.connectionName ?? ""}\u0000${p.storageDestinationName ?? ""}`;
}

/**
 * The newest committed run `reader` serves from (see newestServingEntries),
 * or undefined.
 */
function newestServingRun(
   runs: Materialization[],
   reader: ServingReader,
): Materialization | undefined {
   let newest: Materialization | undefined;
   for (const m of runs) {
      if (!readsRun(m, reader)) continue;
      if (m.status !== "MANIFEST_FILE_READY" || !m.manifest?.entries) continue;
      if (!newest || committedAt(m) > committedAt(newest)) newest = m;
   }
   return newest;
}

/**
 * The tables `run` lists as kept: those the serving line's newest run
 * before it that lists any (an empty list included) listed, and `fresh`,
 * less any no committed run of the package names in its place any more
 * (dropped with its runs) and any `run` builds again, under its name in
 * its place. A kept table whose table segment `run` serves in another place,
 * or under another container, may be that served table (a table's name does
 * not say where it is, and two places can reach one table), so it is listed
 * as `may_be_served` whatever it was kept for, and never advised dropped. `write` says whether the list
 * goes on the run: when it lists any, or when the run before it listed any,
 * so a list that emptied is written empty once and stops being carried.
 */
function keptAfter(
   run: Materialization,
   runs: Materialization[],
   reader: ServingReader,
   fresh: KeptTable[],
): { tables: KeptTable[]; write: boolean } {
   const before = newestListing(runs, reader, { until: run, except: run.id });
   const carried = [
      ...keptTablesIn(before?.metadata ?? null),
      ...keptTablesIn(run.metadata ?? null),
   ];
   const at = (t: KeptTable, e: ManifestEntry): boolean =>
      !!e.physicalTableName &&
      mayBeSameTable(e.physicalTableName, t.physicalTableName) &&
      placeKey(e) === placeKey(t);
   const named = (t: KeptTable) =>
      runs.some(
         (m) =>
            m.status === "MANIFEST_FILE_READY" &&
            Object.values(m.manifest?.entries ?? {}).some((e) => at(t, e)),
      );
   const entries = Object.values(run.manifest?.entries ?? {});
   const reused = (t: KeptTable) => entries.some((e) => at(t, e));
   const servedElsewhere = (t: KeptTable) =>
      entries.some(
         (e) =>
            !!e.physicalTableName &&
            leafKey(e.physicalTableName) === leafKey(t.physicalTableName) &&
            !at(t, e),
      );
   const byKey = new Map<string, KeptTable>();
   for (const t of [...carried, ...fresh]) byKey.set(keptKey(t), t);
   const tables = [...byKey.values()]
      .filter((t) => named(t) && !reused(t))
      .map((t) =>
         t.reason !== "may_be_served" && servedElsewhere(t)
            ? { ...t, reason: "may_be_served" as const }
            : t,
      );
   return {
      tables,
      write:
         tables.length > 0 ||
         keptTablesIn(before?.metadata ?? null).length > 0 ||
         Array.isArray(run.metadata?.[KEPT_TABLES_KEY]),
   };
}

/** The connection's config, for redacting its secrets; undefined for the sandbox. */
function apiConnectionOf(
   environment: MaterializationEnvironment,
   connectionName: string,
): unknown {
   try {
      return environment.getApiConnection(connectionName);
   } catch {
      return undefined;
   }
}

/** The SQLSTATE an error, or one it wraps, carries; undefined for none. */
function sqlStateOf(err: unknown): string | undefined {
   for (let e: unknown = err, depth = 0; e && depth < 8; depth++) {
      const code = (e as { code?: unknown }).code;
      if (typeof code === "string" && /^[0-9A-Z]{5}$/.test(code)) return code;
      e = (e as { cause?: unknown }).cause;
   }
   return undefined;
}

/**
 * A caller's drop the warehouse refused for a reason the caller can act on,
 * answered with the reason it fits; undefined for any other failure (a 5xx).
 * Objects that depend on the table (Postgres 2BP01, DuckDB's "depend on it"):
 * `TABLE_IN_USE`. A connection not allowed to drop it (SQLSTATE 42501), or
 * an object that is not a table (42809): `TABLE_OWNED_ELSEWHERE`, since the
 * connection Publisher builds with does not hold it as a table of its own.
 */
function dropRefusalOf(
   err: unknown,
   where: string,
): MaterializedTableDropRefusedError | undefined {
   if (dependedOn(err)) {
      return new MaterializedTableDropRefusedError(
         "TABLE_IN_USE",
         `Table ${where} is not dropped (TABLE_IN_USE): other objects in the ` +
            `warehouse (a view, a foreign key) depend on it. Drop or repoint ` +
            `them first.`,
      );
   }
   const state = sqlStateOf(err);
   // By SQLSTATE alone: a message that says "permission denied" may be a
   // file the database could not write (DuckDB, DuckLake), a server error.
   if (state === "42501") {
      return new MaterializedTableDropRefusedError(
         "TABLE_OWNED_ELSEWHERE",
         `Table ${where} is not dropped (TABLE_OWNED_ELSEWHERE): the ` +
            `connection may not drop it, so it is not a table this ` +
            `package's builds own.`,
      );
   }
   if (state === "42809") {
      return new MaterializedTableDropRefusedError(
         "TABLE_OWNED_ELSEWHERE",
         `Table ${where} is not dropped (TABLE_OWNED_ELSEWHERE): the name ` +
            `holds something other than a table, which no build of this ` +
            `package makes.`,
      );
   }
   return undefined;
}

/**
 * Every reader that may serve from a package's runs: its unversioned slot,
 * and each version the runs name, read as a `scope: package` version and as
 * a `scope: version` one (which it is is the package's setting, not the
 * run's). Over-wide on purpose: a reader that does not exist only keeps a
 * table.
 */
function servingReadersOf(runs: Materialization[]): ServingReader[] {
   const readers: ServingReader[] = [{}];
   const versions = new Set(
      runs.map((m) => m.version).filter((v): v is string => v !== null),
   );
   for (const v of versions) {
      readers.push({ versionId: v }, { versionId: v, owned: v });
   }
   return readers;
}

/** A drop of a run's manifest entries; see {@link Reclaimer.dropManifestEntries}. */
type DropManifestEntries = Reclaimer["dropManifestEntries"];

/**
 * Giving back what materializations built: the tables of a deleted run, of a
 * failed run whose manifest names no table, and of an archived or deleted
 * version, and finishing at startup a version reclaim a crash cut short.
 * Every drop is best-effort and never drops a table another committed run of
 * the package still references.
 */
export class Reclaimer {
   constructor(
      private readonly deps: {
         repository: () => ResourceRepository;
         host: MaterializationHost;
         lifecycle: RunLifecycle;
         /**
          * How a sweep drops one run's entries: the service passes its run
          * collaborator, so a spec can hand it a failing drop.
          */
         dropManifestEntries: DropManifestEntries;
         /**
          * Run `fn` under the lock a run's start takes for its active-run
          * check (the package's lock for an unversioned package; the
          * version's, then the package's table lock, for a version), so no
          * run starts while the reclaim decides and drops. Absent: no lock
          * (a spec's).
          */
         withRunLock?: <T>(
            environment: MaterializationEnvironment,
            environmentId: string,
            packageName: string,
            versionId: string | undefined,
            fn: () => Promise<T>,
         ) => Promise<T>;
         /**
          * Run `fn` under the package's table lock (TableClaims), which a
          * version's run start takes for its active-run check. A caller's
          * drop holds it under the package's lock, so no run of any version
          * starts while it decides and drops. Absent: no lock (a spec's).
          */
         withTableLock?: <T>(
            environmentId: string,
            packageName: string,
            fn: () => Promise<T>,
         ) => Promise<T>;
      },
   ) {}

   private get repository(): ResourceRepository {
      return this.deps.repository();
   }

   private resolveEnvironmentId(environmentName: string): Promise<string> {
      return resolveEnvironmentId(this.repository, environmentName);
   }

   /**
    * Drop the storage tables a FAILED run created: the run's manifest records
    * no entry (at most each source's failure), and GC reclaims only the tables
    * a manifest's entries record. Each guard closes a way to
    * destroy live data:
    *
    * - ORCHESTRATED runs only (no `owner` for auto-run). Host-assigned
    *   generational names are unique by construction. The still-referenced check
    *   reads THIS environment only, and a BuildID has no environment input, so
    *   with auto-run's stable names two environments sharing a destination could
    *   resolve to the SAME table and a reclaim could drop one another serves.
    *   Do not widen this until colliding persist targets are refused at
    *   validation time.
    * - Only entries this run CREATED (never a carried-forward one).
    * - Only names no other MANIFEST_FILE_READY run references, the same
    *   destination-and-name check a version's reclaim applies.
    *
    * Database failures are left alone: stable names in the customer's own
    * warehouse are overwritten by the next build, and a DROP there is a far
    * larger blast radius for no reclaim.
    */
   async reclaimStorageTablesFromFailedRun(
      builtThisRun: ManifestEntry[],
      environment: BuildEnvironment,
      owner: { environmentId: string; packageName: string },
   ): Promise<void> {
      if (builtThisRun.length === 0) return;
      try {
         const stillReferenced = tablesStillReferenced(
            (await this.repository.listMaterializations(
               owner.environmentId,
               owner.packageName,
            )) ?? [],
            () => true,
         );
         for (const entry of builtThisRun) {
            const dest = entry.storageDestinationName;
            const table = entry.physicalTableName;
            if (!dest || !table) continue;
            if (
               stillReferenced.has(tableKey(entryTable(entry))) ||
               stillReferenced.has(leafReferenceKey(table))
            ) {
               logger.info(
                  "Keeping a table from a failed run: a live manifest still serves it",
                  { destinationName: dest, physicalTableName: table },
               );
               continue;
            }
            await dropStorageTable(
               environment,
               dest,
               { physicalTableName: table },
               "reclaim",
            );
         }
      } catch (err) {
         // Never let cleanup mask the build failure that triggered it.
         logger.warn("Failed-run table reclaim did not complete", {
            error: errMessage(err),
         });
      }
   }

   /**
    * How to reach the connection a run's table was built on. Through the
    * package the build used: its unversioned slot, or its version when that is
    * loaded. A version that is not loaded is never loaded for a drop: its
    * package-local `duckdb` is an in-memory database that went with its
    * unload (undefined: nothing to drop there), and every other connection
    * is the environment's.
    */
   private async connectionResolverFor(
      environment: MaterializationEnvironment,
      packageName: string,
      version: string | null,
   ): Promise<(name: string) => Promise<MalloyConnection | undefined>> {
      const pkg =
         version !== null
            ? environment.peekVersion(packageName, version)
            : await environment.getPackage(
                 parsePackageName(packageName),
                 false,
              );
      if (pkg) return (name) => pkg.getMalloyConnection(name);
      return async (name) =>
         name === "duckdb" ? undefined : environment.getMalloyConnection(name);
   }

   /**
    * A caller's drop of one database-tier table a materialization of the
    * package built on `connection` (one the package reaches; resolved by the
    * caller, outside every lock, since resolving may load the package). It
    * refuses rather than skips, and says why: unlike a reclaim, nothing in
    * Publisher's records asked for this drop.
    *
    * Decided and dropped under the package's lock and its table lock (so no
    * run of any version starts) and then the name's owned-name lock (so no
    * `name=` build claims it), in the run lock order (run/README.md), from a
    * fresh read of the package's runs:
    *
    * 1. No run of the package is active: `MaterializationConflictError`.
    *    A build's staging or backup copy of a table a run records is
    *    refused (`InvalidArgumentError`): it is dropped with its table.
    * 2. Not in use (`TABLE_IN_USE`): no committed run of the package, of any
    *    version, other than `materializationId`'s names a table with this
    *    table segment in any place (a name does not say where it is); the
    *    run `materializationId` names is not the newest any version or the
    *    package reads; and no loaded copy of any package in the environment
    *    is bound to a table with this identity.
    * 3. The package's (probed at the table's site): a table marked as
    *    another package's, or a view Publisher did not make or another
    *    package's, is `TABLE_OWNED_ELSEWHERE`. A table with no mark is
    *    dropped only when the terminal run `materializationId` names records
    *    exactly this table on this connection; otherwise, or when the probe
    *    fails or the dialect cannot be asked, `TABLE_OWNERSHIP_UNVERIFIED`.
    * 4. No table at the name: nothing to drop. "absent" when a terminal run
    *    of the package records the table on this connection (a retried
    *    drop), else `TableNotFoundError`: nothing proves it was ever the
    *    package's.
    *
    * Then the table is dropped through the database tier, quoted as the
    * build quoted it, with the staging and backup copies a crash can leave
    * when a record names its source; counted (occasion `caller`) and
    * logged; and its incremental boundary forgotten.
    */
   async dropConnectionTable(request: {
      environment: MaterializationEnvironment;
      environmentName: string;
      environmentId: string;
      packageName: string;
      connectionName: string;
      connection: MalloyConnection;
      physicalTableName: string;
      materializationId?: string;
   }): Promise<"dropped" | "absent"> {
      const { environment, environmentId, packageName, physicalTableName } =
         request;
      const withRunLock =
         this.deps.withRunLock ??
         (<T>(
            _e: MaterializationEnvironment,
            _i: string,
            _p: string,
            _v: string | undefined,
            fn: () => Promise<T>,
         ) => fn());
      const withTableLock =
         this.deps.withTableLock ??
         (<T>(_i: string, _p: string, fn: () => Promise<T>) => fn());
      return withRunLock(
         environment,
         environmentId,
         packageName,
         undefined,
         () =>
            withTableLock(environmentId, packageName, () =>
               withOwnedNameLock(physicalTableName, async () => {
                  const runs =
                     (await this.repository.listMaterializations(
                        environmentId,
                        packageName,
                     )) ?? [];
                  const evidence = this.assertDroppable(request, runs);
                  // A loaded copy's binding: read after the records, under the
                  // lock no run's start or commit passes.
                  await this.assertNotBound(request);
                  return this.probeAndDrop(request, evidence);
               }),
            ),
      );
   }

   /**
    * Guards 1 and 2 of {@link dropConnectionTable} on the package's runs:
    * throws when a run is active or a committed run names the table.
    * Returns what the records say about the table: the run
    * `materializationId` names, if it records it here, and the entry of any
    * terminal run that does.
    */
   private assertDroppable(
      request: Parameters<Reclaimer["dropConnectionTable"]>[0],
      runs: Materialization[],
   ): { namedRecordsIt: boolean; recorded?: ManifestEntry } {
      const {
         packageName,
         connectionName,
         physicalTableName,
         materializationId,
      } = request;
      const active = runs.find((m) => isActiveRun(m.status));
      if (active) {
         throw new MaterializationConflictError(
            `Run ${active.id} of package ${packageName} is ${active.status}; ` +
               `drop the table once it settles.`,
         );
      }
      const named =
         materializationId === undefined
            ? undefined
            : runs.find((m) => m.id === materializationId);
      if (materializationId !== undefined && !named) {
         throw new MaterializationNotFoundError(
            `Materialization ${materializationId} not found for package ${packageName}`,
         );
      }
      const inUse = (why: string) =>
         new MaterializedTableDropRefusedError(
            "TABLE_IN_USE",
            `Table '${physicalTableName}' on connection '${connectionName}' ` +
               `is not dropped (TABLE_IN_USE): ${why}. Delete or replace ` +
               `that run first.`,
         );
      const namesIt = (e: ManifestEntry) =>
         !!e.physicalTableName &&
         leafKey(e.physicalTableName) === leafKey(physicalTableName);
      const committedNaming = runs.find(
         (m) =>
            m.id !== materializationId &&
            m.status === "MANIFEST_FILE_READY" &&
            Object.values(m.manifest?.entries ?? {}).some(namesIt),
      );
      if (committedNaming) {
         throw inUse(
            `committed run ${committedNaming.id} of the package names a ` +
               `table '${physicalTableName}'`,
         );
      }
      if (
         named?.status === "MANIFEST_FILE_READY" &&
         Object.values(named.manifest?.entries ?? {}).some(namesIt) &&
         servingReadersOf(runs).some(
            (reader) => newestServingRun(runs, reader)?.id === named.id,
         )
      ) {
         throw inUse(
            `run ${named.id} is the newest run the package or one of its ` +
               `versions serves from`,
         );
      }
      // A build's staging or backup copy goes with its table (its leftovers),
      // never alone: a backup carries its table's mark, and the next build
      // restores it.
      // By table segment, as the in-use check compares: a copy reached
      // under another container or case is still the copy.
      const copyOf = runs
         .flatMap((m) => reclaimableEntries(m))
         .find(
            (e) =>
               e.physicalTableName &&
               [stagingSuffix, backupSuffix].some(
                  (suffix) =>
                     leafKey(physicalTableName) ===
                     leafKey(
                        `${e.physicalTableName}${suffix(e.sourceEntityId)}`,
                     ),
               ),
         );
      if (copyOf) {
         throw new InvalidArgumentError(
            `Table '${physicalTableName}' is the staging or backup copy of ` +
               `'${copyOf.physicalTableName}', and is dropped with it: drop ` +
               `'${copyOf.physicalTableName}' instead.`,
         );
      }
      const recordsHere = (m: Materialization) =>
         reclaimableEntries(m).find(
            (e) =>
               !e.storageDestinationName &&
               e.connectionName === connectionName &&
               e.physicalTableName === physicalTableName,
         );
      const namedEntry = named ? recordsHere(named) : undefined;
      let recorded = namedEntry;
      for (const m of runs) {
         if (recorded) break;
         if (!isActiveRun(m.status)) recorded = recordsHere(m);
      }
      return {
         namedRecordsIt: namedEntry !== undefined,
         ...(recorded ? { recorded } : {}),
      };
   }

   /**
    * Guard 2's bindings: no loaded copy of any package in the environment
    * (its unversioned slot or a loaded version) is bound to a table with
    * this identity. By identity alone, whatever the connection: an
    * over-wide match only refuses.
    */
   private async assertNotBound(
      request: Parameters<Reclaimer["dropConnectionTable"]>[0],
   ): Promise<void> {
      const { environment, environmentId, connectionName, physicalTableName } =
         request;
      const identity = leafKey(physicalTableName);
      const copies: { label: string; names: string[] }[] = [];
      for (const pkg of environment.getLoadedPackages()) {
         copies.push({
            label: `package '${pkg.getPackageName()}'`,
            names: pkg.boundTableNames?.() ?? [],
         });
      }
      const versions =
         environment.getVersionService() === null
            ? []
            : await this.repository.listVersionsByEnvironment(environmentId);
      for (const row of versions) {
         const pkg = environment.peekVersion(row.packageName, row.versionId);
         if (!pkg) continue;
         copies.push({
            label: `version ${row.versionId} of package '${row.packageName}'`,
            names: pkg.boundTableNames?.() ?? [],
         });
      }
      const bound = copies.find((c) =>
         c.names.some((name) => leafKey(name) === identity),
      );
      if (bound) {
         throw new MaterializedTableDropRefusedError(
            "TABLE_IN_USE",
            `Table '${physicalTableName}' on connection '${connectionName}' ` +
               `is not dropped (TABLE_IN_USE): ${bound.label} is bound to a ` +
               `table by that name and may be serving it. Rebind it (build ` +
               `again, or delete the run it serves) first.`,
         );
      }
   }

   /** Guards 3 and 4 of {@link dropConnectionTable}, then the drop. */
   private async probeAndDrop(
      request: Parameters<Reclaimer["dropConnectionTable"]>[0],
      evidence: { namedRecordsIt: boolean; recorded?: ManifestEntry },
   ): Promise<"dropped" | "absent"> {
      const {
         environment,
         environmentName,
         environmentId,
         packageName,
         connectionName,
         connection,
         physicalTableName,
         materializationId,
      } = request;
      const where = `'${physicalTableName}' on connection '${connectionName}'`;
      const unverified = (why: string) =>
         new MaterializedTableDropRefusedError(
            "TABLE_OWNERSHIP_UNVERIFIED",
            `Table ${where} is not dropped (TABLE_OWNERSHIP_UNVERIFIED): ${why}.`,
         );
      const runOptions = statementRunOptions(environment, connectionName, {
         queryClass: "ops",
         environment: environmentName,
         package: packageName,
         ...(materializationId ? { runId: materializationId } : {}),
      });
      const site = databaseTableSite(connection, runOptions);
      if (!capabilitiesOf(site.dialect).tableOwnership) {
         throw unverified(
            `whose it is cannot be read on dialect '${site.dialect}'`,
         );
      }
      const owner = packageOwner(environmentName, packageName);
      // The driver's text can name a host or echo SQL or a secret: logged
      // redacted, never answered.
      const redacted = (err: unknown) =>
         redactConnectionSecrets(
            errMessage(err),
            apiConnectionOf(environment, connectionName),
         );
      let holder: TableHolder;
      try {
         const segments = site.segments(physicalTableName);
         // A mark is proof only on the place it names, which the probe
         // reads from the catalog wherever the name resolved: a renamed,
         // moved or copied table carries its source's mark (markedFor).
         holder = markedFor(
            await site.run(ownedTableProbeSql(segments, site.dialect)),
            owner,
         );
      } catch (err) {
         logger.warn("Could not read who holds a table a caller would drop", {
            environmentName,
            packageName,
            connectionName,
            physicalTableName,
            error: redacted(err),
         });
         throw unverified("whose it is could not be read (the probe failed)");
      }
      // Only this package's own leftover name= view, or nothing: no table.
      if (holder.state === "free" || (holder.state === "ours" && holder.view)) {
         if (evidence.recorded) return "absent";
         throw new TableNotFoundError(
            `No table ${where}, and no run of package ${packageName} records ` +
               `one there.`,
         );
      }
      if (holder.state === "taken") {
         // Who holds it (another package's name, a view's) is logged, not
         // answered: the caller learns only that it is not this package's.
         logger.warn("Refused a caller's drop of a table another holds", {
            environmentName,
            packageName,
            connectionName,
            physicalTableName,
            holder: holder.holder,
         });
         throw new MaterializedTableDropRefusedError(
            "TABLE_OWNED_ELSEWHERE",
            `Table ${where} is not dropped (TABLE_OWNED_ELSEWHERE): the name ` +
               `is held by a table or view that is not package ` +
               `${packageName}'s.`,
         );
      }
      if (holder.state === "unmarked" && !evidence.namedRecordsIt) {
         throw unverified(
            materializationId === undefined
               ? `it carries no Publisher ownership mark. Pass the ` +
                    `materializationId of the run that built it, while that ` +
                    `run's record exists`
               : `it carries no Publisher ownership mark, and run ` +
                    `${materializationId} does not record it on this connection`,
         );
      }
      const leftoversOf = evidence.recorded?.sourceEntityId;
      const tier = { connection, runOptions };
      try {
         // The table alone: what the answer reports on.
         await DATABASE.drop(tier, { physicalTableName });
      } catch (err) {
         recordDropTables("failure", "database", "caller");
         logger.warn(
            "Failed to drop a materialized table at a caller's request",
            {
               environmentName,
               packageName,
               connectionName,
               physicalTableName,
               error: redacted(err),
            },
         );
         throw (
            dropRefusalOf(err, where) ??
            databaseAccessFailure(err) ??
            new Error(`Dropping table ${where} failed: ${redacted(err)}`)
         );
      }
      recordDropTables("success", "database", "caller");
      logger.info("Dropped a materialized table at a caller's request", {
         environmentName,
         packageName,
         connectionName,
         physicalTableName,
         ...(materializationId ? { runId: materializationId } : {}),
         mark: holder.state,
      });
      // Then the staging and backup copies a crashed build can leave,
      // best-effort: the table is gone whatever comes of them, and a copy
      // that cannot be dropped is logged and counted as a failed drop.
      if (leftoversOf) {
         for (const copy of [
            `${physicalTableName}${stagingSuffix(leftoversOf)}`,
            `${physicalTableName}${backupSuffix(leftoversOf)}`,
         ]) {
            await DATABASE.drop(tier, { physicalTableName: copy }).catch(
               (err: unknown) => {
                  recordDropTables("failure", "database", "caller");
                  logger.warn(
                     "Dropped a table at a caller's request, but not a build's leftover copy of it",
                     {
                        environmentName,
                        packageName,
                        connectionName,
                        physicalTableName: copy,
                        error: redacted(err),
                     },
                  );
               },
            );
         }
      }
      await this.forgetBoundaries(environmentId, [
         {
            sourceEntityId: leftoversOf ?? "",
            connectionName,
            physicalTableName,
         },
      ]);
      return "dropped";
   }

   /**
    * Best-effort drop of the tables a run's manifest entries name, skipping
    * any `stillReferenced` holds. Failures are logged rather than thrown, and
    * returned: the entries whose table may still exist because its drop failed
    * or its destination could not be reached.
    */
   async dropManifestEntries(
      environment: MaterializationEnvironment,
      environmentName: string,
      packageName: string,
      materializationId: string,
      occasion: ReclaimOccasion,
      entries: ManifestEntry[],
      stillReferenced: Set<string>,
      connectionFor: (name: string) => Promise<MalloyConnection | undefined>,
      // The `name=` views earlier releases recorded, dropped with the tables
      // they read (leftover_name_views.ts).
      nameViews?: RecordedNameView[],
      // Filled with the entries left in place without a drop (still
      // referenced, another package's now, or in an unloaded version's
      // in-memory duckdb), for a caller that must not read them as dropped.
      skipped?: ManifestEntry[],
   ): Promise<ManifestEntry[]> {
      const m = { id: materializationId };
      const owed: ManifestEntry[] = [];
      const reach = dropReach(
         environment,
         environmentName,
         packageName,
         materializationId,
         occasion,
         connectionFor,
      );
      const { viewContext, owner, retireIfReplacedByView } = reach;
      for (const entry of entries) {
         const connectionName = entry.connectionName;
         const physicalTableName = entry.physicalTableName;
         if (!connectionName || !physicalTableName) {
            logger.warn("Skipping manifest entry with no connection/table", {
               runId: m.id,
               sourceEntityId: entry.sourceEntityId,
            });
            continue;
         }

         if (
            await keptInPlace(
               entry,
               connectionName,
               physicalTableName,
               stillReferenced,
               reach,
               materializationId,
            )
         ) {
            skipped?.push(entry);
            continue;
         }

         // Before the table: a Postgres view refuses its table's drop, and a
         // view left reading a dropped table answers nothing.
         if (nameViews) {
            await dropLeftoverNameViews(entry, nameViews, owner, viewContext);
         }

         // A storage= table lives in its destination, not the source
         // warehouse. A failure is logged and the sweep continues.
         if (entry.storageDestinationName) {
            const dropped = await dropStorageTable(
               environment,
               entry.storageDestinationName,
               {
                  physicalTableName,
                  // Its backup name, where an earlier release's `name=` view
                  // replacement kept the table until its run committed, as a
                  // crash or a failed restore may have left it.
                  leftoversOf: entry.sourceEntityId,
                  onDropFailed: retireIfReplacedByView(entry),
               },
               occasion,
               m.id,
            );
            if (!dropped) owed.push(entry);
            continue;
         }

         const dropped = await dropDatabaseTable(
            environment,
            environmentName,
            packageName,
            materializationId,
            entry,
            connectionName,
            reach,
         );
         if (dropped === "skipped") skipped?.push(entry);
         if (dropped === "owed") owed.push(entry);
      }
      return owed;
   }

   /**
    * The one rename rule: once an auto-run has committed and every loaded
    * copy of the package is bound to it, drop each table it, or an earlier
    * run of the same serving line, replaced (RENAMED_TABLES_KEY: a table
    * renamed, moved to another container or another `storage=`
    * destination, or named before readable names) that Publisher can prove
    * is its own, and list the ones it keeps at an author's `name=` or
    * under a name the package serves elsewhere (KEPT_TABLES_KEY). A table is dropped only when every guard holds,
    * and is otherwise left and logged; a table Publisher cannot prove is
    * its own is never dropped:
    *
    * 1. Publisher named it: its table segment ends in one of Publisher's
    *    own suffixes (`__p<8 hex>`, `__v<version>_<8 hex>`), which an
    *    author's `name=` is refused. A table without one is an author's
    *    exact `name=`, never dropped: kept, listed, and logged as
    *    `author_name`.
    * 2. This package's records say so: an earlier committed auto-run that
    *    `reader` reads built it for the same source, on the same connection
    *    and in the same destination. A host-instructed run's tables are the
    *    host's, and never count.
    * 3. No other package has marked it as its own since (`ownedElsewhere`,
    *    checked by the drop).
    * 4. The source is served from this run: its entry for the source is
    *    what `reader` serves now. Judged against what is served, not the
    *    replacement the record named, so a source renamed again before its
    *    old table went (A to B to C) is still decided.
    * 5. Nothing else names it: no table the package serves now may be it,
    *    and no run `reader` does not read (another version's own, an
    *    orchestrated run of another version) names it. "May be" is by name
    *    alone, whatever the place: a table's name does not say where it
    *    is, and two places (two names for one DuckLake, a connection that
    *    reaches a destination) can reach one table. So a table left by a
    *    `storage=` or tier move that kept its name is never dropped: kept,
    *    listed, and logged as `may_be_served`, since it may be the very
    *    table the package serves now.
    *
    * A table in a storage destination that is no longer configured cannot
    * be reached: kept and listed, `destination_missing`.
    *
    * The decision and the drops are made under the lock a run's start takes
    * for its active-run check (`withRunLock`), from a fresh read of the
    * runs, and only while this run is the newest the package serves and no
    * run that writes its tables is active: otherwise nothing is decided,
    * and the run that is newer, or about to be, decides. So no run commits
    * a table between the checks and the drop.
    *
    * A table is forgotten once it is dropped, or decided by a guard that
    * cannot come to pass (2, a name the package serves, or a kept table,
    * which moves to the list); one whose drop failed or was left in place,
    * or that a guard a later run may find passing kept, stays recorded, for
    * the next run to try. While the package is not bound to this run
    * (`ensureBound` false) nothing is dropped and every reclaimable table
    * stays recorded. Best-effort throughout. Within one server: the guards
    * read this server's records, so servers that share a schema must move
    * to this release together.
    *
    * Returns the tables this run lists as kept (carried forward from the
    * serving line's run before it, less any dropped since, by hand or with
    * its runs, or whose name a table of this run takes), for the package's
    * warnings; undefined when the step decided nothing.
    */
   async reclaimRenamedTables(
      environmentName: string,
      packageName: string,
      runId: string,
      reader: ServingReader,
      /**
       * Rebind every loaded copy of the package from the store, answering
       * whether each now reads it; asked before any drop, since a table an
       * earlier run recorded may still be read by a copy this run did not
       * rebind.
       */
      ensureBound: () => Promise<boolean>,
   ): Promise<KeptTable[] | undefined> {
      try {
         const environmentId = await this.resolveEnvironmentId(environmentName);
         const environment = await this.deps.host.getEnvironment(
            parseEnvironmentName(environmentName),
            false,
         );
         // Resolved outside the lock: resolving may load the package, which
         // takes the package's lock.
         const connectionFor = await this.connectionResolverFor(
            environment,
            packageName,
            reader.versionId ?? null,
         );
         const decide = async (log: boolean) => {
            const runs =
               (await this.repository.listMaterializations(
                  environmentId,
                  packageName,
               )) ?? [];
            const run = runs.find((m) => m.id === runId);
            if (run?.status !== "MANIFEST_FILE_READY") return undefined;
            return {
               runs,
               run,
               ...this.decideRenamedTables(runs, run, reader, log),
            };
         };
         // Asked before the lock (a rebind takes the package's lock): only
         // when there may be something to drop.
         const before = await decide(false);
         if (!before) return undefined;
         // A table that becomes reclaimable only under the lock waits for a
         // later run: the package was not asked to bind for it.
         const bound = before.reclaimable.length > 0 && (await ensureBound());
         // Whether each table this run may list as kept still exists,
         // probed before the lock (a probe can be slow, and billed) and a
         // few at a time. A table listed only under the lock is not probed,
         // and stays listed.
         const gone = await this.keptTablesGone(
            keptAfter(before.run, before.runs, reader, before.kept).tables,
            dropReach(
               environment,
               environmentName,
               packageName,
               runId,
               "reclaim",
               connectionFor,
            ).viewContext,
         );
         const withRunLock =
            this.deps.withRunLock ??
            ((_e, _i, _p, _v, fn: () => Promise<KeptTable[] | undefined>) =>
               fn());
         return await withRunLock(
            environment,
            environmentId,
            packageName,
            reader.versionId,
            async () => {
               const now = await decide(true);
               if (!now) return undefined;
               const { runs } = now;
               const busy = runs.find(
                  (m) => isActiveRun(m.status) && readsRun(m, reader),
               );
               const newest = newestServingRun(runs, reader);
               if (busy || newest?.id !== runId) {
                  logger.info(
                     "Deciding nothing about the tables readable names replaced: a newer run commits or has committed",
                     {
                        packageName,
                        runId,
                        ...(busy ? { activeRunId: busy.id } : {}),
                        ...(newest && newest.id !== runId
                           ? { newerRunId: newest.id }
                           : {}),
                     },
                  );
                  return undefined;
               }
               return this.applyRenamedDecision(
                  environment,
                  environmentName,
                  environmentId,
                  packageName,
                  runId,
                  reader,
                  now,
                  { bound, connectionFor, gone },
               );
            },
         );
      } catch (err) {
         logger.warn(
            "Reclaiming tables readable names replaced did not complete",
            { error: errMessage(err) },
         );
         return undefined;
      }
   }

   /**
    * Under the run lock, act on one decision of
    * {@link reclaimRenamedTables}: set aside the tables a missing
    * destination keeps, drop the rest while the package is bound, list what
    * is kept (less the tables gone since), and record what was decided.
    */
   private async applyRenamedDecision(
      environment: MaterializationEnvironment,
      environmentName: string,
      environmentId: string,
      packageName: string,
      runId: string,
      reader: ServingReader,
      decision: ReturnType<Reclaimer["decideRenamedTables"]> & {
         runs: Materialization[];
         run: Materialization;
      },
      options: {
         bound: boolean;
         connectionFor: (name: string) => Promise<MalloyConnection | undefined>;
         /** The kept tables found gone, by keptKey. */
         gone: ReadonlySet<string>;
      },
   ): Promise<KeptTable[]> {
      const { runs, run, recordedOn, decided, retried, kept } = decision;
      // A table in a destination no longer configured cannot be reached.
      const toDrop: ManifestEntry[] = [];
      for (const entry of decision.reclaimable) {
         if (
            entry.storageDestinationName &&
            !destinationConfigured(environment, entry.storageDestinationName)
         ) {
            logKept(entry, "destination_missing");
            kept.push(keptOf(entry, "destination_missing"));
            continue;
         }
         toDrop.push(entry);
      }
      if (toDrop.length > 0 && !options.bound) {
         logger.warn(
            "Keeping the tables readable names replaced: a loaded copy of the package did not rebind",
            { packageName },
         );
         for (const entry of toDrop) retried.add(entryRenamedKey(entry));
         toDrop.length = 0;
      }
      let owed: ManifestEntry[] = [];
      const skipped: ManifestEntry[] = [];
      if (toDrop.length > 0) {
         // 3 is checked by the drop, table by table.
         owed = await this.deps.dropManifestEntries(
            environment,
            environmentName,
            packageName,
            runId,
            "reclaim",
            toDrop,
            new Set(),
            options.connectionFor,
            undefined,
            skipped,
         );
      }
      const skippedKeys = new Set(skipped.map(entryRenamedKey));
      const owedKeys = new Set(owed.map(entryRenamedKey));
      for (const entry of owed) {
         logger.warn("Could not drop a table a readable name replaced", {
            physicalTableName: entry.physicalTableName,
         });
      }
      // A dropped table's incremental boundary describes nothing now.
      // Owed, or left in place by the drop: not dropped.
      await this.forgetBoundaries(
         environmentId,
         toDrop.filter(
            (e) =>
               !owedKeys.has(entryRenamedKey(e)) &&
               !skippedKeys.has(entryRenamedKey(e)),
         ),
      );
      const listed = keptAfter(run, runs, reader, kept);
      // A kept table dropped by hand is no longer listed.
      const present = listed.tables.filter(
         (t) => !options.gone.has(keptKey(t)),
      );
      const write = listed.write || present.length !== listed.tables.length;
      // Forget what was decided; keep what a drop still owes, what it left
      // in place, and what a passing guard may yet let a later run drop.
      // This run lists what it keeps.
      for (const m of new Set([...recordedOn, run])) {
         const metadata = { ...(m.metadata ?? {}) };
         let changed = false;
         const tables = renamedTablesIn(m.metadata ?? null);
         const left = tables.filter(
            (t) =>
               !decided.has(renamedKey(t)) ||
               owedKeys.has(renamedKey(t)) ||
               skippedKeys.has(renamedKey(t)) ||
               retried.has(renamedKey(t)),
         );
         if (left.length !== tables.length) {
            if (left.length > 0) metadata[RENAMED_TABLES_KEY] = left;
            else delete metadata[RENAMED_TABLES_KEY];
            changed = true;
         }
         if (m.id === runId && write) {
            metadata[KEPT_TABLES_KEY] = present;
            changed = true;
         }
         if (changed) {
            await this.repository.updateMaterialization(m.id, { metadata });
         }
      }
      return present;
   }

   /**
    * The keptKeys of `tables` found gone, probed a few at a time. A table
    * whose site cannot be asked, or that a dialect may hide from the
    * session (Snowflake, MySQL: not visible need not mean gone), is not.
    */
   private async keptTablesGone(
      tables: KeptTable[],
      context: TableSiteContext,
   ): Promise<Set<string>> {
      const gone = new Set<string>();
      const queue = [...tables];
      const worker = async () => {
         for (let table = queue.shift(); table; table = queue.shift()) {
            const current = table;
            const exists = await atTableSite(current, context, async (site) =>
               capabilitiesOf(site.dialect).catalogHidesTables
                  ? undefined
                  : tableExistsAt(site, current.physicalTableName),
            ).catch(() => undefined);
            if (exists === false) {
               logger.info("No longer listing a kept table: it is gone", {
                  physicalTableName: current.physicalTableName,
               });
               gone.add(keptKey(current));
            }
         }
      };
      await Promise.all(
         Array.from(
            { length: Math.min(KEPT_PROBES_AT_ONCE, queue.length) },
            worker,
         ),
      );
      return gone;
   }

   /**
    * Decide each table recorded on the runs of `reader`'s serving line once:
    * kept by a guard of {@link reclaimRenamedTables} (logged, `retried` when
    * a later run may find that guard passing, and `kept` when it is
    * listed), else reclaimable, as an entry at its old place.
    */
   private decideRenamedTables(
      runs: Materialization[],
      run: Materialization,
      reader: ServingReader,
      /** Log each kept table's guard (the pass that acts on the decision). */
      log: boolean,
   ): {
      recordedOn: Materialization[];
      decided: Set<string>;
      retried: Set<string>;
      reclaimable: ManifestEntry[];
      kept: KeptTable[];
   } {
      // Where each table was recorded, so a decided one can be forgotten.
      const recordedOn = runs.filter(
         (m) =>
            m.status === "MANIFEST_FILE_READY" &&
            (m.id === run.id || readsRun(m, reader)) &&
            renamedTablesIn(m.metadata ?? null).length > 0,
      );
      const decided = new Set<string>();
      const retried = new Set<string>();
      const reclaimable: ManifestEntry[] = [];
      const kept: KeptTable[] = [];
      for (const table of recordedOn.flatMap((m) =>
         renamedTablesIn(m.metadata ?? null),
      )) {
         const key = renamedKey(table);
         if (decided.has(key)) continue;
         const guard = this.renamedTableGuard(table, run, runs, reader);
         decided.add(key);
         const entry: ManifestEntry = {
            sourceEntityId: table.sourceEntityId,
            physicalTableName: table.previousName,
            connectionName: table.connectionName,
            ...(previousDestinationOf(table)
               ? { storageDestinationName: previousDestinationOf(table) }
               : {}),
         };
         if (guard === undefined) {
            reclaimable.push(entry);
            continue;
         }
         // A guard that a later run may find passing keeps the record.
         if (TRANSIENT_GUARDS.has(guard)) retried.add(key);
         if (log) {
            logger.info("Keeping a table a readable name replaced", {
               physicalTableName: table.previousName,
               replacedBy: table.physicalTableName,
               guard,
            });
         }
         if (guard === "author_name" || guard === "may_be_served") {
            kept.push({
               ...keptOf(entry, guard),
               ...(table.sourceName ? { sourceName: table.sourceName } : {}),
            });
         }
      }
      return { recordedOn, decided, retried, reclaimable, kept };
   }

   /** Forget the incremental boundaries of dropped tables; best-effort. */
   private async forgetBoundaries(
      environmentId: string,
      dropped: ManifestEntry[],
   ): Promise<void> {
      for (const entry of dropped) {
         await this.repository
            .deleteIncrementalLedgerEntry(environmentId, {
               connectionName: entry.connectionName ?? "",
               ...(entry.storageDestinationName
                  ? { storageDestinationName: entry.storageDestinationName }
                  : {}),
               physicalTableName: entry.physicalTableName,
            })
            .catch((err) =>
               logger.warn(
                  "Could not forget the incremental boundary of a dropped table",
                  {
                     physicalTableName: entry.physicalTableName,
                     error: errMessage(err),
                  },
               ),
            );
      }
   }

   /**
    * Which guard of {@link reclaimRenamedTables} keeps `table`, or undefined
    * when none does (guard 3 is the drop's, and a missing destination the
    * caller's). `author_name` and `may_be_served` are the ones that list it
    * as kept.
    */
   private renamedTableGuard(
      table: RenamedTable,
      run: Materialization,
      runs: Materialization[],
      reader: ServingReader,
   ): string | undefined {
      const place = placeKey;
      const oldAt: Place = {
         connectionName: table.connectionName,
         storageDestinationName: previousDestinationOf(table),
      };
      // 4: the source is served from this run, wherever its table is now.
      const current = run.manifest?.entries?.[table.sourceEntityId];
      if (!current?.physicalTableName || isLegacyFailedEntry(current)) {
         return "replacement-not-committed";
      }
      const serving = newestServingEntries(runs, reader);
      const served = serving[table.sourceEntityId];
      if (
         served?.physicalTableName !== current.physicalTableName ||
         place(served) !== place(current)
      ) {
         return "not-serving-replacement";
      }
      // 2: an earlier auto-run of this package built it for this source,
      // there. A host's run named its tables itself.
      const builtIt = (m: Materialization) => {
         const e = m.manifest?.entries?.[table.sourceEntityId];
         return (
            e?.physicalTableName === table.previousName &&
            place(e) === place(oldAt)
         );
      };
      if (
         !runs.some(
            (m) =>
               m.id !== run.id &&
               m.status === "MANIFEST_FILE_READY" &&
               m.metadata?.mode !== "orchestrated" &&
               readsRun(m, reader) &&
               builtIt(m),
         )
      ) {
         return "not-in-package-records";
      }
      // 5: no table the package serves now may be it. By table segment
      // alone (leafKey): a table's name does not say where it
      // is, and two places (two names for one DuckLake, a connection that
      // reaches a destination) can reach one table, which no config or
      // probe proves otherwise; nor does a container (on DuckDB `memory.t`
      // and `main.t` are one table). Served at that very name in this very
      // place, it is the served table; otherwise it may be, so it is never
      // dropped: kept and listed.
      const namesIt = (e: ManifestEntry) =>
         !!e.physicalTableName &&
         leafKey(e.physicalTableName) === leafKey(table.previousName);
      const servedUnderName = Object.values(serving).filter(namesIt);
      if (
         servedUnderName.some(
            (e) =>
               place(e) === place(oldAt) &&
               mayBeSameTable(e.physicalTableName, table.previousName),
         )
      ) {
         return "still-served";
      }
      if (servedUnderName.length > 0) return "may_be_served";
      // 5: no run this run does not supersede still names it.
      if (
         runs.some(
            (m) =>
               m.status === "MANIFEST_FILE_READY" &&
               !readsRun(m, reader) &&
               Object.values(m.manifest?.entries ?? {}).some(namesIt),
         )
      ) {
         return "still-referenced";
      }
      // 5: no active run (an orchestrated run of another version, say) has
      // claimed a name that may be it, to write.
      if (
         runs.some(
            (m) =>
               isActiveRun(m.status) &&
               tableNamesIn(m.metadata, WRITES_TABLES_KEY).some(
                  (name) => leafKey(name) === leafKey(table.previousName),
               ),
         )
      ) {
         return "still-referenced";
      }
      // 1: an author's exact name= is never dropped.
      if (!hasReservedTableSuffix(table.previousName)) return "author_name";
      return undefined;
   }

   /**
    * Reclaim what an archived or deleted version alone owns: the tables its
    * auto-runs built under `scope: version` (named for it, see
    * versionTableSuffix). Started by the archive or the delete, off the
    * request path, after it committed; best-effort throughout, and tried
    * once.
    *
    * Left alone: a table any other run of the package still references; the
    * tables of an orchestrated run, whose names and lifetime the caller that
    * assigned them owns; and everything under `scope: package`, whose tables
    * the package's versions share. A run that
    * failed or was cancelled before it committed is reclaimed too: its
    * manifest names no table, so the tables it recorded writing before it
    * built are dropped instead (reclaimableEntries). The reclaimed runs are
    * then deleted, whether or not every drop succeeded, so an unarchived
    * version serves live until it is built again rather than binding to
    * tables that are gone; a table that could not be dropped is named in one
    * warning and left.
    *
    * Waits for the version's builds to settle (a delete stops its auto-runs), then runs
    * under the version's lock, and only while the version is still archived,
    * or deleted and not published again: an unarchive, a publish of the
    * version, and a run of it (which is refused while it is archived) take
    * the same lock.
    */
   async reclaimVersionTables(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      try {
         const environment = await this.deps.host.getEnvironment(
            parseEnvironmentName(environmentName),
            false,
         );
         const versions = environment.getVersionService();
         if (!versions) return;
         const environmentId = await this.resolveEnvironmentId(environmentName);
         await environment.versionBuildsSettled(packageName, versionId);
         await versions.withVersionLock(packageName, versionId, async () => {
            const row = await this.repository.getVersion(
               environmentId,
               packageName,
               versionId,
            );
            if (row !== null && row.archiveStatus !== "archive") return;
            await this.reclaimOwnedRuns(
               environment,
               environmentName,
               environmentId,
               packageName,
               versionId,
               row === null ? "deleted" : "archived",
            );
         });
      } catch (err) {
         logger.warn("Failed to reclaim a version's tables", {
            environmentName,
            packageName,
            version: versionId,
            error: errMessage(err),
         });
      }
   }

   /**
    * Before a publish creates a version's row: reclaim now what a deleted
    * version of the same id still owes, when that delete's cleanup has not
    * run yet (it waits for the version's builds to settle, an orchestrated
    * one included). Otherwise the new version could bind those tables, and
    * its row clears the records naming them, leaving the tables to nothing.
    * Called holding the version's lock, so it neither takes the lock nor
    * waits: a run still settling is left, as reclaimOwnedRuns leaves it.
    * With no such runs, it reclaims nothing. Takes the Environment, never
    * looks it up: a configured package publishes while the store is still
    * initializing, and a lookup would wait for that. Best-effort: never
    * throws.
    */
   async reclaimOwedNow(
      environment: MaterializationEnvironment,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      const environmentName = environment.getEnvironmentName();
      try {
         const environmentId = await this.resolveEnvironmentId(environmentName);
         await this.reclaimOwnedRuns(
            environment,
            environmentName,
            environmentId,
            packageName,
            versionId,
            "deleted",
         );
      } catch (err) {
         logger.warn("Failed to reclaim what a deleted version owed", {
            environmentName,
            packageName,
            version: versionId,
            error: errMessage(err),
         });
      }
   }

   /**
    * After a version is deleted: stop its own running auto-runs, then reclaim
    * what it alone owns, as an archive does (see reclaimVersionTables, which
    * waits for its builds to settle first). An orchestrated run is left to the
    * caller that instructed it, as its tables are. Best-effort: never throws.
    */
   async cleanUpDeletedVersion(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      try {
         await this.stopVersionRuns(environmentName, packageName, versionId);
      } catch (err) {
         logger.warn("Failed to stop a deleted version's builds", {
            environmentName,
            packageName,
            version: versionId,
            error: errMessage(err),
         });
      }
      await this.reclaimVersionTables(environmentName, packageName, versionId);
   }

   /**
    * Stop every auto-run of a version that has not settled, as stopping each
    * one does (see MaterializationService.stopMaterialization), without
    * looking the version up: it is already deleted.
    */
   private async stopVersionRuns(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const runs = await this.repository.listMaterializations(
         environmentId,
         packageName,
         { version: versionId },
      );
      for (const m of runs) {
         if (m.metadata?.mode !== "auto" || !isActiveRun(m.status)) {
            continue;
         }
         await this.deps.lifecycle.cancel(m.id);
      }
   }

   /**
    * Drop the tables of a version's own committed or failed auto-runs that no
    * other run references, and delete those runs, once each: a table that
    * could not be dropped is named in a warning and left. Called holding the
    * version's lock, with the version archived or deleted, so nothing serves
    * those tables or writes them.
    */
   private async reclaimOwnedRuns(
      environment: MaterializationEnvironment,
      environmentName: string,
      environmentId: string,
      packageName: string,
      versionId: string,
      cause: "archived" | "deleted",
   ): Promise<void> {
      // Read once, the references with the runs they exclude: the version is
      // out of service, so it starts no run, and its tables carry its name,
      // so no other version's run writes them.
      const listed =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
      const runs = listed.filter(
         (m) =>
            m.version === versionId &&
            isVersionOwnedRun(m) &&
            m.metadata?.mode === "auto" &&
            !isActiveRun(m.status),
      );
      if (runs.length === 0) return;
      const reclaimed = new Set(runs.map((m) => m.id));
      const stillReferenced = tablesStillReferenced(
         listed,
         (other) => !reclaimed.has(other.id),
      );
      // Never through getPackage: the version is out of service, and its
      // package-local duckdb went with its unload.
      const connectionFor = await this.connectionResolverFor(
         environment,
         packageName,
         versionId,
      );
      const nameViews = recordedNameViews(listed, runs);
      for (const m of runs) {
         const owed = await this.deps.dropManifestEntries(
            environment,
            environmentName,
            packageName,
            m.id,
            "reclaim",
            reclaimableEntries(m),
            stillReferenced,
            connectionFor,
            nameViews,
         );
         await this.repository.deleteMaterialization(m.id);
         if (owed.length > 0) {
            logger.warn(
               "Reclaiming a version left tables it could not drop; they are not tried again",
               {
                  environmentName,
                  packageName,
                  version: versionId,
                  cause,
                  runId: m.id,
                  tables: owed.map((e) => e.physicalTableName),
               },
            );
         }
      }
      logger.info(
         `Reclaimed ${cause === "archived" ? "an archived" : "a deleted"} version's tables`,
         {
            environmentName,
            packageName,
            version: versionId,
            runs: runs.length,
         },
      );
   }

   /**
    * Finish, once the environments have loaded, a reclaim that a crash cut
    * short: every archived version that still has its own auto-runs, and
    * every deleted version whose own auto-runs are still recorded, across
    * the environments this server holds. A reclaim deletes the runs it
    * tried, whatever came of its drops, so this finds only reclaims that
    * never ran, and finds nothing on a later start. Best-effort, one at a
    * time.
    */
   async reclaimArchivedVersions(): Promise<void> {
      for (const environment of this.deps.host.getLoadedEnvironments()) {
         const environmentName = environment.getEnvironmentName();
         try {
            const environmentId =
               await this.resolveEnvironmentId(environmentName);
            const rows =
               await this.repository.listVersionsByEnvironment(environmentId);
            const held = new Set(
               rows.map((row) => `${row.packageName}@${row.versionId}`),
            );
            const archived = new Set(
               rows
                  .filter((row) => row.archiveStatus === "archive")
                  .map((row) => `${row.packageName}@${row.versionId}`),
            );
            // Each version with its own auto-runs still recorded, once:
            // archived (it was not reclaimed), or deleted (it has no row,
            // even when its package went with it).
            const toFinish = new Map<
               string,
               { packageName: string; versionId: string }
            >();
            for (const m of await this.repository.listEnvironmentMaterializations(
               environmentId,
            )) {
               if (
                  !isVersionOwnedRun(m) ||
                  m.version === null ||
                  m.metadata?.mode !== "auto"
               ) {
                  continue;
               }
               const key = `${m.packageName}@${m.version}`;
               if (archived.has(key) || !held.has(key)) {
                  toFinish.set(key, {
                     packageName: m.packageName,
                     versionId: m.version,
                  });
               }
            }
            for (const { packageName, versionId } of toFinish.values()) {
               await this.reclaimVersionTables(
                  environmentName,
                  packageName,
                  versionId,
               );
            }
         } catch (err) {
            logger.warn("Failed to finish reclaiming versions at startup", {
               environmentName,
               error: errMessage(err),
            });
         }
      }
   }
}

/** A table whose name this package's own `name=` view now holds: not owed. */
function logRetiredByView(
   materializationId: string,
   physicalTableName: string,
): void {
   logger.info(
      "Skipped drop: a name= view this package made in an earlier release replaced the table, which is gone",
      { runId: materializationId, physicalTableName },
   );
}
