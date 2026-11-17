// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Giving back what materializations built ({@link Reclaimer}): the tables of a
 * deleted run, of a failed run that committed no manifest, of an archived or
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
import { logger } from "../../../logger";
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
   writeTargetsIn,
} from "../../versions/materialization_scope";
import type { MaterializationEnvironment, MaterializationHost } from "../host";
import {
   isLegacyFailedEntry,
   type KeptTable,
   type KeptTableReason,
   ManifestEntry,
} from "../domain/types";
import {
   connectionApartFromDestination,
   destinationsApart,
} from "../../destination_overlap";
import { ColocatedTier } from "./colocated_build_strategy";
import { StorageBuildStrategy } from "./storage_build_strategy";
import {
   BuildEnvironment,
   statementRunOptions,
   isActiveRun,
   redactConnectionSecrets,
   tableKeyOf,
} from "./materialization_run_support";
import {
   dropLeftoverNameViews,
   heldByOwnNameView,
   nameViewsIn,
   type RecordedNameView,
} from "./leftover_name_views";
import { packageOwner } from "./table_mark_sql";
import { ownedElsewhere, tableExistsAt } from "./table_ownership";
import { atTableSite } from "./table_site";
import type { RunLifecycle } from "./run_lifecycle";
import {
   mayBeOneTable,
   previousDestinationOf,
   type RenamedTable,
} from "./run_instructions";
import { hasReservedTableSuffix } from "../domain/table_name";

/** The tiers a drop reaches a table through. */
const COLOCATED = new ColocatedTier();
// Only its drop. A build goes through the service's adapter, which routes a
// chained build through the run's collaborators; this one would not.
const STORAGE: Pick<StorageBuildStrategy, "drop"> = new StorageBuildStrategy();

/** A physical table's key for drop bookkeeping: where it lives, and its name. */
function tableKey(destination: string, physicalTableName: string): string {
   return `${destination}:${physicalTableName}`;
}

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
            (t as KeptTable).reason === "destination_missing"),
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
   return `${t.connectionName}\u0000${t.storageDestinationName ?? ""}\u0000${t.physicalTableName}`;
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
   return `${t.connectionName}\u0000${previousDestinationOf(t) ?? ""}\u0000${t.previousName}`;
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
   const named = new Set(
      entries.map((e) =>
         tableKeyOf(
            e.connectionName ?? "",
            e.storageDestinationName,
            e.physicalTableName,
         ),
      ),
   );
   for (const target of writeTargetsIn(m.metadata ?? null)) {
      const key = tableKeyOf(
         target.connectionName,
         target.storageDestinationName,
         target.physicalTableName,
      );
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
   occasion: DropOccasion,
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
   occasion: DropOccasion,
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
 * The destination-and-name keys of every table a MANIFEST_FILE_READY run
 * references, among the `listed` runs `include` admits.
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
         const dest = e.storageDestinationName ?? e.connectionName;
         if (dest && e.physicalTableName) {
            stillReferenced.add(tableKey(dest, e.physicalTableName));
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
   if (stillReferenced.has(tableKey(destForKey, physicalTableName))) {
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
 * Drop one colocated table, with its staging and backup names, on its own
 * connection. Best-effort: `owed` when the drop failed, which is logged;
 * `skipped` when the table lived in an unloaded version's in-memory duckdb.
 */
async function dropColocatedTable(
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
): Promise<"dropped" | "owed" | "skipped"> {
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
      await COLOCATED.drop(
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
      recordDropTables("success", "in_warehouse", occasion);
      logger.info("Dropped materialized table on delete", {
         runId: materializationId,
         physicalTableName,
         connectionName,
      });
      return "dropped";
   } catch (err) {
      recordDropTables("failure", "in_warehouse", occasion);
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
function logKept(entry: ManifestEntry, guard: KeptTableReason): void {
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

/** Whether two places may hold one table under one name. */
type MayBeOnePlace = (a: Place, b: Place) => boolean;

/**
 * Whether two places may be one, by the environment's configs: the same
 * place, or two places their configs do not prove apart. Two storage
 * destinations are apart when they are provably different DuckLakes, or
 * different schemas of one (destinationsApart); a connection's own database
 * and a destination when the connection provably cannot reach the
 * destination's tables (connectionApartFromDestination). Two connections'
 * own databases are never proven apart. A place whose config is gone is
 * not proven apart from anything: it may have been renamed.
 */
function placeAliasing(environment: MaterializationEnvironment): MayBeOnePlace {
   const configOf = (lookup: () => ApiConnection) => {
      try {
         return lookup();
      } catch {
         return undefined;
      }
   };
   const destination = (name: string) =>
      configOf(() => environment.getStorageDestination(name));
   const connection = (name: string) =>
      configOf(() => environment.getApiConnection(name));
   return (a, b) => {
      const da = a.storageDestinationName ?? undefined;
      const db = b.storageDestinationName ?? undefined;
      if (da === undefined && db === undefined) return true;
      if (da !== undefined && db !== undefined) {
         if (da === db) return true;
         const x = destination(da);
         const y = destination(db);
         return !(x && y && destinationsApart(x, y));
      }
      const [own, stored] = da === undefined ? [a, db!] : [b, da];
      const c = connection(own.connectionName ?? "");
      const d = destination(stored);
      return !(c && d && connectionApartFromDestination(c, d));
   };
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
 * less any no committed run of the package names any more (dropped with
 * its runs) and any a table of `run` may be (its name taken again, in a
 * place that may be the same). `write`
 * says whether the list goes on the run: when it lists any, or when the run
 * before it listed any, so a list that emptied is written empty once and
 * stops being carried.
 */
function keptAfter(
   run: Materialization,
   runs: Materialization[],
   reader: ServingReader,
   fresh: KeptTable[],
   mayBeOne: MayBeOnePlace,
): { tables: KeptTable[]; write: boolean } {
   const before = newestListing(runs, reader, { until: run, except: run.id });
   const carried = [
      ...keptTablesIn(before?.metadata ?? null),
      ...keptTablesIn(run.metadata ?? null),
   ];
   const at = (t: KeptTable, e: ManifestEntry): boolean =>
      !!e.physicalTableName &&
      mayBeOneTable(e.physicalTableName, t.physicalTableName) &&
      mayBeOne(e, t);
   const named = (t: KeptTable) =>
      runs.some(
         (m) =>
            m.status === "MANIFEST_FILE_READY" &&
            Object.values(m.manifest?.entries ?? {}).some((e) => at(t, e)),
      );
   const reused = (t: KeptTable) =>
      Object.values(run.manifest?.entries ?? {}).some((e) => at(t, e));
   const byKey = new Map<string, KeptTable>();
   for (const t of [...carried, ...fresh]) byKey.set(keptKey(t), t);
   const tables = [...byKey.values()].filter((t) => named(t) && !reused(t));
   return {
      tables,
      write:
         tables.length > 0 ||
         keptTablesIn(before?.metadata ?? null).length > 0 ||
         Array.isArray(run.metadata?.[KEPT_TABLES_KEY]),
   };
}

/** A drop of a run's manifest entries; see {@link Reclaimer.dropManifestEntries}. */
type DropManifestEntries = Reclaimer["dropManifestEntries"];

/**
 * Giving back what materializations built: the tables of a deleted run, of a
 * failed run that committed no manifest, and of an archived or deleted
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
      },
   ) {}

   private get repository(): ResourceRepository {
      return this.deps.repository();
   }

   private resolveEnvironmentId(environmentName: string): Promise<string> {
      return resolveEnvironmentId(this.repository, environmentName);
   }

   /**
    * Drop the storage tables a FAILED run created: the run commits no manifest,
    * and GC reclaims only what a manifest records. Each guard closes a way to
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
    *   destination-and-name check {@link dropMaterializedTables} applies.
    *
    * Colocated failures are left alone: stable names in the customer's own
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
            if (stillReferenced.has(tableKey(dest, table))) {
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
    * Best-effort drop of every physical table (and leftover staging table) in
    * this run's manifest. Failures are logged and swallowed so cleanup never
    * blocks deleting the record.
    *
    * A name is dropped only when NO other MANIFEST_FILE_READY run references
    * it: generations of a source share one physical name, so dropping a
    * superseded record's table would take out the one still serving.
    */
   async dropMaterializedTables(
      environmentName: string,
      packageName: string,
      m: Materialization,
   ): Promise<void> {
      const entries = m.manifest?.entries;
      if (!entries || Object.keys(entries).length === 0) {
         return;
      }

      const environment = await this.deps.host.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );

      // Names another MANIFEST_FILE_READY run still references, whichever
      // version built it. `m` is still in the repo (deleted after this sweep),
      // so it is excluded by id.
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const listed =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
      await this.deps.dropManifestEntries(
         environment,
         environmentName,
         packageName,
         m.id,
         "delete",
         Object.values(entries),
         tablesStillReferenced(listed, (other) => other.id !== m.id),
         await this.connectionResolverFor(environment, packageName, m.version),
         recordedNameViews(listed, [m]),
      );
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
      occasion: DropOccasion,
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

         const dropped = await dropColocatedTable(
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
    * is its own, and list the ones it keeps at an author's `name=`
    * (KEPT_TABLES_KEY). A table is dropped only when every guard holds,
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
    *    and by place: two places are one unless their configs prove them
    *    apart (placeAliasing), so a move between two names for one
    *    DuckLake, or from a connection that reaches the destination, never
    *    drops the table it moved to.
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
         const mayBeOne = placeAliasing(environment);
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
               ...this.decideRenamedTables(runs, run, reader, mayBeOne, log),
            };
         };
         // Asked before the lock (a rebind takes the package's lock): only
         // when there may be something to drop.
         const before = await decide(false);
         if (!before) return undefined;
         // A table that becomes reclaimable only under the lock waits for a
         // later run: the package was not asked to bind for it.
         const bound = before.reclaimable.length > 0 && (await ensureBound());
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
                  { bound, connectionFor, mayBeOne },
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
         mayBeOne: MayBeOnePlace;
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
      const listed = keptAfter(run, runs, reader, kept, options.mayBeOne);
      // A kept table dropped by hand is no longer listed; one whose site
      // cannot be asked stays.
      const { viewContext } = dropReach(
         environment,
         environmentName,
         packageName,
         runId,
         "reclaim",
         options.connectionFor,
      );
      const present: KeptTable[] = [];
      for (const table of listed.tables) {
         const exists = await atTableSite(table, viewContext, (site) =>
            tableExistsAt(site, table.physicalTableName),
         ).catch(() => undefined);
         if (exists === false) {
            logger.info("No longer listing a kept table: it is gone", {
               physicalTableName: table.physicalTableName,
            });
            continue;
         }
         present.push(table);
      }
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
    * Decide each table recorded on the runs of `reader`'s serving line once:
    * kept by a guard of {@link reclaimRenamedTables} (logged, `retried` when
    * a later run may find that guard passing, and `kept` when it is
    * listed), else reclaimable, as an entry at its old place.
    */
   private decideRenamedTables(
      runs: Materialization[],
      run: Materialization,
      reader: ServingReader,
      mayBeOne: MayBeOnePlace,
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
         const guard = this.renamedTableGuard(
            table,
            run,
            runs,
            reader,
            mayBeOne,
         );
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
         if (guard === "author_name") {
            kept.push({
               ...keptOf(entry, "author_name"),
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
    * caller's). `author_name` is the one that lists it as kept.
    */
   private renamedTableGuard(
      table: RenamedTable,
      run: Materialization,
      runs: Materialization[],
      reader: ServingReader,
      mayBeOne: MayBeOnePlace,
   ): string | undefined {
      const place = (e: Place) =>
         `${e.connectionName ?? ""}\u0000${e.storageDestinationName ?? ""}`;
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
      const namesIt = (e: ManifestEntry) =>
         !!e.physicalTableName &&
         mayBeOneTable(e.physicalTableName, table.previousName) &&
         mayBeOne(e, oldAt);
      // 5: no table the package serves now may be it (another source's, or
      // the source's own under a name, or in a place, that may be it).
      if (Object.values(serving).some(namesIt)) return "still-served";
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
