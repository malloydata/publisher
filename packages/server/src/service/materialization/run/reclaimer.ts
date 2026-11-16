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
import { mergeQueryMetadata, type QueryMetadata } from "../../query_metadata";
import { resolveEnvironmentId } from "../../resolve_environment";
import {
   isVersionOwnedRun,
   newestServingEntries,
   readsRun,
   type ServingReader,
   writeTargetsIn,
} from "../../versions/materialization_scope";
import type { MaterializationEnvironment, MaterializationHost } from "../host";
import { ManifestEntry } from "../domain/types";
import { ColocatedTier } from "./colocated_build_strategy";
import { StorageBuildStrategy } from "./storage_build_strategy";
import {
   BuildEnvironment,
   connectionMetadataLayers,
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
import { ownedElsewhere } from "./table_ownership";
import { atTableSite } from "./table_site";
import type { RunLifecycle } from "./run_lifecycle";
import type { RenamedTable } from "./run_instructions";

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
 * Where a run records the tables it built under readable names in place of
 * tables earlier runs built under the names Publisher gave them before (see
 * Reclaimer.reclaimRenamedTables), until each is dropped or kept.
 */
export const RENAMED_TABLES_KEY = "renamedTables";

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

/** One old table, wherever it is recorded. */
function renamedKey(
   t: Pick<
      RenamedTable,
      "previousName" | "connectionName" | "storageDestinationName"
   >,
): string {
   return `${t.connectionName}\u0000${t.storageDestinationName ?? ""}\u0000${t.previousName}`;
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
   const connectionLayers = connectionMetadataLayers(
      environment,
      connectionName,
   );
   const resolved = mergeQueryMetadata({
      connection: connectionLayers.default,
      enforced: connectionLayers.enforced,
      context: {
         queryClass: "ops",
         environment: environmentName,
         package: packageName,
         runId: materializationId,
      },
   });
   return resolved.metadata ? { queryMetadata: resolved.metadata } : {};
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
    * Drop the tables auto-runs replaced with tables under readable names, once
    * a run has committed and every loaded copy of the package is bound to it.
    * The tables come from the runs' own records (RENAMED_TABLES_KEY): this
    * run's, and any an earlier run of the same serving line recorded and
    * could not finish (a crash between its commit and its drops, a failed
    * drop, a bind that failed). Each table is dropped only when every guard
    * holds, and is otherwise left and logged; a table Publisher cannot prove
    * is its own is never dropped:
    *
    * 1. It is a name Publisher gave this package's table before readable
    *    names: re-derived exactly from the environment, the package and the
    *    version (deriveSelfInstructions only offers such a table), never an
    *    author's `name=`, which could not end that way.
    * 2. Its replacement committed: this run's manifest has the same source,
    *    at the same connection or destination, under its readable name.
    * 3. This package's own records name it: an earlier committed run that
    *    `reader` reads built it for the same source and place.
    * 4. No other package has marked it as its own since (`ownedElsewhere`,
    *    checked by the drop).
    * 5. Nothing still serves it: this run is the newest `reader` serves from,
    *    and no run `reader` does not read (another version's own, an
    *    orchestrated run of another version) names it.
    *
    * A table is forgotten once it is dropped, or kept by a guard that cannot
    * come to pass (guard 3); one whose drop failed or was left in place, or
    * that a guard a later run may find passing kept, stays recorded, for the
    * next run to try. Best-effort
    * throughout. Within one server: the guards read this server's records,
    * so servers that share a schema must move to readable names together.
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
   ): Promise<void> {
      try {
         const environmentId = await this.resolveEnvironmentId(environmentName);
         const runs =
            (await this.repository.listMaterializations(
               environmentId,
               packageName,
            )) ?? [];
         const run = runs.find((m) => m.id === runId);
         if (run?.status !== "MANIFEST_FILE_READY") return;
         // Where each table was recorded, so a decided one can be forgotten.
         const recordedOn = runs.filter(
            (m) =>
               m.status === "MANIFEST_FILE_READY" &&
               (m.id === runId || readsRun(m, reader)) &&
               renamedTablesIn(m.metadata ?? null).length > 0,
         );
         if (recordedOn.length === 0) return;
         const { decided, retried, reclaimable } = this.decideRenamedTables(
            recordedOn,
            run,
            runs,
            reader,
         );
         let owed: ManifestEntry[] = [];
         const skipped: ManifestEntry[] = [];
         if (reclaimable.length > 0 && !(await ensureBound())) {
            logger.warn(
               "Keeping the tables readable names replaced: a loaded copy of the package did not rebind",
               { packageName },
            );
            return;
         }
         if (reclaimable.length > 0) {
            const environment = await this.deps.host.getEnvironment(
               parseEnvironmentName(environmentName),
               false,
            );
            // 4 is checked by the drop, table by table.
            owed = await this.deps.dropManifestEntries(
               environment,
               environmentName,
               packageName,
               runId,
               "reclaim",
               reclaimable,
               new Set(),
               await this.connectionResolverFor(
                  environment,
                  packageName,
                  reader.versionId ?? null,
               ),
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
            reclaimable.filter(
               (e) =>
                  !owedKeys.has(entryRenamedKey(e)) &&
                  !skippedKeys.has(entryRenamedKey(e)),
            ),
         );
         // Forget what was decided; keep what a drop still owes, what it left
         // in place, and what a passing guard may yet let a later run drop.
         for (const m of recordedOn) {
            const tables = renamedTablesIn(m.metadata ?? null);
            const left = tables.filter(
               (t) =>
                  !decided.has(renamedKey(t)) ||
                  owedKeys.has(renamedKey(t)) ||
                  skippedKeys.has(renamedKey(t)) ||
                  retried.has(renamedKey(t)),
            );
            if (left.length === tables.length) continue;
            const metadata = { ...(m.metadata ?? {}) };
            if (left.length > 0) metadata[RENAMED_TABLES_KEY] = left;
            else delete metadata[RENAMED_TABLES_KEY];
            await this.repository.updateMaterialization(m.id, { metadata });
         }
      } catch (err) {
         logger.warn(
            "Reclaiming tables readable names replaced did not complete",
            { error: errMessage(err) },
         );
      }
   }

   /**
    * Decide each table recorded on `recordedOn` once: kept by a guard of
    * {@link reclaimRenamedTables} (logged, and `retried` when a later run may
    * find that guard passing), else reclaimable.
    */
   private decideRenamedTables(
      recordedOn: Materialization[],
      run: Materialization,
      runs: Materialization[],
      reader: ServingReader,
   ): {
      decided: Set<string>;
      retried: Set<string>;
      reclaimable: ManifestEntry[];
   } {
      const decided = new Set<string>();
      const retried = new Set<string>();
      const reclaimable: ManifestEntry[] = [];
      for (const table of recordedOn.flatMap((m) =>
         renamedTablesIn(m.metadata ?? null),
      )) {
         const key = renamedKey(table);
         if (decided.has(key)) continue;
         const kept = this.renamedTableGuard(table, run, runs, reader);
         decided.add(key);
         // A guard that a later run may find passing keeps the record.
         if (kept && TRANSIENT_GUARDS.has(kept)) retried.add(key);
         if (kept) {
            logger.info("Keeping a table a readable name replaced", {
               physicalTableName: table.previousName,
               replacedBy: table.physicalTableName,
               guard: kept,
            });
            continue;
         }
         reclaimable.push({
            sourceEntityId: table.sourceEntityId,
            physicalTableName: table.previousName,
            connectionName: table.connectionName,
            ...(table.storageDestinationName
               ? { storageDestinationName: table.storageDestinationName }
               : {}),
         });
      }
      return { decided, retried, reclaimable };
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
    * when none does (guard 4 is the drop's).
    */
   private renamedTableGuard(
      table: RenamedTable,
      run: Materialization,
      runs: Materialization[],
      reader: ServingReader,
   ): string | undefined {
      const place = (e: {
         connectionName?: string | null;
         storageDestinationName?: string | null;
      }) => `${e.connectionName ?? ""}\u0000${e.storageDestinationName ?? ""}`;
      // 2: the replacement committed, at the same place.
      const replacement = run.manifest?.entries?.[table.sourceEntityId];
      if (
         replacement?.physicalTableName !== table.physicalTableName ||
         place(replacement) !== place(table)
      ) {
         return "replacement-not-committed";
      }
      // 5: this run is what the package serves from now.
      if (
         newestServingEntries(runs, reader)[table.sourceEntityId]
            ?.physicalTableName !== table.physicalTableName
      ) {
         return "not-serving-replacement";
      }
      // 3: an earlier run of this package built it for this source.
      const builtIt = (m: Materialization) => {
         const e = m.manifest?.entries?.[table.sourceEntityId];
         return (
            e?.physicalTableName === table.previousName &&
            place(e) === place(table)
         );
      };
      if (
         !runs.some(
            (m) =>
               m.id !== run.id &&
               m.status === "MANIFEST_FILE_READY" &&
               readsRun(m, reader) &&
               builtIt(m),
         )
      ) {
         return "not-in-package-records";
      }
      // 5: no run this run does not supersede still names it.
      if (
         runs.some(
            (m) =>
               m.status === "MANIFEST_FILE_READY" &&
               !readsRun(m, reader) &&
               Object.values(m.manifest?.entries ?? {}).some(
                  (e) =>
                     e.physicalTableName === table.previousName &&
                     place(e) === place(table),
               ),
         )
      ) {
         return "still-referenced";
      }
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
