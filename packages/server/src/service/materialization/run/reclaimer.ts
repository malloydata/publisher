// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { logger } from "../../../logger";
import { recordDropTables } from "../../../materialization_metrics";
import {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import { parseEnvironmentName, parsePackageName } from "../../identity";
import type { ApiConnection } from "../../model";
import { mergeQueryMetadata, type QueryMetadata } from "../../query_metadata";
import { quoteTablePath } from "../../quoting";
import { resolveEnvironmentId } from "../../resolve_environment";
import {
   isVersionOwnedRun,
   writeTargetsIn,
} from "../../versions/materialization_scope";
import type { MaterializationEnvironment, MaterializationHost } from "../host";
import { ManifestEntry } from "../types";
import { dropStorageTable } from "./materialization_build_session";
import {
   backupSuffix,
   BuildEnvironment,
   connectionMetadataLayers,
   redactConnectionSecrets,
   stagingSuffix,
   tableKeyOf,
} from "./materialization_run_support";
import {
   atNameViewSite,
   heldByOwnNameView,
   nameViewsIn,
   nameViewSuccessors,
   settleNameViewsBeforeDrop,
   type NameViewRecord,
} from "./name_view";
import { nameViewOwner } from "./name_view_sql";
import { ownedElsewhere } from "./table_ownership";
import type { RunLifecycle } from "./run_lifecycle";

/**
 * The `name=` views a sweep must settle before it drops their tables: those
 * the runs being dropped recorded, and where each could point instead.
 */
interface NameViewsToSettle {
   recorded: NameViewRecord[];
   successorFor: (view: NameViewRecord) => NameViewRecord | undefined;
}

/** A physical table's key for drop bookkeeping: where it lives, and its name. */
function tableKey(destination: string, physicalTableName: string): string {
   return `${destination}:${physicalTableName}`;
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
          * How a sweep drops one run's entries. The facade routes this back
          * through its own `dropManifestEntries` so its specs can substitute
          * a failing drop.
          */
         dropManifestEntries: DropManifestEntries;
         /**
          * How a deleted version's cleanup and the startup sweep reclaim one
          * version: back through the facade's public `reclaimVersionTables`,
          * which its specs observe.
          */
         reclaimVersionTables: Reclaimer["reclaimVersionTables"];
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
         const tableKey = (dest: string, table: string) => `${dest}:${table}`;
         const stillReferenced = new Set<string>();
         const others =
            (await this.repository.listMaterializations(
               owner.environmentId,
               owner.packageName,
            )) ?? [];
         for (const other of others) {
            if (other.status !== "MANIFEST_FILE_READY") continue;
            for (const e of Object.values(other.manifest?.entries ?? {})) {
               const dest = e.storageDestinationName ?? e.connectionName;
               if (dest && e.physicalTableName) {
                  stillReferenced.add(tableKey(dest, e.physicalTableName));
               }
            }
         }

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
            const destinationConnection = destinationForCleanup(
               environment,
               dest,
            );
            if (!destinationConnection) continue;
            try {
               await dropStorageTable({
                  destinationName: dest,
                  destinationConnection,
                  physicalTableName: table,
                  environmentPath: environment.getEnvironmentPath(),
               });
               recordDropTables("success", "storage");
               logger.info("Reclaimed a table stranded by a failed build", {
                  destinationName: dest,
                  physicalTableName: table,
               });
            } catch (dropErr) {
               recordDropTables("failure", "storage");
               logger.warn(
                  "Failed to reclaim a table stranded by a failed build",
                  {
                     destinationName: dest,
                     physicalTableName: table,
                     error: redactConnectionSecrets(
                        errMessage(dropErr),
                        destinationConnection,
                     ),
                  },
               );
            }
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
      const stillReferenced = await this.tablesStillReferenced(
         environmentId,
         packageName,
         (other) => other.id !== m.id,
      );
      await this.deps.dropManifestEntries(
         environment,
         environmentName,
         packageName,
         m.id,
         Object.values(entries),
         stillReferenced,
         await this.connectionResolverFor(environment, packageName, m.version),
         await this.nameViewsToSettle(
            environmentId,
            packageName,
            [m],
            (other) => other.id !== m.id,
         ),
      );
   }

   /**
    * The `name=` views `runs` recorded, and for each the newest table another
    * committed run (among those `include` admits) points the same view at;
    * undefined when the runs recorded none.
    */
   private async nameViewsToSettle(
      environmentId: string,
      packageName: string,
      runs: Materialization[],
      include: (other: Materialization) => boolean,
   ): Promise<NameViewsToSettle | undefined> {
      const recorded = runs.flatMap((m) => nameViewsIn(m.metadata));
      if (recorded.length === 0) return undefined;
      const others = (
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? []
      ).filter((o) => include(o) && o.status === "MANIFEST_FILE_READY");
      return { recorded, successorFor: nameViewSuccessors(others) };
   }

   /**
    * The destination-and-name keys of every table a MANIFEST_FILE_READY run of
    * the package references, among the runs `include` admits.
    */
   private async tablesStillReferenced(
      environmentId: string,
      packageName: string,
      include: (other: Materialization) => boolean,
   ): Promise<Set<string>> {
      const stillReferenced = new Set<string>();
      const others =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
      for (const other of others) {
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
      entries: ManifestEntry[],
      stillReferenced: Set<string>,
      connectionFor: (name: string) => Promise<MalloyConnection | undefined>,
      // The `name=` views to settle before their tables go (name_view.ts).
      nameViews?: NameViewsToSettle,
   ): Promise<ManifestEntry[]> {
      const m = { id: materializationId };
      const connectionCache = new Map<string, MalloyConnection>();
      const owed: ManifestEntry[] = [];
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
               m.id,
            ),
      };
      // A table's DROP that fails because this package's own `name=` view
      // holds its name: a later build replaced the table with the view
      // (name_view.ts), so the table is gone and its entry is retired, not
      // owed. Probed only after a failed drop.
      const owner = nameViewOwner(environmentName, packageName);
      const replacedByOwnView = (entry: ManifestEntry) =>
         heldByOwnNameView(
            {
               viewName: entry.physicalTableName,
               owner,
               connectionName: entry.connectionName ?? "",
               ...(entry.storageDestinationName
                  ? { storageDestinationName: entry.storageDestinationName }
                  : {}),
            },
            viewContext,
         );
      for (const entry of entries) {
         const connectionName = entry.connectionName;
         const physicalTableName = entry.physicalTableName;
         if (!connectionName || !physicalTableName) {
            logger.warn("Skipping manifest entry with no connection/table", {
               materializationId: m.id,
               sourceEntityId: entry.sourceEntityId,
            });
            continue;
         }

         const destForKey = entry.storageDestinationName ?? connectionName;
         if (stillReferenced.has(tableKey(destForKey, physicalTableName))) {
            logger.info(
               "Skipping drop: table still referenced by another materialization",
               {
                  materializationId: m.id,
                  physicalTableName,
                  destination: destForKey,
               },
            );
            continue;
         }

         // A table another package has marked as its own since this run
         // built it (it holds that package's `name=` now) is no longer this
         // package's to drop. Retired, not owed (table_ownership.ts).
         const markedBy = await atNameViewSite(
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
                  materializationId: m.id,
                  physicalTableName,
                  destination: destForKey,
                  markedBy,
               },
            );
            continue;
         }

         // Before the table: a Postgres view refuses its table's drop, and a
         // view left reading a dropped table answers nothing.
         if (nameViews) {
            await settleNameViewsBeforeDrop(
               entry,
               nameViews.recorded,
               nameViews.successorFor,
               viewContext,
            );
         }

         // A storage= table lives in the destination, not the source warehouse,
         // and needs a READ-WRITE attach (the serve attach is read-only). A
         // failure is logged and the sweep continues.
         if (entry.storageDestinationName) {
            const destinationConnection = destinationForCleanup(
               environment,
               entry.storageDestinationName,
            );
            if (!destinationConnection) {
               owed.push(entry);
               continue;
            }
            try {
               try {
                  await dropStorageTable({
                     destinationName: entry.storageDestinationName,
                     destinationConnection,
                     physicalTableName,
                     environmentPath: environment.getEnvironmentPath(),
                     // Where a `name=` view's replacement keeps the table
                     // until its run commits (name_view.ts), as a crash or
                     // a failed restore may leave it.
                     alsoDrop: [
                        `${physicalTableName}${backupSuffix(entry.sourceEntityId)}`,
                     ],
                  });
               } catch (err) {
                  if (!(await replacedByOwnView(entry))) throw err;
                  logRetiredByView(m.id, physicalTableName);
               }
               recordDropTables("success", "storage");
               logger.info("Dropped materialized storage table on delete", {
                  materializationId: m.id,
                  physicalTableName,
                  storageDestinationName: entry.storageDestinationName,
               });
            } catch (err) {
               owed.push(entry);
               recordDropTables("failure", "storage");
               logger.warn("Failed to drop a storage-materialized table", {
                  materializationId: m.id,
                  physicalTableName,
                  storageDestinationName: entry.storageDestinationName,
                  // The drop attaches read-write, so a failure can echo the
                  // catalog connstring.
                  error: redactConnectionSecrets(
                     errMessage(err),
                     destinationConnection,
                  ),
               });
            }
            continue;
         }

         try {
            const connection = await cachedConnection(connectionName);
            if (!connection) {
               logger.info(
                  "Skipping drop: the table lived in an unloaded version's in-memory duckdb",
                  { materializationId: m.id, physicalTableName },
               );
               continue;
            }
            // Quoted as buildOneSource quoted it, so what built also drops.
            const dialect = connection.dialectName;
            // Tagged for attribution like a build, but as `ops`.
            const dropOptions = dropRunSQLOptions(
               environment,
               connectionName,
               environmentName,
               packageName,
               m.id,
            );
            try {
               await connection.runSQL(
                  `DROP TABLE IF EXISTS ${quoteTablePath(
                     physicalTableName,
                     dialect,
                  )}`,
                  dropOptions,
               );
            } catch (err) {
               if (!(await replacedByOwnView(entry))) throw err;
               logRetiredByView(m.id, physicalTableName);
            }
            // A crash between staging-create and rename can leave this behind.
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoteTablePath(
                  `${physicalTableName}${stagingSuffix(entry.sourceEntityId)}`,
                  dialect,
               )}`,
               dropOptions,
            );
            // And a crash between the swap's two renames, the previous copy.
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoteTablePath(
                  `${physicalTableName}${backupSuffix(entry.sourceEntityId)}`,
                  dialect,
               )}`,
               dropOptions,
            );
            recordDropTables("success", "in_warehouse");
            logger.info("Dropped materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
            });
         } catch (err) {
            owed.push(entry);
            recordDropTables("failure", "in_warehouse");
            logger.warn("Failed to drop materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
               error: errMessage(err),
            });
         }
      }
      return owed;
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
      await this.deps.reclaimVersionTables(
         environmentName,
         packageName,
         versionId,
      );
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
         if (
            m.metadata?.mode !== "auto" ||
            (m.status !== "PENDING" && m.status !== "MANIFEST_ROWS_READY")
         ) {
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
      const runs = (
         await this.repository.listMaterializations(
            environmentId,
            packageName,
            {
               version: versionId,
            },
         )
      ).filter(
         (m) =>
            isVersionOwnedRun(m) &&
            m.metadata?.mode === "auto" &&
            m.status !== "PENDING" &&
            m.status !== "MANIFEST_ROWS_READY",
      );
      if (runs.length === 0) return;
      const reclaimed = new Set(runs.map((m) => m.id));
      const stillReferenced = await this.tablesStillReferenced(
         environmentId,
         packageName,
         (other) => !reclaimed.has(other.id),
      );
      // Never through getPackage: the version is out of service, and its
      // package-local duckdb went with its unload.
      const connectionFor = await this.connectionResolverFor(
         environment,
         packageName,
         versionId,
      );
      const nameViews = await this.nameViewsToSettle(
         environmentId,
         packageName,
         runs,
         (other) => !reclaimed.has(other.id),
      );
      for (const m of runs) {
         const owed = await this.deps.dropManifestEntries(
            environment,
            environmentName,
            packageName,
            m.id,
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
                  materializationId: m.id,
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
               await this.deps.reclaimVersionTables(
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
      "Skipped drop: a name= view this package made replaced the table, which is gone",
      { materializationId, physicalTableName },
   );
}
