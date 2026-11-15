// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getPersistStorageMode } from "../../../config";
import { logger } from "../../../logger";
import { recordAutoLoadOutcome } from "../../../materialization_metrics";
import { ResourceRepository } from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import { parseEnvironmentName } from "../../identity";
import { resolveEnvironmentId } from "../../resolve_environment";
import {
   newestServingEntries,
   ServingReader,
} from "../../versions/materialization_scope";
import type { MaterializationHost } from "../host";
import {
   BuildInstruction,
   BuildManifestResult,
   boundNativeSchemaOf,
   boundSchemaOf,
   FreshnessManifest,
   isLegacyFailedEntry,
   ManifestEntry,
   ManifestReference,
   SourceFailure,
} from "../domain/types";
import { keyUniquenessAt, type RollupKeyRecord } from "./inline_uniqueness";
import { fetchManifestEntries, splitManifestEntries } from "./manifest_loader";
import type { RunLifecycle } from "./run_lifecycle";

/**
 * A run's manifest, from commit to serving: it commits the manifest a build
 * assembled, binds it into the package models, rebinds every loaded slot of a
 * package from the store, and reads back the committed manifests a later run
 * reuses tables from or resolves references against.
 */
export class ManifestCommitter {
   constructor(
      private readonly deps: {
         repository: () => ResourceRepository;
         host: MaterializationHost;
         lifecycle: RunLifecycle;
      },
   ) {}

   private get repository(): ResourceRepository {
      return this.deps.repository();
   }

   /**
    * Finalize a successful build: advance to MANIFEST_ROWS_READY then
    * MANIFEST_FILE_READY and persist the assembled manifest. The caller
    * supplies the per-run metadata. The build itself happens before this.
    */
   async commit(
      id: string,
      entries: Record<string, ManifestEntry>,
      failures: Record<string, SourceFailure>,
      metadata: Record<string, unknown>,
   ): Promise<void> {
      await this.deps.lifecycle.transition(id, "MANIFEST_ROWS_READY");
      const manifest: BuildManifestResult = {
         builtAt: new Date().toISOString(),
         entries,
         // Omitted rather than {} on a clean run, matching earlier manifests.
         ...(Object.keys(failures).length > 0 ? { failures } : {}),
         strict: false,
      };
      await this.deps.lifecycle.transition(id, "MANIFEST_FILE_READY", {
         completedAt: new Date(),
         manifest,
         metadata,
      });
   }

   /**
    * Load a freshly produced manifest into the package's models so persist
    * references resolve to the materialized tables. Best-effort: a load failure
    * is logged, not fatal (the run already reached MANIFEST_FILE_READY).
    */
   async autoLoad(
      environment: {
         reloadAllModelsForPackage(
            packageName: string,
            manifest: FreshnessManifest,
            versionId?: string,
         ): Promise<void>;
         bindPackageStorageServeBindings(
            packageName: string,
            entries: Record<string, ManifestEntry>,
            versionId?: string,
         ): Promise<void>;
      },
      packageName: string,
      entries: Record<string, ManifestEntry>,
      // The version the run built; undefined for an unversioned package.
      versionId?: string,
      // The rollup key checks this run stands behind (known limitation 29).
      rollupKeys?: Readonly<Record<string, RollupKeyRecord>>,
   ): Promise<void> {
      // Bound un-gated: freshness is stamped only on the control plane's wire
      // manifest. A failed source mirrored into `entries` (the
      // `ManifestEntry.error` deprecation window) names the previous
      // generation's real table under auto-run's stable names, so binding it
      // would serve stale rows as fresh; skip it.
      const builtEntries = Object.fromEntries(
         Object.entries(entries).filter(
            ([, entry]) => !isLegacyFailedEntry(entry),
         ),
      );
      const manifestEntries: FreshnessManifest = {};
      for (const [sourceEntityId, entry] of Object.entries(builtEntries)) {
         // Only colocated entries: a storage entry here would substitute a
         // table on the source's OWN connection, where it does not exist.
         // Storage entries serve via the bindings below.
         if (entry.physicalTableName && !entry.storageDestinationName) {
            const keyUniqueness = keyUniquenessAt(
               rollupKeys,
               sourceEntityId,
               entry,
            );
            manifestEntries[sourceEntityId] = {
               tableName: entry.physicalTableName,
               // For dialect quoting at bind (Package.quoteBoundTableNames): the
               // build CREATEd it quoted, so an unquoted read misses on a
               // case-folding engine.
               connectionName: entry.connectionName,
               // The columns a colocated build captured (v1 cutover), which
               // the served copy binds a v1 table with.
               ...boundSchemaOf(entry.schema),
               ...boundNativeSchemaOf(entry.nativeSchema),
               ...(keyUniqueness ? { keyUniqueness } : {}),
            };
         }
      }
      try {
         await environment.reloadAllModelsForPackage(
            packageName,
            manifestEntries,
            versionId,
         );
         // Storage sources serve cross-connection via the virtual-source
         // transform. No-op without storage= sources.
         // A stored table's own primary key check rides on its entry, from
         // this run's records alone (the join-to-rollup leg in a destination).
         await environment.bindPackageStorageServeBindings(
            packageName,
            Object.fromEntries(
               Object.entries(builtEntries).map(([id, entry]) => {
                  const keyUniqueness = entry.storageDestinationName
                     ? keyUniquenessAt(rollupKeys, id, entry)
                     : undefined;
                  return [
                     id,
                     keyUniqueness ? { ...entry, keyUniqueness } : entry,
                  ];
               }),
            ),
            versionId,
         );
         recordAutoLoadOutcome("success");
         logger.info("Auto-run: loaded manifest into package models", {
            packageName,
            entryCount: Object.keys(manifestEntries).length,
         });
      } catch (err) {
         recordAutoLoadOutcome("failure");
         logger.warn("Auto-run: failed to load manifest into package models", {
            packageName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }

   /**
    * Entries of the most recent successful (MANIFEST_FILE_READY) materialization
    * for this package, used for skip-if-unchanged. Excludes the in-flight run.
    * `reader` says whose runs count (see newestServingEntries): a published
    * version's, or the unversioned slot's.
    */
   async mostRecentEntries(
      environmentId: string,
      packageName: string,
      excludeId: string,
      reader: ServingReader = {},
   ): Promise<Record<string, ManifestEntry>> {
      const list =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
            reader.owned !== undefined ? { version: reader.owned } : undefined,
         )) ?? [];
      // Legacy tolerance, removable with `ManifestEntry.error`: manifests
      // from 0.0.245-0.0.246 record failed sources among their entries.
      // Reusing one would never retry the source and would seed a downstream
      // from a table that was never created.
      return Object.fromEntries(
         Object.entries(newestServingEntries(list, reader, excludeId)).filter(
            ([, entry]) => !isLegacyFailedEntry(entry),
         ),
      );
   }

   /**
    * Fill the fields a thin reference cannot carry from this publisher's own
    * most recent manifest (the skip-if-unchanged cache). Caller-supplied fields
    * WIN: the orchestrator is authoritative across a stateless fleet. Mutates
    * `carried`. Best-effort: a lookup failure leaves references as supplied.
    */
   async resolveReferencesFromStore(
      carried: Record<string, ManifestEntry>,
      environmentId: string,
      packageName: string,
      excludeId: string,
      // Whose runs the references resolve against (see ServingReader).
      reader: ServingReader = {},
   ): Promise<void> {
      let cached: Record<string, ManifestEntry>;
      try {
         cached = await this.mostRecentEntries(
            environmentId,
            packageName,
            excludeId,
            reader,
         );
      } catch (err) {
         logger.warn(
            "Reference resolve-local lookup failed; using references as supplied",
            {
               packageName,
               error: err instanceof Error ? err.message : String(err),
            },
         );
         return;
      }
      for (const [sourceEntityId, ref] of Object.entries(carried)) {
         const local = cached[sourceEntityId];
         if (!local) continue;
         // Local entry is the base; caller-supplied (defined) fields override it.
         const merged: ManifestEntry = { ...local };
         for (const [key, value] of Object.entries(ref)) {
            if (value !== undefined) {
               (merged as Record<string, unknown>)[key] = value;
            }
         }
         carried[sourceEntityId] = merged;
      }
   }

   /**
    * Seed upstream reuse from the package's BOUND manifest (`manifestLocation`),
    * so a worker that never built an upstream can still reuse its table. Adds
    * bound upstreams not already carried and fills gaps in thin references; an
    * explicit `referenceManifest` field always wins. Sources this build produces
    * are skipped. Best-effort: a fetch failure is ignored. Mutates `carried`.
    */
   async seedFromBoundManifest(
      carried: Record<string, ManifestEntry>,
      pkg: { getPackageMetadata(): { manifestLocation?: string | null } },
      instructions: BuildInstruction[],
   ): Promise<void> {
      const manifestLocation = pkg.getPackageMetadata().manifestLocation;
      if (!manifestLocation) return;
      let fetched;
      try {
         fetched = await fetchManifestEntries(manifestLocation);
      } catch (err) {
         logger.warn(
            "Build upstream resolution: bound manifest fetch failed; ignoring",
            {
               manifestLocation,
               error: err instanceof Error ? err.message : String(err),
            },
         );
         return;
      }
      // Storage entries carry their full shape; colocated ones are rebuilt from
      // the tableName manifest.
      const bound: Record<string, ManifestEntry> = {
         ...fetched.storageEntries,
      };
      for (const [eid, e] of Object.entries(fetched.tableNameManifest)) {
         if (!bound[eid]) {
            bound[eid] = {
               sourceEntityId: eid,
               physicalTableName: e.tableName,
               connectionName: e.connectionName,
            };
         }
      }
      const building = new Set(instructions.map((i) => i.sourceEntityId));
      for (const [eid, entry] of Object.entries(bound)) {
         if (building.has(eid)) continue;
         const existing = carried[eid];
         if (!existing) {
            carried[eid] = entry;
            continue;
         }
         // Explicit reference wins; the bound entry fills the gaps it left.
         const merged: ManifestEntry = { ...entry };
         for (const [key, value] of Object.entries(existing)) {
            if (value !== undefined) {
               (merged as Record<string, unknown>)[key] = value;
            }
         }
         carried[eid] = merged;
      }
   }

   /**
    * Re-derive serve routing from the store for every loaded slot of a
    * package: its unversioned slot, or each loaded version. A version reads
    * the runs that own its tables (its own under `scope: version`, the
    * package's otherwise; see ownedVersionOf), and one bound to a host
    * manifest is left alone, as on load. `exceptVersion` skips the version a
    * caller has just bound itself. Best-effort per slot.
    *
    * Colocated is rebound regardless of `PERSIST_STORAGE_MODE` (it is not gated
    * by the storage kill switch); storage= only when the tier is not `off`.
    */
   async rebindLoadedVersions(
      environmentName: string,
      packageName: string,
      exceptVersion?: string,
   ): Promise<void> {
      let environment: Awaited<
         ReturnType<MaterializationHost["getEnvironment"]>
      >;
      let environmentId: string;
      let slots: (string | undefined)[];
      try {
         environmentId = await resolveEnvironmentId(
            this.repository,
            environmentName,
         );
         environment = await this.deps.host.getEnvironment(
            parseEnvironmentName(environmentName),
            false,
         );
         const loaded = environment.getLoadedVersionIds(packageName);
         const versions = environment.getVersionService();
         slots =
            loaded.length > 0
               ? loaded
               : versions && (await versions.isVersioned(packageName))
                 ? [] // A versioned package with nothing loaded: nothing to rebind.
                 : [undefined];
      } catch (err) {
         logger.warn("Failed to rebind serve bindings", {
            packageName,
            error: errMessage(err),
         });
         return;
      }
      for (const versionId of slots) {
         if (versionId !== undefined && versionId === exceptVersion) continue;
         if (versionId !== undefined) {
            // Read and bound under the version's lock; a version that cannot
            // be rebound is unloaded, and binds from the store when next read.
            await environment.rebindVersionFromStore(
               packageName,
               versionId,
               (reader) =>
                  // "" excludes nothing — a deleted record is already gone.
                  this.mostRecentEntries(
                     environmentId,
                     packageName,
                     "",
                     reader,
                  ),
            );
            continue;
         }
         try {
            // "" excludes nothing — a deleted record is already gone.
            const entries = await this.mostRecentEntries(
               environmentId,
               packageName,
               "",
            );
            const { tableNameManifest, storageEntries } = splitManifestEntries(
               entries,
               `rebind (package ${packageName})`,
            );
            // Colocated: re-derive (or clear) regardless of mode.
            await environment.bindPackageColocatedServeManifest(
               packageName,
               tableNameManifest,
               versionId,
            );
            // Storage=: only meaningful when the tier is not off.
            if (getPersistStorageMode() !== "off") {
               await environment.bindPackageStorageServeBindings(
                  packageName,
                  storageEntries,
                  versionId,
               );
            }
         } catch (err) {
            logger.warn(
               "Failed to rebind serve bindings (leaving current bindings; a " +
                  "reload/build will re-derive)",
               { packageName, versionId, error: errMessage(err) },
            );
         }
      }
   }
}

/**
 * Project the caller's upstream reference manifest into the seed entry map
 * the instructed build consumes, keyed by sourceEntityId. The optional
 * `connectionName` lets the seed be dialect-quoted (see quoteSeedTablePath);
 * without it the seed is unquoted.
 */
export function referenceManifestToEntries(
   referenceManifest: ManifestReference[] | undefined,
): Record<string, ManifestEntry> {
   const entries: Record<string, ManifestEntry> = {};
   for (const ref of referenceManifest ?? []) {
      entries[ref.sourceEntityId] = {
         sourceEntityId: ref.sourceEntityId,
         physicalTableName: ref.physicalTableName,
         connectionName: ref.connectionName,
         ...(ref.storageDestinationName
            ? { storageDestinationName: ref.storageDestinationName }
            : {}),
      };
   }
   return entries;
}
