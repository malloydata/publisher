// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A run manifest, from commit to serving ({@link ManifestCommitter}): it
 * records the manifest a build assembled on the run, auto-loads it into the
 * package's models, rebinds the package's loaded versions from the store, and
 * reads back the committed manifests a later run reuses tables from or
 * resolves references against. Also {@link referenceManifestToEntries}, a
 * host's reference manifest as entries. Called by the materialization service.
 *
 * Effects: writes the run store, reloads and rebinds package models, counts
 * each auto-load's outcome once. A load that fails or times out does not fail
 * the run; the run's record says what is served.
 */

import { errorCodeOf } from "../../../error_code";
import { storageWritable } from "../domain/placement";
import { redactStorageUrls } from "../../../pg_helpers";
import { boundNativeSchemaOf, boundSchemaOf } from "../stored_column_types";
import { getPersistStorageMode } from "../../../config";
import { logger } from "../../../logger";
import { recordAutoLoadOutcome } from "../../../materialization_metrics";
import { ResourceRepository } from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import { parseEnvironmentName } from "../../identity";
import { resolveEnvironmentId } from "../../resolve_environment";
import {
   ServingReader,
   storageBindingResolverFor,
   storedServingEntries,
   SUPERSEDED_TABLES_KEY,
} from "../../versions/materialization_scope";
import type { MaterializationEnvironment, MaterializationHost } from "../host";
import {
   BuildInstruction,
   BuildManifestResult,
   FreshnessManifest,
   isLegacyFailedEntry,
   ManifestEntry,
   ManifestReference,
   SourceFailure,
} from "../domain/types";
import {
   keyUniquenessAt,
   withRollupKeyRecords,
   type RollupKeyRecord,
} from "./inline_uniqueness";
import { fetchManifestEntries, splitManifestEntries } from "./manifest_loader";
import { type RunLifecycle } from "./run_lifecycle";

/**
 * What a run records when it reaches MANIFEST_FILE_READY about its manifest
 * load: metadata merged into the run's (`autoLoad`, and on a failure
 * `autoLoadStep` and `autoLoadError`), and on a failure the record's `error`.
 */
interface Served {
   metadata: Record<string, unknown>;
   error?: string;
}

/** Nothing to report: an orchestrated run, which loads nothing. */
const NOT_SERVED: Served = { metadata: {} };

/**
 * How long an auto-run waits for its manifest load before it settles as
 * MANIFEST_FILE_READY with `autoLoad: "failed"`, so a hung load cannot hold
 * the package's build slot until a restart.
 */
const AUTO_LOAD_TIMEOUT_MS = 10 * 60 * 1000;

/** What each failed step leaves serving, for the run's `error`. */
const NOT_SERVED_CONSEQUENCE: Record<string, string> = {
   reload:
      "the package's models were not recompiled with this run's tables until its next reload or restart (or, for a published version, a rebind from the store) binds this run's manifest",
   bind: "the run's database tables are served and its stored ones are not, until the package's next reload or restart binds this run's manifest",
   timeout:
      "what the package serves is undecided until the load finishes, or its next reload or restart binds this run's manifest; a hung reload holds the package's lock until it returns, so the package's reloads, publishes and PATCHes wait (queries do not)",
};

/**
 * Whether a run may rebind its own version from the store once it is
 * MANIFEST_FILE_READY, given what its load recorded: not after a load that
 * timed out, which may still hold the version's lock, so the rebind would wait
 * on it and the run would never settle.
 */
export function rebindsOwnVersionAfter(
   served: Record<string, unknown> | undefined,
): boolean {
   return served?.autoLoadStep !== "timeout";
}

/** Count a run's load outcome once, when it is first decided. */
function recordServed(served: Served): void {
   if (served.metadata.autoLoad === "loaded") recordAutoLoadOutcome("success");
   else if (served.metadata.autoLoad === "failed") {
      recordAutoLoadOutcome("failure");
   }
}

/**
 * A load that did not take: the step, its error, and the record's `error`.
 * Exported for its spec.
 */
export function notServed(step: string, loadError: string): Served {
   // Read back by callers: no storage path in it.
   const error = redactStorageUrls(loadError);
   return {
      metadata: {
         autoLoad: "failed",
         autoLoadStep: step,
         autoLoadError: error,
      },
      error: `Built, but not served (${step}): ${error}; ${NOT_SERVED_CONSEQUENCE[step] ?? ""}`.replace(
         /; $/,
         "",
      ),
   };
}

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
         /**
          * The package's table lock (`TableClaims.withTableLock`), under
          * which another run marks this one's entries superseded: the final
          * write re-reads and keeps those marks under it. Absent in tests
          * that drive one run.
          */
         withTableLock?: (
            environmentId: string,
            packageName: string,
            fn: () => Promise<void>,
         ) => Promise<void>;
      },
   ) {}

   private get repository(): ResourceRepository {
      return this.deps.repository();
   }

   /**
    * Finalize a successful build: record the assembled manifest on the move to
    * MANIFEST_ROWS_READY, run `serve` (an auto-run's {@link autoLoad}), and
    * advance to MANIFEST_FILE_READY when it calls `finish`, with what it
    * reports merged into the metadata. `serve` calls `finish` under the
    * package's lock, after the load, so a caller that waits for
    * MANIFEST_FILE_READY and then queries is answered by the package this
    * run's manifest is loaded into, and no reload can bind an older manifest
    * in between. The caller supplies the per-run metadata; the build itself
    * happens before this.
    *
    * The readers that serve or reuse a run (the serving fold, boot restore,
    * skip-if-unchanged, the reclaimer's references) count MANIFEST_FILE_READY
    * runs only, so the manifest recorded at MANIFEST_ROWS_READY is not served
    * or reused until the final move. Those that mark a run's entries stale
    * (markSuperseded, markTablesGone) read every run with a manifest, and the
    * shared-table check counts a run that is loading its manifest. The record's metadata is left as it is until then, so its table
    * claims hold through the load; the final move keeps any superseded-table
    * mark another run left on it meanwhile. A run interrupted in between (a
    * crash) is settled FAILED when the store next opens, as one interrupted
    * while PENDING is (`failInterrupted`).
    *
    * A `serve` that throws, or does not call `finish` within
    * `serveTimeoutMs`, settles the run with `autoLoad: "failed"`, so a hung
    * load never holds the package's build slot.
    *
    * A stop that lands during `serve` does not cut it short (the caller marks
    * the run `stopIgnored`): the package may already serve the new tables.
    */
   async commit(
      id: string,
      entries: Record<string, ManifestEntry>,
      failures: Record<string, SourceFailure>,
      metadata: Record<string, unknown>,
      serve?: (finish: (served: Served) => Promise<void>) => Promise<void>,
      serveTimeoutMs: number = AUTO_LOAD_TIMEOUT_MS,
   ): Promise<Record<string, unknown>> {
      const manifest: BuildManifestResult = {
         builtAt: new Date().toISOString(),
         entries,
         // Omitted rather than {} on a clean run, matching earlier manifests.
         ...(Object.keys(failures).length > 0 ? { failures } : {}),
         strict: false,
      };
      const rowsReady = await this.deps.lifecycle.transition(
         id,
         "MANIFEST_ROWS_READY",
         { manifest },
      );
      // The first call writes; every later one (a late finish after a timeout,
      // the catch below, the trailing call) awaits that same write, so a write
      // that throws is rethrown to each of them and the run settles FAILED.
      let finishing: Promise<void> | undefined;
      let final: Served = NOT_SERVED;
      const finish = (served: Served = NOT_SERVED): Promise<void> => {
         if (finishing === undefined) {
            final = served;
            recordServed(served);
            finishing = finishOnce(served);
         }
         return finishing;
      };
      const finishOnce = async (served: Served): Promise<void> => {
         // Keep the superseded-table marks another run left on this record
         // while it was MANIFEST_ROWS_READY (markSuperseded, markHeldElsewhere),
         // read and written under the same table lock they are made under.
         // The one read of the record per finish, and under the lock: the
         // marks may have changed since MANIFEST_ROWS_READY.
         const write = async () => {
            const current = await this.repository.getMaterializationById(id);
            const superseded = current?.metadata?.[SUPERSEDED_TABLES_KEY];
            await this.deps.lifecycle.transition(id, "MANIFEST_FILE_READY", {
               completedAt: new Date(),
               manifest,
               metadata: {
                  ...metadata,
                  ...(superseded !== undefined
                     ? { [SUPERSEDED_TABLES_KEY]: superseded }
                     : {}),
                  ...served.metadata,
               },
               ...(served.error !== undefined ? { error: served.error } : {}),
            });
         };
         if (this.deps.withTableLock && rowsReady) {
            await this.deps.withTableLock(
               rowsReady.environmentId,
               rowsReady.packageName,
               write,
            );
         } else {
            await write();
         }
      };
      if (serve) {
         let timer: ReturnType<typeof setTimeout> | undefined;
         const timedOut = new Promise<"timeout">((resolve) => {
            timer = setTimeout(() => resolve("timeout"), serveTimeoutMs);
         });
         try {
            const settled = await Promise.race([
               serve(finish).then(() => "served" as const),
               timedOut,
            ]);
            if (settled === "timeout") {
               await finish(
                  notServed(
                     "timeout",
                     `the manifest load did not finish within ${Math.round(serveTimeoutMs / 1000)}s`,
                  ),
               );
            }
         } catch (err) {
            // A load that threw before it finished; or the final write, which
            // this rethrows.
            await finish(notServed("reload", errMessage(err)));
         } finally {
            clearTimeout(timer);
         }
      }
      await finish();
      return final.metadata;
   }

   /**
    * Load a freshly produced manifest into the package's models so persist
    * references resolve to the materialized tables, and say how it went, for
    * the run's metadata: `autoLoad: "loaded"`, or `autoLoad: "failed"` with
    * `autoLoadError`. Run before the run reaches MANIFEST_FILE_READY
    * ({@link commit}). A failure does not fail the run, whose tables were
    * built: the package keeps serving what it served before, and the run
    * says so.
    */
   async autoLoad(
      environment: Pick<MaterializationEnvironment, "loadRunManifest">,
      packageName: string,
      entries: Record<string, ManifestEntry>,
      // The version the run built; undefined for an unversioned package.
      versionId?: string,
      // The rollup key checks this run stands behind.
      rollupKeys?: Readonly<Record<string, RollupKeyRecord>>,
      // Records the run done, with what this reports; called under the
      // package's lock, after the load (see commit).
      finish: (served: Served) => Promise<void> = async () => {},
   ): Promise<void> {
      // Bound with the freshness facts the run stamped from its tables'
      // declarations (freshness_stamp.ts), as a host's manifest binds with
      // the ones it stamps. A failed source mirrored into `entries` (the
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
         // Only database entries: a storage entry here would substitute a
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
               // The columns a database build captured (v1 cutover), which
               // the served copy binds a v1 table with.
               ...boundSchemaOf(entry.schema),
               ...boundNativeSchemaOf(entry.nativeSchema),
               ...(keyUniqueness ? { keyUniqueness } : {}),
               ...freshnessFactsOf(entry),
            };
         }
      }
      // A stored table's own primary key check rides on its entry, from this
      // run's records alone (the join-to-rollup branch in a destination).
      const storageEntries = Object.fromEntries(
         Object.entries(builtEntries).map(([id, entry]) => {
            const keyUniqueness = entry.storageDestinationName
               ? keyUniquenessAt(rollupKeys, id, entry)
               : undefined;
            return [id, keyUniqueness ? { ...entry, keyUniqueness } : entry];
         }),
      );
      await environment.loadRunManifest({
         packageName,
         versionId,
         database: manifestEntries,
         storage: storageEntries,
         commit: async (outcome) => {
            if (outcome.loaded) {
               logger.info("Auto-run: loaded manifest into package models", {
                  packageName,
                  entryCount: Object.keys(manifestEntries).length,
               });
               await finish({ metadata: { autoLoad: "loaded" } });
               return;
            }
            logger.warn(
               "Auto-run: failed to load manifest into package models",
               {
                  packageName,
                  step: outcome.step,
                  // The message is on the run's record (notServed).
               },
            );
            await finish(notServed(outcome.step, outcome.error));
         },
      });
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
      return storedServingEntries(list, reader, excludeId);
   }

   /**
    * The entries a rebind binds, read as a package load reads them
    * (`storageBindingResolverFor`): each with the key checks the newest run
    * stands behind (`withRollupKeyRecords`). Without them a rebind after a
    * run would drop what the run's own load bound, and the served copy would
    * read every recorded key as unknown.
    */
   private boundEntries(
      environmentId: string,
      packageName: string,
      reader: ServingReader = {},
   ): Promise<Record<string, ManifestEntry>> {
      // Read fresh each time, under the slot's lock the caller holds: what a
      // rebind binds is the store as of the bind, not as of the run's start.
      return storageBindingResolverFor(
         this.repository,
         environmentId,
         withRollupKeyRecords,
      )(packageName, reader);
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
               code: errorCodeOf(err),
            },
         );
         return;
      }
      for (const [sourceEntityId, ref] of Object.entries(carried)) {
         const local = cached[sourceEntityId];
         if (!local) continue;
         // Local entry is the base; caller-supplied (defined) fields override it.
         // Its freshness facts are left out: a standalone run stamped them,
         // and on a host's run the host's facts are the only ones that hold.
         const {
            dataAsOf: _dataAsOf,
            freshnessWindowSeconds: _window,
            freshnessFallback: _fallback,
            ...unstamped
         } = local;
         const merged: ManifestEntry = { ...unstamped };
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
               code: errorCodeOf(err),
            },
         );
         return;
      }
      // Storage entries carry their full shape; database ones are rebuilt from
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
    * Rebind an auto-run's own version from the store when what it bound has
    * moved: a version first loaded while the run's manifest was loading read
    * the store before the run was MANIFEST_FILE_READY, so it bound the run
    * before. Called once the run is MANIFEST_FILE_READY. Best-effort, as
    * {@link rebindLoadedVersions} is.
    */
   async rebindOwnVersionIfMoved(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<void> {
      try {
         const environmentId = await resolveEnvironmentId(
            this.repository,
            environmentName,
         );
         const environment = await this.deps.host.getEnvironment(
            parseEnvironmentName(environmentName),
            false,
         );
         await environment.rebindVersionFromStore(
            packageName,
            versionId,
            (reader) => this.boundEntries(environmentId, packageName, reader),
            { onlyIfMoved: true },
         );
      } catch (err) {
         logger.warn("Failed to rebind a run's own version", {
            packageName,
            versionId,
            code: errorCodeOf(err),
         });
      }
   }

   /**
    * Re-derive serve routing from the store for every loaded slot of a
    * package: its unversioned slot, or each loaded version. A version reads
    * the runs that own its tables (its own under `scope: version`, the
    * package's otherwise; see ownedVersionOf), and one bound to a host
    * manifest is left alone, as on load. `exceptVersion` skips the version a
    * caller has just bound itself. Best-effort per slot: answers whether
    * every slot rebound (or was taken out of service), so a caller can tell
    * that nothing loaded still reads what the store no longer names.
    *
    * Database is rebound regardless of `PERSIST_STORAGE_MODE` (it is not gated
    * by the storage kill switch); storage= only when the tier is not `off`.
    */
   async rebindLoadedVersions(
      environmentName: string,
      packageName: string,
      exceptVersion?: string,
   ): Promise<boolean> {
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
            code: errorCodeOf(err),
         });
         return false;
      }
      // Whether every slot now reads the store: a version that cannot be
      // rebound is taken out of service, which reads nothing old either.
      let rebound = true;
      for (const versionId of slots) {
         if (versionId !== undefined && versionId === exceptVersion) continue;
         if (versionId !== undefined) {
            // Read and bound under the version's lock; a version that cannot
            // be rebound is unloaded, and binds from the store when next read.
            await environment.rebindVersionFromStore(
               packageName,
               versionId,
               (reader) =>
                  this.boundEntries(environmentId, packageName, reader),
            );
            continue;
         }
         try {
            // Read and bound under the package's lock, which an unversioned
            // run is created under too: a run created before the read is
            // active, so the read binds no key check of a table it may be
            // rewriting (uncontestedAutoRun); one created after the bind
            // forgets what was bound before it writes.
            await environment.rebindPackageFromStore(packageName, async () => {
               const { tableNameManifest, storageEntries } =
                  splitManifestEntries(
                     await this.boundEntries(environmentId, packageName),
                     `rebind (package ${packageName})`,
                     "store",
                  );
               return {
                  // Database: re-derive (or clear) regardless of mode.
                  database: tableNameManifest,
                  // Storage=: only meaningful when the tier is not off.
                  ...(storageWritable(getPersistStorageMode())
                     ? { storage: storageEntries }
                     : {}),
               };
            });
         } catch (err) {
            rebound = false;
            logger.warn(
               "Failed to rebind serve bindings (leaving current bindings; a " +
                  "reload/build will re-derive)",
               { packageName, versionId, code: errorCodeOf(err) },
            );
         }
      }
      return rebound;
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

/** An entry's freshness facts, each only when set (manifest_loader.ts reads the same). */
function freshnessFactsOf(
   entry: ManifestEntry,
): Pick<
   FreshnessManifest[string],
   "dataAsOf" | "freshnessWindowSeconds" | "freshnessFallback"
> {
   return {
      ...(entry.dataAsOf !== undefined ? { dataAsOf: entry.dataAsOf } : {}),
      ...(entry.freshnessWindowSeconds !== undefined
         ? { freshnessWindowSeconds: entry.freshnessWindowSeconds }
         : {}),
      ...(entry.freshnessFallback !== undefined
         ? { freshnessFallback: entry.freshnessFallback }
         : {}),
   };
}
