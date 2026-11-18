// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the materialization module needs from the rest of the server, stated as
 * interfaces it owns. `EnvironmentStore`, `Environment` and `Package` satisfy
 * them structurally, so the module depends on these shapes rather than on the
 * concrete classes, and a spec can hand it a fake of exactly this surface.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import type {
   ResourceRepository,
   Version,
} from "../../storage/DatabaseInterface";
import type { EnvironmentName, PackageName, VersionRef } from "../identity";
import type { ApiConnection } from "../model";
import type { ServingReader } from "../versions/materialization_scope";
import type { BuildPlanPackage } from "./domain/plan_input";
import type { ChainBuildRequest, ChainBuildText } from "./domain/chain_build";
import type { KeptTable, LateRows } from "./domain/types";
import type {
   BuildPlan,
   FreshnessManifest,
   ManifestEntry,
   RunManifestLoad,
} from "./domain/types";

/** The package surface a run reads: its plan, its metadata, its connections. */
interface MaterializationPackage extends BuildPlanPackage {
   getPackageName(): string;
   /** The published version this package is; undefined for an unversioned one. */
   getVersionId(): string | undefined;
   getBuildPlan(): BuildPlan | null;
   /**
    * Resolves once the plan reflects the current connections (a connection
    * edit re-derives it).
    */
   whenBuildPlanCurrent(): Promise<void>;
   /**
    * Whether the plan could not be derived (at load, or re-derived after a
    * connection edit).
    */
   isBuildPlanUnavailable(): boolean;
   getPackageMetadata(): {
      scope?: string | null;
      manifestLocation?: string | null;
      materialization?: { schedule?: string | null } | null;
   };
   /** The publish-gate rejections the scheduler honours before it arms a cron. */
   persistencePolicyWarnings(): string[];
   /**
    * Drop the bound rollup key checks (known limitation 29) of the tables at
    * these content addresses, which a run is about to write. Required, so a
    * package that stops implementing it fails to compile rather than
    * silently serving a check over rows the run rewrote.
    */
   forgetKeyUniqueness(sourceEntityIds: Iterable<string>): void;
   /**
    * The text a chained `storage=` build compiles on its destination alone:
    * the request's downstream, with each stored upstream the request carries
    * re-rooted as the served copy re-roots it, from those entries alone and
    * never the package's bound tables.
    */
   chainBuildText(request: ChainBuildRequest): ChainBuildText;
   /**
    * The tables the newest auto-run of this package lists as kept after a
    * rename, which its warnings name (`table-kept`).
    */
   setKeptTables?(tables: readonly KeptTable[]): void;
   /**
    * The `lateRows` the package's bound manifest entries report, by content
    * address, which an incremental delta carries until a rebuild.
    */
   boundLateRows?(): ReadonlyMap<string, LateRows>;
   /**
    * The physical names of the database tables this copy is bound to now,
    * as the bind wrote them (possibly quoted): a caller's drop refuses any of
    * them (Reclaimer.dropConnectionTable).
    */
   boundTableNames?(): string[];
}

/** The part of an environment's version registry a run consults. */
interface MaterializationVersions {
   resolve(
      packageName: string,
      rawVersionId?: unknown,
   ): Promise<{ versionId: string } | null>;
   latestOf(packageName: string): Promise<string | null>;
   isVersioned(packageName: string): Promise<boolean>;
   withVersionLock<T>(
      packageName: string,
      versionId: string,
      fn: () => Promise<T>,
   ): Promise<T>;
   activeVersions(): Promise<Version[]>;
   publishedManifestOf(
      version: Pick<Version, "packageName" | "dirName">,
   ): Promise<Record<string, unknown> | null>;
}

/**
 * The environment surface a run, a reclaim and the scheduler use. The two
 * lookups stay disjoint (see `BuildEnvironment`): `getApiConnection` resolves
 * where a source is READ, `getStorageDestination` where a table is WRITTEN.
 */
export interface MaterializationEnvironment {
   /**
    * Publish again a configured package whose boot publish was refused, so a
    * run resolves the version it makes (Environment.setConfiguredPublishRetry).
    */
   publishRefusedConfiguredPackage(
      packageName: string,
      requestedVersion?: string,
   ): Promise<void>;
   getEnvironmentName(): string;
   getEnvironmentPath(): string;
   getApiConnection(connectionName: string): ApiConnection;
   getStorageDestination(destinationName: string): ApiConnection;
   getMalloyConnection(connectionName: string): Promise<MalloyConnection>;

   getPackage(
      packageName: PackageName,
      reload?: boolean,
      options?: { versionId?: VersionRef },
   ): Promise<MaterializationPackage>;
   getLoadedPackages(): MaterializationPackage[];
   /**
    * Whether the package serves a model older than its files, because its
    * last reload did not compile (a `stale: true` load error).
    */
   getStaleCompileErrors(): ReadonlyMap<string, unknown>;
   withPackageLock<T>(packageName: string, fn: () => Promise<T>): Promise<T>;

   getVersionService(): MaterializationVersions | null;
   /**
    * Whether a host owns the package's binding: bound to a host manifest, or
    * a version a host has bound (see Environment.isHostOwned). Such a
    * package's tables are the host's to build and bind.
    */
   isHostOwned(pkg: MaterializationPackage): boolean;
   /** A host's instructed run of the version committed (see isHostOwned). */
   markVersionHostOwned(packageName: string, versionId: string): Promise<void>;
   peekVersion(
      packageName: string,
      versionId: string,
   ): MaterializationPackage | undefined;
   getLoadedVersionIds(packageName: string): string[];
   /** Hold a version's build open until the returned release is called. */
   registerVersionBuild(packageName: string, versionId: string): () => void;
   versionBuildsSettled(packageName: string, versionId: string): Promise<void>;

   /**
    * Load an auto-run's manifest into the slot it built and run `commit`, all
    * under that slot's lock (see Environment.loadRunManifest).
    */
   loadRunManifest(params: {
      packageName: string;
      versionId?: string;
      database: FreshnessManifest;
      storage: Record<string, ManifestEntry>;
      commit: (outcome: RunManifestLoad) => Promise<void>;
   }): Promise<RunManifestLoad>;
   /** Bind a freshly built database manifest into a slot's models. */
   reloadAllModelsForPackage(
      packageName: string,
      manifest: FreshnessManifest,
      versionId?: string,
   ): Promise<void>;
   /**
    * Read an unversioned package's tables from the store and bind them, the
    * read and the bind under the package's lock (see
    * Environment.rebindPackageFromStore).
    */
   rebindPackageFromStore(
      packageName: string,
      read: () => Promise<{
         database: FreshnessManifest;
         storage?: Record<string, ManifestEntry>;
      }>,
   ): Promise<void>;
   rebindVersionFromStore(
      packageName: string,
      versionId: string,
      read: (reader: ServingReader) => Promise<Record<string, ManifestEntry>>,
      options?: { onlyIfMoved?: boolean },
   ): Promise<boolean>;
}

/**
 * The server the materialization module runs in: the run store, and the
 * environments it builds for. `EnvironmentStore` is one; `main_server.ts`
 * passes it.
 */
export interface MaterializationHost {
   /** The handle the run store (materializations, ledgers, versions) is read through. */
   readonly storageManager: { getRepository(): ResourceRepository };
   getEnvironment(
      environmentName: EnvironmentName,
      reload?: boolean,
   ): Promise<MaterializationEnvironment>;
   /** The environments in memory, without loading any. */
   getLoadedEnvironments(): MaterializationEnvironment[];
}
