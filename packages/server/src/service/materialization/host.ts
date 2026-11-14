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
import type {
   BuildPlan,
   FreshnessManifest,
   ManifestEntry,
} from "./domain/types";

/** The package surface a run reads: its plan, its metadata, its connections. */
interface MaterializationPackage extends BuildPlanPackage {
   getPackageName(): string;
   /** The published version this package is; undefined for an unversioned one. */
   getVersionId(): string | undefined;
   getBuildPlan(): BuildPlan | null;
   /**
    * Resolves once the plan reflects the current connections (a connection
    * edit re-derives it). Optional: a stand-in package's plan never moves.
    */
   whenBuildPlanCurrent?(): Promise<void>;
   /**
    * Whether the plan could not be derived (at load, or re-derived after a
    * connection edit). Optional, like whenBuildPlanCurrent.
    */
   isBuildPlanUnavailable?(): boolean;
   getPackageMetadata(): {
      scope?: string | null;
      manifestLocation?: string | null;
      materialization?: { schedule?: string | null } | null;
   };
   /** The publish-gate rejections the scheduler honours before it arms a cron. */
   persistencePolicyWarnings(): string[];
   /**
    * Drop the bound rollup key checks (known limitation 29) of the tables at
    * these content addresses, which a run is about to write. Optional: a
    * stand-in package binds none.
    */
   forgetKeyUniqueness?(sourceEntityIds: Iterable<string>): void;
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
    * Optional: a test double need not implement it.
    */
   publishRefusedConfiguredPackage?(
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
   withPackageLock<T>(packageName: string, fn: () => Promise<T>): Promise<T>;

   getVersionService(): MaterializationVersions | null;
   peekVersion(
      packageName: string,
      versionId: string,
   ): MaterializationPackage | undefined;
   getLoadedVersionIds(packageName: string): string[];
   /** Hold a version's build open until the returned release is called. */
   registerVersionBuild(packageName: string, versionId: string): () => void;
   versionBuildsSettled(packageName: string, versionId: string): Promise<void>;

   /** Bind a freshly built colocated manifest into a slot's models. */
   reloadAllModelsForPackage(
      packageName: string,
      manifest: FreshnessManifest,
      versionId?: string,
   ): Promise<void>;
   bindPackageColocatedServeManifest(
      packageName: string,
      entries: FreshnessManifest,
      versionId?: string,
   ): Promise<void>;
   bindPackageStorageServeBindings(
      packageName: string,
      entries: Record<string, ManifestEntry>,
      versionId?: string,
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
