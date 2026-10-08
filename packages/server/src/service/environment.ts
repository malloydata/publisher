// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   GivenValue,
   LogMessage,
   Model as MalloyModel,
   ModelDef,
} from "@malloydata/malloy";
import { MalloyError, Runtime } from "@malloydata/malloy";
import { compileDocument, type CompiledDocument } from "./compile_document";
import {
   claimsToBeANotebook,
   isNotebookModelPath,
   notebookReaderProblem,
} from "./notebook";
import { isDashboardModelPath } from "./dashboard";
import { compareSemver, isSemver, versionDirName } from "./semver";
import { hashPackageTree } from "./package_content_hash";
import { notebookLintProblems, reportedByDashboardLint } from "./notebook_lint";
import { publisherMeter } from "../telemetry";
import { Mutex } from "async-mutex";
import crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { components } from "../api";
import {
   API_PREFIX,
   INDEX_MODEL_NAME,
   normalizeModelPath,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
   README_NAME,
   PACKAGE_INSTALL_RECORDS_DIR,
} from "../constants";
import {
   AccessDeniedError,
   BadRequestError,
   CompileRefusedError,
   RenderTagRefusedError,
   ConnectionNotFoundError,
   DestinationNotFoundError,
   EnvironmentNotFoundError,
   ModelCompilationError,
   NotQueryableError,
   PackageManifestError,
   PackageNotFoundError,
   PackageVersionError,
   ServiceUnavailableError,
   UnparseableTextError,
   WriteRolledBackError,
   WriteVerifyError,
   PackageAdmissionRefusedError,
} from "../errors";
import { assertNoCallerAuthorizeAnnotation } from "./authorize";
import type { CallerRegion } from "./caller_joins";
import {
   assertFilterGivensParse,
   malloyGivenToApi,
   type MalloyGiven,
} from "./given";
import {
   assertNoRenderTags,
   assertNoRestrictedConstructs,
} from "./compile_restriction";
import { translatorMalloyError } from "./translator_error";
import { recordAuthorizeGuardRejection } from "../authorize_metrics";
import { getPersistStorageMode, type VersionPromotionMode } from "../config";
import { logger } from "../logger";
import { redactPgSecrets } from "../pg_helpers";
import {
   mergeConnectionUpdate,
   toPublicConnections,
} from "./connection_public_view";
import { recordManifestBind } from "../materialization_metrics";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   assertSafeRelativeModelPath,
   safeJoinUnderRoot,
} from "../path_safety";
import {
   DuplicatePackageVersionError,
   FreshnessManifest,
   ManifestEntry,
   PackageVersion,
   PackageVersionUpdate,
} from "../storage/DatabaseInterface";
import { URL_READER } from "../utils";
import { getPackageLoadPool } from "../package_load/package_load_pool";
import {
   buildEnvironmentMalloyConfig,
   deleteDuckLakeConnectionFile,
   EnvironmentMalloyConfig,
   InternalConnection,
} from "./connection";
import {
   storageDestinationRoot,
   processStorageDestinations,
   processStorageDestinationsOrThrow,
   storageDestinationsEqual,
} from "./connection_config";
import {
   fetchManifestEntries,
   splitManifestEntries,
   type FetchedManifest,
} from "./manifest_loader";
import { ApiConnection, Model } from "./model";
import { Package } from "./package";
import type { PackageMemoryGovernor } from "./package_memory_governor";

/**
 * Sibling dirs under `environmentPath` used by the install/delete pipeline so
 * that long downloads do not hold the per-package mutex.
 *
 *  - `.staging/<pkg>-<uuid>/` — a download in progress. Renamed to the
 *    canonical path under the lock once complete.
 *  - `.retired/<pkg>-<uuid>/` — the previous canonical tree, atomically
 *    renamed out of the way during a swap or delete. `fs.rm`'d asynchronously
 *    after the lock is released.
 *  - `.legacy/<pkg>/` — an unversioned package's tree, moved aside while its
 *    first versioned publish runs. Unlike the other two it is NOT swept at
 *    startup: until that publish commits it is the package's only copy, so a
 *    restart puts it back when the registry holds no version of the package
 *    (see {@link Environment.recoverLegacyTree}), and removes it once one does.
 *
 * All three hold PACKAGE TREES, in the same directory the canonical ones live in, so
 * both are dot-prefixed: anything that enumerates an environment looking for
 * packages must not find a half-downloaded or already-superseded copy. No such
 * enumeration exists today — `listPackages` reads the registered names and this
 * sweep removes these two paths by name — so the prefix is what keeps that true
 * for whatever walks the directory next, rather than a guard on a live path.
 */
const STAGING_DIR_NAME = ".staging";
const RETIRED_DIR_NAME = ".retired";
const LEGACY_DIR_NAME = ".legacy";

// How long to wait for a control-plane manifest fetch during (re)bind before
// giving up and serving live. Binding happens before a package is marked
// SERVING, so an unreachable/slow manifest store must not block the package.
const MANIFEST_FETCH_TIMEOUT_MS = 15_000;

export enum PackageStatus {
   LOADING = "loading",
   SERVING = "serving",
   UNLOADING = "unloading",
}

interface PackageInfo {
   name: string;
   loadTimestamp: number;
   status: PackageStatus;
}

type ApiPackage = components["schemas"]["Package"];
type ApiPackageStatus = NonNullable<ApiPackage["status"]>;
type ApiEnvironment = components["schemas"]["Environment"];
type RetiredConnectionGeneration = {
   label: string;
   releaseConnections: () => Promise<void>;
   timer?: ReturnType<typeof setTimeout>;
};

const RETIRED_CONNECTION_DRAIN_MS = 30_000;

/**
 * The only fields of a storage destination any read reports, so an
 * operator or orchestrator can see which destinations a worker holds without
 * being handed their warehouse credentials. Also the test for a write entry that
 * is a reference to a stored destination rather than a new config for it — see
 * {@link Environment.setStorageDestinations}.
 */
const DESTINATION_READ_FIELDS: ReadonlySet<string> = new Set([
   "name",
   "type",
   "resource",
]);

/**
 * Module-scoped admission-rejection counters. Lazy-initialized so
 * the OTel JS `ProxyMeter` cannot strand them on a NoOp instrument
 * created before the SDK MeterProvider was registered (a real risk
 * in unit tests; see comment in `query_timeout.ts`). Environment
 * name is attached as a label so dashboards can identify hot
 * environments without grepping logs.
 */
import { type Counter } from "@opentelemetry/api";
let queryAdmissionRejectionsCounter: Counter | null = null;
let packageAdmissionRejectionsCounter: Counter | null = null;
function getQueryAdmissionRejectionsCounter(): Counter {
   if (queryAdmissionRejectionsCounter) return queryAdmissionRejectionsCounter;
   queryAdmissionRejectionsCounter = publisherMeter().createCounter(
      "publisher_query_admission_rejections_total",
      {
         description:
            "Queries rejected with 503 because Environment.assertCanAdmitQuery() observed memory back-pressure",
      },
   );
   return queryAdmissionRejectionsCounter;
}
function getPackageAdmissionRejectionsCounter(): Counter {
   if (packageAdmissionRejectionsCounter) {
      return packageAdmissionRejectionsCounter;
   }
   packageAdmissionRejectionsCounter = publisherMeter().createCounter(
      "publisher_package_admission_rejections_total",
      {
         description:
            "Package loads rejected with 503 because Environment.assertCanAdmitNewPackage() observed memory back-pressure",
      },
   );
   return packageAdmissionRejectionsCounter;
}
let compileRefusalsCounter: Counter | null = null;
/**
 * Append-scope compile refusals, by reason.
 *
 * The reasons answer 4xx or 5xx on the same endpoint, so without the label a
 * dependency outage and a caller sending forbidden text are one indistinguishable
 * spike -- and the one that needs paging looks like the one that does not.
 * `restricted_construct` is the caller's text; `base_model_load_failed` is the
 * named model failing to load, which includes the schema-fetch case that answers
 * 503. `render_tag` is a document carrying a URL-producing render tag or markup
 * in a label, also the caller's text but a different fix than a data root.
 */
function getCompileRefusalsCounter(): Counter {
   if (compileRefusalsCounter) return compileRefusalsCounter;
   compileRefusalsCounter = publisherMeter().createCounter(
      "publisher_compile_refusals_total",
      {
         description:
            "Compiles refused at append scope, labelled by reason and environment",
      },
   );
   return compileRefusalsCounter;
}

/**
 * Visible for tests; production code never calls this. Resets the
 * lazy caches so a fresh MeterProvider can capture future writes.
 */
export function resetAdmissionTelemetryForTesting(): void {
   queryAdmissionRejectionsCounter = null;
   packageAdmissionRejectionsCounter = null;
   compileRefusalsCounter = null;
}

/**
 * Run a /compile authorize gate, converting an access denial on a
 * boundary-hidden target into the boundary's generic 404.
 *
 * /compile is exempt from the query boundary so a curated package stays
 * authorable, but the exemption must not turn /compile into an existence
 * oracle. Without this, an unauthorized caller probing a source that is both
 * boundary-hidden and `#(authorize)`-gated gets a 403 naming it — proof the
 * source exists — where the query surface answers a flat 404, letting the
 * hidden namespace be enumerated one guess at a time. Re-running the boundary
 * on the denial path only (it throws for a hidden target and otherwise returns
 * without effect) restores "hidden is indistinguishable from nonexistent" while
 * leaving compile itself ungated: a target the boundary does not hide keeps its
 * informative 403.
 */
/**
 * What the submitted source means to /compile, and how far the check reaches.
 *
 * - "append" (the default, and the historical behavior): the source is
 *   appended to the target model and compiled in its namespace. Right for
 *   validating NEW definitions and queries; an edit to an existing definition
 *   collides ("Cannot redefine"), and diagnostics are positioned in the
 *   concatenated virtual file.
 * - "file": the source is compiled AS the target model file, replacing its
 *   on-disk content for this check. Right for validating an edit before
 *   saving; diagnostics land at true file coordinates.
 * - "package": a dry-run of every .malloy file in the package as saved —
 *   validation with reload's reach but none of its effects on the served
 *   model. An optional source replaces the target file's content, so
 *   importers compile against the edit ("what breaks if I save this?").
 */
export const COMPILE_SCOPES = ["append", "file", "package"] as const;
export type CompileScope = (typeof COMPILE_SCOPES)[number];

/** A compiler diagnostic tagged with the package-relative model it belongs
 *  to, resolvable from `at.url` — load-bearing at scope "package", where
 *  problems from every file share one array. */
export type TaggedLogMessage = LogMessage & { model?: string };

/** The package-relative model path of a file inside the package, `/`-separated on every platform; undefined outside it. */
export function packageRelativeModelPath(
   packagePath: string,
   filePath: string,
   pathModule: Pick<typeof path, "relative" | "isAbsolute" | "sep"> = path,
): string | undefined {
   const rel = pathModule.relative(packagePath, filePath);
   if (rel === "" || rel.startsWith("..") || pathModule.isAbsolute(rel)) {
      return undefined;
   }
   return rel.split(pathModule.sep).join("/");
}

async function denyHiddenAsNotQueryable(
   convert: () => void | Promise<void>,
   gate: () => Promise<void>,
): Promise<void> {
   try {
      await gate();
   } catch (error) {
      if (error instanceof AccessDeniedError) {
         // The conversion must resolve the target at least as well as the gate
         // that denied it: the pre-compile text gate converts on surface
         // syntax, but the compiled gate must convert on the COMPILED run
         // target, or a multi-statement decoy / derivation alias keeps a 403
         // that names the hidden source. Each call site passes the matching
         // boundary check.
         await convert();
      }
      throw error;
   }
}

/** Cap on runtime add failures kept per environment for /status. */
const MAX_RECORDED_ADD_FAILURES = 100;

/**
 * What a request for a package resolves to: the one tree it is served from.
 *
 * An unversioned package has a single slot, keyed by its name and served from
 * `<environment>/<package>`, exactly as before versions existed. A versioned
 * package has one slot per published version, keyed `<package>@<dirName>` (a
 * package name cannot contain `@`, so the two key spaces never meet) and served
 * from `<environment>/<package>/<dirName>`.
 */
export interface PackageSlot {
   name: string;
   /** The published version served, or undefined for an unversioned package. */
   version?: PackageVersion;
   /** Key into the package cache and lock maps. */
   key: string;
   /** The tree this slot is served from. */
   path: string;
}

/** The published versions of one package, as the registry holds them. */
interface PackageVersionIndex {
   latest: string | null;
   versions: Map<string, PackageVersion>;
}

/** A version row as a publish writes it; the registry assigns the rest. */
export type NewPackageVersion = Omit<
   PackageVersion,
   "id" | "environmentId" | "createdAt" | "updatedAt"
>;

/**
 * Refuse a requested version that is not a semantic version, with 400
 * VERSION_ID_INVALID, so a malformed value is told apart from a version the
 * package does not have (404). Every route that takes a version checks it.
 */
export function assertVersionIdFormat(
   packageName: string,
   versionId: string,
): void {
   if (isSemver(versionId)) return;
   throw new PackageVersionError(
      "VERSION_ID_INVALID",
      `"${versionId}" is not a semantic version, so it names no version of package ${packageName}. Send a version such as 1.2.0, with build metadata percent-encoded (+ as %2B).`,
   );
}

/**
 * The window onto the version registry an environment needs, bound to its
 * own database row by the EnvironmentStore (see `setVersionRegistry`). Kept
 * narrow so the environment holds no repository and knows no database id.
 */
export interface VersionRegistry {
   listAllVersions(): Promise<PackageVersion[]>;
   listVersions(packageName: string): Promise<PackageVersion[]>;
   getLatest(packageName: string): Promise<string | null>;
   /**
    * Create the package's row when it has none; a publish needs one. Returns
    * whether it created it.
    */
   ensurePackage(packageName: string, description?: string): Promise<boolean>;
   /**
    * Remove a package row that {@link ensurePackage} created for a publish
    * that then failed before recording any version. The row only: rows keyed
    * by the package's name (versions, runs) are left as they are.
    */
   discardPackage(packageName: string): Promise<void>;
   /** Throws DuplicatePackageVersionError when the version exists. */
   createVersion(version: NewPackageVersion): Promise<PackageVersion>;
   setLatest(
      packageName: string,
      expected: string | null,
      next: string | null,
   ): Promise<boolean>;
   updateVersion(
      id: string,
      updates: PackageVersionUpdate,
   ): Promise<PackageVersion>;
}

/** Fetch a package from `location` into `targetPath`, as a publish does. */
export type VersionTreeFetcher = (
   location: string,
   targetPath: string,
   packageName: string,
) => Promise<void>;

interface StagedVersion {
   stagingPath: string;
   version: string;
   description: string | null;
   dirName: string;
   contentHash: string;
}

/**
 * The version a staged package publishes: the `version` field of its own
 * publisher.json, the only place a version comes from. Missing, or not a
 * semantic version, is refused with the reason a caller can act on.
 */
async function readPublishedVersion(
   packagePath: string,
): Promise<{ version: string; description: string | null }> {
   let text: string;
   try {
      text = await fs.promises.readFile(
         path.join(packagePath, PACKAGE_MANIFEST_NAME),
         "utf8",
      );
   } catch {
      throw new PackageVersionError(
         "MANIFEST_VERSION_MISSING",
         `The package has no ${PACKAGE_MANIFEST_NAME}, so it has no version to publish. Add one with a "version", e.g. "1.0.0".`,
      );
   }
   let manifest: unknown;
   try {
      manifest = JSON.parse(text);
   } catch {
      throw new PackageManifestError(
         `The package's ${PACKAGE_MANIFEST_NAME} is not valid JSON, so its version cannot be read.`,
      );
   }
   if (!manifest || typeof manifest !== "object" || Array.isArray(manifest)) {
      throw new PackageManifestError(
         `The package's ${PACKAGE_MANIFEST_NAME} is not a JSON object, so its version cannot be read.`,
      );
   }
   const { version, description } = manifest as Record<string, unknown>;
   if (version === undefined || version === null || version === "") {
      throw new PackageVersionError(
         "MANIFEST_VERSION_MISSING",
         `The package's ${PACKAGE_MANIFEST_NAME} has no "version", so there is no version to publish. Add one, e.g. "1.0.0", and bump it for each release.`,
      );
   }
   if (typeof version !== "string" || !isSemver(version)) {
      throw new PackageVersionError(
         "MANIFEST_VERSION_INVALID",
         `The "version" in the package's ${PACKAGE_MANIFEST_NAME} is ${JSON.stringify(version)}, which is not a semantic version. Use MAJOR.MINOR.PATCH, e.g. "1.0.0" or "1.0.0-rc1".`,
      );
   }
   return {
      version,
      description: typeof description === "string" ? description : null,
   };
}

export class Environment {
   private packages: Map<string, Package> = new Map();
   // Lock ordering: connectionMutex (environment) MUST be acquired before any
   // packageMutex. Connection updates may invalidate cached package
   // MalloyConfigs and force reloads, so the environment lock is the outer one.
   // Never acquire connectionMutex while holding a packageMutex — that's the
   // AB/BA deadlock path.
   private packageMutexes = new Map<string, Mutex>();
   private packageStatuses: Map<string, PackageInfo> = new Map();
   /**
    * Published versions, per package name, mirrored from the registry
    * (`package_versions` and `packages.latest_version`) so resolving a request
    * to a slot is a map lookup rather than a database read. A package absent
    * from this map has no versions and is served as a single unversioned slot.
    * Written only through {@link setPackageVersions} /
    * {@link clearPackageVersions}, by whoever has just written the registry.
    */
   private packageVersions: Map<string, PackageVersionIndex> = new Map();
   /** See {@link setVersionRegistry}. */
   private versionRegistry: VersionRegistry | undefined;
   private versionTreeFetcher: VersionTreeFetcher | undefined;
   /**
    * Packages with a load, reinstall or recompile in progress here, keyed by
    * name, with when the first in-flight operation began. This is what
    * `Package.status.loading` reports. It is kept apart from
    * `packageStatuses`, whose LOADING/SERVING/UNLOADING answers "is a copy
    * registered to serve", because the two are independent: a reload of a
    * serving package is in flight here while the previous copy stays in
    * `packages` and keeps answering queries. Entries are reference-counted so
    * a nested operation (a reinstall that then rebinds a manifest) stays
    * marked until the outermost one finishes.
    */
   private loadsInFlight: Map<
      string,
      {
         count: number;
         since: number;
         /**
          * Resolves once every load counted here has finished, however each
          * ended. Loads of one package started independently (a reinstall
          * racing a reload, a lazy load joined by an install) share the entry,
          * so a waiter is released only when the last of them is done.
          */
         settled: Promise<void>;
         settle: () => void;
      }
   > = new Map();
   /**
    * Configured packages that failed to load, keyed by name, with the reason.
    *
    * A load failure is not fatal: the package is omitted and its siblings serve
    * on. It is also observable exactly once, because the failing load deletes
    * the `packageStatuses` entry that `listPackages` enumerates, so the next
    * listing no longer knows the package was ever configured. That makes this
    * the only lasting record. Read by EnvironmentStore.getStatus.
    */
   private failedPackages: Map<string, string> = new Map();
   /**
    * Why a configured package never reached the disk, keyed by package name.
    *
    * Separate from {@link failedPackages} because it is the more specific
    * answer and has to win. A package whose location failed to mount is still
    * seeded SERVING by addEnvironment, so its later lazy load fails on the
    * manifest that was never copied and `listPackages` records that instead.
    * Reporting "Package manifest ... does not exist." for what was really a
    * typo'd `location` sends the reader hunting in the wrong place.
    */
   private mountErrors: Map<string, string> = new Map();
   /** Runtime add failures recorded in {@link mountErrors}, oldest first. */
   private recordedAddFailures: string[] = [];
   /**
    * Why a SERVING package's most recent reload failed to compile, keyed by
    * package name.
    *
    * Separate from {@link failedPackages} because the package is NOT failed:
    * a failed reload keeps the last good compiled model serving (see
    * {@link _loadOrGetPackageLocked}), so `getFailedPackages()` and the
    * serving counters must not include it. What this records is staleness:
    * the model answering queries is older than the files on disk. Without it
    * a watch-mode recompile failure is visible only on stderr, and /status
    * keeps reporting a healthy server while queries answer from the previous
    * model. Read by EnvironmentStore.getStatus, which reports each entry as a
    * loadErrors item with `stale: true`.
    */
   private staleCompileErrors: Map<
      string,
      { message: string; failedAt: string }
   > = new Map();
   private malloyConfig: EnvironmentMalloyConfig;
   private connectionMutex = new Mutex();
   private retiredConnectionGenerations =
      new Set<RetiredConnectionGeneration>();
   private apiConnections: ApiConnection[];
   /**
    * Warehouses materialization builds write to and materialized queries are
    * served from. Disjoint from {@link apiConnections}: a destination is not a
    * connection, so it is absent from `listApiConnections`, unresolvable by name
    * from a user's model, and free to share a name with a connection without
    * colliding with it. Two lists rather than one flagged list so that every
    * existing connection consumer excludes destinations structurally, instead of
    * each having to remember a filter.
    */
   private destinations: ApiConnection[];
   /**
    * The live Malloy connections for {@link destinations}, assembled exactly the
    * way the user-facing ones are but from the destination list — so a
    * materialization serve shape can reach a destination while nothing in the
    * namespace a package compiles against can. Rebuilt whenever the list is
    * replaced, retiring the previous generation's handles.
    *
    * Always assigned by the time anything can read it: the constructor sets the
    * destination list unconditionally, and `setStorageDestinations` treats an
    * unbuilt config as a reason to build even when the list has not changed —
    * which is what makes the assertion here true for an environment with no
    * destinations at all.
    */
   private destinationMalloyConfig!: EnvironmentMalloyConfig;
   /**
    * Whether {@link destinations} is the authoritative set for this environment —
    * i.e. safe to reconcile the stored rows against.
    *
    * False when a load could not READ the stored destinations. The list is then
    * "unknown", not "empty", and the two are not interchangeable: the database
    * sync prunes rows the list does not hold, so treating a failed read as an
    * empty list turns a transient error into permanently deleted registrations.
    * An explicit set (config or API) makes it authoritative again.
    */
   private destinationsAuthoritative = true;
   private environmentPath: string;
   private environmentName: string;
   // Resolves a package's latest persisted materialization manifest entries
   // (the full map — colocated tableName entries AND `storage=` cross-connection
   // entries), so serve routing for BOTH tiers is re-established when a package
   // (re)loads — e.g. after a worker restart — instead of only when a build's
   // auto-load runs. Injected by the EnvironmentStore, which owns the
   // materialization repository. Undefined ⇒ no re-bind on load (routing then
   // depends on a fresh build, the old behavior). See
   // {@link rebindServeBindingsFromLocalStore}.
   private storageBindingResolver?: (
      packageName: string,
   ) => Promise<Record<string, ManifestEntry>>;
   public metadata: ApiEnvironment;
   // The shared memory governor that consults process RSS. Optional —
   // when null the gate is a no-op and the environment behaves exactly
   // like it did before the governor was introduced. Set by
   // EnvironmentStore.setMemoryGovernor at server start so we keep the
   // governor as the single owner of the back-pressure boolean.
   private memoryGovernor: PackageMemoryGovernor | null = null;
   // Called with each package the moment it enters `this.packages`. Set by
   // EnvironmentStore (see setPackageLoadedHook); null means nobody listens.
   private packageLoadedHook: ((pkg: Package) => void) | null = null;

   /** Absolute path on disk where this environment's package files live. */
   public getEnvironmentPath(): string {
      return this.environmentPath;
   }

   /** This environment's name (the canonical key used by the API/service). */
   public getEnvironmentName(): string {
      return this.environmentName;
   }

   constructor(
      environmentName: string,
      environmentPath: string,
      malloyConfig: EnvironmentMalloyConfig,
      apiConnections: InternalConnection[],
      storageDestinations: ApiConnection[] = [],
   ) {
      // Sanitizer barrier: every downstream `path.join(this.environmentPath,
      // …)` site (including the static `sweepStaleInstallDirs` sweep) gets a
      // value that has cleared an allowlist check at the gate.
      assertSafeEnvironmentPath(environmentPath);
      this.environmentName = environmentName;
      this.environmentPath = environmentPath;
      this.malloyConfig = malloyConfig;
      this.apiConnections = apiConnections;
      this.destinations = [];
      this.setStorageDestinations(storageDestinations);
      this.metadata = {
         resource: `${API_PREFIX}/environments/${this.environmentName}`,
         name: this.environmentName,
         location: this.environmentPath,
      };
      void this.reloadEnvironmentMetadata();
   }

   private async writeEnvironmentReadme(readme?: string): Promise<void> {
      if (readme === undefined) return;

      const readmePath = path.join(this.environmentPath, "README.md");

      try {
         await fs.promises.writeFile(readmePath, readme, "utf-8");
         logger.info(
            `Updated README.md for environment ${this.environmentName}`,
         );
      } catch (err) {
         logger.error(`Failed to write README.md`, { error: err });
         throw new Error(`Failed to update environment README`, { cause: err });
      }
   }

   public async update(payload: ApiEnvironment) {
      // Ahead of the readme write, so a body carrying a destination list we
      // cannot read is refused before this method has changed anything.
      //
      // Absent means "leave alone", so a caller updating only the readme or the
      // connections cannot blank the destination list by omission. Anything else
      // present is acted on, including a shape we cannot read: the list replaces
      // what is stored, so "we did not understand this" must not resolve to "then
      // keep none of them". An explicit empty list is a different thing and does
      // clear it.
      if (payload.storageDestinations !== undefined) {
         this.setStorageDestinations(payload.storageDestinations, {
            rejectInvalid: true,
         });
      }

      if (payload.readme !== undefined) {
         this.metadata.readme = payload.readme;
         await this.writeEnvironmentReadme(payload.readme);
      }

      // Handle connections update
      // TODO: Update environment connections should have its own API endpoint
      if (payload.connections) {
         const payloadConnections = payload.connections;
         await this.runConnectionUpdateExclusive(async () => {
            logger.info(
               `Updating ${payloadConnections.length} connections for environment ${this.environmentName}`,
            );
            // This list replaces the stored one wholesale, and responses no
            // longer carry credentials, so an entry echoed back from a read
            // arrives without one. Merge each entry against what is stored, the
            // same "an entry that names a stored one and carries no config
            // keeps it" rule storageDestinations already documents. Without
            // this, adding or deleting ANY connection through the app strips
            // the credentials of every other connection in the environment,
            // because the app sends the whole list back. An entry with no
            // stored counterpart is new and passes through as sent.
            const mergedConnections = payloadConnections.map((incoming) => {
               const stored = this.apiConnections.find(
                  (existing) => existing.name === incoming.name,
               );
               return stored
                  ? mergeConnectionUpdate(stored, incoming)
                  : incoming;
            });
            const isUpdateConnectionRequest = true;
            const nextMalloyConfig = buildEnvironmentMalloyConfig(
               mergedConnections,
               this.environmentPath,
               isUpdateConnectionRequest,
            );

            this.updateConnections(nextMalloyConfig);

            logger.info(
               `Successfully updated connections for environment ${this.environmentName}`,
               {
                  apiConnections: this.apiConnections.length,
                  internalConnections: this.apiConnections.length,
               },
            );
         });
      }

      return this;
   }

   static async create(
      environmentName: string,
      environmentPath: string,
      connections: ApiConnection[],
      storageDestinations: ApiConnection[] = [],
   ): Promise<Environment> {
      assertSafeEnvironmentPath(environmentPath);
      if (!(await fs.promises.stat(environmentPath))?.isDirectory()) {
         throw new EnvironmentNotFoundError(
            `Environment path ${environmentPath} not found`,
         );
      }

      logger.info(`Creating environment with connection configuration`);
      const malloyConfig = buildEnvironmentMalloyConfig(
         connections,
         environmentPath,
      );

      logger.info(
         `Loaded ${malloyConfig.apiConnections.length} connections for environment ${environmentName}`,
         {
            connections: malloyConfig.apiConnections.map((c) => ({
               name: c.name,
               type: c.type,
            })),
         },
      );

      const environment = new Environment(
         environmentName,
         environmentPath,
         malloyConfig,
         malloyConfig.apiConnections,
         storageDestinations,
      );

      // Best-effort: a previous run may have crashed mid-install or
      // mid-delete and left orphan dirs under .staging/ or .retired/.
      // Run against the validated constructor argument so the sink path
      // here does NOT route through `this` (which CodeQL conservatively
      // treats as tainted because other methods on this class touch
      // request-derived `packageName` values).
      await Environment.sweepStaleInstallDirs(environmentPath);

      return environment;
   }

   public async reloadEnvironmentMetadata(): Promise<ApiEnvironment> {
      let readme = "";
      try {
         readme = (
            await fs.promises.readFile(
               safeJoinUnderRoot(this.environmentPath, README_NAME),
            )
         ).toString();
      } catch {
         // Readme not found, so we'll just return an empty string
      }
      this.metadata = {
         ...this.metadata,
         resource: `${API_PREFIX}/environments/${this.environmentName}`,
         name: this.environmentName,
         readme,
      };
      return this.metadata;
   }

   public async compileSource(
      packageName: string,
      modelName: string,
      source: string | undefined,
      includeSql: boolean = false,
      givens?: Record<string, GivenValue>,
      scope: CompileScope = "append",
      versionId?: string,
   ): Promise<{
      problems: TaggedLogMessage[];
      sql?: string;
      document?: CompiledDocument;
   }> {
      assertSafePackageName(packageName);
      assertSafeRelativeModelPath(modelName);
      // Resolved here for its refusals (an unknown or archived version), and
      // again under the lock below for the path.
      this.resolveSlot(packageName, versionId);
      if (!COMPILE_SCOPES.includes(scope)) {
         throw new BadRequestError(
            `Invalid compile scope "${String(scope)}": expected one of ` +
               `${COMPILE_SCOPES.map((s) => `"${s}"`).join(", ")}.`,
         );
      }
      // Scope decides what `source` means, so it decides whether one is
      // required: "append" and "file" compile the submitted text (nothing to
      // do without it), while "package" is a dry-run of the files as saved and
      // takes source only as an optional what-if replacement for modelPath.
      if (source === undefined && scope !== "package") {
         throw new BadRequestError(
            `Compile scope "${scope}" requires a source to compile. ` +
               `Fix: pass the Malloy text in "source", or use scope "package" ` +
               `to validate the package's files as saved.`,
         );
      }
      if (scope === "package" && includeSql) {
         throw new BadRequestError(
            `includeSql is not available at scope "package": the dry-run has ` +
               `no single runnable query to extract SQL from. Fix: compile ` +
               `the runnable text at scope "append" or "file" instead.`,
         );
      }
      // The submitted source lands in the package's namespace (appended to the
      // target model, or replacing a file wholesale), so an authorize
      // annotation in it would sit alongside — or displace — the author's.
      // Same rejection as the query path on every scope, and `includeSql`
      // makes this door the more valuable one to an attacker.
      if (source !== undefined) {
         try {
            assertNoCallerAuthorizeAnnotation(source);
         } catch (err) {
            recordAuthorizeGuardRejection("compile_source");
            throw err;
         }
      }
      // /compile interprets modelPath as a .malloy model (namespace context at
      // "append", the file being written at "file"/"package"-with-source). A
      // notebook (.malloynb) is markdown + cells, not a model, so compiling
      // against it only yields a confusing parse error — reject it up front
      // with an actionable message. (Notebooks remain public for
      // discovery/query; this is specific to the compile context.)
      if (
         modelName.endsWith(NOTEBOOK_FILE_SUFFIX) &&
         (scope !== "package" || source !== undefined)
      ) {
         throw new BadRequestError(
            `Cannot compile against a notebook ("${modelName}"). ` +
               `/compile takes a .malloy model path.`,
         );
      }
      // Hold the per-package mutex for the duration of every disk read —
      // both the explicit `fs.readFile(modelPath)` below and the implicit
      // import resolution that `runtime.loadModel` does through the URL
      // reader. This is mutually exclusive with `installPackage`'s Phase 2
      // rename swap and with `deletePackage`'s rename-to-retired, so a
      // compile can never observe a half-rewritten tree. The slow Phase 1
      // download happens outside this lock, so a multi-second clone does
      // not block compiles.
      return this.withResolvedSlotLock(packageName, versionId, async (slot) => {
         // Sanitized join: input segments are allowlisted above; the
         // resolve-and-contain check here is the secondary guard CodeQL's
         // path-injection sanitizer recognises. The slot's path is the
         // package directory, or a published version's tree under it.
         const modelPath = safeJoinUnderRoot(slot.path, modelName);
         const packagePath = slot.path;
         // Where the compiled text lives, by scope. "append": a virtual file
         // in the model's directory (so relative imports resolve) holding the
         // model's content with the source appended — the historical behavior,
         // whose diagnostics are positioned in the CONCATENATED file. "file"
         // (and "package" with a source): the virtual file IS modelPath, so
         // the submitted text replaces the on-disk copy, diagnostics land at
         // true file coordinates, and — at "package" — every importer compiles
         // against the new text. Use `pathToFileURL` rather than hand-prefixing
         // `file://`: on Windows the latter produces a malformed URL
         // (`file://D:\Temp\…`) that round-trips differently than the URL the
         // Malloy runtime synthesizes from the same path, breaking the
         // intercepting reader's string comparison below and falling through
         // to disk for a virtual file that doesn't exist.
         const modelDir = path.dirname(modelPath);
         const virtualUrl =
            scope === "append"
               ? pathToFileURL(path.join(modelDir, "__compile_check.malloy"))
               : pathToFileURL(modelPath);
         const virtualUri = virtualUrl.toString();

         let fullSource = source ?? "";
         // Where the caller's own text starts in the compiled file, so its joins
         // are gated as the query path gates them. "file" and "package" have
         // none: the whole text is the author's file.
         let callerRegion: CallerRegion | undefined;
         if (scope === "append") {
            // Read the full model file so the submitted source inherits the
            // model's complete namespace — imports, source definitions,
            // queries, etc.
            let modelContent = "";
            try {
               modelContent = await fs.promises.readFile(modelPath, "utf8");
            } catch {
               // Empty content here, and compilation reports the problem. Note
               // this fallback no longer decides the missing-model case on its
               // own at `append` scope: the restricted-construct gate below
               // loads the same model to check the caller's text against, and
               // refuses when it cannot, so a model that does not exist is
               // rejected there before this leniency can apply.
            }
            fullSource = modelContent
               ? `${modelContent}\n${source}`
               : (source ?? "");
            // Checked again where the compiler reads it: a saved model ending in an open block note re-lexes the caller's prose.
            // Both checks are load-bearing: this in-context form accepts text the stand-alone check above refuses.
            if (source !== undefined && modelContent) {
               try {
                  assertNoCallerAuthorizeAnnotation(
                     source,
                     `${modelContent}\n`,
                  );
               } catch (err) {
                  recordAuthorizeGuardRejection("compile_source");
                  throw err;
               }
            }
            callerRegion = {
               kind: "span",
               url: virtualUri,
               // 0-based: the appended text starts on the line after the model's.
               fromLine: modelContent ? modelContent.split("\n").length : 0,
               text: source ?? "",
            };
         }

         // Create a URL Reader that serves the source string for the virtual
         // file, but falls back to the disk for everything else (imports). At
         // scope "package" with no source there is nothing to substitute and
         // every file reads from disk as saved.
         const substitute = scope !== "package" || source !== undefined;
         const interceptingReader = {
            readURL: async (url: URL) => {
               if (substitute && url.toString() === virtualUri) {
                  return fullSource;
               }
               return URL_READER.readURL(url);
            },
         };

         // Use the locked variant — we already hold the slot's mutex.
         const pkg = await this._loadOrGetPackageLocked(slot);

         // Authorize gate: /compile is compile-only, but it can still act
         // as a schema oracle (a denied caller learns a gated source's columns
         // from compile errors) and, with includeSql, leak its SQL. Decide the
         // locks the submitted text names BEFORE compiling — mirrors the query
         // path's early gate (see `assertAuthorizedForText` for what each scope
         // reads); the compiled backstop below settles a target this cannot
         // name. The gate runs against the package's cached Model (its
         // `given:` block + authorize annotations), independent of the virtual
         // compile below. A new model path has no cached Model, so its early
         // surface-syntax gate cannot run; the compiled backstop below instead
         // evaluates gates carried by the runnable's own ModelDef.
         let { model: gateModel, exact: hasExactGateModel } =
            pkg.getCompileAuthorizationModel(modelName);
         // A file can exist on disk without being in the cached package model
         // (for example, it was added after the last reload). Compile that
         // author's file once as an ephemeral gate model so file-level givens
         // and authorize annotations come from the correct namespace. A truly
         // new path still falls back to the compiled-runnable gate below.
         if (!hasExactGateModel && source !== undefined) {
            const diskTarget = await fs.promises
               .stat(modelPath)
               .catch(() => undefined);
            if (diskTarget?.isFile()) {
               gateModel = await Model.create(
                  packageName,
                  packagePath,
                  modelName,
                  pkg.getMalloyConfig(),
                  {
                     buildManifest: pkg.getBuildManifestEntries(),
                  },
               );
               hasExactGateModel = true;
            }
         }
         // A document is gated cell by cell and tile by tile below, so one
         // restricted cell does not refuse the cells the caller may read.
         const documentCandidate =
            scope === "append" &&
            source !== undefined &&
            claimsToBeANotebook(source);
         const runEarlyGate = async (): Promise<void> => {
            if (gateModel && hasExactGateModel && source !== undefined) {
               // Only the authorize gate (the *who* axis) applies to /compile.
               // The query boundary (`explores`/`queryableSources`, the *what*
               // axis) deliberately does NOT: compile is the authoring loop
               // (validate -> save -> reload), and gating it made a curated
               // package un-authorable — a QA session (HANDOFF CR-5) had every
               // per-file compile 404 with "Query target is not queryable" the
               // moment `queryableSources: "declared"` was set. The boundary is
               // discovery curation, not access control (the skills say so
               // outright); the accepted trade is that /compile can reveal a
               // non-exported source's schema (and, with includeSql, SQL) —
               // sources whose confidentiality matters are gated by
               // `#(authorize)`, which still applies here in full.
               await denyHiddenAsNotQueryable(
                  () => {
                     gateModel.assertQueryBoundaryEarly(
                        undefined,
                        undefined,
                        source,
                     );
                  },
                  () =>
                     gateModel.assertAuthorizedForText(source, givens ?? {}, {
                        // File and package scope compile the whole file (or, at
                        // package scope with a source, the whole replacement) —
                        // a locked name that is not the statement Malloy runs
                        // must not refuse it, and its joins are author joins.
                        wholeFile: scope !== "append",
                     }),
               );
            }
         };
         if (!documentCandidate) await runEarlyGate();

         // Initialize Runtime with the package's active MalloyConfig so compile
         // checks see the same package-scoped duckdb as execution. This runtime
         // borrows the package config; the package/environment lifecycle owns release.
         // Thread the package's bound build manifest (when present) so the
         // /compile preview routes persist sources to their materialized tables
         // exactly like execution does — otherwise includeSql=true would always
         // show base-table SQL and diverge from what executeQuery actually runs.
         const boundManifestEntries = pkg.getBuildManifestEntries();
         const runtime = new Runtime({
            urlReader: interceptingReader,
            config: pkg.getMalloyConfig(),
            buildManifest: boundManifestEntries
               ? { entries: boundManifestEntries, strict: false }
               : undefined,
         });

         // Tag each diagnostic with the package-relative model it points at,
         // read off `at.url`. Load-bearing at scope "package" (one array,
         // many files) and clarifying everywhere else: an "append"-scope
         // diagnostic can point at pre-existing model content, and the tag is
         // what says so. The append-mode virtual file reports as the model it
         // extends.
         const tagProblems = (problems: LogMessage[]): TaggedLogMessage[] =>
            problems.map((problem) => {
               const url = (problem as { at?: { url?: string } }).at?.url;
               let model: string | undefined;
               if (url && url.startsWith("file:")) {
                  try {
                     model = packageRelativeModelPath(
                        packagePath,
                        fileURLToPath(url),
                     );
                  } catch {
                     // Not a resolvable file URL — leave the tag off.
                  }
               }
               if (scope === "append" && url === virtualUri) {
                  model = modelName;
               }
               return model !== undefined ? { ...problem, model } : problem;
            });

         if (scope === "package") {
            // Gate the caller's replacement once on the main thread. The full
            // package compile below runs in the load worker, but authorization
            // probes use the package's live connection/config and must remain
            // on this side of the worker boundary.
            if (source !== undefined && gateModel) {
               try {
                  const materializer = runtime.loadModel(virtualUrl);
                  await materializer.getModel();
                  let finalQuery: ReturnType<
                     typeof materializer.loadFinalQuery
                  > | null = null;
                  try {
                     finalQuery = materializer.loadFinalQuery();
                  } catch {
                     // No runnable query in the replacement text.
                  }
                  if (finalQuery) {
                     await denyHiddenAsNotQueryable(
                        () => {
                           if (!hasExactGateModel) {
                              throw new NotQueryableError(
                                 "Query target is not queryable.",
                              );
                           }
                           return gateModel.assertCompiledTargetQueryable(
                              finalQuery,
                              source,
                           );
                        },
                        () =>
                           hasExactGateModel
                              ? gateModel.assertAuthorizedForRunnable(
                                   finalQuery,
                                   givens ?? {},
                                )
                              : gateModel.assertAuthorizedFromCompiledRunnable(
                                   finalQuery,
                                   givens ?? {},
                                ),
                     );
                  }
               } catch (error) {
                  // Compiler diagnostics are returned by the worker below,
                  // the translator's plain Error among them (the worker
                  // classifies it). Authorization denials are policy outcomes
                  // and propagate.
                  if (
                     !(error instanceof MalloyError) &&
                     !translatorMalloyError(error)
                  ) {
                     throw error;
                  }
               }
            }

            // Use the exact worker path a reload uses: dotfiles are ignored,
            // both .malloy and .malloynb files are compiled, CPU work is kept
            // off the event loop, and the worker's timeout bounds the request.
            // This does not swap the returned models into the served package.
            let outcome;
            try {
               outcome = await getPackageLoadPool().loadPackage({
                  packagePath,
                  packageName,
                  malloyConfig: pkg.getMalloyConfig(),
                  defaultConnectionName: "duckdb",
                  buildManifest: boundManifestEntries,
                  collectProblems: true,
                  replacement:
                     source === undefined
                        ? undefined
                        : { modelPath: modelName, source },
               });
            } catch (error) {
               // Same split as Package.loadViaWorker: compile errors and an
               // unusable publisher.json keep their 4xx mapping, and only an
               // infrastructure failure reads as a worker outage.
               if (
                  error instanceof MalloyError ||
                  error instanceof ModelCompilationError ||
                  error instanceof PackageManifestError
               ) {
                  throw error;
               }
               throw new ServiceUnavailableError(
                  `Package compile worker unavailable: ${
                     error instanceof Error ? error.message : String(error)
                  }`,
               );
            }

            // Package scope intentionally reports diagnostics from every model
            // the reload compiler sees, including files hidden from discovery.
            // It returns no rows or SQL; authorize still gates caller text.
            const seen = new Set<string>();
            const problems: TaggedLogMessage[] = [];
            const collect = (
               batch: LogMessage[],
               fallbackModel?: string,
            ): void => {
               for (const problem of tagProblems(batch)) {
                  const problemUrl = (problem as { at?: { url?: string } }).at
                     ?.url;
                  const tagged =
                     problem.model === undefined &&
                     problemUrl === undefined &&
                     fallbackModel !== undefined
                        ? { ...problem, model: fallbackModel }
                        : problem;
                  const start = (
                     tagged as {
                        at?: {
                           range?: {
                              start?: { line?: number; character?: number };
                           };
                        };
                     }
                  ).at?.range?.start;
                  const key = `${tagged.model ?? problemUrl ?? ""}|${
                     start?.line ?? -1
                  }|${start?.character ?? -1}|${tagged.severity}|${
                     tagged.message
                  }`;
                  if (seen.has(key)) continue;
                  seen.add(key);
                  problems.push(tagged);
               }
            };
            // The findings a reload would add on the main thread after this
            // same worker compile: render tags and the dashboard, given and
            // drill lints. Read before the notebook lint below, which drops its
            // copy of a finding only when the dashboard lint reported it too.
            const { renderTagWarnings, dashboardWarnings } =
               await Package.lintWorkerOutcome(
                  this.environmentName,
                  packageName,
                  packagePath,
                  pkg.getMalloyConfig(),
                  outcome,
                  boundManifestEntries,
                  source === undefined
                     ? undefined
                     : { modelPath: modelName, source },
               );
            for (const compiled of outcome.models) {
               if (compiled.problems) {
                  collect(
                     compiled.problems as LogMessage[],
                     compiled.modelPath,
                  );
               }
               const readerProblem =
                  compiled.modelDef && compiled.modelSourceText !== undefined
                     ? notebookReaderProblem(
                          compiled.modelPath,
                          compiled.modelSourceText,
                          compiled.modelDef as ModelDef,
                          pathToFileURL(
                             path.join(packagePath, compiled.modelPath),
                          ).toString(),
                       )
                     : undefined;
               if (readerProblem) collect([readerProblem], compiled.modelPath);
               if (compiled.compilationError) {
                  const compilerProblems =
                     compiled.compilationError.malloyProblems;
                  if (compilerProblems) {
                     collect(
                        compilerProblems as LogMessage[],
                        compiled.modelPath,
                     );
                  } else {
                     collect(
                        [
                           {
                              severity: "error",
                              message: compiled.compilationError.message,
                           } as LogMessage,
                        ],
                        compiled.modelPath,
                     );
                  }
               }
               // A file that did not compile carries no text back, so it is read as saved (or as replaced).
               const lintText = !(
                  isNotebookModelPath(compiled.modelPath) ||
                  isDashboardModelPath(compiled.modelPath)
               )
                  ? undefined
                  : (compiled.modelSourceText ??
                    (compiled.modelPath === modelName && source !== undefined
                       ? source
                       : await fs.promises
                            .readFile(
                               path.join(packagePath, compiled.modelPath),
                               "utf8",
                            )
                            .catch(() => undefined)));
               if (lintText !== undefined) {
                  collect(
                     notebookLintProblems(
                        compiled.modelPath,
                        lintText,
                        pathToFileURL(
                           path.join(packagePath, compiled.modelPath),
                        ).toString(),
                     ).filter(
                        (problem) =>
                           !reportedByDashboardLint(
                              problem,
                              compiled.modelPath,
                              dashboardWarnings,
                           ),
                     ),
                     compiled.modelPath,
                  );
               }
            }
            // Each keeps its own severity, so a broken dashboard makes the
            // compile an error, as it should. They carry no position, so the
            // subject (the view, field or given) leads the message: without
            // it, two views with the same finding would collapse into one.
            const asProblem = (
               warning: (typeof renderTagWarnings)[number],
               code: string,
            ): LogMessage =>
               ({
                  severity: warning.severity ?? "warn",
                  message: warning.subject
                     ? `${warning.subject}: ${warning.message}`
                     : warning.message,
                  code,
               }) as LogMessage;
            for (const warning of renderTagWarnings) {
               collect([asProblem(warning, "render-tag")], warning.model);
            }
            for (const warning of dashboardWarnings) {
               collect([asProblem(warning, "dashboard-lint")], warning.model);
            }
            if (
               source !== undefined &&
               outcome.replacementMatchedExisting === false
            ) {
               collect(
                  [
                     {
                        severity: "warn",
                        message:
                           `No existing package file exactly matched ` +
                           `"${modelName}"; the source was validated as a new ` +
                           `file and did not replace another model.`,
                     } as LogMessage,
                  ],
                  modelName,
               );
            }
            return { problems };
         }

         // The model the append-scope fragment is judged against, loaded once for the gate and for a document.
         let appendBase: ReturnType<Runtime["loadModel"]> | undefined;

         // Counted here rather than inside the gate so every reason shares one instrument and one label set; only a refusal is counted, since anything else the gate rethrows is an infrastructure failure.
         const countRefusal = (error: unknown): void => {
            // An unparseable tile is a compile problem for the document, not a refusal.
            if (
               error instanceof CompileRefusedError &&
               !(error instanceof UnparseableTextError)
            ) {
               getCompileRefusalsCounter().add(1, {
                  environment: this.environmentName,
                  reason:
                     error instanceof RenderTagRefusedError
                        ? "render_tag"
                        : "restricted_construct",
               });
            }
         };
         const refuseConstructs = async (
            baseModel: MalloyModel,
            text: string,
            renderTags: boolean,
         ): Promise<void> => {
            try {
               await assertNoRestrictedConstructs(runtime, baseModel, text, {
                  renderTags,
               });
            } catch (error) {
               countRefusal(error);
               throw error;
            }
         };
         const refuseRenderTags = (text: string): void => {
            try {
               assertNoRenderTags(text);
            } catch (error) {
               countRefusal(error);
               throw error;
            }
         };

         // Containment for caller-submitted fragments. Scope "append" is the
         // one scope whose text is a FRAGMENT checked against a curated model
         // rather than a file the author owns, so it has no legitimate need to
         // define its own data roots -- and Malloy resolves a source's schema
         // at compile time, so an unrestricted one reaches the connection, the
         // filesystem and the network without running a query. Scopes "file"
         // and "package" are deliberately NOT gated: there the source IS the
         // model file, and `import` plus `connection.table(...)` /
         // `connection.sql(...)` are how any model declares what it reads.
         // Gating them would make an ordinary package un-authorable.
         if (scope === "append") {
            // The model as saved, WITHOUT the caller's appended text: the
            // fragment is checked against the surface the author published, so
            // the caller cannot widen the namespace it is judged against.
            //
            // Five of the seven restricted constructs are refused on sight,
            // but two are not: `name!type(...)` and the `sql_*` family are
            // classified inside `computeExpression(fs)`, which needs a resolved
            // FieldSpace. With no base model a fragment like
            // `run: base_source -> { ... }` never resolves `base_source`, so
            // the expression is never evaluated, the construct is never
            // classified, and the gate passes text the real compile then runs
            // for real. So a base model that will not load fails the request
            // rather than lowering the gate: the caller's own text is not what
            // failed, and the same argument `assertNoRestrictedConstructs`
            // makes about its own catch applies here -- an infrastructure
            // error carries no evidence either way.
            let baseModel: MalloyModel;
            try {
               appendBase = runtime.loadModel(pathToFileURL(modelPath));
               baseModel = await appendBase.getModel();
            } catch (error) {
               // Three different failures arrive here and they are not one
               // answer. Refusing uniformly would tell a caller their text was
               // bad when the warehouse was down, and a 4xx says "do not
               // retry" -- the opposite of what an outage wants. The detail
               // stays server-side either way: `modelPath` is an absolute path
               // inside the container, so returning it would answer "does this
               // file exist, and is it readable" for any path a caller names,
               // which is the shape of oracle this gate exists to close.
               // `warn`, not `error`: a caller typo in `modelPath` reaches here,
               // and an unauthenticated 400 must not emit ERROR at whatever rate
               // a caller likes.
               logger.warn("Compile gate could not load the base model", {
                  modelPath,
                  error,
               });
               getCompileRefusalsCounter().add(1, {
                  environment: this.environmentName,
                  reason: "base_model_load_failed",
               });
               if (error instanceof MalloyError) {
                  // Every MalloyError here is the same answer: the model this
                  // fragment would be judged against did not compile, so there
                  // is nothing to judge it against.
                  //
                  // There was a 503 branch keyed on `failed-to-fetch-table-schema`,
                  // on the reading that a schema fetch which failed means the
                  // dependency is unreachable. That code does not carry that
                  // meaning: a table that simply DOES NOT EXIST produces it too,
                  // so a permanent authoring error was telling the client to
                  // retry. The reverse also held -- a `conn.sql(...)`-rooted
                  // model whose warehouse was genuinely down fails with
                  // `invalid-sql-source` and took the 400 anyway. Splitting on
                  // it was therefore wrong in both directions, and Malloy
                  // publishes no code here that means "unreachable". Until one
                  // exists, these are one 400 carrying the model's own problems,
                  // which is also what `file` and `package` scope already do
                  // with the same failure.
                  throw new CompileRefusedError(
                     `Cannot validate the submitted source: the model ` +
                        `"${modelName}" does not compile, so there is nothing ` +
                        `to check the submitted source against. Problems: ` +
                        error.problems.map((p) => p.message).join("; "),
                  );
               }
               // Missing file, permission, anything else: the caller named a
               // model this server cannot load, which is theirs to correct.
               throw new CompileRefusedError(
                  `Cannot validate the submitted source: the model ` +
                     `"${modelName}" could not be loaded to check it against.`,
               );
            }
            // The fragment ALONE, against the compiled base model. The
            // concatenation the real compile runs cannot be passed here:
            // `extendModel` judges text as an extension of a model that
            // already holds those declarations, so feeding it the model's
            // own text yields `Cannot redefine` for every source in the file
            // and aborts before the appended fragment is ever classified --
            // which is a bypass rather than a stricter check.
            //
            // What closes the continuation hole instead is the gate refusing
            // when it could not parse what it was given (see
            // assertNoRestrictedConstructs). A continuation fragment is a
            // syntax error on its own, and that is now a refusal rather than
            // silence read as approval.
            // A document's whole-text gate runs on the text that compiles, after the per-cell access phase.
            if (!documentCandidate) {
               await refuseConstructs(baseModel, source ?? "", false);
            }
         }

         if (documentCandidate && source !== undefined) {
            const gate = gateModel;
            const exact = hasExactGateModel;
            const base = appendBase;
            if (!base) throw new Error("append base model was not loaded");
            const baseModel = await base.getModel();
            // Syntactic and cheap, so it runs ahead of the cell reader; the compile-based gate runs once access is settled.
            refuseRenderTags(source);
            const result = await compileDocument({
               base,
               source,
               modelName,
               gates: {
                  text: (text) =>
                     gate && exact
                        ? denyHiddenAsNotQueryable(
                             () => {
                                gate.assertQueryBoundaryEarly(
                                   undefined,
                                   undefined,
                                   text,
                                );
                             },
                             () =>
                                gate.assertAuthorizedForText(
                                   text,
                                   givens ?? {},
                                ),
                          )
                        : Promise.resolve(),
                  compiled: (runnable) =>
                     gate
                        ? denyHiddenAsNotQueryable(
                             () => gate.assertCompiledTargetQueryable(runnable),
                             () =>
                                exact
                                   ? gate.assertAuthorizedForRunnable(
                                        runnable,
                                        givens ?? {},
                                     )
                                   : gate.assertAuthorizedFromCompiledRunnable(
                                        runnable,
                                        givens ?? {},
                                     ),
                          )
                        : Promise.resolve(),
                  constructs: (text) => refuseConstructs(baseModel, text, true),
                  document: (text) => refuseConstructs(baseModel, text, false),
                  nameVisible: (query, definitions) => {
                     gate?.assertTextNameVisible(query, definitions);
                  },
                  boundaryCompiled: async (
                     runnable,
                     compiledSource,
                     query,
                     definitions,
                  ) => {
                     gate?.assertQueryBoundaryCompiled(
                        compiledSource,
                        query,
                        definitions,
                     );
                     await gate?.assertDocumentJoinsQueryable(
                        `${definitions}\n${query}`,
                        runnable,
                     );
                  },
                  boundary: async (query, definitions) => {
                     gate?.assertQueryBoundaryEarly(
                        undefined,
                        undefined,
                        query,
                        definitions,
                     );
                     await gate?.assertDocumentJoinsQueryable(
                        `${definitions}\n${query}`,
                     );
                  },
               },
            });
            if (result) {
               return {
                  problems: result.problems.map((problem) => ({
                     ...problem,
                  })) as TaggedLogMessage[],
                  ...(result.document && { document: result.document }),
               };
            }
            // Not a readable document after all, so the ordinary compile runs and the whole text is judged as one.
            await refuseConstructs(baseModel, source, false);
            await runEarlyGate();
         }

         // Attempt to compile
         try {
            const modelMaterializer = runtime.loadModel(virtualUrl);
            const model = await modelMaterializer.getModel();

            // Resolve the final query's materializer once (if there is one).
            let queryMaterializer: ReturnType<
               typeof modelMaterializer.loadFinalQuery
            > | null = null;
            try {
               queryMaterializer = modelMaterializer.loadFinalQuery();
            } catch {
               // No runnable query (e.g. only source definitions) — nothing to
               // gate or extract. The early text gate ran, but it only REFUSES
               // a gate it cannot classify or graft: every expressible one is
               // deferred to the compiled backstop below, which needs a
               // runnable. Nothing is exposed by skipping it here, since
               // without a runnable there is no result and no SQL.
            }

            // Compiled-source backstops — run REGARDLESS of includeSql. They
            // gate the source the COMPILED final query actually reads, closing
            // the named-query and derivation indirection the early
            // surface-syntax gate cannot see. Compiling a gated source even
            // without SQL is a schema oracle
            // (field-not-found errors leak its columns), so this must not be
            // conditional on SQL extraction. (A `source: x is gated` alias
            // carries the gate: only a declaration of its OWN `#(authorize)`
            // replaces it, and caller text may not declare one.)

            // No boundary backstop here: /compile is exempt from the query
            // boundary by design (see the gate comment above). Only the
            // authorize backstop runs.

            // Authorize backstop (the *who* axis, 403). NOT guarded by
            // hasAuthorize(): that reads only top-level modelDef.contents
            // sources' OWN annotations, so a run target gated solely by what it
            // derives from is invisible to it and this backstop would silently
            // never run for such a model — the same bypass
            // assertAuthorizedForRunnable itself closes on the query path (see
            // model.ts assertAuthorizedForAllSources). The own-source probe and
            // derivation walk it runs are cheap no-ops for an ungated model.
            if (queryMaterializer && gateModel) {
               const materializer = queryMaterializer;
               await denyHiddenAsNotQueryable(
                  () => {
                     if (!hasExactGateModel) {
                        throw new NotQueryableError(
                           "Query target is not queryable.",
                        );
                     }
                     return gateModel.assertCompiledTargetQueryable(
                        materializer,
                        source,
                     );
                  },
                  () =>
                     hasExactGateModel
                        ? gateModel.assertAuthorizedForRunnable(
                             materializer,
                             givens ?? {},
                             callerRegion,
                          )
                        : // No region: this gate model is another file's
                          // namespace, so its source names cannot place a join.
                          gateModel.assertAuthorizedFromCompiledRunnable(
                             materializer,
                             givens ?? {},
                          ),
               );
            }

            // If includeSql is requested and compilation succeeded, attempt to extract SQL
            let sql: string | undefined;
            if (includeSql && queryMaterializer) {
               // A given value the compiled text's own filter types cannot read
               // is a bad request, as on the query route. Checked here, after
               // the gate and against the submitted text's givens rather than
               // the cached model's, because this is the only place /compile
               // binds given values.
               assertFilterGivensParse(
                  Array.from(model.givens.values()).map((g) =>
                     malloyGivenToApi(g as MalloyGiven),
                  ),
                  givens,
               );
               try {
                  sql = await queryMaterializer.getSQL({ givens });
               } catch (error) {
                  // A bad caller given (unknown name, wrong-typed value, finalized
                  // override, ...) surfaces as a Malloy `runtime-given-*` error.
                  // Map it to a 400 rather than silently omitting `sql` (which is
                  // indistinguishable from "no runnable query"). Duck-type on
                  // `.code`; let a MalloyError fall to the outer catch → problems.
                  // The `runtime-given-` prefix is pinned to Malloy's error codes
                  // (given_binding.ts / runtime.ts, same as model.ts) — if they're
                  // renamed upstream a bad given would silently revert to the omit
                  // branch below, so keep the two in sync.
                  const givenCode = (error as { code?: string })?.code;
                  if (
                     typeof givenCode === "string" &&
                     givenCode.startsWith("runtime-given-")
                  ) {
                     throw new BadRequestError(
                        error instanceof Error ? error.message : String(error),
                     );
                  }
                  if (error instanceof MalloyError) {
                     throw error;
                  }
                  // Otherwise the source may just not contain a runnable query
                  // (e.g. only source definitions) — omit the sql field.
               }
            }

            // If successful, return any non-fatal warnings
            const readerProblem = notebookReaderProblem(
               modelName,
               fullSource,
               model._modelDef,
               virtualUri,
            );
            // Its positions are in the concatenated file at "append", so the lint is for a whole file only.
            const lintProblems =
               scope === "append"
                  ? []
                  : notebookLintProblems(modelName, fullSource, virtualUri);
            return {
               problems: tagProblems([
                  ...model.problems,
                  ...(readerProblem ? [readerProblem] : []),
                  ...lintProblems,
               ]),
               sql,
            };
         } catch (thrown) {
            // If parsing/compilation fails, return the errors. The
            // translator's plain Error is one of them, not a server fault.
            const error = translatorMalloyError(thrown) ?? thrown;
            if (error instanceof MalloyError) {
               return {
                  problems: tagProblems([
                     ...error.problems,
                     ...(scope === "append"
                        ? []
                        : notebookLintProblems(
                             modelName,
                             fullSource,
                             virtualUri,
                          )),
                  ]),
               };
            }
            // If it's a system error (e.g. file not found), throw it up
            throw error;
         }
      });
   }

   public listApiConnections(): ApiConnection[] {
      return this.apiConnections;
   }

   public getApiConnection(connectionName: string): ApiConnection {
      const connection = this.apiConnections.find(
         (connection) => connection.name === connectionName,
      );
      if (!connection) {
         throw new ConnectionNotFoundError(
            `Connection ${connectionName} not found`,
         );
      }
      return connection;
   }

   /**
    * Replaces the destination list. Every entry is re-validated here, so this is
    * also the barrier that keeps an unvalidated destination off the environment
    * however it arrived — config file or request body.
    *
    * An entry that carries nothing but the fields a read reports is resolved
    * against the stored list first, so a read-modify-write of the environment
    * keeps the configs it was never shown.
    *
    * `rejectInvalid` picks what an entry we cannot use means. A config file or a
    * restored row is a source that cannot be asked to fix it, so the default
    * drops the entry and keeps serving. A request body can be refused, and is:
    * see {@link processStorageDestinationsOrThrow}.
    */
   public setStorageDestinations(
      storageDestinations: ApiConnection[],
      { rejectInvalid = false }: { rejectInvalid?: boolean } = {},
   ): void {
      const previous = this.destinations;
      // Resolved before validation so an entry that legitimately carries no
      // config of its own — the "keep this one" reference — is validated as the
      // stored destination it names, not as the bare reference.
      const requested = Array.isArray(storageDestinations)
         ? storageDestinations.map((destination) =>
              this.resolveDestinationReference(destination),
           )
         : storageDestinations;
      // Throws before anything is assigned, so a refused update leaves the
      // environment exactly as it was.
      this.destinations = rejectInvalid
         ? processStorageDestinationsOrThrow(requested)
         : processStorageDestinations(requested);
      // An explicit set makes the list authoritative again: whatever could not be
      // read before, this is now the set the store should be reconciled to.
      this.destinationsAuthoritative = true;

      // Nothing to swap when the resolved list describes the same destinations.
      // An orchestrator that reconciles by re-pushing its whole desired state on
      // a loop would otherwise re-attach every destination on every cycle and
      // drop the serve shapes compiled against them, so the comparison ignores
      // list order and config key order, neither of which changes what a
      // destination is.
      //
      // "Same as before" only means there is nothing to do once something has
      // been built. On the constructor's call both lists are empty for every
      // environment with no destinations, so skipping on equality alone would
      // leave the config unassigned for the common case, not the rare one.
      if (
         this.destinationMalloyConfig &&
         storageDestinationsEqual(previous, this.destinations)
      ) {
         return;
      }

      this.rebuildDestinationMalloyConfig();
      // Quiet for the overwhelmingly common case of an environment with no
      // destinations at all, loud for every transition that matters, including
      // one that empties the list.
      if (previous.length > 0 || this.destinations.length > 0) {
         logger.info(
            `Environment ${this.environmentName} has ${this.destinations.length} storage destination(s)`,
            { destinations: this.destinations.map((d) => d.name) },
         );
      }
   }

   /**
    * Substitutes the stored destination for an entry that names one and carries
    * no config of its own. Anything carrying a config is returned untouched and
    * replaces what is stored; a reference to a name that is not stored is
    * returned untouched too, and then fails validation like any config-less
    * entry.
    */
   private resolveDestinationReference(
      destination: ApiConnection,
   ): ApiConnection {
      if (!destination || typeof destination !== "object") {
         return destination;
      }
      const carriesOnlyReportedFields = Object.keys(destination).every(
         (field) => DESTINATION_READ_FIELDS.has(field),
      );
      if (!carriesOnlyReportedFields) {
         return destination;
      }
      return (
         this.destinations.find((stored) => stored.name === destination.name) ??
         destination
      );
   }

   /**
    * Give a freshly loaded package the connections its materialization serve
    * shapes compile against — this environment's destinations, resolved live so a
    * destination-list swap propagates without a package reload.
    *
    * Deliberately a push rather than a `Package.create` argument: the package
    * config a model compiles against must never contain a destination, so this is
    * the only route by which one reaches a compile at all, and it feeds only the
    * synthetic serve shape. Missing it costs serve routing (queries fall back to
    * live), never correctness — so it runs before serve bindings are pushed,
    * which is what routing actually requires.
    */
   private attachDestinationServeConfig(_package: Package): void {
      _package.setServeDestinationConfig(() =>
         this.getStorageDestinationMalloyConfig(),
      );
   }

   /**
    * (Re)assemble the destination connections after the list changed, draining
    * the previous generation's handles on the same delay a connection-generation
    * swap uses.
    *
    * A failure here is not fatal to the environment: destinations are an add-on,
    * and an environment that cannot assemble them still serves its packages —
    * builds refuse and materialized queries fall back to live.
    */
   private rebuildDestinationMalloyConfig(): void {
      const previous = this.destinationMalloyConfig;
      try {
         // Rooted apart from the connections' files so a destination can never
         // share a pooled DuckDB instance with a connection of the same name —
         // see STORAGE_DESTINATIONS_DIR. Created here because the
         // directory has to exist before the first lookup opens a database in it.
         const destinationRoot = storageDestinationRoot(this.environmentPath);
         if (this.destinations.length > 0) {
            fs.mkdirSync(destinationRoot, { recursive: true });
         }
         this.destinationMalloyConfig = buildEnvironmentMalloyConfig(
            this.destinations,
            destinationRoot,
         );
      } catch (error) {
         logger.error(
            `Failed to assemble storage destinations for environment ${this.environmentName}; serving without them`,
            { error },
         );
         this.destinationMalloyConfig = buildEnvironmentMalloyConfig(
            [],
            storageDestinationRoot(this.environmentPath),
         );
      }
      if (previous && previous !== this.destinationMalloyConfig) {
         this.retireConnectionGeneration(
            `environment ${this.environmentName} destinations`,
            () => previous.releaseConnections(),
         );
         // A loaded model memoizes the serve shape it compiled, and that shape
         // holds the connections of the generation just retired — which are
         // released once the drain elapses. The memo is keyed on the BINDING set,
         // so a destination change does not change the key and the stale shape
         // would be reused until the package reloaded: every routed query for it
         // failing over to live, permanently and quietly. Dropped here, after the
         // swap, so the next query recompiles against the config now installed.
         this.invalidateServeShapes();
      }
   }

   /**
    * Drop every loaded model's memoized materialization serve shape. Cheap: the
    * next routed query recompiles one, and a package with no `storage=` bindings
    * has none to drop.
    */
   private invalidateServeShapes(): void {
      for (const _package of this.packages.values()) {
         _package.invalidateServeShapes();
      }
   }

   /**
    * The connections a materialization serve shape may compile against. Separate
    * from {@link getEnvironmentMalloyConfig} — which is what a package's models
    * fall through to — so the two compiles resolve disjoint name sets. Handing
    * out the same object for both is exactly the mistake this split exists to
    * make impossible.
    */
   public getStorageDestinationMalloyConfig() {
      return this.destinationMalloyConfig.malloyConfig;
   }

   /**
    * Records that this environment's stored destinations could not be read, so
    * {@link listStorageDestinations} is a fallback rather than the
    * authoritative set. Callers that reconcile storage must not prune against it.
    */
   public markStorageDestinationsUnknown(): void {
      this.destinationsAuthoritative = false;
   }

   /** See {@link destinationsAuthoritative}. */
   public hasAuthoritativeStorageDestinations(): boolean {
      return this.destinationsAuthoritative;
   }

   /**
    * The destinations configured for this environment, with their configs.
    * Deliberately not exposed by any controller: a destination config carries
    * warehouse credentials and the destination has no endpoint of its own, so
    * nothing can fetch or probe one.
    */
   public listStorageDestinations(): ApiConnection[] {
      return this.destinations;
   }

   public hasStorageDestination(destinationName: string): boolean {
      return this.destinations.some(
         (destination) => destination.name === destinationName,
      );
   }

   /**
    * Resolves a storage destination by name. Never falls back to the
    * connection list: a `storage=` build naming a destination that is not
    * configured must fail rather than write into a same-named connection, which
    * would be the tenant's own warehouse.
    */
   public getStorageDestination(destinationName: string): ApiConnection {
      const destination = this.destinations.find(
         (destination) => destination.name === destinationName,
      );
      if (!destination) {
         throw new DestinationNotFoundError(
            `Storage destination ${destinationName} not found`,
         );
      }
      return destination;
   }

   public async getMalloyConnection(connectionName: string) {
      return this.malloyConfig.malloyConfig.connections.lookupConnection(
         connectionName,
      );
   }

   public getEnvironmentMalloyConfig() {
      return this.malloyConfig.malloyConfig;
   }

   public async runConnectionUpdateExclusive<T>(
      fn: () => Promise<T>,
   ): Promise<T> {
      return this.connectionMutex.runExclusive(fn);
   }

   private retireConnectionGeneration(
      label: string,
      releaseConnections: () => Promise<void>,
   ): void {
      const generation: RetiredConnectionGeneration = {
         label,
         releaseConnections,
      };
      generation.timer = setTimeout(() => {
         void this.releaseRetiredConnectionGeneration(generation);
      }, RETIRED_CONNECTION_DRAIN_MS);
      (
         generation.timer as ReturnType<typeof setTimeout> & {
            unref?: () => void;
         }
      ).unref?.();
      this.retiredConnectionGenerations.add(generation);
   }

   private async releaseRetiredConnectionGeneration(
      generation: RetiredConnectionGeneration,
   ): Promise<void> {
      if (!this.retiredConnectionGenerations.delete(generation)) return;

      if (generation.timer) {
         clearTimeout(generation.timer);
      }

      try {
         await generation.releaseConnections();
      } catch (error) {
         logger.error(
            `Error releasing retired connection generation ${generation.label}`,
            { error },
         );
      }
   }

   private async releaseAllRetiredConnectionGenerations(): Promise<void> {
      await Promise.all(
         [...this.retiredConnectionGenerations].map((generation) =>
            this.releaseRetiredConnectionGeneration(generation),
         ),
      );
   }

   /**
    * Snapshot of the packages currently loaded in memory (does not trigger a
    * load or reload). Used by the standalone materialization scheduler to sweep
    * only already-loaded packages — a not-yet-loaded package is simply not
    * scheduled until something else loads it, so the scheduler never forces a
    * load of its own.
    */
   public getLoadedPackages(): Package[] {
      // One per package: its unversioned slot, or the version `latest` points
      // at. Other loaded versions are serving only requests that name them.
      return [...this.packages.entries()]
         .filter(
            ([key, pkg]) =>
               !key.includes("@") ||
               this.peekPackage(pkg.getPackageName()) === pkg,
         )
         .map(([, pkg]) => pkg);
   }

   /**
    * The packages this environment holds, as the API describes them: one
    * entry per package, each as a request that names no version sees it (the
    * unversioned package, or its `latest` version).
    *
    * A package whose compiled copy is resident is described from that copy
    * without taking its lock, so a reload in progress (which holds the lock
    * and has flipped the registered status to LOADING) is listed with
    * `status.loading` rather than hidden: the previous copy is still what
    * answers queries, and a listing that omitted it would read as the package
    * having left this server. A registered package that is not resident is
    * loaded here, as before, which is what brings an environment's configured
    * packages into memory on first listing.
    *
    * A package that is loading for the first time has nothing compiled to
    * describe. It is listed, as its name and `status` alone, only when
    * `includeLoading` is set; `GET /status?includeLoading=true` sets it so an
    * orchestrator that reads `status` can tell a load it dispatched from a
    * package that is absent. Every other listing leaves it out: a listed
    * package has always meant one that can serve here, and a consumer that
    * reads the listing that way would otherwise route to a copy still
    * downloading or compiling.
    *
    * With `everyLoadedVersion`, a versioned package also contributes an entry
    * for every other version currently loaded, each carrying its own
    * `versionId`. That is the `/status` view: an orchestrator reconciles what a
    * server is serving from it, so it must name every loaded version.
    */
   public async listPackages(
      options: { everyLoadedVersion?: boolean; includeLoading?: boolean } = {},
   ): Promise<ApiPackage[]> {
      logger.debug("Listing packages", {
         environmentPath: this.environmentPath,
      });
      try {
         const names = new Set<string>([
            ...this.packageStatuses.keys(),
            ...(options.includeLoading ? this.loadsInFlight.keys() : []),
         ]);
         const packageMetadata = await Promise.all(
            Array.from(names).map(async (packageName) => {
               const registered = this.packageStatuses.get(packageName)?.status;
               if (registered === PackageStatus.UNLOADING) {
                  return undefined;
               }
               try {
                  const versionIndex = this.packageVersions.get(packageName);
                  if (versionIndex && versionIndex.latest === null) {
                     // Published, but nothing is latest yet (explicit
                     // promotion): listed, with no version to describe.
                     return {
                        resource: `${API_PREFIX}/environments/${this.environmentName}/packages/${packageName}`,
                        name: packageName,
                        versionId: null,
                        latestVersion: null,
                        status: this.describePackageStatus(packageName),
                     } as ApiPackage;
                  }
                  // The copy a request naming no version reaches: a versioned
                  // package's `latest`, or the unversioned package's own.
                  const resident = this.peekPackage(packageName);
                  let metadata: ApiPackage | undefined;
                  if (resident !== undefined) {
                     metadata = resident.getPackageMetadata();
                  } else if (this.loadsInFlight.has(packageName)) {
                     if (!options.includeLoading) {
                        return undefined;
                     }
                     metadata = { name: packageName };
                  } else if (registered === PackageStatus.LOADING) {
                     // Registered as loading with nothing in flight: a load that
                     // was interrupted before it settled. Nothing here can be
                     // described, and loading it from this read path would
                     // repeat the interrupted work on every listing.
                     return undefined;
                  } else {
                     metadata = (
                        await this.getPackage(packageName, false)
                     ).getPackageMetadata();
                  }
                  metadata.name = packageName;
                  metadata.status = this.describePackageStatus(packageName);
                  return metadata;
               } catch (error) {
                  logger.error(
                     `Failed to load package: ${packageName} due to : ${error}`,
                  );
                  // Directory did not contain a valid package.json file -- therefore, it's not a package.
                  // Or it timed out
                  // Redact before this reaches getStatus: compiling a model
                  // resolves the package's connections, so a Postgres/DuckLake
                  // ATTACH failure surfaces here carrying the connection string
                  // (connection.ts builds it, and redacts it before logging for
                  // the same reason). A log line was the old destination; this
                  // one is an HTTP response body.
                  //
                  // redactPgSecrets covers both keyword-form `password=` (what
                  // buildPgConnectionString emits) and URI userinfo
                  // (`postgres://user:pass@host`), so a URL-form connectionString
                  // supplied verbatim in config is redacted here too.
                  this.failedPackages.set(
                     packageName,
                     redactPgSecrets(
                        error instanceof Error ? error.message : String(error),
                     ),
                  );
                  return undefined;
               }
            }),
         );
         const finalMetadata = packageMetadata.filter(
            (metadata): metadata is ApiPackage => metadata !== undefined,
         );

         if (options.everyLoadedVersion) {
            const listed = new Set(
               finalMetadata.map((m) => `${m.name}@${m.versionId ?? ""}`),
            );
            for (const [key, pkg] of this.packages) {
               const versionId = pkg.getVersionId();
               if (!key.includes("@") || versionId === undefined) continue;
               if (listed.has(`${pkg.getPackageName()}@${versionId}`)) continue;
               // A loaded version is resident by definition; its package's
               // in-flight loads are reported on the package's own entry.
               finalMetadata.push({
                  ...pkg.getPackageMetadata(),
                  status: { serving: true, loading: false },
               });
            }
         }

         return finalMetadata;
      } catch (error) {
         logger.error("Error listing packages", { error });
         console.error(error);
         throw error;
      }
   }

   /**
    * The `Package.status` the API reports for a package here: whether a
    * compiled copy is resident to answer queries, and whether a load,
    * reinstall or recompile is in progress. Read without the package lock, so
    * it answers during the operations it describes.
    */
   /**
    * The server's record of where `packageName` was installed from, kept
    * outside the package directory (see {@link PACKAGE_INSTALL_RECORDS_DIR}).
    * Joined under the environment root the way every other path built from a
    * package name here is, so a name that escaped validation cannot name a
    * file elsewhere.
    */
   private installRecordPath(packageName: string): string {
      return safeJoinUnderRoot(
         this.environmentPath,
         PACKAGE_INSTALL_RECORDS_DIR,
         `${packageName}.json`,
      );
   }

   /**
    * Resolve once no load, reinstall or recompile of the package is in flight
    * here. A caller that must decide against the copy that will be resident
    * (a PATCH comparing its `location` with the installed one) waits here
    * first, so it reads the outcome of the install rather than the copy the
    * install is about to replace, or has just failed to.
    */
   public async awaitPackageLoads(packageName: string): Promise<void> {
      let entry = this.loadsInFlight.get(packageName);
      while (entry !== undefined) {
         await entry.settled;
         entry = this.loadsInFlight.get(packageName);
      }
   }

   public describePackageStatus(packageName: string): ApiPackageStatus {
      const inFlight = this.loadsInFlight.get(packageName);
      return {
         // A versioned package is served by its `latest` version's copy.
         serving: this.peekPackage(packageName) !== undefined,
         loading: inFlight !== undefined,
         ...(inFlight !== undefined
            ? { loadingSince: new Date(inFlight.since).toISOString() }
            : {}),
      };
   }

   /**
    * Run `fn` with the package marked as loading here for its duration (see
    * `loadsInFlight`). Every path that allocates a new compiled copy of a
    * package, or recompiles the one it has, runs inside this, so
    * `Package.status.loading` is true from the moment the work is accepted,
    * download included, until it has settled or rolled back.
    */
   private async trackPackageLoad<T>(
      packageName: string,
      fn: () => Promise<T>,
   ): Promise<T> {
      const current = this.loadsInFlight.get(packageName);
      let settle = current?.settle;
      const settled =
         current?.settled ??
         new Promise<void>((resolve) => {
            settle = resolve;
         });
      this.loadsInFlight.set(packageName, {
         count: (current?.count ?? 0) + 1,
         since: current?.since ?? Date.now(),
         settled,
         settle: settle!,
      });
      try {
         return await fn();
      } finally {
         const entry = this.loadsInFlight.get(packageName);
         if (entry !== undefined && entry.count > 1) {
            entry.count -= 1;
         } else {
            this.loadsInFlight.delete(packageName);
            entry?.settle();
         }
      }
   }

   /**
    * One mutex per package name; never replace after create — replacing
    * would allow two loads of the same package to run in parallel and
    * race on the canonical directory.
    *
    * `deletePackage` intentionally leaves the entry behind: a
    * subsequent re-install must serialize against any straggling
    * readers from the deleted generation that are still inside
    * `withPackageLock`. The map therefore grows by the count of
    * *distinct* keys the environment has ever served: one per package name,
    * plus one per published version (`name@dirName`), never by install
    * churn. For the publisher's expected workload (config-declared
    * packages, occasional ad-hoc additions, versions retired by archive
    * rather than by delete) this is bounded in practice. Long-lived deployments that create and
    * delete unique package names indefinitely would need an explicit
    * sweep; we'll add one if/when that pattern appears.
    */
   private getOrCreatePackageMutex(packageName: string): Mutex {
      let packageMutex = this.packageMutexes.get(packageName);
      if (packageMutex === undefined) {
         packageMutex = new Mutex();
         this.packageMutexes.set(packageName, packageMutex);
      }
      return packageMutex;
   }

   /**
    * Run `fn` while holding the per-package mutex. This is the single
    * synchronization primitive that protects a package directory: every
    * code path that mutates `{environmentPath}/{packageName}/` or reads
    * from disk under it must serialize through this lock. See the lock
    * ordering note above the `packageMutexes` field for the wider
    * invariant.
    *
    * `async-mutex` is **not reentrant** — `fn` must not call any other
    * method that calls `withPackageLock` on the same package, or it will
    * deadlock. Use the `_xxxLocked` variants below in that case.
    */
   public async withPackageLock<T>(
      packageName: string,
      fn: () => Promise<T>,
   ): Promise<T> {
      assertSafePackageName(packageName);
      return this.getOrCreatePackageMutex(packageName).runExclusive(fn);
   }

   /**
    * Read a model file's text, or undefined when there is none. The caller
    * holds the package lock.
    */
   private async _readModelFileLocked(
      target: string,
   ): Promise<string | undefined> {
      try {
         return await fs.promises.readFile(target, "utf8");
      } catch (error) {
         if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
         return undefined;
      }
   }

   /**
    * Put text at a model path, atomically: a sibling temporary file is renamed
    * over the target, so a reader never sees a half-written file and a crash
    * leaves either the old text or the new. The caller holds the package lock.
    */
   private async _writeModelFileLocked(
      target: string,
      source: string,
   ): Promise<void> {
      await fs.promises.mkdir(path.dirname(target), { recursive: true });
      const temporary = `${target}.${crypto.randomUUID()}.tmp`;
      await fs.promises.writeFile(temporary, source, "utf8");
      await fs.promises.rename(temporary, target);
   }

   /**
    * Write one model file and reload the package behind it, as one step that
    * either happens or does not.
    *
    * Everything here — the precondition, the write, the reload, the caller's
    * check on the reloaded package, and the restore when that check fails —
    * runs inside a single hold of the package lock. That is the point of the
    * method. Doing it as separate locked calls leaves two windows open: a
    * second writer can land between the write and the reload, so the reload
    * serves their text under this caller's success; and it can land between a
    * failed reload and the restore, so the restore reverts THEIR write instead
    * of this one. Both end with the package serving something nobody asked
    * for, and neither is visible to the caller that lost.
    *
    * `check` refuses by throwing, and runs against the file's current text
    * (undefined when there is none) and the package as loaded before the
    * write (undefined when it is not) — so two saves racing on one file cannot
    * both pass their precondition. `verify` runs against the reloaded package
    * and likewise refuses by throwing; a refusal puts the previous text back
    * (or removes the file, when it is new), reloads again, and raises
    * {@link WriteRolledBackError}.
    *
    * Anything that must NOT be under the lock — compiling above all — belongs
    * before this call: the lock is not reentrant, and compiling the proposed
    * text does not depend on what is on disk.
    */
   public async writeModelFileTransactional<T>(
      packageName: string,
      modelPath: string,
      source: string,
      check: (current: string | undefined, loaded: Package | undefined) => void,
      verify: (reloaded: Package) => Promise<T>,
   ): Promise<{ previous: string | undefined; verified: T }> {
      assertSafePackageName(packageName);
      assertSafeRelativeModelPath(modelPath);
      this.assertNotVersioned(packageName, "have its files written");
      return this.withPackageLock(packageName, async () => {
         // Again under the lock: a first versioned publish may have committed
         // since the check above, and its trees are not this file's.
         this.assertNotVersioned(packageName, "have its files written");
         const target = safeJoinUnderRoot(
            this.environmentPath,
            packageName,
            modelPath,
         );
         const previous = await this._readModelFileLocked(target);
         check(previous, this.packages.get(packageName));
         await this._writeModelFileLocked(target, source);
         try {
            // The locked form, because this whole callback already holds the
            // mutex that `getPackage` would take.
            const reloaded = await this._loadOrGetPackageLocked(
               packageName,
               true,
            );
            return { previous, verified: await verify(reloaded) };
         } catch (error) {
            if (previous !== undefined)
               await this._writeModelFileLocked(target, previous);
            else await fs.promises.rm(target, { force: true });
            await this._loadOrGetPackageLocked(packageName, true).catch(
               () => undefined,
            );
            logger.warn("Dashboard write rolled back", {
               packageName,
               modelPath,
               error,
            });
            // Only a refusal worded for the caller is echoed; anything else can carry a server path.
            const reason =
               error instanceof WriteVerifyError ? error.message : undefined;
            throw new WriteRolledBackError(
               `The package did not reload with the new \`${modelPath}\`` +
                  `${reason ? ` (${reason})` : ""}, so the previous text was ` +
                  `put back and nothing changed.`,
               { cause: error },
            );
         }
      });
   }

   private allocateStagingPath(packageName: string): string {
      return safeJoinUnderRoot(
         this.environmentPath,
         STAGING_DIR_NAME,
         `${packageName}-${crypto.randomUUID()}`,
      );
   }

   private allocateRetiredPath(packageName: string): string {
      return safeJoinUnderRoot(
         this.environmentPath,
         RETIRED_DIR_NAME,
         `${packageName}-${crypto.randomUUID()}`,
      );
   }

   /**
    * Best-effort sweep of `.staging/` and `.retired/` left over from a
    * previous run (crash, OOM, etc). Safe because both dirs are managed
    * exclusively by `installPackage` / `deletePackage`; no in-flight
    * operation in this process can be using them yet.
    *
    * Static + path-as-parameter on purpose: the sink path here must
    * derive from the validated factory argument, not from `this`,
    * because CodeQL's path-injection query conservatively treats every
    * field on this class as tainted (other methods on the same class
    * receive request-derived `packageName` values).
    */
   public static async sweepStaleInstallDirs(
      environmentPath: string,
   ): Promise<void> {
      assertSafeEnvironmentPath(environmentPath);
      for (const dirName of [STAGING_DIR_NAME, RETIRED_DIR_NAME]) {
         const dir = safeJoinUnderRoot(environmentPath, dirName);
         // Inline sanitizer barriers in the precise shape CodeQL's
         // `js/path-injection` query recognises (regex-test +
         // `indexOf("..") !== -1` guard) so the sink right below is
         // covered even when the call chain feeding `environmentPath`
         // is taint-tracked from an HTTP request handler.
         if (dir.indexOf("..") !== -1) continue;
         if (path.basename(dir) !== dirName) continue;
         try {
            await fs.promises.rm(dir, { recursive: true, force: true });
         } catch (err) {
            logger.warn(`Failed to sweep stale ${dirName} dir at ${dir}`, {
               error: err,
            });
         }
      }
   }

   /**
    * Attach (or detach with `null`) the memory governor that gates new
    * package allocations. The single instance is owned by the
    * EnvironmentStore and propagated to every Environment so the
    * back-pressure decision is process-wide.
    */
   public setMemoryGovernor(governor: PackageMemoryGovernor | null): void {
      this.memoryGovernor = governor;
   }

   /**
    * Attach (or detach with `null`) a callback run each time a package
    * enters this environment's package map: at boot, on add, on install, and
    * on reload. The callback must only schedule work; see
    * {@link notifyPackageLoaded}.
    */
   public setPackageLoadedHook(hook: ((pkg: Package) => void) | null): void {
      this.packageLoadedHook = hook;
   }

   /**
    * Tell the hook a package is now served. Called straight after each
    * `this.packages.set`. A throwing hook is logged and swallowed: an
    * observer of the load must never fail it.
    */
   private notifyPackageLoaded(pkg: Package): void {
      try {
         this.packageLoadedHook?.(pkg);
      } catch (error) {
         logger.warn("Package-loaded hook failed", {
            environmentName: this.environmentName,
            packageName: pkg.getPackageName(),
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }

   /**
    * Inject the resolver that fetches a package's latest persisted
    * materialization entries — both tiers (see {@link storageBindingResolver}).
    */
   public setStorageBindingResolver(
      resolver: (packageName: string) => Promise<Record<string, ManifestEntry>>,
   ): void {
      this.storageBindingResolver = resolver;
   }

   /**
    * Re-establish a package's serve routing from its latest persisted
    * materialization when it (re)loads, so serving survives a restart — both
    * tiers, not just one. Serve bindings are otherwise in-memory, set only by a
    * build's post-run auto-load, so a worker restart silently reverted a
    * materialized source to serving live until the next build. Runs beside
    * {@link bindManifestIfConfigured} — the same "bind serve state on load" step.
    * Best-effort: a lookup failure logs and leaves the package serving live (a
    * subsequent build will bind it).
    *
    * Both tiers are restored from the SAME persisted manifest, split by
    * {@link splitManifestEntries}, mirroring {@link bindManifest}:
    *  - **colocated** (same-connection) → {@link Package.bindColocatedServeManifest},
    *    applied as a per-query `buildManifest` override at serve time (no
    *    recompile — the load-time compile already produced flag-carrying models).
    *    Independent of `PERSIST_STORAGE_MODE`: colocated is the v0 path and is
    *    not gated by the storage kill switch (a plain `#@ persist` materializes
    *    and serves even when the tier is `off`), so its routing is restored
    *    regardless of mode.
    *  - **storage=** (cross-connection) → {@link Package.bindStorageServeBindings},
    *    the virtual-source transform. Restored only when the tier is not `off`:
    *    storage serve routing requires the tier on, so an off deployment skips it.
    *
    * Skipped entirely when the package has a bound `manifestLocation`: the two
    * binding producers (this local store and the host's fetched manifest, applied
    * by {@link bindManifest}) are mutually exclusive by manifest presence, and the
    * host is authoritative when one is set. An orchestrated publisher STILL
    * persists its own local materialization records (the host triggers builds
    * through it), so without this guard the local rebind — which runs AFTER
    * {@link bindManifestIfConfigured} on load — would overwrite the host's
    * bindings with a possibly-staler local generation.
    */
   private async rebindServeBindingsFromLocalStore(
      pkg: Package,
   ): Promise<void> {
      if (!this.storageBindingResolver) return;
      // Host-authoritative: a bound manifestLocation means bindManifest already
      // supplied both tiers' serve bindings from the host's manifest; the local
      // store must not clobber them (mutually-exclusive binding sources).
      if (pkg.getPackageMetadata().manifestLocation) return;
      const packageName = pkg.getPackageName();
      try {
         const rawEntries = await this.storageBindingResolver(packageName);
         if (Object.keys(rawEntries).length === 0) return;
         const { tableNameManifest, storageEntries } = splitManifestEntries(
            rawEntries,
            `local store (package ${packageName})`,
         );
         // Colocated: restore regardless of PERSIST_STORAGE_MODE (v0 path, not
         // gated by the storage kill switch).
         if (Object.keys(tableNameManifest).length > 0) {
            pkg.bindColocatedServeManifest(tableNameManifest);
         }
         // Storage=: only meaningful when the tier is not off (serve routing to
         // the external store requires it). Ships dark otherwise.
         if (
            getPersistStorageMode() !== "off" &&
            Object.keys(storageEntries).length > 0
         ) {
            pkg.bindStorageServeBindings(storageEntries);
         }
      } catch (err) {
         logger.warn(
            "Failed to rebind serve bindings from local store on load",
            {
               packageName,
               error: err instanceof Error ? err.message : String(err),
            },
         );
      }
   }

   /**
    * Choke-point check called from every code path that would allocate
    * a *new* package into the in-memory map (lazy load on cache miss,
    * explicit reload, `addPackage`). Throws HTTP 503 when the governor
    * is back-pressured; cheap no-op when the governor is unset or
    * happy.
    *
    * `allowAdmission` is the documented opt-out for read paths that
    * genuinely cannot tolerate 503s. None of the current callers set
    * it; the parameter exists so a future caller (e.g. a
    * health/warmup probe) can self-document its bypass intent.
    */
   private assertCanAdmitNewPackage(
      packageName: string,
      reason: string,
      allowAdmission: boolean,
   ): void {
      if (allowAdmission) return;
      if (!this.memoryGovernor?.isBackpressured()) return;
      // Increment *before* throwing so the metric ticks even on
      // the not-uncommon "caught and swallowed" path. The label
      // shape mirrors `assertCanAdmitQuery` so a dashboard panel
      // can sum both rejection kinds by environment.
      getPackageAdmissionRejectionsCounter().add(1, {
         environment: this.environmentName,
         reason,
      });
      throw new PackageAdmissionRefusedError(
         `Publisher is under memory pressure and cannot ${reason} (package "${packageName}", environment "${this.environmentName}"). Retry after the server's memory usage drops below the low-water mark (PUBLISHER_MEMORY_LOW_WATER_FRACTION of PUBLISHER_MAX_MEMORY_BYTES), or raise PUBLISHER_MAX_MEMORY_BYTES if you have headroom.`,
      );
   }

   /**
    * Reject incoming queries with HTTP 503 when the memory governor
    * has tripped its high-water mark. Used by every query controller
    * (connection SQL, model query, notebook cell, MCP `execute_query`)
    * to shed load before the query runs — complementing
    * {@link assertCanAdmitNewPackage}, which only fires on cache-miss
    * package loads and so leaves already-loaded packages fully
    * queryable under pressure. With this in place, "back-pressured"
    * means "no new work of any kind" until the governor's low-water
    * mark is crossed.
    *
    * Cheap O(1) boolean read; no allocation when happy.
    */
   public assertCanAdmitQuery(): void {
      if (!this.memoryGovernor?.isBackpressured()) return;
      // Tick first so the counter reflects every rejection even
      // when the controller's catch block swallows the error (e.g.
      // an MCP tool surfaces it as a content payload rather than
      // letting it bubble to the HTTP error mapper).
      getQueryAdmissionRejectionsCounter().add(1, {
         environment: this.environmentName,
      });
      throw new ServiceUnavailableError(
         `Publisher is under memory pressure and cannot accept new queries (environment "${this.environmentName}"). Retry after the server's memory usage drops below the low-water mark (PUBLISHER_MEMORY_LOW_WATER_FRACTION of PUBLISHER_MAX_MEMORY_BYTES), or raise PUBLISHER_MAX_MEMORY_BYTES if you have headroom.`,
      );
   }

   /**
    * The package instance currently being served under `name`, or undefined.
    * Never loads from disk, so a caller can ask "is this still served?"
    * without bringing back a package that was unloaded or deleted.
    */
   public peekPackage(name: string): Package | undefined {
      const index = this.packageVersions.get(name);
      if (!index) return this.packages.get(name);
      // A versioned package is "served" as the version a nameless request
      // reaches; the others are loaded only for requests that name them.
      const latest = index.latest
         ? index.versions.get(index.latest)
         : undefined;
      return latest
         ? this.packages.get(`${name}@${latest.dirName}`)
         : undefined;
   }

   /**
    * Replace what this environment knows of a package's published versions:
    * every version the registry holds for it, and its `latest`. The caller has
    * just read or written the registry; this only mirrors it.
    */
   public setPackageVersions(
      packageName: string,
      latest: string | null,
      versions: PackageVersion[],
   ): void {
      assertSafePackageName(packageName);
      this.packageVersions.set(packageName, {
         latest,
         versions: new Map(versions.map((v) => [v.version, v])),
      });
   }

   /** Forget a package's versions, as when the package itself is deleted. */
   public clearPackageVersions(packageName: string): void {
      this.packageVersions.delete(packageName);
   }

   /** Whether the package has published versions (and so no unversioned slot). */
   public isVersionedPackage(packageName: string): boolean {
      return this.packageVersions.has(packageName);
   }

   /**
    * The package's published versions, highest precedence first, and its
    * `latest`. Empty, with no latest, for an unversioned package.
    */
   public listPackageVersions(packageName: string): {
      latest: string | null;
      versions: PackageVersion[];
   } {
      const index = this.packageVersions.get(packageName);
      if (!index) return { latest: null, versions: [] };
      const versions = [...index.versions.values()].sort(
         (a, b) =>
            compareSemver(b.version, a.version) ||
            b.createdAt.getTime() - a.createdAt.getTime(),
      );
      return { latest: index.latest, versions };
   }

   /**
    * Resolve a request for `packageName`, optionally naming a `versionId`, to
    * the one slot that serves it.
    *
    * An unversioned package resolves to its single slot when no version is
    * named, exactly as before versions existed, and refuses a named one: it
    * has none, and serving its only tree under a version it never published
    * would answer a question nobody asked. A versioned package resolves an
    * omitted version to its `latest`. Either way a value that is not a
    * semantic version is 400 VERSION_ID_INVALID, an unknown version is 404
    * VERSION_NOT_FOUND and an archived one 410 VERSION_ARCHIVED.
    */
   public resolveSlot(packageName: string, versionId?: string): PackageSlot {
      assertSafePackageName(packageName);
      // Checked before anything is looked up, so a caller can tell a value it
      // got wrong from a version that does not exist.
      if (versionId) assertVersionIdFormat(packageName, versionId);
      const index = this.packageVersions.get(packageName);
      if (!index) {
         if (versionId) {
            throw new PackageVersionError(
               "VERSION_NOT_FOUND",
               `Package ${packageName} has no published versions, so it has no version ${versionId}.`,
            );
         }
         return {
            name: packageName,
            key: packageName,
            path: safeJoinUnderRoot(this.environmentPath, packageName),
         };
      }
      const wanted = versionId || index.latest;
      if (!wanted) {
         throw new PackageVersionError(
            "VERSION_NOT_FOUND",
            `Package ${packageName} has no latest version. Name one with versionId, or set latest.`,
         );
      }
      const version = index.versions.get(wanted);
      if (!version) {
         throw new PackageVersionError(
            "VERSION_NOT_FOUND",
            `Package ${packageName} has no version ${wanted}.`,
         );
      }
      if (version.archiveStatus === "archive") {
         throw new PackageVersionError(
            "VERSION_ARCHIVED",
            `Version ${wanted} of package ${packageName} is archived. Unarchive it to serve it again.`,
         );
      }
      return {
         name: packageName,
         version,
         key: `${packageName}@${version.dirName}`,
         // From the registry row, not the request: a version's files are
         // where its row says, and the row was found under this same name.
         path: safeJoinUnderRoot(
            this.environmentPath,
            version.packageName,
            version.dirName,
         ),
      };
   }

   public async getPackage(
      packageName: string,
      reload: boolean = false,
      options: { allowAdmission?: boolean; versionId?: string } = {},
   ): Promise<Package> {
      assertSafePackageName(packageName);
      const slot = this.resolveSlot(packageName, options.versionId);
      // A published version is immutable, so there is nothing to reload: a
      // reload of one is an ordinary read, which loads it only if it is not
      // loaded yet.
      const effectiveReload = reload && slot.version === undefined;
      // Fast-path: serve from cache without acquiring the lock. Safe because
      // `Package` references are immutable; the disk-reading methods that
      // actually need protection (compileSource, getModelFileText,
      // reloadAllModelsForPackage, ...) acquire the lock themselves.
      //
      // INVARIANT: callers that consume the returned Package on the fast
      // path (notably the MCP query tools and Model.getModel()) must
      // remain in-memory only. If any code reachable from a `Package`
      // method ever grows new disk I/O against the canonical tree, that
      // path needs to be bracketed by `withPackageLock`; otherwise a
      // concurrent install/delete will race against an unlocked reader.
      const _package = this.packages.get(slot.key);
      if (_package !== undefined && !effectiveReload) {
         return _package;
      }

      // We are either reloading or about to lazy-load on a cache miss
      // — both allocate a new package. This is the single choke point
      // for admission control; controllers no longer need their own
      // back-pressure check.
      this.assertCanAdmitNewPackage(
         packageName,
         effectiveReload ? "reload a package" : "load a package",
         options.allowAdmission === true,
      );

      return this.withSlotLock(slot, () =>
         this._loadOrGetPackageLocked(slot, effectiveReload),
      );
   }

   /**
    * Hold the lock for one slot. For an unversioned package the slot key is
    * its name, so this is the same mutex {@link withPackageLock} takes; each
    * published version has its own, so two versions load in parallel.
    */
   private async withSlotLock<T>(
      slot: PackageSlot,
      fn: () => Promise<T>,
   ): Promise<T> {
      return this.getOrCreatePackageMutex(slot.key).runExclusive(fn);
   }

   /**
    * Resolve a request's slot and hold its lock, re-resolving once the lock is
    * held. A request resolved before a publish it then waited on (the
    * package's first versioned publish, or a move of `latest`) would otherwise
    * read a path that no longer holds its files. When the slot moved, the lock
    * is released and the new slot's taken instead, rather than nesting two
    * slot locks, which two such requests could take in opposite orders.
    */
   private async withResolvedSlotLock<T>(
      packageName: string,
      versionId: string | undefined,
      fn: (slot: PackageSlot) => Promise<T>,
   ): Promise<T> {
      let slot = this.resolveSlot(packageName, versionId);
      for (let attempt = 0; ; attempt++) {
         const held = slot;
         const outcome = await this.withSlotLock<
            { moved: PackageSlot } | { value: T }
         >(held, async () => {
            const fresh = this.resolveSlot(packageName, versionId);
            // Bounded: a slot that keeps moving is served from where it last
            // was, under the lock taken for it, rather than retried forever.
            if (fresh.key !== held.key && attempt < 3) {
               return { moved: fresh };
            }
            return { value: await fn(fresh.key === held.key ? fresh : held) };
         });
         if ("value" in outcome) return outcome.value;
         slot = outcome.moved;
      }
   }

   /**
    * Load (or reload) a package from its slot's disk location. Assumes the
    * caller holds that slot's mutex (via {@link withSlotLock}, which for an
    * unversioned package is {@link withPackageLock}).
    *
    * Used by {@link getPackage} and by {@link compileSource} so the
    * cache-miss path doesn't re-enter the mutex. Takes a package name where a
    * caller has only that, which resolves to the package's unversioned slot or
    * its `latest`.
    */
   private async _loadOrGetPackageLocked(
      slotOrName: PackageSlot | string,
      reload: boolean = false,
   ): Promise<Package> {
      const slot =
         typeof slotOrName === "string"
            ? this.resolveSlot(slotOrName)
            : slotOrName;
      if (slot.version !== undefined) {
         return this._loadVersionLocked(slot);
      }
      if (this.isVersionedPackage(slot.name)) {
         // Resolved as unversioned before the package's first versioned
         // publish committed. Its directory now holds version trees, so it is
         // served through its latest, under that version's own lock (taken
         // inside the package lock this caller holds: the usual order).
         const fresh = this.resolveSlot(slot.name);
         return this.withSlotLock(fresh, () => this._loadVersionLocked(fresh));
      }
      const packageName = slot.name;
      const existingPackage = this.packages.get(packageName);
      if (existingPackage !== undefined && !reload) {
         return existingPackage;
      }

      return this.trackPackageLoad(packageName, async () => {
         this.setPackageStatus(packageName, PackageStatus.LOADING);

         try {
            logger.debug(`Loading package ${packageName}...`);
            const packagePath = slot.path;
            const _package = await Package.create(
               this.environmentName,
               packageName,
               packagePath,
               () => this.malloyConfig.malloyConfig,
            );
            this.attachDestinationServeConfig(_package);
            await this.bindManifestIfConfigured(_package);
            await this.rebindServeBindingsFromLocalStore(_package);
            if (existingPackage !== undefined && reload) {
               this.retireConnectionGeneration(`package ${packageName}`, () =>
                  existingPackage.getMalloyConfig().shutdown("close"),
               );
               _package.noteSurfaceChangeFrom(
                  existingPackage.getPackageMetadata().explores,
               );
            }
            this.packages.set(packageName, _package);
            this.notifyPackageLoaded(_package);
            this.setPackageStatus(packageName, PackageStatus.SERVING);
            // It loaded, so any earlier failure is stale. A package that failed at
            // boot can be fixed on disk and reloaded without a restart.
            this.clearPackageLoadFailure(packageName);
            logger.debug(`Successfully loaded package ${packageName}`);

            return _package;
         } catch (error) {
            logger.error(`Failed to load package ${packageName}`, { error });
            if (existingPackage !== undefined && reload) {
               // A failed RELOAD must not take down a package that is already
               // serving. The compiled model in `packages` is still the last good
               // one (it is only replaced on success), so keep serving it and let
               // the caller surface the error instead of evicting the package and
               // leaving the environment with nothing to answer from.
               this.setPackageStatus(packageName, PackageStatus.SERVING);
               // Serving the last good model is right, but it must not be silent:
               // this is the only record that the served model is now older than
               // the files on disk, and it is what makes a failed watch-mode
               // recompile visible to /status at all (the watch controller only
               // logs to stderr). Cleared on the next successful load via
               // clearPackageLoadFailure. Recording here, not in the watch
               // controller, covers every reload caller: the chokidar watcher,
               // MCP reload_package, and REST ?reload=true.
               this.staleCompileErrors.set(packageName, {
                  message: redactPgSecrets(
                     error instanceof Error ? error.message : String(error),
                  ),
                  failedAt: new Date().toISOString(),
               });
            } else {
               this.packages.delete(packageName);
               this.packageStatuses.delete(packageName);
            }
            throw error;
         }
      });
   }

   /**
    * Load one published version into its slot. Assumes the caller holds the
    * slot's mutex. A version's files never change, so a loaded version is
    * returned as it is; there is nothing to reload.
    *
    * The registry, not the disk, says the version exists. When its tree is
    * missing, it is fetched again from where it was published and checked
    * against the hash recorded at publish before it is served (see
    * {@link restoreVersionTreeIfMissing}).
    */
   private async _loadVersionLocked(slot: PackageSlot): Promise<Package> {
      const loaded = this.packages.get(slot.key);
      if (loaded !== undefined) return loaded;
      // The slot was resolved before this lock was taken, so an archive (or a
      // manifest rebind) may have landed in between. Read the version again:
      // loading from the stale snapshot would put an archived version back
      // in memory, or bind it to a manifest it no longer has.
      const version = this.resolveSlot(
         slot.name,
         (slot.version as PackageVersion).version,
      ).version as PackageVersion;

      await this.restoreVersionTreeIfMissing(slot);
      logger.debug(
         `Loading version ${version.version} of package ${slot.name}...`,
      );
      const pkg = await Package.create(
         this.environmentName,
         slot.name,
         slot.path,
         () => this.malloyConfig.malloyConfig,
      );
      pkg.setVersion(
         version.version,
         this.packageVersions.get(slot.name)?.latest ?? null,
      );
      // The registry row, not the tree's publisher.json, holds the version's
      // manifest binding: it is serving state, and it changes after publish.
      pkg.setManifestLocation(version.manifestLocation);
      this.attachDestinationServeConfig(pkg);
      await this.bindManifestIfConfigured(pkg);
      await this.rebindServeBindingsFromLocalStore(pkg);
      // Read again: a publish that moved latest during the binds above
      // refreshed only the versions already in the map, not this one.
      const latest = this.packageVersions.get(slot.name)?.latest ?? null;
      pkg.setVersion(version.version, latest);
      this.packages.set(slot.key, pkg);
      // The load hook builds the package's retrieval index, which is keyed by
      // package name, so only the version a nameless request reaches feeds it.
      if (latest === version.version) this.notifyPackageLoaded(pkg);
      this.setPackageStatus(slot.name, PackageStatus.SERVING);
      return pkg;
   }

   /**
    * Put a registered version's tree back when it is missing from disk: fetch
    * it from the location it was published from, and serve it only if it
    * hashes to what was published. A location whose content has changed since
    * is refused rather than served under the old version's name.
    */
   private async restoreVersionTreeIfMissing(slot: PackageSlot): Promise<void> {
      const version = slot.version as PackageVersion;
      const present = await fs.promises
         .stat(slot.path)
         .then((stat) => stat.isDirectory())
         .catch(() => false);
      if (present) return;
      if (!this.versionTreeFetcher || !version.sourceLocation) {
         throw new Error(
            `The files of version ${version.version} of package ${slot.name} are missing, and there is no location to fetch them from.`,
         );
      }
      const stagingPath = this.allocateStagingPath(slot.name);
      await fs.promises.mkdir(path.dirname(stagingPath), { recursive: true });
      try {
         await this.versionTreeFetcher(
            version.sourceLocation,
            stagingPath,
            slot.name,
         );
         const contentHash = await hashPackageTree(stagingPath);
         if (contentHash !== version.contentHash) {
            throw new Error(
               `Version ${version.version} of package ${slot.name} was fetched again from ${version.sourceLocation} to replace missing files, but the content there has changed since it was published, so it was not served.`,
            );
         }
         await fs.promises.mkdir(path.dirname(slot.path), { recursive: true });
         await fs.promises.rename(stagingPath, slot.path);
         logger.info("Restored a missing version tree from its location", {
            environmentName: this.environmentName,
            packageName: slot.name,
            version: version.version,
         });
      } catch (error) {
         await fs.promises
            .rm(stagingPath, { recursive: true, force: true })
            .catch(() => {});
         throw error;
      }
   }

   /**
    * Connect this environment to the version registry, and say how to fetch a
    * version's tree again from its location. Set by the EnvironmentStore,
    * which owns the repository; without it, versioned publish is unavailable
    * and every package is served unversioned.
    */
   public setVersionRegistry(
      registry: VersionRegistry | null,
      fetchTree?: VersionTreeFetcher,
   ): void {
      this.versionRegistry = registry ?? undefined;
      this.versionTreeFetcher = fetchTree;
   }

   private requireVersionRegistry(): VersionRegistry {
      if (!this.versionRegistry) {
         throw new Error(
            `Environment ${this.environmentName} has no version registry, so it cannot publish versions.`,
         );
      }
      return this.versionRegistry;
   }

   /**
    * Mirror every package's versions from the registry into the in-memory
    * index. Run once when the environment is restored, before anything loads
    * a package, so a request after a restart resolves the same versions it
    * did before.
    */
   public async loadPackageVersions(): Promise<void> {
      if (!this.versionRegistry) return;
      const all = await this.versionRegistry.listAllVersions();
      const names = new Set(all.map((v) => v.packageName));
      // A first versioned publish interrupted by a crash left the package's
      // unversioned tree aside; settle it before anything loads the package.
      const legacyRoot = safeJoinUnderRoot(
         this.environmentPath,
         LEGACY_DIR_NAME,
      );
      const aside = await fs.promises.readdir(legacyRoot).catch(() => []);
      for (const name of aside) {
         try {
            assertSafePackageName(name);
         } catch {
            continue;
         }
         await this.recoverLegacyTree(name, names.has(name));
      }
      for (const name of names) {
         await this.refreshPackageVersions(name);
      }
   }

   /**
    * Settle an unversioned tree a first versioned publish moved aside to
    * `.legacy/<pkg>`. When a version of the package is registered, the publish
    * committed and the tree is retired. When none is, the publish never did,
    * so the tree goes back: unless the package directory already holds an
    * unversioned tree of its own (its publisher.json at the top), which was
    * installed since and is newer, and the copy aside is dropped instead.
    * A no-op when nothing is aside.
    */
   private async recoverLegacyTree(
      packageName: string,
      committed: boolean,
   ): Promise<void> {
      const aside = safeJoinUnderRoot(
         this.environmentPath,
         LEGACY_DIR_NAME,
         packageName,
      );
      const isDir = (p: string) =>
         fs.promises
            .stat(p)
            .then((stat) => stat.isDirectory())
            .catch(() => false);
      if (!(await isDir(aside))) return;
      const packageDir = safeJoinUnderRoot(this.environmentPath, packageName);
      const ownTree = await fs.promises
         .stat(path.join(packageDir, PACKAGE_MANIFEST_NAME))
         .then(() => true)
         .catch(() => false);
      try {
         if (committed || ownTree) {
            await fs.promises.rm(aside, { recursive: true, force: true });
            return;
         }
         // The package directory holds at most a half-placed version tree.
         await fs.promises.rm(packageDir, { recursive: true, force: true });
         await fs.promises.rename(aside, packageDir);
         logger.warn(
            "Restored an unversioned package whose first versioned publish was interrupted",
            { environmentName: this.environmentName, packageName },
         );
      } catch (error) {
         logger.error(
            "Failed to settle an unversioned package tree moved aside by a versioned publish",
            {
               error,
               environmentName: this.environmentName,
               packageName,
               aside,
            },
         );
      }
   }

   /** Re-read one package's versions and `latest` from the registry. */
   private async refreshPackageVersions(packageName: string): Promise<void> {
      const registry = this.requireVersionRegistry();
      const versions = await registry.listVersions(packageName);
      if (versions.length === 0) {
         this.packageVersions.delete(packageName);
         return;
      }
      const latest = await registry.getLatest(packageName);
      this.setPackageVersions(packageName, latest, versions);
      for (const [key, pkg] of this.packages) {
         const version = pkg.getVersionId();
         if (version !== undefined && key.startsWith(`${packageName}@`)) {
            pkg.setVersion(version, latest);
         }
      }
   }

   /**
    * Publish an immutable version of a package. The version is the `version`
    * field of the package's own publisher.json; the request names none.
    *
    *  - Phase 1 (no lock): `downloader` writes the package into a staging
    *    directory, and its version and content hash are read there.
    *  - Phase 2 (package lock): a version already published with the same
    *    content is placement, not publication, and succeeds without writing
    *    anything (the control plane re-loads a version onto a worker that
    *    already holds it); with different content it is 409 VERSION_CONFLICT.
    *    A new version is renamed into `<package>/<dirName>`, loaded, put
    *    through `validate`, and only then recorded in the registry, so a
    *    failed publish leaves the versions already published exactly as they
    *    were. The package's first versioned publish moves an unversioned tree
    *    aside first, and puts it back if the publish fails.
    *  - `latest` moves under `promotion` once the version is recorded.
    */
   public async publishPackageVersion(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      options: {
         sourceLocation: string;
         promotion: VersionPromotionMode;
         validate?: (pkg: Package) => string | undefined;
         manifestLocation?: string | null;
      },
   ): Promise<Package> {
      assertSafePackageName(packageName);
      this.requireVersionRegistry();
      // A new version is a whole new compiled copy, held beside the versions
      // already serving, so it is gated before the download like any install:
      // under memory back-pressure it is refused with a 503 the caller can
      // retry elsewhere.
      this.assertCanAdmitNewPackage(
         packageName,
         "publish a package version",
         false,
      );
      const stagingPath = this.allocateStagingPath(packageName);
      await fs.promises.mkdir(path.dirname(stagingPath), { recursive: true });
      let staged: StagedVersion;
      try {
         await downloader(stagingPath);
         const manifest = await readPublishedVersion(stagingPath);
         staged = {
            stagingPath,
            ...manifest,
            dirName: versionDirName(manifest.version),
            contentHash: await hashPackageTree(stagingPath),
         };
      } catch (error) {
         await fs.promises
            .rm(stagingPath, { recursive: true, force: true })
            .catch(() => {});
         throw error;
      }
      try {
         return await this.withPackageLock(packageName, () =>
            this._publishVersionLocked(packageName, staged, options),
         );
      } finally {
         // A committed publish renamed the staging directory away, so this
         // only removes what a refused or failed one left.
         await fs.promises
            .rm(stagingPath, { recursive: true, force: true })
            .catch(() => {});
      }
   }

   private async _publishVersionLocked(
      packageName: string,
      staged: StagedVersion,
      options: {
         sourceLocation: string;
         promotion: VersionPromotionMode;
         validate?: (pkg: Package) => string | undefined;
         manifestLocation?: string | null;
      },
   ): Promise<Package> {
      const registry = this.requireVersionRegistry();
      await this.refreshPackageVersions(packageName);
      const index = this.packageVersions.get(packageName);
      const existing = index?.versions.get(staged.version);
      if (existing) {
         return this.republishVersionLocked(packageName, existing, staged, {
            ...options,
            registry,
         });
      }
      for (const other of index?.versions.values() ?? []) {
         if (other.dirName.toLowerCase() === staged.dirName.toLowerCase()) {
            throw new PackageVersionError(
               "VERSION_CONFLICT",
               `Version ${staged.version} of package ${packageName} differs from published version ${other.version} only by letter case, which a case-insensitive filesystem cannot keep apart. Publish it under another version.`,
            );
         }
      }

      const packageDir = safeJoinUnderRoot(this.environmentPath, packageName);
      const onDisk = await fs.promises.lstat(packageDir).catch(() => undefined);
      if (onDisk?.isSymbolicLink()) {
         throw new BadRequestError(
            `Package ${packageName} is mounted in place by watch mode, so it cannot take a published version. Publish it under another name, or restart without --watch-env.`,
         );
      }
      const legacyPackage = index ? undefined : this.packages.get(packageName);
      let retiredLegacy: string | undefined;
      if (!index) {
         // A tree still aside from an earlier attempt whose put-back failed
         // goes back first, so it is what this publish moves aside again.
         await this.recoverLegacyTree(packageName, false);
         const current = await fs.promises
            .lstat(packageDir)
            .catch(() => undefined);
         if (current?.isDirectory()) {
            // Aside where the startup sweep never reaches: until this publish
            // commits, it is the package's only copy (see recoverLegacyTree).
            retiredLegacy = safeJoinUnderRoot(
               this.environmentPath,
               LEGACY_DIR_NAME,
               packageName,
            );
            await fs.promises.mkdir(path.dirname(retiredLegacy), {
               recursive: true,
            });
            await fs.promises.rename(packageDir, retiredLegacy);
         }
      }

      const target = safeJoinUnderRoot(
         this.environmentPath,
         packageName,
         staged.dirName,
      );
      let retiredOrphan: string | undefined;
      let createdPackageRow = false;
      let pkg: Package;
      let row: PackageVersion;
      try {
         await fs.promises.mkdir(packageDir, { recursive: true });
         // A tree at the target that the registry does not know is what a crash
         // between the rename and the registry write leaves behind.
         if (
            await fs.promises.stat(target).then(
               () => true,
               () => false,
            )
         ) {
            retiredOrphan = this.allocateRetiredPath(packageName);
            await fs.promises.mkdir(path.dirname(retiredOrphan), {
               recursive: true,
            });
            await fs.promises.rename(target, retiredOrphan);
         }
         await fs.promises.rename(staged.stagingPath, target);
         pkg = await Package.create(
            this.environmentName,
            packageName,
            target,
            () => this.malloyConfig.malloyConfig,
            // This tree was just staged into place, so a failed load leaves a
            // half-built directory that is ours to remove.
            true,
         );
         this.attachDestinationServeConfig(pkg);
         const validationMsg = options.validate?.(pkg);
         if (validationMsg) {
            throw new BadRequestError(validationMsg);
         }
         createdPackageRow = await registry.ensurePackage(
            packageName,
            staged.description ?? undefined,
         );
         try {
            row = await registry.createVersion({
               packageName,
               version: staged.version,
               dirName: staged.dirName,
               contentHash: staged.contentHash,
               sourceLocation: options.sourceLocation,
               manifestLocation: options.manifestLocation ?? null,
               archiveStatus: "unarchive",
               archivedAt: null,
               description: staged.description,
               gitCommitSha: null,
               gitRef: null,
            });
         } catch (error) {
            if (error instanceof DuplicatePackageVersionError) {
               throw new PackageVersionError(
                  "VERSION_CONFLICT",
                  `Version ${staged.version} of package ${packageName} was published while this publish was running.`,
               );
            }
            throw error;
         }
      } catch (error) {
         await fs.promises
            .rm(target, { recursive: true, force: true })
            .catch(() => {});
         if (createdPackageRow) {
            // No version was recorded, so the row this publish created would
            // otherwise list a package with no files after a restart.
            await registry
               .discardPackage(packageName)
               .catch((err) =>
                  logger.error(
                     "Failed to remove the package row a failed versioned publish created",
                     { error: err, packageName },
                  ),
               );
         }
         if (retiredLegacy) {
            // The package directory holds only the refused version, so the
            // unversioned tree goes back exactly where it was.
            await fs.promises
               .rm(packageDir, { recursive: true, force: true })
               .catch(() => {});
            await fs.promises
               .rename(retiredLegacy, packageDir)
               .catch((err) =>
                  logger.error(
                     "Failed to restore an unversioned package after a refused versioned publish",
                     { error: err, packageName, retiredLegacy },
                  ),
               );
         }
         if (retiredOrphan) this.removeRetiredLater(retiredOrphan);
         throw error;
      }

      // The version is committed. Nothing from here on may undo it or report
      // the publish as failed, so each step is best-effort: a step that fails
      // is logged and the version is served with what is known.
      const key = `${packageName}@${staged.dirName}`;
      const previousLatest = index?.latest ?? null;
      let latest = previousLatest;
      // Held until the version is in the package map. Once the index below
      // makes it resolvable, a read that names it (or, promoted, one that
      // names none) waits here for this instance rather than loading a
      // second copy of its own.
      await this.getOrCreatePackageMutex(key).runExclusive(async () => {
         if (options.promotion === "on-publish") {
            try {
               await this.promoteOnPublish(packageName, staged.version, true);
            } catch (error) {
               logger.error(
                  "Published a package version but could not move latest to it",
                  { error, packageName, version: staged.version },
               );
            }
         }
         try {
            await this.refreshPackageVersions(packageName);
         } catch (error) {
            // The registry could not be read back. Mirror the row this publish
            // wrote, so the version is servable by name until the index is
            // next read; latest stays what it was known to be.
            logger.error(
               "Published a package version but could not re-read its versions",
               { error, packageName, version: staged.version },
            );
            const known = this.packageVersions.get(packageName);
            this.setPackageVersions(packageName, known?.latest ?? null, [
               ...(known?.versions.values() ?? []),
               row,
            ]);
         }
         latest = this.packageVersions.get(packageName)?.latest ?? null;
         pkg.setVersion(staged.version, latest);
         pkg.setManifestLocation(row.manifestLocation);
         // A manifest that cannot be fetched must not undo a publish, so the
         // version serves live instead (bindManifest records the fallback).
         await this.bindManifestIfConfigured(pkg);
         await this.rebindServeBindingsFromLocalStore(pkg);
         this.packages.set(key, pkg);
      });
      this.setPackageStatus(packageName, PackageStatus.SERVING);
      this.clearPackageLoadFailure(packageName);
      if (latest === staged.version) this.notifyPackageLoaded(pkg);

      // The version that stopped being latest is out of the request path a
      // nameless request takes, so it leaves memory now and loads again on
      // the next request that names it. Other versions stay loaded as any
      // package does: one published below latest, every one published under
      // explicit promotion, and one loaded by name, until it is archived, its
      // package is deleted or the server restarts. Each version is its own
      // cache entry, admitted by the memory governor like any package's.
      if (previousLatest && previousLatest !== latest) {
         await this.unloadVersionSlot(packageName, previousLatest);
      }
      if (legacyPackage) {
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            legacyPackage.getMalloyConfig().shutdown("close"),
         );
         this.packages.delete(packageName);
      }
      if (retiredLegacy) this.removeRetiredLater(retiredLegacy);
      if (retiredOrphan) this.removeRetiredLater(retiredOrphan);
      logger.info("Published package version", {
         environmentName: this.environmentName,
         packageName,
         version: staged.version,
         latest,
      });
      return pkg;
   }

   /**
    * A publish of a version the package already has. With the same content it
    * is placement rather than publication and succeeds: nothing is written,
    * the version is loaded if it is not, a newly supplied manifest is bound,
    * and the promotion rule is applied again (it is monotone, so this never
    * moves `latest` backwards). With different content it is a conflict.
    *
    * The version is loaded before `latest` can move to it, as on a first
    * publish: a version that cannot load must never become what every
    * request naming no version reaches. When its tree is missing, the tree
    * this request staged, already checked against the published hash, is put
    * in place rather than fetched again from where the version was first
    * published, which may since have moved on or gone.
    */
   private async republishVersionLocked(
      packageName: string,
      existing: PackageVersion,
      staged: StagedVersion,
      options: {
         promotion: VersionPromotionMode;
         manifestLocation?: string | null;
         registry: VersionRegistry;
      },
   ): Promise<Package> {
      if (existing.contentHash !== staged.contentHash) {
         throw new PackageVersionError(
            "VERSION_CONFLICT",
            `Version ${staged.version} of package ${packageName} is already published with different content. Bump "version" in publisher.json and publish again.`,
         );
      }
      if (existing.archiveStatus === "archive") {
         throw new PackageVersionError(
            "VERSION_ARCHIVED",
            `Version ${staged.version} of package ${packageName} is archived. Unarchive it to serve it again.`,
         );
      }
      // A publish binds a manifest it names and never unbinds: an
      // orchestrator re-loading a version sends no manifest (or null) when it
      // has none to give. PUT .../versions/{v}/manifest unbinds.
      const rebind =
         !!options.manifestLocation &&
         options.manifestLocation !== existing.manifestLocation;
      if (rebind) {
         await options.registry.updateVersion(existing.id, {
            manifestLocation: options.manifestLocation,
         });
         await this.refreshPackageVersions(packageName);
      }
      const previousLatest =
         this.packageVersions.get(packageName)?.latest ?? null;
      const slot = this.resolveSlot(packageName, staged.version);
      const pkg = await this.withSlotLock(slot, async () => {
         const loaded = this.packages.get(slot.key);
         if (loaded) {
            if (rebind) {
               await this.applyVersionManifest(
                  loaded,
                  options.manifestLocation ?? null,
               );
            }
            return loaded;
         }
         const present = await fs.promises
            .stat(slot.path)
            .then((stat) => stat.isDirectory())
            .catch(() => false);
         if (!present) {
            await fs.promises.mkdir(path.dirname(slot.path), {
               recursive: true,
            });
            await fs.promises.rename(staged.stagingPath, slot.path);
         }
         return this._loadVersionLocked(slot);
      });
      if (options.promotion === "on-publish") {
         // Only to a strictly higher version: re-loading an older build of an
         // equal version must not take latest back to it.
         await this.promoteOnPublish(packageName, staged.version, false);
      }
      await this.refreshPackageVersions(packageName);
      const latest = this.packageVersions.get(packageName)?.latest ?? null;
      this.setPackageStatus(packageName, PackageStatus.SERVING);
      if (latest !== previousLatest) {
         // This re-publish moved latest, as a publish would: the old latest
         // leaves memory, and the new one feeds the retrieval index, which it
         // did not when it loaded above, before it was latest.
         if (previousLatest) {
            await this.unloadVersionSlot(packageName, previousLatest);
         }
         if (latest === staged.version) this.notifyPackageLoaded(pkg);
      }
      return pkg;
   }

   /**
    * Bind a loaded version to a build manifest, or back to live when the
    * location is null, the same way a manifestLocation change on the
    * deprecated package PATCH does.
    */
   private async applyVersionManifest(
      pkg: Package,
      manifestLocation: string | null,
   ): Promise<void> {
      pkg.setManifestLocation(manifestLocation);
      if (manifestLocation) {
         await this.bindManifest(pkg, manifestLocation);
      } else {
         await pkg.reloadAllModels({});
         pkg.bindStorageServeBindings({});
      }
   }

   /**
    * Advance `latest` to `version` unless a higher version already is latest.
    * The pointer moves by compare-and-swap, retried when a concurrent move
    * wins, so between versions of different precedence the result does not
    * depend on the order publishes commit in. Two versions of equal
    * precedence (they differ only in build metadata, or in leading zeros the
    * shared semver pattern accepts) go to the later publish, so for those the
    * order does decide. Each server keeps its own registry, so under
    * on-publish promotion two servers that take such a pair in opposite
    * orders can disagree on `latest`.
    */
   private async promoteOnPublish(
      packageName: string,
      version: string,
      // Whether a version of equal precedence (build metadata alone differs)
      // takes latest: true for a new publish, the later build; false for a
      // re-publish, which is placement and must not take latest back to an
      // older build.
      tieAdvances: boolean,
   ): Promise<void> {
      const registry = this.requireVersionRegistry();
      for (let attempt = 0; attempt < 5; attempt++) {
         const current = await registry.getLatest(packageName);
         if (current === version) return;
         if (current !== null) {
            const order = compareSemver(version, current);
            if (order < 0 || (order === 0 && !tieAdvances)) return;
         }
         if (await registry.setLatest(packageName, current, version)) return;
      }
      throw new Error(
         `Could not move the latest version of package ${packageName} to ${version}: it kept changing under this publish.`,
      );
   }

   /**
    * Drop one loaded version from memory; it loads again when next named.
    * Taken under the version's own lock, so a load already in flight finishes
    * first and is then dropped, rather than landing after the unload.
    */
   private async unloadVersionSlot(
      packageName: string,
      version: string,
   ): Promise<void> {
      const index = this.packageVersions.get(packageName);
      const dirName = index?.versions.get(version)?.dirName;
      if (!dirName) return;
      const key = `${packageName}@${dirName}`;
      await this.getOrCreatePackageMutex(key).runExclusive(async () => {
         const pkg = this.packages.get(key);
         if (!pkg) return;
         this.retireConnectionGeneration(`package ${key}`, () =>
            pkg.getMalloyConfig().shutdown("close"),
         );
         this.packages.delete(key);
      });
   }

   private removeRetiredLater(retiredPath: string): void {
      setImmediate(() => {
         void fs.promises
            .rm(retiredPath, { recursive: true, force: true })
            .catch((err) => {
               logger.warn(
                  `Failed to clean up retired package directory ${retiredPath}`,
                  { error: err },
               );
            });
      });
   }

   /** Refuse an in-place change to a package whose versions are immutable. */
   private assertNotVersioned(packageName: string, what: string): void {
      if (this.isVersionedPackage(packageName)) {
         throw new PackageVersionError(
            "PACKAGE_IS_VERSIONED",
            `Package ${packageName} has published versions, which are immutable, so it cannot ${what}. Publish a new version instead.`,
         );
      }
   }

   public async addPackage(
      packageName: string,
      options: { allowAdmission?: boolean } = {},
   ) {
      assertSafePackageName(packageName);
      this.assertNotVersioned(packageName, "be re-registered from a directory");
      const packagePath = safeJoinUnderRoot(this.environmentPath, packageName);
      if (
         !(await fs.promises
            .access(packagePath)
            .then(() => true)
            .catch(() => false)) ||
         !(await fs.promises.stat(packagePath))?.isDirectory()
      ) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      // 404 takes precedence over 503 so a permanent "you forgot to
      // upload the package" failure isn't masked as a transient
      // "retry later" — the gate runs after the existence check.
      this.assertCanAdmitNewPackage(
         packageName,
         "add a new package",
         options.allowAdmission === true,
      );
      logger.info(
         `Adding package ${packageName} to environment ${this.environmentName}`,
         {
            packagePath,
         },
      );

      return this.withPackageLock(packageName, () => {
         // Again under the lock: the package may have published its first
         // version since the check above.
         this.assertNotVersioned(
            packageName,
            "be re-registered from a directory",
         );
         return this._addPackageLocked(packageName);
      });
   }

   private async _addPackageLocked(
      packageName: string,
   ): Promise<Package | undefined> {
      const packagePath = safeJoinUnderRoot(this.environmentPath, packageName);
      const existingPackage = this.packages.get(packageName);
      if (existingPackage !== undefined) {
         return existingPackage;
      }

      return this.trackPackageLoad(packageName, async () => {
         this.setPackageStatus(packageName, PackageStatus.LOADING);
         try {
            const addedPackage = await Package.create(
               this.environmentName,
               packageName,
               packagePath,
               () => this.malloyConfig.malloyConfig,
            );
            this.attachDestinationServeConfig(addedPackage);
            this.packages.set(packageName, addedPackage);
            this.notifyPackageLoaded(addedPackage);
         } catch (error) {
            logger.error("Error adding package", { error });
            this.deletePackageStatus(packageName);
            throw error;
         }
         this.setPackageStatus(packageName, PackageStatus.SERVING);
         // Same reasoning as the load and install paths: it is serving now, so an
         // earlier boot failure is stale. Without this, a package fixed on disk
         // and re-added keeps its loadError for the life of the process.
         this.clearPackageLoadFailure(packageName);
         return this.packages.get(packageName);
      });
   }

   /**
    * Replace a package on disk via stage-and-swap, then load it.
    *
    *  - Phase 1 (no lock): run `downloader(stagingPath)`, writing the new
    *    content into a fresh sibling dir at `.staging/<pkg>-<uuid>/`. This
    *    is where multi-second downloads (git clone, GCS pull, ...) happen.
    *  - Phase 2 (lock held): atomically rename any existing canonical tree
    *    out to `.retired/<pkg>-<uuid>/`, rename staging into the canonical
    *    path, and run `Package.create` against the canonical path.
    *  - Phase 3 (after lock release): retire the old package's connections
    *    via the existing 30s drain and `fs.rm` the retired tree.
    *
    * Concurrent compiles / `getModelFileText` / `reloadAllModels` calls
    * take the same mutex and so are mutually exclusive with the Phase 2
    * swap, but they never queue behind a long Phase 1 download.
    *
    * On failure (Phase 1 download or Phase 2 `Package.create`), the staging
    * dir is removed and — if we already renamed the old tree aside — the
    * old tree is renamed back so the canonical path is restored.
    */
   public async installPackage(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      validate?: (pkg: Package) => string | undefined,
      options: {
         allowAdmission?: boolean;
         /**
          * Metadata to apply to the installed copy inside the install's own lock
          * hold. The `location` it carries is recorded as where the package was
          * installed from; an install is the only path that records one.
          */
         update?: ApiPackage;
      } = {},
   ): Promise<Package> {
      assertSafePackageName(packageName);
      this.assertNotVersioned(packageName, "be re-installed in place");
      // An install allocates a whole new compiled copy, and for a reinstall
      // holds it beside the copy still serving until the swap, so it is the
      // largest single allocation a package can ask for. It is gated before
      // the download, the same way a lazy load and an add are gated, so a
      // server under memory back-pressure refuses it with a 503 the caller
      // can retry elsewhere rather than taking on the work and being killed.
      this.assertCanAdmitNewPackage(
         packageName,
         this.packages.has(packageName)
            ? "reinstall a package"
            : "install a package",
         options.allowAdmission === true,
      );
      return this.trackPackageLoad(packageName, () =>
         this._installPackageTracked(
            packageName,
            downloader,
            validate,
            options,
         ),
      );
   }

   private async _installPackageTracked(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      validate: ((pkg: Package) => string | undefined) | undefined,
      options: { update?: ApiPackage },
   ): Promise<Package> {
      const stagingPath = this.allocateStagingPath(packageName);
      await fs.promises.mkdir(path.dirname(stagingPath), { recursive: true });

      logger.debug("install.phase1.download.started", {
         environmentName: this.environmentName,
         packageName,
         stagingPath,
      });
      const downloadStartedAt = performance.now();
      try {
         await downloader(stagingPath);
      } catch (err) {
         await fs.promises
            .rm(stagingPath, { recursive: true, force: true })
            .catch(() => {});
         throw err;
      }
      logger.debug("install.phase1.download.completed", {
         environmentName: this.environmentName,
         packageName,
         durationMs: performance.now() - downloadStartedAt,
      });

      return this.withPackageLock(packageName, async () => {
         // Again under the lock: the package may have published its first
         // version while this one downloaded, and its directory now holds the
         // version trees the swap below would retire.
         if (this.isVersionedPackage(packageName)) {
            await fs.promises
               .rm(stagingPath, { recursive: true, force: true })
               .catch(() => {});
            this.assertNotVersioned(packageName, "be re-installed in place");
         }
         logger.debug("install.phase2.swap.started", {
            environmentName: this.environmentName,
            packageName,
         });
         const canonicalPath = safeJoinUnderRoot(
            this.environmentPath,
            packageName,
         );
         let retiredPath: string | undefined;

         const oldPackage = this.packages.get(packageName);
         const oldExistsOnDisk = await fs.promises
            .access(canonicalPath)
            .then(() => true)
            .catch(() => false);

         if (oldExistsOnDisk) {
            retiredPath = this.allocateRetiredPath(packageName);
            await fs.promises.mkdir(path.dirname(retiredPath), {
               recursive: true,
            });
            await fs.promises.rename(canonicalPath, retiredPath);
            logger.debug("install.phase2.retired_old", {
               environmentName: this.environmentName,
               packageName,
               retiredPath,
            });
         }

         let newPackage: Package;
         try {
            await fs.promises.rename(stagingPath, canonicalPath);

            this.setPackageStatus(packageName, PackageStatus.LOADING);
            newPackage = await Package.create(
               this.environmentName,
               packageName,
               canonicalPath,
               () => this.malloyConfig.malloyConfig,
               // This tree was just staged into place by the rename above, so a
               // failed load leaves a half-built directory that is ours to
               // remove; the rollback below restores the previous one.
               true,
            );
            this.attachDestinationServeConfig(newPackage);
            // Strict-reject hook (publish/update only — reload passes no
            // validator and stays fail-safe). Throw INSIDE the try so the
            // catch below rolls the swap back: the just-installed tree is
            // wiped and the retired tree (if any) is restored, so a rejected
            // publish/update never leaves the bad tree served on disk.
            const validationMsg = validate?.(newPackage);
            if (validationMsg) {
               throw new BadRequestError(validationMsg);
            }
            logger.debug("install.phase2.committed", {
               environmentName: this.environmentName,
               packageName,
               canonicalPath,
            });
         } catch (err) {
            // Rollback: clobber whatever (partial) content sits at canonical
            // — Package.create's own failure-cleanup may have already rm'd
            // the directory, so the most common outcome here is ENOENT.
            // `force: true` plus the `.catch(() => {})` make this a
            // best-effort wipe whose only job is to leave the rename-back
            // below a clean destination. Then put the old tree back if we
            // moved one aside.
            await fs.promises
               .rm(canonicalPath, { recursive: true, force: true })
               .catch(() => {});
            let restored = false;
            if (retiredPath) {
               try {
                  await fs.promises.rename(retiredPath, canonicalPath);
                  restored = true;
               } catch (restoreErr) {
                  logger.error(
                     "Failed to restore retired package after install rollback",
                     {
                        error: restoreErr,
                        retiredPath,
                        canonicalPath,
                     },
                  );
               }
            }
            await fs.promises
               .rm(stagingPath, { recursive: true, force: true })
               .catch(() => {});
            if (oldPackage && restored) {
               // The rollback put the old tree back and the previous package is
               // still in `this.packages` (it is only replaced on success
               // below), so it is genuinely still serving. Deleting its status
               // would strand it: listPackages enumerates packageStatuses, so
               // the package would answer getPackage while being invisible to
               // listings and discovery until a restart.
               this.setPackageStatus(packageName, PackageStatus.SERVING);
            } else {
               // Either there was nothing to fall back to (a first install), or
               // the restore did not happen: the rename-back threw, or the old
               // tree was never on disk to retire. The canonical path is then
               // missing or still holds the rejected content, so the cached
               // package no longer matches disk and must not be advertised as
               // serving. Drop both, and keep the two maps agreeing.
               this.deletePackageStatus(packageName);
               if (oldPackage) {
                  // Retire before dropping it, the same way every other eviction
                  // here does: once it leaves this.packages, closeAllConnections
                  // can no longer reach its MalloyConfig, so its native handles
                  // would never be released. Retire rather than shut down
                  // inline, because withPackageLock does not cover queries that
                  // already took this Package from an earlier getPackage; the
                  // drain lets those finish first.
                  this.retireConnectionGeneration(
                     `package ${packageName}`,
                     () => oldPackage.getMalloyConfig().shutdown("close"),
                  );
               }
               this.packages.delete(packageName);
            }
            logger.debug("install.phase2.rollback", {
               environmentName: this.environmentName,
               packageName,
               restored,
               errorName: err instanceof Error ? err.name : "Unknown",
            });
            throw err;
         }

         // Best-effort manifest bind happens after the swap commits, outside the
         // rollback window: a manifest that can't be fetched must not undo an
         // otherwise-successful install (the package serves live instead).
         await this.bindManifestIfConfigured(newPackage);
         await this.rebindServeBindingsFromLocalStore(newPackage);

         this.packages.set(packageName, newPackage);
         this.notifyPackageLoaded(newPackage);
         this.setPackageStatus(packageName, PackageStatus.SERVING);
         // Publishing a fixed package clears the boot failure it replaces.
         this.clearPackageLoadFailure(packageName);

         if (oldPackage) {
            this.retireConnectionGeneration(`package ${packageName}`, () =>
               oldPackage.getMalloyConfig().shutdown("close"),
            );
         }

         if (retiredPath) {
            const pathToClean = retiredPath;
            setImmediate(() => {
               logger.debug("install.phase3.retired_cleanup", {
                  environmentName: this.environmentName,
                  packageName,
                  retiredPath: pathToClean,
               });
               void fs.promises
                  .rm(pathToClean, { recursive: true, force: true })
                  .catch((err) => {
                     logger.warn(
                        `Failed to clean up retired package directory ${pathToClean}`,
                        { error: err },
                     );
                  });
            });
         }

         // Metadata the caller sent with the install (a publish's
         // `manifestLocation`, an update's whole body) is applied under this
         // same lock hold. Applied as a second, separately locked step, a
         // delete queued behind the swap ran first and the update then found
         // no package and answered 404 for work that had completed.
         if (options.update !== undefined) {
            await this._updatePackageLocked(packageName, options.update, {
               recordLocation: true,
            });
         }

         return newPackage;
      });
   }

   /**
    * Reload every model in a package against the supplied build manifest,
    * holding the per-package mutex for the duration of the disk reads.
    * Replaces direct `Package.reloadAllModels` calls from outside
    * `Environment`.
    *
    * Skips the recompile when there is nothing to substitute now AND nothing was
    * substituted before — the same condition {@link bindManifest} applies to a
    * pure-storage manifest flip, and for the same reason: a same-connection
    * `tableName` manifest is resolved at COMPILE time, so an empty one over an
    * already-empty one changes nothing a recompile could express. The previous
    * state has to be checked too, because a package that just dropped its last
    * colocated entry still has to recompile to revert the substitution.
    *
    * This matters because every materialization run lands here. Without the
    * guard a `storage=`-only package — or one with no persist sources at all —
    * paid a full package recompile per run: measured at ~1.1MB of RSS per run on
    * the production image, never reclaimed, against a `main` build that reclaims.
    */
   public async reloadAllModelsForPackage(
      packageName: string,
      manifest: FreshnessManifest,
      versionId?: string,
   ): Promise<void> {
      assertSafePackageName(packageName);
      const slot = this.resolveSlot(packageName, versionId);
      return this.trackPackageLoad(packageName, () =>
         this.withSlotLock(slot, async () => {
            const pkg = this.packages.get(slot.key);
            if (!pkg) {
               throw new PackageNotFoundError(
                  `Package ${packageName} is not loaded`,
               );
            }
            const has = Object.keys(manifest).length > 0;
            const had = pkg.hasBoundTableNameManifest();
            if (!has && !had) return;
            await pkg.reloadAllModels(manifest);
         }),
      );
   }

   /**
    * Bind a package's `storage=` serve bindings from a build's FULL manifest
    * entries (carrying `storageDestinationName` + captured `schema`), so a query
    * against a materialized-into-storage source can be routed through the
    * virtual-source serve transform. Distinct from
    * {@link reloadAllModelsForPackage}, which binds the tableName-only manifest
    * for same-connection persistence. No-op (logged) if the package isn't
    * loaded — best-effort, like the post-build auto-load itself.
    */
   public async bindPackageStorageServeBindings(
      packageName: string,
      entries: Record<string, ManifestEntry>,
      versionId?: string,
   ): Promise<void> {
      assertSafePackageName(packageName);
      const slot = this.resolveSlot(packageName, versionId);
      return this.withSlotLock(slot, async () => {
         const pkg = this.packages.get(slot.key);
         if (!pkg) {
            logger.warn(
               "Cannot bind storage serve bindings: package not loaded",
               { packageName },
            );
            return;
         }
         // Host-authoritative: when a manifestLocation is bound, the host's
         // manifest is the sole source of storage serve bindings (see
         // bindManifest + rebindServeBindingsFromLocalStore' matching guard). This
         // method is the LOCAL-store binding path (post-build auto-load and the
         // post-delete rebind); on an orchestrated deployment the publisher
         // still writes its own local materialization records, so without this
         // guard a build's auto-load or a routine retire of a superseded record
         // would clobber the host's bindings with a possibly-staler local
         // generation. Standalone (no manifestLocation) is unaffected.
         if (pkg.getPackageMetadata().manifestLocation) {
            logger.debug(
               "Skipping local-store storage serve binding: manifestLocation " +
                  "is bound (host authoritative)",
               { packageName },
            );
            return;
         }
         pkg.bindStorageServeBindings(entries);
      });
   }

   /**
    * Re-establish a package's COLOCATED (same-connection) serve routing from a
    * FreshnessManifest, holding the package lock — the colocated analogue of
    * {@link bindPackageStorageServeBindings}, used by the post-delete rebind so a
    * reclaimed colocated table is not left routed. No-op (logged) if the package
    * isn't loaded, and skipped when a `manifestLocation` is bound (host
    * authoritative, same as the storage variant). An empty manifest reverts to
    * serving live.
    *
    * Unlike the storage tier — and unlike the load-time colocated rebind
    * ({@link Package.bindColocatedServeManifest}, which restores routing onto
    * FRESHLY-compiled models with a per-query overlay, no recompile) — this path
    * runs against models that a build's auto-load already recompiled with the
    * substitution BAKED IN. Clearing the per-query overlay would not strip a
    * baked substitution, so reverting/rebinding here requires a recompile
    * ({@link Package.reloadAllModels}), exactly as {@link bindManifest} does for
    * colocated. Skipped when there is nothing to substitute AND nothing was
    * previously substituted to clear (no needless recompile for a storage-only or
    * never-materialized package).
    */
   public async bindPackageColocatedServeManifest(
      packageName: string,
      entries: FreshnessManifest,
      versionId?: string,
   ): Promise<void> {
      assertSafePackageName(packageName);
      const slot = this.resolveSlot(packageName, versionId);
      // A recompile, so reported as loading while it runs (see trackPackageLoad).
      return this.trackPackageLoad(packageName, () =>
         this.withSlotLock(slot, async () => {
            const pkg = this.packages.get(slot.key);
            if (!pkg) {
               logger.warn(
                  "Cannot bind colocated serve manifest: package not loaded",
                  { packageName },
               );
               return;
            }
            if (pkg.getPackageMetadata().manifestLocation) {
               logger.debug(
                  "Skipping local-store colocated serve binding: manifestLocation " +
                     "is bound (host authoritative)",
                  { packageName },
               );
               return;
            }
            const hasColocated = Object.keys(entries).length > 0;
            const hadColocated = pkg.hasBoundTableNameManifest();
            if (hasColocated || hadColocated) {
               await pkg.reloadAllModels(entries);
            }
         }),
      );
   }

   /**
    * If the freshly-loaded package declares a `manifestLocation`, fetch the
    * control-plane-computed build manifest and rebind its models so persist
    * references resolve to the materialized tables. Best-effort: a fetch/bind
    * failure logs a warning and leaves the package serving live (the models are
    * already loaded without a manifest). Callers must hold the package lock —
    * this rebinds `pkg` in place rather than re-entering {@link withPackageLock}.
    */
   private async bindManifestIfConfigured(pkg: Package): Promise<void> {
      const manifestLocation = pkg.getPackageMetadata().manifestLocation;
      if (!manifestLocation) {
         return;
      }
      await this.bindManifest(pkg, manifestLocation);
   }

   /** Fetch + bind a specific manifest URI onto an already-loaded package. */
   private async bindManifest(
      pkg: Package,
      manifestLocation: string,
   ): Promise<void> {
      const packageName = pkg.getPackageName();
      try {
         // Bind runs before a package is marked SERVING, so a slow/unreachable
         // manifest store must not block serving indefinitely — bound is the
         // intended state, live is the degraded fallback. Race the fetch against
         // a timeout and fall back to live on either failure.
         const { tableNameManifest, storageEntries } =
            await this.fetchManifestEntriesWithTimeout(manifestLocation);

         // Tier split. Storage entries (cross-connection) bind as serve bindings
         // WITHOUT a recompile — they apply to the already-compiled models via
         // Model.setServeBindings. The host is the authoritative producer here,
         // so this also supersedes the local-store rebind (see
         // rebindServeBindingsFromLocalStore, which no-ops when manifestLocation
         // is set).
         // Bind whenever there ARE storage entries OR there WERE (bindStorage-
         // ServeBindings with the now-empty set clears them): a manifest whose
         // storage entries vanished must drop the old bindings, not leave them
         // routing at a table the host no longer vouches for. Mirrors the
         // hadColocated guard below.
         //
         // Each bind overwrites the state its own guard reads, so take both
         // `had*` reads before either tier applies.
         const hasStorage = Object.keys(storageEntries).length > 0;
         const hadStorage = pkg.hasStorageServeBindings();

         // colocated entries drive the same-connection tableName substitution, which
         // is resolved at COMPILE time — so they require a reloadAllModels
         // recompile (v0's existing cost). Skip the recompile for a pure-storage
         // manifest flip: only when there is nothing to substitute AND nothing
         // previously substituted to clear (otherwise a package that just dropped
         // its last colocated entry must still recompile to revert it).
         const hasColocated = Object.keys(tableNameManifest).length > 0;
         const hadColocated = pkg.hasBoundTableNameManifest();

         // Recompile FIRST. `reloadAllModels` is the only step here that can throw
         // (a compile error, or the worker pool being unavailable), and the catch
         // below reports `live_fallback` — "no manifest applied". Binding storage
         // entries before it would leave the NEW manifest's cross-connection
         // bindings installed and routing while the package reports that nothing
         // was applied and `boundManifestUri` still names the OLD manifest: a
         // caller reading the status as "not serving from manifest tables" would be
         // wrong about tables this package is actively serving from.
         if (hasColocated || hadColocated) {
            await pkg.reloadAllModels(tableNameManifest);
         }

         // Then storage. Ordering between the two is otherwise immaterial:
         // `reloadAllModels` re-pushes whatever bindings are current onto the fresh
         // model set, and `bindStorageServeBindings` pushes its own, so the new set
         // lands either way.
         if (hasStorage || hadStorage) {
            pkg.bindStorageServeBindings(storageEntries);
         }

         // Both tiers have applied by here, so the package is bound regardless of
         // which one carried entries — a pure-`storage=` manifest binds without a
         // colocated entry to count, and deriving the status from that count
         // alone reports `unbound` after a bind that fully succeeded.
         //
         // This is deliberately unconditional, and supersedes the status
         // `recordManifestBinding` derived if the recompile ran: a manifest that
         // binds nothing (empty, or every entry skipped) is still a manifest that
         // was fetched and applied, and reporting it `unbound` would make it
         // permanent drift to a caller that rebinds on anything but `bound`.
         // `manifestEntryCount` and `storageServeBindings` are what distinguish
         // "bound and serving" from "bound and empty".
         pkg.markManifestBound(manifestLocation);
         recordManifestBind("success");
         logger.info("Bound build manifest to package", {
            environmentName: this.environmentName,
            packageName,
            manifestLocation,
            tableNameEntryCount: Object.keys(tableNameManifest).length,
            storageEntryCount: Object.keys(storageEntries).length,
            recompiled: hasColocated || hadColocated,
         });
      } catch (err) {
         pkg.markManifestBindFailed();
         const timedOut =
            err instanceof Error && err.message.includes("Timed out after");
         recordManifestBind(timedOut ? "timeout" : "failure");
         logger.warn("Failed to bind build manifest; serving live", {
            environmentName: this.environmentName,
            packageName,
            manifestLocation,
            timedOut,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }

   /**
    * Fetch manifest entries, rejecting if the fetch exceeds
    * {@link MANIFEST_FETCH_TIMEOUT_MS}. Keeps {@link bindManifest}'s bind-before-
    * serve guarantee from stalling on an unreachable manifest store.
    */
   private async fetchManifestEntriesWithTimeout(
      manifestLocation: string,
   ): Promise<FetchedManifest> {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<never>((_, reject) => {
         timer = setTimeout(
            () =>
               reject(
                  new Error(
                     `Timed out after ${MANIFEST_FETCH_TIMEOUT_MS}ms fetching manifest ${manifestLocation}`,
                  ),
               ),
            MANIFEST_FETCH_TIMEOUT_MS,
         );
      });
      try {
         return await Promise.race([
            fetchManifestEntries(manifestLocation),
            timeout,
         ]);
      } finally {
         if (timer) {
            clearTimeout(timer);
         }
      }
   }

   /**
    * Read a model's source text from disk, holding the per-package mutex
    * so the read is serialized against {@link installPackage} /
    * {@link deletePackage} / {@link updatePackage}.
    */
   public async getModelFileText(
      packageName: string,
      modelPath: string,
      versionId?: string,
   ): Promise<string> {
      assertSafePackageName(packageName);
      assertSafeRelativeModelPath(modelPath);
      return this.withResolvedSlotLock(packageName, versionId, async (slot) => {
         const pkg = this.packages.get(slot.key);
         if (!pkg) {
            throw new PackageNotFoundError(
               `Package ${packageName} is not loaded`,
            );
         }
         return pkg.getModelFileText(modelPath);
      });
   }

   private async writePackageManifest(
      packageName: string,
      metadata: {
         name: string;
         description?: string;
         location?: string;
         explores?: string[];
         queryableSources?: "declared" | "all";
         manifestLocation?: string | null;
         scope?: ApiPackage["scope"];
         queryMetadata?: ApiPackage["queryMetadata"];
         materialization?: ApiPackage["materialization"];
      },
   ): Promise<void> {
      const packagePath = safeJoinUnderRoot(this.environmentPath, packageName);
      const manifestPath = safeJoinUnderRoot(packagePath, "publisher.json");

      try {
         // Read existing manifest
         let existingManifest: Record<string, unknown> = {};
         try {
            const content = await fs.promises.readFile(manifestPath, "utf-8");
            existingManifest = JSON.parse(content);
         } catch (_err) {
            logger.warn(`Could not read manifest for ${packageName}`);
         }

         const onDiskMaterialization =
            existingManifest.materialization !== null &&
            typeof existingManifest.materialization === "object" &&
            !Array.isArray(existingManifest.materialization)
               ? (existingManifest.materialization as Record<string, unknown>)
               : undefined;

         // Scope has two homes: `materialization.scope` (canonical) and the
         // manifest root (deprecated). The server writes BOTH, in sync, for as
         // long as the root form is supported:
         //
         //  - writing only the root would author a manifest this build's loader
         //    refuses, since a root that disagrees with an existing envelope is a
         //    conflict (see resolvePackageScope);
         //  - writing only the envelope would silently downgrade a package read
         //    by an older publisher, which knows only the root and would default
         //    to `package` — cross-version table reuse for a package declared
         //    `version`.
         //
         // A caller can only express scope through the top-level `scope` field
         // (the wire materialization block has no `scope`), so an envelope value
         // already on disk is preserved rather than dropped by a materialization
         // PATCH that says nothing about it.
         const resolvedScope =
            metadata.scope ??
            (onDiskMaterialization?.scope as ApiPackage["scope"] | undefined) ??
            (existingManifest.scope as ApiPackage["scope"] | undefined);

         // A materialization PATCH replaces the block wholesale, which is right
         // for schedule and freshness — they are the policy the caller is
         // setting, and they are mutually exclusive with each other.
         // `queryMetadata` is orthogonal to both: a client setting a schedule
         // has no reason to re-send the package's tags, and dropping them
         // silently untags every statement the package's builds issue. So it is
         // preserved on omission, like `scope` above.
         //
         // A NULL is preserve too, not a clear — one rule at both wire homes.
         // Null used to clear here while null on the canonical field preserved,
         // which protected a client that serializes unset fields as null only
         // if it had already migrated off this home. The clear is an EMPTY bag,
         // which is unambiguous and which no such client emits by accident.
         const preservedQueryMetadata =
            metadata.materialization !== undefined &&
            metadata.materialization?.queryMetadata == null &&
            onDiskMaterialization?.queryMetadata !== undefined
               ? { queryMetadata: onDiskMaterialization.queryMetadata }
               : {};

         const materializationBase: Record<string, unknown> | undefined =
            metadata.materialization !== undefined
               ? { ...metadata.materialization, ...preservedQueryMetadata }
               : onDiskMaterialization !== undefined
                 ? { ...onDiskMaterialization }
                 : undefined;
         const materializationBlock =
            resolvedScope !== undefined
               ? { ...(materializationBase ?? {}), scope: resolvedScope }
               : materializationBase;

         // `queryMetadata` has two homes as well, migrating the opposite way to
         // `scope`: the manifest ROOT is canonical and the envelope is
         // deprecated. Same dual-write rule and the same reason — writing only
         // the root would leave an older publisher, which reads only the
         // envelope, serving untagged statements for a package that asked to be
         // tagged.
         //
         // A caller can express tags at either wire home, so precedence follows
         // the same order the manifest resolver uses: the canonical top-level
         // field, then the deprecated block, then what is already on disk. A
         // client that has not migrated keeps working; one that has is not
         // overruled by a block it did not send.
         //
         // A null anywhere in this chain is "not provided", never a clear — the
         // one rule at both homes (see preservedQueryMetadata). Clearing is an
         // empty bag, which parses to "no tags" and reaches both homes like any
         // other value.
         const resolvedQueryMetadata =
            metadata.queryMetadata ??
            (materializationBlock as { queryMetadata?: unknown } | undefined)
               ?.queryMetadata ??
            (existingManifest as { queryMetadata?: unknown }).queryMetadata;

         // Update with new metadata. `explores`/`queryableSources` are only
         // overwritten when the caller explicitly provides them; otherwise the
         // existing on-disk value is preserved via the spread (an undefined here
         // must not erase it).
         // A convention-derived surface is never written. A GET of an
         // index.malloy-curated package echoes the surface the server derived,
         // so an ordinary read-modify-write PATCH -- one that meant to change
         // only the description -- hands that derived value back, and writing
         // it would freeze a surface that tracks the file into a key that does
         // not. The two behave identically until the file is renamed or
         // replaced: the convention then follows it, while the frozen key names
         // a model that no longer exists, and the package silently lists and
         // serves nothing. Recognizable because it is the exact value the
         // convention produces, in a manifest that declares no `explores` (a
         // surface naming a file that is not there was already rejected
         // upstream by formatInvalidExplores, so the file exists). Declaring it
         // by hand buys nothing the convention does not already give.
         const echoesDerivedSurface =
            existingManifest.explores === undefined &&
            Array.isArray(metadata.explores) &&
            metadata.explores.length === 1 &&
            metadata.explores[0] === INDEX_MODEL_NAME;

         const updatedManifest = {
            ...existingManifest,
            name: metadata.name,
            // Only when provided: an undefined here is dropped by
            // JSON.stringify and so would erase the description on disk, which
            // a PATCH that did not mention it never meant.
            ...(metadata.description !== undefined
               ? { description: metadata.description }
               : {}),
            ...(metadata.explores !== undefined && !echoesDerivedSurface
               ? { explores: metadata.explores }
               : {}),
            ...(metadata.queryableSources !== undefined
               ? { queryableSources: metadata.queryableSources }
               : {}),
            ...(metadata.manifestLocation !== undefined
               ? { manifestLocation: metadata.manifestLocation }
               : {}),
            ...(resolvedScope !== undefined ? { scope: resolvedScope } : {}),
            ...(resolvedQueryMetadata !== undefined
               ? { queryMetadata: resolvedQueryMetadata }
               : {}),
            // Mirrored into the deprecated home too, so a package tagged
            // through the canonical field still reads as tagged on a publisher
            // that only knows the envelope. Only when that home already exists
            // or the caller wrote to it: mirroring unconditionally introduced a
            // `materialization` block into a manifest that never had one, which
            // is the shape being migrated AWAY from, appearing in a file whose
            // author only ever used the canonical field.
            ...(materializationBlock !== undefined
               ? {
                    materialization: {
                       ...materializationBlock,
                       ...(resolvedQueryMetadata !== undefined
                          ? { queryMetadata: resolvedQueryMetadata }
                          : {}),
                    },
                 }
               : {}),
         };

         // Write back to file
         await fs.promises.writeFile(
            manifestPath,
            JSON.stringify(updatedManifest, null, 2),
            "utf-8",
         );
         // The install location lives in the server's own record outside the
         // package directory, so an in-place reload and a restart know where
         // the package came from, and nothing the package's content carries
         // (a `location` in publisher.json, a file of this name in a downloaded
         // tree) is ever read as one. Written only when an install supplies
         // it; never cleared from here.
         if (metadata.location !== undefined && metadata.location !== "") {
            const recordPath = this.installRecordPath(packageName);
            await fs.promises.mkdir(path.dirname(recordPath), {
               recursive: true,
            });
            await fs.promises.writeFile(
               recordPath,
               JSON.stringify({ location: metadata.location }, null, 2),
               "utf-8",
            );
         }

         logger.info(`Updated publisher.json for ${packageName}`);
      } catch (error) {
         logger.error(`Failed to update publisher.json`, { error });
         throw new Error(`Failed to update package manifest`, { cause: error });
      }
   }

   public async updatePackage(packageName: string, body: ApiPackage) {
      assertSafePackageName(packageName);
      this.assertNotVersioned(packageName, "be updated in place");
      // An install downloads before it takes the package lock. A PATCH that
      // arrives then finds the lock free and, during a reinstall, the previous
      // copy resident; applied at once it would land on that copy, and what
      // it wrote would be swapped away a moment later, or kept by a rollback
      // as if the new content had arrived. So a PATCH waits for every load in
      // flight and is applied to whichever copy is resident afterwards. If
      // nothing is, the lookup below answers 404 as for any package that is
      // not here.
      await this.awaitPackageLoads(packageName);
      return this.withPackageLock(packageName, () => {
         // Again under the lock: a first versioned publish may have committed
         // since the check above.
         this.assertNotVersioned(packageName, "be updated in place");
         return this._updatePackageLocked(packageName, body, {
            recordLocation: false,
         });
      });
   }

   /**
    * Apply a metadata PATCH to a loaded package. Assumes the caller holds the
    * per-package mutex: {@link updatePackage} takes it for a standalone PATCH,
    * and {@link installPackage} calls this inside its own hold so an install
    * and the metadata that came with it land as one operation.
    */
   private async _updatePackageLocked(
      packageName: string,
      body: ApiPackage,
      options: {
         /**
          * Whether the body's `location` is recorded as where the package was
          * installed from. True only for the metadata an install applies to the
          * copy it just installed: a metadata PATCH never changes it, because a
          * PATCH naming a different location is a reinstall, decided before it
          * gets here.
          */
         recordLocation: boolean;
      },
   ) {
      const _package = this.packages.get(packageName);
      if (!_package) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      if (body.name) {
         _package.setName(body.name);
      }
      // Preserve `explores` across a metadata PATCH. `setPackageMetadata`
      // replaces the whole object, so a name/description-only update must
      // carry the existing discovery surface through — otherwise the
      // in-memory `explores` is wiped and `listModels()` silently starts
      // serving every model until the next reload. When the body explicitly
      // carries `explores`, honor the new set instead.
      const existing = _package.getPackageMetadata();
      // Normalize API-body explores through the same helper the worker uses
      // for on-disk explores, so `["./index.malloy"]` / backslash paths
      // validate and persist identically regardless of input channel (no
      // misleading publish-time 400, no publish-vs-reload divergence).
      const normalizedExplores = body.explores?.map(normalizeModelPath);
      const explores =
         normalizedExplores !== undefined
            ? normalizedExplores
            : existing.explores;
      const queryableSources =
         body.queryableSources != null
            ? body.queryableSources
            : existing.queryableSources;
      // Preserve the existing manifestLocation unless the body explicitly
      // sets it (including to null, which clears it and reverts to live).
      const manifestLocation =
         body.manifestLocation !== undefined
            ? body.manifestLocation
            : existing.manifestLocation;
      // Persist `scope` and `materialization` (the schedule cron) are
      // editable via the API — both are writable in the schema. When the
      // body carries a value, apply it; otherwise preserve the
      // manifest-derived one (a name/description-only PATCH must not wipe
      // them, and the control plane must not misread the gap as a removal).
      // Changing the schedule re-arms the standalone scheduler on its next
      // tick — no reload needed.
      //
      // A *null* scope/materialization is treated the same as omitted
      // (preserve), NOT a wipe: the control plane's post-build rebind PATCH
      // carries only name/location/manifestLocation, but a client that
      // serializes unset fields as explicit null must not thereby trip the
      // policy gate below (a rejection there fails the orchestrated run) or
      // reset the persisted policy. `manifestLocation` is deliberately
      // different — null there means "clear" (revert to live), which the
      // caller's orchestrated build path relies on.
      const scopeProvided = body.scope != null;
      const materializationProvided = body.materialization != null;
      const editingPolicy = scopeProvided || materializationProvided;
      const scope = scopeProvided ? body.scope : existing.scope;
      // Preserved unless provided, for the same reason as scope: this
      // replaces the whole metadata object, so omitting it would make a
      // name/description-only PATCH silently untag every query the package
      // emits until the next reload. A null is treated as omitted for the
      // same reason as scope — a client that serializes unset fields as null
      // must not thereby untag a package. Clearing is an empty bag, which no
      // such client produces by accident.
      //
      // Resolved ONCE across both wire homes and then written to BOTH, the
      // way writePackageManifest resolves the file. Setting them
      // independently left whichever home the caller did not send holding a
      // stale bag, and the two are read by different paths: the serve path
      // takes the canonical field through getDeclaredQueryMetadata, the build
      // path takes the block. A migrated client PATCHing only the canonical
      // field therefore tagged its served queries with the new bag and its
      // builds with the old one, and getPackageMetadata returned the two
      // homes contradicting each other on a schema that promises both.
      const queryMetadata =
         body.queryMetadata ??
         body.materialization?.queryMetadata ??
         existing.queryMetadata ??
         existing.materialization?.queryMetadata ??
         null;
      const materializationBase = materializationProvided
         ? body.materialization
         : existing.materialization;
      const materialization =
         materializationBase || queryMetadata !== null
            ? { ...(materializationBase ?? {}), queryMetadata }
            : materializationBase;
      // `setPackageMetadata` replaces the whole object, so every field the
      // body omits is carried through from the existing metadata, the way
      // `explores` and `manifestLocation` below already are. A PATCH that
      // names only a new `manifestLocation` (the post-build rebind) must not
      // drop the `location` the package was installed from, which is what a
      // later reload reinstalls from, or the `resource` the orchestrator
      // identifies the package by. A null counts as omitted, as it does for
      // `scope` above: a client that serializes unset fields as null must not
      // blank them. An empty string is a value, and clears a description.
      _package.setPackageMetadata({
         name: body.name != null ? body.name : existing.name,
         description:
            body.description != null ? body.description : existing.description,
         resource: body.resource != null ? body.resource : existing.resource,
         location:
            options.recordLocation && body.location != null
               ? body.location
               : existing.location,
         explores,
         queryableSources,
         manifestLocation,
         materialization,
         queryMetadata,
         scope,
      });

      // Strict-reject, symmetric with the publish path
      // (package.controller.addPackage): validate the resulting explores
      // against the live model set and restore the prior metadata before
      // rejecting, so a bad update neither persists nor mutates the served
      // surface. When the body edits the persistence policy (scope /
      // materialization), also enforce the same scope/schedule/freshness/cron
      // rules a publish enforces — but only then, so a description-only PATCH
      // on a package with a pre-existing (load-tolerated) policy warning is
      // not newly rejected. Cron validity is one of these rules
      // (persistencePolicyWarnings Rule 4), so publish, PATCH, load, and the
      // scheduler all enforce it identically.
      const policyMsg = editingPolicy
         ? _package.formatInvalidPersistencePolicy()
         : "";
      // The explores check is gated the same way: a body that does not touch
      // `explores` (a rebind, the location an install records, a reload) must
      // not fail after the swap on a surface the load itself only warned
      // about, leaving the new tree serving with no record and no binding.
      const exploresMsg =
         normalizedExplores !== undefined
            ? _package.formatInvalidExplores()
            : "";
      const invalidMsg = [exploresMsg, policyMsg].filter(Boolean).join("\n");
      if (invalidMsg) {
         _package.setPackageMetadata(existing);
         throw new BadRequestError(invalidMsg);
      }

      await this.writePackageManifest(packageName, {
         name: packageName,
         description: body.description ?? undefined,
         location: options.recordLocation
            ? (body.location ?? undefined)
            : undefined,
         explores: normalizedExplores,
         queryableSources: body.queryableSources ?? undefined,
         manifestLocation: body.manifestLocation,
         // Only write when explicitly provided (non-null): mirrors the
         // null-as-absent rule above, so a rebind PATCH neither wipes the
         // persisted policy nor writes a stray `scope: null`.
         scope: scopeProvided ? body.scope : undefined,
         queryMetadata: body.queryMetadata ?? undefined,
         materialization: materializationProvided
            ? body.materialization
            : undefined,
      });

      // When the body changes manifestLocation, apply it now so the new
      // binding takes effect without a separate reload: a URI rebinds models
      // to the materialized tables; null/empty reverts the package to live.
      const revertsBinding =
         body.manifestLocation !== undefined &&
         !body.manifestLocation &&
         (_package.hasBoundTableNameManifest() ||
            _package.hasStorageServeBindings());
      if (body.manifestLocation) {
         // A rebind may recompile the package (colocated manifest entries
         // resolve at compile time), so it is reported as loading while it
         // runs, like every other recompile.
         await this.trackPackageLoad(packageName, () =>
            this.bindManifest(_package, body.manifestLocation as string),
         );
      } else if (revertsBinding) {
         // Revert to live: drop the colocated tableName substitution AND the
         // cross-connection storage serve bindings the prior bindManifest
         // applied, so no query still routes to a materialized table after
         // the operator explicitly cleared the manifest. A package with
         // nothing bound has nothing to revert, so a null from a client that
         // serializes unset fields does not recompile it.
         await this.trackPackageLoad(packageName, async () => {
            await _package.reloadAllModels({});
            _package.bindStorageServeBindings({});
         });
      } else {
         // The surface may have changed with no file changing, so the tile
         // findings are re-checked against it. (A manifest rebind above
         // reloads, which re-discovers and re-lints on its own.)
         await _package.relintDashboards();
      }

      return _package.getPackageMetadata();
   }

   public getPackageStatus(packageName: string): PackageInfo | undefined {
      return this.packageStatuses.get(packageName);
   }

   /**
    * Packages this environment is actually serving: registered statuses minus
    * any recorded as failed or un-mounted. Disjoint from getFailedPackages()
    * by construction, so the readiness line's packages= and load_errors=
    * cannot double-count a package that is seeded SERVING at boot and only
    * pruned later by a side-effect load (which a transient DB or memory-
    * pressure error can skip). Cheap: no package load is triggered.
    */
   public getServingPackageCount(): number {
      const failed = this.getFailedPackages();
      let serving = 0;
      for (const name of this.packageStatuses.keys()) {
         if (!failed.has(name)) serving += 1;
      }
      return serving;
   }

   /**
    * Record why a configured package's location never mounted, so /status can
    * name the real cause instead of the missing-manifest fallout it produces.
    * Called by EnvironmentStore right after the environment is created.
    */
   public setPackageMountError(packageName: string, message: string): void {
      this.mountErrors.set(packageName, message);
   }

   /**
    * Record a runtime add that failed before the package could serve, so
    * /status reports it the way it reports a configured package whose location
    * never mounted. Skipped while the name has any status: a failed re-install
    * rolls back to the previous tree, which is not a failed package. Cleared
    * like every other load failure, by a later successful add or install of
    * the name, or by deleting it.
    *
    * Names are the caller's, so the record is bounded: past
    * MAX_RECORDED_ADD_FAILURES the oldest recorded add failure is dropped.
    * Boot-time mount errors are not subject to the cap.
    */
   public recordPackageAddFailure(packageName: string, message: string): void {
      if (this.packageStatuses.has(packageName)) return;
      if (!this.mountErrors.has(packageName)) {
         this.recordedAddFailures.push(packageName);
         while (this.recordedAddFailures.length > MAX_RECORDED_ADD_FAILURES) {
            const evicted = this.recordedAddFailures.shift();
            if (evicted !== undefined) this.mountErrors.delete(evicted);
         }
      }
      this.mountErrors.set(packageName, message);
   }

   /** Forget any recorded failure for a package, whatever its cause. */
   private clearPackageLoadFailure(packageName: string): void {
      this.failedPackages.delete(packageName);
      this.mountErrors.delete(packageName);
      const recorded = this.recordedAddFailures.indexOf(packageName);
      if (recorded !== -1) this.recordedAddFailures.splice(recorded, 1);
      this.staleCompileErrors.delete(packageName);
   }

   /**
    * Packages configured for, or added to, this environment that did not load,
    * and why.
    */
   public getFailedPackages(): ReadonlyMap<string, string> {
      if (this.mountErrors.size === 0) return this.failedPackages;
      // Mount errors last, so the specific cause overwrites the generic
      // manifest error that the un-mounted package produces on its lazy load.
      return new Map([...this.failedPackages, ...this.mountErrors]);
   }

   /**
    * SERVING packages whose most recent reload failed to compile: the served
    * model is stale relative to the files on disk. Disjoint from
    * {@link getFailedPackages} by construction (a successful load clears the
    * entry; a failed first load lands in failedPackages instead).
    */
   public getStaleCompileErrors(): ReadonlyMap<
      string,
      { message: string; failedAt: string }
   > {
      return this.staleCompileErrors;
   }

   public setPackageStatus(packageName: string, status: PackageStatus): void {
      const currentStatus = this.packageStatuses.get(packageName);
      this.packageStatuses.set(packageName, {
         name: packageName,
         loadTimestamp: currentStatus?.loadTimestamp || Date.now(),
         status: status,
      });
   }

   public deletePackageStatus(packageName: string): void {
      this.packageStatuses.delete(packageName);
   }

   /**
    * Delete a package: every version of a versioned one. `forget` runs under
    * the same package lock, and is where the caller removes the package's
    * database rows: outside the lock, a publish of the same name that commits
    * in between would have its rows erased.
    *
    * For a versioned package `forget` runs first. Its rows are what a restart
    * restores the package from, so if removing them fails nothing else has
    * changed, the package keeps serving, and a retry starts clean; the other
    * way round, a failure left a package gone from this process that came back
    * from its rows on the next restart. An unversioned package keeps the
    * original order.
    */
   public async deletePackage(
      packageName: string,
      options: { forget?: () => Promise<void> } = {},
   ): Promise<void> {
      assertSafePackageName(packageName);
      return this.withPackageLock(packageName, async () => {
         if (this.isVersionedPackage(packageName)) {
            await options.forget?.();
            await this.deletePackageLocked(packageName);
            return;
         }
         await this.deletePackageLocked(packageName);
         await options.forget?.();
      });
   }

   private async deletePackageLocked(packageName: string): Promise<void> {
      // Clear the load failure before the early return, not after it. A
      // package that failed to load is not in `packages` (the load catch
      // evicts it), so deleting it takes the early return every time, while
      // the controller still drops its config row. Leaving the entry would
      // make getStatus report a loadError for a package that is no longer
      // configured, which is the one thing that channel must not do.
      this.clearPackageLoadFailure(packageName);

      if (this.isVersionedPackage(packageName)) {
         await this.deleteVersionedPackageLocked(packageName);
         return;
      }

      const _package = this.packages.get(packageName);
      if (!_package) {
         return;
      }
      const packageStatus = this.packageStatuses.get(packageName);

      // The mutex now serializes load/install/compile against delete, so
      // the LOADING-state guard is mostly vestigial — left in place for
      // backwards-compatible error messaging in case anything bypasses
      // the lock.
      if (packageStatus?.status === PackageStatus.LOADING) {
         logger.error("Package loading. Can't unload.", {
            environmentName: this.environmentName,
            packageName,
         });
         throw new Error(
            "Package loading. Can't unload. " +
               this.environmentName +
               " " +
               packageName,
         );
      } else if (packageStatus?.status === PackageStatus.SERVING) {
         this.setPackageStatus(packageName, PackageStatus.UNLOADING);
      }

      // Retire the package's connections via the existing 30s drain so
      // any in-flight queries that already acquired a connection finish
      // before the underlying duckdb handle is released.
      this.retireConnectionGeneration(`package ${packageName}`, () =>
         _package.getMalloyConfig().shutdown("close"),
      );

      // Atomically rename the canonical tree out of the way so no reader
      // can stat into it after the lock is released. The actual fs.rm is
      // deferred to setImmediate to keep the lock-hold time at one
      // rename rather than a (potentially slow) recursive remove.
      const canonicalPath = safeJoinUnderRoot(
         this.environmentPath,
         packageName,
      );
      const retiredPath = this.allocateRetiredPath(packageName);
      let renamed = false;
      try {
         await fs.promises.mkdir(path.dirname(retiredPath), {
            recursive: true,
         });
         await fs.promises.rename(canonicalPath, retiredPath);
         renamed = true;
      } catch (err) {
         logger.error(
            "Error renaming package directory to retired during unload",
            {
               error: err,
               environmentName: this.environmentName,
               packageName,
            },
         );
      }

      this.packages.delete(packageName);
      this.packageStatuses.delete(packageName);
      await fs.promises
         .rm(this.installRecordPath(packageName), { force: true })
         .catch(() => {});

      if (renamed) {
         setImmediate(() => {
            void fs.promises
               .rm(retiredPath, { recursive: true, force: true })
               .catch((err) => {
                  logger.warn(
                     `Failed to clean up retired package directory ${retiredPath}`,
                     { error: err },
                  );
               });
         });
      }
   }

   /**
    * Delete every published version of a package: unload each loaded version
    * under its own lock (so a load in flight finishes before its files go),
    * then move the whole package directory aside in one rename and remove it
    * after the lock is released, the way an unversioned delete does. The
    * caller holds the package lock; the registry rows go with the package row.
    */
   private async deleteVersionedPackageLocked(
      packageName: string,
   ): Promise<void> {
      this.setPackageStatus(packageName, PackageStatus.UNLOADING);
      const versions = this.listPackageVersions(packageName).versions;
      // Forget the versions before unloading them, so a load that resolved
      // one before this delete and is waiting on its lock re-reads it there
      // (see _loadVersionLocked), finds it gone, and loads nothing.
      this.clearPackageVersions(packageName);
      for (const version of versions) {
         const key = `${packageName}@${version.dirName}`;
         await this.getOrCreatePackageMutex(key).runExclusive(async () => {
            const pkg = this.packages.get(key);
            if (!pkg) return;
            this.retireConnectionGeneration(`package ${key}`, () =>
               pkg.getMalloyConfig().shutdown("close"),
            );
            this.packages.delete(key);
         });
      }
      this.packageStatuses.delete(packageName);

      const packageDir = safeJoinUnderRoot(this.environmentPath, packageName);
      const retiredPath = this.allocateRetiredPath(packageName);
      try {
         await fs.promises.mkdir(path.dirname(retiredPath), {
            recursive: true,
         });
         await fs.promises.rename(packageDir, retiredPath);
         this.removeRetiredLater(retiredPath);
      } catch (err) {
         logger.error(
            "Error renaming a versioned package directory to retired during delete",
            { error: err, environmentName: this.environmentName, packageName },
         );
      }
      // A package installed unversioned before its first versioned publish
      // may still have the record of where it was installed from.
      await fs.promises
         .rm(this.installRecordPath(packageName), { force: true })
         .catch(() => {});
   }

   /**
    * Evict a package from the in-memory caches WITHOUT touching its on-disk
    * directory — the non-destructive counterpart to {@link deletePackage}.
    *
    * Used to roll back a no-location `addPackage` (which registers a
    * *pre-existing*, user-owned directory) when post-load validation rejects
    * it: deleting the tree there would destroy content the publisher never
    * created. This still drains and closes the connections the just-created
    * `Package` opened, so the duckdb handle isn't leaked.
    */
   public async unloadPackage(packageName: string): Promise<void> {
      assertSafePackageName(packageName);
      // Only an unversioned package is registered from a pre-existing directory,
      // which is what this rolls back; a versioned one has nothing to unload
      // here (its versions leave memory with deletePackage or archive).
      if (this.isVersionedPackage(packageName)) return;
      return this.withPackageLock(packageName, async () => {
         const _package = this.packages.get(packageName);
         if (!_package) {
            return;
         }
         if (
            this.packageStatuses.get(packageName)?.status ===
            PackageStatus.SERVING
         ) {
            this.setPackageStatus(packageName, PackageStatus.UNLOADING);
         }
         // Same 30s connection drain as deletePackage — just no fs rename/rm.
         this.retireConnectionGeneration(`package ${packageName}`, () =>
            _package.getMalloyConfig().shutdown("close"),
         );
         // Same reason deletePackage clears: a recorded failure describes a
         // package that is serving or configured, and after this it is neither.
         // Today no entry can survive to here (the only caller evicts a package
         // that addPackage just created, and addPackage clears on success), but
         // the eviction and the clear belong together whoever calls next.
         this.clearPackageLoadFailure(packageName);
         this.packages.delete(packageName);
         this.packageStatuses.delete(packageName);
      });
   }

   public updateConnections(
      malloyConfig: EnvironmentMalloyConfig,
      _apiConnections?: ApiConnection[],
      afterPreviousRelease?: () => Promise<void>,
   ): void {
      const previousMalloyConfig = this.malloyConfig;
      this.malloyConfig = malloyConfig;
      this.apiConnections = malloyConfig.apiConnections;

      if (previousMalloyConfig !== malloyConfig) {
         this.retireConnectionGeneration(
            `environment ${this.environmentName}`,
            async () => {
               await previousMalloyConfig.releaseConnections();
               await afterPreviousRelease?.();
            },
         );
      } else {
         void afterPreviousRelease?.();
      }
   }

   public async deleteConnection(connectionName: string): Promise<void> {
      const index = this.apiConnections.findIndex(
         (conn) => conn.name === connectionName,
      );

      if (index !== -1) {
         this.apiConnections.splice(index, 1);
      }

      if (index !== -1) {
         logger.info(
            `Removed connection ${connectionName} from environment ${this.environmentName}`,
         );
      } else {
         logger.warn(
            `Connection ${connectionName} not found in environment ${this.environmentName}`,
         );
      }
   }

   public async closeAllConnections(): Promise<void> {
      // Release the package-scoped MalloyConfigs (each holds the package's own
      // sandbox `duckdb` connection) before tearing down the environment config
      // they wrap. Without this, hard unload leaks per-package DuckDB handles.
      const packageReleases = await Promise.allSettled(
         Array.from(this.packages.values(), (pkg) =>
            pkg.getMalloyConfig().shutdown("close"),
         ),
      );
      for (const result of packageReleases) {
         if (result.status === "rejected") {
            logger.error(
               `Error closing package connections for environment ${this.environmentName}`,
               { error: result.reason },
            );
         }
      }
      this.packages.clear();
      this.packageStatuses.clear();

      try {
         await this.malloyConfig.releaseConnections();
      } catch (error) {
         logger.error(
            `Error closing connections for environment ${this.environmentName}`,
            { error },
         );
      }

      try {
         await this.destinationMalloyConfig.releaseConnections();
      } catch (error) {
         // `{ error }` alone serializes an Error to `{}`, which is what a reader
         // of this line gets told. Carry the message so a shutdown failure is
         // diagnosable from the log rather than only from a debugger.
         logger.error(
            `Error closing storage destinations for environment ${this.environmentName}`,
            { error: error instanceof Error ? error.message : String(error) },
         );
      }

      this.destinations = [];
      // Torn down, so the empty list above describes nothing rather than
      // describing an environment with no destinations — anything that reconciled
      // storage against it from here would be pruning on no information.
      this.destinationsAuthoritative = false;
      await this.releaseAllRetiredConnectionGenerations();

      this.apiConnections = [];

      logger.info(
         `Closed all connections for environment ${this.environmentName}`,
      );
   }

   /**
    * The environment as the API returns it. `everyLoadedVersion` lists every
    * loaded version of a versioned package rather than only its `latest`
    * (see {@link listPackages}); `/status` asks for it.
    */
   public async serialize(
      options: { everyLoadedVersion?: boolean; includeLoading?: boolean } = {},
   ): Promise<ApiEnvironment> {
      return {
         ...this.metadata,
         // Credentials stay server-side, the same rule storageDestinations
         // below already followed. listApiConnections() is the internal view
         // and still carries them, because compiling and connecting need them.
         connections: toPublicConnections(this.listApiConnections()),
         // Name and type only. This is what the status endpoint reports, so it
         // is how an operator or orchestrator confirms which destinations a
         // worker picked up; the configs behind them stay server-side.
         storageDestinations: this.destinations.map(({ name, type }) => ({
            name,
            type,
         })),
         packages: await this.listPackages(options),
      };
   }

   public async deleteDuckDBConnection(connectionName: string): Promise<void> {
      const duckdbPath = path.join(
         this.environmentPath,
         `${connectionName}.duckdb`,
      );
      try {
         await fs.promises.rm(duckdbPath, { force: true });
         logger.info(
            `Removed DuckDB connection file ${connectionName} from environment ${this.environmentName}`,
         );
      } catch (error) {
         logger.error(
            `Failed to remove DuckDB connection file ${connectionName} from environment ${this.environmentName}`,
            { error },
         );
      }
   }

   public async deleteDuckLakeConnection(
      connectionName: string,
   ): Promise<void> {
      await deleteDuckLakeConnectionFile(connectionName, this.environmentPath);
      logger.info(
         `Removed DuckLake connection ${connectionName} from environment ${this.environmentName}`,
      );
   }
}

/**
 * Extracts the preamble from a Malloy model file — the leading block of
 * `##!` pragmas, `import` statements, blank lines, and comments that appear
 * before any `source:`, `query:`, or `run:` definition. This allows a
 * submitted query to inherit the model's import context.
 */
export async function extractPreamble(modelPath: string): Promise<string> {
   try {
      const content = await fs.promises.readFile(modelPath, "utf8");
      return extractPreambleFromSource(content);
   } catch {
      // If the model file can't be read, return empty preamble
      // and let the compilation surface any import errors naturally.
      return "";
   }
}

/**
 * Extracts the preamble from Malloy source text. Exported for testing.
 */
export function extractPreambleFromSource(content: string): string {
   const lines = content.split("\n");
   const preambleLines: string[] = [];

   for (const line of lines) {
      const trimmed = line.trim();
      // Stop at the first source/query/run definition
      if (
         trimmed.startsWith("source:") ||
         trimmed.startsWith("query:") ||
         trimmed.startsWith("run:")
      ) {
         break;
      }
      preambleLines.push(line);
   }

   return preambleLines.join("\n").trimEnd();
}
