import * as fs from "fs/promises";
import * as path from "path";

import "@malloydata/db-duckdb/native";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   Connection,
   ConnectionRuntime,
   contextOverlay,
   EmptyURLReader,
   FixedConnectionMap,
   MalloyConfig,
   MalloyError,
   SourceDef,
} from "@malloydata/malloy";
import { publisherMeter } from "../telemetry";
import recursive from "recursive-readdir";
import { components } from "../api";
import { getPackageLoadPool } from "../package_load/package_load_pool";
import {
   API_PREFIX,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import {
   BadRequestError,
   ModelCompilationError,
   PackageNotFoundError,
   ServiceUnavailableError,
} from "../errors";
import { applyExtensionSessionSettings } from "./connection";
import { isExploresConventionWarning } from "./package_manifest";
import { formatDuration, logger } from "../logger";
import {
   recordBuildPlanComputeDuration,
   recordManifestBindDegraded,
} from "../materialization_metrics";
import {
   LOAD_DURATION_BUCKETS_MS,
   recordPackageLoadPhases,
   type PackageLoadStatus,
} from "../package_load_metrics";
import { assertSafeEnvironmentPath, safeJoinUnderRoot } from "../path_safety";
import {
   BuildManifest,
   BuildPlan,
   FreshnessManifest,
   ManifestEntry,
} from "../storage/DatabaseInterface";
import { errMessage, ignoreDotfiles } from "../utils";
import { getPersistCollisionEnforce, getPersistStorageMode } from "../config";
import { deriveServeBindings } from "./materialization_serve_transform";
import { computePackageBuildPlan, SourceEligibility } from "./build_plan";
import {
   incrementalPolicyAdvisories,
   incrementalPolicyRejections,
   type IncrementalPolicySource,
} from "./incremental_policy";
import { materializationConfigWarnings } from "./materialization_config_validation";
import { CronEvaluator } from "./cron_evaluator";
import { filterFreshManifest } from "./freshness";
import { isQuotedIdentifierPath, quoteManifestTablePath } from "./quoting";
import { Model } from "./model";
import { assertPersistNamesQuoted } from "./persist_annotation_validation";

type ApiDatabase = components["schemas"]["Database"];
type ApiModel = components["schemas"]["Model"];
type ApiNotebook = components["schemas"]["Notebook"];
export type ApiPackage = components["schemas"]["Package"];
type ApiPackageWarning = NonNullable<ApiPackage["warnings"]>[number];
type ApiColumn = components["schemas"]["Column"];
type ApiTableDescription = components["schemas"]["TableDescription"];
// A thunk lets callers pass a live reference to the *current* environment
// MalloyConfig so the package wrapper resolves environment connections against the
// generation that's active at lookup time, not the one that was current when
// the package was first loaded.
type PackageConnectionInput =
   | MalloyConfig
   | Map<string, Connection>
   | (() => MalloyConfig);

/**
 * Project the full wire entries down to the Malloy-runtime binding map
 * (`sourceEntityId -> { tableName }`) used to hydrate models. The freshness
 * fields are dropped here — they gate the serve path per query, not model
 * hydration.
 */
function toTableNameManifest(
   entries: FreshnessManifest,
): BuildManifest["entries"] {
   const out: BuildManifest["entries"] = {};
   for (const [sourceEntityId, entry] of Object.entries(entries)) {
      out[sourceEntityId] = { tableName: entry.tableName };
   }
   return out;
}

/**
 * Classify a failed package load into the `status` label shared by the
 * `malloy_package_load_duration` histogram and the per-phase load metrics, so
 * both slice failures identically. A real Malloy/model compile error is a 4xx
 * `compilation_error`; a gate refusing what the package declares is a 4xx
 * `policy_rejected`; a rewrapped pool-infrastructure failure is a transient
 * `pool_unavailable`; anything else is a generic `error`.
 */
function packageLoadFailureStatus(error: unknown): PackageLoadStatus {
   if (error instanceof ModelCompilationError || error instanceof MalloyError) {
      return "compilation_error";
   }
   if (error instanceof BadRequestError) {
      return "policy_rejected";
   }
   if (error instanceof ServiceUnavailableError) {
      return "pool_unavailable";
   }
   return "error";
}

export class Package {
   private environmentName: string;
   private packageName: string;
   private packageMetadata: ApiPackage;
   private databases: ApiDatabase[];
   private models: Map<string, Model> = new Map();
   private packagePath: string;
   private malloyConfig: MalloyConfig;
   /**
    * Resolves the environment's storage destinations for a serve-shape
    * compile. Set by the owning Environment; see
    * {@link setServeDestinationConfig}.
    */
   private serveDestinationConfig?: () => MalloyConfig;
   // Build-manifest binding state (Malloy Persistence v0). When bound, these
   // entries (sourceEntityId -> { tableName }) are what served queries use to route
   // persist sources to their materialized physical tables; they are also reused
   // by the /compile preview so previewed SQL matches executed SQL. Surfaced on
   // /status (via getPackageMetadata) so the control plane can confirm a worker
   // actually bound the distributed manifest instead of inferring it from logs.
   private buildManifestEntries: BuildManifest["entries"] | undefined;
   // The full wire entries retained at bind (sourceEntityId -> { tableName,
   // dataAsOf, freshnessWindowSeconds, freshnessFallback }). Unlike
   // {@link buildManifestEntries} (the tableName-only projection baked into the
   // hydration runtime and reused by /compile), this keeps the control-plane
   // freshness fields so the serve path can re-evaluate `age vs window` per
   // query. Undefined when the package is serving live (unbound).
   private freshnessEntries: FreshnessManifest | undefined;
   // `storage=` serve bindings (materialized-into-storage sources), derived from
   // a build's full manifest entries and pushed onto each Model so a query can
   // route through the virtual-source serve transform. Distinct from the
   // same-connection freshnessEntries above (which the manifest substitution
   // uses); a storage source serves cross-connection via bindings, not the
   // manifest. Empty ⇒ no storage serve routing.
   private storageServeBindings: ReturnType<typeof deriveServeBindings> = [];
   // Memoized freshness-filtered manifest for the serve path. Almost all queries
   // in a window share the same included set, so this is recomputed only when a
   // retained entry actually crosses its window (`validUntil`, the next
   // staleSince) or when a rebind replaces the entries (cleared in
   // recordManifestBinding). validUntil === Infinity means no included entry has
   // an evaluable window, so the result is stable until the next rebind.
   private freshManifestCache:
      | { manifest: BuildManifest["entries"]; validUntil: number }
      | undefined;
   private manifestBindingStatus: "unbound" | "bound" | "live_fallback" =
      "unbound";
   private manifestEntryCount = 0;
   private boundManifestUri: string | null = null;
   // The package's persist build plan: a deterministic property of the compiled
   // package (per-source sourceEntityId, columns, build SQL, dependency graphs),
   // computed once at load from the live (unbound) models so it is stable for a
   // given (package version, connection config). Null when the package declares
   // no persist source. Surfaced read-only on getPackageMetadata() so a caller
   // can derive build instructions without a separate plan round-trip.
   private buildPlan: BuildPlan | null = null;
   // Sources annotated `#@ persist` that Malloy's getBuildPlan() did not
   // recognize as a materializable build root, so they produced no plan entry
   // and would be a silent no-op (served live). Surfaced as an operator warning
   // and hard-refused at build. See build_plan.detectDroppedPersistSources.
   private droppedPersistSources: { name: string; modelPath: string }[] = [];
   // Which sources may be served from a materialized table, decided at compile
   // (build_plan.collectSourceEligibility) because that is where the compiled
   // sources exist. Consulted when binding serve bindings, which deliberately
   // does not recompile.
   //
   // `undefined` means NOT KNOWN — the plan compute failed (it does live schema
   // RPCs, so a warehouse blip is the ordinary way) and the package loaded
   // anyway. Distinct from "nothing is eligible", and refused the same way: an
   // unknown answer must not read as consent. See bindStorageServeBindings.
   private sourceEligibility: SourceEligibility | undefined = undefined;
   // Per-source incremental-refresh gate inputs (the resolved `refresh=` /
   // `watermark=` / `merge_key=` declaration), keyed by sourceID to match the
   // wire plan. Resolved at
   // compile for the same reason as sourceEligibility: the rules read the
   // compiled output schema and the query definition's field kinds, neither of
   // which the wire plan carries. Empty when the plan compute failed, which
   // leaves the gate silent — safe, because the build path dispatches a delta
   // only for a declaration it can read.
   private incrementalPolicySources: IncrementalPolicySource[] = [];
   // Non-fatal render-tag findings aggregated across the package's models (each
   // tagged with its model path), surfaced read-only on
   // getPackageMetadata().warnings. Refreshed on load and reload. A bad render
   // tag does not fail the load (see Model.validateRenderTags); this is the
   // response-level signal that a tag is misconfigured.
   private renderTagWarnings: ApiPackageWarning[] = [];
   /**
    * Manifest-shape deprecations the load tolerated (a root-level `scope`), kept
    * so publish can report a still-parsing-but-outdated manifest. Not on the wire
    * package: it is a property of the manifest text, not of the loaded package.
    */
   private manifestWarnings: string[] = [];
   /**
    * Warnings raised AFTER load, by an API request rather than by parsing the
    * manifest. Kept separate from {@link manifestWarnings} because that array
    * is replaced wholesale from the worker on every reload, and a reload can
    * happen inside the very request that raised the warning: any PATCH
    * carrying `manifestLocation` triggers one, and every GET emits
    * `manifestLocation: null`, so a client that round-trips the whole object
    * always sends it. Storing these here means a warning cannot be destroyed
    * by the same request that produced it.
    *
    * Cleared when the condition they describe stops holding, which for all of
    * them is the surface origin changing to declared.
    */
   private postLoadWarnings: string[] = [];
   /**
    * True when `explores` was defaulted from the `index.malloy` convention
    * rather than declared in publisher.json.
    *
    * Deliberately a field on the Package rather than part of `packageMetadata`:
    * `setPackageMetadata` replaces that object wholesale (see
    * Environment.updatePackageMetadata), so a name-only PATCH would drop the
    * flag and silently promote a convention-derived surface into an explicit
    * one, which would switch the query boundary on. Refreshed on load and
    * reload, exactly like {@link manifestWarnings}.
    */
   private exploresFromConvention = false;
   private static meter = publisherMeter();
   private static packageLoadHistogram = this.meter.createHistogram(
      "malloy_package_load_duration",
      {
         description: "Time taken to load a Malloy package",
         unit: "ms",
         // OTel's default buckets top out at 10s, censoring the slow-load tail.
         // Use the shared load-duration buckets (→5min) so p95/p99 of large
         // package loads are resolvable. See LOAD_DURATION_BUCKETS_MS.
         advice: { explicitBucketBoundaries: LOAD_DURATION_BUCKETS_MS },
      },
   );

   constructor(
      environmentName: string,
      packageName: string,
      packagePath: string,
      packageMetadata: ApiPackage,
      databases: ApiDatabase[],
      models: Map<string, Model>,
      malloyConfig: MalloyConfig = new MalloyConfig({ connections: {} }),
      /**
       * Whether `explores` came from the `index.malloy` convention. A
       * constructor parameter rather than a post-construction assignment
       * because the two policy pushes below read it, so setting it afterwards
       * would compute the query boundary as if the surface were explicit.
       */
      exploresFromConvention = false,
   ) {
      this.environmentName = environmentName;
      this.packageName = packageName;
      this.packagePath = packagePath;
      this.packageMetadata = packageMetadata;
      this.databases = databases;
      this.models = models;
      this.malloyConfig = malloyConfig;
      this.exploresFromConvention = exploresFromConvention;
      this.applyDiscoveryPolicyToModels();
      this.applyQueryBoundaryToModels();
   }

   /** True when the package has a non-empty discovery surface, from either
    *  source. Single source of truth for the curation/listing derivations so
    *  they can't drift out of sync. NOT the query-boundary predicate: see
    *  {@link boundaryDeclared}, which is deliberately narrower. */
   private exploresDeclared(): boolean {
      const explores = this.packageMetadata.explores;
      return !!(explores && explores.length > 0);
   }

   /**
    * True when the package opts into the QUERY BOUNDARY, which requires an
    * `explores` the author actually wrote in publisher.json.
    *
    * This is narrower than {@link exploresDeclared} on purpose, and it is the
    * one place the two axes diverge. A surface defaulted from the
    * `index.malloy` convention curates listings but must not gate queries: the
    * boundary denies with a 404 that is deliberately indistinguishable from
    * "does not exist" (see errors.ts), so switching it on because a file with
    * a particular name appeared would revoke query access on an existing
    * package with no config edit and no actionable error. Declaring `explores`
    * is the explicit opt-in; the convention only ever adds curation, never
    * takes access away.
    */
   private boundaryDeclared(): boolean {
      return this.exploresDeclared() && !this.exploresFromConvention;
   }

   /** The declared explore set, or null when discovery is uncurated. */
   private exploreSet(): Set<string> | null {
      const explores = this.packageMetadata.explores;
      return explores && explores.length > 0 ? new Set(explores) : null;
   }

   /**
    * Push the discovery-curation policy down onto each Model. Curation (file
    * listing plus within-file `export {}` filtering) is enabled whenever the
    * package has a surface, from EITHER source: a declared `explores` or the
    * `index.malloy` convention. Only a package with neither keeps the legacy
    * uncurated listings. Re-derived on reload and metadata PATCH, since the
    * inputs can change there.
    *
    * Note this is the listing axis only. Access is {@link boundaryDeclared},
    * which counts the declared source alone.
    */
   private applyDiscoveryPolicyToModels(): void {
      const curationEnabled = this.exploresDeclared();
      for (const model of this.models.values()) {
         model.setDiscoveryCuration(curationEnabled);
      }
   }

   /**
    * Push the package-level query-boundary policy down onto each Model so the
    * query chokepoints can enforce it without a back-reference to the Package:
    * `Model.getQueryResults` (the HTTP query route and the MCP tool) and the
    * `/compile` path (via `assertQueryBoundaryForRunnable`). Derived once here
    * (and on reload) rather than per query: the policy only changes when the
    * manifest is (re)read.
    *
    * Policy: queryable == discoverable. The boundary is inert unless `explores`
    * is declared IN publisher.json (no curated surface ⇒ nothing to restrict,
    * and a surface defaulted from the `index.malloy` convention does not
    * count; see {@link boundaryDeclared}) AND `queryableSources` is "declared"
    * (the default; "all" decouples the axes). When active, a model file is a
    * query entry point only if it is listed in `explores`; within-file
    * curation (`export {}`) is read off each Model.
    */
   private applyQueryBoundaryToModels(): void {
      // Both derive from the boundary predicate, not the listing one: under the
      // convention every model stays a valid query entry point.
      const boundaryDeclared = this.boundaryDeclared();
      const exploreSet = boundaryDeclared ? this.exploreSet() : null;
      const mode =
         this.packageMetadata.queryableSources === "all" ? "all" : "declared";
      for (const [modelPath, model] of this.models) {
         model.setQueryBoundary({
            mode,
            exploresDeclared: boundaryDeclared,
            isQueryEntryPoint: exploreSet ? exploreSet.has(modelPath) : true,
         });
      }
   }

   static async create(
      environmentName: string,
      packageName: string,
      packagePath: string,
      environmentMalloyConfig: PackageConnectionInput,
      /**
       * Delete `packagePath` if the load fails. Opt-in, and only correct for a
       * caller that created the directory itself (an install staged into place),
       * where the half-built tree is Publisher's to clean up and `installPackage`
       * rolls the previous one back. Every other caller loads a directory that
       * already existed: a reload of a package that is currently serving, or a
       * user directory registered via addPackage. Deleting those on a transient
       * compile error destroys the source and takes the package offline, so the
       * default is to leave the directory alone.
       */
      cleanupDirectoryOnFailure: boolean = false,
   ): Promise<Package> {
      assertSafeEnvironmentPath(packagePath);
      const startTime = performance.now();
      await Package.validatePackageManifestExistsOrThrowError(packagePath);
      const manifestValidationTime = performance.now();
      logger.info("Package manifest validation completed", {
         packageName,
         duration: formatDuration(manifestValidationTime - startTime),
      });

      try {
         // The MalloyConfig is always built on the main thread — it
         // owns the live native connection handles the package needs
         // to *serve queries* after load (workers can't share native
         // handles across the V8 isolate boundary). The worker proxies
         // non-duckdb connection lookups back through this MalloyConfig
         // during compile.
         const malloyConfig = Package.buildPackageMalloyConfig(
            packagePath,
            typeof environmentMalloyConfig === "function"
               ? environmentMalloyConfig
               : () => Package.toMalloyConfig(environmentMalloyConfig),
         );

         return await Package.loadViaWorker(
            environmentName,
            packageName,
            packagePath,
            malloyConfig,
            startTime,
            manifestValidationTime,
         );
      } catch (error) {
         logger.error(`Error loading package ${packageName}`, { error });
         console.error(error);
         const endTime = performance.now();
         const executionTime = endTime - startTime;
         this.packageLoadHistogram.record(executionTime, {
            malloy_package_name: packageName,
            status: packageLoadFailureStatus(error),
         });
         // Clean up the package directory only when the caller opted in (an
         // install that staged this tree), and never when packagePath is an
         // in-place mount symlink (watch mode). Removing it would unmount the
         // package, so a transient compile error from a half-typed model saved
         // mid-edit would brick the package until a restart. The symlink points
         // at the user's live source, which is left untouched; the next save
         // recompiles against it.
         try {
            if (!cleanupDirectoryOnFailure) {
               logger.info(
                  `Preserving existing package directory after failed load: ${packagePath}`,
               );
            } else {
               const stat = await fs.lstat(packagePath).catch(() => null);
               if (stat?.isSymbolicLink()) {
                  logger.info(
                     `Skipping cleanup of symlinked package path on failure: ${packagePath}`,
                  );
               } else {
                  await fs.rm(packagePath, { recursive: true, force: true });
                  logger.info(
                     `Cleaned up failed package directory: ${packagePath}`,
                  );
               }
            }
         } catch (cleanupError) {
            logger.warn(`Failed to clean up package directory ${packagePath}`, {
               error: cleanupError,
            });
         }
         throw error;
      }
   }

   /**
    * Load the package via the package-load worker pool. The worker
    * performs the CPU-bound bulk of the load off-thread (manifest
    * read, every `.malloy` / `.malloynb` compile) and ships back a
    * structured-clonable `LoadPackageOutcome`. Database probes
    * (`.parquet` / `.csv`) run on the main thread, in parallel with
    * the worker compile, against the package's existing DuckDB
    * connection — they're async-IO-bound and don't compete with the
    * worker for CPU.
    *
    * Pool-infrastructure failures (worker crash, RPC timeout, pool
    * shutting down) are rewrapped as `ServiceUnavailableError` so
    * the HTTP layer responds 503 (transient, retryable). Real compile
    * errors (`MalloyError` / `ModelCompilationError`) propagate
    * unchanged so they keep their 4xx mapping.
    */
   private static async loadViaWorker(
      environmentName: string,
      packageName: string,
      packagePath: string,
      malloyConfig: MalloyConfig,
      startTime: number,
      manifestValidationTime: number,
   ): Promise<Package> {
      const pool = getPackageLoadPool();
      const dispatchTime = performance.now();
      // Submit the worker job and run database probing on the main
      // thread in parallel. We isolate the worker-job promise inside
      // a wrapper so we can map pool-infrastructure failures (worker
      // crash, RPC timeout, pool shutting down) to a 503 without
      // accidentally re-mapping `readDatabases`'s own errors.
      const workerOutcome = pool
         .loadPackage({
            packagePath,
            packageName,
            malloyConfig,
            defaultConnectionName: "duckdb",
         })
         .catch((err: unknown) => {
            // Compile errors surface in-band via
            // `LoadPackageOutcome.models[i].compilationError`; if the
            // pool itself rejects, it's an infra-side failure
            // (shutting down, worker spawn failed, worker crashed,
            // RPC timeout) and the client should retry. Real Malloy
            // compile errors deserialised by the pool still carry
            // their MalloyError / ModelCompilationError identity —
            // let those bubble untouched so they keep their 4xx
            // mapping in `errors.ts`.
            const realError =
               err instanceof Error
                  ? err
                  : new Error(
                       `Package-load worker pool failure: ${String(err)}`,
                    );
            if (
               realError instanceof MalloyError ||
               realError instanceof ModelCompilationError
            ) {
               throw realError;
            }
            throw new ServiceUnavailableError(
               `Package-load worker pool unavailable: ${realError.message}`,
            );
         });
      const [outcome, databases] = await Promise.all([
         workerOutcome,
         Package.readDatabases(packagePath, malloyConfig),
      ]);
      const workerDoneTime = performance.now();
      logger.info("Package load via worker pool completed", {
         packageName,
         manifestValidationMs: dispatchTime - manifestValidationTime,
         workerDurationMs: outcome.loadDurationMs,
         dispatchOverheadMs:
            workerDoneTime - dispatchTime - outcome.loadDurationMs,
         // Phase split of the worker duration (remainder = setup + extraction).
         compileDurationMs: outcome.timings.compileDurationMs,
         schemaFetchDurationMs: outcome.timings.schemaFetchDurationMs,
         schemaFetchCount: outcome.timings.schemaFetchCount,
         modelCount: outcome.models.length,
         databaseCount: databases.length,
      });

      // Override the manifest-derived resource URI — the worker only
      // returns name/description from publisher.json, but the rest of
      // the API surface expects a `resource` field too.
      const packageConfig: ApiPackage = {
         name: outcome.packageMetadata.name,
         description: outcome.packageMetadata.description,
         resource: `${API_PREFIX}/environments/${environmentName}/packages/${packageName}`,
         explores: outcome.packageMetadata.explores,
         queryableSources: outcome.packageMetadata.queryableSources,
         manifestLocation: outcome.packageMetadata.manifestLocation ?? null,
         // Always surface a non-null `materialization` object once the package
         // has loaded (schedule null when the manifest declares no policy). The
         // control plane treats object-present as the authoritative "this is
         // what the manifest says" signal and object-absent as "metadata not
         // available this request" — so it must never be dropped to null on a
         // successfully loaded package, or the CP can misread a transient
         // absence as a schedule removal. See `parsePackageMaterialization`.
         materialization: outcome.packageMetadata.materialization ?? {
            schedule: null,
            freshness: null,
         },
         // Package-level persist scope mode, applied uniformly to every persist
         // source/index. Defaults to "package" (cross-version reuse) when the
         // manifest omits it.
         scope: outcome.packageMetadata.scope ?? "package",
      };

      // Build live `Model`s from worker output. Any per-model compile
      // failure aborts the load — matches the historical behaviour of
      // `Package.create` failing the whole package on the first model
      // error. (`Package.reloadAllModels` keeps the failed-model
      // placeholders instead; that branch goes through a different
      // hydration path.)
      const models = new Map<string, Model>();
      const renderTagWarnings: ApiPackageWarning[] = [];
      try {
         for (const sm of outcome.models) {
            if (sm.compilationError) {
               const err = Model.deserializeCompilationError(
                  sm.compilationError,
               );
               logger.error("Model compilation failed", {
                  packageName,
                  modelPath: sm.modelPath,
                  error: err.message,
               });
               // The outer catch in Package.create records the total metric +
               // cleans the package directory.
               throw err;
            }
            const model = Model.fromSerialized(
               packageName,
               packagePath,
               malloyConfig,
               sm,
            );
            // Validate renderer tags on the main thread (the renderer is too
            // heavy to load inside the pure-CPU package-load worker). A
            // misconfigured tag is logged as a warning naming the subject; it
            // does not fail the load. The findings also ride the package
            // response as non-fatal `warnings`.
            for (const w of await model.validateRenderTags()) {
               renderTagWarnings.push({
                  model: sm.modelPath,
                  // Spelled out rather than spread. A spread is exempt from
                  // excess-property checking exactly like a named interface, so
                  // `...w` would carry a stale field name through a rename in
                  // `api-doc.yaml` without the compiler noticing.
                  subject: w.subject,
                  message: w.message,
                  severity: w.severity,
               });
            }
            // Reject unquoted `#@ persist name=` annotations the same way: an
            // unquoted name is dropped from the build plan, so the source would
            // publish but never materialize. Scan the raw `.malloy` source (the
            // ground truth for quoting); throws a ModelCompilationError (424).
            if (sm.modelPath.endsWith(MODEL_FILE_SUFFIX)) {
               const modelSource = await fs.readFile(
                  path.join(packagePath, sm.modelPath),
                  "utf-8",
               );
               assertPersistNamesQuoted(modelSource, sm.modelPath);
            }
            models.set(sm.modelPath, model);
         }
      } catch (err) {
         // Record the load's phase cost tagged with the terminal status before
         // the error propagates to the outer catch (which records the total).
         // Only in-band compile failures reach here — the worker already
         // produced `outcome.timings`; a pool failure throws before `outcome`
         // exists and carries no timings, so it's simply not recorded.
         recordPackageLoadPhases(
            outcome.timings,
            packageLoadFailureStatus(err),
         );
         throw err;
      }

      const endTime = performance.now();
      const executionTime = endTime - startTime;
      this.packageLoadHistogram.record(executionTime, {
         malloy_package_name: packageName,
         status: "success",
      });
      recordPackageLoadPhases(outcome.timings, "success");
      logger.info(`Successfully loaded package ${packageName}`, {
         packageName,
         duration: formatDuration(executionTime),
      });

      const pkg = new Package(
         environmentName,
         packageName,
         packagePath,
         packageConfig,
         databases,
         models,
         malloyConfig,
         outcome.packageMetadata.exploresFromConvention ?? false,
      );
      pkg.renderTagWarnings = renderTagWarnings;
      pkg.manifestWarnings = outcome.packageMetadata.manifestWarnings ?? [];
      pkg.logExploresConvention();
      // Install the per-query freshness resolver on the freshly-built models.
      // At create time no manifest is bound yet, so the resolver returns
      // undefined (serve live) until a subsequent bindManifest → reloadAllModels.
      pkg.wireFreshnessResolvers();

      // Compute the persist build plan off the live (unbound) models, before the
      // caller binds any configured manifest, so the surfaced plan reflects the
      // canonical build (not the manifest-rewritten SQL). Best-effort: a plan
      // failure is logged, not fatal — the package still serves; the plan is
      // just absent. Recompiles the models (duplicate schema RPCs vs the worker
      // compile); accepted for now.
      //
      // A failure also leaves `sourceEligibility` unknown, which refuses every
      // storage serve binding until a load succeeds (see
      // bindStorageServeBindings). Serving live is a slower answer, not a wrong
      // one; admitting an unexamined binding would be a wrong one.
      try {
         const buildPlanStart = Date.now();
         const {
            plan,
            droppedPersistSources,
            sourceEligibility,
            incrementalDeclarations,
         } = await computePackageBuildPlan(pkg);
         pkg.buildPlan = plan;
         pkg.droppedPersistSources = droppedPersistSources;
         pkg.sourceEligibility = sourceEligibility;
         pkg.incrementalPolicySources = Object.entries(plan?.sources ?? {})
            .filter(([sourceID]) => incrementalDeclarations[sourceID])
            .map(([sourceID, source]) => ({
               sourceName: source.name,
               modelPath: source.modelPath,
               dialect: source.dialect,
               storageDestination: source.annotationFields?.storage,
               // Carried so the gate can spot two sources resolving to ONE
               // address, which collapses them onto one table and one boundary.
               sourceEntityId: source.sourceEntityId,
               declaration: incrementalDeclarations[sourceID],
            }));
         recordBuildPlanComputeDuration(Date.now() - buildPlanStart);
      } catch (err) {
         logger.warn(
            `Failed to compute build plan for package ${packageName}`,
            {
               packageName,
               error: errMessage(err),
            },
         );
      }

      // Fail-safe at load: a bad explores entry doesn't fail the package
      // (its models still load and listModels hides the unmatched entry — it
      // never falls back to listing everything). Warn so the misconfig is
      // visible; the publish path rejects it outright (see package.controller).
      const invalidMsg = pkg.formatInvalidExplores();
      if (invalidMsg) {
         logger.warn(`Package ${packageName} has invalid explores`, {
            packageName,
            detail: invalidMsg,
         });
      }
      // Same fail-safe split for the persistence-policy gate: an existing
      // package whose manifest violates the scope/schedule/freshness rules
      // still loads (warn), but a publish of it is rejected (see
      // package.controller).
      const invalidPolicy = pkg.formatInvalidPersistencePolicy();
      if (invalidPolicy) {
         logger.warn(
            `Package ${packageName} has an invalid persistence policy`,
            {
               packageName,
               detail: invalidPolicy,
            },
         );
      }
      // The incremental-refresh gate does NOT get that fail-safe split: an
      // invalid `#@ persist` declaration fails the load, the way a model that
      // does not compile fails it.
      //
      // Publish cannot be the only strict point, because a package can arrive
      // without one: uploaded to a control plane's storage and loaded by a
      // worker, it never passes through POST/PATCH /packages. Warning here left
      // the rejection in a worker log while the author — the only person who can
      // fix the declaration — saw a clean publish and a source that quietly
      // rebuilt in full forever.
      //
      // BadRequestError, for the 400: a control plane resolves the status off
      // the error and otherwise defaults to 424, which would describe an
      // authoring error as a dependency failure. The message carries EVERY
      // rejection, so two broken declarations take one republish to fix.
      //
      // The accepted cost: `refresh=` was inert metadata before these rules, so
      // a package published earlier saying refresh="incremental" and nothing
      // else now stops serving until it is fixed. That is the one rule with any
      // legacy exposure (watermark= and merge_key= did not exist as keys), the
      // feature is gated behind `##! experimental.persistence`, and a bare
      // refresh="incremental" is an authoring error worth surfacing rather than
      // carrying — so every rule keeps the same severity, with no special cases.
      const invalidIncremental = pkg.formatInvalidIncrementalPolicy();
      if (invalidIncremental) {
         // Logged as well as thrown: the throw is what the author reads, the log
         // is the operator's copy.
         logger.error(
            `Package ${packageName} has an invalid incremental refresh policy`,
            {
               packageName,
               detail: invalidIncremental,
            },
         );
         throw new BadRequestError(invalidIncremental);
      }
      // Persist-target collisions are ALWAYS warn-only at load (never fail an
      // already-published package), regardless of PERSIST_COLLISION_ENFORCE —
      // the flag only governs whether they REJECT a publish (see
      // package.controller). Surface them so an operator can remediate.
      const collisions = pkg.persistenceCollisionWarnings();
      if (collisions.length > 0) {
         logger.warn(`Package ${packageName} has persist-target collisions`, {
            packageName,
            detail: collisions.join("\n"),
         });
      }
      pkg.logEmptyDiscoveryWarnings();

      return pkg;
   }

   public getPackageName(): string {
      return this.packageName;
   }

   /**
    * The package's persist build plan (per-source sourceEntityId, columns, build SQL,
    * dependency graphs), or null when the package declares no persist source.
    * A deterministic property of the compiled package; callers derive build
    * instructions from it for an orchestrated materialization.
    */
   public getBuildPlan(): BuildPlan | null {
      return this.buildPlan;
   }

   /**
    * The package-level `materialization` config (from malloy-publisher.json),
    * the least-specific layer for resolving per-source freshness/schedule in the
    * build plan. Null when the package declares no policy.
    */
   public getMaterializationConfig(): NonNullable<
      ApiPackage["materialization"]
   > | null {
      return this.packageMetadata.materialization ?? null;
   }

   public getPackageMetadata(): ApiPackage {
      // Overlay the server-computed fields onto the stored metadata: the
      // explores misconfig warnings (loading is fail-safe — the package still
      // serves with the bad entry hidden — so this is the only non-log signal
      // that it's broken) and the manifest-binding state (so /status reflects
      // whether persist sources are actually routed to materialized tables).
      // Always returns a copy so these overlays never mutate the stored
      // metadata; the binding fields are authoritative from the private state.
      //
      // `name` is the registered package name (the environment's identity for
      // this package), not the value read from the package's own manifest —
      // those can differ (e.g. a package installed under a different name than
      // its publisher.json declares). `listPackages` already overrides it to
      // the registered name; surfacing it here keeps the single-package GET
      // consistent without relying on a returned-reference mutation.
      const metadata: ApiPackage = {
         ...this.packageMetadata,
         name: this.packageName,
         manifestBindingStatus: this.manifestBindingStatus,
         manifestEntryCount: this.manifestEntryCount,
         boundManifestUri: this.boundManifestUri,
         buildPlan: this.buildPlan,
      };
      const warnings = this.exploreWarnings();
      if (warnings.length > 0) {
         metadata.exploresWarnings = warnings;
      }
      // Render-tag findings and storage= warnings share the one operator-facing
      // warnings array (the {model, subject, message} shape carries both).
      const allWarnings = [
         ...this.renderTagWarnings,
         ...this.storageWarnings(),
         ...this.droppedPersistWarnings(),
         // A within-package persist-target collision spans two or more sources, so
         // there is no single subject field; the message names them. Surfaced here
         // (alongside the load-path log) so an operator can see it on the status
         // API like the other persist warnings — see persistenceCollisionWarnings.
         ...this.persistenceCollisionWarnings().map((message) => ({ message })),
         // Incremental declarations that are LEGAL but probably not what the
         // author meant: an unrecognized persist key (the only guard against a
         // typo'd merge_key=, which degrades silently), and the keyless-delta
         // advisory. Never rejections — see incrementalPolicyAdvisories.
         ...incrementalPolicyAdvisories(this.incrementalPolicySources),
         // Materialization-config findings: a queryMetadata property that will
         // not do what it says, and manifest shapes that still parse but are
         // deprecated. Advisory by design — none of these blocks a publish.
         ...materializationConfigWarnings({
            packageMaterialization: this.packageMetadata.materialization,
            sources: this.buildPlan?.sources
               ? Object.values(this.buildPlan.sources)
               : [],
            // Both channels, so a warning raised by the request in flight is
            // reported even when that same request triggered a reload.
            manifestWarnings: [
               ...this.manifestWarnings,
               ...this.postLoadWarnings,
            ],
         }),
      ];
      if (allWarnings.length > 0) {
         metadata.warnings = allWarnings;
      }
      // Surface what's bound for the cross-connection storage serve so a caller
      // can confirm a materialized source is routed (vs. inferring from logs).
      if (this.storageServeBindings.length > 0) {
         metadata.storageServeBindings = this.storageServeBindings.map((b) => ({
            sourceName: b.sourceName,
            storageDestinationName: b.destinationName,
            tablePath: b.tablePath,
         }));
      }
      return metadata;
   }

   /**
    * The currently-bound build-manifest entries (sourceEntityId -> { tableName }), or
    * undefined when the package is serving live. Reused by the /compile preview
    * so previewed SQL gets the same persist-source -> physical-table routing as
    * execution.
    */
   public getBuildManifestEntries(): BuildManifest["entries"] | undefined {
      return this.buildManifestEntries;
   }

   /**
    * Whether the package currently has a bound (non-empty) same-connection
    * `tableName` manifest — i.e. a prior bind substituted colocated physical tables
    * at compile time. Used by the manifest-rebind tier split to decide whether a
    * pure-storage refresh can skip the {@link reloadAllModels} recompile: it can
    * only skip when there is nothing to substitute now AND nothing was
    * substituted before (otherwise dropping the last colocated entry must recompile
    * to revert it).
    */
   public hasBoundTableNameManifest(): boolean {
      return (
         this.buildManifestEntries !== undefined &&
         Object.keys(this.buildManifestEntries).length > 0
      );
   }

   /**
    * Whether the package currently has (non-empty) `storage=` serve bindings.
    * Used by the manifest-rebind tier split so a refresh whose storage entries
    * vanished still clears the old bindings (rather than leaving them routing at
    * a table the host no longer vouches for) — the storage mirror of
    * {@link hasBoundTableNameManifest}.
    */
   public hasStorageServeBindings(): boolean {
      return this.storageServeBindings.length > 0;
   }

   /**
    * The freshness-filtered build manifest for the serve path, evaluated at
    * `now`. Persistence.md §9.3 Phase B: a query on a `#@ persist` source may
    * use its materialized table only while the table is within its declared
    * freshness window; otherwise it falls back per the entry's `fallback`
    * (`live`/`fail` drop the entry → serve live; `stale_ok` keeps it → serve the
    * stale table). Un-gated entries (no window) always route to the table.
    *
    * Undefined when the package is serving live (unbound) — callers then apply
    * no per-query override and the runtime serves live.
    *
    * Memoized: recomputed only when a retained entry crosses its window
    * (monotonic fresh→stale, so a single `validUntil` deadline suffices) or when
    * a rebind replaces the entries. Cheap O(1) hit on the common path.
    */
   public getFreshBuildManifest(
      now: number = Date.now(),
   ): BuildManifest["entries"] | undefined {
      if (!this.freshnessEntries) return undefined;
      if (this.freshManifestCache && now < this.freshManifestCache.validUntil) {
         return this.freshManifestCache.manifest;
      }
      const { manifest, nextStaleSince } = filterFreshManifest(
         this.freshnessEntries,
         new Date(now),
      );
      this.freshManifestCache = {
         manifest,
         validUntil: nextStaleSince ?? Infinity,
      };
      return manifest;
   }

   /**
    * Install the per-query freshness resolver on every owned model so the serve
    * path (Model.getQueryResults / executeNotebookCell) applies the
    * freshness-filtered manifest as a per-query Malloy `buildManifest` override.
    * Idempotent; called after (re)building the model set.
    */
   private wireFreshnessResolvers(): void {
      for (const model of this.models.values()) {
         model.setFreshnessResolver(() => this.getFreshBuildManifest());
      }
   }

   /**
    * Record the URI whose manifest is currently bound to the served models. May
    * differ from `manifestLocation` after an in-memory auto-load following a
    * materialization build (no URI), in which case it stays null.
    */
   public setBoundManifestUri(uri: string | null): void {
      this.boundManifestUri = uri;
   }

   /**
    * Mark that a configured `manifestLocation` could not be fetched/bound, so
    * the package is serving live despite intending to be materialized-routed.
    */
   public markManifestBindFailed(): void {
      this.manifestBindingStatus = "live_fallback";
   }

   /**
    * Store the entries applied by the latest (re)bind for reuse (/compile) and
    * observability (/status). An empty map means the manifest was cleared, so
    * the package reverts to live (unbound).
    */
   private recordManifestBinding(entries: FreshnessManifest): void {
      const count = Object.keys(entries).length;
      // Full wire entries (with freshness) drive the per-query serve-path gate;
      // the tableName-only projection is what /compile and /status consume.
      this.freshnessEntries = count > 0 ? entries : undefined;
      this.buildManifestEntries =
         count > 0 ? toTableNameManifest(entries) : undefined;
      this.manifestEntryCount = count;
      this.manifestBindingStatus = count > 0 ? "bound" : "unbound";
      // A rebind replaces the entries, so any memoized freshness-filtered
      // manifest is stale — drop it so the next query recomputes.
      this.freshManifestCache = undefined;
      if (count === 0) {
         this.boundManifestUri = null;
      }
   }

   /**
    * Bind (or clear) the package's `storage=` serve bindings from a build's full
    * manifest entries, and push them onto every loaded model so a query can be
    * routed through the virtual-source serve transform. Called by the build's
    * post-run distribution with the full {@link ManifestEntry} map (which
    * carries `storageDestinationName` + captured `schema`); only entries that
    * were materialized into a storage destination produce a binding. Re-applied
    * on model reload via {@link pushStorageServeBindingsToModels}.
    */
   /**
    * Re-establish the colocated (same-connection) serve routing from a
    * FreshnessManifest WITHOUT recompiling — the analogue of
    * {@link bindStorageServeBindings} for the in-warehouse tier, used to
    * restore serving on load from the package's own latest persisted
    * materialization. Colocated routing is applied at query time as a per-query
    * `buildManifest` override (see {@link getFreshBuildManifest} and
    * Model.getQueryResults), so setting the in-memory entries is sufficient; the
    * freshness resolver is already wired on every model from
    * {@link Package.create}. Distinct from {@link reloadAllModels}, which also
    * recompiles — the recompile is not what routes the query, so a pure
    * restore-on-load skips it (no double compile after the load-time compile).
    * An empty map clears the binding (reverts to serving live).
    */
   public bindColocatedServeManifest(entries: FreshnessManifest): void {
      this.recordManifestBinding(entries);
   }

   /**
    * Bind the storage serve bindings a host vouches for — minus any whose source
    * this package's own eligibility gate refuses.
    *
    * The host is authoritative about WHICH TABLE backs a source; it owns
    * generations and rollout. It cannot be authoritative about WHETHER a source
    * may be served from a frozen table at all, because that is decided by
    * compiling the model, which the host does not do. A `given`-referencing
    * source is the case that matters: a given binds per query for row-level
    * access control, so one table built once and served to everyone hands every
    * caller the rows filtered for whoever built it.
    *
    * A manifest can name such a source without anyone being careless — a source
    * that was given-free when it was built acquires a `given` on the next model
    * edit, and the old manifest still points at a real table until convergence
    * catches up.
    *
    * Eligibility must be established POSITIVELY: a binding is honored only if its
    * source is in the eligible set. Two states would otherwise pass as consent,
    * and both are reachable without anyone forging anything —
    *  - the plan compute failed, so nothing was examined at all;
    *  - the source never reached the plan, because Malloy admits only
    *    query-shaped sources as build roots and a `#@ persist` on a filtered
    *    pass-through is dropped (see `droppedPersistSources`) — the same
    *    row-level-access shape this gate exists for.
    *
    * Refused bindings are DROPPED, not fatal: that source serves live, which is
    * always correct because the tier is a performance tier. The rest bind.
    */
   public bindStorageServeBindings(
      entries: Record<string, ManifestEntry>,
   ): void {
      const derived = deriveServeBindings(entries);
      const eligibility = this.sourceEligibility;
      const eligible = new Set(eligibility?.eligible ?? []);
      const allowed = derived.filter((binding) => {
         if (eligible.has(binding.sourceName)) return true;
         const reason =
            eligibility === undefined
               ? "the package build plan could not be computed, so no source was examined for eligibility"
               : (eligibility.refused[binding.sourceName] ??
                 "the source is not in the package build plan, so it was never examined (a `#@ persist` on a non-query-shaped source is dropped)");
         logger.warn(
            "Refusing a storage serve binding: the source is not eligible to be served from a materialized table",
            {
               packageName: this.packageName,
               sourceName: binding.sourceName,
               reason,
            },
         );
         return false;
      });
      this.storageServeBindings = allowed;
      this.pushStorageServeBindingsToModels();
   }

   /** Push the current storage serve bindings onto every loaded model. */
   private pushStorageServeBindingsToModels(): void {
      for (const model of this.models.values()) {
         model.setServeBindings(this.storageServeBindings);
      }
   }

   /**
    * Set the connections this package's materialization serve shapes compile
    * against: the environment's storage destinations, and nothing a model
    * in this package can name. Called by the owning Environment after load.
    *
    * Separate from {@link malloyConfig} on purpose. That one is what the author's
    * models resolve through, and a destination must not be reachable from it;
    * these two configs being the same object is the bug this split exists to
    * prevent. Absent ⇒ no storage serve routing, so queries serve live.
    */
   public setServeDestinationConfig(provider: () => MalloyConfig): void {
      this.serveDestinationConfig = provider;
      this.pushServeDestinationConfigToModels();
   }

   private pushServeDestinationConfigToModels(): void {
      if (!this.serveDestinationConfig) return;
      for (const model of this.models.values()) {
         model.setServeDestinationConfig(this.serveDestinationConfig);
      }
   }

   /**
    * Drop every model's memoized materialization serve shape, so the next routed
    * query recompiles it. Called by the owning Environment when the destinations
    * those shapes were compiled against are replaced.
    */
   public invalidateServeShapes(): void {
      for (const model of this.models.values()) {
         model.invalidateServeShapeCache();
      }
   }

   /**
    * Declared `explores` (publisher.json) that don't resolve to a real
    * `.malloy` model in this package, each with an actionable reason. Empty
    * when explores is absent/empty or every entry resolves.
    *
    * The listing already fails safe — a non-resolving entry matches no model in
    * `listModels`, so it hides rather than exposes. This surfaces *why*, so the
    * load path can warn and the publish path can reject (see package.controller).
    */
   public getInvalidExplores(
      exploresOverride?: string[],
   ): { entry: string; reason: string }[] {
      const declared = exploresOverride ?? this.packageMetadata.explores;
      if (!declared || declared.length === 0) return [];
      const malloyModels = new Set(
         Array.from(this.models.keys()).filter((p) =>
            p.endsWith(MODEL_FILE_SUFFIX),
         ),
      );
      const problems: { entry: string; reason: string }[] = [];
      for (const entry of declared) {
         if (entry.endsWith(NOTEBOOK_FILE_SUFFIX)) {
            problems.push({
               entry,
               reason:
                  `notebooks are always public and cannot be explores. ` +
                  `Fix: remove it, and list a ${MODEL_FILE_SUFFIX} model file instead.`,
            });
         } else if (!malloyModels.has(entry)) {
            problems.push({
               entry,
               reason:
                  `file not found in the package. Fix: list a ${MODEL_FILE_SUFFIX} ` +
                  `file relative to the package root (e.g. "index.malloy").`,
            });
         }
      }
      return problems;
   }

   /** One actionable message per invalid entry (empty when all resolve). */
   public exploreWarnings(exploresOverride?: string[]): string[] {
      return this.getInvalidExplores(exploresOverride).map(
         (p) =>
            `Invalid explores entry '${p.entry}' in ${PACKAGE_MANIFEST_NAME}: ${p.reason}`,
      );
   }

   /**
    * The {@link exploreWarnings} joined into one string, or "" if none.
    * Newline-separated so multiple invalid entries stay one-per-line in the
    * 400 message rather than running together.
    */
   public formatInvalidExplores(exploresOverride?: string[]): string {
      return this.exploreWarnings(exploresOverride).join("\n");
   }

   /**
    * Publish-gate for the package's Malloy Persistence policy (persistence.md
    * §3.1, §9.2–§9.5). Scope is a single package-level mode (`Package.scope`)
    * and a materialization cron is package-root-only and version-scope-only.
    * The rules, read off the resolved build plan + manifest:
    *
    *  1. Per-source `#@ persist ... sharing=`/`schedule=` are retired — declaring
    *     either on a source is an error (scope is package-level; a schedule is
    *     package-root-only). Detected via the raw `annotationFields`.
    *  2. `materialization.schedule` is legal only when `scope: version` (a
    *     package-scoped lineage is reused across versions, so a single
    *     per-version cadence is meaningless).
    *  3. `materialization.schedule` and freshness (package `materialization.freshness`
    *     or any source `freshness`) are mutually exclusive — declare either the
    *     power tier or the objective tier, never both.
    *
    * The cross-package dependency rule (a `scope: version` scheduled package may
    * not depend on a package-scoped upstream) is not enforced here: scope is
    * uniform within a package, so an intra-package dependency can never violate
    * it, and the publisher's build plan is intra-package only — a cross-package
    * upstream's scope is not resolvable from the single `Package` served. It is
    * deferred to the scheduler slice (R1–R3), which resolves cross-package scope.
    *
    * Strict at publish (package.controller), warn-only at load/reload
    * (loadViaWorker) — same split as explores.
    */
   /**
    * Operator-facing warnings for `#@ persist storage=<conn>` sources whose
    * storage annotation is NOT being honored on the serve path, so a degraded
    * source is visible on `/status` rather than silently serving live. Emitted
    * per {@link getPersistStorageMode}:
    *  - `off`: the annotation is ignored entirely (serving live from the source
    *    warehouse) — the kill-switch resting state.
    *  - `write-only`: the source materializes into storage but the serve path is
    *    not routed to the materialized table (served live).
    *  - `on`: no warning — the source is (or falls back to being) served per the
    *    transform; per-query fallback is a query-time event, not a load warning.
    * Read straight off the compiled build plan's `annotationFields.storage`
    * (undefined when the package declares no persist sources).
    */
   /**
    * Operator-facing warnings for `#@ persist` sources that Malloy's
    * getBuildPlan() did not recognize as a materializable build root (observed
    * for a filtered pass-through `X is <table> extend { where … }`). Without a
    * signal the annotation is a silent no-op — no build, no error, served live —
    * so surface it on `/status`. The build path additionally hard-refuses (see
    * MaterializationService). Independent of {@link buildPlan} being null, since
    * a package whose ONLY persist source is dropped has no plan at all.
    */
   private droppedPersistWarnings(): ApiPackageWarning[] {
      return this.droppedPersistSources.map((d) => ({
         model: d.modelPath,
         subject: d.name,
         message:
            `is annotated '#@ persist' but was not recognized as a ` +
            `materializable source, so nothing is materialized and it is served ` +
            `live. Only query/aggregate sources build; a filtered pass-through ` +
            `does not. Persist a query source, or invoke a parameterized source ` +
            `with a bound argument.`,
      }));
   }

   private storageWarnings(): ApiPackageWarning[] {
      const mode = getPersistStorageMode();
      if (mode === "on" || !this.buildPlan?.sources) return [];
      const warnings: ApiPackageWarning[] = [];
      for (const source of Object.values(this.buildPlan.sources)) {
         const storage = source.annotationFields?.storage?.trim();
         if (!storage) continue;
         const message =
            mode === "off"
               ? `declares storage="${storage}" but PERSIST_STORAGE_MODE is off; ` +
                 `the annotation is ignored and the source is served live from ` +
                 `its own warehouse.`
               : `is materialized into storage "${storage}" but ` +
                 `PERSIST_STORAGE_MODE is write-only; the serve path is not ` +
                 `routed to the materialized table (served live).`;
         warnings.push({
            model: source.modelPath ?? "",
            subject: source.name,
            message,
         });
      }
      return warnings;
   }

   public persistencePolicyWarnings(): string[] {
      // REJECTION rules only: a non-empty result fails a publish and disarms the
      // scheduler. Advisory findings (queryMetadata problems, a deprecated
      // manifest shape) go to the operator warnings array instead — see
      // materializationConfigWarnings.
      const warnings: string[] = [];
      const sources = this.buildPlan?.sources
         ? Object.values(this.buildPlan.sources)
         : [];
      const materialization = this.packageMetadata.materialization;
      const packageSchedule = materialization?.schedule ?? null;
      const packageFreshness = materialization?.freshness ?? null;
      const scope = this.packageMetadata.scope ?? "package";

      // Rule 1: per-source sharing/schedule are no longer part of the model.
      for (const source of sources) {
         const fields = source.annotationFields ?? {};
         if (fields.sharing !== undefined) {
            warnings.push(
               `#@ persist source "${source.name}" declares sharing=... which is ` +
                  `no longer supported: scope is a single package-level mode. Set ` +
                  `"materialization": { "scope": "version" | "package" } in ` +
                  `${PACKAGE_MANIFEST_NAME} instead.`,
            );
         }
         if (fields.schedule !== undefined) {
            warnings.push(
               `#@ persist source "${source.name}" declares schedule=... which is ` +
                  `no longer supported: a schedule is package-root-only. Declare ` +
                  `"materialization.schedule" at the ${PACKAGE_MANIFEST_NAME} root ` +
                  `(requires "scope": "version") instead.`,
            );
         }
      }

      // Rule 2: a package-level cron is legal only with scope: version.
      if (packageSchedule && scope !== "version") {
         warnings.push(
            `materialization.schedule (cron) in ${PACKAGE_MANIFEST_NAME} requires ` +
               `"scope": "version": a package-scoped lineage is reused across ` +
               `versions, so a single per-version cadence is meaningless. Set ` +
               `"scope": "version", or remove the schedule.`,
         );
      }

      // Rule 3: schedule and freshness are mutually exclusive.
      if (packageSchedule) {
         const freshnessDeclared =
            !!(
               packageFreshness &&
               (packageFreshness.window || packageFreshness.fallback)
            ) || sources.some((s) => s.freshness != null);
         if (freshnessDeclared) {
            warnings.push(
               `materialization.schedule and freshness are mutually exclusive in ` +
                  `${PACKAGE_MANIFEST_NAME}: declare either a schedule (power tier) ` +
                  `or freshness (objective tier), never both.`,
            );
         }
      }

      // Rule 4: the cron must be a valid 5-field UNIX expression (no L/W/#/?
      // extensions — see CronEvaluator). Enforced here so publish (strict),
      // PATCH (strict), package load (warn), and the standalone scheduler all
      // apply the identical rule — a garbage cron can no longer pass publish
      // and then silently never arm.
      if (packageSchedule && !new CronEvaluator().isValid(packageSchedule)) {
         warnings.push(
            `materialization.schedule in ${PACKAGE_MANIFEST_NAME} is not a valid ` +
               `5-field UNIX cron: ${JSON.stringify(packageSchedule)}. Use ` +
               `"minute hour day-of-month month day-of-week" (no L/W/#/? ` +
               `extensions).`,
         );
      }

      return warnings;
   }

   /**
    * The {@link persistencePolicyWarnings} joined into one string, or "" if the
    * package's persistence policy is valid.
    */
   public formatInvalidPersistencePolicy(): string {
      return this.persistencePolicyWarnings().join("\n");
   }

   /**
    * REJECTION messages for the sources' incremental-refresh declarations
    * (`refresh="incremental"` with `watermark=` / `merge_key=`): an incoherent
    * declaration chain, a malformed key value, a name that is not a materialized
    * output column, an unsupported dialect, or a window function. See
    * incremental_policy for the rules.
    *
    * Kept SEPARATE from {@link persistencePolicyWarnings} — which is about the
    * manifest's scope/schedule/freshness policy — so the two gates stay
    * independently readable; both are joined into the same publish 400. Unlike
    * that one, a non-empty result here also fails the LOAD (see loadViaWorker),
    * so a package is never served with a declaration this rejected.
    */
   public incrementalPolicyWarnings(): string[] {
      return incrementalPolicyRejections(this.incrementalPolicySources);
   }

   /**
    * The {@link incrementalPolicyWarnings} joined into one string, or "" when
    * every source's incremental declaration is valid.
    */
   public formatInvalidIncrementalPolicy(): string {
      return this.incrementalPolicyWarnings().join("\n");
   }

   /**
    * Within-package persist-target COLLISION warnings: two DISTINCT persist
    * sources (different `sourceEntityId`) that resolve to the same physical
    * table — the same resolved `name=` (or source-name fallback) in the same
    * destination connection. The publisher self-assigns a materialized table's
    * physical name from `name=` verbatim, so two such sources would clobber each
    * other: the second build's `CREATE OR REPLACE` overwrites the first, two
    * manifest entries point at one table, and a GC drop of one takes out the
    * other. Two IMPORTS of the SAME source (identical `sourceEntityId`) are
    * intentional dedup and NOT flagged.
    *
    * The destination is resolved from the DECLARED annotation, independent of
    * {@link getPersistStorageMode}, so the kill switch never changes whether a
    * package has a latent collision.
    *
    * Deliberately SEPARATE from {@link persistencePolicyWarnings} because the
    * rollout is staged: these are surfaced warn-only at load AND publish, and
    * only block a publish once `PERSIST_COLLISION_ENFORCE` is set (see
    * {@link getPersistCollisionEnforce}) — a package published before this check
    * existed may carry a latent collision, so an un-gated reject would break a
    * routine re-publish. This is a WITHIN-package/version check only: a `name=`
    * change ACROSS versions, or a cross-package collision, needs the
    * cross-version/global view the host has and the publisher
    * does not.
    */
   public persistenceCollisionWarnings(): string[] {
      const sources = this.buildPlan?.sources
         ? Object.values(this.buildPlan.sources)
         : [];
      const targets = new Map<
         string,
         { name: string; destination: string; sources: Map<string, string> }
      >();
      for (const source of sources) {
         const fields = source.annotationFields ?? {};
         const physicalName = (fields.name || source.name).trim();
         const storage = fields.storage?.trim();
         // storage= into a real destination lands there; absent, the source is
         // built colocated (into its own warehouse).
         const destination = storage ? storage : source.connectionName;
         // Verbatim key. Limitation: names that differ only by case or quoting
         // (`Foo` vs `foo`, `"foo"` vs `foo`) key distinctly here but may fold to
         // the same physical table on a case-folding destination — such variants
         // evade this within-package check. Normalizing correctly is
         // destination-dialect-dependent, so it's left verbatim; the host's
         // ownership-scoped production naming and the destination's own identifier
         // rules are the backstop.
         const key = `${destination} ${physicalName}`;
         const bucket = targets.get(key) ?? {
            name: physicalName,
            destination,
            sources: new Map<string, string>(),
         };
         // Keyed by sourceEntityId so identical-content imports collapse to one.
         bucket.sources.set(source.sourceEntityId, source.name);
         targets.set(key, bucket);
      }
      const warnings: string[] = [];
      for (const {
         name,
         destination,
         sources: colliding,
      } of targets.values()) {
         if (colliding.size < 2) continue;
         const names = [...colliding.values()].sort();
         warnings.push(
            `#@ persist sources ${names
               .map((n) => `"${n}"`)
               .join(", ")} all resolve to the same materialized table ` +
               `"${name}" in destination "${destination}". Distinct sources ` +
               `must not share a physical target — the second build would ` +
               `overwrite the first, and a serve binding or GC drop for one ` +
               `would affect the other. Give each a distinct #@ persist name=.`,
         );
      }
      return warnings;
   }

   /**
    * Collision warnings joined for the publish gate: the string is non-empty
    * (and thus a publish rejection) only when `PERSIST_COLLISION_ENFORCE` is set;
    * otherwise collisions are surfaced warn-only (at load and publish) without
    * blocking. See {@link persistenceCollisionWarnings}.
    */
   public formatPersistenceCollisionRejections(): string {
      if (!getPersistCollisionEnforce()) return "";
      return this.persistenceCollisionWarnings().join("\n");
   }

   /**
    * One message per LISTED model whose discovery surface is empty because it
    * is import-only (imports other files, declares/re-exports nothing). Such a
    * model renders a blank page, which reads as broken; the fix is an explicit
    * re-export. Log-only (see loadViaWorker/reloadAllModels) — deliberately
    * NOT part of exploreWarnings, which is strict-at-publish: import-only
    * files are a legitimate pattern and must not block a publish. Hidden
    * (non-listed) models are skipped — nobody browses them, so an empty
    * surface there is just normal plumbing.
    */
   public emptyDiscoveryWarnings(): string[] {
      const exploreSet = this.exploreSet();
      const warnings: string[] = [];
      for (const [modelPath, model] of this.models) {
         if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
         if (exploreSet && !exploreSet.has(modelPath)) continue;
         if (model.hasEmptyDiscoverySurface()) {
            warnings.push(
               `Model "${modelPath}" is listed but exposes nothing: it only ` +
                  `imports other files and re-exports none of their sources. ` +
                  `Add e.g. 'export { source_name }' to surface sources on ` +
                  `this model.`,
            );
         }
      }
      return warnings;
   }

   /** Log {@link emptyDiscoveryWarnings}; shared by load and reload. */
   private logEmptyDiscoveryWarnings(): void {
      for (const warning of this.emptyDiscoveryWarnings()) {
         logger.warn(`Package ${this.packageName} has a blank-looking model`, {
            packageName: this.packageName,
            detail: warning,
         });
      }
   }

   public listDatabases(): ApiDatabase[] {
      return this.databases;
   }

   public getModel(modelPath: string): Model | undefined {
      return this.models.get(modelPath);
   }

   public async getMalloyConnection(
      connectionName: string,
   ): Promise<Connection> {
      return this.malloyConfig.connections.lookupConnection(connectionName);
   }

   /**
    * Quote each manifest entry's physical table path for its connection's
    * dialect, mirroring the build side: the builder CREATEs the table with
    * {@link quoteTablePath} (per-segment, case-preserved), so on a case-folding
    * engine (Snowflake uppercases unquoted identifiers) the stored name is only
    * reachable through the same quoting. Malloy pastes a manifest `tableName`
    * into `FROM` verbatim by contract (a bare name means "let the engine
    * fold"), so the case-preserving producer — us — must hand it the quoted
    * form. Same module quotes CREATE and read: the two sides cannot drift.
    *
    * A name already carrying a quote character is passed through verbatim (it
    * is already canonical SQL; control-plane-assigned names are sanitized to
    * `[A-Za-z0-9_\-.]` and can never contain one).
    *
    * Two cases bind verbatim, and they are NOT the same signal:
    * - No `connectionName`: a bare/engine-folding producer that never recorded
    *   a connection (simple_builder / malloy-cli). Expected and benign — the
    *   pre-change behavior, bound silently, nothing regresses.
    * - `connectionName` present but unresolvable: a genuine misconfiguration
    *   (the connection was renamed/removed, or the manifest is out of sync with
    *   this package's config). This one entry is degraded — but the source
    *   would not serve regardless of quoting, since Malloy needs the connection
    *   to run any query against it, so we degrade just this entry rather than
    *   fail the whole package bind, and log at ERROR with a fix.
    */
   private async quoteBoundTableNames(
      entries: FreshnessManifest,
   ): Promise<FreshnessManifest> {
      const out: FreshnessManifest = {};
      for (const [sourceEntityId, entry] of Object.entries(entries)) {
         let tableName = entry.tableName;
         if (entry.connectionName && !isQuotedIdentifierPath(tableName)) {
            try {
               const connection = await this.getMalloyConnection(
                  entry.connectionName,
               );
               tableName = quoteManifestTablePath(
                  tableName,
                  connection.dialectName,
               );
            } catch (err) {
               recordManifestBindDegraded();
               logger.error(
                  `Manifest entry '${sourceEntityId}' names connection ` +
                     `'${entry.connectionName}', which this package cannot ` +
                     `resolve: binding its table path unquoted, so this source ` +
                     `will not serve on a case-folding engine (and queries ` +
                     `against it fail regardless, since Malloy needs the ` +
                     `connection to run them). Fix: ensure a connection named ` +
                     `'${entry.connectionName}' exists in this package's ` +
                     `config, or rebuild the manifest against the current config.`,
                  {
                     packageName: this.packageName,
                     sourceEntityId,
                     connectionName: entry.connectionName,
                     error: err instanceof Error ? err.message : String(err),
                  },
               );
            }
         }
         out[sourceEntityId] = { ...entry, tableName };
      }
      return out;
   }

   public getMalloyConfig(): MalloyConfig {
      return this.malloyConfig;
   }

   public getPackagePath(): string {
      return this.packagePath;
   }

   public getModelPaths(): string[] {
      return Array.from(this.models.keys());
   }

   /**
    * Re-compile every model in the package against a new build
    * manifest (called after a materialization build commits new
    * physicalised tables). Runs through the package-load worker pool
    * — same off-main-thread compile path as initial `Package.create`
    * — so a reload of a large package can't block the K8s liveness
    * probe.
    *
    * Unlike `Package.create`, a per-model compile failure here does
    * NOT abort the reload: we keep the failed model as a placeholder
    * (`Model.fromCompilationError`) in `this.models`, matching the
    * historical reload semantics. Whole-pool failures (worker crash,
    * timeout, pool shutting down) propagate as `ServiceUnavailableError`
    * — the caller (manifest service) decides how to retry.
    */
   public async reloadAllModels(entries: FreshnessManifest): Promise<void> {
      // Quote each bound physical name for its connection's dialect BEFORE any
      // projection: everything downstream (model hydration, the per-query
      // freshness gate, /compile, /status) reads the entries recorded here, so
      // this is the one place the write side's quoting is mirrored onto reads.
      entries = await this.quoteBoundTableNames(entries);
      // Models are hydrated against the tableName-only projection; the freshness
      // fields gate the serve path per query (via getFreshBuildManifest), not
      // model hydration.
      const buildManifest = toTableNameManifest(entries);
      const modelPaths = Array.from(this.models.keys());
      logger.info("Reloading all models with build manifest", {
         packageName: this.packageName,
         modelCount: modelPaths.length,
         manifestEntryCount: Object.keys(buildManifest).length,
      });

      const pool = getPackageLoadPool();
      let outcome;
      try {
         outcome = await pool.loadPackage({
            packagePath: this.packagePath,
            packageName: this.packageName,
            malloyConfig: this.malloyConfig,
            defaultConnectionName: "duckdb",
            buildManifest,
         });
      } catch (err) {
         const realError =
            err instanceof Error
               ? err
               : new Error(`Package-load worker pool failure: ${String(err)}`);
         if (
            realError instanceof MalloyError ||
            realError instanceof ModelCompilationError
         ) {
            throw realError;
         }
         throw new ServiceUnavailableError(
            `Package-load worker pool unavailable: ${realError.message}`,
         );
      }

      const nextModels = new Map<string, Model>();
      const renderTagWarnings: ApiPackageWarning[] = [];
      for (const sm of outcome.models) {
         if (sm.compilationError) {
            const err = Model.deserializeCompilationError(sm.compilationError);
            logger.warn("Model compilation failed during reload", {
               packageName: this.packageName,
               modelPath: sm.modelPath,
               error: err.message,
            });
            nextModels.set(
               sm.modelPath,
               Model.fromCompilationError(
                  this.packageName,
                  sm.modelPath,
                  sm.modelType,
                  err,
               ),
            );
         } else {
            const model = Model.fromSerialized(
               this.packageName,
               this.packagePath,
               this.malloyConfig,
               sm,
               { buildManifest },
            );
            // Validate renderer tags here too (loadViaWorker does it for the
            // create path). Render-tag findings are logged as warnings inside
            // validateRenderTags and never throw. The catch is defensive: an
            // unexpected internal failure is recorded as this model's
            // compilationError rather than aborting the whole reload.
            try {
               for (const w of await model.validateRenderTags()) {
                  renderTagWarnings.push({
                     model: sm.modelPath,
                     // Spelled out rather than spread, for the reason given at
                     // the sibling call site on the load path.
                     subject: w.subject,
                     message: w.message,
                     severity: w.severity,
                  });
               }
               nextModels.set(sm.modelPath, model);
            } catch (renderErr) {
               const err =
                  renderErr instanceof Error
                     ? renderErr
                     : new Error(String(renderErr));
               logger.warn("Render-tag validation failed during reload", {
                  packageName: this.packageName,
                  modelPath: sm.modelPath,
                  error: err.message,
               });
               nextModels.set(
                  sm.modelPath,
                  Model.fromCompilationError(
                     this.packageName,
                     sm.modelPath,
                     sm.modelType,
                     err,
                  ),
               );
            }
         }
      }
      this.models = nextModels;
      // The freshly-compiled models start with no serve bindings and no serve
      // connections; re-apply both so a reload preserves serve routing.
      this.pushStorageServeBindingsToModels();
      this.pushServeDestinationConfigToModels();
      this.renderTagWarnings = renderTagWarnings;
      this.manifestWarnings = outcome.packageMetadata.manifestWarnings ?? [];
      // A reload re-reads publisher.json in the worker; pick up any change to
      // the explore set and query-boundary mode so listModels()/the gate
      // reflect edited explores without a full Package.create. The convention
      // flag is re-read with them: adding or deleting an index.malloy, or
      // adding an explicit `explores`, changes which source the surface came
      // from, and a stale flag here would leave the boundary applied (or not)
      // against the wrong one.
      this.exploresFromConvention =
         outcome.packageMetadata.exploresFromConvention ?? false;
      this.packageMetadata.explores = outcome.packageMetadata.explores;
      this.packageMetadata.queryableSources =
         outcome.packageMetadata.queryableSources;
      this.packageMetadata.manifestLocation =
         outcome.packageMetadata.manifestLocation ?? null;
      this.applyDiscoveryPolicyToModels();
      this.applyQueryBoundaryToModels();
      this.logExploresConvention();
      // Remember what we just bound so /compile can route identically and
      // /status can report the binding. An empty map reverts to live (unbound).
      // Retains the full freshness entries so the serve-path gate can evaluate
      // them per query.
      this.recordManifestBinding(entries);
      // Install the per-query freshness resolver on the rebuilt model set so the
      // serve path applies the freshness-filtered manifest as a per-query
      // override.
      this.wireFreshnessResolvers();
      // Re-run the fail-safe warning against the refreshed model set: an edit
      // to publisher.json that introduces a bad entry should surface in the
      // logs on reload too, not only at initial load (loadViaWorker).
      const invalidMsg = this.formatInvalidExplores();
      if (invalidMsg) {
         logger.warn(`Package ${this.packageName} has invalid explores`, {
            packageName: this.packageName,
            detail: invalidMsg,
         });
      }
      this.logEmptyDiscoveryWarnings();
   }

   public async getModelFileText(modelPath: string): Promise<string> {
      const model = this.getModel(modelPath);
      if (!model) {
         throw new Error(`Model not found: ${modelPath}`);
      }
      return await model.getFileText(this.packagePath);
   }

   public async listModels(): Promise<ApiModel[]> {
      // When `explores` is declared in publisher.json, only those models
      // form the public surface; every other .malloy file still compiles for
      // import/join resolution but is hidden from the listing. Absent/empty →
      // every model is listed (backward-compatible default). Notebooks are
      // unaffected (see listNotebooks) — they are always public.
      const exploreSet = this.exploreSet();
      const values = await Promise.all(
         Array.from(this.models.keys())
            .filter((modelPath) => {
               if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) return false;
               return exploreSet ? exploreSet.has(modelPath) : true;
            })
            .map(async (modelPath) => {
               let error: string | undefined;
               try {
                  await this.models.get(modelPath)?.getModel();
               } catch (modelError) {
                  error =
                     modelError instanceof Error
                        ? modelError.message
                        : undefined;
               }
               return {
                  environmentName: this.environmentName,
                  path: modelPath,
                  packageName: this.packageName,
                  error,
               };
            }),
      );
      return values;
   }

   public async listNotebooks(): Promise<ApiNotebook[]> {
      return await Promise.all(
         Array.from(this.models.keys())
            .filter((modelPath) => {
               return modelPath.endsWith(NOTEBOOK_FILE_SUFFIX);
            })
            .map(async (modelPath) => {
               const error = this.models.get(modelPath)?.getNotebookError();
               return {
                  environmentName: this.environmentName,
                  packageName: this.packageName,
                  path: modelPath,
                  error: error?.message,
               };
            }),
      );
   }

   private static buildPackageMalloyConfig(
      packagePath: string,
      getEnvironmentMalloyConfig: () => MalloyConfig,
   ): MalloyConfig {
      const malloyConfig = new MalloyConfig(
         {
            connections: {
               duckdb: {
                  is: "duckdb",
                  databasePath: ":memory:",
               },
            },
         },
         {
            config: contextOverlay({ rootDirectory: packagePath }),
         },
      );

      malloyConfig.wrapConnections((base) => ({
         lookupConnection: async (name?: string) => {
            if (!name || name === "duckdb") {
               const connection = await base.lookupConnection(name);
               // The per-package :memory: sandbox is a Publisher-owned DuckDB
               // session too. Pin it against implicit auto-install so
               // EXTENSION_FETCH_POLICY covers it — otherwise local-only's
               // no-network guarantee had a hole here (the sandbox has no
               // attached databases, so the env attach paths never touch it).
               if (connection instanceof DuckDBConnection) {
                  await applyExtensionSessionSettings(connection);
               }
               return connection;
            }
            // Resolve against the *current* environment MalloyConfig so a
            // connection-generation swap on Environment propagates without a
            // package reload.
            return getEnvironmentMalloyConfig().connections.lookupConnection(
               name,
            );
         },
      }));

      return malloyConfig;
   }

   private static toMalloyConfig(
      input: MalloyConfig | Map<string, Connection>,
   ): MalloyConfig {
      if (input instanceof MalloyConfig) {
         return input;
      }

      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(input, "duckdb"),
      );
      return malloyConfig;
   }

   private static async validatePackageManifestExistsOrThrowError(
      packagePath: string,
   ) {
      const packageConfigPath = safeJoinUnderRoot(
         packagePath,
         PACKAGE_MANIFEST_NAME,
      );
      try {
         await fs.stat(packageConfigPath);
      } catch {
         logger.error(`Can't find ${packageConfigPath}`);
         throw new PackageNotFoundError(
            `Package manifest for ${packagePath} does not exist.`,
         );
      }
   }

   private static async readDatabases(
      packagePath: string,
      malloyConfig: MalloyConfig,
   ): Promise<ApiDatabase[]> {
      const databasePaths = await Package.getDatabasePaths(packagePath);
      if (databasePaths.length === 0) {
         return [];
      }
      // Resolve the package's duckdb connection ONCE and reuse it for
      // every schema/row-count probe in this package. Malloy caches the
      // materialized connection on the MalloyConfig so the same instance
      // will be returned to model compiles later in `Package.create`.
      // This is the substantive optimization over the previous code:
      // we go from `databasePaths.length` separate DuckDBConnections
      // (each doing its own native init + extension load) to one.
      const conn = await malloyConfig.connections.lookupConnection("duckdb");
      return await Promise.all(
         databasePaths.map(async (databasePath): Promise<ApiDatabase> => {
            try {
               return {
                  path: databasePath,
                  info: await Package.getDatabaseInfo(
                     packagePath,
                     databasePath,
                     conn,
                  ),
                  type: "embedded" as const,
               };
            } catch (error) {
               // One unreadable data file (a partial or corrupt spreadsheet,
               // an interrupted download, an extension that failed to bake)
               // must not drop the whole package. Report it the way a model
               // that fails to compile is reported: the entry stays in the
               // listing carrying `error` instead of `info`, so the failure
               // is visible over the API rather than looking like the file
               // was never there.
               const message =
                  error instanceof Error ? error.message : String(error);
               logger.warn("Could not read package database", {
                  packagePath,
                  databasePath,
                  error: message,
               });
               return {
                  path: databasePath,
                  type: "embedded" as const,
                  error: message,
               };
            }
         }),
      );
   }

   private static async getDatabasePaths(
      packagePath: string,
   ): Promise<string[]> {
      const files = await recursive(packagePath, [ignoreDotfiles]);
      return files
         .map((fullPath: string) => {
            return path.relative(packagePath, fullPath).replace(/\\/g, "/");
         })
         .filter((modelPath: string) => {
            // Excel writes a sibling owner file (~$name.xlsx) whenever the
            // workbook is open. It is not real data and is not a valid zip, so
            // skip it before the extension match rather than fail to probe it.
            if (path.basename(modelPath).startsWith("~$")) {
               return false;
            }
            return (
               modelPath.endsWith(".parquet") ||
               modelPath.endsWith(".csv") ||
               modelPath.endsWith(".xlsx")
            );
         });
   }

   private static async getDatabaseInfo(
      packagePath: string,
      databasePath: string,
      conn: Connection,
   ): Promise<ApiTableDescription> {
      const fullPath = path.join(packagePath, databasePath);

      // Create a DuckDB source then:
      // 1. Load the model and get the table schema from model
      // 2. Run a query to get the row count from the table
      // ConnectionRuntime is cheap (just a wrapper), and creating one
      // per call keeps each probe's compile state isolated. The
      // expensive piece — the underlying DuckDBConnection — is shared
      // across all probes via `conn` (resolved once in readDatabases).
      const runtime = new ConnectionRuntime({
         urlReader: new EmptyURLReader(),
         connections: [conn],
      });
      // Normalize path to use forward slashes for cross-platform compatibility
      // DuckDB on Windows supports forward slashes, and this avoids escaping issues
      const normalizedPath = fullPath.replace(/\\/g, "/");
      const model = runtime.loadModel(
         `source: temp is duckdb.table('${normalizedPath}')`,
      );
      const modelDef = await model.getModel();
      const fields = (modelDef._modelDef.contents["temp"] as SourceDef).fields;
      const schema = fields.map((field): ApiColumn => {
         return { type: field.type, name: field.name };
      });
      const runner = model.loadQuery(
         "run: temp->{aggregate: row_count is count()}",
      );
      const result = await runner.run();
      const rowCount = result.data.value[0].row_count?.valueOf() as number;
      return { name: databasePath, rowCount, columns: schema };
   }

   public setName(name: string) {
      this.packageName = name;
   }

   public setEnvironmentName(environmentName: string) {
      this.environmentName = environmentName;
   }

   /**
    * Replace the wire metadata and re-derive both policies from it.
    *
    * `exploresFromConvention` is REQUIRED and separate, because it is not part
    * of the wire object. Required rather than defaulted: the safe value depends
    * entirely on what the caller is doing (a PATCH carrying its own `explores`
    * makes the surface explicit and must pass false; one that does not must
    * pass the previous value), and a caller who forgot it on the first of those
    * would leave the query boundary off on a surface the operator just
    * declared. A missing argument is a compile error instead.
    */
   public setPackageMetadata(
      packageMetadata: ApiPackage,
      exploresFromConvention: boolean,
   ) {
      const originChanged =
         this.exploresFromConvention !== exploresFromConvention;
      this.packageMetadata = packageMetadata;
      this.exploresFromConvention = exploresFromConvention;
      if (originChanged) {
         // The convention warnings describe where the surface came from, so
         // they are false the moment that changes. The manifest is only re-read
         // on reload, so drop them here rather than serve a warning that
         // contradicts the behavior now in force (notably "queryableSources has
         // no effect", right after the PATCH that gave it effect).
         this.manifestWarnings = this.manifestWarnings.filter(
            (w) => !isExploresConventionWarning(w),
         );
         // Same reasoning for the post-load ones: every warning in that array
         // describes a surface that came from the convention, so a change of
         // origin makes all of them false.
         this.postLoadWarnings = this.postLoadWarnings.filter(
            (w) => !isExploresConventionWarning(w),
         );
      }
      this.applyDiscoveryPolicyToModels();
      this.applyQueryBoundaryToModels();
   }

   /** Whether the current `explores` came from the `index.malloy` convention. */
   public exploresAreFromConvention(): boolean {
      return this.exploresFromConvention;
   }

   /**
    * Append a warning raised after load (today, only the PATCH path). Goes to
    * {@link postLoadWarnings} so a reload inside the same request cannot
    * discard it. De-duplicated, because a client that round-trips repeatedly
    * would otherwise stack the same sentence up on every call.
    */
   public addPostLoadWarning(warning: string): void {
      if (!this.postLoadWarnings.includes(warning)) {
         this.postLoadWarnings.push(warning);
      }
   }

   /** A copy of the current manifest warnings, for a caller that may roll back. */
   public getManifestWarnings(): string[] {
      return [...this.manifestWarnings];
   }

   /**
    * Put back warnings a rolled-back edit dropped. `setPackageMetadata`'s filter
    * is destructive and runs again on the way back, so restoring the metadata
    * alone would leave the package quietly missing a warning whose condition
    * never changed.
    */
   public restoreManifestWarnings(warnings: string[]): void {
      this.manifestWarnings = [...warnings];
   }

   /**
    * Say at load which file the convention picked, so a discovery surface that
    * nothing in publisher.json mentions is never invisible to an operator
    * reading the logs to work out why a model stopped being listed.
    */
   private logExploresConvention(): void {
      if (!this.exploresFromConvention) {
         return;
      }
      logger.info("Package discovery surface defaulted from convention", {
         environmentName: this.environmentName,
         packageName: this.packageName,
         explores: this.packageMetadata.explores,
         // Named so the log answers the operator's actual next question:
         // "did this also close the query boundary?"
         queryBoundaryEnforced: false,
      });
   }
}
