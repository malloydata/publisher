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
import {
   materializationConfigWarnings,
   type QueryMetadataDeclaration,
} from "./materialization_config_validation";
import type { QueryMetadata } from "./query_metadata";
import { CronEvaluator } from "./cron_evaluator";
import {
   buildDashboardManifest,
   COMPONENT_FILE_SUFFIXES,
   DASHBOARDS_DIR,
   dashboardSlug,
   isDashboardModelPath,
   matchesDocumentedDashboardName,
   lintDashboard,
   lintDrillTargets,
   lintGivenTags,
   lintSelfDrills,
   lintUndiscoveredDashboard,
   type DashboardManifest,
   type DashboardModelFacts,
   factsCarryArtifactTag,
} from "./dashboard";
import { filterFreshManifest } from "./freshness";
import { isQuotedIdentifierPath, quoteManifestTablePath } from "./quoting";
import { Model } from "./model";
import { assertPersistNamesQuoted } from "./persist_annotation_validation";

type ApiDatabase = components["schemas"]["Database"];
type ApiModel = components["schemas"]["Model"];
type ApiNotebook = components["schemas"]["Notebook"];
type ApiDashboard = components["schemas"]["Dashboard"];
type ApiDashboardManifest = components["schemas"]["DashboardManifest"];
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
   // Memoized {@link getPreaggregateEntityIds}, keyed on the plan it was derived
   // from so a reload recomputes without an explicit invalidation.
   private preaggregateEntityIdCache:
      | { plan: BuildPlan | null; ids: ReadonlySet<string> }
      | undefined;
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
   // Load-time dashboard lint findings, on the same read-only warnings surface
   // as the render-tag ones. Refreshed with the dashboards on load and reload.
   private dashboardWarnings: ApiPackageWarning[] = [];
   // Dashboards discovered in `dashboards/`, keyed by slug, in path order.
   // Computed once per load/reload rather than per request: the artifact tag is
   // a property of the compiled model, so it can only change when the models do.
   // A dashboard file that failed to compile has no readable tag, so it lands
   // here as a slug-titled entry carrying its error rather than disappearing.
   private dashboards: Map<string, DashboardManifest & { error?: string }> =
      new Map();
   /**
    * Manifest-shape deprecations the load tolerated (a root-level `scope`), kept
    * so publish can report a still-parsing-but-outdated manifest. Not on the wire
    * package: it is a property of the manifest text, not of the loaded package.
    */
   private manifestWarnings: string[] = [];
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
   ) {
      this.environmentName = environmentName;
      this.packageName = packageName;
      this.packagePath = packagePath;
      this.packageMetadata = packageMetadata;
      this.databases = databases;
      this.models = models;
      this.malloyConfig = malloyConfig;
      this.applyDiscoveryPolicyToModels();
      this.applyQueryBoundaryToModels();
   }

   /**
    * Push the discovery-curation policy down onto each Model. Curation (file
    * listing via `explores` and within-file `export {}` filtering) is enabled
    * only when `explores` is declared in publisher.json — absent/empty
    * `explores` preserves legacy listings. Re-derived on reload and metadata
    * PATCH (the inputs can change there).
    */
   /** True when the package opts into curated discovery via a non-empty
    *  `explores`. Single source of truth so the curation/boundary/listing
    *  derivations can't drift out of sync. */
   private exploresDeclared(): boolean {
      const explores = this.packageMetadata.explores;
      return !!(explores && explores.length > 0);
   }

   /** The declared explore set, or null when discovery is uncurated. */
   private exploreSet(): Set<string> | null {
      const explores = this.packageMetadata.explores;
      return explores && explores.length > 0 ? new Set(explores) : null;
   }

   private applyDiscoveryPolicyToModels(): void {
      const curationEnabled = this.exploresDeclared();
      for (const model of this.models.values()) {
         model.setDiscoveryCuration(curationEnabled);
      }
   }

   /**
    * Push the package-level query-boundary policy down onto each Model so the
    * query chokepoint can enforce it without a back-reference to the Package:
    * `Model.getQueryResults` (the HTTP query route and the MCP tool). The
    * `/compile` path is deliberately NOT a chokepoint — it is exempt from the
    * boundary (see Environment.compileSource); only `#(authorize)` gates it.
    * Derived once here (and on reload) rather than per query: the policy only
    * changes when the manifest is (re)read.
    *
    * Policy: queryable == discoverable. The boundary is inert unless `explores`
    * is declared (no curated surface ⇒ nothing to restrict) AND
    * `queryableSources` is "declared" (the default; "all" decouples the axes).
    * When active, a model file is a query entry point only if it is listed in
    * `explores`; within-file curation (`export {}`) is read off each Model.
    */
   private applyQueryBoundaryToModels(): void {
      const exploresDeclared = this.exploresDeclared();
      const exploreSet = this.exploreSet();
      const mode =
         this.packageMetadata.queryableSources === "all" ? "all" : "declared";
      // The PACKAGE-wide queryable surface: the union of every explores-listed
      // model's export closure. The source-level gate used to consult only the
      // requested model's own closure, which denied a source that IS declared
      // queryable (its own file is listed in explores) whenever it was
      // addressed through a model that imports it. A client that posts every
      // query to one model path — the observed agent behavior (HANDOFF CR-5) —
      // then loses every source but that file's own.
      //
      // Each entry maps a name to the DEFINITION IDENTITIES exported under it,
      // never to the bare name. Keying on names alone would admit by collision:
      // listed model A exporting `customers` would clear the gate for the name
      // everywhere, and listed model B that imports a different, hidden
      // `customers` would then serve the hidden one, because the gate matched
      // the name while Malloy resolved the declaration in B's namespace. With
      // identities, a request is admitted only when the requested model
      // resolves the name to the very declaration a listed model exported —
      // which is what makes the union genuinely admit nothing new. A legitimate
      // re-export still works: the exporting model's closure carries the
      // declaration's own location, wherever the file it lives in.
      // (Relies on applyDiscoveryPolicyToModels having run first, so
      // getSources()/getQueries() are already export-curated.)
      let packageCuratedSources:
         | ReadonlyMap<string, ReadonlySet<string>>
         | undefined;
      let packageCuratedQueries:
         | ReadonlyMap<string, ReadonlySet<string>>
         | undefined;
      if (mode === "declared" && exploresDeclared && exploreSet) {
         const sources = new Map<string, Set<string>>();
         const queries = new Map<string, Set<string>>();
         const add = (
            into: Map<string, Set<string>>,
            name: string | undefined,
            identity: string | undefined,
         ): void => {
            // No location ⇒ nothing to prove identity with, so contribute
            // nothing and leave the name to each model's own closure.
            if (!name || !identity) return;
            const existing = into.get(name);
            if (existing) existing.add(identity);
            else into.set(name, new Set([identity]));
         };
         for (const [modelPath, model] of this.models) {
            // Only .malloy files curate a query surface. A notebook listed in
            // explores is already invalid (getInvalidExplores flags it) but is
            // served fail-safe, and must not contribute names here — the
            // sibling loops (listModels, emptyDiscoveryWarnings) filter the
            // same way.
            if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
            if (!exploreSet.has(modelPath)) continue;
            for (const source of model.getSources() ?? []) {
               add(sources, source.name, model.definitionIdentity(source.name));
            }
            for (const query of model.getQueries() ?? []) {
               add(queries, query.name, model.definitionIdentity(query.name));
            }
         }
         packageCuratedSources = sources;
         packageCuratedQueries = queries;
      }
      for (const [modelPath, model] of this.models) {
         model.setQueryBoundary({
            mode,
            exploresDeclared,
            isQueryEntryPoint: exploreSet ? exploreSet.has(modelPath) : true,
            packageCuratedSources,
            packageCuratedQueries,
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
         // The canonical home for the package's declared tags, mirrored from the
         // block above — which is where the wire originally carried them. Both
         // are populated for as long as the deprecated home is supported, so a
         // client migrates when it chooses rather than when this ships.
         queryMetadata:
            outcome.packageMetadata.materialization?.queryMetadata ?? null,
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
      );
      pkg.renderTagWarnings = renderTagWarnings;
      await pkg.discoverDashboards();
      pkg.manifestWarnings = outcome.packageMetadata.manifestWarnings ?? [];
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
      // `#@ preaggregate` gets the same strict-at-load treatment, for the reason
      // given on preaggregatePolicyWarnings: a declaration that cannot take
      // effect is invisible in the answers, so warning here would leave the
      // author believing they had a rollup.
      const invalidPreaggregate = pkg.formatInvalidPreaggregatePolicy();
      if (invalidPreaggregate) {
         logger.error(
            `Package ${packageName} has an invalid pre-aggregation declaration`,
            {
               packageName,
               detail: invalidPreaggregate,
            },
         );
         throw new BadRequestError(invalidPreaggregate);
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
      // After the gates, so a package with a bad declaration is rejected rather
      // than having a companion compiled for it. At create time no manifest is
      // bound yet, so the companion routes to a rollup that recomputes from the
      // base until a bind triggers reloadAllModels — correct answers, no
      // acceleration, which is the same resting state as the rest of the persist
      // path.
      await pkg.pushPreaggregateServeModels();
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

   /**
    * The package's declared `queryMetadata` — the least specific author-declared
    * layer, and the counterpart to {@link Model.getDeclaredQueryMetadata}.
    *
    * Prefers the canonical top-level field and falls back to the deprecated
    * `materialization.queryMetadata`. The fallback is not dead code: a PATCH may
    * still set the block (an un-migrated client), and metadata assembled by
    * anything that predates the top-level field carries only the block. Callers
    * want the package's bag, not its build policy, and should not have to know
    * which home it arrived in.
    */
   public getDeclaredQueryMetadata(): QueryMetadata | null {
      return (
         this.packageMetadata.queryMetadata ??
         this.packageMetadata.materialization?.queryMetadata ??
         null
      );
   }

   /**
    * Every `queryMetadata` bag this package's authors declared, at whatever
    * level, as declared. What the publish gate needs in order to name the line
    * to edit.
    *
    * Whether a source persists does not enter into it. A bag on a source rides
    * every query against that source, materialized or not, so a source is listed
    * because it declared something — not because a build reads it.
    */
   private queryMetadataDeclarations(): QueryMetadataDeclaration[] {
      const packageDeclaration = this.getDeclaredQueryMetadata();
      return [
         ...(packageDeclaration
            ? [{ level: "package" as const, queryMetadata: packageDeclaration }]
            : []),
         ...[...this.models.values()].flatMap((model) => {
            const modelDeclaration = model.getDeclaredQueryMetadata();
            return [
               ...(modelDeclaration
                  ? [
                       {
                          level: "model" as const,
                          // The model path goes in `modelPath`, not `subject`:
                          // the wire schema keeps the two apart, so a client
                          // filtering findings by model missed every one of
                          // these while `subject` read like a source name.
                          modelPath: model.getPath(),
                          queryMetadata: modelDeclaration,
                       },
                    ]
                  : []),
               ...model
                  .getDeclaredSourceQueryMetadata()
                  .map(({ sourceName, queryMetadata }) => ({
                     level: "source" as const,
                     subject: sourceName,
                     // A source name is unique within a model, not within a
                     // package: without the path, two models declaring a
                     // same-named source produce identical findings that dedupe
                     // to a single message naming neither file.
                     modelPath: model.getPath(),
                     queryMetadata,
                  })),
            ];
         }),
      ];
   }

   /**
    * How many properties a statement actually SENDS, per model file, once the
    * package, model-file and source layers have merged.
    *
    * The budget is the one rule that cannot be checked per declaration: every
    * layer can sit under the author budget while their merge sits over it, and
    * it is the merge that rides the statement.
    *
    * The FLOOR — package ⊕ model file — is reported for every model, whether or
    * not any of its sources declares anything. Sizing only the declaring sources
    * missed the common shape outright: a package and a model file that overflow
    * together published clean whenever no source in the file happened to add a
    * tag, and every query against every source in that file then shed context
    * properties.
    */
   private queryMetadataEffectiveMerges(): {
      modelPath: string;
      floorSize: number;
      sources: { subject: string; size: number }[];
   }[] {
      const packageDeclaration = this.getDeclaredQueryMetadata();
      return [...this.models.values()].map((model) => {
         const floor = {
            ...(packageDeclaration ?? {}),
            ...(model.getDeclaredQueryMetadata() ?? {}),
         };
         return {
            modelPath: model.getPath(),
            floorSize: Object.keys(floor).length,
            sources: model
               .getDeclaredSourceQueryMetadata()
               .map(({ sourceName, queryMetadata }) => ({
                  subject: sourceName,
                  size: Object.keys({ ...floor, ...queryMetadata }).length,
               })),
         };
      });
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
      // Render-tag findings, dashboard lint findings, and storage= warnings share
      // the one operator-facing warnings array (the {model, subject, message}
      // shape carries all of them).
      const allWarnings = [
         ...this.renderTagWarnings,
         ...this.dashboardWarnings,
         ...this.storageWarnings(),
         ...this.droppedPersistWarnings(),
         // A listed model whose curated surface is empty. Advisory (an
         // import-only file is legitimate and must not block a publish, so it
         // stays out of exploresWarnings), but it must ride the API: the QA
         // shape this closes was a package reporting exploresWarnings: none
         // while listed files surfaced nothing (HANDOFF CR-5).
         ...this.emptyDiscoveryWarnings(),
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
            declarations: this.queryMetadataDeclarations(),
            effectiveMerges: this.queryMetadataEffectiveMerges(),
            manifestWarnings: this.manifestWarnings,
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
         model.setPreaggregateEntityIdResolver(() =>
            this.getPreaggregateEntityIds(),
         );
      }
   }

   /**
    * The `sourceEntityId`s of the sources pre-aggregation SYNTHESIZED, as
    * distinct from the `#@ persist` sources an author declared. Read off the
    * build plan's `origin`, which exists to carry exactly this provenance — not
    * matched on the rollup naming convention, which would silently misclassify
    * an author's source that happened to share the shape.
    *
    * Consumed by Model.withoutPreaggregateEntries, which keeps these entries away
    * from every runnable except the companion model that declares them.
    *
    * Memoized on the build plan's identity: the plan is replaced wholesale on
    * load, so reference equality is a sufficient and always-correct key.
    */
   public getPreaggregateEntityIds(): ReadonlySet<string> {
      if (this.preaggregateEntityIdCache?.plan === this.buildPlan) {
         return this.preaggregateEntityIdCache.ids;
      }
      const ids = new Set<string>();
      for (const source of Object.values(this.buildPlan?.sources ?? {})) {
         if (source.origin === "preaggregate" && source.sourceEntityId) {
            ids.add(source.sourceEntityId);
         }
      }
      this.preaggregateEntityIdCache = { plan: this.buildPlan, ids };
      return ids;
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
      const derived = deriveServeBindings(
         entries,
         this.persistSourceAliasesByName(),
      );
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

   /**
    * Compile each model's synthesized pre-aggregation companion so the serve path
    * can route to a rollup (see Model.buildPreaggregateServeModel). Called after
    * (re)building the model set, and again after a manifest bind, since the
    * companion is compiled against the manifest that substitutes its tables.
    *
    * A no-op for a model with no usable `#@ preaggregate`: synthesis returns
    * nothing and the model's serve path is left exactly as it was.
    */
   private async pushPreaggregateServeModels(): Promise<void> {
      for (const model of this.models.values()) {
         await model.buildPreaggregateServeModel(
            this.packagePath,
            this.malloyConfig,
            this.buildManifestEntries,
         );
      }
   }

   /**
    * For each persist source this package declares, the OTHER sources that
    * materialize into its table.
    *
    * A manifest entry names only the source that built its table, but an address
    * can have several sources: `#@ persist` is inherited and `extend` does not
    * change materialization SQL, so a base and its extension share an address and
    * therefore one table. The extension must not get a table of its own — it must
    * read the base's — and this is what lets {@link deriveServeBindings} bind every
    * name that resolves to the entry instead of only the builder's.
    *
    * Grouped BY content address, then keyed BY name. The grouping has to use the
    * publisher's own addresses, because that is what decides which sources really
    * share a table; the key has to be a name, because that is the one identifier an
    * entry carries that means the same thing whoever built it — an instructed
    * build stamps the CALLER's `sourceEntityId` on its entry, which the publisher
    * treats as opaque.
    *
    * Read off the package's own build plan, which already carries the address per
    * source, so nothing has to travel on the wire or be stored per entry.
    *
    * Each name still faces the eligibility gate in
    * {@link bindStorageServeBindings}: being an alias of a materialized source
    * does not exempt a source from being refused on its own merits.
    */
   private persistSourceAliasesByName(): Record<string, string[]> {
      const namesByAddress = new Map<string, string[]>();
      for (const source of Object.values(this.buildPlan?.sources ?? {})) {
         if (!source.sourceEntityId || !source.name) continue;
         const group = namesByAddress.get(source.sourceEntityId);
         if (group) group.push(source.name);
         else namesByAddress.set(source.sourceEntityId, [source.name]);
      }
      const byName: Record<string, string[]> = {};
      for (const group of namesByAddress.values()) {
         for (const name of group) byName[name] = group;
      }
      return byName;
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
    * REJECTION messages for every `#@ preaggregate` declaration in the package
    * that cannot take effect: one on something other than a measure, one whose
    * measure cannot be re-aggregated from a stored partial, one whose grain does
    * not resolve against its source. See preaggregation_validation for the rules.
    *
    * Strict at publish AND at load, like {@link incrementalPolicyWarnings} and
    * unlike {@link persistencePolicyWarnings}. The reason is specific to this
    * feature: pre-aggregation is invisible when it works, since a query never
    * names a rollup and returns the same answer either way. So an author whose
    * declaration was quietly ignored sees correct numbers, assumes acceleration,
    * and finds out from a bill. Warning at load would put that discovery in an
    * operator's log and leave the one person who can fix it reading a clean
    * publish.
    */
   public preaggregatePolicyWarnings(): string[] {
      const messages: string[] = [];
      for (const [modelPath, model] of this.models) {
         for (const violation of model.preaggregateViolations()) {
            // The model path is prepended because the same source name can occur
            // in two models, and the author needs to know which file to open.
            messages.push(`${modelPath}: ${violation.message}`);
         }
      }
      return messages;
   }

   /**
    * The {@link preaggregatePolicyWarnings} joined into one string, or "" when
    * every `#@ preaggregate` in the package can take effect.
    */
   public formatInvalidPreaggregatePolicy(): string {
      return this.preaggregatePolicyWarnings().join("\n");
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
    * One message per LISTED model whose discovery surface is empty: its export
    * closure yields no sources and no named queries (an import-only file that
    * re-exports nothing, an `export {}` that filters everything out, or an
    * empty file). Such a model renders a blank page and lists as [] to an
    * agent, which reads as broken; the fix is an explicit re-export, or
    * unlisting the file. Log-only at load, and surfaced on the package's
    * warnings array (see getPackageMetadata) — deliberately NOT part of
    * exploreWarnings, which is strict-at-publish: import-only files are a
    * legitimate pattern and must not block a publish. Hidden (non-listed)
    * models are skipped — nobody browses them, so an empty surface there is
    * just normal plumbing.
    */
   public emptyDiscoveryWarnings(): Array<{ model: string; message: string }> {
      const exploreSet = this.exploreSet();
      const warnings: Array<{ model: string; message: string }> = [];
      for (const [modelPath, model] of this.models) {
         if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
         if (exploreSet && !exploreSet.has(modelPath)) continue;
         if (model.hasEmptyDiscoverySurface()) {
            warnings.push({
               model: modelPath,
               message:
                  `Model "${modelPath}" is listed in explores but exposes ` +
                  `nothing: its export closure surfaces no sources or named ` +
                  `queries (typically an import-only file, or an export {} ` +
                  `that filters everything out). Add e.g. ` +
                  `'export { source_name }' to surface sources on this ` +
                  `model, or remove it from explores.`,
            });
         }
      }
      return warnings;
   }

   /** Log {@link emptyDiscoveryWarnings}; shared by load and reload. */
   private logEmptyDiscoveryWarnings(): void {
      for (const warning of this.emptyDiscoveryWarnings()) {
         logger.warn(`Package ${this.packageName} has a blank-looking model`, {
            packageName: this.packageName,
            detail: warning.message,
         });
      }
   }

   public listDatabases(): ApiDatabase[] {
      return this.databases;
   }

   public getModel(modelPath: string): Model | undefined {
      return this.models.get(modelPath);
   }

   /**
    * Authorization evaluator for /compile. Prefer the target's cached Model;
    * a new model path has none, so fall back to any package Model solely to
    * evaluate gates carried by the compiled runnable's own ModelDef.
    */
   public getCompileAuthorizationModel(modelPath: string): {
      model: Model | undefined;
      exact: boolean;
   } {
      const exact = this.models.get(modelPath);
      return exact
         ? { model: exact, exact: true }
         : { model: this.models.values().next().value, exact: false };
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
      // Same for pre-aggregation, and this is also where a manifest bind lands
      // (bindManifest → reloadAllModels), so the companion is recompiled against
      // the manifest that substitutes its rollup tables.
      await this.pushPreaggregateServeModels();
      this.renderTagWarnings = renderTagWarnings;
      this.manifestWarnings = outcome.packageMetadata.manifestWarnings ?? [];
      // A reload re-reads publisher.json in the worker; pick up any change to
      // the explore set and query-boundary mode so listModels()/the gate
      // reflect edited explores without a full Package.create.
      this.packageMetadata.explores = outcome.packageMetadata.explores;
      this.packageMetadata.queryableSources =
         outcome.packageMetadata.queryableSources;
      this.packageMetadata.manifestLocation =
         outcome.packageMetadata.manifestLocation ?? null;
      this.applyDiscoveryPolicyToModels();
      this.applyQueryBoundaryToModels();
      // AFTER the refreshed explore set is installed, never before. Dashboard
      // discovery consults it (see isQueryableEntryPoint), so running it first
      // computed the served set against the PREVIOUS policy: the reload that
      // first curates a package would have kept serving the manifests that
      // curation was meant to withhold, and gone on doing so until some later
      // reload, since nothing recomputes them in between.
      await this.discoverDashboards();
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

   /**
    * Whether a model file may be a top-level query target, the FILE-LEVEL half
    * of the policy `applyQueryBoundaryToModels` pushes onto each Model: inert
    * unless `explores` is declared AND `queryableSources` is `"declared"`.
    *
    * Read here because a dashboard is only worth serving if the queries its
    * manifest advertises can actually run.
    *
    * Deliberately only that half, and the gap is worth stating rather than
    * leaving to be rediscovered. The boundary has a second, within-file level:
    * `assertQueryBoundaryEarly` also requires the target to be inside the
    * model's `export {}` closure. So a file listed in `explores` that only
    * imports and re-exports nothing still refuses every query, and a COMPOSITE
    * dashboard in such a file is served with a manifest whose every tile 404s.
    * Mirroring that second level here would mean resolving each tile against
    * the closure before the manifest exists, which is a bigger change than this
    * slice should make.
    *
    * The warning below covers only the WHOLLY import-only case, which is what
    * `hasEmptyDiscoverySurface` detects. A dashboard file that exports
    * something, but not the source its tiles actually read, still serves a
    * manifest whose tiles 404 and produces no finding at all. That gap is known
    * and is not closed here; do not read the warning as complete coverage.
    */
   private isQueryableEntryPoint(modelPath: string): boolean {
      if (!this.queryBoundaryActive()) return true;
      const exploreSet = this.exploreSet();
      return exploreSet ? exploreSet.has(modelPath) : true;
   }

   /**
    * A reusable form of {@link isQueryableEntryPoint} that resolves the explore
    * set once, for callers testing more than one path.
    */
   private servableEntryPoints(): (modelPath: string) => boolean {
      if (!this.queryBoundaryActive()) return () => true;
      const exploreSet = this.exploreSet();
      return (modelPath: string) =>
         exploreSet ? exploreSet.has(modelPath) : true;
   }

   /**
    * Whether the query boundary restricts anything at all: `explores` declared
    * AND `queryableSources` left at `"declared"`. Under `"all"` the boundary is
    * inert and every compiled source stays directly queryable. An unrecognised
    * `queryableSources` counts as `"declared"`, which is what the spec says.
    *
    * Shared so a finding cannot assert a policy the package does not have.
    * `Model.hasEmptyDiscoverySurface` is about the DISCOVERY surface and is
    * indifferent to this mode, so a warning about queries being refused has to
    * check the mode itself rather than lean on that predicate.
    */
   private queryBoundaryActive(): boolean {
      if (this.packageMetadata.queryableSources === "all") return false;
      // `exploresDeclared` rather than an `exploreSet() !== null` comparison:
      // the two coincide by construction, but this is the stated source of
      // truth for the fact and it does not allocate a Set to answer it.
      return this.exploresDeclared();
   }

   /**
    * Whether a `dashboards/*.malloy` claims to be a dashboard, read from its
    * source text rather than from a `ModelDef`.
    *
    * Consulted on two paths, both of which have in common that the tag cannot
    * be read the normal way. The original is a file that failed to compile, so
    * there is no `ModelDef` to read it off; the alternative there was to list
    * every uncompilable file in the directory, which contradicts the documented
    * rule that an untagged file is a shared include and produced a dashboard
    * that existed only while a sibling include was broken. The second is a drop
    * path: a file that produced no facts and no compile error, which a file
    * that compiled cleanly can reach, so this is no longer only about broken
    * files.
    *
    * NOT consulted on the third drop path, the one where the manifest build
    * threw. There the facts are in scope and the tag is perfectly readable, so
    * that site reads `factsCarryArtifactTag` and this heuristic would be the
    * wrong tool; its comment says so.
    *
    * Deliberately textual and deliberately generous: it looks for an `artifact`
    * annotation at the start of a line. Note the cost of a false positive is
    * NOT the same on every path. On the original one it lists a broken file
    * that was never a dashboard, which is merely noisy. On the drop paths it
    * emits an `error` finding claiming a dashboard should exist and registers
    * the slug, which silences a genuinely dangling `# drill` elsewhere. A false
    * negative hides a real broken dashboard, which is still worse than either.
    * Never throws: an unreadable file is simply not a dashboard.
    */
   private async claimsToBeADashboard(modelPath: string): Promise<boolean> {
      try {
         const source = await fs.readFile(
            safeJoinUnderRoot(this.packagePath, modelPath),
            "utf8",
         );
         return /^[ \t]*##?[ \t]*artifact\b/m.test(source);
      } catch {
         return false;
      }
   }

   /**
    * Re-read the package's dashboards from its compiled models. Called at load
    * and after every reload, because a dashboard is defined by an annotation on
    * a compiled model — it cannot change without the models changing.
    *
    * Never throws: a package whose dashboards can't be read still serves its
    * models. A file in `dashboards/` with no artifact tag is a shared include
    * and is skipped, exactly as Malloyyo treats it.
    */
   private async discoverDashboards(): Promise<void> {
      const discovered = new Map<
         string,
         DashboardManifest & { error?: string }
      >();
      // Kept alongside the manifests so the lint runs once, after every
      // dashboard is known: a drill target can only be checked against the
      // complete slug set.
      const factsByPath = new Map<string, DashboardModelFacts>();
      // Every model, not just the dashboard files, because `# drill` is
      // declared on a model dimension: a tag reachable only from a notebook, or
      // one in a package with no dashboards at all, is exactly as breakable and
      // would otherwise never be checked.
      const allFacts = new Map<string, DashboardModelFacts>();
      // Dashboards the curation gate held back, reported once discovery settles.
      const heldBack: { modelPath: string; name: string }[] = [];
      // Files whose derived slug is outside the documented name pattern. Served
      // anyway; see the comment at the check below.
      const unconventionalSlugs: { modelPath: string; name: string }[] = [];
      // Dashboards served from a file that re-exports nothing, so every query
      // against them is refused by the within-file half of the query boundary.
      // Dashboard files dropped because something threw while reading them, as
      // opposed to because they are not dashboards. Reported for the same
      // reason the lint reports its own truncation: without this the file just
      // vanishes from the listing and a missing dashboard is indistinguishable
      // from one that was never written.
      const droppedByError: { modelPath: string; name: string }[] = [];
      // Every slug this package actually has a dashboard for, INCLUDING the
      // ones withheld below. The drill lint resolves against this rather than
      // against the served set, so withholding a dashboard does not turn a
      // correct `# drill { to=... }` into "not a dashboard in this package".
      const dashboardSlugs = new Set<string>();
      for (const modelPath of Array.from(this.models.keys()).sort()) {
         const model = this.models.get(modelPath);
         if (!model) continue;

         let facts: DashboardModelFacts | undefined;
         try {
            facts = model.getDashboardModelFacts();
            if (facts) allFacts.set(modelPath, facts);
         } catch (err) {
            logger.warn("Reading a model's dashboard facts failed", {
               packageName: this.packageName,
               modelPath,
               error: errMessage(err),
            });
         }

         if (!isDashboardModelPath(modelPath)) continue;
         const name = dashboardSlug(modelPath);

         // Establish that the file IS a dashboard before any of the checks
         // below, because every one of them reports on the assumption that it
         // is. Ordering these the other way round told the author that
         // `dashboards/_shared.malloy`, an untagged shared include the spec
         // says is never listed, was a dashboard being withheld from them.
         let manifest: (DashboardManifest & { error?: string }) | undefined;
         if (facts) {
            try {
               manifest = buildDashboardManifest(facts);
            } catch (err) {
               logger.warn("Dashboard discovery failed", {
                  packageName: this.packageName,
                  modelPath,
                  error: errMessage(err),
               });
               // Gated, because a throw in here says nothing about whether
               // the file was ever a dashboard and a finding about a file with
               // no artifact tag would invent one. Read off `facts`, which is
               // in scope, rather than through the textual
               // `claimsToBeADashboard`: the tag is perfectly readable on this
               // path, it was the manifest CONSTRUCTION that failed, so the
               // exact signal is available and the heuristic would be the wrong
               // tool. This is also the drop site that pays most for a false
               // positive.
               if (factsCarryArtifactTag(facts)) {
                  droppedByError.push({ modelPath, name });
                  dashboardSlugs.add(name);
               }
               continue;
            }
            if (!manifest) {
               // No artifact tag, so a shared include. Its facts are still
               // kept, because a file that produced no dashboard has to be
               // explained if the reason was a tag that failed to parse.
               factsByPath.set(modelPath, facts);
               continue;
            }
         } else {
            // Unreadable, so the file failed to compile and its artifact tag
            // cannot be read. It is listed anyway carrying the error, because a
            // broken dashboard should be visibly broken rather than absent.
            //
            // Only if it actually claims to be one. `api-doc.yaml` promises a
            // file with no artifact tag is a shared include and is not listed,
            // and a compile failure must not turn that promise off: a syntax
            // error in `dashboards/_shared.malloy` otherwise produced a phantom
            // dashboard that vanished once the error was fixed. The tag cannot
            // be read from a model that did not compile, so it is read from the
            // source text, a heuristic used ONLY on this already-broken path.
            const error = model.getCompilationError();
            if (!error || !(await this.claimsToBeADashboard(modelPath))) {
               // Reached with no facts AND no compile error to show, so the
               // branch above drops the file. If it claims to be a dashboard,
               // that is one disappearing with nothing said, which is the case
               // worth reporting; a file that is simply not a dashboard is not,
               // and `claimsToBeADashboard` is what tells them apart.
               //
               // The test is `!facts && !error`, deliberately, NOT "the facts
               // read threw". Facts come back undefined without throwing
               // whenever `modelDef` is undefined, and `Model.fromSerialized`
               // builds exactly that with `compilationError` ALSO undefined.
               // Reachable here from a corrupt worker payload; NOT from an
               // empty notebook, which an earlier version of this comment gave
               // as the example, because `isDashboardModelPath` admits only
               // `.malloy` and the all-markdown shape is a notebook. Keying on
               // the throw left the payload case dropping the file with no
               // finding and no log line either, since the log sits in the
               // catch. That was worse than the case this reports.
               //
               // `!error` is redundant with the `claims` result but is what
               // stops `claimsToBeADashboard` running twice: when `error` is
               // truthy the condition above already called it, and this
               // short-circuits before calling it again. Do not "simplify" it
               // away; that reinstates a second `readFile` per uncompilable
               // non-dashboard.
               if (!error && (await this.claimsToBeADashboard(modelPath))) {
                  logger.warn("Dashboard file produced no facts and no error", {
                     packageName: this.packageName,
                     modelPath,
                  });
                  droppedByError.push({ modelPath, name });
                  // Registered so `lintDrillTargets` does not additionally
                  // report a `# drill` naming this slug as "not a dashboard in
                  // this package", which is false: the package has the file.
                  //
                  // NOT the same as what the curation gate does, and the
                  // difference is a known gap rather than a claim. A withheld
                  // dashboard registers its slug AND gets a per-drill
                  // `withheldDrills` finding saying the click dead-ends. A
                  // dropped one registers the slug only, so a drill at it
                  // dead-ends identically with nothing said against that
                  // dimension. The drop finding above states the root cause,
                  // which is why this is tolerable, but a consumer rendering
                  // findings per `subject` shows nothing on the drill. Closing
                  // it means giving the drop path its own `withheldDrills`
                  // sibling.
                  dashboardSlugs.add(name);
               }
               continue;
            }
            manifest = {
               name,
               title: name,
               autorun: true,
               entryFile: modelPath,
               givens: [],
               error: error.message,
            };
         }

         // The file IS a dashboard, so a drill naming it resolves. Recorded
         // before the curation gate below on purpose: a drill pointing at a
         // dashboard this package really has must not be reported as naming
         // something that does not exist merely because curation withholds it.
         // Every dashboard gets a slug registered here, so a drill naming one
         // always resolves. NOT that every dashboard is reachable: curation can
         // withhold it and `getDashboard` then answers undefined, which is what
         // the withheld-drill finding below reports.
         dashboardSlugs.add(name);

         // Curation gate. Every other listing path in this class consults
         // `exploreSet()`; discovery did not, so a curated package served a
         // full manifest, advertising a query name, given names and
         // suggest-query names that the query boundary then refuses with 404.
         // Held back rather than published unusable, with a warning saying so.
         if (!this.isQueryableEntryPoint(modelPath)) {
            heldBack.push({ modelPath, name });
            continue;
         }

         // From here the dashboard IS served, which is where a finding may say
         // so. Reported after the curation gate rather than before it: saying
         // "is served" about a file the next branch withholds put two
         // contradictory warnings on the same dashboard, and the confident one
         // was the wrong one.
         //
         // A name outside the documented `dashboardName` pattern is served and
         // only noted. Measured rather than assumed: the route is a plain
         // Express param and nothing validates the pattern at runtime, so the
         // encoded URL resolves. An earlier version withheld such a dashboard
         // and told the author it "could not be addressed at a URL", which
         // broke every working dashboard whose file carried a version in its
         // name. The pattern still matters to a client generated from the
         // spec, so it is worth saying; it is not worth refusing to serve.
         if (!matchesDocumentedDashboardName(name)) {
            unconventionalSlugs.push({ modelPath, name });
         }

         discovered.set(name, manifest);
         if (facts) factsByPath.set(modelPath, facts);
      }
      this.dashboards = discovered;
      this.dashboardWarnings = await this.lintDashboards(
         factsByPath,
         allFacts,
         heldBack,
         unconventionalSlugs,
         dashboardSlugs,
         droppedByError,
      );
      for (const warning of this.dashboardWarnings) {
         logger.warn("Dashboard lint", {
            packageName: this.packageName,
            model: warning.model,
            detail: warning.message,
         });
      }
   }

   /**
    * Run the dashboard lint across the package once discovery has settled.
    *
    * Never throws: a lint failure must not cost the package its dashboards. The
    * findings ride the package response as non-fatal `warnings`, the same
    * surface render-tag findings use, rather than a separate verb.
    */
   private async lintDashboards(
      factsByPath: ReadonlyMap<string, DashboardModelFacts>,
      allFacts: ReadonlyMap<string, DashboardModelFacts>,
      heldBack: readonly { modelPath: string; name: string }[],
      unconventionalSlugs: readonly { modelPath: string; name: string }[],
      knownSlugs: ReadonlySet<string>,
      droppedByError: readonly { modelPath: string; name: string }[],
   ): Promise<ApiPackageWarning[]> {
      const warnings: ApiPackageWarning[] = [];
      // Keyed on dimension + destination, so one drill is reported once for the
      // package rather than once per file that imports its source.
      const withheldDrills = new Map<string, ApiPackageWarning>();
      try {
         // First because it cannot throw, so these survive a truncation that
         // costs everything after them, and a dashboard that vanished with no
         // explanation is the worst thing on this surface to lose.
         //
         // Not because it is "the only finding about a dashboard missing from
         // the listing", which an earlier version of this comment claimed and
         // which is false three ways: a held-back dashboard `continue`s before
         // `discovered.set`, its `withheldDrills` sibling describes the same
         // withheld file, and `lintUndiscoveredDashboard` fires for files that
         // produced no manifest AND whose facts reached `factsByPath`.
         //
         // That second condition is load-bearing and an earlier version of this
         // sentence omitted it, which overstated the lint's reach in the one
         // direction that matters. The loop below iterates `factsByPath`, and
         // two paths `continue` before anything is added to it: a manifest
         // build that threw, and a held-back file. Neither lint runs for those,
         // so their parse failures are NOT reported and only the root-cause
         // warning here says anything about them.
         for (const { modelPath, name } of droppedByError) {
            warnings.push({
               model: modelPath,
               subject: name,
               message:
                  `"${modelPath}" carries an artifact tag but could not be ` +
                  `read, so it is not served and there is no dashboard ` +
                  `"${name}". The cause is in the server log. Reload the ` +
                  `package to try again.`,
               severity: "error",
            });
         }
         for (const { modelPath, name } of unconventionalSlugs) {
            warnings.push({
               model: modelPath,
               subject: name,
               message:
                  `is served, but "${name}" is outside the conventional ` +
                  `dashboard name shape (letters, digits, "-" and "_"). This ` +
                  `server routes it and its published URL carries the name ` +
                  `encoded, so the dashboard works. The convention is worth ` +
                  `keeping anyway: a name outside it has to be percent-encoded ` +
                  `by every caller that builds the URL by hand. Rename the ` +
                  `file if that matters to yours.`,
               severity: "warn",
            });
         }
         for (const { modelPath, name } of heldBack) {
            // A drill AT a withheld dashboard still dead-ends: the dashboard is
            // real, so calling it "not a dashboard in this package" is false,
            // but the click 404s all the same. Say which of the two it is.
            //
            // Deduplicated on the dimension and destination for the same reason
            // `lintDrillTargets` is: a drill is declared on a model dimension,
            // so every file importing that source carries it, and reporting per
            // importer emitted the identical finding four times in the test
            // fixture alone.
            for (const file of allFacts.values()) {
               for (const drill of file.drills) {
                  if (!drill.to.includes(name)) continue;
                  const where = `${drill.source}.${drill.dimension}`;
                  withheldDrills.set(`${where}|${name}`, {
                     subject: where,
                     message:
                        `# drill on ${where} targets "${name}", which IS a ` +
                        `dashboard in this package but is not served (see the ` +
                        `finding on "${modelPath}"), so the click has nowhere ` +
                        `to land.`,
                     // `error`, matching `lintDrillTargets`. The two describe
                     // the same broken click and differ only in why the
                     // destination is missing, so they should not differ in
                     // how loudly they say it.
                     severity: "error",
                  });
               }
            }
            warnings.push({
               model: modelPath,
               subject: name,
               message:
                  `is a dashboard, but "${modelPath}" is not listed in ` +
                  `'explores' and this package sets ` +
                  `queryableSources: "declared", so its query would be ` +
                  `refused. It is not served. Add it to 'explores', or set ` +
                  `queryableSources: "all" to keep the curated surface for ` +
                  `discovery only. Listing it is not always sufficient on its ` +
                  `own: the queryable sources are the union of every listed ` +
                  `file's export closure, so a tile reading a source that only ` +
                  `an UNLISTED file exports is still refused. List that file ` +
                  `too, or re-export the source from one already listed.`,
               severity: "warn",
            });
         }
         for (const [modelPath, facts] of factsByPath) {
            const manifest = this.dashboards.get(dashboardSlug(modelPath));
            const findings = manifest
               ? lintDashboard(facts, manifest)
               : lintUndiscoveredDashboard(facts);
            for (const finding of findings) {
               warnings.push({ model: modelPath, ...finding });
            }
         }
         // Drill tags live on model dimensions, not on any one dashboard, so
         // they are reported against the package rather than a single file,
         // and they are scanned across every model rather than only the
         // dashboard files — a notebook cell drills from the same tag.
         const drillFacts = Array.from(allFacts.values());
         for (const finding of withheldDrills.values()) {
            warnings.push(finding);
         }
         for (const finding of lintDrillTargets(drillFacts, knownSlugs)) {
            warnings.push(finding);
         }
         for (const finding of lintSelfDrills(drillFacts)) {
            warnings.push(finding);
         }
         // Scanned across every model for the same reason as the drill lints: a
         // given is declared on a model and reached through imports, so a broken
         // declaration in a file no dashboard imports is exactly as silent.
         for (const finding of lintGivenTags(drillFacts)) {
            warnings.push(finding);
         }
         for (const finding of await this.unsupportedComponentWarnings()) {
            warnings.push(finding);
         }
      } catch (err) {
         logger.warn("Dashboard lint failed", {
            packageName: this.packageName,
            error: errMessage(err),
         });
         // Say so on the surface the findings ride, not only in the server log.
         // This catch wraps every phase above, so a throw keeps the findings
         // collected up to that point and drops the rest, and a shorter list is
         // indistinguishable from a cleaner package. An author acting on "no
         // more findings" would be acting on a truncation.
         //
         // The message names the check CLASSES rather than a phase. The `try`
         // opens on the dropped-by-error pass and runs well before the
         // per-dashboard loop, covering the held-back, empty-surface and
         // unconventional-name passes, and closes after the
         // drill, given and component checks, so "the dashboards after the
         // failure were not checked" would be false for most of its throw
         // sites: a throw in `lintGivenTags` or `unsupportedComponentWarnings`
         // costs a whole class with every dashboard already checked. Which of
         // them was lost depends on where it failed and this catch cannot tell,
         // so it names the full set and says the answer is unknown, rather than
         // guessing and sending an operator after a broken dashboard file that
         // need not exist.
         //
         // It deliberately omits the unconventional-name and empty-surface
         // kinds, and that is not an oversight. Those two, plus the
         // dropped-by-error pass, are the first three loops in this `try`, all
         // pure pushes over arrays already computed in `discoverDashboards`, and
         // the first thing that can throw is the held-back loop after them. So
         // they always survive. Naming a kind that cannot be lost would tell an
         // author to doubt a finding they can trust.
         //
         // The thrown message stays in the log above and off the wire because it
         // is an ARBITRARY throw, so its text is unbounded and nothing else in
         // this array carries one. That, and only that, is the reason. An
         // earlier version of this comment justified it as protecting a
         // path-free response surface; there is no such property to protect,
         // because a Malloy compile error's message is built from
         // `prettyErrors()`, which prepends `FILE: file:///<absolute path>`, and
         // both `listModels` (via `modelError.message`) and these routes (via
         // `manifest.error`) publish that text verbatim. Do NOT restate that as
         // "pre-existing and identical on both", which an earlier draft did and
         // which is false: `listModels` filters on `explores` alone, while the
         // dashboard routes go through `queryBoundaryActive()` and so stop
         // filtering entirely under `queryableSources: "all"`. In that one
         // configuration a broken `dashboards/*.malloy` outside `explores` is
         // hidden from the model listing and still published here. See the
         // follow-up note in the tracker; it is not fixed in this commit.
         //
         // No `model` and no `subject`. The spec reserves `subject` for the one
         // dashboard or `source.dimension` a finding sits on, and this finding
         // sits on none: it is about the lint rather than about the package.
         warnings.push({
            message:
               `Dashboard lint stopped early, so this list is incomplete: an ` +
               `unknown subset of the curation, dashboard, drill, given and ` +
               `unsupported-component checks did not run. Treat a missing ` +
               `finding of any of those kinds as unknown rather than clean, ` +
               `including the absence of a "held back from the listing" ` +
               `finding. Reload the package to run the lint again. The ` +
               `dashboards themselves are unaffected, because they are ` +
               `discovered before the lint runs. An operator can find the ` +
               `cause in the server log under "Dashboard lint failed".`,
            severity: "warn",
         });
      }
      return warnings;
   }

   /**
    * A `dashboards/*.jsx` (or `.tsx`) in a package Publisher is serving.
    *
    * Malloyyo renders such a file as the dashboard's custom component;
    * Publisher does not implement that, so the file is inert here. This exists
    * for the migration case: a Malloyyo repo registered unchanged would
    * otherwise show its custom dashboards as an empty tile grid, or as nothing
    * at all, with no indication that a file was skipped. Say so at load, and
    * name the surface that does run author-written code.
    */
   private async unsupportedComponentWarnings(): Promise<ApiPackageWarning[]> {
      const dir = safeJoinUnderRoot(this.packagePath, DASHBOARDS_DIR);
      let entries: string[];
      try {
         entries = await fs.readdir(dir);
      } catch {
         return [];
      }
      const warnings: ApiPackageWarning[] = [];
      for (const entry of entries.sort()) {
         const suffix = COMPONENT_FILE_SUFFIXES.find((s) => entry.endsWith(s));
         if (!suffix) continue;
         const base = entry.slice(0, -suffix.length);
         warnings.push({
            model: `${DASHBOARDS_DIR}/${entry}`,
            subject: base,
            message:
               `Custom dashboard components are not supported, so ` +
               `"${DASHBOARDS_DIR}/${entry}" is ignored. Any ` +
               `${DASHBOARDS_DIR}/${base}${MODEL_FILE_SUFFIX} beside it still ` +
               `renders from its tags. For a page that runs its own code, use ` +
               `an HTML data app in the package's public/ directory.`,
            severity: "warn",
         });
      }
      return warnings;
   }

   public listDashboards(): ApiDashboard[] {
      // Resolved once rather than per dashboard: `isQueryableEntryPoint` builds
      // a Set from `explores` on every call.
      const servable = this.servableEntryPoints();
      return Array.from(this.dashboards.values())
         .filter((manifest) => servable(manifest.entryFile))
         .map((manifest) => ({
            resource: this.dashboardResource(manifest.name),
            packageName: this.packageName,
            name: manifest.name,
            path: manifest.entryFile,
            title: manifest.title,
            description: manifest.description,
            error: manifest.error,
         }));
   }

   /** The full manifest for one dashboard, or undefined if there is no such slug. */
   public getDashboard(name: string): ApiDashboardManifest | undefined {
      const manifest = this.dashboards.get(name);
      // Re-checked here as well as at discovery, because the two can drift.
      // `setPackageMetadata` (the metadata PATCH) installs a new explore set and
      // re-applies the query boundary WITHOUT re-running discovery, so a PATCH
      // that curates a package would otherwise leave this map serving the
      // manifests curation was meant to withhold until some unrelated reload.
      // The gate is a set lookup, so paying for it per request is free.
      //
      // This is deliberately one-directional: it can withhold a dashboard the
      // cached map still holds, but it cannot surface one discovery already
      // dropped, so RELAXING curation by PATCH needs a reload to take effect.
      // That is the safe direction to be wrong in.
      if (manifest && !this.isQueryableEntryPoint(manifest.entryFile)) {
         return undefined;
      }
      if (!manifest) return undefined;
      return {
         resource: this.dashboardResource(manifest.name),
         packageName: this.packageName,
         name: manifest.name,
         path: manifest.entryFile,
         title: manifest.title,
         description: manifest.description,
         error: manifest.error,
         query: manifest.query,
         tiles: manifest.tiles,
         dashboardColumns: manifest.dashboardColumns,
         startingGivens: manifest.startingGivens,
         autorun: manifest.autorun,
         givens: manifest.givens,
      };
   }

   /**
    * The dashboard's own URL.
    *
    * The NAME is percent-encoded; the environment and package names are not,
    * because both are validated on the way in and cannot carry a character that
    * matters here. The name is different: it is a filename basename, so it can
    * carry anything a filesystem allows. Serving such a dashboard is right, and
    * measured: the route matches and the encoded URL answers 200. Publishing
    * the name RAW was not. `dashboards/a#b.malloy` published
    * `.../dashboards/a#b`, where the `#` opens a fragment, so a client
    * following the link asked for `.../dashboards/a` and got a 404; `?` did the
    * same via a query string, and a space is not a legal URL character at all.
    * Encoded, all three answer 200.
    */
   private dashboardResource(name: string): string {
      return `${API_PREFIX}/environments/${this.environmentName}/packages/${this.packageName}/dashboards/${encodeURIComponent(name)}`;
   }

   public async listNotebooks(): Promise<ApiNotebook[]> {
      return await Promise.all(
         Array.from(this.models.keys())
            .filter((modelPath) => {
               return modelPath.endsWith(NOTEBOOK_FILE_SUFFIX);
            })
            .map(async (modelPath) => {
               const model = this.models.get(modelPath);
               const error = model?.getNotebookError();
               // A notebook that failed to compile has no cells and no
               // annotations to read a title from, so it lists as its filename
               // with the error — the same way a broken dashboard does.
               const listing = model?.getNotebookListing() ?? {};
               return {
                  environmentName: this.environmentName,
                  packageName: this.packageName,
                  path: modelPath,
                  title: listing.title,
                  description: listing.description,
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

   public setPackageMetadata(packageMetadata: ApiPackage) {
      this.packageMetadata = packageMetadata;
      this.applyDiscoveryPolicyToModels();
      this.applyQueryBoundaryToModels();
   }
}
