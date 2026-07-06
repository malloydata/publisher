import * as fs from "fs/promises";
import * as path from "path";

import "@malloydata/db-duckdb/native";
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
   ModelCompilationError,
   PackageNotFoundError,
   ServiceUnavailableError,
} from "../errors";
import { formatDuration, logger } from "../logger";
import { recordBuildPlanComputeDuration } from "../materialization_metrics";
import { assertSafeEnvironmentPath, safeJoinUnderRoot } from "../path_safety";
import { BuildManifest, BuildPlan } from "../storage/DatabaseInterface";
import { errMessage, ignoreDotfiles } from "../utils";
import { computePackageBuildPlan } from "./build_plan";
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

export class Package {
   private environmentName: string;
   private packageName: string;
   private packageMetadata: ApiPackage;
   private databases: ApiDatabase[];
   private models: Map<string, Model> = new Map();
   private packagePath: string;
   private malloyConfig: MalloyConfig;
   // Build-manifest binding state (Malloy Persistence v0). When bound, these
   // entries (sourceEntityId -> { tableName }) are what served queries use to route
   // persist sources to their materialized physical tables; they are also reused
   // by the /compile preview so previewed SQL matches executed SQL. Surfaced on
   // /status (via getPackageMetadata) so the control plane can confirm a worker
   // actually bound the distributed manifest instead of inferring it from logs.
   private buildManifestEntries: BuildManifest["entries"] | undefined;
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
   // Non-fatal render-tag findings aggregated across the package's models (each
   // tagged with its model path), surfaced read-only on
   // getPackageMetadata().warnings. Refreshed on load and reload. A bad render
   // tag does not fail the load (see Model.validateRenderTags); this is the
   // response-level signal that a tag is misconfigured.
   private renderTagWarnings: ApiPackageWarning[] = [];
   private static meter = publisherMeter();
   private static packageLoadHistogram = this.meter.createHistogram(
      "malloy_package_load_duration",
      {
         description: "Time taken to load a Malloy package",
         unit: "ms",
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
    * query chokepoints can enforce it without a back-reference to the Package:
    * `Model.getQueryResults` (the HTTP query route and the MCP tool) and the
    * `/compile` path (via `assertQueryBoundaryForRunnable`). Derived once here
    * (and on reload) rather than per query: the policy only changes when the
    * manifest is (re)read.
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
      for (const [modelPath, model] of this.models) {
         model.setQueryBoundary({
            mode,
            exploresDeclared,
            isQueryEntryPoint: exploreSet ? exploreSet.has(modelPath) : true,
         });
      }
   }

   static async create(
      environmentName: string,
      packageName: string,
      packagePath: string,
      environmentMalloyConfig: PackageConnectionInput,
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
         const status =
            error instanceof ModelCompilationError ||
            error instanceof MalloyError
               ? "compilation_error"
               : error instanceof ServiceUnavailableError
                 ? "pool_unavailable"
                 : "error";
         this.packageLoadHistogram.record(executionTime, {
            malloy_package_name: packageName,
            status,
         });
         // Clean up the package directory on failure, but NOT when packagePath
         // is an in-place mount symlink (watch mode). Removing it would unmount
         // the package, so a transient compile error from a half-typed model
         // saved mid-edit would brick the package until a restart. The symlink
         // points at the user's live source, which is left untouched; the next
         // save recompiles against it.
         try {
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
      };

      // Build live `Model`s from worker output. Any per-model compile
      // failure aborts the load — matches the historical behaviour of
      // `Package.create` failing the whole package on the first model
      // error. (`Package.reloadAllModels` keeps the failed-model
      // placeholders instead; that branch goes through a different
      // hydration path.)
      const models = new Map<string, Model>();
      const renderTagWarnings: ApiPackageWarning[] = [];
      for (const sm of outcome.models) {
         if (sm.compilationError) {
            const err = Model.deserializeCompilationError(sm.compilationError);
            logger.error("Model compilation failed", {
               packageName,
               modelPath: sm.modelPath,
               error: err.message,
            });
            // The outer catch in Package.create records the metric +
            // cleans the package directory.
            throw err;
         }
         const model = Model.fromSerialized(
            packageName,
            packagePath,
            malloyConfig,
            sm,
         );
         // Validate renderer tags on the main thread (the renderer is too heavy
         // to load inside the pure-CPU package-load worker). A misconfigured tag
         // is logged as a warning naming the target; it does not fail the load.
         // The findings also ride the package response as non-fatal `warnings`.
         for (const w of await model.validateRenderTags()) {
            renderTagWarnings.push({ model: sm.modelPath, ...w });
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

      const endTime = performance.now();
      const executionTime = endTime - startTime;
      this.packageLoadHistogram.record(executionTime, {
         malloy_package_name: packageName,
         status: "success",
      });
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

      // Compute the persist build plan off the live (unbound) models, before the
      // caller binds any configured manifest, so the surfaced plan reflects the
      // canonical build (not the manifest-rewritten SQL). Best-effort: a plan
      // failure is logged, not fatal — the package still serves; the plan is
      // just absent. Recompiles the models (duplicate schema RPCs vs the worker
      // compile); accepted for now.
      try {
         const buildPlanStart = Date.now();
         pkg.buildPlan = await computePackageBuildPlan(pkg);
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
      // Same fail-safe split for the materialization cron gate: an existing
      // package whose manifest violates the private-only cron rule still loads
      // (warn), but a publish of it is rejected (see package.controller).
      const invalidSchedule = pkg.formatInvalidSchedule();
      if (invalidSchedule) {
         logger.warn(`Package ${packageName} has an invalid cron schedule`, {
            packageName,
            detail: invalidSchedule,
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
      if (this.renderTagWarnings.length > 0) {
         metadata.warnings = [...this.renderTagWarnings];
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
   private recordManifestBinding(
      buildManifest: BuildManifest["entries"],
   ): void {
      const count = Object.keys(buildManifest).length;
      this.buildManifestEntries = count > 0 ? buildManifest : undefined;
      this.manifestEntryCount = count;
      this.manifestBindingStatus = count > 0 ? "bound" : "unbound";
      if (count === 0) {
         this.boundManifestUri = null;
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
    * Publish-gate for the package-level materialization cron (the power tier
    * of artifact-anchored scheduling): a declared `materialization.schedule`
    * governs every persist source in the package, so it is valid only when
    * each source in the compiled build plan resolves to an explicit
    * `sharing=private`. A cron over any shared/unset source would let one
    * package dictate the refresh cadence of an artifact other packages share —
    * those packages declare `materialization.freshness.window` instead and the
    * control plane schedules the shared artifact from the tightest window.
    * One actionable message per offending source (empty when the cron is
    * valid or no cron is declared). Strict at publish (package.controller),
    * warn-only at load/reload (loadViaWorker) — same split as explores.
    */
   public scheduleWarnings(): string[] {
      const schedule = this.packageMetadata.materialization?.schedule;
      if (!schedule) return [];
      const sources = Object.values(this.buildPlan?.sources ?? {});
      return sources
         .filter((source) => source.sharing !== "private")
         .map(
            (source) =>
               `materialization.schedule (cron) in ${PACKAGE_MANIFEST_NAME} requires every ` +
               `persist source to declare '#@ persist ... sharing=private'; source ` +
               `'${source.name}' resolves to ${
                  source.sharing ? `'${source.sharing}'` : "unset"
               }. Declare 'materialization.freshness.window' instead (the control plane ` +
               `schedules refreshes from it), or mark every persist source sharing=private.`,
         );
   }

   /** The {@link scheduleWarnings} joined into one string, or "" if none. */
   public formatInvalidSchedule(): string {
      return this.scheduleWarnings().join("\n");
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
   public async reloadAllModels(
      buildManifest: BuildManifest["entries"],
   ): Promise<void> {
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
                  renderTagWarnings.push({ model: sm.modelPath, ...w });
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
      this.renderTagWarnings = renderTagWarnings;
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
      // Remember what we just bound so /compile can route identically and
      // /status can report the binding. An empty map reverts to live (unbound).
      this.recordManifestBinding(buildManifest);
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
               return base.lookupConnection(name);
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
         databasePaths.map(async (databasePath) => ({
            path: databasePath,
            info: await Package.getDatabaseInfo(
               packagePath,
               databasePath,
               conn,
            ),
            type: "embedded" as const,
         })),
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
         .filter(
            (modelPath: string) =>
               modelPath.endsWith(".parquet") || modelPath.endsWith(".csv"),
         );
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
