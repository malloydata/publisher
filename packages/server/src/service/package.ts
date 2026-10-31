// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs/promises";
import * as path from "path";
import { pathToFileURL } from "url";

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
   type ModelDef,
} from "@malloydata/malloy";
import { publisherMeter } from "../telemetry";
import recursive from "recursive-readdir";
import { components } from "../api";
import {
   getPackageLoadPool,
   type LoadPackageOutcome,
} from "../package_load/package_load_pool";
import type { LoadPackageRequest } from "../package_load/protocol";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   type PackageRetrievalSettings,
} from "./package_retrieval";
import {
   API_PREFIX,
   INDEX_MODEL_NAME,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
} from "../constants";
import {
   BadRequestError,
   ModelCompilationError,
   PackageManifestError,
   PackageNotFoundError,
   ServiceUnavailableError,
} from "../errors";
import { applyExtensionSessionSettings } from "./connection";
import { formatDuration, logger } from "../logger";
import {
   recordReportDiscovery,
   recordReportDiscoveryDuration,
   summarizeReportDiscovery,
   UNCODED_REPORT_FINDING,
   type ReportDiscoveryDuring,
   type ReportFileStatus,
} from "../report_metrics";
import {
   recordBuildPlanComputeDuration,
   recordConnectionDigestSkipped,
   recordEligibilityRefused,
   type EligibilityRefusalReason,
   recordBuildPlanComputeFailed,
   recordColocatedBindDropped,
   recordManifestBindDegraded,
} from "../materialization_metrics";
import {
   LOAD_DURATION_BUCKETS_MS,
   recordPackageLoadPhaseDurations,
   recordPackageLoadPhases,
   type PackageLoadStatus,
} from "../package_load_metrics";
import { assertSafeEnvironmentPath, safeJoinUnderRoot } from "../path_safety";
import { type SkillEntry } from "../mcp/skills/build_skills_bundle";
import {
   PACKAGE_SKILLS_DIR,
   readPackageAgents,
   readSkillsDir,
   summarizeAgentSkillFile,
   type PackageAgent,
} from "../mcp/skills/package_skills";
import {
   canonicalJson,
   computeSourceContentSha,
   mintServedRevision,
} from "./package_revision";
import {
   BuildManifest,
   BuildPlan,
   FreshnessManifest,
   ManifestEntry,
} from "../storage/DatabaseInterface";
import { errMessage, ignoreDotfiles } from "../utils";
import { getPersistCollisionEnforce, getPersistStorageMode } from "../config";
import {
   deriveServeBindings,
   groupAliasesByName,
} from "./materialization_serve_transform";
import type {
   PreaggregateViolation,
   PreaggregateViolationCode,
} from "./preaggregation_validation";

/**
 * Pre-aggregation violations that warn at both gates rather than refusing: a
 * field the source does not publicly expose. See
 * {@link Package.formatInvalidPreaggregatePolicy}.
 */
const PREAGG_WARN_ONLY: ReadonlySet<PreaggregateViolationCode> = new Set([
   "non_public_measure",
   "grain_dimension_not_public",
]);
import { ColocatedSourceEligibility, SourceEligibility } from "./build_plan";
import { computePackageBuildPlan } from "./build_plan_compile";
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
import { createEventLoopYielder } from "./event_loop_yield";
import { CELLS_UNREADABLE } from "./notebook";
import {
   deprecatedFormatWarnings,
   recordDeprecatedFormatUsage,
} from "./deprecated/lints";
import { ambiguousDashboardNameWarnings } from "./legacy_report_views";
import { recordDeprecatedManifestKeys } from "./deprecated/manifest_keys";
import { isMalloynbPath } from "./deprecated/malloynb";
import {
   EXPLORES_IMPORTER,
   exploresSurfaceFiles,
   exploresSurfaceFix,
   reportListedInExploresMessage,
} from "./deprecated/explores";
import { lintNotebookText, reportedByDashboardLint } from "./notebook_lint";
import {
   buildReportManifest,
   dropGridTileLayoutFindings,
   factsReportTag,
   matchesDocumentedReportName,
   lintReport,
   lintDrillTargets,
   lintGivenTags,
   lintSelfDrills,
   lintUndiscoveredReport,
   queryTiles,
   tileTarget,
   type ServedReportManifest,
   type ReportModelFacts,
} from "./report_manifest";
import {
   isReportEligiblePath,
   isRootIndexPath,
   readReportTagOfText,
   reportName,
   type ReportLayout,
   type ReportTag,
} from "./report";
import { filterFreshManifest } from "./freshness";
import { sqlLiteral } from "./introspection_sql";
import { isQuotedIdentifierPath, quoteManifestTablePath } from "./quoting";
import { Model, type OffSurfaceContext } from "./model";
import { assertPersistNamesQuoted } from "./persist_annotation_validation";
import {
   applyLoadGates,
   assertOutcomeStagesAvailable,
   loadOutcomeOrThrow,
   warnInvalidExplores,
} from "./package_load_gates";

type ApiDatabase = components["schemas"]["Database"];
type ApiModel = components["schemas"]["Model"];
type ApiReport = components["schemas"]["Report"];
type ApiReportManifest = components["schemas"]["ReportManifest"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];

/** One report discovery found and serves, keyed by its path. */
interface ServedReport {
   /** Package-relative path: the report's identity. */
   path: string;
   /** The basename, a display label. */
   name: string;
   layout: ReportLayout;
   /** Tiles, or name and layout plus `error` if it failed to compile; absent on a `.malloynb`. */
   manifest?: ServedReportManifest & { error?: string };
   /** A `.malloy` report's compiled text ("" when unreadable); absent on a `.malloynb`. */
   text?: string;
}
/**
 * What discovery found one file to be, for a file that carries an artifact
 * tag (`docs/reports-design.md` §1):
 *
 * - `served`: a report the package serves (`report`).
 * - `tileless`: a grid report whose `tiles=` is missing or empty, not served
 *   until it has one.
 * - `invalid`: a model-level tag that makes no report: it does not parse, or
 *   it sets the removed `kind=`.
 * - `dropped`: a report that could not be read.
 *
 * Every status but `served` is explained by the lint. A `.malloy` of any
 * status is still the builder's to save (see {@link Package.isWritableReport}).
 * The type is {@link ReportFileStatus}, which the discovery metrics label by.
 */

interface ReportFile {
   status: ReportFileStatus;
   /** The basename, a display label. */
   name: string;
   /** The compiled facts, when the file has them; what the lint reads. */
   facts?: ReportModelFacts;
   /** Set when `status` is `served`. */
   report?: ServedReport;
}

/** What one discovery pass found; kept so the lint can re-run without re-discovering. */
interface ReportDiscovery {
   /** Every file that carries an artifact tag, and every `.malloynb`, by path. */
   files: Map<string, ReportFile>;
   /** Every compiled model's facts, for the package-wide drill and given lints. */
   allFacts: Map<string, ReportModelFacts>;
   /** Root `index.malloy` files (any letter case) whose artifact tag is ignored. */
   taggedIndexPaths: string[];
   /** Linear reports whose cells the reader refused. */
   readerWarnings: ApiPackageWarning[];
}

export type ApiPackage = components["schemas"]["Package"];
type ApiPackageWarning = NonNullable<ApiPackage["warnings"]>[number];
type ApiAgent = components["schemas"]["Agent"];
type ApiAgentSummary = components["schemas"]["AgentSummary"];
type ApiAgentSource = components["schemas"]["AgentSource"];
type ApiAgentSkillRead = components["schemas"]["AgentSkillRead"];

type ApiColumn = components["schemas"]["Column"];
type ApiTableDescription = components["schemas"]["TableDescription"];
// A thunk resolves environment connections against the config generation
// active at lookup time, not the one current when the package loaded.
type PackageConnectionInput =
   | MalloyConfig
   | Map<string, Connection>
   | (() => MalloyConfig);

/**
 * Project wire entries to the `sourceEntityId -> { tableName }` map that
 * hydrates models. Freshness fields gate the serve path per query, not hydration.
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
 * The failure `status` label shared by the load-duration histogram and the
 * per-phase load metrics, so both slice failures identically.
 */
function packageLoadFailureStatus(error: unknown): PackageLoadStatus {
   if (error instanceof ModelCompilationError || error instanceof MalloyError) {
      return "compilation_error";
   }
   if (
      error instanceof BadRequestError ||
      error instanceof PackageManifestError
   ) {
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
   /** The environment's storage destinations for a serve-shape compile; see {@link setServeDestinationConfig}. */
   private serveDestinationConfig?: () => MalloyConfig;
   /** A destination's schema by name, resolved live; see {@link setServeDestinationSchema}. */
   private serveDestinationSchema?: (
      destinationName: string,
   ) => string | undefined;
   // Bound build manifest (sourceEntityId -> { tableName }): routes persist
   // sources to their materialized tables for served queries and the /compile
   // preview alike, so previewed SQL matches executed SQL. Surfaced on /status
   // so the control plane can confirm a worker bound it.
   private buildManifestEntries: BuildManifest["entries"] | undefined;
   // The full wire entries, freshness fields included, so the serve path can
   // re-evaluate age vs window per query. Undefined when serving live.
   private freshnessEntries: FreshnessManifest | undefined;
   // `storage=` serve bindings, pushed onto each Model for the virtual-source
   // serve transform. A storage source serves cross-connection through these,
   // not through the same-connection manifest above.
   private storageServeBindings: ReturnType<typeof deriveServeBindings> = [];
   // Memoized freshness-filtered manifest, recomputed when an entry crosses its
   // window (`validUntil`) or a rebind clears it. Infinity: stable until rebind.
   private freshManifestCache:
      | { manifest: BuildManifest["entries"]; validUntil: number }
      | undefined;
   private manifestBindingStatus: "unbound" | "bound" | "live_fallback" =
      "unbound";
   private manifestEntryCount = 0;
   private boundManifestUri: string | null = null;
   // The persist build plan, computed once at load from the unbound models, so
   // it is stable per (package version, connection config). Null when the
   // package declares no persist source.
   private buildPlan: BuildPlan | null = null;
   // Keyed on the plan it came from, so a reload recomputes it.
   private preaggregateEntityIdCache:
      | { plan: BuildPlan | null; ids: ReadonlySet<string> }
      | undefined;
   // `#@ persist` sources Malloy's build walk did not plan, which
   // would silently serve live. Warned, and refused at build.
   private droppedPersistSources: { name: string; modelPath: string }[] = [];
   // Which sources may be served from a materialized table, decided at compile
   // where the compiled sources exist; binding does not recompile. `undefined`
   // means NOT KNOWN (the plan compute failed) and is refused like "nothing is
   // eligible": an unknown answer must not read as consent.
   private sourceEligibility: SourceEligibility | undefined = undefined;
   // The colocated (same-connection) analogue, keyed by sourceEntityId. Same
   // "undefined means refuse" rule.
   private colocatedSourceEligibility: ColocatedSourceEligibility | undefined =
      undefined;
   // Incremental-refresh gate inputs, resolved at compile because the rules read
   // the compiled schema, which the wire plan lacks. Empty when the plan compute
   // failed; safe, as the build only dispatches a delta it can read.
   private incrementalPolicySources: IncrementalPolicySource[] = [];
   // Non-fatal render-tag findings across the models; a bad tag does not fail
   // the load, so this is its signal.
   private renderTagWarnings: ApiPackageWarning[] = [];
   private reportWarnings: ApiPackageWarning[] = [];
   /** Linear reports whose cells the reader refused; set with the reports. */
   private readerWarnings: ApiPackageWarning[] = [];
   // Every served report keyed by path, in path order, computed per load (the
   // tag only changes with the models). A grid report that failed to compile
   // stays, carrying its error. Its keys define what is a report for the query
   // boundary; each `text` lets a report's derived sources prove their origin.
   private reports = new Map<string, ServedReport>();
   /** Every file discovery found carrying an artifact tag, with its status. */
   private reportFiles = new Map<string, ReportFile>();
   /** The last discovery, which the lint re-runs over. */
   private discovery?: ReportDiscovery;
   /** Manifest deprecations the load tolerated, reported at publish and in `warnings`. */
   private manifestWarnings: string[] = [];
   /**
    * What this package is serving, refreshed on every load. `servedRevision`
    * identifies the load; `sourceContentSha` says whether the served bytes changed.
    */
   private servedRevision: string = mintServedRevision();
   private sourceContentSha: string = "";
   /**
    * The skills in `skills/`, refreshed with the content sha so a skill edit is
    * covered by the reload receipt by construction.
    */
   private packageSkills: SkillEntry[] = [];
   private packageSkillWarnings: string[] = [];
   /**
    * The manifest's raw `agents` value, held apart from `packageMetadata` so a
    * metadata PATCH cannot replace it.
    */
   private rawAgents: unknown;
   private packageAgents = new Map<string, PackageAgent>();
   private packageAgentWarnings: string[] = [];
   /** The manifest's `retrieval` block (prompt files included), re-read on reload. */
   private retrievalSettings: PackageRetrievalSettings =
      DEFAULT_PACKAGE_RETRIEVAL;
   private static meter = publisherMeter();
   private static packageLoadHistogram = this.meter.createHistogram(
      "malloy_package_load_duration",
      {
         description: "Time taken to load a Malloy package",
         unit: "ms",
         // OTel's default buckets stop at 10s and would censor the slow tail.
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
      this.applySiblingModelResolverToModels();
   }

   public getServedRevision(): string {
      return this.servedRevision;
   }

   public getSourceContentSha(): string {
      return this.sourceContentSha;
   }

   /** The agent skills this package ships, in on-disk order. */
   public listSkills(): SkillEntry[] {
      return this.packageSkills;
   }

   /** The agents the manifest declares that passed validation, without their text. */
   public listAgents(): ApiAgentSummary[] {
      return [...this.packageAgents.values()].map((a) => ({
         name: a.name,
         description: a.description,
         model: a.model,
         schedules: a.schedules.map(({ cron, task }) => ({ cron, task })),
      }));
   }

   private agentSource(definitionSha: string): ApiAgentSource {
      const versionId = this.getVersionId();
      return {
         environment: this.environmentName,
         package: this.packageName,
         // A published version's agent can be fetched again by it after
         // `latest` moves; absent for a package with no versions.
         ...(versionId ? { versionId } : {}),
         sourceContentSha: this.sourceContentSha,
         definitionSha,
         servedRevision: this.servedRevision,
      };
   }

   /**
    * One agent resolved, pinned to the load and content it was read from.
    * `summary` swaps each skill's text for its name, description and size; the
    * pin is the same because both are views of one resolved definition.
    */
   public getAgent(
      name: string,
      skills: "full" | "summary" = "full",
   ): ApiAgent | undefined {
      const agent = this.packageAgents.get(name);
      if (!agent) return undefined;
      const { definitionSha, skillMeta, ...definition } = agent;
      return {
         ...definition,
         skills:
            skills === "full"
               ? definition.skills
               : definition.skills.map((f) =>
                    summarizeAgentSkillFile(f, skillMeta),
                 ),
         source: this.agentSource(definitionSha),
      };
   }

   /** One agent skill file, looked up in the resolved set and never joined onto the filesystem. */
   public getAgentSkillFile(
      name: string,
      relativeFilepath: string,
   ): ApiAgentSkillRead | undefined {
      const agent = this.packageAgents.get(name);
      const file = agent?.skills.find(
         (f) => f.relative_filepath === relativeFilepath,
      );
      if (!agent || !file) return undefined;
      return { ...file, source: this.agentSource(agent.definitionSha) };
   }

   /**
    * Re-derive the serving identity and re-read the package's skills and
    * agents, as one operation so the set that is served and the set that is
    * hashed are the same (a skill edit must move the reload receipt).
    *
    * Not run by the constructor: {@link lintWorkerOutcome}'s throwaway Package
    * has no use for it. Call after anything that changes the served files.
    */
   public refreshServingIdentity(): void {
      this.servedRevision = mintServedRevision();
      const skills = readSkillsDir(this.packagePath, PACKAGE_SKILLS_DIR);
      this.packageSkills = skills.skills;
      this.packageSkillWarnings = skills.warnings;
      const agents = readPackageAgents(this.packagePath, this.rawAgents);
      this.packageAgents = agents.agents;
      this.packageAgentWarnings = agents.warnings;
      let declaration: string | undefined;
      try {
         declaration =
            this.rawAgents === undefined
               ? undefined
               : canonicalJson(this.rawAgents);
      } catch {
         // Reload must not stop between the model swap and the serve bindings.
         declaration = "[unhashable]";
      }
      this.sourceContentSha = computeSourceContentSha(
         this.packagePath,
         [
            ...this.models.keys(),
            ...skills.files.map((f) => f.path),
            ...agents.paths,
         ],
         // The declaration is hashed whole, so an edit to a dropped agent moves it too.
         declaration,
      );
   }

   /**
    * True when the package has a non-empty resolved surface, written or derived
    * from `index.malloy` (so "no `explores` key" does not mean "uncurated").
    * The one test curation, boundary and listing all share.
    */
   private exploresDeclared(): boolean {
      const explores = this.packageMetadata.explores;
      return !!(explores && explores.length > 0);
   }

   /**
    * The "surface widened" notice, set on the reload that lost this package's
    * surface. See {@link noteSurfaceChangeFrom}.
    */
   private surfaceWidenedWarning: string | undefined;

   /**
    * Warn that this package was curated before the reload and is not now.
    * Losing a surface (deleting or renaming `index.malloy`) is the one curation
    * change nothing else reports: the withheld sources become listed and
    * queryable by name again. (Curation only; `#(authorize)` is unaffected.)
    *
    * `previousSurface` is the resolved `explores` from before the reload. Only
    * a now-`undefined` surface counts: `"explores": []` was asked for and is
    * already warned about. Said once, on the reload that caused it.
    */
   public noteSurfaceChangeFrom(previousSurface: string[] | undefined): void {
      this.surfaceWidenedWarning = undefined;
      if (
         !previousSurface ||
         previousSurface.length === 0 ||
         this.packageMetadata.explores !== undefined
      ) {
         return;
      }
      const message =
         `This package published "${previousSurface.join('", "')}" before the ` +
         `last reload and publishes no surface now, so every model in it is ` +
         `listed and queryable by name again. Fix: if that was not intended, ` +
         `restore ${INDEX_MODEL_NAME} under that exact name.`;
      this.surfaceWidenedWarning = message;
      logger.warn(`Package ${this.packageName} no longer publishes a surface`, {
         packageName: this.packageName,
         detail: message,
      });
   }

   /**
    * True when this package's surface is its `index.malloy`, including a
    * hand-written `explores: ["index.malloy"]` (nothing records a surface's
    * origin), so the remedies it selects are worded for both. Used ONLY to
    * word warnings; it must never gate behavior.
    */
   private surfaceIsIndexModel(): boolean {
      const explores = this.packageMetadata.explores;
      return (
         this.models.has(INDEX_MODEL_NAME) &&
         explores?.length === 1 &&
         explores[0] === INDEX_MODEL_NAME
      );
   }

   /** The declared explore set, or null when discovery is uncurated. */
   private exploreSet(): Set<string> | null {
      const explores = this.packageMetadata.explores;
      return explores && explores.length > 0 ? new Set(explores) : null;
   }

   /** Push discovery curation (`explores` listing, `export {}` filtering) onto each Model. */
   private applyDiscoveryPolicyToModels(): void {
      const curationEnabled = this.exploresDeclared();
      for (const model of this.models.values()) {
         model.setDiscoveryCuration(curationEnabled);
      }
   }

   /**
    * Push the query-boundary policy onto each Model, where
    * `Model.getQueryResults` enforces it (`/compile` is exempt; only
    * `#(authorize)` gates it). Policy: queryable == discoverable. Inert unless
    * the package resolved a surface AND `queryableSources` is "declared" (the
    * default). When active, a model file is an entry point only if it is on
    * the surface.
    */
   private applyQueryBoundaryToModels(): void {
      const exploresDeclared = this.exploresDeclared();
      const exploreSet = this.exploreSet();
      const mode =
         this.packageMetadata.queryableSources === "all" ? "all" : "declared";
      // The PACKAGE-wide surface: the union of every listed model's export
      // closure, so a queryable source stays queryable through a model that
      // imports it (agents often post every query to one model path).
      //
      // Keyed by name to DEFINITION IDENTITIES, never bare names: by name alone,
      // listed A exporting `customers` would admit a different, hidden
      // `customers` that model B resolves. Identity admits only the very
      // declaration a listed model exported.
      // (Requires applyDiscoveryPolicyToModels first, for export curation.)
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
            // Only .malloy files curate a surface; a listed notebook is invalid
            // and contributes nothing (as in listModels).
            if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
            if (!exploreSet.has(modelPath)) continue;
            // Nor does a report, even when listed: it reads the surface, it
            // never adds to it.
            if (this.isServedReport(modelPath)) continue;
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
      const offSurface: OffSurfaceContext | undefined =
         mode === "declared" && exploresDeclared && exploreSet
            ? {
                 indexModel: this.surfaceIsIndexModel(),
                 files: [...exploreSet].sort(),
              }
            : undefined;
      for (const [modelPath, model] of this.models) {
         // A `.malloy` report only: a `.malloynb` is a notebook by its format.
         const report = modelPath.endsWith(MODEL_FILE_SUFFIX)
            ? this.reports.get(modelPath)
            : undefined;
         model.setQueryBoundary({
            mode,
            exploresDeclared,
            // A discovered report is always an entry point: it is always
            // listed, and the model marks it as admitting nothing of its own.
            isQueryEntryPoint: exploreSet
               ? exploreSet.has(modelPath) || report !== undefined
               : true,
            packageCuratedSources,
            packageCuratedQueries,
            offSurface,
            // The model serves a grid report's tiles and a linear report's
            // cells; both read the surface and add nothing to it.
            layout: report?.layout,
            derivationText: report?.text,
         });
      }
   }

   /**
    * Give each Model a lookup from a `file://` URL to a sibling model's own
    * `ModelDef` (see `SiblingModelDefResolver` in `./filter_binding_guard` for
    * why). Rebuilt whenever `this.models` is replaced, so it never resolves
    * against a stale set.
    */
   private applySiblingModelResolverToModels(): void {
      const byUrl = new Map<string, Model>();
      for (const [modelPath, model] of this.models) {
         byUrl.set(
            pathToFileURL(path.join(this.packagePath, modelPath)).toString(),
            model,
         );
      }
      const resolver = (url: string): ModelDef | undefined =>
         byUrl.get(url)?.getModelDef();
      for (const model of this.models.values()) {
         model.setSiblingModelDefResolver(resolver);
      }
   }

   static async create(
      environmentName: string,
      packageName: string,
      packagePath: string,
      environmentMalloyConfig: PackageConnectionInput,
      /**
       * Delete `packagePath` if the load fails. Only for a caller that created
       * the directory (a staged install); for any existing directory, deleting
       * on a transient compile error would destroy the source.
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
         // Built on the main thread: it owns the native connection handles
         // that serve queries, which cannot cross the isolate boundary. The
         // worker proxies non-duckdb lookups back through it during compile.
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
         const endTime = performance.now();
         const executionTime = endTime - startTime;
         this.packageLoadHistogram.record(executionTime, {
            malloy_package_name: packageName,
            status: packageLoadFailureStatus(error),
         });
         // Clean up only when the caller opted in, and never a watch-mode
         // symlink: removing it would unmount the package on a half-typed save
         // until a restart.
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

   /** The package's API metadata as the worker read it from publisher.json. */
   private static packageConfigFromOutcome(
      environmentName: string,
      packageName: string,
      outcome: LoadPackageOutcome,
   ): ApiPackage {
      return {
         name: outcome.packageMetadata.name,
         description: outcome.packageMetadata.description,
         location: outcome.packageMetadata.location,
         resource: `${API_PREFIX}/environments/${environmentName}/packages/${packageName}`,
         explores: outcome.packageMetadata.explores,
         queryableSources: outcome.packageMetadata.queryableSources,
         manifestLocation: outcome.packageMetadata.manifestLocation ?? null,
         // Never null on a loaded package: the control plane reads an absent
         // object as "metadata unavailable" and a present one as authoritative,
         // so null could be misread as a schedule removal.
         materialization: outcome.packageMetadata.materialization ?? {
            schedule: null,
            freshness: null,
         },
         // The canonical home for the tags, mirrored in `materialization` for
         // as long as that deprecated home is supported.
         queryMetadata:
            outcome.packageMetadata.materialization?.queryMetadata ?? null,
         scope: outcome.packageMetadata.scope ?? "package",
      };
   }

   /**
    * Turn the worker's compiled models into live `Model`s and validate their
    * render tags (here, because the renderer is too heavy for the worker). A
    * bad tag never fails the load; its findings become non-fatal `warnings`.
    * Shared by load, reload and package-scope compile so their findings cannot
    * drift. Touches no package state.
    *
    * `onCompileError`: a load ("throw") fails on the first failed model; a
    * reload ("placeholder") keeps it as `Model.fromCompilationError` so one bad
    * file does not cost the others. `afterHydrate` runs per model before it is
    * added. `ctx.compile` skips the per-model failure log, since the compile
    * response reports those and an iterating agent would flood the log.
    */
   private static async hydrateWorkerModels(
      outcome: LoadPackageOutcome,
      ctx: {
         packageName: string;
         packagePath: string;
         malloyConfig: MalloyConfig;
         buildManifest?: BuildManifest["entries"];
         compile?: boolean;
         /** The what-if text, so a failed model's placeholder carries the edit, not the saved file. */
         replacement?: LoadPackageRequest["replacement"];
      },
      onCompileError: "throw" | "placeholder",
      afterHydrate?: (
         sm: LoadPackageOutcome["models"][number],
      ) => Promise<void>,
   ): Promise<{
      models: Map<string, Model>;
      renderTagWarnings: ApiPackageWarning[];
      /** Wall-clock ms in hydration and in render-tag validation, summed over models. */
      timings: { hydrateMs: number; renderValidationMs: number };
   }> {
      const { packageName, packagePath, malloyConfig, buildManifest } = ctx;
      const models = new Map<string, Model>();
      const renderTagWarnings: ApiPackageWarning[] = [];
      const timings = { hydrateMs: 0, renderValidationMs: 0 };
      const placeholder = (
         sm: LoadPackageOutcome["models"][number],
         err: Error,
      ): Model =>
         Model.fromCompilationError(
            packageName,
            sm.modelPath,
            sm.modelType,
            err,
            sm.modelPath === ctx.replacement?.modelPath
               ? ctx.replacement.source
               : undefined,
         );
      // Hydration and validation run on the main thread, one model after
      // another; let other requests in between models.
      const yielder = createEventLoopYielder();
      for (const sm of outcome.models) {
         await yielder.maybeYield();
         if (sm.compilationError) {
            const err = Model.deserializeCompilationError(sm.compilationError);
            if (onCompileError === "throw") {
               logger.error("Model compilation failed", {
                  packageName,
                  modelPath: sm.modelPath,
                  error: err.message,
               });
               // Package.create's catch records the metric and cleans up.
               throw err;
            }
            if (!ctx.compile) {
               logger.warn("Model compilation failed during reload", {
                  packageName,
                  modelPath: sm.modelPath,
                  error: err.message,
               });
            }
            models.set(sm.modelPath, placeholder(sm, err));
            continue;
         }
         let model: Model;
         const hydrateStart = performance.now();
         try {
            model = Model.fromSerialized(
               packageName,
               packagePath,
               malloyConfig,
               sm,
               {
                  buildManifest,
                  skipAuthorizeWarningLog: ctx.compile,
               },
            );
         } catch (hydrateErr) {
            // A load and reload fail here. A compile keeps going so one model
            // costs only its own findings, reported as an error because the
            // reload it previews would fail.
            if (!ctx.compile) throw hydrateErr;
            logger.warn("Model hydration failed during compile", {
               packageName,
               modelPath: sm.modelPath,
               error: errMessage(hydrateErr),
            });
            renderTagWarnings.push({
               model: sm.modelPath,
               message:
                  `The load-time checks could not read this model ` +
                  `(${errMessage(hydrateErr)}), so its render-tag and dashboard findings ` +
                  `are unknown rather than clean.`,
               severity: "error",
            });
            models.set(
               sm.modelPath,
               placeholder(
                  sm,
                  hydrateErr instanceof Error
                     ? hydrateErr
                     : new Error(String(hydrateErr)),
               ),
            );
            continue;
         } finally {
            timings.hydrateMs += performance.now() - hydrateStart;
         }
         let warnings: Awaited<ReturnType<Model["validateRenderTags"]>>;
         const validationStart = performance.now();
         try {
            warnings = await model.validateRenderTags();
         } catch (renderErr) {
            // Defensive: validateRenderTags should not throw. On reload, keep a
            // placeholder rather than abort.
            if (onCompileError === "throw") throw renderErr;
            const err =
               renderErr instanceof Error
                  ? renderErr
                  : new Error(String(renderErr));
            logger.warn(
               `Render-tag validation failed during ${ctx.compile ? "compile" : "reload"}`,
               {
                  packageName,
                  modelPath: sm.modelPath,
                  error: err.message,
               },
            );
            if (ctx.compile) {
               renderTagWarnings.push({
                  model: sm.modelPath,
                  message:
                     `The render-tag check did not finish for this model, so ` +
                     `its render-tag findings are unknown rather than clean.`,
                  severity: "warn",
               });
            }
            models.set(sm.modelPath, placeholder(sm, err));
            continue;
         } finally {
            timings.renderValidationMs += performance.now() - validationStart;
         }
         for (const w of warnings) {
            renderTagWarnings.push({
               model: sm.modelPath,
               // Spelled out, not spread: a spread escapes excess-property
               // checks, so an api-doc.yaml rename would go unnoticed.
               subject: w.subject,
               message: w.message,
               severity: w.severity,
            });
         }
         const afterHydrateStart = performance.now();
         await afterHydrate?.(sm);
         timings.hydrateMs += performance.now() - afterHydrateStart;
         models.set(sm.modelPath, model);
      }
      return { models, renderTagWarnings, timings };
   }

   /**
    * The render-tag and report findings a reload of this outcome would put on
    * `warnings`, for the package-scope compile. Runs the reload's own steps on
    * a throwaway Package, so a what-if never touches the served one.
    *
    * Not covered: notebook findings (the compile reports those), and warnings
    * that need the build plan or a bound manifest, which only a real load has.
    */
   public static async lintWorkerOutcome(
      environmentName: string,
      packageName: string,
      packagePath: string,
      malloyConfig: MalloyConfig,
      outcome: LoadPackageOutcome,
      buildManifest?: BuildManifest["entries"],
      replacement?: LoadPackageRequest["replacement"],
   ): Promise<{
      renderTagWarnings: ApiPackageWarning[];
      reportWarnings: ApiPackageWarning[];
   }> {
      const { models, renderTagWarnings } = await Package.hydrateWorkerModels(
         outcome,
         {
            packageName,
            packagePath,
            malloyConfig,
            buildManifest,
            compile: true,
            replacement,
         },
         "placeholder",
      );
      const pkg = new Package(
         environmentName,
         packageName,
         packagePath,
         Package.packageConfigFromOutcome(
            environmentName,
            packageName,
            outcome,
         ),
         [],
         models,
         malloyConfig,
      );
      try {
         await pkg.discoverReports("compile");
      } catch (err) {
         // A missing list would read as clean; say the findings are unknown.
         logger.warn("Report lint failed during compile", {
            packageName,
            error: errMessage(err),
         });
         return {
            renderTagWarnings: pkg.surfacedRenderTagWarnings(renderTagWarnings),
            reportWarnings: [
               {
                  message:
                     "The report checks did not run, so their findings " +
                     "are unknown rather than clean. Reload the package to " +
                     'see them. The cause is in the server log under "Report ' +
                     'lint failed during compile".',
                  severity: "warn",
               },
            ],
         };
      }
      return {
         renderTagWarnings: pkg.surfacedRenderTagWarnings(renderTagWarnings),
         reportWarnings: pkg.reportWarnings,
      };
   }

   /**
    * Load the package via the worker pool, which does the CPU-bound manifest
    * read and compiles. Database probes are IO-bound and run on the main thread
    * in parallel. Pool failures become 503s; compile errors keep their 4xx.
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
      // Wrapped on its own so the 503 mapping never touches readDatabases' errors.
      const workerOutcome = loadOutcomeOrThrow(
         pool.loadPackage({
            packagePath,
            packageName,
            environmentName,
            malloyConfig,
            defaultConnectionName: "duckdb",
            computeBuildPlan: true,
            withPreaggregateCompanions: true,
         }),
      );
      const [outcome, databases] = await Promise.all([
         workerOutcome,
         Package.readDatabases(packagePath, malloyConfig),
      ]);
      assertOutcomeStagesAvailable(outcome);
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
         schemaCacheHits: outcome.timings.schemaCacheHits,
         modelCount: outcome.models.length,
         databaseCount: databases.length,
      });

      const packageConfig = Package.packageConfigFromOutcome(
         environmentName,
         packageName,
         outcome,
      );

      // Any model compile failure aborts the load (reload keeps placeholders).
      let models: Map<string, Model>;
      let renderTagWarnings: ApiPackageWarning[];
      let hydrationTimings: { hydrateMs: number; renderValidationMs: number };
      try {
         ({
            models,
            renderTagWarnings,
            timings: hydrationTimings,
         } = await Package.hydrateWorkerModels(
            outcome,
            { packageName, packagePath, malloyConfig },
            "throw",
            async (sm) => {
               // Reject an unquoted `#@ persist name=`: it is dropped from the
               // build plan, so the source would never materialize. Reads the
               // raw source, the ground truth for quoting. `.malloy` only,
               // because `#@ persist` cannot appear in a `.malloynb`; widen this
               // if that changes, to keep the identifier-injection check.
               if (sm.modelPath.endsWith(MODEL_FILE_SUFFIX)) {
                  const modelSource = await fs.readFile(
                     path.join(packagePath, sm.modelPath),
                     "utf-8",
                  );
                  assertPersistNamesQuoted(modelSource, sm.modelPath);
               }
            },
         ));
      } catch (err) {
         // Only in-band compile failures reach here; a pool failure has no
         // timings to record.
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
      const reportTimings = await pkg.discoverReports("load");
      // After discovery: which findings a tile explains depends on the reports.
      pkg.renderTagWarnings = pkg.surfacedRenderTagWarnings(renderTagWarnings);
      pkg.manifestWarnings = outcome.packageMetadata.manifestWarnings ?? [];
      pkg.recordDeprecatedUsage(outcome);
      pkg.retrievalSettings =
         outcome.packageMetadata.retrieval ?? DEFAULT_PACKAGE_RETRIEVAL;
      pkg.rawAgents = outcome.packageMetadata.agents;
      // No manifest is bound yet, so this serves live until a bind.
      pkg.wireFreshnessResolvers();
      pkg.refreshServingIdentity();

      const buildPlanMs = await pkg.applyLoadBuildPlan(outcome.buildPlan);

      applyLoadGates(pkg);
      // After the gates, so a bad declaration is rejected before a companion is
      // compiled. Unbound, the companion recomputes from the base until a bind.
      await pkg.pushPreaggregateServeModels();
      pkg.logEmptyDiscoveryWarnings();

      recordPackageLoadPhaseDurations({
         worker: workerDoneTime - dispatchTime,
         hydrate: hydrationTimings.hydrateMs,
         render_validation: hydrationTimings.renderValidationMs,
         discovery: reportTimings.discoveryMs,
         lint: reportTimings.lintMs,
         build_plan: buildPlanMs,
         total: performance.now() - startTime,
      });
      return pkg;
   }

   /**
    * Apply the persist build plan, derived from the unbound models so it
    * reflects the canonical build. The worker normally supplies it; deriving
    * it here recompiles every model on this thread, so that happens only when
    * the worker left it at its soft deadline. Best-effort: a failure is logged
    * and the package serves without a plan.
    *
    * A failure leaves both eligibilities unknown, which refuses every storage
    * AND colocated serve binding until a load succeeds: serving live is slower,
    * an unexamined binding could be wrong.
    *
    * Returns how long it took, or undefined when it failed.
    */
   private async applyLoadBuildPlan(
      workerPlan: LoadPackageOutcome["buildPlan"],
   ): Promise<number | undefined> {
      const buildPlanPhaseStart = performance.now();
      try {
         // Before a failed plan throws, matching the main-thread derivation,
         // which counts these as it goes.
         for (const [reason, count] of Object.entries(
            workerPlan?.eligibilityRefused ?? {},
         )) {
            for (let i = 0; i < (count ?? 0); i++) {
               recordEligibilityRefused(reason as EligibilityRefusalReason);
            }
         }
         for (const connectionName of workerPlan?.digestSkipped ?? []) {
            recordConnectionDigestSkipped();
            logger.warn(
               "Skipping connection digest; connection did not resolve",
               { connectionName },
            );
         }
         if (workerPlan && !workerPlan.ok) {
            throw new Error(workerPlan.error);
         }
         const {
            plan,
            droppedPersistSources,
            sourceEligibility,
            colocatedSourceEligibility,
            incrementalDeclarations,
         } = workerPlan
            ? workerPlan.outcome
            : await computePackageBuildPlan(this);
         this.buildPlan = plan;
         this.droppedPersistSources = droppedPersistSources;
         this.sourceEligibility = sourceEligibility;
         this.colocatedSourceEligibility = colocatedSourceEligibility;
         this.incrementalPolicySources = Object.entries(plan?.sources ?? {})
            .filter(([sourceID]) => incrementalDeclarations[sourceID])
            .map(([sourceID, source]) => ({
               sourceName: source.name,
               modelPath: source.modelPath,
               dialect: source.dialect,
               storageDestination: source.annotationFields?.storage,
               // So the gate can spot two sources resolving to one table.
               sourceEntityId: source.sourceEntityId,
               declaration: incrementalDeclarations[sourceID],
            }));
         recordBuildPlanComputeDuration(
            workerPlan
               ? workerPlan.durationMs
               : performance.now() - buildPlanPhaseStart,
            workerPlan ? "worker" : "main_thread",
         );
         return performance.now() - buildPlanPhaseStart;
      } catch (err) {
         recordBuildPlanComputeFailed();
         logger.warn(
            `Failed to compute build plan for package ${this.packageName}`,
            {
               packageName: this.packageName,
               error: errMessage(err),
            },
         );
      }
      return undefined;
   }

   /** The published version this package is, or undefined for a package with none. */
   public getVersionId(): string | undefined {
      return this.packageMetadata.versionId ?? undefined;
   }

   public getPackageName(): string {
      return this.packageName;
   }

   /** The persist build plan, or null when the package declares no persist source. */
   public getBuildPlan(): BuildPlan | null {
      return this.buildPlan;
   }

   /** The package-level `materialization` config, the least specific layer; null when none. */
   public getMaterializationConfig(): NonNullable<
      ApiPackage["materialization"]
   > | null {
      return this.packageMetadata.materialization ?? null;
   }

   /**
    * The package's declared `queryMetadata`, the least specific layer. Falls
    * back to the deprecated `materialization.queryMetadata`, which an
    * un-migrated PATCH can still set.
    */
   public getDeclaredQueryMetadata(): QueryMetadata | null {
      return (
         this.packageMetadata.queryMetadata ??
         this.packageMetadata.materialization?.queryMetadata ??
         null
      );
   }

   /**
    * Every `queryMetadata` bag declared at any level, so the publish gate can
    * name the line to edit. Persisting or not, a source bag rides every query.
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
                          // `modelPath`, not `subject`: clients filter by model.
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
                     // Source names are unique per model, not per package.
                     modelPath: model.getPath(),
                     queryMetadata,
                  })),
            ];
         }),
      ];
   }

   /**
    * How many properties a statement actually sends, per model file, once the
    * layers merge: each layer can fit the budget while the merge does not. The
    * floor (package ⊕ model file) is reported for every model, even one whose
    * sources declare nothing.
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

   /** How this package is searched and indexed (publisher.json `retrieval`). */
   public getRetrievalSettings(): PackageRetrievalSettings {
      return this.retrievalSettings;
   }

   /**
    * ON THE `/status` POLL PATH: runs for every package every few seconds.
    * Anything added below must be trivially cheap or memoized on the Model; an
    * unmemoized walk over model fields blocks the event loop for seconds.
    */
   public getPackageMetadata(): ApiPackage {
      // A copy with the server-computed fields overlaid. `name` is the
      // registered name, which can differ from the one in publisher.json.
      const metadata: ApiPackage = {
         ...this.packageMetadata,
         name: this.packageName,
         manifestBindingStatus: this.manifestBindingStatus,
         manifestEntryCount: this.manifestEntryCount,
         boundManifestUri: this.boundManifestUri,
         buildPlan: this.buildPlan,
         servedRevision: this.servedRevision,
         sourceContentSha: this.sourceContentSha,
         // Overlaid so a metadata PATCH body can neither set nor wipe it.
         agents: this.rawAgents === undefined ? undefined : this.listAgents(),
      };
      const warnings = this.exploreWarnings();
      if (warnings.length > 0) {
         metadata.exploresWarnings = warnings;
      }
      // Every advisory finding shares one warnings array. Each must ride the
      // API because the load tolerates it, so otherwise only the log says so.
      const allWarnings = [
         ...this.renderTagWarnings,
         ...this.readerWarnings,
         ...this.reportWarnings,
         ...this.packageSkillWarnings.map((message) => ({
            message,
            severity: "warn" as const,
         })),
         ...this.packageAgentWarnings.map((message) => ({
            message,
            severity: "warn" as const,
         })),
         ...this.storageWarnings(),
         ...this.droppedPersistWarnings(),
         ...this.persistRefusalWarnings(),
         // Not in exploresWarnings: an import-only file must not block publish.
         ...this.emptyDiscoveryWarnings(),
         ...deprecatedFormatWarnings(this.models),
         ...ambiguousDashboardNameWarnings(this),
         // The 404s a broken surface causes name models that are not broken.
         ...this.brokenSurfaceWarnings(),
         ...(this.surfaceWidenedWarning !== undefined
            ? [{ message: this.surfaceWidenedWarning }]
            : []),
         // A collision spans sources, so it has no subject; the message names them.
         ...this.persistenceCollisionWarnings().map((message) => ({ message })),
         ...this.preaggregateAccessWarnings(),
         // Legal but probably unintended, e.g. a typo'd merge_key=.
         ...incrementalPolicyAdvisories(this.incrementalPolicySources),
         ...materializationConfigWarnings({
            declarations: this.queryMetadataDeclarations(),
            effectiveMerges: this.queryMetadataEffectiveMerges(),
            manifestWarnings: this.manifestWarnings,
         }),
      ];
      if (allWarnings.length > 0) {
         metadata.warnings = allWarnings;
      }
      if (this.storageServeBindings.length > 0) {
         // `origin` marks rows (rollups) whose `sourceName` is in no model file.
         metadata.storageServeBindings = this.storageServeBindings.map((b) => ({
            origin: b.origin ?? "persist",
            sourceName: b.sourceName,
            storageDestinationName: b.destinationName,
            tablePath: b.tablePath,
         }));
      }
      return metadata;
   }

   /** The bound manifest entries, or undefined when serving live; reused by /compile. */
   public getBuildManifestEntries(): BuildManifest["entries"] | undefined {
      return this.buildManifestEntries;
   }

   /**
    * Whether a non-empty colocated manifest is bound. A pure-storage refresh may
    * skip the recompile only if nothing is substituted now or was before.
    */
   public hasBoundTableNameManifest(): boolean {
      return (
         this.buildManifestEntries !== undefined &&
         Object.keys(this.buildManifestEntries).length > 0
      );
   }

   /**
    * Whether `storage=` serve bindings exist, so a refresh whose storage entries
    * vanished still clears them.
    */
   public hasStorageServeBindings(): boolean {
      return this.storageServeBindings.length > 0;
   }

   /**
    * The freshness-filtered manifest at `now`: a table is used only within its
    * window, else per its `fallback` (`live`/`fail` drop it, `stale_ok` keeps
    * it); an entry with no window always routes. Undefined when unbound.
    * Memoized: fresh→stale is monotonic, so one `validUntil` deadline suffices.
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

   /** Install the per-query freshness resolvers on every model; idempotent. */
   private wireFreshnessResolvers(): void {
      for (const model of this.models.values()) {
         model.setFreshnessResolver(() => this.getFreshBuildManifest());
         model.setPreaggregateEntityIdResolver(() =>
            this.getPreaggregateEntityIds(),
         );
      }
   }

   /**
    * The `sourceEntityId`s pre-aggregation synthesized, read off the plan's
    * `origin` (never the rollup naming convention, which an author's source
    * could share). Memoized on plan identity; the plan is replaced wholesale.
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
    * Record that `uri`'s manifest is bound. Asserted rather than derived from
    * the entry count (a bind may carry no colocated entries); called after both
    * tiers apply, so it overrides {@link recordManifestBinding}.
    */
   public markManifestBound(uri: string): void {
      this.boundManifestUri = uri;
      this.manifestBindingStatus = "bound";
   }

   /**
    * Mark that a configured `manifestLocation` could not be fetched/bound, so
    * the package is serving live despite intending to be materialized-routed.
    */
   public markManifestBindFailed(): void {
      this.manifestBindingStatus = "live_fallback";
   }

   /** Store the latest bind's entries; an empty map reverts to live (unbound). */
   private recordManifestBinding(entries: FreshnessManifest): void {
      const count = Object.keys(entries).length;
      this.freshnessEntries = count > 0 ? entries : undefined;
      this.buildManifestEntries =
         count > 0 ? toTableNameManifest(entries) : undefined;
      this.manifestEntryCount = count;
      this.manifestBindingStatus = count > 0 ? "bound" : "unbound";
      this.freshManifestCache = undefined;
      if (count === 0) {
         this.boundManifestUri = null;
      }
   }

   /**
    * Restore colocated (same-connection) serve routing on load WITHOUT
    * recompiling: routing is a per-query `buildManifest` override, so setting
    * the entries suffices. An empty map reverts to live.
    *
    * Filtered POSITIVELY against {@link colocatedSourceEligibility}; a miss is
    * dropped, not fatal. This filter covers only this load-time restore path.
    * The three paths through {@link reloadAllModels} record entries UNFILTERED
    * and rely on the per-query `#(authorize)` graft
    * (`Model.buildGraftedMaterializer`); do not treat this check as a boundary
    * they pass through.
    */
   public bindColocatedServeManifest(entries: FreshnessManifest): void {
      const eligibility = this.colocatedSourceEligibility;
      const eligibleEntityIds = eligibility?.eligibleEntityIds;
      const allowed: FreshnessManifest = {};
      let dropped = 0;
      for (const [sourceEntityId, entry] of Object.entries(entries)) {
         if (eligibleEntityIds?.has(sourceEntityId)) {
            allowed[sourceEntityId] = entry;
            continue;
         }
         dropped++;
         const reason =
            eligibility === undefined
               ? "the package build plan could not be computed, so no source was examined for eligibility"
               : (eligibility.refused[sourceEntityId] ??
                 "the source is not in the package build plan, so it was never examined (a `#@ persist` on a non-query-shaped source is dropped)");
         recordColocatedBindDropped(
            eligibility === undefined ? "build_plan_unavailable" : "refused",
         );
         logger.warn(
            "Refusing a colocated serve binding: the source is not eligible to be served from a materialized table",
            {
               packageName: this.packageName,
               sourceEntityId,
               reason,
            },
         );
      }
      if (eligibility === undefined && dropped > 0) {
         // The whole default tier just reverted to live, not one source.
         logger.warn(
            "Colocated serve bindings dropped package-wide: the build plan never computed, so no source could be examined for eligibility",
            { packageName: this.packageName, droppedCount: dropped },
         );
      }
      this.recordManifestBinding(allowed);
   }

   /**
    * Bind the `storage=` serve bindings a host vouches for, minus any this
    * package's eligibility gate refuses, and push them onto every model.
    *
    * The host knows WHICH table backs a source, not WHETHER a frozen table may
    * serve it: only a compile knows that. A `given`-referencing source binds
    * per query for row-level access, so one shared table would hand every
    * caller the builder's rows (a source can gain a `given` after its build).
    *
    * Eligibility must be POSITIVE: a failed plan compute or a source missing
    * from the plan must not read as consent. Refused bindings are dropped, and
    * that source serves live, which is always correct.
    *
    * A published version also binds only the tables its own definition built.
    * Storage bindings are keyed by source NAME, and the versions of a
    * `scope: package` package share its tables under unchanged names, so an
    * entry whose `sourceEntityId` none of this version's same-named sources
    * has was built by another version's definition, and that source serves
    * live. (The colocated tier is keyed by content address already.)
    */
   public bindStorageServeBindings(
      entries: Record<string, ManifestEntry>,
   ): void {
      const derived = deriveServeBindings(
         this.getVersionId() === undefined ? entries : this.ownEntries(entries),
         groupAliasesByName(Object.values(this.buildPlan?.sources ?? {})),
         this.serveDestinationSchema,
      );
      const eligibility = this.sourceEligibility;
      // Keyed by NAME: a rollup passes only because `collectSourceEligibility`
      // also records the synthesized rollups under their generated names.
      // Scoping it to authored sources would silently switch off every storage
      // rollup.
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
               // A generated rollup name means nothing to an operator alone.
               ...(binding.origin === "preaggregate"
                  ? { kind: "pre-aggregation rollup" }
                  : {}),
               reason,
            },
         );
         return false;
      });
      this.storageServeBindings = allowed;
      this.pushStorageServeBindingsToModels();
   }

   /**
    * The entries this version's own sources built: same name, same content
    * address. See {@link bindStorageServeBindings}.
    */
   private ownEntries(
      entries: Record<string, ManifestEntry>,
   ): Record<string, ManifestEntry> {
      const own = new Set(
         Object.values(this.buildPlan?.sources ?? {}).map(
            (source) => `${source.name}\u0000${source.sourceEntityId}`,
         ),
      );
      const kept: Record<string, ManifestEntry> = {};
      for (const [key, entry] of Object.entries(entries)) {
         if (own.has(`${entry.sourceName}\u0000${entry.sourceEntityId}`)) {
            kept[key] = entry;
            continue;
         }
         logger.info(
            "Serving a source live: its stored table was built from another version's definition",
            {
               packageName: this.packageName,
               versionId: this.getVersionId(),
               sourceName: entry.sourceName,
            },
         );
      }
      return kept;
   }

   /**
    * Compile each model's pre-aggregation companion so the serve path can route
    * to a rollup. Re-run after a manifest bind, since the companion compiles
    * against the manifest. A no-op for a model with no `#@ preaggregate`.
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

   /** Push the current storage serve bindings onto every loaded model. */
   private pushStorageServeBindingsToModels(): void {
      for (const model of this.models.values()) {
         model.setServeBindings(this.storageServeBindings);
      }
   }

   /**
    * How a serve binding names a destination's schema, so it reads the table
    * where the build wrote it (storageTablePath). Absent, every destination is
    * `main`, the pre-existing layout.
    */
   public setServeDestinationSchema(
      resolver: (destinationName: string) => string | undefined,
   ): void {
      this.serveDestinationSchema = resolver;
   }

   /**
    * Set the connections serve shapes compile against: the environment's
    * storage destinations. Kept apart from {@link malloyConfig} so no author
    * model can reach a destination. Absent ⇒ queries serve live.
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

   /** Drop every model's memoized serve shape, when its destinations are replaced. */
   public invalidateServeShapes(): void {
      for (const model of this.models.values()) {
         model.invalidateServeShapeCache();
      }
   }

   /**
    * `explores` entries that don't resolve to a `.malloy` model, each with a
    * reason. Listing already fails safe (they hide); this says why, so load
    * can warn and publish can reject.
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
         if (
            entry.endsWith(NOTEBOOK_FILE_SUFFIX) ||
            this.getReportLayout(entry) === "linear"
         ) {
            problems.push({
               entry,
               reason:
                  `notebooks are always listed and cannot be explores. ` +
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

   /** The {@link exploreWarnings}, one per line, or "" if none. */
   public formatInvalidExplores(exploresOverride?: string[]): string {
      return this.exploreWarnings(exploresOverride).join("\n");
   }

   /**
    * Warnings for `#@ persist` sources Malloy's build walk did not plan (e.g.
    * a filtered pass-through), which would otherwise silently serve
    * live. Independent of {@link buildPlan}: a package whose only persist
    * source is dropped has no plan.
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

   /**
    * Warnings for the `#@ persist` sources the compile-time gates refused,
    * carrying the gate's message verbatim so there is one wording to maintain.
    * A rollup's `subject` is its BASE source (its own name is generated), and
    * is omitted when the base is unavailable.
    */
   private persistRefusalWarnings(): ApiPackageWarning[] {
      const refused = this.buildPlan?.refusedSources;
      if (!refused) return [];
      return Object.entries(refused).map(([sourceID, source]) => {
         const subject =
            source.tier === "preaggregate"
               ? this.buildPlan?.sources?.[sourceID]?.preaggregate
                    ?.baseSourceName
               : source.name;
         const warning: ApiPackageWarning = { message: source.message };
         if (source.modelPath) warning.model = source.modelPath;
         if (subject) warning.subject = subject;
         return warning;
      });
   }

   /**
    * Warnings for `storage=` sources the serve path is not honoring, per
    * {@link getPersistStorageMode}: `off` ignores the annotation, `write-only`
    * builds but does not route. `on` warns nothing.
    */
   private storageWarnings(): ApiPackageWarning[] {
      const mode = getPersistStorageMode();
      if (mode === "on" || !this.buildPlan?.sources) return [];
      const warnings: ApiPackageWarning[] = [];
      for (const source of Object.values(this.buildPlan.sources)) {
         const storage = source.annotationFields?.storage?.trim();
         if (!storage) continue;
         // A rollup gets its own wording: nobody queries it by name and there is
         // no live reading of it; queries are answered from the base. It is named
         // by its base source and grain (what the author typed), never its
         // generated name. In the default `off` mode this is the only signal.
         // The grain is a SET: the wire carries sorted dimensions, not the
         // authored order.
         const rollup = source.preaggregate;
         const at = rollup
            ? `Measures of \`${rollup.baseSourceName}\` pre-aggregated at grain ` +
              `(${rollup.grainDimensions.join(", ")}) declare `
            : "";
         const message = rollup
            ? mode === "off"
               ? `${at}storage="${storage}", but PERSIST_STORAGE_MODE is off, so ` +
                 `the rollup is not built. Queries that would have used it are ` +
                 `answered from \`${rollup.baseSourceName}\` instead — correct, ` +
                 `but unaccelerated.`
               : `${at}storage="${storage}", and the rollup is built, but ` +
                 `PERSIST_STORAGE_MODE is write-only, so the serve path does not ` +
                 `route to it. Queries are answered from ` +
                 `\`${rollup.baseSourceName}\` — correct, but unaccelerated.`
            : mode === "off"
              ? `declares storage="${storage}" but PERSIST_STORAGE_MODE is off; ` +
                `the annotation is ignored and the source is served live from ` +
                `its own warehouse.`
              : `is materialized into storage "${storage}" but ` +
                `PERSIST_STORAGE_MODE is write-only; the serve path is not ` +
                `routed to the materialized table (served live).`;
         warnings.push({
            model: source.modelPath ?? "",
            subject: rollup ? rollup.baseSourceName : source.name,
            message,
         });
      }
      return warnings;
   }

   /**
    * The Malloy Persistence policy's REJECTION rules: a non-empty result fails a
    * publish and disarms the scheduler (warn-only at load). Advisory findings go
    * to materializationConfigWarnings. Cross-package scope rules are not
    * checked: the build plan is intra-package only.
    */
   public persistencePolicyWarnings(): string[] {
      const warnings: string[] = [];
      const sources = this.buildPlan?.sources
         ? Object.values(this.buildPlan.sources)
         : [];
      const materialization = this.packageMetadata.materialization;
      const packageSchedule = materialization?.schedule ?? null;
      const packageFreshness = materialization?.freshness ?? null;
      const scope = this.packageMetadata.scope ?? "package";

      // Rule 1: per-source sharing/schedule are retired.
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

      // Rule 4: a valid 5-field cron, here so publish, PATCH, load and the
      // scheduler all apply one rule and a bad cron cannot silently never arm.
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

   /** The {@link persistencePolicyWarnings}, one per line, or "" if valid. */
   public formatInvalidPersistencePolicy(): string {
      return this.persistencePolicyWarnings().join("\n");
   }

   /**
    * REJECTION messages for incremental-refresh declarations (rules in
    * incremental_policy). Unlike {@link persistencePolicyWarnings}, a non-empty
    * result also fails the LOAD.
    */
   public incrementalPolicyWarnings(): string[] {
      return incrementalPolicyRejections(this.incrementalPolicySources);
   }

   /** The {@link incrementalPolicyWarnings}, one per line, or "" if valid. */
   public formatInvalidIncrementalPolicy(): string {
      return this.incrementalPolicyWarnings().join("\n");
   }

   /**
    * REJECTION messages for `#@ preaggregate` declarations that cannot take
    * effect (rules in preaggregation_validation), strict at publish AND load:
    * pre-aggregation is invisible when it works, so an ignored declaration
    * would show correct numbers and no acceleration, with no one told.
    *
    * The access-modifier pair only warns: refusing would stop already-published
    * packages loading, and an inherited annotation on a hiding source yields
    * advice the author cannot follow. Warning is safe because the planner
    * itself skips hidden fields (planSourcePreaggregation), so nothing is built.
    */
   public formatInvalidPreaggregatePolicy(): string {
      return this.preaggregateMessages(
         (code) => !PREAGG_WARN_ONLY.has(code),
      ).join("\n");
   }

   /** The access-modifier violations, surfaced at load as warnings. */
   public preaggregateAccessWarnings(): ApiPackageWarning[] {
      return this.preaggregateFindings((code) =>
         PREAGG_WARN_ONLY.has(code),
      ).map(({ modelPath, violation }) => ({
         model: modelPath,
         subject: violation.fieldName ?? violation.sourceName,
         message: violation.message,
      }));
   }

   private preaggregateMessages(
      keep: (code: PreaggregateViolationCode) => boolean,
   ): string[] {
      return this.preaggregateFindings(keep).map(
         // Source names repeat across models; name the file.
         ({ modelPath, violation }) => `${modelPath}: ${violation.message}`,
      );
   }

   /** Violations kept by `keep`, paired with their model, unformatted for both consumers. */
   private preaggregateFindings(
      keep: (code: PreaggregateViolationCode) => boolean,
   ): { modelPath: string; violation: PreaggregateViolation }[] {
      const findings: {
         modelPath: string;
         violation: PreaggregateViolation;
      }[] = [];
      for (const [modelPath, model] of this.models) {
         for (const violation of model.preaggregateViolations()) {
            if (keep(violation.code)) findings.push({ modelPath, violation });
         }
      }
      return findings;
   }

   /**
    * Within-package persist-target COLLISIONS: distinct sources (by
    * `sourceEntityId`) resolving to one physical table in one destination,
    * where each build would overwrite the other. Imports of the same source are
    * not flagged. The destination comes from the DECLARED annotation, so the
    * storage kill switch never hides a collision.
    *
    * Warn-only unless `PERSIST_COLLISION_ENFORCE` is set, so packages published
    * before this check still republish. Cross-version and cross-package
    * collisions need the host's global view.
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
         const destination = storage ? storage : source.connectionName;
         // Verbatim key: names differing only by case or quoting may still fold
         // to one table on some destinations. Normalizing is dialect-dependent,
         // so the host's naming is the backstop.
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

   /** Collisions as a publish rejection; "" unless `PERSIST_COLLISION_ENFORCE` is set. */
   public formatPersistenceCollisionRejections(): string {
      if (!getPersistCollisionEnforce()) return "";
      return this.persistenceCollisionWarnings().join("\n");
   }

   /**
    * One message per LISTED model that exports no sources or queries, which
    * reads as broken. Advisory, not in exploreWarnings: import-only files are
    * legitimate. Unlisted models are skipped.
    */
   public emptyDiscoveryWarnings(): Array<{ model: string; message: string }> {
      const exploreSet = this.exploreSet();
      const warnings: Array<{ model: string; message: string }> = [];
      for (const [modelPath, model] of this.models) {
         if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
         if (this.getReportLayout(modelPath) === "linear") continue;
         if (exploreSet && !exploreSet.has(modelPath)) continue;
         // A report exports nothing by design; it reads the surface.
         if (exploreSet && this.isServedReport(modelPath)) continue;
         if (model.hasEmptyDiscoverySurface()) {
            warnings.push({
               model: modelPath,
               message: this.surfaceIsIndexModel()
                  ? `${INDEX_MODEL_NAME} exports nothing, so nothing in this ` +
                    `package can be queried. Fix: add an export { ... } ` +
                    `naming the sources to publish.`
                  : `${modelPath} exports nothing, so nothing can be queried ` +
                    `through it. Fix: add an export { ... } naming the ` +
                    `sources to publish, or remove it from "explores".`,
            });
         }
      }
      return warnings;
   }

   /**
    * One message when every model on the surface failed to compile: the surface
    * then exports nothing and the boundary refuses every model with a 404,
    * including ones that compiled. Only reachable through {@link reloadAllModels}
    * called directly (materialization / rebind); a first load fails outright and
    * a normal reload keeps the last good model and reports it stale.
    *
    * Never a fallback to uncurated: falling open on a typo would expose what the
    * author curated away. This only explains the refusal.
    */
   public brokenSurfaceWarnings(): Array<{ model: string; message: string }> {
      const exploreSet = this.exploreSet();
      if (!exploreSet || exploreSet.size === 0) return [];
      const surface = Array.from(this.models.entries()).filter(
         ([modelPath]) =>
            modelPath.endsWith(MODEL_FILE_SUFFIX) &&
            exploreSet.has(modelPath) &&
            !this.isServedReport(modelPath),
      );
      if (surface.length === 0) return [];
      // Only when NOTHING on the surface compiled; otherwise a surface remains.
      if (!surface.every(([, model]) => !!model.getCompilationError())) {
         return [];
      }
      // Off-surface models that compiled fine: refused for someone else's typo.
      const collateral = Array.from(this.models.entries()).filter(
         ([modelPath, model]) =>
            modelPath.endsWith(MODEL_FILE_SUFFIX) &&
            !exploreSet.has(modelPath) &&
            !this.isServedReport(modelPath) &&
            !model.getCompilationError(),
      ).length;
      // ONE message, however many files the surface spans.
      const [firstPath] = surface[0];
      const subject =
         surface.length === 1
            ? `Model "${firstPath}" is this package's whole discovery surface ` +
              `and failed to compile`
            : `Every model on this package's discovery surface ` +
              `(${surface.map(([p]) => `"${p}"`).join(", ")}) failed to ` +
              `compile`;
      return [
         {
            model: firstPath,
            message:
               `${subject}, so the package exposes nothing: every model in ` +
               `it, including the ${collateral} that compiled, is now refused ` +
               `by name with a 404 that reads as "does not exist". Fix the ` +
               `compile error${surface.length === 1 ? "" : "s"} to restore ` +
               `them -- ` +
               surface
                  .map(
                     ([modelPath, model]) =>
                        `${modelPath}: ` +
                        `${model.getCompilationError()?.message ?? "unknown error"}`,
                  )
                  .join("; "),
         },
      ];
   }

   /** Log {@link emptyDiscoveryWarnings}; shared by load and reload. */
   private logEmptyDiscoveryWarnings(): void {
      for (const warning of this.brokenSurfaceWarnings()) {
         logger.warn(
            `Package ${this.packageName} has a broken discovery surface`,
            {
               packageName: this.packageName,
               detail: warning.message,
            },
         );
      }
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
    * Quote each manifest table path for its connection's dialect, mirroring how
    * the builder CREATEs it: Malloy pastes `tableName` into `FROM` verbatim, so
    * on a case-folding engine the case-preserved name is reachable only quoted.
    * An already-quoted name passes through.
    *
    * Verbatim otherwise in two different cases: no `connectionName` (an
    * engine-folding producer; benign), or an unresolvable one (a
    * misconfiguration; that entry is degraded and logged at ERROR, not the
    * whole bind, since it could not serve anyway).
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
    * Recompile every model against a new build manifest, in the worker pool so
    * a large package cannot block the liveness probe. A failed model is kept as
    * a placeholder rather than aborting; pool failures propagate as
    * `ServiceUnavailableError` for the caller to retry.
    */
   public async reloadAllModels(entries: FreshnessManifest): Promise<void> {
      // Quote BEFORE any projection: everything downstream reads these entries.
      entries = await this.quoteBoundTableNames(entries);
      const buildManifest = toTableNameManifest(entries);
      const modelPaths = Array.from(this.models.keys());
      logger.info("Reloading all models with build manifest", {
         packageName: this.packageName,
         modelCount: modelPaths.length,
         manifestEntryCount: Object.keys(buildManifest).length,
      });

      const outcome = await loadOutcomeOrThrow(
         getPackageLoadPool().loadPackage({
            packagePath: this.packagePath,
            packageName: this.packageName,
            environmentName: this.environmentName,
            malloyConfig: this.malloyConfig,
            defaultConnectionName: "duckdb",
            buildManifest,
            withPreaggregateCompanions: true,
         }),
      );

      // Same check as create, before anything is swapped.
      assertOutcomeStagesAvailable(outcome);

      const { models: nextModels, renderTagWarnings } =
         await Package.hydrateWorkerModels(
            outcome,
            {
               packageName: this.packageName,
               packagePath: this.packagePath,
               malloyConfig: this.malloyConfig,
               buildManifest,
            },
            "placeholder",
         );
      // Discover before installing, then install models and reports in one
      // synchronous step: a query must never see one without the other.
      const discoveryStarted = performance.now();
      const discovery = await this.discoverReportsIn(nextModels, "reload");
      const discoveryMs = performance.now() - discoveryStarted;
      this.models = nextModels;
      // Before refreshServingIdentity, which reads the agents it declares.
      this.rawAgents = outcome.packageMetadata.agents;
      // Move the identity as soon as the new models are installed.
      this.refreshServingIdentity();
      // Pick up publisher.json edits to the surface and boundary mode.
      const previousSurface = this.packageMetadata.explores;
      this.packageMetadata.explores = outcome.packageMetadata.explores;
      this.noteSurfaceChangeFrom(previousSurface);
      this.packageMetadata.queryableSources =
         outcome.packageMetadata.queryableSources;
      this.packageMetadata.manifestLocation =
         outcome.packageMetadata.manifestLocation ?? null;
      this.applyDiscoveryPolicyToModels();
      this.applySiblingModelResolverToModels();
      // Applies the query boundary too, against the refreshed explore set.
      this.installReports(discovery);
      // New models start with no serve routing; re-apply it.
      this.pushStorageServeBindingsToModels();
      this.pushServeDestinationConfigToModels();
      // Also where a manifest bind lands, so the companion compiles against it.
      await this.pushPreaggregateServeModels();
      // Read against the reports installed above.
      this.renderTagWarnings =
         this.surfacedRenderTagWarnings(renderTagWarnings);
      this.manifestWarnings = outcome.packageMetadata.manifestWarnings ?? [];
      this.recordDeprecatedUsage(outcome);
      this.retrievalSettings =
         outcome.packageMetadata.retrieval ?? DEFAULT_PACKAGE_RETRIEVAL;
      // After installReports: the tile lint asks the refreshed boundary.
      const lintStarted = performance.now();
      await this.relintReports({ textLint: true });
      this.reportDiscovered(
         "reload",
         discovery,
         discoveryMs + performance.now() - lintStarted,
      );
      this.recordManifestBinding(entries);
      this.wireFreshnessResolvers();
      warnInvalidExplores(this);
      this.logEmptyDiscoveryWarnings();
   }

   public async getModelFileText(modelPath: string): Promise<string> {
      const model = this.getModel(modelPath);
      if (!model) {
         throw new Error(`Model not found: ${modelPath}`);
      }
      return await model.getFileText(this.packagePath);
   }

   public async listModels(
      options: { includeHiddenFilesAndSources?: boolean } = {},
   ): Promise<ApiModel[]> {
      // With a surface, only its models are listed; the rest still compile for
      // imports but are hidden (`includeHiddenFilesAndSources` lists them,
      // marked `isHidden`). No surface lists every model.
      const exploreSet = this.exploreSet();
      const isHidden = (modelPath: string) =>
         !!exploreSet && !exploreSet.has(modelPath);
      const values = await Promise.all(
         Array.from(this.models.keys())
            .filter((modelPath) => {
               if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) return false;
               // A linear report is listed as a report, surface or not.
               if (this.getReportLayout(modelPath) === "linear") return false;
               // A grid report is listed as a report, even when `explores` lists it.
               if (!exploreSet) return true;
               if (this.isServedReport(modelPath)) return false;
               return (
                  options.includeHiddenFilesAndSources ||
                  exploreSet.has(modelPath)
               );
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
                  isHidden: isHidden(modelPath),
                  error,
               };
            }),
      );
      return values;
   }

   /**
    * Whether the query boundary restricts anything: a surface AND
    * `queryableSources` not `"all"`. A warning about refused queries must check
    * this, not the discovery-only `Model.hasEmptyDiscoverySurface`.
    */
   private queryBoundaryActive(): boolean {
      if (this.packageMetadata.queryableSources === "all") return false;
      return this.exploresDeclared();
   }

   /**
    * A file's text as compiled (a compile's replacement is not on disk), else
    * as saved; undefined when unreadable. Never throws.
    */
   private async readModelText(
      model: Model,
      modelPath: string,
   ): Promise<string | undefined> {
      try {
         return (
            model.getCompiledSourceText() ??
            (await fs.readFile(
               safeJoinUnderRoot(this.packagePath, modelPath),
               "utf8",
            ))
         );
      } catch {
         return undefined;
      }
   }

   /** Each `.malloy` / `.malloynb` in the package by the file URL an import names it with. */
   private modelPathsByImportUrl(): Map<string, string> {
      const byUrl = new Map<string, string>();
      for (const modelPath of this.models.keys()) {
         byUrl.set(
            pathToFileURL(path.join(this.packagePath, modelPath)).toString(),
            modelPath,
         );
      }
      return byUrl;
   }

   /**
    * A `.malloy` file's report tag, from its compiled facts or, lacking those,
    * its text. Never throws: an unreadable file is no report.
    */
   private async readFileReportTag(
      model: Model,
      modelPath: string,
      facts: ReportModelFacts | undefined,
   ): Promise<ReportTag> {
      try {
         if (facts) return factsReportTag(facts);
         const text = await this.readModelText(model, modelPath);
         return text === undefined
            ? { status: "none" }
            : readReportTagOfText(text);
      } catch {
         return { status: "none" };
      }
   }

   /**
    * Discover and install the package's reports, at load or as a compile's dry
    * run (which skips cell reading and the text lint, and is not counted).
    * Never throws: unreadable reports do not cost the models.
    */
   private async discoverReports(
      during: Exclude<ReportDiscoveryDuring, "reload">,
   ): Promise<{ discoveryMs: number; lintMs: number }> {
      const started = performance.now();
      const discovery = await this.discoverReportsIn(this.models, during);
      this.installReports(discovery);
      const linting = performance.now();
      await this.relintReports({ textLint: during !== "compile" });
      const finished = performance.now();
      this.reportDiscovered(during, discovery, finished - started);
      return { discoveryMs: linting - started, lintMs: finished - linting };
   }

   /** Time every discovery pass; a load or reload also counts and logs its findings. */
   private reportDiscovered(
      during: ReportDiscoveryDuring,
      discovery: ReportDiscovery,
      durationMs: number,
   ): void {
      recordReportDiscoveryDuration(during, durationMs);
      if (during === "compile") return;
      const findings = [...this.reportWarnings, ...discovery.readerWarnings];
      const summary = summarizeReportDiscovery(
         Array.from(discovery.files.values(), ({ status, report }) => ({
            status,
            layout: report?.layout,
         })),
         findings,
      );
      recordReportDiscovery(summary);
      logger.info("Report discovery", {
         packageName: this.packageName,
         during,
         durationMs: Math.round(durationMs),
         ...summary,
      });
      for (const finding of findings) {
         logger.debug("Report lint finding", {
            packageName: this.packageName,
            model: finding.model,
            code: finding.code ?? UNCODED_REPORT_FINDING,
            severity: finding.severity,
            message: finding.message,
         });
      }
   }

   /** Count the deprecated features this package uses, once per load or reload. */
   private recordDeprecatedUsage(outcome: LoadPackageOutcome): void {
      recordDeprecatedFormatUsage(this.models);
      recordDeprecatedManifestKeys(
         outcome.packageMetadata.deprecatedManifestKeys,
      );
   }

   /**
    * What every file of `models` is as a report (`docs/reports-design.md` §1),
    * keyed by path in path order. Location decides nothing, except that the
    * root `index.malloy` is the surface. Reads `models` only, never installed
    * state, so a reload can discover before swapping models in.
    */
   private async discoverReportsIn(
      models: ReadonlyMap<string, Model>,
      during: ReportDiscoveryDuring,
   ): Promise<ReportDiscovery> {
      const files = new Map<string, ReportFile>();
      // Every model, not just reports: `# drill` lives on model dimensions.
      const allFacts = new Map<string, ReportModelFacts>();
      const taggedIndexPaths: string[] = [];
      const dropped = (modelPath: string, why: string): ReportFile => {
         logger.warn(why, {
            packageName: this.packageName,
            during,
            model: modelPath,
         });
         return { status: "dropped", name: reportName(modelPath) };
      };
      for (const modelPath of Array.from(models.keys()).sort()) {
         const model = models.get(modelPath);
         if (!model) continue;

         let facts: ReportModelFacts | undefined;
         try {
            facts = await model.getCompiledReportModelFacts();
            if (facts) allFacts.set(modelPath, facts);
         } catch (err) {
            logger.warn("Reading a model's report facts failed", {
               packageName: this.packageName,
               during,
               model: modelPath,
               error: errMessage(err),
            });
         }

         if (isMalloynbPath(modelPath)) {
            if (isReportEligiblePath(modelPath)) {
               const name = reportName(modelPath);
               files.set(modelPath, {
                  status: "served",
                  name,
                  report: { path: modelPath, name, layout: "linear" },
               });
            }
            continue;
         }
         if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
         const tag = await this.readFileReportTag(model, modelPath, facts);
         if (isRootIndexPath(modelPath)) {
            // The surface, never a report; a tag on it is reported and ignored.
            if (tag.status !== "none") taggedIndexPaths.push(modelPath);
            continue;
         }
         if (!isReportEligiblePath(modelPath) || tag.status === "none")
            continue;
         const name = reportName(modelPath);
         if (tag.status !== "report") {
            // A tag that makes no report: the lint says why.
            files.set(modelPath, { status: "invalid", name, facts });
            continue;
         }
         if (!facts && !model.getCompilationError()) {
            // A corrupt worker payload can produce neither; do not drop silently.
            files.set(
               modelPath,
               dropped(modelPath, "Report file produced no facts and no error"),
            );
            continue;
         }
         let manifest: (ServedReportManifest & { error?: string }) | undefined;
         try {
            manifest = facts && buildReportManifest(facts);
         } catch (err) {
            logger.warn("Report discovery failed", {
               packageName: this.packageName,
               during,
               model: modelPath,
               error: errMessage(err),
            });
            files.set(modelPath, { status: "dropped", name });
            continue;
         }
         if (facts && !manifest) {
            // A grid tag with no tiles: not served, explained by the lint.
            files.set(modelPath, { status: "tileless", name, facts });
            continue;
         }
         if (!facts) {
            // Failed to compile: listed with its error, visibly broken, not absent.
            manifest = {
               name,
               layout: tag.layout,
               title: name,
               autorun: true,
               entryFile: modelPath,
               givens: [],
               error: model.getCompilationError()?.message,
            };
         }
         // Served whatever the surface: it limits what tiles read, not listing.
         files.set(modelPath, {
            status: "served",
            name,
            facts,
            report: { path: modelPath, name, layout: tag.layout, manifest },
         });
      }
      // The compiled text, so derived sources and cells match the IR, never a
      // newer file.
      for (const file of files.values()) {
         const report = file.report;
         if (!report || !report.path.endsWith(MODEL_FILE_SUFFIX)) continue;
         const model = models.get(report.path);
         if (!model) continue;
         // Unreadable text only costs the report its own derived sources.
         report.text = (await this.readModelText(model, report.path)) ?? "";
      }
      const readerWarnings =
         during === "compile" ? [] : this.attachNotebookCells(models, files);
      return { files, allFacts, taggedIndexPaths, readerWarnings };
   }

   /**
    * The render-tag findings this package surfaces: `raw` less the tile layout
    * tags a grid report reads off the view it names
    * ({@link dropGridTileLayoutFindings}), judged against the reports installed
    * now, so it runs after discovery. Each renderer finding kept is logged as
    * a warning.
    */
   private surfacedRenderTagWarnings(
      raw: ApiPackageWarning[],
   ): ApiPackageWarning[] {
      // Keyed by where each tile's view or query is declared, as the
      // package-wide export map is ({@link Model.definitionIdentity}).
      const identity = (modelPath: string, expression: string) => {
         const target = tileTarget(expression);
         return target
            ? this.models
                 .get(modelPath)
                 ?.definitionIdentity(target.name, target.view)
            : undefined;
      };
      const tileDefinitions = new Set<string>();
      for (const report of this.reports.values()) {
         if (report.layout !== "grid") continue;
         for (const tile of queryTiles(report.manifest?.tiles)) {
            const key = identity(report.path, tile.query);
            if (key) tileDefinitions.add(key);
         }
      }
      const kept = dropGridTileLayoutFindings(raw, (finding) => {
         if (finding.model === undefined || finding.subject === undefined)
            return false;
         const key = identity(finding.model, finding.subject);
         return key !== undefined && tileDefinitions.has(key);
      });
      // One line per checked view, as the check logged them before. A finding
      // with no subject is the check itself not finishing, which
      // hydrateWorkerModels already logged with its cause.
      const byTarget = new Map<string, ApiPackageWarning[]>();
      for (const finding of kept) {
         if (finding.subject === undefined) continue;
         const target = `${finding.model}\0${finding.subject}`;
         byTarget.set(target, [...(byTarget.get(target) ?? []), finding]);
      }
      for (const findings of byTarget.values()) {
         // An inert tag is not an invalid one, so don't call it "Invalid";
         // the per-finding messages already say which kind each is.
         logger.warn(
            `Render tag findings on '${findings[0].subject}': ${findings
               .map((f) => `[${f.severity}] ${f.message}`)
               .join("; ")}`,
         );
      }
      return kept;
   }

   /**
    * Make `discovery` the package's reports, and re-apply the query boundary
    * that depends on them, in one synchronous step so no query sees one
    * without the other.
    */
   private installReports(discovery: ReportDiscovery): void {
      this.reportFiles = discovery.files;
      this.reports = new Map(
         Array.from(discovery.files, ([path, file]) => [
            path,
            file.report,
         ]).filter(
            (entry): entry is [string, ServedReport] => entry[1] !== undefined,
         ),
      );
      this.discovery = discovery;
      this.readerWarnings = discovery.readerWarnings;
      this.applyQueryBoundaryToModels();
   }

   /**
    * Read each linear report's definition cells, for the deprecated notebook
    * route. A reader refusal is returned as a finding, since the model still
    * compiles and nothing else would say so.
    */
   private attachNotebookCells(
      models: ReadonlyMap<string, Model>,
      files: ReadonlyMap<string, ReportFile>,
   ): ApiPackageWarning[] {
      const warnings: ApiPackageWarning[] = [];
      for (const [modelPath, model] of models) {
         const report = files.get(modelPath)?.report;
         if (report?.layout !== "linear" || report.text === undefined) continue;
         const outcome = model.attachServedNotebookCells(report.text);
         const refusal = model.getNotebookReaderRefusal();
         if (outcome !== "refused" || !refusal) continue;
         warnings.push({
            model: modelPath,
            message: refusal.message,
            severity: "error",
            code: CELLS_UNREADABLE,
         });
      }
      return warnings;
   }

   /** Whether the last lint ran the per-file text lint, which a re-lint keeps. */
   private lintIncludesText = false;

   /** Re-run the report lint; also after a metadata PATCH, which can change the surface. */
   public async relintReports(
      options: { textLint?: boolean } = {},
   ): Promise<void> {
      const discovery = this.discovery;
      if (!discovery) return;
      if (options.textLint !== undefined)
         this.lintIncludesText = options.textLint;
      this.reportWarnings = await this.lintReports(
         discovery,
         this.lintIncludesText,
      );
   }

   /**
    * One finding per tile or filter suggest the query route will refuse as
    * off-surface (/compile is exempt, so nothing else would say so). Uses the
    * query route's own check ({@link Model.surfaceRefusal}), so they cannot
    * disagree.
    */
   private async lintTilesAgainstSurface(
      modelPath: string,
      manifest: ServedReportManifest,
   ): Promise<ApiPackageWarning[]> {
      const model = this.models.get(modelPath);
      if (!model || !this.queryBoundaryActive()) return [];
      // `source` is absent when the refusal would not name it (a gated model).
      const { unexported, fixFor } = this.surfaceIsIndexModel()
         ? {
              unexported: `which ${INDEX_MODEL_NAME} doesn't export`,
              fixFor: (source: string | undefined) =>
                 `Fix: add ${source ?? "the source it reads"} to the ` +
                 `export { ... } in ${INDEX_MODEL_NAME}.`,
           }
         : exploresSurfaceFix(this.packageMetadata.explores, (entry) =>
              this.isServedReport(entry),
           );
      const checks: {
         request: { queryName?: string; query?: string };
         describe: (source: string) => string;
      }[] = [];
      for (const tile of queryTiles(manifest.tiles)) {
         checks.push({
            request: { query: `run: ${tile.query}` },
            describe: (source) =>
               `Tile ${tile.query} on ${manifest.layout === "grid" ? "dashboard" : "notebook"} ${manifest.name} reads ` +
               `${source}, ${unexported}, so it won't load.`,
         });
      }
      for (const given of manifest.givens) {
         const suggest = given.suggest;
         if (!suggest) continue;
         const request = suggest.query
            ? { queryName: suggest.query }
            : suggest.source && suggest.dimension
              ? {
                   query: `run: ${suggest.source} -> { group_by: ${suggest.dimension} }`,
                }
              : undefined;
         if (!request) continue;
         checks.push({
            request,
            describe: (source) =>
               `Filter ${given.name} on dashboard ${manifest.name} suggests ` +
               `from ${source}, ${unexported}, so its list will be ` +
               `empty.`,
         });
      }
      // Independent compiles, run together; findings keep their order.
      const refusals = await Promise.all(
         checks.map(({ request }) => model.surfaceRefusal(request)),
      );
      const findings: ApiPackageWarning[] = [];
      refusals.forEach((refusal, i) => {
         if (!refusal) return;
         findings.push({
            model: modelPath,
            subject: manifest.name,
            message: `${checks[i].describe(refusal.source ?? "a source")} ${fixFor(refusal.source)}`,
            severity: "error",
         });
      });
      return findings;
   }

   /**
    * The report lint: per tagged file, plus the package-wide checks (drill,
    * given tags, curation). Per file, manifest and text checks run together so
    * a finding both make (an unparseable tag) is reported once. Never throws.
    */
   private async lintReports(
      discovery: ReportDiscovery,
      textLint: boolean,
   ): Promise<ApiPackageWarning[]> {
      const warnings: ApiPackageWarning[] = [];
      const files = Array.from(discovery.files).filter(([modelPath]) =>
         modelPath.endsWith(MODEL_FILE_SUFFIX),
      );
      try {
         // First, because they cannot throw: a vanished report must always be explained.
         for (const [modelPath, { status, name }] of files) {
            if (status !== "dropped") continue;
            warnings.push({
               model: modelPath,
               subject: name,
               message:
                  `"${modelPath}" carries an artifact tag but could not be ` +
                  `read, so it is not served and there is no report at ` +
                  `that path. The cause is in the server log. Reload the ` +
                  `package to try again.`,
               severity: "error",
            });
         }
         for (const [modelPath, { report, name }] of files) {
            // Served and only noted: the by-name route still works, encoded.
            if (report?.layout !== "grid" || matchesDocumentedReportName(name))
               continue;
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
         for (const finding of this.curationWarnings(discovery)) {
            warnings.push(finding);
         }
         for (const [modelPath, file] of files) {
            if (file.status === "dropped") continue;
            const manifest = file.report?.manifest;
            const facts =
               file.status !== "served" || manifest ? file.facts : undefined;
            const fileFindings: ApiPackageWarning[] = [];
            if (facts) {
               const findings = manifest
                  ? lintReport(facts, manifest)
                  : lintUndiscoveredReport(facts);
               for (const finding of findings) {
                  fileFindings.push({ model: modelPath, ...finding });
               }
               if (manifest) {
                  fileFindings.push(
                     ...(await this.lintTilesAgainstSurface(
                        modelPath,
                        manifest,
                     )),
                  );
               }
            }
            warnings.push(...fileFindings);
            if (!textLint) continue;
            const model = this.models.get(modelPath);
            const text =
               file.report?.text ??
               (model && (await this.readModelText(model, modelPath)));
            if (text === undefined) continue;
            for (const finding of lintNotebookText(modelPath, text)) {
               if (reportedByDashboardLint(finding, modelPath, fileFindings))
                  continue;
               warnings.push({
                  model: modelPath,
                  message: finding.message,
                  severity: finding.severity,
                  code: finding.code,
               });
            }
         }
         // Drill tags live on model dimensions, so every model is scanned. A
         // dropped report still counts as a drill target; its own finding
         // states the cause.
         const drillFacts = Array.from(discovery.allFacts.values());
         const reportPaths = new Set(
            Array.from(discovery.files)
               .filter(
                  ([, { status }]) =>
                     status === "served" || status === "dropped",
               )
               .map(([modelPath]) => modelPath),
         );
         for (const finding of lintDrillTargets(drillFacts, reportPaths)) {
            warnings.push(finding);
         }
         for (const finding of lintSelfDrills(drillFacts)) {
            warnings.push(finding);
         }
         // Every model too: givens are reached through imports.
         for (const finding of lintGivenTags(drillFacts)) {
            warnings.push(finding);
         }
      } catch (err) {
         logger.warn("Report lint failed", {
            packageName: this.packageName,
            error: errMessage(err),
         });
         // A truncated list would read as a cleaner package, so say so. The
         // thrown text, which is unbounded, stays in the log.
         warnings.push({
            message:
               `Report lint stopped early, so this list is incomplete: an ` +
               `unknown subset of the tile, report, drill, given and curation ` +
               `checks did not run. Treat a missing ` +
               `finding of any of those kinds as unknown rather than clean. ` +
               `Reload the package to run the lint again. The ` +
               `reports themselves are unaffected, because they are ` +
               `discovered before the lint runs. An operator can find the ` +
               `cause in the server log under "Report lint failed".`,
            severity: "warn",
         });
      }
      return warnings;
   }

   /**
    * Each file on the curated surface, mapped to what puts it there: its
    * importer on the way from the root `index.malloy`, or `explores`. A file
    * absent here is off the surface.
    */
   private surfaceImporters(): Map<string, string> {
      const importers = new Map<string, string>();
      const byUrl = this.modelPathsByImportUrl();
      const index = this.models.get(INDEX_MODEL_NAME)?.getModelDef();
      const queue: [string, ModelDef][] = index
         ? [[INDEX_MODEL_NAME, index]]
         : [];
      while (queue.length > 0) {
         const [importer, def] = queue.shift()!;
         for (const imported of def.imports ?? []) {
            const modelPath = byUrl.get(imported.importURL);
            if (
               !modelPath ||
               modelPath === INDEX_MODEL_NAME ||
               importers.has(modelPath)
            )
               continue;
            importers.set(modelPath, importer);
            const next = this.models.get(modelPath)?.getModelDef();
            if (next) queue.push([modelPath, next]);
         }
      }
      for (const entry of exploresSurfaceFiles(this.packageMetadata.explores)) {
         if (!importers.has(entry)) importers.set(entry, EXPLORES_IMPORTER);
      }
      return importers;
   }

   /**
    * Curation findings: a tagged root `index.malloy` (the tag is ignored), and a
    * report on the surface, which adds nothing to it.
    */
   private curationWarnings(discovery: ReportDiscovery): ApiPackageWarning[] {
      const warnings: ApiPackageWarning[] = [];
      for (const modelPath of discovery.taggedIndexPaths) {
         warnings.push({
            model: modelPath,
            code: "index-is-not-a-report",
            message:
               modelPath === INDEX_MODEL_NAME
                  ? `${INDEX_MODEL_NAME} carries an artifact tag, which is ignored: ` +
                    `the root ${INDEX_MODEL_NAME} is this package's surface, never a ` +
                    `report. Fix: move the report into a file of its own and remove ` +
                    `the tag from ${INDEX_MODEL_NAME}.`
                  : `${modelPath} carries an artifact tag, which is ignored: a ` +
                    `root ${INDEX_MODEL_NAME}, in any letter case, is reserved for ` +
                    `this package's surface, never a report. Fix: rename it to a ` +
                    `name other than ${INDEX_MODEL_NAME}.`,
            severity: "error",
         });
      }
      for (const [modelPath, importer] of this.surfaceImporters()) {
         if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
         if (!this.isServedReport(modelPath)) continue;
         warnings.push({
            model: modelPath,
            code: "report-on-surface",
            message:
               importer === EXPLORES_IMPORTER
                  ? reportListedInExploresMessage(modelPath)
                  : `${modelPath} is a report (it carries an artifact tag), and ` +
                    `${importer === INDEX_MODEL_NAME ? INDEX_MODEL_NAME : `${importer}, which ${INDEX_MODEL_NAME} reaches,`} ` +
                    `imports it. A report is not a model: it is not listed as ` +
                    `one, and a query addressed to it reads only what ` +
                    `${INDEX_MODEL_NAME} exports. Fix: if ${modelPath} is a ` +
                    `model, remove its artifact tag; if it is a report, move ` +
                    `what ${INDEX_MODEL_NAME} needs from it into an untagged ` +
                    `model that both import.`,
            severity: "warn",
         });
      }
      return warnings;
   }

   /** The path of every served report, of either layout, in path order. */
   public servedReportPaths(): IterableIterator<string> {
      return this.reports.keys();
   }

   /** Whether discovery made this file a served report, of either layout. */
   public isServedReport(modelPath: string): boolean {
      return this.reports.has(modelPath);
   }

   /** The layout of the report at `modelPath`, or undefined when it is not a served report. */
   public getReportLayout(modelPath: string): ReportLayout | undefined {
      return this.reports.get(modelPath)?.layout;
   }

   /**
    * Whether the write path may replace this file: a `.malloy` carrying an
    * artifact tag, served or not (saving the fix is how a broken one gets
    * served). Never an untagged file, a `.malloynb`, the root `index.malloy`,
    * or anything in `public/`.
    */
   public isWritableReport(modelPath: string): boolean {
      return (
         modelPath.endsWith(MODEL_FILE_SUFFIX) &&
         this.reportFiles.has(modelPath)
      );
   }

   /** Whether the file is a report discovery read, served or (a grid report with no tiles yet) not. */
   public isReport(modelPath: string): boolean {
      const status = this.reportFiles.get(modelPath)?.status;
      return status === "served" || status === "tileless";
   }

   /** A report's own URL: its path, each segment percent-encoded. */
   private reportResource(reportPath: string): string {
      return `${API_PREFIX}/environments/${this.environmentName}/packages/${this.packageName}/reports/${reportPath
         .split("/")
         .map(encodeURIComponent)
         .join("/")}`;
   }

   /** One served report as list-reports lists it. */
   private reportEntry(report: ServedReport): ApiReport {
      const model = this.models.get(report.path);
      const manifest = report.manifest;
      const listing =
         report.layout === "linear" ? (model?.getNotebookListing() ?? {}) : {};
      const error =
         report.layout === "linear"
            ? model?.getNotebookError()?.message
            : (manifest as { error?: string } | undefined)?.error;
      return {
         resource: this.reportResource(report.path),
         packageName: this.packageName,
         path: report.path,
         name: report.name,
         layout: report.layout,
         title: manifest?.title ?? listing.title ?? report.name,
         description: manifest?.description ?? listing.description,
         editable: report.path.endsWith(MODEL_FILE_SUFFIX),
         ...(error !== undefined && { error }),
      };
   }

   /** Every served report, of either layout, in path order. */
   public listReports(): ApiReport[] {
      return Array.from(this.reports.values()).map((report) =>
         this.reportEntry(report),
      );
   }

   /**
    * One report by its path (`sales/q3.malloy`, exactly as list-reports gives
    * it), or undefined when there is none. A `.malloy` report carries its
    * manifest; a `.malloynb`, which has none, carries its cells.
    */
   public async getReport(
      reportPath: string,
   ): Promise<ApiReportManifest | undefined> {
      const report = this.reports.get(reportPath);
      if (!report) return undefined;
      const entry = this.reportEntry(report);
      const manifest = report.manifest;
      let notebook: ApiRawNotebook | undefined;
      if (
         !manifest &&
         report.layout === "linear" &&
         entry.error === undefined
      ) {
         try {
            notebook = await this.models.get(report.path)?.getNotebook();
         } catch (err) {
            entry.error = errMessage(err);
         }
      }
      return {
         ...entry,
         ...(manifest && {
            tiles: manifest.tiles,
            dashboardColumns: manifest.dashboardColumns,
            startingGivens: manifest.startingGivens,
            autorun: manifest.autorun,
            givens: manifest.givens,
         }),
         ...(notebook && { notebook }),
      };
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
               // Pin the sandbox against implicit auto-install too, or
               // EXTENSION_FETCH_POLICY local-only would have a network hole.
               if (connection instanceof DuckDBConnection) {
                  await applyExtensionSessionSettings(connection);
               }
               return connection;
            }
            // The *current* environment config, so a connection swap needs no reload.
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
      } catch (error) {
         // Only ENOENT/ENOTDIR mean "does not exist"; an EACCES is rethrown as itself.
         const code = (error as NodeJS.ErrnoException).code;
         if (code !== "ENOENT" && code !== "ENOTDIR") throw error;
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
      // One duckdb connection for every probe (each would otherwise do its
      // own native init); Malloy caches it for later model compiles too.
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
               // One unreadable file must not drop the package; list it with
               // `error` instead of `info`, like a model that fails to compile.
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
            // Excel's lock file (~$name.xlsx) is not data.
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

      // A cheap runtime per probe isolates compile state; the connection is shared.
      const runtime = new ConnectionRuntime({
         urlReader: new EmptyURLReader(),
         connections: [conn],
      });
      // Forward slashes work on Windows DuckDB and avoid escaping.
      const normalizedPath = fullPath.replace(/\\/g, "/");
      // DuckDB reads `[`, `?` and `*` in a table path as a glob, so such a
      // path would report other files' columns and rows (`[x].csv` reads
      // `x.csv`). The check covers the whole path, since a glob in a folder
      // name matches sibling folders. List it as unreadable instead.
      if (/[[?*]/.test(normalizedPath)) {
         throw new Error(
            `Cannot read ${databasePath}: its path holds [, ? or *, which DuckDB reads as a pattern matching other files. Rename the file, or the folder holding it, without those characters.`,
         );
      }
      // DuckDB's table-path check refuses a bare path holding a space or a
      // quote, which left such a file listed as unreadable. So the path goes
      // in as a single-quoted SQL literal, which the check accepts and
      // canonicalizes to the same SQL a bare path gets.
      const tableLiteral = `'${sqlLiteral(normalizedPath, "duckdb")}'`;
      // That literal sits in a Malloy double-quoted string, so `\` and `"` are
      // escaped for it. Normalizing above left no backslash, but escaping it
      // keeps this step correct on its own.
      const malloyString = `"${tableLiteral.replace(/[\\"]/g, "\\$&")}"`;
      const model = runtime.loadModel(
         `source: temp is duckdb.table(${malloyString})`,
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
      this.applySiblingModelResolverToModels();
   }
}
