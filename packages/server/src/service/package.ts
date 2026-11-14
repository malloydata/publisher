// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { quotedIdentifier, quotedPath, stringLiteral } from "./malloy_text";
import { INCREMENTAL_NO_MERGE_KEY_CODE } from "./materialization/plan/incremental_policy";
import * as fs from "fs/promises";
import * as path from "path";
import { pathToFileURL } from "url";

import "@malloydata/db-duckdb/native";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { findingSeverity } from "./annotation_reader";
import {
   Connection,
   ConnectionRuntime,
   contextOverlay,
   EmptyURLReader,
   FixedConnectionMap,
   MalloyConfig,
   MalloyError,
   Model as MalloyModel,
   SourceDef,
   type ModelDef,
} from "@malloydata/malloy";
import { lazyHistogram, withSpan } from "../telemetry";
import {
   type EntitiesModelFile,
   type EntitiesQueryRoute,
   entitiesFindingCode,
   extractPackageEntities,
   type ModelEntitiesPlan,
} from "./entities/extract";
import type { SourceAddress } from "./entities/internal_types";
import { toApiEntities } from "./entities/to_api";
import { isUnconditionalDenyAuthorize } from "./entities/access_gates";
import { splitSourcePath } from "./entities/explore_walk";
import recursive from "recursive-readdir";
import { components } from "../api";
import {
   getPackageLoadPool,
   type LoadPackageOutcome,
} from "../package_load/package_load_pool";
import type { LoadPackageRequest } from "../package_load/protocol";
import { queryBoundaryActive } from "./package_manifest";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   type PackageRetrievalSettings,
} from "./package_retrieval_settings";
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
import { recordManifestBindDegraded } from "../materialization_metrics";
import {
   LOAD_DURATION_BUCKETS_MS,
   recordEntitiesFindings,
   recordPackageLoadPhaseDurations,
   recordPackageLoadPhases,
   type PackageLoadStatus,
} from "../package_load_metrics";
import { assertSafeEnvironmentPath, safeJoinUnderRoot } from "../path_safety";
import { type SkillEntry } from "./skills/build_skills_bundle";
import {
   PACKAGE_SKILLS_DIR,
   readAgentAsset,
   readPackageAgents,
   readSkillsDir,
   summarizeAgentSkillFile,
   type PackageAgent,
} from "./skills/package_skills";
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
} from "./materialization/domain/types";
import type {
   ChainBuildRequest,
   ChainBuildText,
} from "./materialization/domain/chain_build";
import { errMessage, ignoreDotfiles } from "../utils";
import { packageResource } from "../api_resource";
import { isPackageInlineFieldsEnabled } from "../config";
import type {
   ColocatedSourceEligibility,
   SourceEligibility,
} from "./materialization/plan/build_plan";
import {
   type DeploymentFacts,
   PackageMaterialization,
   toTableNameManifest,
} from "./materialization/package_materialization";
import {
   materializationConfigWarnings,
   type QueryMetadataDeclaration,
} from "./materialization/plan/materialization_config_validation";
import type { QueryMetadata } from "./query_metadata";
import { createEventLoopYielder } from "./event_loop_yield";
import { CELLS_UNREADABLE } from "./notebook";
import {
   deprecatedFormatWarnings,
   recordDeprecatedFormatUsage,
} from "./deprecated/lints";
import {
   arrayQueryJoinRefusal,
   arrayQueryJoins,
   governsAccess,
} from "./governance/ir/array_query";
import {
   carriesCeilingLeaks,
   classificationLeaks,
   classificationWarnings,
   prepareCeilingQueries,
   type PreparedCeilingQueries,
} from "./classification_report";
import {
   classificationSummary,
   type ApiClassificationSummary,
} from "./classification_summary";
import {
   isTruncatedWalkFinding,
   JOIN_BYPASS,
   joinBypasses,
} from "./join_bypass";
import {
   ambiguousDashboardNameWarnings,
   type ReportViewPackage,
} from "./legacy_report_views";
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
import { sqlLiteral } from "./introspection_sql";
import { isQuotedIdentifierPath, quoteManifestTablePath } from "./quoting";
import { Model } from "./model";
import {
   PackageModelPolicy,
   type OffSurfaceContext,
   type QueryBoundaryPolicy,
} from "./model_policy";
import {
   type ApiSecureGivenSummary,
   summarizeSecureGivens,
} from "./secure_givens";
import { assertPersistNamesQuoted } from "./materialization/plan/persist_annotation_validation";
import {
   applyLoadGates,
   assertOutcomeStagesAvailable,
   loadOutcomeOrThrow,
   warnInvalidExplores,
   type LoadGatedPackage,
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
type ApiModelEntities = components["schemas"]["ModelEntities"];
type ApiPackageWarning = NonNullable<ApiPackage["warnings"]>[number];
type ApiServeVerdict = components["schemas"]["ServeVerdict"];

interface ClassificationMemo {
   forModels: Map<string, Model>;
   listing: string;
   summary?: { value: ApiClassificationSummary | undefined };
   warnings?: ApiPackageWarning[];
   joinBypass?: string[];
}
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

/**
 * The fields `Package.getPackageMetadata` computes and overlays on what the
 * package declares: what this server holds now (the load, its manifest
 * binding, the sources it serves from storage), the build plan and agents,
 * the secure givens, and the warnings. Never stored, so a caller that
 * round-trips a response (a version load, a manifest rebind) cannot freeze
 * them: the next read computes them again, and omits the inline ones while
 * PUBLISHER_PACKAGE_INLINE_FIELDS is off.
 */
const COMPUTED_PACKAGE_FIELDS = [
   "manifestBindingStatus",
   "manifestEntryCount",
   "boundManifestUri",
   "servedRevision",
   "sourceContentSha",
   "storageServeBindings",
   "serveVerdicts",
   "buildPlan",
   "agents",
   "secureGivens",
   "exploresWarnings",
   "warnings",
] as const satisfies readonly (keyof ApiPackage)[];

/** Why a join-bypass finding is a warning and not a refusal (docs/authorize.md, "Gates are edge controls"). */
const JOIN_BYPASS_NOTE =
   "Each gate governs only the source a query enters through, so exposing these rows to every " +
   "caller this entry admits is the author's choice, and a compliance review flags it; " +
   "otherwise apply the fix above.";

export class Package implements LoadGatedPackage, ReportViewPackage {
   private readonly environmentName: string;
   private packageName: string;
   private packageMetadata: ApiPackage;
   private databases: ApiDatabase[];
   private models: Map<string, Model> = new Map();
   /**
    * The policy every model in {@link models} was constructed with (each holds
    * `modelPolicy.forModel(path)`): curation, query boundary, sibling
    * resolution and freshness. Replaced together with `models` on a reload.
    */
   private modelPolicy: PackageModelPolicy;
   /**
    * {@link summarizeSecureGivens} over the `models` map it was computed for.
    * Keyed on that map's identity, so whatever replaces `models` gets a fresh
    * summary on the next read; a `Model`'s givens and compile error are fixed
    * at construction, so the same map always yields the same answer.
    */
   private secureGivensMemo:
      | {
           forModels: Map<string, Model>;
           value: readonly ApiSecureGivenSummary[] | undefined;
        }
      | undefined;
   /**
    * The classification walks over the `models` map and the listing metadata
    * they were computed for: `explores` and `queryableSources` are all
    * `applyQueryBoundaryToModels` reads, so a PATCH that changes neither keeps it.
    */
   private classificationMemo: ClassificationMemo | undefined;
   /**
    * What every `.malloy` model declares, in the form an indexer reads
    * (`Package.entities`). Computed once at load from the models as they
    * compiled then, before any manifest is bound, and frozen; a reload that
    * rebinds models does not change it.
    */
   private entities: readonly ApiModelEntities[] | undefined;
   /**
    * The compiled files whose entities the last {@link computeEntities}
    * planned on the main thread rather than taking the load worker's plan.
    */
   private entitiesPlannedHere: readonly string[] = [];
   private packagePath: string;
   private malloyConfig: MalloyConfig;
   /**
    * Loaded by a dry run, to check and discard: its load records no
    * per-package or deprecated-usage metrics. See {@link Package.create}.
    */
   private dryRun = false;
   /**
    * The package's materialization state: build plan, eligibility, bound
    * manifest, serve routing and persistence-policy findings.
    */
   private readonly materialization = new PackageMaterialization({
      packageName: () => this.packageName,
      versionId: () => this.getVersionId(),
      metadata: () => this.packageMetadata,
      models: () => this.models,
      modelPolicy: () => this.modelPolicy,
      packagePath: () => this.packagePath,
      malloyConfig: () => this.malloyConfig,
      buildPlanPackage: this,
      publishedSurface: () => this.publishedSurface(),
   });
   // The load path is the only writer of these; the accessors let a spec that
   // isolates a filter set them on the package, where they used to live
   // (protected, not private: nothing in this class reads the setters).
   private get buildPlan(): BuildPlan | null {
      return this.materialization.buildPlan;
   }
   private set buildPlan(plan: BuildPlan | null) {
      this.materialization.buildPlan = plan;
   }
   protected set sourceEligibility(eligibility: SourceEligibility | undefined) {
      this.materialization.sourceEligibility = eligibility;
   }
   protected set colocatedSourceEligibility(
      eligibility: ColocatedSourceEligibility | undefined,
   ) {
      this.materialization.colocatedSourceEligibility = eligibility;
   }
   // Non-fatal render-tag findings across the models; a bad tag does not fail
   // the load, so this is its signal.
   private renderTagWarnings: ApiPackageWarning[] = [];
   /** The annotations the entities extraction left out, as warnings. */
   private entitiesWarnings: ApiPackageWarning[] = [];
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
   /**
    * Under an active query boundary, each declaration a listed non-report file
    * exports (keyed by {@link Model.definitionIdentity}), with the first such
    * file by path and the name it exports it under: where the surface serves
    * it. Set with the boundary; empty without one.
    */
   private servedSourceAddresses = new Map<string, SourceAddress>();
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
   // OTel's default buckets stop at 10s and would censor the slow tail.
   private static packageLoadHistogram = lazyHistogram(
      "malloy_package_load_duration",
      "Time taken to load a Malloy package",
      { buckets: LOAD_DURATION_BUCKETS_MS },
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
       * The policy `models` were hydrated with. Omitted only where `models`
       * are not this package's own hydrations (spec doubles, an empty map).
       */
      modelPolicy: PackageModelPolicy = new PackageModelPolicy(),
   ) {
      this.modelPolicy = modelPolicy;
      this.environmentName = environmentName;
      this.packageName = packageName;
      this.packagePath = packagePath;
      this.packageMetadata = Package.storableMetadata(packageMetadata);
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
         ...(a.budget && { budget: a.budget }),
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
      const { definitionSha, skillMeta, asset_roots, assets, ...definition } =
         agent;
      const counts = new Map<string, number>();
      for (const a of assets ?? []) {
         const skill = a.relative_filepath.split("/")[0]!;
         counts.set(skill, (counts.get(skill) ?? 0) + 1);
      }
      const assetCounts = Object.fromEntries(counts);
      return {
         ...definition,
         skills:
            skills === "full"
               ? definition.skills
               : definition.skills.map((f) =>
                    summarizeAgentSkillFile(f, skillMeta),
                 ),
         ...(assets &&
            (skills === "full"
               ? { asset_roots, assets }
               : { asset_counts: assetCounts })),
         source: this.agentSource(definitionSha),
      };
   }

   /** One agent skill file, looked up in the resolved set and never joined onto the filesystem; a listed asset is matched exactly, then read from disk and re-checked against its listed sha. */
   public getAgentSkillFile(
      name: string,
      relativeFilepath: string,
   ): ApiAgentSkillRead | undefined {
      const agent = this.packageAgents.get(name);
      const file = agent?.skills.find(
         (f) => f.relative_filepath === relativeFilepath,
      );
      if (!agent) return undefined;
      if (file) {
         return { ...file, source: this.agentSource(agent.definitionSha) };
      }
      const text = readAgentAsset(this.packagePath, agent, relativeFilepath);
      if (text === undefined) return undefined;
      return {
         relative_filepath: relativeFilepath,
         file_contents: text,
         source: this.agentSource(agent.definitionSha),
      };
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
         agents.contentShas,
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

   /** Decide discovery curation (`explores` listing, `export {}` filtering) for every model. */
   private applyDiscoveryPolicyToModels(): void {
      this.modelPolicy.setDiscoveryCuration(this.exploresDeclared());
   }

   /**
    * The sources a caller may query, by the model that publishes them, or
    * undefined when every source is queryable (no surface resolved, or
    * `queryableSources: "all"`). The same files
    * {@link applyQueryBoundaryToModels} admits: the surface's `.malloy` files,
    * not its reports, each with the names its export closure lists.
    */
   private publishedSurface():
      | { modelPath: string; names: string[]; queries: string[] }[]
      | undefined {
      if (this.packageMetadata.queryableSources === "all") return undefined;
      const exploreSet = this.exploreSet();
      if (!this.exploresDeclared() || !exploreSet) return undefined;
      const out: { modelPath: string; names: string[]; queries: string[] }[] =
         [];
      for (const [modelPath, model] of this.models) {
         if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
         if (!exploreSet.has(modelPath) || this.isServedReport(modelPath)) {
            continue;
         }
         out.push({
            modelPath,
            names: (model.getSources() ?? []).flatMap((source) =>
               source.name ? [source.name] : [],
            ),
            // Named queries are published too (packageCuratedQueries).
            queries: (model.getQueries() ?? []).flatMap((query) =>
               query.name ? [query.name] : [],
            ),
         });
      }
      return out;
   }

   /**
    * Decide each model's query boundary in the model policy, where
    * `Model.runQuery` enforces it (`/compile` is exempt; only
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
      const servedSourceAddresses = new Map<string, SourceAddress>();
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
         // In path order, so the first file to export a declaration is the
         // one its served address names.
         const byPath = [...this.models].sort(([a], [b]) =>
            a < b ? -1 : a > b ? 1 : 0,
         );
         for (const [modelPath, model] of byPath) {
            // Only .malloy files curate a surface; a listed notebook is invalid
            // and contributes nothing (as in listModels).
            if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
            if (!exploreSet.has(modelPath)) continue;
            // Nor does a report, even when listed: it reads the surface, it
            // never adds to it.
            if (this.isServedReport(modelPath)) continue;
            for (const source of model.getSources() ?? []) {
               const identity = model.definitionIdentity(source.name);
               add(sources, source.name, identity);
               if (
                  source.name &&
                  identity &&
                  !servedSourceAddresses.has(identity)
               ) {
                  servedSourceAddresses.set(identity, {
                     modelFilePath: modelPath,
                     sourceName: source.name,
                  });
               }
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
      const boundaries = new Map<string, QueryBoundaryPolicy>();
      for (const modelPath of this.models.keys()) {
         // A `.malloy` report only: a `.malloynb` is a notebook by its format.
         const report = modelPath.endsWith(MODEL_FILE_SUFFIX)
            ? this.reports.get(modelPath)
            : undefined;
         boundaries.set(modelPath, {
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
      this.modelPolicy.setQueryBoundaries(boundaries);
      this.servedSourceAddresses = servedSourceAddresses;
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
      this.modelPolicy.setSiblingModelDefResolver(
         (url: string): ModelDef | undefined => byUrl.get(url)?.getModelDef(),
      );
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
      /**
       * `dryRun`: load to check, never to serve (`POST …/packages?dryRun=true`).
       * The load is the same; it records no load metrics, deprecated-usage or
       * entities-findings counts, and logs a failure as the caller's answer
       * rather than as an error, since nothing served is affected.
       */
      options: { dryRun?: boolean } = {},
   ): Promise<Package> {
      return withSpan(
         "publisher.package.load",
         {
            "malloy.environment": environmentName,
            "malloy.package": packageName,
            "publisher.package.dry_run": options.dryRun === true,
         },
         () =>
            Package.load(
               environmentName,
               packageName,
               packagePath,
               environmentMalloyConfig,
               cleanupDirectoryOnFailure,
               options.dryRun === true,
            ),
      );
   }

   private static async load(
      environmentName: string,
      packageName: string,
      packagePath: string,
      environmentMalloyConfig: PackageConnectionInput,
      cleanupDirectoryOnFailure: boolean,
      dryRun: boolean,
   ): Promise<Package> {
      assertSafeEnvironmentPath(packagePath);
      const startTime = performance.now();
      await Package.validatePackageManifestExistsOrThrowError(packagePath);
      const manifestValidationTime = performance.now();
      logger.info("Package manifest validation completed", {
         packageName,
         duration: formatDuration(manifestValidationTime - startTime),
      });

      // Declared outside the try, so a failed load can release it below.
      let malloyConfig: MalloyConfig | undefined;
      try {
         // Built on the main thread: it owns the native connection handles
         // that serve queries, which cannot cross the isolate boundary. The
         // worker proxies non-duckdb lookups back through it during compile.
         malloyConfig = Package.buildPackageMalloyConfig(
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
            dryRun,
         );
      } catch (error) {
         // No caller ever holds a package whose load failed, so its own
         // connections (the package DuckDB the load opened) close here; the
         // environment's connections are not the package's and stay open.
         await malloyConfig?.shutdown("close").catch((shutdownError) => {
            logger.warn(
               `Failed to close the connections of package ${packageName} after a failed load`,
               { error: errMessage(shutdownError) },
            );
         });
         Package.reportFailedLoad(packageName, error, startTime, dryRun);
         if (cleanupDirectoryOnFailure) {
            await Package.removeFailedPackageDirectory(packagePath);
         } else {
            logger.info(
               `Preserving existing package directory after failed load: ${packagePath}`,
            );
         }
         throw error;
      }
   }

   /** Log a failed load, and count it unless it was a dry run. */
   private static reportFailedLoad(
      packageName: string,
      error: unknown,
      startTime: number,
      dryRun: boolean,
   ): void {
      if (dryRun) {
         logger.info(`Dry-run load of package ${packageName} failed`, {
            packageName,
            dryRun: true,
            error: errMessage(error),
         });
         return;
      }
      logger.error(`Error loading package ${packageName}`, {
         error: errMessage(error),
      });
      this.packageLoadHistogram().record(performance.now() - startTime, {
         malloy_package_name: packageName,
         status: packageLoadFailureStatus(error),
      });
   }

   /**
    * Remove a staged package directory whose load failed. Never a watch-mode
    * symlink: removing it would unmount the package on a half-typed save until
    * a restart. Never throws.
    */
   private static async removeFailedPackageDirectory(
      packagePath: string,
   ): Promise<void> {
      try {
         const stat = await fs.lstat(packagePath).catch(() => null);
         if (stat?.isSymbolicLink()) {
            logger.info(
               `Skipping cleanup of symlinked package path on failure: ${packagePath}`,
            );
            return;
         }
         await fs.rm(packagePath, { recursive: true, force: true });
         logger.info(`Cleaned up failed package directory: ${packagePath}`);
      } catch (cleanupError) {
         logger.warn(`Failed to clean up package directory ${packagePath}`, {
            error: cleanupError,
         });
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
         resource: packageResource(environmentName, packageName),
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
         /** The policy each hydrated model is constructed with (`forModel(path)`). */
         modelPolicy: PackageModelPolicy;
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
            {
               policy: ctx.modelPolicy.forModel(sm.modelPath),
               sourceText:
                  sm.modelPath === ctx.replacement?.modelPath
                     ? ctx.replacement.source
                     : undefined,
            },
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
                  policy: ctx.modelPolicy.forModel(sm.modelPath),
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
      const modelPolicy = new PackageModelPolicy();
      const { models, renderTagWarnings } = await Package.hydrateWorkerModels(
         outcome,
         {
            packageName,
            packagePath,
            malloyConfig,
            buildManifest,
            compile: true,
            replacement,
            modelPolicy,
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
         modelPolicy,
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
      // The dry run reports what the reload warns about the entities too.
      try {
         await pkg.computeEntities(Package.entityPlansOf(outcome));
      } catch (err) {
         logger.warn("Entities lint failed during compile", {
            packageName,
            error: errMessage(err),
         });
      }
      return {
         renderTagWarnings: pkg.surfacedRenderTagWarnings(renderTagWarnings),
         reportWarnings: [...pkg.reportWarnings, ...pkg.entitiesWarnings],
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
      dryRun: boolean,
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
            withEntities: true,
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
         translationsStored: outcome.timings.translationsStored,
         translationsReused: outcome.timings.translationsReused,
         modelCount: outcome.models.length,
         databaseCount: databases.length,
      });

      const packageConfig = Package.packageConfigFromOutcome(
         environmentName,
         packageName,
         outcome,
      );

      // Any model compile failure aborts the load (reload keeps placeholders).
      const modelPolicy = new PackageModelPolicy();
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
            { packageName, packagePath, malloyConfig, modelPolicy },
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
         if (!dryRun) {
            recordPackageLoadPhases(
               outcome.timings,
               packageLoadFailureStatus(err),
            );
         }
         throw err;
      }

      // `malloy_package_load_duration` measures up to here, before discovery.
      const executionTime = performance.now() - startTime;
      const pkg = new Package(
         environmentName,
         packageName,
         packagePath,
         packageConfig,
         databases,
         models,
         malloyConfig,
         modelPolicy,
      );
      pkg.dryRun = dryRun;
      let reportTimings: Awaited<ReturnType<Package["discoverReports"]>>;
      let buildPlanMs: Awaited<
         ReturnType<PackageMaterialization["applyLoadBuildPlan"]>
      >;
      let entitiesMs: number;
      try {
         reportTimings = await pkg.discoverReports("load");
         // After discovery: which findings a tile explains depends on the reports.
         pkg.renderTagWarnings =
            pkg.surfacedRenderTagWarnings(renderTagWarnings);
         pkg.adoptOutcomeMetadata(outcome);
         // No manifest is bound yet, so this serves live until a bind.
         pkg.materialization.wireFreshnessResolvers();
         pkg.refreshServingIdentity();

         buildPlanMs = await pkg.materialization.applyLoadBuildPlan(
            outcome.buildPlan,
         );
         // Before any bind, so a colocated rollup bound later is routed.
         pkg.materialization.pushServedCopyContextToModels();

         await applyLoadGates(pkg);
         const entitiesStarted = performance.now();
         await pkg.computeEntities(Package.entityPlansOf(outcome));
         entitiesMs = performance.now() - entitiesStarted;
         pkg.logEmptyDiscoveryWarnings();
      } catch (err) {
         if (!dryRun) {
            recordPackageLoadPhases(
               outcome.timings,
               packageLoadFailureStatus(err),
            );
         }
         throw err;
      }

      // Recorded only now: a gate, the extraction or a companion compile can
      // still refuse the load above, and Package.create records that failure.
      if (dryRun) {
         logger.info(`Dry-run load of package ${packageName} succeeded`, {
            packageName,
            dryRun: true,
            duration: formatDuration(executionTime),
         });
         return pkg;
      }
      this.packageLoadHistogram().record(executionTime, {
         malloy_package_name: packageName,
         status: "success",
      });
      recordPackageLoadPhases(outcome.timings, "success");
      logger.info(`Successfully loaded package ${packageName}`, {
         packageName,
         duration: formatDuration(executionTime),
      });
      recordPackageLoadPhaseDurations({
         worker: workerDoneTime - dispatchTime,
         hydrate: hydrationTimings.hydrateMs,
         render_validation: hydrationTimings.renderValidationMs,
         discovery: reportTimings.discoveryMs,
         lint: reportTimings.lintMs,
         build_plan: buildPlanMs,
         entities: entitiesMs,
         total: performance.now() - startTime,
      });
      return pkg;
   }

   /** The published version this package is, or undefined for a package with none. */
   public getVersionId(): string | undefined {
      return this.packageMetadata.versionId ?? undefined;
   }

   public getPackageName(): string {
      return this.packageName;
   }

   /** The host port's `chainBuildText` (see PackageMaterialization.chainBuildText). */
   public chainBuildText(request: ChainBuildRequest): ChainBuildText {
      return this.materialization.chainBuildText(request);
   }

   /** The persist build plan, or null when the package declares no persist source. */
   public getBuildPlan(): BuildPlan | null {
      return this.materialization.getBuildPlan();
   }

   /** Whether the build plan could not be derived at load, so `getBuildPlan` says nothing. */
   public isBuildPlanUnavailable(): boolean {
      return this.materialization.isBuildPlanUnavailable();
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

   /**
    * For each published entry point, its `#(carries)` and its field count by
    * class, or `undefined` when nothing published is classified. Walks every
    * published field, so it is memoized with the classification walks and served by
    * `GET …/classification` (and, deprecated, inline on the single-package
    * read while PUBLISHER_PACKAGE_INLINE_FIELDS is on), never on a listing or
    * the `/status` poll path.
    */
   public getClassificationSummary(): ApiClassificationSummary | undefined {
      const memo = this.currentClassificationMemo();
      memo.summary ??= {
         value: classificationSummary(this.models, (modelPath) =>
            this.isListedModelPath(modelPath),
         ),
      };
      return memo.summary.value;
   }

   /**
    * {@link entities}, or undefined before the package has loaded. Frozen, so
    * callers share it.
    */
   public getEntities(): readonly ApiModelEntities[] | undefined {
      return this.entities;
   }

   /**
    * {@link entities} under the rules `GET …/models/{path}` applies: only the
    * files on the package's surface, and no source locked with
    * `#(authorize) false` (its fields and schema go with it). The whole set is
    * for a host that indexes, and asks for it.
    */
   public getSurfaceEntities(): ApiModelEntities[] {
      return (this.entities ?? []).flatMap((model) => {
         const file = this.models.get(model.modelPath);
         try {
            file?.assertFileOnSurface();
         } catch {
            return [];
         }
         // The file's export closure, as the model GET limits it: a source it
         // imports or leaves unexported is not shown, nor one locked with an
         // unconditional deny.
         const published = file?.getPublishedNames();
         const shown = (name: string) => {
            const root = splitSourcePath(name).rootSourceName;
            return (
               (published === undefined || published.has(root)) &&
               !locked.has(root)
            );
         };
         const locked = new Set(
            (model.sources ?? [])
               .filter((s) =>
                  isUnconditionalDenyAuthorize({
                     authorize: (s.authorize ?? [])
                        .filter((g) => g.route === "authorize")
                        .map((g) => g.expression),
                     accessFilter: (s.authorize ?? [])
                        .filter((g) => g.route === "access_filter")
                        .map((g) => g.expression),
                  }),
               )
               .map((s) => splitSourcePath(s.name).rootSourceName),
         );
         return [
            {
               ...model,
               fields: model.fields?.filter((f) => shown(f.source)),
               sources: model.sources?.filter((s) => shown(s.name)),
               sourceSchemas: model.sourceSchemas?.filter((s) => shown(s.name)),
            },
         ];
      });
   }

   /**
    * What the entities extraction asks this package's query route, so it
    * reports what the route decides rather than a copy of the rule: each
    * source's queryability is {@link Model.admitsSourceByName}, and a served
    * report's sources name the served source they are or are built on.
    *
    * A file this package holds no model for (the extraction is only ever
    * handed this package's models; a spec may hand it others) is answered as
    * a standalone model is: admitted without a boundary, refused under one,
    * where it is no entry point.
    */
   public entitiesQueryRoute(): EntitiesQueryRoute {
      const boundaryActive = this.queryBoundaryActive();
      return {
         boundaryActive,
         admitsSource: (modelPath, sourceName) => {
            const model = this.models.get(modelPath);
            if (!model) return !boundaryActive;
            return model.admitsSourceByName(sourceName);
         },
         isServedReport: (modelPath) => this.isServedReport(modelPath),
         servedSourceOf: (modelPath, sourceName) => {
            const model = this.models.get(modelPath);
            if (!model) return undefined;
            const served = (name: string) => {
               const identity = model.definitionIdentity(name);
               return identity
                  ? this.servedSourceAddresses.get(identity)
                  : undefined;
            };
            // The very declaration a listed file exports, under any name
            // (`import { o is orders }` included)...
            const direct = served(sourceName);
            if (direct) return direct;
            // ...else the one served source it is built on. A compiled file
            // declares each name once, so the chain reaches one base.
            const [base] = model.curatedBasesOf(sourceName) ?? [];
            return base === undefined ? undefined : served(base);
         },
      };
   }

   /**
    * Compute {@link entities} from the models now installed. An annotation an
    * index cannot honor is left out of them and reported as a package warning
    * with a stable code, so the package still loads and a host that indexes can
    * refuse it at its own publish.
    *
    * `plans` are the load worker's ({@link Package.entityPlansOf}); a file
    * without one is planned here, on the main thread.
    */
   private async computeEntities(
      plans?: ReadonlyMap<string, ModelEntitiesPlan>,
   ): Promise<void> {
      const { results, findings, plannedHere } = await extractPackageEntities({
         packageRoot: this.packagePath,
         packageName: this.packageName,
         modelFiles: await this.entitiesModelFiles(),
         queryRoute: this.entitiesQueryRoute(),
         connections: this.malloyConfig.connections,
         plans,
      });
      this.entitiesPlannedHere = Object.freeze(plannedHere);
      this.entitiesWarnings = findings.map((finding) => ({
         model: finding.modelPath,
         message: finding.message,
         severity: "warn" as const,
         code: entitiesFindingCode(finding.reason),
      }));
      if (findings.length > 0) {
         logger.warn("Package declares annotations an index cannot honor", {
            packageName: this.packageName,
            dryRun: this.dryRun,
            reasons: findings.map((f) => f.reason),
         });
         if (!this.dryRun) {
            recordEntitiesFindings(findings.map((f) => f.reason));
         }
      }
      this.entities = Object.freeze(toApiEntities(results));
      logger.info("Package entities computed", {
         packageName: this.packageName,
         models: this.entities.length,
         failedModels: this.entities.filter((m) => m.error !== undefined)
            .length,
         fields: this.entities.reduce((n, m) => n + (m.fields?.length ?? 0), 0),
         fingerprintedDimensions: this.entities.reduce(
            (n, m) =>
               n + (m.fields?.filter((f) => f.indexFingerprint).length ?? 0),
            0,
         ),
         // Files the worker left (past its budget) are planned on this thread.
         plannedOnMainThread: plannedHere.length,
         workerPlanMs: [...(plans?.values() ?? [])].reduce(
            (ms, plan) =>
               ms + (plan.status === "planned" ? plan.durationMs : 0),
            0,
         ),
      });
   }

   /**
    * The compiled files whose {@link entities} were planned on the main
    * thread at load, rather than by the load worker: none, unless the worker
    * ran past its budget or a plan did not hold for the query route.
    */
   public getEntitiesPlannedOnMainThread(): readonly string[] {
      return this.entitiesPlannedHere;
   }

   /**
    * Every `.malloy` model now installed, as the entities extraction takes it:
    * compiled, with the text it compiled from, or the reason it did not.
    */
   public async entitiesModelFiles(): Promise<EntitiesModelFile[]> {
      const modelFiles: EntitiesModelFile[] = [];
      for (const [modelPath, model] of this.models) {
         if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
         const absolutePath = path.join(this.packagePath, modelPath);
         const modelDef = model.getModelDef();
         const compilationError = model.getCompilationError();
         if (compilationError !== undefined || modelDef === undefined) {
            modelFiles.push({
               relativePath: modelPath,
               absolutePath,
               error: compilationError?.message ?? "The model did not compile.",
            });
            continue;
         }
         modelFiles.push({
            relativePath: modelPath,
            absolutePath,
            model: new MalloyModel(modelDef, [], []),
            // The text the model compiled from, so a field's code is sliced
            // from the revision its range describes.
            text:
               model.getCompiledSourceText() ??
               (await fs.readFile(absolutePath, "utf-8")),
         });
      }
      return modelFiles;
   }

   /** The load worker's entities plans in `outcome`, by model path. */
   private static entityPlansOf(
      outcome: LoadPackageOutcome,
   ): Map<string, ModelEntitiesPlan> {
      const plans = new Map<string, ModelEntitiesPlan>();
      for (const model of outcome.models) {
         if (model.entities) plans.set(model.modelPath, model.entities);
      }
      return plans;
   }

   /** How this package is searched and indexed (publisher.json `retrieval`). */
   public getRetrievalSettings(): PackageRetrievalSettings {
      return this.retrievalSettings;
   }

   /**
    * The `#(secure)` givens of the models and notebooks now installed, or
    * `undefined` when that is unknown (see {@link summarizeSecureGivens}).
    * Computed on first read after `models` is replaced and then returned by
    * reference: the array is frozen, so callers share it.
    */
   private currentSecureGivens(): readonly ApiSecureGivenSummary[] | undefined {
      const memo = this.secureGivensMemo;
      if (memo?.forModels === this.models) return memo.value;
      const forModels = this.models;
      const value = summarizeSecureGivens(
         [...forModels.entries()].map(([modelPath, model]) => ({
            modelPath,
            givens: model.getGivens(),
            compiled: model.getCompilationError() === undefined,
         })),
         { packageName: this.packageName },
      );
      this.secureGivensMemo = { forModels, value };
      return value;
   }

   /**
   /** {@link classificationWarnings}, memoized: it runs on every `/status` poll. */
   private classificationWarningsMemoized(): ApiPackageWarning[] {
      const memo = this.currentClassificationMemo();
      memo.warnings ??= classificationWarnings(this.models, (modelPath) =>
         this.isListedModelPath(modelPath),
      );
      return memo.warnings;
   }

   private currentClassificationMemo(): ClassificationMemo {
      const listing = JSON.stringify([
         this.packageMetadata.explores ?? null,
         this.packageMetadata.queryableSources ?? null,
      ]);
      const memo = this.classificationMemo;
      if (memo?.forModels === this.models && memo.listing === listing) {
         return memo;
      }
      return (this.classificationMemo = {
         forModels: this.models,
         listing,
      });
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
      };
      if (isPackageInlineFieldsEnabled()) {
         // The deprecated inline fields, added while
         // PUBLISHER_PACKAGE_INLINE_FIELDS is on: the serving and binding
         // fields (getOperationalFields, also at `GET …/packages/{pkg}/status`)
         // and the build plan and agents (also at their own routes).
         Object.assign(metadata, this.getOperationalFields(), {
            buildPlan: this.buildPlan,
            // Overlaid so a metadata PATCH body can neither set nor wipe it.
            agents:
               this.rawAgents === undefined ? undefined : this.listAgents(),
         });
         // Never stored in `packageMetadata` (see storableMetadata): memoized
         // per models map and frozen, so it is returned by reference. Left
         // off, not `[]`, when it is unknown.
         const secureGivens = this.currentSecureGivens();
         if (secureGivens !== undefined) {
            metadata.secureGivens = secureGivens;
         }
      }
      const warnings = this.exploreWarnings();
      if (warnings.length > 0) {
         metadata.exploresWarnings = warnings;
      }
      // Every advisory finding shares one warnings array. Each must ride the
      // API because the load tolerates it, so otherwise only the log says so.
      const allWarnings = [
         ...this.renderTagWarnings,
         ...this.entitiesWarnings,
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
         ...this.materialization.storageWarnings(),
         ...this.materialization.storedColumnTypeWarnings(),
         ...this.materialization.servedCopyWarnings(),
         ...this.materialization.droppedPersistWarnings(),
         ...this.materialization.rollupsOverStoredBaseWarnings(),
         ...this.materialization.repeatedJoinOneKeyWarnings(),
         ...this.materialization.annotationFindingWarnings(),
         ...this.annotationNearMissWarnings(),
         ...this.materialization.persistRefusalWarnings(),
         // One verdict pass: unreachable, and the deployment facts (P13).
         ...this.materialization.verdictWarnings(),
         // Not in exploresWarnings: an import-only file must not block publish.
         ...this.emptyDiscoveryWarnings(),
         ...deprecatedFormatWarnings(this.models),
         ...this.classificationWarningsMemoized(),
         ...this.joinBypassWarnings(),
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
         ...this.materialization.incrementalPolicyAdvisories(),
         ...materializationConfigWarnings({
            declarations: this.queryMetadataDeclarations(),
            effectiveMerges: this.queryMetadataEffectiveMerges(),
            manifestWarnings: this.manifestWarnings,
         }),
      ];
      if (allWarnings.length > 0) {
         metadata.warnings = allWarnings;
      }
      return metadata;
   }

   /**
    * What this server is serving for the package right now: the load's
    * identity, its manifest binding and the sources it serves from storage.
    * Served at `GET …/packages/{pkg}/status`, which is where an orchestrator
    * reads them, and on a package response only while
    * PUBLISHER_PACKAGE_INLINE_FIELDS is on. Cheap: every field is a value the
    * load or the last bind recorded.
    */
   public getOperationalFields(): Pick<
      ApiPackage,
      | "manifestBindingStatus"
      | "manifestEntryCount"
      | "boundManifestUri"
      | "servedRevision"
      | "sourceContentSha"
      | "storageServeBindings"
   > {
      const binding = this.materialization.bindingState();
      return {
         manifestBindingStatus: binding.manifestBindingStatus,
         manifestEntryCount: binding.manifestEntryCount,
         boundManifestUri: binding.boundManifestUri,
         servedRevision: this.servedRevision,
         sourceContentSha: this.sourceContentSha,
         ...(binding.storageServeBindings.length > 0 && {
            storageServeBindings: binding.storageServeBindings,
         }),
      };
   }

   /** The bound manifest entries, or undefined when serving live; reused by /compile. */
   public getBuildManifestEntries(): BuildManifest["entries"] | undefined {
      return this.materialization.getBuildManifestEntries();
   }

   /** See {@link PackageMaterialization.getFreshBuildManifest}. */
   public getFreshBuildManifest(): BuildManifest["entries"] | undefined {
      return this.materialization.getFreshBuildManifest();
   }

   /** See {@link PackageMaterialization.hasBoundTableNameManifest}. */
   public hasBoundTableNameManifest(): boolean {
      return this.materialization.hasBoundTableNameManifest();
   }

   /** See {@link PackageMaterialization.hasStorageServeBindings}. */
   public hasStorageServeBindings(): boolean {
      return this.materialization.hasStorageServeBindings();
   }

   /** See {@link PackageMaterialization.getPreaggregateEntityIds}. */
   public getPreaggregateEntityIds(): ReadonlySet<string> {
      return this.materialization.getPreaggregateEntityIds();
   }

   /** See {@link PackageMaterialization.markManifestBound}. */
   public markManifestBound(uri: string): void {
      this.materialization.markManifestBound(uri);
   }

   /** See {@link PackageMaterialization.markManifestBindFailed}. */
   public markManifestBindFailed(): void {
      this.materialization.markManifestBindFailed();
   }

   /** See {@link PackageMaterialization.bindColocatedServeManifest}. */
   public bindColocatedServeManifest(entries: FreshnessManifest): void {
      this.materialization.bindColocatedServeManifest(entries);
   }

   /** See {@link PackageMaterialization.forgetKeyUniqueness}. */
   public forgetKeyUniqueness(sourceEntityIds: Iterable<string>): void {
      this.materialization.forgetKeyUniqueness(sourceEntityIds);
   }

   /** See {@link PackageMaterialization.bindStorageServeBindings}. */
   public bindStorageServeBindings(
      entries: Record<string, ManifestEntry>,
   ): void {
      this.materialization.bindStorageServeBindings(entries);
   }

   /** See {@link PackageMaterialization.setServeDestinationSchema}. */
   public setServeDestinationSchema(
      resolver: (destinationName: string) => string | undefined,
   ): void {
      this.materialization.setServeDestinationSchema(resolver);
   }

   /** See {@link PackageMaterialization.setDeploymentFacts}. */
   public setDeploymentFacts(facts: DeploymentFacts): void {
      this.materialization.setDeploymentFacts(facts);
   }

   /** See {@link PackageMaterialization.setServeDestinationConfig}. */
   public setServeDestinationConfig(provider: () => MalloyConfig): void {
      this.materialization.setServeDestinationConfig(provider);
   }

   /** See {@link PackageMaterialization.invalidateServeShapes}. */
   public invalidateServeShapes(): void {
      this.materialization.invalidateServeShapes();
   }

   /** See {@link PackageMaterialization.markBuildPlanStale}. */
   public markBuildPlanStale(changedConnections: Iterable<string> = []): void {
      this.materialization.markBuildPlanStale(changedConnections);
   }

   /** See {@link PackageMaterialization.lastBuildPlanRefreshFailedTransiently}. */
   public lastBuildPlanRefreshFailedTransiently(): boolean {
      return this.materialization.lastBuildPlanRefreshFailedTransiently();
   }

   /** See {@link PackageMaterialization.isBuildPlanStale}. */
   public isBuildPlanStale(): boolean {
      return this.materialization.isBuildPlanStale();
   }

   /** See {@link PackageMaterialization.planConnectionNames}. */
   public planConnectionNames(): ReadonlySet<string> | null {
      return this.materialization.planConnectionNames();
   }

   /**
    * Re-derive a stale build plan (see {@link PackageMaterialization.refreshBuildPlan})
    * in the load worker pool, as a load derives it, so a connection edit's
    * recompile stays off the main thread, at background priority.
    * `retryFailed` re-derives a plan a failed re-derivation left unavailable.
    * Resolves true when it was derived.
    */
   public refreshBuildPlan(
      options: { retryFailed?: boolean } = {},
   ): Promise<boolean> {
      return this.materialization.refreshBuildPlan(async () => {
         const outcome = await loadOutcomeOrThrow(
            getPackageLoadPool().loadPackage({
               packagePath: this.packagePath,
               packageName: this.packageName,
               environmentName: this.environmentName,
               malloyConfig: this.malloyConfig,
               defaultConnectionName: "duckdb",
               computeBuildPlan: true,
               // Behind publishes, reloads and compiles: a connection edit's
               // re-derivations never hold a user's load up for more than
               // the one already running.
               background: true,
            }),
         );
         return outcome.buildPlan;
      }, options);
   }

   /** See {@link PackageMaterialization.whenBuildPlanCurrent}. */
   public whenBuildPlanCurrent(timeoutMs?: number): Promise<void> {
      return this.materialization.whenBuildPlanCurrent(timeoutMs);
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

   /** See {@link PackageMaterialization.persistencePolicyWarnings}. */
   public persistencePolicyWarnings(): string[] {
      return this.materialization.persistencePolicyWarnings();
   }

   /** See {@link PackageMaterialization.formatInvalidPersistencePolicy}. */
   public formatInvalidPersistencePolicy(): string {
      return this.materialization.formatInvalidPersistencePolicy();
   }

   /** See {@link PackageMaterialization.incrementalPolicyWarnings}. */
   public incrementalPolicyWarnings(): string[] {
      return this.materialization.incrementalPolicyWarnings();
   }

   /** See {@link PackageMaterialization.formatInvalidIncrementalPolicy}. */
   public formatInvalidIncrementalPolicy(): string {
      return this.materialization.formatInvalidIncrementalPolicy();
   }

   /**
    * Which sources the load gates judge: the published surface, or every
    * top-level source of every model under `queryableSources: "all"`, where a
    * hidden source answers a query by name.
    */
   private gateScope(): {
      isListed: (modelPath: string) => boolean;
      includeHidden: boolean;
   } {
      if (this.packageMetadata.queryableSources === "all") {
         return {
            isListed: (modelPath) => modelPath.endsWith(MODEL_FILE_SUFFIX),
            includeHidden: true,
         };
      }
      return {
         isListed: (modelPath) => this.isListedModelPath(modelPath),
         includeHidden: false,
      };
   }

   /**
    * REJECTION messages for every field a `#(carries)` surface exposes outside
    * its list (docs/classification.md), one per line, empty when there are
    * none. Strict at load, like an invalid incremental policy: a surface that
    * says what it carries must not serve what it does not.
    */
   public formatClassificationLeaks(): string {
      const { isListed, includeHidden } = this.gateScope();
      return classificationLeaks(this.models, isListed, includeHidden).join(
         "\n",
      );
   }

   /**
    * REJECTION messages, one per model, for a join written inside a query on
    * an array in a model that governs access ({@link governsAccess}): no
    * check of Publisher's can follow what that join reads. Every model is
    * read, listed or not, since a published source can import a hidden one.
    */
   public formatArrayQueryJoins(): string {
      const messages: string[] = [];
      for (const [modelPath, model] of this.models) {
         const modelDef = model.getModelDef();
         const joins = arrayQueryJoins(modelDef);
         if (joins.length === 0 || !governsAccess(modelDef)) continue;
         const refusal = arrayQueryJoinRefusal(`Model ${modelPath}`, joins);
         if (refusal) messages.push(refusal);
      }
      return messages.join("\n");
   }

   /** The async input of {@link formatCarriesCeilingLeaks}, prepared before a listing is judged. */
   public prepareCeilingQueries(): Promise<PreparedCeilingQueries> {
      return prepareCeilingQueries(this.models);
   }

   /** REJECTION messages for every author query that reads past the `#(carries)` of its source, one per line. */
   public formatCarriesCeilingLeaks(prepared: PreparedCeilingQueries): string {
      const { isListed } = this.gateScope();
      return carriesCeilingLeaks(prepared, isListed).join("\n");
   }

   /**
    * `judge`'s result with `listing` in force, the package's own put back
    * before it returns. `judge` must be synchronous: nothing can be served
    * under a listing that is in force only inside one synchronous step.
    */
   public underListing<T>(
      listing: Pick<ApiPackage, "explores" | "queryableSources"> | undefined,
      judge: () => T,
   ): T {
      if (!listing) return judge();
      const existing = this.packageMetadata;
      const memo = this.classificationMemo;
      this.setPackageMetadata({ ...existing, ...listing });
      try {
         return judge();
      } finally {
         this.setPackageMetadata(existing);
         this.classificationMemo = memo;
      }
   }

   /** {@link joinBypasses}, memoized per models map and listing. */
   private joinBypassFindings(): string[] {
      const memo = this.currentClassificationMemo();
      if (!memo.joinBypass) {
         const { isListed, includeHidden } = this.gateScope();
         memo.joinBypass = joinBypasses(this.models, isListed, includeHidden);
      }
      return memo.joinBypass;
   }

   /**
    * The join bypasses of every published source, as `join-bypass` warnings:
    * each gate governs its own entry point, so a join from a broader one is the
    * author's choice of what that entry exposes.
    */
   public joinBypassWarnings(): ApiPackageWarning[] {
      return this.joinBypassFindings().map((message) => ({
         code: JOIN_BYPASS,
         severity: "warn" as const,
         message: isTruncatedWalkFinding(message)
            ? message
            : `${message} ${JOIN_BYPASS_NOTE}`,
      }));
   }

   /** See {@link PackageMaterialization.formatInvalidPreaggregatePolicy}. */
   public formatInvalidPreaggregatePolicy(): string {
      return this.materialization.formatInvalidPreaggregatePolicy();
   }

   /** See {@link PackageMaterialization.formatUnparseableAnnotations}. */
   public formatUnparseableAnnotations(): string {
      return this.materialization.formatUnparseableAnnotations();
   }

   /**
    * The coded `#@` annotation findings this package carries, as the warnings
    * clients see, for the findings metric: derived from the warnings so the
    * codes are declared once, where each warning is made.
    */
   public annotationFindingWarnings(): ApiPackageWarning[] {
      return [
         ...this.materialization.droppedPersistWarnings(),
         ...this.materialization.annotationFindingWarnings(),
         ...this.materialization.rollupsOverStoredBaseWarnings(),
         // Not materialization-unreachable: it reports what the surface
         // curates, not an annotation finding.
         ...this.preaggregateAccessWarnings(),
         ...this.annotationNearMissWarnings(),
         // The incremental declaration's own advisory is about the annotation;
         // the shared-address one is about the SQL, so it is not counted.
         ...this.materialization
            .incrementalPolicyAdvisories()
            .filter((w) => w.code === INCREMENTAL_NO_MERGE_KEY_CODE),
      ].filter((warning) => warning.code !== undefined);
   }

   /**
    * Annotations the package's model files wrote that do nothing: a near miss
    * of a route Publisher owns (`annotation-near-miss`), or a prefix Malloy
    * cannot route (`annotation-malformed-route`). Warnings, never refusals.
    * Each model reports only the notes in its own file, so a shared import
    * is reported once.
    */
   public annotationNearMissWarnings(): ApiPackageWarning[] {
      const warnings: ApiPackageWarning[] = [];
      for (const [modelPath, model] of this.models) {
         // Optional: a model a spec stands in for need not implement it.
         for (const finding of model.annotationNearMisses?.() ?? []) {
            const line = finding.at
               ? ` (line ${finding.at.range.start.line + 1})`
               : "";
            warnings.push({
               model: modelPath,
               ...(finding.subject ? { subject: finding.subject } : {}),
               message: `${finding.message}${line}; ${finding.rewrite}`,
               code: finding.code,
               severity: findingSeverity(finding),
            });
         }
      }
      return warnings;
   }

   /**
    * Each materialized source's serve verdict, as the binding stands now (see
    * {@link PackageMaterialization.serveVerdicts}). Served at
    * `GET …/packages/{pkg}/status`: the entry-point walk is memoized per load,
    * so a poll only loops over the plan.
    */
   public getServeVerdicts(): ApiServeVerdict[] {
      return this.materialization.serveVerdicts();
   }

   /**
    * {@link getServeVerdicts} with each stored source's named views compiled
    * against a serve shape declared from the plan, before anything is built
    * (ms2data/service#7090). For a dry run: it compiles.
    */
   public async previewServeVerdicts(): Promise<ApiServeVerdict[]> {
      return this.materialization.previewServeVerdicts();
   }

   /** See {@link PackageMaterialization.preaggregateAccessWarnings}. */
   public preaggregateAccessWarnings(): ApiPackageWarning[] {
      return this.materialization.preaggregateAccessWarnings();
   }

   /** See {@link PackageMaterialization.persistenceCollisionWarnings}. */
   public persistenceCollisionWarnings(): string[] {
      return this.materialization.persistenceCollisionWarnings();
   }

   /** See {@link PackageMaterialization.formatPersistenceCollisionRejections}. */
   public formatPersistenceCollisionRejections(): string {
      return this.materialization.formatPersistenceCollisionRejections();
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
    *
    * Installs like {@link loadViaWorker} with three deliberate differences:
    * the build plan, the load gates and the entities are a load's alone (a
    * reload keeps the ones the package loaded with), and a reload re-reads the
    * surface from publisher.json and re-applies serve routing to the new
    * models, which a fresh package has none of yet.
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

      // The new models' own policy, undecided until they are installed below.
      const nextModelPolicy = new PackageModelPolicy();
      const { models: nextModels, renderTagWarnings } =
         await Package.hydrateWorkerModels(
            outcome,
            {
               packageName: this.packageName,
               packagePath: this.packagePath,
               malloyConfig: this.malloyConfig,
               buildManifest,
               modelPolicy: nextModelPolicy,
            },
            "placeholder",
         );
      // Discover before installing, then install models and reports in one
      // synchronous step: a query must never see one without the other.
      const discoveryStarted = performance.now();
      const discovery = await this.discoverReportsIn(nextModels, "reload");
      const discoveryMs = performance.now() - discoveryStarted;
      this.models = nextModels;
      this.modelPolicy = nextModelPolicy;
      this.adoptOutcomeMetadata(outcome);
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
      this.materialization.pushStorageServeBindingsToModels();
      this.materialization.pushServeDestinationConfigToModels();
      // Read against the reports installed above.
      this.renderTagWarnings =
         this.surfacedRenderTagWarnings(renderTagWarnings);
      // After installReports: the tile lint asks the refreshed boundary.
      const lintStarted = performance.now();
      await this.relintReports({ textLint: true });
      this.reportDiscovered(
         "reload",
         discovery,
         discoveryMs + performance.now() - lintStarted,
      );
      this.materialization.recordManifestBinding(entries);
      this.materialization.wireFreshnessResolvers();
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

   /**
    * Whether `listModels()` lists a model: a `.malloy` file that is not a
    * report, on the surface when the package has one (or hidden too, with
    * `includeHidden`). The one answer to "is this model published", shared
    * with the lints that read only published models.
    */
   private isListedModelPath(
      modelPath: string,
      includeHidden: boolean = false,
   ): boolean {
      const exploreSet = this.exploreSet();
      if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) return false;
      // A linear report is listed as a report, surface or not.
      if (this.getReportLayout(modelPath) === "linear") return false;
      // A grid report is listed as a report, even when `explores` lists it.
      if (!exploreSet) return true;
      if (this.isServedReport(modelPath)) return false;
      return includeHidden || exploreSet.has(modelPath);
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
            .filter((modelPath) =>
               this.isListedModelPath(
                  modelPath,
                  options.includeHiddenFilesAndSources,
               ),
            )
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
      return queryBoundaryActive(this.packageMetadata);
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

   /**
    * Take the publisher.json settings a load or reload read, and count the
    * deprecated features the installed models use. Before
    * `refreshServingIdentity`, which reads the agents.
    */
   private adoptOutcomeMetadata(outcome: LoadPackageOutcome): void {
      const metadata = outcome.packageMetadata;
      this.rawAgents = metadata.agents;
      this.manifestWarnings = metadata.manifestWarnings ?? [];
      this.retrievalSettings = metadata.retrieval ?? DEFAULT_PACKAGE_RETRIEVAL;
      if (this.dryRun) return;
      recordDeprecatedFormatUsage(this.models);
      recordDeprecatedManifestKeys(metadata.deprecatedManifestKeys);
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
            // malloy-text: a report tile's query, the author's Malloy, run as written
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
                   query: `run: ${quotedIdentifier(suggest.source)} -> { group_by: ${quotedPath(suggest.dimension)} }`,
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
      // That literal sits in a Malloy string, so the module escapes it
      // (quotes, backslashes and any control character a file name holds).
      const malloyString = stringLiteral(tableLiteral);
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

   /**
    * `packageMetadata` without the fields computed from the loaded package
    * (COMPUTED_PACKAGE_FIELDS), so a caller that round-trips
    * `getPackageMetadata()` (or a PATCH body) cannot store a snapshot that
    * outlives the load it describes.
    */
   private static storableMetadata(packageMetadata: ApiPackage): ApiPackage {
      const stored: Record<string, unknown> = { ...packageMetadata };
      for (const key of COMPUTED_PACKAGE_FIELDS) delete stored[key];
      return stored as ApiPackage;
   }

   public setPackageMetadata(packageMetadata: ApiPackage) {
      this.packageMetadata = Package.storableMetadata(packageMetadata);
      this.applyDiscoveryPolicyToModels();
      this.applyQueryBoundaryToModels();
      this.applySiblingModelResolverToModels();
   }
}
