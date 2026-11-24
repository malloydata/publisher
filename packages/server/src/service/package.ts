// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   bigqueryBuildCompareBinary,
   chainBuildStringReads,
   planChainBuildText,
} from "./materialization";
import type {
   BoundStorageColumns,
   BoundStorageContract,
   BoundTableContract,
   PlanChainBuildText,
   PlanChainStringReads,
} from "./materialization/domain/chain_build";
import { quotedIdentifier, quotedPath, stringLiteral } from "./malloy_text";
import { INCREMENTAL_NO_MERGE_KEY_CODE } from "./materialization/plan/incremental_policy";
import {
   managedSchemas,
   managedSchemasOf,
   recordConnectionType,
} from "./connection_type";
import { readUpstreamSchemas, withNameProblems } from "./materialization";
import {
   type UpstreamSchemaReadings,
   type UpstreamTable,
   upstreamSchemaFold,
   upstreamTableKey,
} from "./materialization/domain/upstream_schemas";
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
import { applyExtensionSessionSettings } from "./duckdb_session_setup";
import { authorSqlRefusals, sandboxedDuckDBOptions } from "./duckdb_lockdown";
import { closeMalloyConfig, ownDuckDBConnection } from "./duckdb_hard_close";
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
import type { KeptTable, LateRows } from "./materialization/domain/types";
import type {
   IncrementalCheckFinding,
   LedgerParent,
   UnprovenGeneration,
} from "./materialization/domain/incremental";
import { underCompilerKeys } from "./materialization/domain/binding";
import {
   confinedPackagePath,
   errMessage,
   listPackageTree,
   readPackageURL,
} from "../utils";
import { packageResource } from "../api_resource";
import { isPackageInlineFieldsEnabled } from "../config";
import type {
   DatabaseSourceEligibility,
   PersistNameProblem,
   SourceEligibility,
} from "./materialization/domain/package_plan";
import {
   PackageMaterialization,
   type PackageMaterializationView,
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
   noteGovernsAccess,
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
import { type ReportViewPackage } from "./legacy_report_views";
import { recordDeprecatedManifestKeys } from "./deprecated/manifest_keys";
import {
   isMalloynbPath,
   listMalloynbCellFindings,
   MALLOYNB_CELL_CHECK_FAILED,
   MALLOYNB_CELL_REFUSED,
   MALLOYNB_CELLS_LISTED,
} from "./deprecated/malloynb";
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
import { quoteManifestTablePath, UnquotableIdentifierError } from "./quoting";
import { Model } from "./model";
import {
   callerVisibleText,
   rewriteErrorMessage,
   withPackageRelativeErrors,
} from "../package_relative_text";
import {
   PackageModelPolicy,
   type OffSurfaceContext,
   type QueryBoundaryPolicy,
} from "./model_policy";
import {
   type ApiSecureGivenSummary,
   summarizeSecureGivens,
} from "./secure_givens";
import { persistNameProblems } from "./materialization/plan/persist_annotation_validation";
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
type ApiReportCells = components["schemas"]["ReportCells"];

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
    * This load is an explicit refresh (a reload, a publish): it reads its
    * upstream tables' column types from their catalogs again rather than
    * keep this process's readings (readUpstreamSchemas).
    */
   private refreshesUpstreamSchemas = false;
   /**
    * The package's materialization state: build plan, eligibility, bound
    * manifest, serve routing and persistence-policy findings.
    */
   private readonly mz = new PackageMaterialization({
      packageName: () => this.packageName,
      versionId: () => this.getVersionId(),
      metadata: () => this.packageMetadata,
      models: () => this.models,
      modelPolicy: () => this.modelPolicy,
      packagePath: () => this.packagePath,
      malloyConfig: () => this.malloyConfig,
      connection: (name) => this.getMalloyConnection(name),
      buildPlanPackage: this,
      publishedSurface: () => this.publishedSurface(),
   });
   /**
    * The package's materialization: its plan, bindings, verdicts and
    * warnings. Callers reach the facade through this one accessor.
    */
   public materialization(): PackageMaterializationView {
      return this.mz;
   }
   // The load path is the only writer of these; the accessors let a spec that
   // isolates a filter set them on the package.
   private get buildPlan(): BuildPlan | null {
      return this.mz.plan.buildPlan;
   }
   private set buildPlan(plan: BuildPlan | null) {
      this.mz.plan.buildPlan = plan;
   }
   protected set sourceEligibility(eligibility: SourceEligibility | undefined) {
      this.mz.plan.sourceEligibility = eligibility;
   }
   protected set databaseSourceEligibility(
      eligibility: DatabaseSourceEligibility | undefined,
   ) {
      this.mz.plan.databaseSourceEligibility = eligibility;
   }
   // Non-fatal render-tag findings across the models; a bad tag does not fail
   // the load, so this is its signal.
   private renderTagWarnings: ApiPackageWarning[] = [];
   /**
    * What the models were last compiled against (by {@link reloadAllModels},
    * or by a load given its manifest), so a rebind can skip the compile.
    * `compiledFor` is the part of the entries a compile reads
    * ({@link compileKeyOf}); `bound` the whole entries last bound (freshness
    * and schemas too), undefined while none has been; `quoted` the entries
    * the compile quoted, from `quotedFrom`. Every other part is compared by
    * identity: any other reload, database bind or plan change since makes
    * the next rebind recompile.
    */
   private reloadedFor:
      | {
           compiledFor: string;
           bound: string | undefined;
           quotedFrom: string;
           quoted: FreshnessManifest;
           models: Map<string, Model>;
           database: unknown;
           buildPlan: unknown;
           connectionEdits: number;
        }
      | undefined;
   /** Bumped by each connection edit {@link markBuildPlanStale} records. */
   private connectionEdits = 0;
   /** The annotations the entities extraction left out, as warnings. */
   private entitiesWarnings: ApiPackageWarning[] = [];
   private reportWarnings: ApiPackageWarning[] = [];
   /** Refused `.malloynb` cells the last lint named only in a summary warning. */
   private malloynbCellsUnlisted = 0;
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

   /** The `#@ persist name=` mistakes the raw text of its models shows. */
   private persistNameProblems: PersistNameProblem[] = [];

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
      options: {
         dryRun?: boolean;
         /**
          * The published version this load is, which names its tables per
          * version under `scope: "version"`: set before the load is judged, so
          * the collision check keys the names the version's runs write.
          */
         versionId?: string;
         /**
          * The database entries of the manifest the package is about to be
          * bound to, read while it compiles: its models are hydrated over
          * them, so the bind that follows (`reloadAllModels` with
          * `ifChanged`) binds without compiling the package a second time.
          * Undefined, or a read that fails, loads it as with none; nothing is
          * bound until that bind.
          */
         database?: Promise<FreshnessManifest | undefined>;
         /**
          * An explicit refresh (a reload, a publish): every table and SQL
          * schema the package reads is fetched again rather than read from
          * the schema caches, which keep it after.
          */
         refreshSchemas?: boolean;
      } = {},
   ): Promise<Package> {
      return withSpan(
         "publisher.package.load",
         {
            "malloy.environment": environmentName,
            "malloy.package": packageName,
            "publisher.package.dry_run": options.dryRun === true,
         },
         () =>
            // A failed load's message names the package's files
            // package-relative, whichever path loaded it.
            withPackageRelativeErrors(packagePath, () =>
               Package.load(
                  environmentName,
                  packageName,
                  packagePath,
                  environmentMalloyConfig,
                  cleanupDirectoryOnFailure,
                  options.dryRun === true,
                  options.versionId,
                  options.database,
                  options.refreshSchemas === true,
               ),
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
      versionId?: string,
      database?: Promise<FreshnessManifest | undefined>,
      refreshSchemas: boolean = false,
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
            versionId,
            database,
            refreshSchemas,
         );
      } catch (error) {
         // No caller ever holds a package whose load failed, so its own
         // connections (the package DuckDB the load opened) close here; the
         // environment's connections are not the package's and stay open.
         if (malloyConfig) {
            await closeMalloyConfig(malloyConfig).catch((shutdownError) => {
               logger.warn(
                  `Failed to close the connections of package ${packageName} after a failed load`,
                  { error: errMessage(shutdownError) },
               );
            });
         }
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
      // A model's compile error is read by every caller of the package (a
      // query's 424, get_context, the package's warnings): it names the
      // package's files package-relative and no other server path, whether
      // it is thrown or kept on a placeholder.
      const callerVisible = (err: Error): Error => {
         rewriteErrorMessage(err, (message) =>
            callerVisibleText(message, packagePath),
         );
         return err;
      };
      const placeholder = (
         sm: LoadPackageOutcome["models"][number],
         err: Error,
      ): Model =>
         Model.fromCompilationError(
            packageName,
            sm.modelPath,
            sm.modelType,
            callerVisible(err),
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
            const err = callerVisible(
               Model.deserializeCompilationError(sm.compilationError),
            );
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
         // The worker walked its own copy of the IR to answer this, so the
         // load gate and the governor never walk a model that governs nothing.
         if (sm.governsAccess !== undefined) {
            noteGovernsAccess(model.getModelDef(), sm.governsAccess);
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
      versionId?: string,
      database?: Promise<FreshnessManifest | undefined>,
      refreshSchemas: boolean = false,
   ): Promise<Package> {
      const pool = getPackageLoadPool();
      const dispatchTime = performance.now();
      // Wrapped on its own so the 503 mapping never touches readDatabases' errors.
      const workerOutcome = loadOutcomeOrThrow(
         pool.loadPackage({
            upstreamSchemas: readUpstreamSchemas,
            packagePath,
            packageName,
            environmentName,
            malloyConfig,
            defaultConnectionName: "duckdb",
            computeBuildPlan: true,
            withEntities: true,
            ...(versionId !== undefined ? { publishedVersion: true } : {}),
            ...(refreshSchemas ? { refreshSchemas: true } : {}),
         }),
      );
      const [outcome, databases, databaseEntries] = await Promise.all([
         workerOutcome,
         Package.readDatabases(packagePath, malloyConfig),
         // The bind that follows reports a failed read; this load serves live.
         database?.catch(() => undefined),
      ]);
      assertOutcomeStagesAvailable(outcome);
      const workerDoneTime = performance.now();
      // The worker compiled without them: a build manifest changes the SQL
      // a query generates (through the hydration runtime), never the IR a
      // compile produces, and the build plan describes the canonical build.
      // So hydrating over them serves what a reload against them serves.
      let compiledFor:
         | {
              key: string;
              requested: string;
              quoted: FreshnessManifest;
           }
         | undefined;
      if (databaseEntries && Object.keys(databaseEntries).length > 0) {
         compiledFor = {
            key: Package.compileKeyOf(databaseEntries),
            requested: JSON.stringify(databaseEntries),
            quoted: await Package.quoteTableNames(
               packageName,
               (connectionName) =>
                  malloyConfig.connections.lookupConnection(connectionName),
               databaseEntries,
               Package.planConnectionsOf(
                  outcome.buildPlan?.ok
                     ? outcome.buildPlan.packagePlan.plan
                     : null,
               ),
            ),
         };
      }
      // As Malloy reads it: each entry also under its compiler key.
      const buildManifest = compiledFor
         ? underCompilerKeys(
              { entries: toTableNameManifest(compiledFor.quoted) },
              outcome.buildPlan?.ok
                 ? outcome.buildPlan.packagePlan.compilerKeys
                 : undefined,
           ).entries
         : undefined;
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
      const nameProblems: PersistNameProblem[] = [];
      try {
         ({
            models,
            renderTagWarnings,
            timings: hydrationTimings,
         } = await Package.hydrateWorkerModels(
            outcome,
            {
               packageName,
               packagePath,
               malloyConfig,
               modelPolicy,
               buildManifest,
            },
            "throw",
            async (sm) => {
               // An unquoted or unsafe `#@ persist name=`, read from the raw
               // source, the ground truth for quoting; the load gate fails
               // with every one, beside the package's other invalid
               // annotations. `.malloy` only, because `#@ persist` cannot
               // appear in a `.malloynb`; widen this if that changes, to keep
               // the identifier-injection check.
               if (sm.modelPath.endsWith(MODEL_FILE_SUFFIX)) {
                  const modelSource = await readPackageURL(
                     packagePath,
                     pathToFileURL(path.join(packagePath, sm.modelPath)),
                  );
                  nameProblems.push(
                     ...persistNameProblems(modelSource, sm.modelPath),
                  );
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
      pkg.refreshesUpstreamSchemas = refreshSchemas;
      pkg.persistNameProblems = nameProblems;
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
         // Before the gates judge it: a version names its tables per version.
         if (versionId !== undefined) {
            pkg.updateStoredMetadata({ versionId });
         }
         // No manifest is bound yet, so this serves live until a bind.
         pkg.mz.wireServingResolvers();
         pkg.refreshServingIdentity();

         buildPlanMs = await pkg.mz.applyLoadBuildPlan(outcome.buildPlan);
         // Before any bind, so a database rollup bound later is routed.
         pkg.mz.pushServedCopyContextToModels();

         await applyLoadGates(pkg);
         const entitiesStarted = performance.now();
         await pkg.computeEntities(Package.entityPlansOf(outcome));
         entitiesMs = performance.now() - entitiesStarted;
         pkg.logEmptyDiscoveryWarnings();
         // Compiled against them, bound to none yet (see `database`).
         if (compiledFor) {
            pkg.reloadedFor = pkg.reloadKey(compiledFor.key, undefined, {
               from: compiledFor.requested,
               quoted: compiledFor.quoted,
            });
         }
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

   /** `getPackageMetadata().scope`, without building the rest of the metadata. */
   public getMaterializationScope(): string | null | undefined {
      return this.packageMetadata.scope;
   }

   /**
    * The build manifest the package is configured to bind, as
    * `getPackageMetadata().manifestLocation` reports it, without building the
    * rest of the metadata.
    */
   public getManifestLocation(): string | null | undefined {
      return this.packageMetadata.manifestLocation;
   }

   public getPackageName(): string {
      return this.packageName;
   }

   /**
    * The host port's `forgetKeyUniqueness`: a run about to write these
    * tables drops their bound key checks first (see
    * PackageMaterialization.forgetKeyUniqueness).
    */
   /** The host port's `boundLateRows` (PackageMaterialization.boundLateRows). */
   public boundLateRows(): ReadonlyMap<string, LateRows> {
      return this.mz.boundLateRows();
   }

   /** The host port's `boundParentReads` (PackageMaterialization.boundParentReads). */
   public boundParentReads(): ReadonlyMap<string, readonly LedgerParent[]> {
      return this.mz.boundParentReads();
   }

   /** The host port's `boundIncrementalEntries` (PackageMaterialization.boundIncrementalEntries). */
   public boundIncrementalEntries(): ReadonlyMap<
      string,
      Pick<ManifestEntry, "builtAt" | "ledger">
   > {
      return this.mz.boundIncrementalEntries();
   }

   /** The host port's `setUnprovenGenerations`. */
   public setUnprovenGenerations(list: readonly UnprovenGeneration[]): void {
      this.mz.setUnprovenGenerations(list);
   }

   public forgetKeyUniqueness(sourceEntityIds: Iterable<string>): void {
      this.mz.forgetKeyUniqueness(sourceEntityIds);
   }

   /** The host port's `boundTableNames`: every database table bound now. */
   public boundTableNames(): string[] {
      const names = new Set<string>();
      for (const entries of [
         this.mz.getBuildManifestEntries(),
         this.mz.boundDatabaseEntries(),
      ]) {
         for (const entry of Object.values(entries ?? {})) {
            if (entry.tableName) names.add(entry.tableName);
         }
      }
      return [...names];
   }

   /**
    * The host port's `noteIncrementalCheck`: what a run found of a source's
    * incremental checks (see PackageMaterialization.noteIncrementalCheck).
    */
   public noteIncrementalCheck(
      sourceEntityId: string,
      finding: IncrementalCheckFinding,
   ): void {
      this.mz.noteIncrementalCheck(sourceEntityId, finding);
   }

   /**
    * The host port's `setKeptTables`: the tables the newest auto-run lists as
    * kept after a rename (see PackageMaterialization.setKeptTables).
    */
   public setKeptTables(tables: readonly KeptTable[]): void {
      this.mz.setKeptTables(tables);
   }

   /** The host port's `incompatibleTables` (see PackageMaterialization.incompatibleTables). */
   public incompatibleTables(
      prior?: Readonly<Record<string, ManifestEntry>>,
   ): ReadonlySet<string> {
      return this.mz.incompatibleTables(prior);
   }

   /** The host port's `holdCarriedToContracts` (see PackageMaterialization.holdCarriedToContracts). */
   public holdCarriedToContracts(
      prior: Readonly<Record<string, ManifestEntry>>,
      site?: Parameters<PackageMaterialization["holdCarriedToContracts"]>[1],
      options?: Parameters<PackageMaterialization["holdCarriedToContracts"]>[2],
   ): ReturnType<PackageMaterialization["holdCarriedToContracts"]> {
      return this.mz.holdCarriedToContracts(prior, site, options);
   }

   /** The host port's `chainBuildText` (see PackageMaterialization.chainBuildText). */
   /** See {@link BuildEnvironment.reportIncompatible}. */
   public reportIncompatible(handles: readonly string[]): void {
      this.mz.reportIncompatible(handles);
   }

   public chainBuildText(request: ChainBuildRequest): ChainBuildText {
      return this.mz.chainBuildText(request);
   }

   /** See {@link BuildEnvironment.bigqueryBuildCompareBinary}. */
   public readonly bigqueryBuildCompareBinary = bigqueryBuildCompareBinary;

   /** The persist build plan, or null when the package declares no persist source. */
   public getBuildPlan(): BuildPlan | null {
      return this.mz.getBuildPlan();
   }

   /** Whether the build plan could not be derived at load, so `getBuildPlan` says nothing. */
   public isBuildPlanUnavailable(): boolean {
      return this.mz.isBuildPlanUnavailable();
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
               (await readPackageURL(
                  this.packagePath,
                  pathToFileURL(absolutePath),
               )),
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
         ...this.mz.storageWarnings(),
         ...this.mz.dataInliningWarnings(),
         ...this.mz.storedColumnTypeWarnings(),
         ...this.mz.nativeSchemaWarnings(),
         ...this.mz.databaseTableUnreadWarnings(),
         ...this.mz.servedCopyWarnings(),
         ...this.mz.repeatedJoinOneKeyWarnings(),
         ...this.mz.lateRowWarnings(),
         ...this.mz.incrementalCheckWarnings(),
         ...this.mz.unprovenGenerationWarnings(),
         ...this.mz.keptTableWarnings(),
         ...this.mz.annotationFindingWarnings(),
         ...this.annotationNearMissWarnings(),
         ...this.mz.persistRefusalWarnings(),
         ...this.mz.asOfBuildWarnings(),
         // One verdict pass: unreachable, and the deployment facts (P13).
         ...this.mz.verdictWarnings(),
         // Not in exploresWarnings: an import-only file must not block publish.
         ...this.emptyDiscoveryWarnings(),
         ...deprecatedFormatWarnings(this.models),
         ...this.classificationWarningsMemoized(),
         ...this.joinBypassWarnings(),
         ...this.gateLoadWarnings(),
         // The 404s a broken surface causes name models that are not broken.
         ...this.brokenSurfaceWarnings(),
         ...(this.surfaceWidenedWarning !== undefined
            ? [{ message: this.surfaceWidenedWarning }]
            : []),
         // A collision spans sources, so it has no subject; the message names
         // them. Only under PERSIST_COLLISION_ENFORCE=false: otherwise it
         // fails the load (formatInvalidAnnotations).
         ...this.mz.persistenceCollisionRefusals(),
         // Legal but probably unintended, e.g. a typo'd merge_key=.
         ...this.mz.incrementalPolicyAdvisories(),
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
      const binding = this.mz.bindingState();
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

   /** See {@link PackageMaterialization.markBuildPlanStale}. */
   public markBuildPlanStale(changedConnections: Iterable<string> = []): void {
      this.connectionEdits++;
      this.mz.markBuildPlanStale(changedConnections);
   }

   /**
    * Re-derive a stale build plan (see {@link PackageMaterialization.refreshBuildPlan})
    * in the load worker pool, as a load derives it, so a connection edit's
    * recompile stays off the main thread, at background priority.
    * `retryFailed` re-derives a plan a failed re-derivation left unavailable.
    * Resolves true when it was derived.
    */
   public refreshBuildPlan(
      options: { retryFailed?: boolean; isStale?: () => boolean } = {},
   ): Promise<boolean> {
      return this.mz.refreshBuildPlan(async () => {
         const outcome = await loadOutcomeOrThrow(
            getPackageLoadPool().loadPackage({
               upstreamSchemas: readUpstreamSchemas,
               packagePath: this.packagePath,
               packageName: this.packageName,
               environmentName: this.environmentName,
               malloyConfig: this.malloyConfig,
               defaultConnectionName: "duckdb",
               computeBuildPlan: true,
               ...(this.getVersionId() !== undefined
                  ? { publishedVersion: true }
                  : {}),
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
      return this.mz.whenBuildPlanCurrent(timeoutMs);
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
      return this.mz.persistencePolicyWarnings();
   }

   /** See {@link PackageMaterialization.formatInvalidPersistencePolicy}. */
   public formatInvalidPersistencePolicy(): string {
      return this.mz.formatInvalidPersistencePolicy();
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
         // Governance first, and most models govern nothing: the load worker
         // answered it for a model it compiled (noted at hydration), so such
         // a model costs a lookup here, where the join walk reads every node.
         if (!governsAccess(modelDef)) continue;
         const joins = arrayQueryJoins(modelDef);
         const refusal = arrayQueryJoinRefusal(`Model ${modelPath}`, joins);
         if (refusal) messages.push(refusal);
      }
      return messages.join("\n");
   }

   /**
    * Author SQL in this package's models that would not run read-only on a
    * DuckDB connection, one line per refusal (duckdb_lockdown.ts).
    */
   public async formatAuthorSqlRefusals(): Promise<string> {
      const lines: string[] = [];
      // A connection the model compiled against resolves; one that no longer
      // does is held to the narrowest managed scope, failing closed.
      const schemasOf = (connectionName: string) =>
         this.getMalloyConnection(connectionName).then(
            (connection) => managedSchemasOf(connection),
            () => managedSchemas(),
         );
      for (const [modelPath, model] of this.models) {
         lines.push(
            ...(await authorSqlRefusals(modelPath, model.getModelDef(), {
               managedSchemas: schemasOf,
            })),
         );
      }
      return lines.join("\n");
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

   /**
    * Every invalid materialization annotation, one per line, or "": the
    * package's own ({@link PackageMaterialization.formatInvalidAnnotations})
    * and a misspelled materialization route or directive. Fails the publish
    * and the load.
    */
   public formatInvalidAnnotations(): string {
      return withNameProblems(this.persistNameProblems, [
         this.mz.formatInvalidAnnotations(),
         ...this.mz.nearMissLines(),
      ]);
   }

   /**
    * The load gate's text: every invalid materialization annotation in the
    * package, one line each with its code, or "" when it is valid. The load
    * fails with it; a run over the package's files, a re-derivation and a
    * package compile read the same text off the plan they derive
    * (plan/load_gate_text.ts).
    */
   public formatLoadGateText(): string {
      return this.mz.loadGateText(this.persistNameProblems);
   }

   /**
    * One warning per non-fatal gate finding a model's load made: a gate an
    * entry point cannot express (it denies every request), or one that reads
    * no given or negates a membership test. Logged at load as well.
    */
   private gateLoadWarnings(): ApiPackageWarning[] {
      // A source several models import is checked in each; it is reported
      // once, on the first model that found it.
      const seen = new Set<string>();
      const warnings: ApiPackageWarning[] = [];
      for (const [modelPath, model] of this.models) {
         for (const w of model.gateLoadWarnings()) {
            const key = `${w.code}\u0000${w.sourceName}\u0000${w.message}`;
            if (seen.has(key)) continue;
            seen.add(key);
            warnings.push({
               model: modelPath,
               subject: w.sourceName,
               code: w.code,
               severity: "warn",
               message: w.message,
            });
         }
      }
      return warnings;
   }

   /**
    * The coded `#@` annotation findings this package carries, as the warnings
    * clients see, for the findings metric: derived from the warnings so the
    * codes are declared once, where each warning is made.
    */
   public annotationFindingWarnings(): ApiPackageWarning[] {
      return [
         ...this.mz.annotationFindingWarnings(),
         ...this.mz.findingRefusalWarnings(),
         ...this.annotationNearMissWarnings(),
         // The incremental declaration's own advisory is about the annotation;
         // the shared-address one is about the SQL, so it is not counted.
         ...this.mz
            .incrementalPolicyAdvisories()
            .filter((w) => w.code === INCREMENTAL_NO_MERGE_KEY_CODE),
      ].filter((warning) => warning.code !== undefined);
   }

   /**
    * Annotations the package's model files wrote that do nothing: a near miss
    * of a route Publisher owns (`annotation-near-miss`), or a prefix Malloy
    * cannot route (`annotation-malformed-route`). Warnings, never refusals; a
    * near miss of a materialization route (`materialization-near-miss`) fails
    * the load instead (see {@link formatInvalidAnnotations}). Each model
    * reports only the notes in its own file, so a shared import is reported
    * once.
    */
   public annotationNearMissWarnings(): ApiPackageWarning[] {
      return this.nearMissWarnings().filter((w) => w.severity !== "error");
   }

   private nearMissWarnings(): ApiPackageWarning[] {
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
    * {@link getServeVerdicts} with each stored source's named views compiled
    * against a compile-only served copy over bindings declared from the
    * plan's output columns, before anything is built. For a dry run: it
    * compiles.
    */
   public async previewServeVerdicts(): Promise<ApiServeVerdict[]> {
      return this.mz.previewServeVerdicts();
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
    * After a reload or publish of this package failed to compile, so this
    * copy keeps serving: read again the tables its plan's sources read, and
    * stop serving the stored tables of every source a column of whose
    * tables that its build SQL names changed or is gone (its address would
    * move), so they answer live as the warehouse now stands. A source whose tables did not change, or whose
    * tables cannot be known (no `reads`), keeps serving. A read that fails
    * keeps the table's last reading, so it changes nothing. Returns the
    * names of the sources it stopped serving, with the tables that changed.
    */
   public async withdrawChangedUpstreams(): Promise<{
      sources: string[];
      tables: string[];
   }> {
      const known = this.mz.plan.upstreamSchemas;
      const tables = Object.keys(known).map((key) => {
         const [connectionName, table] = JSON.parse(key) as [string, string];
         return { connectionName, table };
      });
      if (tables.length === 0) return { sources: [], tables: [] };
      const now = await readUpstreamSchemas({
         tables,
         connection: (name) => this.getMalloyConnection(name),
         refresh: true,
      });
      // A source moves when a column its build SQL names changed or is
      // gone, as its address would (upstreamSchemaFold).
      const moved = (reads: UpstreamTable[], sql: string) =>
         upstreamSchemaFold(reads, known, sql) !==
         upstreamSchemaFold(reads, now, sql);
      const withdrawn = Object.entries(
         this.mz.plan.servingPlan()?.sources ?? {},
      ).filter(
         ([, source]) =>
            source.reads !== undefined && moved(source.reads, source.sql),
      );
      const names = [...new Set(withdrawn.map(([, s]) => s.name))].sort();
      const tableNames = [
         ...new Set(
            withdrawn.flatMap(([, source]) =>
               (source.reads ?? [])
                  .filter((r) => moved([r], source.sql))
                  .map((r) => r.table),
            ),
         ),
      ].sort();
      this.mz.plan.withdraw(withdrawn.map(([sourceID]) => sourceID));
      return { sources: names, tables: tableNames };
   }

   /** See {@link BuildPlanPackage.readUpstreamSchemas}. */
   public async readUpstreamSchemas(
      tables: UpstreamTable[],
      use: "load" | "run",
   ): Promise<UpstreamSchemaReadings> {
      const connection = (name: string) => this.getMalloyConnection(name);
      if (use === "load") {
         return readUpstreamSchemas({
            tables,
            connection,
            refresh: this.refreshesUpstreamSchemas,
         });
      }
      // A run addresses each source as the plan in force does: its load's
      // readings, and a reading only of a table that plan did not read.
      const known = this.mz.plan.upstreamSchemas;
      const unknown = tables.filter((t) => !(upstreamTableKey(t) in known));
      const read = unknown.length
         ? await readUpstreamSchemas({
              tables: unknown,
              connection,
              refresh: false,
           })
         : {};
      return Object.fromEntries(
         tables.map((t) => {
            const key = upstreamTableKey(t);
            return [key, known[key] ?? read[key]];
         }),
      );
   }

   /**
    * Quote each manifest table path for its connection's dialect, mirroring how
    * the builder CREATEs it: Malloy pastes `tableName` into `FROM` verbatim, so
    * on a case-folding engine the case-preserved name is reachable only quoted.
    * A name written as SQL is parsed and re-quoted. An entry with no
    * `connectionName` is quoted for its plan source's connection. An entry
    * with no connection either way, an unresolvable one, or a name the
    * dialect cannot read or quote is left out (logged at ERROR, that entry
    * alone), so its source answers live: no name is inlined as given.
    */
   private quoteBoundTableNames(
      entries: FreshnessManifest,
   ): Promise<FreshnessManifest> {
      return Package.quoteTableNames(
         this.packageName,
         (connectionName) => this.getMalloyConnection(connectionName),
         entries,
         Package.planConnectionsOf(this.buildPlan),
      );
   }

   /** Each planned source's connection, by its content address. */
   private static planConnectionsOf(
      plan: BuildPlan | null | undefined,
   ): Map<string, string> {
      const out = new Map<string, string>();
      for (const source of Object.values(plan?.sources ?? {})) {
         if (source.sourceEntityId && source.connectionName) {
            out.set(source.sourceEntityId, source.connectionName);
         }
      }
      return out;
   }

   /** {@link quoteBoundTableNames} for a package not constructed yet. */
   private static async quoteTableNames(
      packageName: string,
      lookupConnection: (connectionName: string) => Promise<Connection>,
      entries: FreshnessManifest,
      planConnections: ReadonlyMap<string, string> = new Map(),
   ): Promise<FreshnessManifest> {
      const out: FreshnessManifest = {};
      for (const [sourceEntityId, entry] of Object.entries(entries)) {
         // The entry's own connection, else its plan source's, as the
         // column read finds it (readDatabaseColumns).
         const connectionName =
            entry.connectionName ?? planConnections.get(sourceEntityId);
         // Never inlined as given: a name with no dialect to quote it in, or
         // one its dialect cannot read or quote, is not bound, and its
         // source answers live.
         const leaveOut = (problem: string, error?: string) => {
            recordManifestBindDegraded();
            logger.error(
               `Manifest entry '${sourceEntityId}' ${problem}, so it is not ` +
                  `bound and its source answers live.`,
               {
                  packageName,
                  sourceEntityId,
                  ...(connectionName ? { connectionName } : {}),
                  ...(error ? { error } : {}),
               },
            );
         };
         if (!connectionName) {
            leaveOut(
               "names no connection, and the build plan has no source at its " +
                  "address to name one, so no dialect quotes its table. Fix: " +
                  "rebuild the manifest, which records each entry's connection",
            );
            continue;
         }
         let connection: Connection;
         try {
            connection = await lookupConnection(connectionName);
         } catch (err) {
            leaveOut(
               `names connection '${connectionName}', which this package ` +
                  `cannot resolve. Fix: ensure a connection named ` +
                  `'${connectionName}' exists in this package's config, or ` +
                  `rebuild the manifest against the current config`,
               err instanceof Error ? err.message : String(err),
            );
            continue;
         }
         let tableName: string;
         try {
            tableName = quoteManifestTablePath(
               entry.tableName,
               connection.dialectName,
            );
         } catch (err) {
            if (!(err instanceof UnquotableIdentifierError)) throw err;
            leaveOut(
               "names a table its connection's dialect cannot read or quote. " +
                  "Fix: rebuild the manifest, or correct the entry's table name",
               err.message,
            );
            continue;
         }
         out[sourceEntityId] = { ...entry, tableName };
      }
      return out;
   }

   /**
    * Close this package's own connections (its DuckDB sandbox), freeing the
    * sandbox's native database once its last query settles
    * (duckdb_hard_close.ts). The environment's connections are not the
    * package's and stay open.
    */
   public releaseConnections(): Promise<void> {
      // Nothing of this package's runs after it is released: its unread
      // tables' retry timer goes too.
      this.mz.dispose();
      return closeMalloyConfig(this.malloyConfig);
   }

   public getMalloyConfig(): MalloyConfig {
      return this.malloyConfig;
   }

   /** See {@link BuildPlanPackage.boundStorageColumns}. */
   public readonly boundStorageColumns: BoundStorageColumns = (
      sourceID,
      destination,
   ) => this.mz.boundStorageColumns(sourceID, destination);

   /** See {@link BuildPlanPackage.boundStorageContract}. */
   public readonly boundStorageContract: BoundStorageContract = (
      sourceID,
      destination,
   ) => this.mz.boundStorageContract(sourceID, destination);

   /** See {@link BuildPlanPackage.boundTableContract}. */
   public readonly boundTableContract: BoundTableContract = (
      sourceID,
      storage,
   ) => this.mz.boundTableContract(sourceID, storage);

   /** See {@link BuildPlanPackage.planChainStringReads}. */
   public readonly planChainStringReads: PlanChainStringReads =
      chainBuildStringReads;

   /** See {@link BuildPlanPackage.planChainBuildText}. */
   public readonly planChainBuildText: PlanChainBuildText = (request, model) =>
      planChainBuildText(this.getPackagePath(), this.getMalloyConfig())(
         request,
         model,
      );

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
   public async reloadAllModels(
      entries: FreshnessManifest,
      options: { ifChanged?: boolean } = {},
   ): Promise<boolean> {
      const requested = JSON.stringify(entries);
      const compiledFor = Package.compileKeyOf(entries);
      if (options.ifChanged && this.isCompiledFor(compiledFor)) {
         const last = this.reloadedFor!;
         // Unchanged, but a table the last bind could not read answers live
         // until one reads it: bind again, which reads it again.
         if (last.bound === requested && !this.mz.hasUnreadDatabaseTables()) {
            logger.debug(
               "Skipped reloading models: the manifest is unchanged",
               {
                  packageName: this.packageName,
               },
            );
            return false;
         }
         // The same tables, with other freshness or schemas (or not bound
         // yet, after a load that compiled against them): what a compile
         // reads is unchanged, so only the bind moves.
         const quoted =
            last.quotedFrom === requested
               ? last.quoted
               : await this.quoteBoundTableNames(entries);
         await this.mz.bindCompiled({
            database: quoted,
            databaseAsGiven: true,
            // Each table's columns as its warehouse holds them now.
            observed: await this.mz.readDatabaseColumns(quoted),
            // The first bind over a load's models publishes to them, as the
            // reload it stands in for would have.
            ...(last.bound === undefined ? { models: "installed" } : {}),
         });
         this.reloadedFor = this.reloadKey(compiledFor, requested, {
            from: requested,
            quoted,
         });
         logger.debug("Rebound the manifest without recompiling the models", {
            packageName: this.packageName,
         });
         return false;
      }
      // Quote BEFORE any projection: everything downstream reads these entries.
      entries = await this.quoteBoundTableNames(entries);
      // Each table's columns as its warehouse holds them now, read once for
      // this bind (one schema read per table), before the models move.
      const observed = await this.mz.readDatabaseColumns(entries);
      // As Malloy reads it: each entry also under its compiler key.
      const buildManifest = underCompilerKeys(
         { entries: toTableNameManifest(entries) },
         this.mz.plan.compilerKeys,
      ).entries;
      const modelPaths = Array.from(this.models.keys());
      logger.info("Reloading all models with build manifest", {
         packageName: this.packageName,
         modelCount: modelPaths.length,
         manifestEntryCount: Object.keys(buildManifest).length,
      });

      const outcome = await loadOutcomeOrThrow(
         getPackageLoadPool().loadPackage({
            upstreamSchemas: readUpstreamSchemas,
            packagePath: this.packagePath,
            packageName: this.packageName,
            environmentName: this.environmentName,
            malloyConfig: this.malloyConfig,
            defaultConnectionName: "duckdb",
            buildManifest,
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
      // A version's binding is serving state on its registry row, which a
      // reload keeps; its publisher.json is immutable content. Only a package
      // with no versions keeps its `manifestLocation` in publisher.json.
      if (this.getVersionId() === undefined) {
         this.packageMetadata.manifestLocation =
            outcome.packageMetadata.manifestLocation ?? null;
      }
      this.applyDiscoveryPolicyToModels();
      this.applySiblingModelResolverToModels();
      // Applies the query boundary too, against the refreshed explore set.
      this.installReports(discovery);
      // New models read the snapshot through their new policy; compile
      // their served copies over it now, once: the destinations and the
      // entries they were compiled with go first, so the one compile the
      // publish starts is not invalidated and started again.
      this.mz.wireServingResolvers();
      this.mz.pushServeDestinationConfigToModels();
      await this.mz.bindCompiled({
         database: entries,
         databaseAsGiven: true,
         observed,
         models: "installed",
      });
      this.reloadedFor = this.reloadKey(compiledFor, requested, {
         from: requested,
         quoted: entries,
      });
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
      warnInvalidExplores(this);
      this.logEmptyDiscoveryWarnings();
      return true;
   }

   /**
    * The part of database entries a compile reads: each table's name and
    * the connection it is quoted for (the models' build manifest is
    * `toTableNameManifest` of the quoted entries). Freshness, schemas and key
    * checks are read per query and by the bind, never by a compile.
    */
   private static compileKeyOf(entries: FreshnessManifest): string {
      return JSON.stringify(
         Object.keys(entries)
            .sort()
            .map((id) => [
               id,
               entries[id].tableName,
               entries[id].connectionName ?? null,
            ]),
      );
   }

   /**
    * What the models compile over now, against entries whose compile key is
    * `compiledFor`, last bound as `bound` (requested entries as JSON,
    * unquoted; undefined when none were), with `quoting.quoted` the entries
    * requested as `quoting.from`, quoted.
    */
   private reloadKey(
      compiledFor: string,
      bound: string | undefined,
      quoting: { from: string; quoted: FreshnessManifest },
   ): NonNullable<Package["reloadedFor"]> {
      return {
         compiledFor,
         bound,
         quotedFrom: quoting.from,
         quoted: quoting.quoted,
         models: this.models,
         database: this.mz.boundDatabaseEntries(),
         buildPlan: this.mz.getBuildPlan(),
         connectionEdits: this.connectionEdits,
      };
   }

   /**
    * Whether the models were last compiled against entries with compile key
    * `compiledFor`, and nothing has moved since.
    */
   private isCompiledFor(compiledFor: string): boolean {
      const last = this.reloadedFor;
      if (!last) return false;
      // A plan a connection edit made stale may not hold the tables its
      // re-derivation will; recompile, as before, rather than keep them.
      if (this.mz.isBuildPlanStale()) return false;
      // A model a reload left as a placeholder may compile now.
      for (const model of this.models.values()) {
         if (model.getCompilationError()) return false;
      }
      return (
         last.compiledFor === compiledFor &&
         last.models === this.models &&
         last.database === this.mz.boundDatabaseEntries() &&
         last.buildPlan === this.mz.getBuildPlan() &&
         last.connectionEdits === this.connectionEdits
      );
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
               const model = this.models.get(modelPath);
               if (model?.getType() === "model") {
                  // What getModel() throws, without building (and
                  // serializing) the compiled model it would return.
                  error = model.getCompilationError()?.message;
               } else {
                  try {
                     await model?.getModel();
                  } catch (modelError) {
                     error =
                        modelError instanceof Error
                           ? modelError.message
                           : undefined;
                  }
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
            (await readPackageURL(
               this.packagePath,
               pathToFileURL(safeJoinUnderRoot(this.packagePath, modelPath)),
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
         [
            ...findings,
            // Each refused cell counts once, listed or only named in a summary.
            ...Array.from({ length: this.malloynbCellsUnlisted }, () => ({
               code: MALLOYNB_CELL_REFUSED,
               severity: "warn",
            })),
         ],
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
    * Read each linear report's definition cells. A reader refusal is returned
    * as a finding, since the model still compiles and nothing else would say
    * so.
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
    * How a surface refusal is explained, and its fix, under the surface this
    * package declares: `index.malloy`, or the deprecated `explores`. `source`
    * is absent when the refusal would not name it (a gated model).
    */
   private surfaceFix(): {
      unexported: string;
      fixFor: (source: string | undefined) => string;
   } {
      return this.surfaceIsIndexModel()
         ? {
              unexported: `which ${INDEX_MODEL_NAME} doesn't export`,
              fixFor: (source: string | undefined) =>
                 `Fix: add ${source ?? "the source it reads"} to the ` +
                 `export { ... } in ${INDEX_MODEL_NAME}.`,
           }
         : exploresSurfaceFix(this.packageMetadata.explores, (entry) =>
              this.isServedReport(entry),
           );
   }

   /**
    * DEPRECATED (`.malloynb` only; deleted with the format). One finding per
    * `.malloynb` code cell the query route refuses for every reader in the
    * Console ({@link Model.malloynbCellRefusals}, which runs the route's own
    * checks), since a cell's errors otherwise show only when a reader opens
    * it. A cell only a reader's givens refuse (`#(authorize)`, a row-level
    * gate) is not reported. One yielder spans every notebook, so the event
    * loop gets a turn between cells. A notebook whose check throws gets one
    * finding saying so, and the other notebooks and lints are unaffected.
    * The findings are bounded by {@link listMalloynbCellFindings}, and the
    * cells it names only in a summary are kept in
    * {@link malloynbCellsUnlisted} so the lint metric counts every one.
    */
   private async lintMalloynbCells(): Promise<ApiPackageWarning[]> {
      const findings: ApiPackageWarning[] = [];
      const curated = this.queryBoundaryActive();
      const { unexported, fixFor } = this.surfaceFix();
      const home = !curated
         ? "a source the notebook declares"
         : this.surfaceIsIndexModel()
           ? `a model file that ${INDEX_MODEL_NAME} exports`
           : "a source the package surface publishes";
      const restrictedFix =
         `Fix: declare the table, or a dimension over the sql_* ` +
         `expression, in ${home}, and run that source.`;
      const yielder = createEventLoopYielder();
      const notebooks: {
         model: string;
         refusals: { cell: number; finding: ApiPackageWarning }[];
      }[] = [];
      for (const [modelPath, model] of this.models) {
         if (!isMalloynbPath(modelPath)) continue;
         let refusals: Awaited<ReturnType<Model["malloynbCellRefusals"]>>;
         try {
            refusals = await model.malloynbCellRefusals(yielder);
         } catch (err) {
            logger.warn(".malloynb cell check failed", {
               packageName: this.packageName,
               modelPath,
               error: errMessage(err),
            });
            findings.push({
               model: modelPath,
               code: MALLOYNB_CELL_CHECK_FAILED,
               severity: "warn",
               message:
                  `The .malloynb cell check could not run for ${modelPath}, ` +
                  `so a cell the query route refuses may have no warning ` +
                  `here. An operator can find the cause in the server log ` +
                  `under ".malloynb cell check failed".`,
            });
            continue;
         }
         const cells: { cell: number; finding: ApiPackageWarning }[] = [];
         for (const refusal of refusals) {
            const { source, problem } = refusal;
            let why: string;
            if (refusal.reason === "off_surface") {
               if (problem) {
                  why =
                     `the package surface refuses it (404), and restricted ` +
                     `mode refuses it too: ${problem} ${restrictedFix}`;
               } else if (source === undefined) {
                  why =
                     `it reads a source the package surface does not ` +
                     `publish, or one the notebook declares (404). ` +
                     `${fixFor(undefined).replace(/\.$/, "")}, declaring ` +
                     `it in a model file first if the notebook declares it.`;
               } else if (refusal.via) {
                  why =
                     `it runs the query ${refusal.via}, and ${refusal.via} ` +
                     `reads ${source}, ${unexported} (404). ${fixFor(source)}`;
               } else if (refusal.declared) {
                  why =
                     `it reads ${source}, a source the notebook declares, and ` +
                     `the query route takes only a source the package ` +
                     `surface publishes (404). Declare ${source} in a model ` +
                     `file instead. ${fixFor(source)}`;
               } else {
                  why = `it reads ${source}, ${unexported} (404). ${fixFor(source)}`;
               }
            } else if (refusal.reason === "caller_annotation") {
               why =
                  `the query route refuses query text that carries an ` +
                  `annotation only the model author may write (400): ` +
                  `${problem} Fix: remove the annotation, or the text that ` +
                  `spells it, from the cell's run: statement, comments ` +
                  `included.`;
            } else if (refusal.reason === "required_filter") {
               why =
                  `the source it runs declares a required #(filter), and the ` +
                  `Console sends no filter value for a notebook cell (400): ` +
                  `${problem}. Fix: move the filter to a given: declaration, ` +
                  `as docs/givens.md#coming-from-filter describes.`;
            } else if (refusal.reason === "restricted_mode") {
               why =
                  `the query route runs its run: statement in restricted ` +
                  `mode, which refuses it (400): ${problem} ${restrictedFix}`;
            } else {
               why =
                  `its run: statement does not compile on its own (400): ` +
                  `${problem}`;
            }
            cells.push({
               cell: refusal.cell,
               finding: {
                  model: modelPath,
                  code: MALLOYNB_CELL_REFUSED,
                  reason: refusal.reason,
                  severity: "warn",
                  message:
                     `Cell ${refusal.cell} of ${modelPath} won't run for any ` +
                     `reader in the Console: ${why}`,
               },
            });
         }
         notebooks.push({ model: modelPath, refusals: cells });
      }
      const { warnings, unlisted } = listMalloynbCellFindings(
         notebooks,
         MALLOYNB_CELLS_LISTED,
      );
      this.malloynbCellsUnlisted = unlisted;
      return [...findings, ...warnings];
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
      const { unexported, fixFor } = this.surfaceFix();
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
      // Set again by the cell check; a pass that stops before it names none.
      this.malloynbCellsUnlisted = 0;
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
         // Last, and isolated per notebook: a throw there costs no other finding.
         warnings.push(...(await this.lintMalloynbCells()));
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
               `unknown subset of the tile, report, drill, given, curation ` +
               `and .malloynb cell checks did not run. Treat a missing ` +
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
      let notebook: ApiReportCells | undefined;
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
               // Caller SQL and model text reach the sandbox, so it runs
               // under Malloy's sandboxed policy: no network, its filesystem
               // confined to the package directory (duckdb_lockdown.ts).
               duckdb: {
                  is: "duckdb",
                  databasePath: ":memory:",
                  ...sandboxedDuckDBOptions(packagePath),
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
               // The package's own sandbox: releasing the package frees it.
               ownDuckDBConnection(malloyConfig, connection);
               // Pin the sandbox against implicit auto-install too, or
               // EXTENSION_FETCH_POLICY local-only would have a network hole.
               if (connection instanceof DuckDBConnection) {
                  await applyExtensionSessionSettings(connection);
               }
               // The sandbox is a DuckDB connection the package owns, which a
               // `storage=` build cannot read (materialization eligibility).
               return recordConnectionType(connection, "duckdb");
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
      // One answer for every manifest Publisher will not read: missing, a
      // link out of the package (whatever it leads to, or nothing), or not a
      // file. So the manifest cannot tell what lies outside the package.
      const notThere = () => {
         logger.error(`Can't find ${packageConfigPath}`);
         return new PackageNotFoundError(
            callerVisibleText(
               `Package manifest for ${packagePath} does not exist.`,
               packagePath,
            ),
         );
      };
      // A package directory Publisher may not open is reported as such (its
      // errno), not as a missing manifest: that is the operator's own tree.
      try {
         await fs.realpath(packagePath);
      } catch (error) {
         const code = (error as NodeJS.ErrnoException).code;
         if (code === "ENOENT" || code === "ENOTDIR") throw notThere();
         throw error;
      }
      let real: string;
      try {
         real = await confinedPackagePath(
            packagePath,
            pathToFileURL(packageConfigPath),
         );
      } catch {
         throw notThere();
      }
      let stat: Awaited<ReturnType<typeof fs.stat>>;
      try {
         stat = await fs.stat(real);
      } catch (error) {
         // Only ENOENT/ENOTDIR mean "does not exist"; an EACCES is rethrown as itself.
         const code = (error as NodeJS.ErrnoException).code;
         if (code !== "ENOENT" && code !== "ENOTDIR") throw error;
         throw notThere();
      }
      if (!stat.isFile()) throw notThere();
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
               // Named by the file's package-relative path, with the
               // connection's own reason, never the SQL the probe compiled;
               // that reason names the package's files package-relative and
               // no other path.
               const reason =
                  error instanceof MalloyError && error.problems.length > 0
                     ? error.problems.map((p) => p.message).join("; ")
                     : message;
               const readable = callerVisibleText(reason, packagePath);
               return {
                  path: databasePath,
                  type: "embedded" as const,
                  error: readable.startsWith(`Cannot read ${databasePath}`)
                     ? readable
                     : `Cannot read ${databasePath}: ${readable}`,
               };
            }
         }),
      );
   }

   private static async getDatabasePaths(
      packagePath: string,
   ): Promise<string[]> {
      const files = await listPackageTree(packagePath);
      const candidates = files
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
      // Only a regular file inside the package is probed: a link out of it,
      // to a file or a directory, is not read through the connection.
      const probed: string[] = [];
      for (const relativePath of candidates) {
         try {
            const real = await confinedPackagePath(
               packagePath,
               pathToFileURL(path.join(packagePath, relativePath)),
            );
            if ((await fs.stat(real)).isFile()) probed.push(relativePath);
         } catch {
            // Outside the package, missing or unreadable: not probed.
         }
      }
      return probed;
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

   /**
    * `setPackageMetadata({ ...getPackageMetadata(), ...fields })` without
    * building the metadata's computed fields (its warnings pass among them),
    * which `setPackageMetadata` drops anyway.
    */
   public updateStoredMetadata(fields: ApiPackage): void {
      this.setPackageMetadata({
         ...this.packageMetadata,
         name: this.packageName,
         ...fields,
      });
   }

   public setPackageMetadata(packageMetadata: ApiPackage) {
      this.packageMetadata = Package.storableMetadata(packageMetadata);
      this.applyDiscoveryPolicyToModels();
      this.applyQueryBoundaryToModels();
      this.applySiblingModelResolverToModels();
   }
}
