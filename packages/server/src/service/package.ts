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
import { llmConfigured } from "../providers/active";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   assertRequiredStagesAvailable,
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
 * Pre-aggregation violations that WARN at both gates rather than refusing.
 *
 * Both concern a field the source does not publicly expose. What stops a hidden
 * field reaching a rollup is the planner's own skip, not either gate — see
 * {@link Package.formatInvalidPreaggregatePolicy} for that, and for the two
 * reasons refusing was wrong rather than merely strict.
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
   COMPONENT_FILE_SUFFIXES,
   definitionKey,
   dropGridTileLayoutFindings,
   factsReportTag,
   matchesDocumentedReportName,
   lintReport,
   lintDrillTargets,
   lintGivenTags,
   lintSelfDrills,
   lintUndiscoveredReport,
   queryTiles,
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
import { isQuotedIdentifierPath, quoteManifestTablePath } from "./quoting";
import { Model, type OffSurfaceContext } from "./model";
import { assertPersistNamesQuoted } from "./persist_annotation_validation";

type ApiDatabase = components["schemas"]["Database"];
type ApiModel = components["schemas"]["Model"];
type ApiReport = components["schemas"]["Report"];
type ApiReportManifest = components["schemas"]["ReportManifest"];
type ApiRawNotebook = components["schemas"]["RawNotebook"];

/**
 * Where Malloyyo looks for a dashboard's custom component. The directory means
 * nothing to Publisher; it is named only to warn about an inert `.jsx` there.
 */
const MALLOYYO_DASHBOARDS_DIR = "dashboards";

/** One report discovery found and serves, keyed by its path. */
interface ServedReport {
   /** Package-relative path: the report's identity. */
   path: string;
   /** The basename, a display label. */
   name: string;
   layout: ReportLayout;
   /**
    * A `.malloy` report's tiles (either layout), or, for one that failed to
    * compile, its name and layout carrying `error`. Absent on a `.malloynb`.
    */
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
 * `compilation_error`; a gate refusing what the package declares, or a
 * publisher.json that cannot be used, is a 4xx `policy_rejected`; a rewrapped
 * pool-infrastructure failure is a transient
 * `pool_unavailable`; anything else is a generic `error`.
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
   // The colocated-tier analogue of `sourceEligibility`
   // (build_plan.collectColocatedSourceEligibility), consulted when binding a
   // colocated (same-connection) manifest. Keyed by sourceEntityId, not name —
   // see that type's doc. Same "undefined means not known, refuse" rule.
   private colocatedSourceEligibility: ColocatedSourceEligibility | undefined =
      undefined;
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
   // Load-time report lint findings, on the same read-only warnings surface
   // as the render-tag ones. Refreshed with the reports on load and reload.
   private reportWarnings: ApiPackageWarning[] = [];
   /** Linear reports whose cells the reader refused; set with the reports. */
   private readerWarnings: ApiPackageWarning[] = [];
   // Every served report, of either layout, keyed by path, in path order
   // (`docs/reports-design.md` §1). Computed once per load/reload rather than
   // per request: the artifact tag is a property of the compiled model, so it
   // can only change when the models do. A grid report that failed to compile
   // has no readable tag, so it lands here carrying its error rather than
   // disappearing. Its keys are what makes a file a report for the query
   // boundary, and each `text` lets a report's own derived sources prove they
   // derive from the surface.
   private reports = new Map<string, ServedReport>();
   /** Every file discovery found carrying an artifact tag, with its status. */
   private reportFiles = new Map<string, ReportFile>();
   /** The last discovery, which the lint re-runs over. */
   private discovery?: ReportDiscovery;
   /**
    * Manifest-shape warnings the load tolerated: the `explores` and
    * `queryableSources` deprecations and a root-level `scope`. Kept so publish
    * can report a still-parsing-but-outdated manifest, and carried on the
    * package's `warnings` (see getPackageMetadata).
    */
   private manifestWarnings: string[] = [];
   /**
    * What this package is currently serving, refreshed on load and on every
    * reload. `servedRevision` identifies the load; `sourceContentSha` is a
    * content hash over the served files, so it answers the question a caller
    * actually has after an edit -- did the bytes the server read change --
    * which `servedRevision` cannot, being minted fresh either way.
    */
   private servedRevision: string = mintServedRevision();
   private sourceContentSha: string = "";
   /**
    * The agent skills this package ships, read from `skills/` on load and on
    * every reload. Refreshed in the same call that recomputes the content sha,
    * so a skill edit is covered by the reload receipt by construction rather
    * than by remembering to add it.
    */
   private packageSkills: SkillEntry[] = [];
   private packageSkillWarnings: string[] = [];
   /**
    * The manifest's `agents` value as the worker read it, unvalidated. Held
    * apart from `packageMetadata` so a metadata PATCH cannot replace it; the
    * validated agents below are derived from it in refreshServingIdentity.
    */
   private rawAgents: unknown;
   private packageAgents = new Map<string, PackageAgent>();
   private packageAgentWarnings: string[] = [];
   /**
    * The manifest's `retrieval` block as read at load (prompt files included).
    * Replaced on reload, which is how an edit to it, or to a prompt file,
    * takes effect.
    */
   private retrievalSettings: PackageRetrievalSettings =
      DEFAULT_PACKAGE_RETRIEVAL;
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
      return {
         environment: this.environmentName,
         package: this.packageName,
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
    * Re-derive the serving identity, and re-read the package's own skills.
    *
    * The two are one operation on purpose. The sha is the reload receipt, and
    * a receipt that ignored skill files would report "nothing changed" for a
    * skill edit that did in fact reach the server -- the exact false negative
    * the receipt exists to prevent. Reading the skills here is what supplies
    * the paths, so the set that is served and the set that is hashed are the
    * same set.
    *
    * Not run by the constructor: {@link lintWorkerOutcome} builds a throwaway
    * Package per package-scope compile and has no use for it. {@link create}
    * and {@link reloadAllModels} call it. Call after anything that changes which
    * files are served or what they contain; a reload that leaves the bytes identical still mints a new
    * revision, and correctly leaves the sha alone.
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
    * Push the discovery-curation policy down onto each Model. Curation (file
    * listing via `explores` and within-file `export {}` filtering) is enabled
    * whenever the package resolves a non-empty surface. That surface is either
    * an `explores` the author wrote in publisher.json or one derived from a
    * root `index.malloy`, and this code cannot tell the two apart by design
    * (see resolveExplores) — so "no `explores` key" does NOT mean "uncurated".
    * No surface at all preserves legacy listings. Re-derived on reload and
    * metadata PATCH (the inputs can change there).
    */
   /** True when the package has a non-empty resolved surface, whoever
    *  authored it. Single source of truth so the curation/boundary/listing
    *  derivations can't drift out of sync. */
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
    * Report that this package was curated a moment ago and is not any more.
    *
    * Losing a surface is the one curation change nothing else reports. Every
    * other transition leaves something behind to look at: a surface that
    * APPEARS warns at load, a malformed `explores` refuses the load, a broken
    * surface file fails the reload and is reported stale. But deleting or
    * renaming `index.malloy` simply resolves to no surface, which is an
    * ordinary uncurated package, and an uncurated package has nothing to say
    * about itself. The sources that file was withholding are listed and
    * queryable again from that moment, and the author who deleted a file they
    * thought was theirs to delete is told nothing.
    *
    * `previousSurface` is the resolved `explores` from before the reload. Every
    * path that replaces this package's surface calls this: an in-place
    * {@link reloadAllModels} (materialization, manifest rebind) passes its own
    * surface from before it re-read the tree, and a full reload in
    * {@link Environment} passes the surface of the package it replaces. A
    * process restart has no "before", so it cannot report this.
    *
    * Only `undefined` counts as lost, and the distinction is the whole point:
    *
    *  - `undefined` is "no `explores` key and no root index.malloy", which is
    *    what deleting or renaming the surface file resolves to. Nobody asked
    *    for it and nothing else says it happened.
    *  - `[]` is an author writing `"explores": []`, the old opt-out. They
    *    asked for exactly this and already get a warning about the key.
    *
    * It is a transition, so it is said once, on the reload that caused it; the
    * next reload of an already-uncurated package clears it.
    *
    * This is curation, not access control -- nothing gated by `#(authorize)`
    * becomes reachable, and hiding was never denying. What widens is what is
    * listed and what answers by name.
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
    * True when this package's surface is its `index.malloy`, which is what the
    * convention produces when the manifest declares no `explores`.
    *
    * Derived from the tree and the resolved surface rather than from a stored
    * origin, deliberately: nothing records where a surface came from, and
    * nothing should. The question this answers is not "who wrote it" but "is
    * there an index.malloy the author is looking at", and that has the same
    * answer either way.
    *
    * It is therefore TRUE for a hand-written `explores: ["index.malloy"]`, and
    * a remedy this selects has to hold for that author too. The file is the
    * same; the manifest key is not, and a remedy that says to delete the file
    * without also clearing the key would leave that package listing nothing.
    * Every message below is worded to cover both.
    *
    * Used ONLY to pick the wording of a warning's remedy. Every message that
    * says "add it to 'explores'" is advice with no target for a package that
    * has no such key, and a dashboard file cannot be exported from an
    * index.malloy at all. It must never gate behavior: the surface behaves
    * identically whichever source produced it.
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
    * Policy: queryable == discoverable. The boundary is inert unless the
    * package resolved a surface (no curated surface ⇒ nothing to restrict) AND
    * `queryableSources` is "declared" (the default; "all" decouples the axes).
    * A surface derived from a root `index.malloy` arms it exactly as a written
    * `explores` does, so an absent manifest key does not mean an inert
    * boundary. When active, a model file is a query entry point only if it is
    * on that surface; within-file curation (`export {}`) is read off each
    * Model.
    */
   private applyQueryBoundaryToModels(): void {
      const exploresDeclared = this.exploresDeclared();
      const exploreSet = this.exploreSet();
      const mode =
         this.packageMetadata.queryableSources === "all" ? "all" : "declared";
      // The PACKAGE-wide queryable surface: the union of every explores-listed
      // model's export closure, not just the requested model's: a source
      // declared queryable (its file is listed in explores) must stay
      // queryable when addressed through a model that imports it, since agents
      // commonly post every query to one model path.
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
            // Nor does a report, even when `explores` lists it (the shape
            // older advice produced): it reads the surface, it never adds to
            // it. An untagged file is not a report wherever it lives, so a
            // listing publishes it like any other file.
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
    * Push a sibling-model lookup down onto each Model: given a `file://` URL,
    * find the `.malloy`/`.malloynb` file it names among this PACKAGE's own
    * discovered models (every one is compiled independently — see the
    * worker's `filterModelPaths`/`recursive` scan — regardless of whether
    * anything imports it, or whether the served model's own compile promoted
    * it to a `modelDef.contents` entry) and hand back that sibling's own
    * `ModelDef`. See `./filter_binding_guard`'s `SiblingModelDefResolver` doc
    * for why a served model needs this at all: a selective `import { name }
    * from "file"` or a notebook cell's narrower per-cell compile can leave an
    * ancestor file with no `modelDef.contents` entry of its own, even though
    * the package compiled it fine on its own.
    *
    * Built fresh from `this.models`/`this.packagePath` (never memoized across
    * calls) and re-run alongside {@link applyDiscoveryPolicyToModels}/
    * {@link applyQueryBoundaryToModels} — both at construction and after a
    * reload swaps `this.models` wholesale — so every model always resolves
    * against the CURRENT model set, never a stale one from before a reload.
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
    * The package's API metadata as the worker read it from publisher.json.
    */
   private static packageConfigFromOutcome(
      environmentName: string,
      packageName: string,
      outcome: LoadPackageOutcome,
   ): ApiPackage {
      // Override the manifest-derived resource URI — the worker only
      // returns name/description from publisher.json, but the rest of
      // the API surface expects a `resource` field too.
      return {
         name: outcome.packageMetadata.name,
         description: outcome.packageMetadata.description,
         location: outcome.packageMetadata.location,
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
   }

   /**
    * Turn the worker's compiled models into live `Model`s and validate their
    * render tags. Render tags are checked here, on the main thread, because the
    * renderer is too heavy to load inside the pure-CPU package-load worker. A
    * misconfigured tag never fails the load; its findings are returned to ride
    * the package response as non-fatal `warnings`.
    *
    * One function for the load, the in-place reload and the package-scope
    * compile ({@link lintWorkerOutcome}), so the render-tag findings a compile
    * reports cannot drift from the ones a reload reports. It touches no
    * package state.
    *
    * `onCompileError` is the one way the callers differ. A load ("throw")
    * fails on the first model that did not compile. A reload ("placeholder")
    * keeps a failed model as `Model.fromCompilationError`, and likewise keeps a
    * model whose render-tag validation threw unexpectedly, so one bad file does
    * not cost the package its other models. `afterHydrate` runs once per
    * compiled model, in order, before it is added (the load path's
    * persist-name check).
    *
    * `ctx.compile` marks the package-scope compile. Its caller reports every
    * failed model in the response, so the per-model "failed during reload"
    * line is not logged: a compile is not a reload, and an agent iterating on
    * a broken file would otherwise fill the operator log. A render-tag check
    * that throws is a server fault, so it is still logged, as "during compile".
    */
   private static async hydrateWorkerModels(
      outcome: LoadPackageOutcome,
      ctx: {
         packageName: string;
         packagePath: string;
         malloyConfig: MalloyConfig;
         buildManifest?: BuildManifest["entries"];
         compile?: boolean;
         /**
          * The what-if text a package-scope compile was given. A model that
          * fails to compile is kept as a placeholder, and without this the
          * placeholder carries no text, so a reader of it judges the saved
          * file instead of the edit.
          */
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
               // The outer catch in Package.create records the total metric +
               // cleans the package directory.
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
            // A load and a reload fail on this, as they always have. A compile
            // keeps going: one model that will not hydrate costs that model's
            // findings, not every other file's. The finding is an error, since
            // the reload this compile previews would fail on it.
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
            // Defensive: validateRenderTags logs its findings and does not
            // throw. On reload an unexpected internal failure is recorded as
            // this model's compilationError rather than aborting the reload.
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
               // Spelled out rather than spread. A spread is exempt from
               // excess-property checking exactly like a named interface, so
               // `...w` would carry a stale field name through a rename in
               // `api-doc.yaml` without the compiler noticing.
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
    * The render-tag and dashboard findings a reload of this worker outcome
    * would put on the package's `warnings`, for the package-scope compile.
    *
    * Runs the reload's own two steps, {@link hydrateWorkerModels} (keeping
    * failed models, as a reload does) and {@link discoverReports}, on a
    * throwaway Package built from the outcome. The served package is never
    * read or written, so a what-if cannot leak into what is served. The
    * outcome's metadata carries the surface the worker resolved from
    * publisher.json or a root index.malloy, so the tile lint answers against
    * the same surface a reload would.
    *
    * Not covered: notebook findings (the compile reports those itself), and
    * every warning that needs the build plan or a bound manifest (storage,
    * persist, materialization, collision), which only a real load computes.
    *
    * With a replacement source, the hydrated models already reflect it. A
    * replacement that fails to compile carries no text back from the worker, so
    * its placeholder is handed the replacement text, and discovery decides from
    * that whether it is a dashboard (and so whether a drill to it resolves)
    * rather than reading the saved file.
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
         // Discovery is meant never to throw, but a fault in it must not
         // reject the whole compile, and a missing list reads exactly like a
         // clean package. Say the findings are unknown instead.
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
            environmentName,
            malloyConfig,
            defaultConnectionName: "duckdb",
            computeBuildPlan: true,
            withPreaggregateCompanions: true,
         })
         .catch((err: unknown) => {
            // Compile errors surface in-band via
            // `LoadPackageOutcome.models[i].compilationError`; if the
            // pool itself rejects, it's an infra-side failure
            // (shutting down, worker spawn failed, worker crashed,
            // RPC timeout) and the client should retry. Real Malloy
            // compile errors deserialised by the pool still carry
            // their MalloyError / ModelCompilationError identity, and an
            // unusable publisher.json its PackageManifestError identity —
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
               realError instanceof ModelCompilationError ||
               realError instanceof PackageManifestError
            ) {
               throw realError;
            }
            throw new ServiceUnavailableError(
               `Package-load worker pool unavailable: ${realError.message}`,
               // The worker's failure may be a refused filesystem access on
               // the package's own files, which the error mapper reads from
               // the cause; a bare 503 would read as "retry".
               { cause: realError },
            );
         });
      const [outcome, databases] = await Promise.all([
         workerOutcome,
         Package.readDatabases(packagePath, malloyConfig),
      ]);
      // A stage the package requires (`enabled: true`) needs the operator's
      // LLM; without one the package does not load.
      assertRequiredStagesAvailable(
         outcome.packageMetadata.retrieval ?? DEFAULT_PACKAGE_RETRIEVAL,
         llmConfigured(),
      );
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

      // Build live `Model`s from worker output. Any per-model compile
      // failure aborts the load — matches the historical behaviour of
      // `Package.create` failing the whole package on the first model
      // error. (`Package.reloadAllModels` keeps the failed-model
      // placeholders instead.)
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
               // Reject unquoted `#@ persist name=` annotations at load:
               // an unquoted name is dropped from the build plan, so the source
               // would publish but never materialize. Scan the raw `.malloy`
               // source (the ground truth for quoting); throws a
               // ModelCompilationError (424). `.malloy` only: `#@ persist` is a
               // model-file directive and does not appear in `.malloynb`
               // notebooks. If that ever changes, widen this guard so the
               // identifier-injection check still covers notebooks.
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
      const reportTimings = await pkg.discoverReports("load");
      // After discovery: which findings a tile explains depends on the reports.
      pkg.renderTagWarnings = pkg.surfacedRenderTagWarnings(renderTagWarnings);
      pkg.manifestWarnings = outcome.packageMetadata.manifestWarnings ?? [];
      pkg.recordDeprecatedUsage(outcome);
      pkg.retrievalSettings =
         outcome.packageMetadata.retrieval ?? DEFAULT_PACKAGE_RETRIEVAL;
      pkg.rawAgents = outcome.packageMetadata.agents;
      // Install the per-query freshness resolver on the freshly-built models.
      // At create time no manifest is bound yet, so the resolver returns
      // undefined (serve live) until a subsequent bindManifest → reloadAllModels.
      pkg.wireFreshnessResolvers();
      pkg.refreshServingIdentity();

      // The persist build plan, derived off the live (unbound) models before
      // the caller binds any configured manifest, so the surfaced plan reflects
      // the canonical build (not the manifest-rewritten SQL). The load worker
      // derives it from its own compile (see LoadPackageResult.buildPlan).
      // Deriving it here compiles every model a second time on this thread,
      // blocking the event loop for as long as the worker's compile took, so it
      // happens only when the worker left the plan at its soft deadline, for a
      // package whose load came close to the job timeout. Best-effort: a plan
      // failure is logged, not fatal — the package still serves; the plan is
      // just absent.
      //
      // A failure also leaves `sourceEligibility` unknown, which refuses every
      // storage serve binding until a load succeeds (see
      // bindStorageServeBindings). Serving live is a slower answer, not a wrong
      // one; admitting an unexamined binding would be a wrong one.
      //
      // It ALSO leaves `colocatedSourceEligibility` unknown, and that one is a
      // bigger blast radius: colocated is the default tier, not gated by
      // PERSIST_STORAGE_MODE, so this silently reverts EVERY colocated binding
      // for the package to live recompute (see bindColocatedServeManifest and
      // its recordColocatedBindDropped("build_plan_unavailable") counter).
      let buildPlanMs: number | undefined;
      const buildPlanPhaseStart = performance.now();
      try {
         const workerPlan = outcome.buildPlan;
         // Before a failed plan throws: the main-thread derivation records
         // each refusal and skipped digest as it goes, so one that fails
         // part-way has already counted them.
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
            : await computePackageBuildPlan(pkg);
         pkg.buildPlan = plan;
         pkg.droppedPersistSources = droppedPersistSources;
         pkg.sourceEligibility = sourceEligibility;
         pkg.colocatedSourceEligibility = colocatedSourceEligibility;
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
         recordBuildPlanComputeDuration(
            workerPlan
               ? workerPlan.durationMs
               : performance.now() - buildPlanPhaseStart,
            workerPlan ? "worker" : "main_thread",
         );
         buildPlanMs = performance.now() - buildPlanPhaseStart;
      } catch (err) {
         recordBuildPlanComputeFailed();
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
      // given on formatInvalidPreaggregatePolicy: a declaration that cannot take
      // effect is invisible in the answers, so warning here would leave the
      // author believing they had a rollup.
      //
      // Except the access-modifier pair, which warns at publish and at load —
      // see formatInvalidPreaggregatePolicy for why, and why warning is safe
      // there when it would not be for the others.
      const accessWarnings = pkg.preaggregateAccessWarnings();
      if (accessWarnings.length > 0) {
         logger.warn(
            `Package ${packageName} pre-aggregates a field the source hides`,
            {
               packageName,
               detail: accessWarnings.map((w) => w.message).join("\n"),
            },
         );
      }
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
    * The package-level `materialization` config (from publisher.json),
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

   /** How this package is searched and indexed (publisher.json `retrieval`). */
   public getRetrievalSettings(): PackageRetrievalSettings {
      return this.retrievalSettings;
   }

   /**
    * ON THE `/status` POLL PATH: this runs for every package in every
    * environment, every few seconds, because `EnvironmentStore.getStatus()`
    * serializes each environment through `listPackages()`. Anything added to the
    * computed spreads below must be trivially cheap or memoized on the Model --
    * an unmemoized walk over model fields here reached seconds of synchronous
    * CPU per poll and blocked the event loop for every other request.
    */
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
         servedRevision: this.servedRevision,
         sourceContentSha: this.sourceContentSha,
         // Overlaid so a metadata PATCH body can neither set nor wipe it.
         agents: this.rawAgents === undefined ? undefined : this.listAgents(),
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
         ...this.readerWarnings,
         ...this.reportWarnings,
         // A package skill that could not be read or is missing a description.
         // Advisory like the rest: a bad markdown file must not cost the
         // package its models, and this is the only non-log signal that a
         // skill the author wrote is not being served.
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
         // A `#@ persist` the compile-time gate refused. Without this the
         // refusal is computed, recorded on the build plan, and read by nobody:
         // the package publishes, the source is served live, and the author who
         // wrote the annotation is told nothing. Same operator-facing fact as
         // storageWarnings() — no table, answers come from the warehouse — so it
         // belongs in the same place.
         ...this.persistRefusalWarnings(),
         // A listed model whose curated surface is empty. Advisory (an
         // import-only file is legitimate and must not block a publish, so it
         // stays out of exploresWarnings), but it must ride the API, or a
         // package whose listed files surface nothing reports no warning.
         ...this.emptyDiscoveryWarnings(),
         // Content on a deprecated format: a `.malloynb` notebook, and a source
         // declaring `#(filter)` annotations. Both still load and run, so this
         // is advisory; it rides the API so a client can say so on the package
         // rather than leave it to the server log.
         ...deprecatedFormatWarnings(this.models),
         // A grid basename two reports share: the deprecated by-name route
         // answers 409 for it.
         ...ambiguousDashboardNameWarnings(this),
         // The whole surface failed to compile, so every model in the package
         // is refused by name. Rides the API for the same reason as the line
         // above, and more urgently: the 404s it causes name models that are
         // not themselves broken, so nothing else points at the cause.
         ...this.brokenSurfaceWarnings(),
         // The surface this package had before the last reload is gone. Rides
         // the API because it is the only record: an uncurated package looks
         // exactly like one that was never curated.
         ...(this.surfaceWidenedWarning !== undefined
            ? [{ message: this.surfaceWidenedWarning }]
            : []),
         // A within-package persist-target collision spans two or more sources, so
         // there is no single subject field; the message names them. Surfaced here
         // (alongside the load-path log) so an operator can see it on the status
         // API like the other persist warnings — see persistenceCollisionWarnings.
         ...this.persistenceCollisionWarnings().map((message) => ({ message })),
         // `#@ preaggregate` on a field the source hides. These are the two
         // violations that WARN at load rather than failing it, and this array is
         // what makes that a warning rather than silence: without it the package
         // loads, the rollup does not happen, and the only trace is a line in the
         // server log. The operator-facing fact is the same one storageWarnings()
         // reports for a mode-off rollup — it is not built, and queries are
         // answered from the base — so it belongs in the same place.
         ...this.preaggregateAccessWarnings(),
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
         // `origin` rides the projection for the same reason it rides the routing
         // metric: this list now contains rows whose `sourceName` names nothing in
         // any model file, and a caller resolving them against the package's
         // models would read a miss as corruption. An added attribute breaks no
         // consumer, where a caller that has to guess from the name shape would be
         // guessing from a digest.
         metadata.storageServeBindings = this.storageServeBindings.map((b) => ({
            origin: b.origin ?? "persist",
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
    * Record that `uri`'s manifest is bound to the served models, and report the
    * package as bound.
    *
    * Asserts the status rather than deriving it, so it holds for a bind that
    * carried no colocated entries to count — see the call site in
    * `Environment.bindManifest` for why the count cannot stand in for it. Called
    * once both tiers have applied, so it is authoritative over anything
    * {@link recordManifestBinding} computed earlier in the same bind.
    *
    * `unbound` remains the state for a package with no manifest bound, and
    * {@link markManifestBindFailed} still owns the degraded `live_fallback`.
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
    *
    * Filtered against {@link colocatedSourceEligibility} the same way
    * {@link bindStorageServeBindings} filters against `sourceEligibility` —
    * POSITIVELY, and dropped rather than fatal on a miss. For an authorize-gated
    * colocated source with a self-built manifest entry that predates the model
    * changing under it (a source built ungated, then re-annotated with an
    * unattributed or unclassifiable gate before the next rebuild converges),
    * THIS filter is what stops that stale entry from being bound here.
    *
    * It is ONE of four paths that reach {@link recordManifestBinding}, and the
    * only one this filter covers — this is the load-time local-store restore
    * path ({@link Environment}'s call at package-load time). The other three
    * all go through {@link reloadAllModels} (`bindManifest`,
    * `bindPackageColocatedServeManifest`, and the materialization service's own
    * `reloadAllModelsForPackage`), which writes `entries` into
    * `recordManifestBinding` UNFILTERED — no eligibility check runs there at
    * all. Those three paths are safe anyway, but not because of anything here:
    * they rely entirely on the per-query `#(authorize)` graft re-applying at
    * read time (`Model.buildGraftedMaterializer`), the same mechanism that
    * makes admitting a proven row-level gate safe in the first place. This
    * function's positive check is a belt for the one path that has no
    * recompile to re-derive eligibility from; it is not a boundary the other
    * three paths pass through, and nothing should be built assuming it is.
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
         // Distinct from the per-entry warnings above: this package's build
         // plan never computed at all, so EVERY colocated binding just
         // dropped — not one source's own refusal. Colocated is the default
         // tier (not gated by PERSIST_STORAGE_MODE), so this is a silent
         // whole-tier availability regression until the next successful load.
         logger.warn(
            "Colocated serve bindings dropped package-wide: the build plan never computed, so no source could be examined for eligibility",
            { packageName: this.packageName, droppedCount: dropped },
         );
      }
      this.recordManifestBinding(allowed);
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
         groupAliasesByName(Object.values(this.buildPlan?.sources ?? {})),
      );
      const eligibility = this.sourceEligibility;
      // Keyed by source NAME, and a rollup's name is generated — so a rollup
      // binding passes this filter only because `collectSourceEligibility` walks
      // the compiled sources, which INCLUDE the synthesized rollups, and records
      // them under those generated names.
      //
      // That is load-bearing and easy to break from a distance: scoping
      // `collectSourceEligibility` to authored sources would switch off every
      // storage rollup in every package, with one warn line per binding as the
      // only symptom and nothing else failing. Stated here because this is where
      // the dependency is consumed rather than where it is created.
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
               // A rollup's name is generated, so on its own it names nothing an
               // operator can look up. Saying what it is rescues the line.
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
    * Surface entries that don't resolve to a real `.malloy` model in this
    * package, each with an actionable reason. Empty when there is no surface
    * or every entry resolves. In practice this can only fire for an `explores`
    * an author wrote: a surface derived from a root `index.malloy` names a
    * file that was just read off disk, so it always resolves.
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

   /**
    * Warnings for the `#@ persist` sources the compile-time eligibility gates
    * refused, read off the build plan's `refusedSources`.
    *
    * The gate's own message is carried VERBATIM. It already names the source, the
    * reason and the remedy, and rebuilding a second wording here from `reason`
    * would be a second text to keep in step with the first — one that drifts
    * silently, since nothing compares them. The cost is that the message repeats
    * the source name the `subject` field also carries; that is the cheaper defect.
    *
    * `subject` names a rollup by its BASE source, never by its own. A rollup's
    * name is synthesized and appears in no file the author can open, so the
    * generated name answers a question nobody asked (the reason
    * {@link storageWarnings} does the same). The base is reachable only while the
    * entry is ALSO in `sources`, which for this tier is the authorize refusal
    * alone — a storage-refused rollup is deliberately absent from `sources`, and
    * there the subject is omitted rather than filled with the digest.
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

   private storageWarnings(): ApiPackageWarning[] {
      const mode = getPersistStorageMode();
      if (mode === "on" || !this.buildPlan?.sources) return [];
      const warnings: ApiPackageWarning[] = [];
      for (const source of Object.values(this.buildPlan.sources)) {
         const storage = source.annotationFields?.storage?.trim();
         if (!storage) continue;
         // A rollup needs its own wording, and not for tidiness. The authored
         // message says the source "is served live from its own warehouse", which
         // for a rollup is doubly wrong: a rollup is not a source anyone queries
         // by name, and there is no live reading of it to fall back to — the
         // acceleration simply does not happen and every query is answered from
         // the base, correctly. Reusing the authored text would also imply the
         // annotation is merely inert, when the thing an operator needs to know is
         // that nothing was built at all.
         const rollup = source.preaggregate;
         // Named by the BASE source and the grain, never by the rollup's own
         // name. A rollup's name is generated and appears in no file the author
         // can open, so a warning carrying it — as the `subject` did — gives them
         // nothing to act on: they wrote `storage=` on a measure and get back a
         // digest. The base and grain are what they typed, and the wire plan
         // carries both right here.
         //
         // This is the only signal in the DEFAULT configuration. `off` is what
         // every deployment ships with, so this warning, not any refusal, is what
         // a first author of a `storage=` rollup actually reads — and in a feature
         // whose characteristic failure is correct answers with no acceleration
         // and no error, a warning nobody can trace back is one step from no
         // signal at all.
         // The grain is rendered as a SET, not quoted back. The wire plan carries
         // only canonically sorted, de-duplicated dimensions — the authored text
         // does not cross that boundary — so quoting would misrepresent any
         // multi-dimension grain the author did not happen to write in
         // alphabetical order. A set claims only what is true of it.
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
            // The base source for a rollup: the thing the author can find. Its
            // own generated name would be the accurate answer to a question
            // nobody asked.
            subject: rollup ? rollup.baseSourceName : source.name,
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
   /**
    * The violations that make a package invalid, which is every one except the
    * access-modifier pair. Used by BOTH the publish gate and the load gate.
    *
    * Those two warn instead, and appear in the package's `warnings`. Two reasons,
    * and neither is that they matter less.
    *
    * Upgrade safety: before this validation existed a package annotating a hidden
    * field published and loaded — nothing read access modifiers, and the companion
    * whose compile would have failed is swallowed — so refusing it now would stop
    * an already-published package from loading because a new rule was added, and a
    * permanent load failure is retried rather than abandoned.
    *
    * And they produce a refusal an author cannot act on. An annotation INHERITED
    * onto a source that then hides the measure raises this, while that source
    * produces no plan at all — the base's rollup is unaffected and nothing is
    * exposed. The message asks them to make the field public or drop an annotation
    * that is not on that source, so following it means giving up either the hiding
    * or the rollup. Refusing a safe model with unfollowable advice is worse than
    * saying plainly that the annotation does nothing here.
    *
    * Safe to warn because the refusal was never the enforcement: the planner skips
    * a hidden measure and a hidden grain dimension outright
    * (planSourcePreaggregation), so no plan exists, nothing is built, and nothing
    * can be served whether or not anyone reads the message. Which is also why that
    * skip has its own tests rather than resting on this gate.
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
         // A source-level violation carries no field, so the source is the
         // subject; a field-level one names the field.
         subject: violation.fieldName ?? violation.sourceName,
         message: violation.message,
      }));
   }

   private preaggregateMessages(
      keep: (code: PreaggregateViolationCode) => boolean,
   ): string[] {
      return this.preaggregateFindings(keep).map(
         // The model path is prepended because the same source name can occur
         // in two models, and the author needs to know which file to open.
         ({ modelPath, violation }) => `${modelPath}: ${violation.message}`,
      );
   }

   /**
    * Violations kept by `keep`, paired with the model that produced them.
    *
    * Structured rather than pre-formatted because the two consumers want
    * different things: the publish/load gates want one prose blob, while the
    * warnings array wants `{model, subject, message}` like every other member of
    * it. Flattening first would make the second parse back out what the violation
    * already carries.
    */
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
         if (this.getReportLayout(modelPath) === "linear") continue;
         if (exploreSet && !exploreSet.has(modelPath)) continue;
         // A report exports nothing by design; it reads the surface.
         if (exploreSet && this.isServedReport(modelPath)) continue;
         if (model.hasEmptyDiscoverySurface()) {
            // When the file IS the whole surface, nothing in the package can be
            // queried, which is the thing worth saying. One empty file among
            // several listed ones only takes itself off the surface.
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
    * One message when every model on this package's surface failed to compile.
    *
    * The curated surface is the union of what the listed models export, so a
    * surface that does not compile exports nothing, and the boundary then
    * refuses every model in the package -- including the ones that compiled
    * perfectly well. Each gets the 404 a hidden model gets: the same words a
    * missing model gets where the model is gated, and an off-surface
    * explanation where it is not.
    *
    * NARROW ON PURPOSE, because the edit paths an author uses already report
    * this better than a warning could:
    *
    *  - FIRST LOAD: a compile error fails the whole package. It is absent, and
    *    named in `/status` loadErrors. Nothing serves an empty surface.
    *  - RELOAD, whether from the chokidar watcher, MCP `reload_package` or REST
    *    `?reload=true`: all three go through `Environment.loadPackage`, which
    *    keeps the last good compiled model serving and records a
    *    `staleCompileErrors` entry, so `/status` reports the package with
    *    `stale: true` AND the compile error. The surface never empties.
    *
    * What is left is {@link reloadAllModels} called directly -- the
    * materialization / manifest rebind paths (`reloadAllModelsForPackage`,
    * `bindManifest`). Those replace a model that fails to compile with a
    * placeholder and do NOT go through `loadPackage`, so they are the one way
    * to reach a package that is serving, is not stale, and whose surface
    * exports nothing. Rare, and the only path where nothing else would say so.
    *
    * Deliberately NOT a fallback to uncurated. Falling open on a typo would
    * expose sources the author curated away, which is worse than refusing them;
    * the gap is that the refusal is unexplained, so this explains it.
    * {@link emptyDiscoveryWarnings} does not cover this -- it skips a model
    * that failed to compile, by design, so that a compile error is reported
    * once rather than twice.
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
      // Only when NOTHING on the surface compiled. One broken file beside a
      // working one still leaves a surface, and that file's own error is
      // report enough.
      if (!surface.every(([, model]) => !!model.getCompilationError())) {
         return [];
      }
      // Models off the surface that compiled FINE, which is the count worth
      // reporting: they are the ones refused for someone else's typo. Served
      // dashboards and notebooks are not on the surface and not refused. A hidden
      // model that failed to compile of its own accord was not taken down by
      // this, and has its own error on the listing.
      const collateral = Array.from(this.models.entries()).filter(
         ([modelPath, model]) =>
            modelPath.endsWith(MODEL_FILE_SUFFIX) &&
            !exploreSet.has(modelPath) &&
            !this.isServedReport(modelPath) &&
            !model.getCompilationError(),
      ).length;
      // ONE message, however many files the surface spans. Emitting it per
      // file would have each copy claim to be the whole surface, which is only
      // true when the surface is one file.
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
            environmentName: this.environmentName,
            malloyConfig: this.malloyConfig,
            defaultConnectionName: "duckdb",
            buildManifest,
            withPreaggregateCompanions: true,
         });
      } catch (err) {
         const realError =
            err instanceof Error
               ? err
               : new Error(`Package-load worker pool failure: ${String(err)}`);
         if (
            realError instanceof MalloyError ||
            realError instanceof ModelCompilationError ||
            realError instanceof PackageManifestError
         ) {
            throw realError;
         }
         throw new ServiceUnavailableError(
            `Package-load worker pool unavailable: ${realError.message}`,
            { cause: realError },
         );
      }

      // Same check as create, before anything is swapped.
      assertRequiredStagesAvailable(
         outcome.packageMetadata.retrieval ?? DEFAULT_PACKAGE_RETRIEVAL,
         llmConfigured(),
      );

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
      // Discover against the models about to be installed, before any of them
      // is, and install both in one synchronous step below: a query must never
      // see the new models with the previous reports (or the reverse), whose
      // entry points and surface flags differ.
      const discoveryStarted = performance.now();
      const discovery = await this.discoverReportsIn(nextModels, "reload");
      const discoveryMs = performance.now() - discoveryStarted;
      this.models = nextModels;
      // Before refreshServingIdentity, which reads the agents it declares.
      this.rawAgents = outcome.packageMetadata.agents;
      // The identity describes the source bytes just compiled, and a caller
      // polling it after an edit needs it to move as soon as the new models
      // are installed.
      this.refreshServingIdentity();
      // A reload re-reads publisher.json in the worker; pick up any change to
      // the explore set and query-boundary mode so listModels()/the gate
      // reflect edited explores without a full Package.create.
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
      // The freshly-compiled models start with no serve bindings and no serve
      // connections; re-apply both so a reload preserves serve routing.
      this.pushStorageServeBindingsToModels();
      this.pushServeDestinationConfigToModels();
      // Same for pre-aggregation, and this is also where a manifest bind lands
      // (bindManifest → reloadAllModels), so the companion is recompiled against
      // the manifest that substitutes its rollup tables.
      await this.pushPreaggregateServeModels();
      // Read against the reports installed above.
      this.renderTagWarnings =
         this.surfacedRenderTagWarnings(renderTagWarnings);
      this.manifestWarnings = outcome.packageMetadata.manifestWarnings ?? [];
      this.recordDeprecatedUsage(outcome);
      this.retrievalSettings =
         outcome.packageMetadata.retrieval ?? DEFAULT_PACKAGE_RETRIEVAL;
      // The tile lint asks the query boundary about each tile, so it runs once
      // the refreshed surface is installed.
      const lintStarted = performance.now();
      await this.relintReports({ textLint: true });
      this.reportDiscovered(
         "reload",
         discovery,
         discoveryMs + performance.now() - lintStarted,
      );
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

   public async listModels(
      options: { includeHiddenFilesAndSources?: boolean } = {},
   ): Promise<ApiModel[]> {
      // When the package resolved a surface — an `explores` in publisher.json
      // or a root `index.malloy` — only those models are listed; every other
      // .malloy file still compiles for import/join resolution but is hidden.
      // No surface → every model is listed (backward-compatible default), but
      // note that means no surface, not merely no manifest key. Notebooks are
      // unaffected (see listNotebooks) — they are always public.
      //
      // `includeHiddenFilesAndSources` lists the hidden files too, each marked
      // `isHidden: true`. Running them takes the same option on the query
      // route (Model.getQueryResults).
      const exploreSet = this.exploreSet();
      const isHidden = (modelPath: string) =>
         !!exploreSet && !exploreSet.has(modelPath);
      const values = await Promise.all(
         Array.from(this.models.keys())
            .filter((modelPath) => {
               if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) return false;
               // A linear report is listed as a report, surface or not.
               if (this.getReportLayout(modelPath) === "linear") return false;
               // A grid report is not a model on the surface even when
               // `explores` lists it: it is listed as a report instead.
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
    * A file's text as the compile read it, else as saved; undefined when it
    * cannot be read. The compiled text first, because a package compile's
    * replacement is not on disk and the saved file would answer for the old
    * text. Never throws.
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
    * What a `.malloy` that can be a report is, by the one tag reader
    * (`report.ts`): a compiled file by its own model-level notes, and a file
    * with no facts (it failed to compile, or its facts could not be read) by
    * the same notes read off its text. Never throws: a file it cannot read is
    * no report.
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
    * Re-read the package's reports from its compiled models and install them.
    * Called at load, and as a dry run from a package-scope compile; a reload
    * discovers against the models it is about to install instead (see
    * {@link reloadAllModels}), so nothing is ever served with the previous
    * set. Never throws: a package whose reports can't be read still serves its
    * models.
    *
    * `compile` is the dry run on the throwaway package
    * {@link lintWorkerOutcome} builds: it skips reading cells and the per-file
    * text lint (the package-scope compile reports those itself, with
    * positions), and is timed but neither counted nor logged as a load (see
    * {@link reportDiscovered}). A check that throws is a server fault, not a
    * finding, so it is still logged, marked `during: "compile"`.
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

   /**
    * Report what one discovery pass found. Every pass is timed. A load or
    * reload also counts its files, reports and findings, logs one `info`
    * summary and a `debug` line per finding (the findings themselves ride the
    * package's `warnings`). A compile's dry run is a what-if, so it stops at
    * the timing.
    */
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
    * Count the deprecated features this package uses, once per load or
    * reload: each seam counts its own (`service/deprecated/*`).
    */
   private recordDeprecatedUsage(outcome: LoadPackageOutcome): void {
      recordDeprecatedFormatUsage(this.models);
      recordDeprecatedManifestKeys(
         outcome.packageMetadata.deprecatedManifestKeys,
      );
   }

   /**
    * What every file of `models` is as a report (`docs/reports-design.md`
    * §1): one map, keyed by path in path order, with a status per file that
    * carries an artifact tag. Where a file lives decides nothing: any `.malloy`
    * outside `public/` whose model-level tag makes it a report is one, except
    * the root `index.malloy`, which is the surface. A `.malloynb` is a linear
    * report. Two reports may share a basename; neither shadows the other.
    *
    * Reads `models` only, never the package's installed state, so a reload can
    * discover against the models it has not swapped in yet. Reads each linear
    * report's cells into its model, unless `during` is `compile`.
    */
   private async discoverReportsIn(
      models: ReadonlyMap<string, Model>,
      during: ReportDiscoveryDuring,
   ): Promise<ReportDiscovery> {
      const files = new Map<string, ReportFile>();
      // Every model, not just the reports, because `# drill` is declared on a
      // model dimension: a tag in a package with no reports at all is exactly
      // as breakable and would otherwise never be checked.
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
            // A tag that makes no report (it does not parse, sets the removed
            // `kind=`, or is a linear report written as `run:` cells): the lint
            // says why, and the file is an ordinary model.
            files.set(modelPath, { status: "invalid", name, facts });
            continue;
         }
         if (!facts && !model.getCompilationError()) {
            // No facts and no compile error to show, which a corrupt worker
            // payload can produce (`Model.fromSerialized` builds a model with
            // neither). A report disappearing with nothing said is the case
            // worth reporting.
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
            // The tag is readable (it was read off the same facts); it was the
            // manifest CONSTRUCTION that failed, and a report has nothing else
            // to show.
            files.set(modelPath, { status: "dropped", name });
            continue;
         }
         if (facts && !manifest) {
            // A grid tag that describes no tiles (or an empty list): not
            // served until it has one, and explained by the lint. A linear
            // tag always lists its tiles (`report.ts`), so it never lands here.
            files.set(modelPath, { status: "tileless", name, facts });
            continue;
         }
         if (!facts) {
            // The file failed to compile, so its tiles cannot be read. It is
            // listed anyway carrying the error, because a broken report
            // should be visibly broken rather than absent.
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
         // Every report is served whatever the surface is: the surface limits
         // what its tiles may read, not whether it is listed (see
         // applyQueryBoundaryToModels and lintTilesAgainstSurface).
         files.set(modelPath, {
            status: "served",
            name,
            facts,
            report: { path: modelPath, name, layout: tag.layout, manifest },
         });
      }
      // The compiled text of each `.malloy` report, so the query boundary
      // derives a report's own sources from the text its IR came from (at a
      // package-scope compile, the caller's replacement), and the cell reader
      // never slices a file newer than the compile.
      for (const file of files.values()) {
         const report = file.report;
         if (!report || !report.path.endsWith(MODEL_FILE_SUFFIX)) continue;
         const model = models.get(report.path);
         if (!model) continue;
         // Unreadable text only costs the file its own derived sources: it is
         // still a report, reading the surface directly.
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
      const tileDefinitions = new Set<string>();
      for (const report of this.reports.values()) {
         if (report.layout !== "grid") continue;
         const modelDef = this.models.get(report.path)?.getModelDef();
         for (const tile of queryTiles(report.manifest?.tiles)) {
            const key = definitionKey(modelDef, tile.query);
            if (key) tileDefinitions.add(key);
         }
      }
      const kept = dropGridTileLayoutFindings(raw, (finding) => {
         if (finding.model === undefined || finding.subject === undefined)
            return false;
         const key = definitionKey(
            this.models.get(finding.model)?.getModelDef(),
            finding.subject,
         );
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
    * Read every linear `.malloy` report's definition cells (its imports and
    * givens), which the deprecated notebook route serves ahead of its tiles. A
    * reader refusal is the report's error, returned as a finding: the model
    * still compiles, so nothing else would say so. It is counted and logged
    * with the other findings (see {@link reportDiscovered}).
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

   /**
    * Re-run the report lint against the current surface. Discovery calls it;
    * so does a metadata PATCH, which can change the surface without changing
    * any file, so the tile findings stay true to what is served.
    */
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
    * One finding per tile or filter suggest that the query
    * route will refuse for reading a source the surface does not publish. The
    * report is listed and every tile compiles, because /compile is exempt
    * from the surface, so without this the author sees nothing wrong until a
    * tile answers 404.
    *
    * Asks the model the same two checks the query route runs
    * ({@link Model.surfaceRefusal}), so the lint and the query cannot
    * disagree. A request that does not compile is left to the other lints.
    */
   private async lintTilesAgainstSurface(
      modelPath: string,
      manifest: ServedReportManifest,
   ): Promise<ApiPackageWarning[]> {
      const model = this.models.get(modelPath);
      if (!model || !this.queryBoundaryActive()) return [];
      // `source` is absent when the query route's refusal would not name it
      // (a gated model), and then neither does the warning.
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
      // Each check compiles one query against the loaded model, independently
      // of the others, so they run together; the order of findings is kept.
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
    * The report lint: one pass per report, of either layout, wherever it
    * lives, and per file whose tag makes no report, plus the package-wide
    * checks (drill, given tags, curation).
    *
    * Per file, the manifest checks (`lintReport`, or for a tagged file that
    * produced no report, `lintUndiscoveredReport`) and the text checks
    * (`lintNotebookText`, with line numbers) run together, so the one finding
    * both can make, an artifact tag that does not parse, is reported once.
    *
    * Never throws: a lint failure must not cost the package its reports. The
    * findings ride the package response as non-fatal `warnings`, the same
    * surface render-tag findings use, rather than a separate verb.
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
         // First because they cannot throw, so these survive a truncation that
         // costs everything after them, and a report that vanished with no
         // explanation is the worst thing on this surface to lose.
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
            // A name outside the documented pattern is served and only noted:
            // it still addresses the deprecated `…/dashboards/{name}` route,
            // encoded.
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
            // A served report is checked against its manifest; a `.malloynb`
            // has none (and no facts).
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
         // Drill tags live on model dimensions, not on any one report, so
         // they are reported against the package rather than a single file,
         // and they are scanned across every model rather than only the
         // reports. A report dropped because it could not be read is still
         // where a drill points; its own finding states the cause.
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
         // Scanned across every model for the same reason as the drill lints: a
         // given is declared on a model and reached through imports, so a broken
         // declaration in a file no report imports is exactly as silent.
         for (const finding of lintGivenTags(drillFacts)) {
            warnings.push(finding);
         }
         for (const finding of await this.unsupportedComponentWarnings()) {
            warnings.push(finding);
         }
      } catch (err) {
         logger.warn("Report lint failed", {
            packageName: this.packageName,
            error: errMessage(err),
         });
         // Say so on the surface the findings ride, not only in the server log.
         // This catch wraps every phase above, so a throw keeps the findings
         // collected up to that point and drops the rest, and a shorter list is
         // indistinguishable from a cleaner package. It names the check
         // CLASSES rather than a phase, because it cannot tell which was lost.
         // It omits the dropped-by-error and unconventional-name kinds on
         // purpose: those are pure pushes over what discovery computed, so
         // they always survive. The thrown message stays in the log: it is an
         // ARBITRARY throw, so its text is unbounded.
         warnings.push({
            message:
               `Report lint stopped early, so this list is incomplete: an ` +
               `unknown subset of the tile, report, drill, given, curation ` +
               `and unsupported-component checks did not run. Treat a missing ` +
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
    * Each file on the curated surface, with the file that puts it there: the
    * one that imports it on the way from the root `index.malloy`, directly or
    * through other files, or `explores` for a file `explores` lists. A file
    * absent here is off the surface. The same closure decides
    * `report-on-surface` and the worker's `explores` advice reads reports by
    * the same tag reader, so following that advice cannot move a file on or
    * off the surface.
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
    * The curation edges reports anywhere make reachable (§6): a tagged root
    * `index.malloy`, whose tag is ignored because it is the surface; and a
    * report on the surface (`index.malloy` imports it, directly or through
    * other files, or `explores` lists it), which is served as a report and
    * adds nothing to the surface.
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

   /**
    * A `.jsx` (or `.tsx`) in the package's top-level `dashboards/` directory.
    *
    * That directory means nothing to Publisher, but it is where Malloyyo looks
    * for a dashboard's custom component, and Publisher does not implement
    * that, so the file is inert here. This exists for the migration case: a
    * Malloyyo repo registered unchanged would otherwise show its custom
    * dashboards as an empty tile grid, or as nothing at all, with no
    * indication that a file was skipped. Say so at load, and name the surface
    * that does run author-written code.
    */
   private async unsupportedComponentWarnings(): Promise<ApiPackageWarning[]> {
      const dir = safeJoinUnderRoot(this.packagePath, MALLOYYO_DASHBOARDS_DIR);
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
            model: `${MALLOYYO_DASHBOARDS_DIR}/${entry}`,
            subject: base,
            message:
               `Custom dashboard components are not supported, so ` +
               `"${MALLOYYO_DASHBOARDS_DIR}/${entry}" is ignored. Any ` +
               `${MALLOYYO_DASHBOARDS_DIR}/${base}${MODEL_FILE_SUFFIX} beside it still ` +
               `renders from its tags. For a page that runs its own code, use ` +
               `an HTML data app in the package's public/ directory.`,
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
    * artifact tag, whatever discovery made of it (a report, served or not,
    * or a tag that makes none, which saving the fix is how it gets served).
    * Never a model, an untagged file, a `.malloynb`, the root `index.malloy`,
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
         // The builder opens a `.malloy` report, which is written as tiles; a
         // `.malloynb` it cannot.
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
            // A report whose notebook cannot be read answers with its error
            // rather than failing the request.
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
      } catch (error) {
         // A missing manifest, or a package path that is not a directory,
         // is "does not exist". Anything else, an EACCES on the package
         // directory above all, is rethrown so it is reported as what it is.
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
      this.applySiblingModelResolverToModels();
   }
}
