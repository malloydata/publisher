// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   BuildTarget,
   MalloyConfig,
   Connection as MalloyConnection,
   Model as MalloyModel,
   ModelDef,
   ModelMaterializer,
   PersistSource,
   Runtime,
   SourceDef,
} from "@malloydata/malloy";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import {
   persistAnnotationFindings,
   persistUnparseableFinding,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import { routeHasKey } from "../../motly";
import { Annotations, isSourceDef } from "@malloydata/malloy";
import { components } from "../../../api";
import { MaterializationEligibilityError } from "../../../errors";
import { logger } from "../../../logger";
import { recordEligibilityRefused } from "../../../materialization_metrics";
import { errMessage } from "../../../utils";
import {
   createGateClassificationDeps,
   collectEntryPointGates,
   resolveGateShape,
   type GateClassificationDeps,
   type GraftScope,
} from "../../gate_classification";
import { malloyGivenToApi, type MalloyGiven } from "../../given";
import {
   resolveIncrementalDeclaration,
   type IncrementalDeclaration,
   type PairDelta,
} from "./incremental_declaration";
import {
   assertColocatedPersistNotAuthorizeGated,
   assertMaterializationEligible,
   assertRollupNameUnshared,
   assertTierEligible,
   isAuthorizeAttributedToEntryPoint,
} from "./materialization_eligibility";

import { type SynthesizedRuntimeFactory } from "./preaggregation_compile";
import type { RollupPlan } from "./preaggregation_synthesis";
import {
   materializeRefusalFinding,
   pairFullRefreshFinding,
   pairKeyColumns,
   pairLevelRowColumn,
   type MaterializePlan,
} from "./build_file_generator";
import { tryCompileMaterializeCompanion } from "./materialize_compile";
import { persistedViewRefusalFinding } from "./persisted_view";
import { tryCompilePersistedViewCompanion } from "./persisted_view_compile";
import { deletingWindowsOf } from "./rolling_window";
import { tryCompileRollupCompanion } from "./rollup_compile";
import { compileV1BuildCompanion } from "./v1_build_companion";
import { rollupGateEnforcedAtRead } from "./rollup_plan";
import { rollupRefusalFinding } from "./rollup_plan";
import { getMaterializeEnabled } from "../../../config";
import { admitGivenPositions, type DynamicTerm } from "./persist_dynamic_terms";
import { resolvePartitionColumns } from "./persist_partition";
import {
   deriveAnnotationFields,
   deriveColumns,
   type PersistSourceGateClassification,
   type PersistSourceGateOutcome,
} from "./persist_source_fields";
import { quoteIdentifier } from "../../quoting";
import {
   addressSQL,
   computeSourceEntityId,
   isUnkeyable,
   plannedSources,
   presentAs,
   recordTarget,
   renameBuildSourceIDs,
   withoutSourceIDs,
   sourceBuildsOf,
   type SourceBuild,
} from "./addressing";
import {
   collectColocatedSourceEligibility,
   collectSourceEligibility,
   collidingRollups,
   refuseCollidingMaterialize,
   storedBaseOfRollup,
   type ColocatedSourceEligibility,
   type SourceEligibility,
} from "./eligibility_collect";
import { resolveFreshness, resolveQueryMetadata } from "./plan_layers";
import {
   finalizeOmissions,
   omittedByID,
   type Omission,
   type OmittedSourceReason,
} from "./omitted_sources";

// Addressing, plan layers and eligibility collection live in their own
// modules; re-exported so every existing import of build_plan keeps working.
export {
   addressSQL,
   addressText,
   buildSQLOf,
   buildTargetOf,
   compilerBuildId,
   computeSourceEntityId,
   plannedSources,
   resolveConnectionDigests,
   setTestAddressForTarget,
   sourceBuildOf,
   sourceBuildsOf,
   sourcesInBuildOrder,
   type SourceBuild,
} from "./addressing";
export {
   collidingRollups,
   storedBaseOfRollup,
   tierGateInputs,
   type ColocatedSourceEligibility,
   type SourceEligibility,
} from "./eligibility_collect";
export {
   composeDeclaredQueryMetadata,
   resolveFreshness,
   resolveQueryMetadata,
   type ReadableTag,
} from "./plan_layers";

export {
   deriveAnnotationFields,
   deriveColumns,
   type PersistSourceGateOutcome,
} from "./persist_source_fields";
import type { WirePackageMaterialization } from "./plan_layers";

type WireBuildGraph = components["schemas"]["BuildGraph"];
type WirePersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type WireRefusedSource = components["schemas"]["RefusedSource"];
type WireStrippedTerm = components["schemas"]["StrippedTerm"];
type WireJoinedTerms = components["schemas"]["JoinedTerms"];
type BuildPlan = components["schemas"]["BuildPlan"];
export type { WirePackageMaterialization };

/**
 * Minimal surface a package must expose to compile its build plan; satisfied by
 * {@link Package} and the materialization service.
 */
export interface BuildPlanPackage {
   getModelPaths(): string[];
   getPackagePath(): string;
   getMalloyConfig(): MalloyConfig;
   getMalloyConnection(name: string): Promise<MalloyConnection>;
   /**
    * The package-level `materialization` config (publisher.json), the
    * least-specific layer for per-source resolution. Optional: callers without
    * it resolve with no package default.
    */
   getMaterializationConfig?(): WirePackageMaterialization | null;
}

/**
 * Result of compiling a package's persist sources. The wire {@link BuildPlan} is
 * a projection of this (see {@link deriveBuildPlan}); the build engine consumes
 * the full structure.
 */
export interface CompiledBuildPlan {
   builds: SourceBuild[];
   sources: Record<string, PersistSource>;
   /**
    * sourceID -> the build target the source maps onto, for every keyable
    * source in `sources` (an unkeyable one has none). Pass it to the
    * addressing helpers to address a source without the fallback lookup.
    */
   targets?: Record<string, BuildTarget>;
   connectionDigests: Record<string, string>;
   connections: Map<string, MalloyConnection>;
   /**
    * sourceID -> package-relative path of the declaring `.malloy` model. Only
    * known here: a sourceID embeds an absolute `file://` URL with no package
    * boundary.
    */
   sourceModelPaths?: Record<string, string>;
   /**
    * `#@ persist` sources Malloy's build walk did NOT recognize as persistable
    * (see {@link detectDroppedPersistSources}). Callers surface these as a
    * load-time warning and a hard build failure so the annotation is never a
    * silent no-op.
    */
   droppedPersistSources?: { name: string; modelPath: string }[];
   /** Findings on the authored `#@` annotations (see annotation_reader). */
   annotationFindings?: PackageAnnotationFinding[];
   /**
    * sourceID -> the rollup it was synthesized from; absent for an authored
    * `#@ persist` source. The provenance behind the wire plan's
    * `origin`/`preaggregate` fields.
    */
   preaggregatePlans?: Record<string, RollupPlan>;
   /**
    * sourceID -> the declared primary key of a planned source whose stored
    * table a build checks for a repeated or NULL key, when it is stored in a
    * destination: set when a `storage=` rollup is read through a join keyed
    * on a source's primary key as a join_many (`readsAsJoinMany`), so the
    * join-to-rollup leg in a destination (#1704) can read the stored rows'
    * key as unique. Absent when no such leg exists.
    */
   storedKeyChecks?: Record<string, string>;
   /**
    * sourceID -> entry-point `#(authorize)` gate classification, computed at
    * compile time where the `{modelDef, materializer}` pair exists (see
    * {@link classifyPersistSourceGate}). Read by
    * `assertColocatedPersistNotAuthorizeGated` to decide whether to admit a gated
    * source.
    */
   sourceGateOutcomes?: Record<string, PersistSourceGateOutcome>;
   /**
    * sourceID -> the `#(materialize)` declaration a generated build source was
    * written from; absent for an authored `#@ persist` source and a rollup.
    * Read by the re-root rewriter, which serves the author's source from the
    * generated source's table (materialization v2, phase 3).
    */
   materializePlans?: Record<string, MaterializePlan>;
   /**
    * Sources planning left out because it FAILED (a companion that did not
    * compile), for the wire plan's `omittedSources`. Absent: none.
    */
   omittedSources?: Omission[];
}

function wireTerm(term: DynamicTerm): WireStrippedTerm {
   return { code: term.code, givens: term.givens };
}

/**
 * The plan's `partition`, `strippedTerms` and `joinedTerms` for one source.
 * Omits a field rather than reporting a wrong one (the gate normally refuses such
 * a source first). Informational only: the build re-resolves these.
 */
function planDynamicFields(
   source: PersistSource,
   annotationFields: Record<string, string>,
): {
   partition?: string[];
   strippedTerms?: WireStrippedTerm[];
   joinedTerms?: WireJoinedTerms[];
} {
   const out: {
      partition?: string[];
      strippedTerms?: WireStrippedTerm[];
      joinedTerms?: WireJoinedTerms[];
   } = {};
   const partition = resolvePartitionColumns(source, annotationFields);
   if (partition.ok && partition.columns.length > 0) {
      out.partition = partition.columns;
   }
   const classified = admitGivenPositions(source);
   if (classified.ok && classified.terms.length > 0) {
      out.strippedTerms = classified.terms.map(wireTerm);
   }
   if (classified.ok && classified.joinedTerms?.length) {
      out.joinedTerms = classified.joinedTerms.map((joined) => ({
         alias: joined.alias,
         source: joined.source,
         terms: joined.terms.map(wireTerm),
      }));
   }
   return out;
}

/**
 * A source's PUBLIC column names, or a throw. The strict counterpart to
 * {@link deriveColumns}: this list decides what gets WRITTEN, so "unknown" must
 * not read as "nothing to narrow". Empty throws too — it means everything
 * the build SQL projects is hidden.
 */
function publicColumnNames(persistSource: PersistSource): string[] {
   let names: string[];
   try {
      names = persistSource._explore.intrinsicFields
         .filter((f) => f.isAtomicField())
         .map((f) => f.name)
         .filter((n): n is string => typeof n === "string" && n.length > 0);
   } catch (err) {
      throw new Error(
         `the compiled source's field list could not be read (${errMessage(err)})`,
      );
   }
   if (names.length === 0) {
      throw new Error("the compiled source exposes no public atomic columns");
   }
   return names;
}

/**
 * Wrap a source's build SQL to project only its PUBLIC columns. The build SQL
 * emits every underlying column, including hidden ones (`except:`, non-public
 * access).
 *
 * A `storage=` build must not put a hidden column's values AT REST in the
 * destination, where direct catalog access reaches them past the source's
 * visibility rules (reads THROUGH the source are bounded separately by
 * `narrowSchemaToPublic`). Applies to both storage build paths.
 *
 * Fails CLOSED: an undeterminable public surface refuses the build before any
 * SQL runs, leaving the previous generation serving.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source.
 */
export function projectToPublicColumns(
   persistSource: PersistSource,
   buildSQL: string,
): string {
   let cols: string[];
   try {
      cols = publicColumnNames(persistSource);
   } catch (err) {
      recordEligibilityRefused("public_surface_unknown");
      throw new MaterializationEligibilityError({
         reason: "public_surface_unknown",
         message:
            `Source '${persistSource.name}' cannot be materialized into a ` +
            `storage destination: its public column surface could not be ` +
            `determined — ${errMessage(err)}. The publisher narrows a stored ` +
            `table to the source's public columns, so it refuses the build ` +
            `rather than materialize columns the source hides. This usually ` +
            `means the compiled-source shape changed; drop 'storage=' to serve ` +
            `this source live in the meantime.`,
      });
   }
   const projection = cols
      .map((n) => quoteIdentifier(n, persistSource.dialectName))
      .join(", ");
   return `SELECT ${projection} FROM (${buildSQL}) AS __public`;
}

/**
 * Resolve a Map<name, Connection> for the names a step is about to touch.
 * The package's MalloyConfig caches each lookup, so repeated calls are cheap.
 * A failed lookup is logged and omitted; downstream code reports the missing
 * connection explicitly.
 */
export async function resolvePackageConnections(
   pkg: { getMalloyConnection(name: string): Promise<MalloyConnection> },
   names: Iterable<string>,
): Promise<Map<string, MalloyConnection>> {
   const map = new Map<string, MalloyConnection>();
   const seen = new Set<string>();
   for (const name of names) {
      if (!name || seen.has(name)) continue;
      seen.add(name);
      try {
         map.set(name, await pkg.getMalloyConnection(name));
      } catch (err) {
         logger.warn(`Failed to resolve connection ${name}`, {
            error: errMessage(err),
         });
      }
   }
   return map;
}

/**
 * Names of sources carrying `#@ persist` that are absent from the model's build
 * plan.
 *
 * Backstop for a gap in Malloy's build walk: it silently plans nothing for a
 * persist source it doesn't treat as persistable (e.g. a filtered pass-through
 * `X is <table> extend { where … }`, which stays type `table`), making the
 * annotation a silent no-op. Detecting it here lets callers refuse loudly.
 *
 * Reads the annotation as Malloy's checkPersistAnnotation does. Fail-open: an
 * introspection failure yields no names rather than failing a healthy build.
 */
function detectDroppedPersistSources(
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   malloyModel: any,
   recognizedNames: Set<string>,
): string[] {
   const dropped: string[] = [];
   try {
      const contents = malloyModel?._modelDef?.contents as
         | Record<string, { type?: string; annotations?: unknown }>
         | undefined;
      if (!contents) return dropped;
      for (const [name, def] of Object.entries(contents)) {
         if (recognizedNames.has(name)) continue;
         if (!def || !def.annotations) continue;
         let isPersist = false;
         try {
            // Presence only, read over every line as Malloy reads it, so a
            // `#@ persist` line carrying an `@env.` reference is still caught.
            isPersist = routeHasKey(
               new Annotations(def.annotations),
               "@",
               "persist",
            );
         } catch {
            continue;
         }
         if (isPersist) dropped.push(name);
      }
   } catch {
      // Fail open: never let introspection break the build plan.
      return [];
   }
   return dropped;
}

/**
 * The sourceIDs of the `#@ persist` sources a compiled model holds, read as
 * {@link detectDroppedPersistSources} reads them: what a model's build copy
 * would have planned, for `omittedSources` when that copy does not compile.
 */
function declaredPersistSourceIDs(malloyModel: MalloyModel): string[] {
   const ids: string[] = [];
   try {
      for (const def of Object.values(malloyModel._modelDef?.contents ?? {})) {
         if (!isSourceDef(def) || !def.annotations) continue;
         try {
            if (routeHasKey(new Annotations(def.annotations), "@", "persist")) {
               if (def.sourceID) ids.push(def.sourceID);
            }
         } catch {
            // An annotation that cannot be read plans nothing either.
         }
      }
   } catch {
      // Fail open, as detectDroppedPersistSources does.
   }
   return ids;
}

/**
 * The sourceIDs of the sources in `malloyModel` carrying a `#@` note at `at`
 * (a `#@` line that did not parse), read in the model's own coordinates.
 * Fail-open: an unreadable annotation names nothing.
 */
function sourceIDsWithNoteAt(
   malloyModel: MalloyModel,
   at: { url: string; range: { start: { line: number } } },
): string[] {
   const ids: string[] = [];
   try {
      for (const def of Object.values(malloyModel._modelDef?.contents ?? {})) {
         if (!isSourceDef(def) || !def.annotations || !def.sourceID) continue;
         try {
            const notes = new Annotations(def.annotations).forRoute("@");
            if (notes.some((note) => noteCovers(note, at))) {
               ids.push(def.sourceID);
            }
         } catch {
            // An annotation that cannot be read names nothing.
         }
      }
   } catch {
      // Fail open, as detectDroppedPersistSources does.
   }
   return ids;
}

/**
 * Classify one persist source's entry-point `#(authorize)` gate(s) and decide
 * whether it is `attributed` — see {@link PersistSourceGateOutcome}.
 *
 * `materializer` MUST be the one that compiled `modelDef`: a fresh `loadModel`
 * mints a new given identity per call (see `liftGateCondition`).
 *
 * Each `GateEntry` is one AND'd group; groups combine by enforcement's dominance
 * rule — refuse if ANY group rejects, else `row_level`. No gate at all is
 * vacuously `row_level`; `attributed` still catches a gate behind a join.
 *
 * Fails CLOSED: any throw records `rejected` + `attributed: false`.
 *
 * Network-free: the lift compiles a probe through the already-loaded
 * materializer, generating SQL text only. This is the only place the
 * `row_level`/`rejected` outcome is computed (load-time probe validation keeps
 * only warnings).
 */
export async function classifyPersistSourceGate(
   persistSource: { _sourceDef: SourceDef; sourceID: string },
   modelDef: ModelDef,
   materializer: ModelMaterializer,
   deps: GateClassificationDeps,
   cacheScope: string,
): Promise<PersistSourceGateOutcome> {
   try {
      const entryStruct = persistSource._sourceDef as SourceDef;
      const groups = collectEntryPointGates(
         entryStruct,
         modelDef,
         new Set(),
         // This IS the entry point (`treatAsOwnGate`).
         true,
      );
      const graftScope: GraftScope = { modelDef, materializer, cacheScope };
      let classification: PersistSourceGateClassification = "row_level";
      for (const group of groups) {
         const shape = await resolveGateShape(
            group,
            modelDef,
            graftScope,
            deps,
         );
         if (shape.shape === "rejected") {
            classification = "rejected";
            break;
         }
      }
      return {
         classification,
         attributed: isAuthorizeAttributedToEntryPoint(persistSource),
      };
   } catch (err) {
      logger.warn("Failed to classify persist source gate; refusing", {
         sourceID: persistSource.sourceID,
         error: errMessage(err),
      });
      return { classification: "rejected", attributed: false };
   }
}

/**
 * The per-model pieces of a package build plan, accumulated over its models
 * in model order. {@link compilePackageBuildPlan} fills one on the main
 * thread; the package-load worker fills one from the models it already
 * compiled, so a load does not compile the package a second time.
 */
export interface BuildPlanParts {
   builds: SourceBuild[];
   sources: Record<string, PersistSource>;
   /** See {@link CompiledBuildPlan.targets}. */
   targets: Record<string, BuildTarget>;
   sourceModelPaths: Record<string, string>;
   droppedPersistSources: { name: string; modelPath: string }[];
   preaggregatePlans: Record<string, RollupPlan>;
   materializePlans: Record<string, MaterializePlan>;
   sourceGateOutcomes: Record<string, PersistSourceGateOutcome>;
   /** Findings on the `#@` annotations of this model's sources (see annotation_reader). */
   annotationFindings: PackageAnnotationFinding[];
   /** See {@link CompiledBuildPlan.omittedSources}. */
   omittedSources: Omission[];
}

export function emptyBuildPlanParts(): BuildPlanParts {
   return {
      builds: [],
      sources: {},
      targets: {},
      sourceModelPaths: {},
      droppedPersistSources: [],
      preaggregatePlans: {},
      materializePlans: {},
      sourceGateOutcomes: {},
      annotationFindings: [],
      omittedSources: [],
   };
}

/**
 * Append `from` to `into`, as if `from`'s model had been collected into `into`
 * directly. Merging per-model parts in model order gives the same parts as
 * collecting the models into one, in that order.
 */
export function mergeBuildPlanParts(
   into: BuildPlanParts,
   from: BuildPlanParts,
): void {
   into.builds.push(...from.builds);
   Object.assign(into.sources, from.sources);
   Object.assign(into.targets, from.targets);
   Object.assign(into.sourceModelPaths, from.sourceModelPaths);
   into.droppedPersistSources.push(...from.droppedPersistSources);
   Object.assign(into.preaggregatePlans, from.preaggregatePlans);
   Object.assign(into.materializePlans, from.materializePlans);
   Object.assign(into.sourceGateOutcomes, from.sourceGateOutcomes);
   // `?? []`: parts a worker built before this field existed.
   into.omittedSources.push(...(from.omittedSources ?? []));
   for (const finding of from.annotationFindings) {
      addAnnotationFinding(into, finding);
   }
}

/**
 * Add a finding once. A model that imports another walks the imported persist
 * sources again, and each walk would otherwise report the same finding, so a
 * finding is keyed by what it says and where (findings are attributed to the
 * DECLARING model, so both walks produce the same key).
 */
function addAnnotationFinding(
   parts: Pick<BuildPlanParts, "annotationFindings">,
   finding: PackageAnnotationFinding,
): void {
   const key = (f: PackageAnnotationFinding) =>
      [f.code, f.model ?? "", f.subject ?? "", f.message].join("\u0000");
   const k = key(finding);
   if (!parts.annotationFindings.some((f) => key(f) === k)) {
      parts.annotationFindings.push(finding);
   }
}

/** Whether a parse error at `at` falls on `note`, a `#@` line of a source. */
function noteCovers(
   note: {
      text: string;
      at: { url: string; range: { start: { line: number } } };
   },
   at: { url: string; range: { start: { line: number } } },
): boolean {
   if (note.at.url !== at.url) return false;
   const first = note.at.range.start.line;
   const last = first + note.text.split("\n").length - 1;
   return at.range.start.line >= first && at.range.start.line <= last;
}

/** Whether a persist source declares an incremental refresh, read like the planner does. */
function isIncrementalSource(source: PersistSource): boolean {
   try {
      return deriveAnnotationFields(source).refresh === "incremental";
   } catch {
      return false;
   }
}

/**
 * The package-relative path of the file `url` names, for attributing a
 * finding to the model that declares the annotation rather than one that
 * imports it; `fallback` when the URL is not a file under the package.
 */
function declaringModelPath(
   url: string | undefined,
   packagePath: string,
   fallback: string,
): string {
   if (!url?.startsWith("file:")) return fallback;
   try {
      const relative = path.relative(packagePath, fileURLToPath(url));
      return relative &&
         !relative.startsWith("..") &&
         !path.isAbsolute(relative)
         ? relative.split(path.sep).join("/")
         : fallback;
   } catch {
      return fallback;
   }
}

/** The declaring file's URL in a Malloy `sourceID` (`<name>@<url>`). */
function sourceIDFileURL(sourceID: string): string | undefined {
   const at = sourceID.indexOf("@file:");
   return at >= 0 ? sourceID.slice(at + 1) : undefined;
}

/**
 * Add the generated `#(materialize)` build sources of one author model to
 * `parts`, as the rollup companion's are added: whole builds (so a generated
 * source orders after a persisted source it reads), and only the generated
 * sources as planned sources, under the AUTHOR's model path, where the
 * annotation is changed. Each is judged by the same gates as any persist source.
 */
async function collectGeneratedMaterialize(
   parts: BuildPlanParts,
   args: {
      modelPath: string;
      packagePath: string;
      getRuntime: SynthesizedRuntimeFactory;
      /** The author's compiled model, and the materializer that compiled it. */
      malloyModel: MalloyModel;
      materializer: ModelMaterializer;
   },
): Promise<void> {
   const authorDef: ModelDef = args.malloyModel._modelDef;
   const { generated, refused, omitted } = await tryCompileMaterializeCompanion(
      {
         ...args,
         contents: authorDef.contents ?? {},
         givens: authorDef.givens ?? {},
         // P9's probes compile against the author's model, never run.
         probe: args.materializer,
      },
   );
   for (const refusal of refused) {
      addAnnotationFinding(
         parts,
         materializeRefusalFinding(refusal, args.modelPath),
      );
   }
   parts.omittedSources.push(...(omitted ?? []));
   if (!generated) return;
   for (const plan of generated.plans) {
      if (plan.pairFullRefresh) {
         addAnnotationFinding(
            parts,
            pairFullRefreshFinding(plan, plan.pairFullRefresh, args.modelPath),
         );
      }
   }
   const targets = await generated.runtime.getBuildTargets(generated.model);
   const planByName = new Map(
      generated.plans.map((p) => [p.buildSourceName, p]),
   );
   // Each generated source is presented as the author's source it builds: its
   // name and sourceID are the author's everywhere past this point, and
   // `<src>__mat` stays inside the companion file. The author's sourceID is
   // free: a source that is both `#@ persist` and `#(materialize)` is refused.
   const generatedPlans = new Map<PersistSource, MaterializePlan>();
   const rename = new Map<string, string>();
   for (const source of plannedSources(targets)) {
      const plan = planByName.get(source.name);
      if (!plan) continue;
      generatedPlans.set(source, plan);
      rename.set(source.sourceID, plan.sourceID);
   }
   // A source declared in a generated file that no plan names (a parent an
   // extension's build copy re-roots on its build source, and the extension
   // over it, P10) is on that build source's table: not a build of its own.
   const unplannedGenerated = (id: string) =>
      !rename.has(id) &&
      generated.generatedURLs.has(id.slice(id.indexOf("@") + 1));
   parts.builds.push(
      ...sourceBuildsOf(targets).map((b) =>
         renameBuildSourceIDs(withoutSourceIDs(b, unplannedGenerated), rename),
      ),
   );
   // The gate is the author's, on the author's source, reading the author's
   // givens: classified there. The companion imports the source but declares
   // none of its givens, so a gate reading one never lifts in it.
   const givens = Array.from(
      args.malloyModel.givens.values(),
   ) as unknown as MalloyGiven[];
   const deps = createGateClassificationDeps(
      givens.map(malloyGivenToApi),
      args.modelPath,
   );
   for (const [generatedSource, plan] of generatedPlans) {
      const source = presentAs(generatedSource, {
         name: plan.sourceName,
         sourceID: plan.sourceID,
      });
      const sourceID = plan.sourceID;
      parts.sources[sourceID] = source;
      recordTarget(parts, sourceID, source);
      parts.sourceModelPaths[sourceID] = args.modelPath;
      parts.materializePlans[sourceID] = plan;
      // By sourceID (the keying rule), never by name.
      const authorSource = Object.values(authorDef.contents ?? {}).find(
         (object): object is SourceDef =>
            isSourceDef(object) && object.sourceID === sourceID,
      );
      parts.sourceGateOutcomes[sourceID] = authorSource
         ? await classifyPersistSourceGate(
              { _sourceDef: authorSource, sourceID },
              authorDef,
              args.materializer,
              deps,
              args.modelPath,
           )
         : { classification: "rejected", attributed: false };
   }
}

/**
 * The `#(preaggregate)` rollups (materialization v2, phase 4) a model declares:
 * one build source per rollup, planned only in the model that declares its
 * base, so a rollup is listed once however many models import the base. Each
 * keeps its compiled `sourceID` (`<rollup>@<declaring model>.preaggregate.malloy`)
 * and is reported under the declaring model's path.
 *
 * A rollup whose build reads a given is left out with a warning: its table
 * would hold one given value's rows, served to every caller.
 */
async function collectGeneratedRollups(
   parts: BuildPlanParts,
   args: {
      modelPath: string;
      packagePath: string;
      getRuntime: SynthesizedRuntimeFactory;
      modelDef: unknown;
   },
): Promise<void> {
   const { generated, refused, omitted } =
      await tryCompileRollupCompanion(args);
   for (const refusal of refused) {
      addAnnotationFinding(
         parts,
         rollupRefusalFinding(refusal, args.modelPath),
      );
   }
   parts.omittedSources.push(...(omitted ?? []));
   if (!generated) return;
   const all = await generated.runtime.getBuildTargets(generated.model);
   const planByName = new Map(
      generated.plans.map((p) => [p.rollupSourceName, p]),
   );
   for (const target of all.connections.flatMap((c) => c.targets)) {
      if (!target.readsGivens) continue;
      for (const source of target.sources) {
         const plan = planByName.get(source.name);
         if (!plan) continue;
         addAnnotationFinding(
            parts,
            rollupRefusalFinding(
               {
                  tier: "unservable",
                  code: "preaggregate-reads-given",
                  message:
                     `#(preaggregate) rollup of source "${plan.baseSourceName}" at ` +
                     `(${plan.grainDimensions.join(", ")}) is not built, so its measures ` +
                     "are served live: its rows read a given, and a table holds one " +
                     "value's rows.",
                  sourceName: plan.baseSourceName,
               },
               args.modelPath,
            ),
         );
      }
   }
   const targets = {
      ...all,
      connections: all.connections.map((c) => ({
         ...c,
         targets: c.targets.filter((t) => !t.readsGivens),
      })),
   };
   const kept = new Set<string>();
   for (const source of plannedSources(targets)) {
      const plan = planByName.get(source.name);
      if (!plan) continue;
      kept.add(source.sourceID);
      parts.sources[source.sourceID] = source;
      recordTarget(parts, source.sourceID, source);
      parts.sourceModelPaths[source.sourceID] = args.modelPath;
      parts.preaggregatePlans[source.sourceID] = plan;
   }
   parts.builds.push(...sourceBuildsOf(targets));
   if (kept.size === 0) return;
   const givens = Array.from(
      generated.model.givens.values(),
   ) as unknown as MalloyGiven[];
   const deps = createGateClassificationDeps(
      givens.map(malloyGivenToApi),
      `${args.modelPath}#preaggregate`,
   );
   for (const source of plannedSources(targets)) {
      if (!kept.has(source.sourceID)) continue;
      parts.sourceGateOutcomes[source.sourceID] =
         await classifyPersistSourceGate(
            source,
            generated.model._modelDef,
            generated.materializer,
            deps,
            `${args.modelPath}#preaggregate`,
         );
   }
}

/**
 * The `#@ persist` views (materialization v2, phase 4, P14) a model declares:
 * one generated build source per view, planned only in the model that
 * declares the view's source, so a view is listed once however many models
 * import it. Each keeps its compiled `sourceID`
 * (`<source>__<view>__view@<declaring model>.persist-view.malloy`) and is
 * reported under the declaring model's path. Its gate is its source's,
 * classified on the author's model, where the source's givens are declared.
 *
 * A view whose build reads a given is left out with a warning: its table
 * would hold one caller's result, served to every caller.
 */
async function collectGeneratedPersistedViews(
   parts: BuildPlanParts,
   args: {
      modelPath: string;
      packagePath: string;
      getRuntime: SynthesizedRuntimeFactory;
      malloyModel: MalloyModel;
      materializer: ModelMaterializer;
   },
): Promise<void> {
   const authorDef: ModelDef = args.malloyModel._modelDef;
   const { generated, refused, omitted } =
      await tryCompilePersistedViewCompanion({
         packagePath: args.packagePath,
         modelPath: args.modelPath,
         modelDef: authorDef,
         materializer: args.materializer,
         getRuntime: args.getRuntime,
      });
   for (const refusal of refused) {
      addAnnotationFinding(
         parts,
         persistedViewRefusalFinding(refusal, args.modelPath),
      );
   }
   parts.omittedSources.push(...(omitted ?? []));
   if (!generated) return;
   const all = await generated.runtime.getBuildTargets(generated.model);
   const planByName = new Map(
      generated.plans.map((p) => [p.buildSourceName, p]),
   );
   for (const target of all.connections.flatMap((c) => c.targets)) {
      if (!target.readsGivens) continue;
      for (const source of target.sources) {
         const plan = planByName.get(source.name);
         if (!plan) continue;
         addAnnotationFinding(
            parts,
            persistedViewRefusalFinding(
               {
                  ...plan,
                  reason:
                     "its result reads a given, so it differs per caller and a table holds one caller's result",
               },
               args.modelPath,
            ),
         );
      }
   }
   const targets = {
      ...all,
      connections: all.connections.map((c) => ({
         ...c,
         targets: c.targets.filter((t) => !t.readsGivens),
      })),
   };
   const givens = Array.from(
      args.malloyModel.givens.values(),
   ) as unknown as MalloyGiven[];
   const deps = createGateClassificationDeps(
      givens.map(malloyGivenToApi),
      args.modelPath,
   );
   let kept = 0;
   for (const source of plannedSources(targets)) {
      const plan = planByName.get(source.name);
      if (!plan) continue;
      kept++;
      parts.sources[source.sourceID] = source;
      recordTarget(parts, source.sourceID, source);
      parts.sourceModelPaths[source.sourceID] = args.modelPath;
      // The view's rows are its source's: its gate is the source's, read on
      // the author's model, by sourceID (the keying rule).
      const authorSource = Object.values(authorDef.contents ?? {}).find(
         (object): object is SourceDef =>
            isSourceDef(object) && object.sourceID === plan.sourceID,
      );
      parts.sourceGateOutcomes[source.sourceID] = authorSource
         ? await classifyPersistSourceGate(
              { _sourceDef: authorSource, sourceID: plan.sourceID },
              authorDef,
              args.materializer,
              deps,
              args.modelPath,
           )
         : { classification: "rejected", attributed: false };
   }
   if (kept > 0) parts.builds.push(...sourceBuildsOf(targets));
}

/**
 * Add one compiled `.malloy` model's builds, persist sources, dropped
 * `#@ persist` sources, rollups and gate outcomes to `parts`. `materializer`
 * must be the one that compiled `malloyModel` (gate classification grafts
 * probes onto it), and `runtime` the one it compiled in, which plans the
 * build. `getRuntime` builds the runtime a synthesized pre-aggregation model
 * compiles in.
 *
 * Imports nothing from `./model`: the package-load worker runs this, and the
 * renderer `./model` loads must not enter that isolate.
 */
export async function collectModelBuildPlan(
   parts: BuildPlanParts,
   args: {
      modelPath: string;
      packagePath: string;
      materializer: ModelMaterializer;
      malloyModel: MalloyModel;
      runtime: Runtime;
      getRuntime: SynthesizedRuntimeFactory;
   },
): Promise<void> {
   const {
      modelPath,
      packagePath,
      materializer,
      malloyModel,
      runtime,
      getRuntime,
   } = args;
   // `#(materialize)` (materialization v2, phase 3), behind
   // PUBLISHER_MATERIALIZE: off, nothing here runs and the plan is exactly what
   // it was. Like pre-aggregation, BEFORE the early `return`s below: the
   // author's model needs no persistence flag, since the generated companion
   // carries it.
   if (getMaterializeEnabled()) {
      await collectGeneratedMaterialize(parts, {
         modelPath,
         packagePath,
         getRuntime,
         malloyModel,
         materializer,
      });
      // `#@ persist` on a view (phase 4, P14), under the same switch.
      await collectGeneratedPersistedViews(parts, {
         modelPath,
         packagePath,
         getRuntime,
         malloyModel,
         materializer,
      });
   }
   // Rollups (phase 4): v1 `#@ preaggregate` lines are read as their
   // declarations (the v1 cutover's G4), and `#(preaggregate)` with
   // PUBLISHER_MATERIALIZE. Before the early `return`s below, since a model
   // declaring only rollups needs no persistence flag.
   {
      await collectGeneratedRollups(parts, {
         modelPath,
         packagePath,
         getRuntime,
         modelDef: malloyModel._modelDef,
      });
   }

   // getBuildTargets() THROWS on a model without `##! experimental.persistence`,
   // which would abort the whole package's plan. Before the v1 cutover such a
   // model plans no persist source, mirroring Malloy's own guard (guard 1).
   // Under it, its build copy carries the flag instead (D3), so the author's
   // file need not. Presence only, decided as Malloy decides it.
   let planModel = malloyModel;
   let planRuntime = runtime;
   if (
      !routeHasKey(
         malloyModel.modelAnnotations,
         "!",
         "experimental",
         "persistence",
      )
   ) {
      const companion = await compileV1BuildCompanion({
         packagePath,
         modelPath,
         getRuntime,
         onCompileFailed: (error) => {
            logger.error(
               "The v1 build copy of a model could not be read or did not compile; its #@ persist sources are left out of the build plan",
               { modelPath, error: errMessage(error) },
            );
            parts.omittedSources.push(
               ...omittedByID(
                  declaredPersistSourceIDs(malloyModel),
                  "v1_build_companion_failed",
               ),
            );
         },
      });
      if (!companion) return;
      planModel = companion.model;
      planRuntime = companion.runtime;
   }

   const targets = await planRuntime.getBuildTargets(planModel);
   const parseErrors = targets.tagParseLog.filter((msg) => {
      logger.warn("Persist annotation issue", {
         modelPath,
         message: msg.message,
         severity: msg.severity,
      });
      return msg.severity === "error";
   });

   // BEFORE the empty-plan `return`, so a model whose only persist source is
   // a dropped shape is still caught.
   const planned = plannedSources(targets);
   // A `#@` line that does not parse is reported by what it cost. Malloy drops
   // a source only when none of its `#@` lines parse; when another does, the
   // source is still planned, built without the broken line's directives.
   // A location in the build copy (D3) sits one line below the author's,
   // since the copy opens with the flag: the finding names the author's line.
   // The copy's own location still matches the planned sources' notes.
   const companionURL =
      planModel !== malloyModel
         ? pathToFileURL(path.resolve(packagePath, modelPath)).href
         : undefined;
   const authorAt = (at: (typeof parseErrors)[number]["at"]) =>
      at && companionURL && at.url === companionURL
         ? {
              ...at,
              range: {
                 start: { ...at.range.start, line: at.range.start.line - 1 },
                 end: { ...at.range.end, line: at.range.end.line - 1 },
              },
           }
         : at;
   for (const msg of parseErrors) {
      const at = msg.at;
      const onPlanned = at
         ? planned
              .map((source) => ({
                 source,
                 note: source.annotations
                    .forRoute("@")
                    .find((note) => noteCovers(note, at)),
              }))
              .find((hit) => hit.note !== undefined)
         : undefined;
      // A source none of whose `#@` lines parse is not planned at all, and
      // the finding names a line: report the source by its sourceID, so its
      // absence is not read as its leaving the plan.
      if (at && !onPlanned?.note) {
         parts.omittedSources.push(
            ...omittedByID(
               sourceIDsWithNoteAt(planModel, at),
               "persist_annotation_unparseable",
            ),
         );
      }
      addAnnotationFinding(
         parts,
         persistUnparseableFinding(
            { ...msg, at: authorAt(msg.at) },
            declaringModelPath(at?.url, packagePath, modelPath),
            onPlanned?.note
               ? { subject: onPlanned.source.name, line: onPlanned.note.text }
               : undefined,
         ),
      );
   }
   const recognizedNames = new Set(planned.map((s) => s.name));
   for (const name of detectDroppedPersistSources(
      malloyModel,
      recognizedNames,
   )) {
      parts.droppedPersistSources.push({ name, modelPath });
   }

   if (planned.length === 0) return;

   parts.builds.push(...sourceBuildsOf(targets));
   // Lazily built on the first persist source.
   let classificationCtx:
      | { modelDef: ModelDef; deps: GateClassificationDeps }
      | undefined;
   for (const source of planned) {
      const sourceID = source.sourceID;
      parts.sources[sourceID] = source;
      recordTarget(parts, sourceID, source);
      parts.sourceModelPaths[sourceID] = modelPath;
      // Unknown and retired keys, on the lines that parsed (a line that did
      // not is reported above). Attributed to the declaring model, so a model
      // importing this source repeats nothing. Never lets the reader break the
      // plan (mirrors deriveAnnotationFields).
      try {
         const declaredIn = declaringModelPath(
            sourceIDFileURL(sourceID),
            packagePath,
            modelPath,
         );
         for (const finding of persistAnnotationFindings(
            source.annotations,
            source.name,
            declaredIn,
            { incremental: isIncrementalSource(source) },
         )) {
            addAnnotationFinding(parts, finding);
         }
      } catch {
         // No findings for a source whose annotations cannot be read.
      }
      if (!classificationCtx) {
         const givens = Array.from(
            malloyModel.givens.values(),
         ) as unknown as MalloyGiven[];
         classificationCtx = {
            modelDef: malloyModel._modelDef,
            deps: createGateClassificationDeps(
               givens.map(malloyGivenToApi),
               modelPath,
            ),
         };
      }
      parts.sourceGateOutcomes[sourceID] = await classifyPersistSourceGate(
         source,
         classificationCtx.modelDef,
         materializer,
         classificationCtx.deps,
         modelPath,
      );
   }
}

/** Project the compiled plan into the trimmed wire BuildPlan. */
export function deriveBuildPlan(
   builds: SourceBuild[],
   sources: Record<string, PersistSource>,
   connectionDigests: Record<string, string>,
   sourceNames?: string[],
   sourceModelPaths?: Record<string, string>,
   packageMaterialization?: WirePackageMaterialization | null,
   options?: {
      preaggregatePlans?: Record<string, RollupPlan>;
      materializePlans?: Record<string, MaterializePlan>;
      sourceGateOutcomes?: Record<string, PersistSourceGateOutcome>;
      /** See {@link CompiledBuildPlan.omittedSources}. */
      omittedSources?: Omission[];
   },
): BuildPlan {
   const include = sourceNames ? new Set(sourceNames) : null;

   // One level holding every source with every table it reads. A host walks
   // these edges for a rerun's upstream closure, freshness inheritance and
   // lineage, so none may be left out.
   // A rollup over a stored base is left out of the plan (storedBaseOfRollup),
   // so of its graph too: every node is in `sources` or `refusedSources`.
   const leftOut = new Set(
      Object.keys(sources).filter((sourceID) =>
         storedBaseOfRollup(
            { sources, preaggregatePlans: options?.preaggregatePlans },
            sourceID,
         ),
      ),
   );
   const wireGraphs: WireBuildGraph[] = builds.map((build) => ({
      connectionName: build.connectionName,
      nodes: [
         build.order
            .filter((sourceID) => !leftOut.has(sourceID))
            .map((sourceID) => ({
               sourceID,
               dependsOn: (build.dependsOn[sourceID] ?? []).filter(
                  (upstream) => !leftOut.has(upstream),
               ),
            })),
      ],
   }));

   const wireSources: Record<string, WirePersistSourcePlan> = {};
   const refusedSources: Record<string, WireRefusedSource> = {};
   const omitted: Omission[] = [...(options?.omittedSources ?? [])];
   for (const [sourceID, source] of Object.entries(sources)) {
      if (include && !include.has(source.name)) continue;
      // What the catch below reports, if this source fails to project: a
      // gate failing with an error (not a refusal) is told apart from the
      // rest of the projection.
      let failedAt: OmittedSourceReason = "projection_failed";
      // One unreadable source must not cost the package its plan: addressing
      // throws for a colocated source referencing a given with no default, and
      // an escaping throw would drop EVERY colocated binding for the package.
      try {
         const annotationFields = deriveAnnotationFields(source);

         // Tier-appropriate eligibility BEFORE computeSourceEntityId(),
         // which fail for a refused source. Mirrors the build path: `storage=`
         // gets the storage-destination gate; plain `#@ persist` gets the
         // colocated gate, which admits a resolved, fully-attributed gate. The
         // storage gate for every source would misreport buildable colocated
         // sources as refused.
         const declaresStorage = !!annotationFields.storage;
         const rollup = options?.preaggregatePlans?.[sourceID];
         // A rollup over a gated base is refused UNCONDITIONALLY, but that
         // governs only whether it MATERIALIZES (enforced at build time); the plan
         // still reports the synthesized rollup. See preaggregation_seams.spec.ts
         // ("gate stops materialization, not synthesis") and
         // docs/materialization.md.
         if (!rollup) {
            try {
               assertTierEligible(
                  source,
                  declaresStorage,
                  options,
                  annotationFields,
               );
            } catch (err) {
               // Only an eligibility refusal is reportable; anything else must not
               // masquerade as a security refusal.
               if (!(err instanceof MaterializationEligibilityError)) {
                  failedAt = "eligibility_check_failed";
                  throw err;
               }
               refusedSources[sourceID] = {
                  name: source.name,
                  sourceID: source.sourceID,
                  modelPath: sourceModelPaths?.[sourceID],
                  tier: declaresStorage ? "storage" : "colocated",
                  reason: err.reason || "authorize",
                  message: errMessage(err),
               };
               continue;
            }
         } else {
            // A storage rollup over a storage= base can neither build nor
            // serve (see storedBaseOfRollup): out of the plan, warned on.
            if (leftOut.has(sourceID)) continue;
            // Before the other gates: a rollup sharing its table with another
            // model's must neither build nor serve, so it leaves `sources`.
            try {
               assertRollupNameUnshared(source, {
                  collidingRollups: collidingRollups({
                     sources,
                     connectionDigests,
                     preaggregatePlans: options?.preaggregatePlans,
                     sourceModelPaths,
                  }),
               });
            } catch (err) {
               if (!(err instanceof MaterializationEligibilityError)) {
                  failedAt = "eligibility_check_failed";
                  throw err;
               }
               refusedSources[sourceID] = {
                  name: source.name,
                  sourceID: source.sourceID,
                  modelPath: sourceModelPaths?.[sourceID],
                  tier: "preaggregate",
                  reason: err.reason || "rollup_name_collision",
                  message: errMessage(err),
               };
               continue;
            }
            // A storage rollup runs BOTH gates; neither subsumes the other. The
            // authorize refusal is unconditional for rollups (they group ACROSS
            // the gated column, so cannot be row-filtered afterwards).
            //
            // An AUTHORIZE refusal leaves the rollup in `sources` too (Malloy
            // synthesized it) — the coexistence the spec permits for this tier
            // alone. A STORAGE refusal removes it: the rollup can never be
            // written, and `Package.storageWarnings` and the auto-run would
            // otherwise treat it as materialized/buildable.
            let storageRefused = false;
            try {
               // A `#(preaggregate)` rollup over a gated base whose gates
               // the variant grafts at read (rows 1 and 21) is not refused
               // for them; a v1 rollup always is.
               const keepsGate = rollupGateEnforcedAtRead(rollup);
               assertColocatedPersistNotAuthorizeGated(
                  source,
                  source.name,
                  "preaggregate",
                  options?.sourceGateOutcomes?.[sourceID],
                  annotationFields,
                  { gateEnforcedAtRead: keepsGate },
               );
               if (declaresStorage) {
                  storageRefused = true;
                  assertMaterializationEligible(
                     source,
                     deriveAnnotationFields(source),
                     { gateEnforcedAtRead: keepsGate },
                  );
                  storageRefused = false;
               }
            } catch (err) {
               if (!(err instanceof MaterializationEligibilityError)) {
                  failedAt = "eligibility_check_failed";
                  throw err;
               }
               refusedSources[sourceID] = {
                  name: source.name,
                  sourceID: source.sourceID,
                  modelPath: sourceModelPaths?.[sourceID],
                  // Always `preaggregate`, even for a storage refusal: only this
                  // tier may coexist with a `sources` entry, and it is the only
                  // provenance marking the entry as a rollup. Which gate refused
                  // is in `reason`.
                  tier: "preaggregate",
                  reason: err.reason || "authorize",
                  message: errMessage(err),
               };
               if (storageRefused) continue;
            }
         }
         // `origin`/`preaggregate`: a synthesized rollup is mechanically an
         // ordinary persist source, so the plan is the only place its provenance,
         // grain and measures are visible.
         wireSources[sourceID] = {
            name: source.name,
            sourceID: source.sourceID,
            connectionName: source.connectionName,
            dialect: source.dialectName,
            origin: rollup ? "preaggregate" : "persist",
            preaggregate: rollup
               ? {
                    baseSourceName: rollup.baseSourceName,
                    grainDimensions: rollup.grainDimensions,
                    measures: rollup.measures.map((m) => m.name),
                 }
               : null,
            sourceEntityId: computeSourceEntityId(source, connectionDigests),
            sql: addressSQL(source),
            // Verbatim; the resolved declaration is deliberately not a wire field
            // (see resolveIncrementalDeclaration).
            refresh: annotationFields.refresh ?? null,
            freshness: resolveFreshness(source, packageMaterialization),
            queryMetadata: resolveQueryMetadata(source, packageMaterialization),
            columns: deriveColumns(source),
            // Resolved, not passed through; a source failing either is in
            // `refusedSources` instead.
            ...planDynamicFields(source, annotationFields),
            annotationFields,
            modelPath: sourceModelPaths?.[sourceID],
         };
      } catch (err) {
         // Reported, so a host does not read the source's absence as its
         // leaving the plan; by a code, never the message, which may quote SQL.
         const reason = isUnkeyable(source) ? "unkeyable" : failedAt;
         logger.warn("Failed to project a persist source into the build plan", {
            sourceID,
            sourceName: source.name,
            reason,
            error: errMessage(err),
         });
         omitted.push({ sourceID, reason });
      }
   }

   return {
      graphs: wireGraphs,
      sources: wireSources,
      refusedSources,
      ...finalizeOmissions(omitted, new Set(Object.keys(wireSources))),
   };
}

/** What a package load keeps from its build plan. */
export interface BuildPlanOutcome {
   plan: BuildPlan | null;
   droppedPersistSources: { name: string; modelPath: string }[];
   /**
    * Storage rollups left out because their base is itself `storage=`
    * persisted (see storedBaseOfRollup). Optional: a worker that predates it
    * sends none.
    */
   rollupsOverStoredBase?: { baseName: string; modelPath?: string }[];
   annotationFindings: PackageAnnotationFinding[];
   sourceEligibility: SourceEligibility;
   colocatedSourceEligibility: ColocatedSourceEligibility;
   incrementalDeclarations: Record<string, IncrementalDeclaration>;
   /**
    * Every connection a persist source builds on, including sources the plan
    * skipped (a connection that did not resolve) or refused, so a connection
    * edit that creates or fixes one re-derives the plan. Optional: a worker
    * that predates it sends none.
    */
   buildConnectionNames?: string[];
   /**
    * The generated `#(materialize)` build sources, by their sourceID (see
    * {@link CompiledBuildPlan.materializePlans}), for the served copy to
    * re-root each author source on its build source's table. Optional: a
    * worker that predates it sends none.
    */
   materializePlans?: Record<string, MaterializePlan>;
   /**
    * The `#(preaggregate)` rollups over a gated base whose gates are grafted
    * on their variant at read ({@link rollupGateEnforcedAtRead}), by their
    * sourceID, so the serve verdict does not call them gated out. Optional:
    * a worker that predates it sends none.
    */
   rollupsKeepingGates?: string[];
}

/**
 * Project a compiled build plan into what a package load keeps. Reads the
 * connection digests, never the live connections, so it runs the same in the
 * package-load worker as on the main thread.
 */
export function deriveBuildPlanOutcome(
   collected: Omit<CompiledBuildPlan, "connections">,
   materializationConfig: WirePackageMaterialization | null,
): BuildPlanOutcome {
   // Before anything reads the plan: a refused source is neither built nor
   // served, and its warning replaces it (#6644).
   const compiled = refuseCollidingMaterialize(collected);
   const droppedPersistSources = compiled.droppedPersistSources ?? [];
   const incrementalDeclarations = collectIncrementalDeclarations(
      compiled.sources,
      compiled.materializePlans,
   );
   // A plan that lost sources is not "no plan": it is kept, so a host reads
   // `omittedSources` rather than an empty plan.
   const plan =
      compiled.builds.length === 0 && !compiled.omittedSources?.length
         ? null
         : deriveBuildPlan(
              compiled.builds,
              compiled.sources,
              compiled.connectionDigests,
              undefined,
              compiled.sourceModelPaths,
              materializationConfig,
              {
                 preaggregatePlans: compiled.preaggregatePlans,
                 materializePlans: compiled.materializePlans,
                 sourceGateOutcomes: compiled.sourceGateOutcomes,
                 omittedSources: compiled.omittedSources,
              },
           );
   // One entry per base: its grains share the reason.
   const overStoredBase = new Map<
      string,
      { baseName: string; modelPath?: string }
   >();
   for (const sourceID of Object.keys(compiled.sources)) {
      const base = storedBaseOfRollup(compiled, sourceID);
      if (!base || overStoredBase.has(base.sourceID)) continue;
      const modelPath = compiled.sourceModelPaths?.[base.sourceID];
      overStoredBase.set(base.sourceID, {
         baseName: base.name,
         ...(modelPath ? { modelPath } : {}),
      });
   }
   return {
      plan,
      droppedPersistSources,
      rollupsOverStoredBase: [...overStoredBase.values()],
      annotationFindings: compiled.annotationFindings ?? [],
      sourceEligibility: collectSourceEligibility(compiled),
      colocatedSourceEligibility: collectColocatedSourceEligibility(compiled),
      incrementalDeclarations,
      buildConnectionNames: [
         ...new Set(compiled.builds.map((b) => b.connectionName)),
      ].sort(),
      materializePlans: compiled.materializePlans ?? {},
      rollupsKeepingGates: Object.entries(compiled.preaggregatePlans ?? {})
         .filter(([, rollup]) => rollupGateEnforcedAtRead(rollup))
         .map(([sourceID]) => sourceID)
         .sort(),
   };
}

/**
 * What an incremental pair table's plan adds to its declaration: the parent
 * key each refresh replaces rows by, and its two generated windows taken off
 * `calculateFields`. Both are partitioned by that key, and the watermark is
 * one value per parent, so a delta holds whole parents and the windows over
 * it compute what the full build's do; the window gate exists for frames that
 * reach outside a delta, which these cannot. Nothing for any other source.
 */
function pairDeclaration(
   declaration: IncrementalDeclaration,
   plan: MaterializePlan | undefined,
): Partial<IncrementalDeclaration> {
   const pair = plan?.pair;
   const blocks = plan?.pairBlocks;
   const watermark = blocks?.watermark ?? pair?.watermark;
   if (!pair || !watermark) return {};
   const delta = pairDelta(pair, blocks);
   const generated = new Set([
      pair.parentRowColumn,
      watermark.column,
      ...(delta.checks ?? []).map((c) => c.flag),
   ]);
   return {
      pair: delta,
      calculateFields: declaration.calculateFields.filter(
         (name) => !generated.has(name),
      ),
   };
}

/**
 * The flags a pair table's delta checks, as the build writes them
 * (`pairFlags`): one table of one hop checks its parent-row flag alone (the
 * default); a path checks each level's flag over the keys down to it; a table
 * of several blocks checks every flag within each block, over that block's
 * own keys (a flag past a block's own leaf partitions on every key it has).
 */
export function pairDelta(
   pair: NonNullable<MaterializePlan["pair"]>,
   blocks: MaterializePlan["pairBlocks"],
): PairDelta {
   const base = { parentKey: pair.parentKey, parentRow: pair.parentRowColumn };
   const flagsOf = (p: typeof pair, levels: number, lane?: number) => {
      const keys = pairKeyColumns(p);
      return Array.from({ length: levels }, (_, level) => ({
         flag: level === 0 ? p.parentRowColumn : pairLevelRowColumn(level),
         keys: keys.slice(0, Math.min(level, keys.length - 1) + 1),
         ...(lane !== undefined ? { lane } : {}),
      }));
   };
   if (blocks) {
      return {
         ...base,
         checks: blocks.blocks.flatMap((p, lane) =>
            flagsOf(p, blocks.levels, lane),
         ),
         laneColumn: blocks.laneColumn,
         lanes: blocks.blocks.length,
      };
   }
   if (!pair.hops || pair.hops.length === 0) return base;
   return { ...base, checks: flagsOf(pair, 1 + pair.hops.length) };
}

/**
 * Each source's resolved incremental declaration, keyed by sourceID (two models
 * may declare same-named sources). Computed here because the gates need the
 * compiled output schema and query definition, which the wire plan does not
 * carry. Also used by the build path (MaterializationService.incrementalRunContext).
 */
export function collectIncrementalDeclarations(
   sources: Record<string, PersistSource>,
   /**
    * The generated `#(materialize)` sources' plans by sourceID: a plan's
    * rolling window rides on its declaration, for the refresh's delete.
    */
   materializePlans: Readonly<Record<string, MaterializePlan>> = {},
): Record<string, IncrementalDeclaration> {
   const declarations: Record<string, IncrementalDeclaration> = {};
   for (const [sourceID, source] of Object.entries(sources)) {
      try {
         const declaration = resolveIncrementalDeclaration(
            source,
            deriveAnnotationFields(source),
         );
         const plan = materializePlans[sourceID];
         // Only a lower bound deletes; an upper bound is left out of the
         // build, so every refresh appends and keeps every row.
         const window = deletingWindowsOf(plan?.readTimeTerms);
         declarations[sourceID] = {
            ...declaration,
            ...(window.length ? { rollingWindow: window } : {}),
            ...pairDeclaration(declaration, plan),
         };
      } catch (err) {
         // One unreadable source must not cost the package its plan. "No
         // declaration" is safe: the build path dispatches a delta only for a
         // declaration it can read.
         logger.warn("Failed to resolve a source's incremental declaration", {
            sourceID,
            error: errMessage(err),
         });
      }
   }
   return declarations;
}
