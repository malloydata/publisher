// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's build plan: one `TablePlan` per planned table, collected model
 * by model, with each source's gate classification and eligibility, and a
 * refusal or omission for each declaration that is not planned.
 *
 * Surface: `collectModelBuildPlan` adds one compiled model's tables to a
 * `BuildPlanParts` (the package-load worker over the models it compiled, and
 * `build_plan_compile.ts` on the main thread), and `mergeBuildPlanParts`
 * joins per-model parts in model order. `deriveBuildPlan` projects the plan
 * into the wire `BuildPlan` (the facade, for REST and the control plane);
 * `derivePackagePlan` projects it into the `PackagePlan`, the plain data that
 * crosses the load worker and the facade holds. `collectIncrementalDeclarations`
 * gives the incremental gate and the run's incremental build each source's
 * resolved declaration. It also re-exports the addressing, plan-layer and
 * eligibility helpers its importers read.
 *
 * Compiles, never runs: on the load worker its connection refuses SQL, and
 * `derivePackagePlan` reads connection digests, never live connections.
 * It imports nothing from `service/model.ts`, so the load worker can run it.
 */

import { refusalKey, tierOf } from "./refusal_key";
import type { RefusedReason } from "../domain/types";
import { errorCodeOf } from "../../../error_code";
import type { KeyCheckCompiler } from "../domain/key_check";
import {
   exploreOf,
   modelDefIfAny,
   modelDefOf,
   sourceDefOf,
} from "../../governance/ir/compiled_model";
import type {
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
import { incrementalUnbuildableReason } from "./incremental_policy";
import {
   nearMissGateLines,
   PREAGG_WARN_ONLY,
   preaggregateGateLines,
   relaxedCollision,
} from "./load_gate_text";
import {
   validateModelPreaggregation,
   type PreaggregateViolation,
} from "./preaggregation_validation";
import {
   annotationNearMisses,
   authoredNotes,
} from "../../annotation_near_miss";
import {
   MATERIALIZE_NOT_BUILT_CODE,
   PERSIST_INVALID_CODE,
   PERSIST_NOT_BUILT_CODE,
   ROLLUP_NOT_BUILT_CODE,
   persistAnnotationFindings,
   persistUnparseableFinding,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import { declares, destinationOf, readNotesIfAny } from "./declaration";
import {
   persistDeclarationProblems,
   persistNameProblems,
} from "./persist_annotation_validation";
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
import { resolveIncrementalDeclaration } from "./incremental_declaration";
import type { IncrementalDeclaration, PairDelta } from "../domain/incremental";
import { isAuthorizeAttributedToEntryPoint } from "./materialization_eligibility";

import {
   companionFailure,
   companionOmissions,
   GeneratedFiles,
   type CompanionFailure,
   type SynthesizedRuntimeFactory,
} from "../text/generated_files";
import type { RollupPlan } from "../domain/rollup_measures";
import {
   isPersistedResultSourceID,
   materializeKindOf,
   isRollupOrigin,
   originOfTable,
   splitSourceID,
   type TableOrigin,
   type MaterializePlan,
   type OverStoredBase,
   type PersistedQueryPlan,
   type PersistSourceGateClassification,
   type PersistSourceGateOutcome,
   type TablePlan,
} from "../domain/table_plans";
import {
   materializeRefusalFinding,
   pairFullRefreshFinding,
} from "./materialize_plan";
import { pairKeyColumns, pairLevelRowColumn } from "./build_file_generator";
import {
   logCompanionFailure,
   tryCompileMaterializeCompanion,
   type CrossFileCompanions,
} from "./materialize_compile";
export {
   newCrossFileCompanions,
   type CrossFileCompanions,
} from "./materialize_compile";
import {
   PERSIST_VIEW_NOT_BUILT,
   persistedQueryReadsGivenRefusal,
   persistedQueryRefusalFinding,
   persistedQueryRefusalKey,
   persistedViewReadsGivenRefusal,
   persistedViewRefusalFinding,
   planModelPersistedViews,
} from "./persisted_view";
import {
   PERSIST_QUERY_NOT_BUILT,
   declaredPersistQueries,
   persistQueryInvalidFindings,
   persistQueryKeyFindings,
   persistQueryNotBuiltFinding,
   type DeclaredPersistQuery,
} from "./persisted_query";
import { tryCompilePersistedViewCompanion } from "./persisted_view_compile";
import { overStoredProven } from "./over_stored_proof";
import { UPSTREAM_TABLE_UNUSABLE_REASON } from "./downstream_upstreams";
import { deletingWindowsOf } from "./rolling_window";
import {
   tryCompileRollupCompanion,
   type MaterializedBases,
} from "./rollup_compile";
import { compileV1BuildCompanion } from "./v1_build_companion";
import {
   measureRefusalKey,
   rollupGateEnforcedAtRead,
   rollupReadsGivenRefusal,
   rollupRefusalFinding,
} from "./rollup_plan";
import {
   getMaterializeEnabled,
   getPersistCollisionEnforce,
} from "../../../config";
import {
   resolveConnectionTypes,
   resolveManagedConnections,
} from "../../connection_type";
import { sourceReads } from "../../governance/ir/source_reads";
import { collectAccessReads } from "./access_closure";
import {
   admitGivenPositions,
   currentTimeReadsOf,
   storedAsOfBuild,
   type DynamicTerm,
} from "./persist_dynamic_terms";
import {
   declaredStorage,
   deriveAnnotationFields,
   deriveColumns,
} from "./persist_source_fields";
import { quoteIdentifier } from "../../quoting";
import {
   addressSQL,
   compilerBuildId,
   foldDistinctTableNames,
   isUnkeyable,
   planAddresses,
   planAddressOf,
   plannedSources,
   plannedTargetOf,
   presentAs,
   renameBuildSourceIDs,
   resolveConnectionDigests,
   withoutSourceIDs,
   sourceBuildsOf,
   withDownstreamEdges,
   upstreamSchemasOfSource,
   upstreamTablesOf,
   type PlanAddresses,
} from "./addressing";
import {
   type UpstreamSchemaReadings,
   type UpstreamTable,
   upstreamTableKey,
} from "../domain/upstream_schemas";
import {
   materializePlansOf,
   type CompiledBuildPlan as PlannedBuild,
   type SourceBuild,
} from "../domain/compiled_build_plan";
import {
   databaseRefusalsByAddress,
   collectDatabaseSourceEligibility,
   collectSourceEligibility,
   EligibilityCheckFailed,
   planVerdicts,
   refuseCollidingMaterialize,
   refuseRewrittenNames,
   sharedTables,
   withoutSources,
   type TableVerdict,
} from "./eligibility_collect";
import {
   countRefusal,
   noModelGateLines,
   type AsOfBuildSource,
   type EligibilityRefusals,
   type ModelGateLines,
   type PackagePlan,
   type PersistedViewName,
   type RefusalNotices,
   type RefusedRollup,
   type TablePlanData,
} from "../domain/package_plan";
import { resolveFreshness, resolveQueryMetadata } from "./plan_layers";
import {
   finalizeOmissions,
   omittedByID,
   type Omission,
   type OmittedSourceReason,
} from "../domain/omitted_sources";

// Addressing, plan layers and eligibility collection live in their own
// modules; re-exported here for build_plan's importers.
export {
   addressSQL,
   addressText,
   buildSQLOf,
   buildTargetOf,
   compiledSourceIDOf,
   compilerBuildId,
   computeSourceEntityId,
   foldDistinctTableNames,
   planAddresses,
   plannedSources,
   setTestAddressForTarget,
   sourceBuildsOf,
} from "./addressing";
export {
   materializePlansOf,
   sourcesInBuildOrder,
   type SourceBuild,
} from "../domain/compiled_build_plan";
export type { TableFolds } from "./addressing";
export { sharedTables, tierGateInputs } from "./eligibility_collect";
export {
   composeDeclaredQueryMetadata,
   resolveFreshness,
   resolveQueryMetadata,
   type ReadableTag,
} from "./plan_layers";

export { deriveAnnotationFields, deriveColumns } from "./persist_source_fields";
import type { WirePackageMaterialization } from "./plan_layers";
import type { BuildPlanPackage } from "../domain/plan_input";

type WireBuildGraph = components["schemas"]["BuildGraph"];
type WirePersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type WireTableRead = components["schemas"]["TableRead"];
type WireRefusedSource = components["schemas"]["RefusedSource"];
type WireStrippedTerm = components["schemas"]["StrippedTerm"];
type WireJoinedTerms = components["schemas"]["JoinedTerms"];
type BuildPlan = components["schemas"]["BuildPlan"];
export type { WirePackageMaterialization };

export type { BuildPlanPackage };

/**
 * A package's compiled build plan (`domain/compiled_build_plan.ts`), its
 * annotation findings typed as the planner reports them.
 */
export type CompiledBuildPlan = PlannedBuild<PackageAnnotationFinding>;

/**
 * A compiled build plan {@link finishBuildPlan} finished: its folds and
 * addresses decided, its sources refused alone, and every part a plan's
 * outcome reads present.
 */
export type FinishedBuildPlan = CompiledBuildPlan &
   Required<
      Pick<
         CompiledBuildPlan,
         | "tableFolds"
         | "addresses"
         | "droppedPersistSources"
         | "annotationFindings"
         | "modelGateLines"
         | "omittedSources"
      >
   >;

function wireTerm(term: DynamicTerm): WireStrippedTerm {
   return { code: term.code, givens: term.givens };
}

/**
 * The plan's `strippedTerms` and `joinedTerms` for one source. Omits a field
 * rather than reporting a wrong one (the gate normally refuses such a source
 * first). Informational only: the build re-resolves these.
 */
function planDynamicFields(source: PersistSource): {
   strippedTerms?: WireStrippedTerm[];
   joinedTerms?: WireJoinedTerms[];
} {
   const out: {
      strippedTerms?: WireStrippedTerm[];
      joinedTerms?: WireJoinedTerms[];
   } = {};
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
 * The stored column a source's field is read from, by the field's name: the
 * column's own name, which a renamed field's definition keeps (`name`) beside
 * the one the source gives it (`as`), and which the build SQL emits. A field
 * the definition does not name maps to itself.
 */
export function storedColumnOf(
   persistSource: PersistSource,
): (field: string) => string {
   const columnOf = new Map<string, string>();
   const def = sourceDefOf(persistSource) as { fields?: unknown[] } | null;
   for (const f of def?.fields ?? []) {
      const { name, as } = f as { name?: unknown; as?: unknown };
      if (typeof name !== "string") continue;
      columnOf.set(typeof as === "string" ? as : name, name);
   }
   return (field) => columnOf.get(field) ?? field;
}

/**
 * The columns a source's stored table holds, by the names its build SQL emits
 * them under, or a throw: every column the source declares, public or hidden
 * (`private:`, `internal:`), so a hidden measure or dimension over one is
 * served from the table. A renamed column is stored under the name its query
 * gives it, which the served copy renames again. A column the source drops
 * with `except:` is not one of its fields, so it is not stored. The strict
 * counterpart to {@link deriveColumns}: this list decides what gets WRITTEN,
 * so "unknown" must not read as "nothing to narrow". Empty throws too.
 *
 * An array column (a BigQuery `ARRAY`, a DuckDB or Postgres list, a
 * persisted query's `nest:`) is stored too, as the destination's list of
 * the element type, and so is a record column (a BigQuery `STRUCT`, a
 * DuckDB `STRUCT`), as the destination's struct of the mapped field types:
 * Malloy types either as a source of its own (an `ExploreField`), not an
 * atomic field, and leaving it out of the table sent every query on a
 * `#(materialize)` source over it live.
 */
export function storedColumnNames(persistSource: PersistSource): string[] {
   let names: string[];
   try {
      const columnOf = storedColumnOf(persistSource);
      names = exploreOf(persistSource)
         .intrinsicFields.filter((f) => f.isAtomicField() || isNestedColumn(f))
         .map((f) => columnOf(f.name))
         .filter((n): n is string => typeof n === "string" && n.length > 0);
   } catch (err) {
      throw new Error(
         `the compiled source's field list could not be read (${errMessage(err)})`,
      );
   }
   if (names.length === 0) {
      throw new Error("the compiled source declares no atomic columns");
   }
   return names;
}

/**
 * Whether an intrinsic field is an array or record column the table reads
 * (a list of scalars or of records, or a struct): intrinsic excludes joins
 * and computed fields, so an intrinsic source-typed field is a column.
 */
function isNestedColumn(field: unknown): boolean {
   const f = field as { isExploreField?: () => boolean };
   return f.isExploreField?.() === true;
}

/**
 * Wrap a source's build SQL to project the columns its stored table holds
 * ({@link storedColumnNames}). The build SQL emits every underlying column,
 * one the source drops with `except:` included, which is left out.
 *
 * A hidden column's values are AT REST in the destination, readable by anyone
 * with direct access to it; reads THROUGH Publisher go through the served
 * copy, which hides the column again exactly as the source does. Applies to
 * both storage build paths.
 *
 * Fails CLOSED: an undeterminable column surface refuses the build before any
 * SQL runs, leaving the previous generation serving.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source.
 */
export function projectToStoredColumns(
   persistSource: PersistSource,
   buildSQL: string,
): string {
   let cols: string[];
   try {
      cols = storedColumnNames(persistSource);
   } catch (err) {
      recordEligibilityRefused("public_surface_unknown");
      throw new MaterializationEligibilityError({
         reason: "public_surface_unknown",
         message:
            `Source '${persistSource.name}' cannot be materialized into a ` +
            `storage destination: its column surface could not be ` +
            `determined — ${errMessage(err)}. The publisher writes a stored ` +
            `table's columns as the source declares them, so it refuses the ` +
            `build rather than materialize columns the source drops. This ` +
            `usually means the compiled-source shape changed; drop 'storage=' ` +
            `to serve this source live in the meantime.`,
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
            code: errorCodeOf(err),
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
      const contents = modelDefIfAny(malloyModel)?.contents as
         | Record<string, { type?: string; annotations?: unknown }>
         | undefined;
      if (!contents) return dropped;
      for (const [name, def] of Object.entries(contents)) {
         if (recognizedNames.has(name)) continue;
         if (!def || !def.annotations) continue;
         // A named query is no source: `#@ persist` on one has its own
         // warning (persisted_query.ts), and never refuses a build.
         if (def.type === "query") continue;
         let isPersist = false;
         try {
            // Presence only, read over every line as Malloy reads it, so a
            // `#@ persist` line carrying an `@env.` reference is still caught.
            isPersist = declares(
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
      for (const def of Object.values(
         modelDefOf(malloyModel)?.contents ?? {},
      )) {
         if (!isSourceDef(def) || !def.annotations) continue;
         try {
            if (declares(new Annotations(def.annotations), "@", "persist")) {
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
      for (const def of Object.values(
         modelDefOf(malloyModel)?.contents ?? {},
      )) {
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
      const entryStruct = sourceDefOf(persistSource) as SourceDef;
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
      // A row filter read off another level (a derivation's base, or an
      // ancestor the entry point extends), not the entry point's own note.
      const inheritedRowFilter = groups.some(
         (g) => g.route === "access_filter" && g.selfContained,
      );
      return {
         classification,
         attributed: isAuthorizeAttributedToEntryPoint(persistSource),
         ...(inheritedRowFilter ? { inheritedRowFilter } : {}),
      };
   } catch (err) {
      logger.warn("Failed to classify persist source gate; refusing", {
         sourceID: persistSource.sourceID,
         code: errorCodeOf(err),
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
   /** See {@link CompiledBuildPlan.tables}. */
   tables: Record<string, TablePlan>;
   droppedPersistSources: { name: string; modelPath: string }[];
   /** Findings on the `#@` annotations of this model's sources (see annotation_reader). */
   annotationFindings: PackageAnnotationFinding[];
   /**
    * Each model's own load-failure lines (plan/load_gate_text.ts), so a run
    * over the files reads them as the load does.
    */
   modelGateLines: ModelGateLines;
   /** See {@link CompiledBuildPlan.persistedViews}. */
   persistedViews: Record<string, PersistedViewName>;
   /** See {@link CompiledBuildPlan.omittedSources}. */
   omittedSources: Omission[];
   /** See {@link CompiledBuildPlan.accessReads}: every model's, merged. */
   accessReads: string[];
}

export function emptyBuildPlanParts(): BuildPlanParts {
   return {
      builds: [],
      tables: {},
      droppedPersistSources: [],
      annotationFindings: [],
      modelGateLines: noModelGateLines(),
      persistedViews: {},
      omittedSources: [],
      accessReads: [],
   };
}

/**
 * Append `from` to `into`, as if `from`'s model had been collected into `into`
 * directly. Merging per-model parts in model order gives the same parts as
 * collecting the models into one, in that order: a table a later model plans
 * again (an imported `#@ persist` source, walked by each model that imports
 * it) is the later model's.
 */
export function mergeBuildPlanParts(
   into: BuildPlanParts,
   from: BuildPlanParts,
): void {
   into.builds.push(...from.builds);
   Object.assign(into.tables, from.tables);
   into.droppedPersistSources.push(...from.droppedPersistSources);
   into.omittedSources.push(...from.omittedSources);
   for (const finding of from.annotationFindings) {
      addAnnotationFinding(into, finding);
   }
   into.modelGateLines.preaggregate.push(...from.modelGateLines.preaggregate);
   into.modelGateLines.nearMiss.push(...from.modelGateLines.nearMiss);
   into.modelGateLines.nameProblems.push(...from.modelGateLines.nameProblems);
   Object.assign(into.persistedViews, from.persistedViews);
   into.accessReads = [
      ...new Set([...into.accessReads, ...from.accessReads]),
   ].sort();
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

/** What every planned table records of its source: the source, its target, its model. */
function plannedFacts(
   source: PersistSource,
   modelPath: string,
): Pick<TablePlan, "source" | "target" | "modelPath"> {
   const target = plannedTargetOf(source);
   return { source, ...(target ? { target } : {}), modelPath };
}

/** The declaring file's URL in a Malloy `sourceID` (`<name>@<url>`). */
function sourceIDFileURL(sourceID: string): string | undefined {
   const url = splitSourceID(sourceID)?.url;
   return url?.startsWith("file:") ? url : undefined;
}

/**
 * A companion compile's refusals, as findings, and the sources it left out,
 * added to `parts`, and its failure logged once: the start every generated
 * companion shares.
 */
function addCompanionOutcome(
   parts: BuildPlanParts,
   findings: readonly PackageAnnotationFinding[],
   omitted: readonly Omission[] | undefined,
   failed?: { failure: CompanionFailure | undefined; modelPath: string },
): void {
   for (const finding of findings) addAnnotationFinding(parts, finding);
   parts.omittedSources.push(...(omitted ?? []));
   if (failed?.failure) logCompanionFailure(failed.failure, failed.modelPath);
}

/**
 * `all` less every target whose build reads a given, each of whose sources
 * is handed to `refuse`: a table would hold one given value's rows, served
 * to every caller.
 */
function withoutGivenReadingTargets<
   T extends {
      connections: {
         targets: { readsGivens?: boolean; sources: PersistSource[] }[];
      }[];
   },
>(all: T, refuse: (source: PersistSource) => void): T {
   for (const target of all.connections.flatMap((c) => c.targets)) {
      if (!target.readsGivens) continue;
      for (const source of target.sources) refuse(source);
   }
   return {
      ...all,
      connections: all.connections.map((c) => ({
         ...c,
         targets: c.targets.filter((t) => !t.readsGivens),
      })),
   };
}

/** The gate classification's deps over `model`'s givens, for `modelPath`. */
function gateDepsFor(
   model: MalloyModel,
   modelPath: string,
): GateClassificationDeps {
   const givens = Array.from(model.givens.values()) as unknown as MalloyGiven[];
   return createGateClassificationDeps(givens.map(malloyGivenToApi), modelPath);
}

/**
 * Add the generated `#(materialize)` build sources of one author model to
 * `parts`, as the rollup companion's are added: whole builds (so a generated
 * source orders after a persisted source it reads), and only the generated
 * sources as planned sources, under the AUTHOR's model path, where the
 * annotation is changed. Each is judged by the same gates as any persist source.
 * Returns the companion's plans and files, which the model's rollup companion
 * reads (a rollup over a stored `#(materialize)` base), or undefined.
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
      keyCheckCompilers?: Map<string, KeyCheckCompiler>;
      crossFileCompanions?: CrossFileCompanions;
   },
): Promise<MaterializedBases | undefined> {
   const authorDef: ModelDef = modelDefOf(args.malloyModel);
   const { generated, refused, omitted, failure } =
      await tryCompileMaterializeCompanion({
         ...args,
         contents: authorDef.contents ?? {},
         givens: authorDef.givens ?? {},
         sourceRegistry: (authorDef as { sourceRegistry?: unknown })
            .sourceRegistry,
         // The caller-dependence probes compile against the author's model,
         // never run.
         probe: args.materializer,
      });
   addCompanionOutcome(
      parts,
      refused.map((r) => materializeRefusalFinding(r, args.modelPath)),
      omitted,
      { failure, modelPath: args.modelPath },
   );
   if (!generated) return undefined;
   for (const [key, compile] of generated.checkCompilers) {
      args.keyCheckCompilers?.set(key, compile);
   }
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
   // A build source of another file's companion, which a cross-file
   // extension's build reads: that file plans its table, and this
   // model's builds name it by the author's source, so the extension orders
   // after it.
   const ownIDs = new Set(generated.plans.map((p) => p.sourceID));
   for (const [from, to] of generated.renames) {
      if (!ownIDs.has(to)) rename.set(from, to);
   }
   for (const source of plannedSources(targets)) {
      if (rename.has(source.sourceID)) continue;
      const plan = planByName.get(source.name);
      if (!plan) continue;
      generatedPlans.set(source, plan);
      rename.set(source.sourceID, plan.sourceID);
   }
   // A source declared in a generated file that no plan names (a parent an
   // extension's build copy re-roots on its build source, and the extension
   // over it) is on that build source's table: not a build of its own.
   const unplannedGenerated = (id: string) =>
      !rename.has(id) &&
      generated.generatedURLs.has(splitSourceID(id)?.url ?? "");
   parts.builds.push(
      ...sourceBuildsOf(targets).map((b) =>
         renameBuildSourceIDs(withoutSourceIDs(b, unplannedGenerated), rename),
      ),
   );
   // The gate is the author's, on the author's source, reading the author's
   // givens: classified there. The companion imports the source but declares
   // none of its givens, so a gate reading one never lifts in it.
   const deps = gateDepsFor(args.malloyModel, args.modelPath);
   for (const [generatedSource, plan] of generatedPlans) {
      const source = presentAs(generatedSource, {
         name: plan.sourceName,
         sourceID: plan.sourceID,
      });
      const sourceID = plan.sourceID;
      parts.tables[sourceID] = {
         kind: materializeKindOf(plan),
         ...plannedFacts(source, args.modelPath),
         materialize: plan,
         gate: await authorSourceGate(sourceID, authorDef, {
            materializer: args.materializer,
            deps,
            modelPath: args.modelPath,
         }),
      };
   }
   return { plans: generated.plans, files: generated.files };
}

/**
 * The gate of the author's source `sourceID`, classified on the author's model
 * where its givens are declared, by sourceID (the keying rule), never by name.
 * A source the model does not hold is `rejected`.
 */
async function authorSourceGate(
   sourceID: string,
   authorDef: ModelDef,
   args: {
      materializer: ModelMaterializer;
      deps: GateClassificationDeps;
      modelPath: string;
   },
): Promise<PersistSourceGateOutcome> {
   const authorSource = Object.values(authorDef.contents ?? {}).find(
      (object): object is SourceDef =>
         isSourceDef(object) && object.sourceID === sourceID,
   );
   return authorSource
      ? await classifyPersistSourceGate(
           { _sourceDef: authorSource, sourceID },
           authorDef,
           args.materializer,
           args.deps,
           args.modelPath,
        )
      : { classification: "rejected", attributed: false };
}

/**
 * The `#(rollup)` rollups a model declares:
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
      materialized?: MaterializedBases;
   },
): Promise<void> {
   const { generated, refused, omitted, failure } =
      await tryCompileRollupCompanion(args);
   addCompanionOutcome(
      parts,
      refused.map((r) => rollupRefusalFinding(r, args.modelPath)),
      omitted,
      { failure, modelPath: args.modelPath },
   );
   if (!generated) return;
   const planByName = new Map(
      generated.plans.map((p) => [p.rollupSourceName, p]),
   );
   const targets = withoutGivenReadingTargets(
      await generated.runtime.getBuildTargets(generated.model),
      (source) => {
         const plan = planByName.get(source.name);
         if (!plan) return;
         addAnnotationFinding(
            parts,
            rollupRefusalFinding(
               rollupReadsGivenRefusal(plan, source),
               args.modelPath,
            ),
         );
      },
   );
   const kept: { source: PersistSource; plan: RollupPlan }[] = [];
   for (const source of plannedSources(targets)) {
      const plan = planByName.get(source.name);
      if (plan) kept.push({ source, plan });
   }
   parts.builds.push(...sourceBuildsOf(targets));
   if (kept.length === 0) return;
   const deps = gateDepsFor(generated.model, `${args.modelPath}#preaggregate`);
   for (const { source, plan } of kept) {
      parts.tables[source.sourceID] = {
         kind: "rollup",
         ...plannedFacts(source, args.modelPath),
         rollup: plan,
         // Proven at plan, and recorded: any other rollup builds from the
         // warehouse, and keeps its place in the order.
         ...provenRollupOverStored(
            source,
            plan,
            generated.overStoredBases.get(source.sourceID),
            parts.tables,
         ),
         ...(generated.storedBaseBuilds.has(source.sourceID)
            ? { storedBase: generated.storedBaseBuilds.get(source.sourceID) }
            : {}),
         gate: await classifyPersistSourceGate(
            source,
            modelDefOf(generated.model),
            generated.materializer,
            deps,
            `${args.modelPath}#preaggregate`,
         ),
      };
   }
}

/**
 * A rollup's build over its base's stored table, when its second compile
 * builds the planned table ({@link overStoredProven}): over a `#(materialize)`
 * base (its table planned before the rollups), the compile re-roots the base
 * on its build source; over a `#@ persist` one, it does not. The run still
 * checks the base is a database `#@ persist` or `#(materialize)` table.
 * Empty otherwise.
 */
function provenRollupOverStored(
   source: PersistSource,
   plan: RollupPlan,
   variant: OverStoredBase | undefined,
   tables: Readonly<Record<string, TablePlan>>,
): { overStoredBase?: OverStoredBase } {
   if (!variant) return {};
   // Over a `#@ persist` base, the address alone proves it: the base's own
   // table plan is collected after the companions'.
   if (!variant.baseBuildSource) {
      return overStoredProven(source, variant)
         ? { overStoredBase: variant }
         : {};
   }
   const baseID = variant.rerootedSourceID ?? plan.baseSourceID;
   const base = baseID === undefined ? undefined : tables[baseID];
   if (base?.kind !== "materialize") return {};
   return overStoredProven(source, variant, base.source)
      ? { overStoredBase: variant }
      : {};
}

/**
 * The `#@ persist` views a model declares:
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
      /** The model's `#(materialize)` companion, when it compiled one. */
      materialized?: MaterializedBases;
   },
): Promise<void> {
   const authorDef: ModelDef = modelDefOf(args.malloyModel);
   const { generated, refused, refusedQueries, omitted, failure } =
      await tryCompilePersistedViewCompanion({
         ...(args.materialized ? { materialized: args.materialized } : {}),
         packagePath: args.packagePath,
         modelPath: args.modelPath,
         modelDef: authorDef,
         materializer: args.materializer,
         getRuntime: args.getRuntime,
      });
   addCompanionOutcome(
      parts,
      [
         ...refused.map((r) => persistedViewRefusalFinding(r, args.modelPath)),
         ...refusedQueries.map((r) =>
            persistedQueryRefusalFinding(r, args.modelPath),
         ),
      ],
      omitted,
      { failure, modelPath: args.modelPath },
   );
   if (!generated) return;
   for (const plan of generated.plans) {
      parts.persistedViews[plan.buildSourceID] = {
         sourceName: plan.sourceName,
         viewName: plan.viewName,
      };
   }
   const planByName = new Map(
      generated.plans.map((p) => [p.buildSourceName, p]),
   );
   const queryByID = new Map(
      generated.queries.map((p) => [p.buildSourceID, p]),
   );
   const targets = withoutGivenReadingTargets(
      await generated.runtime.getBuildTargets(generated.model),
      (source) => {
         const plan = planByName.get(source.name);
         const query = queryByID.get(source.sourceID);
         if (plan) {
            addAnnotationFinding(
               parts,
               persistedViewRefusalFinding(
                  persistedViewReadsGivenRefusal(plan),
                  args.modelPath,
               ),
            );
         } else if (query) {
            addAnnotationFinding(
               parts,
               persistedQueryRefusalFinding(
                  persistedQueryReadsGivenRefusal(query),
                  args.modelPath,
               ),
            );
         }
      },
   );
   const ctx = {
      authorDef,
      materializer: args.materializer,
      deps: gateDepsFor(args.malloyModel, args.modelPath),
      modelPath: args.modelPath,
   };
   let kept = 0;
   for (const source of plannedSources(targets)) {
      const query = queryByID.get(source.sourceID);
      const plan = planByName.get(source.name);
      if (!query && !plan) continue;
      // Offered a build over its upstreams' tables: decided here, once,
      // and recorded. A compile that is not the planned table refuses it:
      // a downstream is never built by recomputing its upstreams from raw.
      // A candidate whose second compile failed builds from its definition.
      const overStoredBase = generated.overStoredSources.get(source.sourceID);
      if (overStoredBase) {
         const rerooted =
            overStoredBase.rerootedSourceID === undefined
               ? undefined
               : parts.tables[overStoredBase.rerootedSourceID];
         if (
            !overStoredProven(
               source,
               overStoredBase,
               rerooted?.kind === "materialize" ? rerooted.source : undefined,
            )
         ) {
            const reason =
               "its build over its upstreams' stored tables did not compile " +
               "to the planned table (a Publisher bug worth reporting), and " +
               "a view or query is never built by recomputing its upstreams " +
               "from raw";
            addAnnotationFinding(
               parts,
               query
                  ? persistedQueryRefusalFinding(
                       {
                          queryName: query.queryName,
                          reason,
                          refusal: "generated_build_failed",
                          declaringURL: query.declaringURL,
                          ...(query.storage !== undefined
                             ? { storage: query.storage }
                             : {}),
                       },
                       args.modelPath,
                    )
                  : persistedViewRefusalFinding(
                       {
                          ...plan!,
                          reason,
                          outcome: { refused: "generated_build_failed" },
                       },
                       args.modelPath,
                    ),
            );
            continue;
         }
      }
      kept++;
      // A persisted query's table is a view's, built from the same
      // companion and read by an exact call.
      parts.tables[source.sourceID] = {
         kind: "view",
         ...plannedFacts(source, args.modelPath),
         ...(overStoredBase ? { overStoredBase } : {}),
         gate: query
            ? await persistedQueryGate(source, query, ctx)
            : // The view's rows are its source's: its gate is the source's.
              await authorSourceGate(plan!.sourceID, authorDef, ctx),
      };
   }
   if (kept > 0) parts.builds.push(...sourceBuildsOf(targets));
}

/**
 * A persisted query's gate. Its rows are its run target's: the gate is the
 * run target's, read on the author's model (by sourceID), or an inline run
 * target's own, which the planner admits only when it has none.
 */
async function persistedQueryGate(
   source: PersistSource,
   query: PersistedQueryPlan,
   ctx: {
      authorDef: ModelDef;
      materializer: ModelMaterializer;
      deps: GateClassificationDeps;
      modelPath: string;
   },
): Promise<PersistSourceGateOutcome> {
   const runTarget =
      query.sourceID !== undefined
         ? Object.values(ctx.authorDef.contents ?? {}).find(
              (object): object is SourceDef =>
                 isSourceDef(object) && object.sourceID === query.sourceID,
           )
         : inlineRunTarget(ctx.authorDef, query.queryName);
   return runTarget
      ? await classifyPersistSourceGate(
           {
              _sourceDef: runTarget,
              sourceID: query.sourceID ?? source.sourceID,
           },
           ctx.authorDef,
           ctx.materializer,
           ctx.deps,
           ctx.modelPath,
        )
      : { classification: "rejected", attributed: false };
}

/**
 * The findings on the `#@ persist` named queries a model file DECLARES (an
 * importing model repeats nothing): a misspelled or retired key, and, with
 * `notBuilt`, the warning that the query is not built and is answered live.
 */
function collectPersistQueryFindings(
   parts: BuildPlanParts,
   args: {
      modelPath: string;
      packagePath: string;
      malloyModel: MalloyModel;
      notBuilt: boolean;
   },
): void {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   let queries: DeclaredPersistQuery[];
   try {
      queries = declaredPersistQueries(
         modelDefIfAny(args.malloyModel),
         (url) => {
            try {
               return path.resolve(fileURLToPath(url)) === modelFile;
            } catch {
               return false;
            }
         },
      );
   } catch {
      return;
   }
   for (const query of queries) {
      for (const finding of persistQueryKeyFindings(query, args.modelPath)) {
         addAnnotationFinding(parts, finding);
      }
      const invalid = persistQueryInvalidFindings(query, args.modelPath);
      for (const finding of invalid) addAnnotationFinding(parts, finding);
      if (args.notBuilt && invalid.length === 0) {
         addAnnotationFinding(
            parts,
            persistQueryNotBuiltFinding(
               query.name,
               args.modelPath,
               persistedQueryRefusalKey(
                  query.name,
                  query.declaringURL,
                  "materialization_off",
                  queryStorage(query),
               ),
            ),
         );
      }
   }
}

/**
 * With PUBLISHER_MATERIALIZE off, the findings on the `#@ persist` views a
 * model file DECLARES, read from the declarations alone (nothing is
 * compiled): an invalid line (one that does not parse, a misspelled or
 * retired key, an incremental key, `@env.`) fails the load, as with the
 * switch on, and every valid one warns `persist-view-not-built` with reason
 * `materialization_off`, as a persisted query's does.
 */
function collectPersistViewFindingsSwitchedOff(
   parts: BuildPlanParts,
   args: { modelPath: string; packagePath: string; malloyModel: MalloyModel },
): void {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   // Not caught: a declaration that cannot be read must not load as if it
   // were absent (load versus refused), so the load fails, as it does with
   // the switch on.
   const planned = planModelPersistedViews(
      modelDefIfAny(args.malloyModel),
      (url) => {
         try {
            return path.resolve(fileURLToPath(url)) === modelFile;
         } catch {
            return false;
         }
      },
   );
   const off = {
      reason: "persisted views are built only with PUBLISHER_MATERIALIZE on",
      outcome: { refused: "materialization_off" as const },
   };
   for (const refusal of planned.refused) {
      addAnnotationFinding(
         parts,
         persistedViewRefusalFinding(
            "malformed" in refusal.outcome ? refusal : { ...refusal, ...off },
            args.modelPath,
         ),
      );
   }
   for (const plan of planned.plans) {
      addAnnotationFinding(
         parts,
         persistedViewRefusalFinding(
            {
               sourceID: plan.sourceID,
               sourceName: plan.sourceName,
               viewName: plan.viewName,
               ...(plan.storage !== undefined ? { storage: plan.storage } : {}),
               ...off,
            },
            args.modelPath,
         ),
      );
   }
}

/** The destination a persisted query's `#@ persist storage=` names. */
function queryStorage(query: DeclaredPersistQuery): string | undefined {
   const tag = readNotesIfAny(query.notes, "@");
   return tag ? destinationOf(tag, "persist") : undefined;
}

/** The inline run target of the named query `name`, when it has one. */
function inlineRunTarget(
   modelDef: ModelDef,
   name: string,
): SourceDef | undefined {
   const query = (modelDef.contents ?? {})[name] as
      | { type?: unknown; structRef?: unknown }
      | undefined;
   const ref = query?.type === "query" ? query.structRef : undefined;
   return ref && typeof ref === "object" && isSourceDef(ref as never)
      ? (ref as SourceDef)
      : undefined;
}

/** Add the sources an access decision of `malloyModel` reads to `parts`. */
function addAccessReads(parts: BuildPlanParts, malloyModel: MalloyModel): void {
   const reads = new Set(parts.accessReads);
   collectAccessReads(modelDefOf(malloyModel)?.contents ?? {}, reads);
   parts.accessReads = [...reads].sort();
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
      /**
       * Where each `#(materialize)` uniqueness check's compiler over a run's
       * compiler manifest is put, on a thread that runs builds; absent on the load
       * worker, whose plan crosses to the main thread as data.
       */
      keyCheckCompilers?: Map<string, KeyCheckCompiler>;
      /**
       * The package plan's companions of cross-file parents' files
       * (materialize_compile.ts), shared by every model the plan collects;
       * absent, each model compiles its own.
       */
      crossFileCompanions?: CrossFileCompanions;
   },
): Promise<void> {
   addAccessReads(parts, args.malloyModel);
   await collectGeneratedTables(parts, args);
   const plannable = await v1PlanModel(parts, args);
   if (!plannable) return;
   const targets = await plannable.runtime.getBuildTargets(plannable.model);
   // BEFORE the empty-plan `return`, so a model whose only persist source is
   // a dropped shape is still caught.
   const planned = plannedSources(targets);
   reportPersistLineFindings(parts, targets, planned, {
      ...args,
      planModel: plannable.model,
   });
   if (planned.length === 0) return;
   parts.builds.push(...sourceBuildsOf(targets));
   await planV1PersistSources(parts, planned, args);
}

type CollectArgs = Parameters<typeof collectModelBuildPlan>[1];

/**
 * The tables a model's generated companions plan: `#(materialize)` sources
 * and `#@ persist` views behind PUBLISHER_MATERIALIZE (off, nothing here
 * runs and the plan is exactly what it was), and rollups. Before the v1
 * walk's early returns: the author's model needs no persistence flag, since
 * each generated companion carries it.
 */
async function collectGeneratedTables(
   parts: BuildPlanParts,
   args: CollectArgs,
): Promise<void> {
   const {
      modelPath,
      packagePath,
      materializer,
      malloyModel,
      getRuntime,
      keyCheckCompilers,
      crossFileCompanions,
   } = args;
   // The model's own load-failure lines, read off the author's model as the
   // load reads them (Model.preaggregateViolations, annotationNearMisses).
   {
      const authorDef = modelDefOf(malloyModel);
      const violations = validateModelPreaggregation(
         authorDef.contents as Record<string, unknown>,
         authorDef.modelID,
      );
      parts.modelGateLines.preaggregate.push(
         ...preaggregateGateLines(modelPath, violations),
      );
      for (const finding of v1RollupRefusals(violations, modelPath)) {
         addAnnotationFinding(parts, finding);
      }
      parts.modelGateLines.nearMiss.push(
         ...nearMissGateLines(
            modelPath,
            annotationNearMisses(authoredNotes(authorDef)),
         ),
      );
      // An unquoted or unsafe `#@ persist name=`, read from the raw text, the
      // ground truth for quoting, as the load reads it. A text that cannot be
      // read adds nothing: the load reads it itself, and the plan's own
      // finding still refuses an unsafe name (an unquoted one plans nothing).
      const text = await GeneratedFiles.over(getRuntime)
         .readIfAny(pathToFileURL(path.join(packagePath, modelPath)))
         .catch(() => undefined);
      if (text !== undefined) {
         parts.modelGateLines.nameProblems.push(
            ...persistNameProblems(text, modelPath),
         );
      }
   }
   // `#@ persist` on a named query: its key findings always, and, with
   // PUBLISHER_MATERIALIZE off, the warning that it is not built. A query
   // needs no persistence flag.
   collectPersistQueryFindings(parts, {
      modelPath,
      packagePath,
      malloyModel,
      notBuilt: !getMaterializeEnabled(),
   });
   // `#@ persist` on a view, read in both modes as a query's line is: with
   // the switch off nothing is compiled, but an invalid line still fails the
   // load and a valid one warns that it is not built.
   if (!getMaterializeEnabled()) {
      collectPersistViewFindingsSwitchedOff(parts, {
         modelPath,
         packagePath,
         malloyModel,
      });
   }
   let materialized: MaterializedBases | undefined;
   if (getMaterializeEnabled()) {
      materialized = await collectGeneratedMaterialize(parts, {
         modelPath,
         packagePath,
         getRuntime,
         malloyModel,
         materializer,
         keyCheckCompilers,
         crossFileCompanions,
      });
      // `#@ persist` on a view, under the same switch.
      await collectGeneratedPersistedViews(parts, {
         modelPath,
         packagePath,
         getRuntime,
         malloyModel,
         materializer,
         ...(materialized ? { materialized } : {}),
      });
   }
   // Rollups: v1 `#@ preaggregate` lines are read as their declarations, and
   // `#(rollup)` with PUBLISHER_MATERIALIZE. A model declaring only rollups needs no
   // persistence flag.
   await collectGeneratedRollups(parts, {
      modelPath,
      packagePath,
      getRuntime,
      modelDef: modelDefOf(malloyModel),
      materialized,
   });
}

/**
 * The valid `#@ preaggregate` declarations a model file declares that are not
 * built ({@link PREAGG_WARN_ONLY}), as refusals: one per measure, or one per
 * source for a refusal of every rollup on it. The violations are the file's
 * own (validateModelPreaggregation reads its own sources only).
 */
function v1RollupRefusals(
   violations: readonly Readonly<PreaggregateViolation>[],
   modelPath: string,
): PackageAnnotationFinding[] {
   return violations.flatMap((violation) => {
      const reason = PREAGG_WARN_ONLY.get(violation.code);
      if (reason === undefined) return [];
      if (violation.sourceID === undefined) {
         // Never silent: a refusal with no source to key it by is reported.
         logger.warn(
            "A #@ preaggregate refusal names no compiled source; it is not recorded",
            {
               modelPath,
               sourceName: violation.sourceName,
               code: violation.code,
            },
         );
         return [];
      }
      const sourceID = violation.sourceID;
      // One per table its declarations would have built, so each names its
      // own tier, as a `#(rollup)` refusal does.
      const tables = violation.tables?.length ? violation.tables : [undefined];
      return tables.map(
         (table): PackageAnnotationFinding => ({
            route: "@",
            tier: "unservable",
            code: ROLLUP_NOT_BUILT_CODE,
            message: violation.message,
            subject: violation.fieldName ?? violation.sourceName,
            model: modelPath,
            refused: measureRefusalKey(
               sourceID,
               violation.sourceName,
               violation.fieldName,
               reason,
               table,
            ),
         }),
      );
   });
}

/**
 * The model and runtime the v1 walk plans `#@ persist` sources in: the
 * author's own, or, for a model without `##! experimental.persistence`, its
 * v1 build copy; undefined when that copy cannot be compiled (its sources
 * are recorded as omitted).
 */
async function v1PlanModel(
   parts: BuildPlanParts,
   args: CollectArgs,
): Promise<{ model: MalloyModel; runtime: Runtime } | undefined> {
   const { modelPath, packagePath, malloyModel, runtime, getRuntime } = args;
   // getBuildTargets() THROWS on a model without `##! experimental.persistence`,
   // which would abort the whole package's plan. Such a model is planned from
   // a build copy that carries the flag (v1_build_companion.ts), so the
   // author's file need not. Presence only, decided as Malloy decides it.
   let planModel = malloyModel;
   let planRuntime = runtime;
   if (
      !declares(
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
         onCompileFailed: (error) =>
            addCompanionOutcome(
               parts,
               [],
               companionOmissions("v1_build", {
                  sourceIDs: declaredPersistSourceIDs(malloyModel),
               }),
               { failure: companionFailure("v1_build", error), modelPath },
            ),
      });
      if (!companion) return undefined;
      planModel = companion.model;
      planRuntime = companion.runtime;
   }
   return { model: planModel, runtime: planRuntime };
}

/**
 * Report what the model's `#@` lines cost: each line that does not parse, by
 * the source it is on (or as omitted, when none of that source's lines
 * parse), and each `#@ persist` source Malloy's walk dropped.
 */
function reportPersistLineFindings(
   parts: BuildPlanParts,
   targets: Awaited<ReturnType<Runtime["getBuildTargets"]>>,
   planned: PersistSource[],
   args: CollectArgs & { planModel: MalloyModel },
): void {
   const { modelPath, packagePath, malloyModel, planModel } = args;
   // Logged at debug, by Malloy's code: an error is also a finding below,
   // which is what an operator sees.
   for (const msg of targets.tagParseLog) {
      logger.debug("Persist annotation issue", {
         modelPath,
         code: msg.code,
         severity: msg.severity,
      });
   }
   const parseErrors = targets.tagParseLog.filter(
      (msg) => msg.severity === "error",
   );

   // A `#@` line that does not parse is reported by what it cost. Malloy drops
   // a source only when none of its `#@` lines parse; when another does, the
   // source is still planned, built without the broken line's directives.
   // A location in the build copy sits one line below the author's,
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
      // A `#@ persist` on a shape Malloy does not plan is a misplaced
      // annotation: it fails the load, as a misplaced `#@ preaggregate` does.
      addAnnotationFinding(parts, {
         route: "@",
         tier: "malformed",
         code: "persist-not-materializable",
         message:
            `#@ persist source "${name}" is not a source Malloy can ` +
            `materialize: only a query or aggregate source builds, and a ` +
            `filtered pass-through, a table with only added fields, or an ` +
            `unbound parameterized source does not. Persist a query source ` +
            `over it, invoke a parameterized source with a bound argument, or ` +
            `remove the annotation`,
         subject: name,
         model: modelPath,
      });
   }
}

/** Plan the v1 walk's `#@ persist` sources: their findings and gates. */
async function planV1PersistSources(
   parts: BuildPlanParts,
   planned: PersistSource[],
   args: CollectArgs,
): Promise<void> {
   const { modelPath, packagePath, materializer, malloyModel } = args;
   // Lazily built on the first persist source.
   let classificationCtx:
      | { modelDef: ModelDef; deps: GateClassificationDeps }
      | undefined;
   for (const source of planned) {
      const sourceID = source.sourceID;
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
         for (const problem of persistDeclarationProblems(source.annotations)) {
            addAnnotationFinding(parts, {
               route: "@",
               tier: "malformed",
               code: PERSIST_INVALID_CODE,
               message: `#@ persist source "${source.name}" is invalid: ${problem}`,
               subject: source.name,
               model: declaredIn,
            });
         }
      } catch (err) {
         // The source is still planned, so its gate is classified below;
         // only its annotation findings are lost, which this line reports.
         logger.warn(
            "Failed to read a persist source's annotations; planned without their findings",
            {
               modelPath,
               sourceID,
               sourceName: source.name,
               code: errorCodeOf(err),
            },
         );
      }
      classificationCtx ??= {
         modelDef: modelDefOf(malloyModel),
         deps: gateDepsFor(malloyModel, modelPath),
      };
      parts.tables[sourceID] = {
         kind: "persist",
         // The DECLARING model, never this one: a model that imports the
         // source walks it again, and the last walk would otherwise decide
         // its path.
         ...plannedFacts(
            source,
            declaringModelPath(
               sourceIDFileURL(sourceID),
               packagePath,
               modelPath,
            ),
         ),
         gate: await classifyPersistSourceGate(
            source,
            classificationCtx.modelDef,
            materializer,
            classificationCtx.deps,
            modelPath,
         ),
      };
   }
}

/**
 * Project the compiled plan into the trimmed wire BuildPlan; `sourceNames`,
 * when given, keeps only the sources of those names.
 */
export function deriveBuildPlan(
   compiled: Pick<
      CompiledBuildPlan,
      | "builds"
      | "tables"
      | "connectionDigests"
      | "connectionTypes"
      | "omittedSources"
      | "tableFolds"
      | "upstreamSchemas"
   > & { addresses: PlanAddresses },
   packageMaterialization?: WirePackageMaterialization | null,
   verdicts: ReadonlyMap<string, TableVerdict> = planVerdicts(compiled),
): BuildPlan {
   const { builds, tables } = compiled;

   // One level holding every source with every table it reads. A host walks
   // these edges for a rerun's upstream closure, freshness inheritance and
   // lineage, so none may be left out.
   const wireGraphs: WireBuildGraph[] = builds.map((build) => ({
      connectionName: build.connectionName,
      nodes: [
         build.order.map((sourceID) => ({
            sourceID,
            dependsOn: build.dependsOn[sourceID] ?? [],
         })),
      ],
   }));

   const wireSources: Record<string, WirePersistSourcePlan> = {};
   const refusedSources: Record<string, WireRefusedSource> = {};
   const omitted: Omission[] = [...(compiled.omittedSources ?? [])];
   for (const [sourceID, table] of Object.entries(tables)) {
      const { source } = table;
      // One unreadable source must not cost the package its plan: addressing
      // throws for a database source referencing a given with no default, and
      // an escaping throw would drop EVERY database binding for the package.
      try {
         const decided = verdicts.get(sourceID)!.wire;
         if ("failed" in decided) throw decided.failed;
         if (decided.refused) {
            // Its declared freshness, so the package's schedule rule reads a
            // refused source's window as it reads a planned one's.
            const freshness = resolveFreshness(
               source,
               packageMaterialization,
               table.rollup?.freshness,
            );
            refusedSources[sourceID] = {
               ...decided.refused,
               ...(freshness ? { freshness } : {}),
            };
         }
         if (!decided.inPlan) continue;
         wireSources[sourceID] = wireSourceOf(
            table,
            decided.annotationFields,
            planAddressOf(compiled, sourceID),
            packageMaterialization,
            compiled.upstreamSchemas,
         );
      } catch (err) {
         // Reported, so a host does not read the source's absence as its
         // leaving the plan; by a code, never the message, which may quote SQL.
         // A gate failing with an error (not a refusal) is told apart from the
         // rest of the projection.
         const reason: OmittedSourceReason = isUnkeyable(source)
            ? "unkeyable"
            : err instanceof EligibilityCheckFailed
              ? "eligibility_check_failed"
              : "projection_failed";
         logger.warn("Failed to project a persist source into the build plan", {
            sourceID,
            sourceName: source.name,
            reason,
            code: errorCodeOf(err),
         });
         omitted.push({ sourceID, reason });
      }
   }

   refuseDatabaseTablesSharedWithARefusal(
      compiled,
      verdicts,
      wireSources,
      refusedSources,
   );

   return {
      graphs: wireGraphs,
      sources: wireSources,
      refusedSources,
      ...finalizeOmissions(omitted, new Set(Object.keys(wireSources))),
   };
}

/**
 * One planned table's wire entry. `origin`/`preaggregate`: a synthesized
 * rollup is mechanically an ordinary persist source, so the plan is the only
 * place its provenance, grain and measures are visible.
 */
function wireSourceOf(
   table: TablePlan,
   annotationFields: Record<string, string>,
   sourceEntityId: string,
   packageMaterialization: WirePackageMaterialization | null | undefined,
   upstreamSchemaReadings: UpstreamSchemaReadings | undefined,
): WirePersistSourcePlan {
   const { source, rollup, modelPath } = table;
   return {
      name: source.name,
      sourceID: source.sourceID,
      connectionName: source.connectionName,
      dialect: source.dialectName,
      origin: originOfTable(table.kind, source.sourceID),
      preaggregate: rollup
         ? {
              baseSourceName: rollup.baseSourceName,
              grainDimensions: rollup.grainDimensions,
              measures: rollup.measures.map((m) => m.name),
           }
         : null,
      sourceEntityId,
      sql: addressSQL(source),
      // Verbatim; the resolved declaration is deliberately not a wire field
      // (see resolveIncrementalDeclaration).
      refresh: annotationFields.refresh ?? null,
      freshness: resolveFreshness(
         source,
         packageMaterialization,
         rollup?.freshness,
      ),
      queryMetadata: resolveQueryMetadata(source, packageMaterialization),
      columns: deriveColumns(source),
      // Omitted, never partial, when the tables cannot be known.
      ...readsOf(source),
      // The readings its address folds in; absent with `reads`.
      ...upstreamSchemasFieldOf(source, upstreamSchemaReadings),
      // Resolved, not passed through; a source failing either is in
      // `refusedSources` instead.
      ...planDynamicFields(source),
      annotationFields,
      modelPath,
   };
}

/** The plan's `upstreamSchemas` for one source; {} when it folds none. */
function upstreamSchemasFieldOf(
   source: PersistSource,
   readings: UpstreamSchemaReadings | undefined,
): Pick<WirePersistSourcePlan, "upstreamSchemas"> {
   const upstreamSchemas = upstreamSchemasOfSource(source, readings);
   return upstreamSchemas ? { upstreamSchemas } : {};
}

/** The plan's `reads` for one source; {} when they cannot be known. */
function readsOf(source: PersistSource): { reads?: WireTableRead[] } {
   const reads = sourceReads(source);
   return reads === undefined ? {} : { reads };
}

/**
 * Move to `refusedSources` each planned v1 database source whose table another
 * source the database gate refuses shares (one content address: identical
 * SQL, different gates). A database table is served by its address, and the
 * bind refuses an address any source on it is refused for
 * (collectDatabaseSourceEligibility), as the build does
 * (instructed_build.ts, databaseRefusalOn): such a table would be built for
 * nothing. The plan says so, with the sibling's reason, in sourceID order.
 */
function refuseDatabaseTablesSharedWithARefusal(
   compiled: Pick<
      CompiledBuildPlan,
      "tables" | "connectionDigests" | "tableFolds"
   > & { addresses: PlanAddresses },
   verdicts: ReadonlyMap<string, TableVerdict>,
   wireSources: Record<string, WirePersistSourcePlan>,
   refusedSources: Record<string, WireRefusedSource>,
): void {
   const { tables } = compiled;
   // v1 `#@ persist` sources only: a `#(materialize)` source's table is
   // addressed by its generated build (the plan's `targets`), which this
   // projection does not see, and the bind keys it the same way.
   const v1 = (id: string) => !tables[id]?.materialize;
   const database = Object.entries(wireSources).filter(
      ([id, s]) =>
         !isRollupOrigin(s.origin) && !s.annotationFields?.storage && v1(id),
   );
   if (database.length === 0) return;
   const refusals = databaseRefusalsByAddress(
      compiled,
      verdicts,
      (id) => !tables[id].rollup && v1(id),
   );
   for (const [sourceID, planned] of database) {
      const sibling = refusals
         .get(planned.sourceEntityId)
         ?.find((r) => r.sourceID !== sourceID);
      if (!sibling) continue;
      delete wireSources[sourceID];
      refusedSources[sourceID] = {
         name: planned.name,
         sourceID: planned.sourceID,
         modelPath: tables[sourceID]?.modelPath,
         tier: "database",
         reason: (sibling.reason ?? "authorize") as WireRefusedSource["reason"],
         ...(planned.freshness ? { freshness: planned.freshness } : {}),
         message:
            `'${planned.name}' is not built: it shares its table with ` +
            `'${sibling.name}', which the eligibility gate refuses for a ` +
            `database build, and a database table is served to every ` +
            `source on it. ${sibling.message}`,
      };
   }
}

/**
 * Move each valid declaration a finding refused (an `unservable` finding
 * with its refusal record) into the plan's `refusedSources`, counted once,
 * so every declaration that is not built is one record whatever its
 * mechanism; the findings keep the rest. A package whose every declaration
 * is refused still has a plan, so its refusals are read.
 */
export function projectRefusals(
   plan: BuildPlan | null,
   findings: readonly PackageAnnotationFinding[],
   counts: EligibilityRefusals = {},
): {
   plan: BuildPlan | null;
   annotationFindings: PackageAnnotationFinding[];
   refusalNotices: RefusalNotices;
} {
   const kept: PackageAnnotationFinding[] = [];
   const refusedSources: Record<string, WireRefusedSource> = {
      ...(plan?.refusedSources ?? {}),
   };
   const refusalNotices: RefusalNotices = {};
   let moved = false;
   for (const finding of findings) {
      const key = finding.refused;
      if (finding.tier !== "unservable" || key === undefined) {
         kept.push(finding);
         continue;
      }
      moved = true;
      if (refusedSources[key.sourceID]) continue;
      refusedSources[key.sourceID] = {
         name: key.name,
         sourceID: key.sourceID,
         ...(finding.model ? { modelPath: finding.model } : {}),
         tier: key.tier,
         reason: key.reason,
         message: finding.rewrite
            ? `${finding.message}; ${finding.rewrite}`
            : finding.message,
      };
      refusalNotices[key.sourceID] = {
         code: finding.code,
         ...(finding.subject !== undefined ? { subject: finding.subject } : {}),
      };
      countRefusal(counts, key.reason);
   }
   if (!moved) return { plan, annotationFindings: kept, refusalNotices };
   return {
      plan: { ...(plan ?? { graphs: [], sources: {} }), refusedSources },
      annotationFindings: kept,
      refusalNotices,
   };
}

/**
 * A package's collected parts as its compiled build plan, the one tail of
 * both plan paths (compilePackageBuildPlan on the main thread, and the load
 * worker): each author `name=` its own table, decided over every source
 * before any is refused, and every address once, then the sources refused
 * alone, in one order: a `name=` the readable name would rewrite
 * ({@link refuseRewrittenNames}, invalid), an author collision
 * ({@link refuseCollidingMaterialize}, invalid whatever else is wrong with
 * the source), then a valid incremental declaration this deployment cannot
 * honour.
 */
export function finishBuildPlan(
   parts: BuildPlanParts,
   ctx: {
      connectionDigests: Record<string, string>;
      /** See {@link CompiledBuildPlan.upstreamSchemas}; absent, none read. */
      upstreamSchemas?: UpstreamSchemaReadings;
      connections: Map<string, MalloyConnection>;
      /** The package names its tables per version (`scope: "version"`). */
      versionScoped: boolean;
      keyCheckCompilers?: ReadonlyMap<string, KeyCheckCompiler>;
      /**
       * Refuse author collisions only, keeping a source whose valid
       * incremental declaration this deployment cannot honour. For a spec
       * that plans incremental machinery on DuckDB, which no load or run does.
       */
      keepIncrementalUnbuildable?: boolean;
   },
): FinishedBuildPlan {
   const plan = {
      ...(ctx.versionScoped ? { versionScoped: true } : {}),
      ...parts,
      // A downstream's edges to the tables it is computed from, which its
      // companion's own targets do not name.
      builds: withDownstreamEdges(parts.builds, parts.tables),
      connectionDigests: ctx.connectionDigests,
      ...(ctx.upstreamSchemas ? { upstreamSchemas: ctx.upstreamSchemas } : {}),
      connectionTypes: resolveConnectionTypes(ctx.connections),
      managedConnections: resolveManagedConnections(ctx.connections),
      connections: ctx.connections,
      ...(ctx.keyCheckCompilers && ctx.keyCheckCompilers.size > 0
         ? { keyCheckCompilers: ctx.keyCheckCompilers }
         : {}),
   };
   // Each `storage=` source's stored columns read as its build writes them.
   const tableFolds = foldDistinctTableNames(plan, storedColumnNames);
   const refused = refuseCollidingMaterialize(
      refuseRewrittenNames({
         ...plan,
         tableFolds,
         addresses: planAddresses({ ...plan, tableFolds }),
      }),
   );
   const known = refuseUpstreamUnread(refused);
   return refuseOverMissingUpstreams(
      ctx.keepIncrementalUnbuildable
         ? known
         : refuseIncrementalUnbuildable(known),
   );
}

/**
 * Refuse, as `upstream_table_unusable`, each downstream (a persisted view or
 * query, or a rollup) planned to build over an upstream's table
 * (`overStoredBase`) whose upstream is not planned, or is refused: the
 * upstream's declaration is read from its notes, but it has no table to
 * read, and a downstream is never built by recomputing its upstream from
 * raw. Decided here, once every table and its eligibility is known, so a
 * run never finds out.
 */
function refuseOverMissingUpstreams<
   T extends Omit<FinishedBuildPlan, "connections">,
>(compiled: T): T {
   const verdicts = planVerdicts(compiled);
   const findings: PackageAnnotationFinding[] = [];
   const dropped = new Set<string>();
   for (const [sourceID, table] of Object.entries(compiled.tables)) {
      if (table.kind !== "view" && table.kind !== "rollup") continue;
      for (const upstreamID of table.overStoredBase?.upstreamSourceIDs ?? []) {
         const upstream = compiled.tables[upstreamID];
         const verdict = verdicts.get(upstreamID)?.wire;
         const refusedWhy =
            verdict === undefined || "failed" in verdict
               ? "it could not be planned"
               : verdict.refused
                 ? `it is refused (${verdict.refused.reason}: ${verdict.refused.message})`
                 : undefined;
         const why =
            upstream === undefined
               ? "it is not planned, so it has no table"
               : refusedWhy;
         if (why === undefined) continue;
         const name =
            upstream?.source.name ??
            splitSourceID(upstreamID)?.name ??
            upstreamID;
         dropped.add(sourceID);
         findings.push(
            notBuiltFinding(
               compiled,
               sourceID,
               UPSTREAM_TABLE_UNUSABLE_REASON,
               `its upstream '${name}' declares a persisted table, but ${why}, ` +
                  "and a downstream is never built by recomputing its " +
                  "upstream from raw",
            ),
         );
         break;
      }
   }
   return dropped.size === 0
      ? compiled
      : withoutSources(compiled, findings, dropped);
}

/**
 * The one tail of every package plan, the load worker's and the main
 * thread's: each connection a build runs on resolved and its digest read,
 * then the parts finished ({@link finishBuildPlan}). Returns the connections
 * whose digest could not be read, for the caller to report; reports nothing
 * itself.
 */
export async function finishPackagePlan(
   parts: BuildPlanParts,
   ctx: Omit<
      Parameters<typeof finishBuildPlan>[1],
      "connectionDigests" | "connections" | "upstreamSchemas"
   > & {
      getMalloyConnection(name: string): Promise<MalloyConnection>;
      /**
       * Read the native column types of the tables the plan's sources read
       * (the load's catalog read, or a run's reuse of its load's readings).
       * Absent, none is read and every address is its SQL's alone: a spec's.
       */
      readUpstreamSchemas?: UpstreamSchemaReader;
   },
): Promise<{ compiled: FinishedBuildPlan; digestSkipped: string[] }> {
   const { getMalloyConnection, readUpstreamSchemas, ...finish } = ctx;
   const connections = await resolvePackageConnections(
      { getMalloyConnection },
      parts.builds.map((b) => b.connectionName),
   );
   const digestSkipped: string[] = [];
   const connectionDigests = await resolveConnectionDigests(
      connections,
      parts.builds,
      (name) => digestSkipped.push(name),
   );
   const upstreamSchemas = readUpstreamSchemas
      ? await readUpstreamSchemas(planUpstreamTables(parts.tables))
      : undefined;
   return {
      compiled: finishBuildPlan(parts, {
         ...finish,
         connectionDigests,
         ...(upstreamSchemas ? { upstreamSchemas } : {}),
         connections,
      }),
      digestSkipped,
   };
}

/**
 * Reads the native column types of `tables`, each answered by
 * `upstreamTableKey`: a reading for every table asked, never a throw.
 */
export type UpstreamSchemaReader = (
   tables: UpstreamTable[],
) => Promise<UpstreamSchemaReadings>;

/**
 * Every table the plan's sources read (their `reads`), distinct, in a stable
 * order: what a load reads the catalog for.
 */
function planUpstreamTables(
   tables: Readonly<Record<string, { source: PersistSource }>>,
): UpstreamTable[] {
   const out = new Map<string, UpstreamTable>();
   for (const { source } of Object.values(tables)) {
      for (const read of upstreamTablesOf(source) ?? []) {
         out.set(upstreamTableKey(read), read);
      }
   }
   return [...out.entries()]
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([, t]) => t);
}

/**
 * Project a finished build plan ({@link finishBuildPlan}) into its
 * {@link PackagePlan}. Reads the connection digests, never the live
 * connections, so it runs the same in the package-load worker as on the main
 * thread; counts each refusal into `eligibilityRefused` (and into `counts`,
 * which a caller holds when the derivation may throw) and records none.
 */
export function derivePackagePlan(
   compiled: Omit<FinishedBuildPlan, "connections">,
   materializationConfig: WirePackageMaterialization | null,
   counts: EligibilityRefusals = {},
): PackagePlan<PackageAnnotationFinding> {
   const incrementalDeclarations = collectIncrementalDeclarations(
      compiled.tables,
   );
   // Each table's eligibility, decided once, and each refused source counted
   // once.
   const verdicts = planVerdicts(compiled);
   for (const { counted } of verdicts.values()) {
      if (counted) countRefusal(counts, counted);
   }
   // A plan that lost sources is not "no plan": it is kept, so a host reads
   // `omittedSources` rather than an empty plan.
   const derived =
      compiled.builds.length === 0 && !compiled.omittedSources.length
         ? null
         : deriveBuildPlan(compiled, materializationConfig, verdicts);
   // A collision this deployment refuses alone, rather than failing the
   // load, is a refusal record like any other.
   const projected = projectRefusals(
      derived,
      compiled.annotationFindings.map((f) =>
         relaxedCollision(f, getPersistCollisionEnforce()),
      ),
      counts,
   );
   const { annotationFindings, refusalNotices } = projected;
   const refusedPlan = withRefusalOrigins(
      projected.plan,
      compiled.tables,
      refusalNotices,
   );
   const asOfBuild = asOfBuildOf(refusedPlan, compiled.tables);
   const plan = withAsOfBuild(refusedPlan, asOfBuild);
   return {
      plan,
      tables: tablePlanDataOf(compiled.tables),
      builds: compiled.builds,
      connectionDigests: compiled.connectionDigests,
      connectionTypes: compiled.connectionTypes ?? {},
      managedConnections: compiled.managedConnections ?? [],
      buildConnectionNames: [
         ...new Set(compiled.builds.map((b) => b.connectionName)),
      ].sort(),
      versionScoped: compiled.versionScoped === true,
      tableFolds: { ...compiled.tableFolds },
      upstreamSchemas: { ...compiled.upstreamSchemas },
      addresses: { ...compiled.addresses },
      compilerKeys: compilerKeysOf(compiled),
      annotationFindings,
      droppedPersistSources: compiled.droppedPersistSources,
      modelGateLines: compiled.modelGateLines,
      persistedViews: compiled.persistedViews ?? {},
      refusalNotices,
      refusedRollups: refusedRollupsOf(plan, compiled.tables),
      sourceEligibility: collectSourceEligibility(compiled, verdicts),
      databaseSourceEligibility: collectDatabaseSourceEligibility(
         compiled,
         verdicts,
      ),
      rollupsKeepingGates: Object.entries(compiled.tables)
         .filter(
            ([, table]) =>
               table.rollup !== undefined &&
               rollupGateEnforcedAtRead(table.rollup),
         )
         .map(([sourceID]) => sourceID)
         .sort(),
      incrementalDeclarations,
      materializePlans: materializePlansOf(compiled.tables),
      asOfBuild,
      accessReads: [...(compiled.accessReads ?? [])],
      eligibilityRefused: { ...counts },
   };
}

/**
 * `plan` with `asOfBuild: true` on each source entry stored as of its build,
 * so a host that reuses a ready table by its address can tell it must
 * rebuild this one on every run.
 */
function withAsOfBuild(
   plan: BuildPlan | null,
   asOfBuild: Readonly<Record<string, AsOfBuildSource>>,
): BuildPlan | null {
   if (!plan || Object.keys(asOfBuild).length === 0) return plan;
   const sources = { ...plan.sources };
   for (const sourceID of Object.keys(asOfBuild)) {
      const entry = sources[sourceID];
      if (entry) sources[sourceID] = { ...entry, asOfBuild: true };
   }
   return { ...plan, sources };
}

/** The planned tables stored as of their build (`PackagePlan.asOfBuild`). */
function asOfBuildOf(
   plan: BuildPlan | null,
   tables: Readonly<Record<string, TablePlan>>,
): Record<string, AsOfBuildSource> {
   const out: Record<string, AsOfBuildSource> = {};
   for (const [sourceID, table] of Object.entries(tables)) {
      if (!plan?.sources?.[sourceID]) continue;
      if (!storedAsOfBuild(table.source, table.kind)) continue;
      out[sourceID] = {
         name: table.source.name,
         ...(table.modelPath ? { modelPath: table.modelPath } : {}),
         reads: currentTimeReadsOf(table.source),
      };
   }
   return out;
}

/** Each planned table as plain data: its compiled source and target left out. */
function tablePlanDataOf(
   tables: Readonly<Record<string, TablePlan>>,
): Record<string, TablePlanData> {
   return Object.fromEntries(
      Object.entries(tables).map(([sourceID, table]) => {
         const {
            source,
            target: _target,
            overStoredBase,
            ...data
         } = table as TablePlan & { overStoredBase?: unknown };
         return [
            sourceID,
            {
               ...data,
               name: source.name,
               connectionName: source.connectionName,
               ...(overStoredBase ? { overStoredBaseCandidate: true } : {}),
            } as TablePlanData,
         ];
      }),
   );
}

/** {@link PackagePlan.compilerKeys}. */
function compilerKeysOf(
   compiled: Pick<
      CompiledBuildPlan,
      "tables" | "connectionDigests" | "tableFolds"
   > & { addresses: PlanAddresses },
): Record<string, string> {
   const keys: Record<string, string> = {};
   for (const [sourceID, { source, target }] of Object.entries(
      compiled.tables,
   )) {
      try {
         const address = planAddressOf(compiled, sourceID);
         const key = compilerBuildId(source, target);
         if (key !== address) keys[address] = key;
      } catch {
         // Unaddressable: refused on its own path.
      }
   }
   return keys;
}

/**
 * The compiled plan less each source whose VALID incremental declaration this
 * deployment cannot honour (incremental_policy's rules 6, 7 and 12), each
 * reported as its route's `*-not-built` warning: not built, answered live,
 * and the rest of the package loads. An invalid declaration is left in, for
 * the load's incremental gate to fail.
 */
function refuseIncrementalUnbuildable<
   T extends Omit<FinishedBuildPlan, "connections">,
>(compiled: T): T {
   const declarations = collectIncrementalDeclarations(compiled.tables);
   const findings: PackageAnnotationFinding[] = [];
   const dropped = new Set<string>();
   // An author collision is invalid whatever else is wrong with the source:
   // a source sharing a table is left in for the collision check to judge.
   const shared = sharedTables(compiled);
   for (const [sourceID, declaration] of Object.entries(declarations)) {
      const source = compiled.tables[sourceID]?.source;
      if (!source || shared.has(sourceID)) continue;
      const view = persistedViewOf(source);
      const reason = incrementalUnbuildableReason({
         sourceName: view?.sourceName ?? source.name,
         ...(view ? { viewName: view.viewName } : {}),
         dialect: source.dialectName,
         declaration,
      });
      if (reason === undefined) continue;
      dropped.add(sourceID);
      findings.push(
         notBuiltFinding(compiled, sourceID, "incremental_unsupported", reason),
      );
   }
   return withoutSources(compiled, findings, dropped);
}

/**
 * The compiled plan less every source a table of whose `reads` has no
 * reading (`unread: "read_failed"`: its catalog read failed, after the
 * reader's retries, and this process holds no earlier reading of it), each
 * refused alone as `upstream_unread` under its mechanism's `*-not-built`
 * warning naming the table. Its content address would fold an unknown
 * schema, which a later successful read moves, so it is neither built nor
 * served: it answers live, no run builds it, and a host's instruction for it
 * is refused, until a reload or a plan refresh reads the catalog and plans it
 * at its address. A reading of what the catalog holds (`no_catalog`,
 * `not_found`, `ambiguous`) is a stable fold and refuses nothing.
 */
function refuseUpstreamUnread<T extends Omit<FinishedBuildPlan, "connections">>(
   compiled: T,
): T {
   const readings = compiled.upstreamSchemas;
   if (!readings) return compiled;
   const findings: PackageAnnotationFinding[] = [];
   const dropped = new Set<string>();
   for (const [sourceID, { source }] of Object.entries(compiled.tables)) {
      const unread = (upstreamTablesOf(source) ?? []).filter((t) => {
         const reading = readings[upstreamTableKey(t)];
         return reading !== undefined && "unread" in reading
            ? reading.unread === "read_failed"
            : false;
      });
      if (unread.length === 0) continue;
      dropped.add(sourceID);
      const tables = unread
         .map((t) => `"${t.table}" on connection "${t.connectionName}"`)
         .join(", ");
      findings.push(
         notBuiltFinding(
            compiled,
            sourceID,
            "upstream_unread",
            `the columns of ${tables}, which its build reads, could not be ` +
               "read from the warehouse's catalog, so its table's address is " +
               "not known; it is planned once a reload, or a plan refresh, " +
               "reads them",
         ),
      );
   }
   return withoutSources(compiled, findings, dropped);
}

/**
 * The `*-not-built` finding refusing the plan's source `sourceID` alone, as
 * `reason`, under its mechanism's code: a `#(materialize)` source's, a
 * persisted view's, a rollup's, or a `#@ persist` source's.
 */
function notBuiltFinding(
   compiled: Omit<FinishedBuildPlan, "connections">,
   sourceID: string,
   reason: RefusedReason,
   why: string,
): PackageAnnotationFinding {
   const table = compiled.tables[sourceID];
   const source = table.source;
   const model = table.modelPath ?? "";
   const tier = tierOf(declaredStorage(source));
   const refused = refusalKey(sourceID, source.name, reason, tier);
   if (table.materialize) {
      return materializeRefusalFinding(
         {
            sourceName: table.materialize.sourceName,
            sourceID,
            reason: why,
            outcome: { refused: { reason, tier } },
         },
         model,
      );
   }
   const view = persistedViewOf(source);
   if (view) {
      return {
         ...persistedViewRefusalFinding(
            { ...view, sourceID, reason: why, outcome: { refused: reason } },
            model,
         ),
         // Keyed by the source the plan holds.
         refused,
      };
   }
   return {
      route: "@",
      tier: "unservable",
      ...(table.kind === "rollup"
         ? {
              code: ROLLUP_NOT_BUILT_CODE,
              message: `Rollup "${source.name}" is not built, so its measures are answered live: ${why}.`,
           }
         : {
              code: PERSIST_NOT_BUILT_CODE,
              message: `#@ persist source "${source.name}" is not built, so it is answered live: ${why}.`,
           }),
      subject: source.name,
      model,
      refused,
   };
}

/** A persisted view's generated build source, read back to the author's names. */
function persistedViewOf(
   source: PersistSource,
): { sourceName: string; viewName: string } | undefined {
   if (!isPersistedResultSourceID(source.sourceID)) return undefined;
   const match = /^(.+)__(.+)__view$/.exec(source.name);
   return match ? { sourceName: match[1], viewName: match[2] } : undefined;
}

/** The mechanism a refusal's `*-not-built` warning code names. */
const ORIGIN_OF_NOT_BUILT_CODE: Readonly<Record<string, TableOrigin>> = {
   [PERSIST_NOT_BUILT_CODE]: "persist",
   [MATERIALIZE_NOT_BUILT_CODE]: "materialize",
   [PERSIST_VIEW_NOT_BUILT]: "view",
   [PERSIST_QUERY_NOT_BUILT]: "query",
   [ROLLUP_NOT_BUILT_CODE]: "rollup",
};

/**
 * The plan with each refusal record's `origin`, the mechanism that declared
 * it, read off the plan and never off a name: a rollup's record (`tier`
 * `preaggregate`), a persisted view's or query's generated build source, the
 * planned table's kind, and, for a declaration refused before it was
 * planned, the mechanism its `*-not-built` warning names.
 */
function withRefusalOrigins(
   plan: BuildPlan | null,
   tables: Readonly<Record<string, Pick<TablePlan, "kind">>>,
   notices: RefusalNotices,
): BuildPlan | null {
   const refused = plan?.refusedSources;
   if (!plan || !refused || Object.keys(refused).length === 0) return plan;
   const originOf = (sourceID: string, tier: string): TableOrigin => {
      // How a record before this release said rollup.
      if (tier === "preaggregate") return "rollup";
      if (isPersistedResultSourceID(sourceID)) {
         return originOfTable("view", sourceID);
      }
      const table = tables[sourceID];
      if (table) return originOfTable(table.kind, sourceID);
      return (
         ORIGIN_OF_NOT_BUILT_CODE[notices[sourceID]?.code ?? ""] ?? "persist"
      );
   };
   return {
      ...plan,
      refusedSources: Object.fromEntries(
         Object.entries(refused).map(([sourceID, r]) => [
            sourceID,
            { ...r, origin: originOf(sourceID, r.tier) },
         ]),
      ),
   };
}

/** {@link PackagePlan.refusedRollups}. */
function refusedRollupsOf(
   plan: BuildPlan | null,
   tables: Readonly<Record<string, Pick<TablePlan, "rollup">>>,
): Record<string, RefusedRollup> {
   const out: Record<string, RefusedRollup> = {};
   for (const sourceID of Object.keys(plan?.refusedSources ?? {})) {
      const rollup = tables[sourceID]?.rollup;
      if (!rollup) continue;
      out[sourceID] = {
         baseSourceName: rollup.baseSourceName,
         grainDimensions: rollup.grainDimensions,
      };
   }
   return out;
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
function pairDelta(
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
   /**
    * The planned tables by sourceID; a `#(materialize)` plan's rolling window
    * rides on its declaration, for the refresh's delete.
    */
   tables: Readonly<Record<string, Pick<TablePlan, "source" | "materialize">>>,
): Record<string, IncrementalDeclaration> {
   const declarations: Record<string, IncrementalDeclaration> = {};
   for (const [sourceID, { source, materialize: plan }] of Object.entries(
      tables,
   )) {
      try {
         const declaration = resolveIncrementalDeclaration(
            source,
            deriveAnnotationFields(source),
         );
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
            code: errorCodeOf(err),
         });
      }
   }
   return declarations;
}
