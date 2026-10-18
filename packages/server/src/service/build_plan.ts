// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   AtomicField,
   BuildGraph as MalloyBuildGraph,
   BuildNode,
   MalloyConfig,
   Connection as MalloyConnection,
   Model as MalloyModel,
   ModelDef,
   ModelMaterializer,
   PersistSource,
   SourceDef,
} from "@malloydata/malloy";
import { Annotations } from "@malloydata/malloy";
import { components } from "../api";
import { MaterializationEligibilityError } from "../errors";
import { logger } from "../logger";
import { recordEligibilityRefused } from "../materialization_metrics";
import { errMessage } from "../utils";
import {
   createGateClassificationDeps,
   collectEntryPointGates,
   resolveGateShape,
   type GateClassificationDeps,
   type GraftScope,
} from "./gate_classification";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import {
   resolveIncrementalDeclaration,
   type IncrementalDeclaration,
} from "./incremental_declaration";
import {
   assertColocatedPersistNotAuthorizeGated,
   assertMaterializationEligible,
   isAuthorizeAttributedToEntryPoint,
} from "./materialization_eligibility";

import {
   tryCompileSynthesizedPreaggregation,
   type SynthesizedPreaggregation,
   type SynthesizedRuntimeFactory,
} from "./preaggregation_compile";
import type { RollupPlan } from "./preaggregation_synthesis";
import {
   classifyDynamicTerms,
   type DynamicTerm,
} from "./persist_dynamic_terms";
import { resolvePartitionColumns } from "./persist_partition";
import { quoteIdentifier } from "./quoting";

type WireBuildGraph = components["schemas"]["BuildGraph"];
type WirePersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type WireRefusedSource = components["schemas"]["RefusedSource"];
type WireColumn = components["schemas"]["Column"];
type WireStrippedTerm = components["schemas"]["StrippedTerm"];
type WireJoinedTerms = components["schemas"]["JoinedTerms"];
type BuildPlan = components["schemas"]["BuildPlan"];
type WireFreshness = components["schemas"]["Freshness"];
export type WirePackageMaterialization =
   components["schemas"]["PackageMaterializationConfig"];
type QueryMetadata = components["schemas"]["QueryMetadata"];

/** The freshness `fallback` values the publisher recognizes; others are dropped. */
const FRESHNESS_FALLBACKS = ["live", "stale_ok", "fail"] as const;
type FreshnessFallback = (typeof FRESHNESS_FALLBACKS)[number];

/** One layer's contribution to the resolved freshness (all optional). */
interface FreshnessLayer {
   window?: string;
   fallback?: FreshnessFallback;
}

/**
 * Minimal reader over a Malloy `Tag`: scalar reads by path, plus the subtree read
 * a property collection like `queryMetadata { … }` needs.
 */
export interface ReadableTag {
   text(...path: string[]): string | undefined;
   tag(...path: string[]): ReadableTag | undefined;
   entries?(): Iterable<[string, { text(): string | undefined }]>;
}

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
   graphs: MalloyBuildGraph[];
   sources: Record<string, PersistSource>;
   connectionDigests: Record<string, string>;
   connections: Map<string, MalloyConnection>;
   /**
    * sourceID -> package-relative path of the declaring `.malloy` model. Only
    * known here: a sourceID embeds an absolute `file://` URL with no package
    * boundary.
    */
   sourceModelPaths?: Record<string, string>;
   /**
    * `#@ persist` sources Malloy's getBuildPlan() did NOT recognize as build
    * roots (see {@link detectDroppedPersistSources}). Callers surface these as a
    * load-time warning and a hard build failure so the annotation is never a
    * silent no-op.
    */
   droppedPersistSources?: { name: string; modelPath: string }[];
   /**
    * sourceID -> the rollup it was synthesized from; absent for an authored
    * `#@ persist` source. The provenance behind the wire plan's
    * `origin`/`preaggregate` fields.
    */
   preaggregatePlans?: Record<string, RollupPlan>;
   /**
    * sourceID -> entry-point `#(authorize)` gate classification, computed at
    * compile time where the `{modelDef, materializer}` pair exists (see
    * {@link classifyPersistSourceGate}). Read by
    * `assertColocatedPersistNotAuthorizeGated` to decide whether to admit a gated
    * source.
    */
   sourceGateOutcomes?: Record<string, PersistSourceGateOutcome>;
}

/**
 * Whether every gate on the entry point RESOLVED, not which route it was written
 * on. `"row_level"` is kept as the name so stored plans still read; an
 * `#(authorize)` lock lands there too, deliberately — this decides whether a
 * source may be BUILT, and `Model.queryEntryPointHasRowLevelGate` blocks routing
 * to frozen rows per request on ANY collected gate.
 */
type PersistSourceGateClassification = "row_level" | "rejected";

/**
 * `classification`: whether the entry point's gates resolved to something
 * enforceable. `attributed`: `false` when {@link isAuthorizeAttributedToEntryPoint}
 * finds a note reachable only through a join — checked independently because
 * `collectEntryPointGates` does not trace joins, so `classification` cannot see
 * a join-carried gate.
 */
export interface PersistSourceGateOutcome {
   classification: PersistSourceGateClassification;
   attributed: boolean;
}

/** Output columns of a persist source, degrading to [] if unavailable. */
export function deriveColumns(persistSource: PersistSource): WireColumn[] {
   try {
      return persistSource._explore.intrinsicFields
         .filter((f) => f.isAtomicField())
         .map((f) => ({
            name: f.name,
            type: String((f as AtomicField).type),
         }));
   } catch (err) {
      logger.warn("Failed to derive columns for persist source", {
         sourceID: persistSource.sourceID,
         error: errMessage(err),
      });
      return [];
   }
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
   const classified = classifyDynamicTerms(source);
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
 * `getSQL` projects is hidden.
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
 * Wrap a source's build SQL to project only its PUBLIC columns. `getSQL` emits
 * every underlying column, including hidden ones (`except:`, non-public access).
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
 * All scalar key=value fields of a source's `#@ persist` annotation, or {} if
 * absent or unparseable. The control plane uses `name` as the table name (it may
 * carry a container path like `dataset.table`). The full set is returned so new
 * directives flow through without a publisher change.
 */
export function deriveAnnotationFields(
   persistSource: PersistSource,
): Record<string, string> {
   const out: Record<string, string> = {};
   try {
      const tag = persistSource.annotations.parseAsTag("@").tag;
      for (const [key, value] of tag.entries()) {
         const text = value.text();
         if (text !== undefined) out[key] = text;
      }
   } catch {
      // Degrade to {}.
   }
   return out;
}

/**
 * Read `freshness.window` / `freshness.fallback` from one tag layer, keeping only
 * recognized values. Nested keys, so {@link deriveAnnotationFields} misses them.
 * Per-source `sharing`/`schedule` are rejected at publish
 * (Package.persistencePolicyWarnings), not resolved here.
 */
function tagFreshnessLayer(tag: ReadableTag | undefined): FreshnessLayer {
   if (!tag || typeof tag.text !== "function") return {};
   const layer: FreshnessLayer = {};
   const window = tag.text("freshness", "window");
   if (typeof window === "string") layer.window = window;
   const fallback = tag.text("freshness", "fallback");
   if (
      typeof fallback === "string" &&
      (FRESHNESS_FALLBACKS as readonly string[]).includes(fallback)
   ) {
      layer.fallback = fallback as FreshnessFallback;
   }
   return layer;
}

/**
 * The two homes a model-file (`##`) knob can be declared in: the
 * `materialization` envelope (mirroring the manifest's shape), then the bare form
 * (`## freshness.window="24h"`), still read so already-published packages keep
 * resolving. Callers resolve per property, so each home can set different knobs.
 */
function modelTagLayers(
   tag: ReadableTag | undefined,
): (ReadableTag | undefined)[] {
   const envelope =
      tag && typeof tag.tag === "function"
         ? tag.tag("materialization")
         : undefined;
   return [envelope, tag];
}

/** The package-level `materialization.freshness` as a resolution layer. */
function packageFreshnessLayer(
   cfg: WirePackageMaterialization | null | undefined,
): FreshnessLayer {
   if (!cfg) return {};
   const layer: FreshnessLayer = {};
   if (cfg.freshness?.window) layer.window = cfg.freshness.window;
   const fallback = cfg.freshness?.fallback;
   if (
      typeof fallback === "string" &&
      (FRESHNESS_FALLBACKS as readonly string[]).includes(fallback)
   ) {
      layer.fallback = fallback as FreshnessFallback;
   }
   return layer;
}

/** The source's `#@` tag, or undefined if it fails to parse (degrade to unset). */
function safeSourceTag(source: PersistSource): ReadableTag | undefined {
   try {
      return source.annotations.parseAsTag("@").tag as ReadableTag;
   } catch {
      return undefined;
   }
}

/**
 * The model-file-level (`##`) tag resolved for the source, or undefined on a
 * parse failure. The model-file layer sits between the source annotation and
 * the package default in most-specific-wins resolution.
 */
function safeModelTag(source: PersistSource): ReadableTag | undefined {
   try {
      return source.modelAnnotations.parseAsTag().tag as ReadableTag;
   } catch {
      return undefined;
   }
}

/**
 * Resolve a source's EFFECTIVE freshness, most-specific-wins per field: source >
 * model-file > package. Null when unset everywhere, so the control plane can tell
 * "unset" from a declaration. Invalid `fallback` values are dropped, never
 * defaulted.
 */
export function resolveFreshness(
   source: PersistSource,
   packageMaterialization: WirePackageMaterialization | null | undefined,
): WireFreshness | null {
   const layers: FreshnessLayer[] = [
      // `#@ persist` declares knobs bare: it IS the materialization declaration.
      tagFreshnessLayer(safeSourceTag(source)),
      ...modelTagLayers(safeModelTag(source)).map(tagFreshnessLayer),
      packageFreshnessLayer(packageMaterialization),
   ];

   const window = layers.map((l) => l.window).find((v) => v !== undefined);
   const fallback = layers.map((l) => l.fallback).find((v) => v !== undefined);

   if (window === undefined && fallback === undefined) return null;
   const freshness: WireFreshness = {};
   if (window !== undefined) freshness.window = window;
   if (fallback !== undefined) freshness.fallback = fallback;
   return freshness;
}

/**
 * Read the `queryMetadata` property collection (block or dotted form) from one
 * tag layer. Every string property is kept verbatim, even ones violating Malloy's
 * bag contract: publish warns and the runtime clamps, so a typo stays visible.
 */
function tagQueryMetadataLayer(tag: ReadableTag | undefined): QueryMetadata {
   const subtree =
      tag && typeof tag.tag === "function"
         ? tag.tag("queryMetadata")
         : undefined;
   if (!subtree || typeof subtree.entries !== "function") return {};
   const layer: QueryMetadata = {};
   try {
      for (const [name, value] of subtree.entries()) {
         const text = value.text();
         if (text !== undefined) layer[name] = text;
      }
   } catch {
      return {};
   }
   return layer;
}

/**
 * Resolve a source's EFFECTIVE per-query metadata for the build path (see
 * {@link composeDeclaredQueryMetadata} for precedence). Attached to every
 * statement the build issues, and deliberately absent from the content address:
 * changing a tag must never re-address a table.
 */
export function resolveQueryMetadata(
   source: PersistSource,
   packageMaterialization: WirePackageMaterialization | null | undefined,
): QueryMetadata | null {
   return composeDeclaredQueryMetadata({
      packageDeclaration: packageMaterialization?.queryMetadata ?? null,
      modelTag: safeModelTag(source),
      sourceTag: safeSourceTag(source),
   });
}

/**
 * The author-declared layers of one statement's metadata, composed
 * most-specific-wins PER PROPERTY: source `#@ queryMetadata.*` > model-file
 * `## queryMetadata.*` > package `queryMetadata`. Each of the latter two has a
 * deprecated `materialization.`-prefixed spelling that sits UNDERNEATH its
 * canonical form — the opposite of freshness (see the body).
 *
 * Takes tags rather than a {@link PersistSource} so the serve path can compose
 * the same layers from a loaded model. An absent layer contributes nothing, so a
 * package-wide `team` survives a source that only overrides `workload`. Null when
 * no layer declares anything.
 */
export function composeDeclaredQueryMetadata(layers: {
   /** The package's declared bag, not the materialization policy carrying it. */
   packageDeclaration?: QueryMetadata | null;
   modelTag?: ReadableTag;
   sourceTag?: ReadableTag;
}): QueryMetadata | null {
   // Least specific first, so later layers overwrite property by property.
   //
   // `modelTagLayers` yields [envelope, bare]. For query metadata the bare form
   // is canonical and the envelope deprecated, so it is consumed as-is, NOT
   // reversed: reversing would let the deprecated spelling override the
   // canonical one.
   const ordered: QueryMetadata[] = [
      layers.packageDeclaration ?? {},
      ...modelTagLayers(layers.modelTag).map(tagQueryMetadataLayer),
      tagQueryMetadataLayer(layers.sourceTag),
   ];
   const resolved: QueryMetadata = Object.assign({}, ...ordered);
   return Object.keys(resolved).length > 0 ? resolved : null;
}

/** Flatten Malloy's nested BuildNode.dependsOn into a list of sourceIDs. */
export function flattenDependsOn(node: {
   dependsOn: { sourceID: string }[];
}): string[] {
   return node.dependsOn.map((d) => d.sourceID);
}

/**
 * Yield every persist source of one graph in dependency order, skipping nodes
 * whose source is absent from `sources`.
 *
 * `getBuildPlan()` puts only ROOT persist sources in `graph.nodes`; transitive
 * dependencies are nested in each node's `dependsOn` tree, so walking only
 * `graph.nodes` would never materialize an intermediate source. Post-order DFS
 * builds dependencies first and dedups diamonds by sourceID, mirroring
 * malloy-cli's `flattenBuildNodes` (src/malloy/build_graph.ts).
 */
export function* iterGraphSources(
   graph: MalloyBuildGraph,
   sources: Record<string, PersistSource>,
): Iterable<PersistSource> {
   const seen = new Set<string>();

   function* visit(node: BuildNode): Iterable<PersistSource> {
      if (seen.has(node.sourceID)) return;
      // Dependencies first: later builds resolve upstream references against
      // tables built earlier (see executeInstructedBuild's Manifest threading).
      for (const dep of node.dependsOn) {
         yield* visit(dep);
      }
      seen.add(node.sourceID);
      const source = sources[node.sourceID];
      if (source) yield source;
   }

   for (const level of graph.nodes) {
      for (const node of level) {
         yield* visit(node);
      }
   }
}

/**
 * The sourceEntityId for a persist source: a stable content address of its
 * connection identity and canonical SQL. The only place the publisher derives
 * the id, so planning, build and serve-time manifest resolution all agree.
 *
 * The connection digest is fingerprint-aware (see applyConnectionFingerprint in
 * connection.ts), so ids survive credential rotation.
 */
export function computeSourceEntityId(
   source: PersistSource,
   connectionDigests: Record<string, string>,
): string {
   // The no-options `getSQL()` is load-bearing: this address keys the
   // covered_through ledger (incremental_build.ts), so it must describe WHAT the
   // source computes, never HOW FAR a run got. A boundary value in this SQL would
   // re-address the table every refresh and re-seed forever. A declaration alone
   // must not move it either (fact 1 in incremental_compiler_contract.spec.ts).
   return source.makeBuildId(
      connectionDigests[source.connectionName],
      addressPartitionLayout(source, source.getSQL()),
   );
}

/**
 * The key the compiler itself files a source's table under: like
 * {@link computeSourceEntityId} but without the partition layout. For a
 * partitioned source the two differ, so a manifest handed to the compiler
 * carries the entry under both (see `manifestForCompiler`).
 */
export function compilerBuildId(
   source: PersistSource,
   connectionDigests: Record<string, string>,
): string {
   return source.makeBuildId(
      connectionDigests[source.connectionName],
      source.getSQL(),
   );
}

/**
 * Fold a source's `partition=` into the text its address is computed from.
 * `getSQL()` carries no annotation bytes, so without this a `partition=` change
 * would be skipped as unchanged and the table would keep the old layout.
 *
 * Derived here rather than passed in so every caller agrees
 * (`build_targets_address_equality` pins it). No `partition=` appends nothing,
 * keeping unpartitioned addresses stable; a change moves the address once, which
 * is the rebuild the author asked for.
 */
function addressPartitionLayout(source: PersistSource, sql: string): string {
   let columns: string[] = [];
   try {
      const resolved = resolvePartitionColumns(
         source,
         deriveAnnotationFields(source),
      );
      // A refusal contributes nothing: such a source cannot build at all.
      if (resolved.ok) columns = resolved.columns;
   } catch {
      // An unreadable annotation addresses as unpartitioned.
   }
   if (columns.length === 0) return sql;
   return `${sql}\n-- partitioned by: ${columns.join(", ")}`;
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
 * Backstop for a Malloy getBuildPlan() gap: it silently returns no graph for a
 * persist source it doesn't treat as a build root (e.g. a filtered pass-through
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
            isPersist = new Annotations(def.annotations)
               .parseAsTag("@")
               .tag.has("persist");
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
   persistSource: PersistSource,
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
   graphs: MalloyBuildGraph[];
   sources: Record<string, PersistSource>;
   sourceModelPaths: Record<string, string>;
   droppedPersistSources: { name: string; modelPath: string }[];
   preaggregatePlans: Record<string, RollupPlan>;
   sourceGateOutcomes: Record<string, PersistSourceGateOutcome>;
}

export function emptyBuildPlanParts(): BuildPlanParts {
   return {
      graphs: [],
      sources: {},
      sourceModelPaths: {},
      droppedPersistSources: [],
      preaggregatePlans: {},
      sourceGateOutcomes: {},
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
   into.graphs.push(...from.graphs);
   Object.assign(into.sources, from.sources);
   Object.assign(into.sourceModelPaths, from.sourceModelPaths);
   into.droppedPersistSources.push(...from.droppedPersistSources);
   Object.assign(into.preaggregatePlans, from.preaggregatePlans);
   Object.assign(into.sourceGateOutcomes, from.sourceGateOutcomes);
}

/**
 * Add one compiled `.malloy` model's build graphs, persist sources, dropped
 * `#@ persist` sources, rollups and gate outcomes to `parts`. `materializer`
 * must be the one that compiled `malloyModel` (gate classification grafts
 * probes onto it). `getRuntime` builds the runtime a synthesized
 * pre-aggregation model compiles in.
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
      getRuntime: SynthesizedRuntimeFactory;
      /**
       * The model's pre-aggregation companion when the caller already compiled
       * it, or null when it has none; compiled here when absent.
       */
      synthesized?: SynthesizedPreaggregation | null;
   },
): Promise<void> {
   const { modelPath, packagePath, materializer, malloyModel, getRuntime } =
      args;
   // Pre-aggregation runs BEFORE the early `return`s below: a model may declare
   // `#@ preaggregate` with no persist source and no `experimental.persistence`
   // flag — the SYNTHESIZED model carries both — and must still get its rollup.
   const synthesized =
      args.synthesized !== undefined
         ? args.synthesized
         : await tryCompileSynthesizedPreaggregation({
              packagePath,
              modelPath,
              getRuntime,
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              contents: (malloyModel as any)._modelDef?.contents ?? {},
           });
   if (synthesized) {
      const rollupPlan = synthesized.model.getBuildPlan();
      const rollupNames = new Set(
         synthesized.plans.map((p) => p.rollupSourceName),
      );
      // Graphs wholesale: they carry the edge to a persisted base, without
      // which a rollup could build before the table it reads.
      parts.graphs.push(...rollupPlan.graphs);
      // Sources: only the rollups. The author's persist sources appear here
      // too (same sourceID, since it embeds the declaring model) and are added
      // by the normal path below.
      const planByName = new Map(
         synthesized.plans.map((p) => [p.rollupSourceName, p]),
      );
      // Lazily built, as in the author-model loop below.
      let rollupClassificationCtx:
         | { modelDef: ModelDef; deps: GateClassificationDeps }
         | undefined;
      for (const [sourceID, source] of Object.entries(rollupPlan.sources)) {
         if (!rollupNames.has(source.name)) continue;
         parts.sources[sourceID] = source;
         // The AUTHOR's model path: that is where the annotation is changed.
         parts.sourceModelPaths[sourceID] = modelPath;
         const rollup = planByName.get(source.name);
         if (rollup) parts.preaggregatePlans[sourceID] = rollup;
         if (!rollupClassificationCtx) {
            const rollupGivens = Array.from(
               synthesized.model.givens.values(),
            ) as unknown as MalloyGiven[];
            rollupClassificationCtx = {
               modelDef: synthesized.model._modelDef,
               deps: createGateClassificationDeps(
                  rollupGivens.map(malloyGivenToApi),
                  `${modelPath}#preaggregate`,
               ),
            };
         }
         parts.sourceGateOutcomes[sourceID] = await classifyPersistSourceGate(
            source,
            rollupClassificationCtx.modelDef,
            synthesized.materializer,
            rollupClassificationCtx.deps,
            `${modelPath}#preaggregate`,
         );
      }
   }

   // getBuildPlan() THROWS on a model without `##! experimental.persistence`,
   // which would abort the whole package's plan. Such a model cannot carry a
   // persist source anyway, so skip it, mirroring Malloy's own guard.
   const modelTag = malloyModel.modelAnnotations.parseAsTag("!").tag;
   if (!modelTag.has("experimental", "persistence")) return;

   const buildPlan = malloyModel.getBuildPlan();
   for (const msg of buildPlan.tagParseLog) {
      logger.warn("Persist annotation issue", {
         modelPath,
         message: msg.message,
         severity: msg.severity,
      });
   }

   // BEFORE the empty-graphs `return`, so a model whose only persist source is
   // a dropped shape is still caught.
   const recognizedNames = new Set(
      Object.values(buildPlan.sources).map((s) => s.name),
   );
   for (const name of detectDroppedPersistSources(
      malloyModel,
      recognizedNames,
   )) {
      parts.droppedPersistSources.push({ name, modelPath });
   }

   if (buildPlan.graphs.length === 0) return;

   parts.graphs.push(...buildPlan.graphs);
   // Lazily built on the first persist source.
   let classificationCtx:
      | { modelDef: ModelDef; deps: GateClassificationDeps }
      | undefined;
   for (const [sourceID, source] of Object.entries(buildPlan.sources)) {
      parts.sources[sourceID] = source;
      parts.sourceModelPaths[sourceID] = modelPath;
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

/**
 * Digest each connection the graphs build on. A connection that does not
 * resolve is reported through `onSkipped` and left out, so its sources'
 * entity ids are computed without a digest.
 */
export async function resolveConnectionDigests(
   connections: Map<string, { getDigest(): string | Promise<string> }>,
   graphs: MalloyBuildGraph[],
   onSkipped: (connectionName: string) => void,
): Promise<Record<string, string>> {
   const connectionDigests: Record<string, string> = {};
   for (const graph of graphs) {
      const conn = connections.get(graph.connectionName);
      if (!conn) {
         onSkipped(graph.connectionName);
         continue;
      }
      if (!connectionDigests[graph.connectionName]) {
         // Fingerprint-aware (applyConnectionFingerprint), so credential
         // rotation does not re-address sources.
         connectionDigests[graph.connectionName] = await conn.getDigest();
      }
   }
   return connectionDigests;
}

/** Project the Malloy build plan into the trimmed wire BuildPlan. */
export function deriveBuildPlan(
   graphs: MalloyBuildGraph[],
   sources: Record<string, PersistSource>,
   connectionDigests: Record<string, string>,
   sourceNames?: string[],
   sourceModelPaths?: Record<string, string>,
   packageMaterialization?: WirePackageMaterialization | null,
   options?: {
      preaggregatePlans?: Record<string, RollupPlan>;
      sourceGateOutcomes?: Record<string, PersistSourceGateOutcome>;
   },
): BuildPlan {
   const include = sourceNames ? new Set(sourceNames) : null;

   const wireGraphs: WireBuildGraph[] = graphs.map((graph) => ({
      connectionName: graph.connectionName,
      nodes: graph.nodes.map((level) =>
         level.map((node) => ({
            sourceID: node.sourceID,
            dependsOn: flattenDependsOn(node),
         })),
      ),
   }));

   const wireSources: Record<string, WirePersistSourcePlan> = {};
   const refusedSources: Record<string, WireRefusedSource> = {};
   for (const [sourceID, source] of Object.entries(sources)) {
      if (include && !include.has(source.name)) continue;
      // One unreadable source must not cost the package its plan: getSQL()
      // throws for a colocated source referencing a given with no default, and
      // an escaping throw would drop EVERY colocated binding for the package.
      try {
         const annotationFields = deriveAnnotationFields(source);

         // Tier-appropriate eligibility BEFORE getSQL()/computeSourceEntityId(),
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
               if (declaresStorage) {
                  assertMaterializationEligible(
                     source,
                     deriveAnnotationFields(source),
                  );
               } else {
                  assertColocatedPersistNotAuthorizeGated(
                     source,
                     source.name,
                     "persist",
                     options?.sourceGateOutcomes?.[sourceID],
                     annotationFields,
                  );
               }
            } catch (err) {
               // Only an eligibility refusal is reportable; anything else must not
               // masquerade as a security refusal.
               if (!(err instanceof MaterializationEligibilityError)) throw err;
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
               assertColocatedPersistNotAuthorizeGated(
                  source,
                  source.name,
                  "preaggregate",
                  options?.sourceGateOutcomes?.[sourceID],
                  annotationFields,
               );
               if (declaresStorage) {
                  storageRefused = true;
                  assertMaterializationEligible(
                     source,
                     deriveAnnotationFields(source),
                  );
                  storageRefused = false;
               }
            } catch (err) {
               if (!(err instanceof MaterializationEligibilityError)) throw err;
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
            sql: source.getSQL(),
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
         logger.warn("Failed to project a persist source into the build plan", {
            sourceID,
            sourceName: source.name,
            error: errMessage(err),
         });
      }
   }

   return { graphs: wireGraphs, sources: wireSources, refusedSources };
}

/** What a package load keeps from its build plan. */
export interface BuildPlanOutcome {
   plan: BuildPlan | null;
   droppedPersistSources: { name: string; modelPath: string }[];
   sourceEligibility: SourceEligibility;
   colocatedSourceEligibility: ColocatedSourceEligibility;
   incrementalDeclarations: Record<string, IncrementalDeclaration>;
}

/**
 * Project a compiled build plan into what a package load keeps. Reads the
 * connection digests, never the live connections, so it runs the same in the
 * package-load worker as on the main thread.
 */
export function deriveBuildPlanOutcome(
   compiled: Omit<CompiledBuildPlan, "connections">,
   materializationConfig: WirePackageMaterialization | null,
): BuildPlanOutcome {
   const droppedPersistSources = compiled.droppedPersistSources ?? [];
   const incrementalDeclarations = collectIncrementalDeclarations(
      compiled.sources,
   );
   const plan =
      compiled.graphs.length === 0
         ? null
         : deriveBuildPlan(
              compiled.graphs,
              compiled.sources,
              compiled.connectionDigests,
              undefined,
              compiled.sourceModelPaths,
              materializationConfig,
              {
                 preaggregatePlans: compiled.preaggregatePlans,
                 sourceGateOutcomes: compiled.sourceGateOutcomes,
              },
           );
   return {
      plan,
      droppedPersistSources,
      sourceEligibility: collectSourceEligibility(compiled.sources),
      colocatedSourceEligibility: collectColocatedSourceEligibility(compiled),
      incrementalDeclarations,
   };
}

/**
 * Each source's resolved incremental declaration, keyed by sourceID (two models
 * may declare same-named sources). Computed here because the gates need the
 * compiled output schema and query definition, which the wire plan does not
 * carry. Also used by the build path (MaterializationService.incrementalRunContext).
 */
export function collectIncrementalDeclarations(
   sources: Record<string, PersistSource>,
): Record<string, IncrementalDeclaration> {
   const declarations: Record<string, IncrementalDeclaration> = {};
   for (const [sourceID, source] of Object.entries(sources)) {
      try {
         declarations[sourceID] = resolveIncrementalDeclaration(
            source,
            deriveAnnotationFields(source),
         );
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

/**
 * Which sources may be served from a materialized table, decided here where the
 * compiled sources exist: binding a host's manifest skips the recompile, so the
 * serve side cannot re-derive this.
 *
 * Carries the ELIGIBLE names so the serve side requires a positive result.
 * Absence is not eligibility: a `#@ persist` filtered pass-through (the
 * row-level-access shape) never reaches this record, and a refusal-only view
 * would read that silence as consent. Keyed by source NAME, as serve bindings are.
 */
export type SourceEligibility = {
   /** Sources compiled, examined, and found eligible. */
   eligible: string[];
   /** Source name -> why it was refused; for the operator-facing log. */
   refused: Record<string, string>;
};

function collectSourceEligibility(
   sources: Record<string, PersistSource>,
): SourceEligibility {
   const eligible: string[] = [];
   const refused: Record<string, string> = {};
   for (const source of Object.values(sources)) {
      try {
         assertMaterializationEligible(source, deriveAnnotationFields(source));
         eligible.push(source.name);
      } catch (err) {
         refused[source.name] = errMessage(err);
      }
   }
   return { eligible, refused };
}

/**
 * Which sources may be served from a COLOCATED materialized table, decided here
 * for the same reason as {@link SourceEligibility}.
 *
 * Keyed by `sourceEntityId`, not name: a colocated binding carries only the id,
 * and two models may declare same-named sources, so a name key would let one's
 * eligibility authorize the other.
 *
 * Two DIFFERENT sources can share an id (identical SQL, different gates, since
 * `getSQL()` excludes annotations). Refusal therefore dominates: one refusal
 * retracts the id for good, regardless of iteration order.
 */
export type ColocatedSourceEligibility = {
   /** sourceEntityId of colocated sources examined and found eligible. */
   eligibleEntityIds: Set<string>;
   /** sourceEntityId -> why it was refused; for the operator-facing log. */
   refused: Record<string, string>;
};

function collectColocatedSourceEligibility(
   compiled: Omit<CompiledBuildPlan, "connections">,
): ColocatedSourceEligibility {
   const eligibleEntityIds = new Set<string>();
   const refused: Record<string, string> = {};
   for (const [sourceID, source] of Object.entries(compiled.sources)) {
      const origin = compiled.preaggregatePlans?.[sourceID]
         ? "preaggregate"
         : "persist";
      try {
         assertColocatedPersistNotAuthorizeGated(
            source,
            source.name,
            origin,
            compiled.sourceGateOutcomes?.[sourceID],
            deriveAnnotationFields(source),
         );
      } catch (err) {
         // Gated BEFORE computeSourceEntityId: the gate never calls getSQL(),
         // so it refuses cleanly even when getSQL() would throw.
         let sourceEntityId: string;
         try {
            sourceEntityId = computeSourceEntityId(
               source,
               compiled.connectionDigests,
            );
         } catch (idErr) {
            // No id to key the refusal under; the log is the only record that
            // it was examined and refused.
            logger.warn(
               "Dropping a refused colocated source: its content address could not be computed",
               {
                  sourceID,
                  sourceName: source.name,
                  refusal: errMessage(err),
                  error: errMessage(idErr),
               },
            );
            continue;
         }
         refused[sourceEntityId] = errMessage(err);
         // Refusal dominates: retract any earlier same-id eligibility.
         eligibleEntityIds.delete(sourceEntityId);
         continue;
      }
      // A given-referencing colocated source's getSQL() can throw; contain it
      // to this source's binding.
      let sourceEntityId: string;
      try {
         sourceEntityId = computeSourceEntityId(
            source,
            compiled.connectionDigests,
         );
      } catch (err) {
         logger.warn(
            "Dropping an eligible colocated source: its content address could not be computed",
            { sourceID, sourceName: source.name, error: errMessage(err) },
         );
         continue;
      }
      // A refusal for this id (earlier or later) must win.
      if (!(sourceEntityId in refused)) {
         eligibleEntityIds.add(sourceEntityId);
      }
   }
   return { eligibleEntityIds, refused };
}
