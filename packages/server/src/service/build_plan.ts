import type {
   AtomicField,
   BuildGraph as MalloyBuildGraph,
   BuildNode,
   MalloyConfig,
   Connection as MalloyConnection,
   ModelDef,
   ModelMaterializer,
   PersistSource,
   SourceDef,
} from "@malloydata/malloy";
import { Annotations } from "@malloydata/malloy";
import { components } from "../api";
import { MaterializationEligibilityError } from "../errors";
import { MODEL_FILE_SUFFIX } from "../constants";
import { logger } from "../logger";
import {
   recordConnectionDigestSkipped,
   recordEligibilityRefused,
} from "../materialization_metrics";
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
import { Model } from "./model";
import { tryCompileSynthesizedPreaggregation } from "./preaggregation_compile";
import type { RollupPlan } from "./preaggregation_synthesis";
import { quoteIdentifier } from "./quoting";

type WireBuildGraph = components["schemas"]["BuildGraph"];
type WirePersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type WireRefusedSource = components["schemas"]["RefusedSource"];
type WireColumn = components["schemas"]["Column"];
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
 * Minimal reader over a Malloy `Tag` (see `@malloydata/malloy-tag`): scalar
 * reads by path, plus the subtree read that a property collection like
 * `queryMetadata { … }` needs.
 */
export interface ReadableTag {
   text(...path: string[]): string | undefined;
   tag(...path: string[]): ReadableTag | undefined;
   entries?(): Iterable<[string, { text(): string | undefined }]>;
}

/**
 * Minimal surface a package must expose to compile its build plan. Both
 * {@link Package} (for the read-only `Package.buildPlan`) and the
 * materialization service (for auto-run and orchestrated builds) satisfy it.
 */
export interface BuildPlanPackage {
   getModelPaths(): string[];
   getPackagePath(): string;
   getMalloyConfig(): MalloyConfig;
   getMalloyConnection(name: string): Promise<MalloyConnection>;
   /**
    * The package-level `materialization` config (from malloy-publisher.json),
    * used as the least-specific layer when resolving per-source freshness /
    * schedule. Optional so existing fixtures/callers that don't track it still
    * typecheck (they resolve without a package default).
    */
   getMaterializationConfig?(): WirePackageMaterialization | null;
}

/**
 * Result of compiling a package's persist sources: the dependency-ordered
 * build graphs, the persist sources keyed by sourceID, the per-connection
 * digests, and the resolved live connections. The wire {@link BuildPlan} is a
 * projection of this (see {@link deriveBuildPlan}); the build engine consumes
 * the full structure.
 */
export interface CompiledBuildPlan {
   graphs: MalloyBuildGraph[];
   sources: Record<string, PersistSource>;
   connectionDigests: Record<string, string>;
   connections: Map<string, MalloyConnection>;
   /**
    * sourceID -> the package-relative path of the `.malloy` model that declares
    * the source (the only place that mapping is known, since a sourceID's
    * embedded modelURL is an absolute `file://` path with no package boundary).
    * Optional so existing fixtures/callers that don't track it still typecheck.
    */
   sourceModelPaths?: Record<string, string>;
   /**
    * Sources carrying a `#@ persist` annotation that Malloy's getBuildPlan() did
    * NOT recognize as a materializable build root, so they produced no plan entry
    * and would otherwise be a silent no-op (served live, never materialized). See
    * {@link detectDroppedPersistSources}. Callers surface these: a load-time
    * warning and a hard build failure, so a persist annotation is never silently
    * dropped.
    */
   droppedPersistSources?: { name: string; modelPath: string }[];
   /**
    * sourceID -> the rollup it was synthesized from, for the sources that were
    * synthesized rather than authored. Absent for an ordinary `#@ persist`
    * source, which is what makes it the provenance signal the wire plan's
    * `origin`/`preaggregate` fields report.
    */
   preaggregatePlans?: Record<string, RollupPlan>;
   /**
    * sourceID -> this source's entry-point `#(authorize)` gate classification,
    * computed HERE (compile time) because this is where the compiled
    * `{modelDef, materializer}` pair a classification needs exists — see
    * {@link classifyPersistSourceGate}. Consumed by a relaxation of the
    * colocated `#@ persist` refusal (`assertColocatedPersistNotAuthorizeGated`)
    * that decides whether to admit a gated source; recording the outcome here
    * changes no refusal on its own. Optional so existing fixtures/callers
    * that don't track it still typecheck.
    */
   sourceGateOutcomes?: Record<string, PersistSourceGateOutcome>;
}

/** {@link CompiledBuildPlan.sourceGateOutcomes}'s per-source classification. */
export type PersistSourceGateClassification = "row_level" | "rejected";

/**
 * `classification` is the entry-point gate's enforcement shape, per
 * `gate_classification.ts`'s vocabulary (every `#(authorize)` gate is a row
 * predicate). `attributed` is `false` when {@link isAuthorizeAttributedToEntryPoint}'s
 * deep walk finds a note reachable only through a join — see that function's
 * doc for why that must gate the relaxation independently of
 * `classification`: `collectEntryPointGates` does not trace joins, so a
 * join-carried gate can be entirely invisible to `classification` while still
 * being real and enforced-against today.
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

/**
 * A source's PUBLIC column names, or a throw if they can't be determined. The
 * strict counterpart to {@link deriveColumns}, which degrades to `[]` because its
 * caller (the wire build plan) only reports columns. Here the list decides what
 * gets WRITTEN, so "unknown" must not read as "nothing to narrow".
 *
 * A name is returned only for a field that is demonstrably a public atomic field;
 * an unreadable field list, an unreadable individual field, or an empty result all
 * throw. Empty is a throw rather than a pass-through because a source with no
 * public atomic columns means everything `getSQL` projects is hidden — the worst
 * case to materialize, not a benign one.
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
 * every underlying column, including ones the source hides (`except:`, non-public
 * access modifiers); this narrows the physical table to the public surface.
 *
 * A `storage=` build must not materialize a hidden column. Reachability THROUGH
 * the source is separately bounded by the declared serve shape (see
 * `narrowSchemaToPublic`, and the `shape-bounds-physical-columns` scenario that
 * proves a physical column absent from the shape does not resolve). What this
 * projection prevents is the hidden column's values sitting AT REST in the
 * destination store, where direct catalog access reaches them and where the data
 * may have crossed a trust boundary the source's visibility rules were meant to
 * hold. Applies to BOTH storage build paths (the warehouse-passthrough
 * single-source build and the chained "stack on the parent" downstream build).
 *
 * Fails CLOSED: if the public surface can't be determined the build is refused,
 * rather than widening to everything `getSQL` projects. This matches the rest of
 * the eligibility surface (see `assertMaterializationEligible`), and it fires
 * before any warehouse or destination SQL runs, so a refusal writes nothing and
 * leaves the previous generation serving.
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
 * All key=value fields of a source's `#@ persist` annotation (e.g.
 * `{ name: "engaged_events", realization: "COPY" }`), degrading to {} if the
 * annotation is absent or unparseable. The control plane consumes these — most
 * importantly `name`, which it uses as the materialized table name (and which
 * may carry a dialect-style container path like `dataset.table`). Returning the
 * full set rather than a fixed subset means new persist directives flow through
 * without a publisher change.
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
      // Degrade to {} — mirrors deriveColumns / selfAssignTableName.
   }
   return out;
}

/**
 * Read the freshness keys (`freshness.window`, `freshness.fallback`) from one
 * Malloy tag layer, keeping only recognized values. These are dotted/nested tag
 * properties, so they are NOT captured by the scalar {@link deriveAnnotationFields}
 * loop and must be read by path here. (Per-source `sharing`/`schedule` were
 * retired — scope is package-level and a schedule is package-root-only — so they
 * are not resolved here; declaring either on a source is a publish-time manifest
 * error, enforced in Package.persistencePolicyWarnings.)
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
 * The two homes a model-file (`##`) knob can be declared in, most specific
 * first: the `materialization` envelope, then the bare form.
 *
 * The model-file level mirrors the manifest's shape verbatim — `##
 * materialization.freshness.window="24h"` is the manifest's block in tag syntax
 * — so that is where a file-level reader looks. The bare form
 * (`## freshness.window="24h"`), which shipped first, stays readable underneath:
 * the annotation rides the published package, so a package published before the
 * envelope existed keeps resolving until it is republished. Resolution is
 * per-property, so a file may declare one knob in each home without the envelope
 * hiding the other.
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
 * Resolve a source's EFFECTIVE freshness with most-specific-wins precedence,
 * per field: source annotation > model-file default > package default. Reported
 * verbatim (unset at every level stays null so the control plane can
 * distinguish "unset" — apply platform default — from an explicit declaration).
 * Invalid `fallback` values are dropped, never defaulted. Freshness is valid in
 * both scope modes.
 */
export function resolveFreshness(
   source: PersistSource,
   packageMaterialization: WirePackageMaterialization | null | undefined,
): WireFreshness | null {
   const layers: FreshnessLayer[] = [
      // `#@ persist` declares knobs bare — the annotation IS a materialization
      // declaration, so there is no envelope to look under.
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
 * Read the `queryMetadata` property collection from one tag layer. A collection,
 * not a scalar: `queryMetadata { team="finance" env="prod" }` and the equivalent
 * dotted form `queryMetadata.team="finance"` both land here, and neither is
 * captured by the scalar {@link deriveAnnotationFields} loop.
 *
 * Every string-valued property is kept verbatim, including ones that violate
 * Malloy's bag contract — publish reports those as warnings and the runtime
 * clamps them, so an author's typo is visible somewhere instead of vanishing
 * between the annotation and the warehouse.
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
      // Degrade to {} — mirrors deriveAnnotationFields / deriveColumns.
      return {};
   }
   return layer;
}

/**
 * Resolve a source's EFFECTIVE per-query metadata, most-specific-wins PER
 * PROPERTY: `#@ persist queryMetadata.*` > model-file
 * `## materialization.queryMetadata.*` (bare `## queryMetadata.*` underneath) >
 * package `materialization.queryMetadata`.
 *
 * Per-property rather than per-layer, exactly like {@link resolveFreshness}, so a
 * package-wide `team` property survives a source that only overrides `workload`.
 * Null when no layer declares anything, so absence on the wire always means
 * "declared nowhere" rather than "declared empty".
 *
 * This is the value the publisher attaches (merged under its own context) to
 * every statement it issues while building the source. It is deliberately absent
 * from the source's content address: changing a tag must never re-address a
 * table.
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
 * `## queryMetadata.*` > package `queryMetadata`.
 *
 * The package and model-file layers each have a deprecated
 * `materialization.`-prefixed spelling, still read, sitting UNDERNEATH its
 * canonical form — so a file declaring both resolves to the canonical one. That
 * is the OPPOSITE of freshness, where the prefixed spelling IS canonical; the
 * ordering note in the body is where that trap is spelled out.
 *
 * Takes tags rather than a {@link PersistSource} so the SERVE path can compose
 * the same layers from a loaded model, where no `PersistSource` exists. A
 * declaration describes the source's traffic, not only its build, so a served
 * query carries it too; {@link resolveQueryMetadata} is the build-path caller,
 * which still reads both tags off the persist source it is building.
 *
 * A layer whose tag is absent contributes nothing rather than clearing what a
 * less specific layer set — so a package-wide `team` survives a source that only
 * overrides `workload`, and a model with no `##` tag does not erase the package
 * default. Null when no layer declares anything, so absence always means
 * "declared nowhere" rather than "declared empty".
 */
export function composeDeclaredQueryMetadata(layers: {
   /**
    * The package's declared bag — a bag, not the materialization policy that
    * carries it on the wire. Nothing here needs a schedule or a freshness
    * window, and threading the policy object through would say this layer is
    * about materialization when it is not.
    */
   packageDeclaration?: QueryMetadata | null;
   modelTag?: ReadableTag;
   sourceTag?: ReadableTag;
}): QueryMetadata | null {
   // Least specific first, so a more specific layer overwrites property by
   // property.
   //
   // `modelTagLayers` yields [envelope, bare], which is most-specific-first for
   // freshness — there the envelope is the canonical spelling and the bare form
   // is the legacy one. Query metadata migrates the other way: the bare `##
   // queryMetadata.*` is canonical and `## materialization.queryMetadata.*` is
   // deprecated. So the list is consumed as-is rather than reversed, putting the
   // deprecated spelling underneath. Reversing it let the spelling we tell
   // authors to stop writing silently override the one we tell them to write.
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
 * Yield every persist source of one graph in dependency order — each source's
 * persist dependencies before the source itself — resolving each node's
 * sourceID against the sources map and skipping nodes whose source is absent.
 * Centralizes the graph→source walk shared by the planning and build loops.
 *
 * <p>Malloy's {@code getBuildPlan()} puts only the <em>root</em> persist sources
 * (the terminals nothing else consumes) in {@code graph.nodes}; every transitive
 * persist dependency is nested under a root in its recursive {@code dependsOn}
 * tree (and present in {@code sources}). Walking only {@code graph.nodes}
 * silently skips every intermediate persist source, so it never gets
 * materialized (it only gets a table by coincidence when it shares a
 * sourceEntityId with a root). We post-order DFS the {@code dependsOn} tree
 * so dependencies are built first (a downstream build can then read its upstream
 * source's freshly materialized table), deduplicating shared (diamond)
 * dependencies by sourceID so each is yielded once. This mirrors the canonical
 * malloy-cli reference build loop (its {@code flattenBuildNodes} +
 * dedup-by-sourceID; see malloydata/malloy-cli src/malloy/build_graph.ts).
 */
export function* iterGraphSources(
   graph: MalloyBuildGraph,
   sources: Record<string, PersistSource>,
): Iterable<PersistSource> {
   const seen = new Set<string>();

   function* visit(node: BuildNode): Iterable<PersistSource> {
      if (seen.has(node.sourceID)) return;
      // Dependencies first: a source built later in the run resolves its
      // upstream references against the tables built earlier (see the Manifest
      // threading in executeInstructedBuild).
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
 * connection identity and canonical SQL. Centralizes the
 * (source, connectionDigests) call shape so planning, self-instruction, build,
 * and serve-time manifest resolution all agree on the same id.
 *
 * <p>This is the single seam between the publisher and the Malloy compiler's
 * source-identity recipe. Today it delegates to
 * {@code PersistSource.makeBuildId(connectionDigest, sql)} — the compiler's
 * current hex content hash. The connection's contribution is already
 * fingerprint-aware: when a connection carries an API `fingerprint`, its
 * digest IS that fingerprint verbatim (see applyConnectionFingerprint in
 * connection.ts), so ids stay stable across credential rotation. When the
 * compiler ships the scoped UUID5 sourceEntityId recipe (scope + connection
 * fingerprint + canonical SQL), swap the delegation below to the new compiler
 * API — this function is the only place the publisher derives the id.
 */
export function computeSourceEntityId(
   source: PersistSource,
   connectionDigests: Record<string, string>,
): string {
   // The no-options `getSQL()` is load-bearing, and incremental refresh is what
   // makes it so: this address is the key the covered_through ledger is stored
   // under (see incremental_build.ts), so it must describe WHAT the source
   // computes and never HOW FAR a run got. A boundary value reaching this SQL —
   // via a buildManifest, a given, or a range predicate — would re-address the
   // table on every refresh, orphaning the data and the boundary together and
   // re-seeding forever. A declaration alone must not move it either, which is
   // pinned as fact 1 in incremental_compiler_contract.spec.ts.
   return source.makeBuildId(
      connectionDigests[source.connectionName],
      source.getSQL(),
   );
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
 * Compile every model in the package and collect the dependency-ordered
 * build graphs, persist sources, connection digests, and resolved
 * connections. The build plan is a pure function of the compiled model plus
 * connection config (no warehouse access).
 */
/**
 * Names of sources in a compiled model that carry a `#@ persist` annotation but
 * are absent from the model's build plan — i.e. Malloy's getBuildPlan() did not
 * recognize them as a materializable build root and silently returned no graph
 * for them.
 *
 * BACKSTOP for a Malloy getBuildPlan() gap: it silently returns no graph for a
 * `#@ persist` source whose shape it doesn't treat as a build root (observed for
 * a filtered pass-through `X is <table> extend { where … }`, which stays type
 * `table`; only query-shaped sources are treated as roots). Without this check
 * the annotation is a silent no-op (served live, no build, no error). The primary
 * fix is in Malloy (recognize the shape or emit a diagnostic); until then, detect
 * the annotated-but-absent source here so callers can refuse loudly.
 *
 * Reads the annotation exactly as Malloy's own checkPersistAnnotation does
 * (`Annotations(def.annotations).parseAsTag('@').tag.has('persist')`). Best-effort
 * and fail-open: any introspection failure yields no dropped names rather than
 * risking a false positive that would wrongly fail a healthy build.
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
 * whether it is `attributed` — see {@link PersistSourceGateOutcome}'s doc.
 *
 * Calls `gate_classification.ts`'s standalone functions directly, rather than
 * standing up a throwaway `Model` — a build-plan compile has exactly the
 * `{modelDef, materializer}` shape those functions operate on (a compiled
 * `ModelDef` plus the live `ModelMaterializer` that produced it), and nothing
 * else. `materializer` MUST be the same one that compiled `modelDef` — see
 * `liftGateCondition`'s doc for why a fresh `loadModel` cannot substitute (it
 * mints a new given identity per call).
 *
 * Gate GROUPS: each `GateEntry` `collectEntryPointGates` returns is one AND'd
 * group (its own OR-disjunction already folded by `resolveGateShape`'s
 * `gateFilterText`); groups are classified independently here and combined
 * with enforcement's own dominance rule — refuse if ANY group rejects, else
 * `row_level`. No entry-point gate at all (`groups.length === 0`) is
 * vacuously `row_level` (no group to reject) — the source is unrestricted at
 * its entry point; `attributed` is what still catches a gate hiding behind a
 * join in that case.
 *
 * Fails CLOSED: a throw anywhere in classification (this function's own
 * `try`, not `resolveGateShape`'s internal one — that already degrades a
 * lift/classify failure to `{shape: "rejected"}` without throwing) records
 * `rejected` + `attributed: false`, never a partial/optimistic result.
 *
 * Deterministic and network-free: `collectEntryPointGates` reads only the
 * already-compiled `modelDef` and struct annotations (no I/O), and
 * `resolveGateShape`'s lift (`liftGateCondition`) compiles a one-row PROBE
 * query through the SAME already-loaded `materializer` — a pure in-memory
 * semantic recompile that only generates SQL text, never opens the warehouse
 * connection or executes anything against it. Load time DOES classify every
 * gate (`validateAuthorizeProbes` calls `classifyAuthorizeGate` per entry
 * point/gate group), but that call returns `Promise<void>` and keeps nothing:
 * the only survivor is `SerializedModel.authorizeWarnings: string[]`, so no
 * classification result is reachable from here — this compile-time call is
 * the only place that outcome is actually computed.
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
         // This IS the entry point — see `collectEntryPointGates`'s
         // `treatAsOwnGate` doc.
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

export async function compilePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<CompiledBuildPlan> {
   const allGraphs: MalloyBuildGraph[] = [];
   const allSources: Record<string, PersistSource> = {};
   const sourceModelPaths: Record<string, string> = {};
   const droppedPersistSources: { name: string; modelPath: string }[] = [];
   const preaggregatePlans: Record<string, RollupPlan> = {};
   const sourceGateOutcomes: Record<string, PersistSourceGateOutcome> = {};

   for (const modelPath of pkg.getModelPaths()) {
      // Only `.malloy` models declare persist sources. Skip `.malloynb`
      // notebooks: getModel() parses a model file as a flat model and throws on
      // the notebook's `>>>` cell delimiter, which would abort the entire
      // package build plan and silently drop every persist source in it.
      if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
      if (signal?.aborted) throw new Error("Build cancelled");

      const { runtime, modelURL, importBaseURL } = await Model.getModelRuntime(
         pkg.getPackagePath(),
         modelPath,
         pkg.getMalloyConfig(),
      );
      // Held onto (rather than chained straight into `.getModel()`) because
      // the gate classification below needs the SAME live materializer that
      // compiled this model — see `classifyPersistSourceGate`'s doc.
      const materializer = runtime.loadModel(modelURL, { importBaseURL });
      const malloyModel = await materializer.getModel();

      // Pre-aggregation, at the build-plan seam. Runs BEFORE the two `continue`s
      // below on purpose: a model can declare `#@ preaggregate` while having no
      // `#@ persist` source of its own (no graphs) and no `experimental.persistence`
      // flag, and in both cases the annotation should still produce a rollup —
      // the SYNTHESIZED model declares the flags it needs and is the only thing
      // holding a persist source. Skipping here would make a valid annotation a
      // silent no-op, which the publish gate exists to prevent.
      const synthesized = await tryCompileSynthesizedPreaggregation({
         packagePath: pkg.getPackagePath(),
         modelPath,
         malloyConfig: pkg.getMalloyConfig(),
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         contents: (malloyModel as any)._modelDef?.contents ?? {},
      });
      if (synthesized) {
         const rollupPlan = synthesized.model.getBuildPlan();
         const rollupNames = new Set(
            synthesized.plans.map((p) => p.rollupSourceName),
         );
         // Graphs wholesale, because they carry the dependency edges: when the
         // base is ITSELF a `#@ persist` source the rollup's node `dependsOn`
         // it, and pruning that would let a rollup build before the table it
         // reads exists.
         allGraphs.push(...rollupPlan.graphs);
         // Sources: only the rollups. The synthesized model imports the
         // author's, so a persist source declared there also appears in this
         // plan — but under the SAME sourceID, since a sourceID embeds the
         // model that DECLARES a source rather than the one importing it. The
         // normal path below adds those from the author's own plan, so leaving
         // them out here avoids re-deriving an identical entry.
         const planByName = new Map(
            synthesized.plans.map((p) => [p.rollupSourceName, p]),
         );
         // Lazily built on the first rollup source: same reasoning as the
         // author-model loop below.
         let rollupClassificationCtx:
            | { modelDef: ModelDef; deps: GateClassificationDeps }
            | undefined;
         for (const [sourceID, source] of Object.entries(rollupPlan.sources)) {
            if (!rollupNames.has(source.name)) continue;
            allSources[sourceID] = source;
            // The AUTHOR's model path, not the synthesized one: a rollup is
            // declared by no file, and the model carrying the annotations is
            // where someone would go to change it (see api-doc's
            // PersistSourcePlan.modelPath).
            sourceModelPaths[sourceID] = modelPath;
            const rollup = planByName.get(source.name);
            if (rollup) preaggregatePlans[sourceID] = rollup;
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
            sourceGateOutcomes[sourceID] = await classifyPersistSourceGate(
               source,
               rollupClassificationCtx.modelDef,
               synthesized.materializer,
               rollupClassificationCtx.deps,
               `${modelPath}#preaggregate`,
            );
         }
      }

      // getBuildPlan() THROWS "Model must have ##! experimental.persistence"
      // on any model that lacks the flag — it does NOT return empty. So a
      // header-less non-persist model in the package (e.g. an imported base
      // model that only defines raw sources) would abort the entire package
      // build plan, silently dropping every persist source in every other
      // model. Mirror Malloy's own guard and skip such models: without the flag
      // a model cannot carry a functioning persist source anyway. Models that
      // DO have the flag but declare no persist source return empty graphs and
      // are skipped by the `graphs.length === 0` check below.
      const modelTag = malloyModel.modelAnnotations.parseAsTag("!").tag;
      if (!modelTag.has("experimental", "persistence")) continue;

      const buildPlan = malloyModel.getBuildPlan();
      for (const msg of buildPlan.tagParseLog) {
         logger.warn("Persist annotation issue", {
            modelPath,
            message: msg.message,
            severity: msg.severity,
         });
      }

      // Detect `#@ persist` sources the plan didn't recognize (see
      // detectDroppedPersistSources). Runs BEFORE the empty-graphs `continue`, so
      // a model whose ONLY persist source is a dropped shape is still caught.
      const recognizedNames = new Set(
         Object.values(buildPlan.sources).map((s) => s.name),
      );
      for (const name of detectDroppedPersistSources(
         malloyModel,
         recognizedNames,
      )) {
         droppedPersistSources.push({ name, modelPath });
      }

      if (buildPlan.graphs.length === 0) continue;

      allGraphs.push(...buildPlan.graphs);
      // Lazily built on the first persist source: avoids reading
      // `malloyModel.givens`/`._modelDef` for a model that declares none (a
      // model can have the persistence flag and yet build no persist source).
      let classificationCtx:
         | { modelDef: ModelDef; deps: GateClassificationDeps }
         | undefined;
      for (const [sourceID, source] of Object.entries(buildPlan.sources)) {
         allSources[sourceID] = source;
         sourceModelPaths[sourceID] = modelPath;
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
         sourceGateOutcomes[sourceID] = await classifyPersistSourceGate(
            source,
            classificationCtx.modelDef,
            materializer,
            classificationCtx.deps,
            modelPath,
         );
      }
   }

   const connections = await resolvePackageConnections(
      pkg,
      allGraphs.map((g) => g.connectionName),
   );
   const connectionDigests: Record<string, string> = {};
   for (const graph of allGraphs) {
      const conn = connections.get(graph.connectionName);
      if (!conn) {
         // The connection failed to resolve (already warned in
         // resolvePackageConnections). Its sourceEntityIds will be computed
         // without a digest, so surface it as a discrete correctness signal
         // rather than skipping silently.
         recordConnectionDigestSkipped();
         logger.warn("Skipping connection digest; connection did not resolve", {
            connectionName: graph.connectionName,
         });
         continue;
      }
      if (!connectionDigests[graph.connectionName]) {
         // getDigest() is fingerprint-aware: a connection configured with an
         // API `fingerprint` returns it verbatim (applyConnectionFingerprint),
         // so a credential rotation does not re-address its sources.
         connectionDigests[graph.connectionName] = await conn.getDigest();
      }
   }

   return {
      graphs: allGraphs,
      sources: allSources,
      connectionDigests,
      connections,
      sourceModelPaths,
      droppedPersistSources,
      preaggregatePlans,
      sourceGateOutcomes,
   };
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
      const annotationFields = deriveAnnotationFields(source);

      // Tier-appropriate eligibility, BEFORE touching getSQL()/
      // computeSourceEntityId() below: a free-parameter or given-referencing
      // source fails those calls, so a refused source must be diverted into
      // `refusedSources` (which needs neither) rather than attempted here. The
      // tier mirrors the build path's own choice (deriveSelfInstructions /
      // executeInstructedBuild): a declared `storage=` gets the full,
      // unconditional storage-destination gate; a plain `#@ persist` gets the
      // colocated gate, which — unlike the storage one — admits a proven
      // row-level, fully-attributed `#(authorize)` gate. Using the storage
      // gate for every source regardless of declared tier (the existing
      // `SourceEligibility.refused`, kept for its own serve-binding purpose)
      // would misreport a now-buildable colocated source as refused.
      const declaresStorage = !!annotationFields.storage;
      const rollup = options?.preaggregatePlans?.[sourceID];
      try {
         if (declaresStorage) {
            assertMaterializationEligible(source);
         } else {
            assertColocatedPersistNotAuthorizeGated(
               source,
               source.name,
               rollup ? "preaggregate" : "persist",
               options?.sourceGateOutcomes?.[sourceID],
            );
         }
      } catch (err) {
         refusedSources[sourceID] = {
            name: source.name,
            sourceID: source.sourceID,
            modelPath: sourceModelPaths?.[sourceID],
            tier: declaresStorage ? "storage" : "colocated",
            reason:
               (err instanceof MaterializationEligibilityError && err.reason) ||
               "authorize",
            message: errMessage(err),
         };
         continue;
      }
      // EFFECTIVE per-source freshness, resolved most-specific-wins
      // (source > model-file > package) and reported verbatim (null = unset at
      // every level). Freshness is a dotted/nested tag key, so it comes from
      // resolveFreshness rather than the scalar annotationFields map, and is
      // valid in both scope modes. Per-source `sharing`/`schedule` are NOT
      // emitted (retired from the contract); if a source declares either it is
      // rejected at publish (Package.persistencePolicyWarnings) — the raw keys
      // still ride `annotationFields` so the validator can detect them.
      // Provenance. A synthesized rollup is an ordinary persist source in every
      // mechanical respect, so nothing downstream can tell it apart from an
      // authored one — which is why the plan has to say. `preaggregate` carries
      // what the rollup covers, since a query never names it and this is the only
      // place its grain and measure set are visible at all.
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
         // Reported verbatim, and no longer inert: the mode is resolved and
         // validated alongside `watermark=`/`merge_key=` (see
         // resolveIncrementalDeclaration, collected per source in
         // computePackageBuildPlan). The resolution itself is deliberately NOT a
         // wire field — the control plane reads the free-form annotationFields.
         refresh: annotationFields.refresh ?? null,
         freshness: resolveFreshness(source, packageMaterialization),
         // EFFECTIVE per-source query metadata, resolved per property across the
         // same layer stack as freshness. A property collection rather than a
         // scalar, so it comes from resolveQueryMetadata rather than the
         // annotationFields map.
         queryMetadata: resolveQueryMetadata(source, packageMaterialization),
         columns: deriveColumns(source),
         annotationFields,
         modelPath: sourceModelPaths?.[sourceID],
      };
   }

   return { graphs: wireGraphs, sources: wireSources, refusedSources };
}

/**
 * Compile and project a package's build plan (null when the package declares no
 * materializable persist source), plus any `#@ persist` sources that were
 * silently dropped from the plan (see {@link detectDroppedPersistSources}) so
 * the caller can surface a load-time warning. A deterministic property of the
 * compiled package; feeds the read-only `Package.buildPlan` field.
 */
export async function computePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<{
   plan: BuildPlan | null;
   droppedPersistSources: { name: string; modelPath: string }[];
   sourceEligibility: SourceEligibility;
   colocatedSourceEligibility: ColocatedSourceEligibility;
   incrementalDeclarations: Record<string, IncrementalDeclaration>;
}> {
   const compiled = await compilePackageBuildPlan(pkg, signal);
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
              pkg.getMaterializationConfig?.() ?? null,
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
 * Each source's resolved incremental declaration, keyed by sourceID to match the
 * wire plan (two models in one package may declare same-named sources).
 *
 * Computed HERE, next to {@link collectSourceEligibility}, for the same reason:
 * this is where the compiled sources exist. The publish gates need the output
 * schema and the query definition's field kinds, neither of which a wire
 * `PersistSourcePlan` carries — and deliberately so, since the control plane's
 * contract gains no typed fields in this phase.
 *
 * Exported for the build path, which resolves the same declarations off its own
 * compile (see MaterializationService.incrementalRunContext).
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
         // One unreadable source must not cost the package its plan. The gates
         // then see no declaration for it, which reads as "declares nothing" —
         // conservative in the direction that matters, since the build path
         // dispatches a delta only for a declaration it can read.
         logger.warn("Failed to resolve a source's incremental declaration", {
            sourceID,
            error: errMessage(err),
         });
      }
   }
   return declarations;
}

/**
 * Which sources may be served from a materialized table, decided HERE because
 * this is where the compiled sources exist. The serve side needs the answer
 * without them: binding a host's manifest deliberately skips the recompile, so it
 * cannot re-derive eligibility and would otherwise have to take the host's word
 * for it.
 *
 * Carries the ELIGIBLE names, not just the refusals, so the serve side can
 * require a positive result. Absence is not eligibility: Malloy admits only
 * query-shaped sources as build roots, so a `#@ persist` on a filtered
 * pass-through (`X is <table> extend { where … }`, which stays type `table`)
 * never reaches this record at all — and that is the row-level-access shape. A
 * refusal-only view would read that silence as consent.
 *
 * Keyed by source NAME because that is what a serve binding carries.
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
         assertMaterializationEligible(source);
         eligible.push(source.name);
      } catch (err) {
         refused[source.name] = errMessage(err);
      }
   }
   return { eligible, refused };
}

/**
 * Which sources may be served from a COLOCATED (same-connection) materialized
 * table, decided HERE for the same reason as {@link SourceEligibility}: the
 * serve side (a manifest bind, whether self-produced or host-supplied) cannot
 * recompile the model to re-derive this, so it needs a positive answer to
 * check against.
 *
 * Keyed by `sourceEntityId`, NOT source name — deliberately unlike
 * {@link SourceEligibility}, whose own doc admits the name key only because
 * that is what a `storage=` serve binding carries. A colocated binding
 * (`FreshnessManifest`) carries no name at all, only `sourceEntityId`, so a
 * name key here would either be unusable at the binding boundary or require
 * resolving a bare name back to a source — exactly the ambiguity the package
 * permits: two different models may each declare a source of the same name,
 * and a name-keyed positive check would let ONE of them being eligible
 * authorize a binding for the OTHER, ineligible one.
 *
 * A `sourceEntityId` collision between two DIFFERENT sources is real, not
 * merely theoretical: it is `makeBuildId(connectionDigest, getSQL())`, and
 * `getSQL()` deliberately excludes annotation bytes, so two persist sources on
 * the same connection with identical compiled SQL but different gates — one
 * eligible, one refused — collide on the same id. Refusal therefore MUST
 * dominate eligibility for a given id: a source is recorded eligible only if
 * every source sharing its id was, and one refusal anywhere retracts the id
 * from `eligibleEntityIds` for good, regardless of iteration order.
 */
export type ColocatedSourceEligibility = {
   /** sourceEntityId of colocated sources examined and found eligible. */
   eligibleEntityIds: Set<string>;
   /** sourceEntityId -> why it was refused; for the operator-facing log. */
   refused: Record<string, string>;
};

function collectColocatedSourceEligibility(
   compiled: CompiledBuildPlan,
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
         );
      } catch (err) {
         // Gate BEFORE computeSourceEntityId, matching the build path's own
         // convention: the colocated gate never calls getSQL(), so it still
         // gives a clean refusal even when getSQL() would throw for an
         // unrelated reason. If computing the id ALSO throws there is
         // nothing to key this refusal under, so the source is dropped —
         // same "unreadable, so silent" fallback as an incremental
         // declaration that fails to resolve.
         let sourceEntityId: string;
         try {
            sourceEntityId = computeSourceEntityId(
               source,
               compiled.connectionDigests,
            );
         } catch {
            continue;
         }
         refused[sourceEntityId] = errMessage(err);
         // A same-id source already recorded eligible by an earlier
         // iteration must be retracted: refusal dominates regardless of
         // which one this loop happens to see first.
         eligibleEntityIds.delete(sourceEntityId);
         continue;
      }
      const sourceEntityId = computeSourceEntityId(
         source,
         compiled.connectionDigests,
      );
      // A refusal recorded for this id (from an earlier or a LATER
      // iteration, see above) must win, so only add when none exists yet.
      if (!(sourceEntityId in refused)) {
         eligibleEntityIds.add(sourceEntityId);
      }
   }
   return { eligibleEntityIds, refused };
}
