import type {
   AtomicField,
   BuildGraph as MalloyBuildGraph,
   BuildNode,
   MalloyConfig,
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { Annotations } from "@malloydata/malloy";
import { components } from "../api";
import { MODEL_FILE_SUFFIX } from "../constants";
import { logger } from "../logger";
import { recordConnectionDigestSkipped } from "../materialization_metrics";
import { errMessage } from "../utils";
import { Model } from "./model";
import { quoteIdentifier } from "./quoting";

type WireBuildGraph = components["schemas"]["BuildGraph"];
type WirePersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type WireColumn = components["schemas"]["Column"];
type BuildPlan = components["schemas"]["BuildPlan"];
type WireFreshness = components["schemas"]["Freshness"];
type WirePackageMaterialization =
   components["schemas"]["PackageMaterializationConfig"];

/** The freshness `fallback` values the publisher recognizes; others are dropped. */
const FRESHNESS_FALLBACKS = ["live", "stale_ok", "fail"] as const;
type FreshnessFallback = (typeof FRESHNESS_FALLBACKS)[number];

/** One layer's contribution to the resolved freshness (all optional). */
interface FreshnessLayer {
   window?: string;
   fallback?: FreshnessFallback;
}

/** Minimal path-reader over a Malloy `Tag` (see `@malloydata/malloy-tag`). */
interface ReadableTag {
   text(...path: string[]): string | undefined;
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
 * Wrap a source's build SQL to project only its PUBLIC columns. `getSQL` emits
 * every underlying column, including ones the source hides (`except:`, non-public
 * access modifiers); {@link deriveColumns} returns just the public surface (the
 * source's intrinsic atomic fields). A `storage=` build must not materialize a
 * hidden column: a DuckLake virtual source exposes every physical column of the
 * stored table regardless of the served `::Shape`, so a hidden column would be
 * reachable over storage (and would sit at rest). Projecting here makes the
 * physical table equal the public surface. Applies to BOTH storage build paths
 * (the warehouse-passthrough single-source build and the chained "stack on the
 * parent" downstream build). Fails open: if the public columns can't be derived,
 * returns `buildSQL` unchanged — never widens beyond what `getSQL` projected.
 */
export function projectToPublicColumns(
   persistSource: PersistSource,
   buildSQL: string,
): string {
   const cols = deriveColumns(persistSource)
      .map((c) => c.name)
      .filter((n): n is string => typeof n === "string" && n.length > 0);
   if (cols.length === 0) return buildSQL;
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
   const sourceLayer = tagFreshnessLayer(safeSourceTag(source));
   const modelLayer = tagFreshnessLayer(safeModelTag(source));
   const pkgLayer = packageFreshnessLayer(packageMaterialization);

   const window = sourceLayer.window ?? modelLayer.window ?? pkgLayer.window;
   const fallback =
      sourceLayer.fallback ?? modelLayer.fallback ?? pkgLayer.fallback;

   if (window === undefined && fallback === undefined) return null;
   const freshness: WireFreshness = {};
   if (window !== undefined) freshness.window = window;
   if (fallback !== undefined) freshness.fallback = fallback;
   return freshness;
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

export async function compilePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<CompiledBuildPlan> {
   const allGraphs: MalloyBuildGraph[] = [];
   const allSources: Record<string, PersistSource> = {};
   const sourceModelPaths: Record<string, string> = {};
   const droppedPersistSources: { name: string; modelPath: string }[] = [];

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
      const malloyModel = await runtime
         .loadModel(modelURL, { importBaseURL })
         .getModel();

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
      for (const [sourceID, source] of Object.entries(buildPlan.sources)) {
         allSources[sourceID] = source;
         sourceModelPaths[sourceID] = modelPath;
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
   for (const [sourceID, source] of Object.entries(sources)) {
      if (include && !include.has(source.name)) continue;
      const annotationFields = deriveAnnotationFields(source);
      // EFFECTIVE per-source freshness, resolved most-specific-wins
      // (source > model-file > package) and reported verbatim (null = unset at
      // every level). Freshness is a dotted/nested tag key, so it comes from
      // resolveFreshness rather than the scalar annotationFields map, and is
      // valid in both scope modes. Per-source `sharing`/`schedule` are NOT
      // emitted (retired from the contract); if a source declares either it is
      // rejected at publish (Package.persistencePolicyWarnings) — the raw keys
      // still ride `annotationFields` so the validator can detect them.
      wireSources[sourceID] = {
         name: source.name,
         sourceID: source.sourceID,
         connectionName: source.connectionName,
         dialect: source.dialectName,
         sourceEntityId: computeSourceEntityId(source, connectionDigests),
         sql: source.getSQL(),
         refresh: annotationFields.refresh ?? null,
         freshness: resolveFreshness(source, packageMaterialization),
         columns: deriveColumns(source),
         annotationFields,
         modelPath: sourceModelPaths?.[sourceID],
      };
   }

   return { graphs: wireGraphs, sources: wireSources };
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
}> {
   const compiled = await compilePackageBuildPlan(pkg, signal);
   const droppedPersistSources = compiled.droppedPersistSources ?? [];
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
           );
   return { plan, droppedPersistSources };
}
