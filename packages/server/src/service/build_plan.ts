import type {
   AtomicField,
   BuildGraph as MalloyBuildGraph,
   MalloyConfig,
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { components } from "../api";
import { MODEL_FILE_SUFFIX } from "../constants";
import { logger } from "../logger";
import { recordConnectionDigestSkipped } from "../materialization_metrics";
import { Model } from "./model";

type WireBuildGraph = components["schemas"]["BuildGraph"];
type WirePersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type WireColumn = components["schemas"]["Column"];
type BuildPlan = components["schemas"]["BuildPlan"];

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
         error: err instanceof Error ? err.message : String(err),
      });
      return [];
   }
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

/** Flatten Malloy's nested BuildNode.dependsOn into a list of sourceIDs. */
export function flattenDependsOn(node: {
   dependsOn: { sourceID: string }[];
}): string[] {
   return node.dependsOn.map((d) => d.sourceID);
}

/**
 * The buildId for a persist source: a stable digest of its connection identity
 * and canonical SQL. Centralizes the (source, connectionDigests) call shape so
 * planning, self-instruction, and build all agree on the same id.
 */
export function computeBuildId(
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
            error: err instanceof Error ? err.message : String(err),
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
export async function compilePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<CompiledBuildPlan> {
   const allGraphs: MalloyBuildGraph[] = [];
   const allSources: Record<string, PersistSource> = {};

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

      // getBuildPlan() returns empty graphs for models with no #@ persist
      // sources, so non-persist models are simply skipped below.
      const buildPlan = malloyModel.getBuildPlan();
      for (const msg of buildPlan.tagParseLog) {
         logger.warn("Persist annotation issue", {
            modelPath,
            message: msg.message,
            severity: msg.severity,
         });
      }
      if (buildPlan.graphs.length === 0) continue;

      allGraphs.push(...buildPlan.graphs);
      for (const [sourceID, source] of Object.entries(buildPlan.sources)) {
         allSources[sourceID] = source;
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
         // resolvePackageConnections). Its buildIds will be computed without a
         // digest, so surface it as a discrete correctness signal rather than
         // skipping silently.
         recordConnectionDigestSkipped();
         logger.warn("Skipping connection digest; connection did not resolve", {
            connectionName: graph.connectionName,
         });
         continue;
      }
      if (!connectionDigests[graph.connectionName]) {
         connectionDigests[graph.connectionName] = await conn.getDigest();
      }
   }

   return {
      graphs: allGraphs,
      sources: allSources,
      connectionDigests,
      connections,
   };
}

/** Project the Malloy build plan into the trimmed wire BuildPlan. */
export function deriveBuildPlan(
   graphs: MalloyBuildGraph[],
   sources: Record<string, PersistSource>,
   connectionDigests: Record<string, string>,
   sourceNames?: string[],
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
      wireSources[sourceID] = {
         name: source.name,
         sourceID: source.sourceID,
         connectionName: source.connectionName,
         dialect: source.dialectName,
         buildId: computeBuildId(source, connectionDigests),
         sql: source.getSQL(),
         columns: deriveColumns(source),
         annotationFields: deriveAnnotationFields(source),
      };
   }

   return { graphs: wireGraphs, sources: wireSources };
}

/**
 * Compile and project a package's build plan, or null when the package
 * declares no persist source. Convenience for the read-only `Package.buildPlan`
 * field, which is a deterministic property of the compiled package.
 */
export async function computePackageBuildPlan(
   pkg: BuildPlanPackage,
   signal?: AbortSignal,
): Promise<BuildPlan | null> {
   const compiled = await compilePackageBuildPlan(pkg, signal);
   if (compiled.graphs.length === 0) {
      return null;
   }
   return deriveBuildPlan(
      compiled.graphs,
      compiled.sources,
      compiled.connectionDigests,
   );
}
