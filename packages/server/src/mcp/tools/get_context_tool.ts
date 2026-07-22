import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import lunr from "lunr";
import { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import {
   EmbeddingProvider,
   embeddingConfigured,
   getEmbeddingProvider,
} from "../../service/embedding_provider";
import { buildMalloyUri } from "../handler_utils";
import { logger } from "../../logger";
import { trySemanticSearch } from "./embedding_index";

/**
 * A retrievable model entity: a source, one of its views, a field (dimension or
 * measure) defined on a source, or a named query. Sources, views, and fields come
 * from the compiled SourceInfo (Model.getSourceInfos()); named queries from
 * Model.getQueries().
 */
interface Entity {
   id: string;
   kind: "source" | "view" | "query" | "dimension" | "measure";
   name: string;
   source: string | undefined;
   modelPath: string;
   doc: string;
}

/** One tier-4 result. `score` (cosine) rides only on semantic results. */
interface ResultEntity {
   kind: string;
   name: string;
   source: string | undefined;
   environmentName: string;
   packageName: string;
   modelPath: string;
   doc: string;
   score?: number;
}

const getContextShape = {
   environmentName: z
      .string()
      .optional()
      .describe("Environment name. Omit to list the available environments."),
   packageName: z
      .string()
      .optional()
      .describe(
         "Package name. Omit, with environmentName set, to list the packages in that environment.",
      ),
   query: z
      .string()
      .max(500)
      .optional()
      .describe(
         'Plain-English description of what you need, e.g. "revenue by product category". Omit, with environmentName and packageName set, to list the package\'s sources.',
      ),
   sourceName: z
      .string()
      .optional()
      .describe(
         "Optional. Narrow results to entities within this source (the drill-down phase).",
      ),
   limit: z
      .number()
      .int()
      .positive()
      .max(50)
      .optional()
      .describe(
         "Maximum results to return (max 50). Ranked retrieval defaults to 10; the listing tiers return everything unless you set this.",
      ),
};
type GetContextParams = z.infer<z.ZodObject<typeof getContextShape>>;

/**
 * Pull #(doc) text from annotation lines, falling back to the raw lines.
 * SourceInfo sources/fields carry Annotation objects ({ value }); named queries
 * carry raw strings, so accept both.
 */
export function docText(
   annotations?: Array<string | { value: string }>,
): string {
   if (!annotations || annotations.length === 0) return "";
   const lines = annotations.map((a) => (typeof a === "string" ? a : a.value));
   const docs = lines
      .map((a) => a.match(/#\(doc\)\s*(.*)/)?.[1]?.trim() ?? "")
      .filter(Boolean);
   const chosen = docs.length > 0 ? docs : lines;
   return chosen.join(" ").replace(/\s+/g, " ").trim();
}

export function sanitize(query: string): string {
   return query.replace(/[~^:*+\-"]/g, " ").trim();
}

/**
 * Walk every model in the package and collect sources, their views and
 * dimension/measure fields, and named queries. Returns the full set; the
 * optional source-level drill-down is applied by the caller after retrieval.
 */
async function collectEntities(pkg: Package): Promise<Entity[]> {
   // listModels() already returns only .malloy model files (notebooks are listed separately).
   const models = await pkg.listModels();

   const entities: Entity[] = [];
   let n = 0;
   for (const apiModel of models) {
      // path is optional in the generated API types; skip models without one.
      const modelPath = apiModel.path;
      if (!modelPath) continue;
      const model = pkg.getModel(modelPath);
      if (!model) continue;
      // SourceInfo carries the full schema (views plus dimension/measure fields);
      // named queries come from getQueries().
      const sourceInfos = model.getSourceInfos() ?? [];
      const queries = model.getQueries() ?? [];

      for (const sourceInfo of sourceInfos) {
         const sourceName = sourceInfo.name;
         entities.push({
            id: String(n++),
            kind: "source",
            name: sourceName,
            source: sourceName,
            modelPath,
            doc: docText(sourceInfo.annotations),
         });
         for (const field of sourceInfo.schema.fields ?? []) {
            // v1 indexes the queryable surface: views and dimension/measure
            // fields. Joins (structural) and calculate (window) are skipped.
            if (
               field.kind !== "view" &&
               field.kind !== "dimension" &&
               field.kind !== "measure"
            ) {
               continue;
            }
            entities.push({
               id: String(n++),
               kind: field.kind,
               name: field.name,
               source: sourceName,
               modelPath,
               doc: docText(field.annotations),
            });
         }
      }

      for (const query of queries) {
         if (!query.name) continue;
         entities.push({
            id: String(n++),
            kind: "query",
            name: query.name,
            source: query.sourceName,
            modelPath,
            doc: docText(query.annotations),
         });
      }
   }

   // A package can re-export the same source from more than one model (e.g. a
   // model that extends another), which surfaces the same entity twice. Keep the
   // first occurrence per (kind, source, name).
   const seen = new Set<string>();
   return entities.filter((e) => {
      const key = `${e.kind}|${e.source ?? ""}|${e.name}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
}

interface PackageIndex {
   pkg: Package;
   byId: Map<string, Entity>;
   index: lunr.Index;
   entityCount: number;
}

// Cache the built entity index per Package instance. environment.getPackage()
// serves a cached Package, and a reload swaps in a new instance, so a stale entry
// is dropped automatically (WeakMap) and the next call rebuilds.
const indexCache = new WeakMap<Package, PackageIndex>();

/** Get, or lazily build and cache, the lunr entity index for a package. */
async function getPackageIndex(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
): Promise<PackageIndex> {
   const environment = await environmentStore.getEnvironment(
      environmentName,
      false,
   );
   const pkg = await environment.getPackage(packageName, false);
   const cached = indexCache.get(pkg);
   if (cached) return cached;

   const entities = await collectEntities(pkg);
   const byId = new Map(entities.map((e) => [e.id, e]));
   const index = lunr(function () {
      this.ref("id");
      this.field("name", { boost: 4 });
      this.field("source");
      this.field("doc");
      for (const e of entities) {
         this.add({
            id: e.id,
            name: e.name,
            source: e.source ?? "",
            doc: e.doc,
         });
      }
   });
   const built: PackageIndex = {
      pkg,
      byId,
      index,
      entityCount: entities.length,
   };
   indexCache.set(pkg, built);
   logger.debug("[MCP Tool getContext] Built and cached entity index", {
      packageName,
      entityCount: entities.length,
   });
   return built;
}

const GET_CONTEXT_DESCRIPTION = `Discover what a Publisher deployment exposes and retrieve the model entities most relevant to a plain-English question, so you can ground a query in what the model actually defines instead of guessing. This is the starting point when you do not yet know the environment, package, or model names.

## Progressive discovery
Call it with as much as you know and omit the rest; it answers at the appropriate level:
- No arguments: lists the available environments, each with its package names.
- environmentName only: lists the packages in that environment, with descriptions.
- environmentName + packageName: lists that package's sources.
- environmentName + packageName + query: returns the sources, views, named queries, and dimension/measure fields most relevant to the question.

## Parameters
- environmentName (optional): omit to list environments.
- packageName (optional): omit, with environmentName set, to list packages.
- query (optional): a plain-English description of what you need; omit, with environmentName and packageName set, to list the package's sources.
- sourceName (optional): narrow retrieval to entities within one source (the drill-down phase).
- limit (optional): cap the number of results (max 50). Retrieval defaults to 10; the listing tiers return all unless set.

## Response
A JSON object with a results array whose items carry a kind field. For retrieval, each entity has kind (source / view / query / dimension / measure), name, source, modelPath, and doc; environmentName, packageName, modelPath, and source map directly onto malloy_executeQuery parameters, and for a view or named query you pass its name as queryName with sourceName. When the server is configured with an embedding provider, retrieval is ranked by semantic similarity: the payload then carries a retrieval field ("semantic", or "lexical" when the provider is unavailable) and each semantic entity a score.

## Contract rules
- Use the names verbatim; do not invent environments, packages, or entities not in the results.
- Start broad and narrow down: list environments, then packages, then sources, then query.

## Worked example
{ "environmentName": "examples", "packageName": "storefront", "query": "revenue by product category" }`;

/**
 * Wrap a JSON payload in the MCP resource-content shape every tier of this tool
 * returns. isError marks a tool-level error (e.g. an unknown environment/package).
 */
function jsonResource(uri: string, payload: unknown, isError = false) {
   const content = [
      {
         type: "resource" as const,
         resource: {
            type: "application/json",
            uri,
            text: JSON.stringify(payload),
         },
      },
   ];
   return isError ? { isError: true, content } : { content };
}

/**
 * Registers the malloy_getContext MCP tool. It is a progressive-discovery tool:
 * with no environment it lists environments, with an environment but no package
 * it lists packages, with a package but no query it lists the package's sources,
 * and with a query it runs lexical (lunr/BM25) retrieval over the package's model
 * entities (sources, views, dimension/measure fields, named queries). The entity
 * index is built once per Package and cached (see getPackageIndex), rebuilding
 * automatically when the package reloads.
 */
export function registerGetContextTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool(
      "malloy_getContext",
      GET_CONTEXT_DESCRIPTION,
      getContextShape,
      async (params: GetContextParams) => {
         const { environmentName, packageName, query, sourceName, limit } =
            params;
         const max = limit ?? 10;
         logger.info("[MCP Tool getContext] Retrieving context", {
            environmentName,
            packageName,
            query,
            sourceName,
            limit,
         });

         // Tier 1: no environment -> enumerate the available environments, each
         // with its package names, so an agent with no prior knowledge can start.
         if (!environmentName) {
            try {
               const environments = await environmentStore.listEnvironments();
               const results = environments.map((env) => ({
                  kind: "environment" as const,
                  name: env.name,
                  packages: (env.packages ?? [])
                     .map((p) => p.name)
                     .filter((n): n is string => Boolean(n)),
               }));
               return jsonResource(buildMalloyUri({}, "get-context"), {
                  results,
               });
            } catch (error) {
               const message =
                  error instanceof Error ? error.message : "Unknown error";
               logger.warn(
                  "[MCP Tool getContext] listing environments failed",
                  { error: message },
               );
               return jsonResource(
                  buildMalloyUri({}, "get-context"),
                  { error: message, results: [] },
                  true,
               );
            }
         }

         // Tier 2: environment but no package -> enumerate its packages.
         if (!packageName) {
            try {
               const environment = await environmentStore.getEnvironment(
                  environmentName,
                  false,
               );
               const packages = await environment.listPackages();
               const results = packages.map((pkg) => ({
                  kind: "package" as const,
                  name: pkg.name,
                  description: pkg.description,
                  environmentName,
               }));
               return jsonResource(
                  buildMalloyUri(
                     { environment: environmentName },
                     "get-context",
                  ),
                  { results },
               );
            } catch (error) {
               const message =
                  error instanceof Error ? error.message : "Unknown error";
               logger.warn("[MCP Tool getContext] listing packages failed", {
                  environmentName,
                  error: message,
               });
               return jsonResource(
                  buildMalloyUri(
                     { environment: environmentName },
                     "get-context",
                  ),
                  { error: message, results: [] },
                  true,
               );
            }
         }

         // Tiers 3 and 4 need the package's entity index.
         let pkgIndex: PackageIndex;
         try {
            pkgIndex = await getPackageIndex(
               environmentStore,
               environmentName,
               packageName,
            );
         } catch (error) {
            const message =
               error instanceof Error ? error.message : "Unknown error";
            logger.warn("[MCP Tool getContext] index build failed", {
               environmentName,
               packageName,
               sourceName,
               error: message,
            });
            return jsonResource(
               buildMalloyUri(
                  { environment: environmentName, package: packageName },
                  "get-context",
               ),
               { error: message, results: [] },
               true,
            );
         }

         const { byId, index } = pkgIndex;
         const uri = buildMalloyUri(
            { environment: environmentName, package: packageName },
            "get-context",
         );

         // Tier 3: package but no query -> list the package's sources as an
         // overview the agent can then query or drill into.
         const sanitized = query ? sanitize(query) : "";
         if (!sanitized) {
            // Enumeration: return every source unless the caller sets an explicit
            // limit. slice(0, undefined) keeps the whole list, so discovery is
            // not silently capped the way ranked retrieval (tier 4) is.
            const results = Array.from(byId.values())
               .filter((e) => e.kind === "source")
               .filter((e) => !sourceName || e.source === sourceName)
               .slice(0, limit)
               .map((e) => ({
                  kind: e.kind,
                  name: e.name,
                  source: e.source,
                  environmentName,
                  packageName,
                  modelPath: e.modelPath,
                  doc: e.doc,
               }));
            return jsonResource(uri, { results });
         }

         // Tier 4: retrieval over the package's entities. With an
         // embedding provider configured, ranking is semantic (DuckDB
         // cosine over cached entity embeddings); otherwise, or whenever
         // the semantic path is unavailable (index still building,
         // provider down, oversized package), it is lexical lunr. The
         // `retrieval` marker and per-entity `score` appear ONLY when a
         // provider is configured, so the unconfigured payload stays
         // byte-identical to the lexical-only releases.
         const configured = embeddingConfigured();
         let semanticResults: ResultEntity[] | undefined;
         if (configured) {
            let provider: EmbeddingProvider | null = null;
            try {
               provider = getEmbeddingProvider();
            } catch (error) {
               logger.warn(
                  "[MCP Tool getContext] Embedding configuration invalid; using lexical ranking",
                  {
                     error:
                        error instanceof Error ? error.message : String(error),
                  },
               );
            }
            if (provider) {
               try {
                  // The raw query embeds better than the lunr-sanitized
                  // one; sanitize() only exists to strip lunr operators.
                  const semantic = await trySemanticSearch({
                     db: environmentStore.storageManager.getDuckDbConnection(),
                     provider,
                     pkg: pkgIndex.pkg,
                     environmentName,
                     packageName,
                     entities: Array.from(byId.values()),
                     query: query ?? sanitized,
                     limit: max,
                     // "" means no drill-down, matching the lexical
                     // path's truthiness filter.
                     sourceName: sourceName || undefined,
                  });
                  if ("hits" in semantic) {
                     const byKey = new Map(
                        Array.from(byId.values()).map((e) => [
                           `${e.kind}|${e.source ?? ""}|${e.name}`,
                           e,
                        ]),
                     );
                     // Rows are only a vector cache: modelPath and doc
                     // come from the live entity, and a hit with no live
                     // entity (deleted since the last sync) is dropped.
                     semanticResults = semantic.hits.flatMap((hit) => {
                        const e = byKey.get(
                           `${hit.kind}|${hit.source ?? ""}|${hit.name}`,
                        );
                        if (!e) return [];
                        return [
                           {
                              kind: e.kind,
                              name: e.name,
                              source: e.source,
                              environmentName,
                              packageName,
                              modelPath: e.modelPath,
                              doc: e.doc,
                              score: Math.round(hit.score * 10_000) / 10_000,
                           },
                        ];
                     });
                  }
               } catch (error) {
                  // Defensive: trySemanticSearch does not throw, but the
                  // storage handle lookup can (e.g. before initialization
                  // or under a partial test double). Semantic retrieval
                  // must never take tier 4 down with it.
                  logger.warn(
                     "[MCP Tool getContext] Semantic retrieval unavailable; using lexical ranking",
                     {
                        error:
                           error instanceof Error
                              ? error.message
                              : String(error),
                     },
                  );
               }
            }
         }

         if (semanticResults !== undefined) {
            return jsonResource(uri, {
               retrieval: "semantic",
               results: semanticResults,
            });
         }

         let hits: lunr.Index.Result[] = [];
         try {
            hits = index.search(sanitized);
         } catch (error) {
            logger.warn("[MCP Tool getContext] lunr search failed", {
               query,
               error: error instanceof Error ? error.message : String(error),
            });
            hits = [];
         }

         // Defensive: skip any hit whose ref is missing from the entity map.
         const results = hits
            .map((hit) => byId.get(hit.ref))
            .filter((e): e is Entity => e !== undefined)
            // Drill-down: narrow to one source when sourceName is set.
            .filter((e) => !sourceName || e.source === sourceName)
            .slice(0, max)
            .map((e) => ({
               kind: e.kind,
               name: e.name,
               source: e.source,
               environmentName,
               packageName,
               modelPath: e.modelPath,
               doc: e.doc,
            }));

         return jsonResource(
            uri,
            configured ? { retrieval: "lexical", results } : { results },
         );
      },
   );
}
