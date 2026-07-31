// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import { logger } from "../../logger";
import { entityRowKey, trySemanticSearch } from "./embedding_index";

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
   // Human-facing doc for the response (may fall back to raw annotations).
   doc: string;
   // #(doc)-only text used as embedding input; never carries predicate
   // annotations (#(authorize) etc.) that must not leave the machine.
   embedDoc: string;
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
/**
 * Extract ONLY `#(doc)` annotation text, empty when there is none. This is
 * the safe input for embedding: unlike docText it never falls back to the
 * raw annotation lines, so predicate-bearing annotations (`#(authorize)`
 * row-level-security rules, tenant lists, `#(malloy)` internals) are never
 * sent to an external embedding provider.
 */
export function docOnlyText(
   annotations?: Array<string | { value: string }>,
): string {
   if (!annotations || annotations.length === 0) return "";
   const docs = annotations
      .map((a) => (typeof a === "string" ? a : a.value))
      .map((a) => a.match(/#\(doc\)\s*(.*)/)?.[1]?.trim() ?? "")
      .filter(Boolean);
   return docs.join(" ").replace(/\s+/g, " ").trim();
}

/**
 * Human-facing doc text for the response `doc` field. Prefers `#(doc)`
 * text and falls back to the raw annotation lines when there is none.
 * Pre-existing lexical behaviour; NOT used as embedding input (see
 * docOnlyText and Entity.embedDoc).
 */
export function docText(
   annotations?: Array<string | { value: string }>,
): string {
   const doc = docOnlyText(annotations);
   if (doc) return doc;
   if (!annotations || annotations.length === 0) return "";
   const lines = annotations.map((a) => (typeof a === "string" ? a : a.value));
   return lines.join(" ").replace(/\s+/g, " ").trim();
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
            embedDoc: docOnlyText(sourceInfo.annotations),
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
               embedDoc: docOnlyText(field.annotations),
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
            embedDoc: docOnlyText(query.annotations),
         });
      }
   }

   // A package can re-export the same source from more than one model (e.g. a
   // model that extends another), which surfaces the same entity twice. Keep the
   // first occurrence per (kind, source, name).
   const seen = new Set<string>();
   return entities.filter((e) => {
      const key = entityRowKey(e.kind, e.source ?? "", e.name);
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

const GET_CONTEXT_DESCRIPTION = `Discover what a Publisher deployment exposes and retrieve the model entities most relevant to a plain-English question, so you ground a query in what the model defines, not a guess. Start here when you do not know the environment, package, or model names.

## Contract rules
- Use the names it returns verbatim; never invent an environment, package, or entity that is not in the results.
- Start broad and narrow down: environments, then packages, then sources, then a query.
- An error, stale, or note field means the data did not load or predates the files: read it before trusting a number.

## Parameters
All optional; supply what you know. Each combination answers at its own level.
- none: lists the environments, each with its package names.
- environmentName: lists that environment's packages, with descriptions.
- + packageName: lists that package's sources.
- + query: a plain-English description of what you need; returns the most relevant sources, views, named queries, and dimension/measure fields.
- sourceName: narrows to one source. Without a query it lists that source, then its views, dimensions, measures and named queries — how you see a source's fields; with a query it restricts retrieval to that source. An unmatched name returns an empty results array, not an error.
- limit: caps results (max 50). Retrieval defaults to 10; the listing levels return all unless set. A drill-down's source row counts toward it.

## Response
A JSON object with a results array. Each entity has kind (source / view / query / dimension / measure), name, source, modelPath, and doc; environmentName, packageName, modelPath, and source map onto malloy_executeQuery parameters; pass a view or named query as queryName with sourceName. With an embedding provider configured, retrieval is ranked semantically: the payload carries a retrieval field ("semantic", or "lexical" if the provider is down) plus a per-entity score. With no provider both are absent rather than defaulted — not an error.

## Worked example
{ "environmentName": "examples", "packageName": "storefront", "query": "revenue by product category" }`;

/**
 * Every tier of this tool answers with `results`, so an error keeps that key
 * (empty) alongside `error`. Callers can read `results` unconditionally without
 * branching on success first.
 *
 * Routed through classifyToolError for the same reason its three sibling tools
 * are: it homes each error class to real remediation, so an unknown package
 * says so instead of arriving as a bare message with no suggestions. It also
 * replaces a per-site `error instanceof Error ? error.message : "Unknown
 * error"`, which was the one path in this file that could produce exactly the
 * unhelpful string this tool's callers reported.
 */
function contextError(uri: string, identifier: string, error: unknown) {
   return jsonToolError(
      uri,
      classifyToolError("getContext", identifier, error),
      {
         results: [],
      },
   );
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
               logger.warn(
                  "[MCP Tool getContext] listing environments failed",
                  {
                     error:
                        error instanceof Error ? error.message : String(error),
                  },
               );
               return contextError(
                  buildMalloyUri({}, "get-context"),
                  "environments",
                  error,
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
               // A stale package is SERVING, so it is in the listing above and
               // looks healthy there. Marking it here is the point: an agent
               // that reads a normal-looking listing and queries it gets
               // confident numbers from the model compiled BEFORE the last
               // save. `error` carries why the reload failed, the same field
               // the failed-load entries below use, and `stale: true` is what
               // separates "still answering, from an older model" from "not
               // there at all".
               const staleErrors = environment.getStaleCompileErrors();
               const results: Array<{
                  kind: "package";
                  name: string | undefined;
                  description?: string;
                  environmentName: string;
                  error?: string;
                  stale?: boolean;
               }> = packages.map((pkg) => {
                  const stale = pkg.name
                     ? staleErrors.get(pkg.name)
                     : undefined;
                  return {
                     kind: "package" as const,
                     name: pkg.name,
                     description: pkg.description,
                     environmentName,
                     // Spread so a current package's entry stays byte-identical
                     // to what it was before staleness was reported at all.
                     ...(stale && { error: stale.message, stale: true }),
                  };
               });
               // listPackages() omits packages that failed to load, which
               // reads as "does not exist" to an agent. List them with their
               // load error instead, so a broken package is distinguishable
               // from an absent one. (Messages are already secret-redacted
               // where they are recorded.)
               for (const [name, message] of environment.getFailedPackages()) {
                  results.push({
                     kind: "package" as const,
                     name,
                     environmentName,
                     error: message,
                  });
               }
               return jsonResource(
                  buildMalloyUri(
                     { environment: environmentName },
                     "get-context",
                  ),
                  { results },
               );
            } catch (error) {
               logger.warn("[MCP Tool getContext] listing packages failed", {
                  environmentName,
                  error: error instanceof Error ? error.message : String(error),
               });
               return contextError(
                  buildMalloyUri(
                     { environment: environmentName },
                     "get-context",
                  ),
                  environmentName,
                  error,
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
            logger.warn("[MCP Tool getContext] index build failed", {
               environmentName,
               packageName,
               sourceName,
               error: error instanceof Error ? error.message : String(error),
            });
            return contextError(
               buildMalloyUri(
                  { environment: environmentName, package: packageName },
                  "get-context",
               ),
               `${environmentName}/${packageName}`,
               error,
            );
         }

         const { byId, index } = pkgIndex;
         const uri = buildMalloyUri(
            { environment: environmentName, package: packageName },
            "get-context",
         );

         // A stale package answers every tier below exactly like a current one:
         // the index is the last model that compiled, so the names are real and
         // the queries succeed, and the numbers are from before the last save.
         // Nothing else in this payload can say so, and telling the agent to go
         // call malloy_getStatus is weaker than saying it here, where it is
         // already looking. Attached to tiers 3 and 4 alike, because tier 4 is
         // the path that goes straight from a question to field names to a
         // query.
         //
         // Best effort: this is a health annotation, so a lookup that fails
         // must not take discovery down with it. Logged, never thrown.
         let staleNote: string | undefined;
         try {
            const environment = await environmentStore.getEnvironment(
               environmentName,
               false,
            );
            const stale = environment.getStaleCompileErrors().get(packageName);
            if (stale) {
               staleNote = `This package is STALE: its most recent reload failed to compile at ${stale.failedAt}, so these names, and any query you run against them, come from the model compiled BEFORE that save, not from the files on disk. Fix the model and call malloy_reloadPackage; malloy_getStatus has the compile error.`;
            }
         } catch (error) {
            logger.debug("[MCP Tool getContext] staleness lookup failed", {
               environmentName,
               packageName,
               error: error instanceof Error ? error.message : String(error),
            });
         }
         /**
          * Spread into a payload to attach `note`. Returns {} when there is
          * nothing to say, so a healthy package's payload stays byte-identical
          * to what it was before notes existed.
          */
         const noteFor = (extra?: string) => {
            const note = [staleNote, extra].filter(Boolean).join(" ");
            return note ? { note } : {};
         };

         // Tier 3: package but no query -> list the package's sources as an
         // overview the agent can then query or drill into.
         const sanitized = query ? sanitize(query) : "";
         if (!sanitized) {
            // With sourceName set this is the drill-down, so it lists every
            // entity the named source offers: the source row itself, then its
            // views, dimensions, measures, and any named query built on it.
            // Filtering to kind === "source" here as well returned exactly one
            // row — the source — which the caller already had from the tier-3
            // listing, and never the fields the tool description promises.
            // collectEntities pushes a source ahead of its own fields, and
            // Array.from preserves that insertion order, so the source's doc
            // still leads the drill-down.
            //
            // Enumeration: return everything unless the caller sets an explicit
            // limit. slice(0, undefined) keeps the whole list, so discovery is
            // not silently capped the way ranked retrieval (tier 4) is.
            const results = Array.from(byId.values())
               .filter((e) =>
                  sourceName ? e.source === sourceName : e.kind === "source",
               )
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
            // An empty enumeration is ambiguous to an agent: "no data here" and
            // "the package exposes nothing" look identical. The package DID
            // load (a failed load throws out of getPackageIndex above), so an
            // empty result means its models expose no sources: a curation gap
            // (explores/export {}), not an empty database. Say so, only in the
            // empty case, so the populated payload stays byte-identical.
            if (results.length === 0 && !sourceName) {
               return jsonResource(uri, {
                  results,
                  ...noteFor(
                     "This package loaded but exposes no sources. That is a curation gap, not an empty database: check the package's explores list and export {} statements, and call malloy_getStatus for load errors and stale packages.",
                  ),
               });
            }
            return jsonResource(uri, { results, ...noteFor() });
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
                           entityRowKey(e.kind, e.source ?? "", e.name),
                           e,
                        ]),
                     );
                     // Rows are only a vector cache: modelPath and doc
                     // come from the live entity, and a hit with no live
                     // entity (deleted since the last sync) is dropped.
                     semanticResults = semantic.hits.flatMap((hit) => {
                        const e = byKey.get(
                           entityRowKey(hit.kind, hit.source ?? "", hit.name),
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
               ...noteFor(),
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
            configured
               ? { retrieval: "lexical", results, ...noteFor() }
               : { results, ...noteFor() },
         );
      },
   );
}
