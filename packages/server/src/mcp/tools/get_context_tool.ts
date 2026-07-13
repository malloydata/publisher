import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import lunr from "lunr";
import { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import { buildMalloyUri } from "../handler_utils";
import { logger } from "../../logger";

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

const getContextShape = {
   environmentName: z.string().describe("Environment name."),
   packageName: z.string().describe("Package name."),
   query: z
      .string()
      .max(500)
      .describe(
         'Plain-English description of what you need, e.g. "revenue by product category" or "customer churn".',
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
      .describe("Maximum number of entities to return. Default 10, max 50."),
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
   const built: PackageIndex = { byId, index, entityCount: entities.length };
   indexCache.set(pkg, built);
   logger.debug("[MCP Tool getContext] Built and cached entity index", {
      packageName,
      entityCount: entities.length,
   });
   return built;
}

const GET_CONTEXT_DESCRIPTION = `Retrieve the model entities (sources, views, named queries, and dimension/measure fields) most relevant to a plain-English question, so you can ground a query in what the model actually defines instead of guessing.

## When to use
- Before writing a query, to find which source and which prebuilt views fit the question.
- Two-phase: call once with just a query to discover the most relevant sources and views (discovery), then optionally call again with sourceName set to focus on one source (drill-down).

## Parameters
- environmentName, packageName (required): where to look.
- query (required): a plain-English description of what you need.
- sourceName (optional): narrow to entities within one source.
- limit (optional): maximum entities to return; default 10.

## Response
A JSON object with results: a ranked list of entities, each with kind (source / view / query / dimension / measure), name, source, modelPath, and doc. The environmentName, packageName, modelPath, and source map directly onto malloy_executeQuery parameters; for a view or named query, pass its name as queryName with sourceName.

## Contract rules
- Use the names verbatim; do not invent entities not in the results.
- Results include field-level dimensions and measures. If you need the full field list of a source or want to confirm exact spelling, call malloy_getContext again with sourceName set to drill into that source.

## Worked example
{ "environmentName": "samples", "packageName": "ecommerce", "query": "revenue by product category" }`;

/**
 * Registers the malloy_getContext MCP tool: lexical (lunr/BM25) retrieval over a
 * package's model entities (sources, views, dimension/measure fields, named
 * queries). The entity index is built once per Package and cached (see
 * getPackageIndex), rebuilding automatically when the package reloads.
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
            return {
               isError: true,
               content: [
                  {
                     type: "resource" as const,
                     resource: {
                        type: "application/json",
                        uri: buildMalloyUri(
                           {
                              environment: environmentName,
                              package: packageName,
                           },
                           "get-context",
                        ),
                        text: JSON.stringify({ error: message, results: [] }),
                     },
                  },
               ],
            };
         }

         const { byId, index } = pkgIndex;

         const sanitized = sanitize(query);
         let hits: lunr.Index.Result[] = [];
         if (sanitized) {
            try {
               hits = index.search(sanitized);
            } catch (error) {
               logger.warn("[MCP Tool getContext] lunr search failed", {
                  query,
                  error: error instanceof Error ? error.message : String(error),
               });
               hits = [];
            }
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

         return {
            content: [
               {
                  type: "resource" as const,
                  resource: {
                     type: "application/json",
                     uri: buildMalloyUri(
                        { environment: environmentName, package: packageName },
                        "get-context",
                     ),
                     text: JSON.stringify({ results }),
                  },
               },
            ],
         };
      },
   );
}
