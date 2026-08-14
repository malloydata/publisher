import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import lunr from "lunr";
import type { Relationship } from "@malloydata/malloy-interfaces";
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
import {
   entityRowKey,
   humanizeName,
   trySemanticSearch,
} from "./embedding_index";

/**
 * A retrievable model entity: a source, one of its views, a field (dimension or
 * measure) defined on a source, a join it declares, or a named query. Sources,
 * views, fields, and joins come from the compiled SourceInfo
 * (Model.getSourceInfos()); named queries from Model.getQueries().
 */
interface Entity {
   id: string;
   kind: "source" | "view" | "query" | "dimension" | "measure" | "join";
   name: string;
   source: string | undefined;
   modelPath: string;
   // Human-facing doc for the response (may fall back to raw annotations).
   doc: string;
   // #(doc)-only text used as embedding input; never carries predicate
   // annotations (#(authorize) etc.) that must not leave the machine.
   embedDoc: string;
   // Join cardinality, on `kind: "join"` entities only. Tells an agent whether
   // traversing the join fans out (many) before it writes a query against it.
   relationship?: Relationship;
   // Other spellings of this same field in the same source that were
   // collapsed into it (see collapseAliases). Present only when non-empty.
   aliases?: string[];
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
   relationship?: Relationship;
   /** Other spellings of this field in its own source, collapsed into it. */
   aliases?: string[];
   /** Other sources carrying this same concept, when near-identically scored. */
   alsoIn?: string[];
   score?: number;
}

/**
 * Caps on doc text carried as CONTEXT rather than as a result. A source's own
 * doc arrives in full when the source is itself a hit; these caps apply only
 * to the copy that rides along with a field hit, where the point is to deliver
 * the grain caveat, not to reproduce the model file.
 */
export const SOURCE_DOC_MAX_CHARS = 500;
export const JOIN_DOC_MAX_CHARS = 200;

/** A join as reported in source context: enough to write the traversal. */
interface SourceContextJoin {
   name: string;
   relationship: Relationship;
   doc?: string;
}

/**
 * The parent-source context for one source represented in `results`.
 *
 * A field hit alone tells an agent nothing about the grain, population rule,
 * or reporting convention its source carries, because a source's `#(doc)` only
 * reached the agent when the source itself independently cleared the relevance
 * floor for that query. That made whether guidance arrived a function of query
 * phrasing. Every source behind a result now reports its doc and its complete
 * join list exactly once per response.
 */
interface SourceContextEntry {
   name: string;
   modelPath: string;
   /** Truncated to SOURCE_DOC_MAX_CHARS; the model file has the full text. */
   doc: string;
   /**
    * Every join the source declares, not just retrieved ones. An empty array
    * is therefore an authoritative "this source declares no joins", which is
    * what an agent needs to stop probing for one and write it inline.
    */
   joins: SourceContextJoin[];
}

/**
 * How close two sibling scores must be before they are treated as the same
 * concept found in parallel sources rather than as two ranked answers.
 * Tuned against get_context_eval.ts; deliberately tight, so a sibling that
 * is genuinely a worse match keeps its own row.
 */
export const SIBLING_SCORE_EPSILON = 0.03;

/**
 * Ceiling on what the semantic query may fetch, matching the `limit`
 * parameter's own maximum. Sibling collapsing over-fetches to refill the
 * window, and this bounds the scan it can ask for.
 */
const SEMANTIC_MAX_LIMIT = 50;

/**
 * Collapse the same concept appearing in parallel sources into one row that
 * names the others.
 *
 * A model with sibling source families returns the same field from each of
 * them at effectively the same score: `"site of the building"` returned
 * `SITE` at 0.96 from all three of `fac_building`, `fclt_building` and
 * `fclt_building_hist` — identical scores, presented as peers, with nothing
 * in the response to tell an agent they were near-duplicates or how to
 * choose. Agents picked one arbitrarily, and choosing wrong between sibling
 * families was the single largest failure class measured.
 *
 * Collapsing does double duty: it returns the wasted slots to genuinely
 * different concepts, and `alsoIn` makes the ambiguity explicit instead of
 * leaving it to be inferred from three rows that look independent. Only
 * near-identical scores group — a sibling that really is a worse match is a
 * ranked answer, not a duplicate.
 */
function groupSiblings(results: ResultEntity[], limit: number): ResultEntity[] {
   const keptByConcept = new Map<string, ResultEntity>();
   const kept: ResultEntity[] = [];
   for (const r of results) {
      const key = `${r.kind}|${humanizeName(r.name)}`;
      const peer = keptByConcept.get(key);
      if (
         peer &&
         r.source &&
         peer.source !== r.source &&
         Math.abs((peer.score ?? 0) - (r.score ?? 0)) <= SIBLING_SCORE_EPSILON
      ) {
         peer.alsoIn = [...(peer.alsoIn ?? []), r.source];
         continue;
      }
      // Keep scanning the over-fetch after the window is full: a later hit
      // can still be the sibling that makes an already-kept row's ambiguity
      // visible, and dropping it would report a lone confident answer where
      // the model actually offers three.
      if (kept.length >= limit) continue;
      keptByConcept.set(key, r);
      kept.push(r);
   }
   return kept;
}

/**
 * The source-context entries for a result set: one per distinct source behind
 * a result, in the order those sources first appear, so the most relevant
 * source's guidance reads first. Keyed on the sources present rather than on
 * "the first hit", so re-ranking never moves which entry carries what.
 */
function contextForResults(
   results: ResultEntity[],
   sourceContext: Map<string, SourceContextEntry>,
): SourceContextEntry[] {
   const seen = new Set<string>();
   const entries: SourceContextEntry[] = [];
   for (const r of results) {
      if (!r.source || seen.has(r.source)) continue;
      seen.add(r.source);
      const entry = sourceContext.get(r.source);
      if (entry) entries.push(entry);
   }
   return entries;
}

/** Cut over-long context text on a word boundary, marking that it was cut. */
function truncateDoc(doc: string, max: number): string {
   if (doc.length <= max) return doc;
   const cut = doc.slice(0, max);
   const lastSpace = cut.lastIndexOf(" ");
   return `${(lastSpace > max * 0.6 ? cut.slice(0, lastSpace) : cut).trimEnd()}…`;
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
 * Walk every model in the package and collect sources, their views,
 * dimension/measure fields and declared joins, and named queries. Returns the
 * full set; the optional source-level drill-down is applied by the caller
 * after retrieval.
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
            // Joins are indexed as entities in their own right: an agent that
            // cannot see a declared join concludes the model has none and
            // burns queries guessing one. The join's own #(doc) is a common
            // home for the rule that governs the relationship, so it has to
            // be retrievable. `calculate` (window) fields stay unindexed:
            // they are not referenceable outside the view that defines them.
            if (field.kind === "join") {
               entities.push({
                  id: String(n++),
                  kind: "join",
                  name: field.name,
                  // The source that DECLARES the join, so a drill-down on
                  // that source sees it. The stable JoinInfo inlines the
                  // target's schema without naming the target source, so
                  // there is no targetSource to report.
                  source: sourceName,
                  modelPath,
                  doc: docText(field.annotations),
                  embedDoc: docOnlyText(field.annotations),
                  relationship: field.relationship,
               });
               // Deliberately NOT recursing into field.schema: those fields
               // are the target source's own, already indexed under it.
               // Recursing would duplicate every joined field once per join.
               continue;
            }
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
   const deduped = entities.filter((e) => {
      const key = entityRowKey(e.kind, e.source ?? "", e.name);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
   return collapseAliases(deduped);
}

/**
 * Collapse entities within one source that differ only in the spelling of
 * the same name, keeping the documented one and recording the rest.
 *
 * A model that renames a physical column without hiding the original leaves
 * both in the schema — `SITE` and `site` are one column, indexed twice — and
 * both then compete for the same scarce result slots. Measured across 8
 * representative queries on a 42-source model, 34% of returned slots were a
 * concept the same result set already contained, and the worst case spent
 * six of eight slots on one concept, three of them a raw column sitting
 * beside its own alias.
 *
 * Detection is by humanized name, and it has to be: the stable Malloy
 * interface gives a dimension only `{name, type, annotations}`, with no
 * expression, so there is no way to prove `site` is a rename of `SITE`
 * rather than a derivation. The heuristic covers the measured case (a pure
 * case/separator respelling inside one source) and stops there. Two
 * genuinely distinct fields whose names humanize identically would collapse,
 * but they would also have embedded near-identically, so what is lost is a
 * near-duplicate rather than a distinct concept — and the dropped name is
 * still reported in `aliases`.
 *
 * The real fix belongs in the model: Malloy's `include { internal: ... }`
 * hides the raw column outright, and the indexer already honours it, because
 * the compiler drops non-public fields before this code ever sees them (see
 * the access-modifier spec). This collapse is what the tool can do for the
 * models that have not done that.
 */
function collapseAliases(entities: Entity[]): Entity[] {
   const groups = new Map<string, Entity[]>();
   for (const e of entities) {
      // Sources are their own namespace, and joins name a relationship
      // rather than a column; only fields within one source can be two
      // spellings of one thing.
      if (e.kind !== "dimension" && e.kind !== "measure") continue;
      const key = `${e.kind}|${e.source ?? ""}|${humanizeName(e.name)}`;
      const group = groups.get(key);
      if (group) group.push(e);
      else groups.set(key, [e]);
   }

   const dropped = new Map<string, Entity>();
   for (const group of groups.values()) {
      if (group.length < 2) continue;
      // Prefer the documented spelling: a modeller who wrote a #(doc) said
      // which name they meant an agent to use. Failing that, prefer the
      // lowercase-looking one (`site` over `SITE`), then be deterministic.
      const [keep, ...rest] = [...group].sort((a, b) => {
         if (Boolean(b.embedDoc) !== Boolean(a.embedDoc)) {
            return b.embedDoc ? 1 : -1;
         }
         const aRaw = a.name === a.name.toUpperCase();
         const bRaw = b.name === b.name.toUpperCase();
         if (aRaw !== bRaw) return aRaw ? 1 : -1;
         return a.name.localeCompare(b.name);
      });
      keep.aliases = rest.map((e) => e.name);
      for (const e of rest) {
         dropped.set(entityRowKey(e.kind, e.source ?? "", e.name), e);
      }
   }
   if (dropped.size === 0) return entities;
   return entities.filter(
      (e) => !dropped.has(entityRowKey(e.kind, e.source ?? "", e.name)),
   );
}

interface PackageIndex {
   pkg: Package;
   byId: Map<string, Entity>;
   index: lunr.Index;
   entityCount: number;
   /** Per-source context, keyed by source name. Built once with the index. */
   sourceContext: Map<string, SourceContextEntry>;
}

/**
 * Derive per-source context from the collected entities: the source's own doc
 * and every join declared on it. Built once per package alongside the index,
 * so attaching it to a response costs a lookup rather than a model walk.
 */
function buildSourceContext(
   entities: Entity[],
): Map<string, SourceContextEntry> {
   const context = new Map<string, SourceContextEntry>();
   for (const e of entities) {
      if (e.kind !== "source") continue;
      context.set(e.name, {
         name: e.name,
         modelPath: e.modelPath,
         doc: truncateDoc(e.doc, SOURCE_DOC_MAX_CHARS),
         joins: [],
      });
   }
   for (const e of entities) {
      if (e.kind !== "join" || !e.relationship) continue;
      // A join declared on a source the collector never emitted (defensive:
      // every join reaches us through its source) has nowhere to hang.
      const parent = e.source ? context.get(e.source) : undefined;
      if (!parent) continue;
      parent.joins.push({
         name: e.name,
         relationship: e.relationship,
         ...(e.doc ? { doc: truncateDoc(e.doc, JOIN_DOC_MAX_CHARS) } : {}),
      });
   }
   return context;
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
      sourceContext: buildSourceContext(entities),
   };
   indexCache.set(pkg, built);
   logger.debug("[MCP Tool getContext] Built and cached entity index", {
      packageName,
      entityCount: entities.length,
   });
   return built;
}

/**
 * Kept under the truncation budget pinned by server.protocol.spec.ts: a client
 * was observed cutting this description off mid-sentence, and a tail cut takes
 * whatever is last. So the contract rules an agent cannot self-correct come
 * first and the reference material last, and the reference stays terse to buy
 * room for it. Full prose belongs in docs/ai-agents.md, not here.
 */
const GET_CONTEXT_DESCRIPTION = `Discover what a Publisher deployment exposes and retrieve the model entities most relevant to a plain-English question, so you ground a query in names the model actually defines. Start here when you do not know the environment, package, or model names.

## Contract rules
- Use the names it returns verbatim; never invent an environment, package, or entity that is not in the results.
- Start broad and narrow down: environments, then packages, then sources, then a query.
- An error, stale, or note field means the data did not load or predates the files: read it before trusting a number.
- A source's joins list is complete: empty means it declares none, so write that relationship inline rather than probing for one.
- Read a source's doc before querying it: it carries grain and population rules its fields do not.

## Parameters
All optional; supply what you know.
- none: the environments and their package names.
- environmentName: that environment's packages.
- + packageName: that package's sources, each with its joins.
- + query: what you need, in plain English; returns the most relevant sources, views, named queries, joins and fields.
- sourceName: drill down into one source. Its own entity comes back only when relevant; its doc and joins always arrive in sources.
- limit: caps results (max 50; retrieval defaults to 10). Listing levels return all unless set.

## Response
results[]: kind (source/view/query/dimension/measure/join), name, source, modelPath, doc — these map onto malloy_executeQuery; pass a view or named query as queryName with sourceName. A join adds relationship ("one"/"many"/"cross"), traversed as joinName.fieldName. alsoIn names other sources holding the same concept at the same score; choose by their docs rather than taking the first.
sources[]: per source behind a result — its doc (may be truncated) and complete joins.
With an embedding provider, ranking is semantic and each entity carries a score.

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
 * entities (sources, views, dimension/measure fields, joins, named queries). The entity
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

         const { byId, index, sourceContext } = pkgIndex;
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
                  // The overview is where an agent decides how to combine
                  // sources, so each one states its relationships here rather
                  // than making that a second call. Empty means none declared.
                  joins: sourceContext.get(e.name)?.joins ?? [],
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
         // A drill-down is confined to one source, so no two hits can be the
         // same concept in parallel sources and there is nothing to collapse.
         const scoped = Boolean(sourceName);
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
                     // Over-fetch so sibling collapsing can refill the
                     // window with genuinely different concepts instead of
                     // returning fewer results than asked for. A drill-down
                     // is already confined to one source, so nothing there
                     // can collapse and the extra rows would be waste.
                     limit: scoped
                        ? max
                        : Math.min(SEMANTIC_MAX_LIMIT, max * 3),
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
                     const ranked = semantic.hits.flatMap((hit) => {
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
                              ...(e.relationship
                                 ? { relationship: e.relationship }
                                 : {}),
                              ...(e.aliases ? { aliases: e.aliases } : {}),
                              score: Math.round(hit.score * 10_000) / 10_000,
                           },
                        ];
                     });
                     semanticResults = scoped
                        ? ranked.slice(0, max)
                        : groupSiblings(ranked, max);
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
            const sources = contextForResults(semanticResults, sourceContext);
            return jsonResource(uri, {
               retrieval: "semantic",
               results: semanticResults,
               ...(sources.length > 0 ? { sources } : {}),
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
               ...(e.relationship ? { relationship: e.relationship } : {}),
               ...(e.aliases ? { aliases: e.aliases } : {}),
            }));

         const sources = contextForResults(results, sourceContext);
         const context = sources.length > 0 ? { sources } : {};
         return jsonResource(
            uri,
            configured
               ? { retrieval: "lexical", results, ...context, ...noteFor() }
               : { results, ...context, ...noteFor() },
         );
      },
   );
}
