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
import { getDimensionValueIndexMode, getMcpTraceMode } from "../../config";
import { McpTraceStore } from "../../service/mcp_trace_store";
import {
   compactRankedSummary,
   RETRIEVAL_VERSION,
   TYPED_RETRIEVAL_VERSION,
   retrievalConfigHash,
   type RetrievalEvidence,
} from "../../service/retrieval_evidence";
import {
   collectSourceMeta,
   ensureDimensionValueIndex,
   searchDimensionValues,
} from "../../service/dimension_value_index";
import {
   buildSourceCards,
   buildTypedEnvelope,
   isProminenceListing,
   isTypedRequest,
   kindsForTarget,
   resolveScope,
   searchTargetSchema,
   scopeSchema,
   toTypedEntity,
   validateTypedCall,
   type SearchTarget,
   type TypedEntity,
} from "./get_context_typed";
import {
   entityRowKey,
   getEmbeddingIndexStatus,
   humanizeName,
   trySemanticSearch,
   type EmbeddingIndexStatus,
   type SemanticUnavailableReason,
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
 * Why a server that HAS an embedding provider answered lexically. Reported
 * so an agent can act on it: "indexing" is a cold index that clears on its
 * own within seconds and is worth one retry, while the rest are conditions
 * an immediate retry cannot fix.
 */
type RetrievalReason =
   | "indexing"
   | "cooldown"
   | "too-many-entities"
   | "provider-error"
   | "unavailable";

/**
 * The index's internal reasons, mapped to the wire vocabulary. "error" is
 * widened to "provider-error" because from the caller's side that is what it
 * is: the embedding endpoint failed, not this tool.
 */
const REASON_BY_UNAVAILABLE: Record<
   SemanticUnavailableReason,
   RetrievalReason
> = {
   indexing: "indexing",
   cooldown: "cooldown",
   "too-many-entities": "too-many-entities",
   error: "provider-error",
};

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
         'Legacy adapter. Plain-English description of what you need. Prefer search_targets. Omit, with environmentName and packageName set, to list the package\'s sources.',
      ),
   sourceName: z
      .string()
      .optional()
      .describe(
         "Optional. Narrow results to entities within this source (the drill-down phase). Prefer scopes[].source.",
      ),
   search_targets: z
      .array(searchTargetSchema)
      .optional()
      .describe(
         "Typed search targets. Each has target_type (source, dimension, measure, view, dimensional_value) and optional search_text (null lists by prominence). Do not mix source targets with other types.",
      ),
   scopes: z
      .array(scopeSchema)
      .optional()
      .describe(
         "Optional scopes. Each needs environment and package; model_path and source narrow a drill-down.",
      ),
   filter_params: z
      .record(z.union([z.string(), z.array(z.string())]))
      .nullable()
      .optional()
      .describe(
         "Filter values for sources that declare #(filter). Unused on the legacy path; protected sources are not value-indexed.",
      ),
   user_prompt: z
      .string()
      .max(2000)
      .nullable()
      .optional()
      .describe(
         "Optional. The user prompt that triggered this call, for traces only. Not used for ranking.",
      ),
   offset: z
      .number()
      .int()
      .min(0)
      .optional()
      .describe(
         "Source-listing page offset, copied from next_offset. Rejected with search_text or non-source targets.",
      ),
   limit: z
      .number()
      .int()
      .positive()
      .max(50)
      .optional()
      .describe(
         "Maximum results to return (max 50). Ranked retrieval defaults to 10; typed source listings default to 20; other listings return everything unless you set this.",
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
const GET_CONTEXT_DESCRIPTION = `Discover what a Publisher deployment exposes and retrieve the model entities most relevant to a question. Prefer typed search_targets; the older query string still works.

## Contract rules
- Use the names it returns verbatim; never invent an environment, package, or entity.
- Do not mix source targets with other types in the same call.
- Only combine entities from calls with identical scope (environment/package/model_path/source).
- A source's joins list is complete: empty means it declares none.
- Read a source's doc before querying it: it carries grain its fields do not.
- An error, stale, or note field means the data did not load or predates the files.
- Pass resource_id fields to malloy_executeQuery verbatim.

## Call modes
1. Listing: omit search_targets and query. none → environments; environmentName → packages; + packageName → sources with joins.
2. Phase 1: source targets (usually with environmentName, no package) to pick sources.
3. Phase 2: scoped dimension/measure/view/dimensional_value targets.

## Parameters
search_targets: [{target_type, search_text}]; null search_text lists by prominence.
scopes: [{environment, package, model_path, source}].
environmentName/packageName/query/sourceName: legacy adapter. sourceName still drills into one source.
limit (max 50). offset: source listings only.

## Response
Typed: ranking, total_available, returned, sources[] (resource_id, doc, joins, givens, authorize) plus per-target ranked entities (kind, name, source, modelPath, doc, alsoIn, relationship).
Legacy: results[] of the same entity shape.
belowCutoffCount / retrievalReason as today.

## Worked example
{"search_targets":[{"target_type":"source","search_text":"orders and revenue"}],"scopes":[{"environment":"examples","package":"storefront"}]}`;

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

async function packageScopedResource(
   environmentStore: EnvironmentStore,
   uri: string,
   payload: Record<string, unknown>,
   pkg: Package,
   request: {
      environmentName: string;
      packageName: string;
      query?: string;
      sourceName?: string;
      limit?: number;
      search_targets?: SearchTarget[];
      offset?: number;
   },
   extras?: {
      retrievalVersion?: string;
      dimensionalValues?: RetrievalEvidence["index"]["dimensionalValues"];
   },
) {
   let embeddingStatus: EmbeddingIndexStatus | undefined;
   if (embeddingConfigured()) {
      try {
         embeddingStatus = await getEmbeddingIndexStatus(
            environmentStore.storageManager.getDuckDbConnection(),
            request.environmentName,
            request.packageName,
            Array.from(
               (
                  await getPackageIndex(
                     environmentStore,
                     request.environmentName,
                     request.packageName,
                  )
               ).byId.keys(),
            ).length,
         );
      } catch {
         embeddingStatus = undefined;
      }
   }
   const servedRevision =
      typeof pkg.getServedRevision === "function"
         ? pkg.getServedRevision()
         : undefined;
   const sourceContentSha =
      typeof pkg.getSourceContentSha === "function"
         ? pkg.getSourceContentSha()
         : undefined;
   const evidence: RetrievalEvidence = {
      servedRevision,
      sourceContentSha,
      retrievalVersion: extras?.retrievalVersion ?? RETRIEVAL_VERSION,
      retrievalConfigHash: retrievalConfigHash({
         resultLimit:
            typeof request.limit === "number" ? request.limit : undefined,
         retrievalVersion: extras?.retrievalVersion,
         dimensionValueIndex: getDimensionValueIndexMode(),
      }),
      index: {
         lexical: "ready",
         semantic: embeddingConfigured()
            ? (embeddingStatus?.status ?? "indexing")
            : "unavailable",
         ...(extras?.dimensionalValues
            ? { dimensionalValues: extras.dimensionalValues }
            : {}),
         generation: embeddingStatus
            ? Date.parse(embeddingStatus.lastSyncedAt ?? "") || undefined
            : undefined,
      },
   };
   const includeEvidence =
      typeof pkg.getServedRevision === "function" ||
      getMcpTraceMode() !== "off";
   const results = Array.isArray(payload.results)
      ? payload.results
      : Array.isArray(payload.targets)
        ? (payload.targets as Array<{ results?: unknown[] }>).flatMap(
             (target) => target.results ?? [],
          )
        : [];
   const rankedSummary = compactRankedSummary(results);
   let traceId: string | undefined;
   if (getMcpTraceMode() !== "off") {
      try {
         const db = environmentStore.storageManager.getDuckDbConnection();
         const store = new McpTraceStore(db);
         traceId = await store.record({
            toolName: "malloy_getContext",
            request,
            response: payload,
            rankedSummary,
            resultCount: rankedSummary.resultCount,
            environmentName: request.environmentName,
            packageName: request.packageName,
            retrievalConfigHash: evidence.retrievalConfigHash,
         });
      } catch (error) {
         logger.debug("[MCP Tool getContext] trace persist failed", {
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }
   return jsonResource(uri, {
      ...payload,
      ...(includeEvidence ? { evidence } : {}),
      ...(traceId ? { traceId } : {}),
   });
}

interface RankedRetrieval {
   results: ResultEntity[];
   belowCutoffCount: number;
   retrieval?: "semantic" | "lexical";
   retrievalReason?: RetrievalReason;
}

async function rankEntities(args: {
   environmentStore: EnvironmentStore;
   environmentName: string;
   packageName: string;
   pkgIndex: PackageIndex;
   query: string;
   sourceName?: string;
   kinds?: Set<string>;
   limit: number;
}): Promise<RankedRetrieval> {
   const { environmentStore, environmentName, packageName, pkgIndex } = args;
   const { byId, index } = pkgIndex;
   const kindOk = (kind: string) => !args.kinds || args.kinds.has(kind);
   const scoped = Boolean(args.sourceName);
   const configured = embeddingConfigured();
   let semanticResults: ResultEntity[] | undefined;
   let belowCutoffCount = 0;
   let retrievalReason: RetrievalReason | undefined;
   const pool = Array.from(byId.values()).filter(
      (e) =>
         kindOk(e.kind) &&
         (!args.sourceName || e.source === args.sourceName),
   );

   if (configured) {
      let provider: EmbeddingProvider | null = null;
      try {
         provider = getEmbeddingProvider();
      } catch (error) {
         retrievalReason = "unavailable";
         logger.warn(
            "[MCP Tool getContext] Embedding configuration invalid; using lexical ranking",
            {
               error: error instanceof Error ? error.message : String(error),
            },
         );
      }
      if (provider) {
         try {
            const semantic = await trySemanticSearch({
               db: environmentStore.storageManager.getDuckDbConnection(),
               provider,
               pkg: pkgIndex.pkg,
               environmentName,
               packageName,
               entities: pool,
               query: args.query,
               limit: scoped
                  ? args.limit
                  : Math.min(SEMANTIC_MAX_LIMIT, args.limit * 3),
               sourceName: args.sourceName || undefined,
            });
            if ("hits" in semantic) {
               const byKey = new Map(
                  pool.map((e) => [
                     entityRowKey(e.kind, e.source ?? "", e.name),
                     e,
                  ]),
               );
               const ranked = semantic.hits.flatMap((hit) => {
                  const e = byKey.get(
                     entityRowKey(hit.kind, hit.source ?? "", hit.name),
                  );
                  if (!e || !kindOk(e.kind)) return [];
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
                  ? ranked.slice(0, args.limit)
                  : groupSiblings(ranked, args.limit);
               belowCutoffCount = semantic.belowCutoffCount;
            } else {
               retrievalReason = REASON_BY_UNAVAILABLE[semantic.unavailable];
            }
         } catch (error) {
            retrievalReason = "unavailable";
            logger.warn(
               "[MCP Tool getContext] Semantic retrieval unavailable; using lexical ranking",
               {
                  error:
                     error instanceof Error ? error.message : String(error),
               },
            );
         }
      }
   }

   if (semanticResults !== undefined) {
      return {
         results: semanticResults,
         belowCutoffCount,
         retrieval: "semantic",
      };
   }

   const sanitized = sanitize(args.query);
   let hits: lunr.Index.Result[] = [];
   try {
      hits = index.search(sanitized);
   } catch (error) {
      logger.warn("[MCP Tool getContext] lunr search failed", {
         error: error instanceof Error ? error.message : String(error),
      });
      hits = [];
   }
   const results = hits
      .map((hit) => byId.get(hit.ref))
      .filter((e): e is Entity => e !== undefined)
      .filter((e) => kindOk(e.kind))
      .filter((e) => !args.sourceName || e.source === args.sourceName)
      .slice(0, args.limit)
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
   return {
      results,
      belowCutoffCount: 0,
      retrieval: configured ? "lexical" : undefined,
      ...(retrievalReason ? { retrievalReason } : {}),
   };
}

function listKind(
   pkgIndex: PackageIndex,
   kinds: Set<string>,
   environmentName: string,
   packageName: string,
   sourceName: string | undefined,
   modelPath: string | undefined,
): ResultEntity[] {
   return Array.from(pkgIndex.byId.values())
      .filter((e) => kinds.has(e.kind))
      .filter((e) => !sourceName || e.source === sourceName)
      .filter((e) => !modelPath || e.modelPath === modelPath)
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
}

async function typedSourceDiscovery(
   environmentStore: EnvironmentStore,
   environmentName: string,
   targets: SearchTarget[],
   limit: number,
   offset?: number,
) {
   const environment = await environmentStore.getEnvironment(
      environmentName,
      false,
   );
   const packages = await environment.listPackages();
   const sourceRows: ResultEntity[] = [];
   const mergedContext = new Map<
      string,
      {
         name: string;
         modelPath: string;
         doc: string;
         joins: Array<{
            name: string;
            relationship: Relationship;
            doc?: string;
         }>;
      }
   >();
   for (const pkg of packages) {
      if (!pkg.name) continue;
      let pkgIndex: PackageIndex;
      try {
         pkgIndex = await getPackageIndex(
            environmentStore,
            environmentName,
            pkg.name,
         );
      } catch {
         continue;
      }
      for (const [name, entry] of pkgIndex.sourceContext) {
         mergedContext.set(`${pkg.name}::${name}`, {
            ...entry,
            name,
         });
      }
      sourceRows.push(
         ...listKind(
            pkgIndex,
            new Set(["source"]),
            environmentName,
            pkg.name,
            undefined,
            undefined,
         ),
      );
   }

   const searchText = targets.find((t) => t.search_text)?.search_text;
   let ranked = sourceRows;
   let retrieval: "semantic" | "lexical" | undefined;
   let retrievalReason: RetrievalReason | undefined;
   let belowCutoffCount = 0;
   if (searchText) {
      const collected: ResultEntity[] = [];
      for (const pkg of packages) {
         if (!pkg.name) continue;
         let pkgIndex: PackageIndex;
         try {
            pkgIndex = await getPackageIndex(
               environmentStore,
               environmentName,
               pkg.name,
            );
         } catch {
            continue;
         }
         const one = await rankEntities({
            environmentStore,
            environmentName,
            packageName: pkg.name,
            pkgIndex,
            query: searchText,
            kinds: new Set(["source"]),
            limit,
         });
         collected.push(...one.results);
         if (one.retrieval === "semantic") retrieval = "semantic";
         else if (one.retrieval === "lexical" && retrieval !== "semantic") {
            retrieval = "lexical";
            retrievalReason = one.retrievalReason;
         }
         belowCutoffCount += one.belowCutoffCount;
      }
      ranked = collected
         .sort((a, b) => (b.score ?? 0) - (a.score ?? 0))
         .slice(0, limit);
   }

   const typedEntities = ranked.map((row, index) => toTypedEntity(row, index + 1));
   const byPackageContext = new Map<
      string,
      {
         name: string;
         modelPath: string;
         doc: string;
         joins: Array<{
            name: string;
            relationship: Relationship;
            doc?: string;
         }>;
      }
   >();
   for (const entity of typedEntities) {
      const key = entity.source ?? entity.name;
      const fromPkg = mergedContext.get(`${entity.packageName}::${key}`);
      if (fromPkg) byPackageContext.set(key, fromPkg);
   }
   const cards = typedEntities.map((entity) => {
      const context =
         byPackageContext.get(entity.source ?? entity.name) ??
         mergedContext.get(`${entity.packageName}::${entity.name}`);
      return {
         name: entity.name,
         modelPath: entity.modelPath,
         doc: context?.doc ?? entity.doc,
         joins: context?.joins ?? [],
         resource_id: {
            environment: environmentName,
            package: entity.packageName,
            model_path: entity.modelPath,
            source: entity.name,
         },
         ...(entity.score !== undefined ? { score: entity.score } : {}),
      };
   });
   const prominence = !searchText;
   const envelope = buildTypedEnvelope({
      ranking: prominence ? "prominence" : "relevance",
      sources: cards,
      targets: targets.map((target) => ({
         target_type: target.target_type,
         search_text: target.search_text ?? null,
         results: typedEntities,
      })),
      offset,
      limit,
      pageSources: prominence,
   });
   return jsonResource(
      buildMalloyUri({ environment: environmentName }, "get-context"),
      {
         ...envelope,
         ...(retrieval ? { retrieval } : {}),
         ...(retrievalReason ? { retrievalReason } : {}),
         ...(retrieval === "semantic" ? { belowCutoffCount } : {}),
      },
   );
}

async function typedPackageSearch(args: {
   environmentStore: EnvironmentStore;
   environmentName: string;
   packageName: string;
   pkgIndex: PackageIndex;
   targets: SearchTarget[];
   sourceName?: string;
   modelPath?: string;
   entityName?: string;
   limit: number;
   offset?: number;
   uri: string;
   noteFor: (extra?: string) => Record<string, string>;
}) {
   const {
      environmentStore,
      environmentName,
      packageName,
      pkgIndex,
      targets,
      sourceName,
      modelPath,
      entityName,
      limit,
      offset,
      uri,
      noteFor,
   } = args;
   const sourceMeta = await collectSourceMeta(pkgIndex.pkg);
   let dimensionalValuesStatus:
      | RetrievalEvidence["index"]["dimensionalValues"]
      | undefined;
   const targetResults: Array<{
      target_type: SearchTarget["target_type"];
      search_text: string | null;
      results: TypedEntity[];
   }> = [];
   const allEntities: TypedEntity[] = [];
   let retrieval: "semantic" | "lexical" | undefined;
   let retrievalReason: RetrievalReason | undefined;
   let belowCutoffCount = 0;
   const prominence = isProminenceListing(targets);

   for (const target of targets) {
      if (target.target_type === "dimensional_value") {
         let db;
         try {
            db = environmentStore.storageManager.getDuckDbConnection();
         } catch {
            db = undefined;
         }
         if (db && getDimensionValueIndexMode() !== "off") {
            const meta = await ensureDimensionValueIndex({
               db,
               pkg: pkgIndex.pkg,
               environmentName,
               packageName,
            });
            dimensionalValuesStatus = meta?.status ?? "unavailable";
            const hits = await searchDimensionValues({
               db,
               environmentName,
               packageName,
               searchText: target.search_text,
               sourceName,
               dimensionName: entityName,
               limit,
            });
            const typedHits = hits.map((hit) =>
               toTypedEntity(
                  {
                     kind: "dimensional_value",
                     name: hit.name,
                     source: hit.source,
                     environmentName,
                     packageName,
                     modelPath: hit.modelPath,
                     doc: "",
                     dimension: hit.dimension,
                  },
                  hit.rank,
               ),
            );
            targetResults.push({
               target_type: target.target_type,
               search_text: target.search_text ?? null,
               results: typedHits,
            });
            allEntities.push(...typedHits);
         } else {
            dimensionalValuesStatus = "off";
            targetResults.push({
               target_type: target.target_type,
               search_text: target.search_text ?? null,
               results: [],
            });
         }
         continue;
      }

      const kinds = new Set(kindsForTarget(target.target_type));
      const searchText = target.search_text?.trim();
      let rows: ResultEntity[];
      if (!searchText) {
         rows = listKind(
            pkgIndex,
            kinds,
            environmentName,
            packageName,
            sourceName,
            modelPath,
         ).slice(0, limit);
      } else {
         const ranked = await rankEntities({
            environmentStore,
            environmentName,
            packageName,
            pkgIndex,
            query: searchText,
            sourceName,
            kinds,
            limit,
         });
         rows = ranked.results;
         if (ranked.retrieval === "semantic") retrieval = "semantic";
         else if (ranked.retrieval === "lexical" && retrieval !== "semantic") {
            retrieval = "lexical";
            retrievalReason = ranked.retrievalReason;
         }
         belowCutoffCount += ranked.belowCutoffCount;
      }
      const typedRows = rows.map((row, index) => toTypedEntity(row, index + 1));
      targetResults.push({
         target_type: target.target_type,
         search_text: target.search_text ?? null,
         results: typedRows,
      });
      allEntities.push(...typedRows);
   }

   const cards = buildSourceCards({
      entities: allEntities,
      environmentName,
      packageName,
      sourceContext: pkgIndex.sourceContext,
      sourceMeta,
   });
   const envelope = buildTypedEnvelope({
      ranking: prominence ? "prominence" : "relevance",
      sources: cards,
      targets: targetResults,
      offset,
      limit,
      pageSources: prominence && targets.every((t) => t.target_type === "source"),
   });
   const extraNote =
      dimensionalValuesStatus === "off" &&
      targets.some((t) => t.target_type === "dimensional_value")
         ? "Dimensional values are not indexed on this server (PUBLISHER_DIMENSION_VALUE_INDEX=off). Query the dimension's distinct values with malloy_executeQuery."
         : undefined;
   return packageScopedResource(
      environmentStore,
      uri,
      {
         ...envelope,
         ...(retrieval ? { retrieval } : {}),
         ...(retrievalReason ? { retrievalReason } : {}),
         ...(retrieval === "semantic" ? { belowCutoffCount } : {}),
         ...noteFor(extraNote),
      },
      pkgIndex.pkg,
      {
         environmentName,
         packageName,
         sourceName,
         limit,
         search_targets: targets,
         offset,
      },
      {
         retrievalVersion: TYPED_RETRIEVAL_VERSION,
         dimensionalValues: dimensionalValuesStatus,
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
         const typed = isTypedRequest(params);
         const targets = params.search_targets ?? [];
         if (typed) {
            const typedError = validateTypedCall({
               targets,
               offset: params.offset,
            });
            if (typedError) {
               return jsonToolError(
                  buildMalloyUri({}, "get-context"),
                  {
                     message: typedError,
                     suggestions: [
                        "Use only source targets for discovery, then a separate call for dimension/measure/view/dimensional_value.",
                     ],
                  },
                  { results: [] },
               );
            }
         }
         const scope = resolveScope(params);
         const environmentName = scope.environmentName;
         const packageName = scope.packageName;
         const query = params.query;
         const sourceName = scope.sourceName;
         const limit = params.limit;
         const max = limit ?? (typed ? 20 : 10);
         logger.info("[MCP Tool getContext] Retrieving context", {
            environmentName,
            packageName,
            hasQuery: Boolean(query),
            hasTargets: typed,
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

         // Typed phase 1: source targets with an environment and no package
         // search every package in that environment.
         if (
            typed &&
            environmentName &&
            !packageName &&
            targets.some((t) => t.target_type === "source")
         ) {
            try {
               return await typedSourceDiscovery(
                  environmentStore,
                  environmentName,
                  targets,
                  max,
                  params.offset,
               );
            } catch (error) {
               logger.warn("[MCP Tool getContext] typed source discovery failed", {
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

         const { byId, sourceContext } = pkgIndex;
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

         if (typed) {
            return typedPackageSearch({
               environmentStore,
               environmentName,
               packageName,
               pkgIndex,
               targets,
               sourceName,
               modelPath: scope.modelPath,
               entityName: scope.entityName,
               limit: max,
               offset: params.offset,
               uri,
               noteFor,
            });
         }

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
               return packageScopedResource(
                  environmentStore,
                  uri,
                  {
                     results,
                     ...noteFor(
                        "This package loaded but exposes no sources. That is a curation gap, not an empty database: check the package's explores list and export {} statements, and call malloy_getStatus for load errors and stale packages.",
                     ),
                  },
                  pkgIndex.pkg,
                  { environmentName, packageName, query, sourceName, limit },
               );
            }
            return packageScopedResource(
               environmentStore,
               uri,
               { results, ...noteFor() },
               pkgIndex.pkg,
               { environmentName, packageName, query, sourceName, limit },
            );
         }

         // Tier 4: retrieval over the package's entities. With an
         // embedding provider configured, ranking is semantic (DuckDB
         // cosine over cached entity embeddings); otherwise, or whenever
         // the semantic path is unavailable (index still building,
         // provider down, oversized package), it is lexical lunr. The
         // `retrieval` marker and per-entity `score` appear ONLY when a
         // provider is configured, so the unconfigured payload stays
         // byte-identical to the lexical-only releases.
         const ranked = await rankEntities({
            environmentStore,
            environmentName,
            packageName,
            pkgIndex,
            query: query ?? sanitized,
            sourceName,
            limit: max,
         });
         const sources = contextForResults(ranked.results, sourceContext);
         const context = sources.length > 0 ? { sources } : {};
         if (ranked.retrieval === "semantic") {
            return packageScopedResource(
               environmentStore,
               uri,
               {
                  retrieval: "semantic",
                  belowCutoffCount: ranked.belowCutoffCount,
                  results: ranked.results,
                  ...context,
                  ...noteFor(),
               },
               pkgIndex.pkg,
               { environmentName, packageName, query, sourceName, limit },
            );
         }
         return packageScopedResource(
            environmentStore,
            uri,
            ranked.retrieval === "lexical"
               ? {
                    retrieval: "lexical",
                    ...(ranked.retrievalReason
                       ? { retrievalReason: ranked.retrievalReason }
                       : {}),
                    results: ranked.results,
                    ...context,
                    ...noteFor(),
                 }
               : { results: ranked.results, ...context, ...noteFor() },
            pkgIndex.pkg,
            { environmentName, packageName, query, sourceName, limit },
         );
      },
   );
}

/**
 * The semantic index state for one package, or undefined when this server has
 * no embedding provider and therefore no index to describe.
 *
 * Composed here rather than in the controller because `totalEntities` means
 * "entities this package exposes to retrieval", which is exactly what
 * collectEntities decides — including the joins it now indexes and the
 * aliases it collapses. Reusing the same cached index keeps the number
 * honest instead of letting a second definition of "entity" drift from the
 * one retrieval actually uses.
 */
export async function getPackageEmbeddingStatus(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
): Promise<EmbeddingIndexStatus | undefined> {
   if (!embeddingConfigured()) return undefined;
   const pkgIndex = await getPackageIndex(
      environmentStore,
      environmentName,
      packageName,
   );
   return getEmbeddingIndexStatus(
      environmentStore.storageManager.getDuckDbConnection(),
      environmentName,
      packageName,
      pkgIndex.entityCount,
   );
}
