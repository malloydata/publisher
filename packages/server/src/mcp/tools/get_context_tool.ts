// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import { referencedGivenNames } from "../../service/authorize";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import { logger } from "../../logger";
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
   // Malloy type of a dimension or measure ("string", "number", "date", ...).
   // What an agent needs to know before it filters or aggregates the field.
   dataType?: string;
   // For a field reached THROUGH a join, the traversal that reaches it
   // ("orders", or "order_items.inventory_items"), which is also the dotted
   // prefix of `name`. Absent on a source's own fields. See collectJoinedFields.
   joinPath?: string;
}

/**
 * A stable, package-scoped identity for one entity: `kind:source:name`.
 *
 * The response already carries `kind`, `name` and `source` separately, so this
 * adds no information — it adds AGREEMENT. Every consumer that wants to say
 * "the same entity" across two responses was assembling this string itself,
 * and they did not assemble it the same way: a harness scoring retrieval
 * against a fixed list of expected entities is comparing identity, so a
 * one-character difference in how it joins the parts reads as a total miss on
 * a call that returned exactly the right thing.
 *
 * `Entity.id` cannot serve. It is a per-build sequence number, so it is not
 * stable across a reload, let alone across two deployments of the same model.
 *
 * Two invariants callers depend on:
 *
 * - ALWAYS three colon-separated segments. A form that drops the middle
 *   segment when there is no source produces two ids of different shape for
 *   one caller to parse, and the caller that splits on ":" and reads [1] as
 *   the source gets the name instead, silently. A colon inside a segment
 *   would break the same caller the same way, and a backtick-quoted Malloy
 *   identifier may contain one, so segments are percent-encoded. Ordinary
 *   names contain neither ":" nor "%", so their ids are unchanged.
 * - A container is its own source, so a source is `source:orders:orders`.
 *   Same reason: shape before brevity. A model-level named query with no
 *   declared source follows the same rule.
 *
 * Scoped to the package, not global: `environmentName` and `packageName` ride
 * on the result beside this, and entity results in one response never span
 * packages. Callers that need a global key join the three.
 */
export function entityId(
   kind: string,
   source: string | undefined,
   name: string,
): string {
   return [kind, source ?? name, name].map(encodeIdSegment).join(":");
}

/**
 * Percent-encode the two characters that would break `split(":")`: the
 * separator itself, and the escape character, so the encoding is reversible.
 */
function encodeIdSegment(segment: string): string {
   return segment.replace(/%/g, "%25").replace(/:/g, "%3A");
}

/**
 * One ranked entity, INTERNAL to ranking. `score` (cosine) rides only on
 * semantic results. This never reaches the wire: toSourceResults converts a
 * list of these into the response shape, which is where identity is assigned.
 */
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
   /**
    * The lexical hit's score, normalized against the top hit so it shares the
    * [0, 1] scale the semantic path uses. Internal to ranking: serialization
    * never reads it, because a lexical relevance is not comparable between
    * queries and publishing one would invite exactly that comparison.
    */
   lexicalScore?: number;
   /** Malloy type of a dimension or measure. */
   dataType?: string;
   /** The join traversal reaching this field; absent on a source's own. */
   joinPath?: string;
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
   /**
    * The source's doc reduced to its first line, capped, which is the short
    * label a catalog view shows beside a source name. Absent when undocumented.
    */
   oneLineSummary?: string;
   /**
    * Model-level `given:` parameters in scope for this source. An agent spends
    * them on malloy_executeQuery, so knowing they exist is what stops it
    * guessing at a value the model already defaults.
    */
   givens?: SourceContextGiven[];
   /**
    * The `#(authorize)` gates in force on this source, and the givens each one
    * reads. Retrieval that offered a gated source without saying so spent a
    * slot on an entity the caller could not query, and the agent learned that
    * only from the denial.
    *
    * Report-only, and never a predicate to evaluate caller-side: the list
    * flattens gates carried in from elsewhere, they are AND-ed rather than
    * OR-ed, and an unattributable gate reports the fail-closed placeholder
    * "false" that no author wrote. Read it as "this source is gated, and these
    * are the givens to supply". See docs/authorize.md.
    */
   authorize?: SourceContextAuthorize[];
}

/** A `given:` a caller may supply, as the model declares it. */
interface SourceContextGiven {
   name: string;
   type?: string;
   annotations?: string[];
   default?: string;
}

/** One authorize gate, with the givens its expression reads. */
interface SourceContextAuthorize {
   expression: string;
   given_names: string[];
}

/**
 * How close two sibling scores must be before they are treated as the same
 * concept found in parallel sources rather than as two ranked answers.
 * Tuned against get_context_eval.ts; deliberately tight, so a sibling that
 * is genuinely a worse match keeps its own row.
 */
export const SIBLING_SCORE_EPSILON = 0.03;

/**
 * The most entities one call may return, and so also the ceiling on what the
 * semantic query may fetch: sibling collapsing over-fetches to refill the
 * window, and this bounds the scan it can ask for. Defined once because the
 * parameter schema, the truncation warning and that scan must agree.
 */
const MAX_LIMIT = 50;

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
 * them at effectively the same score, presented as independent peers with
 * nothing to say they are near-duplicates or how to choose between them.
 *
 * Collapsing does double duty: it returns the wasted slots to genuinely
 * different concepts, and `alsoIn` makes the ambiguity explicit instead of
 * leaving it to be inferred from three rows that look independent. Only
 * near-identical scores group — a sibling that really is a worse match is a
 * ranked answer, not a duplicate.
 */
/** The [0, 1] score this row was ranked on, whichever path produced it. */
function rankScore(r: ResultEntity): number | undefined {
   return r.score ?? r.lexicalScore;
}

function groupSiblings(results: ResultEntity[], limit: number): ResultEntity[] {
   const keptByConcept = new Map<string, ResultEntity>();
   const kept: ResultEntity[] = [];
   for (const r of results) {
      // joinPath is in the key for the reason collapseAliases carries it:
      // humanizeName maps "." and "_" alike to a space, so a joined
      // `orders.amount` and a local `orders_amount` key identically.
      const key = `${r.kind}\x00${r.joinPath ?? ""}\x00${humanizeName(r.name)}`;
      const peer = keptByConcept.get(key);
      const peerScore = rankScore(peer ?? r);
      const rowScore = rankScore(r);
      if (
         peer &&
         r.source &&
         peer.source !== r.source &&
         // Unscored rows never group: with no score there is nothing to say
         // the two are equally good, and collapsing on the name alone would
         // hide a genuinely better match behind a worse one.
         peerScore !== undefined &&
         rowScore !== undefined &&
         Math.abs(peerScore - rowScore) <= SIBLING_SCORE_EPSILON &&
         // A sibling carrying its OWN, different doc is kept as its own row.
         // The score gate cannot stand in for this: entityFacets embeds a
         // name facet of humanizeName(name) with no source in it, so two
         // same-named fields in different sources embed byte-identical text
         // and score IDENTICALLY whenever the name facet wins -- however far
         // apart their docs are. Folding there destroys the one thing that
         // tells the caller which source to trust, and a folded row leaves
         // the result entirely: its doc, data_type and entity_id are
         // unrecoverable, and alsoIn carries only a source name. Same rule,
         // and the same reasoning, as collapseAliases after 3e8cb76b.
         (!r.doc || r.doc === peer.doc)
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
 * The response shape: source-centric, snake_case, identity in a structured
 * `resource_id`. It matches a hosted Malloy retrieval API's `get_context`, so
 * one parser serves both and a retrieval score computed here is comparable to
 * one computed there.
 *
 * ONLY SERIALIZATION HAPPENS HERE. Ranking, sibling grouping, alias collapse
 * and the relevance floor all run over the flat `ResultEntity[]` upstream;
 * this converts that list at the boundary, so a ranking bug and a shape bug
 * stay separately reviewable.
 *
 * Three deliberate divergences from the published shape:
 *
 * 1. `entity_type` is a SUPERSET of its enum, adding `join` (an agent that
 *    cannot see a declared join concludes the model has none) and `query`
 *    (a model-level named query). A `source` never appears as an entity_type:
 *    a source is the container.
 * 2. Publisher-only fields ride along: `source_info.joins`, `entity_id`,
 *    `relationship`, `aliases`, `also_in`, and the response-level `retrieval`
 *    / `retrieval_reason` / `below_cutoff_count` / `total_entities`.
 * 3. Fields Publisher cannot honestly fill are OMITTED, not sent empty:
 *    `summary`, `prominence`, `values`, `values_indexed`, `match_reason`.
 *    That spec omits null fields, so absence is in-contract on both sides.
 */
interface ResourceId {
   environment: string;
   package: string;
   model_path: string;
   source: string;
}

interface SourceCardInfo {
   resource_id: ResourceId;
   one_line_summary?: string;
   docs?: string;
   givens?: SourceContextGiven[];
   authorize?: SourceContextAuthorize[];
   /** Publisher extension. Complete, so `[]` means "declares none". */
   joins: SourceContextJoin[];
}

interface SourceCardEntity {
   name: string;
   /** The published view/measure/dimension, plus Publisher's join and query. */
   entity_type: string;
   relevance?: number;
   description?: string;
   data_type?: string;
   /** Publisher extension. Stable `kind:source:name`; see {@link entityId}. */
   entity_id: string;
   relationship?: Relationship;
   /** The join traversal reaching this field, when it is not the source's own. */
   join_path?: string;
   aliases?: string[];
   also_in?: string[];
}

interface SourceCard {
   source_info: SourceCardInfo;
   relevance?: number;
   entities?: SourceCardEntity[];
}

/**
 * Group a ranked entity list into the source-centric response shape.
 *
 * Source order is the order sources first appear in the ranked list, which
 * keeps the best-matching source first without re-sorting — the ranking is
 * already correct and re-deriving it here could disagree with it. Entities
 * keep their rank order within a source for the same reason.
 *
 * A `kind: "source"` hit becomes the CONTAINER carrying a relevance, not a row
 * in its own `entities`. That is what the shape means by a source, and
 * it is why the source's score has somewhere to live once entities nest.
 */
function toSourceResults(
   results: ResultEntity[],
   sourceContext: Map<string, SourceContextEntry>,
   environmentName: string,
   packageName: string,
): SourceCard[] {
   const bySource = new Map<string, SourceCard>();

   const cardFor = (
      name: string | undefined,
      modelPathFallback: string,
   ): SourceCard | undefined => {
      if (!name) return undefined;
      let entry = bySource.get(name);
      if (!entry) {
         const ctx = sourceContext.get(name);
         entry = {
            source_info: {
               resource_id: {
                  environment: environmentName,
                  package: packageName,
                  model_path: ctx?.modelPath ?? modelPathFallback,
                  source: name,
               },
               ...(ctx?.oneLineSummary
                  ? { one_line_summary: ctx.oneLineSummary }
                  : {}),
               ...(ctx?.doc ? { docs: ctx.doc } : {}),
               ...(ctx?.givens ? { givens: ctx.givens } : {}),
               ...(ctx?.authorize ? { authorize: ctx.authorize } : {}),
               joins: ctx?.joins ?? [],
            },
         };
         bySource.set(name, entry);
      }
      return entry;
   };

   for (const r of results) {
      const entry = cardFor(r.source, r.modelPath);
      if (!entry) continue;
      // A folded sibling left the result set, so nothing else would emit its
      // source. Its card is exactly where the grain rule or the `where:` that
      // makes it a DIFFERENT number from the row we kept would be written, so
      // materialise it here, entity-less, adjacent to the peer that names it.
      // Without this, collapsing does not merely cost a row -- it removes the
      // only evidence that a second reading of the concept exists.
      for (const sibling of r.alsoIn ?? []) {
         cardFor(sibling, r.modelPath);
      }
      if (r.kind === "source") {
         // The source itself matched: its score belongs on the container, and
         // its full (untruncated) doc supersedes the truncated context copy.
         if (r.score !== undefined) entry.relevance = r.score;
         if (r.doc) entry.source_info.docs = r.doc;
         continue;
      }
      const entity: SourceCardEntity = {
         name: r.name,
         entity_type: r.kind,
         entity_id: entityId(r.kind, r.source, r.name),
         ...(r.score !== undefined ? { relevance: r.score } : {}),
         ...(r.doc ? { description: r.doc } : {}),
         ...(r.dataType ? { data_type: r.dataType } : {}),
         ...(r.relationship ? { relationship: r.relationship } : {}),
         ...(r.joinPath ? { join_path: r.joinPath } : {}),
         ...(r.aliases ? { aliases: r.aliases } : {}),
         ...(r.alsoIn ? { also_in: r.alsoIn } : {}),
      };
      (entry.entities ??= []).push(entity);
      // A source with no hit of its own still ranks by its best entity, so a
      // caller reading source relevance never sees a matched source at null.
      if (
         r.score !== undefined &&
         (entry.relevance === undefined || r.score > entry.relevance)
      ) {
         entry.relevance = r.score;
      }
   }
   return Array.from(bySource.values());
}

/**
 * Distinct sources across a ranked list, which is what `total_available`
 * counts on a search tier. Taken over every matched row rather than over the
 * windowed ones, so it can exceed `returned`: setting both from the returned
 * cards made the pair read "N of N" on every search, including one the entity
 * cap had cut, and a caller reading them saw nothing was left behind.
 */
function distinctSourceCount(rows: ResultEntity[]): number {
   const sources = new Set<string>();
   for (const r of rows) {
      if (r.source) sources.add(r.source);
      // A folded sibling has no row of its own but does get a card, so it is
      // one of the sources this count is about.
      for (const sibling of r.alsoIn ?? []) sources.add(sibling);
   }
   return sources.size;
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
         "Optional. Narrow results to one source. Not a required second step: a query already returns each matching source with its fields nested.",
      ),
   limit: z
      .number()
      .int()
      .positive()
      .max(MAX_LIMIT)
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
 * How many joins deep a field path is indexed. Depth 1 reaches
 * `orders.amount`; depth 2 reaches `order_items.inventory_items.cost`, the
 * depth the published shape's own `join_path` example uses. Deeper paths
 * exist and stay unindexed: each level multiplies the entity count by the
 * joined source's field count, against a hard cap of MAX_EMBEDDED_ENTITIES.
 */
const MAX_JOIN_PATH_DEPTH = 2;

/** The shape of a field inside a join's inlined schema. */
interface JoinSchemaField {
   kind?: string;
   name: string;
   annotations?: Array<string | { value: string }>;
   relationship?: Relationship;
   schema?: { fields?: JoinSchemaField[] };
   type?: { kind?: string };
}

/**
 * Index the fields reachable THROUGH a join, under their dotted Malloy path.
 *
 * A join used to be indexed as a single entity and its target's fields left
 * out, on the reasoning that they were already indexed under the target
 * source. That reasoning does not survive contact with the caller: the path
 * is what a query needs, and the path cannot be derived from anything the
 * response carries. The stable `JoinInfo` inlines the target's schema without
 * naming the target source (#1100), so an agent holding a join entity knows a
 * relationship exists and has no way to learn what it reaches. Meanwhile the
 * tool description tells it to "traverse a join as joinName.fieldName" -- a
 * path it was being asked to guess. The published shape indexes these as
 * entities in their own right and reports the traversal as `join_path`.
 *
 * `fanout` is the widest relationship on the path, not the last one: a single
 * `many` hop anywhere means aggregating through this field fans out, which is
 * the fact that decides whether a measure reached this way can be trusted.
 * Views are deliberately not collected: a joined view is not referenceable as
 * `joinName.viewName` the way a dimension or measure is.
 */
function collectJoinedFields(args: {
   fields: JoinSchemaField[];
   sourceName: string;
   modelPath: string;
   joinPath: string;
   fanout: Relationship;
   depth: number;
   nextId: () => string;
   out: Entity[];
}): void {
   const {
      fields,
      sourceName,
      modelPath,
      joinPath,
      fanout,
      depth,
      nextId,
      out,
   } = args;
   for (const field of fields) {
      if (field.kind === "join") {
         if (depth >= MAX_JOIN_PATH_DEPTH) continue;
         collectJoinedFields({
            ...args,
            fields: field.schema?.fields ?? [],
            joinPath: `${joinPath}.${field.name}`,
            fanout:
               field.relationship === "one"
                  ? fanout
                  : (field.relationship ?? fanout),
            depth: depth + 1,
         });
         continue;
      }
      if (field.kind !== "dimension" && field.kind !== "measure") continue;
      out.push({
         id: nextId(),
         kind: field.kind,
         name: `${joinPath}.${field.name}`,
         // The source that DECLARES the traversal, so the field nests under
         // the card a caller would actually query from.
         source: sourceName,
         modelPath,
         doc: docText(field.annotations),
         embedDoc: docOnlyText(field.annotations),
         joinPath,
         relationship: fanout,
         ...(field.kind === "measure" || field.kind === "dimension"
            ? { dataType: malloyType(field) }
            : {}),
      });
   }
}

/**
 * Walk every model in the package and collect sources, their views,
 * dimension/measure fields and declared joins, and named queries. Returns the
 * full set; the optional source-level drill-down is applied by the caller
 * after retrieval.
 */
async function collectEntities(pkg: Package): Promise<CollectedModel> {
   // listModels() already returns only .malloy model files (notebooks are listed separately).
   // Sorted by path because a package can expose one source from several models
   // and only the first is kept: filesystem order would otherwise decide which
   // model_path a source reports, and which model's givens ride along with it,
   // differently on two machines serving the same package.
   const models = [...(await pkg.listModels())].sort((a, b) =>
      (a.path ?? "").localeCompare(b.path ?? ""),
   );

   const entities: Entity[] = [];
   const governance = new Map<string, SourceGovernance>();
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
      // The compiled ApiSource carries what SourceInfo does not: the givens in
      // scope and the authorize gates in force. Keyed by name so the card can
      // pick up its own, and read defensively because a spec's model stand-in
      // implements only the two accessors above.
      const apiSources = model.getSources?.() ?? [];

      for (const sourceInfo of sourceInfos) {
         const sourceName = sourceInfo.name;
         // First model wins, matching the entity dedupe below, so a source's
         // identity and its governance always come from the same model.
         if (!governance.has(sourceName)) {
            const apiSource = apiSources.find((c) => c.name === sourceName);
            if (apiSource) {
               governance.set(sourceName, {
                  givens: (apiSource.givens ?? []).flatMap((given) =>
                     given.name
                        ? [
                             {
                                name: given.name,
                                ...(given.type ? { type: given.type } : {}),
                                ...(given.annotations?.length
                                   ? { annotations: given.annotations }
                                   : {}),
                                ...(given.default !== undefined
                                   ? { default: given.default }
                                   : {}),
                             },
                          ]
                        : [],
                  ),
                  authorize: (apiSource.authorize ?? []).map((expression) => ({
                     expression,
                     given_names: referencedGivenNames(expression),
                  })),
               });
            }
         }
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
               // The join's own entity above says a relationship exists; the
               // fields below say what it reaches and under what path. Both
               // are needed, and neither substitutes for the other.
               collectJoinedFields({
                  fields: (field.schema?.fields ?? []) as JoinSchemaField[],
                  sourceName,
                  modelPath,
                  joinPath: field.name,
                  fanout: field.relationship,
                  depth: 1,
                  nextId: () => String(n++),
                  out: entities,
               });
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
               ...(field.kind === "view"
                  ? {}
                  : { dataType: malloyType(field) }),
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
   return { entities: collapseAliases(deduped), governance };
}

/** What the compiled model says about querying a source, beyond its schema. */
interface SourceGovernance {
   givens: SourceContextGiven[];
   authorize: SourceContextAuthorize[];
}

/** The entities of a package, plus the per-source governance beside them. */
interface CollectedModel {
   entities: Entity[];
   governance: Map<string, SourceGovernance>;
}

/**
 * A field's Malloy type as a plain name: "string", "number", "date".
 *
 * The interface spells these `string_type`, `number_type` and so on, which is
 * an encoding rather than a name an agent would write, so the suffix comes off.
 * Absent when the interface reports no type, which a model stand-in does.
 */
function malloyType(field: { type?: { kind?: string } }): string | undefined {
   const kind = field.type?.kind;
   return kind ? kind.replace(/_type$/, "") : undefined;
}

/**
 * Collapse entities within one source that differ only in the spelling of
 * the same name, keeping the documented one and recording the rest.
 *
 * A model that renames a physical column without hiding the original leaves
 * both in the schema: `SITE` and `site` are one column, indexed twice, and
 * both then compete for the same scarce result slots.
 *
 * Detection is by humanized name, and it has to be: the stable Malloy
 * interface gives a dimension only `{name, type, annotations}`, with no
 * expression, so there is no way to prove `site` is a rename of `SITE`
 * rather than a derivation. The heuristic covers the measured case (a pure
 * case/separator respelling inside one source) and stops there. Two
 * genuinely distinct fields whose names humanize identically would collapse,
 * so the heuristic is narrowed by the one signal available: a spelling that
 * carries its own, different `#(doc)` is kept as its own row, because that
 * doc is what a caller would read to choose between them and a dropped entity
 * leaves the index entirely. What folds is an undocumented spelling, or one
 * repeating the kept doc, and its name is still reported in `aliases`.
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
      // joinPath is part of the key because humanizeName maps "." and "_"
      // alike to a space, so a joined `orders.amount` and a local
      // `orders_amount` would otherwise key identically and collapse into
      // one row -- two different columns, one of them reached through a join
      // that may filter or fan out.
      const key = `${e.kind}\x00${e.source ?? ""}\x00${e.joinPath ?? ""}\x00${humanizeName(e.name)}`;
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
      // Only fold in a spelling that adds no documentation of its own, or
      // repeats the kept one's. Two documented spellings whose docs DIFFER
      // are the evidence available that they are two concepts rather than
      // one column named twice, and the doc is exactly what a caller would
      // read to tell them apart -- so collapsing there would destroy the
      // thing that resolves the ambiguity. A dropped entity is removed from
      // the index entirely, not merely hidden from a result, so its doc is
      // unrecoverable; `aliases` carries only a name.
      const folded = rest.filter(
         (e) => !e.embedDoc || e.embedDoc === keep.embedDoc,
      );
      if (folded.length === 0) continue;
      keep.aliases = folded.map((e) => e.name);
      for (const e of folded) {
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

/** Longest a one-line summary may be, matching the hosted API's own cap. */
const ONE_LINE_SUMMARY_MAX_CHARS = 120;

/**
 * A source's doc reduced to one line: its first sentence or line, capped.
 *
 * A catalog view wants a label, not a paragraph, and the full text is still
 * on the card as `docs`. Undocumented sources get nothing rather than a
 * fabricated summary, which is what an absent field means on both sides.
 */
function oneLineSummary(doc: string): string | undefined {
   const firstLine = doc.split("\n")[0]?.trim();
   if (!firstLine) return undefined;
   const sentenceEnd = firstLine.search(/[.!?](\s|$)/);
   const summary =
      sentenceEnd === -1 ? firstLine : firstLine.slice(0, sentenceEnd + 1);
   return summary.length > ONE_LINE_SUMMARY_MAX_CHARS
      ? truncateDoc(summary, ONE_LINE_SUMMARY_MAX_CHARS)
      : summary;
}

/**
 * Derive per-source context from the collected entities: the source's own doc,
 * every join declared on it, and the givens and gates that govern querying it. Built once per package alongside the index,
 * so attaching it to a response costs a lookup rather than a model walk.
 */
function buildSourceContext(
   collected: CollectedModel,
): Map<string, SourceContextEntry> {
   const { entities, governance } = collected;
   const context = new Map<string, SourceContextEntry>();
   for (const e of entities) {
      if (e.kind !== "source") continue;
      const gates = governance.get(e.name);
      const summary = oneLineSummary(e.doc);
      context.set(e.name, {
         name: e.name,
         modelPath: e.modelPath,
         doc: truncateDoc(e.doc, SOURCE_DOC_MAX_CHARS),
         joins: [],
         ...(summary ? { oneLineSummary: summary } : {}),
         ...(gates?.givens.length ? { givens: gates.givens } : {}),
         ...(gates?.authorize.length ? { authorize: gates.authorize } : {}),
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

   const collected = await collectEntities(pkg);
   const { entities } = collected;
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
      sourceContext: buildSourceContext(collected),
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
const GET_CONTEXT_DESCRIPTION = `Discover what a Publisher deployment exposes and retrieve the entities most relevant to a plain-English question, so you ground queries in names the model defines.

## Contract rules
- Use the names it returns verbatim; never invent one that is not in the results.
- A query returns whole source cards with fields nested: one call, no drill-down.
- Read warnings and any error/stale field before trusting a number: data may be missing, stale, or cut short.
- A source's joins list is complete: empty means it declares none, so write that relationship inline rather than probe for one.
- Read a source's doc before querying it: it carries grain and population rules its fields do not.
- A source with authorize is gated: supply the givens it names or the query is denied.

## Parameters
All optional; supply what you know. None lists environments and their packages; environmentName lists its packages; + packageName lists its sources with their joins; + query ranks its entities. sourceName alone returns one source's full card ([] = no such source); with a query it ranks inside it. limit caps entities (max 50; retrieval 10).

## Response
sources[], best first. source_info: resource_id (environment/package/model_path/source) -> malloy_executeQuery's environmentName/packageName/modelPath; docs (… = truncated), one_line_summary, complete joins, givens, authorize (report-only). entities[] nest under it: name, entity_type (dimension/measure/view/join/query), description, data_type, relationship (fan-out), join_path, aliases (other spellings), also_in (equal-scoring peers; pick by docs), relevance, entity_id. A joined field's name IS its full dotted path: use it verbatim. Pass a source as sourceName, a view or query as queryName.
ranking (relevance|prominence), returned of total_available sources, warnings[] for what was cut or stale.
Semantic fills relevance: no sources means nothing cleared the floor; below_cutoff_count of total_entities were rejected. A "lexical" retrieval adds retrieval_reason; only "indexing" is worth a retry.

## Example
{"environmentName":"examples","packageName":"storefront","query":"revenue by category"}`;

/**
 * An error keeps the empty collection its tier would have answered with, so a
 * caller can read the payload unconditionally instead of branching on success
 * first. BOTH keys, because the two tiers no longer answer alike: the
 * environment and package listings still return `results`, while everything
 * that returns sources returns `sources`, and an error is raised before the
 * tier is known. One extra empty array is cheaper than an agent reading
 * `sources.length === 0` on an error payload and concluding the package models
 * nothing.
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
         sources: [],
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
          * Spread into a payload to attach `warnings`. Returns {} when there is
          * nothing to say, so a healthy package's payload carries no key at all
          * rather than an empty array a caller has to test.
          *
          * A list rather than one joined string: staleness and truncation are
          * separate facts with separate remedies, and joining them made the
          * second read as a continuation of the first.
          */
         const warningsFor = (...extra: (string | undefined)[]) => {
            const warnings = [staleNote, ...extra].filter(
               (warning): warning is string => Boolean(warning),
            );
            return warnings.length > 0 ? { warnings } : {};
         };

         /**
          * The warning for a capped result set, or undefined when nothing was
          * cut. It names the remedy that works here: raising the limit, or
          * narrowing the question. Telling an agent to "search more
          * specifically" when the cut was a flat cap sends it to re-query at
          * the one stage that cannot change the outcome.
          */
         const truncationWarning = (returned: number, matched: number) =>
            matched > returned
               ? `Returned ${returned} of ${matched} matching entities. Raise limit (max ${MAX_LIMIT}) or narrow the query to see the rest.`
               : undefined;

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
            // A drill-down (sourceName, no query) lists every entity the
            // named source offers, so the card carries its views, dimensions,
            // measures, joins and named queries. Listing only the source row
            // there returned what the caller already had from the package
            // listing and never the fields the tool description promises.
            // collectEntities pushes a source ahead of its own fields, and
            // Array.from preserves that insertion order, so toSourceResults
            // sees the source before the entities that nest under it.
            //
            // Enumeration: return everything unless the caller sets an explicit
            // limit. slice(0, undefined) keeps the whole list, so discovery is
            // not silently capped the way ranked retrieval (tier 4) is.
            const inScope = Array.from(byId.values()).filter((e) =>
               sourceName ? e.source === sourceName : e.kind === "source",
            );
            // The limit caps entities. A drill-down's source row becomes the
            // card rather than an entity, so it does not spend a slot; a
            // package listing has only source rows, so there the cap is on
            // cards, which is what the listing is made of.
            let entityBudget = limit ?? Number.POSITIVE_INFINITY;
            const capped = inScope.filter((e) => {
               if (sourceName && e.kind === "source") return true;
               if (entityBudget <= 0) return false;
               entityBudget -= 1;
               return true;
            });
            // Counted in the same unit the cap spends, so the warning below is
            // about the rows a caller actually lost.
            const cappable = sourceName
               ? inScope.filter((e) => e.kind !== "source").length
               : inScope.length;
            // The overview is where an agent decides how to combine sources,
            // so each one states its relationships here rather than making
            // that a second call. toSourceResults carries the complete joins
            // list onto every entry, so empty means none declared.
            const sources = toSourceResults(
               capped.map((e) => ({
                  kind: e.kind,
                  name: e.name,
                  source: e.source,
                  environmentName,
                  packageName,
                  modelPath: e.modelPath,
                  doc: e.doc,
                  ...(e.relationship ? { relationship: e.relationship } : {}),
                  ...(e.aliases ? { aliases: e.aliases } : {}),
                  ...(e.joinPath ? { joinPath: e.joinPath } : {}),
                  ...(e.dataType ? { dataType: e.dataType } : {}),
               })),
               sourceContext,
               environmentName,
               packageName,
            );
            // A listing is deterministic catalog order, not a ranking, and
            // Publisher has no query-usage signal to fill the hosted API's
            // `prominence` with, so it names the ordering and omits the
            // score rather than inventing a number nobody should rank on.
            // Both counts are in SOURCES, the unit the payload is made of. A
            // drill-down is one source's card, so it is 1-of-1 however many
            // entities nest inside it; the entity cap is reported separately.
            const listingEnvelope = {
               ranking: "prominence" as const,
               total_available: sourceName
                  ? Math.min(inScope.length, 1)
                  : inScope.length,
               returned: sources.length,
            };
            // An empty enumeration is ambiguous to an agent: "no data here" and
            // "the package exposes nothing" look identical. The package DID
            // load (a failed load throws out of getPackageIndex above), so an
            // empty result means its models expose no sources: a curation gap
            // (explores/export {}), not an empty database. Say so, only in the
            // empty case, so the populated payload stays byte-identical.
            if (sources.length === 0 && !sourceName) {
               return jsonResource(uri, {
                  sources,
                  ...listingEnvelope,
                  ...warningsFor(
                     "This package loaded but exposes no sources. That is a curation gap, not an empty database: check the package's explores list and export {} statements, and call malloy_getStatus for load errors and stale packages.",
                  ),
               });
            }
            return jsonResource(uri, {
               sources,
               ...listingEnvelope,
               ...warningsFor(
                  truncationWarning(
                     capped.length - (sourceName ? 1 : 0),
                     cappable,
                  ),
               ),
            });
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
         let belowCutoffCount = 0;
         // The denominator belowCutoffCount is read against; see
         // SemanticSearchResult. Undefined on the lexical path, where there
         // is no floor and so no count to anchor.
         let totalEntities: number | undefined;
         // Why a configured server answered lexically. Without it "lexical"
         // is a dead end: an agent cannot tell a cold index, which clears in
         // seconds and is worth retrying, from a down provider, which is not.
         let retrievalReason: RetrievalReason | undefined;
         // How many entities matched before the limit cut the list, so a
         // capped response can say what it left behind.
         let semanticMatchCount: number | undefined;
         // Distinct sources across everything that matched, so total_available
         // can exceed the returned card count when the entity cap cut rows.
         let semanticTotalSources: number | undefined;
         if (configured) {
            let provider: EmbeddingProvider | null = null;
            try {
               provider = getEmbeddingProvider();
            } catch (error) {
               retrievalReason = "unavailable";
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
                     limit: scoped ? max : Math.min(MAX_LIMIT, max * 3),
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
                              ...(e.joinPath ? { joinPath: e.joinPath } : {}),
                              ...(e.dataType ? { dataType: e.dataType } : {}),
                              score: Math.round(hit.score * 10_000) / 10_000,
                           },
                        ];
                     });
                     // Grouped ONCE, with no window, then windowed. Sibling
                     // collapse records the duplicates it merges ON the row it
                     // keeps, so calling it twice appends every also_in twice;
                     // and because it only ever drops rows past the limit, the
                     // full run's first `max` rows are what a limited run
                     // returns. That makes the ungrouped length the honest
                     // denominator for the truncation warning: concepts
                     // available, not rows before merging.
                     const grouped = scoped
                        ? ranked
                        : groupSiblings(ranked, ranked.length);
                     semanticMatchCount = grouped.length;
                     semanticTotalSources = distinctSourceCount(grouped);
                     semanticResults = grouped.slice(0, max);
                     belowCutoffCount = semantic.belowCutoffCount;
                     totalEntities = semantic.totalEntities;
                  } else {
                     retrievalReason =
                        REASON_BY_UNAVAILABLE[semantic.unavailable];
                  }
               } catch (error) {
                  // Defensive: trySemanticSearch does not throw, but the
                  // storage handle lookup can (e.g. before initialization
                  // or under a partial test double). Semantic retrieval
                  // must never take tier 4 down with it.
                  retrievalReason = "unavailable";
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
            const sources = toSourceResults(
               semanticResults,
               sourceContext,
               environmentName,
               packageName,
            );
            return jsonResource(uri, {
               sources,
               ranking: "relevance" as const,
               total_available: semanticTotalSources ?? sources.length,
               returned: sources.length,
               retrieval: "semantic",
               // Always present on a semantic response, including 0: the
               // reading depends on being able to tell 0 from absent. Paired
               // with total_entities, without which the count is a bare number
               // an agent cannot scale -- see SemanticSearchResult for why
               // the ratio, not the count, carries the signal.
               below_cutoff_count: belowCutoffCount,
               ...(totalEntities !== undefined
                  ? { total_entities: totalEntities }
                  : {}),
               // Both counts in ranked ROWS, the unit the cap spends and the
               // unit the lexical path below already uses. entityCountIn
               // counts nested entities, which excludes a source that matched
               // on its own terms -- toSourceResults turns that into the card
               // rather than a row under it -- so pairing it with a row count
               // reported a cut that never happened whenever a source ranked.
               ...warningsFor(
                  truncationWarning(
                     semanticResults.length,
                     semanticMatchCount ?? semanticResults.length,
                  ),
               ),
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
         const matched = hits
            .map((hit) => byId.get(hit.ref))
            .filter((e): e is Entity => e !== undefined)
            // Drill-down: narrow to one source when sourceName is set.
            .filter((e) => !sourceName || e.source === sourceName);
         // Normalized against the top hit, so the sibling epsilon means the
         // same thing here as it does on the semantic path: lunr's raw scores
         // are relative to the query, not on a fixed scale.
         const topScore = hits[0]?.score ?? 0;
         const scoreByRef = new Map(
            hits.map((hit) => [
               hit.ref,
               topScore > 0 ? hit.score / topScore : undefined,
            ]),
         );
         const scored: ResultEntity[] = matched.map((e) => ({
            kind: e.kind,
            name: e.name,
            source: e.source,
            environmentName,
            packageName,
            modelPath: e.modelPath,
            doc: e.doc,
            ...(e.relationship ? { relationship: e.relationship } : {}),
            ...(e.aliases ? { aliases: e.aliases } : {}),
            ...(e.joinPath ? { joinPath: e.joinPath } : {}),
            ...(e.dataType ? { dataType: e.dataType } : {}),
            ...(scoreByRef.get(e.id) !== undefined
               ? { lexicalScore: scoreByRef.get(e.id) }
               : {}),
         }));
         // Grouped once, unwindowed, then windowed: see the semantic path for
         // why calling it twice doubles every alsoIn. A drill-down is confined
         // to one source, so it has no siblings to collapse.
         const grouped = scoped ? scored : groupSiblings(scored, scored.length);
         const results = grouped.slice(0, max);

         const sources = toSourceResults(
            results,
            sourceContext,
            environmentName,
            packageName,
         );
         const envelope = {
            sources,
            ranking: "relevance" as const,
            total_available: distinctSourceCount(grouped),
            returned: sources.length,
         };
         const lexicalWarnings = warningsFor(
            truncationWarning(results.length, grouped.length),
         );
         return jsonResource(
            uri,
            configured
               ? {
                    ...envelope,
                    retrieval: "lexical",
                    ...(retrievalReason
                       ? { retrieval_reason: retrievalReason }
                       : {}),
                    ...lexicalWarnings,
                 }
               : { ...envelope, ...lexicalWarnings },
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
   const provider = getEmbeddingProvider();
   if (!provider) return undefined;
   const pkgIndex = await getPackageIndex(
      environmentStore,
      environmentName,
      packageName,
   );
   return getEmbeddingIndexStatus(
      environmentStore.storageManager.getDuckDbConnection(),
      provider,
      environmentName,
      packageName,
      Array.from(pkgIndex.byId.values()),
   );
}
