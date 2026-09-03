// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import lunr from "lunr";
import type { Relationship } from "@malloydata/malloy-interfaces";
import type { ModelDef } from "@malloydata/malloy";
import { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import {
   EmbeddingProvider,
   embeddingConfigured,
   getEmbeddingProvider,
} from "../../service/embedding_provider";
import { referencedGivenNames } from "../../service/authorize";
import { InvalidArgumentError } from "../../errors";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import { logger } from "../../logger";
import {
   entityRowKey,
   getEmbeddingIndexStatus,
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
   // The authored expression text, from the compiled model. Absent on a
   // physical column, which HAS no expression -- so absence is the fact that
   // the field is raw, not a gap. Serialized only when the caller asks with
   // `include_code`.
   code?: string;
   // When this field's whole definition is a reference to another field of
   // the SAME source (`dimension: site is SITE`), the name it refers to.
   // This is what makes alias collapsing exact instead of a guess about
   // names; see collapseAliases. A reference that traverses a join carries a
   // multi-segment path and is deliberately NOT recorded here: that is a
   // different column reached through a relationship, not another spelling.
   aliasOf?: string;
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
   /** The field's Malloy expression; serialized only on `include_code`. */
   code?: string;
   score?: number;
   /** Malloy type of a dimension or measure. */
   dataType?: string;
   /** The join traversal reaching this field; absent on a source's own. */
   joinPath?: string;
   /** Score per search target index that matched this row, for matched_targets. */
   targetScores?: Map<number, number>;
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
    * them on execute_query, so knowing they exist is what stops it
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
   /** Filters the source declares via `#(filter)`. */
   filters?: SourceContextFilter[];
}

/** A `given:` a caller may supply, as the model declares it. */
interface SourceContextGiven {
   name: string;
   type?: string;
   annotations?: string[];
   default?: string;
}

/**
 * A filter a source exposes via `#(filter)`, as the model declares it. A
 * REQUIRED one is the reason this is on the card at all: a caller that cannot
 * see it cannot supply it, and the query fails when they use the source. That
 * is the same argument that keeps `givens` on a card, and it is why this is
 * reported rather than left to a follow-up call the way a description is.
 */
interface SourceContextFilter {
   name: string;
   type: string;
   dimension?: string;
   required?: boolean;
}

/** One authorize gate, with the givens its expression reads. */
interface SourceContextAuthorize {
   expression: string;
   given_names: string[];
}

/**
 * The most entities one call may return, and so also the ceiling on what the
 * semantic query may fetch: the ranked scan over-fetches to fill its source
 * window, and this bounds the scan it can ask for. Defined once because the
 * parameter schema, the truncation warning and that scan must agree.
 */
const MAX_LIMIT = 150;

/**
 * Sources a ranked search returns when the caller sets no limit. A listing
 * defaults to MAX_LIMIT instead: a browse that stops early is not a browse,
 * and its cards are cheap because they carry no entities. Both match the
 * published defaults.
 */
const DEFAULT_RANKED_LIMIT = 20;

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
 * The response shape: source-centric, snake_case, identity in a structured
 * `resource_id`. It matches a hosted Malloy retrieval API's `get_context`, so
 * one parser serves both and a retrieval score computed here is comparable to
 * one computed there.
 *
 * ONLY SERIALIZATION HAPPENS HERE. Ranking, alias collapse and the relevance
 * floor all run over the flat `ResultEntity[]` upstream;
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
 *    `relationship`, `aliases`, `code` (only when the caller asks for it with
 *    `include_code`), and the response-level `retrieval`
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
   filter_params?: SourceContextFilter[];
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
   /**
    * Publisher extension. The field's Malloy expression, present only when the
    * caller passed `include_code`. A physical column has none, so absence
    * under that flag means the field is raw rather than derived.
    */
   code?: string;
   relationship?: Relationship;
   /** The join traversal reaching this field, when it is not the source's own. */
   join_path?: string;
   /** Which search targets matched this entity, and how well. */
   matched_targets?: Array<{ search_text: string; relevance: number }>;
   aliases?: string[];
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
   /** Search texts by target index, for matched_targets. Empty on a listing. */
   searchTexts: Map<number, string> = new Map(),
   /** Serialize each entity's Malloy expression; off unless asked for. */
   includeCode = false,
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
               ...(ctx?.filters ? { filter_params: ctx.filters } : {}),
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
         ...(includeCode && r.code ? { code: r.code } : {}),
         ...matchedTargetsFor(r, searchTexts),
         ...(r.aliases ? { aliases: r.aliases } : {}),
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
 * Which of the caller's search targets matched this row, and how well.
 *
 * Only targets that carry text can match, so a listing produces none and the
 * key is omitted rather than sent empty -- the published shape omits null
 * fields, so absence is in-contract. Ordered by target index so the response
 * reads in the order the caller wrote its targets, not in score order, which
 * would make two responses to the same request look different.
 */
function matchedTargetsFor(
   r: ResultEntity,
   searchTexts: Map<number, string>,
): { matched_targets?: Array<{ search_text: string; relevance: number }> } {
   if (!r.targetScores || r.targetScores.size === 0) return {};
   const matched = [...r.targetScores.entries()]
      .sort((a, b) => a[0] - b[0])
      .flatMap(([index, relevance]) => {
         const search_text = searchTexts.get(index);
         if (search_text === undefined) return [];
         return [
            { search_text, relevance: Math.round(relevance * 10_000) / 10_000 },
         ];
      });
   return matched.length > 0 ? { matched_targets: matched } : {};
}

/**
 * The most entities one source card carries per search target. A card is a
 * source to CHOOSE, not a schema dump: `flights` alone exposes 124 entities,
 * and returning all of them says less than returning the ten that answer the
 * question. Per TARGET rather than per card, so a broad target cannot fill a
 * card and hide a narrow one inside it -- the same failure the source window
 * below prevents between cards.
 */
const MAX_ENTITIES_PER_SOURCE_TARGET = 10;

/**
 * Window a ranked list into `limit` SOURCE cards, capping the entities each
 * one carries per target.
 *
 * `limit` counts sources because that is what the published contract says it
 * counts, and because counting entities makes targets compete for one pool:
 * with a shared budget of 4, asking for a measure alone returned 3 hits while
 * asking for it alongside a dimension and a view returned 2. The caller had
 * added questions, not removed any, so the answer to the first one should not
 * have shrunk. Bucketing by source removes the competition rather than
 * refereeing it -- targets no longer spend from the same pool, so a target's
 * answer inside a set is the answer it gives alone.
 *
 * Source order is the order sources first appear in the ranked list, which is
 * already best-first; re-sorting here could disagree with the ranking that
 * produced it.
 */
function windowBySource(
   rows: ResultEntity[],
   sourceLimit: number,
): {
   rows: ResultEntity[];
   totalSources: number;
   returnedSources: number;
   entitiesDropped: number;
} {
   const order: string[] = [];
   const bySource = new Map<string, ResultEntity[]>();
   for (const r of rows) {
      const key = r.source ?? "";
      let bucket = bySource.get(key);
      if (!bucket) {
         bucket = [];
         bySource.set(key, bucket);
         order.push(key);
      }
      bucket.push(r);
   }
   const keptSources = order.slice(0, sourceLimit);
   const kept: ResultEntity[] = [];
   let entitiesDropped = 0;
   for (const key of keptSources) {
      const perTarget = new Map<number, number>();
      for (const r of bySource.get(key) ?? []) {
         // A source row becomes the card itself, so it never spends a slot.
         if (r.kind === "source") {
            kept.push(r);
            continue;
         }
         const best = [...(r.targetScores ?? [])].sort(
            (a, b) => b[1] - a[1],
         )[0];
         // -1 buckets the rows no target claims (the lexical path carries no
         // per-target scores), so they share one cap rather than none.
         const target = best ? best[0] : -1;
         const taken = perTarget.get(target) ?? 0;
         if (taken >= MAX_ENTITIES_PER_SOURCE_TARGET) {
            entitiesDropped += 1;
            continue;
         }
         perTarget.set(target, taken + 1);
         kept.push(r);
      }
   }
   return {
      rows: kept,
      totalSources: order.length,
      returnedSources: keptSources.length,
      entitiesDropped,
   };
}

/** Cut over-long context text on a word boundary, marking that it was cut. */
/**
 * The fields every retrieval path carries from an indexed entity onto a ranked
 * one.
 *
 * Browse, semantic and lexical each wrote this projection out by hand, and a
 * field added to one was simply absent from the others: `matched_targets` never
 * reached the semantic path. That was not caught by a test, because a missing
 * field is a smaller response, not a failing one. One projection means
 * a new field arrives everywhere or nowhere.
 *
 * The ranking-specific fields stay at the call site on purpose: `score` and
 * `targetScores` are each honest on one path and not the other, and folding
 * them in here would invite exactly the mistake of publishing a lexical score
 * as a relevance.
 */
function projectEntity(
   e: Entity,
   environmentName: string,
   packageName: string,
): ResultEntity {
   return {
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
      ...(e.code ? { code: e.code } : {}),
   };
}

/**
 * The tail both ranked paths share: window by source, then serialize.
 * Semantic and lexical retrieval differ in how they SCORE, not in what happens
 * to the rows afterwards, so the ordering lives here once instead of being
 * restated at two call sites.
 *
 * Nothing is folded across sources. Two sources exposing a same-named measure
 * are two different numbers -- `in_store_sales.sales` and `online_sales.sales`,
 * or a source and a filtered extension of it -- and the response nests each
 * under its own card with its own `source_info.docs`, which is what tells a
 * caller which to use. An earlier version merged them into one row naming the
 * others; it was written when this returned a flat `results[]` with no source
 * cards to separate them, and it dropped the losing row's doc, data_type and
 * entity_id to do it.
 *
 * The envelope stays with the caller: `retrieval`, `below_cutoff_count` and
 * `retrieval_reason` are each meaningful on one path only.
 */
function finishRanked(args: {
   rows: ResultEntity[];
   max: number;
   sourceContext: Map<string, SourceContextEntry>;
   environmentName: string;
   packageName: string;
   searchTexts: Map<number, string>;
   includeCode: boolean;
}): { sources: SourceCard[]; totalSources: number; entitiesDropped: number } {
   const windowed = windowBySource(args.rows, args.max);
   const sources = toSourceResults(
      windowed.rows,
      args.sourceContext,
      args.environmentName,
      args.packageName,
      args.searchTexts,
      args.includeCode,
   );
   return {
      sources,
      totalSources: windowed.totalSources,
      entitiesDropped: windowed.entitiesDropped,
   };
}

function truncateDoc(doc: string, max: number): string {
   if (doc.length <= max) return doc;
   const cut = doc.slice(0, max);
   const lastSpace = cut.lastIndexOf(" ");
   return `${(lastSpace > max * 0.6 ? cut.slice(0, lastSpace) : cut).trimEnd()}…`;
}

/**
 * The request shape, matching the hosted retrieval API's `GetContextRequest`
 * field for field, so one caller, one skill and one eval harness serve a local
 * server and a hosted one. See the response-shape note on SourceCard for the
 * other half.
 *
 * Two behavioural divergences, both stated rather than left to be discovered:
 *
 * 1. `scopes` is optional in the published schema; here exactly one entry is
 *    REQUIRED, and it must name an environment and a package. Publisher builds
 *    its retrieval indexes per package and lazily -- a package nothing has
 *    queried has no vectors at all -- so an unscoped search would answer from
 *    whatever happened to be warm and could not tell "not modelled" from "not
 *    indexed yet". Refusing is the honest answer; guessing is not. Relaxing
 *    this later is purely additive and changes no caller.
 * 2. `target_type` is a SUPERSET, adding `join`: a model's declared joins are
 *    retrievable entities here, and an agent that cannot see one concludes the
 *    model has none. `dimensional_value` is accepted and answered with a
 *    warning, because Publisher indexes no dimensional values.
 */
const SEARCH_TARGET_TYPES = [
   "source",
   "dimension",
   "measure",
   "view",
   "dimensional_value",
   "join",
] as const;

type SearchTargetType = (typeof SEARCH_TARGET_TYPES)[number];

/**
 * Which indexed entity kinds a target selects. `view` covers named queries as
 * well, because the published shape defines that target as "pre-built analyses
 * or named queries" and a model-level named query is exactly that.
 * `dimensional_value` selects nothing: there is no value index to select from.
 */
const KINDS_BY_TARGET: Record<SearchTargetType, string[]> = {
   source: ["source"],
   dimension: ["dimension"],
   measure: ["measure"],
   view: ["view", "query"],
   join: ["join"],
   dimensional_value: [],
};

const convergedContextShape = {
   search_targets: z
      .array(
         z.object({
            target_type: z
               .enum(SEARCH_TARGET_TYPES)
               .describe(
                  "What to look for. `dimension`/`measure`/`view` match fields and the response groups them by source; `source` matches or lists sources; `join` matches a declared relationship.",
               ),
            search_text: z
               .string()
               .max(500)
               .nullish()
               .describe(
                  'Plain-English description of what you need, e.g. "the total revenue". Omit or null to enumerate this type instead of ranking it.',
               ),
         }),
      )
      .min(1)
      .describe(
         "What to find. One target per concept; several targets of different types answer one question in one call.",
      ),
   scopes: z
      .array(
         z.object({
            environment: z.string().describe("Environment name."),
            package: z.string().describe("Package name."),
            version: z
               .string()
               .nullish()
               .describe("Package version. Omit to use the served version."),
            model_path: z
               .string()
               .nullish()
               .describe('Model file within the package, e.g. "model.malloy".'),
            source: z
               .string()
               .nullish()
               .describe("Narrow to one source within the model."),
            entity_name: z
               .string()
               .nullish()
               .describe("Narrow to one entity within the source."),
         }),
      )
      .length(1)
      .describe(
         "Required, exactly one: the environment and package to search, optionally narrowed to a model, source, or entity. Call list_packages for the names.",
      ),
   filter_params: z
      .record(z.union([z.string(), z.array(z.string())]))
      .nullish()
      .describe(
         "Values for filters a source declares via #(filter), keyed by filter name.",
      ),
   user_prompt: z
      .string()
      .nullish()
      .describe(
         "The user's question that led to this call, verbatim on the first turn. Used for observability; does not affect matching.",
      ),
   include_code: z
      .boolean()
      .nullish()
      .describe(
         "Return each field's Malloy expression as `code`. Off by default: a source's #(doc) should say what a field means, and expressions are long. Turn it on to inspect what a measure actually computes -- typically once you have narrowed to the few fields you care about.",
      ),
   limit: z
      .number()
      .int()
      .min(1)
      .max(MAX_LIMIT)
      .nullish()
      .describe(
         `Sources per page (max ${MAX_LIMIT}). PREFER OMITTING: ranked search defaults to ${DEFAULT_RANKED_LIMIT}, a pure listing to ${MAX_LIMIT}.`,
      ),
   offset: z
      .number()
      .int()
      .min(0)
      .nullish()
      .describe(
         "Sources to skip, from a previous response's next_offset. Pure listings only; ranked results cannot be resumed.",
      ),
};

type GetContextParams = z.infer<z.ZodObject<typeof convergedContextShape>>;

/** One search target that carries text, kept with the index it came in at. */
interface ResolvedSearch {
   /** Position in the caller's search_targets, reported as matched_targets. */
   targetIndex: number;
   targetType: SearchTargetType;
   text: string;
   /** Entity kinds this target may match. */
   kinds: string[];
}

/**
 * The request reduced to what retrieval works in: one scope, the set of kinds
 * in play, and the searches to rank. Resolved ONCE here rather than re-derived
 * per tier, so the listing and ranked paths cannot disagree about what the
 * caller asked for.
 */
interface ResolvedRequest {
   /**
    * Only `source` targets, none with text: the catalog browse. It is the one
    * request with a deterministic order that can be resumed, so it is the only
    * one `offset` means anything on, and the one whose cards go thin.
    */
   pureSourceListing: boolean;
   environmentName: string;
   packageName: string;
   modelPath?: string;
   sourceName?: string;
   entityName?: string;
   /** Every kind any target selects. Empty only if every target was unsupported. */
   kinds: Set<string>;
   searches: ResolvedSearch[];
   /** True when no target carried search text: enumerate, do not rank. */
   listingOnly: boolean;
   /** Serialize each entity's Malloy expression as `code`. */
   includeCode: boolean;
   /** Targets naming a type this server cannot search, for a warning. */
   unsupported: SearchTargetType[];
   limit: number;
   offset: number;
}

export function resolveRequest(params: GetContextParams): ResolvedRequest {
   const scope = params.scopes[0];
   const kinds = new Set<string>();
   const searches: ResolvedSearch[] = [];
   const unsupported: SearchTargetType[] = [];

   params.search_targets.forEach((target, targetIndex) => {
      const targetKinds = KINDS_BY_TARGET[target.target_type];
      if (targetKinds.length === 0) {
         unsupported.push(target.target_type);
         return;
      }
      for (const kind of targetKinds) kinds.add(kind);
      // Whitespace-only text is not a search; treat it as an enumeration of
      // that type rather than ranking every entity against nothing.
      const text = target.search_text?.trim();
      if (text) {
         searches.push({
            targetIndex,
            targetType: target.target_type,
            text,
            kinds: targetKinds,
         });
      }
   });

   const listingOnly = searches.length === 0;
   const pureSourceListing =
      listingOnly &&
      params.search_targets.length > 0 &&
      params.search_targets.every((t) => t.target_type === "source");
   return {
      pureSourceListing,
      environmentName: scope.environment,
      packageName: scope.package,
      ...(scope.model_path ? { modelPath: scope.model_path } : {}),
      ...(scope.source ? { sourceName: scope.source } : {}),
      ...(scope.entity_name ? { entityName: scope.entity_name } : {}),
      kinds,
      searches,
      listingOnly,
      includeCode: params.include_code ?? false,
      unsupported,
      limit: params.limit ?? (listingOnly ? MAX_LIMIT : DEFAULT_RANKED_LIMIT),
      offset: params.offset ?? 0,
   };
}

/** Does this entity survive the scope's model/source/entity refinements? */
function matchesScope(
   e: { source?: string; name: string; modelPath: string; kind: string },
   request: ResolvedRequest,
): boolean {
   if (request.modelPath && e.modelPath !== request.modelPath) return false;
   if (request.sourceName && e.source !== request.sourceName) return false;
   // A source row survives an entity_name scope: it is the card the named
   // entity nests in, not a competitor for the slot.
   if (
      request.entityName &&
      e.kind !== "source" &&
      e.name !== request.entityName
   ) {
      return false;
   }
   return true;
}

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

/**
 * What the compiled model knows about a field that the stable SourceInfo does
 * not: the expression it was defined by. `code` is the authored text
 * ("sale_price.sum()"); `aliasOf` is set only when the whole definition is a
 * reference to a sibling field of the same source.
 */
interface FieldProvenance {
   code?: string;
   aliasOf?: string;
}

/**
 * Read per-field provenance for one source out of the compiled model IR.
 *
 * Keyed by field name so the caller can join it onto the curated
 * `SourceInfo.schema.fields` it already walks. The IR is deliberately
 * uncurated (see Model.getModelDef), so it is used as a LOOKUP for fields the
 * curated list already admitted, never as a field list of its own.
 *
 * The alias rule is `e.node === "field"` with a SINGLE-segment path, and that
 * restriction carries the whole correctness argument. It is not theoretical:
 * in the bundled storefront model `order_items.category`, `.brand` and
 * `.region` are all bare field references -- to `["products","category"]`,
 * `["products","brand"]` and `["regions","region"]`. Those traverse a join to
 * reach a DIFFERENT column, and folding them into their target is exactly the
 * error this guard exists to prevent. Only a single-segment path names a
 * sibling field of the same source.
 */
function readFieldProvenance(
   modelDef: ModelDef | undefined,
   sourceName: string,
): Map<string, FieldProvenance> {
   const provenance = new Map<string, FieldProvenance>();
   const contents = modelDef?.contents;
   if (!contents) return provenance;
   // Match on the ACTIVE name -- `as` when a rename set one, else `name`.
   // That is what `to_stable` puts in SourceInfo, and it is what the caller
   // joins on. Keying the raw `name` here silently gave every renamed field
   // no provenance at all: `include { rename: b is a }` compiles to
   // `{name: "a", as: "b"}`, so the map said "a" while the lookup asked "b".
   const active = (v: { name?: string; as?: string }) => v.as ?? v.name;
   const entry = Object.values(contents).find(
      (candidate) =>
         active(candidate as { name?: string; as?: string }) === sourceName,
   ) as { fields?: unknown[] } | undefined;
   if (!entry?.fields) return provenance;
   for (const raw of entry.fields) {
      const field = raw as {
         name?: string;
         as?: string;
         code?: string;
         e?: { node?: string; path?: string[] };
      };
      const fieldName = active(field);
      if (!fieldName) continue;
      const referent =
         field.e?.node === "field" && field.e.path?.length === 1
            ? field.e.path[0]
            : undefined;
      provenance.set(fieldName, {
         ...(field.code ? { code: field.code } : {}),
         // A self-reference cannot occur in valid Malloy; guarded anyway,
         // because recording one would make a field its own alias and drop it.
         ...(referent && referent !== fieldName ? { aliasOf: referent } : {}),
      });
   }
   return provenance;
}

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
 * Indexing the join alone is not enough, even though the target source is
 * already indexed on its own: the caller needs the PATH, and the path cannot
 * be derived from anything else the response carries. The stable `JoinInfo`
 * inlines the target's schema without naming the target source (#1100), so an
 * agent holding only a join entity knows a relationship exists with no way to
 * learn what it reaches -- while the tool description tells it to "traverse a
 * join as joinName.fieldName". So each reachable field is indexed as an entity
 * in its own right, reporting its traversal as `join_path`.
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
      // The compiled IR, for the one thing SourceInfo cannot carry: a field's
      // expression. Optional-chained on the same grounds as getSources above
      // -- a spec's model stand-in implements only the accessors it needs --
      // and a model that failed to compile has none. Either way the fields
      // still index, just without provenance.
      const modelDef = model.getModelDef?.();

      for (const sourceInfo of sourceInfos) {
         const sourceName = sourceInfo.name;
         const provenance = readFieldProvenance(modelDef, sourceName);
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
                  filters: (apiSource.filters ?? []).flatMap((filter) =>
                     filter.name && filter.type
                        ? [
                             {
                                name: filter.name,
                                type: filter.type,
                                ...(filter.dimension
                                   ? { dimension: filter.dimension }
                                   : {}),
                                ...(filter.required ? { required: true } : {}),
                             },
                          ]
                        : [],
                  ),
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
            // A view's definition is a query pipeline rather than a scalar
            // expression, and nothing folds or displays it, so provenance is
            // read for dimensions and measures only.
            const fieldProvenance =
               field.kind === "view" ? undefined : provenance.get(field.name);
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
               ...(fieldProvenance?.code ? { code: fieldProvenance.code } : {}),
               ...(fieldProvenance?.aliasOf
                  ? { aliasOf: fieldProvenance.aliasOf }
                  : {}),
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
   filters: SourceContextFilter[];
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
 * Collapse a field that is DEFINED AS another field of the same source into
 * it, keeping the documented spelling and reporting the rest in `aliases`.
 *
 * A model that renames a physical column without hiding the original leaves
 * both in the schema: `dimension: site is SITE` puts `site` beside `SITE` --
 * one column indexed twice, both competing for the same scarce slots.
 *
 * Detection is EXACT, read off the compiled model: `aliasOf` is set only when
 * a field's entire definition is a reference to a single-segment sibling name
 * (see readFieldProvenance). This replaces a heuristic that matched humanized
 * names, which could not tell `site is SITE` from a derivation that happened
 * to humanize the same way. The comment here used to justify that heuristic by
 * saying the expression was unavailable. It is available: the stable
 * `Malloy.DimensionInfo` does not carry it, but the compiled `ModelDef` does,
 * and Publisher holds one (see Model.getModelDef).
 *
 * Because a reference is proof rather than a guess, the differing-doc escape
 * hatch is gone. The old rule kept two spellings apart when each carried its
 * own `#(doc)`, since a doc was the only evidence they might be two concepts.
 * A proven reference settles it: they ARE one column, and two docs on one
 * column is a modelling mistake, not a second concept. The documented
 * spelling still wins, so the doc a modeller wrote is the one returned.
 *
 * The better fix remains in the model: `include { internal: ... }` hides the
 * raw column outright and the indexer honours it already, because the compiler
 * drops non-public fields before this code sees them (see the access-modifier
 * spec). This is what the tool can do for models that have not done that.
 */
function collapseAliases(entities: Entity[]): Entity[] {
   // Every same-source field a reference could name. Dimensions and measures
   // only: a source is its own namespace, a join names a relationship rather
   // than a column, and a view is a pipeline. A joined field is indexed under
   // its dotted path and belongs to the source it is reached FROM, so it can
   // never be a same-source sibling -- and its own expression may reference a
   // leaf name that collides with one of that source's own fields, which is
   // exactly the fold that must not happen.
   const foldable = (e: Entity) =>
      (e.kind === "dimension" || e.kind === "measure") && !e.joinPath;
   const key = (e: Entity) => `${e.source ?? ""}\x00${e.name}`;
   const byName = new Map<string, Entity>();
   for (const e of entities) if (foldable(e)) byName.set(key(e), e);

   /** The field this one is defined as, when the index holds it. */
   const referentOf = (e: Entity): Entity | undefined => {
      if (!e.aliasOf || !foldable(e)) return undefined;
      const target = byName.get(`${e.source ?? ""}\x00${e.aliasOf}`);
      // The referent has to be a field this index actually holds, and it may
      // not be: `include { internal: SITE }` hides the raw column from the
      // public schema while `site` still references it. That is the model
      // doing the right thing -- `site` is then the only spelling and there
      // is nothing to fold. A rename moves a field's active name without
      // rewriting its siblings' expressions, which lands here the same way.
      if (!target || target === e) return undefined;
      // A measure defined over a dimension is not another spelling of it.
      return target.kind === e.kind ? target : undefined;
   };

   // Walk each chain to its root, so `a is b` and `b is c` land in ONE group
   // rather than making `b` both a survivor and a dropped row -- which would
   // leave `a` reporting an alias of a field no longer in the index.
   const rootOf = new Map<Entity, Entity>();
   const groups = new Map<Entity, Entity[]>();
   for (const e of entities) {
      if (!foldable(e) || !e.aliasOf) continue;
      const seen = new Set<Entity>([e]);
      let node = e;
      for (;;) {
         const next = referentOf(node);
         // A cycle cannot occur in valid Malloy; the guard costs nothing and
         // turns a malformed model into "no fold" instead of a hang.
         if (!next || seen.has(next)) break;
         seen.add(next);
         node = next;
      }
      if (node === e) continue;
      rootOf.set(e, node);
      const group = groups.get(node);
      if (group) group.push(e);
      else groups.set(node, [e]);
   }

   const dropped = new Map<string, Entity>();
   for (const [root, refs] of groups) {
      const members = [root, ...refs];
      // The NAME that survives is an authored alias, never the raw column it
      // points at: `dimension: site is SITE` says the modeller wants `site`
      // used, and it is the spelling that still works once they hide the raw
      // one with `include { internal: SITE }`. Direction is the thing the old
      // name-matching rule could not know, so it had to guess from casing.
      // Among several authored names (a chain, or two names for one column)
      // prefer a documented one, then be deterministic.
      const candidates = refs.length > 0 ? refs : members;
      const keep = [...candidates].sort((a, b) => {
         if (Boolean(b.embedDoc) !== Boolean(a.embedDoc)) {
            return b.embedDoc ? 1 : -1;
         }
         return a.name.localeCompare(b.name);
      })[0];
      // The DOC survives independently of the name. A raw column that carries
      // the only `#(doc)` in the group would otherwise lose it, and `aliases`
      // carries names, not text. Two members documented DIFFERENTLY is a
      // modelling mistake rather than two concepts -- the reference proves one
      // column -- so the survivor's own doc wins and the other is dropped.
      if (!keep.embedDoc) {
         const donor = members.find((e) => e !== keep && e.embedDoc);
         if (donor) {
            keep.doc = donor.doc;
            keep.embedDoc = donor.embedDoc;
         }
      }
      const folded = members.filter((e) => e !== keep);
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
         ...(gates?.filters.length ? { filters: gates.filters } : {}),
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
 * The converged tool's description. Same budget and same ordering rule as the
 * one below it: contract rules first, reference last, because a tail-truncating
 * client drops whatever is last. See server.protocol.spec.ts.
 */
const GET_CONTEXT_DESCRIPTION = `Retrieve the entities in a Malloy package most relevant to a plain-English question.

## Contract rules
- Use the names it returns verbatim; never invent one that is not in the results.
- One call answers: describe the fields you need as search_targets; each matching source returns with those fields nested. No drill-down call.
- scopes is REQUIRED: exactly one, naming an environment and package. list_packages lists them.
- Read warnings and any error/stale field before trusting a number.
- A source's joins list is complete: empty means it declares none, so write that relationship inline.
- Read a source's doc before querying: it carries grain and population rules its fields do not.
- authorize means gated: supply the givens it names or the query is denied.

## Parameters
search_targets: one per concept, {target_type, search_text}; target_type is source|dimension|measure|view|join|dimensional_value, omitting search_text enumerates that type. scopes: {environment, package} + optional model_path, source, entity_name. limit caps sources (max 150). offset pages a listing. filter_params sets #(filter) values. user_prompt: the question asked. include_code adds each field's expression as code.

## Response
sources[], best first. source_info: resource_id (environment/package/model_path/source) -> execute_query's environmentName/packageName/modelPath/sourceName; docs (… = truncated), one_line_summary, complete joins, givens, authorize (report-only), filter_params. entities[] nest under it: name, entity_type (dimension/measure/view/join/query), description, data_type, relationship (fan-out), join_path, aliases, matched_targets, relevance, entity_id. A joined field's name IS its dotted path; use it verbatim.
ranking, returned of total_available sources, next_offset on a listing, warnings[].
Semantic fills relevance: no sources = nothing cleared the floor; below_cutoff_count of total_entities rejected. "lexical" adds retrieval_reason; only "indexing" is worth a retry.

## Example
{"search_targets":[{"target_type":"measure","search_text":"total revenue"}],"scopes":[{"environment":"examples","package":"storefront"}]}`;

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
 * Tier 3 (enumerate) and tier 4 (rank) over ONE package, driven by an already
 * resolved request, so a ranking decision and a request-parsing decision stay
 * separately reviewable. Retrieval runs over the package's model entities
 * (sources, views, dimension/measure fields, joins, named queries), semantic
 * when an embedding provider is configured and lexical (lunr/BM25) otherwise.
 * The entity index is built once per Package and cached (see getPackageIndex),
 * rebuilding automatically when the package reloads.
 *
 * There is no progressive-discovery tier here any more: the catalog is
 * `list_packages`, and every request that reaches this names one package,
 * because `scopes` requires it.
 */
async function runContextQuery(
   request: ResolvedRequest,
   environmentStore: EnvironmentStore,
   extraWarnings: string[] = [],
): Promise<ReturnType<typeof jsonResource>> {
   const { environmentName, packageName, sourceName } = request;
   const max = request.limit;
   // One lookup from target index back to the text the caller wrote, so
   // matched_targets can name a target without re-walking the request.
   const searchTextsByIndex = new Map(
      request.searches.map((search) => [search.targetIndex, search.text]),
   );
   const byTargetIndex = new Map(
      request.searches.map((search) => [search.targetIndex, search]),
   );
   logger.info("[MCP Tool getContext] Retrieving context", {
      environmentName,
      packageName,
      sourceName,
      searches: request.searches.length,
      listingOnly: request.listingOnly,
   });

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
   // call get_status is weaker than saying it here, where it is
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
         staleNote = `This package is STALE: its most recent reload failed to compile at ${stale.failedAt}, so these names, and any query you run against them, come from the model compiled BEFORE that save, not from the files on disk. Fix the model and call reload_package; get_status has the compile error.`;
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
      const warnings = [staleNote, ...extraWarnings, ...extra].filter(
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

   /**
    * The two cuts a ranked response can make, each named in its own unit. They
    * have different remedies -- one is the page size, the other is per source
    * -- and the old single warning reported both as "entities" while the
    * envelope beside it counted sources.
    */
   const sourceCutWarning = (returned: number, matched: number) =>
      matched > returned
         ? `Returned ${returned} of ${matched} matching sources. Raise limit (max ${MAX_LIMIT}) or narrow with scopes to see the rest.`
         : undefined;
   /**
    * A browse that stopped early. Its remedy is NOT the ranked one: a listing
    * has a resumable order, so the rest is one `offset` away and raising the
    * limit is the wrong advice -- it caps at 150 either way, and paging is
    * what actually reaches source 151.
    */
   const listingPageWarning = (returned: number, matched: number) =>
      matched > returned
         ? `Returned ${returned} of ${matched} sources in scope. Pass next_offset back as offset for the next page, or narrow with search_text or more targeted scopes.`
         : undefined;
   const entityCutWarning = (dropped: number) =>
      dropped > 0
         ? `${dropped} further ${dropped === 1 ? "entity" : "entities"} matched but were cut at ${MAX_ENTITIES_PER_SOURCE_TARGET} per source per target. Scope to one source to list all of its fields.`
         : undefined;

   // Tier 3: nothing to rank -> enumerate what the targets name.
   if (request.listingOnly) {
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
      const inScope = Array.from(byId.values()).filter(
         (e) =>
            // A scoped source's own row always survives: it becomes the
            // card rather than competing for an entity slot.
            ((sourceName && e.kind === "source" && e.source === sourceName) ||
               request.kinds.has(e.kind)) &&
            matchesScope(e, request),
      );
      // The limit caps entities. A drill-down's source row becomes the
      // card rather than an entity, so it does not spend a slot; a
      // package listing has only source rows, so there the cap is on
      // cards, which is what the listing is made of.
      // A browse is the one listing with a stable order, so it is the one
      // that can be resumed. Skipping into any other shape would silently
      // drop rows whose position is not reproducible between two calls.
      const paged = request.pureSourceListing
         ? inScope.slice(request.offset)
         : inScope;
      let entityBudget = request.limit;
      const capped = paged.filter((e) => {
         if (sourceName && e.kind === "source") return true;
         if (entityBudget <= 0) return false;
         entityBudget -= 1;
         return true;
      });
      /**
       * Rows the cap actually spends from, which is the unit both sides of the
       * truncation warning have to be counted in.
       *
       * A scoped source's own row never spends a slot, so it is EXCLUDED from
       * both counts rather than subtracted from one. Subtracting assumed the
       * row was there: scope to a source that does not exist and `capped` is
       * empty, so `capped.length - 1` was -1, and the warning fired claiming
       * "Returned -1 of 0 matching entities" on a result that lost nothing.
       */
      const spendable = (rows: typeof inScope) =>
         sourceName
            ? rows.filter((e) => e.kind !== "source").length
            : rows.length;
      const cappable = spendable(inScope);
      // The overview is where an agent decides how to combine sources,
      // so each one states its relationships here rather than making
      // that a second call. toSourceResults carries the complete joins
      // list onto every entry, so empty means none declared.
      const sources = toSourceResults(
         capped.map((e) => projectEntity(e, environmentName, packageName)),
         sourceContext,
         environmentName,
         packageName,
         new Map(),
         request.includeCode,
      );
      // A listing is deterministic catalog order, not a ranking, and
      // Publisher has no query-usage signal to fill the hosted API's
      // `prominence` with, so it names the ordering and omits the
      // score rather than inventing a number nobody should rank on.
      // Both counts are in SOURCES, the unit the payload is made of. A
      // drill-down is one source's card, so it is 1-of-1 however many
      // entities nest inside it; the entity cap is reported separately.
      // next_offset is present only while sources remain past this page, and
      // only on the browse, matching where `offset` is honoured.
      const consumed = request.offset + sources.length;
      const listingEnvelope = {
         ranking: "prominence" as const,
         total_available: sourceName
            ? Math.min(inScope.length, 1)
            : inScope.length,
         returned: sources.length,
         ...(request.pureSourceListing && consumed < inScope.length
            ? { next_offset: consumed }
            : {}),
      };
      // An empty enumeration is ambiguous to an agent: "no data here" and
      // "the package exposes nothing" look identical. The package DID
      // load (a failed load throws out of getPackageIndex above), so an
      // empty result means its models expose no sources: a curation gap
      // (explores/export {}), not an empty database. Say so, only in the
      // empty case, so the populated payload stays byte-identical.
      // `kinds` is empty only when EVERY target named a type this server does
      // not index, and unsupportedTargetWarnings has already said so exactly.
      // Adding the curation line there contradicts it and misdiagnoses a
      // healthy package: a `dimensional_value` search against storefront
      // reported that its seven sources were a curation gap.
      if (sources.length === 0 && !sourceName && request.kinds.size > 0) {
         return jsonResource(uri, {
            sources,
            ...listingEnvelope,
            ...warningsFor(
               "This package loaded but exposes no sources. That is a curation gap, not an empty database: check the package's explores list and export {} statements, and call get_status for load errors and stale packages.",
            ),
         });
      }
      return jsonResource(uri, {
         sources,
         ...listingEnvelope,
         ...warningsFor(
            // A pure browse pages; every other listing shape is capped in
            // entities and says so.
            request.pureSourceListing
               ? listingPageWarning(sources.length, inScope.length)
               : truncationWarning(spendable(capped), cappable),
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
   // A drill-down is confined to one source, so the scan needs no over-fetch
   // to reach a spread of source cards.
   const scoped = Boolean(sourceName);
   /** Ranked but NOT yet collapsed or windowed; finishRanked does both. */
   let semanticRanked: ResultEntity[] | undefined;
   let belowCutoffCount = 0;
   // The denominator belowCutoffCount is read against; see
   // SemanticSearchResult. Undefined on the lexical path, where there
   // is no floor and so no count to anchor.
   let totalEntities: number | undefined;
   // Why a configured server answered lexically. Without it "lexical"
   // is a dead end: an agent cannot tell a cold index, which clears in
   // seconds and is worth retrying, from a down provider, which is not.
   let retrievalReason: RetrievalReason | undefined;
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
            // One pass per target, merged on score. A max ACROSS passes is
            // meaningful here and only here: cosine is an absolute scale,
            // so 0.7 from the measure target and 0.7 from the dimension
            // target mean the same thing. (The lexical path below has to
            // normalize first, because lunr scores are relative to their
            // own query.) The next commit collapses these passes into one
            // batched embed and one scan; the merge rule does not change.
            const merged = new Map<string, ResultEntity>();
            const matchedTargets = new Map<string, Map<number, number>>();
            let searchFailure: RetrievalReason | undefined;
            let unionTotalEntities: number | undefined;
            let unionBelowCutoff: number | undefined;
            {
               // ONE call for every target: it batches the embeddings into a
               // single provider request and scores them in a single pass
               // over the vector cache, returning each hit's per-target
               // scores. The raw text embeds better than the lunr-sanitized
               // form; sanitize() only exists to strip lunr operators.
               const semantic = await trySemanticSearch({
                  db: environmentStore.storageManager.getDuckDbConnection(),
                  provider,
                  pkg: pkgIndex.pkg,
                  environmentName,
                  packageName,
                  entities: Array.from(byId.values()),
                  queries: request.searches.map((search) => ({
                     targetIndex: search.targetIndex,
                     text: search.text,
                  })),
                  // Over-fetch, because `max` counts SOURCE CARDS while
                  // this limit counts entity ROWS, and windowBySource admits
                  // up to MAX_ENTITIES_PER_SOURCE_TARGET rows per source per
                  // target. Fetching exactly `max` rows lets them all land in
                  // one source and return a single card where `max` were
                  // asked for. A drill-down is confined to one source, so
                  // there the extra rows are waste.
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
                           ...projectEntity(e, environmentName, packageName),
                           score: Math.round(hit.score * 10_000) / 10_000,
                           targetScores: hit.targetScores,
                        },
                     ];
                  });
                  for (const row of ranked) {
                     // A target only claims the kinds it selects, so drop the
                     // targets that cannot own this row and, with them, any
                     // row no surviving target claims. That is what stops a
                     // `measure` target surfacing a dimension which happened
                     // to embed near its text.
                     const claimed = new Map<number, number>();
                     for (const [targetIndex, score] of row.targetScores ??
                        []) {
                        const search = byTargetIndex.get(targetIndex);
                        if (search && search.kinds.includes(row.kind)) {
                           claimed.set(targetIndex, score);
                        }
                     }
                     if (claimed.size === 0) continue;
                     const key = entityRowKey(
                        row.kind,
                        row.source ?? "",
                        row.name,
                     );
                     // Rank on the best target that may actually claim it,
                     // not on the scan's max across every target.
                     merged.set(key, {
                        ...row,
                        score: Math.max(...claimed.values()),
                     });
                     matchedTargets.set(key, claimed);
                  }
                  // The denominator counts the package's entities, not the
                  // query's hits, so it is the same whichever target asked.
                  unionTotalEntities = semantic.totalEntities;
                  unionBelowCutoff = semantic.belowCutoffCount;
               } else {
                  searchFailure = REASON_BY_UNAVAILABLE[semantic.unavailable];
               }
            }
            if (merged.size > 0 || searchFailure === undefined) {
               const ranked = [...merged.entries()]
                  .sort((a, b) => (b[1].score ?? 0) - (a[1].score ?? 0))
                  .map(([key, row]) => ({
                     ...row,
                     targetScores: matchedTargets.get(key),
                  }));
               // Collapse, windowing and serialization are finishRanked's,
               // shared with the lexical path so the two cannot drift.
               semanticRanked = ranked;
               totalEntities = unionTotalEntities;
               // An entity that cleared NO target's floor is below the
               // cutoff. Derived from the union rather than taken from one

               // Straight from the scan, which counts entities whose BEST
               // score across every target fell under the floor. Deriving it
               // from the returned rows would fold the page limit into it and
               // report a crowded-out entity -- one that cleared the floor and
               // simply did not fit -- as rejected, which is the opposite of
               // what this number tells a caller.
               belowCutoffCount = unionBelowCutoff ?? 0;
            } else {
               retrievalReason = searchFailure;
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
                  error: error instanceof Error ? error.message : String(error),
               },
            );
         }
      }
   }

   if (semanticRanked !== undefined) {
      const { sources, totalSources, entitiesDropped } = finishRanked({
         rows: semanticRanked,
         max,
         sourceContext,
         environmentName,
         packageName,
         searchTexts: searchTextsByIndex,
         includeCode: request.includeCode,
      });
      return jsonResource(uri, {
         sources,
         ranking: "relevance" as const,
         total_available: totalSources,
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
         // Each cut in its own unit: `limit` drops whole sources, the
         // per-source cap drops entities inside the ones it kept.
         ...warningsFor(
            // Counted in CARDS, the same unit `returned` reports, so the
            // two cannot disagree.
            sourceCutWarning(sources.length, totalSources),
            entityCutWarning(entitiesDropped),
         ),
      });
   }

   // One lunr pass per target, merged. Each target's hits are normalized
   // against ITS OWN top hit before merging, which is what makes two
   // targets' scores comparable at all: raw lunr scores are relative to
   // the query that produced them. All targets share one index, so the
   // IDF corpus is the same and the normalization is the only correction
   // needed.
   const bestByRef = new Map<string, Map<number, number>>();
   for (const search of request.searches) {
      const sanitized = sanitize(search.text);
      if (!sanitized) continue;
      let targetHits: lunr.Index.Result[] = [];
      try {
         targetHits = index.search(sanitized);
      } catch (error) {
         logger.warn("[MCP Tool getContext] lunr search failed", {
            query: search.text,
            error: error instanceof Error ? error.message : String(error),
         });
         continue;
      }
      const top = targetHits[0]?.score ?? 0;
      for (const hit of targetHits) {
         const entity = byId.get(hit.ref);
         // A target only claims the kinds it selects, so a `measure`
         // target never surfaces a dimension that happened to match.
         if (!entity || !search.kinds.includes(entity.kind)) continue;
         const score = top > 0 ? hit.score / top : 0;
         const scores = bestByRef.get(hit.ref) ?? new Map<number, number>();
         scores.set(
            search.targetIndex,
            Math.max(scores.get(search.targetIndex) ?? 0, score),
         );
         bestByRef.set(hit.ref, scores);
      }
   }
   // Already normalized per target and merged, so this is the ranked list
   // rather than raw lunr output -- no fake lunr Result needs constructing
   // to carry it.
   const ranking = [...bestByRef.entries()]
      .flatMap(([ref, targetScores]) => {
         const e = byId.get(ref);
         // Defensive: skip a ref missing from the entity map.
         if (!e) return [];
         // Drill-down: narrow to one source when sourceName is set.
         if (sourceName && e.source !== sourceName) return [];
         if (!matchesScope(e, request)) return [];
         // The row ranks on its best target, the same MAX-over-facets rule
         // the semantic path uses one level down.
         const score = Math.max(...targetScores.values());
         return [{ e, score, targetScores }];
      })
      .sort((a, b) => b.score - a.score);

   const scored: ResultEntity[] = ranking.map(({ e }) => ({
      ...projectEntity(e, environmentName, packageName),
      // targetScores is deliberately NOT carried on this path. It would reach
      // the wire as matched_targets[].relevance, whose relevance is required
      // and would therefore publish a lexical score -- the exact number this
      // path withholds from the entity's own `relevance`, because a lunr score
      // is relative to its own query and comparing two of them means nothing.
      // Naming the matched target is not worth contradicting that; a semantic
      // response answers both. The normalized lexical score itself is not
      // carried either: the only thing that ever read it was the sibling
      // grouping this no longer does.
   }));
   // This path carries no per-target scores, so its rows share one cap per
   // source rather than one each; everything else about the tail is the
   // semantic path's, which is why it is the same function.
   const { sources, totalSources, entitiesDropped } = finishRanked({
      rows: scored,
      max,
      sourceContext,
      environmentName,
      packageName,
      searchTexts: searchTextsByIndex,
      includeCode: request.includeCode,
   });
   const envelope = {
      sources,
      ranking: "relevance" as const,
      total_available: totalSources,
      returned: sources.length,
   };
   const lexicalWarnings = warningsFor(
      sourceCutWarning(sources.length, totalSources),
      entityCutWarning(entitiesDropped),
   );
   return jsonResource(
      uri,
      configured
         ? {
              ...envelope,
              retrieval: "lexical",
              ...(retrievalReason ? { retrieval_reason: retrievalReason } : {}),
              ...lexicalWarnings,
           }
         : { ...envelope, ...lexicalWarnings },
   );
}

/**
 * A target type this server cannot search, said plainly with the remedy that
 * works. Publisher indexes the model, not the values inside it, so a
 * `dimensional_value` target has nothing to match -- and the answer is the one
 * the analysis skill already gives: target the dimension, then read its
 * distinct values with a query.
 */
function unsupportedTargetWarnings(request: ResolvedRequest): string[] {
   if (request.unsupported.length === 0) return [];
   const named = [...new Set(request.unsupported)].join(", ");
   return [
      `No index for target_type ${named}: this server indexes the semantic model, not the values stored in it. Target the dimension instead, then read its distinct values with execute_query (e.g. run: source -> { group_by: the_dimension }).`,
   ];
}

const LIST_ENVIRONMENTS_DESCRIPTION = `List what this Publisher serves: every environment, and the packages in each.

Call this when you do not already know an environment and package name. get_context requires one of each in its \`scopes\`, and those names come from here; never guess them.

Each package reports \`name\`, its \`description\` where it has one, and two health facts that are otherwise invisible:
- \`error\` with no \`stale\`: the package FAILED to load. It is not queryable, and it would otherwise simply be missing, which reads as "does not exist".
- \`error\` with \`stale: true\`: the package IS serving and answering, but its most recent reload failed to compile, so its names and any numbers you get from it come from the model compiled BEFORE that save. Fix the model and call reload_package.

An environment carries \`error\` on the same terms: its packages could not be listed at all (an unreachable store, expired credentials), so its \`packages\` is empty because the listing FAILED, not because the environment is empty. Without that field the two are the same payload. Report the error rather than telling the user there is nothing there.

Takes no arguments.`;

/**
 * The environment and package catalog, which `get_context` deliberately does
 * not carry: its `search_targets` are required and its `scopes` name a package,
 * so something has to answer "which packages are there" first. The hosted
 * surface splits the same way, for the same reason -- its scope parameters are
 * required and a sibling tool supplies them.
 *
 * This also inherits the job the flat tool's environment listing did: a package
 * that failed to load is ABSENT from a plain listing, which reads as "does not
 * exist" rather than "is broken". Reporting it with its error is the only place
 * that distinction is visible outside get_status.
 */
export function registerListPackagesTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool(
      "list_packages",
      LIST_ENVIRONMENTS_DESCRIPTION,
      {},
      async () => {
         const uri = buildMalloyUri({}, "list-environments");
         try {
            const environments = await environmentStore.listEnvironments();
            const results = [];
            for (const env of environments) {
               const name = env.name;
               if (!name) continue;
               let packages: unknown[] = [];
               let environmentError: string | undefined;
               try {
                  const environment = await environmentStore.getEnvironment(
                     name,
                     false,
                  );
                  const staleErrors = environment.getStaleCompileErrors();
                  packages = (await environment.listPackages()).map((pkg) => {
                     const stale = pkg.name
                        ? staleErrors.get(pkg.name)
                        : undefined;
                     return {
                        name: pkg.name,
                        ...(pkg.description
                           ? { description: pkg.description }
                           : {}),
                        ...(stale && { error: stale.message, stale: true }),
                     };
                  });
                  for (const [
                     failed,
                     message,
                  ] of environment.getFailedPackages()) {
                     packages.push({ name: failed, error: message });
                  }
               } catch (error) {
                  // One unreachable environment must not hide the others: the
                  // point of this tool is to say what IS there. But an empty
                  // list is what a genuinely empty environment looks like, so
                  // the failure has to be reported as well as survived --
                  // otherwise an unreachable store reads as "models nothing",
                  // which is the same wrong conclusion `error` exists to stop
                  // a failed PACKAGE from producing.
                  environmentError =
                     error instanceof Error ? error.message : String(error);
                  logger.warn(
                     "[MCP Tool listPackages] listing packages failed",
                     {
                        environmentName: name,
                        error: environmentError,
                     },
                  );
                  packages = [];
               }
               results.push({
                  name,
                  ...(environmentError ? { error: environmentError } : {}),
                  packages,
               });
            }
            return jsonResource(uri, { environments: results });
         } catch (error) {
            logger.warn("[MCP Tool listPackages] listing failed", {
               error: error instanceof Error ? error.message : String(error),
            });
            return contextError(uri, "environments", error);
         }
      },
   );
}

export function registerGetContextTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool(
      "get_context",
      GET_CONTEXT_DESCRIPTION,
      convergedContextShape,
      async (params: GetContextParams) => {
         const request = resolveRequest(params);
         if (request.offset > 0 && !request.pureSourceListing) {
            // Refused rather than ignored. A ranked response has no
            // reproducible order to resume from, so honouring the offset
            // would drop rows the caller could never get back, and dropping
            // the offset silently would hand them page 1 while they believed
            // they were reading page 2.
            return contextError(
               buildMalloyUri(
                  {
                     environment: request.environmentName,
                     package: request.packageName,
                  },
                  "get-context",
               ),
               `${request.environmentName}/${request.packageName}`,
               // InvalidArgumentError, not a bare Error. This is a caller
               // mistake with a stated fix, and an unclassified throw lands on
               // classifyToolError's internal-fault branch -- so a deliberate
               // refusal came back as "An unexpected internal error occurred
               // during getContext.: Invalid offset: ...", which reads as a
               // server bug to report and invites the retry that cannot work.
               new InvalidArgumentError(
                  "Invalid offset: paging works only on a pure source listing (every search_target of type `source`, none with search_text), which is the one response with a resumable order. Fix: drop `offset`, and narrow with search_text or scopes instead.",
               ),
            );
         }
         return runContextQuery(
            request,
            environmentStore,
            unsupportedTargetWarnings(request),
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
