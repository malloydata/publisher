// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Relationship } from "@malloydata/malloy-interfaces";
import { HttpRequestError } from "../../service/http_retry";
import { classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import type { SemanticUnavailableReason } from "./embedding_index";
import { foldRelevance } from "./get_context_assembly";
import type { StageError } from "./get_context_llm";
import type {
   CardState,
   PipelineContext,
   Unavailable,
} from "./get_context_pipeline";
import { MAX_LIMIT } from "./get_context_request";
import type {
   Entity,
   ResolvedRequest,
   ResultEntity,
   RetrievalReason,
   SourceContextAuthorize,
   SourceContextEntry,
   SourceContextFilter,
   SourceContextGiven,
   SourceContextJoin,
} from "./get_context_types";
import { sourceContextKey } from "./package_entities";
import { round4 } from "./retrieval_shared";
import type { StoredSourceSummary } from "./source_summaries";

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
 * The index's internal reasons, mapped to the wire vocabulary. "error" is
 * widened to "provider-error" because from the caller's side that is what it
 * is: the embedding endpoint failed, not this tool.
 */
export const REASON_BY_UNAVAILABLE: Record<
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
 *    `summary`, `prominence`, `values`, `values_indexed`. (`match_reason`
 *    rides on `matched_targets` only when refine ran and gave one.)
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
   /**
    * The LLM's one-line summary of the source when one is stored; otherwise the
    * first line of its doc (absent when undocumented).
    */
   one_line_summary?: string;
   /**
    * The LLM's full summary of the source. Only when one is stored AND the
    * request pins the source or a source search matched at most one source.
    */
   summary?: string;
   docs?: string;
   givens?: SourceContextGiven[];
   accessFilter?: SourceContextAuthorize[];
   /** The `#(authorize)` locks — see `SourceContextEntry.authorize`. */
   authorize?: SourceContextAuthorize[];
   filter_params?: SourceContextFilter[];
   /** Publisher extension. Complete, so `[]` means "declares none". */
   joins: SourceContextJoin[];
}

interface MatchedTarget {
   search_text: string;
   relevance: number;
   /** Refine's one-sentence reason. Present only when refine ran and gave one. */
   match_reason?: string;
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
    * The entity's definition, present when the caller passed `include_code` or
    * pinned `scopes[].entity_name`. A dimension or measure carries its Malloy
    * expression; a view carries its definition as written in the model file. A
    * physical column has neither, so absence under that flag means the field
    * is raw rather than derived.
    */
   code?: string;
   relationship?: Relationship;
   /** The join traversal reaching this field, when it is not the source's own. */
   join_path?: string;
   /** Which search targets matched this entity, and how well. */
   matched_targets?: MatchedTarget[];
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
export function toSourceResults(
   results: ResultEntity[],
   sourceContext: Map<string, SourceContextEntry>,
   environmentName: string,
   packageName: string,
   /** Search texts by target index, for matched_targets. Empty on a listing. */
   searchTexts: Map<number, string> = new Map(),
   /** Serialize each entity's Malloy expression; off unless asked for. */
   includeCode = false,
   /** Deny-all sources collectEntities already dropped; refuse to mint a bare card for one. */
   droppedSources: Set<string> = new Set(),
   /** Stored LLM source summaries, when the package has any. */
   summaries?: SummaryView,
): SourceCard[] {
   const bySource = new Map<string, SourceCard>();

   const cardFor = (
      name: string | undefined,
      modelPathFallback: string,
   ): SourceCard | undefined => {
      // Type narrowing only: collectEntities excludes the one entity kind
      // that can lack a source, so no ranked row reaches here nameless.
      if (!name) return undefined;
      // Defense in depth: collectEntities already keeps every dropped source's
      // name off every entity's `source` field, but this is the one place that
      // can mint a card out of a bare name, so it is where a future path that
      // forgets the drop gets caught instead of leaking the name and shape.
      const key = sourceContextKey(modelPathFallback, name);
      if (droppedSources.has(key)) {
         return undefined;
      }
      let entry = bySource.get(key);
      if (!entry) {
         const ctx = sourceContext.get(key);
         const stored = summaries?.stored.get(key);
         const oneLine = stored?.oneLineSummary ?? ctx?.oneLineSummary;
         entry = {
            source_info: {
               resource_id: {
                  environment: environmentName,
                  package: packageName,
                  model_path: modelPathFallback,
                  source: name,
               },
               ...(oneLine ? { one_line_summary: oneLine } : {}),
               ...(stored && summaries?.full.has(key)
                  ? { summary: stored.summary }
                  : {}),
               ...(ctx?.doc ? { docs: ctx.doc } : {}),
               ...(ctx?.givens ? { givens: ctx.givens } : {}),
               ...(ctx?.accessFilter ? { accessFilter: ctx.accessFilter } : {}),
               ...(ctx?.authorize ? { authorize: ctx.authorize } : {}),
               ...(ctx?.filters ? { filter_params: ctx.filters } : {}),
               joins: ctx?.joins ?? [],
            },
         };
         bySource.set(key, entry);
      }
      return entry;
   };

   for (const r of results) {
      const entry = cardFor(r.source, r.modelPath);
      if (!entry) continue;
      if (r.kind === "source") {
         // The source itself matched: its score belongs on the container, and
         // its full (untruncated) doc supersedes the truncated context copy.
         const relevance = foldRelevance(entry.relevance, r);
         if (relevance !== undefined) entry.relevance = relevance;
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
      const relevance = foldRelevance(entry.relevance, r);
      if (relevance !== undefined) entry.relevance = relevance;
   }
   return Array.from(bySource.values());
}

/**
 * The stored LLM summaries a response may use, and for which sources it may
 * carry the full text. Built by {@link summaryViewFor}; absent when the package
 * has no stored summary, which leaves every card as it always was.
 */
interface SummaryView {
   /** Every current stored summary of the package, by sourceContextKey. */
   stored: ReadonlyMap<string, StoredSourceSummary>;
   /** Keys of the cards that carry `summary`, not just `one_line_summary`. */
   full: ReadonlySet<string>;
}

/**
 * The full summary rides only on a card the caller has narrowed to: the scope
 * pins the source, or a source search matched at most one source. A broader
 * answer carries the one-liner alone, so the response stays a list of sources
 * to choose from instead of a page of prose.
 */
export function summaryViewFor(
   stored: ReadonlyMap<string, StoredSourceSummary> | undefined,
   request: Pick<ResolvedRequest, "sourceName">,
   cards: ReadonlyArray<{
      key: string;
      source: string;
      rows: readonly ResultEntity[];
   }>,
): SummaryView | undefined {
   if (!stored || stored.size === 0) return undefined;
   const full = new Set<string>();
   if (request.sourceName) {
      for (const card of cards) {
         if (card.source === request.sourceName) full.add(card.key);
      }
   }
   // A source row is there only because a source target matched it. Counted by
   // source name: the same source resolved from two files is one answer.
   const matched = new Set(
      cards
         .filter((card) => card.rows.some((r) => r.kind === "source"))
         .map((card) => card.source),
   );
   if (matched.size <= 1) {
      for (const card of cards) {
         if (matched.has(card.source)) full.add(card.key);
      }
   }
   return { stored, full };
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
): { matched_targets?: MatchedTarget[] } {
   if (!r.targetScores || r.targetScores.size === 0) return {};
   const matched = [...r.targetScores.entries()]
      .sort((a, b) => a[0] - b[0])
      .flatMap(([index, relevance]) => {
         const search_text = searchTexts.get(index);
         if (search_text === undefined) return [];
         const match_reason = r.targetReasons?.get(index);
         return [
            {
               search_text,
               relevance: round4(relevance),
               ...(match_reason ? { match_reason } : {}),
            },
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
export const MAX_ENTITIES_PER_SOURCE_TARGET = 10;

/**
 * The most dotted index rows (joined fields assembly cannot rebuild) one source
 * contributes per search target, on top of {@link MAX_ENTITIES_PER_SOURCE_TARGET}
 * own fields. On a benchmark package whose joins are all inline tables,
 * sharing one window let 625 such rows push the labelled fields from rank 4 to
 * 10 down to rank 13 to 33; an own window of 3 (or 1) recovered the loss and
 * left a joined field findable.
 */
export const MAX_JOINED_ROWS_PER_SOURCE_TARGET = 3;

/** The target index with the highest score, or undefined when there is none. */
export function bestTargetOf(scores: Map<number, number>): number | undefined {
   let best: [number, number] | undefined;
   for (const entry of scores) {
      if (!best || entry[1] > best[1]) best = entry;
   }
   return best?.[0];
}

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
export function projectEntity(
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
      ...(e.embedDoc ? { embedDoc: e.embedDoc } : {}),
      ...(e.relationship ? { relationship: e.relationship } : {}),
      ...(e.aliases ? { aliases: e.aliases } : {}),
      ...(e.joinPath ? { joinPath: e.joinPath } : {}),
      ...(e.dataType ? { dataType: e.dataType } : {}),
      ...(e.code ? { code: e.code } : {}),
   };
}

/**
 * The largest whole-card prefix of `cards` whose JSON fits in
 * `maxChars - reserveChars`. Keeps at least one card, so a budget smaller
 * than the first card still answers. A null budget returns the input as is.
 * Only ranked responses are fitted; a listing pages by `offset` instead.
 */
export function fitBudget(
   cards: SourceCard[],
   maxChars: number | null,
   reserveChars: number,
): { cards: SourceCard[]; dropped: number } {
   if (maxChars === null) return { cards, dropped: 0 };
   const room = maxChars - reserveChars;
   // A JSON array costs 2 for the brackets plus one comma between cards.
   let used = 2;
   let keep = 0;
   for (const card of cards) {
      used += JSON.stringify(card).length + (keep > 0 ? 1 : 0);
      if (used > room && keep > 0) break;
      keep += 1;
   }
   return { cards: cards.slice(0, keep), dropped: cards.length - keep };
}

/**
 * The tail both ranked paths share: page the cards, serialize them, then fit
 * the byte budget. Semantic and lexical retrieval differ in how they SCORE,
 * not in what happens to the rows afterwards, so the ordering lives here once
 * instead of being restated at two call sites.
 *
 * `limit` counts SOURCE cards, because that is what the published contract
 * says it counts. Counting entities made targets compete for one pool: with
 * a budget of 4, asking for a measure alone returned 3 hits while asking for
 * it alongside a dimension and a view returned 2. Cards are paged in the
 * order sources first appear in the ranked rows, which is already best-first;
 * re-sorting here could disagree with the ranking that produced it.
 * `totalSources` counts the same unit, so `returned` and `total_available`
 * cannot disagree.
 *
 * The envelope stays with the caller: `retrieval`, `below_cutoff_count` and
 * `retrieval_reason` are each meaningful on one path only. Assembly
 * (get_context_assembly.ts) has already grouped rows by source and capped
 * each card per target; nothing is folded across sources.
 */
export function shapeCards(
   state: CardState,
   ctx: PipelineContext,
): {
   sources: SourceCard[];
   totalSources: number;
   entitiesDropped: number;
   budgetDropped: number;
} {
   const { request, pkgIndex, settings } = ctx;
   const paged = state.cards.slice(0, request.limit);
   // Over ALL the matched cards, not the page: whether a source search matched
   // one source or many does not depend on how many the caller asked to see.
   const summaries = summaryViewFor(ctx.sourceSummaries, request, state.cards);
   const built = toSourceResults(
      paged.flatMap((card) => card.rows),
      pkgIndex.sourceContext,
      request.environmentName,
      request.packageName,
      // One lookup from target index back to the text the caller wrote, so
      // matched_targets can name a target without re-walking the request.
      new Map(
         request.searches.map((search) => [search.targetIndex, search.text]),
      ),
      request.includeCode,
      pkgIndex.droppedSources,
      summaries,
   );
   // toSourceResults folds a card's relevance from its rows. After rerank the
   // card's relevance is the reranker's score, which no row carries, so it is
   // put back from the draft.
   const sources = state.reranked
      ? built.map((card) => {
           const id = card.source_info.resource_id;
           const draft = paged.find(
              (d) => d.key === sourceContextKey(id.model_path, id.source),
           );
           return draft?.relevance === undefined
              ? card
              : { ...card, relevance: draft.relevance };
        })
      : built;
   const fitted = fitBudget(sources, settings.maxChars, settings.reserveChars);
   // Entities cut inside a card the budget then removed are not reported: the
   // card is not in the answer, and its own cut warning is the budget's.
   const kept =
      fitted.dropped > 0
         ? paged.slice(0, paged.length - fitted.dropped)
         : paged;
   return {
      sources: fitted.cards,
      // Cards the reranker kept out of its top N still matched, so they count.
      totalSources: state.cards.length + (state.discarded ?? 0),
      entitiesDropped: kept.reduce(
         (sum, card) => sum + card.entitiesDropped,
         0,
      ),
      budgetDropped: fitted.dropped,
   };
}

/**
 * An error keeps the empty collection the tool would have answered with, so a
 * caller can read the payload unconditionally instead of branching on success
 * first: `sources: []` here, `environments: []` from listPackagesError. Each
 * tool has one success shape, so each error matches it -- an agent reading
 * `sources.length === 0` on an error payload and concluding the package models
 * nothing is the failure this prevents.
 *
 * Routed through classifyToolError for the same reason its three sibling tools
 * are: it homes each error class to real remediation, so an unknown package
 * says so instead of arriving as a bare message with no suggestions. It also
 * replaces a per-site `error instanceof Error ? error.message : "Unknown
 * error"`, which was the one path in this file that could produce exactly the
 * unhelpful string this tool's callers reported.
 */
export function contextError(uri: string, identifier: string, error: unknown) {
   return jsonToolError(
      uri,
      classifyToolError("getContext", identifier, error),
      { sources: [] },
   );
}

/**
 * The warnings that open every payload, in order: staleness, then the caller's
 * extras, then whatever the path itself adds.
 */
export function makeWarningsFor(ctx: PipelineContext) {
   /**
    * Spread into a payload to attach `warnings`. Returns {} when there is
    * nothing to say, so a healthy package's payload carries no key at all
    * rather than an empty array a caller has to test.
    *
    * A list rather than one joined string: staleness and truncation are
    * separate facts with separate remedies, and joining them made the
    * second read as a continuation of the first.
    */
   return (...extra: (string | undefined)[]) => {
      const warnings = [...ctx.warnings, ...extra].filter(
         (warning): warning is string => Boolean(warning),
      );
      return warnings.length > 0 ? { warnings } : {};
   };
}

/** Most description characters `skillsFor` attaches to one response, across every skill. */
const SKILLS_DESCRIPTION_BUDGET = 4000;

/**
 * Spread into a payload to attach `skills`: the guides this package ships about
 * itself, name and description only. Returns {} when it ships none, so most
 * payloads carry no key at all.
 *
 * Every description rather than a ranked subset: a one-line description is
 * cheap, and ranking could hide the guide that says the question is being asked
 * wrong. Bodies stay behind get_skill. Sits outside the response envelope,
 * beside `warnings`, because the envelope tracks the hosted retrieval API's
 * shape.
 */
export function skillsFor(ctx: PipelineContext) {
   // Descriptions are author-written and ride every response, so their total is bounded.
   let budget = SKILLS_DESCRIPTION_BUDGET;
   const skills = ctx.pkgIndex.pkg
      .listSkills()
      // Reference entries are on-demand detail addressed by the parent's
      // pointer, not something to choose from a list.
      .filter((skill) => !skill.name.includes("/"))
      .map((skill) => {
         const description = skill.description.slice(0, budget);
         budget -= description.length;
         return { name: skill.name, description };
      });
   return skills.length > 0
      ? {
           skills,
           skills_note:
              "This package ships its own guidance, listed above. Read the relevant one before relying on names or conventions from this package. If it is already loaded as a skill, use that; otherwise fetch it with get_skill(skill_name, scopes), and if that tool is not available to you, proceed and say which guidance you could not read. A guide whose name matches a built-in one replaces it here.",
        }
      : {};
}

/**
 * The warning for a capped result set, or undefined when nothing was
 * cut. It names the remedy that works here: raising the limit, or
 * narrowing the question. Telling an agent to "search more
 * specifically" when the cut was a flat cap sends it to re-query at
 * the one stage that cannot change the outcome.
 */
export const truncationWarning = (returned: number, matched: number) =>
   matched > returned
      ? `Returned ${returned} of ${matched} matching entities. Raise limit (max ${MAX_LIMIT}) or narrow the query to see the rest.`
      : undefined;

/**
 * The two cuts a ranked response can make, each named in its own unit. They
 * have different remedies -- one is the page size, the other is per source
 * -- and the old single warning reported both as "entities" while the
 * envelope beside it counted sources.
 */
export const sourceCutWarning = (returned: number, matched: number) =>
   matched > returned
      ? `Returned ${returned} of ${matched} matching sources. Raise limit (max ${MAX_LIMIT}) or narrow with scopes to see the rest.`
      : undefined;
/**
 * Cards the reranker left out because only its top N are ranked. They matched,
 * and `total_available` counts them, but no page size reaches them: the remedy
 * is a narrower question.
 */
export const rerankCutWarning = (dropped: number, top: number | undefined) =>
   dropped > 0
      ? `${dropped} further ${dropped === 1 ? "source" : "sources"} matched but ${dropped === 1 ? "was" : "were"} not ranked: only the best ${top ?? "few"} are scored (retrieval.rerank.topSources). Narrow with scopes or a more specific question to see ${dropped === 1 ? "it" : "them"}.`
      : undefined;
/**
 * A browse that stopped early. Its remedy is NOT the ranked one: a listing
 * has a resumable order, so the rest is one `offset` away and raising the
 * limit is the wrong advice -- it caps at 150 either way, and paging is
 * what actually reaches source 151.
 */
export const listingPageWarning = (returned: number, matched: number) =>
   matched > returned
      ? `Returned ${returned} of ${matched} sources in scope. Pass next_offset back as offset for the next page, or narrow with search_text or more targeted scopes.`
      : undefined;
/**
 * Whole cards left out because the response would pass the size budget. Its
 * remedy is not the page size: raising `limit` makes the answer bigger, and
 * the budget cuts it back.
 */
export const budgetCutWarning = (dropped: number, maxChars: number | null) =>
   dropped > 0 && maxChars !== null
      ? `${dropped} further ${dropped === 1 ? "source" : "sources"} matched but ${dropped === 1 ? "was" : "were"} left out to keep the response under ${maxChars.toLocaleString("en-US")} characters. Narrow with scopes or a more specific question to see ${dropped === 1 ? "it" : "them"}.`
      : undefined;
export const entityCutWarning = (dropped: number) =>
   dropped > 0
      ? `${dropped} further ${dropped === 1 ? "entity" : "entities"} matched but were cut at ${MAX_ENTITIES_PER_SOURCE_TARGET} per source per target. Scope to one source to list all of its fields.`
      : undefined;

/**
 * The answer while a package's semantic index is still building: a normal
 * result with no sources, `retrieval: "indexing"` and how far the build has
 * got. It is not an error, because the condition clears by itself and the
 * right response is to ask again shortly. It is not a lexical answer either:
 * with an embedding provider configured, a keyword ranking would differ from
 * what the finished index returns, and an agent cannot tell the two apart.
 */
export function indexingResponse(
   uri: string,
   ctx: PipelineContext,
   result: Unavailable,
) {
   const status = result.status;
   const progress = status
      ? { embedded: status.embeddedRows, total: status.totalRows }
      : undefined;
   const built = progress
      ? ` (${progress.embedded} of ${progress.total} vectors built)`
      : "";
   return jsonResource(uri, {
      sources: [],
      ranking: "relevance" as const,
      total_available: 0,
      returned: 0,
      retrieval: "indexing",
      ...(progress ? { retrieval_progress: progress } : {}),
      ...makeWarningsFor(ctx)(
         `This package's semantic index is still being built${built}, so there are no results yet. ` +
            `Nothing is wrong: ask the same question again in a few seconds. ` +
            `It was not answered by keyword search because an embedding provider is configured, ` +
            `and a keyword ranking would differ from what the finished index returns. ` +
            `A request with no search_text (a listing) works now.`,
      ),
   });
}

/**
 * The answer when an LLM stage (refine, rerank or source_match, or building
 * the LLM client for them) failed after the provider layer's retries. An error
 * result that names the stage and the reason, with `sources: []` like every
 * get_context error. It is never replaced by an unrefined or lexical answer: a
 * ranking that quietly differs from the one the working stages give is harder
 * to trust than an error.
 *
 * The advice names the package setting that turns the failing step off, and
 * the three that turn every request-time LLM step off. The index-time steps
 * (keyphrases, source summaries) fail a sync, not a request, and are reported
 * through the index status instead.
 */
const STAGE_SETTING: Readonly<Record<string, string>> = {
   refine: "retrieval.refine",
   rerank: "retrieval.rerank",
   source_match: "retrieval.sourceMatch",
};
/**
 * The stage's reason as the MCP caller sees it. A vendor failure is worded
 * without the endpoint (host, project path) and without the rest of the
 * vendor's body: the caller gets the status and the vendor's own message, and
 * the server log keeps the full text.
 */
function callerReason(error: StageError): string {
   let cause: unknown = error.cause;
   for (let depth = 0; cause instanceof Error && depth < 5; depth++) {
      if (cause instanceof HttpRequestError && cause.publicMessage) {
         return cause.publicMessage;
      }
      cause = cause.cause;
   }
   return error.reason;
}

export function stageFailureError(
   uri: string,
   error: StageError,
   extra: Record<string, unknown> = {},
) {
   return jsonToolError(
      uri,
      {
         message: `get_context failed in the ${error.stage} step: ${callerReason(error)}`,
         suggestions: [
            "Ask the same question again; a timeout or a malformed model reply is often transient.",
            "If it keeps failing, the operator should check the LLM settings (retrieval.llm in publisher.config.json, LLM_API_KEY) and the server log. " +
               (STAGE_SETTING[error.stage]
                  ? `To skip just this step, set the package's ${STAGE_SETTING[error.stage]} to enabled: false in publisher.json. `
                  : "") +
               "To search without any LLM step, set the package's retrieval.refine, retrieval.rerank and retrieval.sourceMatch to enabled: false.",
            "A request with no search_text (a listing) runs no LLM step and still works.",
         ],
      },
      {
         sources: [],
         retrieval: "error",
         retrieval_reason: "llm-stage-failed",
         retrieval_stage: error.stage,
         ...extra,
      },
   );
}

/**
 * The answer when a configured server cannot rank a search at all: an error
 * result that names the reason and says what to do. Still carries
 * `sources: []` like every get_context error, so a caller can read the
 * payload without branching on success first.
 */
export function semanticUnavailableError(uri: string, result: Unavailable) {
   const reason = result.unavailable;
   const status = result.status;
   const lastError = status?.lastError;
   const retryAt = lastError?.retryAt;
   const listing =
      "A request with no search_text (a listing) still works; it needs no embeddings.";
   let message: string;
   let suggestions: string[];
   switch (reason) {
      case "too-many-entities":
         // The status text already says the count, the cap and the key to raise.
         message =
            lastError?.message ??
            "This package has more entities than the semantic index cap (retrieval.indexing.maxEntities in publisher.config.json).";
         suggestions = [
            "Ask the operator to raise retrieval.indexing.maxEntities in publisher.config.json and restart the server.",
            listing,
         ];
         break;
      case "cooldown":
      case "provider-error": {
         const cause = lastError?.message ?? "the embedding provider failed";
         message =
            reason === "provider-error"
               ? `The embedding provider failed to embed the search text: ${cause}`
               : `Semantic search for this package is paused after an embedding failure: ${cause}`;
         suggestions = [
            retryAt
               ? // No timer fires at retryAt: the retry starts when a question
                 // arrives after it (see kickSync in embedding_index.ts).
                 `The next question after ${retryAt} tries again; nothing retries on its own before then.`
               : reason === "cooldown"
                 ? // A cool-down always has a retryAt, so none means the vendor
                   // refused the request itself and nothing will retry it.
                   "Asking again will not help: this failure is not retried until the package or its retrieval settings change, or the server restarts. The operator should read the server log."
                 : "Try again in a minute.",
            "If it keeps failing, the operator should check EMBEDDING_API_BASE, EMBEDDING_API_KEY and EMBEDDING_MODEL and that the endpoint is reachable. The server log has the full error.",
            listing,
         ];
         break;
      }
      default:
         message = `Semantic search is unavailable: ${result.detail ?? lastError?.message ?? "the embedding configuration or index storage could not be used"}`;
         suggestions = [
            "The operator should check the server log. For an invalid embedding configuration, fix the named variable (EMBEDDING_API_BASE, EMBEDDING_API_KEY, EMBEDDING_MODEL, EMBEDDING_DIMENSIONS) and restart the server.",
            listing,
         ];
   }
   return jsonToolError(
      uri,
      { message, suggestions },
      { sources: [], retrieval: "error", retrieval_reason: reason },
   );
}

/**
 * A target type this server cannot search, said plainly with the remedy that
 * works. Publisher indexes the model, not the values inside it, so a
 * `dimensional_value` target has nothing to match -- and the answer is the one
 * the analysis skill already gives: target the dimension, then read its
 * distinct values with a query.
 */
export function unsupportedTargetWarnings(request: ResolvedRequest): string[] {
   if (request.unsupported.length === 0) return [];
   const named = [...new Set(request.unsupported)].join(", ");
   return [
      `No index for target_type ${named}: this server indexes the semantic model, not the values stored in it. Target the dimension instead, then read its distinct values with execute_query (e.g. run: source -> { group_by: the_dimension }).`,
   ];
}
