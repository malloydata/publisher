// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Relationship } from "@malloydata/malloy-interfaces";
import { classifyToolError } from "../handler_utils";
import { jsonToolError } from "../tool_response";
import { MAX_LIMIT } from "./get_context_request";
import type {
   Entity,
   ListingContext,
   ResolvedRequest,
   ResultEntity,
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
 * A stable, package-scoped identity for one entity: `kind:source:name`, so
 * every consumer comparing entities across responses (e.g. a retrieval
 * scorer) agrees on identity. `Entity.id` is a per-build sequence number and
 * is not stable across reloads.
 *
 * Always exactly three segments, so a caller splitting on ":" can read [1] as
 * the source: a source is its own container (`source:orders:orders`), and
 * segments are percent-encoded because a backtick-quoted identifier may
 * contain ":".
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
 * A card's relevance after reading one more row. A source's own row sets it
 * outright; any other scored row raises it. Shared with toSourceResults so a
 * card and its wire form cannot disagree.
 */
export function foldRelevance(
   current: number | undefined,
   row: ResultEntity,
): number | undefined {
   if (row.score === undefined) return current;
   if (row.kind === "source") return row.score;
   // A source with no hit of its own still ranks by its best entity, so a
   // caller reading source relevance never sees a matched source at null.
   return current === undefined || row.score > current ? row.score : current;
}

/**
 * The response shape: source-centric, snake_case, matching a hosted Malloy
 * retrieval API's `get_context` so one parser serves both. Only serialization
 * happens here; ranking runs upstream over the flat `ResultEntity[]`.
 *
 * Deliberate divergences: `entity_type` adds `join` and `query`;
 * Publisher-only fields (`source_info.joins`, `entity_id`, `relationship`,
 * `aliases`, `code`, the response-level retrieval fields) ride along; fields
 * Publisher cannot honestly fill (`prominence`, `values`, `values_indexed`)
 * are omitted rather than sent empty.
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
   /** The judge's one-sentence reason. Present only when it gave one. */
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
   /**
    * The stored strings of this dimension that a `dimensional_value` target
    * matched, best first. Publisher extension of the published shape: the
    * hosted service's `values[]` carries `prominence` too, which a server with
    * no value index has no counts to fill, so it is omitted.
    */
   values?: Array<{ value: string; relevance: number; search_text: string }>;
   /**
    * fcj engine (provisional `alsoLikely`): true when the entity is not one of
    * the judge's picks but a field returned beside them; the reason says why
    * (a pick's same-named field on another source, or a name the judge's note
    * wrote). Absent on a pick.
    */
   also_likely?: true;
   also_likely_reason?: string;
   /**
    * fcj engine (provisional `joinedCopies: "routes"`): every route from
    * another source of this answer to the field, shortest first, with the id
    * the field has there (`dimension:orders:customer.region.name`).
    */
   routes?: Array<{
      entity_id: string;
      from_source: string;
      join_path: string;
   }>;
}

export interface SourceCard {
   source_info: SourceCardInfo;
   relevance?: number;
   entities?: SourceCardEntity[];
}

/**
 * Group a ranked entity list into the source-centric response shape.
 *
 * Sources and entities keep their first-appearance (rank) order rather than
 * being re-sorted, so this cannot disagree with the ranking. A `kind: "source"`
 * hit becomes the container's relevance, not a row in its `entities`.
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
      // Type narrowing only: no ranked row reaches here without a source.
      if (!name) return undefined;
      // Defense in depth: collectEntities already drops these, but this is the
      // one place that can mint a card from a bare name, so a future path that
      // forgets the drop cannot leak a deny-all source's name and shape.
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
         ...(r.values && r.values.length > 0
            ? {
                 values: r.values.map((v) => ({
                    value: v.value,
                    relevance: Math.round(v.score * 10_000) / 10_000,
                    search_text: v.searchText,
                 })),
              }
            : {}),
         ...(r.alsoLikely
            ? { also_likely: true as const, also_likely_reason: r.alsoLikely }
            : {}),
         ...(r.routes && r.routes.length > 0
            ? {
                 routes: r.routes.map((x) => ({
                    entity_id: x.id,
                    from_source: x.from,
                    join_path: x.path,
                 })),
              }
            : {}),
      };
      (entry.entities ??= []).push(entity);
      const relevance = foldRelevance(entry.relevance, r);
      if (relevance !== undefined) entry.relevance = relevance;
   }
   return Array.from(bySource.values());
}

/**
 * The stored LLM summaries a response may use, and for which sources it may
 * carry the full text. Built by {@link summaryViewFor}.
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
 * Omitted when none did (a listing). Ordered by target index, the order the
 * caller wrote them, so the response reads in request order.
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
 * The fields every retrieval path (browse, semantic, lexical) carries from an
 * indexed entity onto a ranked one, shared so a new field reaches every path.
 *
 * `score` and `targetScores` stay at the call site: each is honest on one path
 * only, and folding them in here invites publishing a lexical score as a
 * relevance.
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
 * An error payload keeps the success shape's empty `sources: []`, so a caller
 * can read it unconditionally. Routed through classifyToolError so each error
 * class carries real remediation (e.g. an unknown package says so).
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
export function makeWarningsFor(ctx: Pick<ListingContext, "warnings">) {
   /**
    * Spread into a payload to attach `warnings`; {} when there is nothing to
    * say. A list, not one joined string, because each warning has its own
    * remedy.
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
 * Spread into a payload to attach `skills`: the package's own guides, name and
 * description only ({} when it ships none). Every description rather than a
 * ranked subset, because ranking could hide the guide that says the question
 * is being asked wrong. Bodies stay behind get_skill.
 */
export function skillsFor(ctx: Pick<ListingContext, "pkgIndex">) {
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
 * The warning for a capped result set, or undefined when nothing was cut.
 * Each cut warning below names the remedy that works for that cut.
 */
export const truncationWarning = (returned: number, matched: number) =>
   matched > returned
      ? `Returned ${returned} of ${matched} matching entities. Raise limit (max ${MAX_LIMIT}) or narrow the query to see the rest.`
      : undefined;

/**
 * A browse that stopped early. Its remedy is paging by `offset`, not raising
 * the limit, which is capped either way.
 */
export const listingPageWarning = (returned: number, matched: number) =>
   matched > returned
      ? `Returned ${returned} of ${matched} sources in scope. Pass next_offset back as offset for the next page, or narrow with search_text or more targeted scopes.`
      : undefined;
/**
 * A target type this server cannot search, said plainly with the remedy that
 * works. Reached only when value search is turned off
 * (`retrieval.dimensionValues.enabled: false`): then a `dimensional_value`
 * target has nothing to match, and the answer is the one the analysis skill
 * gives: target the dimension, then read its distinct values with a query.
 */
export function unsupportedTargetWarnings(request: ResolvedRequest): string[] {
   const warnings: string[] = [];
   if (request.unsupported.length > 0) {
      const named = [...new Set(request.unsupported)].join(", ");
      warnings.push(
         `No index for target_type ${named}: this server indexes the semantic model, not the values stored in it. Target the dimension instead, then read its distinct values with execute_query (e.g. run: source -> { group_by: the_dimension }).`,
      );
   }
   if (request.textlessValueTargets) {
      warnings.push(
         "A dimensional_value target needs search_text: it looks up the value you give it and does not list values. To list a dimension's values, run it with execute_query (e.g. run: source -> { group_by: the_dimension }).",
      );
   }
   return warnings;
}
