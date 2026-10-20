// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Relationship } from "@malloydata/malloy-interfaces";
import type { Package } from "../../service/package";
import type { SearchTargetType } from "./get_context_request";

/**
 * A retrievable model entity: a source, one of its views, a field (dimension or
 * measure) defined on a source, a join it declares, or a named query. Sources,
 * views, fields, and joins come from the compiled SourceInfo
 * (Model.getSourceInfos()); named queries from Model.getQueries().
 */
export interface Entity {
   id: string;
   kind: "source" | "view" | "query" | "dimension" | "measure" | "join";
   name: string;
   source: string | undefined;
   modelPath: string;
   // Human-facing doc for the response (may fall back to raw annotations).
   doc: string;
   // #(doc)-only text used as embedding input; never carries predicate
   // annotations (#(access_filter) etc.) that must not leave the machine.
   embedDoc: string;
   // Join cardinality, on `kind: "join"` entities only: whether traversing
   // the join fans out.
   relationship?: Relationship;
   // On `kind: "join"` entities: the source the join leads to, when the
   // compiled model names one. Read by the source summary; never serialized.
   joinTarget?: string;
   // The file defining `joinTarget`; with it, identifies the source when two
   // files define a source of the same name.
   joinTargetModelPath?: string;
   // Other spellings of this same field collapsed into it (see
   // collapseAliases). Present only when non-empty.
   aliases?: string[];
   // The authored expression text. Absent on a physical column, so absence
   // means the field is raw. Serialized only with `include_code`.
   code?: string;
   // When this field's whole definition references another field of the SAME
   // source (`dimension: site is SITE`), that name; it makes alias collapsing
   // exact. A reference through a join is deliberately not recorded: it is a
   // different column, not another spelling.
   aliasOf?: string;
   // Malloy type of a dimension or measure ("string", "number", "date", ...).
   dataType?: string;
   // For a field reached THROUGH a join, the traversal that reaches it
   // ("orders", or "order_items.inventory_items"), which is also the dotted
   // prefix of `name`. Absent on a source's own fields. See collectJoinedFields.
   joinPath?: string;
}

/**
 * One ranked entity, INTERNAL to ranking. `score` (cosine) rides only on
 * semantic results. This never reaches the wire: toSourceResults converts a
 * list of these into the response shape, which is where identity is assigned.
 */
export interface ResultEntity {
   kind: string;
   name: string;
   source: string | undefined;
   environmentName: string;
   packageName: string;
   modelPath: string;
   doc: string;
   /**
    * The `#(doc)` text alone, with no annotation fallback (see Entity.embedDoc).
    * The LLM stages read this, never `doc`, which can carry raw annotation
    * lines. Never serialized.
    */
   embedDoc?: string;
   relationship?: Relationship;
   /** Other spellings of this field in its own source, collapsed into it. */
   aliases?: string[];
   /**
    * The entity's definition: a dimension's or measure's Malloy expression, or
    * a view's definition sliced from the model file. Serialized only when the
    * request asks for code.
    */
   code?: string;
   score?: number;
   /**
    * The score before it was rounded to the four places the response publishes.
    * Ordering uses it, so two rows whose scores differ in the fifth place are
    * not treated as tied. Never serialized.
    */
   rawScore?: number;
   /** Malloy type of a dimension or measure. */
   dataType?: string;
   /** The join traversal reaching this field; absent on a source's own. */
   joinPath?: string;
   /**
    * Score per search target index that matched this row, for
    * matched_targets. Semantic only: a lunr score is relative to its own
    * query and not safe to publish as a relevance.
    */
   targetScores?: Map<number, number>;
   /**
    * The model's one-sentence reason for each target that picked this row,
    * behind `matched_targets[].match_reason`. Absent when it gave none.
    */
   targetReasons?: Map<number, string>;
   /**
    * The dimension's stored strings that matched a `dimensional_value` target,
    * best first. Never copied onto a joined copy: a value was read under one
    * source's access gate and must not appear on another source's card.
    */
   values?: Array<{ value: string; score: number; searchText: string }>;
   /**
    * fcj engine, provisional `alsoLikely`: the row is not a judge pick but a
    * field returned beside them, and this is why. Serialized as
    * `also_likely: true` and `also_likely_reason`.
    */
   alsoLikely?: string;
   /**
    * fcj engine, provisional `joinedCopies: "routes"`: how the answer's other
    * sources reach this field, each with the id the field has there.
    */
   routes?: Array<{ id: string; from: string; path: string }>;
}

/** A join as reported in source context: enough to write the traversal. */
export interface SourceContextJoin {
   name: string;
   relationship: Relationship;
   doc?: string;
}

/**
 * The parent-source context for one source represented in `results`, reported
 * once per response for every source behind a result, so a field hit always
 * arrives with its source's grain and join guidance regardless of phrasing.
 */
export interface SourceContextEntry {
   name: string;
   modelPath: string;
   /** Truncated to SOURCE_DOC_MAX_CHARS; the model file has the full text. */
   doc: string;
   /**
    * Every join the source declares, not just retrieved ones, so an empty
    * array authoritatively means "declares no joins".
    */
   joins: SourceContextJoin[];
   /** The source's doc reduced to its first line, capped. Absent when undocumented. */
   oneLineSummary?: string;
   /** Model-level `given:` parameters in scope for this source. */
   givens?: SourceContextGiven[];
   /**
    * The `#(access_filter)` gates in force on this source, and the givens each
    * one reads.
    *
    * Report-only, never a predicate to evaluate caller-side: the list flattens
    * gates carried in from elsewhere, they are AND-ed, and an unattributable
    * gate reports the fail-closed placeholder "false". Read it as "this source
    * is gated, and these are the givens to supply". See docs/authorize.md.
    */
   accessFilter?: SourceContextAuthorize[];
   /**
    * The `#(authorize)` locks in force on this source: they decide whether the
    * caller may reach the source at all (403, not an empty result). Same
    * report-only caveats. A source gated only by an unconditional deny never
    * reaches a card.
    */
   authorize?: SourceContextAuthorize[];
   /** Filters the source declares via `#(filter)`. */
   filters?: SourceContextFilter[];
}

/** A `given:` a caller may supply, as the model declares it. */
export interface SourceContextGiven {
   name: string;
   type?: string;
   annotations?: string[];
   default?: string;
}

/**
 * A filter a source exposes via `#(filter)`. On the card because a caller that
 * cannot see a required filter cannot supply it, and the query fails.
 */
export interface SourceContextFilter {
   name: string;
   type: string;
   dimension?: string;
   required?: boolean;
}

/** One gate, on either route, with the givens its expression reads. */
export interface SourceContextAuthorize {
   expression: string;
   given_names: string[];
}

/** One search target that carries text, kept with the index it came in at. */
export interface ResolvedSearch {
   /** Position in the caller's search_targets, reported as matched_targets. */
   targetIndex: number;
   targetType: SearchTargetType;
   text: string;
   /** Entity kinds this target may match. */
   kinds: string[];
}

/** A `dimensional_value` target that carries text, kept with its index. */
export interface ResolvedValueSearch {
   targetIndex: number;
   text: string;
}

/**
 * The request reduced to what retrieval works in: one scope, the kinds in
 * play, and the searches to rank. Resolved once so the listing and ranked
 * paths cannot disagree about what was asked.
 */
export interface ResolvedRequest {
   /**
    * Only `source` targets, none with text: the catalog browse. The one
    * request with a resumable order, so the only one `offset` applies to, and
    * the one whose cards go thin.
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
   /**
    * `dimensional_value` targets with text, answered by the values stage rather
    * than by entity retrieval. Empty when value search is turned off.
    */
   valueSearches: ResolvedValueSearch[];
   /** `dimensional_value` targets sent with no `search_text`, for a warning. Absent when none. */
   textlessValueTargets?: number;
   /**
    * Given values the caller sent. Used only to read dimension values through
    * the query path, where they unlock `#(authorize)` and `#(access_filter)`
    * exactly as they do for `execute_query`; entity ranking never reads them.
    */
   givens?: Record<string, unknown>;
   /** Filter parameter values; read by value lookups for sources with required filters. */
   filterParams?: Record<string, string | string[]>;
   /**
    * Literals the caller extracted for a dimension or value target, by target
    * index (`search_targets[].example_values`). Read by the fcj engine's value
    * index only.
    */
   exampleValues?: Map<number, string[]>;
   /** True when no target carried search text: enumerate, do not rank. */
   listingOnly: boolean;
   /** Serialize each entity's Malloy expression as `code`. */
   includeCode: boolean;
   /** Targets naming a type this server cannot search, for a warning. */
   unsupported: SearchTargetType[];
   limit: number;
   offset: number;
}

/** What a listing needs: the request, the package's entity index, and the warnings to lead with. */
export interface ListingContext {
   request: ResolvedRequest;
   pkgIndex: PackageIndex;
   warnings: string[];
}

/** One source reachable from a root source by joins. */
export interface JoinReach {
   /** Name of the source the fields live on. */
   targetSource: string;
   /**
    * The file defining that source; with `targetSource`, the source's identity,
    * since two files can define a source of the same name.
    */
   targetModelPath: string;
   /** Join names from the root, root not included. Aliases, not source names. */
   path: string[];
   /** Widest relationship on the path; "many" anywhere means fan-out. */
   fanout: Relationship;
}

/** For each root source (sourceContextKey), every source reachable by joins. */
export type JoinTopology = ReadonlyMap<string, readonly JoinReach[]>;

export interface PackageIndex {
   pkg: Package;
   byId: Map<string, Entity>;
   /** `byId`'s values as one frozen array, the same on every call. */
   retrievalEntities: readonly Entity[];
   /**
    * `retrievalEntities` without the joined copies (those with a joinPath),
    * frozen once.
    */
   directEntities: readonly Entity[];
   /**
    * Which sources each source reaches by joins, up to
    * JOIN_TOPOLOGY_MAX_DEPTH deep, read from the compiled model. A source
    * whose model has no compiled IR reaches nothing here.
    */
   topology: JoinTopology;
   entityCount: number;
   /** Per-source context, keyed by source name. Built once with the index. */
   sourceContext: Map<string, SourceContextEntry>;
   /** Sources dropped for an unconditional deny-all gate; cardFor refuses these. */
   droppedSources: Set<string>;
}
