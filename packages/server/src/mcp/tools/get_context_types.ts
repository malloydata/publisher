// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type lunr from "lunr";
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
   // Join cardinality, on `kind: "join"` entities only. Tells an agent whether
   // traversing the join fans out (many) before it writes a query against it.
   relationship?: Relationship;
   // On `kind: "join"` entities: the source the join leads to, when the
   // compiled model names one (the topology below has it; a model with no
   // compiled IR does not). Read by the source summary to nest a joined
   // source's fields; never serialized.
   joinTarget?: string;
   // On `kind: "join"` entities: the package-relative path of the file that
   // defines `joinTarget`. Together they identify the source; the name alone
   // cannot when two files define a source of the same name.
   joinTargetModelPath?: string;
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
    * query, so the lexical path never sets this and nothing it could set
    * would be safe to publish as a relevance.
    */
   targetScores?: Map<number, number>;
   /**
    * The target this row is budgeted under in windowBySource: the one that
    * claimed it best. Set on BOTH ranked paths, because the per-target window
    * is a budgeting rule, not a scoring one -- the lexical path knows which
    * target found a row even though it withholds how well.
    */
   bestTarget?: number;
   /** Set by refine. LOW=1, MEDIUM=2, HIGH=3. */
   level?: number;
   /**
    * Set by refine only: the unpublished score, `level + cosine`, 1 to 4. When
    * it is set, `score` is this value through the knots (see
    * get_context_scoring). Assembly damps joined copies on this number, and
    * rerank sorts cards by the best of it.
    */
   raw?: number;
   /** The per-target version of `raw`, behind `targetScores` as `score` is behind `raw`. */
   targetRaw?: Map<number, number>;
   /**
    * Set by refine: the model's one-sentence reason for each target that
    * rated this row, behind `matched_targets[].match_reason`. Absent when
    * refine did not run or the model gave no reason.
    */
   targetReasons?: Map<number, string>;
   /** Unused until assembly makes joined copies. Joins crossed to reach the field. */
   joinHops?: number;
}

/** A join as reported in source context: enough to write the traversal. */
export interface SourceContextJoin {
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
export interface SourceContextEntry {
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
    * The `#(access_filter)` gates in force on this source, and the givens each
    * one reads. Retrieval that offered a gated source without saying so spent
    * a slot on an entity the caller could not query, and the agent learned
    * that only from the denial.
    *
    * Report-only, and never a predicate to evaluate caller-side: the list
    * flattens gates carried in from elsewhere, they are AND-ed rather than
    * OR-ed, and an unattributable gate reports the fail-closed placeholder
    * "false" that no author wrote. Read it as "this source is gated, and these
    * are the givens to supply". See docs/authorize.md.
    */
   accessFilter?: SourceContextAuthorize[];
   /**
    * The `#(authorize)` locks in force on this source, reported separately
    * from `accessFilter` above — they decide whether the caller may reach the
    * source at all, and answer 403 rather than an empty result. Same
    * report-only caveats apply. A source gated ONLY by an unconditional deny
    * on either route never reaches this card at all — see the collector's
    * drop.
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
 * A filter a source exposes via `#(filter)`, as the model declares it. A
 * REQUIRED one is the reason this is on the card at all: a caller that cannot
 * see it cannot supply it, and the query fails when they use the source. That
 * is the same argument that keeps `givens` on a card, and it is why this is
 * reported rather than left to a follow-up call the way a description is.
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

/**
 * Why a server that HAS an embedding provider could not rank a search. Reported
 * so an agent can act on it: "indexing" is an index still building, answered
 * with an empty result (retrieval: "indexing") that is worth asking again, while the rest are errors
 * (retrieval_reason) an immediate retry cannot fix.
 */
export type RetrievalReason =
   | "indexing"
   | "cooldown"
   | "too-many-entities"
   | "provider-error"
   | "unavailable";

/** One search target that carries text, kept with the index it came in at. */
export interface ResolvedSearch {
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
export interface ResolvedRequest {
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

/** One source reachable from a root source by joins. */
export interface JoinReach {
   /** Name of the source the fields live on. */
   targetSource: string;
   /**
    * The package-relative path of the file that defines that source. Together
    * with `targetSource` it is the source's identity: two files can each
    * define a source of the same name, and a join reaches only one of them.
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
   /**
    * `byId`'s values as one frozen array, the entity set handed to the
    * semantic index. Every call for this Package passes this same array, which
    * is what lets the index cache its content fingerprint instead of hashing
    * every facet on each question and each status read (see fingerprintFor
    * in embedding_index).
    */
   retrievalEntities: readonly Entity[];
   /**
    * `retrievalEntities` without the joined copies (those with a joinPath),
    * frozen once so it can be fingerprinted and cached the same way.
    */
   directEntities: readonly Entity[];
   /**
    * Which sources each source reaches by joins, up to
    * JOIN_TOPOLOGY_MAX_DEPTH deep, read from the compiled model. A source
    * whose model has no compiled IR reaches nothing here.
    */
   topology: JoinTopology;
   index: lunr.Index;
   entityCount: number;
   /** Per-source context, keyed by source name. Built once with the index. */
   sourceContext: Map<string, SourceContextEntry>;
   /** Sources dropped for an unconditional deny-all gate; cardFor refuses these. */
   droppedSources: Set<string>;
}
