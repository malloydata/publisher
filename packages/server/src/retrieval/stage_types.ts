// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a post-scoring stage needs from a ranked row. get_context_tool's
 * internal `ResultEntity` satisfies it structurally, which is why the stages
 * are generic over it rather than importing it: the stages live in their own
 * modules, and get_context_tool imports them, so a type import the other way
 * would be a cycle.
 *
 * Everything marked INTERNAL below is used by ranking only and never reaches
 * the wire; the response is built field by field in toSourceResults.
 */
export interface StageRow {
   kind: string;
   name: string;
   source: string | undefined;
   modelPath: string;
   dataType?: string;
   joinPath?: string;
   /** INTERNAL: `#(doc)`-only text, the one description safe to send out. */
   embedDoc?: string;
   /** INTERNAL: an LLM keyphrase for an entity whose docs are sparse. */
   keyphrase?: string;
   /** Published relevance (semantic path, or any row a stage has scored). */
   score?: number;
   /** Published per-target relevance, for matched_targets. */
   targetScores?: Map<number, number>;
   bestTarget?: number;
   /** INTERNAL: the score the row currently ranks on. */
   rankScore?: number;
   /**
    * INTERNAL: per-target score BEFORE any LLM stage (cosine, or normalised
    * lunr), on both paths. `targetScores` cannot serve: on the lexical path it
    * is withheld because a lunr score is not a relevance.
    */
   candidateScores?: Map<number, number>;
   /** Published per-target reason (`matched_targets[].match_reason`). */
   matchReasons?: Map<number, string>;
   /** Indexed values of this dimension that matched a value target, best first. */
   values?: Array<{ value: string }>;
}

export interface SearchTargetText {
   targetIndex: number;
   text: string;
}

/**
 * How an LLM stage ended, as it appears in `retrieval_stages`:
 * `ok`, `partial:<failed>/<total>`, `skipped:<why>` or `failed:<kind>`.
 */
export type StageStatus = string;

export interface StageOutcome<T> {
   rows: T[];
   status: StageStatus;
   /** Plain-language notes for the response's `warnings`. */
   warnings: string[];
   /** Rows removed, by reason, for the trace. */
   dropped: Record<string, number>;
   rowsIn: number;
   /**
    * What the stage decided about each candidate, per search target, keyed by
    * the row's `entityRowKey`. For the full trace only: it is what lets a sweep
    * over `refine.minLevel` be replayed offline, which needs the level of the
    * candidates the run dropped as well as the ones it kept.
    */
   verdicts?: Map<string, Map<number, StageVerdict>>;
}

/** `level` is LOW/MEDIUM/HIGH when `outcome` is `scored`; otherwise why there is none. */
export interface StageVerdict {
   outcome: "scored" | "omitted" | "unscored" | "capped";
   level?: string;
   reason?: string;
}
