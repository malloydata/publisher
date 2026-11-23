// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An incremental source's declaration as data: what its `refresh=`,
 * `watermark=` and `merge_key=` keys resolved to on the compiled source, the
 * strategy they imply, what is malformed, and, for a `#(materialize)` source,
 * its rolling windows and a pair table's delta layout.
 *
 * The planner resolves it (`plan/incremental_declaration.ts`) and judges it
 * (`plan/incremental_policy.ts`); the run reads it to seed or advance a table
 * (`run/incremental_build.ts`, `run/incremental_apply.ts`).
 *
 * Pure.
 */

import type { RollingWindow } from "./read_time_terms";
import type { IncrementalStrategy } from "./types";

/** What a declared name turned out to be on the compiled source. */
export type ResolvedName =
   | {
        name: string;
        kind: "dimension" | "aggregate";
        /** The column's Malloy type (`date`, `timestamp`, `number`, …). */
        malloyType: string;
     }
   | { name: string; kind: "unresolved" };

/** A key whose VALUE is unusable, as opposed to absent. */
export interface MalformedValue {
   key: "refresh" | "watermark" | "merge_key" | "lookback";
   problem:
      | "array" // tag-array form: watermark=["a","b"]
      | "empty" // empty string: watermark=""
      | "empty-entry" // an empty name in the split list: merge_key="a,,b"
      | "duplicate"; // a repeated name: merge_key="a, a"
   /** The offending name, when one name of several is at fault. */
   detail?: string;
}

export interface IncrementalDeclaration {
   /** The effective `refresh=` value verbatim; undefined when unset. */
   refresh?: string;
   /** True only when `refresh=` is exactly `"incremental"`. */
   incremental: boolean;
   /** Set when `refresh=` is declared but outside {full, incremental}. */
   invalidRefresh?: string;
   /** Whether the key is present on the raw tag, whatever its value's shape. */
   declaredWatermark: boolean;
   declaredMergeKey: boolean;
   /** Whether `lookback=` is present on the raw tag; absent reads as false. */
   declaredLookback?: boolean;
   /**
    * `lookback=` as written, and in seconds when it reads as a duration
    * (domain/freshness.ts, `durationSeconds`); undefined when not declared or
    * malformed. A delta starts this far below the newest stored watermark.
    */
   lookback?: { text: string; seconds?: number };
   /** The watermark, resolved against the output schema. */
   watermark?: ResolvedName;
   /** Whether the watermark's type supports a `<` range. */
   watermarkOrderable: boolean;
   /** `merge_key=` split on commas, in declared order, each resolved. */
   mergeKeys: ResolvedName[];
   /**
    * The source's own columns its stripped dynamic terms constrain. A
    * `merge_key=` is chosen against the source filtered to one caller, but the
    * stored table holds every caller's rows; these columns restore the intended
    * relation inside the merge's match.
    */
   scopeColumns: string[];
   /**
    * True when a stripped term contributes no column of this source (it reaches
    * through a join, or its field usage could not be read). A caller must refuse,
    * never partially scope: scoping by the other terms alone still matches across
    * callers.
    */
   scopeIncomplete: boolean;
   /** True when `merge_key=` repeats the watermark dimension. */
   watermarkInMergeKeys: boolean;
   /**
    * The strategy the keys imply, present only when the declaration is coherent
    * enough to derive one (incremental + a resolved watermark).
    */
   strategy?: IncrementalStrategy;
   malformed: MalformedValue[];
   /** Unrecognized persist-tag keys, in tag order. */
   unknownKeys: string[];
   /**
    * Names of `calculate:` (window) fields anywhere in the query definition.
    * Presence alone gates an incremental source for now (see the publish rules).
    */
   calculateFields: string[];
   /** Output column names, for "did you mean" context in messages. */
   outputColumns: string[];
   /**
    * A `#(materialize)` source's rolling windows (rolling_window.ts), set from
    * its plan's read-time terms by `collectIncrementalDeclarations`: each
    * refresh deletes the rows below every window's new lower bound.
    */
   rollingWindow?: RollingWindow[];
   /**
    * An incremental pair table's parent key and parent-row flag column (a
    * `#(materialize)` inlined join_many), set by
    * `collectIncrementalDeclarations` from its plan: each refresh replaces
    * every row of each parent whose watermark moved, by the key, rather than
    * merging row by row (incremental_apply.ts).
    */
   pair?: PairDelta;
}

/**
 * What a pair table's delta replaces rows by and checks after: the parent key
 * and the parent-row flag, and, for a path of join_manys or a table of
 * several blocks, every row flag the delta must leave exact, each with the
 * stored keys it marks one row per distinct value of (from the source down
 * to its level), and for a table of several blocks the block it is checked
 * within (`lane`, a value of `laneColumn`). Absent `checks`: the parent-row
 * flag over the parent key, as a one-hop table of one block has.
 */
export interface PairDelta {
   parentKey: string;
   parentRow: string;
   checks?: { flag: string; keys: string[]; lane?: number }[];
   laneColumn?: string;
   /**
    * For a table of several blocks: how many there are. Every parent has rows
    * in each (a block is a LEFT JOIN from the parents), which the delta also
    * checks.
    */
   lanes?: number;
}

/**
 * The code on the warning for an incremental source's late rows: what the
 * late-row check found below a delta (`LateRows`, domain/types.ts), raised
 * while the run that found them is bound.
 */
export const INCREMENTAL_LATE_ROWS_CODE = "persist-incremental-late-rows";

/**
 * The code on the warning for an incremental source whose `merge_key=` a
 * delta found repeated in the source's rows: its delta is rolled back and
 * the source rebuilt in full (`merge_key_not_unique`), on every refresh whose
 * delta repeats a key.
 */
export const INCREMENTAL_MERGE_KEY_NOT_UNIQUE_CODE =
   "persist-incremental-merge-key-not-unique";

/**
 * The code on the warning for an incremental source a check of its refresh
 * could not run on, so it is rebuilt in full on every refresh until it can:
 * the repeated-key check after a merge source's full rebuild, or the read of
 * a delta's column types.
 */
export const INCREMENTAL_CHECK_FAILED_CODE = "persist-incremental-check-failed";

/** A check an incremental refresh runs whose failure rebuilds in full. */
export type IncrementalCheck = "merge_key" | "delta_types";

/**
 * What a run found of one source's incremental check (see
 * IncrementalRunContext.noteIncrementalCheck): passed, a merge key repeated
 * in the source, or the check failed with an error's code (errorCodeOf,
 * never its message).
 */
export type IncrementalCheckFinding =
   | { check: IncrementalCheck; outcome: "passed" }
   | { check: "merge_key"; outcome: "not_unique" }
   | { check: IncrementalCheck; outcome: "failed"; errorCode: string };
