// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A rollup's measures as data: the measure grammar's expression tree and its
 * aggregate leaves, and the synthesized rollup (`RollupPlan`) the build plan's
 * wire `preaggregate` entry describes, with the merge each measure's partial
 * takes.
 *
 * The measure grammar (`plan/rollup_measure_grammar.ts`) and the rollup
 * synthesis (`plan/preaggregation_synthesis.ts`) produce them; the rollup
 * companion's writer (`text/read_text.ts`) and the rollup leg read them.
 *
 * Types only.
 */

/** The aggregates whose partials merge. */
export type MergeableAggregate = "sum" | "count" | "min" | "max";

/** The aggregate applied to a stored partial to merge it. */
export type Reaggregate = "sum" | "min" | "max";

/** One node of a measure expression. */
export type MeasureExpr =
   | { kind: "number"; text: string }
   | { kind: "string"; value: string }
   | { kind: "literal"; value: "true" | "false" | "null" }
   | { kind: "path"; path: string[] }
   | { kind: "call"; name: string; args: MeasureExpr[] }
   | { kind: "method"; path: string[]; fn: string; args: MeasureExpr[] }
   | { kind: "filtered"; expr: MeasureExpr; where: MeasureExpr[] }
   | { kind: "unary"; op: "-" | "not"; expr: MeasureExpr }
   | { kind: "binary"; op: BinaryOp; left: MeasureExpr; right: MeasureExpr }
   | { kind: "paren"; expr: MeasureExpr }
   /** A stored partial, merged: only in a decomposed measure's `merged`. */
   | { kind: "partial"; column: string; reaggregate: Reaggregate };

export type BinaryOp =
   | "+"
   | "-"
   | "*"
   | "/"
   | "%"
   | "="
   | "!="
   | "<"
   | "<="
   | ">"
   | ">="
   | "~"
   | "!~"
   | "and"
   | "or";

/** One aggregate leaf of a measure: what the build stores as one partial. */
export interface AggregateLeaf {
   aggregate: MergeableAggregate;
   /** The leaf as written, with every filter that applies to it. */
   expr: MeasureExpr;
   /**
    * The field paths the leaf reads: the method's path (`carriers.fleet` in
    * `carriers.fleet.sum()`), the argument's and the filters'. The planner
    * reads join locality from their heads.
    */
   reads: string[][];
   /** A join path the aggregate is taken at (`carriers` in `carriers.count()`). */
   joinPath: string[];
}

/** The aggregate applied to the STORED partial column to merge it. */
export type ReaggregateFunction = "sum" | "min" | "max";

/** One measure served by a rollup, with the merge to apply to its partial. */
export interface RollupMeasure {
   /** The measure's name on the base, and on the rollup member. */
   name: string;
   /** The stored column holding the partial aggregate. */
   partialName: string;
   /** The aggregate applied to `partialName` to merge it. */
   reaggregate: ReaggregateFunction;
   /**
    * The measure's annotation lines (`#(classification)`, `#(doc)`, a render
    * tag such as `# currency`), written again on the merged measure so a
    * rollup's result column carries what live's does. `#@` directives are
    * left out: they declare builds, not the column.
    */
   notes?: string[];
}

/** One synthesized rollup: one base, one grain, one table, one `GROUP BY`. */
export interface RollupPlan {
   /** The source whose measures were annotated. Queries name this. */
   baseSourceName: string;
   /** The synthesized `#@ persist` source's name. Deterministic. */
   rollupSourceName: string;
   /**
    * The namespace the rollup's table is created in: this grain's own
    * `#@ preaggregate namespace=`, else everything before the last dot of the
    * base's `#@ persist name=`, else undefined when neither names one.
    *
    * A rollup of X belongs where X lives unless its author said otherwise. Not
    * cosmetic: BigQuery rejects an unqualified CREATE.
    */
   namespace?: string;
   /**
    * The storage destination this rollup is built into and served from — this
    * grain's own `#@ preaggregate storage=`, and only ever that. Undefined for a
    * colocated rollup built into the base's own warehouse.
    *
    * NOT inherited from the base's `#@ persist storage=`; see
    * {@link basePersistNamespace} for why a destination is not inherited.
    *
    * Mutually exclusive with {@link namespace} (refused at publish as
    * `namespace_with_storage`).
    */
   storage?: string;
   /** The one grain, canonically sorted. */
   grainDimensions: string[];
   /** The measures served here, sorted by name. */
   measures: RollupMeasure[];
   /**
    * The base's Malloy `sourceID`, `<declared name>@<declaring file URL>`. A
    * model that only imports the base (an `index.malloy` re-export) cannot
    * rename-import it from itself, so the companion imports it from the file
    * that declares it (preaggregation_seams.spec.ts pins it). Absent when the
    * compiler gave none.
    */
   baseSourceID?: string;
}
