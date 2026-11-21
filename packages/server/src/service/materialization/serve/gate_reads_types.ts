// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shape of what an entry point's row-level gates and row scope read
 * (`gate_reads.ts` computes it, `gate_collation.ts` judges its string
 * comparisons), and the collation probe port that judges them, in a module
 * of its own so none of those modules imports another for it.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import type { TableRead } from "../domain/gate_policy";

/** What an entry point's row-level gates read. */
/** A column of a warehouse table, as a model names it. */
export interface ColumnOrigin {
   /** The `.table()` source's connection. */
   readonly connectionName: string;
   /** Its table path, as `PersistSourcePlan.reads` names it. */
   readonly table: string;
   /** The column's name in that table. */
   readonly column: string;
}

export interface GateReads {
   /**
    * Every name the gates read, and every field those are computed from,
    * through renames, expressions, joins and query stages. Over-collected on
    * purpose, for the readers that decline on a match (the zone checks).
    */
   readonly names: ReadonlySet<string>;
   /**
    * Each stored column the gates read, as the names it goes by along the way
    * from the gate to the column (a rename's, a query stage's output, the
    * base column's), any of which a stored table may hold it under.
    */
   readonly columns: readonly (readonly string[])[];
   /**
    * For each of {@link columns}, by position, the warehouse table column it
    * is a plain read of (a field of a `.table()` source with no expression,
    * through renames and joins), or undefined when anything computes it on
    * the way (an expression, a query stage). What a stored copy's gate
    * column holds a copy of, which the gate policy reads its upstream type
    * by. Absent: none known.
    */
   readonly columnOrigins?: readonly (ColumnOrigin | undefined)[];
   /**
    * A name the gates read that could not be followed to a column: a reader
    * that judges the columns must fail closed.
    */
   readonly unresolved: boolean;
   /**
    * The rows the query reads are scoped by a `where:` (the source's own, one
    * it extends, a composite member's, or a query-shaped source's base's or
    * stage's): its columns are read here as a gate's are.
    */
   readonly scoped: boolean;
   /** The entry point carries a gate: the gate policy judges these columns past it. */
   readonly gated: boolean;
   /**
    * Every comparison the gates and scopes make, and every computed field they
    * read through, is an equality (`=`, `!=`, `in`, `is null`) over fields,
    * givens and literals ({@link equalityOnlyExpr}), or compares only values
    * that are not strings (`amount > 100`, read as `comparesNoString` reads
    * a leaf), which no collation orders. Equality is byte-exact
    * under a deterministic collation; anything else (an ordering, a pattern,
    * a function) compares as the column's collation orders it, so it holds
    * on a stored copy only where that collation is binary.
    */
   readonly equalityOnly: boolean;
   /**
    * The field paths, from the entry point, that the gates and the entry
    * point's own `where:` terms compare; `entryPathsComplete` is false when a
    * scope is read from elsewhere (a composite member, a join, a query-shaped
    * source's base or stage), whose fields have no path from the entry point.
    */
   readonly entryPaths: readonly (readonly string[])[];
   readonly entryPathsComplete: boolean;
   /**
    * Every comparison the row-level gates and scopes make is over values that
    * are not strings: each leaf of each expression is a field whose type is
    * known and not a string, a number, boolean, date or timestamp literal,
    * null, `now`, or a given whose declared type is not a string, under only
    * comparison and logical operators and time arithmetic (`delta`, `trunc`,
    * `extract`) over a non-string operand; and every computed field they read
    * through holds the same of its own expression (a boolean dimension over a
    * string, `region > 'M'`, compares that string). A string, a cast, a function, a
    * `pick` or a node this does not know reads false. A lock (`#(authorize)`)
    * is decided in Publisher, exactly, not by the warehouse, so its text is
    * not a comparison here.
    */
   readonly comparesNoString: boolean;
   /**
    * Every string these compare is a column's, or derived from one: none is
    * a literal or a given alone, so no comparison falls back to the
    * database's default collation (a column's collation outranks it).
    * Absent reads false.
    */
   readonly comparesOnlyColumns?: boolean;
   /**
    * Groups of entry paths that one value combines (`cc = cu`,
    * `concat(cc, cu)`): live compares them under one collation only when
    * every column in a group holds the same, and fails otherwise.
    */
   readonly sameCollation?: readonly (readonly (readonly string[])[])[];
   /**
    * Of {@link entryPaths}, those whose every read is of a source known to be
    * read live (a live join beside a stored table), which no stored table's
    * collation changes: the stored-side check passes over them. Set only for
    * a query's result reads (`result_reads.ts`), never for a gate's, whose
    * columns must each be held. Absent: none.
    */
   readonly livePaths?: readonly (readonly string[])[];
}

/** A table an answer read, as the stored-side probe reads it. */
export type ProbedTable = TableRead & {
   readonly virtualHandle?: string;
   readonly tablePath?: string;
   readonly destinationName?: string;
};

/**
 * The collation probes, as a port: whether the source compares what `reads`
 * compare as a byte-exact copy does, and whether the stored tables an answer
 * read compare their strings as their sources do. Each answers why not, or
 * undefined. They read the warehouse; admission only reads their answers.
 */
export interface CollationProbe {
   sourceRefusal(args: {
      authorRunnable: QueryMaterializer;
      reads: GateReads | undefined;
      givens?: Record<string, unknown>;
      /** The bind the query was pinned to: a probe may keep reads per bind. */
      bind?: object;
   }): Promise<string | undefined>;
   storedRefusal(args: {
      tables: readonly ProbedTable[];
      answerRunnable: QueryMaterializer;
      virtualMap: ReadonlyMap<string, ReadonlyMap<string, string>> | undefined;
      authorRunnable: QueryMaterializer;
      reads: GateReads | undefined;
      givens?: Record<string, unknown>;
      /**
       * The stored side's warehouse reads for one answer, by what they read:
       * every comparison asked of that answer shares them.
       */
      storedReads?: Map<string, Promise<unknown>>;
      /** The bind the query was pinned to: a probe may keep reads per bind. */
      bind?: object;
      /** A database table its catalog does not list is passed over (a result's). */
      unlistedPasses?: boolean;
   }): Promise<string | undefined>;
}
