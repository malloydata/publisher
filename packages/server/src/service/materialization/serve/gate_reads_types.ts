// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shape of what an entry point's row-level gates and row scope read
 * (`gate_reads.ts` computes it, `gate_collation.ts` judges its string
 * comparisons), in a module of its own so neither imports the other for it.
 */

/** What an entry point's row-level gates read. */
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
    * null, or a given whose declared type is not a string, under only
    * comparison and logical operators; and every computed field they read
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
}
