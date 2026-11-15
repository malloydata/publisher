// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the planner decides for each stored table: `TablePlan`, one per
 * planned table with its kind fixed at plan, and, as plain data the build and
 * the serving legs both read, a `#(materialize)` source's plan (its stored
 * columns, read-time terms, pair layout and the bases its served copy keeps),
 * a persisted view's and a persisted named query's, a `#(preaggregate)`
 * rollup's and a table's classified gate, with the one reader of the
 * `sourceID` that keys each of them.
 *
 * Planned in `plan/` (`build_file_generator.ts`, `persisted_view.ts`,
 * `rollup_plan.ts`); read by the run's builds, the bind, the served copy and
 * the rollup, view and query legs, and the read-text writers
 * (`text/read_text.ts`).
 *
 * Pure.
 */

import type { BuildTarget, PersistSource } from "@malloydata/malloy";
import type { MaterializeReadTimeTerm, TermUse } from "./read_time_terms";
import type { AggregateLeaf, MeasureExpr, RollupPlan } from "./rollup_measures";

/** A stored column's name and type, in DuckDB's spelling (`DESCRIBE`'s). */
export interface StoredColumn {
   name: string;
   type: string;
}

/** A zero-based Malloy range, as a compiled source's `location.range` carries. */
export interface KeptRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** One ancestor on the kept walk, as the compiled model places it. */
export interface KeptAncestor {
   sourceID: string;
   /** Its declaration's range in the file (`<name> is <definition>`). */
   range: KeptRange;
   /**
    * Renames of it declared in the file (`b2 is base`, and renames of those).
    * The base edge reads an extension of one as an extension of the ancestor
    * itself (a rename adds nothing, so the two are alike and the edge takes
    * the one the other is built from), so the text may name one of these
    * where the walk names the ancestor.
    */
   aliases?: KeptAlias[];
}

/** A plain rename on the kept walk ({@link KeptAncestor.aliases}). */
export interface KeptAlias {
   sourceID: string;
   range: KeptRange;
}

/** `<name>@<url>` split, or undefined for any other shape. */
export function splitSourceID(
   sourceID: string,
): { name: string; url: string } | undefined {
   const match = /^(.+)@([a-z][a-z0-9+.-]*:.*)$/i.exec(sourceID);
   return match ? { name: match[1], url: match[2] } : undefined;
}

/** One `#@ persist` view, as planned. */
export interface PersistedViewPlan {
   /** Tells a view's plan from a named query's (`kind: "query"`). */
   kind?: "view";
   /** The declaring source's `sourceID` (`<name>@<declaring file URL>`). */
   sourceID: string;
   /** The source's name in its declaring file. */
   sourceName: string;
   /** The view's name on the source. */
   viewName: string;
   /** The generated build source's name, inside the companion. */
   buildSourceName: string;
   /** The generated build source's `sourceID`, as the companion compiles it. */
   buildSourceID: string;
   /** The author's `#@` lines on the view, verbatim, trimmed. */
   persistLines: string[];
   /** The `storage=` destination, when the view declares one. */
   storage?: string;
   /** The declaring file's URL. */
   declaringURL: string;
}

/** One `#@ persist` named query, as planned. */
export interface PersistedQueryPlan {
   kind: "query";
   /** The query's name in its declaring file. */
   queryName: string;
   /**
    * The run target's `sourceID` (`<name>@<declaring file URL>`), when the
    * query runs a source declared by name in its own file; absent for a run
    * target written inline (`query: q is duckdb.table('t') -> { … }`).
    */
   sourceID?: string;
   /** The run target's name, beside {@link sourceID}. */
   sourceName?: string;
   /**
    * For a run target imported from another file (which compiles into the
    * query whole, as an inline one does): the name the model gives it.
    */
   importedSource?: string;
   /** The generated build source's name, inside the companion. */
   buildSourceName: string;
   /** The generated build source's `sourceID`, as the companion compiles it. */
   buildSourceID: string;
   /** The author's `#@` lines on the query, verbatim, trimmed. */
   persistLines: string[];
   /** The `storage=` destination, when the query declares one. */
   storage?: string;
   /** The declaring file's URL. */
   declaringURL: string;
}

/** One join past the first of an inlined path of join_manys. */
interface MaterializePairHop {
   /** The join's name on the previous hop's joined source. */
   join: string;
   /** This hop's joined source's primary key, as that source names it. */
   key: string;
   /** This hop's joined source's declaration. */
   sourceID: string;
   /**
    * The row flag of the level above this hop ({@link pairLevelRowColumn}):
    * one row per distinct row down to the previous hop.
    */
   rowColumn: string;
}

/** How a pair table (an inlined join_many, P5) is laid out. */
export interface MaterializePair {
   /** The inlined join_many's name on the materialized source. */
   join: string;
   /** The source's primary key, a stored column: one parent per value. */
   parentKey: string;
   /** The joined source's primary key, as the joined source names it. */
   childKey: string;
   /** The joined source's declaration, whose text the served copy re-roots. */
   childSourceID: string;
   /** The parent-row flag's column ({@link PAIR_PARENT_ROW}). */
   parentRowColumn: string;
   /**
    * For a path of join_manys (`inline=["items.options"]`): the joins past the
    * first, in order. Absent: one hop.
    */
   hops?: MaterializePairHop[];
   /**
    * For an incremental pair table: its watermark column
    * ({@link PAIR_WATERMARK}) and the stored columns it is the greatest of,
    * at least one of the source's own and one of the joined source's.
    */
   watermark?: { column: string; parts: string[] };
   /**
    * For a path of more than one hop: the levels between the source and the
    * leaf (1 for the path's first join) each of whose rows has exactly one
    * row of every level above it, so its row flag marks one row per row of
    * that level whichever parent it is read under. Proven from the joins and
    * the build's key check ({@link pairKeyChecks}): each join down to the
    * level is keyed on its parent level's primary key, which the build checks
    * unique and never null, so a row meets at most one parent row. A middle
    * level's aggregate beside a deeper level is served only at these levels
    * (see `governance/ir/pair_flat_query.ts`). Absent: none.
    */
   oneParentLevels?: number[];
}

/**
 * A pair table that stores more than one inlined path: one block of rows per
 * path, each block the rows of that path's own pair table, marked by
 * {@link PAIR_LANE_COLUMN}. Every block stores every column, the other
 * blocks' as typed nulls, and every level flag down to the deepest path.
 */
export interface MaterializePairBlocks {
   /** The blocks, by block number; the first is the plan's `pair`. */
   blocks: MaterializePair[];
   /** The block-number column ({@link PAIR_LANE_COLUMN}). */
   laneColumn: string;
   /** The connection the build's SQL source runs on: the source's own. */
   connection: string;
   /**
    * Each joined column's Malloy type, for the other blocks' typed nulls
    * (see {@link typedNull}).
    */
   columnTypes: Record<string, string>;
   /** The number of level flags every block writes: the deepest path's depth. */
   levels: number;
   /**
    * For an incremental table: its watermark ({@link PAIR_WATERMARK}), per
    * parent the greatest of `parts` over the parent's rows in every block,
    * so a range selects a parent's rows in all of them or none.
    */
   watermark?: { column: string; parts: string[] };
}

/** One stored column of a materialized source. */
export interface MaterializeColumn {
   /** The dimension's name on the author's source. */
   dimension: string;
   /** The column the build stores it as: the dimension's own name. */
   column: string;
   /**
    * Whether the source computes it (the served copy drops its definition so
    * the stored column stands in) or reads it from its table.
    */
   computed: boolean;
   /**
    * The access modifier the author's source gives the field, when it is not
    * public (P3). The build reads a copy of the source with its own access
    * modifiers lifted so it can select the field (see
    * {@link buildCopyText}); the served copy keeps the modifier, so a caller
    * still cannot select it.
    */
   access?: "private" | "internal";
   /**
    * For a stored computed dimension of the source's own: the probe compiler
    * found its value the same in every query time zone (P9), so a query that
    * sets a `timezone:` reads it from the table. Absent: unknown, and such a
    * query is answered live.
    */
   zoneFree?: true;
   /**
    * For a column of an inlined join's joined source: the join's name, with
    * {@link dimension} the joined source's field. The served copy declares
    * these under the join's name, in place of the join.
    */
   join?: string;
   /**
    * For a column of a joined source two or more joins down an inlined path
    * of join_manys: the joins from the source to it, {@link join} first
    * (`["items", "options"]`), with {@link dimension} its field. Absent for
    * the path's first join's columns.
    */
   path?: string[];
}

/** The P8 uniqueness check for one inlined join_one, run before each build. */
export interface InlineUniquenessCheck {
   /** The join's name on the materialized source. */
   join: string;
   /** The joined source's column the join is keyed on. */
   keyColumn: string;
   /** The generated query's name in the build companion. */
   queryName: string;
   /** The joined source's declaration, which the check groups. */
   joinedSourceID: string;
   /** The connection the check runs on: the joined source's. */
   connectionName?: string;
   /**
    * The check's SQL, compiled with the companion (never run at plan time).
    * Any row it returns is a key the joined source holds twice.
    */
   sql?: string;
   /**
    * Set on a pair table's key check: the level of the path whose key it
    * checks, 0 for the source's own. Such a check also fails on a null key,
    * since a level's row flag marks one row per distinct key, where live
    * counts each row
    * ({@link pairKeyChecks}). `join` is then the path down to that level
    * (empty for the source).
    */
   pairLevel?: number;
   /**
    * Set on a pair table's leaf key check ({@link pairKeyChecks}): it never
    * fails the build. An auto-run runs it once the table is written and
    * records what it found beside the table (unique and never null, under
    * {@link joinedSourceID}); the served copy re-declares a count through the
    * path over the leaf's key only where that record passed.
    */
   recordOnly?: true;
}

/** What a caller sets that a left-out dimension's value depends on (P9). */
export type CallerInput = "given" | "current_time" | "time_zone";

/**
 * A dimension of a materialized source left out of its build because its
 * value depends on the caller (P9): the served copy computes it at read.
 */
export interface LeftOutDimension {
   dimension: string;
   reads: CallerInput[];
}

/** A given a read-time term reads, and the file that declares it. */
export interface MaterializeGivenDeclaration {
   name: string;
   url: string;
   /**
    * The path the generated file imports it from, relative to the companion;
    * set by the caller that knows the package layout. Absent: the author's
    * model, which declares it.
    */
   from?: string;
}

/** What the generator writes for one `#(materialize)` source. */
export interface MaterializePlan {
   /** The author's source name in this model. */
   sourceName: string;
   /** The source's identity (the keying rule): its `sourceID`. */
   sourceID: string;
   /** The generated build source's name. */
   buildSourceName: string;
   /** The stored columns, in the source's field order. */
   columns: MaterializeColumn[];
   /**
    * The build source's compiled output, each column in DuckDB's type
    * spelling (`storedColumnsOfOutput`), read off the compiled companion.
    * A colocated table's schema is not captured at build, so its serve
    * binding declares these. Absent before the companion compiles.
    */
   storedColumns?: StoredColumn[];
   /**
    * The `#@ persist` keys carried from the declaration, by key path: `name`,
    * or `freshness.window` for a property of a carried group.
    */
   persist: Record<string, string>;
   /**
    * The source's read-time `where:` terms: the ones the served copy applies
    * again over the stored table at every read. Absent when it has none.
    */
   readTimeTerms?: MaterializeReadTimeTerm[];
   /**
    * The declaration's range in its declaring file, when it has given terms
    * or hidden columns: where its build copy takes the terms out and lifts
    * its own access modifiers.
    */
   declaration?: TermUse;
   /** The givens its given-reading terms read, for the generated file to import. */
   givenDeclarations?: MaterializeGivenDeclaration[];
   /** The joins the declaration inlines (`inline=[…]`). */
   inline?: string[];
   /**
    * The computed dimensions left out of the build because each depends on
    * the caller (P9), in field order. Absent when none is.
    */
   leftOut?: LeftOutDimension[];
   /**
    * The probe compiler found the source's rows the same in every query time
    * zone (P9): its `where:` terms and base relation compile to the same SQL
    * at every probe zone. Absent: a `where:` against a timestamp literal,
    * which compiles in the query's zone (or a base the probe could not
    * clear), filtered the table's rows in the build's zone, so a query that
    * sets a `timezone:` over it is answered live.
    */
   zoneFreeRows?: true;
   /** One uniqueness check per inlined join_one (P8). */
   uniquenessChecks?: InlineUniquenessCheck[];
   /**
    * For an inlined join_many: the pair table's layout (P5); for a table
    * that stores more than one inlined path, its first block's.
    */
   pair?: MaterializePair;
   /** For a pair table that stores more than one inlined path: its blocks. */
   pairBlocks?: MaterializePairBlocks;
   /**
    * Why a pair table's `refresh="incremental"` is not honoured, when it is
    * not: the plan carries no incremental key, so the table is rebuilt in
    * full on every refresh, and the package warns
    * ({@link MATERIALIZE_PAIR_FULL_REFRESH_CODE}).
    */
   pairFullRefresh?: string;
   /**
    * The table takes the source's own name because the declaration has no
    * `name=`, which the refusal of a colliding table name says (see
    * `refuseCollidingMaterialize`).
    */
   nameDefaulted?: true;
   /**
    * For an extension table (P10): the materialized parent whose build source
    * this source's build reads, so its table is computed from the parent's.
    * Absent: built over the raw chain.
    */
   buildsOver?: MaterializeParent;
}

/** The parent an extension table builds over (P10). */
export interface MaterializeParent {
   /** The parent's identity: its `sourceID` (the keying rule). */
   sourceID: string;
   /** The parent's name in the shared declaring file; display only. */
   sourceName: string;
   /** The parent's declaration in that file, which the build copy re-roots. */
   declaration: TermUse;
   /**
    * The declarations from just below the parent to the extension's own,
    * root first, each an `extend` or a rename of the one before, in the same
    * file: what a `storage=` build carries over the parent's stored table.
    */
   path: TermUse[];
   /**
    * For a `storage=` pair, set with the build copy: the text the extension's
    * build compiles in its destination over the parent's stored table
    * ({@link extensionOverStoredModel}). Absent until the build copy is
    * written; absent for a colocated pair, whose build reads the parent's
    * build source in its own warehouse.
    */
   overStored?: StoredChain;
   /**
    * Declarations of the same file, off the chain, that the build copy
    * replaces with an empty source over the parent's build source: each read
    * the parent's re-rooted declaration and did not compile over it, which
    * would fail this extension's build copy for another source's sake
    * (materialize_compile.ts, `compileContained`). Empty, so anything on the
    * chain that reads one fails to compile and the extension builds over the
    * raw chain instead: an omission never changes what the build reads.
    */
   omit?: TermUse[];
}

/**
 * What a `storage=` extension table's build compiles in its destination
 * (P10): no source but the parent's stored table, so the build reads the
 * destination alone, never the warehouse.
 */
export interface StoredChain {
   /** The parent's generated build source, which the declarations re-root on. */
   parentBuildSourceName: string;
   /** The author file's `##!` flags, which its declarations may need. */
   flags: string[];
   /**
    * `source:` statements, root first: the parent re-rooted on
    * {@link parentBuildSourceName}, then each declaration on
    * {@link MaterializeParent.path}, as the extension's build copy writes them.
    */
   declarations: string;
}

/** One stored partial of a measure. */
interface RollupPartial {
   /** The stored column. */
   column: string;
   leaf: AggregateLeaf;
}

/** One measure a rollup serves. */
export interface RollupMeasure {
   name: string;
   partials: RollupPartial[];
   /** The measure re-stated over its merged partials. */
   merged: MeasureExpr;
   /**
    * Grain dimensions this measure's join-level `sum` or `count` merges across
    * (A2): every grain dimension but the join's key. Its partial is the joined
    * rows' total within one group, and two groups that share a key hold the
    * same joined rows, so a query reads the measure from the rollup only when
    * each of these is grouped or fixed at every level that aggregates it.
    */
   fanOutDimensions: string[];
}

/**
 * One `#(preaggregate)` rollup: one base, one grain, one placement, one table.
 * A {@link RollupPlan} for the build plan's plumbing (its wire `preaggregate`
 * entry, eligibility and run instructions), with what the variant needs.
 */
export interface RollupVariantPlan extends RollupPlan {
   /**
    * Marks a rollup the rollup leg plans and serves: a `#(preaggregate)`
    * one, or a v1 `#@ preaggregate` line read as one.
    */
   variant: true;
   baseSourceID: string;
   /** The base's declared name (its name in its declaring file). */
   baseDeclaredName: string;
   /** The measures served, sorted by name. */
   rollupMeasures: RollupMeasure[];
   /**
    * Grain dimensions read through a join_many or join_cross (A6): a query
    * reads the rollup only when each is fixed at every aggregating level.
    */
   fanOutDimensions: string[];
   /**
    * Whether the base declares a row-level gate (`#(access_filter)` or
    * `#(authorize)`): the join-to-rollup branch (A7) reads the rollup through a
    * join, where no gate is grafted, so it declines such a base.
    */
   baseGated: boolean;
   /**
    * Grain dimensions the base declares `internal` (A4: only a field a gate
    * reads), stored, and redeclared `internal` on the variant.
    */
   internalGrain: string[];
   /**
    * The uniqueness check of the grain's one dimension (known limitation 29):
    * set when the grain is one dimension of the base and the companion
    * compiled the check, for a colocated and a `storage=` rollup alike. A run builds it only
    * when a join_one in the package is keyed on that dimension (see
    * {@link joinOneKeysOf}); the join-to-rollup branch (A7) reads its outcome.
    */
   keyCheck?: RollupKeyCheck;
   /**
    * The primary key checks of the query sources that read this rollup through
    * a join read as a join_many (see {@link readsAsJoinMany}): set at plan time
    * for each such source the package declares (see {@link parentKeyNeedsOf}),
    * and run by the rollup's build. The join-to-rollup branch (A7) reads their
    * outcome: a rollup row is met by every parent row that shares its key, so
    * the leg answers as live does only when the query source's key is unique
    * and never NULL.
    */
   parentKeyChecks?: ParentKeyCheck[];
}

/**
 * A query source's primary key check: whether a source that reads a rollup
 * through a join read as a join_many holds a value of its declared
 * `primary_key` more than once, or a NULL. The SQL is compiled from the
 * author's model, over the source's rows as a query reads them, and returns
 * one row when the key repeats or is NULL.
 */
export interface ParentKeyCheck {
   /** The query source whose key is checked. */
   sourceID: string;
   keyColumn: string;
   /** The connection the query source reads; the check runs only on it. */
   connectionName: string | undefined;
   sql: string;
}

/**
 * A rollup's key check: whether its base holds a value of its one grain
 * dimension (a join_one's key) on more than one row. The SQL is compiled from
 * the rollup companion, over the rows the rollup's build reads, and returns
 * one row when a non-NULL value repeats.
 */
export interface RollupKeyCheck {
   keyColumn: string;
   sql: string;
}

/**
 * Whether every gate on the entry point RESOLVED, not which route it was written
 * on. `"row_level"` is kept as the name so stored plans still read; an
 * `#(authorize)` lock lands there too, deliberately — this decides whether a
 * source may be BUILT, and `Model.queryEntryPointHasRowLevelGate` blocks routing
 * to frozen rows per request on ANY collected gate.
 */
export type PersistSourceGateClassification = "row_level" | "rejected";

/**
 * `classification`: whether the entry point's gates resolved to something
 * enforceable. `attributed`: `false` when `isAuthorizeAttributedToEntryPoint`
 * (`plan/materialization_eligibility.ts`) finds a note reachable only through a join —
 * checked independently because `collectEntryPointGates` does not trace joins,
 * so `classification` cannot see a join-carried gate. `inheritedRowFilter`:
 * an `#(access_filter)` the entry point reads from a source it derives from or
 * extends rather than declares itself. A table of it holds every caller's
 * rows, and the served copy, which re-roots the entry point and so replaces
 * the source that declares the gate, cannot graft it: every query is declined
 * and answered live, so such a table is never built. Absent reads false (a
 * plan recorded before it).
 */
export interface PersistSourceGateOutcome {
   classification: PersistSourceGateClassification;
   attributed: boolean;
   inheritedRowFilter?: boolean;
}

/**
 * What a planned table holds, fixed when it is planned and carried to the
 * build, its telemetry and the binding: never re-derived from which other
 * plan a source also has, nor from a name.
 */
type TableKind = "persist" | "materialize" | "pair" | "rollup" | "view";

/** What every planned table carries, whatever its kind. */
interface TablePlanFacts {
   /** The planned source, as Malloy's persistence planner planned it. */
   source: PersistSource;
   /**
    * The build target the source maps onto, for a keyable source (an
    * unkeyable one has none). The addressing helpers address it without the
    * fallback lookup.
    */
   target?: BuildTarget;
   /**
    * The package-relative path of the declaring `.malloy` model. Only known
    * at plan: a sourceID embeds an absolute `file://` URL with no package
    * boundary.
    */
   modelPath?: string;
   /** The entry point's `#(authorize)` gate, classified where it compiled. */
   gate?: PersistSourceGateOutcome;
   /**
    * The declared primary key a build checks for a repeated or NULL value
    * over this table, when it is stored in a destination and read beside a
    * `storage=` rollup through a join_many keyed on it (the join-to-rollup
    * leg in a destination). Absent: no such leg, so no check.
    */
   storedKeyCheck?: string;
}

/**
 * One table a run should build: its kind, the source that builds it and what
 * the planner decided for it. A `materialize` or `pair` table carries its
 * `#(materialize)` plan and a `rollup` its rollup; no other kind carries
 * either.
 *
 * Holds the planned Malloy source, so it stays on the thread that planned
 * it; what crosses the package-load worker is the plan's projection.
 */
export type TablePlan = TablePlanFacts &
   (
      | {
           kind: Extract<TableKind, "persist" | "view">;
           materialize?: undefined;
           rollup?: undefined;
        }
      | {
           kind: Extract<TableKind, "materialize" | "pair">;
           materialize: MaterializePlan;
           rollup?: undefined;
        }
      | {
           kind: Extract<TableKind, "rollup">;
           rollup: RollupPlan;
           materialize?: undefined;
        }
   );

/** A `#(materialize)` plan's kind: `pair` when it stores an inlined join_many. */
export function materializeKindOf(
   plan: Pick<MaterializePlan, "pair">,
): "materialize" | "pair" {
   return plan.pair ? "pair" : "materialize";
}
