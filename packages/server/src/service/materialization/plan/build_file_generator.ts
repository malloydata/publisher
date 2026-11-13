// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The build-file generator for `#(materialize)` (materialization v2, phase 3).
 *
 * `#(materialize)` desugars as the plan defines it: for a source `src`
 * annotated `#(materialize)`, Publisher writes a generated build source,
 *
 *     #@ persist …
 *     source: src__mat is src -> { select: <src's stored columns> }
 *
 * in a companion model beside the author's (`<model>.materialize.malloy`),
 * never on disk and never in the package's model list, and Malloy's own
 * persistence planner builds it on v1's pipeline: fingerprinting, storage,
 * incremental and hosted builds all apply unchanged, and `addressing.ts`
 * addresses the generated target like any other persist source.
 *
 * What is stored: every atomic dimension of `src`, each under its own name,
 * computed or not (decided 2026-11-12), so the stored table carries the
 * author's names and an outside reader sees `order_month`, not a suffixed
 * column. The served copy (the re-root rewriter) drops a stored computed
 * dimension's definition, so the column stands in for it, and carries the
 * dimension's annotations onto the served `type:` declaration. Measures, views
 * and joins are not stored: the served copy keeps them as written, re-bound
 * over the stored columns.
 *
 * Hidden fields (P3, decided 2026-11-12). A query cannot select a `private`
 * or `internal` field, so the source's own and its inherited `internal`
 * fields are built from a copy of its file with its own access modifiers
 * lifted ({@link buildCopyText}), and stored under their own names, each
 * column marked with its `access`; `materialize_compile.ts` keeps only the
 * ones the compiled copy lets the build select, so a field private on a
 * source it extends, which the source cannot read, is never stored, nor is
 * an `except:`ed field. The served copy writes the modifiers again, so a
 * caller cannot select a stored hidden column.
 *
 * The table: named after the author's source (`src`), or the declaration's
 * `name=`. `src__mat` is only the generated build source's internal name in
 * the companion model. Without a `name=`, a `storage=` source's table defaults
 * to the source's own name (the destination is Publisher's, and the existing
 * collision checks refuse two sources on one table). A colocated source (no
 * `storage=`) is not built at all until the served copy can serve it
 * ({@link COLOCATED_NOT_SERVED}); v1's colocated `#@ persist` is unchanged.
 *
 * Joins (phase 3 PR 4). A join is a reference by default: it is not stored,
 * the served copy keeps it as written, and it reads the joined source (its own
 * table, when that source is materialized) at query time. So a dimension whose
 * value reads through a reference join is not stored either; the served copy
 * computes it at read, through the join. `inline=["j"]` on a join_one stores
 * the dimensions that read through `j` (one hop: not through a join of the
 * joined source), and the joined source's own exposed dimensions as
 * `j__<field>` columns, which the served copy answers `j.<field>` from with no
 * join at read (served_copy.ts, `InlinedJoin`).
 * An inlined join is checked unique before a build (P8): the plan carries a
 * generated group-by over the joined source on the join's key, and a build
 * whose check finds a key matched twice fails, naming the join. An auto-run
 * skips it only when the joined table provably has not changed since it
 * passed (run/inline_uniqueness.ts).
 *
 * `inline=["j"]` on a join_many (phase 4, P5) stores a pair table: one row per
 * row of live's LEFT JOIN through the join, as `select:` through it reads them,
 * so a parent with no child keeps one row with null child columns. The build
 * is a `group_by:` of the source's own stored columns and the joined source's
 * `j__<field>` columns, whose rows are distinct because the pair's keys (the
 * source's primary key and the joined source's) are among them, and one flag
 * written by a window over them (Evidence 8): {@link PAIR_PARENT_ROW}, true on
 * exactly one row per parent, childless parents included.
 *
 * The served copy reads the table as the source itself, with no join (see
 * served_copy.ts): the join's name is a record of the `j__<field>` columns, a
 * measure over the source's own fields aggregates the rows the flag marks, and
 * an aggregate through the join reads every row, which are exactly the rows
 * live aggregates through it (malloydata/malloy#1762's count of a childless
 * parent included). One flag is enough: a second, child-row flag would mark
 * every row but a childless parent's, and live counts that row too.
 *
 * What reads through the join_many on the source itself is not stored (a
 * dimension through it has one value per pair, not per parent): the served
 * copy computes it at read, over the record. Each join declared on the same
 * connection, its condition reading no other join, every side with a declared
 * primary key that is stored, and a joined source with no joins of its own
 * but the next of its path; anything else is refused.
 *
 * A path of join_manys (`inline=["items.options"]`) is stored as one pair
 * table through every join of the path (columns `items__options__<field>`),
 * with one row flag per level above the leaf ({@link pairLevelRowColumn}), and
 * more than one inlined join_many as blocks of one table, one block per path,
 * marked by {@link PAIR_LANE_COLUMN} ({@link pairBlocksSource}); the served
 * copy reads one block per query (served_copy_serve.ts). A path through a
 * join_one is refused: live aggregates past it by the joined row's key,
 * however many parents share the row, which no stored row reproduces.
 *
 * An incremental one-hop pair table refreshes whole parents over a per-parent
 * watermark ({@link pairIncremental}), or is rebuilt in full with a warning;
 * a path's table and a table of several blocks are always rebuilt in full.
 *
 * Not yet (each is a later pull request's, and is refused or left out here
 * rather than built wrong):
 * - `inline=` on a join_cross is refused.
 * - A dimension that reads a given is not left out of the build: a source
 *   with one is refused by the same eligibility gates that refuse it under
 *   `#@ persist` today.
 * - A dimension that depends on the query time zone (`hour(ts)`) is stored as
 *   the build computes it; the plan computes it at read instead, which needs
 *   the rewriter's served copy to tell such a column apart.
 * - The build reads the author's file as written, not a copy with its
 *   `#@ persist` lines removed, so a source that also carries its own
 *   `#@ persist` is refused rather than stored twice.
 *
 * A current-time `where:` term is a rolling window (rolling_window.ts): a lower
 * bound builds as written, since the build's own `now` is the widest-set
 * literal, and is recorded on the plan so the served copy re-applies it at
 * read and an incremental refresh deletes below it. Any other current-time
 * term refuses the source. A dimension whose value depends on the current time
 * is not stored: the served copy keeps its definition and computes it at read.
 *
 * Given-reading `where:` terms (P2). A `where:` that reads a given is left
 * out of the build and re-applied at read, so the table holds every caller's
 * rows and each read filters to its own. The build cannot simply select from
 * the source, which applies every `where:` it has, so the generator writes a
 * build copy of the author's model for each such source
 * (`<model>.<n>.materialize-build.malloy`, an overlay like the companion) in
 * which that source's terms, and no other's, are replaced by `true`; builds
 * from the copy's source; and writes the terms onto the generated source's own
 * `extend { where: … }`, where Malloy's persistence leaves them out of the
 * build SQL and v1's machinery reports them as stripped terms
 * (`strippedTerms`, the merge-key scope, the eligibility gate). The served
 * copy keeps each term because it is in the declaration's own trailing
 * `extend` run, which the re-root keeps as written; both sides read that run
 * with one reader (`../declaration_text.ts`). A term is only taken out when
 * that holds, and the source is refused otherwise: a term written on a source
 * this one extends, or anywhere in its declaration but that run (the re-root
 * would drop it); one that reads a field the table does not store (it could
 * not be re-applied over the table); one whose text cannot be found exactly
 * once where the compiler says it is; and a source whose build reads a
 * `#@ persist` source (the copy would plan that source a second time).
 *
 * Extension tables (P10). A materialized source whose nearest materialized
 * ancestor is planned in the same file, reached through `extend` and renames
 * only, builds over that ancestor's build source, so Malloy's planner orders
 * the two builds and the extension's table is computed from its parent's
 * (`buildsOver` on the plan). Its build copy of the author's file re-roots
 * the parent on the parent's generated build source, keeping the parent's
 * own `extend` blocks less the dimensions its table stores, and the
 * extension's text is unchanged, so it reads the parent's table. Generated
 * sources then sit in level files (`<model>.<n>.materialize-level.malloy`),
 * one per depth of the chain, which the companion imports, since a build
 * copy imports its parent's level and the companion cannot be imported by
 * what it imports. Only a pair in one `storage=` destination with no
 * incremental keys chains (a colocated `#(materialize)` is not built at all,
 * {@link COLOCATED_NOT_SERVED}, so no colocated pair is planned). A `storage=`
 * extension's warehouse cannot read its parent's stored table, so its build
 * runs in the destination from a text of its own ({@link storedChainText},
 * {@link extensionOverStoredModel}): the parent's build source declared as
 * the virtual source of its stored table, then the declarations from the
 * parent to the extension as the build copy writes them, and nothing else of
 * the file, which the plan compiles against a stand-in for the destination
 * before it chains. An extension that does not chain, or whose chained build
 * does not compile, builds over the raw chain as before: the same rows, from
 * the warehouse.
 *
 * A declaration that is not built is reported as a package warning
 * (`materialize-not-built`), so an author sees why, and the source serves live.
 *
 * Every name and value in the generated text goes through `malloy_text`.
 */

import { Annotations } from "@malloydata/malloy";
import { ownLevelNotes, sameNotePosition } from "../../annotations";
import { declaredNotes } from "../../governance/ir/declared_notes";
import {
   MATERIALIZE_KEYS,
   nearestKey,
   routeOfNote,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import { annotationValue, identifier, stringLiteral } from "../../malloy_text";
import { parseRouteTag, routeHasKey } from "../../motly";
import {
   accessLiftEdits,
   afterModelHeader,
   applyEdits,
   declarationSpans,
   documentFlagLines,
   dropDimensions,
   inverseRenameBlocks,
   type TextEdit,
} from "../declaration_text";
import {
   dropJoinDefinition,
   inlinedJoinRenames,
   inlinedJoinStatements,
   intoFirstExtend,
   type InlinedJoinText,
} from "../inlined_join_text";
import { offsetOf } from "../malloy_lexer";
import {
   classifyWhereTerms,
   currentTimeFields,
   type ClassifiedWhereTerm,
   type MalloyRange,
   type TermUse,
} from "./persist_dynamic_terms";
import { rollingWindowTerms, type CurrentTimeReadTerm } from "./rolling_window";
import { SourceMap, type FieldJoinReads, type SourceJoin } from "./source_map";

/** The route `#(materialize)` is on. */
export const MATERIALIZE_ROUTE = "materialize";

/** Appended to an author model path to name its generated build companion. */
export const MATERIALIZE_COMPANION_SUFFIX = ".materialize.malloy";

/**
 * Ends the name of a build copy: the author's file with one materialized
 * source's given-reading `where:` terms taken out (P2).
 */
export const MATERIALIZE_BUILD_COPY_SUFFIX = ".materialize-build.malloy";

/**
 * The `index`th build copy of an author model, by its file name. One copy per
 * source with read-time terms, so each source's build leaves out its own
 * terms and no other's (see {@link buildCopyText}).
 */
export function buildCopyName(modelFileName: string, index: number): string {
   return `${modelFileName}.${index}${MATERIALIZE_BUILD_COPY_SUFFIX}`;
}

/** The generated build source's name for a materialized source. */
export function buildSourceName(declaredName: string): string {
   return `${declaredName}__mat`;
}

/**
 * The `#(materialize)` keys carried onto the generated `#@ persist` line as
 * one string each, in this order.
 */
const CARRIED_KEYS = [
   "name",
   "storage",
   "refresh",
   "watermark",
   "merge_key",
   "partition",
] as const;

/**
 * The keys carried as a group of string properties (`freshness.window="24h"`,
 * `queryMetadata.team="ops"`), as `#@ persist` reads them.
 */
const CARRIED_GROUPS = ["freshness", "queryMetadata"] as const;

/** A property name inside a carried group that the persist line can hold bare. */
const PLAIN_PROPERTY = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Why a colocated `#(materialize)` (one with no `storage=`) is not built: the
 * served copy reads only a storage destination, so the table would be built
 * and never served.
 */
export const COLOCATED_NOT_SERVED =
   "colocated #(materialize) is not served yet; use storage= or #@ persist";

/** A table name `#@ persist name=` accepts (see persist_annotation_validation.ts). */
const PLAIN_TABLE_NAME = /^[A-Za-z0-9_-]+$/;

/** Keys `#(materialize)` reads for the plan itself, not the persist line. */
const PLANNED_KEYS = ["inline"] as const;

/** Keys `#(materialize)` accepts but a later pull request implements. */
const DEFERRED_KEYS: Readonly<Record<string, string>> = {};

/**
 * A pair table's parent-row flag column: true on exactly one row per parent
 * (see the module comment).
 */
export const PAIR_PARENT_ROW = "__parent_row";

/**
 * The row flag of a level between the source and the leaf of a path of more
 * than one hop: `__child_row` for the path's first join, `__child_row_2` for
 * its second, and so on. True on one row per distinct row of that level (the
 * keys from the source down to it), a childless row's null-extended row
 * included; past a block's own last hop, true on every row.
 */
export function pairLevelRowColumn(level: number): string {
   return level === 1 ? "__child_row" : `__child_row_${level}`;
}

/**
 * A table that stores more than one inlined path (one block per path) marks
 * each row with its block's number in this column.
 */
export const PAIR_LANE_COLUMN = "__pair_lane";

/** One join past the first of an inlined path of join_manys. */
export interface MaterializePairHop {
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

/**
 * An incremental pair table's watermark column: per parent, the greatest of
 * the declared watermark fields over the parent's row and every child row, so
 * a child added to (or restated under) a stored parent moves its parent's
 * watermark, and every row of one parent carries the same value (see
 * {@link pairIncremental}).
 */
export const PAIR_WATERMARK = "__pair_watermark";

/**
 * The code of the warning on a pair table whose `refresh="incremental"` is
 * not honoured: the table is rebuilt in full on every refresh, and served.
 */
export const MATERIALIZE_PAIR_FULL_REFRESH_CODE =
   "materialize-pair-full-refresh";

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
}

/**
 * Every key the generator reads, which must be the route's closed set in the
 * annotation reader (`MATERIALIZE_KEYS`); a spec holds the two equal.
 */
export const GENERATOR_KEYS: readonly string[] = [
   ...CARRIED_KEYS,
   ...CARRIED_GROUPS,
   ...PLANNED_KEYS,
   ...Object.keys(DEFERRED_KEYS),
];

/** The code of a `#(materialize)` key outside the route's closed set. */
export const MATERIALIZE_UNKNOWN_KEY_CODE = "materialize-unknown-key";

/** The code of a `#(materialize)` line that does not parse. */
export const MATERIALIZE_UNPARSEABLE_CODE = "materialize-unparseable";

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
    * Set on a pair table's key check (a path of join_manys, or a table of
    * several blocks): the level of the path whose key it checks, 0 for the
    * source's own. Such a check also fails on a null key, since a level's row
    * flag marks one row per distinct key, where live counts each row
    * ({@link pairKeyChecks}). `join` is then the path down to that level
    * (empty for the source).
    */
   pairLevel?: number;
}

/**
 * A given-reading `where:` term a materialized source's build leaves out and
 * its served copy keeps (P2). `columns` are the stored columns it reads, so a
 * read over the table can apply it. The served copy writes nothing for it:
 * the term is in the declaration's own trailing `extend`, which the re-root
 * keeps as written (see {@link termSpans}).
 */
export interface GivenReadTerm {
   kind: "given";
   /** The term as the author wrote it, verbatim. */
   code: string;
   /** The givens it reads. */
   givens: string[];
   /** The stored columns it reads. */
   columns: string[];
   /** Where it reads each field and given, in the declaring file. */
   at: TermUse[];
}

/**
 * A `where:` term of a materialized source that depends on the read, so the
 * served copy applies it again over the stored table, by what it reads
 * (`classifyWhereTerms`, the one term classifier), told apart by `kind`. A
 * current-time term (rolling_window.ts) is built as written, since the
 * build's own `now` stores the widest set, and is also the bound an
 * incremental refresh deletes below. A given-reading term (P2) is left out of
 * the build and kept by the served copy as the author wrote it.
 */
export type MaterializeReadTimeTerm = CurrentTimeReadTerm | GivenReadTerm;

/** The given-reading terms (P2) among a plan's read-time terms. */
export function givenReadTerms(
   plan: Pick<MaterializePlan, "readTimeTerms">,
): GivenReadTerm[] {
   return (plan.readTimeTerms ?? []).filter(
      (t): t is GivenReadTerm => t.kind === "given",
   );
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
    * written; every chained pair is a `storage=` pair, since a colocated
    * `#(materialize)` is not built ({@link COLOCATED_NOT_SERVED}).
    */
   overStored?: StoredChain;
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

/** A declaration the generator could not plan, and why. */
export interface MaterializeRefusal {
   sourceName: string;
   sourceID?: string;
   reason: string;
   /**
    * Set when the declaration cannot be read as written (it does not parse,
    * or has an unknown key):
    * the finding is `malformed` under this code, refused at publish, rather
    * than the `unservable` `materialize-not-built`.
    */
   malformedCode?: string;
}

interface SourceDefLike {
   sourceID?: unknown;
   /** Where the source's own statement was parsed. */
   location?: {
      url?: string;
      range?: { start?: { line: number; character: number } };
   };
   connection?: unknown;
   primaryKey?: unknown;
   annotations?: Parameters<typeof ownLevelNotes>[0];
   fields?: unknown[];
   filterList?: unknown;
}

interface FieldLike {
   name?: unknown;
   as?: unknown;
   type?: unknown;
   e?: unknown;
   expressionType?: unknown;
   accessModifier?: unknown;
   join?: unknown;
}

/** The Malloy atomic types a stored column can hold. */
const ATOMIC_TYPES = new Set([
   "string",
   "number",
   "boolean",
   "date",
   "timestamp",
   "timestamptz",
   "json",
   "sql native",
]);

/**
 * A compiled field's access modifier when it is not public, or undefined. Fails
 * closed, as `isHidden` in preaggregation_synthesis.ts does: a modifier it does
 * not know reads as private. The one reader of `accessModifier` for P3: the
 * plan, the build copy's selectable set and the served copy's access check.
 */
export function hiddenAccess(field: {
   accessModifier?: unknown;
}): "private" | "internal" | undefined {
   if (field.accessModifier == null || field.accessModifier === "public") {
      return undefined;
   }
   return field.accessModifier === "internal" ? "internal" : "private";
}

/** A Malloy `sourceID`'s declared name and declaring file, or undefined. */
export function splitSourceID(
   sourceID: string,
): { name: string; url: string } | undefined {
   const match = /^(.+)@([a-z][a-z0-9+.-]*:.*)$/i.exec(sourceID);
   return match ? { name: match[1], url: match[2] } : undefined;
}

/**
 * The source's own `#(materialize)` declaration, read from the notes its own
 * statement declares (not inherited: an extension of a materialized source is
 * planned only when it declares its own). `contents` is the compiled model's
 * `contents`, `def` among them (see {@link declaredNotes}). Undefined when the
 * source declares none.
 */
export function readMaterializeDeclaration(
   def: SourceDefLike,
   contents: Readonly<Record<string, unknown>> = {},
):
   | {
        tag: ReturnType<typeof parseRouteTag>["tag"];
        keys: string[];
        /** Parse errors: a declaration with any is not read at all. */
        errors: string[];
     }
   | undefined {
   const notes = declaredNotes(def, contents).filter(
      (note) => routeOfNote(note.text) === MATERIALIZE_ROUTE,
   );
   if (notes.length === 0) return undefined;
   const { tag, log } = parseRouteTag(
      new Annotations({ notes } as ConstructorParameters<
         typeof Annotations
      >[0]),
      MATERIALIZE_ROUTE,
   );
   return {
      tag,
      keys: [...tag.entries()].map(([key]) => key),
      errors: log
         .filter((entry) => entry.severity === "error")
         .map((entry) => entry.message),
   };
}

/** Whether the source carries its own `#@ persist` (not an inherited one). */
function declaresOwnPersist(
   def: SourceDefLike,
   contents: Readonly<Record<string, unknown>>,
): boolean {
   const notes = declaredNotes(def, contents).filter(
      (note) => routeOfNote(note.text) === "@",
   );
   if (notes.length === 0) return false;
   return routeHasKey(
      new Annotations({ notes } as ConstructorParameters<
         typeof Annotations
      >[0]),
      "@",
      "persist",
   );
}

/**
 * Whether the source is persisted through a `#@ persist` it INHERITS (it
 * extends or renames a persisted source, here or in an imported file) rather
 * than declares, and the source that declares it when this model holds it.
 * Read as Malloy reads it: the whole annotation chain decides whether the
 * source persists, and {@link declaredNotes} decides which of those notes its
 * own statement wrote. Undefined when it does not persist, or persists by its
 * own annotation (see {@link declaresOwnPersist}).
 */
function inheritedPersist(
   def: SourceDefLike,
   contents: Readonly<Record<string, unknown>>,
): { from?: string } | undefined {
   const chain = new Annotations(
      (def.annotations ?? {}) as ConstructorParameters<typeof Annotations>[0],
   );
   if (!routeHasKey(chain, "@", "persist")) return undefined;
   const own = declaredNotes(def, contents);
   const inherited = chain
      .forRoute("@")
      .filter(
         (note) =>
            !own.some(
               (o) => o.at && note.at && sameNotePosition(o.at, note.at),
            ),
      );
   if (inherited.length === 0) return undefined;
   for (const [name, object] of Object.entries(contents)) {
      if (object === def || !object || typeof object !== "object") continue;
      const other = object as SourceDefLike;
      if (!declaresOwnPersist(other, contents)) continue;
      const declares = declaredNotes(other, contents).some((o) =>
         inherited.some(
            (note) => o.at && note.at && sameNotePosition(o.at, note.at),
         ),
      );
      if (declares) return { from: name };
   }
   return {};
}

/**
 * The declaration's key outside the route's closed set, as a malformed
 * refusal, or undefined. A misspelt `name=` must never fall back to a default
 * table, so the source is not built, and the publish is refused.
 */
function unknownKeyRefusal(
   declaration: NonNullable<ReturnType<typeof readMaterializeDeclaration>>,
): string | undefined {
   const unknown = declaration.keys.filter((key) => !MATERIALIZE_KEYS.has(key));
   if (unknown.length === 0) return undefined;
   const near = unknown
      .map((k) => nearestKey(k, MATERIALIZE_KEYS))
      .find((k) => k !== undefined);
   return (
      `#(materialize) does not accept ${unknown.map((k) => `\`${k}=\``).join(", ")}; ` +
      `it reads ${[...CARRIED_KEYS, ...CARRIED_GROUPS, ...PLANNED_KEYS].join(", ")}` +
      (near ? ` (did you mean \`${near}=\`?)` : "")
   );
}

/**
 * The declaration's `#@ persist` keys by key path, or why one cannot be
 * carried: a value of the wrong shape (an array where a string goes would
 * otherwise be dropped silently). Unknown keys are refused first
 * ({@link unknownKeyRefusal}).
 */
function carriedPersistKeys(
   declaration: NonNullable<ReturnType<typeof readMaterializeDeclaration>>,
):
   | { persist: Record<string, string>; watermarks?: string[] }
   | { reason: string } {
   const persist: Record<string, string> = {};
   let watermarks: string[] | undefined;
   for (const key of CARRIED_KEYS) {
      if (!declaration.keys.includes(key)) continue;
      // A list of watermark fields is a pair table's (see pairIncremental);
      // the caller refuses it on any other source.
      const list = key === "watermark" ? declaration.tag.array(key) : undefined;
      if (list !== undefined) {
         const names = list.map((i) => i.text());
         if (names.some((n) => typeof n !== "string" || n.trim() === "")) {
            return {
               reason:
                  "#(materialize) `watermark=` must be a string, or a list of " +
                  'field names such as watermark=["updated_at", "items.loaded_at"]',
            };
         }
         watermarks = [...new Set((names as string[]).map((n) => n.trim()))];
         continue;
      }
      const value = declaration.tag.text(key);
      if (value === undefined) {
         return { reason: `#(materialize) \`${key}=\` must be a string` };
      }
      persist[key] = value;
   }
   for (const group of CARRIED_GROUPS) {
      if (!declaration.keys.includes(group)) continue;
      const properties = [...(declaration.tag.tag(group)?.entries() ?? [])];
      if (properties.length === 0) {
         return {
            reason: `#(materialize) \`${group}\` takes properties, as \`${group}.<name>="…"\``,
         };
      }
      for (const [property, value] of properties) {
         const text = value.text();
         if (!PLAIN_PROPERTY.test(property) || text === undefined) {
            return {
               reason: `#(materialize) \`${group}.${property}\` must be a plain name with a string value`,
            };
         }
         persist[`${group}.${property}`] = text;
      }
   }
   return watermarks ? { persist, watermarks } : { persist };
}

/**
 * A compiled field's name when it is an atomic dimension a table can hold,
 * public or not (see {@link hiddenAccess}).
 */
function storableDimension(field: FieldLike): string | undefined {
   const dimension =
      typeof field.as === "string"
         ? field.as
         : typeof field.name === "string"
           ? field.name
           : undefined;
   if (dimension === undefined) return undefined;
   if (field.join !== undefined) return undefined;
   if (typeof field.type !== "string" || !ATOMIC_TYPES.has(field.type)) {
      return undefined;
   }
   if (
      field.expressionType === "aggregate" ||
      field.expressionType === "analytic" ||
      field.expressionType === "ungrouped_aggregate"
   ) {
      return undefined;
   }
   return dimension;
}

/**
 * Whether a compiled field is a public array its table can hold as it is: a
 * persisted query's `nest:` (a repeated record) or an array column the source
 * reads, neither computed. A declared `join_many:` is the joined source's
 * type, never `array`, so it is not one. A hidden array is not stored.
 */
function storableArray(field: FieldLike): string | undefined {
   if (field.type !== "array" || field.e !== undefined) return undefined;
   if (hiddenAccess(field)) return undefined;
   const dimension =
      typeof field.as === "string"
         ? field.as
         : typeof field.name === "string"
           ? field.name
           : undefined;
   return dimension;
}

/**
 * The stored columns of a source: its atomic dimensions and public arrays
 * ({@link storableArray}), less any whose value reads through a join that is
 * not inlined (a reference join is read at query time, so what reads through
 * it is computed at read). A field whose join usage is unknown is stored only
 * when the source has no join. `inline` names the inlined joins; `joins` comes
 * from the source map.
 *
 * A private or internal dimension is a candidate too, marked with its
 * `access` (P3): whether the build can select it is settled on the build
 * copy (`materialize_compile.ts`), which drops the ones it cannot, such as a
 * field private on a source this one extends. One with a timeframe is stored
 * too: the served copy restores its timeframe with the same `__stored`
 * redefinition a public one gets, both statements carrying the field's own
 * modifier (`served_copy.ts`).
 */
export function materializeColumns(
   def: SourceDefLike,
   options: {
      joinReads?: ReadonlyMap<string, FieldJoinReads | undefined>;
      inline?: ReadonlySet<string>;
      hasJoins?: boolean;
   } = {},
): MaterializeColumn[] {
   const out: MaterializeColumn[] = [];
   const perRead = currentTimeFields(def.fields);
   for (const raw of def.fields ?? []) {
      const field = raw as FieldLike;
      const nested = storableArray(field);
      if (nested !== undefined) {
         out.push({ dimension: nested, column: nested, computed: false });
         continue;
      }
      const dimension = storableDimension(field);
      if (dimension === undefined) continue;
      if (perRead.has(dimension)) continue;
      const access = hiddenAccess(field);
      const computed = field.e !== undefined;
      if (computed && options.joinReads) {
         const reads = options.joinReads.get(dimension);
         if (reads === undefined) {
            if (options.hasJoins) continue;
         } else if (
            reads.nested ||
            [...reads.joins].some((j) => !options.inline?.has(j))
         ) {
            continue;
         }
      }
      out.push({
         dimension,
         column: dimension,
         computed,
         ...(access ? { access } : {}),
      });
   }
   return out;
}

/** A dimension's Malloy type on a compiled source, or "" when unknown. */
function columnType(def: SourceDefLike, dimension: string): string {
   for (const raw of def.fields ?? []) {
      const field = raw as FieldLike;
      const name = typeof field.as === "string" ? field.as : field.name;
      if (name === dimension && typeof field.type === "string") {
         return field.type;
      }
   }
   return "";
}

/**
 * The columns of one inlined join's joined source: its public atomic
 * dimensions (never a hidden one) that read no join of their own (one hop),
 * each stored as `<join>__<field>`, or, for a join further down an inlined
 * path of join_manys, as `<join>__<next join>__…__<field>` (`path` names the
 * joins from the source to this one). When the join itself is private or
 * internal on the source, `access` is its modifier and each column carries
 * it: the build copy lifts the join so its build can read it (P3), and the
 * served copy's record keeps the join's modifier, so a caller reads
 * `<join>.<field>` exactly where live lets it.
 */
function inlinedJoinColumns(
   map: SourceMap,
   join: SourceJoin,
   path: readonly string[] = [join.name],
   access?: "private" | "internal",
): MaterializeColumn[] {
   const joined = map.joinedReadsOf(join);
   const reads = joined.fieldJoins;
   const hasJoins = joined.joins.length > 0;
   // Computed at read, like the source's own (Rolling windows).
   const perRead = currentTimeFields(join.fields);
   const out: MaterializeColumn[] = [];
   for (const raw of join.fields) {
      const field = raw as FieldLike;
      const dimension = storableDimension(field);
      if (dimension === undefined) continue;
      // The joined source's exposed dimensions only: its hidden fields are
      // its own, not this source's to store.
      if (hiddenAccess(field)) continue;
      if (perRead.has(dimension)) continue;
      if (field.e !== undefined) {
         const r = reads.get(dimension);
         if (r === undefined ? hasJoins : r.joins.size > 0 || r.nested) {
            continue;
         }
      }
      out.push({
         dimension,
         column: `${path.join("__")}__${dimension}`,
         computed: true,
         join: path[0],
         ...(path.length > 1 ? { path: [...path] } : {}),
         ...(access ? { access } : {}),
      });
   }
   return out;
}

/** The Malloy types a pair table can store as a typed null (`null::<type>`). */
const NULLABLE_TYPES: ReadonlySet<string> = new Set([
   "string",
   "number",
   "boolean",
   "date",
   "timestamp",
   "timestamptz",
]);

/**
 * The joins an `inline=` entry names, from the source down (`"items"`, or a
 * path of join_manys, `"items.options"`), or why it cannot be inlined. The
 * first is checked as {@link inlineRefusal} checks any inlined join; each one
 * past it must be a join_many of the joined source before it, declared as a
 * source, on the source's connection, keyed on that joined source's own
 * columns. A path through a join_one is refused: live aggregates a join_many
 * past a join_one once per row of that join_many however many parents share
 * the joined row (a symmetric aggregate on its key), which no flat row
 * reproduces.
 */
function inlinedPathOf(
   entry: string,
   joins: readonly SourceJoin[],
   map: SourceMap,
   connection: unknown,
): { path: SourceJoin[] } | { reason: string } {
   const names = entry.split(".");
   const quoted = quotedName(entry);
   if (names.some((n) => n === "")) {
      return {
         reason: `inline= names ${quoted}, which is not a join or a path of joins`,
      };
   }
   const first = inlineRefusal(names[0], joins, connection);
   if (first) return { reason: first };
   const path = [joins.find((j) => j.name === names[0]) as SourceJoin];
   if (names.length === 1) return { path };
   const throughOne =
      `inline= names ${quoted}, a path through a join_one; only a path of ` +
      "join_manys is stored as a pair table (live counts a row past a " +
      "join_one once however many parents share it, which no stored row " +
      "reproduces), so inline the join_one alone";
   if (path[0].relationship !== "many") return { reason: throughOne };
   for (let k = 1; k < names.length; k++) {
      const prev = path[k - 1];
      const next = map
         .joinedReadsOf(prev)
         .joins.find((j) => j.name === names[k]);
      if (!next) {
         return {
            reason: `inline= names ${quoted}, and ${quotedName(names[k])} is not a join of the source ${quotedName(prev.name)} joins`,
         };
      }
      if (next.relationship !== "many") return { reason: throughOne };
      if (
         typeof connection === "string" &&
         next.connection !== undefined &&
         next.connection !== connection
      ) {
         return {
            reason:
               `inline= names ${quoted}, whose ${quotedName(names[k])} reads ` +
               "another connection than the source, so one build query cannot read both",
         };
      }
      const through = next.condition
         ? [...next.condition.joins].filter((j) => j !== next.name)
         : undefined;
      if (!through || through.length > 0 || next.condition?.nested) {
         return {
            reason:
               `inline= names ${quoted}, whose ${quotedName(names[k])} is keyed ` +
               "on another join than the one before it; inline only a path " +
               "whose every join is keyed on the joined source before it",
         };
      }
      if (!next.sourceID || !splitSourceID(next.sourceID)) {
         return {
            reason:
               `inline= names ${quoted}, whose ${quotedName(names[k])} joins a ` +
               "source defined in place; declare it as a source so the served " +
               "copy can read it from the pair table",
         };
      }
      path.push(next);
   }
   return { path };
}

/** A name as a refusal message shows it, in backticks. */
function quotedName(name: string): string {
   return "`" + name + "`";
}

/**
 * Why `inline=` cannot inline the join named `name` on a source, or undefined
 * when it can: a join_one or join_many on the source's own connection, its
 * condition reading no other join, joining a declared source; a join_one keyed
 * by one equality on a column of it (what the uniqueness check groups by).
 * What a join_many needs beyond this is {@link pairRefusal}'s.
 */
function inlineRefusal(
   name: string,
   joins: readonly SourceJoin[],
   connection: unknown,
): string | undefined {
   const join = joins.find((j) => j.name === name);
   if (!join) {
      return `inline= names \`${name}\`, which is not a join of this source`;
   }
   if (join.relationship === "cross") {
      return (
         `inline= names \`${name}\`, a join_cross: inlining a join_cross ` +
         "arrives in a later release"
      );
   }
   if (
      typeof connection === "string" &&
      join.connection !== undefined &&
      join.connection !== connection
   ) {
      return (
         `inline= names \`${name}\`, which reads another connection than the ` +
         "source, so one build query cannot read both"
      );
   }
   const through = join.condition
      ? [...join.condition.joins].filter((j) => j !== name)
      : undefined;
   if (!through || through.length > 0 || join.condition?.nested) {
      return (
         `inline= names \`${name}\`, whose condition reads ` +
         (through && through.length > 0
            ? `another join (${through.map(quotedName).join(", ")})`
            : "another join") +
         ", so a source row could be stored once per row of that join; " +
         "inline only a join keyed on the source's own columns"
      );
   }
   if (join.relationship === "one" && !join.keyColumn) {
      return (
         `inline= names \`${name}\`, whose condition is not one equality on a ` +
         "column of the joined source, so its uniqueness cannot be checked"
      );
   }
   if (!join.sourceID || !splitSourceID(join.sourceID)) {
      return (
         `inline= names \`${name}\`, which joins a source defined in place; ` +
         (join.relationship === "one"
            ? "declare it as a source so its uniqueness can be checked at build"
            : "declare it as a source so the served copy can read it from the pair table")
      );
   }
   return undefined;
}

/**
 * Why an inlined path of join_manys cannot be stored as a pair table, or its
 * layout (see the module comment for each rule). `path` is the path's joins,
 * the source's own first; `columns` are the source's own stored columns;
 * `joined` every inlined join's columns.
 */
function pairRefusal(
   path: readonly SourceJoin[],
   def: SourceDefLike,
   map: SourceMap,
   columns: readonly MaterializeColumn[],
   joined: readonly MaterializeColumn[],
): { pair: MaterializePair } | { reason: string } {
   const join = path[0];
   const names = path.map((j) => j.name);
   const name = quotedName(names.join("."));
   const parentKey =
      typeof def.primaryKey === "string" ? def.primaryKey : undefined;
   if (!parentKey || !columns.some((c) => c.column === parentKey)) {
      return {
         reason:
            `inline= names ${name}, a join_many, and a pair table needs the ` +
            "source's own primary key, declared and stored (a public " +
            "dimension), to keep one row per parent",
      };
   }
   for (let k = 0; k < path.length; k++) {
      const key = path[k].primaryKey;
      const prefix = names.slice(0, k + 1).join("__");
      if (!key || !joined.some((c) => c.column === `${prefix}__${key}`)) {
         return {
            reason:
               `inline= names ${name}, a join_many, and a pair table needs the ` +
               (path.length > 1
                  ? `primary key of ${quotedName(names[k])}'s joined source, `
                  : "joined source's primary key, ") +
               "declared and stored (a public dimension), to keep one row per " +
               (k === path.length - 1 ? "child" : "row of that level"),
         };
      }
      const own = map.joinedReadsOf(path[k]).joins;
      const others = own.filter((j) => j.name !== names[k + 1]);
      if (others.length > 0) {
         return {
            reason:
               path.length === 1
                  ? `inline= names ${name}, a join_many whose joined source has joins ` +
                    "of its own; a pair table stores one hop, so inline only a " +
                    "join_many to a source with no joins"
                  : `inline= names ${name}, and ${quotedName(names[k])}'s joined ` +
                    `source has joins off the path (${others.map((j) => quotedName(j.name)).join(", ")}); ` +
                    "a pair table stores only the joins it names",
         };
      }
   }
   const taken = new Set([...columns, ...joined].map((c) => c.column));
   const flags = [
      PAIR_PARENT_ROW,
      PAIR_WATERMARK,
      ...path.slice(1).map((_, k) => pairLevelRowColumn(k + 1)),
   ];
   const clash = flags.find((c) => taken.has(c));
   if (clash) {
      return {
         reason:
            `the pair table's \`${clash}\` column has the name of one of the ` +
            "source's dimensions",
      };
   }
   return {
      pair: {
         join: join.name,
         parentKey,
         childKey: join.primaryKey as string,
         childSourceID: join.sourceID as string,
         parentRowColumn: PAIR_PARENT_ROW,
         ...(path.length > 1
            ? {
                 hops: path.slice(1).map((j, k) => ({
                    join: j.name,
                    key: j.primaryKey as string,
                    sourceID: j.sourceID as string,
                    rowColumn: pairLevelRowColumn(k + 1),
                 })),
              }
            : {}),
      },
   };
}

/**
 * The blocks of a pair table that stores more than one inlined path, or why
 * it cannot: every block's columns stored on every row (the other blocks' as
 * typed nulls, so each joined column needs a type a null can take), no block
 * column or level flag named like a stored column, and no incremental
 * refresh.
 */
function pairBlocksOf(
   pairs: readonly MaterializePair[],
   paths: readonly (readonly SourceJoin[])[],
   columns: readonly MaterializeColumn[],
   connection: unknown,
): { blocks: MaterializePairBlocks } | { reason: string } {
   const listed = paths.map((p) => quotedName(p.map((j) => j.name).join(".")));
   if (typeof connection !== "string") {
      return {
         reason:
            `inline= names more than one join_many (${listed.join(", ")}), and ` +
            "the source's connection is unknown, which the pair table's build reads",
      };
   }
   const columnTypes: Record<string, string> = {};
   for (const path of paths) {
      for (let k = 0; k < path.length; k++) {
         const prefix = path.slice(0, k + 1).map((j) => j.name);
         for (const c of columns) {
            if (c.join !== prefix[0]) continue;
            const at = c.path ?? [c.join];
            if (at.join(".") !== prefix.join(".")) continue;
            const type = joinedColumnType(path[k], c.dimension);
            if (!NULLABLE_TYPES.has(type)) {
               return {
                  reason:
                     `inline= names more than one join_many (${listed.join(", ")}), ` +
                     "so each one's columns are stored as nulls on the other's " +
                     `rows, and \`${c.column}\` has a type no null can take` +
                     (type ? ` (${type})` : ""),
               };
            }
            columnTypes[c.column] = type;
         }
      }
   }
   const levels = Math.max(...pairs.map((p) => 1 + (p.hops?.length ?? 0)));
   const taken = new Set(columns.map((c) => c.column));
   const clash = [
      PAIR_LANE_COLUMN,
      ...Array.from({ length: levels - 1 }, (_, k) =>
         pairLevelRowColumn(k + 1),
      ),
   ].find((c) => taken.has(c));
   if (clash) {
      return {
         reason:
            `the pair table's \`${clash}\` column has the name of one of the ` +
            "source's dimensions",
      };
   }
   return {
      blocks: {
         blocks: [...pairs],
         laneColumn: PAIR_LANE_COLUMN,
         connection,
         columnTypes,
         levels,
      },
   };
}

/**
 * Another block's column as one block's rows store it: a null of a type the
 * `UNION ALL` cannot widen the column's own type to. A number's null is not
 * `null::number`, which is the warehouse's floating type (DuckDB's `DOUBLE`,
 * Postgres's `double precision`): the union would widen an integer or a
 * decimal column to it, so a key past 2^53, or a decimal's last digits, would
 * be stored rounded. It is a null of an integer literal's type, which every
 * numeric type absorbs without changing (an integer stays an integer, a
 * decimal keeps its scale, a float stays a float).
 */
function typedNull(type: string): string {
   return type === "number" ? "pick 0 when false else null" : `null::${type}`;
}

/** A joined source's field's Malloy type, or "" when it has none. */
function joinedColumnType(join: SourceJoin, dimension: string): string {
   for (const raw of join.fields) {
      const field = raw as FieldLike;
      const name = typeof field.as === "string" ? field.as : field.name;
      if (name === dimension && typeof field.type === "string") {
         return field.type;
      }
   }
   return "";
}

/** The Malloy types a pair table's watermark can be built over. */
const PAIR_WATERMARK_TYPES: ReadonlySet<string> = new Set([
   "date",
   "timestamp",
   "number",
   "string",
]);

/**
 * How an incremental pair table refreshes, or why it is rebuilt in full.
 *
 * The unit a refresh replaces is a whole parent: every pair row of each
 * parent whose watermark moved is deleted by the parent key and written
 * again, so `__parent_row` stays exactly one row per parent. The watermark
 * is {@link PAIR_WATERMARK}, per parent the greatest of the named fields
 * over the parent's row and all its child rows: the same value on every row
 * of one parent, so a watermark range selects whole parents, and the flag
 * the build's window writes over them is the full build's. A child-only
 * change (a child added to a stored parent, a late-arriving child, a child
 * restated in place) is seen only through a child field that moves with it,
 * so the declaration must name one: `watermark=["updated_at",
 * "segments.loaded_at"]`. Without one, or without a field of the source's own
 * (a new childless parent would not move it), the table is rebuilt in full,
 * with a warning. So too for a `merge_key=` other than the source's primary
 * key (the unit is the parent), and a rolling window over a child field
 * (its delete would remove part of a parent).
 *
 * Returns nothing to do when no watermark is named: the incremental gate
 * refuses that as it refuses it on any source.
 */
function pairIncremental(args: {
   pair: MaterializePair;
   persist: Readonly<Record<string, string>>;
   /** The watermark fields as declared: a source field, or `<join>.<field>`. */
   names: readonly string[];
   columns: readonly MaterializeColumn[];
   columnTypeOf: (c: MaterializeColumn) => string;
   /** The stored columns the source's rolling windows read. */
   windowFields: readonly string[];
}):
   | { watermark: { column: string; parts: string[] } }
   | { fullRefresh: string }
   | { none: true } {
   const { pair, names, columns } = args;
   if (names.length === 0) return { none: true };
   const join = quotedName(pair.join);
   const parts: MaterializeColumn[] = [];
   for (const name of names) {
      const dot = name.indexOf(".");
      const column =
         dot < 0
            ? columns.find((c) => c.join === undefined && c.dimension === name)
            : columns.find(
                 (c) =>
                    c.join === pair.join &&
                    c.join === name.slice(0, dot) &&
                    c.dimension === name.slice(dot + 1),
              );
      if (!column) {
         return {
            fullRefresh:
               `watermark= names \`${name}\`, which is not a stored dimension ` +
               `of the source or of the inlined join ${join}`,
         };
      }
      parts.push(column);
   }
   if (!parts.some((c) => c.join !== undefined)) {
      return {
         fullRefresh:
            `watermark= names only the source's own fields, so a child added ` +
            `to a stored parent through ${join} would not move it and would ` +
            `never be stored; name a field of ${join} that moves with each ` +
            `child as well, such as watermark=["${names[0]}", ` +
            `"${pair.join}.loaded_at"]`,
      };
   }
   if (!parts.some((c) => c.join === undefined)) {
      return {
         fullRefresh:
            `watermark= names only fields of ${join}, so a new parent with ` +
            `no children would not move it; name one of the source's own ` +
            `fields as well`,
      };
   }
   const types = new Set(parts.map((c) => args.columnTypeOf(c)));
   const [type] = [...types];
   if (types.size !== 1 || !PAIR_WATERMARK_TYPES.has(type)) {
      return {
         fullRefresh:
            `the watermark= fields must all be of one type, a date, ` +
            `timestamp, number or string (they are ${[...types]
               .map((t) => t || "unknown")
               .join(", ")})`,
      };
   }
   const mergeKey = args.persist.merge_key;
   if (mergeKey !== undefined && mergeKey.trim() !== pair.parentKey) {
      return {
         fullRefresh:
            `merge_key= names \`${mergeKey}\`; a pair table replaces whole ` +
            `parents, by the source's primary key \`${pair.parentKey}\`, so ` +
            `name that or leave merge_key= out`,
      };
   }
   const childWindow = args.windowFields.find((f) =>
      columns.some((c) => c.column === f && c.join !== undefined),
   );
   if (childWindow !== undefined) {
      return {
         fullRefresh:
            `its rolling window reads \`${childWindow}\` of ${join}, and ` +
            `deleting below it would remove part of a parent`,
      };
   }
   return {
      watermark: {
         column: PAIR_WATERMARK,
         parts: [...new Set(parts.map((c) => c.column))],
      },
   };
}

/** Whether `inner` lies within `outer` (both zero-based, end exclusive). */
function rangeWithin(inner: MalloyRange, outer: MalloyRange): boolean {
   const before = (
      a: { line: number; character: number },
      b: { line: number; character: number },
   ) => a.line < b.line || (a.line === b.line && a.character <= b.character);
   return before(outer.start, inner.start) && before(inner.end, outer.end);
}

/** `{ declaration }` when there is one, else nothing. */
function withDeclaration(declaration: TermUse | undefined): {
   declaration?: TermUse;
} {
   return declaration ? { declaration } : {};
}

/** A compiled declaration's location, or undefined when it has none. */
function declarationOf(def: SourceDefLike): TermUse | undefined {
   const location = def.location as
      | { url?: unknown; range?: MalloyRange }
      | undefined;
   if (
      typeof location?.url !== "string" ||
      typeof location.range?.start?.line !== "number" ||
      typeof location.range?.end?.line !== "number"
   ) {
      return undefined;
   }
   return { url: location.url, range: location.range };
}

/**
 * The given-reading `where:` terms of a materialized source (the `given` kind
 * of its classified terms, {@link classifyWhereTerms}), each one its own
 * declaration writes and over stored columns only, or why one is not (P2).
 * See the module comment for why each refusal is one. A term that reads both
 * a given and the current time is not this function's: rolling_window.ts
 * refuses it.
 */
export function readTimeTermsOf(
   def: SourceDefLike,
   classified: readonly ClassifiedWhereTerm[],
   columns: readonly MaterializeColumn[],
   givens: Record<string, unknown> = {},
):
   | {
        ok: true;
        terms: GivenReadTerm[];
        declaration?: TermUse;
        givenDeclarations?: MaterializeGivenDeclaration[];
     }
   | { ok: false; reason: string } {
   const located = classified.flatMap(({ dependsOn }) =>
      dependsOn.kind === "given"
         ? [
              {
                 ...dependsOn.term,
                 at: dependsOn.at,
                 givenIds: dependsOn.givenIds,
              },
           ]
         : [],
   );
   if (located.length === 0) return { ok: true, terms: [] };
   const declaration = declarationOf(def);
   const stored = new Set(columns.map((c) => c.column));
   const terms: GivenReadTerm[] = [];
   const givenDeclarations = new Map<string, string>();
   for (const term of located) {
      // malloy-text: markdown code formatting in a refusal reason, not Malloy
      const shown = term.code ? `\`where: ${term.code}\`` : "a `where:`";
      if (term.givens.length === 0 || term.code === "") {
         return {
            ok: false,
            reason: `${shown} reads a given that could not be read back, so it cannot be left out of the build`,
         };
      }
      if (
         !declaration ||
         term.at.length === 0 ||
         !term.at.every(
            (use) =>
               use.url === declaration.url &&
               rangeWithin(use.range, declaration.range),
         )
      ) {
         return {
            ok: false,
            reason:
               `${shown} reads a given and is written on a source this one ` +
               `is built from, not on this source's own declaration; the ` +
               `served copy re-roots only this declaration, so it could not ` +
               `re-apply the term at read. Move the \`where:\` onto this ` +
               `source, or materialize the source that writes it`,
         };
      }
      if (
         term.columns.length === 0 ||
         !term.columns.every((column) => stored.has(column))
      ) {
         return {
            ok: false,
            reason:
               `${shown} reads a given and a field the table does not store ` +
               `(only dimensions of this source are stored), so it ` +
               `could not be re-applied over the table at read`,
         };
      }
      // The generated `where:` reads each given by the name the author wrote,
      // so each must be importable under that name from the file declaring it.
      const declared = term.givenIds.map((id) =>
         givenDeclarationOf(givens[id]),
      );
      if (
         declared.length === 0 ||
         declared.some((d) => d === undefined) ||
         [...new Set(declared.map((d) => d?.name))].sort().join() !==
            [...new Set(term.givens)].sort().join()
      ) {
         return {
            ok: false,
            reason: `${shown} reads a given whose declaration could not be read back, so the build cannot re-write the term`,
         };
      }
      for (const d of declared as { name: string; url: string }[]) {
         givenDeclarations.set(d.name, d.url);
      }
      terms.push({
         kind: "given",
         code: term.code,
         givens: term.givens,
         columns: term.columns,
         at: term.at,
      });
   }
   return {
      ok: true,
      terms,
      declaration,
      givenDeclarations: [...givenDeclarations]
         .map(([name, url]) => ({ name, url }))
         .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)),
   };
}

/** A compiled given's name and declaring file, or undefined. */
function givenDeclarationOf(
   given: unknown,
): { name: string; url: string } | undefined {
   const g = given as
      | { name?: unknown; location?: { url?: unknown } }
      | undefined;
   return typeof g?.name === "string" && typeof g.location?.url === "string"
      ? { name: g.name, url: g.location.url }
      : undefined;
}

/**
 * Plan the `#(materialize)` sources a compiled model's `contents` declares,
 * sorted by source name so the generated text does not depend on declaration
 * order. Only a source DECLARED in this model is planned (`declaredHere`
 * decides from its `sourceID`'s file): a source another model imports or
 * re-exports through `index.malloy` is that file's to build, so a source has
 * one build source, one address and one table however many models import it.
 * Without `declaredHere` every source with a declaration is planned. Only a
 * source whose own statement declares `#(materialize)` is planned, never an
 * extension or rename that merely inherits its base's (see
 * {@link declaredNotes}).
 */
export function planModelMaterialize(
   contents: Record<string, unknown>,
   declaredHere?: (declaringURL: string) => boolean,
   givens?: Record<string, unknown>,
): {
   plans: MaterializePlan[];
   refused: MaterializeRefusal[];
} {
   const plans: MaterializePlan[] = [];
   const refused: MaterializeRefusal[] = [];
   // The joins each source has, read through the source map.
   const map = SourceMap.of({ contents });
   // A plain code-unit order, not localeCompare: the generated text must not
   // depend on the host's locale.
   for (const [name, object] of Object.entries(contents).sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
   )) {
      if (!object || typeof object !== "object") continue;
      const def = object as SourceDefLike;
      if (typeof def.sourceID !== "string") continue;
      const declaredIn = splitSourceID(def.sourceID)?.url;
      if (declaredHere && declaredIn && !declaredHere(declaredIn)) continue;
      let declaration;
      try {
         declaration = readMaterializeDeclaration(def, contents);
      } catch (error) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: `#(materialize) does not parse: ${error instanceof Error ? error.message : String(error)}`,
            malformedCode: MATERIALIZE_UNPARSEABLE_CODE,
         });
         continue;
      }
      if (!declaration) continue;
      if (declaration.errors.length > 0) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: `#(materialize) does not parse: ${declaration.errors.join("; ")}`,
            malformedCode: MATERIALIZE_UNPARSEABLE_CODE,
         });
         continue;
      }
      // Before any other refusal: a key outside the closed set is malformed,
      // refused at publish, whatever else the source would be refused for.
      const unknownKey = unknownKeyRefusal(declaration);
      if (unknownKey) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: unknownKey,
            malformedCode: MATERIALIZE_UNKNOWN_KEY_CODE,
         });
         continue;
      }
      if (declaresOwnPersist(def, contents)) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason:
               "the source also carries its own #@ persist; declare one or the other",
         });
         continue;
      }
      // A source that inherits `#@ persist` is already its base's table under
      // v1 (an extension is its base's table): the plan holds it under its
      // own sourceID, so a generated build source presented as it would
      // overwrite that entry and read itself in the build graph.
      const inherited = inheritedPersist(def, contents);
      if (inherited) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason:
               `the source inherits #@ persist from ${inherited.from ? `\`${inherited.from}\`` : "the source it is built on"}, ` +
               "so it is already stored as that source's table; put #(materialize) on a " +
               "source that does not inherit #@ persist, or drop the #@ persist",
         });
         continue;
      }
      const deferred = declaration.keys.find((key) => key in DEFERRED_KEYS);
      if (deferred) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: DEFERRED_KEYS[deferred],
         });
         continue;
      }
      let inline: string[] = [];
      if (declaration.keys.includes("inline")) {
         const items = declaration.tag.array("inline")?.map((i) => i.text());
         if (!items || items.some((i) => typeof i !== "string" || i === "")) {
            refused.push({
               sourceName: name,
               sourceID: def.sourceID,
               reason:
                  'inline= must be a list of join names, such as inline=["carriers"]',
            });
            continue;
         }
         inline = [...new Set(items as string[])];
      }
      const joins = map.joinsOf(def.sourceID);
      // Each entry's joins from the source down: a join, or a path of
      // join_manys (`items.options`).
      const resolved = inline.map((entry) =>
         inlinedPathOf(entry, joins, map, def.connection),
      );
      const inlineProblem = resolved.find(
         (r): r is { reason: string } => "reason" in r,
      );
      if (inlineProblem) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: inlineProblem.reason,
         });
         continue;
      }
      // A private or internal join's access, by name. An inlined join_one
      // keeps it: its build reads the join through the build copy's lift
      // (P3), and the served record carries the join's modifier. A pair
      // table's record (an inlined join_many) does not carry one yet, so a
      // hidden join_many is refused here, and this source serves live while
      // the companion still compiles for every other source in the model.
      const joinAccess = new Map<string, "private" | "internal">();
      for (const raw of def.fields ?? []) {
         const f = raw as FieldLike;
         const access = hiddenAccess(f);
         const fieldName = f.as ?? f.name;
         if (access && typeof fieldName === "string" && f.join !== undefined) {
            joinAccess.set(fieldName, access);
         }
      }
      const entries = (resolved as { path: SourceJoin[] }[]).map((r) => r.path);
      const hiddenMany = entries
         .map((p) => p[0].name)
         .find(
            (j) =>
               joinAccess.has(j) &&
               joins.find((x) => x.name === j)?.relationship !== "one",
         );
      if (hiddenMany !== undefined) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason:
               `inline= names ${quotedName(hiddenMany)}, a join_many that is ` +
               "not public, which cannot be inlined as a pair table yet",
         });
         continue;
      }
      const ones = entries
         .filter((p) => p[0].relationship !== "many")
         .map((p) => p[0].name);
      // An inlined join_many is a pair table (P5): one path of join_manys
      // below each one inlined. A path that is the start of a longer one is
      // stored by the longer one's (its level flag answers it).
      const byFirst = new Map<string, SourceJoin[][]>();
      for (const p of entries.filter((e) => e[0].relationship === "many")) {
         byFirst.set(p[0].name, [...(byFirst.get(p[0].name) ?? []), p]);
      }
      const pathKey = (p: readonly SourceJoin[]) =>
         p.map((j) => j.name).join(".");
      let parted: string | undefined;
      const paths: SourceJoin[][] = [];
      for (const group of byFirst.values()) {
         const longest = group.reduce((a, b) => (b.length > a.length ? b : a));
         const apart = group.find(
            (p) =>
               !pathKey(longest).startsWith(pathKey(p) + ".") &&
               pathKey(p) !== pathKey(longest),
         );
         if (apart) {
            parted =
               `inline= names two paths through ${quotedName(longest[0].name)} that ` +
               `part (${quotedName(pathKey(apart))}, ${quotedName(pathKey(longest))}); ` +
               "a pair table stores one path below each join it inlines";
            break;
         }
         paths.push(longest);
      }
      if (parted) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: parted,
         });
         continue;
      }
      // What reads through an inlined join_many has one value per pair, not
      // per parent, so it is computed at read through the served join.
      const columns = materializeColumns(def, {
         joinReads: map.fieldJoinsOf(def.sourceID),
         inline: new Set(ones),
         hasJoins: joins.length > 0,
      });
      const ownColumns = new Set(columns.map((c) => c.column));
      const joinedColumns = [
         ...ones.flatMap((j) =>
            inlinedJoinColumns(
               map,
               joins.find((x) => x.name === j) as SourceJoin,
               undefined,
               joinAccess.get(j),
            ),
         ),
         ...paths.flatMap((p) =>
            p.flatMap((j, k) =>
               inlinedJoinColumns(
                  map,
                  j,
                  p.slice(0, k + 1).map((x) => x.name),
               ),
            ),
         ),
      ];
      const clash = joinedColumns.find((c) => ownColumns.has(c.column));
      if (clash) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason:
               `the inlined column \`${clash.column}\` has the name of one of the ` +
               "source's own dimensions",
         });
         continue;
      }
      columns.push(...joinedColumns);
      if (columns.length === 0) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: "the source has no dimension to store",
         });
         continue;
      }
      const classified = classifyWhereTerms(def);
      const window = rollingWindowTerms(
         classified,
         new Map(columns.map((c) => [c.column, columnType(def, c.dimension)])),
      );
      if (!window.ok) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: window.detail,
         });
         continue;
      }
      const readTime = readTimeTermsOf(def, classified, columns, givens);
      if (!readTime.ok) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: readTime.reason,
         });
         continue;
      }
      const carried = carriedPersistKeys(declaration);
      if ("reason" in carried) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: carried.reason,
         });
         continue;
      }
      const { persist } = carried;
      // Colocated (no `storage=`): the served copy reads only a storage
      // destination, so a table built into the source's own warehouse would
      // be built and never read. Not built until it can be served; a v1
      // `#@ persist` is colocated as before.
      if (persist.storage === undefined) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: COLOCATED_NOT_SERVED,
         });
         continue;
      }
      // A source is planned in the model that declares it (see `declaredHere`),
      // so its name here is the name its author gave it.
      const tableName = name;
      if (persist.name === undefined) {
         if (!PLAIN_TABLE_NAME.test(tableName)) {
            refused.push({
               sourceName: name,
               sourceID: def.sourceID,
               reason:
                  `the source's name \`${tableName}\` is not a plain table name; ` +
                  "give #(materialize) a name=",
            });
            continue;
         }
         persist.name = tableName;
      }
      let pair: MaterializePair | undefined;
      let pairBlocks: MaterializePairBlocks | undefined;
      if (paths.length > 0) {
         const laid = paths.map((p) =>
            pairRefusal(
               p,
               def,
               map,
               columns.filter((c) => c.join === undefined),
               joinedColumns,
            ),
         );
         const problem = laid.find(
            (l): l is { reason: string } => "reason" in l,
         );
         if (problem) {
            refused.push({
               sourceName: name,
               sourceID: def.sourceID,
               reason: problem.reason,
            });
            continue;
         }
         const pairs = (laid as { pair: MaterializePair }[]).map((l) => l.pair);
         pair = pairs[0];
         if (pairs.length > 1) {
            const blocks = pairBlocksOf(pairs, paths, columns, def.connection);
            if ("reason" in blocks) {
               refused.push({
                  sourceName: name,
                  sourceID: def.sourceID,
                  reason: blocks.reason,
               });
               continue;
            }
            pairBlocks = blocks.blocks;
         }
      }
      if (carried.watermarks && !(pair && persist.refresh === "incremental")) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason:
               "#(materialize) `watermark=` takes a list only on an incremental " +
               "pair table (an inlined join_many); name one dimension",
         });
         continue;
      }
      let pairFullRefresh: string | undefined;
      if (
         pair &&
         persist.refresh === "incremental" &&
         (pair.hops !== undefined || pairBlocks !== undefined)
      ) {
         // The parent-replace rule keeps one `__parent_row` per parent in one
         // path's table; a path's level flags and a table of several blocks
         // have no increment rule yet, so they are rebuilt in full.
         pairFullRefresh =
            pairBlocks !== undefined
               ? "the pair table stores more than one inlined join_many, one block each, which an increment cannot replace parent by parent yet"
               : "the pair table stores a path of join_manys, whose level row flags an increment cannot keep unique yet";
         for (const key of INCREMENTAL_KEYS) {
            if (key !== "partition") delete persist[key];
         }
      } else if (pair && persist.refresh === "incremental") {
         const incremental = pairIncremental({
            pair,
            persist,
            names:
               carried.watermarks ??
               (persist.watermark !== undefined ? [persist.watermark] : []),
            columns,
            columnTypeOf: (c) =>
               c.join === undefined
                  ? columnType(def, c.dimension)
                  : joinedColumnType(
                       joins.find((x) => x.name === c.join) as SourceJoin,
                       c.dimension,
                    ),
            windowFields: window.terms.map((t) => t.window.field),
         });
         if ("watermark" in incremental) {
            pair = { ...pair, watermark: incremental.watermark };
            persist.watermark = incremental.watermark.column;
            persist.merge_key = pair.parentKey;
         } else if ("fullRefresh" in incremental) {
            pairFullRefresh = incremental.fullRefresh;
            for (const key of INCREMENTAL_KEYS) {
               if (key !== "partition") delete persist[key];
            }
         }
      }
      const build = buildSourceName(name);
      const checked = ones;
      const uniquenessChecks = [
         ...checked.map((j): InlineUniquenessCheck => {
            const join = joins.find((x) => x.name === j) as SourceJoin;
            return {
               join: j,
               keyColumn: join.keyColumn as string,
               queryName: `${build}__unique__${j}`,
               joinedSourceID: join.sourceID as string,
               ...(join.connection ? { connectionName: join.connection } : {}),
            };
         }),
         ...(pair
            ? pairKeyChecks(
                 pair,
                 pairBlocks,
                 def.sourceID,
                 build,
                 typeof def.connection === "string"
                    ? def.connection
                    : undefined,
              )
            : []),
      ];
      plans.push({
         sourceName: name,
         sourceID: def.sourceID,
         buildSourceName: build,
         columns,
         persist,
         ...(window.terms.length + readTime.terms.length > 0
            ? { readTimeTerms: [...window.terms, ...readTime.terms] }
            : {}),
         ...(readTime.terms.length > 0
            ? {
                 declaration: readTime.declaration,
                 givenDeclarations: readTime.givenDeclarations,
              }
            : columns.some((c) => c.access)
              ? withDeclaration(declarationOf(def))
              : {}),
         ...(inline.length > 0 ? { inline } : {}),
         ...(uniquenessChecks.length > 0 ? { uniquenessChecks } : {}),
         ...(pair ? { pair } : {}),
         ...(pairBlocks ? { pairBlocks } : {}),
         ...(pairFullRefresh ? { pairFullRefresh } : {}),
         ...(declaration.keys.includes("name") ? {} : { nameDefaulted: true }),
      });
   }
   chainExtensionTables(plans, map);
   return { plans, refused };
}

/**
 * The `#(materialize)` keys that make a table incremental. A chained build
 * reads its parent's table, whose rows an incremental refresh moves on its
 * own schedule, so neither side of a chain may carry one.
 */
const INCREMENTAL_KEYS = ["refresh", "watermark", "merge_key", "partition"];

/**
 * Give each plan that can build over its nearest materialized ancestor's build
 * source (P10) its `buildsOver`. The ancestor is the first planned source up
 * the chain, reached through `extend` and renames only (a query-shaped hop
 * starts a new segment, which the build cannot re-root on a table), declared
 * in the same file (the build copy re-roots it in that file's text), as is
 * every declaration between them (`path`). Both must be `storage=` the same
 * destination (a colocated `#(materialize)` is refused before this, and a
 * colocated pair never chains), carry no incremental
 * key, and the parent no given-reading `where:` (its build source's own
 * `extend` re-applies that term, which would scope the extension's build to
 * one caller). A plan that does not qualify builds over the raw chain, as
 * before: the same rows.
 */
function chainExtensionTables(plans: MaterializePlan[], map: SourceMap): void {
   const byID = new Map(plans.map((p) => [p.sourceID, p]));
   const declarationOfID = (sourceID: string) =>
      declarationOf(map.node(sourceID)?.def as SourceDefLike);
   for (const plan of plans) {
      let node = map.node(plan.sourceID);
      let parent: MaterializePlan | undefined;
      // The sources from the extension up to just below the parent.
      const below: string[] = [];
      for (let hops = 0; node && hops < 64 && !parent; hops++) {
         const base = node.base;
         if (!base || base.hop === "query" || base.sourceID === undefined) {
            break;
         }
         below.push(node.sourceID);
         parent = byID.get(base.sourceID);
         node = map.node(base.sourceID);
      }
      if (!parent || parent === plan) continue;
      const declaration = declarationOfID(parent.sourceID);
      const path = below.reverse().map(declarationOfID);
      if (
         !declaration ||
         path.some((d) => d === undefined || d.url !== declaration.url)
      ) {
         continue;
      }
      // One storage destination on both sides, whose build reads the
      // parent's table there (see `overStored`). A colocated plan is refused
      // before this (COLOCATED_NOT_SERVED); never chained should one reach it.
      if (plan.persist.storage === undefined) continue;
      if (plan.persist.storage !== parent.persist.storage) continue;
      const incremental = (p: MaterializePlan) =>
         Object.keys(p.persist).some((k) => INCREMENTAL_KEYS.includes(k));
      if (incremental(plan) || incremental(parent)) continue;
      // A table that stacks several inlined paths is a SQL source over the
      // author's model: neither builds over the other's build source.
      if (plan.pairBlocks || parent.pairBlocks) continue;
      if (givenReadTerms(parent).length > 0) continue;
      plan.buildsOver = {
         sourceID: parent.sourceID,
         sourceName: parent.sourceName,
         declaration,
         path: path as TermUse[],
      };
   }
}

/**
 * A refused declaration as a package warning: the source is not materialized
 * and serves live, which is the `unservable` tier, or `malformed` (refused at
 * publish) when the declaration cannot be read as written.
 */
export function materializeRefusalFinding(
   refusal: MaterializeRefusal,
   model: string,
): PackageAnnotationFinding {
   return {
      route: MATERIALIZE_ROUTE,
      tier: refusal.malformedCode ? "malformed" : "unservable",
      code: refusal.malformedCode ?? "materialize-not-built",
      message:
         `#(materialize) on source "${refusal.sourceName}" is not built, ` +
         `so the source is served live: ${refusal.reason}.`,
      subject: refusal.sourceName,
      model,
   };
}

/**
 * A pair table whose `refresh="incremental"` is not honoured, as a package
 * warning: it is built, served, and rebuilt in full on every refresh (the
 * `unservable` tier: well-formed, not honoured here).
 */
export function pairFullRefreshFinding(
   plan: Pick<MaterializePlan, "sourceName">,
   reason: string,
   model: string,
): PackageAnnotationFinding {
   return {
      route: MATERIALIZE_ROUTE,
      tier: "unservable",
      code: MATERIALIZE_PAIR_FULL_REFRESH_CODE,
      message:
         `#(materialize) on source "${plan.sourceName}" declares ` +
         `refresh="incremental", but its pair table is rebuilt in full on ` +
         `every refresh: ${reason}.`,
      subject: plan.sourceName,
      model,
   };
}

/** One generated build source's `#@ persist` line. */
function persistLine(plan: MaterializePlan): string {
   // Key paths are CARRIED_KEYS and PLAIN_PROPERTY-checked group properties,
   // so they are bare-safe; every value is quoted. A fixed order: the carried
   // keys as listed, then group properties by path.
   const rank = (key: string) => {
      const at = (CARRIED_KEYS as readonly string[]).indexOf(key);
      return at >= 0 ? at : CARRIED_KEYS.length;
   };
   // malloy-text: each key is a CARRIED_KEYS or PLAIN_PROPERTY-checked bare path, each value annotationValue
   const keys = Object.entries(plan.persist)
      .sort(([a], [b]) => rank(a) - rank(b) || (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, value]) => `${key}=${annotationValue(value)}`)
      .join(" ");
   return keys ? `#@ persist ${keys}` : "#@ persist";
}

/** One stored column as a build's select list reads it. */
function selectItem(c: MaterializeColumn): string {
   if (c.join === undefined) return `    ${identifier(c.dimension)}`;
   const through = (c.path ?? [c.join]).map(identifier).join(".");
   return `    ${identifier(c.column)} is ${through}.${identifier(c.dimension)}`;
}

/**
 * A generated build source's body: a `select:` of every stored column, or a
 * pair table's build over them ({@link pairBody}).
 */
function buildSourceBody(plan: MaterializePlan): string {
   const select = plan.columns.map(selectItem).join(",\n");
   return plan.pair ? pairBody(plan.pair, select) : `  select:\n${select}`;
}

/** One generated build source. */
function emitBuildSource(plan: MaterializePlan): string {
   const given = givenReadTerms(plan);
   // malloy-text: each term is the author's own given-reading where:, re-applied as written on its own declaration
   const readTime = given.length
      ? ` extend {\n${given.map((term) => `  where: ${term.code}`).join("\n")}\n}`
      : "";
   if (plan.pairBlocks) {
      return `${persistLine(plan)}\nsource: ${identifier(plan.buildSourceName)} is ${pairBlocksSource(plan, plan.pairBlocks)}${readTime}`;
   }
   const body = buildSourceBody(plan);
   const head = `source: ${identifier(plan.buildSourceName)} is ${identifier(importAlias(plan))} -> {`;
   return `${persistLine(plan)}\n${head}\n${body}\n}${readTime}`;
}

/**
 * The stored key column of each level of a pair's path, the source's first:
 * the source's primary key, then each joined source's key as stored
 * (`items__item_id`, `items__options__option_id`).
 */
function pairKeyColumns(pair: MaterializePair): string[] {
   const names = [pair.join];
   const keys = [pair.parentKey, `${pair.join}__${pair.childKey}`];
   for (const hop of pair.hops ?? []) {
      names.push(hop.join);
      keys.push(`${names.join("__")}__${hop.key}`);
   }
   return keys;
}

/**
 * A pair table's row flags, as `calculate:` items: the parent-row flag and
 * each level flag down to `levels` (one per level above the leaf), each true
 * on one row per distinct keys down to its level. Past the pair's own leaf
 * (a block of a table whose deepest path is deeper) a flag partitions on
 * every key the pair has, which is every row.
 */
function pairFlags(pair: MaterializePair, levels: number): string {
   const keys = pairKeyColumns(pair).map(identifier);
   const flag = (column: string, level: number) =>
      `    ${identifier(column)} is row_number() { partition_by: ${keys.slice(0, Math.min(level, keys.length - 1) + 1).join(", ")} } = 1`;
   return [
      flag(pair.parentRowColumn, 0),
      ...Array.from({ length: levels - 1 }, (_, k) =>
         flag(pairLevelRowColumn(k + 1), k + 1),
      ),
   ].join("\n");
}

/**
 * A pair table's build: a `group_by:` of every stored column (distinct rows,
 * since every level's key is among them) that writes the row flags (see the
 * module comment) by ranking each row within its keys. The rows are written
 * in parent-key order, so a warehouse that keeps insertion order (DuckDB)
 * reads a parent's rows together.
 */
function pairBody(pair: MaterializePair, columns: string): string {
   const parent = identifier(pair.parentKey);
   // malloy-text: pairFlags writes every name through identifier
   const flags = pairFlags(pair, 1 + (pair.hops?.length ?? 0));
   // malloy-text: columns is emitBuildSource's select list, every name through identifier
   return `  group_by:
${columns}
  calculate:
${flags}${pair.watermark ? `\n    ${pairWatermarkCalculation(pair.watermark, parent)}` : ""}
  order_by: ${parent}`;
}

/**
 * The build of a pair table that stores more than one inlined path: one
 * block per path, each the query that path's own pair table would build,
 * every block listing every column in the same order (another block's
 * columns as typed nulls, its own block number in the block column, every
 * level flag), and the blocks stacked with `UNION ALL`. Malloy has no union,
 * so the stack is a SQL source on the source's own connection whose blocks
 * are Malloy queries embedded with `%{ … }`; the queries are compiled by
 * Malloy like any other, and the SQL around them is only the union.
 */
function pairBlocksSource(
   plan: MaterializePlan,
   blocks: MaterializePairBlocks,
): string {
   const inBlocks = new Set(blocks.blocks.map((p) => p.join));
   const block = (pair: MaterializePair, lane: number) => {
      // The source's own columns and an inlined join_one's are on every
      // block's rows; another block's path's are nulls on this one's.
      const own = (c: MaterializeColumn) =>
         c.join === undefined || !inBlocks.has(c.join) || c.join === pair.join;
      const items = plan.columns.map((c) =>
         own(c)
            ? selectItem(c)
            : `    ${identifier(c.column)} is ${typedNull(blocks.columnTypes[c.column])}`,
      );
      items.push(`    ${identifier(blocks.laneColumn)} is ${lane}`);
      const parent = identifier(pair.parentKey);
      // malloy-text: each item a selectItem or a typedNull, names through identifier, each type one of NULLABLE_TYPES
      const listed = items.join(",\n");
      // malloy-text: pairFlags writes every name through identifier
      const flags = pairFlags(pair, blocks.levels);
      return `%{ ${identifier(importAlias(plan))} -> {
  group_by:
${listed}
  calculate:
${flags}
  order_by: ${parent}
} }`;
   };
   const union = blocks.blocks
      .map(
         (pair, lane) =>
            `SELECT * FROM (${block(pair, lane)}) AS ${PAIR_LANE_COLUMN}_${lane}`,
      )
      .join("\nUNION ALL\n");
   return `${identifier(blocks.connection)}.sql("""\n${union}\n""")`;
}

/**
 * An incremental pair table's watermark (see {@link pairIncremental}): the
 * greatest non-null value of its parts across the parent's rows. Malloy's
 * `greatest` is null when any argument is, on every dialect, so each argument
 * is one part with the others as its fallbacks: null only when every part is.
 */
function pairWatermarkCalculation(
   watermark: NonNullable<MaterializePair["watermark"]>,
   parent: string,
): string {
   const parts = watermark.parts.map(identifier);
   const each = parts.map((p) =>
      parts.length === 1
         ? p
         : `coalesce(${[p, ...parts.filter((q) => q !== p)].join(", ")})`,
   );
   const value = each.length === 1 ? each[0] : `greatest(${each.join(", ")})`;
   // malloy-text: every name through identifier
   return `${identifier(watermark.column)} is max_window(${value}) { partition_by: ${parent} }`;
}

/**
 * The key checks of a pair table whose rows carry a flag below the parent's
 * (a path of join_manys) or more than one block, run before each build like
 * an inlined join_one's (P8): the source's primary key, and the key of each
 * level between it and the leaf, must be unique and never null. A level's row
 * flag marks one row per distinct key down to that level, so two rows of one
 * key (or two null keys) share one flagged row, where live aggregates that
 * level over each row: a middle-level measure, and a parent measure beside a
 * deeper level, would come up short. The leaf needs no check: its aggregates
 * read every row. A one-hop table of one block checks nothing, as before.
 */
function pairKeyChecks(
   pair: MaterializePair,
   blocks: MaterializePairBlocks | undefined,
   sourceID: string,
   build: string,
   connection: string | undefined,
): InlineUniquenessCheck[] {
   const all = blocks?.blocks ?? [pair];
   if (!blocks && !(pair.hops && pair.hops.length > 0)) return [];
   const on = connection ? { connectionName: connection } : {};
   const out: InlineUniquenessCheck[] = [
      {
         join: "",
         keyColumn: pair.parentKey,
         queryName: `${build}__pair_key`,
         joinedSourceID: sourceID,
         pairLevel: 0,
         ...on,
      },
   ];
   for (const p of all) {
      // Each level above the leaf: the first join's source, then each hop's
      // but the last.
      const levels = [
         { names: [p.join], key: p.childKey, sourceID: p.childSourceID },
         ...(p.hops ?? []).map((h, k) => ({
            names: [
               p.join,
               ...(p.hops ?? []).slice(0, k + 1).map((x) => x.join),
            ],
            key: h.key,
            sourceID: h.sourceID,
         })),
      ].slice(0, p.hops?.length ?? 0);
      levels.forEach((l, k) => {
         out.push({
            join: l.names.join("."),
            keyColumn: l.key,
            queryName: `${build}__pair_key__${l.names.join("__")}`,
            joinedSourceID: l.sourceID,
            pairLevel: k + 1,
            ...on,
         });
      });
   }
   return out;
}

/**
 * One inlined join's uniqueness check (P8): the joined source's keys that
 * occur more than once. A build runs it first and fails on any row. A pair
 * table's key check ({@link pairKeyChecks}) also returns a null key.
 */
function emitUniquenessCheck(check: InlineUniquenessCheck): string {
   const key = identifier(check.keyColumn);
   if (check.pairLevel !== undefined) {
      // malloy-text: every name through identifier
      return `query: ${identifier(check.queryName)} is ${identifier(checkAlias(check))} -> {
  group_by: ${key}
  aggregate: matches is count()
} -> {
  where: matches > 1 or ${key} is null
  select: ${key}
  limit: 1
}`;
   }
   return `query: ${identifier(check.queryName)} is ${identifier(checkAlias(check))} -> {
  where: ${key} is not null
  group_by: ${key}
  aggregate: matches is count()
  having: matches > 1
  limit: 1
}`;
}

/** The alias a generated file imports a uniqueness check's joined source under. */
function checkAlias(check: InlineUniquenessCheck): string {
   return `${check.queryName}__source`;
}

/** The alias a generated file imports an author source under. */
function importAlias(plan: MaterializePlan): string {
   return `${plan.buildSourceName}__base`;
}

/**
 * The author's source a generated name stands for, among `plans`, or
 * undefined for a name no generated file declares: a build source is its
 * author's source; the alias an extension table's build reads its base by
 * (P10) is on its parent's table, since its build copy re-roots the parent on
 * the parent's build source, so it is the parent.
 */
export function generatedNameOwner(
   name: string,
   plans: Iterable<MaterializePlan>,
): string | undefined {
   for (const plan of plans) {
      if (plan.buildSourceName === name) return plan.sourceName;
      if (plan.buildsOver && importAlias(plan) === name) {
         return plan.buildsOver.sourceName;
      }
   }
   return undefined;
}

/**
 * The build copy of one planned source's declaring file: `text` with that
 * source's read-time terms replaced by `true` (P2), so a query over the
 * copy's source applies every `where:` but those, or why a term cannot be
 * placed (see {@link termSpans}), which refuses the source rather than guess.
 *
 * With `liftAccess` (P3), the source's own access modifiers are lifted too
 * ({@link accessLiftEdits}) and the declaration is wrapped in
 * `include { public: * }`, which lifts the `internal` fields it inherits, so
 * the build can select its hidden fields. A field private on a source it
 * extends stays private, and is never stored. The copy then needs the
 * `access_modifiers` flag, which its first line adds.
 *
 * One copy per source, never one for the file: a copy with two sources' terms
 * taken out would let the one source's build read the other (a join to it, a
 * query over it) without that other's term, and store its rows for every
 * caller. In a copy of its own, a source that reads another such source still
 * reads that one's given, and the eligibility gate refuses its build
 * (`given_in_persisted_query`).
 *
 * For an extension table (P10), `chain` names its parent's plan and the file
 * the parent's build source is imported from: the copy also re-roots the
 * parent's declaration on that build source ({@link rerootOnBuildSource}) and
 * imports it, so the extension, as written, reads the parent's table.
 */
export function buildCopyText(
   text: string,
   plan: MaterializePlan,
   chain?: { parent: MaterializePlan; importFrom: string },
   options: { liftAccess?: boolean } = {},
): { copy: string } | { reason: string } {
   const edits = copyEdits(text, plan, chain?.parent, options.liftAccess);
   if ("reason" in edits) return edits;
   let copy = applyEdits(text, edits.edits);
   if (copy === undefined) return { reason: NOT_FOUND };
   if (chain) {
      const at = afterModelHeader(copy);
      const line = `import { ${identifier(chain.parent.buildSourceName)} } from ${stringLiteral(chain.importFrom)}\n`;
      copy = `${copy.slice(0, at)}${line}${copy.slice(at)}`;
   }
   return { copy };
}

/** Lifts the `internal` fields a build copy's source inherits (P3). */
const LIFT_INHERITED = " include { public: * }";

/** The compiler flag `include { … }` needs, as the build copy's first line. */
const ACCESS_FLAG = "##! experimental.access_modifiers";

const ACCESS_NOT_LIFTED =
   "the source's declaration could not be read to lift its access " +
   "modifiers for the build";

/**
 * The edits {@link buildCopyText} makes to `text`: the plan's given-reading
 * terms replaced by `true` (P2); with `parent`, the parent's declaration
 * re-rooted on its build source (P10); and with `liftAccess`, the plan's own
 * declaration's access modifiers lifted and the declaration wrapped in
 * `include { public: * }` (P3), with the flag `include` needs as the copy's
 * first line. The lift edits only the plan's own declaration, so a chained
 * extension's build sees its hidden fields lifted over its parent's re-root
 * exactly as over the raw chain.
 */
function copyEdits(
   text: string,
   plan: MaterializePlan,
   parent?: MaterializePlan,
   liftAccess = false,
   joinRecords = false,
): { edits: TextEdit[] } | { reason: string } {
   const edits: TextEdit[] = [];
   const given = givenReadTerms(plan);
   if (given.length > 0) {
      const found = plan.declaration
         ? termSpans(text, plan.declaration, given)
         : { reason: NOT_FOUND };
      if ("reason" in found) return found;
      for (const span of found.spans) edits.push({ ...span, insert: "true" });
   }
   if (parent) {
      const range = plan.buildsOver?.declaration.range;
      const from = range ? offsetOf(text, range.start) : -1;
      const to = range ? offsetOf(text, range.end) : -1;
      const rerooted =
         from >= 0 && to >= from
            ? rerootOnBuildSource(text.slice(from, to), parent, {
                 joinRecords,
              })
            : undefined;
      if (
         rerooted === undefined ||
         edits.some((e) => e.start < to && from < e.end)
      ) {
         return { reason: PARENT_NOT_REROOTED };
      }
      edits.push({ start: from, end: to, insert: rerooted });
   }
   if (liftAccess) {
      const from = plan.declaration
         ? offsetOf(text, plan.declaration.range.start)
         : -1;
      const to = plan.declaration
         ? offsetOf(text, plan.declaration.range.end)
         : -1;
      const lifted =
         from >= 0 && to >= from
            ? accessLiftEdits(text.slice(from, to))
            : undefined;
      if (!lifted) return { reason: ACCESS_NOT_LIFTED };
      edits.push(
         ...lifted.map((e) => ({
            start: from + e.start,
            end: from + e.end,
            insert: e.insert,
         })),
         { start: to, end: to, insert: LIFT_INHERITED },
         { start: 0, end: 0, insert: `${ACCESS_FLAG}\n` },
      );
   }
   if (edits.length === 0) return { reason: NOT_FOUND };
   return { edits };
}

/**
 * The text a `storage=` extension table's build compiles in its destination
 * (P10): the parent's declaration and each one on the path to the
 * extension's, edited as the build copy edits them (the parent re-rooted on
 * its build source, the extension's given-reading terms out) and nothing else
 * of the file, so the build reads no source but the parent's stored table.
 * Each join_one the parent inlines is read from the parent's stored
 * `<join>__<field>` columns, as the served copy reads it: its definition taken
 * out of the parent's block and a record of those columns in its place,
 * under the join's own modifier, so an extension that reads `<join>.<field>`
 * builds over the parent's table rather than the raw chain. Undefined when
 * the copy's edits cannot be made, or a declaration is not where the plan
 * says.
 */
export function storedChainText(
   text: string,
   plan: MaterializePlan,
   parent: MaterializePlan,
   options: { liftAccess?: boolean } = {},
): StoredChain | undefined {
   const over = plan.buildsOver;
   if (!over) return undefined;
   const edits = copyEdits(text, plan, parent, options.liftAccess, true);
   if ("reason" in edits) return undefined;
   const statements: string[] = [];
   for (const { range } of [over.declaration, ...over.path]) {
      const from = offsetOf(text, range.start);
      const to = offsetOf(text, range.end);
      if (from < 0 || to < from) return undefined;
      const inside = edits.edits
         .filter((e) => e.start >= from && e.end <= to)
         .map((e) => ({ ...e, start: e.start - from, end: e.end - from }));
      const edited = applyEdits(text.slice(from, to), inside);
      if (edited === undefined) return undefined;
      // malloy-text: the author's own declaration, edited as its build copy edits it
      statements.push(`source: ${edited}`);
   }
   return {
      parentBuildSourceName: parent.buildSourceName,
      // The lift's `include { public: * }` needs its flag here too (P3).
      flags: [
         ...documentFlagLines(text),
         ...(options.liftAccess ? [ACCESS_FLAG] : []),
      ],
      declarations: statements.join("\n"),
   };
}

/** A stored column of the parent, as {@link extensionOverStoredModel} declares it. */
export interface StoredColumnType {
   name: string;
   /** A Malloy basic type: `string`, `number`, `boolean`, `date`, `timestamp` or `json`. */
   type: string;
}

/**
 * The model a `storage=` extension table builds from in its destination
 * (P10): the parent's build source declared as the virtual source of its
 * stored table on `connectionName`, the extension's {@link StoredChain} over
 * it, and the extension's generated build source, `#@ persist storage=`, last.
 * Its given-reading terms are not written: the build leaves them out, and the
 * table's address is the companion's, not this model's.
 */
export function extensionOverStoredModel(params: {
   plan: MaterializePlan;
   chain: StoredChain;
   /** The connection the stored table is read through: the destination. */
   connectionName: string;
   /** The virtualMap handle of the parent's stored table. */
   virtualHandle: string;
   /** The parent's stored columns. */
   columns: readonly StoredColumnType[];
}): string {
   const { plan, chain } = params;
   const typeName = identifier(`${chain.parentBuildSourceName}__shape`);
   // malloy-text: names through identifier, each type a Malloy basic type name
   const fields = params.columns
      .map((c) => `   ${identifier(c.name)}::${c.type}`)
      .join(",\n");
   const body = buildSourceBody(plan);
   const flags = [
      // malloy-text: the author file's own `##!` lines, carried as written
      ...chain.flags,
      "##! experimental { persistence virtual_source }",
   ].join("\n");
   // malloy-text: the author's own declarations, edited as the build copy edits them (storedChainText)
   const declarations = chain.declarations;
   return `${flags}
type: ${typeName} is {
${fields}
}
source: ${identifier(chain.parentBuildSourceName)} is ${identifier(params.connectionName)}.virtual(${stringLiteral(params.virtualHandle)})::${typeName}
${declarations}
source: ${identifier(importAlias(plan))} is ${identifier(plan.sourceName)}
#@ persist storage=${annotationValue(params.connectionName)}
source: ${identifier(plan.buildSourceName)} is ${identifier(importAlias(plan))} -> {
${body}
}
`;
}

const PARENT_NOT_REROOTED =
   "the parent's declaration could not be re-rooted on its build source";

/**
 * A parent's declaration in an extension's build copy (P10): `<name> is
 * <build source>`, then the blocks that undo its renames
 * ({@link inverseRenameBlocks}), then its own trailing `extend` run less each
 * computed dimension its table stores, so the stored column stands in for it,
 * as the served copy re-roots a source on its table. Its measures, joins,
 * views, its fields computed at read and its `where:` are kept as written, so
 * the extension reads every field it read from the parent. With
 * `joinRecords` (the build in a `storage=` destination, {@link storedChainText}),
 * each join_one the parent inlines is replaced by the record of its stored
 * columns ({@link inlinedJoinStatements}), since the destination holds no
 * joined source. Undefined when the declaration does not parse, or a join to
 * replace is declared there by another kind of join.
 */
function rerootOnBuildSource(
   declaration: string,
   parent: MaterializePlan,
   options: { joinRecords?: boolean } = {},
): string | undefined {
   const spans = declarationSpans(declaration);
   if (!spans) return undefined;
   const name = declaration.slice(0, spans.nameEnd).trimEnd();
   const run =
      spans.extendStart === undefined
         ? ""
         : declaration.slice(spans.extendStart).trimEnd();
   const stored = parent.columns.filter((c) => c.join === undefined);
   const kept = dropDimensions(
      run,
      new Set(stored.filter((c) => c.computed).map((c) => c.dimension)),
   );
   if (!kept) return undefined;
   const renames = inverseRenameBlocks(
      kept.text,
      new Set(stored.map((c) => c.column)),
   );
   if (renames === undefined) return undefined;
   let text = kept.text;
   if (options.joinRecords) {
      const records = parentJoinRecords(parent);
      for (const j of records) {
         const dropped = dropJoinDefinition(text, j.join);
         if (dropped === undefined) return undefined;
         text = dropped;
      }
      if (records.length > 0) {
         const opened = intoFirstExtend(
            text,
            inlinedJoinStatements(
               records,
               inlinedJoinRenames(
                  records,
                  parent.columns.map((c) => c.column),
               ),
               true,
            ),
         );
         if (opened === undefined) return undefined;
         text = opened;
      }
   }
   const base = identifier(parent.buildSourceName);
   return `${name} is ${base}${renames}${text ? ` ${text}` : ""}`;
}

/**
 * The join_ones a parent's table stores inline (`inline=`), each with its
 * stored columns and the join's modifier, as {@link inlinedJoinStatements}
 * writes their records. A pair table's join_many is not one: its table is
 * a row per pair, never chained on as the parent's rows.
 */
function parentJoinRecords(parent: MaterializePlan): InlinedJoinText[] {
   const out: InlinedJoinText[] = [];
   for (const join of parent.inline ?? []) {
      if (join === parent.pair?.join) continue;
      const columns = parent.columns.filter((c) => c.join === join);
      if (columns.length === 0) continue;
      const access = columns[0].access;
      out.push({
         join,
         columns: columns.map((c) => ({ name: c.dimension, column: c.column })),
         ...(access ? { access } : {}),
      });
   }
   return out;
}

/** Ends a level file's name: one depth of a chain's generated sources (P10). */
export const MATERIALIZE_LEVEL_SUFFIX = ".materialize-level.malloy";

/** The level file of `level`'s generated sources, beside the author's model. */
export function levelFileName(modelFileName: string, level: number): string {
   return `${modelFileName}.${level}${MATERIALIZE_LEVEL_SUFFIX}`;
}

/**
 * Each plan's depth in its chain of extension tables (P10), by sourceID: 0
 * for one built over the raw chain, its parent's plus one otherwise. A plan
 * whose parent is not among `plans` is depth 0 (see {@link settledChains}).
 */
export function chainLevels(
   plans: readonly MaterializePlan[],
): Map<string, number> {
   const byID = new Map(plans.map((p) => [p.sourceID, p]));
   const levels = new Map<string, number>();
   const levelOf = (plan: MaterializePlan, depth = 0): number => {
      const known = levels.get(plan.sourceID);
      if (known !== undefined) return known;
      const parent = plan.buildsOver
         ? byID.get(plan.buildsOver.sourceID)
         : undefined;
      const level =
         parent && depth < plans.length ? levelOf(parent, depth + 1) + 1 : 0;
      levels.set(plan.sourceID, level);
      return level;
   };
   for (const plan of plans) levelOf(plan);
   return levels;
}

/**
 * `plans` with every `buildsOver` whose parent is no longer planned removed,
 * to a fixpoint: a parent refused after planning leaves its extension to
 * build over the raw chain.
 */
export function settledChains(
   plans: readonly MaterializePlan[],
): MaterializePlan[] {
   const ids = new Set(plans.map((p) => p.sourceID));
   return plans.map((p) =>
      p.buildsOver && !ids.has(p.buildsOver.sourceID) ? withoutParent(p) : p,
   );
}

/** A plan built over the raw chain instead of its parent's build source. */
export function withoutParent(plan: MaterializePlan): MaterializePlan {
   if (!plan.buildsOver) return plan;
   const { buildsOver: _parent, ...rest } = plan;
   return rest;
}

const NOT_FOUND =
   "a `where:` reading a given could not be found as written in the " +
   "source's declaration, so it cannot be left out of the build";

/**
 * Each term's span in `text`, or why one cannot be taken out.
 *
 * A term is found as its verbatim text (`code`) at the one occurrence inside
 * the declaration that covers every place the compiler says it reads a field
 * or a given, so the same text in a comment, a string, another term or
 * another source never matches; none, or more than one, is refused.
 *
 * The occurrence must also lie in the declaration's trailing `extend { … }`
 * run, read with the re-root rewriter's own reader ({@link declarationSpans}):
 * that run is the only text the served copy keeps, so a term written anywhere
 * else (a parenthesised head, an inline `extend` in the base) would be built
 * out of the table and then never re-applied. The served copy refuses such a
 * source at read too (`read_terms_not_kept`); refusing it here means its
 * table is never built for nothing.
 */
function termSpans(
   text: string,
   declaration: TermUse,
   terms: readonly GivenReadTerm[],
): { spans: { start: number; end: number }[] } | { reason: string } {
   const from = offsetOf(text, declaration.range.start);
   const to = offsetOf(text, declaration.range.end);
   if (from < 0 || to < from) return { reason: NOT_FOUND };
   const extendStart = declarationSpans(text.slice(from, to))?.extendStart;
   const spans: { start: number; end: number }[] = [];
   for (const term of terms) {
      const uses = term.at.map((use) => ({
         start: offsetOf(text, use.range.start),
         end: offsetOf(text, use.range.end),
      }));
      if (uses.some((u) => u.start < 0 || u.end < u.start)) {
         return { reason: NOT_FOUND };
      }
      const matches: number[] = [];
      for (
         let at = text.indexOf(term.code, from);
         at >= 0 && at + term.code.length <= to;
         at = text.indexOf(term.code, at + 1)
      ) {
         const end = at + term.code.length;
         if (uses.every((u) => u.start >= at && u.end <= end)) matches.push(at);
      }
      if (matches.length !== 1) return { reason: NOT_FOUND };
      const span = { start: matches[0], end: matches[0] + term.code.length };
      if (spans.some((s) => s.start < span.end && span.start < s.end)) {
         return { reason: NOT_FOUND };
      }
      if (extendStart === undefined || span.start < from + extendStart) {
         return {
            reason:
               `\`where: ${term.code}\` reads a given and is not in this ` +
               "source's trailing `extend { … }`, the only part of its " +
               "declaration the served copy keeps, so it could not be " +
               "re-applied at read. Write it in a trailing `extend` on this " +
               "source",
         };
      }
      spans.push(span);
   }
   return { spans };
}

/**
 * The generated build companion for one author model, or undefined when the
 * model declares nothing. `importPath` resolves the author's model from the
 * companion's own URL; every planned source is declared there (see
 * {@link planModelMaterialize}). A uniqueness check imports its joined source
 * from the file that declares it, by its declared name, when
 * `importPathForFile` resolves that file (it may be imported into the author's
 * model under another name). The experimental flag lives here, so the
 * author's file needs none (D3).
 *
 * A plan with a build copy, `buildCopyPaths` by sourceID (given-reading terms
 * taken out, P2; built over its parent's build source, P10; or its own access
 * modifiers lifted, P3), is imported from that copy; one with given-reading
 * terms also imports the givens they read,
 * which the re-written `where:` needs in scope: each from the file that
 * declares it (`from`), or the author's model. A model with no such plan
 * generates exactly the text it did before P2.
 */
export function generateBuildFile(
   plans: MaterializePlan[],
   importPath: string,
   importPathForFile?: (url: string) => string | undefined,
   buildCopyPaths: ReadonlyMap<string, string> = new Map(),
): string | undefined {
   if (plans.length === 0) return undefined;
   const copyOf = (plan: MaterializePlan) => buildCopyPaths.get(plan.sourceID);
   const imports = plans.map((plan) => {
      const own = `import { ${identifier(importAlias(plan))} is ${identifier(plan.sourceName)} } from ${stringLiteral(copyOf(plan) ?? importPath)}`;
      const checks = (plan.uniquenessChecks ?? []).map((check) => {
         const joined = splitSourceID(check.joinedSourceID);
         const path = (joined && importPathForFile?.(joined.url)) ?? importPath;
         return `import { ${identifier(checkAlias(check))} is ${identifier(joined?.name ?? check.join)} } from ${stringLiteral(path)}`;
      });
      return [own, ...checks].join("\n");
   });
   const givensByPath = new Map<string, Set<string>>();
   for (const plan of plans) {
      if (copyOf(plan) === undefined) continue;
      for (const given of plan.givenDeclarations ?? []) {
         const from = given.from ?? importPath;
         const names = givensByPath.get(from) ?? new Set<string>();
         names.add(given.name);
         givensByPath.set(from, names);
      }
   }
   for (const [from, names] of [...givensByPath].sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
   )) {
      imports.push(
         `import { ${[...names]
            .sort()
            .map((g) => identifier(g))
            .join(", ")} } from ${stringLiteral(from)}`,
      );
   }
   const flag =
      givensByPath.size > 0
         ? "##! experimental { persistence givens }"
         : "##! experimental.persistence";
   return `${flag}
${imports.join("\n")}

${plans
   .flatMap((plan) => [
      emitBuildSource(plan),
      ...(plan.uniquenessChecks ?? []).map(emitUniquenessCheck),
   ])
   .join("\n\n")}
`;
}

/** The generated files for one author model. */
export interface GeneratedBuildFiles {
   /** The companion: compiled, and its build targets are every generated source. */
   entry: string;
   /**
    * Level files by file name (P10), each holding one depth of a chain's
    * generated sources; the companion imports them. Empty with no chain, when
    * the companion is exactly {@link generateBuildFile}'s.
    */
   levels: Map<string, string>;
}

/**
 * {@link generateBuildFile}, with the extension tables of P10. A build copy
 * that re-roots a parent imports the parent's build source, and the
 * companion imports the extension's base from that copy, so the parent's
 * build source cannot sit in the companion (the import would cycle): each
 * depth of a chain gets a level file of its own, written as
 * {@link generateBuildFile} writes a companion (without the uniqueness
 * checks), and the companion imports every generated source from its level
 * and holds the checks. Without a chain, the companion is exactly the text
 * {@link generateBuildFile} writes.
 */
export function generateBuildFiles(
   plans: MaterializePlan[],
   importPath: string,
   importPathForFile?: (url: string) => string | undefined,
   buildCopyPaths: ReadonlyMap<string, string> = new Map(),
): GeneratedBuildFiles | undefined {
   if (plans.length === 0) return undefined;
   const levels = chainLevels(plans);
   if (![...levels.values()].some((level) => level > 0)) {
      const entry = generateBuildFile(
         plans,
         importPath,
         importPathForFile,
         buildCopyPaths,
      );
      return entry === undefined ? undefined : { entry, levels: new Map() };
   }
   const byLevel = new Map<number, MaterializePlan[]>();
   for (const plan of plans) {
      const level = levels.get(plan.sourceID) ?? 0;
      byLevel.set(level, [...(byLevel.get(level) ?? []), plan]);
   }
   const files = new Map<string, string>();
   const imports: string[] = [];
   for (const [level, group] of [...byLevel].sort(([a], [b]) => a - b)) {
      const name = levelFileName(importPath, level);
      const text = generateBuildFile(
         group.map(({ uniquenessChecks: _checks, ...plan }) => plan),
         importPath,
         undefined,
         buildCopyPaths,
      );
      if (text === undefined) continue;
      files.set(name, text);
      imports.push(
         `import { ${group.map((p) => identifier(p.buildSourceName)).join(", ")} } from ${stringLiteral(name)}`,
      );
   }
   const checks = plans.flatMap((plan) => plan.uniquenessChecks ?? []);
   for (const check of checks) {
      const joined = splitSourceID(check.joinedSourceID);
      const path = (joined && importPathForFile?.(joined.url)) ?? importPath;
      imports.push(
         `import { ${identifier(checkAlias(check))} is ${identifier(joined?.name ?? check.join)} } from ${stringLiteral(path)}`,
      );
   }
   const queries = checks.map(emitUniquenessCheck).join("\n\n");
   return {
      entry: `##! experimental.persistence
${imports.join("\n")}
${queries ? `\n${queries}\n` : ""}`,
      levels: files,
   };
}
