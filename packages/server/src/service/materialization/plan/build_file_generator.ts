// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The text of the `#(materialize)` build companion: each planned source's
 * generated build source, its pair and key-check queries, the level files an
 * extension chain sits in, and the names the generated files declare. What
 * is planned is `materialize_plan.ts`'s; the build copy, the stored chain and
 * the read-time terms are each their own module (`materialize_build_copy.ts`,
 * `materialize_stored_chain.ts`, `materialize_read_time.ts`). The design they
 * share follows.
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
 * computed or not, so the stored table carries the
 * author's names and an outside reader sees `order_month`, not a suffixed
 * column. The served copy (the re-root rewriter) drops a stored computed
 * dimension's definition, so the column stands in for it, and carries the
 * dimension's annotations onto the served `type:` declaration. Measures, views
 * and joins are not stored: the served copy keeps them as written, re-bound
 * over the stored columns.
 *
 * Hidden fields (P3). A query cannot select a `private`
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
 * `storage=`) is built into its own warehouse and needs a `name=`
 * ({@link COLOCATED_NEEDS_NAME}): its default could be the very table it
 * reads. The served copy re-roots it on that table, on its own connection,
 * as it re-roots a `storage=` source on its destination; v1's colocated
 * `#@ persist` is still served through Malloy's build manifest.
 *
 * Joins. A join is a reference by default: it is not stored,
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
 * `inline=["j"]` on a join_many (P5) stores a pair table: one row per
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
 * an aggregate through the join reads every row, a childless parent's
 * null-extended row included. Live reads that row for a `min` or `max`, and
 * skips it for a `count()`, `sum` or `avg`, so
 * the served copy re-declares a measure through the join that counts, or
 * sums a value not null on that row, over the rows where the joined
 * source's stored key is not null, as live does. That test needs the key
 * unique and never null on a row of the joined source, which the build
 * checks ({@link pairKeyChecks}): a level above the leaf on every build, the
 * leaf by a check that never fails the build and is recorded beside the
 * table. Without a passing record, and for an aggregate written in the query
 * rather than in a measure, the served copy declines (`pair_flat_query.ts`,
 * known limitation 40).
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
 * An incremental pair table, of one hop, a path or several blocks, refreshes
 * whole parents over a per-parent watermark ({@link pairIncremental}; across
 * blocks, {@link PAIR_BLOCK_WATERMARK}), or is rebuilt in full with a warning.
 *
 * Refused rather than built wrong:
 * - `inline=` on a join_cross is refused.
 * - A given-reading `where:` or dimension written anywhere but the
 *   materialized source's own declaration (a base in this file or another,
 *   a parenthesised head) refuses the source (Bases, below).
 * - The build reads the author's file as written, not a copy with its
 *   `#@ persist` lines removed, so a source that also carries its own
 *   `#@ persist` is refused rather than stored twice.
 *
 * Caller-dependent dimensions (P9). A dimension whose value depends on what
 * the caller sets is left out of the build and computed at read by the served
 * copy, which keeps its definition (only stored dimensions' definitions are
 * dropped), over its inputs, which are stored: one that reads a given, the
 * current time or the query time zone. The probe compiler (probe_compiler.ts)
 * answers which, by compiling a probe of each computed dimension, never
 * running it; the IR walks ({@link givenReadingFields}, `currentTimeFields`)
 * answer too, and either marking a dimension leaves it out. One that reads a
 * given or the current time and is declared anywhere but the source's own
 * declaration refuses the source: the served copy keeps no other text, so it
 * could not compute the dimension at read, and an author should hear why
 * rather than get a table no query reads. The time zone leaves out only a
 * dimension whose definition the served copy keeps (the source's own): any
 * other is stored as the build computes it, in the default
 * zone, and a query that sets a `timezone:` over its table is answered live.
 * A stored computed dimension the probe cleared of the time zone is marked
 * `zoneFree`, so a `timezone:` query over a table that stores only such
 * dimensions is not declined for it. The source's rows are probed too: a
 * `where:` term or base relation whose SQL moves with the zone (a timestamp
 * literal) filtered the table in the build's zone, so only a source the
 * probe cleared is marked `zoneFreeRows`, and a `timezone:` query over any
 * other's table is answered live. Each left-out dimension is listed on the
 * plan (`leftOut`).
 *
 * Bases. Only a source's own declaration is left out of the build and
 * re-applied at read: the served copy replaces the declaration's base with the
 * stored table and keeps the declaration's own trailing `extend`, so what a
 * base declares is gone from the served source. A given-reading `where:` the
 * source inherits from any base (in this file or another, through `extend`,
 * a rename or a materialized parent) refuses the source, as does a
 * given-reading dimension a base declares: the source is not built and
 * answers live, under the term, and the warning says to move the term or the
 * dimension onto the materialized source itself. A zone-dependent dimension a
 * base declares is stored as built, in the default zone, and a `timezone:`
 * query over the table is answered live. A re-root that dropped a
 * given-reading `where:` would serve every caller's rows, so the served copy's
 * given-term check also refuses any re-root whose compiled filters lost one
 * (`keepsGivenTerms`).
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
 * with one reader (`../text/declaration_text.ts`). A term is only taken out when
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
 * what it imports. Only a pair on one tier (both colocated, or both in one
 * `storage=` destination) with no incremental keys chains. A colocated
 * extension's build reads its parent's build source, which Malloy's planner
 * resolves to the parent's table through the build manifest. A `storage=`
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

import type {
   MaterializePlan,
   MaterializeColumn,
   MaterializePair,
   MaterializePairBlocks,
   InlineUniquenessCheck,
} from "../domain/table_plans";
import type { GivenReadTerm } from "../domain/read_time_terms";
import type { ownLevelNotes } from "../../annotations";
import {
   annotationValue,
   identifier,
   nowComparison,
   stringLiteral,
} from "../../malloy_text";
import { buildQueryWindows } from "./rolling_window";
import { keyCheckPipeline } from "../text/key_check_text";

/** The route `#(materialize)` is on. */
export const MATERIALIZE_ROUTE = "materialize";

/** Appended to an author model path to name its generated build companion. */
export const MATERIALIZE_COMPANION_SUFFIX = ".materialize.malloy";

/** The generated build source's name for a materialized source. */
export function buildSourceName(declaredName: string): string {
   return `${declaredName}__mat`;
}

/**
 * The `#(materialize)` keys carried onto the generated `#@ persist` line as
 * one string each, in this order.
 */
export const CARRIED_KEYS = [
   "name",
   "storage",
   "refresh",
   "watermark",
   "merge_key",
] as const;

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

/**
 * An incremental pair table's watermark column: per parent, the greatest of
 * the declared watermark fields over the parent's row and every child row, so
 * a child added to (or restated under) a stored parent moves its parent's
 * watermark, and every row of one parent carries the same value (see
 * {@link pairIncremental}).
 */
export const PAIR_WATERMARK = "__pair_watermark";

/** The given-reading terms (P2) among a plan's read-time terms. */
export function givenReadTerms(
   plan: Pick<MaterializePlan, "readTimeTerms">,
): GivenReadTerm[] {
   return (plan.readTimeTerms ?? []).filter(
      (t): t is GivenReadTerm => t.kind === "given",
   );
}

export interface SourceDefLike {
   sourceID?: unknown;
   /** Where the source's own statement was parsed. */
   location?: {
      url?: string;
      range?: { start?: { line: number; character: number } };
   };
   connection?: unknown;
   /** The source's dialect (Malloy's `dialectName`). */
   dialect?: unknown;
   primaryKey?: unknown;
   annotations?: Parameters<typeof ownLevelNotes>[0];
   fields?: unknown[];
   filterList?: unknown;
}

export interface FieldLike {
   name?: unknown;
   as?: unknown;
   type?: unknown;
   e?: unknown;
   expressionType?: unknown;
   accessModifier?: unknown;
   join?: unknown;
}

/** A Malloy `sourceID`'s declared name and declaring file, or undefined. */
export function splitSourceID(
   sourceID: string,
): { name: string; url: string } | undefined {
   const match = /^(.+)@([a-z][a-z0-9+.-]*:.*)$/i.exec(sourceID);
   return match ? { name: match[1], url: match[2] } : undefined;
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
export function buildSourceBody(plan: MaterializePlan): string {
   const select = plan.columns.map(selectItem).join(",\n");
   // A two-sided window's lower bound, its term left out of the build copy,
   // written again from its parts: no literal, so the address never moves.
   const where = buildQueryWindows(plan.readTimeTerms)
      .map((w) => `  where: ${nowComparison(w.field, w.op, w.offset)}\n`)
      .join("");
   return `${where}${plan.pair ? pairBody(plan.pair, select) : `  select:\n${select}`}`;
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
export function pairKeyColumns(pair: MaterializePair): string[] {
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
      // An incremental table's block writes its own parents' greatest
      // watermark; the query over the union takes the greatest across blocks.
      const watermark = blocks.watermark
         ? `\n    ${pairWatermarkCalculation(
              { ...blocks.watermark, column: PAIR_BLOCK_WATERMARK },
              parent,
           )}`
         : "";
      return `%{ ${identifier(importAlias(plan))} -> {
  group_by:
${listed}
  calculate:
${flags}${watermark}
  order_by: ${parent}
} }`;
   };
   const union = blocks.blocks
      .map(
         (pair, lane) =>
            `SELECT * FROM (${block(pair, lane)}) AS ${PAIR_LANE_COLUMN}_${lane}`,
      )
      .join("\nUNION ALL\n");
   const sql = `${identifier(blocks.connection)}.sql("""\n${union}\n""")`;
   if (!blocks.watermark) return sql;
   // Every column the blocks write: distinct rows already (each block's are,
   // and the block column tells blocks apart). The block watermark is kept:
   // Malloy's window reads an output field.
   const stored = [
      ...plan.columns.map((c) => c.column),
      blocks.laneColumn,
      plan.pair?.parentRowColumn ?? PAIR_PARENT_ROW,
      ...Array.from({ length: blocks.levels - 1 }, (_, k) =>
         pairLevelRowColumn(k + 1),
      ),
      PAIR_BLOCK_WATERMARK,
   ];
   const parent = identifier(blocks.blocks[0].parentKey);
   return `${sql} -> {
  group_by:
${stored.map((c) => `    ${identifier(c)}`).join(",\n")}
  calculate:
    ${identifier(blocks.watermark.column)} is max_window(${identifier(PAIR_BLOCK_WATERMARK)}) { partition_by: ${parent} }
  order_by: ${parent}
}`;
}

/**
 * An incremental table of several blocks: each block's own greatest watermark
 * per parent, which the build takes the greatest of across blocks into
 * {@link PAIR_WATERMARK}. Stored beside it, and read by nothing.
 */
export const PAIR_BLOCK_WATERMARK = "__pair_watermark_block";

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
 * The key checks of a pair table, run before each build like an inlined
 * join_one's (P8): the source's primary key, and the key of each level between
 * it and the leaf (a path of join_manys), must be unique and never null. A
 * level's row flag marks one row per distinct key down to that level, so two
 * rows of one key (or two null keys) share one flagged row, where live
 * aggregates that level over each row: a parent measure, and a middle-level
 * measure beside a deeper level, would come out wrong.
 *
 * The leaf's key is checked too, but never fails the build
 * ({@link InlineUniquenessCheck.recordOnly}): its sums of stored columns and
 * its `min`s and `max`es read every row as they stand. A count through the
 * path, or a sum of a value not null on a childless parent's row, is served
 * over the rows whose leaf key is not null, which are the leaf's own rows only
 * where the key is never null on one; and a key held twice by one parent's
 * children could store two equal children as one row, which a count would
 * then count once.
 */
export function pairKeyChecks(
   pair: MaterializePair,
   blocks: MaterializePairBlocks | undefined,
   sourceID: string,
   build: string,
   connection: string | undefined,
): InlineUniquenessCheck[] {
   const all = blocks?.blocks ?? [pair];
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
      const hops = p.hops ?? [];
      const last = hops[hops.length - 1];
      const leafNames = [p.join, ...hops.map((h) => h.join)];
      out.push({
         join: leafNames.join("."),
         keyColumn: last?.key ?? p.childKey,
         queryName: `${build}__pair_leaf_key__${leafNames.join("__")}`,
         joinedSourceID: last?.sourceID ?? p.childSourceID,
         pairLevel: leafNames.length,
         recordOnly: true,
         ...on,
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
   const kind =
      check.pairLevel !== undefined ? "pair_level" : "inline_join_one";
   // malloy-text: keyCheckPipeline writes its names through identifier
   return `query: ${identifier(check.queryName)} is ${keyCheckPipeline(kind, checkAlias(check), check.keyColumn)}`;
}

/**
 * The import of one uniqueness check's source. A joined source's check reads
 * it from the file that declares it. A pair table's own key check (level 0)
 * reads the source from the file its build reads it from, its build copy when
 * it has one: the copy leaves out the terms the build leaves out (a given's,
 * a rolling window's upper bound), so the check counts the rows the table
 * stores. Over the author's source it would count only the rows those terms
 * keep, and pass while the table holds a key twice.
 */
function checkImport(
   check: InlineUniquenessCheck,
   plan: MaterializePlan,
   importPath: string,
   importPathForFile: ((url: string) => string | undefined) | undefined,
   buildCopyPaths: ReadonlyMap<string, string>,
): string {
   const joined = splitSourceID(check.joinedSourceID);
   const own = check.pairLevel === 0 && check.joinedSourceID === plan.sourceID;
   const path = own
      ? (buildCopyPaths.get(plan.sourceID) ?? importPath)
      : ((joined && importPathForFile?.(joined.url)) ?? importPath);
   return `import { ${identifier(checkAlias(check))} is ${identifier(joined?.name ?? check.join)} } from ${stringLiteral(path)}`;
}

/** The alias a generated file imports a uniqueness check's joined source under. */
function checkAlias(check: InlineUniquenessCheck): string {
   return `${check.queryName}__source`;
}

/** The alias a generated file imports an author source under. */
export function importAlias(plan: MaterializePlan): string {
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
 * Every name a generated file declares or imports for one plan: its build
 * source, the alias its build reads its base by, and each uniqueness check's
 * query and the alias it reads the joined source by. A compile problem on a
 * line naming one of these is that plan's (materialize_compile.ts).
 */
export function generatedNamesOf(plan: MaterializePlan): string[] {
   return [
      plan.buildSourceName,
      importAlias(plan),
      ...(plan.uniquenessChecks ?? []).flatMap((check) => [
         check.queryName,
         checkAlias(check),
      ]),
   ];
}

/** Ends a level file's name: one depth of a chain's generated sources (P10). */
const MATERIALIZE_LEVEL_SUFFIX = ".materialize-level.malloy";

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
      const checks = (plan.uniquenessChecks ?? []).map((check) =>
         checkImport(
            check,
            plan,
            importPath,
            importPathForFile,
            buildCopyPaths,
         ),
      );
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
interface GeneratedBuildFiles {
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
   for (const plan of plans) {
      for (const check of plan.uniquenessChecks ?? []) {
         imports.push(
            checkImport(
               check,
               plan,
               importPath,
               importPathForFile,
               buildCopyPaths,
            ),
         );
      }
   }
   const queries = checks.map(emitUniquenessCheck).join("\n\n");
   return {
      entry: `##! experimental.persistence
${imports.join("\n")}
${queries ? `\n${queries}\n` : ""}`,
      levels: files,
   };
}
