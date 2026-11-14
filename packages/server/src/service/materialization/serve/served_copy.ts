// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The re-root rewriter: the served copy of an author's model file
 * (materialization v2, phase 3).
 *
 * The served copy IS the author's file, served in memory in place of the file
 * on disk, with each persisted source that has a stored table re-rooted on it:
 *
 *    source: daily is ev -> { group_by: r; aggregate: total is amt.sum() }
 *       extend { dimension: big is total > 1 }
 *
 * becomes
 *
 *    source: daily is <alias>.virtual('<handle>')::<type>
 *       extend { dimension: big is total > 1 }
 *
 * under its own name, so everything the author built on it (extensions in the
 * same file or in an importing file, joins to it, views, named queries) is the
 * author's own text, unchanged, and now reads the stored table. Only the base
 * of the declaration changes: the trailing `extend { … }` blocks are kept byte
 * for byte, annotations before `source:` are untouched (gates and docs stay),
 * and nothing else in the file moves.
 *
 * Text in, text out: no compiler internals are edited. The declaration's
 * location comes from the compiled model (a top-level source's range starts
 * after `source: ` and covers `<name> is <definition>`), and every emitted name
 * and string goes through `malloy_text.ts`.
 *
 * `<alias>` is a connection name only the server knows (see
 * served_copy_config.ts), so an author's file can never name a storage
 * destination: the served copy reaches the destination only through
 * `virtual()` handles the server mapped.
 */

import {
   doubleQuotedString,
   identifier,
   nowComparison,
   stringLiteral,
} from "../../malloy_text";
import { noteSource } from "../field_notes";
import type { MaterializeReadTimeTerm } from "../plan/build_file_generator";
import { storedMalloyType } from "./materialization_serve_transform";

export { storedMalloyType };
import {
   declarationSpans,
   declaredBaseName,
   dropDimensions,
   dropExceptItems,
   inverseRenameBlocks,
   inverselyRenamedColumns,
   rewriteDimensions,
} from "../declaration_text";
import {
   isName,
   lexAll,
   offsetOf,
   statementHead,
   statementStarts,
} from "../malloy_lexer";
import {
   dropJoinDefinition,
   inlinedJoinRenames,
   inlinedJoinStatements,
   intoFirstExtend,
   labelledRenames,
   renameAccess,
   renameBlock,
   TIMEFRAMES,
   type GeneratedRename,
} from "../inlined_join_text";

export {
   declarationSpans,
   declaredBaseName,
   dropDimensions,
   dropJoinDefinition,
   rewriteDimensions,
   TIMEFRAMES,
};

/** A zero-based Malloy range, as a compiled source's `location.range` carries. */
export interface ServedCopyRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** One persisted source to re-root on its stored table. */
export interface Reroot {
   /** The file that declares the source, as its compiled `location.url`. */
   url: string;
   /** The declaration's range in that file (`<name> is <definition>`). */
   range: ServedCopyRange;
   /** The source's name in its declaring file. */
   sourceName: string;
   /** The connection name the served copy reaches the destination by. */
   connectionAlias: string;
   /** The virtualMap handle for the stored table. */
   virtualHandle: string;
   /** The `type:` the virtual source is declared as; unique in the file. */
   typeName: string;
   /**
    * The stored table's columns, under the author's spelling (a hidden one
    * with its access modifier, P3), each with
    * the annotation lines its field carries (`#(doc) "…"`, `#(classification)
    * …`), written on the field in the `type:` so they read back on it.
    */
   columns: readonly ServedColumn[];
   /**
    * For a `#(materialize)` re-root: the computed dimensions its table stores
    * under their own names. Its table is the whole source (`src -> { select:
    * … }`, its `where:` applied), so any declaration shape re-roots, and each
    * of these definitions is dropped from the source's own `extend` blocks so
    * the stored column is the field. Absent for a `#@ persist` re-root.
    */
   storedDimensions?: readonly string[];
   /**
    * Every field name the model declares, on any source. A stored timeframe
    * column is renamed internally (see {@link ServedColumn.timeframe}), and
    * the internal name must not collide with any of them: an extension sees an
    * `internal` field.
    */
   reservedNames?: readonly string[];
   /**
    * For a `#(materialize)` re-root: its plan's read-time terms, applied again
    * over the stored table at every read ({@link readTimeTermText}). A
    * current-time term inherited from a base is not in the source's own text,
    * so the re-root writes it right on the stored table (one the source's own
    * `extend` repeats is applied twice, which is harmless). A given-reading
    * term is kept as the author wrote it, in the source's own blocks or a
    * base's kept block ({@link keptBlocks}).
    */
   readTimeTerms?: readonly MaterializeReadTimeTerm[];
   /**
    * The trailing `extend` runs of the bases the source is built from in this
    * file, root first, as their own declarations wrote them
    * (kept_ancestors.ts). For an extension table (P10), whose table holds
    * every one of their rows and stored columns, and for a source whose plan
    * left out a base's given-reading `where:` or caller-dependent dimension
    * (`keptBases`), which only that base's block re-applies at read. The
    * re-root keeps their blocks ahead of its own, and their measures, joins
    * and views stay the source's.
    */
   keptBlocks?: string;
   /**
    * A v1 query-shaped source's re-root under the cutover (review F3): the
    * run it keeps is everything after its query, `include { … }` blocks
    * included, so the author's own `rename:`, `except:` and access
    * modifiers apply to the stored columns as they applied to the query's
    * output; the re-root writes no access modifier of its own.
    */
   keepsIncludes?: true;
   /**
    * With {@link keepsIncludes}: an `except:` item naming no stored column
    * (a v1 `storage=` table stores public columns only) is deleted from the
    * kept text, since the re-rooted source has nothing of that name to
    * exclude and Malloy refuses the statement (`dropExceptItems`).
    */
   dropsUnstoredExcepts?: true;
   /**
    * A v1 `#@ persist` source declared by refinement alone (`high_value is
    * raw extend { where: … }`) whose base chain carries no filter and no
    * gate: its table holds its root's rows, so re-rooting it with its own
    * `extend` run kept (its `where:` applied again) loses nothing. Without
    * it such a declaration is left as written (`not_query_shaped`).
    */
   refinementRooted?: true;
   /**
    * For a `#(materialize)` source that inlines a join_many: its stored table
    * is a pair table (P5), read as the source itself with no join
    * ({@link PairReroot}).
    */
   pair?: PairReroot;
   /**
    * For a `#(materialize)` re-root: the join_ones it inlines (`inline=`),
    * each answered from its stored `<join>__<field>` columns with no join at
    * read ({@link InlinedJoin}).
    */
   inlinedJoins?: readonly InlinedJoin[];
   /**
    * The hidden dimensions the kept text defines, computed at read rather
    * than read from a stored column (set by the rewriter, P3). A rolling
    * window over one cannot be written on the stored table, where it is not
    * yet defined: the kept text's own `where:` applies it
    * ({@link ServedCopySpan.termsKeptAsWritten}).
    */
   computedAtRead?: readonly string[];
}

/**
 * How the served copy reads a pair table (an inlined join_many, P5): as the
 * source itself, one row per row of live's LEFT JOIN, with no join at read.
 * The join's name is a record of the joined source's stored columns, so
 * `<join>.<field>` reads the column on the same row; the join_many statement
 * is replaced by that record. Each measure over the source's own fields is
 * re-declared to aggregate only the rows the parent-row flag marks (one per
 * parent), and an aggregate through the join reads every row, as live's does.
 * The stored flag and child columns are renamed internally, so no query reads
 * them. What the record cannot carry (the joined source's measures and views,
 * its fields' annotations) is not in the served source; a query that needs it
 * is answered live (governance/ir/pair_flat_query.ts says which).
 */
export interface PairReroot {
   /** The join_many's name on the re-rooted source. */
   join: string;
   /** The parent-row flag's stored column. */
   parentRowColumn: ServedColumn;
   /** The internal name the flag is read under. */
   parentRowField: string;
   /**
    * The joined source's stored fields ({@link JoinedColumn}), each with
    * `internalName`, what its stored column is renamed to.
    */
   childColumns: readonly (JoinedColumn & { internalName: string })[];
   /**
    * The child fields the author annotated, as {@link InlinedJoin}'s
    * `annotatedFields`: a query that returns one as a column is answered live.
    */
   annotatedFields?: readonly string[];
   /**
    * The source's measures over its own fields only, re-declared over the
    * parent rows: each renamed internally to `internalName` and declared again
    * under its name with the author's annotations and access.
    */
   parentMeasures: readonly PairMeasure[];
   /**
    * The joined source's computed dimensions the pair table stores, evaluated
    * at build (so a query that sets a timezone over them is answered live).
    */
   childStoredDimensions: readonly string[];
   /**
    * For a path of more than one hop (`items.options`): each join past the
    * first, in order, with its joined source's stored fields, each a record
    * nested in the one before (`items.options.kind`).
    */
   hops?: readonly PairHopColumns[];
   /**
    * For a path of more than one hop: the row flag of each level between the
    * source and the leaf (level 1 first), its stored column and the internal
    * name it is read under.
    */
   levelRows?: readonly { column: ServedColumn; field: string }[];
   /**
    * The plan's {@link MaterializePair.oneParentLevels}: the middle levels each
    * of whose rows has one row of every level above it.
    */
   oneParentLevels?: readonly number[];
   /**
    * For a table that stores more than one inlined path: which block this
    * served source reads, and the other blocks' records.
    */
   block?: PairBlockReroot;
   /**
    * The join_many's access modifier on the author's source, when it is not
    * public (known limitation 24), read off the author's compiled source. The
    * record is declared under it in place of the join_many statement, and
    * the stored columns it reads (every level of a path) are renamed
    * `private` in that same block, the only one that reads them, so a caller
    * reads `<join>.<field>` exactly where live lets it (an `internal` join
    * through a caller's own `extend`, a `private` one never) and the stored
    * columns never, while the author's own fields, measures, views and gates
    * over the join still read it.
    */
   access?: "private" | "internal";
}

/** The stored fields of one join past the first of an inlined path. */
export interface PairHopColumns {
   join: string;
   columns: readonly (JoinedColumn & { internalName: string })[];
}

/**
 * The block of a multi-path pair table a served source reads: only the rows
 * marked with its number, so every row is a row of its own path's table.
 * The other paths' joins are records too, over columns that are null on this
 * block's rows, so the author's text over them compiles; a query that reads
 * one is answered from that one's block, never this one's.
 */
export interface PairBlockReroot {
   lane: number;
   laneColumn: ServedColumn;
   laneField: string;
   others: readonly {
      join: string;
      childColumns: readonly (JoinedColumn & { internalName: string })[];
      hops?: readonly PairHopColumns[];
      /** As {@link PairReroot.access}, for that block's join_many. */
      access?: "private" | "internal";
   }[];
}

/**
 * A stored column that holds a joined source's field: `name` is the field on
 * the joined source, `column` the stored column (`<join>__<field>`), with the
 * field's type, notes and timeframe as a {@link ServedColumn} carries them.
 * What exposes stored columns under a join's name reads this shape: the
 * record that replaces an inlined join_one, and the record of a pair table's
 * child columns.
 */
export type JoinedColumn = ServedColumn & { column: string };

/**
 * An inlined join_one in the served copy. Its definition is taken out of the
 * re-rooted source's own text and a dimension of the join's name takes its
 * place: a record of the joined source's stored fields, each reading its
 * `<join>__<field>` column (renamed internally, so only the record shows
 * them). `j.field` in a caller's query, a view or a gate compiles unchanged
 * and reads the column, with no join in the SQL; an unmatched row reads
 * nulls, as live's LEFT JOIN does. What the record cannot answer fails to
 * compile (a measure, view or join of the joined source: no such field) or is
 * declined at read (an aggregate at the join's grain, which a record would
 * compute per row), and is answered live.
 */
export interface InlinedJoin {
   /** The join's name on the re-rooted source. */
   join: string;
   /** The joined source's stored fields. */
   columns: readonly JoinedColumn[];
   /**
    * The joined fields the author annotated. Their notes are on the stored
    * columns, so the record's fields keep their class, but a result column
    * that is one of them comes back without them; such a query is answered
    * live (governance/ir/joined_field_output.ts).
    */
   annotatedFields?: readonly string[];
   /**
    * The join's access modifier on the author's source, when it is not
    * public. The record is declared under it, and its stored columns are
    * renamed `private` ahead of the include that hides them
    * (`renameAccess`), so a caller reads `<join>.<field>` exactly where live
    * lets it (an `internal` join through a caller's own `extend`, a `private`
    * one never) and its stored columns never, while the author's own fields,
    * views and gates over it still read it.
    */
   access?: "private" | "internal";
}

/** One measure a pair table's served source re-declares over its parent rows. */
export interface PairMeasure {
   name: string;
   internalName: string;
   notes?: readonly string[];
   /** The author's access modifier; absent for a public measure. */
   access?: "internal";
   /**
    * The level it aggregates at, when it is a level between the source and
    * the leaf of a path of more than one hop: re-declared under that level's
    * row flag. Absent: the source's own fields, under the parent-row flag.
    */
   level?: number;
}

/** One stored column of a re-rooted source. */
export interface ServedColumn {
   name: string;
   /** The stored (warehouse) type. */
   type: string;
   /** The annotation lines its field carries, written on it again. */
   notes?: readonly string[];
   /**
    * The access modifier the author's source gives the field, when it is not
    * public (P3). The re-root writes it again on the stored table,
    * `include { public: *; private: org_id }`, so a caller cannot select the
    * column, and the source's own `extend` reads it as it read the field.
    */
   access?: "private" | "internal";
   /**
    * The timeframe the author's field truncates to (`order_date.month`). A
    * `type:` field cannot carry one, so the stored column is renamed
    * internally and the field redefined as that column truncated again, which
    * is the identity on a truncated value and restores the field's timeframe:
    *
    *    internal rename: order_month__stored is order_month
    *    dimension: order_month is order_month__stored.month
    */
   timeframe?: string;
   /**
    * The native SQL type the stored column is read through (`HUGEINT`), for a
    * big-integer field (`bigIntegerReads`, known limitation 12): a `type:`
    * field can only be a `number`, which Malloy reads through a JavaScript
    * number and rounds above 2^53, so the column is renamed internally, as a
    * timeframe column is, and the field redefined as it cast to that type,
    * which Malloy types as a `bigint` number and answers with every digit:
    *
    *    internal rename: id__stored is id
    *    dimension: id is id__stored::"HUGEINT"
    */
   nativeCast?: string;
   /**
    * The Malloy type the column is read as when its stored type does not
    * serve as its declared one but a cast makes it faithful
    * (`faithfulCastTarget`: an integer flag as `boolean`, a timestamp as
    * `date`; the v1 cutover's G5, for a table an older build typed). The
    * `type:` declares the stored type, and the column is renamed internally
    * and the field redefined as its cast, as for a timeframe (both at once
    * for a truncation stored as a timestamp):
    *
    *    internal rename: is_big__stored is is_big
    *    dimension: is_big is is_big__stored::boolean
    *
    * (a date as `::"DATE"`, see `timeframeExpression`).
    */
   castTo?: "boolean" | "date";
   /**
    * Declared `timestamptz` rather than the stored type's `timestamp`: a
    * TIMESTAMP WITH TIME ZONE column whose author field is a `timestamptz`,
    * so the field reads back as one (the v1 cutover).
    */
   timestamptz?: true;
}

/** A timeframe in {@link TIMEFRAMES}, as Malloy text. */
function timeframeKeyword(timeframe: string | undefined): string {
   switch (timeframe) {
      case "second":
         return "second";
      case "minute":
         return "minute";
      case "hour":
         return "hour";
      case "day":
         return "day";
      case "week":
         return "week";
      case "month":
         return "month";
      case "quarter":
         return "quarter";
      default:
         return "year";
   }
}

/**
 * For a source declared over a stored table read through `virtual()` (a
 * persisted view's result, a rollup's grain): each column with a timeframe,
 * which the table's type cannot carry, renamed internally and redefined as
 * that column truncated again (the identity on a truncated value), so the
 * field keeps its timeframe. Text that opens an `extend { … }` block; empty
 * when no column has a timeframe in {@link TIMEFRAMES}.
 */
export function timeframeRestatements(
   columns: readonly {
      name: string;
      timeframe?: string;
      /** The author's annotation lines, written again on the redefined field. */
      notes?: readonly string[];
   }[],
): string {
   const truncated = columns.filter(
      (c) => c.timeframe !== undefined && TIMEFRAMES.has(c.timeframe),
   );
   if (truncated.length === 0) return "";
   const renames = truncated
      .map((c) => `    ${identifier(`${c.name}__tf`)} is ${identifier(c.name)}`)
      .join("\n");
   // malloy-text: the author's own notes, then names through identifier and a timeframe from TIMEFRAMES
   const dims = truncated
      .map(
         (c) =>
            (c.notes ?? []).map((n) => noteSource(n, "    ")).join("") +
            `    ${identifier(c.name)} is ${identifier(`${c.name}__tf`)}.${timeframeKeyword(c.timeframe)}`,
      )
      .join("\n");
   return `\n  internal rename:\n${renames}\n  dimension:\n${dims}`;
}

/** Why a declaration could not be re-rooted; the source then serves live. */
export interface RerootRefusal {
   sourceName: string;
   url: string;
   /**
    * `not_query_shaped`: the declaration's base is not a query (`x -> { … }`)
    * nor a connection's table or SQL (`conn.sql("…") extend { … }`, whose
    * table is that SQL's rows, with the declaration's own `extend` run kept
    * and applied again over it). A source declared by refinement alone
    * (`deals is all_deals extend { where: … }`) stores its root's rows, and
    * the serve side re-applies the whole chain's filters, so re-rooting it
    * would drop its base's `where:`. It is left as written; it still inherits
    * a re-rooted base by name.
    */
   reason:
      | "range_not_found"
      | "definition_not_parsed"
      | "not_query_shaped"
      | "pair_join_not_found";
}

/**
 * Where one applied re-root's own text lies in its file's served text, as
 * offsets: its `type:` declaration and its re-rooted declaration. A compile
 * problem placed in either is that re-root's (the author's own text there
 * compiles live), so the serve side can drop that re-root alone.
 */
export interface ServedCopySpan {
   sourceName: string;
   type: { start: number; end: number };
   declaration: { start: number; end: number };
   /**
    * The author's text of each current-time term the re-root does not write
    * on the stored table because it reads a hidden dimension the kept text
    * computes at read ({@link Reroot.computedAtRead}): the kept text applies
    * it as the author wrote it, which the serve side confirms on the
    * compiled filters (fails closed).
    */
   termsKeptAsWritten?: string[];
}

/** The served copy of the files that declare re-rooted sources. */
export interface ServedCopy {
   /** File URL → its served text. Files with nothing re-rooted are absent. */
   files: Map<string, string>;
   /** Re-roots that did not apply. */
   refused: RerootRefusal[];
   /** File URL → where each applied re-root's text lies in it. */
   spans: Map<string, ServedCopySpan[]>;
}

/** The compiler flag every served copy needs for `virtual()` sources. */
export const SERVED_COPY_FLAG = "##! experimental { virtual_source }";

/**
 * The flag for a served copy that writes access modifiers again on a stored
 * table (P3): `include { … }` needs `access_modifiers` too.
 */
export const SERVED_COPY_ACCESS_FLAG =
   "##! experimental { virtual_source access_modifiers }";

/**
 * Produce the served copy of every file that declares a source in `reroots`.
 * `fileText` reads a file by URL (the author's bytes, as the compile read
 * them). A file whose text is unavailable refuses all its re-roots.
 */
export function buildServedCopy(
   reroots: readonly Reroot[],
   fileText: (url: string) => string | undefined,
): ServedCopy {
   const byFile = new Map<string, Reroot[]>();
   for (const r of reroots) {
      const list = byFile.get(r.url) ?? [];
      list.push(r);
      byFile.set(r.url, list);
   }
   const files = new Map<string, string>();
   const refused: RerootRefusal[] = [];
   const spans = new Map<string, ServedCopySpan[]>();
   for (const [url, list] of byFile) {
      const text = fileText(url);
      if (text === undefined) {
         for (const r of list) {
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "range_not_found",
            });
         }
         continue;
      }
      const applied: Reroot[] = [];
      // Each applied declaration's place in `out`, kept current as earlier
      // ones (spliced later, from the last back) move it.
      const placed: {
         sourceName: string;
         start: number;
         end: number;
         kept: string[];
      }[] = [];
      let out = text;
      // Splice from the last declaration back, so earlier offsets stay valid.
      const ordered = [...list].sort(
         (a, b) =>
            offsetOf(text, b.range.start) - offsetOf(text, a.range.start),
      );
      for (const r of ordered) {
         const start = offsetOf(out, r.range.start);
         const end = offsetOf(out, r.range.end);
         if (start < 0 || end < 0 || end < start) {
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "range_not_found",
            });
            continue;
         }
         const declaration = out.slice(start, end);
         const spans = declarationSpans(declaration);
         if (
            spans &&
            !spans.queryShaped &&
            !spans.connectionRooted &&
            !r.storedDimensions &&
            !r.refinementRooted
         ) {
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "not_query_shaped",
            });
            continue;
         }
         const effective = keepHiddenDefinitions(declaration, r);
         const replaced =
            effective && rerootDeclaration(declaration, effective);
         if (
            effective &&
            replaced === undefined &&
            r.pair &&
            spans &&
            // Without the pair it re-roots: what failed is the pair's join,
            // not the declaration (an inverse rename block behind an
            // include, known limitation 30, fails either way).
            rerootDeclaration(declaration, {
               ...effective,
               pair: undefined,
            }) !== undefined
         ) {
            // The pair's join is not in the source's own `extend` text (a base
            // the re-root replaces declares it, or it shares its statement),
            // so it cannot be replaced by its record.
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "pair_join_not_found",
            });
            continue;
         }
         if (!effective || replaced === undefined) {
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "definition_not_parsed",
            });
            continue;
         }
         out = out.slice(0, start) + replaced + out.slice(end);
         const delta = replaced.length - (end - start);
         for (const p of placed) {
            p.start += delta;
            p.end += delta;
         }
         placed.push({
            sourceName: r.sourceName,
            start,
            end: start + replaced.length,
            kept: termsKeptAsWritten(effective).map((t) => t.code),
         });
         applied.push(effective);
      }
      if (applied.length === 0) continue;
      const typed = withTypes(out, applied);
      files.set(url, typed.text);
      spans.set(
         url,
         // `placed` and `applied` are in the same order, as are the types.
         placed.map((p, k) => ({
            sourceName: p.sourceName,
            type: typed.types[k],
            declaration: {
               start: p.start + typed.shift,
               end: p.end + typed.shift,
            },
            ...(p.kept.length > 0 ? { termsKeptAsWritten: p.kept } : {}),
         })),
      );
   }
   return { files, refused, spans };
}

/**
 * The re-root with each hidden stored dimension the kept text defines (the
 * declaration's own trailing `extend`, and for an extension table the kept
 * blocks of its ancestors, P10) left to that definition (P3): its column is left
 * out of the `type:` and its definition is kept as written, access modifier
 * and all, and computed at read from the stored columns it reads. Dropping
 * it instead would leave the stored column in its place, public. A hidden
 * column the kept text does not define (one an `include` in a replaced base
 * hides, or one it inherits from outside the kept blocks) stays a stored
 * column, and the
 * re-root writes its modifier again ({@link hiddenInclude}). Undefined when
 * the declaration does not lex.
 */
function keepHiddenDefinitions(
   declaration: string,
   r: Reroot,
): Reroot | undefined {
   const hidden = new Set(
      r.columns.filter((c) => c.access !== undefined).map((c) => c.name),
   );
   if (hidden.size === 0) return r;
   const spans = declarationSpans(declaration);
   if (!spans) return undefined;
   const own =
      spans.extendStart === undefined
         ? ""
         : declaration.slice(spans.extendStart);
   // Every block the re-root keeps: an extension table's ancestors' (P10),
   // then its own. A hidden dimension an ancestor's kept block defines is
   // kept as that block wrote it, as one the source's own block defines is.
   const extend = [r.keptBlocks?.trim() ?? "", own].filter(Boolean).join(" ");
   const defined = rewriteDimensions(extend, hidden, new Map());
   if (!defined) return undefined;
   if (defined.dropped.size === 0) return r;
   return {
      ...r,
      computedAtRead: [...defined.dropped],
      columns: r.columns.filter((c) => !defined.dropped.has(c.name)),
      ...(r.storedDimensions
         ? {
              storedDimensions: r.storedDimensions.filter(
                 (n) => !defined.dropped.has(n),
              ),
           }
         : {}),
   };
}

/**
 * The `include { … }` that writes the author's access modifiers again on the
 * stored table's hidden columns (P3), or "" when it has none. Every other
 * column stays public (`public: *`; an `include` makes what it does not list
 * private). It sits just before the author's own `extend`, which reads a
 * field the `include` hides as it read the field on the author's source.
 */
export function hiddenInclude(columns: readonly ServedColumn[]): string {
   return accessInclude(columns);
}

/** {@link hiddenInclude} over names and their access, public when absent. */
function accessInclude(
   columns: readonly { name: string; access?: "private" | "internal" }[],
): string {
   const byAccess = (access: "private" | "internal") =>
      columns.filter((c) => c.access === access).map((c) => identifier(c.name));
   const items = [
      "public: *",
      ...(["private", "internal"] as const).flatMap((access) => {
         const names = byAccess(access);
         return names.length > 0 ? [`${access}: ${names.join(", ")}`] : [];
      }),
   ];
   return items.length > 1 ? ` include { ${items.join("; ")} }` : "";
}

/**
 * Whether a re-root writes access modifiers again, so needs their flag: a
 * hidden stored column, or a hidden inlined join's record.
 */
function hasHiddenColumns(
   r: Pick<Reroot, "columns" | "inlinedJoins" | "pair">,
): boolean {
   return (
      r.columns.some((c) => c.access !== undefined) ||
      (r.inlinedJoins ?? []).some((j) => j.access !== undefined) ||
      r.pair?.access !== undefined ||
      (r.pair?.block?.others ?? []).some((o) => o.access !== undefined)
   );
}

/**
 * The re-rooted declaration text for one source: `<name> is <virtual>` plus the
 * author's trailing `extend { … }` blocks, verbatim. Undefined when the
 * declaration text does not parse as `<name> is <definition>`, or, for a pair
 * table, when the join_many it replaces is not in that text on its own.
 */
export function rerootDeclaration(
   declaration: string,
   r: Pick<
      Reroot,
      | "sourceName"
      | "connectionAlias"
      | "virtualHandle"
      | "typeName"
      | "storedDimensions"
      | "reservedNames"
      | "readTimeTerms"
      | "keptBlocks"
      | "pair"
      | "inlinedJoins"
      | "computedAtRead"
      | "keepsIncludes"
      | "dropsUnstoredExcepts"
   > & { columns?: Reroot["columns"] },
): string | undefined {
   const spans = declarationSpans(declaration, {
      throughIncludes: r.keepsIncludes === true,
   });
   if (!spans) return undefined;
   const virtual =
      `${identifier(r.connectionAlias)}.virtual(${stringLiteral(r.virtualHandle)})` +
      `::${identifier(r.typeName)}`;
   const name = declaration.slice(0, spans.nameEnd).trimEnd();
   const inlined = r.inlinedJoins ?? [];
   // No stored timeframe column is renamed to an inlined join's column.
   const reserved = [
      ...(r.reservedNames ?? []),
      ...inlined.flatMap((j) => j.columns.map((c) => c.column)),
   ];
   const own =
      spans.extendStart === undefined
         ? ""
         : declaration.slice(spans.extendStart).trimEnd();
   let kept = [r.keptBlocks?.trim() ?? "", own].filter(Boolean).join(" ");
   if (r.dropsUnstoredExcepts) {
      const stored = new Set(
         (r.columns ?? []).map((c) => c.name.toLowerCase()),
      );
      const edited = dropExceptItems(
         kept,
         (name) => !stored.has(name.toLowerCase()),
      );
      if (edited === undefined) return undefined;
      kept = edited.text;
   }
   // The table stores a renamed field under its new name; the kept text
   // renames it from the old one, so the old name is given back first.
   const renames = inverseRenameBlocks(
      kept,
      new Set((r.columns ?? []).map((c) => c.name)),
   );
   if (renames === undefined) return undefined;
   const columns = r.columns ?? [];
   const redefined = timeframedColumns(columns, reserved);
   // A cast column the inverse renames give an old name back cannot be read
   // under its stored name after them: it is cast in a block of its own just
   // ahead of them, and the renames then rename the cast field.
   const renamedBack = inverselyRenamedColumns(
      kept,
      new Set(columns.map((c) => c.name)),
   );
   const early = redefined.filter(
      (c) => c.nativeCast !== undefined && renamedBack.has(c.name),
   );
   const timeframed = redefined.filter((c) => !early.includes(c));
   const joinRenames = inlinedJoinRenames(inlined, [
      ...(r.reservedNames ?? []),
      ...columns.map((c) => c.name),
      ...timeframed.map((c) => c.internalName),
   ]);
   const generated: GeneratedRename[] = [
      ...timeframed.map((c) => ({
         internal: c.internalName,
         column: c.name,
         access: renameAccess(c),
      })),
      ...joinRenames,
   ];
   // The author's access modifiers written again on the stored hidden
   // columns (P3), on the stored table under its stored names, so before the
   // inverse renames and every kept block (an ancestor's, P10, and the
   // author's own), which read them as they read the author's fields. Only a
   // hidden column kept under its own name needs that `include`: when the
   // re-root writes one, every internal name is renamed ahead of it and the
   // include hides it ({@link renameAccess}), since Malloy refuses a rename
   // in the block just after an include. A hidden timeframe column or a
   // hidden join's columns alone are hidden by labelled renames in the first
   // block (`private rename:`) instead, so an author's `rename:` (and the
   // inverse block before it) still compiles; an include there would refuse
   // it, and with it the whole model's served copy.
   const ahead = columns.some(
      (c) =>
         c.access !== undefined && !timeframed.some((t) => t.name === c.name),
   );
   let extend = servedExtend(kept, r.storedDimensions, timeframed, !ahead);
   if (extend === undefined) return undefined;
   if (inlined.length > 0) {
      for (const j of inlined) {
         const dropped = dropJoinDefinition(extend, j.join);
         if (dropped === undefined) return undefined;
         extend = dropped;
      }
      const opened = intoFirstExtend(
         extend,
         inlinedJoinStatements(inlined, joinRenames, !ahead),
      );
      if (opened === undefined) return undefined;
      extend = opened;
   }
   let pairHead = "";
   if (r.pair) {
      // A hidden join_many's statement is replaced from its access label
      // on, since its record is declared under the join's compiled modifier.
      let replaced: string | undefined = replacePairJoin(
         extend,
         r.pair.join,
         pairRecord(r.pair),
         r.pair.access !== undefined,
      );
      for (const other of r.pair.block?.others ?? []) {
         if (replaced === undefined) break;
         replaced = replacePairJoin(
            replaced,
            other.join,
            pairRecord(other),
            other.access !== undefined,
         );
      }
      if (replaced === undefined) return undefined;
      // Behind an include, the pair's renames go ahead of it with the
      // others (Malloy refuses a rename in the block just after an include,
      // known limitation 30), and the include hides each internally.
      if (ahead) {
         generated.push(
            ...pairStoredColumns(r.pair).map((c) => ({
               internal: c.internalName,
               column: c.column,
               access: "internal" as const,
            })),
         );
      }
      pairHead = pairRenames(r.pair, !ahead);
      if (pairHead) pairHead = ` ${pairHead}`;
      extend = `${replaced}${pairMeasures(r.pair)}`;
   }
   // The inverse renames just after the include are refused by Malloy,
   // which would fail the whole served copy and send every source in the
   // model live: refuse this source's re-root alone instead (known
   // limitation 30). A pair table's renames go ahead of the include (above).
   if (ahead && renames !== "") return undefined;
   // A hidden cast column renamed back would need its private name read
   // past the block that defines it: left to its fallback.
   if (early.some((c) => c.access !== undefined)) return undefined;
   const castAhead =
      early.length > 0
         ? ` ${intoFirstExtend("", timeframeStatements(early, early, true))}`
         : "";
   const keptAsWritten = new Set(termsKeptAsWritten(r));
   const terms = (r.readTimeTerms ?? [])
      .filter((t) => !keptAsWritten.has(t))
      .map(readTimeTermText)
      .filter((t) => t !== undefined);
   const reread = terms.length ? ` extend { where: ${terms.join(", ")} }` : "";
   const hide = ahead
      ? `${renameBlock(generated)}${accessInclude([
           ...columns
              .filter((c) => !timeframed.some((t) => t.name === c.name))
              .map((c) => ({ name: c.name, access: c.access })),
           ...generated.map((g) => ({ name: g.internal, access: g.access })),
        ])}`
      : "";
   return `${name} is ${virtual}${reread}${hide}${pairHead}${castAhead}${renames}${extend ? ` ${extend}` : ""}`;
}

/**
 * A run of `extend { … }` blocks as the served copy keeps it over stored
 * columns: each stored computed dimension's definition dropped (the column
 * stands in for it), and each stored timeframe column renamed internally and
 * its field redefined with the timeframe a `type:` cannot declare. Undefined
 * when the text does not lex. `storedDimensions` is undefined for a
 * `#@ persist` re-root, and set (maybe empty) for a `#(materialize)` one.
 */
function servedExtend(
   text: string,
   storedDimensions: readonly string[] | undefined,
   timeframed: readonly Timeframed[],
   withRenames: boolean,
): string | undefined {
   let extend = text;
   const stored = new Set(storedDimensions ?? []);
   const rewrite = new Map<string, string>();
   for (const c of timeframed) {
      // Only a `#(materialize)` computed dimension has a definition in the
      // source's own text to rewrite; a `#@ persist` query's output has none.
      if (stored.has(c.name)) rewrite.set(c.name, timeframeExpression(c));
   }
   let rewritten = new Set<string>();
   if (stored.size > 0) {
      // A dimension the source inherits is not in its own text: the base the
      // re-root replaces defined it, and the stored column stands in alone.
      const kept = rewriteDimensions(
         extend,
         new Set([...stored].filter((n) => !rewrite.has(n))),
         rewrite,
      );
      if (!kept) return undefined;
      extend = kept.text;
      rewritten = kept.rewritten;
   }
   if (timeframed.length > 0) {
      const opened = intoFirstExtend(
         extend,
         timeframeStatements(
            timeframed,
            timeframed.filter((c) => !rewritten.has(c.name)),
            withRenames,
         ),
      );
      if (opened === undefined) return undefined;
      extend = opened;
   }
   return extend;
}

/** Every stored column a pair table's served source renames internally. */
function pairStoredColumns(
   pair: PairReroot,
): { column: string; internalName: string }[] {
   const joined = (
      childColumns: readonly (JoinedColumn & { internalName: string })[],
      hops: readonly PairHopColumns[] | undefined,
   ) => [...childColumns, ...(hops ?? []).flatMap((h) => h.columns)];
   return [
      { column: pair.parentRowColumn.name, internalName: pair.parentRowField },
      ...(pair.levelRows ?? []).map((l) => ({
         column: l.column.name,
         internalName: l.field,
      })),
      ...(pair.block
         ? [
              {
                 column: pair.block.laneColumn.name,
                 internalName: pair.block.laneField,
              },
           ]
         : []),
      // A hidden join's stored columns are renamed in its record's block.
      ...(pair.access ? [] : joined(pair.childColumns, pair.hops)),
      ...(pair.block?.others ?? []).flatMap((o) =>
         o.access ? [] : joined(o.childColumns, o.hops),
      ),
   ];
}

/**
 * The extend block that opens a pair table's served source: the flags and
 * each child column renamed internally, so the record and the re-declared
 * measures read them and no query does; for a block of a multi-path table,
 * the rows of that block only. Without `withRenames` (a re-root that writes
 * an include renames them ahead of it), only the block's `where:`, or "".
 */
function pairRenames(pair: PairReroot, withRenames = true): string {
   const renames = pairStoredColumns(pair)
      .map(
         (c) => `    ${identifier(c.internalName)} is ${identifier(c.column)}`,
      )
      .join("\n");
   const lane = pair.block
      ? `\n  where: ${identifier(pair.block.laneField)} = ${Math.trunc(pair.block.lane)}`
      : "";
   if (!withRenames) return lane ? `extend {${lane}\n}` : "";
   return `extend {\n  internal rename:\n${renames}${lane}\n}`;
}

/**
 * The record a pair table's join_many is replaced by: `<join> is { <field> is
 * <internal column>, … }`, a timeframe field truncated again as its stored
 * value was, and for a path of more than one hop each further join a record
 * nested in the one before. For a hidden join ({@link PairReroot.access}) the
 * record is declared under the join's modifier and followed, in the same
 * block, by a `private rename:` of every stored column it reads: a private
 * name is readable only in the block that defines it, which is the record's.
 */
function pairRecord(record: {
   join: string;
   childColumns: readonly (JoinedColumn & { internalName: string })[];
   hops?: readonly PairHopColumns[];
   access?: "private" | "internal";
}): string {
   const { join, childColumns, hops, access } = record;
   const field = (c: JoinedColumn & { internalName: string }, pad: string) =>
      // malloy-text: names through identifier, a timeframe from TIMEFRAMES and a cast through doubleQuotedString
      `${pad}${identifier(c.name)} is ${identifier(c.internalName)}` +
      (c.nativeCast
         ? `::${doubleQuotedString(c.nativeCast)}`
         : c.timeframe && TIMEFRAMES.has(c.timeframe)
           ? `.${c.timeframe}`
           : "");
   const level = (k: number, pad: string): string => {
      const columns = k === 0 ? childColumns : (hops ?? [])[k - 1].columns;
      const items = columns.map((c) => field(c, pad));
      if (k < (hops?.length ?? 0)) {
         const next = (hops ?? [])[k];
         items.push(
            `${pad}${identifier(next.join)} is {\n${level(k + 1, `${pad}  `)}\n${pad}}`,
         );
      }
      return items.join(",\n");
   };
   const dimension = `${accessPrefix(access)}dimension: ${identifier(join)} is {\n${level(0, "    ")}\n  }`;
   if (access === undefined) return dimension;
   const renames = [...childColumns, ...(hops ?? []).flatMap((h) => h.columns)]
      .map(
         (c) => `    ${identifier(c.internalName)} is ${identifier(c.column)}`,
      )
      .join("\n");
   return `${dimension}\n  private rename:\n${renames}`;
}

/**
 * The extend block that closes a pair table's served source: each measure
 * over one level above the leaf (the source's own fields, or a level between
 * it and the leaf of a longer path) renamed internally and declared again,
 * under its own name, annotations and access, over the rows that level's
 * flag marks. A measure that reads it by name (a ratio of two, a view) reads
 * the re-declared one. Empty when there is none.
 */
function pairMeasures(pair: PairReroot): string {
   if (pair.parentMeasures.length === 0) return "";
   const flagOf = (m: PairMeasure) => {
      const level = m.level ?? 0;
      return identifier(
         level === 0
            ? pair.parentRowField
            : ((pair.levelRows ?? [])[level - 1]?.field ?? pair.parentRowField),
      );
   };
   const renames = pair.parentMeasures
      .map((m) => `    ${identifier(m.internalName)} is ${identifier(m.name)}`)
      .join("\n");
   const notes = (m: PairMeasure) =>
      // malloy-text: the author's own notes, as written on the measure
      (m.notes ?? []).map((n) => noteSource(n, "    ")).join("");
   const definitions = (list: readonly PairMeasure[]) =>
      list
         .map(
            (m) =>
               notes(m) +
               `    ${identifier(m.name)} is ${identifier(m.internalName)} { where: ${flagOf(m)} }`,
         )
         .join("\n");
   const visible = pair.parentMeasures.filter((m) => m.access === undefined);
   const internal = pair.parentMeasures.filter((m) => m.access === "internal");
   const blocks = [`  internal rename:\n${renames}`];
   if (visible.length > 0) blocks.push(`  measure:\n${definitions(visible)}`);
   if (internal.length > 0) {
      blocks.push(`  internal measure:\n${definitions(internal)}`);
   }
   return ` extend {\n${blocks.join("\n")}\n}`;
}

/**
 * In a run of `extend { … }` blocks, replace the `join_many:` statement that
 * declares `join` (and nothing else) with `statement`, from its keyword on, so
 * the annotations and access label written before it stay on the statement
 * that replaces it. Undefined when the text does not lex, declares no such
 * join_many (one inherited from a base the re-root replaces is not in it), or
 * declares it in a statement with other joins. With `fromLabel`, the
 * statement's access label (`internal join_many: …`) is replaced too, for a
 * `statement` that writes its own.
 */
export function replacePairJoin(
   text: string,
   join: string,
   statement: string,
   fromLabel = false,
): string | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const level = tokens.filter((t) => t.depth === 1 && t.kind !== "close");
   const closes = tokens.filter((t) => t.kind === "close" && t.depth === 0);
   const starts = statementStarts(level);
   const nameOf = (t: { text: string }) =>
      t.text.startsWith("`") ? t.text.slice(1, -1) : t.text;
   for (let si = 0; si < starts.length; si++) {
      const kw = level[starts[si]];
      if (kw.text !== "join_many") continue;
      const colon = level[starts[si] + 1];
      const nextStart =
         si + 1 < starts.length
            ? statementHead(level, starts[si + 1]).start
            : Infinity;
      const blockEnd =
         closes.find((t) => t.start > kw.start)?.start ?? Infinity;
      const stmtEnd = Math.min(nextStart, blockEnd);
      const body = level.filter(
         (t) => t.start > colon.start && t.start < stmtEnd,
      );
      const first = body[0];
      if (!first || !isName(first) || nameOf(first) !== join) continue;
      // One definition: no comma at the statement's own level, and no second
      // `<name> on`/`<name> is` opening a line of its own.
      const another = body.some(
         (t, k) =>
            k > 0 &&
            (t.text === "," ||
               (isName(t) &&
                  (body[k + 1]?.text === "on" || body[k + 1]?.text === "is") &&
                  text.slice(body[k - 1].end, t.start).includes("\n"))),
      );
      if (another) return undefined;
      const inStatement = tokens.filter(
         (t) => t.start >= kw.start && t.end <= stmtEnd,
      );
      const end = inStatement[inStatement.length - 1]?.end;
      if (end === undefined) return undefined;
      const from = fromLabel
         ? statementHead(level, starts[si]).start
         : kw.start;
      return text.slice(0, from) + statement + text.slice(end);
   }
   return undefined;
}

/**
 * The `where:` text the re-root writes over the stored table for one
 * read-time term, or undefined when the source's own text already carries it.
 * Exhaustive by `kind`, so a new kind of read-time term must say how it is
 * re-applied.
 */
function readTimeTermText(term: MaterializeReadTimeTerm): string | undefined {
   switch (term.kind) {
      case "current_time":
         // Written from the classified parts, never the author's text: a term
         // inherited from a base is re-applied here, outside its own file. A
         // lower bound trims what the build's `now` kept; an upper bound, left
         // out of the build, is applied here alone (a two-sided term is two).
         return nowComparison(
            term.window.field,
            term.window.op,
            term.window.offset,
         );
      case "given":
         // Kept, not written: a given-reading term (P2) is in the
         // declaration's own trailing `extend` or a kept base's
         // (`keptBlocks`), which the re-root keeps as the author wrote it (the
         // build generator takes a term out only there).
         return;
   }
}

/**
 * The current-time terms a re-root leaves to the kept text: each that reads
 * a hidden dimension the kept text computes at read
 * ({@link Reroot.computedAtRead}). Such a dimension is defined only in the
 * kept text, so the term cannot be written on the stored table ahead of it,
 * and the term itself is in that text too (whatever reads a field the
 * source's own or a kept base's block defines is written there or after it,
 * on the chain the re-root keeps), so it is applied as the author wrote it,
 * as a given-reading term is. The serve side confirms each on the served
 * compile's filters and leaves the source as written when one is missing.
 */
export function termsKeptAsWritten(
   r: Pick<Reroot, "readTimeTerms" | "computedAtRead">,
): MaterializeReadTimeTerm[] {
   const atRead = new Set(r.computedAtRead ?? []);
   if (atRead.size === 0) return [];
   return (r.readTimeTerms ?? []).filter(
      (t) =>
         t.kind === "current_time" &&
         (atRead.has(t.window.field) || t.columns.some((c) => atRead.has(c))),
   );
}

/**
 * A column whose field the served copy redefines over its stored value (a
 * timeframe, a native cast), with the internal name that value is renamed to.
 */
interface Timeframed extends ServedColumn {
   internalName: string;
}

/**
 * The columns whose field is redefined over the stored value (one that
 * carries a timeframe, one read through a cast or a native cast), each with an
 * internal name that is none of `reserved` and no stored column's name.
 */
function timeframedColumns(
   columns: readonly ServedColumn[],
   reserved: readonly string[],
): Timeframed[] {
   const taken = new Set([...reserved, ...columns.map((c) => c.name)]);
   const out: Timeframed[] = [];
   for (const c of columns) {
      const timeframe =
         c.timeframe && TIMEFRAMES.has(c.timeframe) ? c.timeframe : undefined;
      if (!timeframe && !c.castTo && !c.nativeCast) continue;
      let internalName = `${c.name}__stored`;
      for (let n = 2; taken.has(internalName); n++) {
         internalName = `${c.name}__stored_${n}`;
      }
      taken.add(internalName);
      const { timeframe: _tf, ...rest } = c;
      out.push({ ...rest, ...(timeframe ? { timeframe } : {}), internalName });
   }
   return out;
}

/**
 * `<internal>.<timeframe>`: the stored value truncated again; with a cast,
 * the stored value cast first, so a column an older build typed reads as its
 * declared type (`(<internal>::"DATE").month` for both).
 *
 * A date is cast with the SQL type (`::"DATE"`), which Malloy writes as a
 * plain `CAST(… AS DATE)`: Malloy's own `::date` of a timestamp converts it
 * to a query's `timezone:` first, which would move the stored date by a day
 * (the serve shape cast in a query-shaped source, in the session's zone).
 * A big-integer column is read as its native type instead
 * (`<internal>::"HUGEINT"`, {@link ServedColumn.nativeCast}).
 */
function timeframeExpression(c: Timeframed): string {
   const stored = identifier(c.internalName);
   // A big-integer column read as its native type, which Malloy types as a
   // `bigint` number (known limitation 12).
   if (c.nativeCast) return `${stored}::${doubleQuotedString(c.nativeCast)}`;
   const read =
      c.castTo === "date"
         ? `${stored}::"DATE"`
         : c.castTo
           ? `${stored}::${c.castTo}`
           : stored;
   if (!c.timeframe) return read;
   return c.castTo ? `(${read}).${c.timeframe}` : `${read}.${c.timeframe}`;
}

/** A statement's access label, as Malloy text: empty for a public one. */
function accessPrefix(access: "private" | "internal" | undefined): string {
   switch (access) {
      case "private":
         return "private ";
      case "internal":
         return "internal ";
      default:
         return "";
   }
}

/**
 * The statements that open the first extend block: with `withRenames`, the
 * internal renames ({@link labelledRenames}), and a `dimension:` for each
 * timeframe field whose definition is not in the source's own text (an
 * inherited one, or a `#@ persist` query's output column), with the field's
 * annotations.
 *
 * A hidden field (P3) is redefined under the author's modifier, and its
 * stored column's internal name is `private` ({@link renameAccess}), so no
 * caller reaches it by any path (a `private rename:` here, or, when the
 * re-root writes an `include` for other hidden columns, a rename ahead of it
 * that the include hides, and `withRenames` is false). A public field's stored
 * column is renamed `internal`, so no `select:` reaches it.
 */
function timeframeStatements(
   all: readonly Timeframed[],
   emitted: readonly Timeframed[],
   withRenames: boolean,
): string {
   let out = withRenames
      ? labelledRenames(
           all.map((c) => ({
              internal: c.internalName,
              column: c.name,
              access: renameAccess(c),
           })),
        )
      : "";
   for (const access of [undefined, "internal", "private"] as const) {
      // malloy-text: the author's own notes, then names through identifier and a timeframe from TIMEFRAMES
      const dims = emitted
         .filter((c) => c.access === access)
         .map(
            (c) =>
               (c.notes ?? []).map((n) => noteSource(n, "    ")).join("") +
               `    ${identifier(c.name)} is ${timeframeExpression(c)}`,
         )
         .join("\n");
      if (dims) out += `\n  ${accessPrefix(access)}dimension:\n${dims}`;
   }
   return out;
}

/**
 * The Malloy type a stored column is declared as in the `type:`: the one rule
 * every stored table's declaration follows ({@link storedMalloyType}). A
 * column it cannot declare is left out before a declaration is written, so
 * the `json` here is never reached by one that compiles: Malloy refuses it in
 * a `type:`, which fails that copy and leaves the source to its fallback.
 */
export function storedTypeText(stored: string): string {
   return storedMalloyType(stored) ?? "json";
}

/** The emitted `type:` declaration for one re-rooted source. */
export function typeDeclaration(
   r: Pick<Reroot, "typeName" | "columns" | "inlinedJoins">,
): string {
   // An inlined join's columns under their stored names, each with its
   // joined field's notes: a record literal's field cannot carry an
   // annotation, so the field it reads carries them, and the record's field
   // takes its class from it.
   const joined = (r.inlinedJoins ?? []).flatMap((j) =>
      j.columns.map(
         (c): ServedColumn => ({
            name: c.column,
            type: c.type,
            ...(c.notes ? { notes: c.notes } : {}),
         }),
      ),
   );
   // malloy-text: the author's own notes, then names through identifier and a Malloy type name
   const fields = [...r.columns, ...joined]
      .map(
         (c) =>
            // A timeframe or cast column's notes go on its redefined field
            // instead.
            ((c.timeframe && TIMEFRAMES.has(c.timeframe)) ||
            c.castTo ||
            c.nativeCast
               ? []
               : (c.notes ?? [])
            )
               .map((note) => noteSource(note, "   "))
               .join("") +
            `   ${identifier(c.name)}::${c.timestamptz ? "timestamptz" : storedTypeText(c.type)}`,
      )
      .join(",\n");
   return `type: ${identifier(r.typeName)} is {\n${fields}\n}`;
}

/**
 * The stored columns a pair table's served source reads, beside the
 * source's own: its flags, its block column, and every child column of every
 * path it records, each carrying its field's annotations, so the record's
 * fields, derived from it, keep its classification (a record's own fields
 * cannot be annotated).
 */
function pairTypedColumns(pair: PairReroot): ServedColumn[] {
   const joined = (c: JoinedColumn): ServedColumn => ({
      name: c.column,
      type: c.type,
      ...(c.notes ? { notes: c.notes } : {}),
   });
   const record = (
      childColumns: readonly JoinedColumn[],
      hops: readonly PairHopColumns[] | undefined,
   ) => [
      ...childColumns.map(joined),
      ...(hops ?? []).flatMap((h) => h.columns.map(joined)),
   ];
   return [
      pair.parentRowColumn,
      ...(pair.levelRows ?? []).map((l) => l.column),
      ...(pair.block ? [pair.block.laneColumn] : []),
      ...record(pair.childColumns, pair.hops),
      ...(pair.block?.others ?? []).flatMap((o) =>
         record(o.childColumns, o.hops),
      ),
   ];
}

/**
 * Insert the compiler flag and the `type:` declarations after the file's
 * leading model annotations (`##` lines), blank lines and comments, so no
 * statement's own annotations (`#@ persist`, `#(authorize)`) are separated
 * from it.
 */
function withTypes(
   text: string,
   applied: readonly Reroot[],
): {
   text: string;
   /** How far the author's statements moved down. */
   shift: number;
   /** Each re-root's `type:` declaration, in `applied`'s order, as offsets. */
   types: { start: number; end: number }[];
} {
   const lines = text.split("\n");
   let i = 0;
   while (i < lines.length) {
      const t = lines[i].trim();
      if (
         t === "" ||
         t.startsWith("##") ||
         t.startsWith("--") ||
         t.startsWith("//")
      ) {
         i++;
         continue;
      }
      break;
   }
   const declarations = applied.map((r) =>
      typeDeclaration(
         r.pair
            ? {
                 typeName: r.typeName,
                 inlinedJoins: r.inlinedJoins,
                 // Each child column carries its field's annotations, so
                 // the record's fields, derived from it, keep its
                 // classification (a record's own fields cannot be
                 // annotated).
                 columns: [...r.columns, ...pairTypedColumns(r.pair)],
              }
            : r,
      ),
   );
   const types = declarations.join("\n");
   const head = lines.slice(0, i).join("\n");
   const rest = lines.slice(i).join("\n");
   const flag = applied.some(hasHiddenColumns)
      ? SERVED_COPY_ACCESS_FLAG
      : SERVED_COPY_FLAG;
   const prefix = `${head}${head ? "\n" : ""}${flag}\n`;
   const out = `${prefix}${types}\n\n${rest}`;
   const restAt = i === 0 ? 0 : head.length + 1;
   const typeSpans: { start: number; end: number }[] = [];
   let at = prefix.length;
   for (const declaration of declarations) {
      typeSpans.push({ start: at, end: at + declaration.length });
      at += declaration.length + 1;
   }
   return {
      text: out,
      shift: out.length - rest.length - restAt,
      types: typeSpans,
   };
}
