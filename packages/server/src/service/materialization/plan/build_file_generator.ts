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
 * What is stored: every public atomic dimension of `src`, each under its own
 * name, computed or not (decided 2026-11-12), so the stored table carries the
 * author's names and an outside reader sees `order_month`, not a suffixed
 * column. The served copy (the re-root rewriter) drops a stored computed
 * dimension's definition, so the column stands in for it, and carries the
 * dimension's annotations onto the served `type:` declaration. Measures, views
 * and joins are not stored: the served copy keeps them as written, re-bound
 * over the stored columns. A field that is not public (`internal`, `private`)
 * is never stored, matching today's narrowing of a `storage=` table to the
 * public surface.
 *
 * The table: named after the author's source (`src`), or the declaration's
 * `name=`. `src__mat` is only the generated build source's internal name in
 * the companion model. Without a `name=`, a `storage=` source's table defaults
 * to the source's own name (the destination is Publisher's, and the existing
 * collision checks refuse two sources on one table). A colocated source, built
 * into the author's own warehouse, must name its table: its default would be
 * the source's own name, which for a source over `conn.table('orders')` is
 * very often the very table it reads, and the build would replace it.
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
 * An inlined join is checked unique at every build (P8): the plan carries a
 * generated group-by over the joined source on the join's key, and a build
 * whose check finds a key matched twice fails, naming the join.
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
 * copy computes it at read, over the record. One join_many per source,
 * declared on the same connection, its condition reading no other join, both
 * sides with a declared primary key that is stored, the joined source with no
 * joins of its own (one hop), and no incremental refresh (the flag is written
 * over the whole table: an increment that adds a child to a parent already
 * stored would mark that parent twice); anything else is refused.
 *
 * Not yet (each is a later pull request's, and is refused or left out here
 * rather than built wrong):
 * - `inline=` on a join_cross is refused.
 * - A materialized extension builds from its own source over the raw tables,
 *   not from its parent's build source (P10): building over the parent needs
 *   the rewriter's re-rooted copy, so it waits for it.
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
import { declarationSpans } from "../declaration_text";
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
    * For a column of an inlined join's joined source: the join's name, with
    * {@link dimension} the joined source's field. The served copy declares
    * these under the join's name, in place of the join.
    */
   join?: string;
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
   /** The declaration's range in its declaring file, when it has given terms. */
   declaration?: TermUse;
   /** The givens its given-reading terms read, for the generated file to import. */
   givenDeclarations?: MaterializeGivenDeclaration[];
   /** The joins the declaration inlines (`inline=[…]`). */
   inline?: string[];
   /** One uniqueness check per inlined join_one (P8). */
   uniquenessChecks?: InlineUniquenessCheck[];
   /** For an inlined join_many: the pair table's layout (P5). */
   pair?: MaterializePair;
   /**
    * The table takes the source's own name because the declaration has no
    * `name=`, which the refusal of a colliding table name says (see
    * `refuseCollidingMaterialize`).
    */
   nameDefaulted?: true;
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

/** Fails closed, as `isHidden` in preaggregation_synthesis.ts does. */
function isHidden(field: FieldLike): boolean {
   return field.accessModifier != null && field.accessModifier !== "public";
}

/** A Malloy `sourceID`'s declared name and declaring file, or undefined. */
function splitSourceID(
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
): { persist: Record<string, string> } | { reason: string } {
   const persist: Record<string, string> = {};
   for (const key of CARRIED_KEYS) {
      if (!declaration.keys.includes(key)) continue;
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
   return { persist };
}

/** Whether a compiled field is a public atomic dimension a table can hold. */
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
   if (isHidden(field)) return undefined;
   return dimension;
}

/**
 * Whether a compiled field is a public array its table can hold as it is: a
 * persisted query's `nest:` (a repeated record) or an array column the source
 * reads, neither computed. A declared `join_many:` is the joined source's
 * type, never `array`, so it is not one.
 */
function storableArray(field: FieldLike): string | undefined {
   if (field.type !== "array" || field.e !== undefined) return undefined;
   if (isHidden(field)) return undefined;
   const dimension =
      typeof field.as === "string"
         ? field.as
         : typeof field.name === "string"
           ? field.name
           : undefined;
   return dimension;
}

/**
 * The stored columns of a source: its public atomic dimensions and arrays
 * ({@link storableArray}), less any
 * whose value reads through a join that is not inlined (a reference join is
 * read at query time, so what reads through it is computed at read). A field
 * whose join usage is unknown is stored only when the source has no join.
 * `inline` names the inlined joins; `joins` comes from the source map.
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
 * dimensions that read no join of their own (one hop), each stored as
 * `<join>__<field>`.
 */
function inlinedJoinColumns(
   map: SourceMap,
   join: SourceJoin,
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
      if (perRead.has(dimension)) continue;
      if (field.e !== undefined) {
         const r = reads.get(dimension);
         if (r === undefined ? hasJoins : r.joins.size > 0 || r.nested) {
            continue;
         }
      }
      out.push({
         dimension,
         column: `${join.name}__${dimension}`,
         computed: true,
         join: join.name,
      });
   }
   return out;
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
 * Why an inlined join_many cannot be stored as a pair table, or its layout
 * (see the module comment for each rule). `columns` are the source's own
 * stored columns; `joined` the join's `j__<field>` columns.
 */
function pairRefusal(
   join: SourceJoin,
   def: SourceDefLike,
   map: SourceMap,
   columns: readonly MaterializeColumn[],
   joined: readonly MaterializeColumn[],
   persist: Readonly<Record<string, string>>,
): { pair: MaterializePair } | { reason: string } {
   const name = quotedName(join.name);
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
   const childKey = join.primaryKey;
   if (
      !childKey ||
      !joined.some((c) => c.join === join.name && c.dimension === childKey)
   ) {
      return {
         reason:
            `inline= names ${name}, a join_many, and a pair table needs the ` +
            "joined source's primary key, declared and stored (a public " +
            "dimension), to keep one row per child",
      };
   }
   if (map.joinedReadsOf(join).joins.length > 0) {
      return {
         reason:
            `inline= names ${name}, a join_many whose joined source has joins ` +
            "of its own; a pair table stores one hop, so inline only a " +
            "join_many to a source with no joins",
      };
   }
   if (persist.refresh === "incremental") {
      return {
         reason:
            `inline= names ${name}, a join_many, on an incremental source; a ` +
            "pair table's parent-row flag is written over the whole table " +
            "(an increment that adds a child to a stored parent would mark " +
            "that parent twice), so it is rebuilt in full",
      };
   }
   const taken = new Set([...columns, ...joined].map((c) => c.column));
   const clash = [PAIR_PARENT_ROW].find((c) => taken.has(c));
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
         childKey,
         childSourceID: join.sourceID as string,
         parentRowColumn: PAIR_PARENT_ROW,
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
               `(only public dimensions of this source are stored), so it ` +
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
      const inlineProblem = inline
         .map((j) => inlineRefusal(j, joins, def.connection))
         .find((r) => r !== undefined);
      if (inlineProblem) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: inlineProblem,
         });
         continue;
      }
      // An inlined join_many is a pair table (P5): at most one per source.
      const fanned = inline.filter(
         (j) => joins.find((x) => x.name === j)?.relationship === "many",
      );
      if (fanned.length > 1) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason:
               `inline= names more than one join_many (${fanned.map(quotedName).join(", ")}); ` +
               "a pair table pairs a parent with the children of one",
         });
         continue;
      }
      // What reads through an inlined join_many has one value per pair, not
      // per parent, so it is computed at read through the served join.
      const columns = materializeColumns(def, {
         joinReads: map.fieldJoinsOf(def.sourceID),
         inline: new Set(inline.filter((j) => !fanned.includes(j))),
         hasJoins: joins.length > 0,
      });
      const ownColumns = new Set(columns.map((c) => c.column));
      const joinedColumns = inline.flatMap((j) =>
         inlinedJoinColumns(map, joins.find((x) => x.name === j) as SourceJoin),
      );
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
            reason: "the source has no public dimension to store",
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
      // A source is planned in the model that declares it (see `declaredHere`),
      // so its name here is the name its author gave it.
      const tableName = name;
      if (persist.name === undefined) {
         if (persist.storage === undefined) {
            refused.push({
               sourceName: name,
               sourceID: def.sourceID,
               reason:
                  "a #(materialize) source built into its own warehouse needs name=: " +
                  `without one its table would be \`${tableName}\`, which may be the very ` +
                  "table it reads",
            });
            continue;
         }
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
      if (fanned.length === 1) {
         const laid = pairRefusal(
            joins.find((x) => x.name === fanned[0]) as SourceJoin,
            def,
            map,
            columns.filter((c) => c.join === undefined),
            joinedColumns,
            persist,
         );
         if ("reason" in laid) {
            refused.push({
               sourceName: name,
               sourceID: def.sourceID,
               reason: laid.reason,
            });
            continue;
         }
         pair = laid.pair;
      }
      const build = buildSourceName(name);
      const checked = inline.filter((j) => !fanned.includes(j));
      const uniquenessChecks = checked.map((j): InlineUniquenessCheck => {
         const join = joins.find((x) => x.name === j) as SourceJoin;
         return {
            join: j,
            keyColumn: join.keyColumn as string,
            queryName: `${build}__unique__${j}`,
            joinedSourceID: join.sourceID as string,
            ...(join.connection ? { connectionName: join.connection } : {}),
         };
      });
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
            : {}),
         ...(inline.length > 0 ? { inline } : {}),
         ...(uniquenessChecks.length > 0 ? { uniquenessChecks } : {}),
         ...(pair ? { pair } : {}),
         ...(declaration.keys.includes("name") ? {} : { nameDefaulted: true }),
      });
   }
   return { plans, refused };
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

/** One generated build source. */
function emitBuildSource(plan: MaterializePlan): string {
   const select = plan.columns
      .map((c) =>
         c.join === undefined
            ? `    ${identifier(c.dimension)}`
            : `    ${identifier(c.column)} is ${identifier(c.join)}.${identifier(c.dimension)}`,
      )
      .join(",\n");
   const body = plan.pair
      ? pairBody(plan.pair, select)
      : `  select:\n${select}`;
   const given = givenReadTerms(plan);
   // malloy-text: each term is the author's own given-reading where:, re-applied as written on its own declaration
   const readTime = given.length
      ? ` extend {\n${given.map((term) => `  where: ${term.code}`).join("\n")}\n}`
      : "";
   const head = `source: ${identifier(plan.buildSourceName)} is ${identifier(importAlias(plan))} -> {`;
   return `${persistLine(plan)}\n${head}\n${body}\n}${readTime}`;
}

/**
 * A pair table's build: a `group_by:` of every stored column (distinct rows,
 * since both keys are among them) that writes the parent-row flag (see the
 * module comment) by ranking each row within its parent key. The rows are
 * written in parent-key order, so a warehouse that keeps insertion order
 * (DuckDB) reads a parent's rows together.
 */
function pairBody(pair: MaterializePair, columns: string): string {
   const parent = identifier(pair.parentKey);
   // malloy-text: columns is emitBuildSource's select list, every name through identifier
   return `  group_by:
${columns}
  calculate:
    ${identifier(pair.parentRowColumn)} is row_number() { partition_by: ${parent} } = 1
  order_by: ${parent}`;
}

/**
 * One inlined join's uniqueness check (P8): the joined source's keys that
 * occur more than once. A build runs it first and fails on any row.
 */
function emitUniquenessCheck(check: InlineUniquenessCheck): string {
   const key = identifier(check.keyColumn);
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
 * The build copy of one planned source's declaring file: `text` with that
 * source's read-time terms replaced by `true` (P2), so a query over the
 * copy's source applies every `where:` but those, or why a term cannot be
 * placed (see {@link termSpans}), which refuses the source rather than guess.
 *
 * One copy per source, never one for the file: a copy with two sources' terms
 * taken out would let the one source's build read the other (a join to it, a
 * query over it) without that other's term, and store its rows for every
 * caller. In a copy of its own, a source that reads another such source still
 * reads that one's given, and the eligibility gate refuses its build
 * (`given_in_persisted_query`).
 */
export function buildCopyText(
   text: string,
   plan: MaterializePlan,
): { copy: string } | { reason: string } {
   const found =
      plan.declaration && givenReadTerms(plan).length > 0
         ? termSpans(text, plan.declaration, givenReadTerms(plan))
         : { reason: NOT_FOUND };
   if ("reason" in found) return found;
   let copy = text;
   for (const span of [...found.spans].sort((a, b) => b.start - a.start)) {
      copy = `${copy.slice(0, span.start)}true${copy.slice(span.end)}`;
   }
   return { copy };
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
 * A plan with read-time terms (P2) is imported from its own build copy,
 * `buildCopyPaths` by sourceID, together with the givens its terms read,
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
   const copyOf = (plan: MaterializePlan) =>
      givenReadTerms(plan).length > 0
         ? buildCopyPaths.get(plan.sourceID)
         : undefined;
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
