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
 * `j__<field>` columns for readers outside Malloy (the served source does not
 * declare them; it reaches the joined source through the join, as written).
 * An inlined join is checked unique at every build (P8): the plan carries a
 * generated group-by over the joined source on the join's key, and a build
 * whose check finds a key matched twice fails, naming the join.
 *
 * Not yet (each is a later pull request's, and is refused or left out here
 * rather than built wrong):
 * - `inline=` on a join_many or join_cross (pair tables) is phase 4's; a
 *   declaration naming one is refused.
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
 * what it imports. Only a pair on one tier (both colocated, or both in one
 * `storage=` destination) with no incremental keys chains. A `storage=`
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
   routeOfNote,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import { annotationValue, identifier, stringLiteral } from "../../malloy_text";
import { parseRouteTag, routeHasKey } from "../../motly";
import {
   afterModelHeader,
   declarationSpans,
   documentFlagLines,
   dropDimensions,
   inverseRenameBlocks,
} from "../declaration_text";
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
    * {@link dimension} the joined source's field. The served copy does not
    * declare these; it reads the joined source through the join.
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
   /** The joins the declaration inlines (`inline=[…]`), each a join_one. */
   inline?: string[];
   /** One uniqueness check per inlined join (P8). */
   uniquenessChecks?: InlineUniquenessCheck[];
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
    * ({@link extensionOverStoredModel}). Absent for a colocated pair.
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
}

interface SourceDefLike {
   sourceID?: unknown;
   /** Where the source's own statement was parsed. */
   location?: {
      url?: string;
      range?: { start?: { line: number; character: number } };
   };
   connection?: unknown;
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
 * The declaration's `#@ persist` keys by key path, or why one cannot be
 * carried: a key `#(materialize)` does not accept (a misspelt `name=` must not
 * fall back to a default table), or a value of the wrong shape (an array where
 * a string goes would otherwise be dropped silently).
 */
function carriedPersistKeys(
   declaration: NonNullable<ReturnType<typeof readMaterializeDeclaration>>,
): { persist: Record<string, string> } | { reason: string } {
   const accepted = new Set<string>([
      ...CARRIED_KEYS,
      ...CARRIED_GROUPS,
      ...PLANNED_KEYS,
      ...Object.keys(DEFERRED_KEYS),
   ]);
   const unknown = declaration.keys.filter((key) => !accepted.has(key));
   if (unknown.length > 0) {
      return {
         reason:
            `#(materialize) does not accept ${unknown.map((k) => `\`${k}=\``).join(", ")}; ` +
            `it reads ${[...CARRIED_KEYS, ...CARRIED_GROUPS, ...PLANNED_KEYS].join(", ")}`,
      };
   }
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
 * when it can: a join_one on the source's own connection, keyed by one
 * equality on a column of a declared source (what the uniqueness check groups
 * by).
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
   if (join.relationship !== "one") {
      return (
         `inline= names \`${name}\`, a join_${join.relationship}: inlining a ` +
         "join_many or join_cross (a pair table) arrives in a later release"
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
   if (!join.keyColumn) {
      return (
         `inline= names \`${name}\`, whose condition is not one equality on a ` +
         "column of the joined source, so its uniqueness cannot be checked"
      );
   }
   if (!join.sourceID || !splitSourceID(join.sourceID)) {
      return (
         `inline= names \`${name}\`, which joins a source defined in place; ` +
         "declare it as a source so its uniqueness can be checked at build"
      );
   }
   return undefined;
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
         });
         continue;
      }
      if (!declaration) continue;
      if (declaration.errors.length > 0) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: `#(materialize) does not parse: ${declaration.errors.join("; ")}`,
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
      const columns = materializeColumns(def, {
         joinReads: map.fieldJoinsOf(def.sourceID),
         inline: new Set(inline),
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
      const build = buildSourceName(name);
      const uniquenessChecks = inline.map((j): InlineUniquenessCheck => {
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
         ...(inline.length > 0 ? { inline, uniquenessChecks } : {}),
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
 * every declaration between them (`path`). Both must be on one tier (both
 * colocated, or both `storage=` the same destination), carry no incremental
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
      // One tier on both sides: colocated, or one storage destination, whose
      // build reads the parent's table there (see `overStored`).
      if (plan.persist.storage !== parent.persist.storage) continue;
      const incremental = (p: MaterializePlan) =>
         Object.keys(p.persist).some((k) => INCREMENTAL_KEYS.includes(k));
      if (incremental(plan) || incremental(parent)) continue;
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
 * and serves live, which is the `unservable` tier.
 */
export function materializeRefusalFinding(
   refusal: MaterializeRefusal,
   model: string,
): PackageAnnotationFinding {
   return {
      route: MATERIALIZE_ROUTE,
      tier: "unservable",
      code: "materialize-not-built",
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

/** A generated build source's `select:` list: every stored column. */
function buildSourceSelect(plan: MaterializePlan): string {
   return plan.columns
      .map((c) =>
         c.join === undefined
            ? `    ${identifier(c.dimension)}`
            : `    ${identifier(c.column)} is ${identifier(c.join)}.${identifier(c.dimension)}`,
      )
      .join(",\n");
}

/** One generated build source. */
function emitBuildSource(plan: MaterializePlan): string {
   const select = buildSourceSelect(plan);
   const given = givenReadTerms(plan);
   // malloy-text: each term is the author's own given-reading where:, re-applied as written on its own declaration
   const readTime = given.length
      ? ` extend {\n${given.map((term) => `  where: ${term.code}`).join("\n")}\n}`
      : "";
   return `${persistLine(plan)}
source: ${identifier(plan.buildSourceName)} is ${identifier(importAlias(plan))} -> {
  select:
${select}
}${readTime}`;
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
): { copy: string } | { reason: string } {
   const edits = copyEdits(text, plan, chain?.parent);
   if ("reason" in edits) return edits;
   let copy = applyEdits(text, edits.edits);
   if (chain) {
      const at = afterModelHeader(copy);
      const line = `import { ${identifier(chain.parent.buildSourceName)} } from ${stringLiteral(chain.importFrom)}\n`;
      copy = `${copy.slice(0, at)}${line}${copy.slice(at)}`;
   }
   return { copy };
}

/** One splice of a file's text: `[start, end)` replaced by `insert`. */
interface TextEdit {
   start: number;
   end: number;
   insert: string;
}

/**
 * The edits {@link buildCopyText} makes to `text`: the plan's given-reading
 * terms replaced by `true` (P2), and, with `parent`, the parent's
 * declaration re-rooted on its build source (P10).
 */
function copyEdits(
   text: string,
   plan: MaterializePlan,
   parent?: MaterializePlan,
): { edits: TextEdit[] } | { reason: string } {
   const edits: TextEdit[] = [];
   const given = givenReadTerms(plan);
   if (given.length > 0 || !parent) {
      const found =
         plan.declaration && given.length > 0
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
            ? rerootOnBuildSource(text.slice(from, to), parent)
            : undefined;
      if (
         rerooted === undefined ||
         edits.some((e) => e.start < to && from < e.end)
      ) {
         return { reason: PARENT_NOT_REROOTED };
      }
      edits.push({ start: from, end: to, insert: rerooted });
   }
   return { edits };
}

/** `text` with `edits` applied; none may overlap. */
function applyEdits(text: string, edits: readonly TextEdit[]): string {
   let out = text;
   for (const edit of [...edits].sort((a, b) => b.start - a.start)) {
      out = `${out.slice(0, edit.start)}${edit.insert}${out.slice(edit.end)}`;
   }
   return out;
}

/**
 * The text a `storage=` extension table's build compiles in its destination
 * (P10): the parent's declaration and each one on the path to the
 * extension's, edited as the build copy edits them (the parent re-rooted on
 * its build source, the extension's given-reading terms out) and nothing else
 * of the file, so the build reads no source but the parent's stored table.
 * Undefined when the copy's edits cannot be made, or a declaration is not
 * where the plan says.
 */
export function storedChainText(
   text: string,
   plan: MaterializePlan,
   parent: MaterializePlan,
): StoredChain | undefined {
   const over = plan.buildsOver;
   if (!over) return undefined;
   const edits = copyEdits(text, plan, parent);
   if ("reason" in edits) return undefined;
   const statements: string[] = [];
   for (const { range } of [over.declaration, ...over.path]) {
      const from = offsetOf(text, range.start);
      const to = offsetOf(text, range.end);
      if (from < 0 || to < from) return undefined;
      const inside = edits.edits
         .filter((e) => e.start >= from && e.end <= to)
         .map((e) => ({ ...e, start: e.start - from, end: e.end - from }));
      // malloy-text: the author's own declaration, edited as its build copy edits it
      statements.push(`source: ${applyEdits(text.slice(from, to), inside)}`);
   }
   return {
      parentBuildSourceName: parent.buildSourceName,
      flags: documentFlagLines(text),
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
   const select = buildSourceSelect(plan);
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
  select:
${select}
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
 * the extension reads every field it read from the parent. Undefined when the
 * declaration does not parse.
 */
function rerootOnBuildSource(
   declaration: string,
   parent: MaterializePlan,
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
   const base = identifier(parent.buildSourceName);
   return `${name} is ${base}${renames}${kept.text ? ` ${kept.text}` : ""}`;
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
 * A plan with read-time terms (P2), or one built over its parent's build
 * source (P10), is imported from its own build copy,
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
      givenReadTerms(plan).length > 0 || plan.buildsOver
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
