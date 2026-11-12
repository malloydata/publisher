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
 * Not yet (each is a later pull request's, and is refused or left out here
 * rather than built wrong):
 * - `inline=` (inlining a join into the stored row) is phase 3's joins pull
 *   request; a declaration naming it is refused.
 * - A materialized extension builds from its own source over the raw tables,
 *   not from its parent's build source (P10): building over the parent needs
 *   the rewriter's re-rooted copy, so it waits for it.
 * - Given-reading and current-time `where:` terms (P2, rolling windows) are
 *   not stripped from the build, and a dimension that reads a given is not
 *   left out of it: a source with either is refused by the same eligibility
 *   gates that refuse it under `#@ persist` today.
 * - A dimension that depends on the query time zone (`hour(ts)`) is stored as
 *   the build computes it; the plan computes it at read instead, which needs
 *   the rewriter's served copy to tell such a column apart.
 * - The build reads the author's file as written, not a copy with its
 *   `#@ persist` lines removed, so a source that also carries its own
 *   `#@ persist` is refused rather than stored twice.
 *
 * A declaration that is not built is reported as a package warning
 * (`materialize-not-built`), so an author sees why, and the source serves live.
 *
 * Every name and value in the generated text goes through `malloy_text`.
 */

import { Annotations } from "@malloydata/malloy";
import { ownLevelNotes, type AnnotationNote } from "../../annotations";
import {
   routeOfNote,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import { annotationValue, identifier, stringLiteral } from "../../malloy_text";
import { parseRouteTag, routeHasKey } from "../../motly";

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

/** Keys `#(materialize)` accepts but a later pull request implements. */
const DEFERRED_KEYS: Readonly<Record<string, string>> = {
   inline: "inlining a join (`inline=`) arrives with the joins pull request",
};

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
      range?: { start: Position; end: Position };
   };
   annotations?: Parameters<typeof ownLevelNotes>[0];
   fields?: unknown[];
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

/** A position in a document, as the IR carries one. */
interface Position {
   line: number;
   character: number;
}

function precedes(a: Position, b: Position): boolean {
   return a.line < b.line || (a.line === b.line && a.character <= b.character);
}

/**
 * The notes the source's OWN statement declares. Malloy copies a base's
 * annotation onto an extension or rename that declares none of its own, by
 * reference, and the IR does not always say which source is the base
 * (`extends` is absent on an extension of a table-backed extension). So a note
 * is read as the source's own only where the note was parsed: in the source's
 * own file, with no other source of the model declared between the note and
 * this source's statement. `source: east_orders is orders extend {…}` carries
 * `orders`' `#(materialize)`, parsed before `orders`, so it is `orders`' alone.
 * A source the IR gives no location keeps every note.
 */
function declaredNotes(
   def: SourceDefLike,
   others: readonly SourceDefLike[],
): AnnotationNote[] {
   const notes = ownLevelNotes(def.annotations);
   const url = def.location?.url;
   const start = def.location?.range?.start;
   if (!start) return notes;
   // Where each other source of this file starts: strictly before this one.
   const earlier = others.flatMap((other) => {
      const otherStart = other.location?.range?.start;
      return other !== def &&
         other.location?.url === url &&
         otherStart &&
         !precedes(start, otherStart)
         ? [otherStart]
         : [];
   });
   return notes.filter((note) => {
      if (!note.at) return true;
      if (note.at.url !== url) return false;
      const noteEnd = note.at.range.end;
      return !earlier.some((otherStart) => precedes(noteEnd, otherStart));
   });
}

/**
 * The source's own `#(materialize)` declaration, read from the notes its own
 * statement declares (not inherited: an extension of a materialized source is
 * planned only when it declares its own). `others` are the model's other
 * sources (see {@link declaredNotes}). Undefined when the source declares none.
 */
export function readMaterializeDeclaration(
   def: SourceDefLike,
   others: readonly SourceDefLike[] = [],
):
   | {
        tag: ReturnType<typeof parseRouteTag>["tag"];
        keys: string[];
        /** Parse errors: a declaration with any is not read at all. */
        errors: string[];
     }
   | undefined {
   const notes = declaredNotes(def, others).filter(
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
   others: readonly SourceDefLike[],
): boolean {
   const notes = declaredNotes(def, others).filter(
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
      ...Object.keys(DEFERRED_KEYS),
   ]);
   const unknown = declaration.keys.filter((key) => !accepted.has(key));
   if (unknown.length > 0) {
      return {
         reason:
            `#(materialize) does not accept ${unknown.map((k) => `\`${k}=\``).join(", ")}; ` +
            `it reads ${[...CARRIED_KEYS, ...CARRIED_GROUPS].join(", ")}`,
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

/** The stored columns of a source: its public atomic dimensions. */
export function materializeColumns(def: SourceDefLike): MaterializeColumn[] {
   const out: MaterializeColumn[] = [];
   for (const raw of def.fields ?? []) {
      const field = raw as FieldLike;
      const dimension =
         typeof field.as === "string"
            ? field.as
            : typeof field.name === "string"
              ? field.name
              : undefined;
      if (dimension === undefined) continue;
      if (field.join !== undefined) continue;
      if (typeof field.type !== "string" || !ATOMIC_TYPES.has(field.type)) {
         continue;
      }
      if (
         field.expressionType === "aggregate" ||
         field.expressionType === "analytic" ||
         field.expressionType === "ungrouped_aggregate"
      ) {
         continue;
      }
      if (isHidden(field)) continue;
      const computed = field.e !== undefined;
      out.push({
         dimension,
         column: dimension,
         computed,
      });
   }
   return out;
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
): {
   plans: MaterializePlan[];
   refused: MaterializeRefusal[];
} {
   const plans: MaterializePlan[] = [];
   const refused: MaterializeRefusal[] = [];
   const others = Object.values(contents).filter(
      (object): object is SourceDefLike =>
         !!object && typeof object === "object",
   );
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
         declaration = readMaterializeDeclaration(def, others);
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
      if (declaresOwnPersist(def, others)) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason:
               "the source also carries its own #@ persist; declare one or the other",
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
      const columns = materializeColumns(def);
      if (columns.length === 0) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: "the source has no public dimension to store",
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
      plans.push({
         sourceName: name,
         sourceID: def.sourceID,
         buildSourceName: buildSourceName(name),
         columns,
         persist,
      });
   }
   return { plans, refused };
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

/** One generated build source. */
function emitBuildSource(plan: MaterializePlan): string {
   const select = plan.columns
      .map((c) => `    ${identifier(c.dimension)}`)
      .join(",\n");
   return `${persistLine(plan)}
source: ${identifier(plan.buildSourceName)} is ${identifier(importAlias(plan))} -> {
  select:
${select}
}`;
}

/** The alias a generated file imports an author source under. */
function importAlias(plan: MaterializePlan): string {
   return `${plan.buildSourceName}__base`;
}

/**
 * The generated build companion for one author model, or undefined when the
 * model declares nothing. `importPath` resolves the author's model from the
 * companion's own URL; every planned source is declared there (see
 * {@link planModelMaterialize}). The experimental flag lives here, so the
 * author's file needs none (D3).
 */
export function generateBuildFile(
   plans: MaterializePlan[],
   importPath: string,
): string | undefined {
   if (plans.length === 0) return undefined;
   const imports = plans
      .map(
         (plan) =>
            `import { ${identifier(importAlias(plan))} is ${identifier(plan.sourceName)} } from ${stringLiteral(importPath)}`,
      )
      .join("\n");
   return `##! experimental.persistence
${imports}

${plans.map((plan) => emitBuildSource(plan)).join("\n\n")}
`;
}
