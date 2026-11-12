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
 * What is stored: every public atomic dimension of `src`. A column the table
 * already holds (a field with no expression of its own) is selected under its
 * own name; a computed dimension is stored as `<name>__s`, so the served copy
 * (the re-root rewriter) can redefine the dimension as a read of that column
 * without its name colliding with the column's. Measures, views and joins are
 * not stored: the served copy keeps them as written, re-bound over the stored
 * columns. A field that is not public (`internal`, `private`) is never stored,
 * matching today's narrowing of a `storage=` table to the public surface.
 *
 * Not yet (each is a later pull request's, and is refused or left out here
 * rather than built wrong):
 * - `inline=` (inlining a join into the stored row) is phase 3's joins pull
 *   request; a declaration naming it is refused.
 * - A materialized extension builds from its own source over the raw tables,
 *   not from its parent's build source (P10): building over the parent needs
 *   the rewriter's re-rooted copy, so it waits for it.
 * - Given-reading and current-time `where:` terms (P2, rolling windows) are
 *   not stripped from the build: a source with one is refused by the same
 *   eligibility gates that refuse it under `#@ persist` today.
 *
 * Every name and value in the generated text goes through `malloy_text`.
 */

import { Annotations } from "@malloydata/malloy";
import { ownLevelNotes } from "../../annotations";
import { routeOfNote } from "../../annotation_reader";
import { annotationValue, identifier, stringLiteral } from "../../malloy_text";
import { parseRouteTag } from "../../motly";

/** The route `#(materialize)` is on. */
export const MATERIALIZE_ROUTE = "materialize";

/** Appended to an author model path to name its generated build companion. */
export const MATERIALIZE_COMPANION_SUFFIX = ".materialize.malloy";

/** The generated build source's name for a materialized source. */
export function buildSourceName(declaredName: string): string {
   return `${declaredName}__mat`;
}

/** The stored column for a computed dimension (see the module doc). */
export function storedDimensionColumn(dimension: string): string {
   return `${dimension}__s`;
}

/** The `#(materialize)` keys this pull request reads. */
const CARRIED_KEYS = [
   "name",
   "storage",
   "refresh",
   "watermark",
   "merge_key",
   "freshness",
   "partition",
] as const;

/** Keys `#(materialize)` accepts but a later pull request implements. */
const DEFERRED_KEYS: Readonly<Record<string, string>> = {
   inline: "inlining a join (`inline=`) arrives with the joins pull request",
};

/** One stored column of a materialized source. */
export interface MaterializeColumn {
   /** The dimension's name on the author's source. */
   dimension: string;
   /** The column the build stores it as. */
   column: string;
   /** Whether the source computes it (stored as `<name>__s`) or reads it from its table. */
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
   /** The `#@ persist` keys carried from the declaration, by name. */
   persist: Partial<Record<(typeof CARRIED_KEYS)[number], string>>;
   /** The declaring file's URL, when the source is imported from another file. */
   declaringURL?: string;
}

/** A declaration the generator could not plan, and why. */
export interface MaterializeRefusal {
   sourceName: string;
   sourceID?: string;
   reason: string;
}

interface SourceDefLike {
   sourceID?: unknown;
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

/**
 * The source's own `#(materialize)` declaration, read from its own notes only
 * (not inherited: an extension of a materialized source declares its own).
 * Undefined when the source carries none.
 */
export function readMaterializeDeclaration(def: SourceDefLike):
   | {
        tag: ReturnType<typeof parseRouteTag>["tag"];
        keys: string[];
     }
   | undefined {
   const notes = ownLevelNotes(def.annotations).filter(
      (note) => routeOfNote(note.text) === MATERIALIZE_ROUTE,
   );
   if (notes.length === 0) return undefined;
   const { tag } = parseRouteTag(
      new Annotations({ notes } as ConstructorParameters<
         typeof Annotations
      >[0]),
      MATERIALIZE_ROUTE,
   );
   return { tag, keys: [...tag.entries()].map(([key]) => key) };
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
         column: computed ? storedDimensionColumn(dimension) : dimension,
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
 * Without `declaredHere` every source with a declaration is planned.
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
   for (const [name, object] of Object.entries(contents).sort(([a], [b]) =>
      a.localeCompare(b),
   )) {
      if (!object || typeof object !== "object") continue;
      const def = object as SourceDefLike;
      if (typeof def.sourceID !== "string") continue;
      const declaredIn = splitSourceID(def.sourceID)?.url;
      if (declaredHere && declaredIn && !declaredHere(declaredIn)) continue;
      let declaration;
      try {
         declaration = readMaterializeDeclaration(def);
      } catch (error) {
         refused.push({
            sourceName: name,
            sourceID: def.sourceID,
            reason: `#(materialize) does not parse: ${error instanceof Error ? error.message : String(error)}`,
         });
         continue;
      }
      if (!declaration) continue;
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
      const persist: MaterializePlan["persist"] = {};
      for (const key of CARRIED_KEYS) {
         const value = declaration.tag.text(key);
         if (value !== undefined) persist[key] = value;
      }
      const declared = splitSourceID(def.sourceID);
      plans.push({
         sourceName: name,
         sourceID: def.sourceID,
         buildSourceName: buildSourceName(declared?.name ?? name),
         columns,
         persist,
         ...(declared ? { declaringURL: declared.url } : {}),
      });
   }
   return { plans, refused };
}

/** One generated build source's `#@ persist` line. */
function persistLine(plan: MaterializePlan): string {
   const keys = CARRIED_KEYS.filter((key) => plan.persist[key] !== undefined)
      .map((key) => `${key}=${annotationValue(plan.persist[key] as string)}`)
      .join(" ");
   return keys ? `#@ persist ${keys}` : "#@ persist";
}

/** One generated build source. */
function emitBuildSource(plan: MaterializePlan, alias: string): string {
   const select = plan.columns
      .map((c) =>
         c.computed
            ? `    ${identifier(c.column)} is ${identifier(c.dimension)}`
            : `    ${identifier(c.dimension)}`,
      )
      .join(",\n");
   return `${persistLine(plan)}
source: ${identifier(plan.buildSourceName)} is ${alias} -> {
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
 * companion's own URL; `importPathForFile` resolves a source declared in
 * another file (as the rollup companion does), so an imported source is built
 * from its declaring file and keeps one identity. The experimental flag lives
 * here, so the author's file needs none (D3).
 */
export function generateBuildFile(
   plans: MaterializePlan[],
   importPath: string,
   importPathForFile?: (declaringURL: string) => string | undefined,
): string | undefined {
   if (plans.length === 0) return undefined;
   const imports = plans
      .map((plan) => {
         const declared = splitSourceID(plan.sourceID);
         const fromDeclaring =
            declared && plan.declaringURL
               ? importPathForFile?.(plan.declaringURL)
               : undefined;
         const importedName =
            fromDeclaring && declared ? declared.name : plan.sourceName;
         return `import { ${identifier(importAlias(plan))} is ${identifier(importedName)} } from ${stringLiteral(fromDeclaring ?? importPath)}`;
      })
      .join("\n");
   return `##! experimental.persistence
${imports}

${plans.map((plan) => emitBuildSource(plan, identifier(importAlias(plan)))).join("\n\n")}
`;
}
