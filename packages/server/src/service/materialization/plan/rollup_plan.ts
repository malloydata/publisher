// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(preaggregate)` (materialization v2, phase 4): reading the declarations,
 * planning the rollups, and writing their build file.
 *
 *     source: flights is duckdb.table('flights.parquet') extend {
 *       dimension: dep_day is dep_time.day
 *       #(preaggregate) grain=["carrier", "dep_day"]
 *       measure: flight_count is count()
 *     }
 *
 * plans one rollup of `flights` at (carrier, dep_day) storing `flight_count`'s
 * partial, which the build companion declares as
 *
 *     #@ persist
 *     source: flights__rollup__carrier_dep_day__1a2b3c4d is flights__rollup_base -> {
 *       group_by: carrier, dep_day
 *       aggregate: flight_count__partial is count()
 *     }
 *
 * and Malloy's own persistence planner builds on v1's pipeline. The serve side
 * (serve/rollup_variants.ts) offers it to the router as a variant.
 *
 * - **A1, on the measure.** A declaration belongs to the source that wrote it:
 *   a tag at the declaring site is inherited by reference onto every
 *   extension's field, so a note an extension inherits from its base (the same
 *   note, by position) is the base's, and an include-list item's note is the
 *   including source's own. A source therefore has its rollups once, wherever
 *   it is imported.
 * - **A2, qualifying measures.** The measure grammar (rollup_measure_grammar.ts)
 *   over the measure's own text. A join-level aggregate is stored only when
 *   the join's key is in the grain, so each joined row falls in one group.
 * - **A3, grain dimensions.** Dimensions the source declares, by name. The
 *   rollup's name carries its declaring model: a digest of the base's
 *   package-relative `sourceID`, so same-named bases in two models never
 *   share a table, and every model that imports the base plans the same name.
 * - **A4, access filters.** A row-level gate whose fields the grain does not
 *   keep cannot be re-applied to the rollup's rows: a publish warning, and the
 *   rollup is not built.
 * - **A6, fan-out grains.** A grain dimension read through a join_many or
 *   join_cross is recorded as a fan-out dimension; the serve side reads the
 *   rollup only for a query that fixes it at every aggregating level.
 *
 * Outcomes follow the three tiers: a declaration that cannot be read as
 * written, or sits on a field that cannot be merged, is `malformed` (refused
 * at publish); one that is well formed but cannot be honoured here is
 * `unservable` (a coded warning, the measure serves live). Skips are per query,
 * on the serve side.
 *
 * Pure apart from the IR reads, which go through the source map and
 * `governance/ir/source_graph.ts`.
 */

import { Annotations } from "@malloydata/malloy";
import { createHash } from "node:crypto";
import { ownLevelNotes } from "../../annotations";
import {
   payloadsOnRoute,
   routeOfNote,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import {
   fieldJoinUsage,
   joinBaseKey,
   sourceJoins,
} from "../../governance/ir/source_graph";
import {
   annotationValue,
   doubleQuotedString,
   identifier,
} from "../../malloy_text";
import { parseRouteTag } from "../../motly";
import { malloyIdentifiers } from "../../query_text";
import { isSpliceableNamespace } from "./preaggregation_namespace";
import type { RollupPlan } from "./preaggregation_synthesis";
import {
   decomposeMeasure,
   emitMeasureExpr,
   isMeasureRefusal,
   type AggregateLeaf,
   type MeasureExpr,
   type Reaggregate,
} from "./rollup_measure_grammar";
import type { SourceMap, SourceNode } from "./source_map";

/** The route `#(preaggregate)` is on. */
export const PREAGGREGATE_ROUTE = "preaggregate";

/** Appended to a declaring model's path to name its rollup build companion. */
export const ROLLUP_COMPANION_SUFFIX = ".preaggregate.malloy";

/** The keys `#(preaggregate)` reads; anything else is malformed (a closed set). */
const ROLLUP_KEYS: ReadonlySet<string> = new Set([
   "grain",
   "storage",
   "namespace",
]);

/** One stored partial of a measure. */
export interface RollupPartial {
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
}

/**
 * One `#(preaggregate)` rollup: one base, one grain, one placement, one table.
 * A {@link RollupPlan} for the build plan's plumbing (its wire `preaggregate`
 * entry, eligibility and run instructions), with what the variant needs.
 */
export interface RollupVariantPlan extends RollupPlan {
   /** Marks a `#(preaggregate)` rollup apart from a v1 `#@ preaggregate` one. */
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
}

/** Why a declaration was not planned. */
export interface RollupRefusal {
   tier: "malformed" | "unservable";
   code: string;
   message: string;
   /** The base source's name in the planning model. */
   sourceName: string;
   measure?: string;
}

interface FieldLike {
   name?: unknown;
   as?: unknown;
   type?: unknown;
   code?: unknown;
   e?: unknown;
   join?: unknown;
   expressionType?: unknown;
   accessModifier?: unknown;
   annotations?: Parameters<typeof ownLevelNotes>[0] & { inherits?: unknown };
}

interface SourceLike {
   sourceID?: unknown;
   annotations?: Parameters<typeof ownLevelNotes>[0];
   fields?: unknown[];
}

interface NoteLike {
   text: string;
   at?: {
      url: string;
      range: {
         start: { line: number; character: number };
         end: { line: number; character: number };
      };
   };
}

const fieldNameOf = (f: FieldLike): string | undefined =>
   typeof f.as === "string"
      ? f.as
      : typeof f.name === "string"
        ? f.name
        : undefined;

const isMeasure = (f: FieldLike) =>
   f.expressionType === "aggregate" &&
   f.join === undefined &&
   f.type !== "turtle";

const isDimension = (f: FieldLike) =>
   f.join === undefined &&
   f.type !== "turtle" &&
   f.expressionType !== "aggregate" &&
   f.expressionType !== "ungrouped_aggregate" &&
   f.expressionType !== "analytic";

/** Fails closed, as `isHidden` does elsewhere: anything but `public` is hidden. */
const isHidden = (f: FieldLike | undefined) =>
   f?.accessModifier != null && f.accessModifier !== "public";

function noteKey(note: NoteLike): string | undefined {
   const at = note.at;
   return at
      ? `${at.url}#${at.range.start.line}:${at.range.start.character}`
      : undefined;
}

/** Every note on an annotation chain, its own and every inherited level. */
function allNotes(annotations: unknown, depth = 0): NoteLike[] {
   if (!annotations || typeof annotations !== "object" || depth > 64) {
      return [];
   }
   const a = annotations as {
      notes?: NoteLike[];
      blockNotes?: NoteLike[];
      inherits?: unknown;
   };
   return [
      ...(a.blockNotes ?? []),
      ...(a.notes ?? []),
      ...allNotes(a.inherits, depth + 1),
   ];
}

/**
 * The `#(preaggregate)` notes a field of `node` declares itself: its own notes
 * on the route, less any its base source's field of the same name also
 * carries (by position), which the base wrote and this source inherited.
 */
function declaredRollupNotes(
   field: FieldLike,
   baseField: FieldLike | undefined,
): NoteLike[] {
   const own = ownLevelNotes(field.annotations).filter(
      (n) => routeOfNote(n.text) === PREAGGREGATE_ROUTE,
   ) as NoteLike[];
   if (own.length === 0 || !baseField) return own;
   const inherited = new Set(
      allNotes(baseField.annotations)
         .map(noteKey)
         .filter((k): k is string => k !== undefined),
   );
   return own.filter((n) => {
      const key = noteKey(n);
      return key === undefined || !inherited.has(key);
   });
}

/** One `#(preaggregate)` line, read. */
interface Declaration {
   measure: string;
   grain: string[];
   storage?: string;
   namespace?: string;
}

/**
 * Read one note as a declaration, or a refusal. Unknown keys are malformed on
 * this route (a new route's key set is closed); so is a grain that is not a
 * list of names, a namespace beside a storage destination, and an `@env.`
 * reference, which is never read from a route Publisher owns.
 */
function readDeclaration(
   note: NoteLike,
   measure: string,
   sourceName: string,
): Declaration | RollupRefusal {
   const refuse = (code: string, why: string): RollupRefusal => ({
      tier: "malformed",
      code,
      message: `#(preaggregate) on measure "${measure}" of source "${sourceName}" ${why}`,
      sourceName,
      measure,
   });
   if (/@env\./.test(note.text)) {
      return refuse(
         "preaggregate-malformed",
         "reads the environment (`@env.`); write the value itself",
      );
   }
   const { tag, log } = parseRouteTag(
      new Annotations({ notes: [note] } as ConstructorParameters<
         typeof Annotations
      >[0]),
      PREAGGREGATE_ROUTE,
   );
   const errors = log.filter((m) => m.severity === "error");
   if (errors.length > 0) {
      return refuse(
         "preaggregate-malformed",
         `does not parse (${errors.map((e) => e.message).join("; ")})`,
      );
   }
   const keys = [...tag.entries()].map(([k]) => k);
   const unknown = keys.filter((k) => !ROLLUP_KEYS.has(k));
   if (unknown.length > 0) {
      return refuse(
         "preaggregate-unknown-key",
         `has ${unknown.map((k) => `\`${k}=\``).join(", ")}, which it does not read; it reads grain=, storage= and namespace=`,
      );
   }
   const items = tag.array("grain");
   if (!items) {
      return refuse(
         "preaggregate-malformed",
         'needs grain=[…], a list of the dimensions to store it at, such as grain=["category"]',
      );
   }
   const grain = items.map((item) => item.text());
   if (
      grain.length === 0 ||
      grain.some((g) => typeof g !== "string" || g.trim() === "")
   ) {
      return refuse(
         "preaggregate-malformed",
         "has a grain=[…] that names nothing, or holds something other than quoted dimension names",
      );
   }
   const storage = keys.includes("storage") ? tag.text("storage") : undefined;
   const namespace = keys.includes("namespace")
      ? tag.text("namespace")
      : undefined;
   if (keys.includes("storage") && !storage?.trim()) {
      return refuse(
         "preaggregate-malformed",
         "has a storage= that is not a destination name",
      );
   }
   if (keys.includes("namespace") && !namespace?.trim()) {
      return refuse(
         "preaggregate-malformed",
         "has a namespace= that is not a name",
      );
   }
   if (storage !== undefined && namespace !== undefined) {
      return refuse(
         "preaggregate-malformed",
         "names both storage= and namespace=; a destination places its own tables",
      );
   }
   if (namespace !== undefined && !isSpliceableNamespace(namespace)) {
      return refuse(
         "preaggregate-malformed",
         `has namespace="${namespace}", which is not plain dot-separated names`,
      );
   }
   return {
      measure,
      grain: [...new Set((grain as string[]).map((g) => g.trim()))].sort(
         byCodepoint,
      ),
      ...(storage !== undefined ? { storage: storage.trim() } : {}),
      ...(namespace !== undefined ? { namespace } : {}),
   };
}

function byCodepoint(a: string, b: string): number {
   return a < b ? -1 : a > b ? 1 : 0;
}

/** A short digest, for names. */
function digest(parts: readonly string[]): string {
   return createHash("sha256")
      .update(parts.join("\u0000"))
      .digest("hex")
      .slice(0, 8);
}

/** How much of the grain goes in the readable part of the name. */
const NAME_SLUG_LIMIT = 40;

/**
 * A rollup's name: its base's declared name, its grain, and a digest of the
 * base's package-relative `sourceID`, grain and placement. The digest carries
 * the declaring model, so two same-named bases in two models never share a
 * table, and every model that imports the base names it the same. Names are
 * for display and for the self-assigned table name; nothing keys on them.
 */
export function rollupName(
   baseDeclaredName: string,
   packageRelativeSourceID: string,
   grain: readonly string[],
   placement: { storage?: string; namespace?: string },
): string {
   const slug = grain.join("_").replace(/[^A-Za-z0-9_]/g, "_");
   return `${baseDeclaredName}__rollup__${slug.slice(0, NAME_SLUG_LIMIT)}__${digest(
      [
         packageRelativeSourceID,
         ...grain,
         `storage=${placement.storage ?? ""}`,
         `namespace=${placement.namespace ?? ""}`,
      ],
   )}`;
}

/** A Malloy `sourceID`'s declared name and declaring file, or undefined. */
export function splitSourceID(
   sourceID: string,
): { name: string; url: string } | undefined {
   const match = /^(.+)@([a-z][a-z0-9+.-]*:.*)$/i.exec(sourceID);
   return match ? { name: match[1], url: match[2] } : undefined;
}

/** What planning needs beyond the source map. */
export interface RollupPlanContext {
   /** The base's `sourceID` relative to the package (`orders@models/o.malloy`). */
   packageRelativeSourceID(sourceID: string): string;
}

/**
 * Plan the rollups `node` declares (its own `#(preaggregate)` notes only), in
 * coarsest-first order (fewest grain dimensions, then name).
 */
export function planSourceRollups(
   node: SourceNode,
   ctx: RollupPlanContext,
): { plans: RollupVariantPlan[]; refused: RollupRefusal[] } {
   const def = node.def as SourceLike;
   const sourceName = node.name ?? splitSourceID(node.sourceID)?.name ?? "";
   const refused: RollupRefusal[] = [];
   const fields = (def.fields ?? []) as FieldLike[];
   const byName = new Map<string, FieldLike>();
   for (const f of fields) {
      const n = fieldNameOf(f);
      if (n !== undefined) byName.set(n, f);
   }
   const baseFields = new Map<string, FieldLike>();
   if (node.base && node.base.hop !== "query") {
      for (const f of (node.base.def?.fields ?? []) as FieldLike[]) {
         const n = fieldNameOf(f);
         if (n !== undefined) baseFields.set(n, f);
      }
   }

   // Every declaration this source writes, and a refusal for one on a field
   // that is not a measure (the wrong kind of field: malformed).
   const declarations: Declaration[] = [];
   for (const field of fields) {
      const name = fieldNameOf(field);
      if (name === undefined) continue;
      const notes = declaredRollupNotes(field, baseFields.get(name));
      if (notes.length === 0) continue;
      if (!isMeasure(field)) {
         refused.push({
            tier: "malformed",
            code: "preaggregate-not-a-measure",
            message: `#(preaggregate) is on "${name}" of source "${sourceName}", which is not a measure; it goes on the measure to store, and the dimensions to store it at go in its grain=[…]`,
            sourceName,
            measure: name,
         });
         continue;
      }
      for (const note of notes) {
         const read = readDeclaration(note, name, sourceName);
         if ("tier" in read) refused.push(read);
         else declarations.push(read);
      }
   }
   // A source-level declaration names no measure.
   for (const note of ownLevelNotes(def.annotations)) {
      if (routeOfNote(note.text) !== PREAGGREGATE_ROUTE) continue;
      refused.push({
         tier: "malformed",
         code: "preaggregate-not-a-measure",
         message: `#(preaggregate) is on source "${sourceName}"; it goes on a measure, with the dimensions to store it at in its grain=[…]`,
         sourceName,
      });
   }
   if (declarations.length === 0) return { plans: [], refused };

   const joins = new Map(sourceJoins(def).map((j) => [j.name, j]));
   const joinUsage = fieldJoinUsage(def);
   const relativeID = ctx.packageRelativeSourceID(node.sourceID);
   const declaredName = splitSourceID(node.sourceID)?.name ?? sourceName;
   const gateFields = gateFieldNames(def, byName);
   const scope = {
      measureCode: (n: string) => {
         const f = byName.get(n);
         return f && isMeasure(f) && typeof f.code === "string"
            ? f.code
            : undefined;
      },
      isJoin: (n: string) => joins.has(n),
   };
   /** The joins a field path reads, or undefined when unknown. */
   const joinsRead = (path: readonly string[]): string[] | undefined => {
      const head = path[0];
      if (head === undefined) return [];
      if (joins.has(head)) return [head];
      const f = byName.get(head);
      if (!f || isMeasure(f)) return [];
      const usage = joinUsage.get(head);
      if (usage === undefined) return joinUsage.has(head) ? undefined : [];
      if (usage.nested) return undefined;
      return [...usage.joins];
   };
   const unservable = (
      code: string,
      measure: string,
      why: string,
   ): RollupRefusal => ({
      tier: "unservable",
      code,
      message: `#(preaggregate) on measure "${measure}" of source "${sourceName}" is not built, so the measure is served live: ${why}.`,
      sourceName,
      measure,
   });

   // Group by grain and placement: ten measures at one grain are one table.
   const groups = new Map<
      string,
      { decl: Declaration; measures: RollupMeasure[] }
   >();
   const decomposed = new Map<string, ReturnType<typeof decomposeMeasure>>();
   for (const decl of declarations) {
      const field = byName.get(decl.measure) as FieldLike;
      if (isHidden(field)) {
         refused.push(
            unservable(
               "preaggregate-hidden-field",
               decl.measure,
               "the measure is not public, and a rollup would publish it",
            ),
         );
         continue;
      }
      const grainProblem = decl.grain
         .map((g) => {
            const f = byName.get(g);
            if (g.includes(".")) {
               return `the grain names \`${g}\`, a path; declare a dimension on the source that reads it, and name that`;
            }
            if (!f || !isDimension(f)) {
               return `the grain names \`${g}\`, which is not a dimension of the source`;
            }
            return undefined;
         })
         .find((p) => p !== undefined);
      if (grainProblem) {
         refused.push({
            tier: "malformed",
            code: "preaggregate-unknown-grain",
            message: `#(preaggregate) on measure "${decl.measure}" of source "${sourceName}": ${grainProblem}`,
            sourceName,
            measure: decl.measure,
         });
         continue;
      }
      const hiddenGrain = decl.grain.find((g) => isHidden(byName.get(g)));
      if (hiddenGrain) {
         refused.push(
            unservable(
               "preaggregate-hidden-field",
               decl.measure,
               `its grain dimension \`${hiddenGrain}\` is not public, and a rollup would publish it`,
            ),
         );
         continue;
      }
      // A4: a gate's fields must be in the grain for its rows to be filtered.
      const ungated = gateFields.filter((g) => !decl.grain.includes(g));
      if (ungated.length > 0) {
         refused.push(
            unservable(
               "preaggregate-access-filter",
               decl.measure,
               // malloy-text: a warning message for a person, not Malloy
               `the source's access filter reads ${ungated.map((g) => `\`${g}\``).join(", ")}, which the grain does not keep, so the rollup's rows could not be filtered per caller`,
            ),
         );
         continue;
      }
      let d = decomposed.get(decl.measure);
      if (!d) {
         d = decomposeMeasure(decl.measure, scope);
         decomposed.set(decl.measure, d);
      }
      if (isMeasureRefusal(d)) {
         refused.push(
            d.kind === "not_mergeable"
               ? {
                    tier: "malformed",
                    code: "preaggregate-not-mergeable",
                    message: `#(preaggregate) on measure "${decl.measure}" of source "${sourceName}": its partials cannot be merged (${d.detail}). Store sum, count, min and max measures, and derive the rest from them`,
                    sourceName,
                    measure: decl.measure,
                 }
               : unservable(
                    "preaggregate-unreadable-measure",
                    decl.measure,
                    `its definition is outside what #(preaggregate) reads (${d.detail})`,
                 ),
         );
         continue;
      }
      // A2: a join-level aggregate merges only when each joined row falls in
      // one group, which the join's key in the grain guarantees.
      let joinProblem: string | undefined;
      for (const leaf of d.leaves) {
         const read = new Set<string>();
         let unknown = false;
         for (const path of [leaf.joinPath, ...leaf.reads]) {
            const js = joinsRead(path);
            if (js === undefined) unknown = true;
            else for (const j of js) read.add(j);
         }
         if (unknown) {
            joinProblem =
               "it reads a field whose joins Publisher cannot trace, so a joined row could be counted in two groups";
            break;
         }
         for (const j of read) {
            const key = joinBaseKey(def, j);
            if (!key || !decl.grain.includes(key)) {
               joinProblem = key
                  ? `it aggregates through join \`${j}\`, whose key \`${key}\` is not in the grain, so a joined row could be counted in two groups`
                  : `it aggregates through join \`${j}\`, which is not keyed on one field of the source, so a joined row could be counted in two groups`;
               break;
            }
         }
         if (joinProblem) break;
      }
      if (joinProblem) {
         refused.push(
            unservable("preaggregate-join-level", decl.measure, joinProblem),
         );
         continue;
      }
      const columns =
         d.leaves.length === 1
            ? [`${decl.measure}__partial`]
            : d.leaves.map((_, i) => `${decl.measure}__partial${i}`);
      const clash = columns.find((c) => byName.has(c));
      if (clash) {
         refused.push(
            unservable(
               "preaggregate-name-clash",
               decl.measure,
               `the source already has a field named \`${clash}\`, the column its partial is stored as`,
            ),
         );
         continue;
      }
      const key = [
         ...decl.grain,
         `storage=${decl.storage ?? ""}`,
         `namespace=${decl.namespace ?? ""}`,
      ].join("\u0000");
      const group = groups.get(key) ?? { decl, measures: [] };
      if (!group.measures.some((m) => m.name === decl.measure)) {
         group.measures.push({
            name: decl.measure,
            partials: d.leaves.map((leaf, i) => ({
               column: columns[i] as string,
               leaf,
            })),
            merged: d.merged(columns),
         });
      }
      groups.set(key, group);
   }

   const plans: RollupVariantPlan[] = [...groups.values()].map(
      ({ decl, measures }) => {
         const name = rollupName(declaredName, relativeID, decl.grain, decl);
         const sorted = [...measures].sort((a, b) =>
            byCodepoint(a.name, b.name),
         );
         return {
            variant: true,
            baseSourceName: sourceName,
            baseDeclaredName: declaredName,
            baseSourceID: node.sourceID,
            rollupSourceName: name,
            grainDimensions: decl.grain,
            ...(decl.storage !== undefined ? { storage: decl.storage } : {}),
            ...(decl.namespace !== undefined
               ? { namespace: decl.namespace }
               : {}),
            // The v1 shape the build plan's wire entry reads (measure names).
            measures: sorted.map((m) => ({
               name: m.name,
               partialName: m.partials[0]?.column ?? "",
               reaggregate: reaggregateOf(m.partials[0]?.leaf),
            })),
            rollupMeasures: sorted,
            fanOutDimensions: decl.grain.filter((g) => {
               const usage = joinUsage.get(g);
               if (usage === undefined) return joinUsage.has(g);
               return (
                  usage.nested ||
                  [...usage.joins].some(
                     (j) => joins.get(j)?.relationship !== "one",
                  )
               );
            }),
         };
      },
   );
   plans.sort(
      (a, b) =>
         a.grainDimensions.length - b.grainDimensions.length ||
         byCodepoint(a.rollupSourceName, b.rollupSourceName),
   );
   return { plans, refused };
}

function reaggregateOf(leaf: AggregateLeaf | undefined): Reaggregate {
   return leaf?.aggregate === "min"
      ? "min"
      : leaf?.aggregate === "max"
        ? "max"
        : "sum";
}

/**
 * The source's own field names a row-level gate on it reads
 * (`#(access_filter)`, `#(authorize)`), its inherited gates included.
 */
function gateFieldNames(
   def: SourceLike,
   byName: ReadonlyMap<string, FieldLike>,
): string[] {
   const texts = allNotes(def.annotations).map((n) => n.text);
   const out = new Set<string>();
   for (const route of ["access_filter", "authorize"]) {
      for (const payload of payloadsOnRoute(texts, route)) {
         for (const id of malloyIdentifiers(payload)) {
            const f = byName.get(id);
            if (f && isDimension(f)) out.add(id);
         }
      }
   }
   return [...out].sort(byCodepoint);
}

/**
 * Plan every rollup a compiled model declares. Only a source DECLARED in this
 * model is planned (`declaredHere` decides from its `sourceID`'s file): a
 * source another model imports or re-exports is that file's to build, so a
 * rollup has one build source, one address and one table however many models
 * import its base. Without `declaredHere`, every source is planned.
 */
export function planModelRollups(
   map: SourceMap,
   ctx: RollupPlanContext,
   declaredHere?: (declaringURL: string) => boolean,
): { plans: RollupVariantPlan[]; refused: RollupRefusal[] } {
   const plans: RollupVariantPlan[] = [];
   const refused: RollupRefusal[] = [];
   const nodes = [...map.nodes()].sort((a, b) =>
      byCodepoint(a.sourceID, b.sourceID),
   );
   for (const node of nodes) {
      const declaredIn = splitSourceID(node.sourceID)?.url;
      if (declaredHere && declaredIn && !declaredHere(declaredIn)) continue;
      const planned = planSourceRollups(node, ctx);
      plans.push(...planned.plans);
      refused.push(...planned.refused);
   }
   return { plans, refused };
}

/** A refusal as a package finding. */
export function rollupRefusalFinding(
   refusal: RollupRefusal,
   model: string,
): PackageAnnotationFinding {
   return {
      route: PREAGGREGATE_ROUTE,
      tier: refusal.tier,
      code: refusal.code,
      message: refusal.message,
      subject: refusal.measure
         ? `${refusal.sourceName}.${refusal.measure}`
         : refusal.sourceName,
      model,
   };
}

// ---------------------------------------------------------------------------
// The build file
// ---------------------------------------------------------------------------

/** The alias a build file imports a base under. */
export function rollupBaseAlias(baseDeclaredName: string): string {
   return `${baseDeclaredName}__rollup_base`;
}

/** One rollup's `#@ persist` line. */
function persistLine(plan: RollupVariantPlan): string {
   if (plan.storage !== undefined) {
      return `#@ persist storage=${annotationValue(plan.storage)}`;
   }
   if (plan.namespace !== undefined) {
      return `#@ persist name=${annotationValue(`${plan.namespace}.${plan.rollupSourceName}`)}`;
   }
   return "#@ persist";
}

/** One rollup's build source. */
function emitRollupBuildSource(plan: RollupVariantPlan): string {
   const grain = plan.grainDimensions
      .map((g) => `    ${identifier(g)}`)
      .join("\n");
   // malloy-text: each leaf re-emitted from the measure grammar's parse, every name and value through malloy_text
   const partials = plan.rollupMeasures
      .flatMap((m) => m.partials)
      .map(
         (p) =>
            `    ${identifier(p.column)} is ${emitMeasureExpr(p.leaf.expr)}`,
      )
      .join("\n");
   return `${persistLine(plan)}
source: ${identifier(plan.rollupSourceName)} is ${identifier(rollupBaseAlias(plan.baseDeclaredName))} -> {
  group_by:
${grain}
  aggregate:
${partials}
}`;
}

/**
 * The generated build companion for one declaring model, or undefined when it
 * declares no rollup. Every planned base is declared in that model, so each is
 * imported from it by `importPath`. The experimental flag lives here, so the
 * author's file needs none.
 */
export function generateRollupBuildFile(
   plans: readonly RollupVariantPlan[],
   importPath: string,
): string | undefined {
   if (plans.length === 0) return undefined;
   const bases = [...new Set(plans.map((p) => p.baseDeclaredName))].sort(
      byCodepoint,
   );
   const imports = bases
      .map(
         (base) =>
            `import { ${identifier(rollupBaseAlias(base))} is ${identifier(base)} } from ${doubleQuotedString(importPath)}`,
      )
      .join("\n");
   return `##! experimental.persistence
${imports}

${plans.map((p) => emitRollupBuildSource(p)).join("\n\n")}
`;
}
