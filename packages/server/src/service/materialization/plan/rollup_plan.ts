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
 *   over the measure's own text, checked against the compiled expression
 *   (governance/ir/measure_aggregates.ts): the leaves must be exactly the
 *   aggregates the compiler computes, or the measure is not stored. The text
 *   is read because the build re-emits each leaf as Malloy text, which the
 *   compiled IR cannot give back. A join-level aggregate is stored only when
 *   the join's key is in the grain, so each joined row falls in one group.
 * - **A3, grain dimensions.** Dimensions the source declares, by name. The
 *   rollup's name carries its declaring model: a digest of the base's
 *   package-relative `sourceID`, so same-named bases in two models never
 *   share a table, and every model that imports the base plans the same name.
 * - **A4, access filters.** The base's entry-point gates, as the governor
 *   walks them (`collectEntryPointGates`: own, inherited, a derivation's
 *   base), so the build and the read-time proof never disagree about which
 *   gates a rollup covers. A gate is exact on a rollup only when every field
 *   it reads is a grain dimension the base reads through no join: a gate whose
 *   fields the grain does not keep, that reads through a join, or a base that
 *   joins a gated source, is a publish warning and the rollup is not built.
 *   A gated base's rollups are built only when a query can read them past
 *   their gate ({@link rollupGateEnforcedAtRead}); otherwise none is (row 21).
 *   An `internal` grain dimension a gate reads is the one hidden field a
 *   rollup stores, redeclared `internal` on the variant.
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
 * Pure apart from the IR reads, which go through the source map (its join
 * readers are `governance/ir/source_graph.ts`'s single walk).
 */

import { Annotations, type ModelDef, type SourceDef } from "@malloydata/malloy";
import { getMaterializeGatedSources } from "../../../config";
import { readPreaggregateAnnotation } from "./preaggregation_annotation";
import { basePersistNamespace } from "./preaggregation_synthesis";
import { collectEntryPointGates } from "../../gate_classification";
import { createHash } from "node:crypto";
import { ownLevelNotes, type AnnotationNote } from "../../annotations";
import {
   declaredFieldNotes,
   type NotedSourceDef,
   type SourceScope,
} from "../../governance/ir/declared_notes";
import {
   PREAGGREGATE_ROUTE_KEYS,
   routeOfNote,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import {
   annotationValue,
   doubleQuotedString,
   identifier,
} from "../../malloy_text";
import { parseRouteTag } from "../../motly";
import { malloyIdentifiers } from "../../query_text";
import {
   classifyWhereTerms,
   currentTimeFields,
   readsCurrentTime,
} from "./persist_dynamic_terms";
import { splitSourceID } from "./persisted_view";
import { isSpliceableNamespace } from "./preaggregation_namespace";
import type { RollupPlan } from "./preaggregation_synthesis";
import {
   measureAggregates,
   type MeasureAggregate,
} from "../../governance/ir/measure_aggregates";
import {
   decomposeMeasure,
   emitMeasureExpr,
   isMeasureRefusal,
   type AggregateLeaf,
   type DecomposedMeasure,
   type MeasureExpr,
   type MeasureRefusal,
   type Reaggregate,
} from "./rollup_measure_grammar";
import type { SourceMap, SourceNode } from "../text/source_map";

/** The route `#(preaggregate)` is on. */
const PREAGGREGATE_ROUTE = "preaggregate";

/** A v1 `#@ preaggregate` (or `#@ -preaggregate`) line. */
const V1_PREAGGREGATE_NOTE = /^\s*#@\s*-?preaggregate\b/;

/** Appended to a declaring model's path to name its rollup build companion. */
export const ROLLUP_COMPANION_SUFFIX = ".preaggregate.malloy";

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
   /**
    * Whether the base declares a row-level gate (`#(access_filter)` or
    * `#(authorize)`): the join-to-rollup leg (A7) reads the rollup through a
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
    * set when the grain is one dimension of the base, the rollup is
    * colocated, and the companion compiled the check. A run builds it only
    * when a join_one in the package is keyed on that dimension (see
    * {@link joinOneKeysOf}); the join-to-rollup leg (A7) reads its outcome.
    */
   keyCheck?: RollupKeyCheck;
   /**
    * The primary key checks of the query sources that read this rollup through
    * a join read as a join_many (see {@link readsAsJoinMany}): set at plan time
    * for each such source the package declares (see {@link parentKeyNeedsOf}),
    * and run by the rollup's build. The join-to-rollup leg (A7) reads their
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
 * A source that may read a rollup through a join keyed on its declared
 * primary key: what {@link readsAsJoinMany} matches against a rollup, and
 * what its check is compiled from.
 */
export interface ParentKeyNeed {
   /** The query source. */
   sourceID: string;
   /** Its name in the model it was found in. */
   name: string;
   /** Its declared primary key, which the join is keyed on. */
   keyColumn: string;
   connectionName: string | undefined;
   /** The joined source's declaration. */
   joinedSourceID: string;
   /** The joined side's column the join is keyed on. */
   joinKeyColumn: string;
   relationship: "one" | "many";
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
 * Whether a `storage=` rollup over a gated base may be built and read. Not
 * until question 14's managed-tier review: a stored rollup puts tenant-keyed
 * aggregates (the gate's field and each tenant's partials) in a storage
 * destination, which the review covers for `#(materialize)` tables and is
 * proposed to cover for rollups too. A colocated rollup stays in the author's
 * warehouse, so it is read past its gate under `materialize.gatedSources`
 * alone; this keeps the two apart. Only a test turns it on
 * ({@link allowGatedStorageRollupsForTests}).
 */
let gatedStorageRollupsReviewed = false;

/**
 * For tests only: let `storage=` rollups over gated bases be built and read
 * (behind `materialize.gatedSources`), as they will be once question 14's
 * review passes. Returns the restore.
 */
export function allowGatedStorageRollupsForTests(on = true): () => void {
   const before = gatedStorageRollupsReviewed;
   gatedStorageRollupsReviewed = on;
   return () => {
      gatedStorageRollupsReviewed = before;
   };
}

/**
 * Whether a rollup at `placement` over a gated base may be read past its
 * base's gate (and so is worth building): `materialize.gatedSources` is on,
 * and the rollup is colocated, or in a storage destination once the
 * managed-tier review allows it.
 */
export function rollupsServeGatedBases(placement: {
   storage?: string;
}): boolean {
   if (!getMaterializeGatedSources()) return false;
   return placement.storage === undefined || gatedStorageRollupsReviewed;
}

/** A `#(preaggregate)` rollup's plan, never a v1 `#@ preaggregate` one. */
function isRollupVariantPlan(plan: unknown): plan is RollupVariantPlan {
   return (
      typeof plan === "object" &&
      plan !== null &&
      (plan as { variant?: unknown }).variant === true
   );
}

/**
 * Whether a planned rollup's base gate is enforced when it is read (R3 for
 * rollups): `govern()` grafts and proves the base's gates on the rollup
 * variant, so the tier checks that refuse a gated build (the colocated and
 * storage `authorize` refusals) do not apply to it. True only for a
 * `#(preaggregate)` rollup (a v1 `#@ preaggregate` rollup's serve shape keeps
 * no gates) over a gated base, whose every gate the planner proved exact on
 * the rollup (table 1: each field a grain dimension, no gated join), at a
 * placement {@link rollupsServeGatedBases} allows. The build plan, the
 * host-instructed build and the serve bind all ask this one question, through
 * `gateEnforcedAtRead`, so a rollup is never built that cannot be read, nor
 * read because it was built.
 */
export function rollupGateEnforcedAtRead(plan: unknown): boolean {
   if (!isRollupVariantPlan(plan)) return false;
   return plan.baseGated && rollupsServeGatedBases(plan);
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
   queryTimezone?: unknown;
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

/** Every compiled source of `map`, once per map: what A1's reader looks among. */
const scopes = new WeakMap<SourceMap, unknown[]>();
function scopeOf(map: SourceMap): unknown[] {
   let scope = scopes.get(map);
   if (!scope) {
      scope = map.nodes().map((n) => n.def);
      scopes.set(map, scope);
   }
   return scope;
}

/**
 * The `#(preaggregate)` notes `def`'s own statement declared on `field` (A1),
 * by the shared rule (`declaredFieldNotes`): a note a source inherited from
 * the source it extends, renames or imports is that source's, and an
 * include-list item's note is the including source's own.
 */
function declaredRollupNotes(
   def: SourceLike,
   field: FieldLike,
   scope: SourceScope,
): AnnotationNote[] {
   return declaredFieldNotes(def as NotedSourceDef, field, scope).filter(
      (n) => routeOfNote(n.text) === PREAGGREGATE_ROUTE,
   );
}

/** One `#(preaggregate)` line, read. */
interface Declaration {
   measure: string;
   grain: string[];
   storage?: string;
   namespace?: string;
   /**
    * Read from a v1 `#@ preaggregate` line (the cutover's G4): a finding v1's
    * own validation already reports with the same code is not reported again.
    */
   v1?: true;
}

/**
 * Read one note as a declaration, or a refusal. Unknown keys are malformed on
 * this route (a new route's key set is closed); so is a grain that is not a
 * list of names, a namespace beside a storage destination, and an `@env.`
 * reference, which is never read from a route Publisher owns.
 */
function readDeclaration(
   note: AnnotationNote,
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
   const unknown = keys.filter((k) => !PREAGGREGATE_ROUTE_KEYS.has(k));
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

/** What planning needs beyond the source map. */
interface RollupPlanContext {
   /** The base's `sourceID` relative to the package (`orders@models/o.malloy`). */
   packageRelativeSourceID(sourceID: string): string;
   /**
    * For tests: whether a gated base's rollups are read past its gate,
    * standing in for {@link rollupsServeGatedBases} at every placement.
    */
   readUnderGates?: boolean;
   /**
    * Whether v1 `#@ preaggregate` lines are read as `#(preaggregate)`
    * declarations (the v1 cutover's G4). Absent: they are.
    */
   readsV1?: boolean;
   /**
    * Whether `#(preaggregate)` declarations are read. Absent: they are. A
    * caller that plans only because the v1 cutover is on passes
    * `getMaterializeEnabled()`, so v2's route stays unread with its switch
    * off.
    */
   readsV2?: boolean;
}

/**
 * Plan the rollups `node` declares (its own `#(preaggregate)` notes only), in
 * coarsest-first order (fewest grain dimensions, then name).
 */
export function planSourceRollups(
   map: SourceMap,
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
   const sources = scopeOf(map);

   // Every declaration this source writes, and a refusal for one on a field
   // that is not a measure (the wrong kind of field: malformed).
   const declarations: Declaration[] = [];
   // `#(preaggregate)` is read unless the caller runs only for the v1
   // cutover (`readsV2: false`), which reads v1's lines below alone.
   const readsV2 = ctx.readsV2 ?? true;
   for (const field of readsV2 ? fields : []) {
      const name = fieldNameOf(field);
      if (name === undefined) continue;
      const notes = declaredRollupNotes(def, field, sources);
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
   // The v1 cutover (G4): a measure's own `#@ preaggregate` lines, read by
   // v1's own reader (a grain per line, `#@ -preaggregate` clearing those
   // above it, `storage=` and `namespace=` bound to their line), are this
   // leg's declarations too, so a v1 rollup is planned, built (under the v2
   // rollup's table name, so it rebuilds once) and served here, and the v1
   // composite is not. A line v1 reports as unusable is left to v1's own
   // validation.
   if (ctx.readsV1 ?? true) {
      // As v1 places it: a grain naming no namespace (and no storage=) is
      // created beside its base's own `#@ persist name=`.
      const inheritedNamespace = basePersistNamespace({
         annotations: def.annotations,
      });
      for (const field of fields) {
         const name = fieldNameOf(field);
         if (name === undefined || !isMeasure(field)) continue;
         const notes = declaredFieldNotes(
            def as NotedSourceDef,
            field,
            sources,
         ).filter((n) => V1_PREAGGREGATE_NOTE.test(n.text));
         if (notes.length === 0) continue;
         const read = readPreaggregateAnnotation({
            name,
            annotations: { notes },
         });
         for (const grain of read.grains) {
            declarations.push({
               measure: name,
               grain: grain.dimensions,
               v1: true,
               ...(grain.storage !== undefined
                  ? { storage: grain.storage }
                  : {}),
               ...(grain.namespace !== undefined
                  ? { namespace: grain.namespace }
                  : grain.storage === undefined &&
                      inheritedNamespace !== undefined
                    ? { namespace: inheritedNamespace }
                    : {}),
            });
         }
      }
   }
   // A source-level declaration names no measure.
   for (const note of readsV2 ? ownLevelNotes(def.annotations) : []) {
      if (routeOfNote(note.text) !== PREAGGREGATE_ROUTE) continue;
      refused.push({
         tier: "malformed",
         code: "preaggregate-not-a-measure",
         message: `#(preaggregate) is on source "${sourceName}"; it goes on a measure, with the dimensions to store it at in its grain=[…]`,
         sourceName,
      });
   }
   if (declarations.length === 0) return { plans: [], refused };

   const joins = new Map(map.joinsOf(node.sourceID).map((j) => [j.name, j]));
   const joinUsage = map.fieldJoinsOf(node.sourceID);
   const relativeID = ctx.packageRelativeSourceID(node.sourceID);
   const declaredName = splitSourceID(node.sourceID)?.name ?? sourceName;
   const gate = gateFieldNames(def, byName, map.model(), joinUsage);
   // S25b: what reads the current time. A rollup stores what its build saw,
   // and a live query re-reads `now` every time, so anything here that reads
   // it (a rolling window on the source or a query-shaped base, a grain
   // dimension, a measure or a join that reads `now`) would fix the window at
   // build time: not built, the measure served live.
   const windowed =
      classifyWhereTerms(def).some(
         (t) =>
            t.dependsOn.kind === "current_time" ||
            t.dependsOn.kind === "given_and_current_time",
      ) || readsCurrentTime((def as { query?: unknown }).query);
   const timeFields = currentTimeFields(def.fields);
   // #1686: a source that sets its own `timezone:` (or inherits one) built
   // its times in that zone, and the base leg reads no query on it from a
   // rollup (`TIME_ZONE`), whose variant is redeclared without the zone. The
   // only reader left is the join leg (A7): through a join from a source in
   // the same zone, colocated or, since #1704, in a `storage=` destination
   // the joining source is stored in too (the leg declines at read when it
   // is not). Anything else is never read.
   const zone =
      typeof def.queryTimezone === "string" && def.queryTimezone !== ""
         ? def.queryTimezone
         : undefined;
   const joinedInZone =
      zone !== undefined &&
      map
         .nodes()
         .some(
            (n) =>
               n.sourceID !== node.sourceID &&
               (n.def as SourceLike).queryTimezone === zone &&
               map
                  .joinsOf(n.sourceID)
                  .some((j) => j.sourceID === node.sourceID),
         );
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
         // v1's validation warns for its own line (preaggregate-hidden-field).
         if (decl.v1) continue;
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
      const timeRead = windowed
         ? "the source's rows read the current time (a `where:` with `now`, a rolling window)"
         : timeFields.has(decl.measure)
           ? "the measure reads the current time (`now`)"
           : decl.grain
                .filter((g) => timeFields.has(g))
                .map(
                   (g) =>
                      `its grain dimension \`${g}\` reads the current time (\`now\`)`,
                )[0];
      if (timeRead) {
         refused.push(
            unservable(
               "preaggregate-current-time",
               decl.measure,
               `${timeRead}, which a rollup would fix at build time while a live query reads it again at every read`,
            ),
         );
         continue;
      }
      if (zone !== undefined && !joinedInZone) {
         refused.push(
            unservable(
               "preaggregate-time-zone",
               decl.measure,
               `the source sets its own time zone (\`timezone: '${zone}'\`), and a query on it is never read from a rollup, whose stored times would be read in another zone, nor through a join (no source in the same zone joins it), so its table would cost a build no query reads`,
            ),
         );
         continue;
      }
      // An `internal` grain dimension a gate reads is stored (A4): the gate
      // needs it on the rollup's rows, and the variant redeclares it
      // `internal`, so no query selects it. Any other hidden field is not.
      const hiddenGrain = decl.grain.find(
         (g) =>
            isHidden(byName.get(g)) &&
            !(
               byName.get(g)?.accessModifier === "internal" &&
               gate.dimensions.includes(g)
            ),
      );
      if (hiddenGrain) {
         if (decl.v1) continue;
         refused.push(
            unservable(
               "preaggregate-hidden-field",
               decl.measure,
               `its grain dimension \`${hiddenGrain}\` is not public, and a rollup would publish it`,
            ),
         );
         continue;
      }
      // Row 21: a gated base's rollup is built only where a query can read
      // it past the gate; elsewhere its table would cost a build no query
      // reads.
      if (
         gate.declared &&
         !(ctx.readUnderGates ?? rollupsServeGatedBases(decl))
      ) {
         refused.push(
            unservable(
               "preaggregate-access-filter",
               decl.measure,
               decl.storage !== undefined && getMaterializeGatedSources()
                  ? "the source has a row-level gate (`#(access_filter)` or `#(authorize)`), and a rollup in a storage destination is not read past a gate yet, so its table would cost a build no query reads; leave out storage= to build it in the source's own warehouse"
                  : "the source has a row-level gate (`#(access_filter)` or `#(authorize)`), and a query on a gated source is not read from a rollup unless PUBLISHER_MATERIALIZE_GATED_SOURCES is on, so its table would cost a build no query reads",
            ),
         );
         continue;
      }
      // A gate on a source the base joins is not the base's to graft: the
      // rollup is not built (A4, fail closed).
      if (gate.declared && gate.joinGated) {
         refused.push(
            unservable(
               "preaggregate-access-filter",
               decl.measure,
               "a source it joins has a row-level gate, which a rollup cannot re-apply to its rows",
            ),
         );
         continue;
      }
      // A4: a gate's fields must be in the grain for its rows to be filtered;
      // a gate that reads through a join (or anything but a dimension) can
      // never be re-applied to a rollup's rows.
      const ungated = [
         ...gate.unkeepable,
         ...gate.dimensions.filter((g) => !decl.grain.includes(g)),
      ];
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
         d = checkedAgainstIR(
            decomposeMeasure(decl.measure, scope),
            measureAggregates(def, decl.measure),
         );
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
      const measureFanOut = new Set<string>();
      for (const leaf of d.leaves) {
         // A join-level sum or count is deduplicated at the joined rows, so
         // its partials add up only across groups with different keys: every
         // other grain dimension must be fixed when it is read. Through more
         // than one join, two keys in the grain can share a joined row.
         if (
            leaf.joinPath.length > 0 &&
            leaf.aggregate !== "min" &&
            leaf.aggregate !== "max"
         ) {
            if (leaf.joinPath.length > 1) {
               joinProblem = `it aggregates through more than one join (\`${leaf.joinPath.join(".")}\`), so a joined row could be counted in two groups`;
               break;
            }
            const key = joins.get(leaf.joinPath[0] as string)?.baseKey;
            for (const g of decl.grain) if (g !== key) measureFanOut.add(g);
         }
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
            const key = joins.get(j)?.baseKey;
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
            fanOutDimensions: [...measureFanOut].sort(byCodepoint),
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
            baseGated: gate.declared,
            internalGrain: decl.grain.filter(
               (g) => byName.get(g)?.accessModifier === "internal",
            ),
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

/**
 * `d`, unless the compiled measure disagrees with it. The grammar reads the
 * measure's text because the build emits Malloy text and the compiler has no
 * printer; the compiler's own expression is the authority on what the measure
 * aggregates. So the leaves the grammar found must be exactly the aggregates
 * the compiled expression computes (each kind at each join, as a multiset):
 * any other aggregate, an aggregate function call, an ungrouped or nested
 * aggregate, or a compiled form that cannot be read makes the measure
 * unreadable, served live and never stored. Fails closed.
 */
export function checkedAgainstIR(
   d: DecomposedMeasure | MeasureRefusal,
   compiled: readonly MeasureAggregate[] | undefined,
): DecomposedMeasure | MeasureRefusal {
   if (isMeasureRefusal(d)) return d;
   // A `min` or `max` is the same over duplicated rows, so the compiler
   // records no join for one (`carriers.fleet.max()` compiles as
   // `max(carriers.fleet)`): it is compared by kind alone. The joins its
   // argument reads are still the grammar's, and each must be keyed in the
   // grain (planSourceRollups).
   const key = (fn: string, at: readonly string[]) =>
      fn === "min" || fn === "max" ? fn : `${fn}@${at.join(".")}`;
   const fromText = d.leaves.map((l) => key(l.aggregate, l.joinPath)).sort();
   const fromIR = (compiled ?? []).map((a) => key(a.fn, a.structPath)).sort();
   if (
      compiled === undefined ||
      fromText.length !== fromIR.length ||
      fromText.some((k, i) => k !== fromIR[i])
   ) {
      return {
         kind: "unreadable",
         detail: `its text reads as ${fromText.join(", ") || "nothing"}, but it compiles to ${compiled === undefined ? "an expression Publisher cannot read" : fromIR.join(", ") || "no aggregate"}`,
      };
   }
   return d;
}

function reaggregateOf(leaf: AggregateLeaf | undefined): Reaggregate {
   return leaf?.aggregate === "min"
      ? "min"
      : leaf?.aggregate === "max"
        ? "max"
        : "sum";
}

/**
 * The base's field names its row-level gates read, as the governor walks
 * them (`collectEntryPointGates`: own, inherited, a derivation's base), so
 * the build and the read-time proof walk the same gates. `unkeepable` holds
 * what a grain can never keep: a join, a measure, a view, a dimension read
 * through a join (a fan-out grain is not fixed by a grafted filter, A6), or a
 * gate the walk could not read. `joinGated`: a source the base joins carries
 * a gate, which is not the base's to graft.
 */
function gateFieldNames(
   def: SourceLike,
   byName: ReadonlyMap<string, FieldLike>,
   modelDef: unknown,
   joinUsage: ReturnType<SourceMap["fieldJoinsOf"]>,
): {
   dimensions: string[];
   unkeepable: string[];
   declared: boolean;
   joinGated: boolean;
} {
   const dimensions = new Set<string>();
   const unkeepable = new Set<string>();
   let entries: ReturnType<typeof collectEntryPointGates>;
   try {
      entries = collectEntryPointGates(
         def as unknown as SourceDef,
         modelDef as ModelDef,
         new Set(),
         true,
      );
   } catch {
      entries = [
         {
            label: "",
            exprs: ["false"],
            route: "authorize",
            selfContained: true,
         },
      ];
      unkeepable.add("(an unreadable gate)");
   }
   for (const entry of entries) {
      // The walk's fail-closed deny: no entry point to graft on.
      if (!entry.struct) unkeepable.add("(an unreadable gate)");
      for (const expr of entry.exprs) {
         for (const id of malloyIdentifiers(expr)) {
            const f = byName.get(id);
            if (!f) continue;
            const usage = joinUsage.get(id);
            const throughJoin =
               joinUsage.has(id) &&
               (usage === undefined || usage.nested || usage.joins.size > 0);
            if (isDimension(f) && !throughJoin) dimensions.add(id);
            else unkeepable.add(id);
         }
      }
   }
   const joinGated = ((def.fields ?? []) as FieldLike[]).some(
      (f) => f.join !== undefined && gatedJoin(f, modelDef),
   );
   return {
      dimensions: [...dimensions].sort(byCodepoint),
      unkeepable: [...unkeepable].sort(byCodepoint),
      declared: entries.length > 0,
      joinGated,
   };
}

/** Whether a joined source carries a row-level gate (fails closed). */
function gatedJoin(join: FieldLike, modelDef: unknown): boolean {
   try {
      return (
         collectEntryPointGates(
            join as unknown as SourceDef,
            modelDef as ModelDef,
            new Set(),
            true,
         ).length > 0
      );
   } catch {
      return true;
   }
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
      const planned = planSourceRollups(map, node, ctx);
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
   // A grain an `internal` gate field is in reads it through
   // `include { public: * }`, which lifts the fields the base hides from a
   // query (A4); the SQL, and so the table's address, is the same.
   const base = identifier(rollupBaseAlias(plan.baseDeclaredName));
   const from =
      (plan.internalGrain ?? []).length > 0
         ? `${base} include { public: * }`
         : base;
   return `${persistLine(plan)}
source: ${identifier(plan.rollupSourceName)} is ${from} -> {
  group_by:
${grain}
  aggregate:
${partials}
}`;
}

/**
 * The Malloy text of a rollup's key check (known limitation 29), or undefined
 * when the rollup's grain is not one dimension or it is not colocated (the
 * join-to-rollup leg reads a colocated rollup only). Run against the rollup
 * companion, which imports the base under {@link rollupBaseAlias}, so it reads
 * exactly the rows the rollup's build groups. A NULL key meets no joining
 * row, so its repeats do not count.
 */
export function rollupKeyCheckQuery(
   plan: RollupVariantPlan,
): string | undefined {
   const [key, ...rest] = plan.grainDimensions;
   if (key === undefined || rest.length > 0 || plan.storage !== undefined) {
      return undefined;
   }
   const k = identifier(key);
   return `run: ${identifier(rollupBaseAlias(plan.baseDeclaredName))} -> {
  where: ${k} != null
  group_by: ${k}
  aggregate: key_rows__check is count()
  having: key_rows__check > 1
  limit: 1
}`;
}

/**
 * Every named source in `map` with a declared primary key and a join, keyed
 * on that key, to a declared source: the query sources whose key the
 * join-to-rollup leg may need checked. A join keyed on another field never
 * takes the leg as a join_many (`keyRepeatRefusal`), so it needs no check.
 */
export function parentKeyNeedsOf(map: SourceMap): ParentKeyNeed[] {
   const out: ParentKeyNeed[] = [];
   for (const node of map.nodes()) {
      const def = node.def as { primaryKey?: unknown; connection?: unknown };
      const keyColumn =
         typeof def.primaryKey === "string" ? def.primaryKey : undefined;
      if (node.name === undefined || keyColumn === undefined) continue;
      for (const join of map.joinsOf(node.sourceID)) {
         if (
            join.relationship === "cross" ||
            join.sourceID === undefined ||
            join.keyColumn === undefined ||
            join.baseKey !== keyColumn
         ) {
            continue;
         }
         out.push({
            sourceID: node.sourceID,
            name: node.name,
            keyColumn,
            connectionName:
               typeof def.connection === "string" ? def.connection : undefined,
            joinedSourceID: join.sourceID,
            joinKeyColumn: join.keyColumn,
            relationship: join.relationship,
         });
      }
   }
   return out;
}

/**
 * A joined source whose declared primary key is not the column a join to it
 * is keyed on: the join-to-rollup leg needs that key checked never NULL and
 * unique before it serves a count, sum or average of the source's own rows
 * through the join. Since malloydata/malloy#3208 live takes those at a joined
 * source over the rows whose key is not NULL (`CASE WHEN key IS NOT NULL`,
 * `COUNT(DISTINCT key)` where the rows fan out), while the rollup, built at
 * the source's own grain, counts and sums every row.
 */
export interface JoinedKeyNeed {
   /** The joined source, the base of its rollups. */
   sourceID: string;
   /** Its name in the model it was found in. */
   name: string;
   /** Its declared primary key. */
   keyColumn: string;
   connectionName: string | undefined;
}

/**
 * Every named source in `map` that a join reaches on a column other than its
 * declared primary key (see {@link JoinedKeyNeed}). A join keyed on the
 * joined source's primary key (`join_one: crew with id`) meets no row whose
 * key is NULL, so it needs no check.
 */
export function joinedKeyNeedsOf(map: SourceMap): JoinedKeyNeed[] {
   const out = new Map<string, JoinedKeyNeed>();
   for (const node of map.nodes()) {
      for (const join of map.joinsOf(node.sourceID)) {
         if (
            join.relationship === "cross" ||
            join.sourceID === undefined ||
            join.keyColumn === undefined ||
            out.has(join.sourceID)
         ) {
            continue;
         }
         const joined = map.node(join.sourceID);
         const def = joined?.def as
            | { primaryKey?: unknown; connection?: unknown }
            | undefined;
         const keyColumn =
            typeof def?.primaryKey === "string" ? def.primaryKey : undefined;
         if (
            joined?.name === undefined ||
            keyColumn === undefined ||
            keyColumn === join.keyColumn
         ) {
            continue;
         }
         out.set(join.sourceID, {
            sourceID: join.sourceID,
            name: joined.name,
            keyColumn,
            connectionName:
               typeof def?.connection === "string" ? def.connection : undefined,
         });
      }
   }
   return [...out.values()];
}

/**
 * Whether the join-to-rollup leg reads rollup `plan` through `need`'s join as
 * a join_many: the join is declared one, or the rollup's grain is finer than
 * the join's key, so the leg re-points it as one (`joinRollupLegs`).
 */
export function readsAsJoinMany(
   plan: RollupVariantPlan,
   need: ParentKeyNeed,
): boolean {
   if (
      plan.baseSourceID !== need.joinedSourceID ||
      !plan.grainDimensions.includes(need.joinKeyColumn)
   ) {
      return false;
   }
   const keyGrain =
      plan.grainDimensions.length === 1 &&
      plan.grainDimensions[0] === need.joinKeyColumn;
   return need.relationship === "many" || !keyGrain;
}

/**
 * The stored-table key checks a build runs (`CompiledBuildPlan.storedKeyChecks`):
 * each of `sourceIDs` (the planned sources, the rollups left out) with a declared
 * primary key, by sourceID, when some `storage=` rollup in `plans` is read
 * through a join keyed on a source's primary key as a join_many. That leg
 * reads the query source's stored table in the destination (#1704), so the
 * key is checked over that table at its own build. Empty otherwise: no scan
 * for nothing.
 */
export function storedKeyChecksOf(
   plans: readonly RollupVariantPlan[],
   needs: readonly ParentKeyNeed[],
   primaryKeyOf: (sourceID: string) => string | undefined,
   sourceIDs: Iterable<string>,
): Record<string, string> {
   const out: Record<string, string> = {};
   const needed = plans.some(
      (plan) =>
         plan.variant === true &&
         plan.storage !== undefined &&
         needs.some((need) => readsAsJoinMany(plan, need)),
   );
   if (!needed) return out;
   for (const sourceID of sourceIDs) {
      const key = primaryKeyOf(sourceID);
      if (key !== undefined) out[sourceID] = key;
   }
   return out;
}

/**
 * The Malloy text of a query source's primary key check, run against the
 * author's model that names it `name`: one row when a value of `keyColumn`
 * is held by more than one row, or is NULL.
 */
export function parentKeyCheckQuery(name: string, keyColumn: string): string {
   return `run: ${identifier(name)} -> {
  group_by: key__check is ${identifier(keyColumn)}
  aggregate: key_rows__check is count()
} -> {
  where: key_rows__check > 1 or key__check = null
  select: key__check
  limit: 1
}`;
}

/** The key {@link joinOneKeysOf} holds for a join_one on `keyColumn` of `sourceID`. */
export function joinOneKey(sourceID: string, keyColumn: string): string {
   return `${sourceID}\u0000${keyColumn}`;
}

/**
 * The join_ones declared in `map` keyed on one column of a declared source,
 * each as {@link joinOneKey}: the rollup key checks a run needs (known
 * limitation 29). A rollup whose base and grain no join_one names is never
 * read through a join_one, so its check would be a scan for nothing.
 */
export function joinOneKeysOf(map: SourceMap): Set<string> {
   const keys = new Set<string>();
   for (const node of map.nodes()) {
      for (const join of map.joinsOf(node.sourceID)) {
         if (
            join.relationship === "one" &&
            join.sourceID !== undefined &&
            join.keyColumn !== undefined
         ) {
            keys.add(joinOneKey(join.sourceID, join.keyColumn));
         }
      }
   }
   return keys;
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
   // `include { public: * }` (an internal gate field in a grain) needs the
   // access-modifier flag; the SQL, and so every table's address, is the same.
   const flags = plans.some((p) => (p.internalGrain ?? []).length > 0)
      ? "##! experimental { access_modifiers persistence }"
      : "##! experimental.persistence";
   return `${flags}
${imports}

${plans.map((p) => emitRollupBuildSource(p)).join("\n\n")}
`;
}
