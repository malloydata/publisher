// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(preaggregate)`: reading the declarations,
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
 *   A hidden grain dimension is stored, redeclared with its modifier on
 *   the variant (a `private` one only when the base itself declares it,
 *   read through its build copy).
 * - **Given-reading terms.** A `where:` of the base's own that reads a given,
 *   over grain dimensions only, is left out of the build (its build copy
 *   replaces it by `true`) and written again on the variant, where each
 *   caller's givens bind it; any other is refused with the reason the one
 *   classifier gives it on `#(materialize)` (`inherited_where_with_given`,
 *   `given_in_persisted_query`).
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

import { refusalKey } from "./refusal_key";
import { keyCheckPipeline } from "../text/key_check_text";
import {
   primaryKeyOf,
   recordedTimezoneOf,
   timezoneOf,
} from "../../governance/ir/compiled_model";
import type { ModelDef, SourceDef } from "@malloydata/malloy";
import { gateSwitches } from "../../../config";
import { tableKeepsGates } from "../domain/gate_policy";
import {
   gateRefusalReason,
   type GateRefusalReason,
} from "../domain/gate_refusal";
import { byReadCost } from "../domain/read_cost";
import { readPreaggregateAnnotation } from "./preaggregation_annotation";
import { hasFanoutJoin } from "./preaggregation_validation";
import { basePersistNamespace } from "./preaggregation_synthesis";
import { collectEntryPointGates } from "../../gate_classification";
import { createHash } from "node:crypto";
import {
   ownLevelNotes,
   type AnnotationNote,
   type AnnotationsDef,
} from "../../annotations";
import {
   declaredFieldNotes,
   declaredNotes,
   type NotedSourceDef,
   type SourceScope,
} from "../../governance/ir/declared_notes";
import { canonicalAuthorizeRoute } from "../../authorize_routes";
import {
   PREAGGREGATE_INVALID_CODE,
   PREAGGREGATE_NAMESPACE_DEPRECATED_CODE,
   PREAGGREGATE_NOT_BUILT_CODE,
   PREAGGREGATE_ROUTE_KEYS,
   routeOfNote,
   type PackageAnnotationFinding,
   type RefusalKey,
} from "../../annotation_reader";
import type { RefusedReason } from "../domain/types";
import {
   destinationOf,
   placementOf,
   readNotes,
   readNotesIfAny,
   readRouteIfAny,
} from "./declaration";
import { Annotations } from "@malloydata/malloy";
import type { DeclaredFreshness } from "../domain/freshness";
import {
   mergeRollupFreshness,
   readFreshnessGroup,
   rollupSourceFreshness,
} from "./freshness_declaration";
import { tagFreshnessLayer } from "./plan_layers";
import { malloyIdentifiers } from "../../query_text";
import {
   classifyWhereTerms,
   currentTimeFields,
   readsCurrentTime,
} from "./persist_dynamic_terms";
import { declarationOf, readTimeTermsOf } from "./materialize_read_time";
import type { SourceDefLike } from "./build_file_generator";
import type { GivenReadTerm, TermUse } from "../domain/read_time_terms";
import {
   namespaceRewrite,
   readRollupContainer,
} from "./preaggregation_namespace";
import {
   measureAggregates,
   type MeasureAggregate,
} from "../../governance/ir/measure_aggregates";
import {
   decomposeMeasure,
   isMeasureRefusal,
   type DecomposedMeasure,
   type MeasureRefusal,
} from "./rollup_measure_grammar";
import type { AggregateLeaf, Reaggregate } from "../domain/rollup_measures";
import {
   splitSourceID,
   type ParentKeyCheck,
   type RollupKeyCheck,
   type RollupMeasure,
   type RollupVariantPlan,
} from "../domain/table_plans";
import {
   ROLLUP_COMPANION_SUFFIX,
   rollupBaseAlias,
   rollupCompanionURLOf,
} from "../text/read_text";

export type { ParentKeyCheck, RollupKeyCheck };
import type { SourceMap, SourceNode } from "../text/source_map";

/** The route `#(preaggregate)` is on. */
const PREAGGREGATE_ROUTE = "preaggregate";

/** A v1 `#@ preaggregate` (or `#@ -preaggregate`) line. */
const V1_PREAGGREGATE_NOTE = /^\s*#@\s*-?preaggregate\b/;

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
 * Whether a rollup at `placement` over a gated base may be read past its
 * base's gate (and so is worth building), by the gate policy: colocated,
 * under `materialize.gatedSources` or, with it off, under G3 when every gate
 * is the base's `own`; in a storage destination once the managed-tier review
 * allows it.
 */
function rollupsServeGatedBases(
   placement: { storage?: string },
   own: boolean,
): boolean {
   return tableKeepsGates(
      {
         kind: "rollup",
         tier: placement.storage === undefined ? "colocated" : "storage",
         v1: false,
         baseGateProven: true,
         gate: baseGateOf(own),
      },
      gateSwitches(),
   );
}

/**
 * A rollup base's gates as the gate policy reads them: row-level, and
 * `attributed` when every one is the base's own (G3 for rollups).
 */
function baseGateOf(own: boolean): {
   rowLevel: boolean;
   attributed: boolean;
   inheritedRowFilter: boolean;
} {
   return { rowLevel: true, attributed: own, inheritedRowFilter: !own };
}

/**
 * A rollup the rollup leg planned: a `#(preaggregate)` one, or a v1
 * `#@ preaggregate` line read as one.
 */
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
 * storage `authorize` refusals) do not apply to it. True only for a rollup
 * the rollup leg planned (a `#(preaggregate)` one, or a v1 `#@ preaggregate`
 * line read as one) over a gated base, whose every gate the
 * planner proved exact on the rollup (table 1: each field a grain dimension,
 * no gated join), at a
 * placement the gate policy allows (`domain/gate_policy.ts`). The build plan, the
 * host-instructed build and the serve bind all ask this one question, through
 * `gateEnforcedAtRead`, so a rollup is never built that cannot be read, nor
 * read because it was built.
 */
export function rollupGateEnforcedAtRead(plan: unknown): boolean {
   if (typeof plan !== "object" || plan === null) return false;
   const variant = isRollupVariantPlan(plan);
   return tableKeepsGates(
      {
         kind: "rollup",
         tier:
            (plan as { storage?: unknown }).storage === undefined
               ? "colocated"
               : "storage",
         v1: !variant,
         baseGateProven: variant && plan.baseGated === true,
         gate: baseGateOf(variant && plan.baseGateOwn === true),
      },
      gateSwitches(),
   );
}

/**
 * Why a declaration was not planned: an invalid one (`malformed`, which fails
 * the load), or a valid one refused alone with its refusal record; or one
 * planned with a warning naming its rewrite (`deprecated`).
 */
export type RollupRefusal = {
   code: string;
   message: string;
   /** The base source's name in the planning model. */
   sourceName: string;
   measure?: string;
} & (
   | { tier: "malformed" }
   | { tier: "deprecated" }
   | { tier: "unservable"; refused: RefusalKey }
);

/**
 * The refusal record of a rollup declaration refused before its rollup is
 * planned: named by the measure's path on its base (the base alone for a
 * refusal of every rollup on it), and keyed, in the base file's rollup
 * companion where no author source is declared, by that name and the table
 * it would have had (its grain and destination), so two refused
 * declarations on one measure are two records.
 */
export function measureRefusalKey(
   baseSourceID: string,
   baseName: string,
   measure: string | undefined,
   reason: RefusedReason,
   table?: { grain: readonly string[]; storage?: string },
): RefusalKey {
   const url = splitSourceID(baseSourceID)?.url ?? "";
   const name = measure !== undefined ? `${baseName}.${measure}` : baseName;
   const at = table
      ? `[${[...table.grain].sort().join(",")}${table.storage !== undefined ? ` in ${table.storage}` : ""}]`
      : "";
   return refusalKey(
      `${name}${at}@${rollupCompanionURLOf(url)?.href ?? `${url}${ROLLUP_COMPANION_SUFFIX}`}`,
      name,
      reason,
      "preaggregate",
   );
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

/** Whether `f` is declared inside the base's own statement (its location). */
function declaredWithin(
   f: FieldLike,
   facts: Pick<RollupFacts, "declaration">,
): boolean {
   const at = (f as { location?: TermUse }).location;
   const within = facts.declaration;
   if (!at?.range || !within || at.url !== within.url) return false;
   const before = (
      a: { line: number; character: number },
      b: { line: number; character: number },
   ) => a.line < b.line || (a.line === b.line && a.character <= b.character);
   return (
      before(within.range.start, at.range.start) &&
      before(at.range.end, within.range.end)
   );
}

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
   /**
    * The schema or dataset its table is created in: its `name=`'s container,
    * its deprecated `namespace=`, or its base's `name=` container.
    */
   namespace?: string;
   /** {@link namespace} was written as the deprecated `namespace=`. */
   deprecatedNamespace?: true;
   /** Its `name=`'s leaf, the readable prefix of its table's name. */
   prefix?: string;
   /** Its `freshness` group (freshness_declaration.ts). */
   freshness?: DeclaredFreshness;
   /**
    * Read from a v1 `#@ preaggregate` line: a finding v1's
    * own validation already reports with the same code is not reported again.
    */
   v1?: true;
}

/**
 * Read one note as a declaration, or a refusal. Unknown keys are malformed on
 * this route (the key set is closed); so is a grain that is not a list of
 * names, a `name=` with no container to place its table in, a container
 * beside a storage destination, and an `@env.` reference,
 * which is never read from a route Publisher owns. Each fails the load.
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
         PREAGGREGATE_INVALID_CODE,
         "reads the environment (`@env.`); write the value itself",
      );
   }
   const { tag, log } = readNotes([note], PREAGGREGATE_ROUTE);
   const errors = log.filter((m) => m.severity === "error");
   if (errors.length > 0) {
      return refuse(
         "preaggregate-unparseable",
         `does not parse (${errors.map((e) => e.message).join("; ")})`,
      );
   }
   const keys = [...tag.entries()].map(([k]) => k);
   const unknown = keys.filter((k) => !PREAGGREGATE_ROUTE_KEYS.has(k));
   if (unknown.length > 0) {
      return refuse(
         "preaggregate-unknown-key",
         `has ${unknown.map((k) => `\`${k}=\``).join(", ")}, which it does not read; it reads grain=, storage=, name= and freshness.window= / freshness.fallback=`,
      );
   }
   const fresh = readFreshnessGroup(tag, ["freshness"]);
   if ("unknownKeys" in fresh) {
      return refuse(
         "preaggregate-unknown-key",
         `has ${fresh.unknownKeys.map((k) => `\`freshness.${k}=\``).join(", ")}, which it does not read; use freshness.window= or freshness.fallback=`,
      );
   }
   if ("invalid" in fresh)
      return refuse(PREAGGREGATE_INVALID_CODE, fresh.invalid);
   const items = tag.array("grain");
   if (!items) {
      return refuse(
         PREAGGREGATE_INVALID_CODE,
         'needs grain=[…], a list of the dimensions to store it at, such as grain=["category"]',
      );
   }
   const grain = items.map((item) => item.text());
   if (
      grain.length === 0 ||
      grain.some((g) => typeof g !== "string" || g.trim() === "")
   ) {
      return refuse(
         PREAGGREGATE_INVALID_CODE,
         "has a grain=[…] that names nothing, or holds something other than quoted dimension names",
      );
   }
   const placed = placementOf(tag);
   if ("refused" in placed) {
      return refuse(
         PREAGGREGATE_INVALID_CODE,
         "has a storage= that is not a destination name",
      );
   }
   const keyText = (key: string) =>
      keys.includes(key) ? (tag.text(key) ?? "") : undefined;
   const placedIn = readRollupContainer({
      name: keyText("name"),
      namespace: keyText("namespace"),
   });
   if ("invalid" in placedIn) {
      return refuse(PREAGGREGATE_INVALID_CODE, placedIn.invalid);
   }
   const namespace = placedIn.container;
   const storage =
      "placement" in placed && placed.placement.tier === "storage"
         ? placed.placement.destination
         : undefined;
   if (storage !== undefined && namespace !== undefined) {
      return refuse(
         PREAGGREGATE_INVALID_CODE,
         `names both storage= and ${placedIn.deprecatedNamespace ? "namespace=" : "name="}; a destination places its own tables`,
      );
   }
   return {
      measure,
      grain: [...new Set((grain as string[]).map((g) => g.trim()))].sort(
         byCodepoint,
      ),
      ...(storage !== undefined ? { storage } : {}),
      ...(namespace !== undefined ? { namespace } : {}),
      ...(placedIn.deprecatedNamespace ? { deprecatedNamespace: true } : {}),
      ...(placedIn.prefix !== undefined ? { prefix: placedIn.prefix } : {}),
      ...(fresh.freshness ? { freshness: fresh.freshness } : {}),
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
    * declarations. Absent: they are.
    */
   readsPreaggregateLines?: boolean;
   /**
    * Whether `#(preaggregate)` declarations are read. Absent: they are. A
    * caller that plans v1 rollups whatever the switch passes
    * `getMaterializeEnabled()`, so `#(preaggregate)` stays unread with its
    * switch off.
    */
   readsPreaggregateDeclarations?: boolean;
}

/**
 * Plan the rollups `node` declares (its own `#(preaggregate)` notes only),
 * cheapest read first ({@link byReadCost}).
 */
export function planSourceRollups(
   map: SourceMap,
   node: SourceNode,
   ctx: RollupPlanContext,
): { plans: RollupVariantPlan[]; refused: RollupRefusal[] } {
   const def = node.def as SourceLike;
   const sourceName = node.name ?? splitSourceID(node.sourceID)?.name ?? "";
   const fields = (def.fields ?? []) as FieldLike[];
   const byName = new Map<string, FieldLike>();
   for (const f of fields) {
      const n = fieldNameOf(f);
      if (n !== undefined) byName.set(n, f);
   }
   const { declarations, refused } = rollupDeclarationsOf(
      map,
      def,
      sourceName,
      ctx,
   );
   if (declarations.length === 0) return { plans: [], refused };
   const facts = rollupFactsOf(map, node, def, byName, sourceName);

   // Group by grain and placement: ten measures at one grain are one table.
   const groups = new Map<
      string,
      {
         decl: Declaration;
         measures: RollupMeasure[];
         declarations: Declaration[];
      }
   >();
   const decomposed = new Map<string, ReturnType<typeof decomposeMeasure>>();
   for (const decl of declarations) {
      const declined = declarationRefusal(decl, facts, ctx);
      if (declined === "skip") continue;
      if (declined) {
         refused.push(declined);
         continue;
      }
      let d = decomposed.get(decl.measure);
      if (!d) {
         d = checkedAgainstIR(
            decomposeMeasure(decl.measure, facts.scope),
            measureAggregates(def, decl.measure),
         );
         decomposed.set(decl.measure, d);
      }
      // v1's validation fails the load for its own non-additive measure.
      if (decl.v1 && isMeasureRefusal(d) && d.kind === "not_mergeable") {
         continue;
      }
      const measure = rollupMeasureOf(decl, d, facts);
      if ("tier" in measure) {
         refused.push(measure);
         continue;
      }
      const key = [
         ...decl.grain,
         `storage=${decl.storage ?? ""}`,
         `namespace=${decl.namespace ?? ""}`,
      ].join("\u0000");
      const group = groups.get(key) ?? {
         decl,
         measures: [],
         declarations: [],
      };
      if (!group.measures.some((m) => m.name === decl.measure)) {
         group.measures.push(measure);
      }
      group.declarations.push(decl);
      groups.set(key, group);
   }

   // One table has one freshness: measures at one grain and placement that
   // declare different ones are malformed, and the rollup is not planned. A
   // conflict among v1 lines alone is v1 validation's to report
   // (`conflicting_freshness`); one that a `#(preaggregate)` takes part in,
   // which v1 validation does not see, is reported here.
   const plans: RollupVariantPlan[] = [];
   const baseFreshness = declaredBaseFreshness(def);
   for (const group of groups.values()) {
      const merged = mergeRollupFreshness(
         group.declarations.map((d) => d.freshness),
      );
      if ("conflict" in merged) {
         if (!group.declarations.every((d) => d.v1)) {
            refused.push({
               tier: "malformed",
               code: PREAGGREGATE_INVALID_CODE,
               message: `rollups of source "${sourceName}" declare a different freshness.${merged.conflict}= on measures at the grain [${group.decl.grain.join(", ")}]; one grain is one rollup table, so give every measure at it the same one`,
               sourceName,
            });
         }
         continue;
      }
      // One table has one name: measures at one grain and placement that
      // write different `name=` leaves are malformed.
      const prefixes = new Set(group.declarations.map((d) => d.prefix ?? ""));
      if (prefixes.size > 1) {
         refused.push({
            tier: "malformed",
            code: PREAGGREGATE_INVALID_CODE,
            message: `rollups of source "${sourceName}" name different tables (name=) on measures at the grain [${group.decl.grain.join(", ")}]; one grain is one rollup table, so give every measure at it the same name=`,
            sourceName,
         });
         continue;
      }
      const freshness = rollupSourceFreshness(merged.freshness, baseFreshness);
      plans.push({
         ...rollupPlanOf(group, node, facts, ctx),
         ...(freshness ? { freshness } : {}),
      });
   }
   plans.sort(byReadCost);
   return { plans, refused };
}

/**
 * The base's own source-level freshness: its `#@ persist` line's, else its
 * `#(materialize)` declaration's, field by field. A rollup inherits it under
 * its own (freshness_declaration.ts), so a rollup and its base go stale
 * together unless the rollup says otherwise.
 */
function declaredBaseFreshness(def: SourceLike): DeclaredFreshness | undefined {
   if (!def.annotations) return undefined;
   const annotations = new Annotations(
      def.annotations as ConstructorParameters<typeof Annotations>[0],
   );
   return rollupSourceFreshness(
      tagFreshnessLayer(readRouteIfAny(annotations, "@")),
      tagFreshnessLayer(readRouteIfAny(annotations, "materialize")),
   );
}

/**
 * Every rollup declaration `def` writes, its `#(preaggregate)` notes and its
 * measures' own v1 `#@ preaggregate` lines, with a refusal
 * for a declaration on a field that is not a measure (malformed).
 */
function rollupDeclarationsOf(
   map: SourceMap,
   def: SourceLike,
   sourceName: string,
   ctx: RollupPlanContext,
): { declarations: Declaration[]; refused: RollupRefusal[] } {
   const refused: RollupRefusal[] = [];
   const fields = (def.fields ?? []) as FieldLike[];
   const sources = scopeOf(map);
   const declarations: Declaration[] = [];
   // `#(preaggregate)` is read unless the caller reads only v1's lines below
   // (`readsPreaggregateDeclarations: false`).
   const readsPreaggregateDeclarations =
      ctx.readsPreaggregateDeclarations ?? true;
   // One placement rule for both spellings: a grain with no container of its
   // own (and no storage=) is created beside its base's colocated `name=`,
   // and a deprecated `namespace=` is read with a warning naming `name=`.
   const inherited = basePersistNamespace(
      { annotations: def.annotations },
      { materialize: readsPreaggregateDeclarations },
   );
   const deprecationWarned = new Set<string>();
   const placed = (decl: Declaration): Declaration => {
      if (decl.deprecatedNamespace && decl.namespace !== undefined) {
         const key = `${decl.measure}\u0000${decl.namespace}`;
         if (!deprecationWarned.has(key)) {
            deprecationWarned.add(key);
            refused.push({
               tier: "deprecated",
               code: PREAGGREGATE_NAMESPACE_DEPRECATED_CODE,
               message: `Measure "${decl.measure}" of source "${sourceName}" places its rollup with namespace="${decl.namespace}": ${namespaceRewrite(decl.namespace)}.`,
               sourceName,
               measure: decl.measure,
            });
         }
      }
      return decl.namespace === undefined &&
         decl.storage === undefined &&
         inherited !== undefined
         ? { ...decl, namespace: inherited }
         : decl;
   };
   for (const field of readsPreaggregateDeclarations ? fields : []) {
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
         else declarations.push(placed(read));
      }
   }
   // A measure's own v1 `#@ preaggregate` lines, read by v1's own reader (a
   // grain per line, `#@ -preaggregate` clearing those above it, `storage=`
   // and `name=` bound to their line), are this leg's declarations too,
   // so a v1 rollup is planned, built (under the `#(preaggregate)` rollup's
   // table name) and served here. A line v1 reports as unusable is left to
   // v1's own validation. A source with a fan-out join has its v1 rollups
   // refused alone (`base_source_has_fanout_join`, which v1's validation
   // reports): none is planned, so its measures answer live.
   if ((ctx.readsPreaggregateLines ?? true) && !hasFanoutJoin({ fields })) {
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
            declarations.push(
               placed({
                  measure: name,
                  grain: grain.dimensions,
                  v1: true,
                  ...(grain.freshness !== undefined
                     ? { freshness: grain.freshness }
                     : {}),
                  ...(grain.storage !== undefined
                     ? { storage: grain.storage }
                     : {}),
                  ...(grain.namespace !== undefined
                     ? { namespace: grain.namespace }
                     : {}),
                  ...(grain.deprecatedNamespace
                     ? { deprecatedNamespace: true }
                     : {}),
                  ...(grain.prefix !== undefined
                     ? { prefix: grain.prefix }
                     : {}),
               }),
            );
         }
      }
   }
   // A source-level declaration names no measure.
   for (const note of readsPreaggregateDeclarations
      ? ownLevelNotes(def.annotations)
      : []) {
      if (routeOfNote(note.text) !== PREAGGREGATE_ROUTE) continue;
      refused.push({
         tier: "malformed",
         code: "preaggregate-not-a-measure",
         message: `#(preaggregate) is on source "${sourceName}"; it goes on a measure, with the dimensions to store it at in its grain=[…]`,
         sourceName,
      });
   }
   return { declarations, refused };
}

/** What deciding each of a source's rollup declarations reads. */
interface RollupFacts {
   sourceName: string;
   byName: ReadonlyMap<string, FieldLike>;
   joins: ReadonlyMap<string, ReturnType<SourceMap["joinsOf"]>[number]>;
   joinUsage: ReturnType<SourceMap["fieldJoinsOf"]>;
   declaredName: string;
   gate: ReturnType<typeof gateFieldNames>;
   /** The source's rows read the current time (S25b). */
   windowed: boolean;
   /**
    * The base's own given-reading `where:` terms, re-applied on the variant
    * (each over grain dimensions only), or why they cannot be.
    */
   given:
      | { terms: GivenReadTerm[] }
      | {
           refused: string;
           /** The shared classifier's reason (`readTimeTermsOf`), as on `#(materialize)`. */
           reason: "inherited_where_with_given" | "given_in_persisted_query";
        };
   /** Where the base is declared, for its build copy's edits. */
   declaration: TermUse | undefined;
   timeFields: ReturnType<typeof currentTimeFields>;
   /** The time zone the source sets, and whether a source in it joins it. */
   zone: ReturnType<typeof timezoneOf>;
   joinedInZone: boolean;
   /** Whether a same-zone joiner reads a rollup placed in `storage` (none: colocated). */
   readInZone(storage: string | undefined): boolean;
   scope: Parameters<typeof decomposeMeasure>[1];
   /** The joins a field path reads, or undefined when unknown. */
   joinsRead(path: readonly string[]): string[] | undefined;
   unservable(
      reason: RefusedReason,
      decl: Declaration,
      why: string,
   ): RollupRefusal;
}

function rollupFactsOf(
   map: SourceMap,
   node: SourceNode,
   def: SourceLike,
   byName: ReadonlyMap<string, FieldLike>,
   sourceName: string,
): RollupFacts {
   const joins = new Map(map.joinsOf(node.sourceID).map((j) => [j.name, j]));
   const joinUsage = map.fieldJoinsOf(node.sourceID);
   const declaredName = splitSourceID(node.sourceID)?.name ?? sourceName;
   const gate = gateFieldNames(
      def,
      byName,
      map.model(),
      joinUsage,
      scopeOf(map),
   );
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
   // A given-reading `where:` of the base's own, over its dimensions, is left
   // out of the build and written again on the variant, as the served copy
   // re-applies one (P2); an inherited one (`inherited_where_with_given`), or
   // one over anything else (`given_in_persisted_query`), is refused by the
   // one classifier `#(materialize)` reads, with its reason.
   const read = readTimeTermsOf(
      def as SourceDefLike,
      classifyWhereTerms(def).filter((t) => t.dependsOn.kind === "given"),
      [...byName].flatMap(([n, f]) =>
         isDimension(f) ? [{ column: n }] : [],
      ) as never,
      ((map.model() as { givens?: Record<string, unknown> } | undefined)
         ?.givens ?? {}) as Record<string, unknown>,
   );
   const given = read.ok
      ? { terms: read.terms }
      : { refused: read.reason, reason: read.refusal };
   const declaration = declarationOf(def as SourceDefLike);
   // A source that sets its own `timezone:` (or inherits one) built its
   // times in that zone, and the base branch reads no query on it from a
   // rollup (`TIME_ZONE`), whose variant is redeclared without the zone. The
   // only reader left is the join branch (A7): through a join from a source
   // in the same zone, to a colocated rollup or to one in a `storage=`
   // destination the joining source is stored in too (the branch reads no
   // other: `CROSS_CONNECTION`). Anything else is never read.
   // rollup_plan.spec.ts pins both.
   const zone = timezoneOf(def);
   const joinersInZone =
      zone === undefined
         ? []
         : [...map.nodes()].filter(
              (n) =>
                 n.sourceID !== node.sourceID &&
                 recordedTimezoneOf(n.def) === zone &&
                 map
                    .joinsOf(n.sourceID)
                    .some((j) => j.sourceID === node.sourceID),
           );
   /** Whether a same-zone joiner reads a rollup placed in `storage` (none: colocated). */
   const readInZone = (storage: string | undefined) =>
      storage === undefined
         ? joinersInZone.length > 0
         : joinersInZone.some((n) =>
              storedDestinationsOf(map, n.def).has(storage),
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
      reason: RefusedReason,
      decl: Declaration,
      why: string,
   ): RollupRefusal => ({
      tier: "unservable",
      code: PREAGGREGATE_NOT_BUILT_CODE,
      message: `#(preaggregate) on measure "${decl.measure}" of source "${sourceName}" is not built, so the measure is served live: ${why}.`,
      sourceName,
      measure: decl.measure,
      refused: measureRefusalKey(
         node.sourceID,
         declaredName,
         decl.measure,
         reason,
         decl,
      ),
   });

   return {
      sourceName,
      byName,
      joins,
      joinUsage,
      declaredName,
      gate,
      windowed,
      given,
      declaration,
      timeFields,
      zone,
      joinedInZone: joinersInZone.length > 0,
      readInZone,
      scope,
      joinsRead,
      unservable,
   };
}

/**
 * Why `decl` is not built, as a refusal; `skip` for one v1's own validation
 * already reports; undefined when it may be built. Checked in order: a hidden
 * measure, a grain that is not the source's dimensions, the current time, the
 * source's own time zone, a hidden grain dimension, and the source's gates.
 */
function declarationRefusal(
   decl: Declaration,
   facts: RollupFacts,
   ctx: RollupPlanContext,
): RollupRefusal | "skip" | undefined {
   const { byName, sourceName, unservable, gate, timeFields, zone } = facts;
   const field = byName.get(decl.measure) as FieldLike;
   if (isHidden(field)) {
      // v1's validation warns for its own line (preaggregate-hidden-field).
      if (decl.v1) return "skip";
      return unservable(
         "hidden_field",
         decl,
         "the measure is not public, and a rollup would publish it",
      );
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
      // v1's validation fails the load for its own line, in its words.
      if (decl.v1) return "skip";
      return {
         tier: "malformed",
         code: "preaggregate-unknown-grain",
         message: `#(preaggregate) on measure "${decl.measure}" of source "${sourceName}": ${grainProblem}`,
         sourceName,
         measure: decl.measure,
      };
   }
   const timeRead = facts.windowed
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
      return unservable(
         "current_time",
         decl,
         `${timeRead}, which a rollup would fix at build time while a live query reads it again at every read`,
      );
   }
   if (zone !== undefined && !facts.readInZone(decl.storage)) {
      const noJoin =
         decl.storage === undefined || !facts.joinedInZone
            ? "no source in the same zone joins it"
            : `no source in the same zone that joins it is stored in \`${decl.storage}\`, the only place a join reads a rollup there from`;
      return unservable(
         "time_zone",
         decl,
         `the source sets its own time zone (\`timezone: '${zone}'\`), and a query on it is never read from a rollup, whose stored times would be read in another zone, nor through a join (${noJoin}), so its table would cost a build no query reads`,
      );
   }
   // A hidden grain dimension is stored and redeclared with its modifier on
   // the variant, so no query selects it, as the served copy stores a hidden
   // column: `internal` lifted by the build's `include { public: * }`,
   // `private` by its build copy, which lifts only what the base's own
   // statement declares. One private on a source it extends stays private,
   // and is never stored.
   const hiddenGrain = decl.grain.find((g) => {
      const f = byName.get(g);
      return (
         isHidden(f) &&
         f?.accessModifier !== "internal" &&
         !(f?.accessModifier === "private" && declaredWithin(f, facts))
      );
   });
   if (hiddenGrain) {
      return unservable(
         "hidden_field",
         decl,
         `its grain dimension \`${hiddenGrain}\` is private on a source it extends, which a rollup's build cannot read`,
      );
   }
   if ("refused" in facts.given) {
      return unservable(facts.given.reason, decl, facts.given.refused);
   }
   const ungrained = facts.given.terms
      .flatMap((t) => t.columns)
      .filter((c) => !decl.grain.includes(c));
   if (ungrained.length > 0) {
      // A given-reading `where:` over a field the table does not store, as
      // the classifier names it on `#(materialize)`.
      return unservable(
         "given_in_persisted_query",
         decl,
         // malloy-text: a warning message for a person, not Malloy
         `the source's \`where:\` reads a given and ${[...new Set(ungrained)].map((c) => `\`${c}\``).join(", ")}, which the grain does not keep, so it could not be re-applied to the rollup's rows per caller`,
      );
   }
   // Row 21: a gated base's rollup is built only where a query can read
   // it past the gate; elsewhere its table would cost a build no query
   // reads.
   if (
      gate.declared &&
      !(ctx.readUnderGates ?? rollupsServeGatedBases(decl, gate.own))
   ) {
      return unservable(
         gate.reason.base,
         decl,
         decl.storage !== undefined
            ? "the source has a row-level gate (`#(access_filter)` or `#(authorize)`), and a rollup in a storage destination is not read past a gate yet, so its table would cost a build no query reads; leave out storage= to build it in the source's own warehouse"
            : "the source's row-level gate (`#(access_filter)` or `#(authorize)`) is inherited from a source it extends, and a rollup keeps only its base's own gate unless PUBLISHER_MATERIALIZE_GATED_SOURCES is on, so its table would cost a build no query reads",
      );
   }
   // A gate on a source the base joins is not the base's to graft: the
   // rollup is not built (A4, fail closed).
   if (gate.declared && gate.joinGated) {
      return unservable(
         gate.reason.joined,
         decl,
         "a source it joins has a row-level gate, which a rollup cannot re-apply to its rows",
      );
   }
   // A4: a gate's fields must be in the grain for its rows to be filtered;
   // a gate that reads through a join (or anything but a dimension) can
   // never be re-applied to a rollup's rows.
   const ungated = [
      ...gate.unkeepable,
      ...gate.dimensions.filter((g) => !decl.grain.includes(g)),
   ];
   if (ungated.length > 0) {
      return unservable(
         gate.reason.base,
         decl,
         // malloy-text: a warning message for a person, not Malloy
         `the source's access filter reads ${ungated.map((g) => `\`${g}\``).join(", ")}, which the grain does not keep, so the rollup's rows could not be filtered per caller`,
      );
   }
   return undefined;
}

/**
 * The measure `decl` stores, its partials and merge, or the refusal of a
 * measure whose partials cannot be merged or read, that aggregates through a
 * join its grain does not key, or whose partial's column is taken.
 */
function rollupMeasureOf(
   decl: Declaration,
   d: ReturnType<typeof decomposeMeasure>,
   facts: RollupFacts,
): RollupMeasure | RollupRefusal {
   const { byName, sourceName, unservable, joins, joinsRead } = facts;
   if (isMeasureRefusal(d)) {
      return d.kind === "not_mergeable"
         ? {
              tier: "malformed",
              code: "preaggregate-not-mergeable",
              message: `#(preaggregate) on measure "${decl.measure}" of source "${sourceName}": its partials cannot be merged (${d.detail}). Store sum, count, min and max measures, and derive the rest from them`,
              sourceName,
              measure: decl.measure,
           }
         : unservable(
              "unreadable_measure",
              decl,
              `its definition is outside what #(preaggregate) reads (${d.detail})`,
           );
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
      return unservable("join_level", decl, joinProblem);
   }
   const columns =
      d.leaves.length === 1
         ? [`${decl.measure}__partial`]
         : d.leaves.map((_, i) => `${decl.measure}__partial${i}`);
   const clash = columns.find((c) => byName.has(c));
   if (clash) {
      return unservable(
         "name_clash",
         decl,
         `the source already has a field named \`${clash}\`, the column its partial is stored as`,
      );
   }
   return {
      name: decl.measure,
      partials: d.leaves.map((leaf, i) => ({
         column: columns[i] as string,
         leaf,
      })),
      merged: d.merged(columns),
      fanOutDimensions: [...measureFanOut].sort(byCodepoint),
   };
}

/** The plan of one rollup table: a grain and placement, and its measures. */
function rollupPlanOf(
   { decl, measures }: { decl: Declaration; measures: RollupMeasure[] },
   node: SourceNode,
   facts: RollupFacts,
   ctx: RollupPlanContext,
): RollupVariantPlan {
   const { byName, joins, joinUsage, declaredName, gate, sourceName } = facts;
   const relativeID = ctx.packageRelativeSourceID(node.sourceID);
   const name = rollupName(declaredName, relativeID, decl.grain, decl);
   const sorted = [...measures].sort((a, b) => byCodepoint(a.name, b.name));
   const privateGrain = decl.grain.filter(
      (g) => byName.get(g)?.accessModifier === "private",
   );
   return {
      variant: true,
      baseSourceName: sourceName,
      baseDeclaredName: declaredName,
      baseSourceID: node.sourceID,
      rollupSourceName: name,
      grainDimensions: decl.grain,
      ...(decl.storage !== undefined ? { storage: decl.storage } : {}),
      ...(decl.namespace !== undefined ? { namespace: decl.namespace } : {}),
      ...(decl.prefix !== undefined ? { prefix: decl.prefix } : {}),
      // The v1 shape the build plan's wire entry reads (measure names).
      measures: sorted.map((m) => ({
         name: m.name,
         partialName: m.partials[0]?.column ?? "",
         reaggregate: reaggregateOf(m.partials[0]?.leaf),
      })),
      rollupMeasures: sorted,
      baseGated: gate.declared,
      ...(gate.declared && gate.own ? { baseGateOwn: true as const } : {}),
      internalGrain: decl.grain.filter(
         (g) => byName.get(g)?.accessModifier === "internal",
      ),
      ...(privateGrain.length > 0 ? { privateGrain } : {}),
      ...("terms" in facts.given && facts.given.terms.length > 0
         ? { readTimeTerms: facts.given.terms }
         : {}),
      ...((privateGrain.length > 0 ||
         ("terms" in facts.given && facts.given.terms.length > 0)) &&
      facts.declaration
         ? { declaration: facts.declaration }
         : {}),
      fanOutDimensions: decl.grain.filter((g) => {
         const usage = joinUsage.get(g);
         if (usage === undefined) return joinUsage.has(g);
         return (
            usage.nested ||
            [...usage.joins].some((j) => joins.get(j)?.relationship !== "one")
         );
      }),
   };
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
   scope: SourceScope,
): {
   dimensions: string[];
   unkeepable: string[];
   declared: boolean;
   joinGated: boolean;
   /** Every gate is the base's own: none inherited, none from a derivation. */
   own: boolean;
   /** The refusal name for the base's own gates, and for its joins' ({@link gateRefusalReason}). */
   reason: { base: GateRefusalReason; joined: GateRefusalReason };
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
   const joinedRoutes = ((def.fields ?? []) as FieldLike[]).flatMap((f) =>
      f.join !== undefined ? joinGateRoutes(f, modelDef) : [],
   );
   const joinGated = joinedRoutes.length > 0;
   return {
      dimensions: [...dimensions].sort(byCodepoint),
      unkeepable: [...unkeepable].sort(byCodepoint),
      declared: entries.length > 0,
      joinGated,
      reason: {
         base: gateRefusalReason(entries.map((e) => e.route)),
         joined: gateRefusalReason(joinedRoutes),
      },
      own:
         entries.length > 0 &&
         unkeepable.size === 0 &&
         entries.every((e) => !e.selfContained && e.struct !== undefined) &&
         gateNotesDeclaredBy(def, scope),
   };
}

/**
 * Whether every gate note on `def`'s own level is one its own statement
 * declared ({@link declaredNotes}): Malloy copies a base's notes onto a source
 * that extends it, so a gate the walk reads as the entry point's may be
 * inherited. Only a base's own gate is kept under G3.
 */
function gateNotesDeclaredBy(def: SourceLike, scope: SourceScope): boolean {
   const gates = ownLevelNotes(def.annotations).filter(
      (n) => canonicalAuthorizeRoute(routeOfNote(n.text)) !== undefined,
   );
   const declared = new Set(
      declaredNotes(def as NotedSourceDef, scope as Record<string, unknown>),
   );
   return gates.length > 0 && gates.every((n) => declared.has(n));
}

/**
 * The routes of the row-level gates a joined source carries, empty when it
 * carries none (fails closed: an unreadable one reads as a lock).
 */
function joinGateRoutes(join: FieldLike, modelDef: unknown): string[] {
   try {
      return collectEntryPointGates(
         join as unknown as SourceDef,
         modelDef as ModelDef,
         new Set(),
         true,
      ).map((g) => g.route);
   } catch {
      return ["authorize"];
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
      ...(refusal.tier === "unservable" ? { refused: refusal.refused } : {}),
   };
}

// ---------------------------------------------------------------------------
// The build file
// ---------------------------------------------------------------------------

/**
 * The `storage=` destinations a source's table, or the table of a source it is
 * built on, is declared in: by `#(materialize) storage=` or
 * `#@ persist storage=` on a source of its chain. The join branch reads a rollup
 * in a destination only for a query source the served copy reads from there.
 */
function storedDestinationsOf(map: SourceMap, struct: unknown): Set<string> {
   const out = new Set<string>();
   for (const id of map.chainOf(struct)) {
      const notes = ownLevelNotes(
         (map.node(id)?.def as { annotations?: AnnotationsDef } | undefined)
            ?.annotations,
      );
      const read = (route: string, key?: string) => {
         const own = notes.filter((n) => routeOfNote(n.text) === route);
         const tag = own.length > 0 ? readNotesIfAny(own, route) : undefined;
         if (tag && (key === undefined || tag.has(key))) {
            const destination = destinationOf(tag, key);
            if (destination !== undefined) out.add(destination);
         }
      };
      read("materialize");
      read("@", "persist");
   }
   return out;
}

/**
 * The Malloy text of a rollup's key check (known limitation 29), or undefined
 * when the rollup's grain is not one dimension. Run against the rollup
 * companion, which imports the base under {@link rollupBaseAlias}, so it reads
 * exactly the rows the rollup's build groups: the base's, on its own
 * connection, for a colocated and a `storage=` rollup alike. A NULL key meets
 * no joining row, so its repeats do not count.
 */
export function rollupKeyCheckQuery(
   plan: RollupVariantPlan,
): string | undefined {
   const [key, ...rest] = plan.grainDimensions;
   if (key === undefined || rest.length > 0) return undefined;
   // malloy-text: keyCheckPipeline writes its names through identifier
   return `run: ${keyCheckPipeline("rollup_key", rollupBaseAlias(plan.baseDeclaredName), key)}`;
}

/**
 * Every named source in `map` with a declared primary key and a join, keyed
 * on that key, to a declared source: the query sources whose key the
 * join-to-rollup branch may need checked. A join keyed on another field never
 * takes the branch as a join_many (`keyRepeatRefusal`), so it needs no check.
 */
export function parentKeyNeedsOf(map: SourceMap): ParentKeyNeed[] {
   const out: ParentKeyNeed[] = [];
   for (const node of map.nodes()) {
      const def = node.def as { connection?: unknown };
      const keyColumn = primaryKeyOf(def);
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
 * is keyed on: the join-to-rollup branch needs that key checked never NULL
 * and unique before it serves a count, sum or average of the source's own
 * rows through the join. Live takes those at a joined source over the rows
 * whose key is not NULL (`CASE WHEN key IS NOT NULL`,
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
         const def = joined?.def as { connection?: unknown } | undefined;
         const keyColumn = primaryKeyOf(def);
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
 * Whether the join-to-rollup branch reads rollup `plan` through `need`'s join
 * as a join_many: the join is declared one, or the rollup's grain is finer
 * than the join's key, so the branch re-points it as one (`joinRollupBranches`).
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
 * The stored-table key checks a build runs (each table's `storedKeyCheck`):
 * each of `sourceIDs` (the planned sources, the rollups left out) with a declared
 * primary key, by sourceID, when some `storage=` rollup in `plans` is read
 * through a join keyed on a source's primary key as a join_many. That branch
 * reads the query source's stored table in the destination, so the
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
   // malloy-text: keyCheckPipeline writes its names through identifier
   return `run: ${keyCheckPipeline("parent_key", name, keyColumn)}`;
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
