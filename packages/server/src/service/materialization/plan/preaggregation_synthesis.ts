// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 `#@ preaggregate` planner: turn validated declarations into rollup
 * plans, one per distinct grain, which the rollup leg reads as
 * `#(preaggregate)` plans and the build plan builds.
 *
 * ## Determinism is a correctness property
 *
 * A rollup's name and table derive from its plan, so grain dimensions,
 * measures and plans are sorted, and the name derives from the canonical
 * grain: two plannings of one model must name the same tables.
 *
 * Pure: takes compiled IR, returns plans, touches no I/O.
 */

import { Annotations } from "@malloydata/malloy";
import { placementOf, readRouteIfAny } from "./declaration";
import { createHash } from "node:crypto";
import {
   readPreaggregateAnnotation,
   type AnnotatableMeasure,
} from "./preaggregation_annotation";
import { classifyMeasureAdditivity } from "./preaggregation_classifier";
import type { RollupMeasure, RollupPlan } from "../domain/rollup_measures";
import { nameContainer } from "./preaggregation_namespace";
import { fieldNotes } from "../text/field_notes";
import {
   hasFanoutJoin,
   type ValidatableSource,
} from "./preaggregation_validation";

/** Suffix on the stored partial-aggregate column for a measure. */
const PARTIAL_SUFFIX = "__partial";

/**
 * A short digest of the canonical grain, appended to the rollup's name.
 *
 * Needed because the readable part is lossy: a grain of `[a_b, c]` and one of
 * `[a, b_c]` both slug to `a_b_c`, and two rollups sharing a name would collapse
 * into one table serving the wrong queries. The digest is over the joined grain
 * with a separator that cannot appear in a Malloy identifier, so it distinguishes
 * them.
 */
function grainDigest(grainDimensions: string[]): string {
   return createHash("sha256")
      .update(grainDimensions.join("\u0000"))
      .digest("hex")
      .slice(0, 8);
}

/** How much of the grain goes in the readable part of the name. */
const NAME_SLUG_LIMIT = 40;

/**
 * The rollup's source name: readable enough to recognize in a build plan, a
 * run manifest and a log line, and unique by digest.
 */
export function rollupSourceName(
   baseSourceName: string,
   grainDimensions: string[],
): string {
   // Dots are folded out so the name, which the rollup's build source and
   // manifest entry carry (its table takes a readable name of its own,
   // domain/table_name.ts), stays one unqualified identifier; a consumer
   // reading a dotted name as schema-qualified would address the wrong thing. Dotted
   // grains are refused at publish anyway; this makes it a property of the
   // name. The digest is over the RAW dimensions, so `a.b` and `a_b` differ.
   const slug = grainDimensions
      .join("_")
      .replace(/\./g, "_")
      .slice(0, NAME_SLUG_LIMIT);
   return `${baseSourceName}__preagg__${slug}__${grainDigest(grainDimensions)}`;
}

/**
 * Order two rollups on one base so the COARSER is offered first.
 *
 * The rollup leg offers rollups in this order and uses the first that
 * answers, so the order decides which covering rollup answers a query. Fewer grain dimensions win
 * (which subsumes a strict-subset rule, and also orders incomparable grains).
 *
 * A dimension count is only a proxy for size. Ordering on the run manifest's
 * `rowCount` is deliberately NOT used: the order would then depend on the
 * bound run manifest, and the build and serve legs would synthesize different text
 * (see this module's header).
 *
 * The generated name is the final tie-break, so the order is total.
 */
function compareRollupBreadth(a: RollupPlan, b: RollupPlan): number {
   return (
      a.grainDimensions.length - b.grainDimensions.length ||
      a.rollupSourceName.localeCompare(b.rollupSourceName)
   );
}

/**
 * The container a rollup inherits from its base's colocated `name=`: its
 * `#@ persist name=`, else (with `materialize`, the switch that reads it) its
 * `#(materialize) name=`, everything before the last dot; undefined when there
 * is none to inherit. A rollup of a table belongs beside it, `#(preaggregate)`
 * and `#@ preaggregate` alike.
 *
 * Read from annotations because synthesis runs before a build plan exists.
 * Unreadable annotations are treated as absent, as in
 * {@link readPreaggregateAnnotation}.
 *
 * **A `storage=` base lends nothing — not its destination, and not a container.**
 *
 * Not the DESTINATION: a rollup's placement is its own line's and part of its
 * identity (the rollup's name digests it), so adding or removing `storage=` on
 * a base never moves its rollups' tables. A rollup over a stored base builds
 * from the author's rows, not the base's table, wherever its own line puts it.
 *
 * Not the CONTAINER, because the base's `name=` is a name in the destination's
 * catalog and means nothing in the source warehouse where the rollup is built.
 */
export function basePersistNamespace(
   source: ValidatableSource,
   options: { materialize?: boolean } = {},
): string | undefined {
   if (!source.annotations) return undefined;
   try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const annotations = new Annotations(source.annotations as any);
      const persist = readRouteIfAny(annotations, "@");
      const declared = persist?.has("persist")
         ? placementOf(persist, "persist")
         : options.materialize
           ? (() => {
                const tag = readRouteIfAny(annotations, "materialize");
                return tag ? placementOf(tag) : undefined;
             })()
           : undefined;
      // A base built in a destination, or refused, lends no container.
      if (
         !declared ||
         !("placement" in declared) ||
         declared.placement.tier !== "colocated"
      ) {
         return undefined;
      }
      return nameContainer(declared.placement.name);
   } catch {
      return undefined;
   }
}

/**
 * A field the source does not publicly expose.
 *
 * Mirrors `isAccessRestricted` in serve/bindings.ts and
 * preaggregation_validation.ts, and fails CLOSED: anything not exactly `public`
 * is hidden, so a future modifier kind is refused by default. No modifier at
 * all is public.
 */
function isHidden(field: { accessModifier?: unknown } | undefined): boolean {
   return field?.accessModifier != null && field.accessModifier !== "public";
}

/**
 * Group one source's `#@ preaggregate` declarations into rollups, one per
 * distinct grain (ten measures at one grain are one table and one `GROUP BY`).
 *
 * Assumes the source passed {@link validateSourcePreaggregation}: a declaration
 * that would be refused is skipped here, so the two agree on what is buildable.
 */
export function planSourcePreaggregation(
   baseSourceName: string,
   source: ValidatableSource,
): RollupPlan[] {
   // Refused alone, as the validator reports it: no rollup over a fan-out join.
   if (hasFanoutJoin(source)) return [];
   const inheritedNamespace = basePersistNamespace(source);
   /** Resolve a grain dimension back to the field it names, if it names one. */
   const fieldNamed = (name: string) =>
      (source.fields ?? []).find((f) => (f.as ?? f.name) === name);
   // Canonical grain -> the measures declared at it. Keyed on the sorted grain so
   // two authors writing the same dimensions in either order land in one entry.
   const byGrain = new Map<
      string,
      {
         grainDimensions: string[];
         measures: RollupMeasure[];
         namespace?: string;
         storage?: string;
      }
   >();

   for (const field of source.fields ?? []) {
      const declaration = readPreaggregateAnnotation(
         field as AnnotatableMeasure,
      );
      if (!declaration.declared || declaration.errors.length > 0) continue;

      // Hidden measures (and hidden grain dimensions, below) never reach a
      // rollup. This skip IS the access control; the publish refusal is only
      // the message. It guarantees no table is built and nothing is served.
      if (isHidden(field)) continue;

      const additivity = classifyMeasureAdditivity(field as never);
      if (!additivity.additive) continue;

      // Several grains on one measure are several rollups (see
      // preaggregation_annotation.ts for why).
      for (const grain of declaration.grains) {
         const grainDimensions = grain.dimensions;
         if (grainDimensions.length === 0) continue;
         // A grain is STORED, so a hidden dimension in it would be reachable
         // through the rollup. Checked per grain, since only one of a measure's
         // grains may name a hidden field. A dimension resolving to no field is
         // not hidden; the validator refuses it as unknown.
         if (grainDimensions.some((d) => isHidden(fieldNamed(d)))) continue;

         const key = grainDimensions.join("\u0000");
         const entry = byGrain.get(key) ?? { grainDimensions, measures: [] };
         const name = field.as ?? field.name;
         entry.measures.push({
            name,
            partialName: `${name}${PARTIAL_SUFFIX}`,
            reaggregate: additivity.reaggregate,
            notes: fieldNotes(field).filter((n) => !n.startsWith("#@")),
         });
         // First one named wins. Safe only because differing values at ONE
         // grain are refused at publish (`conflicting_namespace`,
         // `conflicting_storage`).
         entry.namespace ??= grain.namespace;
         entry.storage ??= grain.storage;
         byGrain.set(key, entry);
      }
   }

   return [...byGrain.values()]
      .map(({ grainDimensions, measures, namespace, storage }) => ({
         baseSourceName,
         rollupSourceName: rollupSourceName(baseSourceName, grainDimensions),
         namespace: namespace ?? inheritedNamespace,
         // Never inherited from the base (see basePersistNamespace).
         storage,
         grainDimensions,
         // Sorted so the emitted text does not depend on field order in the IR.
         measures: [...measures].sort((a, b) => a.name.localeCompare(b.name)),
      }))
      .sort(compareRollupBreadth);
}
