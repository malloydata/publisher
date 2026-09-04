// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Turning a storage manifest's ROLLUP entries into serve bindings the transform
 * can emit — the half of lake-served pre-aggregation that decides what may be
 * served, separately from how it is emitted.
 *
 * ## Why a rollup cannot go through the ordinary binding path
 *
 * An ordinary `storage=` binding is finished by `Model.serveBindingsWithRefinements`,
 * which does two lookups into the AUTHOR's compiled model, both keyed by the
 * binding's source name: it reads that source's fields to lift its dimensions,
 * measures, joins and views, and it narrows the captured table schema to the
 * columns that source publicly exposes.
 *
 * A rollup fails both, in opposite directions depending on the name it is bound
 * under, and neither failure is loud:
 *
 *  - **Under its own synthesized name**, the author model has no such source. The
 *    field list is `undefined`, `narrowSchemaToPublic` intersects against an empty
 *    set and returns nothing, and the caller drops any binding whose public schema
 *    is empty. The rollup silently never serves.
 *  - **Under its base's author name**, the field list is the BASE's. The stored
 *    `<measure>__partial` columns are not public fields of the base, so narrowing
 *    strips exactly the columns the rollup's measures read; meanwhile the base's
 *    own measures are lifted and reference columns (`amount`) the rollup table
 *    does not have. The shape then fails to compile — and because the shape is
 *    built once for the whole package, `compileServeShape`'s escalation drops
 *    views and joins for every OTHER storage-bound source too, to accommodate a
 *    rollup it can never satisfy.
 *
 * The second is the one worth stating plainly: the blast radius of a bad rollup
 * binding is the whole package's storage serving, not the rollup's.
 *
 * So a rollup's refinements are derived from its {@link RollupPlan}, which already
 * holds exactly what is needed — the merged measure per stored partial — and its
 * captured schema is used as-is. That is not a relaxation of the narrowing rule:
 * a rollup's stored columns ARE its whole surface, because synthesis chose them.
 * There is no wider physical table to hide part of, which is the thing narrowing
 * exists to prevent.
 *
 * Pure: takes bindings and plans, returns bindings and conflicts, touches no I/O.
 */

import { logger } from "../logger";
import type {
   ServeBinding,
   SourceRefinement,
} from "./materialization_serve_transform";
import {
   compareRollupBreadth,
   type RollupPlan,
} from "./preaggregation_synthesis";

/** One base source's rollups, in the order they should be offered. */
export interface RollupServeGroup {
   /**
    * The AUTHOR's source name — what a query names, and what the emitted shape
    * must re-expose. Never a rollup's own name: nothing queries that.
    */
   baseSourceName: string;
   /**
    * The rollup members, coarsest grain first, matching the order synthesis emits
    * compose() members in. The resolver takes the first that covers, so this is
    * what makes the cheapest covering rollup win; ordering them differently here
    * than in the synthesized text would give the two legs different routing for
    * the same query.
    */
   members: ServeBinding[];
}

/** A base whose rollups cannot be served, and why. */
export interface RollupServeConflict {
   baseSourceName: string;
   reason: string;
}

/**
 * The merged measures a rollup's virtual source must declare.
 *
 * `total is total__partial.sum()` — the same text synthesis emits into the rollup
 * member's `extend {}`, for the same reason: the measure keeps the BASE's name so
 * a query asking for `total` finds it, and only the stored column is renamed.
 *
 * Emitted as {@link SourceRefinement}s so the existing `serveShapeFragment` path
 * renders them; the shape of a measure refinement (`name` plus `code`) is exactly
 * what a merge is.
 */
export function rollupServeRefinements(plan: RollupPlan): SourceRefinement[] {
   return plan.measures.map((m) => ({
      kind: "measure" as const,
      name: m.name,
      code: `${m.partialName}.${m.reaggregate}()`,
   }));
}

/**
 * Group a package's rollup serve bindings by the base source they re-expose,
 * attaching each one's merged measures from its plan.
 *
 * Bindings whose origin is not `preaggregate` are ignored — they take the
 * ordinary path — but they ARE consulted, because a base that is itself
 * `storage=`-persisted has a binding of its own under the same author name, and
 * one name cannot rebind to two shapes.
 *
 * A conflict DROPS that base's rollups rather than failing anything: its queries
 * serve from the base, which is always correct because the tier is a performance
 * tier. Dropping is also the only safe answer — emitting two `source: orders is …`
 * declarations would fail the whole package's serve shape, and `compileServeShape`
 * treats base-only as its guaranteed floor, so a duplicate name breaches a floor
 * the escalation ladder cannot recover from.
 */
export function rollupServeBindings(
   bindings: ServeBinding[],
   plans: RollupPlan[],
): { groups: RollupServeGroup[]; conflicts: RollupServeConflict[] } {
   const planByRollupName = new Map(
      plans.map((plan) => [plan.rollupSourceName, plan]),
   );
   // Author names already claimed by an ordinary binding. A rollup group cannot
   // take a name one of these holds.
   const claimedByPersist = new Set(
      bindings
         .filter((b) => b.origin !== "preaggregate")
         .map((b) => b.sourceName),
   );

   const byBase = new Map<
      string,
      { plan: RollupPlan; binding: ServeBinding }[]
   >();
   for (const binding of bindings) {
      if (binding.origin !== "preaggregate") continue;
      const plan = planByRollupName.get(binding.sourceName);
      if (!plan) {
         // The manifest holds a rollup this model's declarations no longer
         // produce — an annotation removed or a grain changed since the build.
         // Its table is real but nothing can describe it, so it cannot be served.
         // Not an error: the next build drops the table, and until then queries
         // serve from the base.
         logger.debug(
            "Skipping a rollup serve binding with no matching plan; serving live",
            { sourceName: binding.sourceName },
         );
         continue;
      }
      // The manifest entry was matched to this plan by NAME, and a rollup's name
      // folds only its base and grain — not its measures. So a plan whose measure
      // set has grown since the table was built matches an entry whose schema
      // predates the new measure, and the member would declare
      // `revenue is revenue__partial.sum()` over a table with no such column.
      //
      // That is worth catching here rather than at compile because of what it
      // costs there: an uncompilable member fails the shape, and the shape is
      // built once per package, so one stale entry takes every other base's
      // rollups with it. Checked against the captured schema, which is the only
      // description of the table that actually exists.
      const columns = new Set(binding.schema.map((c) => c.name));
      const missing = [
         ...plan.grainDimensions,
         ...plan.measures.map((m) => m.partialName),
      ].filter((name) => !columns.has(name));
      if (missing.length > 0) {
         logger.info(
            "Skipping a rollup serve binding whose table predates its plan; serving from the base",
            {
               sourceName: binding.sourceName,
               missingColumns: missing,
            },
         );
         continue;
      }
      const forBase = byBase.get(plan.baseSourceName) ?? [];
      forBase.push({ plan, binding });
      byBase.set(plan.baseSourceName, forBase);
   }

   const groups: RollupServeGroup[] = [];
   const conflicts: RollupServeConflict[] = [];
   for (const [baseSourceName, entries] of [...byBase.entries()].sort(
      ([a], [b]) => a.localeCompare(b),
   )) {
      // Every member of a composite must live on ONE connection — the compiler
      // raises `composite-source-connection-mismatch` otherwise, which is the same
      // constraint that made this a lake-ONLY composite in the first place. Two
      // grains on one base CAN name different destinations (they are two tables,
      // so validation has nothing to refuse), and that is unservable under one
      // name. Refused rather than partially served: picking one destination's
      // members would silently drop the others, and which ones survived would
      // depend on nothing the author can see.
      const destinations = new Set(
         entries.map((e) => e.binding.destinationName),
      );
      if (destinations.size > 1) {
         conflicts.push({
            baseSourceName,
            reason:
               `its rollups are built into different storage destinations ` +
               `(${[...destinations].sort().join(", ")}), and every member of one ` +
               `composite must live on a single connection; give every grain on ` +
               `\`${baseSourceName}\` the same \`storage=\``,
         });
         continue;
      }
      if (claimedByPersist.has(baseSourceName)) {
         conflicts.push({
            baseSourceName,
            reason:
               `the source is itself materialized into storage, so its own binding ` +
               `already claims the name \`${baseSourceName}\`; one name cannot be ` +
               `rebound to both a stored copy of the source and its rollups`,
         });
         continue;
      }
      groups.push({
         baseSourceName,
         members: entries
            // Coarsest first, the same order synthesis emits compose() members in.
            .sort((a, b) => compareRollupBreadth(a.plan, b.plan))
            .map(({ plan, binding }) => ({
               ...binding,
               // Derived from the plan, never from the author model: see this
               // module's header for what each of the two lookups does to a
               // rollup.
               refinements: rollupServeRefinements(plan),
            })),
      });
   }
   return { groups, conflicts };
}
