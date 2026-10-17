// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Turns a storage manifest's ROLLUP entries into serve bindings: what may be
 * served, separately from how the transform emits it. Pure, no I/O.
 *
 * A rollup cannot take the ordinary path (`Model.serveBindingsWithRefinements`),
 * which looks up the binding's source in the AUTHOR model to lift fields and to
 * narrow the schema to public columns. Both lookups fail silently for a rollup:
 *  - Under its synthesized name there is no such source, the public schema is
 *    empty, and the binding is dropped: the rollup never serves.
 *  - Under the base's name, narrowing strips the `<measure>__partial` columns
 *    and the base's measures reference columns the rollup lacks. The shape fails
 *    to compile, and since it is built once per package, the escalation thins
 *    every OTHER storage-bound source too.
 *
 * So a rollup's refinements come from its {@link RollupPlan} and its captured
 * schema is used as-is. That does not relax narrowing: synthesis chose the
 * stored columns, so there is no wider physical table to hide.
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
interface RollupServeGroup {
   /** The AUTHOR's source name, which queries use; never a rollup's own name. */
   baseSourceName: string;
   /**
    * Coarsest grain first, the order synthesis emits compose() members in. The
    * resolver takes the first that covers, so the cheapest rollup wins; a
    * different order here would route the two legs differently.
    */
   members: ServeBinding[];
}

/** A base whose rollups cannot be served, and why. */
interface RollupServeConflict {
   baseSourceName: string;
   reason: string;
}

/**
 * The merged measures a rollup's virtual source must declare.
 *
 * `total is total__partial.sum()`, matching what synthesis emits: the measure
 * keeps the BASE's name so a query for `total` finds it. Returned as
 * {@link SourceRefinement}s so `serveShapeFragment` renders them.
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
 * Non-`preaggregate` bindings take the ordinary path but are consulted: a base
 * that is itself `storage=`-persisted already claims its name.
 *
 * A conflict DROPS that base's rollups so its queries serve from the base. A
 * duplicate `source:` declaration would fail the whole package's serve shape,
 * and the escalation ladder cannot recover from it (thinning refinements does
 * not remove a declaration).
 */
export function rollupServeBindings(
   bindings: ServeBinding[],
   plans: RollupPlan[],
): { groups: RollupServeGroup[]; conflicts: RollupServeConflict[] } {
   const planByRollupName = new Map(
      plans.map((plan) => [plan.rollupSourceName, plan]),
   );
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
         // A rollup the declarations no longer produce (annotation removed or
         // grain changed since the build). Not an error: the next build drops
         // the table, and until then queries serve from the base.
         logger.debug(
            "Skipping a rollup serve binding with no matching plan; serving live",
            { sourceName: binding.sourceName },
         );
         continue;
      }
      // A rollup's name folds only base and grain, not measures, so a plan that
      // gained a measure since the build matches a table lacking its partial
      // column. Caught here because an uncompilable member would fail the
      // package-wide shape and take every other base's rollups with it.
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
      // Composite members must share one connection (else the compiler raises
      // `composite-source-connection-mismatch`), but two grains on one base may
      // name different destinations. Refused rather than partially served,
      // which would drop members arbitrarily.
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
            .sort((a, b) => compareRollupBreadth(a.plan, b.plan))
            .map(({ plan, binding }) => ({
               ...binding,
               // From the plan, never the author model (see module header).
               refinements: rollupServeRefinements(plan),
            })),
      });
   }
   return { groups, conflicts };
}
