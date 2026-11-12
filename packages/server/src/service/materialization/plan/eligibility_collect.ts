// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which planned sources may build and serve, decided where the compiled
 * sources exist: rollups over a stored base, rollups that would share a
 * table, the tier gate's inputs, and the per-tier eligibility records the
 * serve side requires. Split out of build_plan.ts; behaviour unchanged.
 */

import type { BuildTarget, PersistSource } from "@malloydata/malloy";
import { logger } from "../../../logger";
import { errMessage } from "../../../utils";
import { computeSourceEntityId } from "./addressing";
import {
   assertMaterializationEligible,
   assertRollupNameUnshared,
   assertTierEligible,
   type TierGateInputs,
} from "./materialization_eligibility";
import type { RollupPlan } from "./preaggregation_synthesis";
import {
   declaredStorage,
   deriveAnnotationFields,
   type PersistSourceGateOutcome,
} from "./persist_source_fields";

/**
 * The slice of a compiled build plan eligibility reads; `CompiledBuildPlan`
 * satisfies it. Declared here so this module is not typed against its caller.
 */
export interface EligibilityPlan {
   sources: Record<string, PersistSource>;
   targets?: Record<string, BuildTarget>;
   connectionDigests: Record<string, string>;
   preaggregatePlans?: Record<string, RollupPlan>;
   sourceModelPaths?: Record<string, string>;
   sourceGateOutcomes?: Record<string, PersistSourceGateOutcome>;
}

/**
 * The base a `storage=` rollup is declared over, when that base is itself
 * persisted with `storage=`; undefined for every other source. Such a rollup
 * can neither build nor serve today (#6293): the chained storage build lifts a
 * downstream's definition from its file, and a rollup lives in the
 * synthesized companion, which is never written; and the base's own stored
 * binding claims the name the rollup would be served under. So it is left out
 * of the plan and every run, with a package warning, and the base still builds
 * and serves from storage. Rollups over a stored base are materialization v2
 * phase 4 (`#(preaggregate)`).
 */
export function storedBaseOfRollup(
   compiled: {
      sources: Record<string, PersistSource>;
      preaggregatePlans?: Record<string, RollupPlan>;
   },
   sourceID: string,
): PersistSource | undefined {
   const plan = compiled.preaggregatePlans?.[sourceID];
   const rollup = compiled.sources[sourceID];
   if (!plan?.baseSourceID || !rollup || !declaredStorage(rollup)) {
      return undefined;
   }
   const base = compiled.sources[plan.baseSourceID];
   return base && declaredStorage(base) ? base : undefined;
}

/** Memo for {@link collidingRollups}, per compiled `sources` record. */
const collidingRollupsBySources = new WeakMap<
   Record<string, PersistSource>,
   ReadonlyMap<string, string>
>();

/**
 * Rollups that would share one table with a DIFFERENT rollup, by sourceID, each
 * with the refusal to report. See `assertRollupNameUnshared`.
 *
 * Grouped by physical target (destination and name, as the collision warning
 * groups them); a group is a collision only when it holds two content
 * addresses. Two models that synthesize the same rollup over the same base
 * share one address, and so one table, legitimately.
 */
export function collidingRollups(compiled: {
   sources: Record<string, PersistSource>;
   targets?: Record<string, BuildTarget>;
   connectionDigests: Record<string, string>;
   preaggregatePlans?: Record<string, RollupPlan>;
   sourceModelPaths?: Record<string, string>;
}): ReadonlyMap<string, string> {
   const memo = collidingRollupsBySources.get(compiled.sources);
   if (memo) return memo;
   const byTarget = new Map<
      string,
      { sourceID: string; address: string; model?: string }[]
   >();
   for (const [sourceID, source] of Object.entries(compiled.sources)) {
      if (!compiled.preaggregatePlans?.[sourceID]) continue;
      // Left out of the plan entirely, so it shares no table with anyone.
      if (storedBaseOfRollup(compiled, sourceID)) continue;
      let address: string;
      try {
         address = computeSourceEntityId(
            source,
            compiled.connectionDigests,
            compiled.targets?.[sourceID],
         );
      } catch {
         // No address: the other gates refuse it on their own.
         continue;
      }
      const fields = deriveAnnotationFields(source);
      const target = `${fields.storage?.trim() || source.connectionName}\u0000${(fields.name || source.name).trim()}`;
      const group = byTarget.get(target) ?? [];
      group.push({
         sourceID,
         address,
         model: compiled.sourceModelPaths?.[sourceID],
      });
      byTarget.set(target, group);
   }
   const colliding = new Map<string, string>();
   for (const group of byTarget.values()) {
      if (new Set(group.map((g) => g.address)).size < 2) continue;
      const models = [
         ...new Set(group.map((g) => g.model?.replace(/#preaggregate$/, ""))),
      ]
         .filter((m): m is string => !!m)
         .sort();
      for (const { sourceID } of group) {
         const name = compiled.sources[sourceID].name;
         colliding.set(
            sourceID,
            `Pre-aggregation rollup '${name}' is generated by more than one ` +
               `model${models.length > 0 ? ` (${models.join(", ")})` : ""} ` +
               `from different sources with the same name and grain, so they ` +
               `would share one table. Neither is built or served from it; each ` +
               `model's queries answer from its base. Rename one of the base ` +
               `sources.`,
         );
      }
   }
   collidingRollupsBySources.set(compiled.sources, colliding);
   return colliding;
}

/** A compiled plan as the tier gate reads it, rollup collisions included. */
export function tierGateInputs(compiled: EligibilityPlan): TierGateInputs {
   return { ...compiled, collidingRollups: collidingRollups(compiled) };
}

/**
 * Which sources may be served from a materialized table, decided here where the
 * compiled sources exist: binding a host's manifest skips the recompile, so the
 * serve side cannot re-derive this.
 *
 * Carries the ELIGIBLE names so the serve side requires a positive result.
 * Absence is not eligibility: a `#@ persist` filtered pass-through (the
 * row-level-access shape) never reaches this record, and a refusal-only view
 * would read that silence as consent. Keyed by source NAME, as serve bindings are.
 *
 * Keyed by `sourceID`, never by name (the source map's keying rule): two models
 * may declare same-named sources, and a synthesized rollup is recorded under
 * its own `sourceID` rather than passing on its generated name.
 */
export type SourceEligibility = {
   /** sourceIDs compiled, examined, and found eligible. */
   eligible: string[];
   /** sourceID -> why it was refused; for the operator-facing log. */
   refused: Record<string, string>;
};

export function collectSourceEligibility(
   compiled: EligibilityPlan,
): SourceEligibility {
   const eligible: string[] = [];
   const refused: Record<string, string> = {};
   const gate = { collidingRollups: collidingRollups(compiled) };
   for (const [sourceID, source] of Object.entries(compiled.sources)) {
      try {
         assertRollupNameUnshared(source, gate);
         assertMaterializationEligible(source, deriveAnnotationFields(source));
         eligible.push(sourceID);
      } catch (err) {
         refused[sourceID] = errMessage(err);
      }
   }
   return { eligible, refused };
}

/**
 * Which sources may be served from a COLOCATED materialized table, decided here
 * for the same reason as {@link SourceEligibility}.
 *
 * Keyed by `sourceEntityId`, not name: a colocated binding carries only the id,
 * and two models may declare same-named sources, so a name key would let one's
 * eligibility authorize the other.
 *
 * Two DIFFERENT sources can share an id (identical SQL, different gates, since
 * the build SQL excludes annotations). Refusal therefore dominates: one refusal
 * retracts the id for good, regardless of iteration order.
 */
export type ColocatedSourceEligibility = {
   /** sourceEntityId of colocated sources examined and found eligible. */
   eligibleEntityIds: Set<string>;
   /** sourceEntityId -> why it was refused; for the operator-facing log. */
   refused: Record<string, string>;
};

export function collectColocatedSourceEligibility(
   compiled: EligibilityPlan,
): ColocatedSourceEligibility {
   const eligibleEntityIds = new Set<string>();
   const refused: Record<string, string> = {};
   for (const [sourceID, source] of Object.entries(compiled.sources)) {
      try {
         assertTierEligible(source, false, tierGateInputs(compiled));
      } catch (err) {
         // Gated BEFORE computeSourceEntityId: the gate never addresses the
         // source, so it refuses cleanly even when addressing would throw.
         let sourceEntityId: string;
         try {
            sourceEntityId = computeSourceEntityId(
               source,
               compiled.connectionDigests,
               compiled.targets?.[sourceID],
            );
         } catch (idErr) {
            // No id to key the refusal under; the log is the only record that
            // it was examined and refused.
            logger.warn(
               "Dropping a refused colocated source: its content address could not be computed",
               {
                  sourceID,
                  sourceName: source.name,
                  refusal: errMessage(err),
                  error: errMessage(idErr),
               },
            );
            continue;
         }
         refused[sourceEntityId] = errMessage(err);
         // Refusal dominates: retract any earlier same-id eligibility.
         eligibleEntityIds.delete(sourceEntityId);
         continue;
      }
      // A given-referencing colocated source's addressing can throw; contain it
      // to this source's binding.
      let sourceEntityId: string;
      try {
         sourceEntityId = computeSourceEntityId(
            source,
            compiled.connectionDigests,
            compiled.targets?.[sourceID],
         );
      } catch (err) {
         logger.warn(
            "Dropping an eligible colocated source: its content address could not be computed",
            { sourceID, sourceName: source.name, error: errMessage(err) },
         );
         continue;
      }
      // A refusal for this id (earlier or later) must win.
      if (!(sourceEntityId in refused)) {
         eligibleEntityIds.add(sourceEntityId);
      }
   }
   return { eligibleEntityIds, refused };
}
