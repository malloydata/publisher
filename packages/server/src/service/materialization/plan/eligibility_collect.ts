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
   gateEnforcedAtRead,
   type TierGateInputs,
} from "./materialization_eligibility";
import type { PackageAnnotationFinding } from "../../annotation_reader";
import type { SourceBuild } from "./addressing";
import {
   materializeRefusalFinding,
   type MaterializePlan,
} from "./build_file_generator";
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
interface EligibilityPlan {
   sources: Record<string, PersistSource>;
   targets?: Record<string, BuildTarget>;
   connectionDigests: Record<string, string>;
   preaggregatePlans?: Record<string, RollupPlan>;
   /** The `#(materialize)` plans, by the author's sourceID. */
   materializePlans?: Record<string, unknown>;
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
 * and serves from storage. A `#(preaggregate)` rollup over a stored base
 * builds and serves (materialization v2 phase 4), so it is never named here.
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
   // A `#(preaggregate)` rollup builds over the author's rows, not its base's
   // table, and serves as its own router variant: the real fix (#6293).
   if ((plan as { variant?: unknown } | undefined)?.variant === true) {
      return undefined;
   }
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

/**
 * The table a persist source writes: its destination and physical name, as
 * the build names it, and the key two sources share exactly when they write
 * one table. The one definition of a persist-target collision, read by the
 * serve refusal ({@link sharedTables}), the `#(materialize)` refusal and the
 * publish check (`persistenceCollisionWarnings`), so the three never disagree.
 * Verbatim: names differing only by case or quoting may still fold to one
 * table on some destinations, and normalizing is dialect-dependent.
 */
export function persistTableTarget(
   fields: { name?: string; storage?: string },
   sourceName: string,
   connectionName: string,
): { key: string; name: string; destination: string } {
   const name = (fields.name || sourceName).trim();
   const destination = fields.storage?.trim() || connectionName;
   return { key: `${destination}\u0000${name}`, name, destination };
}

function physicalTarget(source: PersistSource) {
   return persistTableTarget(
      deriveAnnotationFields(source),
      source.name,
      source.connectionName,
   );
}

/** One source sharing a table with another, for {@link sharedTables}. */
interface SharedTableMember {
   sourceID: string;
   name: string;
   address: string;
   model?: string;
}

/**
 * Sources that are not rollups, grouped by the table they write, keeping only
 * the groups that hold two content addresses: two DIFFERENT sources writing
 * one table, where the later build overwrites the earlier and each source's
 * reader reads the other's rows (#6644). The same source reached through two
 * models' imports is one address, and shares its table legitimately. Rollups
 * are {@link collidingRollups}'.
 */
function sharedTableGroups(
   compiled: EligibilityPlan,
): { table: string; destination: string; members: SharedTableMember[] }[] {
   const byTarget = new Map<
      string,
      { table: string; destination: string; members: SharedTableMember[] }
   >();
   for (const [sourceID, source] of Object.entries(compiled.sources)) {
      if (compiled.preaggregatePlans?.[sourceID]) continue;
      let address: string;
      let target: ReturnType<typeof physicalTarget>;
      try {
         address = computeSourceEntityId(
            source,
            compiled.connectionDigests,
            compiled.targets?.[sourceID],
         );
         target = physicalTarget(source);
      } catch {
         // No address: the other gates refuse it on their own.
         continue;
      }
      const group = byTarget.get(target.key) ?? {
         table: target.name,
         destination: target.destination,
         members: [],
      };
      group.members.push({
         sourceID,
         name: source.name,
         address,
         model: compiled.sourceModelPaths?.[sourceID],
      });
      byTarget.set(target.key, group);
   }
   return [...byTarget.values()].filter(
      (g) => new Set(g.members.map((m) => m.address)).size > 1,
   );
}

function describeMembers(members: readonly SharedTableMember[]): string {
   return members
      .map((m) => `"${m.name}"${m.model ? ` (${m.model})` : ""}`)
      .sort()
      .join(", ");
}

/** Memo for {@link sharedTables}, per compiled `sources` record. */
const sharedTablesBySources = new WeakMap<
   Record<string, PersistSource>,
   ReadonlyMap<string, string>
>();

/**
 * Sources that write one table with a DIFFERENT source (see
 * {@link sharedTableGroups}), by sourceID, each with the refusal to report.
 * Neither may be served from that table: it holds whichever build ran last,
 * so serving it hands one source's readers the other's rows. Both are still
 * planned and built, as before; a query on either is answered live until each
 * has a `name=` of its own. Serving is refused whatever
 * `PERSIST_COLLISION_ENFORCE` says: that flag (on by default) decides whether
 * a publish is refused, and a wrong answer is never the alternative.
 */
export function sharedTables(
   compiled: EligibilityPlan,
): ReadonlyMap<string, string> {
   const memo = sharedTablesBySources.get(compiled.sources);
   if (memo) return memo;
   const shared = new Map<string, string>();
   for (const { table, destination, members } of sharedTableGroups(compiled)) {
      for (const member of members) {
         shared.set(
            member.sourceID,
            `Sources ${describeMembers(members)} all write the table ` +
               `"${table}" in destination "${destination}", so it holds ` +
               `whichever was built last; none is served from it, and queries ` +
               `on them are answered live. Give each a distinct name=.`,
         );
      }
   }
   sharedTablesBySources.set(compiled.sources, shared);
   return shared;
}

/**
 * The compiled plan less every `#(materialize)` source whose table is also
 * another source's table (same destination, same physical name, a different
 * content address), each reported as `materialize-not-built` naming the
 * others. Refused outright, not built, whether its name is the default (the
 * source's own, with no `name=`) or a `name=` the author chose:
 * `#(materialize)` has no packages to stay compatible with, and a build over
 * another source's table would hand one source's readers the other's rows.
 * The same source reached through two models is one address, and is no
 * collision. What still shares a table afterwards (v1 sources only) is
 * {@link sharedTables}', served live, and refused at publish under
 * `PERSIST_COLLISION_ENFORCE`.
 */
export function refuseCollidingMaterialize<
   T extends EligibilityPlan & {
      builds: SourceBuild[];
      materializePlans?: Record<string, MaterializePlan>;
      annotationFindings?: PackageAnnotationFinding[];
   },
>(compiled: T): T {
   const plans = compiled.materializePlans ?? {};
   const refused = new Map<string, string>();
   for (const { table, destination, members } of sharedTableGroups(compiled)) {
      for (const member of members) {
         const plan = plans[member.sourceID];
         if (!plan) continue;
         const others = members.filter((m) => m.address !== member.address);
         const named = plan.nameDefaulted
            ? `with no name=, its table takes the source's name \`${table}\``
            : `its name= \`${table}\` is a table`;
         refused.set(
            member.sourceID,
            `${named} in "${destination}", which ${describeMembers(others)} also ` +
               `${others.length > 1 ? "write" : "writes"}, so each build would ` +
               `overwrite the other and one source would read the other's rows; ` +
               `give each source a distinct name=`,
         );
      }
   }
   if (refused.size === 0) return compiled;
   const keep = (sourceID: string) => !refused.has(sourceID);
   const without = <V>(record: Record<string, V> | undefined) =>
      record &&
      Object.fromEntries(Object.entries(record).filter(([id]) => keep(id)));
   const findings = [...(compiled.annotationFindings ?? [])];
   for (const [sourceID, reason] of refused) {
      findings.push(
         materializeRefusalFinding(
            {
               sourceName: compiled.sources[sourceID].name,
               sourceID,
               reason,
            },
            compiled.sourceModelPaths?.[sourceID] ?? "",
         ),
      );
   }
   return {
      ...compiled,
      builds: compiled.builds
         .map((build) => ({
            ...build,
            order: build.order.filter(keep),
            dependsOn: Object.fromEntries(
               Object.entries(build.dependsOn)
                  .filter(([id]) => keep(id))
                  .map(([id, upstream]) => [id, upstream.filter(keep)]),
            ),
         }))
         .filter((build) => build.order.length > 0),
      sources: without(compiled.sources)!,
      targets: without(compiled.targets),
      sourceModelPaths: without(compiled.sourceModelPaths),
      sourceGateOutcomes: without(compiled.sourceGateOutcomes),
      materializePlans: without(compiled.materializePlans),
      annotationFindings: findings,
   };
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
   const shared = sharedTables(compiled);
   for (const [sourceID, source] of Object.entries(compiled.sources)) {
      const sharing = shared.get(sourceID);
      if (sharing !== undefined) {
         refused[sourceID] = sharing;
         continue;
      }
      try {
         assertRollupNameUnshared(source, gate);
         assertMaterializationEligible(source, deriveAnnotationFields(source), {
            gateEnforcedAtRead: gateEnforcedAtRead(sourceID, compiled),
         });
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
   const shared = sharedTables(compiled);
   for (const [sourceID, source] of Object.entries(compiled.sources)) {
      try {
         assertTierEligible(source, false, tierGateInputs(compiled));
         const sharing = shared.get(sourceID);
         if (sharing !== undefined) throw new Error(sharing);
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
