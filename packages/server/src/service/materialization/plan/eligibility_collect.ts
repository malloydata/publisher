// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which planned sources may build and serve, decided where the compiled
 * sources exist: rollups that would share a table, the tier gate's inputs,
 * and the per-tier eligibility records the serve side requires.
 */

import { tableNamesFoldCase } from "../domain/storage_table_path";
import { tableNameParts, tableNamesMeet } from "../domain/table_name";
import type { PersistSource } from "@malloydata/malloy";
import { logger } from "../../../logger";
import { errMessage } from "../../../utils";
import { computeSourceEntityId, type TableFolds } from "./addressing";
import {
   assertBuildReadsNoCurrentTime,
   assertMaterializationEligible,
   assertTierEligible,
   gateEnforcedAtRead,
   storageKeepsOwnGate,
   type TierGateInputs,
} from "./materialization_eligibility";
import {
   MATERIALIZATION_TABLE_COLLISION_CODE,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import type { SourceBuild } from "../domain/compiled_build_plan";
import { materializeRefusalFinding } from "./materialize_plan";
import {
   isPersistedResultSourceID,
   type TablePlan,
} from "../domain/table_plans";
import { deriveAnnotationFields } from "./persist_source_fields";

/**
 * The slice of a compiled build plan eligibility reads; `CompiledBuildPlan`
 * satisfies it. Declared here so this module is not typed against its caller.
 */
interface EligibilityPlan {
   tables: Record<string, TablePlan>;
   connectionDigests: Record<string, string>;
   /** The plan's table folds (CompiledBuildPlan.tableFolds). */
   tableFolds?: TableFolds;
   /** See `CompiledBuildPlan.connectionTypes`. */
   connectionTypes?: Record<string, string>;
   /** See `CompiledBuildPlan.versionScoped` ({@link persistTableTarget}). */
   versionScoped?: boolean;
}

/**
 * The table a persist source writes: its destination and physical name, as
 * the build names it, and the key two sources share exactly when they write
 * one table. The one definition of a persist-target collision, read by the
 * serve refusal ({@link sharedTables}), the `#(materialize)` refusal and the
 * publish check (`persistenceCollisionWarnings`), so the three never disagree.
 *
 * The key is the table the run writes. An author's `name=` in an
 * unversioned or `scope: "package"` package is written exactly as given, so
 * two equal names (folded where the destination folds case,
 * {@link tableNamesFoldCase}) are one table. Every other name is Publisher's
 * readable name (domain/table_name.ts): a name the author did not choose
 * (`defaulted`: no `name=` on a `#@ persist`, or a `#(materialize)` with
 * none), and in a published version of a `scope: "version"` package
 * (`versionScoped`) every name, `name=` included, which then sets only the
 * readable prefix. A readable name ends in
 * a hash of the source's identity (its declaring model and name, and the
 * version), so two different declarations never share one but by a hash
 * collision, which the run refuses (`table_name_collision`); its key is that
 * identity, the source's `sourceID`.
 */
export function persistTableTarget(
   fields: { name?: string; storage?: string },
   sourceName: string,
   connectionName: string,
   written: {
      defaulted?: boolean;
      dialect?: string;
      versionScoped?: boolean;
      /** The source's `sourceID`: the identity its readable name hashes. */
      identity: string;
   },
): {
   key: string;
   name: string;
   destination: string;
   /**
    * The name as compared ({@link tableNamesMeet}): case-folded where the
    * destination folds case. Undefined for a readable name, whose key is
    * its identity.
    */
   written?: string;
} {
   const name = (fields.name || sourceName).trim();
   const storage = fields.storage?.trim();
   const destination = storage || connectionName;
   const readable =
      written.versionScoped || (written.defaulted ?? !fields.name?.trim());
   if (readable) {
      return {
         key: `${destination}\u0000identity\u0000${written.identity}`,
         name,
         destination,
      };
   }
   const folded = tableNamesFoldCase(written.dialect, !!storage)
      ? name.toLowerCase()
      : name;
   // Keyed by the table segment: `main.t` and `t` may be one table, which
   // the group then decides by tableNamesMeet.
   return {
      key: `${destination}\u0000named\u0000${tableNameParts(folded).leaf}`,
      name,
      destination,
      written: folded,
   };
}

/**
 * The members of one key's group that write one table with a member of a
 * different content address ({@link tableNamesMeet} on their written names;
 * a readable name's group is one table by its key).
 */
export function membersOnOneTable<
   T extends { address: string; written?: string },
>(members: readonly T[]): T[] {
   return members.filter((m) =>
      members.some(
         (o) =>
            o.address !== m.address &&
            (m.written === undefined ||
               o.written === undefined ||
               tableNamesMeet(m.written, o.written)),
      ),
   );
}

function physicalTarget(
   source: PersistSource,
   materialize: { nameDefaulted?: true } | undefined,
   versionScoped: boolean | undefined,
) {
   return persistTableTarget(
      deriveAnnotationFields(source),
      source.name,
      source.connectionName,
      {
         ...(materialize ? { defaulted: !!materialize.nameDefaulted } : {}),
         dialect: source.dialectName,
         ...(versionScoped ? { versionScoped } : {}),
         identity: source.sourceID,
      },
   );
}

/** One source sharing a table with another, for {@link sharedTables}. */
interface SharedTableMember {
   sourceID: string;
   /** As its author wrote it: a source's name, or a view's or query's. */
   name: string;
   address: string;
   model?: string;
   /** See {@link persistTableTarget}'s `written`. */
   written?: string;
}

/**
 * Sources that are not rollups, grouped by the table they write, keeping only
 * the groups that hold two content addresses: two DIFFERENT sources writing
 * one table, where the later build overwrites the earlier and each source's
 * reader reads the other's rows. The same source reached through two
 * models' imports is one address, and shares its table legitimately. A
 * rollup's name digests its declaring sourceID, so two rollups never share
 * one.
 */
function sharedTableGroups(
   compiled: EligibilityPlan,
): { table: string; destination: string; members: SharedTableMember[] }[] {
   const byTarget = new Map<
      string,
      { table: string; destination: string; members: SharedTableMember[] }
   >();
   for (const [sourceID, table] of Object.entries(compiled.tables)) {
      const { source } = table;
      if (table.rollup) continue;
      let address: string;
      let target: ReturnType<typeof physicalTarget>;
      try {
         address = computeSourceEntityId(
            source,
            compiled.connectionDigests,
            table.target,
            compiled.tableFolds,
         );
         target = physicalTarget(
            source,
            table.materialize,
            compiled.versionScoped,
         );
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
         name: authorNameOf(sourceID, source.name),
         address,
         model: table.modelPath,
         ...(target.written !== undefined ? { written: target.written } : {}),
      });
      byTarget.set(target.key, group);
   }
   return [...byTarget.values()]
      .map((g) => ({ ...g, members: membersOnOneTable(g.members) }))
      .filter((g) => g.members.length > 1);
}

/**
 * A planned source named as its author wrote it: a persisted view's or
 * query's generated build source (`<source>__<view>__view`,
 * `<query>__query`) as that view or query, every other source by its name.
 */
export function authorNameOf(sourceID: string, name: string): string {
   if (isPersistedResultSourceID(sourceID)) {
      const view = /^(.+)__(.+)__view$/.exec(name);
      if (view) return `view ${view[2]} of ${view[1]}`;
      const query = /^(.+)__query$/.exec(name);
      if (query) return `query ${query[1]}`;
   }
   return name;
}

/** The text a collision's message names a member by: `#@ persist source "x"`. */
function collisionSubjectOf(sourceID: string, name: string): string {
   const author = authorNameOf(sourceID, name);
   const view = /^view (.+) of (.+)$/.exec(author);
   // malloy-text: names quoted in a finding's message, not generated Malloy
   if (view) return `#@ persist on view "${view[1]}" of source "${view[2]}"`;
   const query = /^query (.+)$/.exec(author);
   // malloy-text: names quoted in a finding's message, not generated Malloy
   if (query) return `#@ persist on query "${query[1]}"`;
   return `#@ persist source "${name}"`;
}

/** Each member as `"name" (model)`, sorted, for a collision's message. */
export function describeMembers(
   members: readonly { name: string; model?: string }[],
): string {
   return members
      .map((m) => `"${m.name}"${m.model ? ` (${m.model})` : ""}`)
      .sort()
      .join(", ");
}

/** Memo for {@link sharedTables}, per compiled `tables` record. */
const sharedTablesByTables = new WeakMap<
   Record<string, TablePlan>,
   ReadonlyMap<string, string>
>();

/**
 * Sources that write one table with a DIFFERENT source (see
 * {@link sharedTableGroups}), by sourceID, each with the refusal to report.
 * Neither may be served from that table: it holds whichever build ran last,
 * so serving it hands one source's readers the other's rows. Both are still
 * planned and built; a query on either is answered live until each
 * has a `name=` of its own. Serving is refused whatever
 * `PERSIST_COLLISION_ENFORCE` says: that flag (on by default) decides whether
 * a publish is refused, and a wrong answer is never the alternative.
 */
export function sharedTables(
   compiled: EligibilityPlan,
): ReadonlyMap<string, string> {
   const memo = sharedTablesByTables.get(compiled.tables);
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
   sharedTablesByTables.set(compiled.tables, shared);
   return shared;
}

/**
 * The compiled plan less every `#(materialize)` source whose table is also
 * another source's table (same destination, same physical name, a different
 * content address), each reported as `materialization-table-collision`
 * naming the others, which fails the load (under
 * `PERSIST_COLLISION_ENFORCE=false`, a `materialize-not-built` refusal). Refused outright, not built, whether its name is the default (the
 * source's own, with no `name=`) or a `name=` the author chose:
 * `#(materialize)` has no packages to stay compatible with, and a build over
 * another source's table would hand one source's readers the other's rows.
 * The same source reached through two models is one address, and is no
 * collision. What still shares a table afterwards (v1 sources only) is
 * {@link sharedTables}', served live, and a load failure under
 * `PERSIST_COLLISION_ENFORCE`.
 */
export function refuseCollidingMaterialize<
   T extends EligibilityPlan & {
      builds: SourceBuild[];
      annotationFindings?: PackageAnnotationFinding[];
   },
>(compiled: T): T {
   const refused = new Map<string, PackageAnnotationFinding>();
   for (const { table, destination, members } of sharedTableGroups(compiled)) {
      // Only a group a `#(materialize)` source is in: one of v1 sources alone
      // is the persistence check's (persistenceCollisionWarnings).
      if (!members.some((m) => compiled.tables[m.sourceID]?.materialize)) {
         continue;
      }
      for (const member of members) {
         const plan = compiled.tables[member.sourceID]?.materialize;
         const others = members.filter((m) => m.address !== member.address);
         const named = plan?.nameDefaulted
            ? `with no name=, its table takes the source's name \`${table}\``
            : `its name= \`${table}\` is a table`;
         const reason =
            `${named} in "${destination}", which ${describeMembers(others)} also ` +
            `${others.length > 1 ? "write" : "writes"}, so each build would ` +
            `overwrite the other and one source would read the other's rows; ` +
            `give each source a distinct name=`;
         const model = compiled.tables[member.sourceID]?.modelPath ?? "";
         // An author's names collide: invalid, unless the deployment opts
         // out (PERSIST_COLLISION_ENFORCE=false, applied where the package
         // loads), which refuses each source alone. A `#@ persist` partner is
         // refused with it, so neither side is built.
         refused.set(
            member.sourceID,
            plan
               ? materializeRefusalFinding(
                    {
                       sourceName: compiled.tables[member.sourceID].source.name,
                       sourceID: member.sourceID,
                       reason,
                       malformedCode: MATERIALIZATION_TABLE_COLLISION_CODE,
                    },
                    model,
                 )
               : {
                    route: "@",
                    tier: "malformed",
                    code: MATERIALIZATION_TABLE_COLLISION_CODE,
                    message: `${collisionSubjectOf(member.sourceID, compiled.tables[member.sourceID].source.name)} is invalid: ${reason}.`,
                    subject: compiled.tables[member.sourceID].source.name,
                    model,
                 },
         );
      }
   }
   if (refused.size === 0) return compiled;
   return withoutSources(
      compiled,
      [...refused.values()],
      new Set(refused.keys()),
   );
}

/**
 * The compiled plan less the sources in `drop` (neither built nor served:
 * they answer live), with `findings` added to say why.
 */
export function withoutSources<
   T extends EligibilityPlan & {
      builds: SourceBuild[];
      annotationFindings?: PackageAnnotationFinding[];
   },
>(
   compiled: T,
   findings: PackageAnnotationFinding[],
   drop: ReadonlySet<string>,
): T {
   if (drop.size === 0) return compiled;
   const keep = (sourceID: string) => !drop.has(sourceID);
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
      tables: Object.fromEntries(
         Object.entries(compiled.tables).filter(([id]) => keep(id)),
      ),
      annotationFindings: [...(compiled.annotationFindings ?? []), ...findings],
   };
}

/** A compiled plan as the tier gate reads it. */
export function tierGateInputs(compiled: EligibilityPlan): TierGateInputs {
   return {
      tables: compiled.tables,
      connectionTypes: compiled.connectionTypes,
   };
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
   /**
    * The eligible sourceIDs admitted past their own row-level gate under G3
    * (`gateEnforcedAtRead`), whose bindings the router lets a served answer
    * read past that gate. Absent, as in a record made before it: none.
    */
   gateKept?: string[];
};

export function collectSourceEligibility(
   compiled: EligibilityPlan,
): SourceEligibility {
   const eligible: string[] = [];
   const gateKept: string[] = [];
   const refused: Record<string, string> = {};
   const gate = tierGateInputs(compiled);
   const shared = sharedTables(compiled);
   for (const [sourceID, { source }] of Object.entries(compiled.tables)) {
      const sharing = shared.get(sourceID);
      if (sharing !== undefined) {
         refused[sourceID] = sharing;
         continue;
      }
      try {
         const kept = gateEnforcedAtRead(sourceID, compiled, true);
         assertMaterializationEligible(source, deriveAnnotationFields(source), {
            gateEnforcedAtRead: kept,
            connectionTypes: compiled.connectionTypes,
         });
         // A table built before this refusal existed is not served either.
         assertBuildReadsNoCurrentTime(source, gate);
         eligible.push(sourceID);
         if (kept && storageKeepsOwnGate(source, compiled)) {
            gateKept.push(sourceID);
         }
      } catch (err) {
         refused[sourceID] = errMessage(err);
      }
   }
   return { eligible, refused, gateKept };
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
   for (const [sourceID, { source, target }] of Object.entries(
      compiled.tables,
   )) {
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
               target,
               compiled.tableFolds,
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
            target,
            compiled.tableFolds,
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
