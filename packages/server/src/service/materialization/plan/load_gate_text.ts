// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one text a package's invalid materialization annotations fail with,
 * read off a derived build plan: the load gate, the run's guard over the
 * files on disk, and a connection edit's re-derivation all read it here, so
 * the three never disagree on what is invalid.
 *
 * It carries every `malformed` finding (the invalid annotations, near misses
 * excepted, which are the loaded models'), two sources an author's `name=`
 * puts on one table (unless `PERSIST_COLLISION_ENFORCE=false`), and the
 * invalid incremental declarations, each line with its code.
 */

import {
   codedLine,
   MATERIALIZATION_TABLE_COLLISION_CODE,
   MATERIALIZE_INVALID_CODE,
   PERSIST_INVALID_CODE,
   PERSIST_NOT_BUILT_CODE,
   PERSIST_VIEW_INVALID_CODE,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import type { components } from "../../../api";
import { PERSISTED_VIEW_COMPANION_SUFFIX } from "../text/read_text";
import type { MaterializePlan } from "./build_file_generator";
import {
   describeMembers,
   membersOnOneTable,
   persistTableTarget,
} from "./eligibility_collect";
import type { IncrementalDeclaration } from "./incremental_declaration";
import {
   incrementalPolicyRejections,
   type IncrementalPolicySource,
} from "./incremental_policy";

type BuildPlan = components["schemas"]["BuildPlan"];

/** What the gate text reads of a derived plan. */
interface GatedPlan {
   plan: BuildPlan | null;
   annotationFindings: readonly PackageAnnotationFinding[];
   incrementalDeclarations: Readonly<Record<string, IncrementalDeclaration>>;
   materializePlans?: Readonly<Record<string, MaterializePlan>>;
}

/** How this deployment judges the plan. */
interface GateJudgement {
   /** `PERSIST_COLLISION_ENFORCE`: a collision fails the load. */
   enforceCollisions: boolean;
   /** A published version of a `scope: "version"` package. */
   versionScoped: boolean;
   /** A persisted view's build source, by sourceID, named as its author wrote it. */
   persistedViews?: ReadonlyMap<
      string,
      { sourceName: string; viewName: string }
   >;
}

/**
 * A `#(materialize)` table collision as this deployment judges it: invalid
 * (it fails the load) by default, or, under `PERSIST_COLLISION_ENFORCE=false`,
 * that source refused alone, as `materialize-not-built`.
 */
export function relaxedCollision(
   finding: PackageAnnotationFinding,
   enforceCollisions: boolean,
): PackageAnnotationFinding {
   if (
      finding.code !== MATERIALIZATION_TABLE_COLLISION_CODE ||
      enforceCollisions
   ) {
      return finding;
   }
   return {
      ...finding,
      tier: "unservable",
      code: "materialize-not-built",
      message: finding.message.replace(
         " is invalid: ",
         " is not built, so the source is served live: ",
      ),
   };
}

/** Each `malformed` finding as a load failure's line, with its code. */
export function invalidFindingLines(
   findings: readonly PackageAnnotationFinding[],
): string[] {
   return findings
      .filter((finding) => finding.tier === "malformed")
      .map((finding) =>
         codedLine(
            `${finding.model ? `${finding.model}: ` : ""}${finding.message}` +
               (finding.rewrite ? `; ${finding.rewrite}` : ""),
            finding.code,
         ),
      );
}

/** The not-built code of a planned source: a persisted view's, a query's, or a source's. */
function refusalCodeOf(sourceID: string, name: string): string {
   const url = sourceID.slice(sourceID.indexOf("@") + 1);
   if (url.endsWith(PERSISTED_VIEW_COMPANION_SUFFIX)) {
      if (name.endsWith("__view")) return "persist-view-not-built";
      if (name.endsWith("__query")) return "persist-query-not-built";
   }
   return PERSIST_NOT_BUILT_CODE;
}

/**
 * Within-package persist-target COLLISIONS: distinct sources (by
 * `sourceEntityId`) an author's `name=` puts on one physical table in one
 * destination (persistTableTarget, tableNamesMeet), where each build would
 * overwrite the other. Imports of the same source are not flagged. The
 * destination comes from the DECLARED annotation, so the storage kill switch
 * never hides a collision. Each carries the not-built code its sources are
 * refused under when the deployment opts out.
 */
export function planCollisions(
   plan: BuildPlan | null,
   materializePlans: Readonly<Record<string, MaterializePlan>> | undefined,
   versionScoped: boolean,
): { message: string; code: string; subject: string }[] {
   type Member = {
      address: string;
      name: string;
      model?: string;
      written?: string;
      kind: string;
   };
   const targets = new Map<
      string,
      { name: string; destination: string; members: Member[] }
   >();
   for (const [sourceID, source] of Object.entries(plan?.sources ?? {})) {
      const materialize = materializePlans?.[sourceID];
      const target = persistTableTarget(
         source.annotationFields ?? {},
         source.name,
         source.connectionName,
         {
            ...(materialize ? { defaulted: !!materialize.nameDefaulted } : {}),
            ...(source.dialect ? { dialect: source.dialect } : {}),
            ...(versionScoped ? { versionScoped: true } : {}),
            identity: sourceID,
         },
      );
      const bucket = targets.get(target.key) ?? {
         name: target.name,
         destination: target.destination,
         members: [],
      };
      // One member per content address: identical-content imports are one
      // source.
      if (!bucket.members.some((m) => m.address === source.sourceEntityId)) {
         bucket.members.push({
            address: source.sourceEntityId,
            name: source.name,
            ...(source.modelPath ? { model: source.modelPath } : {}),
            ...(target.written !== undefined
               ? { written: target.written }
               : {}),
            kind: refusalCodeOf(sourceID, source.name),
         });
      }
      targets.set(target.key, bucket);
   }
   const out: { message: string; code: string; subject: string }[] = [];
   for (const { name, destination, members } of targets.values()) {
      const colliding = membersOnOneTable(members);
      if (colliding.length < 2) continue;
      const kinds = new Set(colliding.map((m) => m.kind));
      out.push({
         // Refused alone under the opt-out: a view's or a query's own
         // not-built code when every member is one, else a source's.
         code: kinds.size === 1 ? [...kinds][0] : PERSIST_NOT_BUILT_CODE,
         subject: colliding.map((m) => m.name).sort()[0],
         message:
            `#@ persist sources ${describeMembers(colliding)} all resolve ` +
            `to the same materialized table "${name}" in destination ` +
            `"${destination}". Distinct sources must not share a physical ` +
            `target — the second build would overwrite the first, and a ` +
            `serve binding or GC drop for one would affect the other, so ` +
            `none is built or served from it and queries on them are ` +
            `answered live. Give each a distinct #@ persist name=.`,
      });
   }
   return out;
}

/** The incremental gate's inputs, one per planned source that declares one. */
export function incrementalPolicySourcesOf(
   plan: BuildPlan | null,
   declarations: Readonly<Record<string, IncrementalDeclaration>>,
   materializePlans: Readonly<Record<string, MaterializePlan>> | undefined,
   persistedViews: ReadonlyMap<
      string,
      { sourceName: string; viewName: string }
   > = new Map(),
): IncrementalPolicySource[] {
   return Object.entries(plan?.sources ?? {})
      .filter(([sourceID]) => declarations[sourceID])
      .map(([sourceID, source]) => ({
         ...(persistedViews.get(sourceID) ?? { sourceName: source.name }),
         invalidCode: persistedViews.has(sourceID)
            ? PERSIST_VIEW_INVALID_CODE
            : materializePlans?.[sourceID]
              ? MATERIALIZE_INVALID_CODE
              : PERSIST_INVALID_CODE,
         ...(source.modelPath ? { modelPath: source.modelPath } : {}),
         ...(source.dialect ? { dialect: source.dialect } : {}),
         ...(source.annotationFields?.storage
            ? { storageDestination: source.annotationFields.storage }
            : {}),
         // So the gate can spot two sources resolving to one table.
         sourceEntityId: source.sourceEntityId,
         declaration: declarations[sourceID],
      }));
}

/** The invalid incremental declarations, one line each with its route's code. */
export function incrementalGateLines(
   sources: readonly IncrementalPolicySource[],
): string[] {
   return sources.flatMap((source) =>
      incrementalPolicyRejections([source]).map((m) =>
         codedLine(m, source.invalidCode ?? PERSIST_INVALID_CODE),
      ),
   );
}

/** The collisions as load-failure lines, with the operator's opt-out named once. */
export function collisionGateLines(
   collisions: readonly { message: string }[],
): string[] {
   if (collisions.length === 0) return [];
   return [
      ...collisions.map((c) =>
         codedLine(c.message, MATERIALIZATION_TABLE_COLLISION_CODE),
      ),
      "A deployment that must load such a package anyway can set " +
         "PERSIST_COLLISION_ENFORCE=false, which refuses the colliding " +
         "sources alone and serves them live.",
   ];
}

/**
 * The load-failure text of a derived plan, or "" when it is valid: its
 * invalid findings, its collisions (when enforced) and its invalid
 * incremental declarations. The `#@ preaggregate` violations and the near
 * misses are the loaded models', which the load adds.
 */
export function loadGateText(
   gated: GatedPlan,
   judgement: GateJudgement,
): string {
   return [
      ...invalidFindingLines(
         gated.annotationFindings.map((f) =>
            relaxedCollision(f, judgement.enforceCollisions),
         ),
      ),
      ...(judgement.enforceCollisions
         ? collisionGateLines(
              planCollisions(
                 gated.plan,
                 gated.materializePlans,
                 judgement.versionScoped,
              ),
           )
         : []),
      ...incrementalGateLines(
         incrementalPolicySourcesOf(
            gated.plan,
            gated.incrementalDeclarations,
            gated.materializePlans,
            judgement.persistedViews,
         ),
      ),
   ].join("\n");
}
