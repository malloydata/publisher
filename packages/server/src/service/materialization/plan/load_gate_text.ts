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
   PREAGGREGATE_INVALID_CODE,
   type AnnotationFinding,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import type {
   PreaggregateViolation,
   PreaggregateViolationCode,
} from "./preaggregation_validation";
import type { components } from "../../../api";
import { PERSISTED_VIEW_COMPANION_SUFFIX } from "../text/read_text";
import type { MaterializePlan } from "../domain/table_plans";
import {
   authorNameOf,
   describeMembers,
   membersOnOneTable,
   persistTableTarget,
} from "./eligibility_collect";
import type { IncrementalDeclaration } from "../domain/incremental";
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
   /**
    * The models' own invalid `#@ preaggregate` and near-miss lines
    * ({@link preaggregateGateLines}, {@link nearMissGateLines}), as the plan
    * collected them from the files.
    */
   modelGateLines?: { preaggregate: string[]; nearMiss: string[] };
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
 * A table collision found as the plan is refused alone (a `#(materialize)`
 * source, or a `#@ persist` partner on its table) as this deployment judges
 * it: invalid (it fails the load) by default, or, under
 * `PERSIST_COLLISION_ENFORCE=false`, that source refused alone under its
 * own not-built code.
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
      // Each source under its own not-built code.
      code:
         finding.route !== "@"
            ? "materialize-not-built"
            : finding.message.startsWith("#@ persist on view")
              ? "persist-view-not-built"
              : finding.message.startsWith("#@ persist on query")
                ? "persist-query-not-built"
                : PERSIST_NOT_BUILT_CODE,
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
export function refusalCodeOf(sourceID: string, name: string): string {
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
): {
   /** The opt-out's warning: each refused alone and answered live. */
   message: string;
   /** The load failure's line, when enforced. */
   enforced: string;
   code: string;
   subject: string;
}[] {
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
            name: authorNameOf(sourceID, source.name),
            ...(source.modelPath ? { model: source.modelPath } : {}),
            ...(target.written !== undefined
               ? { written: target.written }
               : {}),
            kind: refusalCodeOf(sourceID, source.name),
         });
      }
      targets.set(target.key, bucket);
   }
   const out: {
      message: string;
      enforced: string;
      code: string;
      subject: string;
   }[] = [];
   for (const { name, destination, members } of targets.values()) {
      const colliding = membersOnOneTable(members);
      if (colliding.length < 2) continue;
      const kinds = new Set(colliding.map((m) => m.kind));
      const what =
         `#@ persist sources ${describeMembers(colliding)} all resolve ` +
         `to the same materialized table "${name}" in destination ` +
         `"${destination}". Distinct sources must not share a physical ` +
         `target — the second build would overwrite the first, and a ` +
         `serve binding or GC drop for one would affect the other, so `;
      out.push({
         // Refused alone under the opt-out: a view's or a query's own
         // not-built code when every member is one, else a source's.
         code: kinds.size === 1 ? [...kinds][0] : PERSIST_NOT_BUILT_CODE,
         subject: colliding.map((m) => m.name).sort()[0],
         message:
            `${what}none is built or served from it and queries on them are ` +
            `answered live. Give each a distinct #@ persist name=.`,
         enforced:
            `${what}the package does not load until each has a distinct ` +
            `#@ persist name= (a deployment that must load it anyway can ` +
            `set PERSIST_COLLISION_ENFORCE=false, which refuses the ` +
            `colliding sources alone and serves them live).`,
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

/** The collisions as load-failure lines, each ending with its code. */
export function collisionGateLines(
   collisions: readonly { enforced: string }[],
): string[] {
   return collisions.map((c) =>
      codedLine(c.enforced, MATERIALIZATION_TABLE_COLLISION_CODE),
   );
}

/**
 * The load-failure text of a derived plan, or "" when it is valid: its
 * invalid findings, its collisions (when enforced) and its invalid
 * incremental declarations, then the models' invalid `#@ preaggregate`
 * declarations and near misses as the plan collected them: the same text, in
 * the same order, as the load's (Package.formatLoadGateText).
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
      ...(gated.modelGateLines?.preaggregate ?? []),
      ...(gated.modelGateLines?.nearMiss ?? []),
   ].join("\n");
}

/**
 * `#@ preaggregate` violations of a VALID declaration Publisher cannot build
 * here: that rollup alone is refused with a warning (its code here), its
 * measures answered live, and the package loads. Every other violation is an
 * invalid declaration and fails the load (see formatInvalidPreaggregatePolicy).
 * The planner skips each of these itself, so nothing is built.
 */
export const PREAGG_WARN_ONLY: ReadonlyMap<PreaggregateViolationCode, string> =
   new Map([
      // Governance: an extension that hides an inherited field cannot fix
      // its base's annotation.
      ["non_public_measure", "preaggregate-hidden-field"],
      ["grain_dimension_not_public", "preaggregate-hidden-field"],
      // A rollup over a fan-out join, as `#(preaggregate)` refuses one.
      ["base_source_has_fanout_join", "preaggregate-join-level"],
   ]);

/**
 * The code a `#@ preaggregate` violation that fails the load is named by, the
 * same as `#(preaggregate)`'s for the same mistake.
 */
function preaggregateViolationCode(code: PreaggregateViolationCode): string {
   if (code === "unknown_key") return "preaggregate-unknown-key";
   if (code === "unparseable") return "preaggregate-unparseable";
   if (code === "non_additive_measure") return "preaggregate-not-mergeable";
   if (code.startsWith("misplaced_")) return "preaggregate-not-a-measure";
   if (
      code === "unknown_grain_dimension" ||
      code.startsWith("grain_") ||
      code === "truncation_on_non_temporal"
   ) {
      return "preaggregate-unknown-grain";
   }
   return PREAGGREGATE_INVALID_CODE;
}

/**
 * The invalid `#@ preaggregate` declarations of one model, one line each
 * with its code (rules in preaggregation_validation); the ones Publisher
 * refuses alone ({@link PREAGG_WARN_ONLY}) only warn.
 */
export function preaggregateGateLines(
   modelPath: string,
   violations: readonly Readonly<PreaggregateViolation>[],
): string[] {
   return violations
      .filter((v) => !PREAGG_WARN_ONLY.has(v.code))
      .map((v) =>
         // Source names repeat across models; name the file.
         codedLine(
            `${modelPath}: ${v.message}`,
            preaggregateViolationCode(v.code),
         ),
      );
}

/**
 * One model's misspelled materialization routes and directives, one line
 * each with its code (annotation_near_miss); every other near miss only
 * warns.
 */
export function nearMissGateLines(
   modelPath: string,
   findings: readonly AnnotationFinding[],
): string[] {
   return findings
      .filter((f) => f.tier === "malformed")
      .map((f) =>
         codedLine(
            `${modelPath}: ${f.message}${f.at ? ` (line ${f.at.range.start.line + 1})` : ""}; ${f.rewrite}`,
            f.code,
         ),
      );
}
