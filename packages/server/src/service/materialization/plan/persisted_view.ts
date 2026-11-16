// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#@ persist` on a view (P14): reading the
 * declarations and writing their build file.
 *
 *     source: ops is duckdb.table('flights') extend {
 *       #@ persist name="ops_by_carrier"
 *       view: by_carrier is { group_by: carrier; aggregate: flight_count }
 *     }
 *
 * stores the view's result, and that table answers a query only when the query
 * calls the view exactly (`run: ops -> by_carrier`), or starts from it and
 * goes on (`run: ops -> by_carrier -> { … }`); never a refinement, a nest, or a
 * hand-written look-alike (the serve side, `serve/view_variants.ts`).
 *
 * The build is generated Malloy, in a companion model beside the declaring
 * file (`<file>.persist-view.malloy`, never on disk and never in the package's
 * model list), which Malloy's own persistence planner builds on v1's pipeline:
 *
 *     ##! experimental.persistence
 *     import { ops__pv_base is ops } from "flights.malloy"
 *     #@ persist name="ops_by_carrier"      // the author's line, verbatim
 *     source: ops__by_carrier__view is ops__pv_base -> by_carrier
 *
 * The companion reads the author's files with every source-level `#@` line
 * blanked (positions kept), so a persisted view never chains onto its source's
 * own table: it reads the rows the live query reads (the same reason the
 * `#(preaggregate)` companion blanks them).
 *
 * Keying: a declaration by its source's `sourceID` and the view's name; the
 * stored table by the generated source's `sourceEntityId` and destination,
 * like every other persist source. Only a source DECLARED in a model is planned
 * there, so a view has one build source and one table however many models
 * import its source.
 *
 * What is refused here, from the declaration alone (each a publish warning,
 * `persist-view-not-built`, and the view serves live): an `@env.` reference on
 * the line, the unsupported `partition=`, an incremental key (`watermark`,
 * `merge_key`; a view's result is rebuilt whole), and two views whose
 * generated names collide. The
 * build plan refuses the rest once it has compiled them (`build_plan.ts`): a
 * view that reads a given or the current time (its result differs per caller
 * or per read), a nested result stored with `storage=` (a destination's
 * `type:` cannot declare it), and a gated source whose gate the view's result
 * cannot keep (question 3).
 *
 * Behind `PUBLISHER_MATERIALIZE`. Every name in the generated text goes
 * through `malloy_text`; the author's own `#@` line is copied verbatim.
 *
 * `#@ persist` on a top-level `query:` (read in `persisted_query.ts`) is
 * planned here too, and built in the same companion, one per declaring file
 * (`generatePersistedResultFile` in `text/read_text.ts`):
 *
 *     ##! experimental.persistence
 *     import { top_carriers, flights } from "flights.malloy"
 *     #@ persist                               // the author's line, verbatim
 *     source: top_carriers__query is top_carriers
 *
 * The `__query` segment is the query marker in the build source's name, and
 * so in its default table name (`<query>__query__p<8 hex>`), so no existing
 * table name moves and a source of the query's name never shares its table.
 *
 * Refused here, from the declaration alone (each a `persist-query-not-built`
 * warning; the query is answered live): an `@env.` reference on the line, an
 * incremental key (a result is rebuilt whole), a line spanning several lines,
 * an empty `storage=`, a run target named in the query's file but declared in
 * another (the overlay that serves it reads one file), and a generated name
 * that collides with a name the file already has. A run target imported from
 * another file compiles into the query whole, as an inline one does. What
 * needs a compile, a gate the query reaches among it, is judged in
 * `persisted_view_compile.ts`.
 */

import { type AnnotationNote, type AnnotationsDef } from "../../annotations";
import {
   byCodepoint,
   PERSIST_VIEW_INVALID_CODE,
   PERSIST_VIEW_UNPARSEABLE_CODE,
   RECOGNIZED_PERSIST_KEYS,
   readRouteTag,
   routeOfNote,
   type PackageAnnotationFinding,
   type RefusalKey,
} from "../../annotation_reader";
import type { RefusedReason } from "../domain/types";
import {
   declaredFieldNotes,
   type NotedSourceDef,
   type SourceScope,
} from "../../governance/ir/declared_notes";
import { Annotations } from "@malloydata/malloy";
import { placementOf, readNotesIfAny } from "./declaration";
import { persistDeclarationProblems } from "./persist_annotation_validation";
import { SourceMap, type SourceNode } from "../text/source_map";
import { persistedViewCompanionHref } from "../text/read_text";
import {
   splitSourceID,
   type PersistedQueryPlan,
   type PersistedViewPlan,
} from "../domain/table_plans";
import {
   declaredPersistQueries,
   INCREMENTAL_KEYS,
   persistQueryNotBuiltFinding,
   persistQueryProblems,
   unparseableNotes,
   writesPersistLine,
} from "./persisted_query";

/** The package warning for a persisted view that is not built, or not served. */
export const PERSIST_VIEW_NOT_BUILT = "persist-view-not-built";

/** A declaration the planner would not build. */
export interface PersistedViewRefusal {
   sourceID: string;
   sourceName: string;
   viewName: string;
   reason: string;
   /**
    * Set when the declaration is invalid (it cannot be read as written, or
    * says something no view can mean): the finding is `malformed` under this
    * code and fails the load, rather than the `persist-view-not-built`
    * refusal of a valid one.
    */
   code?: string;
   /** A valid declaration's refusal reason (`refusedSources`). */
   refusal?: RefusedReason;
   /** The destination its `storage=` names, when it names one. */
   storage?: string;
}

/** The generated build source's name for `source -> view`. */
function persistedViewBuildName(source: string, view: string): string {
   return `${source}__${view}__view`;
}

interface FieldLike {
   type?: unknown;
   name?: unknown;
   as?: unknown;
   annotations?: AnnotationsDef;
}

function fieldName(f: FieldLike): string | undefined {
   return typeof f.as === "string"
      ? f.as
      : typeof f.name === "string"
        ? f.name
        : undefined;
}

/**
 * The `#@` notes the source's own statement declared on a view: never those
 * of a view it inherits (Malloy copies a view onto every extension, rename
 * and import of its source; see {@link declaredFieldNotes}).
 */
function persistNotesOf(
   def: SourceNode["def"],
   view: FieldLike,
   scope: SourceScope,
): AnnotationNote[] {
   return declaredFieldNotes(def as NotedSourceDef, view, scope).filter(
      (n) => routeOfNote(n.text) === "@",
   );
}

/**
 * The persisted views one source declares itself. `scope` holds every source
 * of the compiled model (see {@link declaredFieldNotes}), which tells a view
 * the source's statement annotated from one it inherited.
 */
function planSourcePersistedViews(
   node: SourceNode,
   scope: SourceScope,
): {
   plans: PersistedViewPlan[];
   refused: PersistedViewRefusal[];
} {
   const plans: PersistedViewPlan[] = [];
   const refused: PersistedViewRefusal[] = [];
   const declared = splitSourceID(node.sourceID);
   if (!declared) return { plans, refused };
   for (const f of (node.def.fields ?? []) as FieldLike[]) {
      if (f.type !== "turtle") continue;
      const viewName = fieldName(f);
      if (viewName === undefined) continue;
      // Planned on the source whose statement annotates the view, never one
      // that inherits it.
      const notes = persistNotesOf(node.def, f, scope);
      if (notes.length === 0) continue;
      const refuse = (
         reason: string,
         code: { malformed: string } | { refused: RefusedReason },
      ) =>
         refused.push({
            sourceID: node.sourceID,
            sourceName: declared.name,
            viewName,
            reason,
            ...("malformed" in code
               ? { code: code.malformed }
               : { refusal: code.refused }),
         });
      // A `#@ persist` line that does not parse is invalid, never dropped.
      const unparseable = writesPersistLine(notes)
         ? unparseableNotes(notes)
         : undefined;
      if (unparseable !== undefined) {
         refuse(`its \`#@\` line does not parse (${unparseable})`, {
            malformed: PERSIST_VIEW_UNPARSEABLE_CODE,
         });
         continue;
      }
      const annotations = new Annotations({ notes } as ConstructorParameters<
         typeof Annotations
      >[0]);
      // A line reading `@env.`, a `storage=` naming nothing, a bad `name=`.
      const problems = persistDeclarationProblems(annotations);
      if (problems.length > 0) {
         refuse(problems.join("; "), { malformed: PERSIST_VIEW_INVALID_CODE });
         continue;
      }
      // Read as Malloy reads them: `persist`, not `-persist`.
      const tag = readNotesIfAny(notes, "@");
      if (!tag?.has("persist")) continue;
      // The key set is closed, as on a source: an unknown or retired key
      // (`partition=`) is invalid.
      const keyFindings = readRouteTag(
         annotations,
         "@",
         RECOGNIZED_PERSIST_KEYS,
      ).findings;
      if (keyFindings.length > 0) {
         for (const finding of keyFindings) {
            refuse(
               finding.message +
                  (finding.rewrite ? `; ${finding.rewrite}` : ""),
               { malformed: finding.code },
            );
         }
         continue;
      }
      const incremental = INCREMENTAL_KEYS.filter(
         (k) => tag.has("persist", k) || tag.has(k),
      );
      if (incremental.length > 0) {
         refuse(
            `a view's result is rebuilt whole, so it takes no incremental key (${incremental.join(", ")})`,
            { malformed: PERSIST_VIEW_INVALID_CODE },
         );
         continue;
      }
      // Valid, but a copy limit of Publisher's: refused alone.
      if (notes.some((n) => n.text.trim().includes("\n"))) {
         refuse("its `#@` line spans more than one line", {
            refused: "multi_line_annotation",
         });
         continue;
      }
      const placed = placementOf(tag, "persist");
      if ("refused" in placed) continue;
      const storage =
         placed.placement.tier === "storage"
            ? placed.placement.destination
            : undefined;
      const buildSourceName = persistedViewBuildName(declared.name, viewName);
      plans.push({
         sourceID: node.sourceID,
         sourceName: declared.name,
         viewName,
         buildSourceName,
         buildSourceID: `${buildSourceName}@${persistedViewCompanionHref(declared.url)}`,
         persistLines: notes.map((n) => n.text.trim()),
         ...(storage !== undefined ? { storage } : {}),
         declaringURL: declared.url,
      });
   }
   return { plans, refused };
}

/**
 * Plan every persisted view a compiled model can see, or, with `declaredHere`,
 * only those of the sources it DECLARES (the build plan's view: one build
 * source per view). Two views whose generated names collide are both refused.
 */
export function planModelPersistedViews(
   modelDef: unknown,
   declaredHere?: (declaringURL: string) => boolean,
): { plans: PersistedViewPlan[]; refused: PersistedViewRefusal[] } {
   const plans: PersistedViewPlan[] = [];
   const refused: PersistedViewRefusal[] = [];
   const nodes = [...SourceMap.of(modelDef).nodes()].sort((a, b) =>
      byCodepoint(a.sourceID, b.sourceID),
   );
   const scope = nodes.map((n) => n.def);
   for (const node of nodes) {
      const declaredIn = splitSourceID(node.sourceID)?.url;
      if (!declaredIn) continue;
      if (declaredHere && !declaredHere(declaredIn)) continue;
      const planned = planSourcePersistedViews(node, scope);
      plans.push(...planned.plans);
      refused.push(...planned.refused);
   }
   const byID = new Map<string, PersistedViewPlan[]>();
   for (const p of plans) {
      byID.set(p.buildSourceID, [...(byID.get(p.buildSourceID) ?? []), p]);
   }
   const kept: PersistedViewPlan[] = [];
   for (const group of byID.values()) {
      if (group.length === 1) {
         kept.push(group[0]);
         continue;
      }
      for (const p of group) {
         refused.push({
            sourceID: p.sourceID,
            sourceName: p.sourceName,
            viewName: p.viewName,
            reason: `its generated build source \`${p.buildSourceName}\` collides with another persisted view's; rename one of them`,
            refusal: "build_name_collision",
            ...(p.storage !== undefined ? { storage: p.storage } : {}),
         });
      }
   }
   return { plans: kept, refused };
}

/**
 * A refusal as a package finding: `unservable` for a valid view Publisher
 * refuses alone (it serves live), `malformed` for an invalid declaration,
 * which fails the load.
 */
export function persistedViewRefusalFinding(
   refusal: Pick<
      PersistedViewRefusal,
      | "sourceID"
      | "sourceName"
      | "viewName"
      | "reason"
      | "code"
      | "refusal"
      | "storage"
   >,
   model: string,
): PackageAnnotationFinding {
   // malloy-text: names quoted in a finding's message, not generated Malloy
   const on = `#@ persist on view "${refusal.viewName}" of source "${refusal.sourceName}"`;
   if (refusal.code !== undefined) {
      return {
         route: "@",
         tier: "malformed",
         code: refusal.code,
         message: `${on} is invalid: ${refusal.reason}.`,
         subject: `${refusal.sourceName} -> ${refusal.viewName}`,
         model,
      };
   }
   return {
      route: "@",
      tier: "unservable",
      code: PERSIST_VIEW_NOT_BUILT,
      message: `${on} is not built, so the view is served live: ${refusal.reason}.`,
      subject: `${refusal.sourceName} -> ${refusal.viewName}`,
      model,
      ...(refusal.refusal
         ? {
              refused: resultRefusalKey(
                 persistedViewBuildName(refusal.sourceName, refusal.viewName),
                 splitSourceID(refusal.sourceID)?.url ?? "",
                 refusal.refusal,
                 refusal.storage,
              ),
           }
         : {}),
   };
}

/**
 * The refusal record of a persisted view or query: keyed by the generated
 * build source it would have had.
 */
function resultRefusalKey(
   buildSourceName: string,
   declaringURL: string,
   reason: RefusedReason,
   storage: string | undefined,
): RefusalKey {
   return {
      sourceID: `${buildSourceName}@${persistedViewCompanionHref(declaringURL)}`,
      name: buildSourceName,
      tier: storage !== undefined ? "storage" : "colocated",
      reason,
   };
}

/** A persisted query the planner would not build. */
export interface PersistedQueryRefusal {
   queryName: string;
   reason: string;
   refusal: RefusedReason;
   declaringURL: string;
   storage?: string;
}

/** The generated build source's name for query `q`. */
function persistedQueryBuildName(query: string): string {
   return `${query}__query`;
}

/**
 * Plan every persisted query a compiled model can see, or, with
 * `declaredHere`, only those its admitted files DECLARE (the build plan's
 * view: one build source per query).
 */
export function planModelPersistedQueries(
   modelDef: unknown,
   declaredHere?: (declaringURL: string) => boolean,
): { plans: PersistedQueryPlan[]; refused: PersistedQueryRefusal[] } {
   const plans: PersistedQueryPlan[] = [];
   const refused: PersistedQueryRefusal[] = [];
   const contents =
      (modelDef as { contents?: Record<string, unknown> })?.contents ?? {};
   for (const query of declaredPersistQueries(modelDef, declaredHere)) {
      // An invalid declaration is reported as one (persistQueryInvalidFindings,
      // which fails the load), never planned.
      if (persistQueryProblems(query).length > 0) continue;
      const notes = query.notes;
      const tag = readNotesIfAny(notes, "@");
      if (!tag?.has("persist")) continue;
      const placed = placementOf(tag, "persist");
      if ("refused" in placed) continue;
      const storage =
         placed.placement.tier === "storage"
            ? placed.placement.destination
            : undefined;
      const refuse = (reason: string, refusal: RefusedReason) =>
         refused.push({
            queryName: query.name,
            reason,
            refusal,
            declaringURL: query.declaringURL,
            ...(storage !== undefined ? { storage } : {}),
         });
      // Valid, but a copy limit of Publisher's: refused alone.
      if (notes.some((n) => n.text.trim().includes("\n"))) {
         refuse(
            "its `#@` line spans more than one line",
            "multi_line_annotation",
         );
         continue;
      }
      // The run target: a source declared by name in the query's own file, or
      // one written inline. A name declared in another file needs an overlay
      // of two files to serve, which comes later.
      let runTarget: { sourceID: string; sourceName: string } | undefined;
      const ref = query.def.structRef;
      if (typeof ref === "string") {
         const entry = contents[ref] as { sourceID?: unknown } | undefined;
         const declared =
            typeof entry?.sourceID === "string"
               ? splitSourceID(entry.sourceID)
               : undefined;
         if (!declared) {
            refuse(
               `its source \`${ref}\` is not in this model's namespace; import the file that declares the query whole`,
               "run_target_unsupported",
            );
            continue;
         }
         if (declared.url !== query.declaringURL || declared.name !== ref) {
            refuse(
               `its source \`${ref}\` is declared in another file, and a persisted query is built only over a source its own file declares (declare the query beside its source)`,
               "run_target_unsupported",
            );
            continue;
         }
         runTarget = { sourceID: entry!.sourceID as string, sourceName: ref };
      } else if (!ref || typeof ref !== "object") {
         refuse("its source cannot be read", "run_target_unsupported");
         continue;
      }
      const importedID =
         typeof ref === "object"
            ? (ref as { sourceID?: unknown }).sourceID
            : undefined;
      const importedSource =
         typeof importedID === "string"
            ? Object.entries(contents).find(
                 ([, e]) =>
                    (e as { sourceID?: unknown })?.sourceID === importedID,
              )?.[0]
            : undefined;
      const buildSourceName = persistedQueryBuildName(query.name);
      if (buildSourceName in contents) {
         refuse(
            `its generated build source \`${buildSourceName}\` collides with a name the model already declares; rename one of them`,
            "build_name_collision",
         );
         continue;
      }
      plans.push({
         kind: "query",
         queryName: query.name,
         ...(runTarget ?? {}),
         ...(importedSource !== undefined ? { importedSource } : {}),
         buildSourceName,
         buildSourceID: `${buildSourceName}@${persistedViewCompanionHref(query.declaringURL)}`,
         persistLines: notes.map((n) => n.text.trim()),
         ...(storage !== undefined ? { storage } : {}),
         declaringURL: query.declaringURL,
      });
   }
   return { plans, refused };
}

/** A refusal as a package finding (unservable: the query is answered live). */
export function persistedQueryRefusalFinding(
   refusal: PersistedQueryRefusal,
   model: string,
): PackageAnnotationFinding {
   return persistQueryNotBuiltFinding(
      refusal.queryName,
      model,
      persistedQueryRefusalKey(
         refusal.queryName,
         refusal.declaringURL,
         refusal.refusal,
         refusal.storage,
      ),
      refusal.reason,
   );
}

/** {@link resultRefusalKey} for the named query `queryName`. */
export function persistedQueryRefusalKey(
   queryName: string,
   declaringURL: string,
   reason: RefusedReason,
   storage: string | undefined,
): RefusalKey {
   return resultRefusalKey(
      persistedQueryBuildName(queryName),
      declaringURL,
      reason,
      storage,
   );
}
