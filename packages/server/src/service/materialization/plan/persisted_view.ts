// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#@ persist` on a view (materialization v2, phase 4, P14): reading the
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
 * `#(preaggregate)` companion blanks them, #6293).
 *
 * Keying: a declaration by its source's `sourceID` and the view's name; the
 * stored table by the generated source's `sourceEntityId` and destination,
 * like every other persist source. Only a source DECLARED in a model is planned
 * there, so a view has one build source and one table however many models
 * import its source.
 *
 * What is refused here, from the declaration alone (each a publish warning,
 * `persist-view-not-built`, and the view serves live): an `@env.` reference on
 * the line, an incremental key (`watermark`, `merge_key`, `partition`; a view's
 * result is rebuilt whole), and two views whose generated names collide. The
 * build plan refuses the rest once it has compiled them (`build_plan.ts`): a
 * view that reads a given or the current time (its result differs per caller
 * or per read), a nested result stored with `storage=` (a destination's
 * `type:` cannot declare it), and a gated source whose gate the view's result
 * cannot keep (question 3).
 *
 * Behind `PUBLISHER_MATERIALIZE`. Every name in the generated text goes
 * through `malloy_text`; the author's own `#@` line is copied verbatim.
 */

import { Annotations } from "@malloydata/malloy";
import { type AnnotationNote, type AnnotationsDef } from "../../annotations";
import {
   byCodepoint,
   routeOfNote,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import {
   declaredFieldNotes,
   type NotedSourceDef,
   type SourceScope,
} from "../../governance/ir/declared_notes";
import { doubleQuotedString, identifier } from "../../malloy_text";
import { hasEnvReference, parseRouteTag } from "../../motly";
import { SourceMap, type SourceNode } from "./source_map";

/** Appended to a declaring file's URL to name its persisted-view companion. */
export const PERSISTED_VIEW_COMPANION_SUFFIX = ".persist-view.malloy";

/** The package warning for a persisted view that is not built, or not served. */
export const PERSIST_VIEW_NOT_BUILT = "persist-view-not-built";

/** `#@ persist` keys that make a build incremental: a view rebuilds whole. */
const INCREMENTAL_KEYS = ["watermark", "merge_key", "partition"] as const;

/** One `#@ persist` view, as planned. */
export interface PersistedViewPlan {
   /** The declaring source's `sourceID` (`<name>@<declaring file URL>`). */
   sourceID: string;
   /** The source's name in its declaring file. */
   sourceName: string;
   /** The view's name on the source. */
   viewName: string;
   /** The generated build source's name, inside the companion. */
   buildSourceName: string;
   /** The generated build source's `sourceID`, as the companion compiles it. */
   buildSourceID: string;
   /** The author's `#@` lines on the view, verbatim, trimmed. */
   persistLines: string[];
   /** The `storage=` destination, when the view declares one. */
   storage?: string;
   /** The declaring file's URL. */
   declaringURL: string;
}

/** A declaration the planner would not build. */
export interface PersistedViewRefusal {
   sourceID: string;
   sourceName: string;
   viewName: string;
   reason: string;
}

/** `<name>@<url>` split, or undefined for any other shape. */
export function splitSourceID(
   sourceID: string,
): { name: string; url: string } | undefined {
   const match = /^(.+)@([a-z][a-z0-9+.-]*:.*)$/i.exec(sourceID);
   return match ? { name: match[1], url: match[2] } : undefined;
}

/** The companion URL for a declaring file, as an `href`. */
export function persistedViewCompanionHref(declaringURL: string): string {
   return `${declaringURL}${PERSISTED_VIEW_COMPANION_SUFFIX}`;
}

/** The generated build source's name for `source -> view`. */
function persistedViewBuildName(source: string, view: string): string {
   return `${source}__${view}__view`;
}

/** The alias the companion imports a declaring source under. */
function baseAlias(source: string): string {
   return `${source}__pv_base`;
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

/** Whether `#@` notes declare `persist` (not `-persist`), read as Malloy reads them. */
function declaresPersist(notes: readonly AnnotationNote[]): boolean {
   try {
      const { tag } = parseRouteTag(
         new Annotations({ notes } as ConstructorParameters<
            typeof Annotations
         >[0]),
         "@",
      );
      return tag.has("persist");
   } catch {
      return false;
   }
}

/** The incremental keys the notes set, for the refusal. */
function incrementalKeys(notes: readonly AnnotationNote[]): string[] {
   try {
      const { tag } = parseRouteTag(
         new Annotations({ notes } as ConstructorParameters<
            typeof Annotations
         >[0]),
         "@",
      );
      return INCREMENTAL_KEYS.filter(
         (k) => tag.has("persist", k) || tag.has(k),
      );
   } catch {
      return [];
   }
}

/** The `storage=` the notes set, or undefined. */
function storageOf(notes: readonly AnnotationNote[]): string | undefined {
   try {
      const { tag } = parseRouteTag(
         new Annotations({ notes } as ConstructorParameters<
            typeof Annotations
         >[0]),
         "@",
      );
      // `#@ persist storage="d"` reads `storage` beside `persist`.
      const value = tag.text("persist", "storage") ?? tag.text("storage");
      return typeof value === "string" && value.trim() !== ""
         ? value.trim()
         : undefined;
   } catch {
      return undefined;
   }
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
      const refuse = (reason: string) =>
         refused.push({
            sourceID: node.sourceID,
            sourceName: declared.name,
            viewName,
            reason,
         });
      if (notes.some((n) => hasEnvReference(n.text))) {
         refuse("its `#@` line reads `@env.`, which a persist line may not");
         continue;
      }
      if (!declaresPersist(notes)) continue;
      const incremental = incrementalKeys(notes);
      if (incremental.length > 0) {
         refuse(
            `a view's result is rebuilt whole, so it takes no incremental key (${incremental.join(", ")})`,
         );
         continue;
      }
      if (notes.some((n) => n.text.trim().includes("\n"))) {
         refuse("its `#@` line spans more than one line");
         continue;
      }
      const buildSourceName = persistedViewBuildName(declared.name, viewName);
      const storage = storageOf(notes);
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
         });
      }
   }
   return { plans: kept, refused };
}

/** A refusal as a package finding (unservable: the view serves live). */
export function persistedViewRefusalFinding(
   refusal: Pick<PersistedViewRefusal, "sourceName" | "viewName" | "reason">,
   model: string,
): PackageAnnotationFinding {
   return {
      route: "@",
      tier: "unservable",
      code: PERSIST_VIEW_NOT_BUILT,
      message:
         `#@ persist on view "${refusal.viewName}" of source ` +
         `"${refusal.sourceName}" is not built, so the view is served live: ` +
         `${refusal.reason}.`,
      subject: `${refusal.sourceName} -> ${refusal.viewName}`,
      model,
   };
}

/**
 * The companion's text for the views `plans` declares, all in the file
 * imported as `importPath`, or undefined when there are none.
 */
export function generatePersistedViewFile(
   plans: readonly PersistedViewPlan[],
   importPath: string,
): string | undefined {
   if (plans.length === 0) return undefined;
   const sources = [...new Set(plans.map((p) => p.sourceName))].sort(
      byCodepoint,
   );
   const imports = sources
      .map(
         (s) =>
            `import { ${identifier(baseAlias(s))} is ${identifier(s)} } from ${doubleQuotedString(importPath)}`,
      )
      .join("\n");
   const sourcesText = plans
      .map(
         (p) =>
            // malloy-text: the author's own `#@` lines, verbatim (single lines, no `@env.`; see planSourcePersistedViews)
            `${p.persistLines.join("\n")}
source: ${identifier(p.buildSourceName)} is ${identifier(baseAlias(p.sourceName))} -> ${identifier(p.viewName)}`,
      )
      .join("\n\n");
   return `##! experimental.persistence
${imports}

${sourcesText}
`;
}
