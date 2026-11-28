// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The index query guard: whether an index query (`queryClass: index`, the
 * dimensional-index pipeline's) that the router sent live must be refused
 * instead, because the source it queries declares a stored table of its own
 * and none is ready. An index query reads every distinct value of its
 * source's dimensions, which on a large warehouse table is a full scan; a
 * source that declares a table is answered from that table or not at all.
 *
 * Only the queried source's OWN declaration counts: `#(materialize)` on its
 * statement, or `#@ persist` on its statement. A source that inherits one (an
 * extension of a stored source, which Malloy copies the base's notes onto)
 * declares nothing here and answers as before. The query pipeline asks this
 * only after governance has admitted the caller (`model_serving.ts`), so the
 * refusal never reveals a source the caller may not query.
 *
 * A cost guard on the run target's own declaration, not a guarantee for
 * every spelling of the same data: an import rename keeps the sourceID and
 * is refused (under the name the caller used), but a model-file rename
 * (`source: r is s`, a new sourceID), an inline `extend` in the query (which
 * carries no sourceID) and a `compose(…)` resolving to a stored member are
 * answered live.
 *
 * Pure: reads the compiled query, the model's definition, the query's pin
 * and the plan's refusals, and changes nothing.
 */

import { Annotations, type ModelDef } from "@malloydata/malloy";
import type { IndexSourceUnbuiltReason } from "../errors";
import { routeOfNote } from "./annotation_reader";
import type { AnnotationNote } from "./annotations";
import {
   declaredNotes,
   type NotedSourceDef,
} from "./governance/ir/declared_notes";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "./governance/ir/prepared_query";
import type { Pinned } from "./materialization/domain/binding";
import { routeHasKey } from "./motly";

/** What the guard reads of the package's build plan, by `sourceID`. */
export interface IndexGuardPlan {
   /** The content address the plan gives a planned source, if it plans it. */
   plannedEntityId(sourceID: string): string | undefined;
   /** Whether the plan refused to build this declaration. */
   refused(sourceID: string): boolean;
}

/** An index query to refuse: the source as the caller queried it, and why. */
export interface UnbuiltIndexSource {
   sourceName: string;
   unbuiltReason: IndexSourceUnbuiltReason;
}

/**
 * Whether `notes` (one statement's own) declare `#@ persist`, or
 * `#(materialize)` while materialization v2 is on (off, it is read as not
 * written, and the plan is what it was without it).
 */
function declaresStoredTable(
   notes: readonly AnnotationNote[],
   materializeEnabled: boolean,
): boolean {
   if (
      materializeEnabled &&
      notes.some((note) => routeOfNote(note.text) === "materialize")
   ) {
      return true;
   }
   const persistRoute = notes.filter((note) => routeOfNote(note.text) === "@");
   if (persistRoute.length === 0) return false;
   try {
      return routeHasKey(
         new Annotations({ notes: persistRoute } as ConstructorParameters<
            typeof Annotations
         >[0]),
         "@",
         "persist",
      );
   } catch {
      // Unparseable: the load would have refused an invalid declaration, so
      // a line that reads nothing declares nothing here.
      return false;
   }
}

/**
 * The run target's declaration in the model, under the name the caller's
 * query used for it. An import rename (`import { pub is secret }`) keeps the
 * original `sourceID`, so several `contents` entries can carry one: the
 * name is the one the query referenced (a `structRef` string, else the
 * struct's `as`), never another entry's, which may be off the surface. The
 * def is that entry (the very object {@link declaredNotes} compares
 * identities against), else the only entry carrying the `sourceID`, else
 * the compiled struct itself. Undefined when no name the caller used can be
 * told, so nothing is refused under a name the caller could not query.
 */
function declarationOf(
   structRef: unknown,
   struct: { sourceID?: unknown; as?: unknown } | undefined,
   modelDef: ModelDef,
): { name: string; def: NotedSourceDef; sourceID: string } | undefined {
   const sourceID = struct?.sourceID;
   if (typeof sourceID !== "string") return undefined;
   const contents = modelDef.contents ?? {};
   const carries = (entry: unknown) =>
      (entry as { sourceID?: unknown } | undefined)?.sourceID === sourceID;
   // Never the struct's `name`: on a table source it is the physical table.
   const referenced =
      typeof structRef === "string"
         ? structRef
         : typeof struct?.as === "string"
           ? struct.as
           : undefined;
   if (referenced !== undefined && carries(contents[referenced])) {
      return {
         name: referenced,
         def: contents[referenced] as NotedSourceDef,
         sourceID,
      };
   }
   const matches = Object.entries(contents).filter(([, entry]) =>
      carries(entry),
   );
   if (referenced === undefined) {
      return matches.length === 1
         ? {
              name: matches[0][0],
              def: matches[0][1] as NotedSourceDef,
              sourceID,
           }
         : undefined;
   }
   return {
      name: referenced,
      def: (matches.length === 1 ? matches[0][1] : struct) as NotedSourceDef,
      sourceID,
   };
}

/**
 * Whether a ready table of `sourceID` is bound at the pin: a binding storing
 * it (fresh or past its window: a table past its window is built, and the
 * served copy's decline of it is case (b), held), or its planned content
 * address in the bound database entries. A table a failed reload withdrew is
 * not ready: it waits on a rebuild.
 */
function boundAtPin(
   sourceID: string,
   pinned: Pinned,
   plan: IndexGuardPlan | undefined,
): boolean {
   const { snapshot } = pinned;
   const withdrawn = snapshot.withdrawn;
   if (
      snapshot.bindings.some(
         (b) =>
            (b.kind === "materialize" || b.kind === "persist") &&
            (b.sourceIDs ?? []).includes(sourceID) &&
            !withdrawn?.has(b.virtualHandle),
      )
   ) {
      return true;
   }
   const entityId = plan?.plannedEntityId(sourceID);
   return (
      entityId !== undefined &&
      entityId in snapshot.entries &&
      !withdrawn?.has(entityId)
   );
}

/**
 * The refusal for an index query the router sent live, or undefined when it
 * answers live as before: its run target declares no stored table of its
 * own (d), or a table of it is bound and the served copy declined it
 * (b, held: still answered live).
 */
export async function unbuiltIndexSource(args: {
   /** The caller's own (live) compile. */
   runnable: { getPreparedQuery(): Promise<unknown> };
   modelDef: ModelDef | undefined;
   pinned: Pinned;
   plan: IndexGuardPlan | undefined;
   /** `PUBLISHER_MATERIALIZE`: whether `#(materialize)` is read at all. */
   materializeEnabled: boolean;
}): Promise<UnbuiltIndexSource | undefined> {
   const { modelDef } = args;
   if (!modelDef) return undefined;
   const prepared = await readPreparedQuery(args.runnable);
   const struct = resolveStructRefSource<{ sourceID?: unknown; as?: unknown }>(
      prepared.structRef,
      prepared.modelDef ?? modelDef,
   );
   const declaration = declarationOf(prepared.structRef, struct, modelDef);
   if (!declaration) return undefined;
   const { name, def, sourceID } = declaration;
   if (
      !declaresStoredTable(
         declaredNotes(def, modelDef.contents ?? {}),
         args.materializeEnabled,
      )
   ) {
      return undefined;
   }
   if (boundAtPin(sourceID, args.pinned, args.plan)) return undefined;
   // `building` and `failed` are a run's, which this server's query path
   // does not hold (a host may build elsewhere): never claimed here.
   return {
      sourceName: name,
      unbuiltReason: args.plan?.refused(sourceID) ? "refused" : "unbuilt",
   };
}
