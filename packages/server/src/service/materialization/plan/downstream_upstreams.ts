// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a downstream (a persisted view, a named query, a rollup) is computed
 * from: the persisted sources its definition reaches, each with where its
 * table lives, read from that source's own notes in the author's model. The
 * plan classifies each downstream from it once, at plan time: one built
 * over its upstreams' tables, or one refused, whose refusal names which
 * upstream and why (persisted_view_compile.ts, rollup_compile.ts).
 *
 * Pure.
 */

import { ownLevelNotes, type AnnotationsDef } from "../../annotations";
import { routeOfNote } from "../../annotation_reader";
import type { RefusedReason } from "../domain/types";
import { reachableSourceIDs, type SourceMap } from "../text/source_map";
import {
   notesDeclarePersist,
   placementOf,
   readNotesIfAny,
} from "./declaration";

/**
 * The refusal reason of a database downstream whose upstream's table lives
 * in a `storage=` destination, which a database build cannot read in place
 * of the upstream (a downstream is never built by recomputing its upstream
 * from raw, in a standalone build or a host's). `message`
 * names the upstream and its destination.
 */
export const UPSTREAM_TABLE_UNUSABLE_REASON: RefusedReason =
   "upstream_table_unusable";

/** A persisted source a downstream's definition reaches. */
interface ReachedUpstream {
   sourceID: string;
   /** Its name in its declaring file. */
   name: string;
   /** The destination its table lives in; absent for its own warehouse. */
   storage?: string;
}

/**
 * The sourceIDs `root` reaches ({@link reachableSourceIDs}) that `map`
 * holds, with a source a named query refers to by its name in the model
 * (its `structRef`) resolved to that source's sourceID, and its own reach
 * added.
 */
export function reachedSourceIDsOf(
   root: unknown,
   modelDef: unknown,
   map: SourceMap,
): Set<string> {
   const contents =
      (modelDef as { contents?: Record<string, unknown> }).contents ?? {};
   const ids = new Set<string>();
   const pending = [root];
   const seen = new Set<unknown>();
   while (pending.length > 0) {
      const next = pending.pop();
      if (next === undefined || seen.has(next)) continue;
      seen.add(next);
      for (const id of reachableSourceIDs([next], modelDef)) {
         if (map.node(id)) {
            ids.add(id);
            continue;
         }
         const named = contents[id] as { sourceID?: unknown } | undefined;
         if (named && typeof named.sourceID === "string") pending.push(named);
      }
   }
   return ids;
}

/**
 * The persisted sources `root` reaches ({@link reachableSourceIDs}), with a
 * source a named query refers to by its name in the model resolved to its
 * sourceID: each that its own notes declare `#@ persist` or
 * `#(materialize)`, in sourceID order, with the destination its own notes
 * place it in. A `#@ persist` on a source that is not a query builds no
 * table, so it is no upstream. Read from the declaration alone, whatever
 * `PERSIST_STORAGE_MODE` says: with `storage=` off such a source has no
 * table at all, and a downstream refused over it is answered live too.
 */
export function reachedUpstreams(
   root: unknown,
   modelDef: unknown,
   map: SourceMap,
): ReachedUpstream[] {
   const ids = reachedSourceIDsOf(root, modelDef, map);
   const out: ReachedUpstream[] = [];
   for (const sourceID of [...ids].sort()) {
      const node = map.node(sourceID);
      const notes = ownLevelNotes(
         (node?.def as { annotations?: AnnotationsDef } | undefined)
            ?.annotations,
      );
      const name = node?.name ?? sourceID;
      const own = notes.filter((n) => routeOfNote(n.text) === "@");
      const materialize = notes.filter(
         (n) => routeOfNote(n.text) === "materialize",
      );
      const persisted = notesDeclarePersist(own);
      if (!persisted && materialize.length === 0) continue;
      // A `#@ persist` on a source that is not a query builds no table
      // (`persist-not-materializable`): nothing stored to read.
      if (
         persisted &&
         materialize.length === 0 &&
         (node?.def as { type?: unknown } | undefined)?.type !== "query_source"
      ) {
         continue;
      }
      const tag = persisted
         ? readNotesIfAny(own, "@")
         : readNotesIfAny(materialize, "materialize");
      const placed = tag
         ? placementOf(tag, persisted ? "persist" : undefined)
         : undefined;
      const storage =
         placed && "placement" in placed && placed.placement.tier === "storage"
            ? placed.placement.destination
            : undefined;
      out.push({
         sourceID,
         name,
         ...(storage !== undefined ? { storage } : {}),
      });
   }
   return out;
}

/**
 * Why a database downstream cannot be built over its upstreams' tables
 * because one is stored in a `storage=` destination, naming that upstream,
 * or undefined when none is.
 */
export function storedElsewhereReason(
   upstreams: readonly ReachedUpstream[],
): string | undefined {
   const stored = upstreams.find((u) => u.storage !== undefined);
   return stored
      ? `its upstream '${stored.name}' is stored in the storage= destination ` +
           `'${stored.storage}', whose table a build in the warehouse cannot ` +
           `read, and a downstream is never built by recomputing its upstream ` +
           `from raw; it is ` +
           `answered live, over '${stored.name}''s table`
      : undefined;
}

/**
 * Why a planned table placed at `downstream` (a `storage=` destination's
 * name, or undefined for its own warehouse) cannot be built over the table
 * of an upstream placed at `upstream.storage`, or undefined when it can: a
 * warehouse build cannot read a `storage=` table, and a `storage=` build
 * reads only its own destination's tables (or the warehouse's). Neither is
 * ever answered by recomputing the upstream from raw.
 */
export function placementReason(
   downstream: string | undefined,
   upstream: { name: string; storage?: string },
): string | undefined {
   if (upstream.storage === undefined || upstream.storage === downstream) {
      return undefined;
   }
   if (downstream === undefined) {
      return storedElsewhereReason([upstream as ReachedUpstream]);
   }
   return (
      `its upstream '${upstream.name}' is stored in the storage= destination ` +
      `'${upstream.storage}', and it is built into the storage= destination ` +
      `'${downstream}', which cannot read that table; a downstream is never ` +
      `built by recomputing its upstream from raw, so it is answered live, ` +
      `over '${upstream.name}''s table`
   );
}
