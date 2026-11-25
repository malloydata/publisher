// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database `#(materialize)` upstream a downstream (a rollup, a persisted
 * view or named query) builds from the stored table of: whether one
 * qualifies, and the copy of its declaring model that re-roots it on its
 * generated build source, which the downstream's second compile imports so
 * Malloy's planner reads the upstream's table through a run's build
 * manifest (rollup_compile.ts, persisted_view_compile.ts).
 */

import * as path from "path";
import { pathToFileURL } from "url";
import { routeOfNote } from "../../annotation_reader";
import { ownLevelNotes, type AnnotationsDef } from "../../annotations";
import type { SourceMap } from "../text/source_map";
import { reachedSourceIDsOf } from "./downstream_upstreams";
import { overlayKey } from "../../overlay_key";
import { type MaterializePlan, splitSourceID } from "../domain/table_plans";
import type { TermUse } from "../domain/read_time_terms";
import { rollupStoredBaseCopyName } from "../text/read_text";
import {
   chainLevels,
   levelFileName,
   MATERIALIZE_COMPANION_SUFFIX,
} from "./build_file_generator";
import { placementOf, readNotesIfAny } from "./declaration";
import { storedSourceCopyText } from "./materialize_build_copy";
import { declarationOf } from "./materialize_read_time";

/**
 * A model's `#(materialize)` companion, as its downstreams read it: its plans
 * and its generated files, which a downstream over one of its sources
 * imports to build from that source's stored table.
 */
export interface MaterializedBases {
   plans: readonly MaterializePlan[];
   files: ReadonlyMap<string, string>;
}

/** A `#(materialize)` upstream a downstream may build from the stored table of. */
export interface StoredMaterializeBase {
   plan: MaterializePlan;
   /** The base's declaration in the author's model: what the copy re-roots. */
   declaration: TermUse;
   /** The generated file that declares its build source, beside the model. */
   importFrom: string;
}

/**
 * The base `baseSourceID` as a `#(materialize)` base whose stored table a
 * downstream may build from, or undefined: its own notes declare a database
 * `#(materialize)` (read by declaration.ts), and the companion planned it as
 * a table in its own warehouse, declared in this model. A table with
 * read-time terms holds rows its source filters again at read, and is not
 * read. A pair table holds a row per pair, not the base's rows: it is read
 * only with `pairs` (a persisted view or query, whose copy puts the join
 * back, pair_reroot.ts), never by a rollup.
 */
function storedMaterializeBase(
   notes: readonly { text: string }[],
   def: unknown,
   baseSourceID: string,
   modelFile: string,
   materialized: MaterializedBases | undefined,
   pairs: boolean,
): StoredMaterializeBase | string {
   if (!materialized) return "no #(materialize) table is planned in its model";
   const own = notes.filter((n) => routeOfNote(n.text) === "materialize");
   if (own.length === 0) return "it declares no #(materialize) of its own";
   const tag = readNotesIfAny(own, "materialize");
   const placed = tag ? placementOf(tag) : undefined;
   if (!placed || !("placement" in placed)) {
      return "its #(materialize) cannot be placed";
   }
   if (placed.placement.tier !== "database") {
      return "its table is in a storage= destination";
   }
   const plan = materialized.plans.find((p) => p.sourceID === baseSourceID);
   if (!plan) return "it is not planned in its model";
   if (!pairs && (plan.pair || plan.pairBlocks)) {
      return "its table is a pair table, one row per pair of an inlined join_many";
   }
   if (plan.persist.storage !== undefined) {
      return "its table is in a storage= destination";
   }
   if ((plan.readTimeTerms ?? []).length > 0) {
      return "its source filters its table's rows again at read";
   }
   const declaration = declarationOf(
      def as Parameters<typeof declarationOf>[0],
   );
   const modelHref = pathToFileURL(modelFile).href;
   if (!declaration || overlayKey(declaration.url) !== overlayKey(modelHref)) {
      return "it is declared in another file";
   }
   // Where the build source is declared: the companion, or, with a chain
   //, its level's file (build_file_generator.ts).
   const levels = chainLevels(materialized.plans);
   const modelName = path.basename(modelFile);
   const importFrom = [...levels.values()].some((level) => level > 0)
      ? levelFileName(modelName, levels.get(plan.sourceID) ?? 0)
      : `${modelName}${MATERIALIZE_COMPANION_SUFFIX}`;
   const declaring = pathToFileURL(
      path.join(path.dirname(modelFile), importFrom),
   ).href;
   if (!materialized.files.has(declaring)) {
      return "its build source was not generated";
   }
   return { plan, declaration, importFrom };
}

/**
 * The copy each stored `#(materialize)` base is imported from by the second
 * compile, by the base's declared name: `text`, the model as the companion
 * reads it, with the base re-rooted on its build source
 * ({@link storedSourceCopyText}), at its URL beside the model. A base whose
 * copy cannot be made is left out, so its rollups build from the warehouse.
 */
export function storedBaseCopies(
   bases: ReadonlyMap<string, StoredMaterializeBase>,
   text: string | undefined,
   modelFile: string,
): Map<string, { url: string; text: string }> {
   const out = new Map<string, { url: string; text: string }>();
   if (text === undefined) return out;
   for (const [name, base] of bases) {
      const copy = storedSourceCopyText(
         text,
         base.plan,
         base.declaration,
         base.importFrom,
      );
      if (copy === undefined) continue;
      out.set(name, {
         url: pathToFileURL(
            path.join(
               path.dirname(modelFile),
               rollupStoredBaseCopyName(path.basename(modelFile), name),
            ),
         ).href,
         text: copy,
      });
   }
   return out;
}

/**
 * The one database `#(materialize)` upstream `root` (a downstream's source)
 * reaches whose stored table it may build from ({@link storedMaterializeBase}),
 * undefined when it reaches none, or `unusable` with why when it reaches
 * several, or one that does not qualify, or one whose zone a stage between
 * differs from (its stored truncations would be read in another zone, and
 * truncated again).
 */
export function storedMaterializeUpstream(
   map: SourceMap,
   root: unknown,
   modelDef: unknown,
   modelFile: string,
   materialized: MaterializedBases | undefined,
   options: { pairs?: boolean } = {},
): StoredMaterializeBase | { unusable: string } | undefined {
   // A `#(materialize)` source planned as one: an extension carries its
   // base's notes as its own, but its base's plan is the one it reads.
   const planned = new Set(materialized?.plans.map((p) => p.sourceID) ?? []);
   const ids = [...reachedSourceIDsOf(root, modelDef, map)].filter(
      (id) =>
         planned.has(id) ||
         (!materialized &&
            ownLevelNotes(
               (
                  map.node(id)?.def as
                     | { annotations?: AnnotationsDef }
                     | undefined
               )?.annotations,
            ).some((n) => routeOfNote(n.text) === "materialize")),
   );
   if (ids.length === 0) return undefined;
   // The root's own table when it has one (an extension table chains on
   // its parent's), else the one source it extends that has one.
   const own = (root as { sourceID?: unknown } | undefined)?.sourceID;
   if (typeof own === "string" && ids.includes(own))
      ids.splice(0, ids.length, own);
   const nameOf = (id: string) => splitSourceID(id)?.name ?? id;
   if (ids.length > 1) {
      return {
         unusable:
            `it reaches several #(materialize) sources ` +
            `(${ids.map((id) => `'${nameOf(id)}'`).join(", ")}), and its ` +
            `build reads one upstream's table`,
      };
   }
   const def = map.node(ids[0])?.def as
      | { annotations?: AnnotationsDef }
      | undefined;
   const base = storedMaterializeBase(
      ownLevelNotes(def?.annotations),
      def,
      ids[0],
      modelFile,
      materialized,
      options.pairs ?? false,
   );
   if (typeof base === "string") {
      return {
         unusable: `its #(materialize) upstream '${nameOf(ids[0])}' has no table it can build from: ${base}`,
      };
   }
   const zone = zoneOf(def);
   if ([...timezonesIn(root)].some((z) => z !== zone)) {
      return {
         unusable:
            `it reads its #(materialize) upstream '${nameOf(ids[0])}' in ` +
            `another time zone than its table was built in, so a stored ` +
            `truncation would be truncated again`,
      };
   }
   return base;
}

/** A definition's own query time zone, if it sets one. */
function zoneOf(def: unknown): string | undefined {
   const zone = (def as { queryTimezone?: unknown } | undefined)?.queryTimezone;
   return typeof zone === "string" ? zone : undefined;
}

/** Every query time zone a definition sets anywhere in it, bounded. */
function timezonesIn(def: unknown): Set<string> {
   const out = new Set<string>();
   const seen = new Set<unknown>();
   const walk = (node: unknown, depth: number): void => {
      if (depth > 64 || node === null || typeof node !== "object") return;
      if (seen.has(node)) return;
      seen.add(node);
      if (Array.isArray(node)) {
         for (const item of node) walk(item, depth + 1);
         return;
      }
      for (const [key, value] of Object.entries(node)) {
         if (key === "queryTimezone" && typeof value === "string") {
            out.add(value);
         } else {
            walk(value, depth + 1);
         }
      }
   };
   walk(def, 0);
   return out;
}
