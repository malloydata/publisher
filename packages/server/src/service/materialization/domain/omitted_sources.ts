// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `BuildPlan.omittedSources` and `omittedFiles`: the sources a build plan left
 * out because planning them failed, so a host can tell "this source left the
 * plan" from "this plan could not say" before it retires a table.
 *
 * Filled by the planners that compile a companion and by the build plan; read
 * by the wire projection. A source the plan decides not to build is refused
 * or warned on instead, and is never one of these. An entry carries a
 * sourceID and a code, never the failure's message, which could quote the
 * source's SQL or a value.
 *
 * Pure.
 */

import { components } from "../../../api";

export type OmittedSource = components["schemas"]["OmittedSource"];
export type OmittedFile = components["schemas"]["OmittedFile"];

/**
 * One omission as planning records it: a source by its sourceID, or, when the
 * failed planner could not name its sources, the file whose sources it would
 * have planned (`BuildPlan.omittedFiles`).
 */
export type Omission = OmittedSource | OmittedFile;

/**
 * The codes Publisher reports today. The wire field is an open string, so a
 * client must accept a code it does not know; this union only keeps the
 * server's own spelling consistent.
 */
export type OmittedSourceReason =
   | "unkeyable"
   | "eligibility_check_failed"
   | "projection_failed"
   | "rollup_companion_failed"
   | "rollup_companion_blanking_failed"
   | "materialize_companion_failed"
   | "persisted_view_companion_failed"
   | "v1_build_companion_failed"
   | "persist_annotation_unparseable";

/**
 * One entry per source a failed companion file would have planned
 * (`<name>@<fileURL>`), or a single file entry standing for every source of
 * that file when `names` is unknown (the planner itself failed). A file is
 * never written as a sourceID: a source may be named `*`, so no in-band
 * wildcard could be told from a real sourceID.
 */
export function omittedFromFile(
   fileURL: string,
   names: readonly string[] | undefined,
   reason: OmittedSourceReason,
): Omission[] {
   if (names === undefined) return [{ file: fileURL, reason }];
   return names.map((name) => ({ sourceID: `${name}@${fileURL}`, reason }));
}

/** One entry per sourceID, each with `reason`. */
export function omittedByID(
   sourceIDs: Iterable<string>,
   reason: OmittedSourceReason,
): OmittedSource[] {
   return [...sourceIDs].map((sourceID) => ({ sourceID, reason }));
}

const byKey =
   <T>(key: (t: T) => string) =>
   (a: T, b: T) =>
      key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0;

/**
 * The plan's final lists, `omittedSources` and `omittedFiles`: one entry per
 * sourceID or file (the first reason recorded wins), no source the plan did
 * list in `sources` (a model that imports another walks its sources again,
 * and one walk may plan what another lost), each sorted so the plan is
 * deterministic.
 */
export function finalizeOmissions(
   recorded: readonly Omission[],
   planned: ReadonlySet<string>,
): { omittedSources: OmittedSource[]; omittedFiles: OmittedFile[] } {
   const sources = new Map<string, OmittedSource>();
   const files = new Map<string, OmittedFile>();
   for (const entry of recorded) {
      if ("file" in entry) {
         if (!files.has(entry.file)) {
            files.set(entry.file, { file: entry.file, reason: entry.reason });
         }
         continue;
      }
      if (planned.has(entry.sourceID) || sources.has(entry.sourceID)) continue;
      sources.set(entry.sourceID, {
         sourceID: entry.sourceID,
         reason: entry.reason,
      });
   }
   return {
      omittedSources: [...sources.values()].sort(byKey((e) => e.sourceID)),
      omittedFiles: [...files.values()].sort(byKey((e) => e.file)),
   };
}
