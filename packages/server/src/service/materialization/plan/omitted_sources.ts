// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `BuildPlan.omittedSources`: the sources a build plan left out because
 * planning them FAILED, so a host can tell "this source left the plan" from
 * "this plan could not say" before it retires a table. A source the plan
 * DECIDES not to build is refused or warned on instead, and is not one.
 *
 * Each entry carries a sourceID and a code, never the failure's message,
 * which could quote the source's SQL or a value; the message is logged where
 * the failure is caught.
 */

import { components } from "../../../api";

export type OmittedSource = components["schemas"]["OmittedSource"];

/**
 * The codes Publisher reports today. The wire field is an open string, so a
 * client must accept a code it does not know; this union only keeps the
 * server's own spelling consistent.
 */
export type OmittedSourceReason =
   | "unkeyable"
   | "eligibility_check_failed"
   | "projection_failed"
   | "preaggregate_companion_failed"
   | "rollup_companion_failed"
   | "rollup_companion_blanking_failed"
   | "materialize_companion_failed"
   | "persisted_view_companion_failed"
   | "v1_build_companion_failed";

/** The name an entry carries when the failed planner could not name its sources. */
export const ANY_SOURCE_OF_FILE = "*";

/**
 * One entry per source a failed companion file would have planned
 * (`<name>@<fileURL>`), or a single `*@<fileURL>` entry standing for every
 * source of that file when `names` is unknown (the planner itself failed).
 */
export function omittedFromFile(
   fileURL: string,
   names: readonly string[] | undefined,
   reason: OmittedSourceReason,
): OmittedSource[] {
   if (names === undefined) {
      return [{ sourceID: `${ANY_SOURCE_OF_FILE}@${fileURL}`, reason }];
   }
   return names.map((name) => ({ sourceID: `${name}@${fileURL}`, reason }));
}

/** One entry per sourceID, each with `reason`. */
export function omittedByID(
   sourceIDs: Iterable<string>,
   reason: OmittedSourceReason,
): OmittedSource[] {
   return [...sourceIDs].map((sourceID) => ({ sourceID, reason }));
}

/**
 * The plan's final list: one entry per sourceID (the first reason recorded
 * wins), none for a sourceID the plan did list in `sources` (a model that
 * imports another walks its sources again, and one walk may plan what
 * another lost), sorted so the plan is deterministic.
 */
export function finalizeOmittedSources(
   recorded: readonly OmittedSource[],
   planned: ReadonlySet<string>,
): OmittedSource[] {
   const byID = new Map<string, OmittedSource>();
   for (const entry of recorded) {
      if (planned.has(entry.sourceID) || byID.has(entry.sourceID)) continue;
      byID.set(entry.sourceID, {
         sourceID: entry.sourceID,
         reason: entry.reason,
      });
   }
   return [...byID.values()].sort((a, b) =>
      a.sourceID < b.sourceID ? -1 : a.sourceID > b.sourceID ? 1 : 0,
   );
}
