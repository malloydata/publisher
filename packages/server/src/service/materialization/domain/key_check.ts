// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Key checks: what a uniqueness check on a stored key proves, and what its
 * failure does, by kind, from one table; and what a check found.
 *
 * Every check asks whether a key column repeats. Whether a NULL key counts
 * against it, and whether a repeat fails the build or is only recorded, is
 * the kind's, and the reasons differ:
 *
 * | Kind              | NULL key | On a repeat | Why                                                                                                                      |
 * | ----------------- | -------- | ----------- | ------------------------------------------------------------------------------------------------------------------------ |
 * | `inline_join_one` | ignored  | fail build  | a NULL never joins, so it cannot inflate the join_one an inlined join stands in for; a repeat would, in every row        |
 * | `pair_level`      | fails    | fail build  | a level's row flag marks one row per distinct key, where live counts each row: a NULL key breaks the flag as a repeat does |
 * | `rollup_key`      | ignored  | record      | a NULL never joins the rollup; a repeat only stops a join reading it as a join_one, which the branch declines             |
 * | `parent_key`      | fails    | record      | the join-to-rollup branch counts a parent once per key, which a NULL or repeated key would get wrong; it declines instead  |
 * | `stored_key`      | fails    | record      | the same parent check, over the source's stored table in a destination                                                   |
 *
 * A check is plain data (it crosses the load worker); the run compiles and
 * runs it. A verdict is never absent: a check that did not run is `unknown`,
 * and only `unique` satisfies {@link requireUnique}.
 *
 * Pure.
 */

import type { BuildManifest } from "./types";

/** The kinds of key check. */
export type KeyCheckKind =
   | "inline_join_one"
   | "pair_level"
   | "rollup_key"
   | "parent_key"
   | "stored_key";

/** What a check of a kind proves, and what its failure does. */
interface KeyRule {
   /** Whether a NULL key fails the check, or is left out of it. */
   nullKey: "fails" | "ignored";
   /** Whether a repeat fails the build, or is recorded for the readers. */
   onRepeat: "fail_build" | "record";
}

/** The rule for each kind (the table in this module's header). */
export const KEY_RULES = {
   inline_join_one: { nullKey: "ignored", onRepeat: "fail_build" },
   pair_level: { nullKey: "fails", onRepeat: "fail_build" },
   rollup_key: { nullKey: "ignored", onRepeat: "record" },
   parent_key: { nullKey: "fails", onRepeat: "record" },
   stored_key: { nullKey: "fails", onRepeat: "record" },
} as const satisfies Record<KeyCheckKind, KeyRule>;

/** What a check found: never absent, so "not checked" cannot read as unique. */
export type KeyVerdict = "unique" | "repeated" | "unknown";

/** Whether a reader that needs a unique key may rely on `verdict`. */
export function requireUnique(verdict: KeyVerdict): boolean {
   return verdict === "unique";
}

/**
 * Compiles a check against a run's compiler manifest, so it reads the
 * persisted tables the manifest names. The plan that compiled the check's
 * companion offers one per check it compiled; a check from a plan without
 * one (a load worker's plan) is never compiled over a stored table.
 */
export type KeyCheckCompiler = (
   buildManifest: BuildManifest,
   connectionDigests: Record<string, string>,
) => Promise<string>;

/** The key a plan's {@link KeyCheckCompiler}s are held under, per check. */
export function keyCheckCompilerKey(
   sourceID: string,
   queryName: string,
): string {
   return `${sourceID}\u0000${queryName}`;
}

/** A recorded check result (true unique, false not, absent not checked) as a verdict. */
export function verdictOf(unique: boolean | undefined): KeyVerdict {
   return unique === undefined ? "unknown" : unique ? "unique" : "repeated";
}
