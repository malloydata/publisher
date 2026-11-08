// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `GovernedRunnable`: a compiled query that has been through governance, the
 * only kind `prepareAndRun` (`../query_execution.ts`) will execute.
 *
 * A type brand, not a wrapper: the value IS the `QueryMaterializer`
 * `Model.authorizeAndBindRunnable` returned (for a row-level gate, the
 * recompiled runnable carrying the graft, where the reassignment IS the
 * enforcement). The brand makes "ran the runnable from before the bind" a
 * compile error instead of an ungated read.
 *
 * Minted ONLY by {@link mintGovernedRunnable}, called ONLY inside
 * `Model.authorizeAndBindRunnable`, which also holds the explicit bypass
 * branch. `governed_runnable_audit.spec.ts` fails if anything else mints one
 * or casts to the type.
 */

import type { QueryMaterializer } from "@malloydata/malloy";

declare const GOVERNED: unique symbol;

/** A runnable governance returned; see the module doc. */
export type GovernedRunnable = QueryMaterializer & {
   readonly [GOVERNED]: true;
};

/**
 * Brand `runnable` as governed. Call it only on the runnable governance has
 * just decided (or, on the explicit bypass, deliberately skipped): the brand
 * is the claim that this is so.
 */
export function mintGovernedRunnable(
   runnable: QueryMaterializer,
): GovernedRunnable {
   return runnable as GovernedRunnable;
}
