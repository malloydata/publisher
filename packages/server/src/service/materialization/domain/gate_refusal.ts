// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one name a gate refusal takes on every mechanism: a declaration
 * refused for a row-level gate is named by the gate's kind, whichever
 * planner refused it (a `#@ persist` or `#(materialize)` source, a rollup, a
 * persisted view or query).
 *
 * | Gates that refused it                     | `reason`        |
 * | ----------------------------------------- | --------------- |
 * | any row filter (`#(access_filter)`)        | `access_filter` |
 * | only locks (`#(authorize)`), or unreadable | `authorize`     |
 *
 * A row filter wins over a lock: it is the gate a table holding every
 * caller's rows could not keep. A gate that cannot be read is named a lock,
 * as the gate walk fails closed to a deny.
 *
 * Pure.
 */

import type { RefusedReason } from "./types";

/** The two names a gate refusal takes. */
export type GateRefusalReason = Extract<
   RefusedReason,
   "access_filter" | "authorize"
>;

/**
 * The refusal reason for a declaration refused for the gates on these
 * routes (each gate's annotation route: `access_filter` or `authorize`).
 */
export function gateRefusalReason(
   routes: Iterable<string | undefined>,
): GateRefusalReason {
   for (const route of routes) {
      if (route === "access_filter") return "access_filter";
   }
   return "authorize";
}
