// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The one shape of a refusal record's key, for every planner that refuses a
 * valid declaration: where it sits in `refusedSources`, and the tier it would
 * have been built in.
 */

import type { RefusalKey } from "../../annotation_reader";
import type { RefusedReason } from "../domain/types";

/** The tier a table declaring `storage` (or none) is built in. */
export function tierOf(storage: string | undefined): "database" | "storage" {
   return storage !== undefined ? "storage" : "database";
}

/** A refusal record's key. */
export function refusalKey(
   sourceID: string,
   name: string,
   reason: RefusedReason,
   tier: RefusalKey["tier"],
): RefusalKey {
   return { sourceID, name, tier, reason };
}
