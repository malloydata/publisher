// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Detects the per-dimension policy keys a `#(index ... )` annotation may not
 * declare. Scope and schedule are package-level (`Package.scope`), so a
 * `sharing=` or `schedule=` on a dimension is refused at load rather than
 * silently ignored.
 */

import {
   indexNotePayload,
   PUBLISHER_ROUTES,
   tagPayloadKeys,
} from "../annotation_reader";

/** The keys `#(index)` refuses: the route's retired keys, each package-level. */
const FORBIDDEN_KEYS: Readonly<Record<string, string>> =
   PUBLISHER_ROUTES.index?.retiredKeys ?? {};

/**
 * Detects any no-longer-supported per-dimension policy keys (`sharing`,
 * `schedule`) declared on a field's `#(index ... )` annotation, read as a tag
 * by the shared annotation reader (keys folded to lower case). Returns the
 * forbidden keys found, in the order written (empty when none). The discovery
 * layer reports a non-empty result as a package warning instead of silently
 * ignoring the knob. Only the first `#(index)` tagline with a payload is
 * inspected (a field carries at most one).
 */
export function findForbiddenIndexPolicyKeys(taglines: string[]): string[] {
   for (const tagline of taglines) {
      const payload = indexNotePayload(tagline);
      if (payload === undefined) continue;
      return tagPayloadKeys(payload).filter((key) =>
         Object.hasOwn(FORBIDDEN_KEYS, key),
      );
   }
   return [];
}
