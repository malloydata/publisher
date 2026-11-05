// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Detects the per-dimension policy keys a `#(index ... )` annotation may not
 * declare. Scope and schedule are package-level (`Package.scope`), so a
 * `sharing=` or `schedule=` on a dimension is refused at load rather than
 * silently ignored.
 */

import { tokenizeAnnotationBody } from "./filter_parsing";

const FORBIDDEN_KEYS = new Set(["sharing", "schedule"]);

// The body between the `index`/`index_values` keyword and the closing paren:
// `#( index <body> )`.
const INDEX_TAG_BODY = /^#\s*\(\s*index(?:_values)?\b(.*)\)\s*$/;

/**
 * Detects any no-longer-supported per-dimension policy keys (`sharing`,
 * `schedule`) declared on a field's `#(index ... )` annotation. Returns the set
 * of forbidden keys found (empty when none). The discovery layer turns a
 * non-empty result into a publish-time rejection so authors get a clear error
 * instead of a silently-ignored knob. Only the first matching `#(index)` tagline
 * is inspected (a field carries at most one).
 */
export function findForbiddenIndexPolicyKeys(taglines: string[]): string[] {
   for (const tagline of taglines) {
      const match = INDEX_TAG_BODY.exec(tagline.trim());
      if (!match) continue;
      const tokens = tokenizeAnnotationBody((match[1] ?? "").trim());
      const found: string[] = [];
      for (const token of tokens) {
         const eq = token.indexOf("=");
         if (eq < 0) continue;
         const key = token.slice(0, eq).toLowerCase();
         if (FORBIDDEN_KEYS.has(key) && !found.includes(key)) {
            found.push(key);
         }
      }
      return found;
   }
   return [];
}
