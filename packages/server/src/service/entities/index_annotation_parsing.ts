// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Detects the per-dimension policy keys a `#(index ... )` annotation may not
 * declare. Scope and schedule are package-level (`Package.scope`), so a
 * `sharing=` or `schedule=` on a dimension is refused at load rather than
 * silently ignored.
 */

import { tokenize } from "../filter";

const FORBIDDEN_KEYS = new Set(["sharing", "schedule"]);

// An `#(index)` tagline's payload: inside the parens (`#( index <body> )`,
// which Malloy warns is a malformed route) and after them (`#(index) <body>`).
const INDEX_TAG = /^#\s*\(\s*index(?:_values)?\b([^)]*)\)(.*)$/;

/**
 * Detects any no-longer-supported per-dimension policy keys (`sharing`,
 * `schedule`) declared on a field's `#(index ... )` annotation. Returns the set
 * of forbidden keys found (empty when none). The discovery layer reports a
 * non-empty result as a package warning instead of silently ignoring the knob. Only the first matching `#(index)` tagline
 * is inspected (a field carries at most one).
 */
export function findForbiddenIndexPolicyKeys(taglines: string[]): string[] {
   for (const tagline of taglines) {
      const match = INDEX_TAG.exec(tagline.trim());
      if (!match) continue;
      const tokens = [match[1], match[2]].flatMap((part) =>
         tokenize((part ?? "").trim()),
      );
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
