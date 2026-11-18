// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a source's row-level gates include an `#(access_filter)` declared
 * on a composite source (`compose(…)`), reached however the governor's gate
 * walk reaches it: the source itself, a source it extends (whose notes it
 * inherits), or the source a query-shaped source, or its derivation, reads.
 *
 * Live refuses every caller of such a source: the filter binding guard finds
 * the filter's fields resolving to a different struct on the executed
 * member (`filter_binding_guard.ts`). A stored answer would not, so a rollup
 * of it is not planned and no leg answers it from a stored table: the
 * mismatch fails closed on every leg. A walk that cannot be read reads true.
 */

import { annotationTexts, type AnnotationsDef } from "./annotations";
import { routeOfNote } from "./annotation_reader";
import {
   ACCESS_FILTER_ROUTE,
   canonicalAuthorizeRoute,
} from "./authorize_routes";

interface StructLike {
   type?: unknown;
   annotations?: AnnotationsDef;
   query?: { structRef?: unknown };
}

export function rowFilterOnComposite(
   struct: unknown,
   modelDef: unknown,
): boolean {
   const contents = (modelDef as { contents?: Record<string, unknown> })
      ?.contents;
   const seen = new Set<unknown>();
   const visit = (s: unknown, depth: number): boolean => {
      if (depth > 64) return true;
      if (s === null || typeof s !== "object" || seen.has(s)) return false;
      seen.add(s);
      const def = s as StructLike;
      if (def.type === "composite") {
         const texts = annotationTexts(def.annotations) ?? [];
         if (
            texts.some(
               (t) =>
                  canonicalAuthorizeRoute(routeOfNote(t)) ===
                  ACCESS_FILTER_ROUTE,
            )
         ) {
            return true;
         }
      }
      const ref = def.query?.structRef;
      if (ref === undefined) return false;
      const base = typeof ref === "string" ? contents?.[ref] : ref;
      if (base === undefined) return true;
      return visit(base, depth + 1);
   };
   try {
      return visit(struct, 0);
   } catch {
      return true;
   }
}
