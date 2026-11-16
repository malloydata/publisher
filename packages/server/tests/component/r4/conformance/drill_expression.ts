// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseAnnotation } from "@malloydata/malloy-tag";

/**
 * A `#(malloy)` annotation with only its `drill_expression` set aside, which a
 * column read from a stored table legitimately writes differently (its code is
 * the stored column, or the definition, not live's). Every other key Malloy
 * writes there (`reference_id`, `calculation`, `drill_filters`, `limit`,
 * `ordered_by`, `query_timezone`) is kept, so a measure served as a dimension
 * (it loses `calculation`) still differs. Any other annotation is returned as
 * is.
 */
export function withoutDrillExpression(note: string): unknown {
   if (!note.startsWith("#(malloy)")) return note;
   const { tag, log } = parseAnnotation(note);
   if (log.length > 0) return note;
   const { drill_expression: _d, ...rest } = tag.getProperties();
   return Object.fromEntries(
      Object.entries(rest).map(([k, v]) => [k, v.toJSON()]),
   );
}

/**
 * A result type with each `#(malloy)` annotation's `drill_expression` set
 * aside, a nest's fields included.
 */
export function typeWithoutDrillExpression(type: unknown, depth = 0): unknown {
   if (depth > 32 || type === null || typeof type !== "object") return type;
   if (Array.isArray(type)) {
      return type.map((a) =>
         typeof (a as { value?: unknown })?.value === "string"
            ? {
                 ...(a as object),
                 value: withoutDrillExpression((a as { value: string }).value),
              }
            : typeWithoutDrillExpression(a, depth + 1),
      );
   }
   return Object.fromEntries(
      Object.entries(type as Record<string, unknown>).map(([k, v]) => [
         k,
         typeWithoutDrillExpression(v, depth + 1),
      ]),
   );
}
