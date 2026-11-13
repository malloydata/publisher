// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which joined fields a compiled query returns as its own output columns,
 * read from the query's IR. A table that answers a join from stored columns
 * exposes the join as a record of them, and a record literal's field cannot
 * carry an annotation, so a joined field the author annotated (`#(doc)`,
 * `#(classification)`, a render tag such as `# currency`) comes back without
 * its notes when the query returns it as a column. Live returns them, so such
 * a query is answered live; every other query on the table is not affected.
 *
 * Shared by every served shape that replaces a join with a record of stored
 * columns: an inlined join_one (#1665) and an inlined join_many's pair table
 * (#1658). Malloy exports no type for the IR, so every node is duck-typed
 * here, once.
 *
 * Only a column that IS the joined field carries its notes: `group_by:
 * j.field`, `select: j.field`, in any stage or nest, or in a view the query
 * reads by name. A field that reads it under another name (`x is j.field`,
 * a dimension `d is j.field`) carries no notes live either, and a filter or
 * an aggregate returns no column of the field.
 */

/** A joined field returned as an output column. */
export interface JoinedFieldOutput {
   /** `[join, field]`. */
   path: string[];
}

interface QueryField {
   type?: unknown;
   path?: unknown;
   pipeline?: unknown;
   name?: unknown;
   as?: unknown;
}

const MAX_DEPTH = 64;

/**
 * The first output column of a compiled query that is one of `annotated`'s
 * joined fields (`annotated` maps a join's name to its annotated fields),
 * with `unreadable` set when a part of the pipeline cannot be read, so a
 * caller fails closed. `pipeline` is the prepared query's `_query.pipeline`
 * (governance/ir/prepared_query.ts reads it); `sourceFields` the queried
 * source's fields, through which a view read by name (`nest: v`) resolves.
 */
export function annotatedJoinedOutput(
   pipeline: unknown,
   sourceFields: unknown,
   annotated: ReadonlyMap<string, ReadonlySet<string>>,
): { output?: JoinedFieldOutput; unreadable: boolean } {
   const views = new Map<string, QueryField>();
   for (const f of Array.isArray(sourceFields)
      ? (sourceFields as QueryField[])
      : []) {
      const name = typeof f.as === "string" ? f.as : f.name;
      if (f.type === "turtle" && typeof name === "string") views.set(name, f);
   }
   let unreadable = false;
   const visiting = new Set<string>();
   const walk = (
      segments: unknown,
      depth: number,
   ): JoinedFieldOutput | void => {
      if (depth > MAX_DEPTH || !Array.isArray(segments)) {
         unreadable = true;
         return;
      }
      for (const segment of segments as { queryFields?: unknown }[]) {
         const fields = segment?.queryFields;
         if (fields === undefined) continue;
         if (!Array.isArray(fields)) {
            unreadable = true;
            continue;
         }
         for (const qf of fields as QueryField[]) {
            if (qf?.type === "turtle") {
               const found = walk(qf.pipeline, depth + 1);
               if (found) return found;
               continue;
            }
            if (qf?.type !== "fieldref") continue;
            const p = qf.path;
            if (
               !Array.isArray(p) ||
               p.some((name) => typeof name !== "string")
            ) {
               unreadable = true;
               continue;
            }
            const path = p as string[];
            if (path.length === 2 && annotated.get(path[0])?.has(path[1])) {
               return { path };
            }
            // A view read by name: its own columns are the query's.
            const view = path.length === 1 ? views.get(path[0]) : undefined;
            if (view && !visiting.has(path[0])) {
               visiting.add(path[0]);
               const found = walk(view.pipeline, depth + 1);
               visiting.delete(path[0]);
               if (found) return found;
            }
         }
      }
   };
   const output = walk(pipeline, 0);
   return { ...(output ? { output } : {}), unreadable };
}
