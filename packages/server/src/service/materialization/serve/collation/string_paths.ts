// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which compared paths are strings, and which collations a table read holds
 * a column under: the shared steps of read admission's collation lookups
 * (`admission_lookup.ts`).
 */

import { fieldPathsIn } from "../gate_paths";

/**
 * The entry-level paths that read a string field, or undefined when a path
 * cannot be followed (fail closed). A path through a join is followed into
 * the joined source's fields. A computed field compares the fields its
 * expression reads, so each of those must be a path here too (the walk
 * lists them), and a query-shaped source's stage output, which may be
 * computed from fields no path names, fails closed unless it is a string; a
 * field its `extend { … }` declares is followed by its expression.
 */
export function stringPaths(
   struct: unknown,
   paths: readonly (readonly string[])[],
): string[][] | undefined {
   const out: string[][] = [];
   const seen = new Set<string>();
   for (const path of paths) {
      let at: unknown = struct;
      let owner: unknown = struct;
      let field: { type?: unknown; e?: unknown } | undefined;
      for (const name of path) {
         owner = at;
         field = (
            ((at as { fields?: unknown[] } | undefined)?.fields ?? []) as {
               name?: unknown;
               as?: unknown;
               type?: unknown;
               e?: unknown;
            }[]
         ).find((f) => String(f.as ?? f.name) === name);
         if (!field) return undefined;
         at = field;
      }
      if (field?.type !== "string") {
         // A query-shaped source's stage output carries no expression: what
         // it was computed from is no path here, so it fails closed. A field
         // its `extend { … }` declares carries one, followed as any other
         // computed field's (`dimension: is_open is close.type is null`).
         if (
            (owner as { query?: unknown } | undefined)?.query !== undefined &&
            field?.e === undefined
         ) {
            return undefined;
         }
         if (field?.e !== undefined) {
            const prefix = path.slice(0, -1);
            const listed = new Set(paths.map((p) => p.join(".")));
            const reads = fieldPathsIn(field.e);
            if (
               reads.length === 0 ||
               !reads.every((p) => listed.has([...prefix, ...p].join(".")))
            ) {
               return undefined;
            }
         }
         continue;
      }
      const key = path.join(".");
      if (seen.has(key)) continue;
      seen.add(key);
      out.push([...path]);
   }
   return out;
}

/** Record that a table read holds `name` under `collation`. */
export function holdColumn(
   held: Map<string, Set<string | null>>,
   name: string,
   collation: string | null,
): void {
   let set = held.get(name);
   if (!set) {
      set = new Set();
      held.set(name, set);
   }
   set.add(collation);
}
