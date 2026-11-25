// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The field paths a Malloy IR expression reads, for the gate walk
 * (`gate_reads.ts`) and the collation check (`admission_lookup.ts`), in a leaf
 * module so neither imports the other for it.
 */

/** Every field path an expression reads (`node: "field"`), at any depth. */
export function fieldPathsIn(
   e: unknown,
   out: string[][] = [],
   depth = 0,
): string[][] {
   if (depth > 64 || e === null || typeof e !== "object") return out;
   if (Array.isArray(e)) {
      for (const x of e) fieldPathsIn(x, out, depth + 1);
      return out;
   }
   const n = e as { node?: unknown; path?: unknown };
   if (
      n.node === "field" &&
      Array.isArray(n.path) &&
      n.path.every((p) => typeof p === "string")
   ) {
      out.push(n.path as string[]);
   }
   for (const v of Object.values(e)) fieldPathsIn(v, out, depth + 1);
   return out;
}
