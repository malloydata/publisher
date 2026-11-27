// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How a `sql_select`'s `%{ … }` embeds are read off the IR: every
 * `sql_select` under a node, and the source struct(s) each embed reads.
 *
 * One reading, shared by the load's gate check (`sql_embed_gates.ts`), the
 * reach walk (`reachableSourceIDs`) and the served copy's cross-connection
 * check (`serve/sql_embed_reads.ts`), so none of them can disagree on what an
 * embed reads. The IR shape relied on is pinned in `sql_embed_gates.spec.ts`.
 *
 * Pure.
 */

import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";

export type Obj = Record<string, unknown>;

export const isObj = (value: unknown): value is Obj =>
   typeof value === "object" && value !== null;

/** What `via` holds for a `sql()` written inline with no name (e.g. in a query). */
export const INLINE_SQL = "<inline sql()>";

export const labelOf = (struct: Obj): string => {
   const label = (struct.as as string | undefined) ?? struct.name;
   if (typeof label !== "string") return "<unnamed>";
   // An unnamed `conn.sql(...)` is named by Malloy's `sql://conn/<uuid>` key.
   return label.startsWith("sql://") ? INLINE_SQL : label;
};

/** Every `sql_select` with embeds reachable from `root`, the root included. */
export function sqlSelectsWithin(root: unknown): Obj[] {
   const found: Obj[] = [];
   const seen = new Set<Obj>();
   const stack: unknown[] = [root];
   while (stack.length > 0) {
      const node = stack.pop();
      if (Array.isArray(node)) {
         stack.push(...node);
         continue;
      }
      if (!isObj(node) || seen.has(node)) continue;
      seen.add(node);
      if (node.type === "sql_select" && Array.isArray(node.selectSegments)) {
         found.push(node);
      }
      stack.push(...Object.values(node));
   }
   return found;
}

/**
 * The source struct(s) one non-text segment reads, or a reason it cannot be
 * read. An embedded query reads its `structRef` and, over a composite, the
 * member Malloy resolved. Also the reach walk's reading of a `%{}`
 * (`reachableSourceIDs`), so the two never disagree on what an embed reads.
 */
export function embeddedStructs(
   segment: Obj,
   modelDef: ModelDef,
): { structs: SourceDef[] } | { unreadable: string; label: string } {
   if (segment.type === "query") {
      const ref = segment.structRef;
      let base: unknown = ref;
      if (typeof ref === "string") base = modelDef.contents[ref];
      if (!isObj(base) || !isSourceDef(base as unknown as SourceDef)) {
         return {
            unreadable: "its source reference does not resolve in the model",
            label: typeof ref === "string" ? ref : "<embedded query>",
         };
      }
      const structs = [base as unknown as SourceDef];
      const resolved = segment.compositeResolvedSourceDef;
      if (isObj(resolved) && isSourceDef(resolved as unknown as SourceDef)) {
         structs.push(resolved as unknown as SourceDef);
      }
      return { structs };
   }
   if (isSourceDef(segment as unknown as SourceDef)) {
      return { structs: [segment as unknown as SourceDef] };
   }
   return {
      unreadable: `its segment shape (type ${JSON.stringify(segment.type)}) is not one Publisher reads`,
      label: labelOf(segment),
   };
}
