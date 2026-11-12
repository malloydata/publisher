// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The governance view of a compiled (prepared) query: the ONE place governance
 * code reads Malloy's private `_query` / `_modelDef` and decodes a
 * `structRef`. Malloy exports no type for any of these, so every field is
 * duck-typed here, once.
 *
 * Each decoder below preserves the exact semantics of the ad hoc decoder it
 * replaced, and the differences between them are deliberate, so they are
 * separate functions rather than one with flags:
 *
 *  - {@link resolveStructRef}: a string names a `contents` entry (ANY entry,
 *    not checked to be a source); an object IS the struct.
 *  - {@link resolveStructRefSource}: as above, but a string must name a
 *    `SourceDef` entry (`isSourceDef`), else `undefined`.
 *  - {@link sqlTargetRef}: the compiler's own precedence,
 *    `compositeResolvedSourceDef ?? structRef`, never a union.
 *  - {@link structRefName}: a string ref is the name; an object ref is
 *    `as || name` (an EMPTY `as` falls through to `name`, unlike `as ?? name`).
 *  - {@link probeFilterList}: `structRef.filterList` read off an OBJECT ref
 *    only; a string ref has none.
 *
 * Pure and SDK-type-only, so it bundles anywhere governance code runs.
 */

import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";

/** The raw `_query` of a prepared query, duck-typed. */
export interface IrQueryLike {
   structRef?: unknown;
   compositeResolvedSourceDef?: unknown;
   location?: unknown;
   pipeline?: unknown;
   [key: string]: unknown;
}

/** A prepared query as governance reads it. */
export interface PreparedQueryView {
   /** `_query`, as compiled; `undefined` when the prepared query has none. */
   readonly query: IrQueryLike | undefined;
   /** `_query.structRef`: the run target, a `contents` name or an inline struct. */
   readonly structRef: unknown;
   /** `_query.compositeResolvedSourceDef`: the composite member SQL is generated from. */
   readonly compositeResolvedSourceDef: unknown;
   /** `_query.location`. */
   readonly location: unknown;
   /** `_query.pipeline`. */
   readonly pipeline: unknown;
   /** `_modelDef`, with NO fallback: callers supply their own. */
   readonly modelDef: ModelDef | undefined;
}

/** Read a prepared query (what `getPreparedQuery()` resolved to). */
export function preparedQueryView(prepared: unknown): PreparedQueryView {
   const p = (prepared ?? {}) as {
      _query?: IrQueryLike;
      _modelDef?: ModelDef;
   };
   const query = p._query;
   return {
      query,
      structRef: query?.structRef,
      compositeResolvedSourceDef: query?.compositeResolvedSourceDef,
      location: query?.location,
      pipeline: query?.pipeline,
      modelDef: p._modelDef,
   };
}

/** `runnable.getPreparedQuery()`, read as a {@link PreparedQueryView}. Throws what it throws. */
export async function readPreparedQuery(runnable: {
   getPreparedQuery(): Promise<unknown>;
}): Promise<PreparedQueryView> {
   return preparedQueryView(await runnable.getPreparedQuery());
}

/**
 * A `structRef` resolved to its struct: a string names a `contents` entry
 * (not checked to be a source); an object is the struct itself. Anything that
 * is not an object after resolution is `undefined`.
 */
export function resolveStructRef(
   ref: unknown,
   modelDef: ModelDef | undefined,
): SourceDef | undefined {
   const resolved = typeof ref === "string" ? modelDef?.contents[ref] : ref;
   return resolved && typeof resolved === "object"
      ? (resolved as SourceDef)
      : undefined;
}

/**
 * {@link resolveStructRef}, except a string must name a `SourceDef`
 * (`isSourceDef`) in `contents`, else `undefined`. An object ref is returned
 * as is, unchecked.
 */
export function resolveStructRefSource<T = SourceDef>(
   ref: unknown,
   modelDef: ModelDef | undefined,
): T | undefined {
   if (typeof ref === "string") {
      const entry = modelDef?.contents[ref];
      return entry && isSourceDef(entry) ? (entry as unknown as T) : undefined;
   }
   return ref && typeof ref === "object" ? (ref as T) : undefined;
}

/**
 * The ref Malloy generates SQL from: `compositeResolvedSourceDef ??
 * structRef`. Mirrors the compiler's precedence; never a union, since checking
 * the struct that does not generate SQL proves nothing.
 */
export function sqlTargetRef(view: PreparedQueryView): unknown {
   return view.compositeResolvedSourceDef ?? view.structRef;
}

/**
 * The name a `structRef` is read under: the string itself, or an inline
 * struct's `as || name` (an empty `as` falls through to `name`).
 */
export function structRefName(ref: unknown): string | undefined {
   if (typeof ref === "string") return ref;
   if (ref && typeof ref === "object") {
      const s = ref as { as?: string; name?: string };
      return s.as || s.name;
   }
   return undefined;
}

/**
 * `structRef.filterList` of a prepared probe query: where a `run: X extend {
 * where: … }` probe's compiled condition lands. Read off an inline (object)
 * ref only; a string ref carries no list.
 */
export function probeFilterList<T>(view: PreparedQueryView): T[] | undefined {
   const ref = view.structRef as { filterList?: T[] } | undefined;
   return ref?.filterList;
}

/**
 * The join paths a prepared query aggregates at a join's own grain: each
 * `expandedFieldUsage` entry (in every segment of its pipeline, nested ones
 * included) that carries a `uniqueKeyRequirement`, which Malloy records where
 * it needs a symmetric aggregate (`j.x.sum()`, `j.count()`, a measure that
 * reads one) and nowhere else. A path is relative to the segment's source;
 * the empty path is the source's own grain. `unreadable` is set when an entry
 * carries a requirement on a path that is not a list of names, so a caller
 * fails closed.
 */
export function joinGrainAggregatePaths(view: PreparedQueryView): {
   paths: string[][];
   unreadable: boolean;
} {
   const out: string[][] = [];
   let unreadable = false;
   const walk = (node: unknown, depth: number): void => {
      if (!node || typeof node !== "object" || depth > 64) return;
      if (Array.isArray(node)) {
         for (const n of node) walk(n, depth + 1);
         return;
      }
      const usage = (node as { expandedFieldUsage?: unknown })
         .expandedFieldUsage;
      if (Array.isArray(usage)) {
         for (const u of usage as {
            path?: unknown;
            uniqueKeyRequirement?: unknown;
         }[]) {
            if (!u || u.uniqueKeyRequirement === undefined) continue;
            if (
               !Array.isArray(u.path) ||
               u.path.some((p) => typeof p !== "string")
            ) {
               unreadable = true;
               continue;
            }
            out.push(u.path as string[]);
         }
      }
      for (const [key, value] of Object.entries(node)) {
         // A segment's source and output shape are not the query's reads.
         if (key === "structRef" || key === "outputStruct") continue;
         walk(value, depth + 1);
      }
   };
   walk(view.pipeline, 0);
   return { paths: out, unreadable };
}
