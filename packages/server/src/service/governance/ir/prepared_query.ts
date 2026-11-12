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
 * One level of a query that reads its source's rows: the first stage, or the
 * first stage of a `nest:` in it. What a stored rollup must reproduce to
 * answer it (see `serve/rollup_variants.ts`).
 */
export interface QueryLevelFacts {
   /** `reduce` (group_by/aggregate), `project` (select:), `index`, or another kind. */
   kind: string;
   /** The `where:` filters' text, in order. */
   whereCodes: string[];
   /** The `having:` filters' text (filters on an aggregate), in order. */
   havingCodes: string[];
   /**
    * Each `nest:` defined inline at this level, by output name. A nest that
    * names a view (`nest: by_category`) is not here: its stages are on the
    * source, not the query.
    */
   nests: Map<string, QueryLevelFacts>;
}

interface IrStageLike {
   type?: unknown;
   filterList?: { code?: unknown; expressionType?: unknown }[];
   queryFields?: {
      type?: unknown;
      name?: unknown;
      as?: unknown;
      path?: unknown;
      pipeline?: unknown;
   }[];
}

/**
 * The first stage of a prepared query and of each inline nest in it, or
 * undefined when the query has no stage. A later stage reads the previous
 * stage's output, not the source's rows, so it is not read.
 */
export function queryLevelFacts(
   view: PreparedQueryView,
): QueryLevelFacts | undefined {
   const pipeline = view.pipeline;
   if (!Array.isArray(pipeline) || pipeline.length === 0) return undefined;
   const read = (stage: IrStageLike, depth: number): QueryLevelFacts => {
      const facts: QueryLevelFacts = {
         kind: typeof stage.type === "string" ? stage.type : "unknown",
         whereCodes: [],
         havingCodes: [],
         nests: new Map(),
      };
      for (const f of stage.filterList ?? []) {
         const code = typeof f.code === "string" ? f.code : "";
         if (f.expressionType === "scalar") facts.whereCodes.push(code);
         else facts.havingCodes.push(code);
      }
      for (const field of stage.queryFields ?? []) {
         const name =
            typeof field.as === "string"
               ? field.as
               : typeof field.name === "string"
                 ? field.name
                 : Array.isArray(field.path)
                   ? String(field.path[field.path.length - 1])
                   : undefined;
         if (name === undefined) continue;
         if (field.type !== "turtle") continue;
         const first = Array.isArray(field.pipeline)
            ? (field.pipeline[0] as IrStageLike | undefined)
            : undefined;
         if (first && depth < 64) facts.nests.set(name, read(first, depth + 1));
      }
      return facts;
   };
   return read(pipeline[0] as IrStageLike, 0);
}
