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
 * `_query.givenUsage` of a prepared query: the ids of the givens the query
 * reads, through the fields and joins it uses and their defaults, and not
 * through a field it leaves unread. Undefined when the prepared query carries
 * no such list (a caller decides what that means; every caller today fails
 * closed). The one reader of this slot: the governor's surface filter, report
 * tiles and the probe compiler (`../../probe_compiler.ts`) all read it here.
 */
export function queryGivenUsageIds(prepared: unknown): string[] | undefined {
   const usage = preparedQueryView(prepared).query?.givenUsage;
   if (!Array.isArray(usage)) return undefined;
   return usage
      .map((g) => (g as { id?: unknown } | null)?.id)
      .filter((id): id is string => typeof id === "string");
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

/**
 * One field of a {@link QueryLevelFacts} level, as the compiler recorded it.
 */
export interface QueryLevelField {
   /** Its output name. */
   name: string;
   /**
    * The field it names as is (`group_by: category`, `aggregate: total`):
    * its path. Undefined for a field the query defines (`x is …`).
    */
   ref?: string[];
   /** For a field the query defines: the compiler's expression type. */
   expressionType?: string;
   /**
    * Whether its own expression computes an aggregate over the source's rows
    * (`count()`, `x.sum()`, an aggregate function such as `stddev`), as
    * opposed to reading measures by name.
    */
   computesAggregate: boolean;
   /** Whether its expression is ungrouped (`all(…)`, `exclude(…)`). */
   ungrouped: boolean;
   /**
    * For a field the query defines: every field path its expression reads
    * (`segments.seg_miles` reads `["segments", "seg_miles"]`), undefined when
    * one is not a list of names. Empty for a field it names as is (`ref`).
    */
   reads?: string[][];
   /** For a `nest:` with its own stages: the nest's first stage. */
   nest?: QueryLevelFacts;
   /** Whether it is a `nest:` at all (with or without a readable stage). */
   isNest: boolean;
}

/**
 * One level of a query that reads its source's rows: the first stage, or the
 * first stage of a `nest:` in it. What a stored rollup must reproduce to
 * answer it (see `serve/rollup_variants.ts`). Read from the compiled IR only.
 */
export interface QueryLevelFacts {
   /** `reduce` (group_by/aggregate), `project` (select:), `index`, or another kind. */
   kind: string;
   /** The level's fields, in order. */
   fields: QueryLevelField[];
   /**
    * The fields its `where:` filters fix to one value: a conjunct
    * `field = literal` (either way round) or `field = null`, by name.
    */
   fixedByWhere: Set<string>;
   /**
    * The same conjuncts by path, a joined field's included
    * (`segments.kind = 'p'` fixes `["segments", "kind"]`).
    */
   fixedPathsByWhere: string[][];
   /** Its `having:` filters (filters on an aggregate). */
   havings: { computesAggregate: boolean }[];
}

interface IrExprLike {
   node?: unknown;
   path?: unknown;
   kids?: { left?: unknown; right?: unknown };
   e?: unknown;
   overload?: { returnType?: { expressionType?: unknown } };
}

interface IrStageLike {
   type?: unknown;
   filterList?: { e?: unknown; expressionType?: unknown }[];
   queryFields?: {
      type?: unknown;
      name?: unknown;
      as?: unknown;
      path?: unknown;
      e?: unknown;
      expressionType?: unknown;
      pipeline?: unknown;
   }[];
}

/** Keys of an IR node that never hold an expression the query computes. */
const NOT_EXPRESSIONS = new Set([
   "at",
   "overload",
   "refSummary",
   "stableFilter",
]);

/** Whether `e` holds an IR node `test` accepts (not under a non-expression key). */
function holds(
   e: unknown,
   test: (n: IrExprLike) => boolean,
   depth = 0,
): boolean {
   if (depth > 256) return true;
   if (Array.isArray(e)) return e.some((k) => holds(k, test, depth + 1));
   if (!e || typeof e !== "object") return false;
   const n = e as IrExprLike;
   if (test(n)) return true;
   return Object.entries(n).some(
      ([key, v]) =>
         !NOT_EXPRESSIONS.has(key) &&
         !!v &&
         typeof v === "object" &&
         holds(v, test, depth + 1),
   );
}

/** An aggregate the expression itself computes over rows. */
function computesAggregate(e: unknown): boolean {
   return holds(
      e,
      (n) =>
         n.node === "aggregate" ||
         (n.node === "function_call" &&
            n.overload?.returnType?.expressionType === "aggregate"),
   );
}

const LITERALS = new Set(["numberLiteral", "stringLiteral", "true", "false"]);

/** The field path a node reads, if it is exactly a field. */
function fieldPathOf(e: unknown): string[] | undefined {
   const n = e as IrExprLike | undefined;
   return n?.node === "field" &&
      Array.isArray(n.path) &&
      n.path.length > 0 &&
      n.path.every((p) => typeof p === "string")
      ? (n.path as string[])
      : undefined;
}

/** The fields a `where:` condition fixes to one value, by its conjuncts, by path. */
function fixedFields(e: unknown, out: string[][], depth = 0): void {
   const n = e as IrExprLike | undefined;
   if (!n || depth > 64) return;
   if (n.node === "and") {
      fixedFields(n.kids?.left, out, depth + 1);
      fixedFields(n.kids?.right, out, depth + 1);
      return;
   }
   if (n.node === "is-null") {
      const f = fieldPathOf(n.e);
      if (f !== undefined) out.push(f);
      return;
   }
   if (n.node !== "=") return;
   const left = n.kids?.left as IrExprLike | undefined;
   const right = n.kids?.right as IrExprLike | undefined;
   const isLiteral = (x: IrExprLike | undefined) =>
      typeof x?.node === "string" && LITERALS.has(x.node);
   const f = isLiteral(right)
      ? fieldPathOf(left)
      : isLiteral(left)
        ? fieldPathOf(right)
        : undefined;
   if (f !== undefined) out.push(f);
}

/**
 * Every field path `e` reads, or undefined when one is not a list of names or
 * the expression is too deep to read whole.
 */
function fieldPathsRead(e: unknown): string[][] | undefined {
   const out: string[][] = [];
   let unreadable = false;
   const walk = (x: unknown, depth: number): void => {
      if (unreadable || !x || typeof x !== "object") return;
      if (depth > 256) {
         unreadable = true;
         return;
      }
      if (Array.isArray(x)) {
         for (const k of x) walk(k, depth + 1);
         return;
      }
      const n = x as IrExprLike;
      if (n.node === "field") {
         const p = fieldPathOf(n);
         if (p) out.push(p);
         else unreadable = true;
      }
      for (const [key, v] of Object.entries(n)) {
         if (!NOT_EXPRESSIONS.has(key)) walk(v, depth + 1);
      }
   };
   walk(e, 0);
   return unreadable ? undefined : out;
}

/**
 * The first stage of a prepared query and of each nest in it, or undefined
 * when the query has no stage. A later stage reads the previous stage's
 * output, not the source's rows, so it is not read.
 */
export function queryLevelFacts(
   view: PreparedQueryView,
): QueryLevelFacts | undefined {
   const pipeline = view.pipeline;
   if (!Array.isArray(pipeline) || pipeline.length === 0) return undefined;
   const read = (stage: IrStageLike, depth: number): QueryLevelFacts => {
      const facts: QueryLevelFacts = {
         kind: typeof stage.type === "string" ? stage.type : "unknown",
         fields: [],
         fixedByWhere: new Set(),
         fixedPathsByWhere: [],
         havings: [],
      };
      for (const f of stage.filterList ?? []) {
         if (f.expressionType === "scalar")
            fixedFields(f.e, facts.fixedPathsByWhere);
         else facts.havings.push({ computesAggregate: computesAggregate(f.e) });
      }
      for (const p of facts.fixedPathsByWhere) {
         if (p.length === 1) facts.fixedByWhere.add(p[0] as string);
      }
      for (const field of stage.queryFields ?? []) {
         const path = Array.isArray(field.path)
            ? field.path.filter((p): p is string => typeof p === "string")
            : undefined;
         const name =
            typeof field.as === "string"
               ? field.as
               : typeof field.name === "string"
                 ? field.name
                 : path?.[path.length - 1];
         if (name === undefined) continue;
         const isNest = field.type === "turtle";
         const first =
            isNest && Array.isArray(field.pipeline)
               ? (field.pipeline[0] as IrStageLike | undefined)
               : undefined;
         facts.fields.push({
            name,
            ...(field.type === "fieldref" && path ? { ref: path } : {}),
            ...(typeof field.expressionType === "string"
               ? { expressionType: field.expressionType }
               : {}),
            computesAggregate: computesAggregate(field.e),
            ungrouped: holds(
               field.e,
               (n) => n.node === "all" || n.node === "exclude",
            ),
            ...(field.type === "fieldref"
               ? { reads: [] }
               : (() => {
                    const reads = fieldPathsRead(field.e);
                    return reads ? { reads } : {};
                 })()),
            isNest,
            ...(first && depth < 64 ? { nest: read(first, depth + 1) } : {}),
         });
      }
      return facts;
   };
   return read(pipeline[0] as IrStageLike, 0);
}

/**
 * The views of `struct` a compiled query reads by name: `called` (a view the
 * query's text runs, `src -> v`, which the compiled pipeline holds expanded,
 * without its name), then every `nest: v` in the first stage of the query or
 * of a view it reads, at any depth (a nest under a view's name counts as that
 * view, so a reader may declare one view more than it needs, never one
 * fewer). Each comes after the views it nests; a view that nests itself is
 * read once.
 */
export function namedViewsRead(
   view: PreparedQueryView,
   struct: unknown,
   called?: string,
): string[] {
   const turtles = new Map<string, { pipeline?: unknown }>();
   for (const f of ((struct as { fields?: unknown[] } | undefined)?.fields ??
      []) as {
      type?: unknown;
      name?: unknown;
      as?: unknown;
      pipeline?: unknown;
   }[]) {
      const n =
         typeof f.as === "string"
            ? f.as
            : typeof f.name === "string"
              ? f.name
              : undefined;
      if (n !== undefined && f.type === "turtle") turtles.set(n, f);
   }
   const out: string[] = [];
   const seen = new Set<string>();
   // Each view after the views it nests, so they can be declared in order.
   const add = (name: string, depth: number) => {
      const turtle = turtles.get(name);
      if (!turtle || seen.has(name)) return;
      seen.add(name);
      visit(firstStage(turtle.pipeline), depth + 1);
      out.push(name);
   };
   const visit = (stage: IrStageLike | undefined, depth: number): void => {
      if (!stage || depth > 64) return;
      for (const field of stage.queryFields ?? []) {
         if (
            field.type === "fieldref" &&
            Array.isArray(field.path) &&
            field.path.length === 1 &&
            typeof field.path[0] === "string"
         ) {
            add(field.path[0], depth);
         } else if (field.type === "turtle") {
            // `nest: v` compiles to a turtle named `v`, the view expanded. A
            // nest of the query's own under a view's name reads as that view
            // too: one more view declared, never one fewer.
            if (typeof field.name === "string") add(field.name, depth);
            visit(firstStage(field.pipeline), depth + 1);
         }
      }
   };
   if (called !== undefined) add(called, 0);
   visit(firstStage(view.pipeline), 0);
   return out;
}

function firstStage(pipeline: unknown): IrStageLike | undefined {
   return Array.isArray(pipeline)
      ? (pipeline[0] as IrStageLike | undefined)
      : undefined;
}

/**
 * Whether a compiled query sets a `timezone:` anywhere in it: a stage, a later
 * stage, or a `nest:` at any depth. A stored table truncated or filtered its
 * times in the model's zone, so a leg that reads one declines any of these.
 */
export function setsTimezone(query: unknown, depth = 0): boolean {
   if (!query || typeof query !== "object" || depth > 64) return false;
   if (Array.isArray(query)) {
      return query.some((q) => setsTimezone(q, depth + 1));
   }
   for (const [k, v] of Object.entries(query)) {
      if (k === "queryTimezone" && typeof v === "string") return true;
      if (typeof v === "object" && setsTimezone(v, depth + 1)) return true;
   }
   return false;
}

/**
 * Whether a compiled expression or query holds raw SQL (the `sql_*` family,
 * `genericSQLExpr`) anywhere it computes or reads. Its text names columns by
 * itself (`${TABLE}.region`, or a bare `region`), which the compiler records
 * as no field read, so a reader of field references cannot tell what it
 * needs.
 */
export function readsRawSql(node: unknown): boolean {
   let found = false;
   eachQueryNode(node, (n) => {
      if (n.node === "genericSQLExpr") found = true;
   });
   return found;
}

/** Keys of a stage that hold no expression the query computes or reads. */
const NOT_QUERY_READS = new Set([
   ...NOT_EXPRESSIONS,
   "structRef",
   "outputStruct",
]);

/** Visit every object under `node` but those under {@link NOT_QUERY_READS}. */
function eachQueryNode(
   node: unknown,
   visit: (n: Record<string, unknown>) => void,
   depth = 0,
): void {
   if (!node || typeof node !== "object" || depth > 256) return;
   if (Array.isArray(node)) {
      for (const n of node) eachQueryNode(n, visit, depth + 1);
      return;
   }
   const n = node as Record<string, unknown>;
   visit(n);
   for (const [key, value] of Object.entries(n)) {
      if (NOT_QUERY_READS.has(key)) continue;
      if (value && typeof value === "object") {
         eachQueryNode(value, visit, depth + 1);
      }
   }
}

/**
 * Every field path a prepared query reads, as the compiler expanded them
 * (`expandedFieldUsage` on each segment, nested ones included): a measure or
 * dimension named by the query is followed into what it reads, so a source's
 * measure that reads a join shows the join's paths too. Relative to the
 * segment's source. Undefined when an entry is not a list of names, so a
 * caller fails closed.
 */
export function queryFieldUsagePaths(
   view: PreparedQueryView,
): string[][] | undefined {
   const out: string[][] = [];
   let unreadable = false;
   eachQueryNode(view.pipeline, (n) => {
      const usage = n.expandedFieldUsage;
      if (!Array.isArray(usage)) return;
      for (const u of usage as { path?: unknown }[]) {
         if (
            !Array.isArray(u?.path) ||
            u.path.some((p) => typeof p !== "string")
         ) {
            unreadable = true;
            continue;
         }
         out.push(u.path as string[]);
      }
   });
   return unreadable ? undefined : out;
}

/** An aggregate node: `x.sum()`, `count()`, or an aggregate function call. */
function isAggregateNode(n: IrExprLike): boolean {
   return (
      n.node === "aggregate" ||
      (n.node === "function_call" &&
         n.overload?.returnType?.expressionType === "aggregate")
   );
}

/**
 * Whether `e` computes an aggregate of its own over the rows of `join`: one
 * taken at the join (`j.count()`, `j.x.sum()`) or over a value read through
 * it (`sum(j.x)`, `count(j.k)`, `count_approx(j.k)`). Reading a measure of
 * the joined source by name (`j.total`) is not one: that aggregate is the
 * joined source's own.
 */
function aggregatesThrough(e: unknown, join: string): boolean {
   const readsJoin = (x: unknown) =>
      holds(
         x,
         (k) =>
            k.node === "field" &&
            Array.isArray(k.path) &&
            k.path.length > 1 &&
            k.path[0] === join,
      );
   return holds(e, (n) => {
      if (!isAggregateNode(n)) return false;
      const at = (n as { structPath?: unknown }).structPath;
      if (Array.isArray(at) && at[0] === join) return true;
      const { node: _node, ...rest } = n as Record<string, unknown>;
      return readsJoin(rest);
   });
}

/**
 * The first field (by name) a prepared query reads that computes an aggregate
 * of its own over `join`'s rows: one the query writes (in a field, a filter,
 * a `having:`, a query-level `extend:` or a nest), or a measure of
 * `sourceDef` that the query reads, directly or through another of its
 * measures (`measure: n is segments.count()`). Undefined when there is none.
 * A table built at the joined source's grain answers only the joined
 * source's own measures; any other aggregate over its rows would count its
 * rows, not the joined rows.
 */
export function aggregateWrittenThroughJoin(
   view: PreparedQueryView,
   sourceDef: unknown,
   join: string,
): string | undefined {
   return firstAggregateWhere(view.pipeline, view, sourceDef, (e) =>
      aggregatesThrough(e, join),
   );
}

/**
 * Whether `e` computes an aggregate at a grain other than `join`'s: one not
 * taken at the join (`count()`, `distance.sum()`, `other.x.sum()`). An
 * aggregate taken at the join (`j.x.sum()`, `sum(j.x)`) is not one.
 */
function aggregatesOutside(e: unknown, join: string): boolean {
   return holds(e, (n) => {
      if (!isAggregateNode(n)) return false;
      const at = (n as { structPath?: unknown }).structPath;
      if (Array.isArray(at) && at.length > 0) return at[0] !== join;
      const { node: _node, ...rest } = n as Record<string, unknown>;
      let reads = false;
      const outside = holds(rest, (k) => {
         if (k.node !== "field" || !Array.isArray(k.path)) return false;
         if (k.path.length > 1 && k.path[0] === join) {
            reads = true;
            return false;
         }
         return true;
      });
      return outside || !reads;
   });
}

/**
 * The first field (by name) the FIRST stage of a prepared query reads that
 * computes an aggregate at a grain other than `join`'s: a `count()` or a sum
 * over the source's own rows, or a measure of another join, written in the
 * query (in a field, a filter, a `having:` or a nest) or read by name, from
 * `sourceDef`'s measures or Malloy's own record of the aggregates it needs a
 * key for. Undefined when every aggregate is taken at `join`. Through a
 * join_one that is not unique on its key, live repeats each parent row once
 * per joined row, so such an aggregate counts the repeats; a table with one
 * row per key does not. Later stages read the first stage's output rows and
 * are not read.
 */
export function aggregateOutsideJoin(
   view: PreparedQueryView,
   sourceDef: unknown,
   join: string,
): string | undefined {
   const first = (view.pipeline as unknown[] | undefined)?.[0];
   if (first === undefined) return undefined;
   // A count at the source's grain or any aggregate at another join's, as
   // the compiler recorded it.
   let unreadable = false;
   let keyed: string | undefined;
   eachQueryNode(first, (n) => {
      const usage = n.expandedFieldUsage;
      if (!Array.isArray(usage)) return;
      for (const u of usage as {
         path?: unknown;
         uniqueKeyRequirement?: unknown;
      }[]) {
         if (!u || u.uniqueKeyRequirement === undefined) continue;
         if (!Array.isArray(u.path)) {
            unreadable = true;
            continue;
         }
         if (u.path.length !== 1 || u.path[0] !== join) {
            keyed ??=
               u.path.length === 0
                  ? "count()"
                  : `an aggregate of \`${u.path.join(".")}\``;
         }
      }
   });
   if (unreadable) return "a field the compiler did not record";
   if (keyed !== undefined) return keyed;
   return firstAggregateWhere(first, view, sourceDef, (e) =>
      aggregatesOutside(e, join),
   );
}

/**
 * The first field (by name) under `pipeline` whose expression `offends`, or
 * a measure of `sourceDef` the query reads (directly or through another of
 * its measures) whose definition does.
 */
function firstAggregateWhere(
   pipeline: unknown,
   view: PreparedQueryView,
   sourceDef: unknown,
   offendsExpr: (e: unknown) => boolean,
): string | undefined {
   let found: string | undefined;
   eachQueryNode(pipeline, (n) => {
      if (found !== undefined) return;
      const e = (n as { e?: unknown }).e;
      if (e !== undefined && offendsExpr(e)) {
         found =
            typeof n.as === "string"
               ? n.as
               : typeof n.name === "string"
                 ? n.name
                 : typeof n.code === "string"
                   ? n.code
                   : "an expression";
      }
   });
   if (found !== undefined) return found;
   // The source's own fields that do, followed through each other by name.
   const fields = ((sourceDef as { fields?: unknown[] } | undefined)?.fields ??
      []) as { name?: unknown; as?: unknown; e?: unknown }[];
   const byName = new Map<string, unknown>();
   for (const f of fields) {
      const name =
         typeof f.as === "string"
            ? f.as
            : typeof f.name === "string"
              ? f.name
              : undefined;
      if (name !== undefined && f.e !== undefined) byName.set(name, f.e);
   }
   const memo = new Map<string, boolean>();
   const offends = (name: string, depth = 0): boolean => {
      const known = memo.get(name);
      if (known !== undefined) return known;
      const e = byName.get(name);
      if (e === undefined || depth > 64) return depth > 64;
      memo.set(name, false);
      const result =
         offendsExpr(e) ||
         holds(
            e,
            (k) =>
               k.node === "field" &&
               Array.isArray(k.path) &&
               k.path.length === 1 &&
               typeof k.path[0] === "string" &&
               k.path[0] !== name &&
               offends(k.path[0], depth + 1),
         );
      memo.set(name, result);
      return result;
   };
   const usage = queryFieldUsagePaths(view);
   if (usage === undefined) return "a field the compiler did not record";
   for (const path of usage) {
      const head = path[0];
      if (path.length === 1 && head !== undefined && offends(head)) return head;
   }
   return undefined;
}

/**
 * How each field of `sourceDef` reads `join`, by name: `none` when its value
 * reads nothing through the join, directly or through the source's other
 * fields; else `scalar` for a dimension and `aggregate` for anything else (a
 * measure, an analytic, an unrecorded type). A name the source does not
 * declare, or a definition too deep or unreadable, is `aggregate`, so a
 * caller fails closed.
 */
export function sourceFieldJoinReads(
   sourceDef: unknown,
   join: string,
): (name: string) => "none" | "scalar" | "aggregate" {
   const fields = ((sourceDef as { fields?: unknown[] } | undefined)?.fields ??
      []) as {
      name?: unknown;
      as?: unknown;
      e?: unknown;
      join?: unknown;
      expressionType?: unknown;
   }[];
   const byName = new Map<string, (typeof fields)[number]>();
   for (const f of fields) {
      const name =
         typeof f.as === "string"
            ? f.as
            : typeof f.name === "string"
              ? f.name
              : undefined;
      if (name !== undefined) byName.set(name, f);
   }
   const memo = new Map<string, boolean>();
   const readsJoin = (name: string, depth: number): boolean => {
      const known = memo.get(name);
      if (known !== undefined) return known;
      const f = byName.get(name);
      if (f === undefined || depth > 64) return true;
      if (f.join !== undefined) return name === join;
      // Guards a cycle; a field that reads itself does not compile.
      memo.set(name, false);
      const paths = f.e === undefined ? [] : fieldPathsRead(f.e);
      const result =
         paths === undefined ||
         paths.some((p) =>
            p.length > 1
               ? p[0] === join
               : p[0] !== name && readsJoin(p[0] as string, depth + 1),
         );
      memo.set(name, result);
      return result;
   };
   return (name) => {
      const f = byName.get(name);
      if (f === undefined) return "aggregate";
      if (!readsJoin(name, 0)) return "none";
      return f.expressionType === "scalar" ? "scalar" : "aggregate";
   };
}
