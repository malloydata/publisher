// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The workload shape of one query (materialization v2, T1): what it asked of
 * its source, with every literal replaced, so a hundred runs of one dashboard
 * tile with different filter values are one shape. The workload log keys on
 * it, and what reads the log later (table stats, what-if, compare) groups by
 * it: which shapes a stored table answers, and how often each ran.
 *
 * Read from the compiled query's FIRST stage, which is the one that reads the
 * source; a later stage reads that stage's output, which no materialization
 * of the source changes. Only the names the query uses and the structure of
 * its expressions are kept, never a value: a filter `region = 'west'` records
 * that `region` was filtered, and its expression as `=(region,?)`. A string in
 * an expression is kept only where it is a node kind, a function, a time unit
 * or a name, so SQL text and literals never reach the record.
 */

import { createHash } from "crypto";
import type { ModelDef } from "@malloydata/malloy";
import { forEachFieldPath } from "./governance/ir/array_query";
import { resolveStructRef } from "./governance/ir/prepared_query";

/** One query's shape. */
export interface WorkloadShape {
   /**
    * A stable digest of the fields below except `view` and `complete`: two
    * queries with the same key ask the same thing of the same source, however
    * the request was phrased.
    */
   key: string;
   /** The source the query entered. */
   source?: string;
   /**
    * The view the request named, when it named one (`queryName` on a source).
    * Not in the key: `run: s -> v` and `queryName: v` compile to the same
    * stage, and are one shape.
    */
   view?: string;
   /** `reduce` (group_by / aggregate), `project` (select) or `index`. */
   kind: string;
   /** The pipeline's stage count; only the first is in the shape. */
   stages: number;
   /** What the first stage groups by (or selects), each a path or an expression. */
   dimensions: string[];
   /** What the first stage aggregates, each a measure's path or an expression. */
   measures: string[];
   /** The fields a `where:` reads, by path. */
   filteredDimensions: string[];
   /** The `where:` and `having:` expressions, literals replaced by `?`. */
   filters: string[];
   /** The query time zone the stage runs in, when it sets one. */
   timezone?: string;
   /**
    * True when the first stage nests a query, inline or a named view; its
    * nests are not in the shape, and a nested view is not a dimension.
    */
   nested?: boolean;
   /** False when a field could not be placed as a dimension or a measure. */
   complete: boolean;
}

/** Keys whose string values are structure, kept in an expression; any other string is a value and becomes `?`. */
const STRUCTURE_KEYS = new Set(["node", "function", "units", "name"]);
/**
 * Keys that are positions, carried type information or compiler internals,
 * never expression. `overload` is a function's whole definition (every
 * dialect's implementation), which would make a key change on every Malloy
 * upgrade; the call is identified by its `name`.
 */
const SKIPPED_KEYS = new Set([
   "at",
   "location",
   "typeDef",
   "node",
   "function",
   "units",
   "literal",
   "overload",
   "structPath",
   "partitionBy",
   "not",
]);

/** A list of values is one `?`, however long, so a multi-select is one shape. */
const collapseValues = (items: readonly string[]): string[] => {
   const out: string[] = [];
   for (const item of items) {
      if (item === "?" && out.includes("?")) continue;
      out.push(item);
   }
   return out;
};

/**
 * An expression as text with every value replaced by `?`: a field reference
 * is its path, an output reference its name, every other node
 * `kind[:function][:units](children)` with children in key order, and a
 * plain container (`kids`, `args`) its children alone.
 */
export function canonicalExpression(e: unknown): string {
   // The nodes on the path being rendered: a true cycle renders empty, while
   // a node the compiler shares between two branches renders in both.
   const ancestors = new Set<object>();
   const render = (n: unknown, key?: string): string => {
      if (n === null || n === undefined) return "";
      if (typeof n !== "object") {
         return typeof n === "string" && key && STRUCTURE_KEYS.has(key)
            ? n
            : "?";
      }
      if (ancestors.has(n)) return "";
      ancestors.add(n);
      try {
         return renderObject(n, key);
      } finally {
         ancestors.delete(n);
      }
   };
   const renderObject = (n: object, key?: string): string => {
      if (Array.isArray(n)) {
         const items = n.map((item) => render(item));
         return `[${(key === "oneOf" ? collapseValues(items) : items).join(",")}]`;
      }
      const obj = n as Record<string, unknown>;
      if (obj.node === "field" && Array.isArray(obj.path)) {
         return obj.path.map(String).join(".");
      }
      if (obj.node === "outputField" && typeof obj.name === "string") {
         return obj.name;
      }
      if ("literal" in obj) return "?";
      const head = [
         obj.node,
         obj.function,
         obj.units,
         obj.node === "in" && obj.not === true ? "not" : undefined,
      ]
         .filter((part): part is string => typeof part === "string" && !!part)
         .join(":");
      const kids = Object.keys(obj)
         .filter((k) => !SKIPPED_KEYS.has(k))
         .sort()
         .map((k) => render(obj[k], k))
         .filter((text) => text !== "");
      // A window function's partition is structure: the field names it
      // partitions by.
      if (Array.isArray(obj.partitionBy) && obj.partitionBy.length > 0) {
         kids.push(`partitionBy=[${obj.partitionBy.map(String).join(",")}]`);
      }
      // A plain container (`kids`, `args`) adds no structure of its own.
      return head ? `${head}(${kids.join(",")})` : kids.join(",");
   };
   return render(e);
}

/**
 * Every field path an expression reads, in order of first appearance; one
 * inside an array query is re-rooted under its array.
 */
function fieldPaths(e: unknown): string[] {
   const out: string[] = [];
   forEachFieldPath(
      e,
      (parts) => {
         const path = parts.join(".");
         if (path && !out.includes(path)) out.push(path);
      },
      { skipKeys: FIELD_PATH_SKIPPED_KEYS },
   );
   return out;
}

const FIELD_PATH_SKIPPED_KEYS = new Set(["at", "location"]);

interface FieldLike {
   name?: string;
   type?: string;
   as?: string;
   expressionType?: string;
   fields?: FieldLike[];
}

/** The field a path names in a source's field list, following joins. */
function fieldAt(
   fields: readonly FieldLike[] | undefined,
   path: readonly string[],
): FieldLike | undefined {
   let scope = fields;
   let found: FieldLike | undefined;
   for (const part of path) {
      found = scope?.find((f) => (f.as ?? f.name) === part);
      if (!found) return undefined;
      scope = found.fields;
   }
   return found;
}

const isAggregate = (expressionType: unknown): boolean =>
   typeof expressionType === "string" && expressionType.includes("aggregate");

/** Sorted and without repeats, so the same fields in another order are one shape. */
const sortedUnique = (values: Iterable<string>): string[] =>
   Array.from(new Set(values)).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

/**
 * The shape of a compiled query, or `undefined` when it has no pipeline. Pure:
 * `query` is the prepared query's `_query`, `modelDef` the model it compiled
 * against, which holds a source the caller declared in the query.
 */
export function workloadShapeOf(args: {
   query: unknown;
   modelDef: ModelDef | undefined;
   source?: string;
   view?: string;
}): WorkloadShape | undefined {
   const q = args.query as
      | { structRef?: unknown; pipeline?: unknown[] }
      | undefined;
   const stages = Array.isArray(q?.pipeline) ? q.pipeline : [];
   if (stages.length === 0) return undefined;
   const stage = (stages[0] ?? {}) as {
      type?: string;
      queryFields?: unknown[];
      filterList?: unknown[];
      queryTimezone?: unknown;
      extendSource?: FieldLike[];
   };
   const struct = resolveStructRef(q?.structRef, args.modelDef) as
      | { fields?: FieldLike[] }
      | undefined;
   const fields = [...(struct?.fields ?? []), ...(stage.extendSource ?? [])];

   let complete = struct !== undefined;
   let nested = false;
   const dimensions: string[] = [];
   const measures: string[] = [];
   for (const raw of stage.queryFields ?? []) {
      const qf = (raw ?? {}) as {
         type?: string;
         path?: unknown[];
         expressionType?: string;
         e?: unknown;
      };
      if (qf.type === "turtle") {
         nested = true;
         continue;
      }
      if (qf.type === "fieldref" && Array.isArray(qf.path)) {
         const path = qf.path.map(String);
         const field = fieldAt(fields, path);
         // `nest: by_category`: a named view, nested, not a dimension.
         if (field?.type === "turtle") {
            nested = true;
            continue;
         }
         if (!field) complete = false;
         (isAggregate(field?.expressionType) ? measures : dimensions).push(
            path.join("."),
         );
         continue;
      }
      // A field the query defines: its expression, without the alias the
      // author chose for it, so `n is count()` and `c is count()` agree.
      const text = canonicalExpression(qf.e);
      (isAggregate(qf.expressionType) ? measures : dimensions).push(text);
   }

   const filterExprs = (stage.filterList ?? []).map(
      (f) => (f as { e?: unknown }).e,
   );
   const filteredDimensions = sortedUnique(
      (stage.filterList ?? [])
         .filter(
            (f) =>
               !isAggregate((f as { expressionType?: unknown }).expressionType),
         )
         .flatMap((f) => fieldPaths((f as { e?: unknown }).e)),
   );
   const timezone =
      typeof stage.queryTimezone === "string" && stage.queryTimezone
         ? stage.queryTimezone
         : undefined;

   // `view` stays out of the key (see WorkloadShape.view).
   const keyed = {
      source: args.source,
      kind: stage.type ?? "reduce",
      stages: stages.length,
      dimensions: sortedUnique(dimensions),
      measures: sortedUnique(measures),
      filteredDimensions,
      filters: sortedUnique(filterExprs.map(canonicalExpression)),
      timezone,
      nested: nested || undefined,
   };
   const key = createHash("sha256")
      .update(JSON.stringify(keyed))
      .digest("hex")
      .slice(0, 16);
   return {
      key,
      ...(keyed.source ? { source: keyed.source } : {}),
      ...(args.view ? { view: args.view } : {}),
      kind: keyed.kind,
      stages: keyed.stages,
      dimensions: keyed.dimensions,
      measures: keyed.measures,
      filteredDimensions: keyed.filteredDimensions,
      filters: keyed.filters,
      ...(timezone ? { timezone } : {}),
      ...(nested ? { nested: true } : {}),
      complete,
   };
}
