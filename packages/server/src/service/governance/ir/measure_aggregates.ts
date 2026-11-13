// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The aggregates a compiled measure computes, read from its IR expression
 * (`field.e`), with every reference to another measure of the same source
 * followed into that measure's own expression. One entry per aggregate the
 * compiler emits: `x.sum()` is `{fn: "sum"}`, `carriers.count()` is
 * `{fn: "count", structPath: ["carriers"]}`, `count(x)` is `{fn: "distinct"}`.
 *
 * `#(preaggregate)` reads a measure's partials from its text (it has to emit
 * Malloy text, and the compiler offers no printer), and checks that reading
 * against this one: a measure whose text the grammar decomposed into leaves
 * the compiler does not agree with is not stored. Anything here that is not a
 * plain aggregate (an ungrouped `all`/`exclude`, an aggregate function call
 * such as `stddev`, an aggregate inside an aggregate, a reference that loops)
 * is reported as such, so the comparison fails closed.
 */

/** One aggregate in a measure, as the compiler recorded it. */
export interface MeasureAggregate {
   /**
    * The compiler's aggregate (`sum`, `count`, `min`, `max`, `avg`,
    * `distinct`), `ungrouped` for `all`/`exclude`, `call:<name>` for an
    * aggregate function call, or `nested` for an aggregate of an aggregate.
    */
   fn: string;
   /** The join the aggregate is taken at; empty for the source's own rows. */
   structPath: string[];
}

interface IrNode {
   node?: unknown;
   function?: unknown;
   name?: unknown;
   path?: unknown;
   structPath?: unknown;
   e?: unknown;
   kids?: unknown;
   overload?: { returnType?: { expressionType?: unknown } };
}

interface IrField {
   name?: unknown;
   as?: unknown;
   e?: unknown;
   expressionType?: unknown;
}

/** Keys of an IR node that never hold an expression the measure computes. */
const NOT_EXPRESSIONS = new Set([
   "at",
   "overload",
   "refSummary",
   "stableFilter",
]);

/**
 * The aggregates measure `name` of `def` computes, or undefined when `name`
 * is not a measure of `def` with a compiled expression.
 */
export function measureAggregates(
   def: unknown,
   name: string,
): MeasureAggregate[] | undefined {
   const fields = ((def as { fields?: unknown[] } | undefined)?.fields ??
      []) as IrField[];
   const byName = new Map<string, IrField>();
   for (const f of fields) {
      const n =
         typeof f.as === "string"
            ? f.as
            : typeof f.name === "string"
              ? f.name
              : undefined;
      if (n !== undefined) byName.set(n, f);
   }
   const isMeasure = (f: IrField | undefined) =>
      f?.expressionType === "aggregate" && f.e !== undefined;
   const root = byName.get(name);
   if (!isMeasure(root)) return undefined;

   const out: MeasureAggregate[] = [];
   const walk = (
      e: unknown,
      expanding: ReadonlySet<string>,
      inAggregate: boolean,
      depth: number,
   ): void => {
      if (depth > 256) {
         out.push({ fn: "nested", structPath: [] });
         return;
      }
      if (Array.isArray(e)) {
         for (const k of e) walk(k, expanding, inAggregate, depth + 1);
         return;
      }
      if (!e || typeof e !== "object") return;
      const n = e as IrNode;
      if (n.node === "aggregate") {
         if (inAggregate) {
            out.push({ fn: "nested", structPath: [] });
            return;
         }
         out.push({
            fn: typeof n.function === "string" ? n.function : "unknown",
            structPath: Array.isArray(n.structPath)
               ? n.structPath.filter((p): p is string => typeof p === "string")
               : [],
         });
         walk(n.e, expanding, true, depth + 1);
         return;
      }
      if (n.node === "all" || n.node === "exclude") {
         out.push({ fn: "ungrouped", structPath: [] });
         return;
      }
      if (
         n.node === "function_call" &&
         n.overload?.returnType?.expressionType === "aggregate"
      ) {
         out.push({
            fn: `call:${typeof n.name === "string" ? n.name : "unknown"}`,
            structPath: [],
         });
         return;
      }
      // A joined source's measure, read outside an aggregate: the joined
      // source's expression is not followed here.
      if (
         n.node === "field" &&
         !inAggregate &&
         Array.isArray(n.path) &&
         n.path.length > 1
      ) {
         out.push({ fn: "reference", structPath: [] });
         return;
      }
      if (
         n.node === "field" &&
         Array.isArray(n.path) &&
         n.path.length === 1 &&
         typeof n.path[0] === "string"
      ) {
         const ref = n.path[0];
         const f = byName.get(ref);
         if (isMeasure(f)) {
            if (inAggregate || expanding.has(ref)) {
               out.push({ fn: "nested", structPath: [] });
               return;
            }
            walk(f?.e, new Set([...expanding, ref]), false, depth + 1);
            return;
         }
      }
      for (const [key, v] of Object.entries(n)) {
         if (NOT_EXPRESSIONS.has(key)) continue;
         if (v && typeof v === "object") {
            walk(v, expanding, inAggregate, depth + 1);
         }
      }
   };
   walk(root?.e, new Set([name]), false, 0);
   return out;
}
