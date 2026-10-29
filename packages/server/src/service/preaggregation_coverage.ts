// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether the rollup a composite resolved to can answer a query faithfully.
 *
 * Malloy's composite resolver picks the first member that DEFINES every field a
 * query uses. That is coverage by field availability, and it assumes every
 * member holds the same rows. A rollup does not: it holds one row per grain
 * group. A declared measure survives that because synthesis re-declares it as a
 * merge of its stored partial, but an aggregate evaluated directly over the
 * rollup's rows counts or sums GROUPS:
 *
 *  - `count()` uses no field at all, so every member covers it, and the first
 *    member (the coarsest rollup) answers with its own row count.
 *  - `qty.sum()` with `qty` in the grain resolves too, and sums one `qty` per
 *    group rather than one per base row.
 *
 * Both compile and return a plausible, wrong number. So after the resolver has
 * chosen, this walks the query's first stage: an aggregate is allowed over a
 * rollup only when it is reached through one of the rollup's merged measures. A
 * direct aggregate anywhere else (an output field, a nest, a `having:`, or a
 * measure an inline `extend` defines) refuses the rollup, and the caller serves
 * the query from the base.
 *
 * Deliberately strict: `count(grain_dim)`, `min` and `max` over a grain
 * dimension are correct from a rollup but are refused too, so the rule an author
 * reads is one line. Refusing costs acceleration, never an answer.
 *
 * Pure: reads the compiled IR, touches no I/O.
 */

import type { RollupPlan } from "./preaggregation_synthesis";

/** The IR fragments read here; everything else is walked generically. */
interface IrNode {
   node?: string;
   path?: readonly string[];
   overload?: { returnType?: { expressionType?: string } };
   [key: string]: unknown;
}

interface IrField {
   name?: string;
   as?: string;
   e?: IrNode;
}

interface IrStage {
   queryFields?: readonly (IrField & {
      type?: string;
      path?: readonly string[];
      pipeline?: readonly IrStage[];
   })[];
   filterList?: readonly { e?: IrNode }[];
}

interface IrQuery {
   compositeResolvedSourceDef?: { as?: string; fields?: readonly IrField[] };
   pipeline?: readonly IrStage[];
}

/** Each rollup member's name, mapped to the merged measures it declares. */
export function rollupMeasuresByMember(
   plans: readonly RollupPlan[],
): ReadonlyMap<string, ReadonlySet<string>> {
   return new Map(
      plans.map((p) => [
         p.rollupSourceName,
         new Set(p.measures.map((m) => m.name)),
      ]),
   );
}

/**
 * Why the rollup the query resolved to cannot answer it, or undefined when it
 * can (or when the query did not resolve to a rollup at all).
 *
 * `query` is a prepared query's `_query`.
 */
export function rollupCoverageViolation(
   query: unknown,
   rollupMeasures: ReadonlyMap<string, ReadonlySet<string>>,
): string | undefined {
   const ir = query as IrQuery | undefined;
   const member = ir?.compositeResolvedSourceDef?.as;
   if (!member) return undefined;
   const merged = rollupMeasures.get(member);
   if (!merged) return undefined;

   const memberFields = new Map(
      (ir?.compositeResolvedSourceDef?.fields ?? []).map((f) => [
         f.as ?? f.name,
         f,
      ]),
   );
   const followed = new Set<string>();
   let violation: string | undefined;

   /**
    * A reference into the member. A merged measure is the one sanctioned
    * aggregate; anything else defined on the member (a measure an inline
    * `extend` added, a dimension) is followed into its definition.
    */
   const visitRef = (path: readonly string[] | undefined) => {
      if (!path || path.length !== 1) return;
      const name = path[0];
      if (merged.has(name) || followed.has(name)) return;
      followed.add(name);
      visitExpr(memberFields.get(name)?.e);
   };

   const visitExpr = (e: unknown): void => {
      if (violation || !e || typeof e !== "object") return;
      if (Array.isArray(e)) {
         for (const item of e) visitExpr(item);
         return;
      }
      const n = e as IrNode;
      if (n.node === "aggregate") {
         violation = `aggregate ${String(n.function)}() is computed over the rollup's rows, not from a stored partial`;
         return;
      }
      if (
         n.node === "function_call" &&
         n.overload?.returnType?.expressionType === "aggregate"
      ) {
         violation = `aggregate function ${String(n.name)}() is computed over the rollup's rows, not from a stored partial`;
         return;
      }
      if (n.node === "field") {
         visitRef(n.path);
         return;
      }
      for (const [key, value] of Object.entries(n)) {
         // `overload` holds the function's per-dialect templates and parameter
         // types, not this call's arguments.
         if (key === "overload" || key === "at") continue;
         visitExpr(value);
      }
   };

   // Only a stage that reads the member: a later stage reads the previous
   // stage's output, where `count()` means what it says.
   const visitStage = (stage: IrStage | undefined) => {
      for (const field of stage?.queryFields ?? []) {
         if (field.type === "fieldref") visitRef(field.path);
         else if (field.type === "turtle") visitStage(field.pipeline?.[0]);
         else visitExpr(field.e);
      }
      for (const filter of stage?.filterList ?? []) visitExpr(filter.e);
   };

   visitStage(ir?.pipeline?.[0]);
   return violation ? `${member}: ${violation}` : undefined;
}
