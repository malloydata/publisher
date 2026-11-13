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
 * A `select:` or `index:` stage is refused outright, wherever it is: both read
 * rows, and a rollup has one per grain group where the source has one per row.
 *
 * Deliberately strict: `count(grain_dim)`, `min` and `max` over a grain
 * dimension are correct from a rollup but are refused too, so the rule an author
 * reads is one line. Refusing costs acceleration, never an answer.
 *
 * Fails open: a query whose IR names no member, or names one that is not a
 * rollup, is not checked. That is safe only while the IR keeps the shape read
 * here, which is why preaggregation_coverage.spec.ts compiles real queries and
 * asserts the member each one resolved to. Keep it on real IR across a
 * `@malloydata/malloy` bump rather than moving it to fixtures.
 *
 * Pure: reads the compiled IR, touches no I/O.
 */

import type { RollupPlan } from "./preaggregation_synthesis";

/**
 * A rollup refused for a query it cannot answer: an expected, per-query outcome
 * rather than a misconfiguration, so callers log it quietly.
 */
export class RollupCoverageError extends Error {
   /** Why, as a code the path report maps to its skip reason. */
   readonly reason: RollupCoverageReason;
   /**
    * The violation in the caller's own terms (its field paths and aggregates),
    * without the rollup's synthesized name: safe to show the caller.
    */
   readonly detail: string;
   constructor(violation: RollupCoverageViolation) {
      super(`${violation.member}: ${violation.detail}`);
      this.name = "RollupCoverageError";
      this.reason = violation.reason;
      this.detail = violation.detail;
   }
}

/**
 * Why a rollup cannot answer a query: `missing_field`, the query reaches a
 * field the rollup does not carry (a path through a join); `count_semantics`,
 * an aggregate or a row-level stage the rollup's grain rows cannot reproduce.
 */
export type RollupCoverageReason = "missing_field" | "count_semantics";

/** One reason a resolved rollup cannot answer a query. */
export interface RollupCoverageViolation {
   /** The rollup member the query resolved to (a synthesized name). */
   member: string;
   reason: RollupCoverageReason;
   /** In the caller's own terms; see {@link RollupCoverageError.detail}. */
   detail: string;
}

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
   type?: string;
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
): RollupCoverageViolation | undefined {
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
   let violation: { reason: RollupCoverageReason; detail: string } | undefined;

   /**
    * A reference into the member. A merged measure is the one sanctioned
    * aggregate; anything else defined on the member (a measure an inline
    * `extend` added, a dimension) is followed into its definition. A path
    * through a join is refused rather than followed: a rollup declares no
    * joins, so one reached here was added by the query.
    */
   const visitRef = (path: readonly string[] | undefined) => {
      if (!path) return;
      if (path.length !== 1) {
         violation = {
            reason: "missing_field",
            detail: `reference ${path.join(".")} reaches past the rollup's own fields`,
         };
         return;
      }
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
         violation = {
            reason: "count_semantics",
            detail: `aggregate ${String(n.function)}() is computed over the rollup's rows, not from a stored partial`,
         };
         return;
      }
      if (
         n.node === "function_call" &&
         n.overload?.returnType?.expressionType === "aggregate"
      ) {
         violation = {
            reason: "count_semantics",
            detail: `aggregate function ${String(n.name)}() is computed over the rollup's rows, not from a stored partial`,
         };
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
      if (violation) return;
      if (stage?.type === "project" || stage?.type === "index") {
         violation = {
            reason: "count_semantics",
            detail: `${stage.type === "project" ? "a select:" : "an index:"} stage over the rollup returns one row per grain group, not the source's rows`,
         };
         return;
      }
      for (const field of stage?.queryFields ?? []) {
         if (field.type === "fieldref") visitRef(field.path);
         else if (field.type === "turtle") visitStage(field.pipeline?.[0]);
         else visitExpr(field.e);
      }
      for (const filter of stage?.filterList ?? []) visitExpr(filter.e);
   };

   visitStage(ir?.pipeline?.[0]);
   return violation ? { member, ...violation } : undefined;
}
