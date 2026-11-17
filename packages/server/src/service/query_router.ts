// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The variant router: which compiled form of a caller's query answers it.
 *
 * A query has one live form and any number of candidate VARIANTS, each a
 * compiled form over stored data: one per serving leg (`serve/leg.ts`), made
 * into candidates by `variantsOf` (`query_serving.ts`). The router tries the
 * candidates in the order given, takes the first that compiles and covers the
 * query, and otherwise answers live. It records every candidate it considered
 * and why each was skipped, which is what the path report reads (D1).
 *
 * It never rewrites the caller's text and never bends a variant to fit: a
 * candidate either answers the query as written or is skipped (R2). It takes
 * its candidates as input, so the same loop answers what-if over a proposed
 * catalog (T3) and gains legs without changing.
 *
 * Route, then govern: the router runs before `QueryGovernor.govern()`, which
 * grafts and proves the gates on whatever it chose. A row-level gate vetoes
 * candidates first ({@link RowLevelVeto}): a candidate is skipped with
 * reason `row_level_gate` for a row-level gate the variant cannot keep: one
 * beyond the entry point, one the walk could not decide, or the entry
 * point's own on a variant that does not keep the contracts
 * ({@link Variant.keepsContracts}). Which legs keep
 * the contracts is the gate policy's call
 * (`materialization/domain/gate_policy.ts`), made where the candidates are
 * built.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import {
   declineOfCode,
   type VariantSkipExplanation,
} from "./materialization/domain/leg_decline";
import {
   recordQueryRoute,
   recordQueryRouteDuration,
   type RouteReason,
} from "../materialization_metrics";

/**
 * The kinds of candidate, one per serving leg (`LegKind` in `serve/leg.ts`,
 * whose `makeLegs` sets their order): `view`, a persisted view's table
 * (`#@ persist` on a view), which answers only an exact call of its view
 * (R1); `query`, a persisted named query's table, also by exact call;
 * `rollup`, the coarsest `#(rollup)` rollup that reproduces the query;
 * and `served`, the served copy on `virtual`.
 */
export type VariantKind = "view" | "query" | "served" | "rollup";

/**
 * Why a candidate did not answer, in the router's own terms. It is kept on
 * {@link ConsideredVariant} and never sent as is: the path report
 * (`serve/query_path.ts`) maps it onto its public `SkipReason`, and the route
 * metric onto its `RouteReason` ({@link routeReasonOf}).
 */
type RouterSkipReason =
   /**
    * A row-level gate the variant cannot keep: one beyond the entry point,
    * one the walk could not decide, or the entry point's own on a variant
    * that does not keep the contracts.
    */
   | "row_level_gate"
   /** The candidate does not apply to this model or request at all. */
   | "not_applicable"
   /** The candidate failed to compile the query, or does not cover it. */
   | "does_not_answer";

/** One candidate's attempt at the query: its runnable, and what it needs to run. */
interface VariantAnswer<Extra> {
   runnable: QueryMaterializer;
   extra: Extra;
}

/** A candidate variant, as the model that owns it describes it. */
export interface Variant<Extra = unknown> {
   kind: VariantKind;
   /**
    * Whether the candidate can apply to this request before anything is
    * compiled. `false` skips it as `not_applicable`, with no compile.
    */
   applicable: boolean;
   /**
    * Compile the query against this candidate. Resolves when it answers the
    * query as written; throws when it does not (a compile error, a coverage
    * refusal), which skips it.
    */
   attempt(): Promise<VariantAnswer<Extra>>;
   /** Told why the attempt failed, for the candidate's own logs and metrics. */
   onSkip?(error: unknown): void;
   /**
    * Classify a failed attempt for the path report: a code, and a detail safe
    * to show the caller (in the caller's own terms, never an internal message).
    * Undefined leaves the skip unexplained beyond `does_not_answer`.
    */
   explain?(error: unknown): VariantSkipExplanation | undefined;
   /**
    * The candidate keeps the domain's contracts (R3): it compiles the author's
    * own text, gates included, so `govern()` grafts and proves a row-level gate
    * on it exactly as on live. An `entry_point` veto skips every candidate
    * without it (`row_level_gate`); this one is tried. Any other veto skips
    * it too.
    */
   keepsContracts?: boolean;
   /**
    * With {@link keepsContracts}, narrows it per answer: called with an
    * answer the attempt produced past an `entry_point` veto, and `false`
    * skips that answer (`row_level_gate`) as if the candidate did not keep
    * the contracts. For a candidate that keeps them only for some of the
    * tables it may read (the served copy, judged by the tables its answer
    * read). Absent: every answer keeps them.
    */
   keepsContractsFor?(extra: Extra): boolean;
}

export type { VariantSkipExplanation };

/** What the router decided about one candidate. */
export interface ConsideredVariant {
   kind: VariantKind;
   outcome: "used" | "skipped";
   reason?: RouterSkipReason;
   /**
    * The compile or coverage error's message, for a `does_not_answer` skip.
    * Internal: for logs only, never the wire (it can name storage paths and
    * synthesized sources).
    */
   detail?: string;
   /** The candidate's classification of a `does_not_answer` skip, if it gave one. */
   code?: string;
   /** With {@link code}: a detail safe to show the caller. */
   callerDetail?: string;
   /**
    * The rollup leg's own decline for each rollup it tried, by synthesized
    * source name ({@link VariantSkipExplanation.members}).
    */
   members?: VariantSkipExplanation["members"];
}

/**
 * The row-level veto for one query: `true` skips every candidate; `entry_point`
 * (only the run target's own chain is gated) skips every candidate but one
 * that {@link Variant.keepsContracts}; `false` skips none.
 */
type RowLevelVeto = boolean | "entry_point";

/** The router's decision for one query. */
export type RouteDecision<Extra> =
   | {
        served: "variant";
        kind: VariantKind;
        runnable: QueryMaterializer;
        extra: Extra;
        considered: ConsideredVariant[];
        /**
         * Set when this candidate answered past an `entry_point` veto (it
         * {@link Variant.keepsContracts}): the query's entry point is gated, so
         * governing the answer must enforce exactly the live entry point's
         * gates on it.
         */
        pastEntryPointGate?: true;
     }
   | { served: "live"; considered: ConsideredVariant[] };

/**
 * Route one query over `variants`, in order. `blockedByGate` is asked at most
 * once, and only when some candidate applies to this request (it can cost an
 * entry-point walk and a probe compile); the caller lists a candidate only when
 * its variant exists for this model, and marks it inapplicable for this request.
 */
export async function routeQuery<Extra>(
   variants: ReadonlyArray<Variant<Extra>>,
   blockedByGate: () => Promise<RowLevelVeto>,
   /**
    * Count each candidate in `publisher_query_route_total` now. Off
    * when the caller counts after the run instead ({@link recordVariantRouting}),
    * so a variant whose run fails and falls back live is not counted `used`.
    */
   { recordMetrics = true }: { recordMetrics?: boolean } = {},
): Promise<RouteDecision<Extra>> {
   const startedAt = performance.now();
   const decision = await decideRoute(variants, blockedByGate);
   // Timed only when a leg applied: otherwise there was nothing to choose.
   if (variants.some((v) => v.applicable)) {
      recordQueryRouteDuration(
         performance.now() - startedAt,
         decision.served === "variant" ? decision.kind : "live",
      );
   }
   if (recordMetrics) recordVariantRouting(decision.considered);
   return decision;
}

/**
 * One count per candidate the router considered, once the query is answered
 * (`publisher_query_route_total`). With `runFailed`, the variant it chose
 * failed at run time and the query was answered live, so that variant counts
 * as `fallback` with reason `RUN_FAILED`, not `used`. A candidate that did
 * not apply is not counted. When live answered a query the router counted a
 * candidate for, `live` counts as `used`.
 */
export function recordVariantRouting(
   considered: readonly ConsideredVariant[],
   runFailed = false,
): void {
   for (const c of considered) {
      if (c.outcome === "used") {
         if (runFailed) recordQueryRoute(c.kind, "fallback", "RUN_FAILED");
         else recordQueryRoute(c.kind, "used", "NONE");
      } else if (c.reason !== "not_applicable") {
         recordQueryRoute(c.kind, "skipped", routeReasonOf(c));
      }
   }
   const counted = considered.some((c) => c.reason !== "not_applicable");
   if (
      counted &&
      (runFailed || !considered.some((c) => c.outcome === "used"))
   ) {
      recordQueryRoute("live", "used", "NONE");
   }
}

/**
 * The public reason for a skipped candidate: a gate it cannot keep is
 * `CONTRACT`; a decline is its code's `SkipReason`, and anything unexplained
 * is `MISSING_FIELD`, as the path report reads it.
 */
function routeReasonOf(c: ConsideredVariant): RouteReason {
   if (c.reason === "row_level_gate") return "CONTRACT";
   return declineOfCode(c.code)?.reason ?? "MISSING_FIELD";
}

async function decideRoute<Extra>(
   variants: ReadonlyArray<Variant<Extra>>,
   blockedByGate: () => Promise<RowLevelVeto>,
): Promise<RouteDecision<Extra>> {
   const considered: ConsideredVariant[] = [];
   const veto: RowLevelVeto =
      variants.some((v) => v.applicable) && (await blockedByGate());
   for (const variant of variants) {
      if (!variant.applicable) {
         considered.push({
            kind: variant.kind,
            outcome: "skipped",
            reason: "not_applicable",
         });
         continue;
      }
      if (
         veto === true ||
         (veto === "entry_point" && !variant.keepsContracts)
      ) {
         considered.push({
            kind: variant.kind,
            outcome: "skipped",
            reason: "row_level_gate",
         });
         continue;
      }
      try {
         const answer = await variant.attempt();
         if (
            veto === "entry_point" &&
            variant.keepsContractsFor &&
            !variant.keepsContractsFor(answer.extra)
         ) {
            considered.push({
               kind: variant.kind,
               outcome: "skipped",
               reason: "row_level_gate",
            });
            continue;
         }
         considered.push({ kind: variant.kind, outcome: "used" });
         return {
            served: "variant",
            kind: variant.kind,
            runnable: answer.runnable,
            extra: answer.extra,
            considered,
            ...(veto === "entry_point" ? { pastEntryPointGate: true } : {}),
         };
      } catch (error) {
         considered.push({
            kind: variant.kind,
            outcome: "skipped",
            reason: "does_not_answer",
            detail: error instanceof Error ? error.message : String(error),
            ...explanationOf(variant, error),
         });
         variant.onSkip?.(error);
      }
   }
   return { served: "live", considered };
}

/** A variant's {@link Variant.explain}, never throwing into the router. */
function explanationOf(
   variant: Variant<unknown>,
   error: unknown,
): Partial<Pick<ConsideredVariant, "code" | "callerDetail" | "members">> {
   try {
      const explained = variant.explain?.(error);
      if (!explained) return {};
      return {
         code: explained.code,
         ...(explained.callerDetail
            ? { callerDetail: explained.callerDetail }
            : {}),
         ...(explained.members ? { members: explained.members } : {}),
      };
   } catch {
      return {};
   }
}
