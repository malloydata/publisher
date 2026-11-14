// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The variant router: which compiled form of a caller's query answers it.
 *
 * A query has one live form and any number of candidate VARIANTS, each a
 * compiled form over stored data (today: the `storage=` serve shape, then the
 * pre-aggregation companion). The router tries the candidates in catalog
 * order, takes the first that compiles and covers the query, and otherwise
 * answers live. It records every candidate it considered and why each was
 * skipped, which is what the path report reads (materialization v2, D1).
 *
 * It never rewrites the caller's text and never bends a variant to fit: a
 * candidate either answers the query as written or is skipped (R2). It takes
 * its candidates as input, so the same loop answers what-if over a proposed
 * catalog (T3) and gains legs in later phases without changing.
 *
 * Route, then govern: the router runs before `QueryGovernor.govern()`, which
 * grafts and proves the gates on whatever it chose. Until a variant can carry
 * the gates (phase 3), an entry point with a row-level gate skips every
 * candidate, with reason `row_level_gate`.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import type { VariantSkipExplanation } from "./materialization/domain/leg_decline";
import { recordQueryVariantRouting } from "../materialization_metrics";

/** The kinds of candidate the router knows, in their usual catalog order. */
/**
 * `served` is the served copy on `virtual` (materialization v2), tried before
 * `storage` (today's serve shape) when `materialize.enabled` is on. `view` is
 * a persisted view's table (`#@ persist` on a view, phase 4), tried first: it
 * answers only an exact call of its view (R1).
 */
export type VariantKind = "view" | "served" | "storage" | "rollup";

/**
 * Why a candidate did not answer. Internal for now; the path report maps
 * these onto its public `SkipReason` values.
 */
type RouterSkipReason =
   /** The entry point carries a row-level gate the variant cannot keep. */
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
    * on it exactly as on live. An `entry_point` veto skips every other
    * candidate (`row_level_gate`), whose shape carries no annotations; this
    * one is tried. Any other veto skips it too.
    */
   keepsContracts?: boolean;
   /**
    * With {@link keepsContracts}, narrows it per answer: called with an
    * answer the attempt produced past an `entry_point` veto, and `false`
    * skips that answer (`row_level_gate`) as if the candidate did not keep
    * the contracts. For a candidate that keeps them only for some of the
    * tables it may read (the served copy's colocated tables under the v1
    * cutover). Absent: every answer keeps them.
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
    * Count each candidate in `publisher_query_variant_routing_total` now. Off
    * when the caller counts after the run instead ({@link recordVariantRouting}),
    * so a variant whose run fails and falls back live is not counted `used`.
    */
   { recordMetrics = true }: { recordMetrics?: boolean } = {},
): Promise<RouteDecision<Extra>> {
   const decision = await decideRoute(variants, blockedByGate);
   if (recordMetrics) recordVariantRouting(decision.considered);
   return decision;
}

/**
 * One count per candidate the router considered. With `runFailed`, the
 * variant it chose failed at run time and the query was answered live, so
 * that variant counts as `skipped` with reason `run_failed`, not `used`.
 */
export function recordVariantRouting(
   considered: readonly ConsideredVariant[],
   runFailed = false,
): void {
   for (const c of considered) {
      if (runFailed && c.outcome === "used") {
         recordQueryVariantRouting(c.kind, "skipped", "run_failed");
      } else {
         recordQueryVariantRouting(c.kind, c.outcome, c.reason ?? "none");
      }
   }
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
): Partial<Pick<ConsideredVariant, "code" | "callerDetail">> {
   try {
      const explained = variant.explain?.(error);
      if (!explained) return {};
      return {
         code: explained.code,
         ...(explained.callerDetail
            ? { callerDetail: explained.callerDetail }
            : {}),
      };
   } catch {
      return {};
   }
}
