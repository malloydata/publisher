// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A serving leg, and the answer one query gets from the legs.
 *
 * A leg answers a routed query from stored tables or declines: the view leg
 * (a persisted view called exactly), the query leg (a persisted named query
 * called exactly), the rollup leg, and the served copy (the author's source
 * re-rooted on its stored table, gates and all). Each is
 * `{ kind, applies, attempt }` and nothing more: `applies` is pure and
 * compiles nothing; `attempt` resolves with a {@link LegAnswer} or throws a
 * `LegDecline` (domain/leg_decline.ts). A leg does not know the gate policy or
 * the router: the query pipeline (`service/model_serving.ts`) asks the policy
 * before an attempt, by leg kind, and after it, over the bindings the answer
 * read, and it reads the decline's code itself.
 *
 * {@link makeLegs} makes the legs for one pinned snapshot, in the router's
 * order (R1: the view or named query by exact call, then the coarsest
 * rollup, then the served copy). A leg is listed only when it exists for the model and the snapshot, so the
 * gate check runs exactly when one could route.
 *
 * {@link Answer} is the one record of how a query was served: built once from
 * the router's decision, replaced (never mutated) when a stored run fails and
 * falls back live, and finished with the colocated entries the answering run
 * read. `servedFrom`, the run's map and manifest, whether a failed run may
 * retry live, the access record's path and the path report all read it, so no
 * two of them can disagree.
 *
 * Pure. Adding a leg is one module implementing {@link Leg}, an entry in
 * {@link makeLegs}, and its rows in `domain/leg_decline.ts`.
 */

import type {
   GivenValue,
   QueryMaterializer,
   VirtualMap,
} from "@malloydata/malloy";
import type { GraftScope } from "../../gate_classification";
import type { ConsideredVariant } from "../../query_router";
import { type Binding, isColocated, type Pinned } from "../domain/binding";
import type { BuildManifest } from "../domain/types";
import type { RollupVariantServe } from "./rollup_variants";
import type { ServedCopyServe } from "./served_copy_serve";
import type { PersistedViewServe, ViewTable } from "./view_variants";
import type { PersistedQueryServe } from "./query_variants";

/** The query leg's serve, for the model that makes one (beside the legs it makes). */
export { PersistedQueryServe } from "./query_variants";

/** The legs, in their usual order. */
export type LegKind = "view" | "query" | "rollup" | "served";

/** What a leg reads of one query. */
export interface QueryFacts {
   /** The caller's text as the router sees it (filters injected). */
   readonly queryString: string;
   /** The givens the query reads on the model's surface. */
   readonly givens: Record<string, GivenValue> | undefined;
   /** The caller's own compile against the author's model. */
   readonly authorRunnable: QueryMaterializer;
}

/** A leg that may answer a routed query from stored tables. */
export interface Leg {
   readonly kind: LegKind;
   /** Whether the leg can help this query at all, before anything compiles. */
   applies(q: QueryFacts, p: Pinned): boolean;
   /** Answer the query as written, or throw a `LegDecline`. */
   attempt(q: QueryFacts, p: Pinned): Promise<LegAnswer>;
}

/** What a leg answered with: its runnable, and what running it reads. */
export interface LegAnswer {
   readonly runnable: QueryMaterializer;
   /**
    * The stored tables the answer reads, as the path report names them:
    * the served copy's tables, or a `storage=` view's binding. Undefined
    * when the leg reports none of its own (a rollup, a colocated view).
    */
   readonly tablesRead: readonly Binding[] | undefined;
   /**
    * The bindings the runnable's shape was built from: a store failure is
    * retried live only when every one allows it. Empty when the answer reads
    * nothing through a virtual map.
    */
   readonly shapeBindings: readonly Binding[];
   /** How the runnable's `virtual()` handles resolve, when it reads storage. */
   readonly virtualMap: VirtualMap | undefined;
   /** Where `govern()` grafts a row-level gate on this answer (R3). */
   readonly graftScope: GraftScope | undefined;
   /** For a rollup answer: the rollup that answered. */
   readonly rollupMember?: string;
   /** For a view or query answer: the persisted view's or named query's table. */
   readonly viewTable?: ViewTable & { readonly kind?: "query" };
}

/** How one query was served. */
export type Answer =
   | {
        readonly leg: "live";
        /** Each candidate the router considered, and what it decided. */
        readonly considered: readonly ConsideredVariant[];
        /** The colocated manifest entries the answering run read. */
        readonly colocatedRead: ReadonlySet<string>;
     }
   | {
        readonly leg: LegKind;
        readonly considered: readonly ConsideredVariant[];
        readonly answer: LegAnswer;
        /**
         * The answering leg was let past an entry-point gate because it keeps
         * the contracts (R3): `govern()` must find and enforce that gate on it.
         */
        readonly pastEntryPointGate: boolean;
        /** The stored run failed and the query was answered live. */
        readonly runFailed: boolean;
        readonly colocatedRead: ReadonlySet<string>;
     };

/** A routed answer. */
export type RoutedAnswer = Exclude<Answer, { leg: "live" }>;

/** The answer before routing, and when nothing could route. */
export const LIVE_ANSWER: Answer = Object.freeze({
   leg: "live",
   considered: [],
   colocatedRead: new Set<string>(),
});

/** The legs that exist for a model, each present only when it is on. */
export interface LegContext {
   /** The served copy, when it is on for the model. */
   readonly servedCopy?: ServedCopyServe;
   /** The rollup leg, when the model has a rollup and the leg is on. */
   readonly rollupVariants?: RollupVariantServe;
   /** The view leg, when the model has a persisted view and the leg is on. */
   readonly persistedViews?: PersistedViewServe;
   /** The query leg, when the model has a persisted named query and the leg is on. */
   readonly persistedQueries?: PersistedQueryServe;
}

/**
 * The legs for one pinned snapshot, in the router's order. The served copy is
 * listed only when a table it could re-root on is bound in the snapshot.
 */
export function makeLegs(p: Pinned, ctx: LegContext): readonly Leg[] {
   const legs: Leg[] = [];
   const { persistedViews, persistedQueries, rollupVariants, servedCopy } = ctx;
   if (persistedViews) legs.push(viewLeg(persistedViews));
   if (persistedQueries) legs.push(queryLeg(persistedQueries));
   if (rollupVariants) legs.push(rollupLeg(rollupVariants));
   if (servedCopy?.hasBindings(p.snapshot)) legs.push(servedLeg(servedCopy));
   return legs;
}

/** The pinned build manifest, or undefined when nothing colocated is bound. */
function pinnedManifest(p: Pinned): BuildManifest | undefined {
   return p.hasEntries ? p.compilerManifest : undefined;
}

/**
 * A persisted view first (R1): applicable only when the text calls one, so a
 * look-alike or a nest never costs a compile.
 */
function viewLeg(views: PersistedViewServe): Leg {
   return {
      kind: "view",
      applies: (q) => !!views.calledView(q.queryString),
      attempt: async (q, p) => {
         const answer = await views.attempt({
            queryString: q.queryString,
            givens: q.givens,
            authorRunnable: q.authorRunnable,
            pinned: p,
            buildManifest: pinnedManifest(p),
         });
         return {
            runnable: answer.runnable,
            tablesRead: answer.binding ? [answer.binding] : undefined,
            shapeBindings: answer.binding ? [answer.binding] : [],
            virtualMap: answer.virtualMap,
            graftScope: answer.graftScope,
            viewTable: answer.table,
         };
      },
   };
}

/**
 * A persisted named query by exact call (`run: q`, `run: q -> { … }`), after
 * the view leg. The gate policy has no row for it, so it is never tried past
 * an entry-point gate: a persisted query is never built over a gated source.
 */
function queryLeg(queries: PersistedQueryServe): Leg {
   return {
      kind: "query",
      applies: (q) => !!queries.calledQuery(q.queryString),
      attempt: async (q, p) => {
         const answer = await queries.attempt({
            queryString: q.queryString,
            givens: q.givens,
            authorRunnable: q.authorRunnable,
            pinned: p,
            buildManifest: pinnedManifest(p),
         });
         return {
            runnable: answer.runnable,
            tablesRead: answer.binding ? [answer.binding] : undefined,
            shapeBindings: answer.binding ? [answer.binding] : [],
            virtualMap: answer.virtualMap,
            graftScope: answer.graftScope,
            viewTable: { ...answer.table, kind: "query" },
         };
      },
   };
}

/**
 * Then `#(preaggregate)` rollups: the caller's text compiled where its
 * source's name means the coarsest rollup that reproduces it. A stored rollup
 * reads through `virtual()`, and a run failure on it may degrade to live,
 * since a rollup only accelerates (its binding never vetoes).
 */
function rollupLeg(rollups: RollupVariantServe): Leg {
   return {
      kind: "rollup",
      applies: () => true,
      attempt: async (q, p) => {
         const answer = await rollups.attempt({
            queryString: q.queryString,
            givens: q.givens,
            authorRunnable: q.authorRunnable,
            pinned: p,
         });
         return {
            runnable: answer.runnable,
            tablesRead: undefined,
            // Every stored table the answer reads, when that is more than the
            // rollup's (a join leg answered in a storage destination reads the
            // query source's table there too): together they decide whether a
            // failed run may be retried live.
            shapeBindings: answer.virtualMap
               ? (answer.bindings ?? (answer.binding ? [answer.binding] : []))
               : [],
            virtualMap: answer.virtualMap,
            graftScope: answer.graftScope,
            rollupMember: answer.rollupSourceName,
         };
      },
   };
}

/**
 * The served copy on `virtual`: the author's own files with each stored
 * source re-rooted on its table.
 */
function servedLeg(servedCopy: ServedCopyServe): Leg {
   return {
      kind: "served",
      applies: () => true,
      attempt: async (q, p) => {
         let served: Awaited<ReturnType<ServedCopyServe["loadQuery"]>>;
         try {
            served = await servedCopy.loadQuery(q.queryString, q.givens, p);
         } catch (err) {
            // A table the query would read, left out of the pin as stale.
            throw await servedCopy.staleDeclineFor(err, q.authorRunnable, p);
         }
         return {
            runnable: served.runnable,
            tablesRead: served.tablesRead,
            shapeBindings: served.bindings,
            virtualMap: served.virtualMap,
            graftScope: served.graftScope,
         };
      },
   };
}

/** The same answer after its stored run failed and the live retry ran. */
export function liveFallbackAnswer(answer: RoutedAnswer): Answer {
   return Object.freeze({ ...answer, runFailed: true });
}

/** The answer with the manifest entries its answering run read. */
export function answeredWith(
   answer: Answer,
   colocatedRead: ReadonlySet<string>,
): Answer {
   return Object.freeze({ ...answer, colocatedRead });
}

/**
 * How a query's answer was ultimately produced, once it reached a stored
 * table at all. `storage` = served from a stored table through `virtual()`.
 * `live_fallback` = it routed, then a run-time store failure degraded it to
 * the live warehouse (a success the caller cannot distinguish from a storage
 * hit, which is exactly why it is reported separately; on the error metric,
 * the live retry failed too). Absent means no stored table answered.
 */
export type ServedFrom = "storage" | "live_fallback";

/**
 * `servedFrom` for the answer: a storage hit, a storage run that fell back
 * live (`live_fallback`, a success that must NOT count as a storage hit), or
 * undefined for everything else.
 */
export function servedFromOf(answer: Answer): ServedFrom | undefined {
   if (answer.leg === "live") return undefined;
   if (answer.runFailed) return "live_fallback";
   const { tablesRead, virtualMap } = answer.answer;
   // A served-copy answer that read only colocated `#(materialize)` tables
   // read the source's own warehouse, as a colocated `#@ persist` answer
   // does, which reports no storage hit; its path report names the table.
   if (
      answer.leg === "served" &&
      tablesRead !== undefined &&
      tablesRead.length > 0 &&
      tablesRead.every(isColocated)
   ) {
      return undefined;
   }
   // A `storage=` persisted view or `#(preaggregate)` rollup reads the
   // storage tier too, through `virtual()`.
   return answer.leg === "served" || virtualMap !== undefined
      ? "storage"
      : undefined;
}

/**
 * Whether a run-time store failure may be retried against the live warehouse,
 * decided from the bindings that produced the answer's shape.
 *
 * An authored binding declaring anything but `live` vetoes for the whole shape
 * (deliberately coarse). A rollup binding never vetoes and never enables on its
 * own: pre-aggregation has no correctness dimension (docs/preaggregation.md: a
 * stale rollup recomputes from the base with the same answer), so a rollup must
 * not error on a store failure. A rollup must also not be filtered out, or a
 * rollup-only package could never degrade. Empty is false: nothing routed.
 */
function bindingsAllowDegradeToLive(bindings: readonly Binding[]): boolean {
   return (
      bindings.length > 0 &&
      bindings.every(
         (b) => b.kind === "rollup" || b.freshnessFallback === "live",
      )
   );
}

/**
 * Whether a run failure under this answer may be retried live:
 * `freshnessFallback=live` marks the tier as an optimisation, not a
 * dependency. Deliberately narrow: only an answer read through a virtual map,
 * only per {@link bindingsAllowDegradeToLive} over the bindings that produced
 * THIS shape (a sibling source must not decide), never for a client error (a
 * bad given) or an abort.
 */
export function canDegradeToLive(
   answer: Answer,
   error: unknown,
   opts: { hasLiveRunnable: boolean; aborted: boolean },
): answer is RoutedAnswer {
   return (
      answer.leg !== "live" &&
      !!answer.answer.virtualMap &&
      opts.hasLiveRunnable &&
      !opts.aborted &&
      !String((error as { code?: string })?.code ?? "").startsWith(
         "runtime-given-",
      ) &&
      bindingsAllowDegradeToLive(answer.answer.shapeBindings)
   );
}

/**
 * The build manifest the answering run takes. A run on a virtual map (the
 * served copy, a `storage=` view or rollup) takes none; a colocated rollup
 * reads the full pinned manifest (its entries), a colocated persisted view
 * the one without rollups, and the live compile an empty one.
 */
export function runManifestOf<M>(
   answer: Answer,
   manifests: { pinned: M | undefined; withoutRollups: M | undefined; live: M },
): M | undefined {
   if (answer.leg === "live") return manifests.live;
   if (answer.runFailed) return manifests.live;
   if (answer.answer.virtualMap) return undefined;
   if (answer.leg === "rollup") return manifests.pinned;
   if (answer.leg === "view" || answer.leg === "query") {
      return manifests.withoutRollups;
   }
   return manifests.live;
}
