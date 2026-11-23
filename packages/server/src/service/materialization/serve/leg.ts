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
 * the router: the query pipeline (`variantsOf` in `service/query_serving.ts`)
 * asks the policy before an attempt, by leg kind, and after it, over the
 * bindings the answer read, and it reads the decline's code itself.
 *
 * {@link makeLegs} makes the legs for one pinned snapshot, in the router's
 * order (the view or named query by exact call, then the coarsest
 * rollup, then the served copy). A leg is listed only when it exists for
 * the model and the snapshot, so the gate check runs exactly when one could
 * route.
 *
 * {@link Answer} is the one record of how a query was served: built once from
 * the router's decision, replaced (never mutated) when a stored run fails and
 * falls back live, and finished with the database entries the answering run
 * read. `servedFrom`, the run's map and manifest, whether a failed run may
 * retry live, the access record's path and the path report all read it, so no
 * two of them can disagree.
 *
 * Pure. Adding a leg is one module implementing {@link Leg}, an entry in
 * {@link makeLegs}, and its rows in `domain/leg_decline.ts`.
 */

import { isDuckLakeCatalogUnavailable } from "../../ducklake_catalog_unavailable";
import type { Leg as LegName } from "../domain/vocabulary";
import type {
   GivenValue,
   QueryMaterializer,
   VirtualMap,
} from "@malloydata/malloy";
import type { GraftScope } from "../../gate_classification";
import type { ConsideredVariant } from "../../query_router";
import {
   type Binding,
   isDatabase,
   type KeyedTable,
   type Pinned,
   underCompilerKeys,
} from "../domain/binding";
import type { BuildManifest } from "../domain/types";
import type { TableRead } from "../domain/gate_policy";
import { assertSameCompositeMember } from "./composite_member";
import type { RollupVariantServe } from "./rollup_variants";
import type { ServedCopyServe } from "./served_copy_serve";
import type { PersistedViewServe, ViewTable } from "./view_variants";
import type { PersistedQueryServe } from "./query_variants";

/** The query leg's serve, for the model that makes one (beside the legs it makes). */
export { PersistedQueryServe } from "./query_variants";

/** The legs, in their usual order. */
export type LegKind = LegName;

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
    * when the leg reports none of its own (a rollup, a database view).
    */
   readonly tablesRead: readonly Binding[] | undefined;
   /**
    * The tables the answer is judged to read when the router decides whether
    * it keeps an entry point's gate, which fails closed: for the served copy,
    * every re-rooted `storage=` table not proven unread; for a rollup, its
    * own. Undefined means {@link tablesRead}.
    */
   readonly gateTablesRead?: readonly TableRead[];
   /**
    * For the served copy: the author sources its compile re-rooted on their
    * tables, by `sourceID`, read or not. A join to one reads its table, never
    * its query, so the row-scope check leaves that query out.
    */
   readonly rerootedSourceIDs?: ReadonlySet<string>;
   /**
    * The bindings the runnable's shape was built from: a store failure is
    * retried live only when every one allows it. Empty when the answer reads
    * nothing through a virtual map.
    */
   readonly shapeBindings: readonly Binding[];
   /** How the runnable's `virtual()` handles resolve, when it reads storage. */
   readonly virtualMap: VirtualMap | undefined;
   /** Where `govern()` grafts a row-level gate on this answer. */
   readonly graftScope: GraftScope | undefined;
   /** For a rollup answer: the rollup that answered. */
   readonly rollupMember?: string;
   /** For a rollup answer: the `sourceID` of the source that declares it. */
   readonly rollupBaseSourceID?: string;
   /**
    * A database table the answer reads that Malloy files under a key not its
    * address (sources of one build placed in two tables): the run's manifest
    * carries its entry under that key, as the leg's compile did
    * (`manifestUnderCompilerKey`).
    */
   readonly keyedTable?: KeyedTable;
   /** For a view or query answer: the persisted view's or named query's table. */
   readonly viewTable?: ViewTable & { readonly kind?: "query" };
   /**
    * For a database view or query answer: the table its later stages read,
    * for the check that their filters compare as live's do.
    */
   readonly resultTableRead?: TableRead & {
      readonly tablePath?: string;
      readonly destinationName?: string;
   };
}

/** How one query was served. */
export type Answer =
   | {
        readonly leg: "live";
        /** Each candidate the router considered, and what it decided. */
        readonly considered: readonly ConsideredVariant[];
        /** The database manifest entries the answering run read. */
        readonly databaseRead: ReadonlySet<string>;
     }
   | {
        readonly leg: LegKind;
        readonly considered: readonly ConsideredVariant[];
        readonly answer: LegAnswer;
        /**
         * The answering leg was let past an entry-point gate because it keeps
         * the contracts: `govern()` must find and enforce that gate on it.
         */
        readonly pastEntryPointGate: boolean;
        /** The stored run failed and the query was answered live. */
        readonly runFailed: boolean;
        readonly databaseRead: ReadonlySet<string>;
     };

/** A routed answer. */
export type RoutedAnswer = Exclude<Answer, { leg: "live" }>;

/** The answer before routing, and when nothing could route. */
export const LIVE_ANSWER: Answer = Object.freeze({
   leg: "live",
   considered: [],
   databaseRead: new Set<string>(),
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
   if (persistedViews) {
      legs.push(
         persistedResultLeg("view", persistedViews, (text) =>
            persistedViews.calledView(text),
         ),
      );
   }
   if (persistedQueries) {
      legs.push(
         persistedResultLeg("query", persistedQueries, (text) =>
            persistedQueries.calledQuery(text),
         ),
      );
   }
   if (rollupVariants) legs.push(rollupLeg(rollupVariants));
   if (servedCopy?.hasBindings(p.snapshot)) legs.push(servedLeg(servedCopy));
   return legs;
}

/** The pinned build manifest, or undefined when nothing database is bound. */
function pinnedManifest(p: Pinned): BuildManifest | undefined {
   return p.hasEntries ? p.compilerManifest : undefined;
}

/**
 * A persisted view, then a persisted named query by exact call (`run:
 * q`, `run: q -> { … }`): each applicable only when the text calls one, so a
 * look-alike or a nest never costs a compile. The gate policy has no row for
 * the query leg, so it is never tried past an entry-point gate: a persisted
 * query is never built over a gated source.
 */
function persistedResultLeg(
   kind: "view" | "query",
   serve: Pick<PersistedViewServe | PersistedQueryServe, "attempt">,
   called: (text: string) => unknown,
): Leg {
   return {
      kind,
      applies: (q) => !!called(q.queryString),
      attempt: async (q, p) => {
         const answer = await serve.attempt({
            queryString: q.queryString,
            givens: q.givens,
            authorRunnable: q.authorRunnable,
            pinned: p,
            // As Malloy reads it (underCompilerKeys).
            buildManifest: underCompilerKeys(
               pinnedManifest(p),
               p.snapshot.compilerKeys,
            ),
         });
         return {
            runnable: answer.runnable,
            tablesRead: answer.binding ? [answer.binding] : undefined,
            shapeBindings: answer.binding ? [answer.binding] : [],
            virtualMap: answer.virtualMap,
            graftScope: answer.graftScope,
            viewTable:
               kind === "query" ? { ...answer.table, kind } : answer.table,
            ...(answer.databaseTableRead
               ? { resultTableRead: answer.databaseTableRead }
               : {}),
            ...(answer.keyedTable ? { keyedTable: answer.keyedTable } : {}),
         };
      },
   };
}

/**
 * Then `#(rollup)` rollups: the caller's text compiled where its
 * source's name means the coarsest rollup that reproduces it. A `storage=`
 * rollup reads through `virtual()`, a database one through the build manifest,
 * and a run failure on either may degrade to live, since a rollup only
 * accelerates (its binding never vetoes).
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
            // The tables it read, for the gate policy (for rollups) and
            // the row-scope and collation checks: a join branch in a storage
            // destination, the query source's tables there and the
            // rollup's, as the branch lists them; else the rollup, a
            // `storage=` one by its binding, a database one through the
            // build manifest.
            gateTablesRead:
               answer.gateTablesRead ??
               (answer.binding
                  ? [answer.binding]
                  : [
                       {
                          tier: answer.virtualMap ? "storage" : "database",
                          // The database rollup's table, for the check of
                          // its own collation (`gate_collation.ts`), which
                          // reads its kind: a rollup's build applied its
                          // base's own `where:`.
                          ...(answer.databaseTablePath
                             ? { tablePath: answer.databaseTablePath }
                             : {}),
                          ...{ kind: "rollup" },
                       },
                    ]),
            // Every stored table the answer reads, when that is more than the
            // rollup's (a join branch answered in a storage destination reads
            // the query source's table there too): together they decide
            // whether a failed run may be retried live.
            shapeBindings: answer.virtualMap
               ? (answer.bindings ?? (answer.binding ? [answer.binding] : []))
               : [],
            virtualMap: answer.virtualMap,
            graftScope: answer.graftScope,
            rollupMember: answer.rollupSourceName,
            rollupBaseSourceID: answer.rollupBaseSourceID,
            ...(answer.keyedTable ? { keyedTable: answer.keyedTable } : {}),
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
         // A composite answers from the member live resolves to, or live.
         await assertSameCompositeMember(q.authorRunnable, served.runnable);
         return {
            runnable: served.runnable,
            tablesRead: served.tablesRead,
            gateTablesRead: served.gateTablesRead,
            rerootedSourceIDs: served.rerootedSourceIDs,
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
   databaseRead: ReadonlySet<string>,
): Answer {
   return Object.freeze({ ...answer, databaseRead });
}

/**
 * Which tier answered a query, when a stored table did: `storage`, a table in
 * a `storage=` destination read through `virtual()`; `database`, a table in
 * the source's own database (Malloy's substitution, the served copy over a
 * database table, a database rollup or persisted view); `live_fallback`, a
 * routed answer whose stored run failed and degraded to live (a success that
 * must NOT count as a storage hit). Absent means no stored table answered.
 */
export type ServedFrom = "storage" | "database" | "live_fallback";

/** `servedFrom` for the answer, the same tier the path report's `servedBy` names. */
export function servedFromOf(answer: Answer): ServedFrom | undefined {
   if (answer.leg === "live") {
      return answer.databaseRead.size > 0 ? "database" : undefined;
   }
   if (answer.runFailed) return "live_fallback";
   const { tablesRead, virtualMap } = answer.answer;
   if (answer.leg === "served") {
      return tablesRead !== undefined &&
         tablesRead.length > 0 &&
         tablesRead.every(isDatabase)
         ? "database"
         : "storage";
   }
   // Decided by the table's own tier: a database table altered outside its
   // build is read through `virtual()` too, on its own connection, and is
   // still a database read, as the path report's `servedBy.tier` says.
   const tier =
      answer.answer.viewTable?.tier ?? answer.answer.gateTablesRead?.[0]?.tier;
   if (tier !== undefined) return tier === "storage" ? "storage" : "database";
   // A `storage=` persisted view or rollup reads through `virtual()`; a
   // database one through the build manifest.
   return virtualMap !== undefined ? "storage" : "database";
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
 * THIS shape (a sibling source must not decide), for DECIMAL arithmetic the
 * destination cannot hold ({@link isStoredDecimalArithmeticError}), or for a
 * destination whose catalog is unavailable
 * ({@link isDestinationCatalogUnavailableError}), never for a client error (a
 * bad given) or an abort. A database rollup reads its table through the build
 * manifest, not a virtual map, and carries no binding; it degrades whatever the
 * failure, as a stored rollup's binding lets it, since a rollup only
 * accelerates.
 */
export function canDegradeToLive(
   answer: Answer,
   error: unknown,
   opts: { hasLiveRunnable: boolean; aborted: boolean },
): answer is RoutedAnswer {
   if (
      answer.leg === "live" ||
      !opts.hasLiveRunnable ||
      opts.aborted ||
      String((error as { code?: string })?.code ?? "").startsWith(
         "runtime-given-",
      )
   ) {
      return false;
   }
   // A stored table that no longer holds what the query reads (a column or
   // the table gone since the bind): the table is the incompatible one, and
   // live answers, on every leg.
   if (isStoredSchemaError(error)) return true;
   if (!answer.answer.virtualMap) return answer.leg === "rollup";
   return (
      bindingsAllowDegradeToLive(answer.answer.shapeBindings) ||
      isStoredDecimalArithmeticError(error) ||
      isDestinationCatalogUnavailableError(error)
   );
}

/**
 * Whether `error` is a stored read finding the table not as its bind found it:
 * a column or the table itself missing (DuckDB's binder and catalog errors,
 * Postgres 42703 and 42P01, BigQuery's unrecognized name and not-found
 * table, Snowflake's invalid identifier and missing object). The failed-read
 * trigger of the drift handler (`reportIncompatible`).
 */
export function isStoredSchemaError(error: unknown): boolean {
   const code = String((error as { code?: unknown })?.code ?? "");
   if (code === "42703" || code === "42P01") return true;
   const message = error instanceof Error ? error.message : String(error);
   return /Binder Error: (Referenced column|Table)|Catalog Error: Table with name .* does not exist|column "[^"]+" does not exist|relation "[^"]+" does not exist|Unrecognized name:|Not found: Table|invalid identifier|does not exist or not authorized/i.test(
      message,
   );
}

/**
 * Whether `error` is a stored read finding its destination's catalog missing
 * from the worker's instance: the lookup's own check, or a read whose binder
 * error the connection confirmed by checking the instance
 * (DuckLakeCatalogUnavailableError, connection.ts). A binder error naming a
 * catalog that is there, a misbinding, is not one. The stored table is an
 * optimisation and the fault is the worker's, so such an answer degrades to
 * live whatever the bindings' `freshnessFallback` says; the path report names
 * it `READ_FAILED`.
 */
export function isDestinationCatalogUnavailableError(error: unknown): boolean {
   return isDuckLakeCatalogUnavailable(error);
}

/**
 * Whether `error` is DuckDB refusing DECIMAL arithmetic past its 38 digits: a
 * product whose scale exceeds 38 (`DECIMAL(38,20) * DECIMAL(38,20)`), or a
 * product, sum or addition that overflows. A Postgres `numeric` is stored as
 * the exact DECIMAL(38, s) that holds it (`run/passthrough/postgres.ts`),
 * where live's arbitrary precision has room the destination's arithmetic does
 * not. The table is right and only the arithmetic ran out, so such an answer
 * degrades to live whatever the bindings' `freshnessFallback` says.
 */
export function isStoredDecimalArithmeticError(error: unknown): boolean {
   const message = error instanceof Error ? error.message : String(error);
   return /Out of Range Error: (Needed scale .*DECIMAL|Overflow in [^\n]*DECIMAL|Overflow in HUGEINT)/.test(
      message,
   );
}

/**
 * The build manifest the answering run takes. A run on a virtual map (the
 * served copy, a `storage=` view or rollup) takes none; a database rollup
 * reads the full pinned manifest (its entries), a database persisted view
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
