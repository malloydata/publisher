// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served model on `virtual`: one model's served copy (served_copy.ts),
 * compiled once per binding set and answering queries the router sends it
 * (materialization v2, phase 3).
 *
 * Behind `materialize.enabled` (`PUBLISHER_MATERIALIZE`, default off), the one switch for `#(materialize)` build and serve. With it off nothing here
 * runs and serving goes through today's storage serve shape, unchanged.
 *
 * The served copy compiles in a runtime whose connections are the package's
 * own plus each storage destination under a per-process alias
 * (served_copy_config.ts), so the author's text never names a destination.
 * A compile failure leaves the model's sources serving through the next
 * candidate (today's serve shape, then live) and is reported as a coded
 * package warning.
 */

import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import {
   MalloyError,
   type MalloyConfig,
   type ModelDef,
   type ModelMaterializer,
   type QueryMaterializer,
} from "@malloydata/malloy";
import {
   collectEntryPointGates,
   type GraftRuntime,
   type GraftScope,
} from "../../gate_classification";
import {
   buildRowLevelProbe,
   gateFilterText,
   liftProbeFilterCondition,
} from "../../authorize";
import { quoteManifestTablePath } from "../../quoting";
import { keyedOverlay, overlayKey } from "../../overlay_key";
import {
   createModelRuntime,
   type ModelConnectionInput,
} from "../../model_runtime";
import { SourceMap } from "../plan/source_map";
import { givenReadingFilterTerms } from "../plan/persist_dynamic_terms";
import type {
   MaterializePair,
   MaterializePlan,
} from "../plan/build_file_generator";
import { rollingWindowsOf } from "../plan/rolling_window";
import { evaluateManifestFreshness } from "../run/freshness";
import {
   declaredColumnsOf,
   storedColumnTypeMismatches,
} from "../stored_column_types";
import { annotatedJoinedOutput } from "../../governance/ir/joined_field_output";
import {
   joinGrainAggregatePaths,
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { joinedDefinitionOf } from "../../governance/ir/source_graph";
import {
   flatPairQueryVerdict,
   pairConditionReadsChild,
   pairMeasureLevels,
   type FlatPair,
} from "../../governance/ir/pair_flat_query";
import {
   authorModelLiftContext,
   buildVirtualMap,
   narrowSchemaToPublic,
   optsOutOfPersist,
   type DerivedSourceDef,
   type ServeBinding,
} from "./materialization_serve_transform";
import {
   buildServedCopy,
   declaredBaseName,
   type InlinedJoin,
   type JoinedColumn,
   type PairMeasure,
   type PairReroot,
   type Reroot,
   type ServedColumn,
   type ServedCopyRange,
   storedMalloyType,
} from "./served_copy";
import { servedCopyConfig, servedCopyAlias } from "./served_copy_config";
import { lexAll, offsetOf } from "../malloy_lexer";
import { classificationFindingsBySource } from "../../classification_findings";
import {
   readSourceClassifications,
   type FieldClassification,
   type IrField,
} from "../../classification";
import { annotationValue } from "../../malloy_text";
import {
   isCrossConnectionRefusal,
   leaveOutCrossConnectionJoins,
   type LeftOutDefinition,
} from "../plan/cross_connection_joins";

type VirtualMap = Map<string, Map<string, string>>;

/**
 * Why the served copy declined a query, for the path report: `time_zone`, the
 * query sets a timezone over a stored computed dimension; `cross_connection`,
 * the copy waits on a join across connections, or the query reads a join it
 * leaves out across connections; `not_served`, the copy (or the
 * queried source in it) carries a `served-copy-not-served` warning;
 * `opt_out`, the queried source opts out with `#@ -persist`;
 * `count_semantics`, the query aggregates at the grain of a join the copy
 * answers from stored columns (an inlined join_one), which only live's join
 * computes; `joined_annotations`, the query returns a field of such a join
 * that the author annotated, whose notes its record cannot carry;
 * `pair_shape`, the query aggregates a pair table (an inlined join_many,
 * served flat) in a way that table does not reproduce exactly;
 * `pair_parent_only`, the query reads nothing through the inlined join, so
 * the pair table (one row per child) is the costly way to answer it.
 */
export type ServedCopyDeclineCode =
   | "time_zone"
   | "cross_connection"
   | "not_served"
   | "opt_out"
   | "count_semantics"
   | "joined_annotations"
   | "pair_shape"
   | "pair_parent_only";

/** A query the served copy declines, with why. */
export class ServedCopyDecline extends Error {
   constructor(
      message: string,
      readonly code: ServedCopyDeclineCode,
      /** What the path report may tell the caller; the message stays in logs. */
      readonly callerDetail?: string,
   ) {
      super(message);
   }
}

/** The caller's detail for a join-grain aggregate through an inlined join. */
const JOIN_GRAIN_DETAIL =
   "The query aggregates at the grain of a join whose fields this table stores inline (a measure of the joined source, or an aggregate such as j.x.sum() or j.count()), which only the join computes, so it is answered live.";

/** The caller's detail for a query that returns an annotated field of an inlined join. */
const JOINED_ANNOTATIONS_DETAIL =
   "The query returns a field of a join whose fields this table stores inline, and the field carries annotations (a #(doc), a #(classification), a render tag) that the stored copy cannot attach to it, so it is answered live.";

/** The caller's detail for a query through a join left out across connections. */
const LEFT_OUT_JOIN_DETAIL =
   "The query uses a join to a source on another connection than the destination this table is stored in, so it is answered live.";

/**
 * Whether a query's compile failure reads a definition the served copy left
 * out across connections: a missing field whose name, where the problem points
 * in the query's text, is one left out (the copy keeps every other field of
 * the author's). A problem whose place cannot be read counts, since declining
 * only sends the query on to live.
 */
function readsLeftOutDefinition(
   compiled: Compiled,
   error: unknown,
   queryString: string,
): boolean {
   if (!compiled.leftOut?.length) return false;
   if (!(error instanceof MalloyError)) return false;
   const names = new Set(compiled.leftOut.map((l) => l.name));
   return (
      error.problems as {
         code?: string;
         at?: { range?: { start: { line: number; character: number } } };
      }[]
   ).some((p) => {
      if (p.code === undefined || !MISSING_FIELD_CODES.has(p.code)) {
         return false;
      }
      const start = p.at?.range?.start;
      const offset = start ? offsetOf(queryString, start) : -1;
      if (offset < 0) return true;
      const path = /^[`A-Za-z0-9_.]+/.exec(queryString.slice(offset))?.[0];
      if (!path) return true;
      return path
         .split(".")
         .some((part) => names.has(part.replace(/^`|`$/g, "")));
   });
}

/** Malloy's codes for a compile that reads a field the model does not have. */
const MISSING_FIELD_CODES: ReadonlySet<string> = new Set([
   "field-not-found",
   "aggregate-source-not-found",
]);

/** The code on a package warning for a served copy that did not compile or re-root. */
export const SERVED_COPY_WARNING_CODE = "served-copy-not-served";

/** Why a model's served copy, or one source in it, is not served. */
export interface ServedCopyProblem {
   /** The source, when the problem is one source's. */
   sourceName?: string;
   reason:
      | "compile_failed"
      | "cross_connection_unbuilt"
      | "column_types"
      | "no_declaration"
      | "range_not_found"
      | "definition_not_parsed"
      | "pair_join_not_found"
      | "pair_not_compiled"
      | "fields_not_carried"
      | "read_terms_not_kept"
      | "cross_connection_join"
      | "inline_join_kept"
      | "classification_not_kept";
   /**
    * For `cross_connection_join`: the join the served copy leaves out, because
    * it reads a source on another connection than the destination its source
    * is stored in. For `inline_join_kept`: the inlined join_one the served
    * copy keeps as written, because the copy did not compile with the record
    * of stored columns in its place (`detail` says why).
    */
   join?: string;
   /**
    * For `classification_not_kept`: the served copy's `#(carries)` leaks on
    * the source, in the words the load would refuse it with, and its
    * classification-coverage findings, which the compile of record does not
    * have.
    */
   leaks?: string[];
   coverage?: string[];
   /** The compiler's message, for logs and the package warning only. */
   detail?: string;
}

/** What the served copy reads from the model that owns it. */
export interface ServedCopyHost {
   modelPath(): string;
   modelDef(): unknown;
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
}

interface Compiled {
   key: string;
   /** The invalidation generation it was compiled under. */
   generation: number;
   materializer?: ModelMaterializer;
   /** The served copy's compiled model, read (never edited) for source chains. */
   modelDef?: unknown;
   /** The runtime it compiled in: a row-level graft reloads through it (R3). */
   runtime?: GraftRuntime;
   /** The `sourceID`s of sources that opt out with `#@ -persist`. */
   optOutIDs?: ReadonlySet<string>;
   /**
    * The `sourceID`s of sources whose surface in the served copy has a
    * `#(carries)` leak or an unclassified field that the compile of record
    * does not: the served copy never answers a query on one.
    */
   unsafeIDs?: ReadonlySet<string>;
   /** Bindings actually re-rooted, with their destination under its alias. */
   rerooted: ServeBinding[];
   /**
    * Tables that store a computed dimension (`#(materialize)`), by table
    * path. The build evaluated it in the default timezone, so a query that
    * sets its own `timezone:` would read values live computes differently.
    */
   computedTables?: ReadonlySet<string>;
   /**
    * The join_ones each table answers from stored columns (an inlined join,
    * {@link InlinedJoin}), by table path, each with the joined fields the
    * author annotated: a query that aggregates at one's grain is declined
    * (`count_semantics`), and so is one that returns an annotated field as a
    * column (`joined_annotations`), whose notes a record cannot carry.
    */
   inlinedJoins?: ReadonlyMap<string, ReadonlyMap<string, ReadonlySet<string>>>;
   aliased: ServeBinding[];
   problems: ServedCopyProblem[];
   /**
    * Queries this compiled copy declined for their shape (they read no
    * re-rooted table, or read an opted-out source), by query text, with the
    * reason. Neither depends on the givens or the clock, only on the query and
    * this compile, so a repeat is declined without compiling it again
    * (decision 6). Dropped with the compile: a rebind or reload starts empty.
    */
   declined?: Map<string, ServedCopyDecline>;
   /**
    * The pair tables (inlined join_many, P5) the copy reads flat: a query that
    * reads one is checked against it (governance/ir/pair_flat_query.ts).
    */
   pairs?: readonly {
      sourceID: string;
      tablePath: string;
      pair: FlatPair;
   }[];
   /**
    * Whether the row-level gates on a pair source's entry point read its
    * inlined join, by entry point and gate text: they are grafted on the
    * copy after the query is checked, so the check reads them itself.
    */
   pairGatesReadChild?: Map<string, Promise<boolean>>;
   /**
    * Definitions left out because a join they read crosses connections in the
    * served copy (question 11): a query that reads one skips the served copy
    * with reason `cross_connection`.
    */
   leftOut?: readonly LeftOutDefinition[];
}

/**
 * A re-root with the author's declaration it was made from: the guards compare
 * the served copy's compile of it against this definition, which is the
 * declaring model's when this model reaches the source only through a join.
 */
type AuthorReroot = Reroot & { sourceID: string; authorDef: unknown };

/** How many declined queries one compiled copy remembers before starting over. */
const DECLINED_LIMIT = 1000;

/** One model's served copy. */
export class ServedCopyServe {
   private packagePath?: string;
   private packageConfig?: ModelConnectionInput;
   private materializePlans: Readonly<Record<string, MaterializePlan>> = {};
   /**
    * Every binding the package pushed, unfiltered: a `#(materialize)` build
    * source (`<src>__mat`) is no source of this model's by name, so the serve
    * shape's own filter drops it. The served copy matches each binding to a
    * declaration of this model by `sourceID` instead.
    */
   private bindings: ServeBinding[] = [];
   /** A declaration's definition from the package model that holds it. */
   private declarationOf?: (sourceID: string) => unknown;
   private compiled?: Compiled;
   private inFlight?: {
      key: string;
      generation: number;
      promise: Promise<Compiled>;
   };
   /** Bumped on every invalidation, so a compile started before it is never kept. */
   private generation = 0;

   constructor(private readonly host: ServedCopyHost) {}

   /**
    * The package directory and connections the served copy compiles with, and
    * the build plan's generated `#(materialize)` sources by sourceID.
    */
   setContext(
      packagePath: string,
      packageConfig: ModelConnectionInput,
      materializePlans: Readonly<Record<string, MaterializePlan>> = {},
      declarationOf?: (sourceID: string) => unknown,
   ): void {
      this.packagePath = packagePath;
      this.packageConfig = packageConfig;
      this.materializePlans = materializePlans;
      this.declarationOf = declarationOf;
      this.invalidate();
   }

   /** The package's bindings, as pushed to the model. */
   setBindings(bindings: ServeBinding[]): void {
      this.bindings = bindings;
      this.invalidate();
   }

   /**
    * The bindings of `#(materialize)` tables, which only the served copy reads
    * (the serve shape never takes one), keyed like every binding by the
    * author's sourceID; with `now`, only the fresh ones. For the path report,
    * which places them on the queried source's chain.
    */
   materializeBindings(now?: number): ServeBinding[] {
      const bindings =
         now === undefined ? this.bindings : this.freshBindings(now);
      return bindings.filter((b) =>
         (b.sourceIDs ?? []).some((id) => this.materializePlans[id]),
      );
   }

   /** Whether any binding could re-root a source (before compiling). */
   hasBindings(): boolean {
      return this.bindings.some((b) => b.origin !== "preaggregate");
   }

   /** Drop the compiled copy (bindings, destinations or the model changed). */
   invalidate(): void {
      this.generation++;
      this.compiled = undefined;
      this.inFlight = undefined;
   }

   /**
    * The author `sourceID`s the compiled copy re-rooted on a stored table,
    * which are the sources it answers from storage; undefined before the
    * first compile.
    */
   rerootedSourceIDs(): ReadonlySet<string> | undefined {
      if (!this.compiled) return undefined;
      return new Set(this.compiled.rerooted.flatMap((b) => b.sourceIDs ?? []));
   }

   /** Why the last compile left sources unserved; empty before the first. */
   problems(): readonly ServedCopyProblem[] {
      return this.compiled?.problems ?? [];
   }

   /** The served copy's compiled model, compiling it first; for reading only. */
   async compiledModelDef(): Promise<unknown> {
      return (await this.ensureCompiled()).modelDef;
   }

   /** Compile now (at bind), so warnings exist before the first query. */
   async prepare(): Promise<void> {
      try {
         await this.ensureCompiled();
      } catch {
         // Recorded on the compiled result; serving falls through.
      }
   }

   /**
    * Compile `queryString` against the served copy. Throws when the served copy
    * cannot answer it (no binding, a compile failure, a query reading a source
    * the copy left out); the router then tries the next candidate.
    */
   async loadQuery(
      queryString: string,
      givens?: Record<string, unknown>,
   ): Promise<{
      runnable: QueryMaterializer;
      virtualMap: VirtualMap;
      bindings: ServeBinding[];
      tablesRead: ServeBinding[];
      /**
       * The served copy's compiled model, where a row-level gate is grafted
       * when this answer runs (R3): the gate's text is the author's, kept in
       * the copy, so the graft and its proofs read the same compile that runs.
       */
      graftScope: GraftScope;
   }> {
      const compiled = await this.ensureCompiled();
      if (!compiled.materializer || compiled.rerooted.length === 0) {
         throw new ServedCopyDecline(
            compiled.problems[0]?.detail ??
               "no stored table is re-rooted in this model's served copy",
            compiled.problems.some(
               (p) => p.reason === "cross_connection_unbuilt",
            )
               ? "cross_connection"
               : "not_served",
         );
      }
      const declined = compiled.declined?.get(queryString);
      if (declined !== undefined) throw declined;
      const decline = (
         reason: string,
         code: ServedCopyDeclineCode,
         callerDetail?: string,
      ): never => {
         const memo = (compiled.declined ??= new Map());
         if (memo.size >= DECLINED_LIMIT) memo.clear();
         const error = new ServedCopyDecline(reason, code, callerDetail);
         memo.set(queryString, error);
         throw error;
      };
      // A query through a join left out across connections (question 11)
      // fails to compile for the field it reads: declined as
      // `cross_connection`, which depends only on the query and this compile.
      const compiling = async <T>(work: () => Promise<T>): Promise<T> => {
         try {
            return await work();
         } catch (error) {
            if (readsLeftOutDefinition(compiled, error, queryString)) {
               decline(
                  "the query reads a join the served copy leaves out across connections",
                  "cross_connection",
                  LEFT_OUT_JOIN_DETAIL,
               );
            }
            throw error;
         }
      };
      const virtualMap = buildVirtualMap(compiled.aliased);
      const runnable = compiled.materializer.loadRestrictedQuery(queryString);
      // `#@ -persist` means recompute rather than read the pre-built table. In
      // the served copy every name reads its re-rooted base, so a query whose
      // source is an opted-out reader, or derives from one, is declined here
      // and answered by the next candidate (which never carries one).
      // A source whose served surface the load-time classification checks
      // flag is declined the same way: answered live, or refused there.
      if (
         (compiled.optOutIDs && compiled.optOutIDs.size > 0) ||
         (compiled.unsafeIDs && compiled.unsafeIDs.size > 0)
      ) {
         const prepared = await compiling(() => readPreparedQuery(runnable));
         const struct = resolveStructRefSource<unknown>(
            prepared.structRef,
            compiled.modelDef as never,
         );
         const chain = SourceMap.of(compiled.modelDef).chainOf(struct);
         if (chain.some((id) => compiled.unsafeIDs?.has(id))) {
            decline(
               "the query reads a source whose served copy exposes a field the compile of record does not classify or carry",
               "not_served",
            );
         }
         if (chain.some((id) => compiled.optOutIDs?.has(id))) {
            decline(
               "the query reads a source that opts out of persist",
               "opt_out",
            );
         }
      }
      // Compile eagerly so a query the copy cannot answer fails inside the
      // router's attempt, not at run time past the fallback.
      const sql = await compiling(() =>
         runnable.getSQL({
            virtualMap,
            givens: givens as never,
         }),
      );
      const tablesRead = compiled.rerooted.filter((b) =>
         sql.includes(quoteManifestTablePath(b.tablePath, "duckdb")),
      );
      if (tablesRead.length === 0) {
         // The query reads no stored table: the served copy would only add
         // work over live. Let the next candidate answer.
         // A table on the queried source's chain that is not re-rooted was
         // left unserved, with a package warning saying why.
         decline("the query reads no re-rooted table", "not_served");
      }
      // A join answered from stored columns is a record, so an aggregate at
      // its grain (`j.x.sum()`, `j.count()`, a measure that reads one) would
      // be computed per row of the source instead of per joined row. Malloy
      // marks each such aggregate where it needs a symmetric one; live's
      // join answers those.
      const annotated = new Map<string, ReadonlySet<string>>();
      for (const b of tablesRead) {
         for (const [join, fields] of compiled.inlinedJoins?.get(b.tablePath) ??
            []) {
            annotated.set(join, fields);
         }
      }
      if (annotated.size > 0) {
         const inlined = new Set(annotated.keys());
         const prepared = await readPreparedQuery(runnable);
         const grain = joinGrainAggregatePaths(prepared);
         if (
            grain.unreadable ||
            grain.paths.some((p) => p.some((name) => inlined.has(name))) ||
            readsJoinStar(queryString, inlined)
         ) {
            decline(
               "the query aggregates at the grain of a join the served copy answers from stored columns",
               "count_semantics",
               JOIN_GRAIN_DETAIL,
            );
         }
         // A record literal's field carries no annotation onto a result, so
         // a query that returns an annotated joined field as a column (its
         // `#(doc)`, `#(classification)`, render tags) is answered live, and
         // every other query keeps the table.
         if ([...annotated.values()].some((f) => f.size > 0)) {
            const struct = resolveStructRefSource<{ fields?: unknown }>(
               prepared.structRef,
               compiled.modelDef as never,
            );
            const out = annotatedJoinedOutput(
               prepared.pipeline,
               struct?.fields,
               annotated,
            );
            if (out.unreadable || out.output) {
               decline(
                  out.output
                     ? `the query returns ${out.output.path.join(".")}, whose annotations the served copy's record of stored columns cannot carry`
                     : "the query's output columns cannot be read",
                  "joined_annotations",
                  JOINED_ANNOTATIONS_DETAIL,
               );
            }
         }
      }
      // A pair table (an inlined join_many) is read flat: exact only for the
      // aggregates pair_flat_query.ts proves, and only as the queried
      // source's own table, never through a join to it (its rows would fan
      // out the joining source).
      const pairsRead = (compiled.pairs ?? []).filter((p) =>
         tablesRead.some((b) => b.tablePath === p.tablePath),
      );
      if (pairsRead.length > 0) {
         const prepared = await readPreparedQuery(runnable);
         const struct = resolveStructRefSource<{
            fields?: unknown;
            filterList?: unknown;
         }>(prepared.structRef, compiled.modelDef as never);
         const chain = struct
            ? SourceMap.of(compiled.modelDef).chainOf(struct as never)
            : [];
         const own = pairsRead.find((p) => chain.includes(p.sourceID));
         const verdict =
            pairsRead.length > 1 || !own
               ? {
                    kind: "refused" as const,
                    reason:
                       "the query reads a pair table other than as the source it stores (through a join to it, whose rows would fan out the joining source, or from a source built on it)",
                 }
               : flatPairQueryVerdict(
                    prepared.query,
                    struct,
                    own.pair,
                    await pairGatesReadChild(compiled, struct, own.pair.join),
                 );
         if (verdict.kind !== "served") {
            // A query that reads no child field goes to the next candidate
            // (another stored table, or live) even when the flat rows would
            // answer it exactly: they hold each parent once per child.
            if (verdict.kind === "parent_only") {
               decline(
                  "the query reads nothing through the inlined join_many, so its pair table is not read for it",
                  "pair_parent_only",
                  "The table stores this source once per child of its inlined join_many and the query reads no child field, so it is answered without this table, which would read every child row to count each parent once.",
               );
            } else {
               decline(
                  `the pair table cannot answer this query exactly: ${verdict.reason}`,
                  "pair_shape",
                  `The table stores an inlined join_many flat, which cannot answer this query exactly (${verdict.reason}), so it is answered without this table.`,
               );
            }
         }
      }
      // A query that sets a timezone is answered live when it reads either
      // of two tables. A stored computed dimension (`order_ts.day`,
      // `day_of_week(order_ts)`) was evaluated at build in the default
      // timezone; live evaluates it in the query's. And a rolling window's
      // cut-off moves: Malloy takes its offset in the zone's wall time, so
      // across a daylight-saving change a read at the build's own clock needs
      // up to an hour older than the build stored. The serve shape never
      // carries a `#(materialize)` table.
      const computed = tablesRead.some((b) =>
         compiled.computedTables?.has(b.tablePath),
      );
      const windowed = tablesRead.some((b) => this.readsRollingWindow(b));
      if (
         (computed || windowed) &&
         setsTimezone((await readPreparedQuery(runnable)).query)
      ) {
         decline(
            computed
               ? "the query sets a timezone over a stored computed dimension"
               : "the query sets a timezone, which moves a rolling window's cut-off past what its table stores",
            "time_zone",
         );
      }
      return {
         runnable,
         virtualMap,
         bindings: compiled.rerooted,
         tablesRead,
         graftScope: {
            modelDef: compiled.modelDef as ModelDef,
            materializer: compiled.materializer,
            // Its own cache scope: a graft built on the served copy must never
            // be shared with one built on the author's model, and a new
            // compile (new bindings, a reload) starts afresh.
            cacheScope: `served_copy\u0000${compiled.key}`,
            runtime: compiled.runtime,
         },
      };
   }

   /** Whether a binding stores a `#(materialize)` source with a rolling window. */
   private readsRollingWindow(binding: ServeBinding): boolean {
      return (binding.sourceIDs ?? []).some(
         (id) =>
            rollingWindowsOf(this.materializePlans[id]?.readTimeTerms).length >
            0,
      );
   }

   private freshBindings(now: number): ServeBinding[] {
      const at = new Date(now);
      return this.bindings.filter(
         (b) =>
            b.origin !== "preaggregate" &&
            evaluateManifestFreshness(
               {
                  tableName: b.tablePath,
                  dataAsOf: b.freshAsOf,
                  freshnessWindowSeconds: b.freshnessWindowSeconds,
                  freshnessFallback: b.freshnessFallback,
               },
               at,
            ) === "serve_table",
      );
   }

   private async ensureCompiled(): Promise<Compiled> {
      const bindings = this.freshBindings(Date.now());
      const key = [
         ...bindings
            .map(
               (b) =>
                  `${b.sourceName}@${b.destinationName}/${b.virtualHandle}/${b.freshnessFallback ?? ""}`,
            )
            .sort(),
      ].join("|");
      const generation = this.generation;
      if (
         this.compiled &&
         this.compiled.key === key &&
         this.compiled.generation === generation
      ) {
         return this.compiled;
      }
      if (
         !this.inFlight ||
         this.inFlight.key !== key ||
         this.inFlight.generation !== generation
      ) {
         const started = {
            key,
            generation,
            promise: this.compile(key, generation, bindings),
         };
         this.inFlight = started;
         void started.promise.finally(() => {
            if (this.inFlight === started) this.inFlight = undefined;
         });
      }
      const compiled = await this.inFlight.promise;
      // An invalidation while it compiled makes it stale: keep it only when
      // it is still the current generation's.
      if (compiled.generation === this.generation) {
         this.compiled = compiled;
      }
      return compiled;
   }

   /**
    * The re-root for one binding, when it stores a source this model
    * declares (directly or through an import), else undefined.
    */
   private rerootFor(
      binding: ServeBinding,
      map: SourceMap,
      problems: ServedCopyProblem[],
      reservedNames: readonly string[],
   ): AuthorReroot | undefined {
      // `#(materialize)`: the binding is the generated build source's; its
      // plan names the author's source, by sourceID.
      const plan = binding.sourceIDs
         ?.map((id) => this.materializePlans[id])
         .find((p) => p !== undefined);
      const authorIDs = plan
         ? [plan.sourceID]
         : (binding.sourceIDs ??
           [map.sourceIDOf(binding.sourceName)].filter(
              (id): id is string => id !== undefined,
           ));
      const node =
         authorIDs.map((id) => map.node(id)).find((n) => n !== undefined) ??
         this.joinedDeclaration(authorIDs, map);
      // Another model's source: not this served copy's to re-root.
      if (!node) return undefined;
      const sourceName = plan?.sourceName ?? binding.sourceName;
      const def = node.def as {
         fields?: unknown[];
         location?: { url?: string; range?: ServedCopyRange };
      };
      const url = def.location?.url;
      const range = def.location?.range;
      if (!url || !range) {
         problems.push({ sourceName, reason: "no_declaration" });
         return undefined;
      }
      // A table an older build typed reads through a cast in today's serve
      // shape; the served copy does not carry casts yet, so it leaves the
      // source to that shape (or live).
      if (binding.readCasts?.length) {
         problems.push({ sourceName, reason: "column_types" });
         return undefined;
      }
      // The author's field of each name: its annotations, and the timeframe a
      // truncation carries, which the served copy restores on the field.
      const fieldsByName = new Map(
         (
            (def.fields ?? []) as {
               name?: unknown;
               as?: unknown;
               timeframe?: unknown;
            }[]
         ).map((f) => [String(f.as ?? f.name), f] as const),
      );
      const timeframeOf = (name: string) => {
         const tf = fieldsByName.get(name)?.timeframe;
         return typeof tf === "string" ? { timeframe: tf } : {};
      };
      let columns: ServedColumn[];
      let inlinedJoins: InlinedJoin[] = [];
      if (plan) {
         // Exactly what the build stored: the plan's columns (the source's
         // public atomic dimensions, computed ones too, each under its own
         // name), typed as stored, with each field's annotations.
         const stored = new Map(binding.schema.map((c) => [c.name, c.type]));
         // Each field's class as the compile of record reads it, so a stored
         // column whose field infers its class (a dimension computed from a
         // classified one) declares it: the column has no expression to read
         // it from.
         const lineage = readSourceClassifications(
            def.fields as IrField[] | undefined,
         );
         columns = [];
         for (const c of plan.columns) {
            // An inlined join's columns are read under the join's name
            // (inlinedJoinsOf, below), not as fields of the source.
            if (c.join !== undefined) continue;
            const type = stored.get(c.column);
            // A build that stored a dimension under another name cannot be
            // read back as that field.
            if (type === undefined || c.column !== c.dimension) {
               problems.push({ sourceName, reason: "column_types" });
               return undefined;
            }
            const notes = storedColumnNotes(
               fieldsByName.get(c.dimension),
               lineage.get(c.dimension),
            );
            columns.push({
               name: c.column,
               type,
               ...(notes.length > 0 ? { notes } : {}),
               ...timeframeOf(c.dimension),
            });
         }
         const joined = inlinedJoinsOf(plan, def.fields, stored, lineage);
         if (
            joined === undefined ||
            joined.some((j) =>
               j.columns.some((c) => storedMalloyType(c.type) === undefined),
            )
         ) {
            problems.push({ sourceName, reason: "column_types" });
            return undefined;
         }
         inlinedJoins = joined;
      } else {
         const narrowed = narrowSchemaToPublic(binding.schema, def.fields);
         if (
            storedColumnTypeMismatches(narrowed, declaredColumnsOf(def.fields))
               .length > 0
         ) {
            problems.push({ sourceName, reason: "column_types" });
            return undefined;
         }
         // Each with its field's annotations (`#(doc)`, `# label`, `#
         // percent` on the query's output field), as a `#(materialize)` column
         // carries them, so the stored field reads back as the author's.
         columns = narrowed.map((c) => {
            const notes = fieldNotes(fieldsByName.get(c.name));
            return {
               ...c,
               ...(notes.length > 0 ? { notes } : {}),
               ...timeframeOf(c.name),
            };
         });
      }
      // A column the `type:` cannot declare faithfully would fail the whole
      // model's copy, not just this source: leave this one to its fallback.
      if (columns.some((c) => storedMalloyType(c.type) === undefined)) {
         problems.push({ sourceName, reason: "column_types" });
         return undefined;
      }
      let pair: PairReroot | undefined;
      if (plan?.pair) {
         pair = pairRerootOf(
            plan,
            plan.pair,
            def.fields ?? [],
            new Map(binding.schema.map((c) => [c.name, c.type])),
            map,
            [...reservedNames, ...plan.columns.map((c) => c.column)],
         );
         if (!pair) {
            problems.push({ sourceName, reason: "column_types" });
            return undefined;
         }
      }
      return {
         url,
         range,
         sourceName,
         ...(pair ? { pair } : {}),
         connectionAlias: servedCopyAlias(binding.destinationName),
         virtualHandle: binding.virtualHandle,
         typeName: `${sourceName}__served`,
         columns,
         reservedNames,
         ...(plan
            ? {
                 storedDimensions: plan.columns
                    .filter((c) => c.computed && c.join === undefined)
                    .map((c) => c.dimension),
              }
            : {}),
         ...(plan?.readTimeTerms?.length
            ? { readTimeTerms: plan.readTimeTerms }
            : {}),
         ...(inlinedJoins.length > 0 ? { inlinedJoins } : {}),
         sourceID: node.sourceID,
         authorDef: node.def,
      } as AuthorReroot;
   }

   /**
    * A source this model reaches only as a join (#6643): its declaration, as
    * the package model holding it compiled it, when one of this model's
    * sources joins it. Re-rooting it in its declaring file is what lets the
    * joining source's re-root keep its join on the destination.
    */
   private joinedDeclaration(
      authorIDs: readonly string[],
      map: SourceMap,
   ): { sourceID: string; def: unknown } | undefined {
      for (const sourceID of authorIDs) {
         if (
            !map.nodes().some((n) => joinedDefinitionOf(n.def, sourceID)) ||
            !this.declarationOf
         ) {
            continue;
         }
         const def = this.declarationOf(sourceID);
         if (def) return { sourceID, def };
      }
      return undefined;
   }

   private async compile(
      key: string,
      generation: number,
      bindings: ServeBinding[],
   ): Promise<Compiled> {
      const problems: ServedCopyProblem[] = [];
      const destinations = this.host.serveDestinationConfig();
      if (!destinations || !this.packagePath || !this.packageConfig) {
         return { key, generation, rerooted: [], aliased: [], problems };
      }
      const modelDef = this.host.modelDef();
      const map = SourceMap.of(modelDef);
      let candidates: {
         binding: ServeBinding;
         reroot: AuthorReroot;
      }[] = [];
      const reservedNames = modelFieldNames(map);
      const lift = authorModelLiftContext(modelDef, (u) => {
         try {
            return readFileSync(fileURLToPath(u), "utf8");
         } catch {
            return undefined;
         }
      });
      for (const binding of bindings) {
         const reroot = this.rerootFor(binding, map, problems, reservedNames);
         if (reroot) {
            candidates.push({
               binding,
               reroot: reroot as AuthorReroot,
            });
         }
      }
      // An extension of a re-rooted source reads it by name already, and
      // re-rooting the extension too would replace the base it inherits its
      // parent's fields from (a `#(materialize)` extension inherits the
      // annotation, so it has a table of its own). Leave it as written.
      // The chain comes from the compiled `extends`, which Malloy records only
      // in a model with the persistence flag (a `#(materialize)` model needs
      // none), so the declaration's own text, `<name> is <base> extend …`,
      // answers too.
      const rerootedIDs = new Set(candidates.map((c) => c.reroot.sourceID));
      const rerootedNames = new Set(candidates.map((c) => c.reroot.sourceName));
      candidates = candidates.filter((c) => {
         const node = map.node(c.reroot.sourceID);
         const chain = node ? map.chainOf(node.def) : [];
         if (chain.slice(1).some((id) => rerootedIDs.has(id))) return false;
         const text = lift.fileText(c.reroot.url);
         const base =
            text === undefined
               ? undefined
               : declaredBaseName(text, c.reroot.range);
         return base === undefined || !rerootedNames.has(base);
      });
      // Inlined joins kept as written because the copy did not compile with
      // their records (see keepInlinedJoins, below), and pair tables dropped
      // because it did not compile with them: each reported only once it
      // compiles without them (when it still does not, they were not the
      // cause).
      let inlinedKept: ServedCopyProblem[] = [];
      let pairsDropped: ServedCopyProblem[] = [];
      // At most one pass per candidate: each pass either compiles with every
      // remaining source whole, or drops the sources it lost fields on; and
      // one more each that keeps inlined joins as written and drops the pair
      // tables.
      for (let pass = 0; pass <= candidates.length + 2; pass++) {
         const copy = buildServedCopy(
            candidates.map((c) => c.reroot),
            (u) => lift.fileText(u),
         );
         const refused = new Set(
            copy.refused.map((r) => `${r.url}\u0000${r.sourceName}`),
         );
         if (pass === 0) {
            for (const r of copy.refused) {
               // A source declared by refinement stays as written by design,
               // and still reads a re-rooted base by name: nothing to warn of.
               if (r.reason === "not_query_shaped") continue;
               problems.push({ sourceName: r.sourceName, reason: r.reason });
            }
         }
         candidates = candidates.filter(
            (c) => !refused.has(`${c.reroot.url}\u0000${c.reroot.sourceName}`),
         );
         if (candidates.length === 0) {
            // Nothing left to compile: a pair table dropped was the only one.
            problems.push(...pairsDropped);
            return { key, generation, rerooted: [], aliased: [], problems };
         }
         const rerooted = candidates.map((c) => c.binding);
         const aliased = rerooted.map((b) => ({
            ...b,
            destinationName: servedCopyAlias(b.destinationName),
         }));
         const overlay = new Map<string, string>();
         for (const [url, text] of copy.files) {
            overlay.set(overlayKey(url), text);
         }
         const packagePath = this.packagePath;
         const config = servedCopyConfig(
            this.packageConfig,
            destinations(),
            new Set(rerooted.map((b) => b.destinationName)),
         );
         const load = async (files: ReadonlyMap<string, string>) => {
            const { runtime, modelURL, importBaseURL } =
               await createModelRuntime(
                  packagePath,
                  this.host.modelPath(),
                  config,
                  { overlay: files },
               );
            const m = runtime.loadModel(modelURL, { importBaseURL });
            const model = await m.getModel();
            return {
               materializer: m,
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               def: (model as any)._modelDef,
               runtime: runtime as unknown as GraftRuntime,
            };
         };
         let materializer: ModelMaterializer;
         let servedDef: unknown;
         let servedRuntime: GraftRuntime;
         let leftOut: LeftOutDefinition[] = [];
         // The record that replaces an inlined join is the one text the
         // served copy writes over a join's place; when the copy does not
         // compile with it (the source's own text reads the join in a way
         // the record cannot answer), compile again with every inlined join
         // kept as written, which reads the joined source through the join.
         // Each join kept is reported once the copy compiles without the
         // records: when it still does not, they were not the cause.
         const keepInlinedJoins = (error: unknown): boolean => {
            if (!candidates.some((c) => c.reroot.inlinedJoins)) return false;
            const detail =
               error instanceof Error ? error.message : String(error);
            inlinedKept = candidates.flatMap((c) =>
               (c.reroot.inlinedJoins ?? []).map((j) => ({
                  sourceName: c.reroot.sourceName,
                  reason: "inline_join_kept" as const,
                  join: j.join,
                  detail,
               })),
            );
            candidates = candidates.map((c) => {
               if (!c.reroot.inlinedJoins) return c;
               const { inlinedJoins: _kept, ...reroot } = c.reroot;
               return { ...c, reroot };
            });
            return true;
         };
         try {
            ({
               materializer,
               def: servedDef,
               runtime: servedRuntime,
            } = await load(overlay));
         } catch (err) {
            // A reference join the re-root moved across connections (question
            // 11): leave it out, with what reads it, so every other query
            // still reads the table.
            const byURL = new Map(
               [...copy.files].map(([url, text]) => [url, text] as const),
            );
            const kept = await leaveOutCrossConnectionJoins({
               firstError: err,
               overlay: byURL,
               readText: (u) => lift.fileText(u),
               compile: async (files) => {
                  await load(keyedOverlay(files));
               },
            }).catch(() => undefined);
            // A pair table's served source drops what its record cannot carry
            // (the joined source's measures and views): author text that reads
            // one through the join does not compile there. When the copy does
            // not compile, try again without the pair tables first, so the
            // model's other tables still serve.
            const withoutPairs = (detail: string): boolean => {
               const pairs = candidates.filter((c) => c.reroot.pair);
               if (pairs.length === 0) return false;
               pairsDropped = pairs.map((c) => ({
                  sourceName: c.reroot.sourceName,
                  reason: "pair_not_compiled" as const,
                  detail,
               }));
               candidates = candidates.filter((c) => !c.reroot.pair);
               return true;
            };
            if (!kept) {
               if (keepInlinedJoins(err)) continue;
               const detail = err instanceof Error ? err.message : String(err);
               if (withoutPairs(detail)) continue;
               problems.push({
                  reason: classifyServedCopyFailure(err),
                  detail,
               });
               return { key, generation, rerooted: [], aliased: [], problems };
            }
            try {
               ({
                  materializer,
                  def: servedDef,
                  runtime: servedRuntime,
               } = await load(keyedOverlay(kept.overlay)));
            } catch (again) {
               if (keepInlinedJoins(again)) continue;
               const detail =
                  again instanceof Error ? again.message : String(again);
               if (withoutPairs(detail)) continue;
               problems.push({ reason: "compile_failed", detail });
               return { key, generation, rerooted: [], aliased: [], problems };
            }
            leftOut = kept.leftOut;
         }
         problems.push(...inlinedKept);
         inlinedKept = [];
         problems.push(...pairsDropped);
         pairsDropped = [];
         // A re-rooted source must keep every field the author's has. One it
         // inherited from a base the re-root replaced (a measure, a view, a
         // join declared on the base) is not in its own text, so it is lost:
         // that source is left as written and the copy compiled again.
         const servedMap = SourceMap.of(servedDef);
         // What a cross-connection join took with it is reported on its own,
         // when it was left out of this source or one it is built from.
         const lost = candidates.filter((c) => {
            const author = fieldNames(c.reroot.authorDef);
            const served = fieldNames(
               servedDefOf(servedMap, c.reroot.sourceID),
            );
            const node = map.node(c.reroot.sourceID);
            const chain = new Set(
               (node ? map.chainOf(node.def) : [c.reroot.sourceID])
                  .map((id) => map.node(id)?.name)
                  .filter((n): n is string => n !== undefined),
            );
            chain.add(c.reroot.sourceName);
            const excused = new Set(
               leftOut
                  .filter((l) => chain.has(l.declaration ?? l.name))
                  .map((l) => l.name),
            );
            return [...author].some(
               (name) => !served.has(name) && !excused.has(name),
            );
         });
         // Fail closed on tenant isolation: every `where:` term that reads a
         // given (re-applied per caller at read, never in the table) must be
         // applied by the served source too. One written where the re-root
         // replaced it (a parenthesised head, an inline extend in the base)
         // is gone from the served source's compiled filters, and the copy
         // would answer every caller with every caller's rows.
         // A source the served compile holds only as a join (#6643) and that
         // applies a `where:` is not re-rooted either: the governor proves a
         // joined source's filters against its declaring file's live compile,
         // whose text the re-root changed, so it would refuse every caller
         // (403) where live answers. Left as written, what reads it is live.
         const unscoped = candidates.filter(
            (c) =>
               !lost.includes(c) &&
               (!keepsGivenTerms(
                  c.reroot.authorDef,
                  servedDefOf(servedMap, c.reroot.sourceID),
               ) ||
                  (!hasOwnNode(servedMap, c.reroot.sourceID) &&
                     appliesFilters(c.reroot.authorDef))),
         );
         if (lost.length > 0 || unscoped.length > 0) {
            for (const c of lost) {
               problems.push({
                  sourceName: c.reroot.sourceName,
                  reason: "fields_not_carried",
               });
            }
            for (const c of unscoped) {
               problems.push({
                  sourceName: c.reroot.sourceName,
                  reason: "read_terms_not_kept",
               });
            }
            candidates = candidates.filter(
               (c) => !lost.includes(c) && !unscoped.includes(c),
            );
            continue;
         }
         // Every declaration the model reaches, imported ones included.
         const optOutIDs = new Set<string>();
         for (const node of servedMap.nodes()) {
            if (optsOutOfPersist(node.def as DerivedSourceDef)) {
               optOutIDs.add(node.sourceID);
            }
         }
         for (const l of leftOut) {
            if (!l.crossConnection) continue;
            problems.push({
               sourceName: l.declaration ?? l.name,
               reason: "cross_connection_join",
               join: l.name,
            });
         }
         const unsafeIDs = classificationNotKept(
            this.host.modelPath(),
            modelDef,
            servedDef,
            servedMap,
            problems,
         );
         return {
            key,
            generation,
            // An inlined join's columns are the joined source's fields as the
            // build computed them, in its time zone.
            computedTables: new Set(
               candidates
                  .filter(
                     (c) =>
                        (c.reroot.storedDimensions ?? []).length > 0 ||
                        (c.reroot.inlinedJoins ?? []).length > 0 ||
                        (c.reroot.pair?.childStoredDimensions ?? []).length > 0,
                  )
                  .map((c) => c.binding.tablePath),
            ),
            inlinedJoins: new Map(
               candidates
                  .filter((c) => c.reroot.inlinedJoins?.length)
                  .map((c) => [
                     c.binding.tablePath,
                     new Map(
                        (c.reroot.inlinedJoins ?? []).map((j) => [
                           j.join,
                           new Set(j.annotatedFields),
                        ]),
                     ),
                  ]),
            ),
            materializer,
            modelDef: servedDef,
            runtime: servedRuntime,
            optOutIDs,
            unsafeIDs,
            rerooted,
            aliased,
            problems,
            pairs: candidates.flatMap((c) =>
               c.reroot.pair
                  ? [
                       {
                          sourceID: c.reroot.sourceID,
                          tablePath: c.binding.tablePath,
                          pair: {
                             join: c.reroot.pair.join,
                             parentRowField: c.reroot.pair.parentRowField,
                             annotatedChildFields: new Set(
                                c.reroot.pair.childColumns
                                   .filter(
                                      (col) => (col.notes ?? []).length > 0,
                                   )
                                   .map((col) => col.name),
                             ),
                          },
                       },
                    ]
                  : [],
            ),
            ...(leftOut.length > 0 ? { leftOut } : {}),
         };
      }
      return { key, generation, rerooted: [], aliased: [], problems };
   }
}

/**
 * Whether a row-level gate (`#(access_filter)`) on the queried entry point
 * reads a pair table's inlined join. The governor grafts the gate on the
 * served copy after the router chose it, so the pair check never sees it in
 * the query: a gate that filters on a child field would drop a parent's
 * flagged row while its other rows stay, and its parent measures would
 * undercount. Each gate is compiled as the governor's own probe and read from
 * its IR; one that does not compile or cannot be read counts as reading it.
 */
async function pairGatesReadChild(
   compiled: Compiled,
   struct: unknown,
   join: string,
): Promise<boolean> {
   const gates = collectEntryPointGates(
      struct as never,
      compiled.modelDef as never,
   ).filter((g) => g.route === "access_filter");
   if (gates.length === 0) return false;
   const materializer = compiled.materializer;
   if (!materializer) return true;
   const reads = async (): Promise<boolean> => {
      for (const gate of gates) {
         const target = gate.struct?.as ?? gate.struct?.name;
         if (typeof target !== "string") return true;
         const filterText = gateFilterText(gate.exprs);
         try {
            const prepared = await materializer
               .loadQuery(buildRowLevelProbe(target, filterText))
               .getPreparedQuery();
            const lifted = liftProbeFilterCondition(
               prepared,
               `row-level probe for "${target}"`,
               filterText,
            );
            // The gate reads the entry point's fields, as it is grafted there.
            const source = (gate.struct ?? struct) as { fields?: unknown };
            if (pairConditionReadsChild(lifted, source, join) !== false) {
               return true;
            }
         } catch {
            return true;
         }
      }
      return false;
   };
   const key = gates
      .map(
         (g) =>
            `${g.struct?.as ?? g.struct?.name}\u0000${g.exprs.join("\u0000")}`,
      )
      .join("\u0001");
   const memo = (compiled.pairGatesReadChild ??= new Map());
   let answer = memo.get(key);
   if (answer === undefined) {
      answer = reads();
      memo.set(key, answer);
   }
   return answer;
}

/**
 * How the served copy reads one pair table (P5), from its plan, the stored
 * schema, the author's compiled source (`sourceFields`) and the joined
 * source's: each stored child column renamed internally under a name no
 * field of the model has (`taken`), and each measure over the source's own
 * fields only re-declared over the parent rows. Undefined when the table does
 * not store what the plan says (a column missing).
 */
function pairRerootOf(
   plan: MaterializePlan,
   pair: MaterializePair,
   sourceFields: readonly unknown[],
   stored: ReadonlyMap<string, string>,
   map: SourceMap,
   taken: readonly string[],
): PairReroot | undefined {
   const parentRow = stored.get(pair.parentRowColumn);
   if (!parentRow) return undefined;
   const used = new Set(taken);
   const fresh = (base: string) => {
      let name = `${base}__pair`;
      for (let n = 2; used.has(name); n++) name = `${base}__pair_${n}`;
      used.add(name);
      return name;
   };
   const childFields = new Map(
      (
         ((map.node(pair.childSourceID)?.def as { fields?: unknown[] })
            ?.fields ?? []) as {
            name?: unknown;
            as?: unknown;
            e?: unknown;
            timeframe?: unknown;
         }[]
      ).map((f) => [String(f.as ?? f.name), f] as const),
   );
   const childColumns: PairReroot["childColumns"][number][] = [];
   const childStoredDimensions: string[] = [];
   for (const c of plan.columns) {
      if (c.join !== pair.join) continue;
      const type = stored.get(c.column);
      if (type === undefined) return undefined;
      const field = childFields.get(c.dimension);
      const notes = fieldNotes(field);
      childColumns.push({
         name: c.dimension,
         column: c.column,
         internalName: fresh(c.column),
         type,
         ...(notes.length > 0 ? { notes } : {}),
         ...(typeof field?.timeframe === "string"
            ? { timeframe: field.timeframe }
            : {}),
      });
      if (field?.e !== undefined) childStoredDimensions.push(c.dimension);
   }
   const parentRowField = fresh(pair.parentRowColumn);
   // A measure over the source's own fields only is re-declared over the
   // parent rows; one through the join reads every row as it stands, and one
   // reading both (or anything else) is left as it is, so a query that reads
   // it is answered live (pair_flat_query.ts).
   const levels = pairMeasureLevels(sourceFields, pair.join);
   const parentMeasures: PairMeasure[] = [];
   for (const f of sourceFields as {
      name?: unknown;
      as?: unknown;
      accessModifier?: unknown;
   }[]) {
      const name = String(f.as ?? f.name);
      const level = levels.get(name);
      if (!level || level.size !== 1 || !level.has("parent")) continue;
      // A private measure cannot be renamed by an extension; a public one
      // that reads it carries the flag to it.
      if (f.accessModifier === "private") continue;
      const notes = fieldNotes(f);
      parentMeasures.push({
         name,
         internalName: fresh(name),
         ...(notes.length > 0 ? { notes } : {}),
         ...(f.accessModifier === "internal"
            ? { access: "internal" as const }
            : {}),
      });
   }
   return {
      join: pair.join,
      parentRowColumn: { name: pair.parentRowColumn, type: parentRow },
      parentRowField,
      childColumns,
      parentMeasures,
      childStoredDimensions,
   };
}

/**
 * The load-time classification checks, run on the served copy's own compile:
 * the compile that answers a query, which may reach fields the compile of
 * record never had (a join #6982 left out of it, answered once both sides are
 * built). A source whose served surface has a `#(carries)` leak, or an
 * unclassified field, that the compile of record does not is never served
 * from the copy, and a `classification_not_kept` problem names each finding.
 * Returns those sources' `sourceID`s.
 */
function classificationNotKept(
   modelPath: string,
   recordDef: unknown,
   servedDef: unknown,
   servedMap: SourceMap,
   problems: ServedCopyProblem[],
): Set<string> {
   const unsafe = new Set<string>();
   const before = classificationFindingsBySource(
      modelPath,
      recordDef as ModelDef,
   );
   const after = classificationFindingsBySource(
      modelPath,
      servedDef as ModelDef,
   );
   for (const [source, found] of after) {
      // A source the copy generates (a re-rooted base's) is reached only
      // through the author's sources, which are checked.
      const was = before.get(source);
      if (!was) continue;
      const leaks = found.leaks.filter((m) => !was?.leaks.includes(m));
      const coverage = found.coverage.filter((m) => !was?.coverage.includes(m));
      if (leaks.length === 0 && coverage.length === 0) continue;
      const id = servedMap.sourceIDOf(source);
      if (id) unsafe.add(id);
      problems.push({
         sourceName: source,
         reason: "classification_not_kept",
         leaks,
         coverage,
      });
   }
   return unsafe;
}

/**
 * The annotation lines on a compiled field, outermost first: what it inherits,
 * then its statement's (block) notes, then its own. Each is the author's text
 * (`#(doc) "…"`), so it can be written again on the field in a `type:`.
 */
function fieldNotes(field: unknown): string[] {
   interface Annotation {
      inherits?: Annotation;
      blockNotes?: { text?: unknown }[];
      notes?: { text?: unknown }[];
   }
   const out: string[] = [];
   const walk = (a: Annotation | undefined, depth: number) => {
      if (!a || depth > 16) return;
      walk(a.inherits, depth + 1);
      for (const n of [...(a.blockNotes ?? []), ...(a.notes ?? [])]) {
         if (typeof n.text === "string" && !out.includes(n.text)) {
            out.push(n.text);
         }
      }
   };
   const f = field as
      | { annotations?: Annotation; annotation?: Annotation }
      | undefined;
   walk(f?.annotations ?? f?.annotation, 0);
   return out;
}

/**
 * The notes a stored column carries in the `type:`: its field's own
 * ({@link fieldNotes}), and, when the field declares no class but the
 * compile of record infers one from what its expression reads, that class
 * declared, since the column stands in for the expression. A malformed
 * declaration is left as written.
 */
export function storedColumnNotes(
   field: unknown,
   classified: FieldClassification | undefined,
): string[] {
   const notes = fieldNotes(field);
   if (
      !classified ||
      classified.declared.length > 0 ||
      classified.malformed !== undefined ||
      classified.classifications.length === 0
   ) {
      return notes;
   }
   const classes = classified.classifications.map(annotationValue).join(", ");
   return [...notes, `#(classification) [${classes}]`];
}

/**
 * Whether the served source applies every given-reading `where:` term the
 * author's applies: the same text, reading the same givens, in its own
 * compiled filters (so a term in a comment, a string or another source never
 * counts). Unreadable or unrecorded terms fail closed.
 */
export function keepsGivenTerms(author: unknown, served: unknown): boolean {
   if (!author || !served) return false;
   try {
      const key = (t: { code: string; givens: string[] }) =>
         `${t.code.replace(/\s+/g, " ").trim()}\u0000${t.givens.join(",")}`;
      const kept = new Set(givenReadingFilterTerms(served).map(key));
      return givenReadingFilterTerms(author).every(
         (t) => t.code.trim() !== "" && kept.has(key(t)),
      );
   } catch {
      return false;
   }
}

/** Whether a compiled query sets a `timezone:` anywhere in it. */
function setsTimezone(query: unknown, depth = 0): boolean {
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
 * A declaration's definition in the served copy's compile: its node, or, for
 * a source this model reaches only as a join (an index that imports the file
 * joining it, not the file declaring it, #6643), the joined definition a join
 * holds, which carries the declaration's fields and filters. Undefined when
 * neither has it, which the guards read as everything lost.
 */
function servedDefOf(map: SourceMap, sourceID: string): unknown {
   const node = map.node(sourceID);
   if (hasOwnNode(map, sourceID)) return node?.def;
   for (const n of map.nodes()) {
      const join = joinedDefinitionOf(n.def, sourceID);
      if (join !== undefined) return join;
   }
   return node?.def;
}

/** Whether the compile holds `sourceID` as a node with fields of its own. */
function hasOwnNode(map: SourceMap, sourceID: string): boolean {
   return Array.isArray(map.node(sourceID)?.def.fields);
}

/** Whether a source definition applies any `where:` term. */
function appliesFilters(def: unknown): boolean {
   const filters = (def as { filterList?: unknown[] } | undefined)?.filterList;
   return Array.isArray(filters) && filters.length > 0;
}

/** Every field name any source in the model declares. */
function modelFieldNames(map: SourceMap): string[] {
   const names = new Set<string>();
   for (const node of map.nodes()) {
      for (const n of fieldNames(node.def)) names.add(n);
   }
   return [...names];
}

/** The names of a compiled source's fields. */
function fieldNames(def: unknown): Set<string> {
   const fields = (def as { fields?: { name?: unknown; as?: unknown }[] })
      ?.fields;
   return new Set(
      (fields ?? [])
         .map((f) => (typeof f.as === "string" ? f.as : f.name))
         .filter((n): n is string => typeof n === "string"),
   );
}

/**
 * Why a served copy did not compile. A join across origin connections compiles
 * only once both sides are re-rooted onto one destination; until the other
 * side is built, Malloy refuses the join ("Cannot join … on connection …"),
 * and that is a coded wait, not a broken model.
 */
export function classifyServedCopyFailure(
   error: unknown,
): "cross_connection_unbuilt" | "compile_failed" {
   return isCrossConnectionRefusal(error)
      ? "cross_connection_unbuilt"
      : "compile_failed";
}

/**
 * The join_ones a `#(materialize)` plan inlines, as the served copy answers
 * them from stored columns: each stored `<join>__<field>` column with its
 * stored type and the joined field's timeframe. A join whose joined source
 * filters by a given is left out (kept as written, so each caller's read
 * applies the filter through the join). Undefined when the table lacks a
 * column the plan stores.
 */
function inlinedJoinsOf(
   plan: MaterializePlan,
   fields: unknown[] | undefined,
   stored: ReadonlyMap<string, string>,
   lineage: ReadonlyMap<string, FieldClassification>,
): InlinedJoin[] | undefined {
   const joinDefs = new Map(
      (
         (fields ?? []) as {
            name?: unknown;
            as?: unknown;
            join?: unknown;
            fields?: unknown;
            accessModifier?: unknown;
         }[]
      )
         // A join the author hides stays as written: the record that would
         // replace it is a public dimension.
         .filter(
            (f) =>
               f.join === "one" &&
               (f.accessModifier == null || f.accessModifier === "public"),
         )
         .map((f) => [String(f.as ?? f.name), f] as const),
   );
   const out: InlinedJoin[] = [];
   for (const join of plan.inline ?? []) {
      const def = joinDefs.get(join);
      if (!def || givenReadingFilterTerms(def).length > 0) continue;
      const joinedFields = new Map(
         (
            (Array.isArray(def.fields) ? def.fields : []) as {
               name?: unknown;
               as?: unknown;
               timeframe?: unknown;
            }[]
         ).map((f) => [String(f.as ?? f.name), f] as const),
      );
      const columns: JoinedColumn[] = [];
      const annotatedFields: string[] = [];
      for (const c of plan.columns) {
         if (c.join !== join) continue;
         const type = stored.get(c.column);
         if (type === undefined) return undefined;
         const field = joinedFields.get(c.dimension);
         if (fieldNotes(field).length > 0) annotatedFields.push(c.dimension);
         const timeframe = field?.timeframe;
         // The joined field's own notes go on its stored column in the
         // `type:`, so the record's field, which reads it, keeps its class.
         const notes = storedColumnNotes(
            field,
            lineage.get(`${join}.${c.dimension}`),
         );
         columns.push({
            name: c.dimension,
            column: c.column,
            type,
            ...(notes.length > 0 ? { notes } : {}),
            ...(typeof timeframe === "string" ? { timeframe } : {}),
         });
      }
      if (columns.length > 0) out.push({ join, columns, annotatedFields });
   }
   return out;
}

/**
 * Whether a query reads `<join>.*` through any of `joins`: a star expands to
 * the joined source's fields, which a record of its stored fields may not all
 * be. Unlexable text counts.
 */
function readsJoinStar(
   queryString: string,
   joins: ReadonlySet<string>,
): boolean {
   const tokens = lexAll(queryString);
   if (!tokens) return true;
   return tokens.some(
      (t, i) =>
         joins.has(t.text.startsWith("`") ? t.text.slice(1, -1) : t.text) &&
         tokens[i + 1]?.text === "." &&
         tokens[i + 2]?.text === "*",
   );
}
