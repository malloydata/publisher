// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served model on `virtual`: one model's served copy (served_copy.ts),
 * compiled once per binding set and answering queries the router sends it
 * (materialization v2, phase 3).
 *
 * Behind `materialize.enabled` (`PUBLISHER_MATERIALIZE`, default off), the one switch for `#(materialize)` build and serve. With it off nothing here
 * runs and serving goes through today's storage serve shape and the colocated
 * build manifest, unchanged.
 *
 * The served copy compiles in a runtime whose connections are the package's
 * own plus each storage destination under a per-process alias
 * (served_copy_config.ts), so the author's text never names a destination. A
 * colocated `#(materialize)` table (colocated_bindings.ts) is read through the
 * source's own connection, which the package's connections already hold, so a
 * copy of colocated tables alone needs no storage tier.
 * A compile failure leaves the model's sources serving through the next
 * candidate (today's serve shape, then live) and is reported as a coded
 * package warning.
 */

import { readFileSync } from "fs";
import { lazyCounter } from "../../../telemetry";
import { fileURLToPath } from "url";
import {
   MalloyError,
   MalloyConfig,
   type ModelDef,
   type ModelMaterializer,
   type QueryMaterializer,
} from "@malloydata/malloy";
import {
   collectEntryPointGates,
   type GraftRuntime,
   type GraftScope,
} from "../../gate_classification";
import { gateDeclaringSourceResolves } from "../../filter_binding_guard";
import {
   buildRowLevelProbe,
   gateFilterText,
   liftProbeFilterCondition,
} from "../../authorize";
import { keyedOverlay, overlayKey } from "../../overlay_key";
import {
   createModelRuntime,
   type ModelConnectionInput,
} from "../../model_runtime";
import { SourceMap } from "../plan/source_map";
import { givenReadingFilterTerms } from "../plan/persist_dynamic_terms";
import {
   hiddenAccess,
   type MaterializePair,
   type MaterializePlan,
} from "../plan/build_file_generator";
import { rollingWindowsOf } from "../plan/rolling_window";
import { evaluateManifestFreshness } from "../run/freshness";
import {
   bigIntegerReadOf,
   bigIntegerReads,
   type BigIntegerRead,
   type BigIntegerReadOptions,
   nativeTypeLookup,
   declaredColumnsOf,
   queryTimezoneOf,
   serveTypePlan,
   storedColumnTypeMismatches,
} from "../stored_column_types";
import { annotatedJoinedOutput } from "../../governance/ir/joined_field_output";
import {
   joinGrainAggregatePaths,
   readPreparedQuery,
   setsTimezone,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { joinedDefinitionOf } from "../../governance/ir/source_graph";
import {
   flatPairQueryVerdict,
   pairConditionReads,
   pairMeasureLevels,
   pairQueryReadLevel,
   type FlatPair,
} from "../../governance/ir/pair_flat_query";
import {
   authorModelLiftContext,
   boundTablePath,
   sqlReadsTable,
   buildVirtualMap,
   narrowSchemaToPublic,
   optsOutOfPersist,
   type DerivedSourceDef,
   type ServeBinding,
} from "./materialization_serve_transform";
import { fieldNotes } from "../field_notes";
import {
   buildServedCopy,
   declaredBaseName,
   type InlinedJoin,
   type JoinedColumn,
   type PairBlockReroot,
   type PairHopColumns,
   type PairMeasure,
   type PairReroot,
   type Reroot,
   type ServedColumn,
   type ServedCopyRange,
   storedMalloyType,
} from "./served_copy";
import { servedCopyConfig, servedCopyAlias } from "./served_copy_config";
import {
   failingReroots,
   rerootKey,
   type TraceMode,
} from "./served_copy_containment";
import { logger } from "../../../logger";
import { lexAll, offsetOf } from "../malloy_lexer";
import { confirmedAncestors, keptAncestorChain } from "../kept_ancestors";
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
import { LegDecline, type ServedCopyDeclineCode } from "./leg_decline";

type VirtualMap = Map<string, Map<string, string>>;

/**
 * The connection a binding's table is read through in the served copy: a
 * storage destination under its server-only alias, a colocated table on the
 * source's own connection (colocated_bindings.ts), which the package's
 * connections already hold.
 */
function servedConnectionOf(b: ServeBinding): string {
   return b.colocated ? b.destinationName : servedCopyAlias(b.destinationName);
}

/** No storage destination: a served copy of colocated tables alone. */
const NO_DESTINATIONS = (): MalloyConfig =>
   new MalloyConfig({ connections: {} });

/** The caller's detail for a join-grain aggregate through an inlined join. */
const JOIN_GRAIN_DETAIL =
   "The query aggregates at the grain of a join whose fields this table stores inline (a measure of the joined source, or an aggregate such as j.x.sum() or j.count()), which only the join computes, so it is answered live.";

/** The caller's detail for a query that returns an annotated field of an inlined join. */
const JOINED_ANNOTATIONS_DETAIL =
   "The query returns a field of a join whose fields this table stores inline, and the field carries annotations (a #(doc), a #(classification), a render tag) that the stored copy cannot attach to it, so it is answered live.";

/** The caller's detail for a query that reads two blocks of a pair table, or one it cannot place. */
const PAIR_BLOCKS_DETAIL =
   "The table stores more than one inlined join_many, one block of rows each, and the query reads more than one of them (live aggregates each by its own key over their cross product, which no block holds), so it is answered without this table.";

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
   /**
    * The left-out source's identity, when the problem is one source's: what a
    * caller keys on (the keying rule), never its name.
    */
   sourceID?: string;
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
      | "reroot_not_compiled"
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
const staleBindingsWithheld = lazyCounter(
   "publisher_storage_bindings_withheld_total",
   "Storage serve bindings withheld from a routed query because the freshness gate found them stale; each one is read live instead. Until now staleness was visible only as a later live read.",
);

export interface ServedCopyHost {
   modelPath(): string;
   modelDef(): unknown;
   /**
    * The storage destinations, while the storage tier serves
    * (`PERSIST_STORAGE_MODE=on`); undefined otherwise, when only colocated
    * tables are read.
    */
   serveDestinationConfig(): (() => MalloyConfig) | undefined;
   /**
    * The shape ({@link resultShapeOf}) of `queryString`'s result compiled
    * against the author's model, reading no table; undefined when it does
    * not compile there. Without it, a query reading a table whose re-root
    * leaves a column out is declined.
    */
   authorResultShape?(
      queryString: string,
      givens?: Record<string, unknown>,
   ): Promise<string | undefined>;
   /**
    * Build the compiled copy's QueryModel once, so every query compiled
    * against it reuses it rather than building its own (the host's
    * `ensureSharedQueryModel`). Absent: each query builds its own.
    */
   ensureSharedQueryModel?(materializer: ModelMaterializer): Promise<void>;
   /**
    * A throwaway copy that only asks whether queries compile (a dry run's
    * view verdicts): what a served copy logs as a warning, it logs at debug,
    * since nothing is served.
    */
   quiet?: boolean;
}

interface Compiled {
   key: string;
   /** The invalidation generation it was compiled under. */
   generation: number;
   /**
    * The block every multi-path pair table is read for in this copy (see
    * {@link ServedCopyServe.ensureCompiled}); 0 for the copy compiled at bind.
    */
   lane: number;
   materializer?: ModelMaterializer;
   /** The served copy's compiled model, read (never edited) for source chains. */
   modelDef?: unknown;
   /** Whether any source in {@link modelDef} names a row-level gate; read once. */
   hasRowLevelGates?: boolean;
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
    * Extensions of a re-rooted source left as written, which read their
    * base's table by name: each with its chain of ancestor `sourceID`s and the
    * base its text names (see `readsRerootedBase`).
    */
   throughBase?: readonly {
      sourceID: string;
      sourceName: string;
      chain: readonly string[];
      baseName?: string;
   }[];
   /**
    * Tables that store a computed dimension (`#(materialize)`) whose value
    * may depend on the time zone, by table path: one of the source's own the
    * probe compiler did not clear (P9), or an inlined join's or pair table's
    * child column; or whose rows the probe did not clear (a `where:` with a
    * timestamp literal). The build evaluated it in the default timezone, so a
    * query that sets its own `timezone:` would read values live computes
    * differently.
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
   /**
    * The tables whose re-root leaves stored columns out of its `type:`
    * (`columnsLeftOut`: a column no cast reads as its declared type, a
    * big-integer column no native cast reads exactly, an undeclarable one),
    * by table path. A query naming one fails to compile and answers live;
    * a wildcard (`select: *`) expands without it, so a query reading one of
    * these tables is checked against the author's compile of it.
    */
   leftOutTables?: ReadonlySet<string>;
   aliased: ServeBinding[];
   problems: ServedCopyProblem[];
   /**
    * Queries this compiled copy declined for their shape (they read no
    * re-rooted table, or read an opted-out source), by query text, with the
    * reason. Neither depends on the givens or the clock, only on the query and
    * this compile, so a repeat is declined without compiling it again
    * (decision 6). Dropped with the compile: a rebind or reload starts empty.
    */
   declined?: Map<string, LegDecline>;
   /**
    * The pair tables (inlined join_many, P5) the copy reads flat: a query that
    * reads one is checked against it (governance/ir/pair_flat_query.ts).
    */
   pairs?: readonly {
      sourceID: string;
      tablePath: string;
      pair: FlatPair;
      /**
       * For a table of several inlined paths: every block's path, by block
       * number, which a query is read against to pick the block it reads.
       */
      blocks?: readonly { join: string; hops?: readonly string[] }[];
      /**
       * The joined source's fields that carry annotations, which the record
       * cannot: a query that returns one as a column is declined
       * (`joined_annotations`), as for an inlined join_one.
       */
      annotatedChildFields: ReadonlySet<string>;
   }[];
   /**
    * What the row-level gates on a pair source's entry point read of its
    * inlined path, by path, entry point and gate text: they are grafted on
    * the copy after the query is checked, so the check reads them itself.
    */
   pairGatesRead?: Map<string, Promise<{ level: number; fansOut: boolean }>>;
   /**
    * Definitions left out because a join they read crosses connections in the
    * served copy (question 11): a query that reads one skips the served copy
    * with reason `cross_connection`.
    */
   leftOut?: readonly LeftOutDefinition[];
   /**
    * The overlay the copy compiled from (the re-rooted files, by overlay
    * key) and the connections it compiled with: the join-to-rollup leg
    * compiles a variant over the same files (see {@link ServedCopyServe.joinRollupBasis}).
    */
   files?: ReadonlyMap<string, string>;
   config?: ModelConnectionInput;
}

/**
 * A re-root with the author's declaration it was made from: the guards compare
 * the served copy's compile of it against this definition, which is the
 * declaring model's when this model reaches the source only through a join.
 */
type AuthorReroot = Reroot & {
   sourceID: string;
   authorDef: unknown;
   /**
    * For a `#(materialize)` re-root: its table holds something the build
    * evaluated in the default zone that the probe compiler did not clear of
    * the query time zone (P9): a stored computed dimension, or its rows (a
    * `where:` term or base relation whose SQL moves with the zone).
    */
   storesZonedDimension?: true;
   /**
    * The stored columns left out of the `type:`, so only a query reading
    * one fails to compile here and answers live; their absence is not a lost
    * field. For a v1 re-root under the cutover's per-column types (G5), the
    * columns no cast reads as their declared type; on any re-root, the
    * big-integer columns no native cast reads exactly (known limitation 12:
    * one stored as a float, a warehouse whose bigint type Malloy does not
    * read back as one).
    */
   columnsLeftOut?: readonly string[];
};

/**
 * What the join-to-rollup leg reads of a compiled served copy, to answer a
 * query in a storage destination (see {@link ServedCopyServe.joinRollupBasis}).
 */
export interface ServedCopyJoinBasis {
   /** The compiled copy's identity: a variant over it is cached under this. */
   key: string;
   /** The re-rooted files, by overlay key, as the copy compiled them. */
   files: ReadonlyMap<string, string>;
   /** The connections the copy compiled with: the package's and its destinations under their aliases. */
   config: ModelConnectionInput;
   /** The copy's compiled model, for reading only. */
   modelDef: unknown;
   /** The bindings the copy re-rooted. */
   rerooted: readonly ServeBinding[];
   /** The same, each under the connection the copy reads it through (for the virtual map). */
   aliased: readonly ServeBinding[];
   /** The definitions the copy left out because a join they read crosses connections. */
   leftOut: readonly LeftOutDefinition[];
   /** The copy's checks on a query a variant over it answers; throws a {@link LegDecline}. */
   check(args: {
      /** The query source's author `sourceID`. */
      sourceID: string;
      runnable: QueryMaterializer;
      queryString: string;
      /** The re-rooted tables the variant's SQL reads. */
      tablesRead: readonly ServeBinding[];
   }): Promise<void>;
}

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
   /**
    * The compiled copy for each block a multi-path pair table is read for
    * (lane 0, compiled at bind, reads every table's first block; another is
    * compiled the first time a query reads that block).
    */
   private compiledLanes = new Map<number, Compiled>();
   private inFlightLanes = new Map<
      number,
      {
         key: string;
         generation: number;
         promise: Promise<Compiled>;
      }
   >();
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

   /**
    * Whether any binding could re-root a source (before compiling): a
    * colocated table, or a storage table while the storage tier serves.
    */
   hasBindings(): boolean {
      return this.bindings.some((b) => this.usable(b));
   }

   /**
    * Whether the served copy may read a binding: never a rollup's; a storage
    * table only while the host has storage destinations to serve from
    * (`PERSIST_STORAGE_MODE=on`); a colocated table always, on its own
    * connection.
    */
   private usable(b: ServeBinding): boolean {
      if (b.origin === "preaggregate") return false;
      return b.colocated === true || !!this.host.serveDestinationConfig();
   }

   /** Drop the compiled copy (bindings, destinations or the model changed). */
   invalidate(): void {
      this.generation++;
      this.compiledLanes.clear();
      this.inFlightLanes.clear();
   }

   /**
    * The author `sourceID`s the compiled copy re-rooted on a stored table,
    * which are the sources it answers from storage; undefined before the
    * first compile.
    */
   rerootedSourceIDs(): ReadonlySet<string> | undefined {
      const compiled = this.compiledLanes.get(0);
      if (!compiled) return undefined;
      return new Set(compiled.rerooted.flatMap((b) => b.sourceIDs ?? []));
   }

   /**
    * The author `sourceID`s the compiled copy leaves as written because they
    * extend a source it re-rooted, and so answer from that source's table by
    * name (an extension with no table of its own); empty before the first
    * compile. Not one that opts out of persist, or whose served surface the
    * load-time checks flag: those are declined at query time.
    */
   servedThroughBaseSourceIDs(): ReadonlySet<string> {
      const compiled = this.compiledLanes.get(0);
      if (!compiled?.throughBase) return new Set();
      const ids = new Set(compiled.rerooted.flatMap((b) => b.sourceIDs ?? []));
      const names = new Set(compiled.rerooted.map((b) => b.sourceName));
      const declined = (id: string) =>
         !!compiled.optOutIDs?.has(id) || !!compiled.unsafeIDs?.has(id);
      return new Set(
         compiled.throughBase
            .filter(
               (e) =>
                  (e.chain.some((id) => ids.has(id)) ||
                     (e.baseName !== undefined && names.has(e.baseName))) &&
                  !declined(e.sourceID) &&
                  !e.chain.some(declined),
            )
            .map((e) => e.sourceID),
      );
   }

   /**
    * The author `sourceID`s the compiled copy re-roots, or serves through a
    * re-rooted base, whose row-level gate it cannot graft
    * ({@link gateCannotGraft}): their queries are declined and answered live.
    */
   gateDeclinedSourceIDs(): ReadonlySet<string> {
      const compiled = this.compiledLanes.get(0);
      const out = new Set<string>();
      if (!compiled || !this.mayHaveRowLevelGates(compiled)) return out;
      for (const b of compiled.rerooted) {
         if (this.gateCannotGraft(compiled, b.sourceName)) {
            for (const id of b.sourceIDs ?? []) out.add(id);
         }
      }
      for (const e of compiled.throughBase ?? []) {
         if (this.gateCannotGraft(compiled, e.sourceName)) out.add(e.sourceID);
      }
      return out;
   }

   /** Whether the author's model names a row-level gate at all; read once. */
   private mayHaveRowLevelGates(compiled: Compiled): boolean {
      compiled.hasRowLevelGates ??= JSON.stringify(
         this.host.modelDef() ?? {},
      ).includes("access_filter");
      return compiled.hasRowLevelGates;
   }

   /**
    * Whether the author's source `name` carries a row-level gate whose
    * declaring source the served copy cannot resolve: a re-root that replaced
    * the source that declared it (a stored source over a same-file gated
    * base). The governor grafts the gate on this compile and would deny every
    * caller; the served copy declines instead, and live answers under the
    * author's gate.
    */
   private gateCannotGraft(compiled: Compiled, name: string): boolean {
      const authorModel = this.host.modelDef() as ModelDef | undefined;
      const authorStruct = authorModel?.contents[name];
      const copyStruct = (compiled.modelDef as ModelDef | undefined)?.contents[
         name
      ];
      if (!authorStruct || !copyStruct) return false;
      const gated = collectEntryPointGates(
         authorStruct as never,
         authorModel as never,
      ).some((g) => g.route === "access_filter");
      return (
         gated &&
         !gateDeclaringSourceResolves(
            copyStruct as never,
            compiled.modelDef as never,
         )
      );
   }

   /** Why the last compile left sources unserved; empty before the first. */
   problems(): readonly ServedCopyProblem[] {
      return this.compiledLanes.get(0)?.problems ?? [];
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
      // A stored table past its freshness window is left out of the copy and
      // read live: counted per query, since a query succeeds either way and
      // this is how an operator sees the tier degrade.
      const usable = this.bindings.filter((b) => this.usable(b));
      const withheld = usable.length - this.freshBindings(Date.now()).length;
      if (withheld > 0 && !this.host.quiet) {
         staleBindingsWithheld().add(withheld);
      }
      return this.answer(await this.ensureCompiled(), queryString, givens);
   }

   /**
    * {@link loadQuery} in one compiled copy: lane 0's first, then, for a query
    * that reads another block of a multi-path pair table, that block's copy.
    */
   private async answer(
      compiled: Compiled,
      queryString: string,
      givens?: Record<string, unknown>,
   ): Promise<Awaited<ReturnType<ServedCopyServe["loadQuery"]>>> {
      if (!compiled.materializer || compiled.rerooted.length === 0) {
         throw new LegDecline(
            compiled.problems.some(
               (p) => p.reason === "cross_connection_unbuilt",
            )
               ? "served_copy_cross_connection"
               : "served_copy_not_served",
            undefined,
            {
               message:
                  compiled.problems[0]?.detail ??
                  "no stored table is re-rooted in this model's served copy",
            },
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
         const error = new LegDecline(code, callerDetail, { message: reason });
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
                  "served_copy_cross_connection",
                  LEFT_OUT_JOIN_DETAIL,
               );
            }
            throw error;
         }
      };
      const virtualMap = buildVirtualMap(compiled.aliased);
      await this.host.ensureSharedQueryModel?.(compiled.materializer);
      const runnable = compiled.materializer.loadRestrictedQuery(queryString);
      // The prepared query, read once, when a check below needs it. No check
      // mutates it.
      let preparedRead: ReturnType<typeof readPreparedQuery> | undefined;
      const prepared = () => (preparedRead ??= readPreparedQuery(runnable));
      // A row-level gate is grafted on this compile when the answer runs, and
      // the graft must find the source that declared it. A re-root that
      // replaced that source (a stored source over a same-file gated base)
      // leaves no declarer here, and the graft would deny every caller: the
      // query is declined instead, and the next candidate (live) answers it
      // under the author's gate.
      if (this.mayHaveRowLevelGates(compiled)) {
         const { structRef } = await compiling(prepared);
         const struct = resolveStructRefSource<{ as?: string; name?: string }>(
            structRef,
            compiled.modelDef as never,
         );
         const name = struct?.as ?? struct?.name;
         if (name !== undefined && this.gateCannotGraft(compiled, name)) {
            decline(
               "a row-level gate on the queried source was declared on a source the served copy re-roots, so it cannot be grafted here",
               "served_copy_not_served",
            );
         }
      }
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
         const { structRef } = await compiling(prepared);
         const struct = resolveStructRefSource<unknown>(
            structRef,
            compiled.modelDef as never,
         );
         const chain = SourceMap.of(compiled.modelDef).chainOf(struct);
         if (chain.some((id) => compiled.unsafeIDs?.has(id))) {
            decline(
               "the query reads a source whose served copy exposes a field the compile of record does not classify or carry",
               "served_copy_not_served",
            );
         }
         if (chain.some((id) => compiled.optOutIDs?.has(id))) {
            decline(
               "the query reads a source that opts out of persist",
               "served_copy_opt_out",
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
         sqlReadsTable(sql, boundTablePath(b)),
      );
      if (tablesRead.length === 0) {
         // The query reads no stored table: the served copy would only add
         // work over live. Let the next candidate answer.
         // A table on the queried source's chain that is not re-rooted was
         // left unserved, with a package warning saying why.
         decline(
            "the query reads no re-rooted table",
            "served_copy_not_served",
         );
      }
      // A table whose re-root leaves a stored column out answers a query
      // naming that column live (it does not compile here), but a wildcard
      // (`select: *`, in the query or a view it calls) expands without it:
      // the answer would lack a column live returns. Such a query is
      // answered here only when its result has the shape the author's
      // compile gives it.
      if (tablesRead.some((b) => compiled.leftOutTables?.has(b.tablePath))) {
         const live = await this.host
            .authorResultShape?.(queryString, givens)
            .catch(() => undefined);
         const served = resultShapeOf(
            await compiling(() =>
               runnable.getPreparedResult({
                  virtualMap,
                  givens: givens as never,
               }),
            ),
         );
         if (live === undefined || live !== served) {
            decline(
               "the query's result includes a column the served copy leaves out of a stored table's type",
               "served_copy_not_served",
            );
         }
      }
      // A join answered from stored columns is a record, so an aggregate at
      // its grain (`j.x.sum()`, `j.count()`, a measure that reads one) would
      // be computed per row of the source instead of per joined row. Malloy
      // marks each such aggregate where it needs a symmetric one; live's
      // join answers those.
      const inlined = new Set<string>();
      // Each join the copy answers from a record of stored columns (an
      // inlined join_one, or the inlined join_many of a pair table read
      // flat), with its fields that carry annotations.
      const annotated = new Map<string, ReadonlySet<string>>();
      for (const b of tablesRead) {
         for (const [join, fields] of compiled.inlinedJoins?.get(b.tablePath) ??
            []) {
            inlined.add(join);
            annotated.set(join, fields);
         }
      }
      const pairsRead = (compiled.pairs ?? []).filter((p) =>
         tablesRead.some((b) => b.tablePath === p.tablePath),
      );
      // A table of several inlined paths is read one block at a time: the
      // block of the one path the query reads, from that block's copy. A
      // query that reads two paths is answered live: live aggregates each of
      // the two joins by its own key over their cross product, which no
      // block holds.
      const multi = pairsRead.find((p) => p.blocks);
      if (multi?.blocks && pairsRead.length === 1) {
         const { structRef, query } = await prepared();
         const struct = resolveStructRefSource<{
            fields?: unknown;
            filterList?: unknown;
         }>(structRef, compiled.modelDef as never);
         const levels = multi.blocks.map((b) =>
            pairQueryReadLevel(query, struct, b),
         );
         if (levels.some((l) => l === undefined)) {
            decline(
               "the query could not be read to tell which inlined join_many it reads",
               "served_copy_pair_shape",
               PAIR_BLOCKS_DETAIL,
            );
         }
         const read = levels.flatMap((l, i) => ((l ?? 0) > 0 ? [i] : []));
         if (read.length > 1) {
            decline(
               "the query reads more than one inlined join_many of a pair table",
               "served_copy_pair_shape",
               PAIR_BLOCKS_DETAIL,
            );
         }
         if (read.length === 1 && read[0] !== compiled.lane) {
            return this.answer(
               await this.ensureCompiled(read[0]),
               queryString,
               givens,
            );
         }
      }
      for (const p of pairsRead) {
         annotated.set(p.pair.join, p.annotatedChildFields);
      }
      await checkRecordJoins({
         inlined,
         annotated,
         prepared,
         modelDef: compiled.modelDef,
         queryString,
         decline,
      });
      // A pair table (an inlined join_many) is read flat: exact only for the
      // aggregates pair_flat_query.ts proves, and only as the queried
      // source's own table, never through a join to it (its rows would fan
      // out the joining source).
      if (pairsRead.length > 0) {
         const { structRef, query } = await prepared();
         const struct = resolveStructRefSource<{
            fields?: unknown;
            filterList?: unknown;
         }>(structRef, compiled.modelDef as never);
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
               : await (async () => {
                    const gates = await pairGatesRead(
                       compiled,
                       struct,
                       own.pair,
                    );
                    return gates.fansOut
                       ? {
                            kind: "refused" as const,
                            reason:
                               "a row-level gate on the queried source reads through a join that fans out the pair table's rows",
                         }
                       : flatPairQueryVerdict(
                            query,
                            struct,
                            own.pair,
                            gates.level,
                         );
                 })();
         if (verdict.kind !== "served") {
            // A query that reads no child field goes to the next candidate
            // (another stored table, or live) even when the flat rows would
            // answer it exactly: they hold each parent once per child.
            if (verdict.kind === "parent_only") {
               decline(
                  "the query reads nothing through the inlined join_many, so its pair table is not read for it",
                  "served_copy_pair_parent_only",
                  "The table stores this source once per child of its inlined join_many and the query reads no child field, so it is answered without this table, which would read every child row to count each parent once.",
               );
            } else {
               decline(
                  `the pair table cannot answer this query exactly: ${verdict.reason}`,
                  "served_copy_pair_shape",
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
      if ((computed || windowed) && setsTimezone((await prepared()).query)) {
         decline(
            computed
               ? "the query sets a timezone over a table built in the default zone (a stored computed dimension or a row filter that depends on it)"
               : "the query sets a timezone, which moves a rolling window's cut-off past what its table stores",
            "served_copy_time_zone",
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
            // A block's copy of a multi-path pair table is a model of its
            // own (the same bindings, another block's rows).
            cacheScope: `served_copy\u0000${compiled.key}\u0000${compiled.lane}`,
            runtime: compiled.runtime,
         },
      };
   }

   /**
    * What the join-to-rollup leg (A7) builds on to answer a query in a
    * storage destination (known limitation 26): the copy's re-rooted files
    * and connections, over which it compiles the query's source with one
    * join re-pointed at a joined source's rollup in the same destination,
    * and the checks this copy runs on a query it answers, for the tables
    * that variant reads. Undefined when the copy re-roots nothing.
    */
   async joinRollupBasis(): Promise<ServedCopyJoinBasis | undefined> {
      const compiled = await this.ensureCompiled();
      const { materializer, files, config } = compiled;
      if (
         !materializer ||
         !files ||
         !config ||
         compiled.rerooted.length === 0
      ) {
         return undefined;
      }
      return {
         key: `${compiled.generation}\u0000${compiled.key}`,
         files,
         config,
         modelDef: compiled.modelDef,
         rerooted: compiled.rerooted,
         aliased: compiled.aliased,
         leftOut: compiled.leftOut ?? [],
         check: (args) => this.checkJoinVariant(compiled, args),
      };
   }

   /**
    * The served copy's own checks on a query a join-to-rollup variant
    * compiled over it answers (see {@link joinRollupBasis}): the queried
    * source must not opt out of persist or be one whose served surface the
    * load-time classification checks flag; a pair table is never read
    * through it (its rows fan out per child, and the join would read the
    * rollup once per pair row); a join the copy answers from stored columns
    * keeps its grain and annotation rules. The copy's time-zone rule needs
    * no check here: the join leg declines every query that sets a
    * `timezone:` (`TIME_ZONE`) before it compiles one. Throws a
    * {@link LegDecline}.
    */
   private async checkJoinVariant(
      compiled: Compiled,
      args: {
         sourceID: string;
         runnable: QueryMaterializer;
         queryString: string;
         tablesRead: readonly ServeBinding[];
      },
   ): Promise<void> {
      const decline = (
         reason: string,
         code: ServedCopyDeclineCode,
         callerDetail?: string,
      ): never => {
         throw new LegDecline(code, callerDetail, { message: reason });
      };
      const map = SourceMap.of(compiled.modelDef);
      const node = map.node(args.sourceID);
      const chain = node ? map.chainOf(node.def) : [args.sourceID];
      if (chain.some((id) => compiled.unsafeIDs?.has(id))) {
         decline(
            "the query reads a source whose served copy exposes a field the compile of record does not classify or carry",
            "served_copy_not_served",
         );
      }
      if (chain.some((id) => compiled.optOutIDs?.has(id))) {
         decline(
            "the query reads a source that opts out of persist",
            "served_copy_opt_out",
         );
      }
      const { tablesRead } = args;
      if (
         (compiled.pairs ?? []).some((p) =>
            tablesRead.some((b) => b.tablePath === p.tablePath),
         )
      ) {
         decline(
            "the query reads a pair table, which holds its source once per child of its inlined join_many",
            "served_copy_pair_shape",
         );
      }
      let preparedRead: ReturnType<typeof readPreparedQuery> | undefined;
      const prepared = () =>
         (preparedRead ??= readPreparedQuery(args.runnable));
      const inlined = new Set<string>();
      const annotated = new Map<string, ReadonlySet<string>>();
      for (const b of tablesRead) {
         for (const [join, fields] of compiled.inlinedJoins?.get(b.tablePath) ??
            []) {
            inlined.add(join);
            annotated.set(join, fields);
         }
      }
      await checkRecordJoins({
         inlined,
         annotated,
         prepared,
         modelDef: (await prepared()).modelDef ?? compiled.modelDef,
         queryString: args.queryString,
         decline,
      });
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
            this.usable(b) &&
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

   /**
    * The compiled copy for `lane`: every pair table that stores several
    * inlined paths is read for its block of that number (its last, past its
    * own count), the rest of the copy as at lane 0. Lane 0 is the copy a
    * query is first compiled in; another is compiled only when a query reads
    * that block, and kept like lane 0 until the next invalidation.
    */
   private async ensureCompiled(lane = 0): Promise<Compiled> {
      const bindings = this.freshBindings(Date.now());
      const key = [
         ...bindings
            .map(
               (b) =>
                  `${b.sourceName}@${b.colocated ? "colocated:" : ""}${b.destinationName}/${b.virtualHandle}/${b.tablePath}/${b.freshnessFallback ?? ""}`,
            )
            .sort(),
      ].join("|");
      const generation = this.generation;
      const current = this.compiledLanes.get(lane);
      if (current && current.key === key && current.generation === generation) {
         return current;
      }
      let inFlight = this.inFlightLanes.get(lane);
      if (
         !inFlight ||
         inFlight.key !== key ||
         inFlight.generation !== generation
      ) {
         const started = {
            key,
            generation,
            promise: this.compile(key, generation, bindings, lane),
         };
         this.inFlightLanes.set(lane, started);
         void started.promise.finally(() => {
            if (this.inFlightLanes.get(lane) === started) {
               this.inFlightLanes.delete(lane);
            }
         });
         inFlight = started;
      }
      const compiled = await inFlight.promise;
      // An invalidation while it compiled makes it stale: keep it only when
      // it is still the current generation's.
      if (compiled.generation === this.generation) {
         this.compiledLanes.set(lane, compiled);
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
      lane = 0,
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
         problems.push({
            sourceName,
            sourceID: node.sourceID,
            reason: "no_declaration",
         });
         return undefined;
      }
      // A table an older build typed reads through a cast in today's serve
      // shape; the served copy does not carry casts yet, so it leaves the
      // source to that shape (or live).
      if (binding.readCasts?.length) {
         problems.push({
            sourceName,
            sourceID: node.sourceID,
            reason: "column_types",
         });
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
      let columnsLeftOut: string[] = [];
      if (plan) {
         // Exactly what the build stored: the plan's columns (the source's
         // atomic dimensions, computed ones too, each under its own name, a
         // hidden one with its access modifier), typed as stored, with each
         // field's annotations.
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
               problems.push({
                  sourceName,
                  sourceID: node.sourceID,
                  reason: "column_types",
               });
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
               ...(c.access ? { access: c.access } : {}),
            });
         }
         const joined = inlinedJoinsOf(plan, def.fields, stored, lineage);
         if (
            joined === undefined ||
            joined.some((j) =>
               j.columns.some((c) => storedMalloyType(c.type) === undefined),
            )
         ) {
            problems.push({
               sourceName,
               sourceID: node.sourceID,
               reason: "column_types",
            });
            return undefined;
         }
         // A joined big-integer field is read through its native type in the
         // record; one no cast reads exactly leaves the source to its
         // fallback (known limitation 12).
         const joinedReads = joined.map((j) => {
            const fields = joinedFieldsAt(def.fields, [j.join]);
            const reads = new Map<string, string | undefined>();
            for (const c of j.columns) {
               const read = bigIntegerReadOf(
                  fields.find((f) => fieldNameOf(f) === c.name),
                  { name: c.column, type: c.type },
                  bigIntegerOptionsOf(def, binding),
               );
               if (read) reads.set(c.name, read.castTo);
            }
            return reads;
         });
         if (joinedReads.some((reads) => [...reads.values()].some((r) => !r))) {
            problems.push({
               sourceName,
               sourceID: node.sourceID,
               reason: "column_types",
            });
            return undefined;
         }
         inlinedJoins = joined.map((j, i) => ({
            ...j,
            columns: j.columns.map((c) => {
               const nativeCast = joinedReads[i].get(c.name);
               return nativeCast ? { ...c, nativeCast } : c;
            }),
         }));
      } else {
         // A v1 `storage=` table holds public columns only. A v1 colocated
         // one holds what Malloy built, hidden columns too (G2): each is
         // kept with its access modifier, written again on the stored table
         // (P3), so a measure over it compiles and the column stays hidden.
         // The cutover's per-column handling (G5, review F3): a colocated
         // table's columns are every column it holds, with no access
         // modifier: the author's own text after the query (kept whole,
         // `keepsIncludes`) renames, drops and re-hides them.
         const named = binding.colocated
            ? storedColumnsWithAccess(binding.schema, def.fields).map(
                 ({ name, type }) => ({ name, type }),
              )
            : narrowSchemaToPublic(binding.schema, def.fields);
         // A stored column no field of the source names is one its own
         // `extend` block drops (`except:`, `accept:`) or an `include`
         // re-hides under another name: it stays on the stored table so the
         // author's text compiles and removes it again (review F3). Should
         // the text not remove it, the access check below finds a public
         // field the author's source lacks and leaves the source as written.
         const narrowed = [
            ...named,
            ...unnamedStoredColumns(binding.schema, def.fields),
         ];
         const mismatches = storedColumnTypeMismatches(
            narrowed,
            declaredColumnsOf(def.fields),
         );
         // A column the served copy cannot read as its declared type: cast
         // on read when a cast is faithful, else left out so only a query
         // reading it answers live (G5, the serve shape's handling, which
         // retired plan call 4).
         const typePlan =
            mismatches.length > 0
               ? serveTypePlan(mismatches, {
                    queryTimezone: queryTimezoneOf(def),
                 })
               : undefined;
         const castTo = new Map(
            (typePlan?.casts ?? []).map((c) => [
               c.column,
               c.castTo === "BOOLEAN"
                  ? ("boolean" as const)
                  : ("date" as const),
            ]),
         );
         const leftOut = new Set(
            (typePlan?.unservable ?? []).map((m) => m.column),
         );
         columnsLeftOut = [...leftOut];
         // A nest of the persisted query a destination did not store (a lake
         // that cannot hold a repeated record): left out like a column no
         // type reads, so only a query through it answers live, as the serve
         // shape kept the traversal local.
         const stored = new Set(
            binding.schema.map((c) => c.name.toLowerCase()),
         );
         for (const f of (def.fields ?? []) as {
            name?: unknown;
            as?: unknown;
            join?: unknown;
            type?: unknown;
         }[]) {
            const name = String(f.as ?? f.name ?? "");
            // A nest is a repeated (or single) record; a join the source
            // declares (`join_one: c is …`) is never a stored column, and
            // its re-root keeps it as written.
            if (
               typeof f.join === "string" &&
               (f.type === "array" || f.type === "record") &&
               name !== "" &&
               !stored.has(name.toLowerCase())
            ) {
               columnsLeftOut.push(name);
            }
         }
         // Each with its field's annotations (`#(doc)`, `# label`, `#
         // percent` on the query's output field), as a `#(materialize)` column
         // carries them, so the stored field reads back as the author's.
         columns = narrowed
            .filter((c) => !leftOut.has(c.name))
            .map((c) => {
               const notes = fieldNotes(fieldsByName.get(c.name));
               const cast = castTo.get(c.name);
               const field = fieldsByName.get(c.name) as
                  | { type?: unknown }
                  | undefined;
               return {
                  ...c,
                  ...(notes.length > 0 ? { notes } : {}),
                  ...timeframeOf(c.name),
                  ...(cast ? { castTo: cast } : {}),
                  ...(field?.type === "timestamptz" &&
                  /^(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ)$/i.test(
                     c.type.trim(),
                  )
                     ? { timestamptz: true as const }
                     : {}),
               };
            });
         if (columns.length === 0) {
            problems.push({
               sourceName,
               sourceID: node.sourceID,
               reason: "column_types",
            });
            return undefined;
         }
      }
      // A column live answers above 2^53 (a BIGINT, known limitation 12) is
      // read through its native type, which keeps every digit; one no cast
      // reads exactly is left out, so only a query reading it answers live.
      const ownReads = bigIntegerReadsOf(columns, def.fields, def, binding);
      columns = columns.flatMap((c) => {
         if (!ownReads.has(c.name)) return [c];
         const nativeCast = ownReads.get(c.name);
         if (nativeCast) return [{ ...c, nativeCast }];
         columnsLeftOut.push(c.name);
         return [];
      });
      if (columns.length === 0) {
         problems.push({
            sourceName,
            sourceID: node.sourceID,
            reason: "column_types",
         });
         return undefined;
      }
      // A column the `type:` cannot declare faithfully would fail the whole
      // model's copy, not just this source. For a v1 table it is left out
      // (a nest a destination stores as a type Malloy cannot read back,
      // say), so only a query that reads it answers live, as the serve
      // shape kept it local; otherwise the source is left to its fallback.
      if (!plan) {
         const undeclarable = columns
            .filter((c) => storedMalloyType(c.type) === undefined)
            .map((c) => c.name);
         if (undeclarable.length > 0 && undeclarable.length < columns.length) {
            columns = columns.filter((c) => !undeclarable.includes(c.name));
            columnsLeftOut = [...columnsLeftOut, ...undeclarable];
         }
      }
      if (columns.some((c) => storedMalloyType(c.type) === undefined)) {
         problems.push({
            sourceName,
            sourceID: node.sourceID,
            reason: "column_types",
         });
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
            readSourceClassifications(def.fields as IrField[] | undefined),
            lane,
            (field, column) =>
               bigIntegerReadOf(
                  field,
                  column,
                  bigIntegerOptionsOf(def, binding),
               ),
         );
         if (!pair) {
            problems.push({
               sourceName,
               sourceID: node.sourceID,
               reason: "column_types",
            });
            return undefined;
         }
      }
      return {
         url,
         range,
         sourceName,
         ...(pair ? { pair } : {}),
         connectionAlias: servedConnectionOf(binding),
         virtualHandle: binding.virtualHandle,
         typeName: `${sourceName}__served`,
         columns,
         reservedNames,
         ...(plan
            ? {
                 storedDimensions: plan.columns
                    .filter(
                       (c) =>
                          c.computed &&
                          c.join === undefined &&
                          !columnsLeftOut.includes(c.dimension),
                    )
                    .map((c) => c.dimension),
              }
            : {}),
         ...(plan &&
         (!plan.zoneFreeRows ||
            plan.columns.some(
               (c) => c.computed && c.join === undefined && !c.zoneFree,
            ))
            ? { storesZonedDimension: true }
            : {}),
         ...(plan?.readTimeTerms?.length
            ? { readTimeTerms: plan.readTimeTerms }
            : {}),
         ...(inlinedJoins.length > 0 ? { inlinedJoins } : {}),
         ...(columnsLeftOut.length > 0 ? { columnsLeftOut } : {}),
         ...(!plan
            ? {
                 keepsIncludes: true as const,
                 dropsUnstoredExcepts: true as const,
                 ...(refinesUnfilteredBase(map.node(node.sourceID), map)
                    ? { refinementRooted: true as const }
                    : {}),
              }
            : {}),
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

   /**
    * The trailing `extend` runs of a `#(materialize)` source's ancestors in
    * its own file, root first ({@link keptAncestorChain}, confirmed against
    * the file's text), which its re-root keeps ahead of its own blocks. Kept
    * for an extension table (P10), whose table holds every one of their rows
    * and stored columns, so it loses no measure, join or view any of them
    * declares; and for a source whose plan needs its bases' blocks
    * (`keptBases`): a given-reading `where:` or a caller-dependent dimension a
    * base declares, which the build left out and only the base's block
    * re-applies at read. Undefined when the source is neither, or no such
    * ancestor is on the walk: a source with none keeps only its own blocks
    * (decision 2); and for one whose bases declare a measure, view or join it
    * inherits (G8), which the re-root would otherwise lose.
    */
   private keptBlocksFor(
      reroot: Reroot & { sourceID: string },
      map: SourceMap,
      fileText: (url: string) => string | undefined,
   ): string | undefined {
      const plan = this.materializePlans[reroot.sourceID];
      if (!plan) return undefined;
      const text = fileText(reroot.url);
      if (text === undefined) return undefined;
      const confirmed = confirmedAncestors(
         text,
         reroot.range,
         keptAncestorChain(map, reroot.sourceID, reroot.url),
      );
      const needed =
         (plan.keptBases?.length ?? 0) > 0 ||
         confirmed.some(
            (a) => this.materializePlans[a.sourceID] !== undefined,
         ) ||
         // G8: a base the re-root replaces declares a measure, view or join
         // the source inherits; only its block, written again over the
         // table, keeps it (else the source is not carried, call 2).
         confirmed.some((a) => declaresBlocks(map.node(a.sourceID)?.def));
      if (!needed) return undefined;
      return confirmed
         .flatMap((a) =>
            a.run ? [text.slice(a.run.start, a.run.end).trimEnd()] : [],
         )
         .reverse()
         .join(" ");
   }

   private async compile(
      key: string,
      generation: number,
      bindings: ServeBinding[],
      lane = 0,
   ): Promise<Compiled> {
      const problems: ServedCopyProblem[] = [];
      // Storage bindings reach here only with destinations (`usable`); a
      // copy of colocated tables alone needs none.
      const destinations =
         this.host.serveDestinationConfig() ?? NO_DESTINATIONS;
      if (!this.packagePath || !this.packageConfig) {
         return { key, generation, lane, rerooted: [], aliased: [], problems };
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
         const reroot = this.rerootFor(
            binding,
            map,
            problems,
            reservedNames,
            lane,
         );
         if (reroot) {
            candidates.push({
               binding,
               reroot: reroot as AuthorReroot,
            });
         }
      }
      // An extension of a re-rooted source reads it by name already, and
      // re-rooting the extension too would replace the base it inherits its
      // parent's fields from. Leave it as written, unless it is an extension
      // table (P10): a `#(materialize)` source with its own table, whose
      // re-root keeps its ancestors' blocks in its file (`keptBlocks`), so it
      // loses nothing it inherits from them.
      // The chain comes from the source map; the declaration's own text,
      // `<name> is <base> extend …`, answers too, for a chain the compiled
      // model does not record.
      const rerootedIDs = new Set(candidates.map((c) => c.reroot.sourceID));
      const rerootedNames = new Set(candidates.map((c) => c.reroot.sourceName));
      const throughBase: NonNullable<Compiled["throughBase"]>[number][] = [];
      // Only through refinements (`extend`, rename): a query-shaped source
      // (`child is base -> { … }`) starts rows of its own, which its own table
      // holds, so it is re-rooted on that table even when its base is too, as
      // the serve shape read a chained child's own table.
      const refinesRerootedSource = (sourceID: string): boolean => {
         let node = map.node(sourceID);
         for (let hops = 0; node && hops < 64; hops++) {
            const base = node.base;
            if (!base || base.hop === "query" || base.sourceID === undefined) {
               return false;
            }
            if (rerootedIDs.has(base.sourceID)) return true;
            node = map.node(base.sourceID);
         }
         return false;
      };
      const readsRerootedBase = (c: (typeof candidates)[number]) => {
         const node = map.node(c.reroot.sourceID);
         const chain = node ? map.chainOf(node.def) : [];
         const text = lift.fileText(c.reroot.url);
         const base =
            text === undefined
               ? undefined
               : declaredBaseName(text, c.reroot.range);
         const reads =
            refinesRerootedSource(c.reroot.sourceID) ||
            (base !== undefined && rerootedNames.has(base));
         if (reads) {
            throughBase.push({
               sourceID: c.reroot.sourceID,
               sourceName: c.reroot.sourceName,
               chain: chain.slice(1),
               ...(base !== undefined ? { baseName: base } : {}),
            });
         }
         return reads;
      };
      // An extension table that does not compile over its kept blocks falls
      // back to what it was before P10: left as written when it reads a
      // re-rooted base, else re-rooted with its own blocks only.
      const withoutKept = new Map<
         (typeof candidates)[number],
         (typeof candidates)[number] | undefined
      >();
      candidates = candidates.flatMap((c) => {
         const kept = this.keptBlocksFor(c.reroot, map, (u) =>
            lift.fileText(u),
         );
         const reads = readsRerootedBase(c);
         if (kept === undefined) return reads ? [] : [c];
         const extension = { ...c, reroot: { ...c.reroot, keptBlocks: kept } };
         withoutKept.set(extension, reads ? undefined : c);
         return [extension];
      });
      // Inlined joins kept as written because the copy did not compile with
      // their records (see keepInlinedJoins, below), and pair tables dropped
      // because it did not compile with them: each reported only once it
      // compiles without them (when it still does not, they were not the
      // cause).
      let inlinedKept: ServedCopyProblem[] = [];
      let pairsDropped: ServedCopyProblem[] = [];
      // At most three passes per candidate: each pass either compiles with
      // every remaining source whole, or takes one step down a source's
      // ladder (its inlined joins kept as written, its kept blocks dropped,
      // the source dropped: see `contain`), or drops the sources it lost
      // fields on; and one more each that keeps every inlined join as written
      // and drops the pair tables.
      const maxPasses = 3 * candidates.length + 3;
      for (let pass = 0; pass <= maxPasses; pass++) {
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
            return {
               key,
               generation,
               lane,
               rerooted: [],
               aliased: [],
               problems,
            };
         }
         const rerooted = candidates.map((c) => c.binding);
         const aliased = rerooted.map((b) => ({
            ...b,
            destinationName: servedConnectionOf(b),
         }));
         const overlay = new Map<string, string>();
         for (const [url, text] of copy.files) {
            overlay.set(overlayKey(url), text);
         }
         const packagePath = this.packagePath;
         const config = servedCopyConfig(
            this.packageConfig,
            destinations(),
            new Set(
               rerooted
                  .filter((b) => !b.colocated)
                  .map((b) => b.destinationName),
            ),
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
         let files: ReadonlyMap<string, string> = overlay;
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
            // Added to, not replaced: a join containment already kept as
            // written for its own source (see `contain`) stays reported.
            const reported = new Set(
               inlinedKept.map((p) => `${p.sourceName}\u0000${p.join}`),
            );
            inlinedKept = [
               ...inlinedKept,
               ...candidates.flatMap((c) =>
                  (c.reroot.inlinedJoins ?? [])
                     .filter(
                        (j) =>
                           !reported.has(
                              `${c.reroot.sourceName}\u0000${j.join}`,
                           ),
                     )
                     .map((j) => ({
                        sourceName: c.reroot.sourceName,
                        reason: "inline_join_kept" as const,
                        join: j.join,
                        detail,
                     })),
               ),
            ];
            candidates = candidates.map((c) => {
               if (!c.reroot.inlinedJoins) return c;
               const { inlinedJoins: _kept, ...reroot } = c.reroot;
               return { ...c, reroot };
            });
            return true;
         };
         // A column left out of its source's `type:` (a v1 column no cast
         // makes faithful, G5; a big-integer column no native cast reads
         // exactly, known limitation 12). When the model's own text names it
         // (a dimension or measure over it, an extension's dimension), the
         // copy cannot compile with it gone: that source is left as written
         // and answers live, as the serve shape withholds a binding whose
         // unservable column a refinement names, and every other source
         // keeps its table.
         const withoutLeftOutColumn = (error: unknown): boolean => {
            const message =
               error instanceof Error ? error.message : String(error);
            const named = new Set(
               [
                  ...message.matchAll(
                     /'([^']+)' is not defined|Reference to undefined (?:value|object) `?([^`\s]+)/g,
                  ),
               ].map((m) => m[1] ?? m[2]),
            );
            if (named.size === 0) return false;
            const naming = candidates.filter((c) =>
               (c.reroot.columnsLeftOut ?? []).some((n) => named.has(n)),
            );
            if (naming.length === 0) return false;
            // The re-root the failure is placed in, when it is one of them:
            // another source that leaves out a column of the same name keeps
            // its table. A failure placed elsewhere (an extension left as
            // written) costs every source that leaves the name out.
            const traced = new Set(
               failingReroots(error, copy.files, copy.spans, "precise").map(
                  (f) => rerootKey(f.url, f.sourceName),
               ),
            );
            const placed = naming.filter((c) =>
               traced.has(rerootKey(c.reroot.url, c.reroot.sourceName)),
            );
            const failing = placed.length > 0 ? placed : naming;
            for (const c of failing) {
               problems.push({
                  sourceName: c.reroot.sourceName,
                  sourceID: c.reroot.sourceID,
                  reason: "column_types",
               });
               withoutKept.delete(c);
            }
            candidates = candidates.filter((c) => !failing.includes(c));
            return true;
         };
         // Containment (served_copy_containment.ts): a failure traced to one
         // re-root's own text costs that source alone. Its ladder: a source
         // that inlines join_ones keeps them as written first (the record is
         // the one text the copy writes over a join's place, as
         // keepInlinedJoins below, but for this source only); an extension
         // table falls back to its own blocks (or to as written); any other
         // source is dropped and answers live, with a coded warning. Each
         // step is one pass; the rest of the model compiles again.
         const contain = (
            error: unknown,
            files: ReadonlyMap<string, string>,
            mode: TraceMode,
         ): boolean => {
            const failing = new Map(
               failingReroots(error, files, copy.spans, mode).map((f) => [
                  rerootKey(f.url, f.sourceName),
                  f,
               ]),
            );
            if (failing.size === 0) return false;
            const detail =
               error instanceof Error ? error.message : String(error);
            let changed = false;
            candidates = candidates.flatMap((c) => {
               const f = failing.get(
                  rerootKey(c.reroot.url, c.reroot.sourceName),
               );
               if (!f) return [c];
               changed = true;
               logger[this.host.quiet ? "debug" : "warn"](
                  "A re-root does not compile in the served copy; that source alone steps back",
                  {
                     modelPath: this.host.modelPath(),
                     sourceName: c.reroot.sourceName,
                     problem: f.message,
                  },
               );
               if (c.reroot.inlinedJoins?.length) {
                  inlinedKept.push(
                     ...c.reroot.inlinedJoins.map((j) => ({
                        sourceName: c.reroot.sourceName,
                        reason: "inline_join_kept" as const,
                        join: j.join,
                        detail,
                     })),
                  );
                  const { inlinedJoins: _kept, ...reroot } = c.reroot;
                  const next = { ...c, reroot };
                  if (withoutKept.has(c)) {
                     withoutKept.set(next, withoutKept.get(c));
                     withoutKept.delete(c);
                  }
                  return [next];
               }
               problems.push({
                  sourceName: c.reroot.sourceName,
                  sourceID: c.reroot.sourceID,
                  reason: c.reroot.pair
                     ? "pair_not_compiled"
                     : "reroot_not_compiled",
                  detail,
               });
               if (withoutKept.has(c)) {
                  const fallback = withoutKept.get(c);
                  withoutKept.delete(c);
                  return fallback ? [fallback] : [];
               }
               return [];
            });
            return changed;
         };
         try {
            ({
               materializer,
               def: servedDef,
               runtime: servedRuntime,
            } = await load(overlay));
         } catch (err) {
            if (withoutLeftOutColumn(err)) continue;
            if (contain(err, copy.files, "precise")) continue;
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
               if (withoutKept.size > 0) {
                  // Try again without the extension tables' own re-roots,
                  // reporting each, before giving up the whole copy.
                  for (const c of withoutKept.keys()) {
                     problems.push({
                        sourceName: c.reroot.sourceName,
                        reason: "compile_failed",
                        detail,
                     });
                  }
                  candidates = candidates.flatMap((c) => {
                     if (!withoutKept.has(c)) return [c];
                     const fallback = withoutKept.get(c);
                     return fallback ? [fallback] : [];
                  });
                  withoutKept.clear();
                  continue;
               }
               if (contain(err, copy.files, "last_resort")) continue;
               problems.push({
                  reason: classifyServedCopyFailure(err),
                  detail,
               });
               return {
                  key,
                  generation,
                  lane,
                  rerooted: [],
                  aliased: [],
                  problems,
               };
            }
            try {
               files = keyedOverlay(kept.overlay);
               ({
                  materializer,
                  def: servedDef,
                  runtime: servedRuntime,
               } = await load(files));
            } catch (again) {
               if (contain(again, kept.overlay, "precise")) continue;
               if (keepInlinedJoins(again)) continue;
               const detail =
                  again instanceof Error ? again.message : String(again);
               if (withoutPairs(detail)) continue;
               if (contain(again, kept.overlay, "last_resort")) continue;
               problems.push({ reason: "compile_failed", detail });
               return {
                  key,
                  generation,
                  lane,
                  rerooted: [],
                  aliased: [],
                  problems,
               };
            }
            leftOut = kept.leftOut;
         }
         // Only a join of a source still re-rooted was kept for it.
         problems.push(
            ...inlinedKept.filter((p) =>
               candidates.some((c) => c.reroot.sourceName === p.sourceName),
            ),
         );
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
            const excused = new Set([
               ...leftOut
                  .filter((l) => chain.has(l.declaration ?? l.name))
                  .map((l) => l.name),
               ...(c.reroot.columnsLeftOut ?? []),
            ]);
            // A private field the table does not hold (one private on a
            // source this one extends, which the build never selects, P3)
            // cannot be read by the source, its extensions or a caller, so
            // its absence changes no answer: a served text that read it
            // would not compile.
            const unreadable = privateFieldNames(c.reroot.authorDef);
            return [...author].some(
               (name) =>
                  !served.has(name) &&
                  !excused.has(name) &&
                  !unreadable.has(name),
            );
         });
         // Fail closed on access (P3): every field the re-rooted source has
         // must be exactly as hidden as on the author's, so a stored private
         // or internal column never becomes selectable. One that is not is
         // not carried, and its source is left as written.
         const exposed = candidates.filter(
            (c) =>
               !lost.includes(c) &&
               !keepsAccess(
                  c.reroot.authorDef,
                  // A source the served compile holds only as a join
                  // (#6643) is read from that join, as the given-term
                  // check below reads it.
                  servedDefOf(servedMap, c.reroot.sourceID),
               ),
         );
         lost.push(...exposed);
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
         // The same for a rolling window the re-root leaves to the kept text
         // (one over a hidden dimension computed at read): it must be in the
         // served source's compiled filters as the author wrote it.
         const keptAsWritten = (c: (typeof candidates)[number]) =>
            (copy.spans.get(c.reroot.url) ?? []).find(
               (s) => s.sourceName === c.reroot.sourceName,
            )?.termsKeptAsWritten ?? [];
         const unscoped = candidates.filter(
            (c) =>
               !lost.includes(c) &&
               (!keepsGivenTerms(
                  c.reroot.authorDef,
                  servedDefOf(servedMap, c.reroot.sourceID),
               ) ||
                  !keepsTermsAsWritten(
                     servedDefOf(servedMap, c.reroot.sourceID),
                     keptAsWritten(c),
                  ) ||
                  (!hasOwnNode(servedMap, c.reroot.sourceID) &&
                     appliesFilters(c.reroot.authorDef))),
         );
         for (const c of [...lost, ...unscoped]) withoutKept.delete(c);
         if (lost.length > 0 || unscoped.length > 0) {
            for (const c of lost) {
               problems.push({
                  sourceName: c.reroot.sourceName,
                  sourceID: c.reroot.sourceID,
                  reason: "fields_not_carried",
               });
            }
            for (const c of unscoped) {
               problems.push({
                  sourceName: c.reroot.sourceName,
                  sourceID: c.reroot.sourceID,
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
            lane,
            // An inlined join's columns are the joined source's fields as the
            // build computed them, in its time zone. A stored computed
            // dimension of the source's own is too, unless the probe compiler
            // found it the same in every zone (P9): one that is not was left
            // out of the build when the source declares it, and is computed
            // at read in the query's zone.
            computedTables: new Set(
               candidates
                  .filter(
                     (c) =>
                        c.reroot.storesZonedDimension === true ||
                        (c.reroot.inlinedJoins ?? []).length > 0 ||
                        (c.reroot.pair?.childStoredDimensions ?? []).length > 0,
                  )
                  .map((c) => c.binding.tablePath),
            ),
            leftOutTables: new Set(
               candidates
                  .filter((c) => (c.reroot.columnsLeftOut ?? []).length > 0)
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
            ...(throughBase.length > 0 ? { throughBase } : {}),
            aliased,
            problems,
            pairs: candidates.flatMap((c) =>
               c.reroot.pair
                  ? [
                       {
                          sourceID: c.reroot.sourceID,
                          tablePath: c.binding.tablePath,
                          pair: flatPairOf(c.reroot.pair),
                          ...(c.reroot.pair.block
                             ? {
                                  blocks: blockPaths(
                                     this.materializePlans[c.reroot.sourceID],
                                  ),
                               }
                             : {}),
                          annotatedChildFields: new Set(
                             c.reroot.pair.annotatedFields,
                          ),
                       },
                    ]
                  : [],
            ),
            ...(leftOut.length > 0 ? { leftOut } : {}),
            files,
            config,
         };
      }
      return { key, generation, lane, rerooted: [], aliased: [], problems };
   }
}

/**
 * The checks for a join the copy answers from a record of stored columns (an
 * inlined join_one, or a pair table's join_many read flat), on one query
 * compiled over the copy: an aggregate at such a join's grain is declined
 * (`count_semantics`), and so is a query that returns one of its annotated
 * fields as a column (`joined_annotations`). `inlined` are the inlined
 * join_ones the query's tables answer from records, `annotated` each record
 * join's annotated fields; `modelDef` resolves the query's source.
 */
async function checkRecordJoins(args: {
   inlined: ReadonlySet<string>;
   annotated: ReadonlyMap<string, ReadonlySet<string>>;
   prepared: () => ReturnType<typeof readPreparedQuery>;
   modelDef: unknown;
   queryString: string;
   decline: (
      reason: string,
      code: ServedCopyDeclineCode,
      callerDetail?: string,
   ) => never;
}): Promise<void> {
   const { inlined, annotated, prepared, decline } = args;
   if (inlined.size > 0) {
      const grain = joinGrainAggregatePaths(await prepared());
      if (
         grain.unreadable ||
         grain.paths.some((p) => p.some((name) => inlined.has(name))) ||
         readsJoinStar(args.queryString, inlined)
      ) {
         decline(
            "the query aggregates at the grain of a join the served copy answers from stored columns",
            "served_copy_count_semantics",
            JOIN_GRAIN_DETAIL,
         );
      }
   }
   // A record literal's field carries no annotation onto a result, so a
   // query that returns an annotated joined field as a column (its
   // `#(doc)`, `#(classification)`, render tags) is answered live, and
   // every other query keeps the table: one check for both legs, before
   // the pair table's own.
   if ([...annotated.values()].some((f) => f.size > 0)) {
      const { structRef, pipeline } = await prepared();
      const struct = resolveStructRefSource<{ fields?: unknown }>(
         structRef,
         args.modelDef as never,
      );
      const out = annotatedJoinedOutput(pipeline, struct?.fields, annotated);
      if (out.unreadable || out.output) {
         decline(
            out.output
               ? `the query returns ${out.output.path.join(".")}, whose annotations the served copy's record of stored columns cannot carry`
               : "the query's output columns cannot be read",
            "served_copy_joined_annotations",
            JOINED_ANNOTATIONS_DETAIL,
         );
      }
   }
}

/**
 * What the row-level gates (`#(access_filter)`) on the queried entry point
 * read of a pair table: the deepest level of its inlined path, and whether
 * one reads through a join that fans out its rows (another join_many, or
 * another block's record). The governor grafts the gate on the served copy
 * after the router chose it, so the pair check never sees it in the query: a
 * gate that filters on a child field would drop a parent's flagged row while
 * its other rows stay, and its parent measures would undercount; one that
 * reads another join_many joins it into every pair row, which no aggregate
 * over the flat rows survives. Each gate is compiled as the governor's own
 * probe and read from its IR; one that does not compile or cannot be read
 * counts as reading through such a join.
 */
async function pairGatesRead(
   compiled: Compiled,
   struct: unknown,
   pair: FlatPair,
): Promise<{ level: number; fansOut: boolean }> {
   const gates = collectEntryPointGates(
      struct as never,
      compiled.modelDef as never,
   ).filter((g) => g.route === "access_filter");
   if (gates.length === 0) return { level: 0, fansOut: false };
   const materializer = compiled.materializer;
   const unreadable = { level: 0, fansOut: true };
   if (!materializer) return unreadable;
   const reads = async (): Promise<{ level: number; fansOut: boolean }> => {
      let level = 0;
      for (const gate of gates) {
         const target = gate.struct?.as ?? gate.struct?.name;
         if (typeof target !== "string") return unreadable;
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
            const read = pairConditionReads(lifted, source, pair);
            if (read === undefined || read.fansOut) return unreadable;
            level = Math.max(level, read.level);
         } catch {
            return unreadable;
         }
      }
      return { level, fansOut: false };
   };
   const key = [
      pair.join,
      ...gates.map(
         (g) =>
            `${g.struct?.as ?? g.struct?.name}\u0000${g.exprs.join("\u0000")}`,
      ),
   ].join("\u0001");
   const memo = (compiled.pairGatesRead ??= new Map());
   let answer = memo.get(key);
   if (answer === undefined) {
      answer = reads();
      memo.set(key, answer);
   }
   return answer;
}

/** The query check's view of a served pair table ({@link FlatPair}). */
function flatPairOf(pair: PairReroot): FlatPair {
   return {
      join: pair.join,
      parentRowField: pair.parentRowField,
      ...(pair.hops ? { hops: pair.hops.map((h) => h.join) } : {}),
      ...(pair.levelRows
         ? { levelRowFields: pair.levelRows.map((l) => l.field) }
         : {}),
      ...(pair.oneParentLevels
         ? {
              oneParentLevels: pair.oneParentLevels,
              computedFields: pair.childStoredDimensions.map(
                 (d) => `${pair.join}.${d}`,
              ),
           }
         : {}),
      ...(pair.block
         ? { otherRecords: pair.block.others.map((o) => o.join) }
         : {}),
   };
}

/** Each block's path of a multi-path pair table, by block number. */
function blockPaths(
   plan: MaterializePlan | undefined,
): { join: string; hops?: string[] }[] {
   return (plan?.pairBlocks?.blocks ?? []).map((b) => ({
      join: b.join,
      ...(b.hops ? { hops: b.hops.map((h) => h.join) } : {}),
   }));
}

/**
 * How the served copy reads one pair table (P5), from its plan, the stored
 * schema, the author's compiled source (`sourceFields`) and the joined
 * sources': each stored child column renamed internally under a name no
 * field of the model has (`taken`), and each measure over one level above the
 * leaf re-declared over that level's rows. `lane` picks the block a table of
 * several inlined paths is read for (its first, otherwise). Undefined when
 * the table does not store what the plan says (a column missing).
 */
function pairRerootOf(
   plan: MaterializePlan,
   pair: MaterializePair,
   sourceFields: readonly unknown[],
   stored: ReadonlyMap<string, string>,
   map: SourceMap,
   taken: readonly string[],
   lineage: ReadonlyMap<string, FieldClassification>,
   lane = 0,
   bigRead: (
      field: unknown,
      column: { name: string; type: string },
   ) => BigIntegerRead | undefined = () => undefined,
): PairReroot | undefined {
   const blocks = plan.pairBlocks;
   const block = blocks
      ? blocks.blocks[Math.min(Math.max(0, lane), blocks.blocks.length - 1)]
      : pair;
   const parentRow = stored.get(block.parentRowColumn);
   if (!parentRow) return undefined;
   const used = new Set(taken);
   const fresh = (base: string) => {
      let name = `${base}__pair`;
      for (let n = 2; used.has(name); n++) name = `${base}__pair_${n}`;
      used.add(name);
      return name;
   };
   const childStoredDimensions: string[] = [];
   const annotatedFields: string[] = [];
   let missing = false;
   // The stored fields of the joined source at one point of a path: `names`
   // from the source down to it, `sourceID` its declaration.
   const levelColumns = (
      names: readonly string[],
      sourceID: string,
      own: boolean,
   ): (JoinedColumn & { internalName: string })[] => {
      const fieldsOf = new Map(
         (
            ((map.node(sourceID)?.def as { fields?: unknown[] })?.fields ??
               []) as {
               name?: unknown;
               as?: unknown;
               e?: unknown;
               timeframe?: unknown;
            }[]
         ).map((f) => [String(f.as ?? f.name), f] as const),
      );
      const at = names.join(".");
      const out: (JoinedColumn & { internalName: string })[] = [];
      for (const c of plan.columns) {
         if (c.join === undefined || (c.path ?? [c.join]).join(".") !== at) {
            continue;
         }
         const type = stored.get(c.column);
         if (type === undefined) {
            missing = true;
            continue;
         }
         const field = fieldsOf.get(c.dimension);
         // A big-integer child field is read through its native type in the
         // record (known limitation 12); one no cast reads exactly leaves
         // the table to its fallback.
         const big = bigRead(field, { name: c.column, type });
         if (big && !big.castTo) {
            missing = true;
            continue;
         }
         const { notes, annotated } = joinedColumnNotes(
            field,
            lineage.get(`${at}.${c.dimension}`),
         );
         // A field read only through another block's record never reaches a
         // result from this block, which declines any query reading it.
         if (annotated && own) {
            annotatedFields.push(
               names.length > 1
                  ? `${names.slice(1).join(".")}.${c.dimension}`
                  : c.dimension,
            );
         }
         if (own && field?.e !== undefined) {
            childStoredDimensions.push(
               `${names.slice(1).concat(c.dimension).join(".")}`,
            );
         }
         out.push({
            name: c.dimension,
            column: c.column,
            internalName: fresh(c.column),
            type,
            ...(big?.castTo ? { nativeCast: big.castTo } : {}),
            ...(notes.length > 0 ? { notes } : {}),
            ...(typeof field?.timeframe === "string"
               ? { timeframe: field.timeframe }
               : {}),
         });
      }
      return out;
   };
   // A join the author hides keeps its modifier on its record (known
   // limitation 24), read off the author's compiled source.
   const joinAccess = (join: string) => {
      const def = (
         sourceFields as {
            name?: unknown;
            as?: unknown;
            join?: unknown;
            accessModifier?: unknown;
         }[]
      ).find((f) => f.join !== undefined && String(f.as ?? f.name) === join);
      return def ? hiddenAccess(def) : undefined;
   };
   const recordOf = (p: MaterializePair, own: boolean) => {
      const names = [p.join];
      const access = joinAccess(p.join);
      const childColumns = levelColumns(names, p.childSourceID, own);
      const hops: PairHopColumns[] = [];
      for (const hop of p.hops ?? []) {
         names.push(hop.join);
         hops.push({
            join: hop.join,
            columns: levelColumns(names, hop.sourceID, own),
         });
      }
      return {
         join: p.join,
         childColumns,
         ...(hops.length > 0 ? { hops } : {}),
         ...(access ? { access } : {}),
      };
   };
   const own = recordOf(block, true);
   const others = (blocks?.blocks ?? [])
      .filter((b) => b !== block)
      .map((b) => recordOf(b, false));
   if (missing) return undefined;
   const parentRowField = fresh(block.parentRowColumn);
   const levelRows: { column: ServedColumn; field: string }[] = [];
   for (const hop of block.hops ?? []) {
      const type = stored.get(hop.rowColumn);
      if (type === undefined) return undefined;
      levelRows.push({
         column: { name: hop.rowColumn, type },
         field: fresh(hop.rowColumn),
      });
   }
   let blockReroot: PairBlockReroot | undefined;
   if (blocks) {
      const type = stored.get(blocks.laneColumn);
      if (type === undefined) return undefined;
      blockReroot = {
         lane: blocks.blocks.indexOf(block),
         laneColumn: { name: blocks.laneColumn, type },
         laneField: fresh(blocks.laneColumn),
         others,
      };
   }
   // A measure over one level above the leaf is re-declared over that
   // level's rows; one at the leaf reads every row as it stands, and one
   // reading several levels (or anything else) is left as it is, so a query
   // that reads it is answered live (pair_flat_query.ts).
   const path = {
      join: block.join,
      hops: (block.hops ?? []).map((h) => h.join),
   };
   const leaf = 1 + path.hops.length;
   const levels = pairMeasureLevels(sourceFields, path);
   const parentMeasures: PairMeasure[] = [];
   for (const f of sourceFields as {
      name?: unknown;
      as?: unknown;
      accessModifier?: unknown;
   }[]) {
      const name = String(f.as ?? f.name);
      const level = levels.get(name);
      if (!level || level.size !== 1) continue;
      const only = [...level][0];
      if (only >= leaf) continue;
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
         ...(only > 0 ? { level: only } : {}),
      });
   }
   return {
      join: block.join,
      parentRowColumn: { name: block.parentRowColumn, type: parentRow },
      parentRowField,
      childColumns: own.childColumns,
      annotatedFields,
      parentMeasures,
      childStoredDimensions,
      ...(own.hops ? { hops: own.hops } : {}),
      ...(levelRows.length > 0 ? { levelRows } : {}),
      ...(block.oneParentLevels && block.oneParentLevels.length > 0
         ? { oneParentLevels: block.oneParentLevels }
         : {}),
      ...(blockReroot ? { block: blockReroot } : {}),
      ...(own.access ? { access: own.access } : {}),
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
 * A joined field's stored column, as both records of stored columns read it
 * (an inlined join_one's and a pair table's child columns): `notes` go on
 * the column in the `type:` ({@link storedColumnNotes}, an inferred class
 * included), so the record's field, which reads it, keeps its class; and
 * `annotated` says the author's field carries notes of its own (inherited
 * ones included), which a result column of the record's field cannot carry,
 * so a query returning it is answered live (`joined_annotations`). An
 * inferred class is not a note on the result either way, so it alone does
 * not make the field annotated.
 */
function joinedColumnNotes(
   field: unknown,
   classified: FieldClassification | undefined,
): { notes: string[]; annotated: boolean } {
   return {
      notes: storedColumnNotes(field, classified),
      annotated: fieldNotes(field).length > 0,
   };
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

/**
 * Whether the served source's compiled filters hold each of `codes` (the
 * author's text of a `where:` term), whitespace aside. True for none;
 * unreadable filters fail closed.
 */
export function keepsTermsAsWritten(
   served: unknown,
   codes: readonly string[],
): boolean {
   if (codes.length === 0) return true;
   const filters = (served as { filterList?: unknown } | undefined)?.filterList;
   if (!Array.isArray(filters)) return false;
   const norm = (code: string) => code.replace(/\s+/g, " ").trim();
   const held = new Set(
      (filters as { code?: unknown }[])
         .map((f) => (typeof f?.code === "string" ? norm(f.code) : ""))
         .filter((c) => c !== ""),
   );
   return codes.every((c) => norm(c) !== "" && held.has(norm(c)));
}

/**
 * Whether every field of the served source has the access modifier the
 * author's field of that name has (P3): `public` and none read alike, and a
 * modifier it does not know reads as private, so it never matches a public
 * one. A field only the served copy has (a timeframe column's internal
 * rename, `order_month__stored`; an inlined join's stored column,
 * `carriers__name__stored`) is the re-root's own and must not be public, so a
 * stored column never surfaces under a name the author's source lacks. Nor
 * may it be less hidden than the author's field it is named after (`<field>__…`,
 * the field a timeframe column or an inlined join's column stores): a private
 * field's stored column renamed only `internal` would let a caller's own
 * `extend` read it. Unreadable sources fail closed.
 */
export function keepsAccess(author: unknown, served: unknown): boolean {
   const accessByName = (def: unknown) => {
      const fields = (def as { fields?: unknown } | undefined)?.fields;
      if (!Array.isArray(fields)) return undefined;
      const out = new Map<string, string>();
      for (const f of fields as {
         name?: unknown;
         as?: unknown;
         accessModifier?: unknown;
      }[]) {
         const name = typeof f.as === "string" ? f.as : f.name;
         if (typeof name !== "string") continue;
         out.set(name, hiddenAccess(f) ?? "public");
      }
      return out;
   };
   const before = accessByName(author);
   const after = accessByName(served);
   if (!before || !after) return false;
   const rank = (access: string) =>
      access === "public" ? 0 : access === "internal" ? 1 : 2;
   for (const [name, access] of after) {
      const was = before.get(name);
      if (was !== undefined) {
         if (was !== access) return false;
         continue;
      }
      // The re-root's own: hidden, and at least as hidden as every author
      // field whose stored column it could be.
      let floor = 1;
      for (const [field, fieldAccess] of before) {
         if (name.startsWith(`${field}__`)) {
            floor = Math.max(floor, rank(fieldAccess));
         }
      }
      if (rank(access) < floor) return false;
   }
   return true;
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

/** The names of a compiled source's private fields. */
function privateFieldNames(def: unknown): Set<string> {
   const fields = (
      def as {
         fields?: { name?: unknown; as?: unknown; accessModifier?: unknown }[];
      }
   )?.fields;
   return new Set(
      (fields ?? [])
         .filter((f) => hiddenAccess(f) === "private")
         .map((f) => (typeof f.as === "string" ? f.as : f.name))
         .filter((n): n is string => typeof n === "string"),
   );
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

/** A compiled field's name on its source (`as`, else `name`). */
function fieldNameOf(field: unknown): string | undefined {
   const f = (field ?? {}) as { name?: unknown; as?: unknown };
   const name = f.as ?? f.name;
   return typeof name === "string" ? name : undefined;
}

/**
 * The fields of the source reached through `path` (join names from the
 * re-rooted source down), read off the author's compiled fields; empty when
 * the path does not resolve.
 */
function joinedFieldsAt(
   fields: readonly unknown[] | undefined,
   path: readonly string[],
): unknown[] {
   let level: readonly unknown[] = fields ?? [];
   for (const join of path) {
      const next = level.find(
         (f) =>
            (f as { join?: unknown }).join !== undefined &&
            fieldNameOf(f) === join,
      ) as { fields?: unknown } | undefined;
      if (!next || !Array.isArray(next.fields)) return [];
      level = next.fields;
   }
   return [...level];
}

/**
 * The big-integer columns among `columns` (known limitation 12), by name, to
 * the native type each is read through, or to undefined when no cast reads it
 * exactly ({@link bigIntegerReads}). Decided by the author's source's dialect
 * (the one live answers on) and where the binding's table is.
 */
function bigIntegerReadsOf(
   columns: readonly { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
   def: unknown,
   binding: ServeBinding,
): Map<string, string | undefined> {
   return new Map(
      bigIntegerReads(columns, fields, bigIntegerOptionsOf(def, binding)).map(
         (r) => [r.column, r.castTo] as const,
      ),
   );
}

/**
 * What decides a big-integer column's read for a binding: the author's
 * source's dialect, where the table is, and, for a colocated table, the
 * warehouse types its build captured (`nativeSchema`).
 */
function bigIntegerOptionsOf(
   def: unknown,
   binding: ServeBinding,
): BigIntegerReadOptions {
   return {
      dialect: (def as { dialect?: unknown }).dialect,
      colocated: binding.colocated === true,
      ...(binding.colocated
         ? { nativeTypeOf: nativeTypeLookup(binding.nativeSchema) }
         : {}),
   };
}

/**
 * The join_ones a `#(materialize)` plan inlines, as the served copy answers
 * them from stored columns: each stored `<join>__<field>` column with its
 * stored type and the joined field's timeframe, and the join's own access
 * modifier when the author hides it, read off the author's compiled source. A join whose joined source
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
         // A join the author hides is replaced by a record under the
         // join's own modifier ({@link InlinedJoin.access}).
         .filter((f) => f.join === "one")
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
         const timeframe = field?.timeframe;
         const { notes, annotated } = joinedColumnNotes(
            field,
            lineage.get(`${join}.${c.dimension}`),
         );
         if (annotated) annotatedFields.push(c.dimension);
         columns.push({
            name: c.dimension,
            column: c.column,
            type,
            ...(notes.length > 0 ? { notes } : {}),
            ...(typeof timeframe === "string" ? { timeframe } : {}),
         });
      }
      const access = hiddenAccess(def);
      if (columns.length > 0) {
         out.push({
            join,
            columns,
            annotatedFields,
            ...(access ? { access } : {}),
         });
      }
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

/**
 * A v1 colocated table's columns as its source's fields (G2): every stored
 * column matched to the author's field of its name (exactly, else one field
 * whose name folds to it, as {@link narrowSchemaToPublic} matches), with the
 * field's access modifier when it is `internal` or `private`. Malloy built
 * the table from the source's own query, hidden columns included, so a column
 * no field names is left out (the served copy finds it missing).
 */
export function storedColumnsWithAccess(
   schema: readonly { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
): { name: string; type: string; access?: "private" | "internal" }[] {
   const byName = new Map<string, { accessModifier?: unknown }>();
   const byFold = new Map<string, string[]>();
   for (const f of fields ?? []) {
      const name = (f as { name?: unknown }).name;
      if (typeof name !== "string") continue;
      byName.set(name, f as { accessModifier?: unknown });
      const folded = name.toLowerCase();
      byFold.set(folded, [...(byFold.get(folded) ?? []), name]);
   }
   const out: {
      name: string;
      type: string;
      access?: "private" | "internal";
   }[] = [];
   const emitted = new Set<string>();
   for (const c of schema) {
      const fold = byFold.get(c.name.toLowerCase());
      const name = byName.has(c.name)
         ? c.name
         : fold?.length === 1
           ? fold[0]
           : undefined;
      if (name === undefined || emitted.has(name)) continue;
      emitted.add(name);
      const access = hiddenAccess(byName.get(name)!);
      out.push({ name, type: c.type, ...(access ? { access } : {}) });
   }
   return out;
}

/**
 * Whether a compiled source declares (or inherits) a measure, a view or a
 * join: what a re-root that replaces it as a base would lose unless it keeps
 * its blocks (G8).
 */
function declaresBlocks(def: unknown): boolean {
   const fields = (def as { fields?: unknown[] } | undefined)?.fields ?? [];
   return fields.some((f) => {
      const x = f as { type?: unknown; expressionType?: unknown };
      return (
         x.type === "turtle" ||
         (typeof x === "object" && x !== null && "join" in x) ||
         (typeof x.expressionType === "string" && x.expressionType !== "scalar")
      );
   });
}

/**
 * The stored columns no field of the source names, exactly or by a unique
 * case-fold (see {@link storedColumnsWithAccess}): what the source's own text
 * drops or renames away after the build.
 */
export function unnamedStoredColumns(
   schema: readonly { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
): { name: string; type: string }[] {
   const names = new Set<string>();
   for (const f of fields ?? []) {
      const x = f as { name?: unknown; as?: unknown };
      for (const n of [x.name, x.as]) {
         if (typeof n === "string") names.add(n.toLowerCase());
      }
   }
   return schema
      .filter((c) => !names.has(c.name.toLowerCase()))
      .map((c) => ({ name: c.name, type: c.type }));
}

/**
 * Whether `node` is declared by refinement (`extend` or rename) of a base
 * whose compiled chain carries no filter and no row-level gate, so its
 * stored table (its root's rows) re-rooted with its own `extend` run kept
 * answers exactly as the source: nothing the base applies is lost.
 */
export function refinesUnfilteredBase(
   node: { base?: { hop: string; def?: unknown } } | undefined,
   map: SourceMap,
): boolean {
   const base = node?.base;
   if (!base || (base.hop !== "extends" && base.hop !== "rename")) {
      return false;
   }
   const def = base.def as { filterList?: unknown[] } | undefined;
   if (!def || (def.filterList?.length ?? 0) > 0) return false;
   try {
      return (
         collectEntryPointGates(def as never, map.model() as never).length === 0
      );
   } catch {
      return false;
   }
}

/**
 * A query result's shape: its fields' names, nested results' fields in
 * place, in order. Two compiles of one query that agree on it return the
 * same columns.
 */
export function resultShapeOf(prepared: { resultExplore: unknown }): string {
   type Field = {
      name: string;
      isExploreField?: () => boolean;
      allFields?: Field[];
   };
   const shape = (fields: readonly Field[]): unknown[] =>
      fields.map((f) =>
         f.isExploreField?.() === true
            ? { [f.name]: shape(f.allFields ?? []) }
            : f.name,
      );
   return JSON.stringify(
      shape((prepared.resultExplore as { allFields: Field[] }).allFields),
   );
}
