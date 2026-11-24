// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served model on `virtual`: one model's served copy (served_copy.ts),
 * compiled once per binding set and answering queries the router sends it
 * (the router's served leg).
 *
 * It reads a source's own table, `#(materialize)` or v1 `#@ persist`
 * (never a rollup's or a persisted view's): a `storage=` table only while
 * the host has storage destinations to serve from
 * (`PERSIST_STORAGE_MODE=on`), a database one always.
 *
 * The served copy compiles in a runtime whose connections are the package's
 * own plus each storage destination under a per-process alias
 * (served_copy_config.ts), so the author's text never names a destination. A
 * database table (`deriveDatabaseServeBindings`, bind.ts) is read through
 * the source's own connection, which the package's connections already hold,
 * so a copy of database tables alone needs no storage tier.
 * A compile failure leaves the model's sources answering live and is
 * reported as a coded package warning.
 */

import { isDuckLakeCatalogUnavailable } from "../../ducklake_catalog_unavailable";
import { CompiledCopyLanes, readsLapsedAt } from "./served_copy_lanes";
import { createHash } from "crypto";
import { requireUnique, verdictOf } from "../domain/key_check";
import {
   hiddenAccess,
   modelDefOf,
   primaryKeyOf,
   queryStageZone,
   queryTimezoneOf,
   sameZone,
} from "../../governance/ir/compiled_model";
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
import {
   classificationNotKept,
   gateDeclaringSourceResolves,
   proveReroot,
} from "../../filter_binding_guard";
import {
   buildRowLevelProbe,
   gateFilterText,
   liftProbeFilterCondition,
} from "../../authorize";
import { keyedOverlay } from "../../overlay_key";
import { createEventLoopYielder } from "../../event_loop_yield";
import type { ModelConnectionInput } from "../../model_runtime";
import { packageFiles } from "../text/package_files";
import { SourceMap } from "../text/source_map";
import {
   chainBuildFileSet,
   rollupBuildFileSet,
   withPersistenceFlag,
} from "../text/chain_build_files";
import type {
   ChainBuildRequest,
   ChainBuildText,
   PlanChainBuildText,
} from "../domain/chain_build";
import type { ManifestEntry } from "../domain/types";
import { givenReadingFilterTerms } from "../plan/persist_dynamic_terms";
import type { MaterializePair, MaterializePlan } from "../domain/table_plans";
import { rollingWindowsOf } from "../domain/read_time_terms";
import {
   bigIntegerReadOf,
   bigIntegerReads,
   type BigIntegerRead,
   type BigIntegerReadOptions,
   nativeTypeLookup,
   declaredColumnsOf,
   holdsDucklakeDouble,
   liveKeepsBigIntegers,
   storedColumnReadPlan,
   storedTypeServesAs,
} from "../stored_column_types";
import {
   queryTimezones,
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { joinedDefinitionOf } from "../../governance/ir/source_graph";
import {
   flatPairQueryVerdict,
   pairConditionReads,
   pairMeasureLevels,
   pairMeasurePresence,
   pairQueryReadLevel,
   type FlatPair,
} from "../../governance/ir/pair_flat_query";
import {
   sqlTableReads,
   tablesReadForGates,
   buildVirtualMap,
   optsOutOfPersist,
} from "./bindings";
import {
   type Binding,
   bindingServesModelSource,
   checkedKeyIs,
   deriveServeBindings,
   EMPTY_SNAPSHOT,
   isDatabase,
   pin,
   type Pinned,
   type Snapshot,
} from "../domain/binding";
import { servedCopyReadsTier } from "../domain/placement";
import type { Tier } from "../domain/vocabulary";
import {
   authorModelLiftContext,
   type DerivedSourceDef,
} from "../text/author_model";
import { storedMalloyTypeFor } from "../stored_column_types";
import { fieldNotes } from "../text/field_notes";
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
   TIMEFRAMES,
} from "./served_copy";
import { servedCopyConfig, servedCopyAlias } from "./served_copy_config";
import {
   failingReroots,
   rerootKey,
   type TraceMode,
} from "./served_copy_containment";
import { logger } from "../../../logger";
import { offsetOf } from "../text/malloy_lexer";
import {
   confirmedAncestors,
   keptAncestorChainOrDeclared,
} from "../text/kept_ancestors";
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
} from "../text/cross_connection_text";
import type { LeftOutDefinition } from "../text/definition_cut";
import { LegDecline, type ServedCopyDeclineCode } from "../domain/leg_decline";
import { compiledImports, compiledTextReader } from "../../../utils";
import type {
   AnswerAttempt,
   Compiled,
   ServedCopyProblem,
   SourceDecline,
   SourceDeclines,
   VirtualMap,
} from "./served_copy_types";
import {
   chainZoneOf,
   checkGateColumnsKept,
   checkOptOut,
   checkRecordJoins,
   checkSourceZone,
   checkZonedColumns,
   inlineBaseZoneOf,
   leftAsWritten,
   optOutDeclineOf,
   readsOptOuts,
   SOURCE_ZONE_DECLINE,
   sourceZoneDiffers,
   readsInAnotherZone,
   readsRerootedTables,
   readZonesOf,
   recordJoinsRead,
   ZONED_TRUNCATION_REASON,
} from "./served_copy_checks";
import { reportQueryWalk } from "./query_walks";
import type { ScopeLane } from "./scope_facts";

export type { ServedCopyProblem } from "./served_copy_types";

/**
 * The connection a binding's table is read through in the served copy: a
 * storage destination under its server-only alias, a database table on the
 * source's own connection, which the package's connections already hold.
 */
function servedConnectionOf(b: Binding): string {
   return isDatabase(b)
      ? b.destinationName
      : servedCopyAlias(b.destinationName);
}

/** Nothing bound, pinned: what a copy reads with no host tables. */
const NOTHING_PINNED = pin(EMPTY_SNAPSHOT, new Date(0));

/** No storage destination: a served copy of database tables alone. */
const NO_DESTINATIONS = (): MalloyConfig =>
   new MalloyConfig({ connections: {} });

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
   return readsMissingName(
      new Set(compiled.leftOut.map((l) => l.name)),
      error,
      queryString,
   );
}

/**
 * Whether `error` is a compile that reads a field the model lacks, named by a
 * path through one of `names` (or at a place the query text cannot show).
 */
function readsMissingName(
   names: ReadonlySet<string>,
   error: unknown,
   queryString: string,
): boolean {
   if (!(error instanceof MalloyError)) return false;
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

/** What the served copy reads from the model that owns it. */
interface ServedCopyHost {
   modelPath(): string;
   modelDef(): unknown;
   /**
    * The storage destinations, while the storage tier serves
    * (`PERSIST_STORAGE_MODE=on`); undefined otherwise, when only database
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
   /**
    * Read a stored big-integer column as stored, with no exact-digit cast: a
    * chained build compiles the copy to SQL and never reads a value into
    * JavaScript, where the cast keeps every digit.
    */
   readsStoredIntegers?: boolean;
   /**
    * The model's tables, pinned now, for a caller outside a query: the
    * compile at bind, the warnings, a test. A query passes its own pin.
    * Absent, nothing is bound.
    */
   pinned?(): Pinned;
   /**
    * Compile one pass's files off this thread (the load worker) and hydrate
    * the compiled model here, in a runtime over `config`. Resolves undefined
    * when it cannot (no worker free now, or the job failed), and the pass
    * compiles inline; rejects with the compile's failure as the inline
    * compile throws it. Absent: every pass compiles inline.
    */
   compileElsewhere?(args: {
      packagePath: string;
      modelPath: string;
      config: MalloyConfig;
      /** The pass's files, by overlay key. */
      files: ReadonlyMap<string, string>;
   }): Promise<Awaited<ReturnType<CopyLoad>> | undefined>;
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
    * For a `#(materialize)` re-root: its rows depend on the zone it was built
    * in, which the probe compiler did not clear: a `where:` term or base
    * relation whose SQL moves with the zone. Any read in another zone is
    * declined.
    */
   zonedRows?: true;
   /**
    * For a `#(materialize)` re-root: the stored computed dimensions the probe
    * compiler did not clear of the zone, each as the build evaluated it
    * in the re-root's zone. Only a read of one in another zone is declined.
    */
   zonedDimensions?: string[];
   /**
    * The timestamp truncations its `type:` restates over the stored value,
    * which truncate it again in the reading query's zone
    * ({@link zonedTruncations}).
    */
   restatedTruncations?: string[];
   /** The timestamp truncations it leaves out, stored in another zone than any read's. */
   zonedLeftOut?: string[];
   /**
    * For a query-shaped re-root whose query computed in a zone: its
    * timestamp truncations, restated over the stored value, and the one
    * zone its query's stages truncated them in ({@link zonedTruncations}).
    */
   stageZonedTruncations?: { zone: string | undefined; names: string[] };
   /**
    * The stored columns left out of the `type:`, so only a query reading
    * one fails to compile here and answers live; their absence is not a lost
    * field. For a v1 re-root, the columns no cast reads as their declared
    * type; on any re-root, the big-integer columns no native cast reads
    * exactly (one stored as a float, a warehouse whose
    * bigint type Malloy does not read back as one).
    */
   columnsLeftOut?: readonly string[];
   /**
    * Why each of {@link columnsLeftOut} is left out, as a caller-safe phrase
    * (`leftOutPhrase`), for the decline of a query that reads one.
    */
   leftOutWhy?: ReadonlyMap<string, string>;
};

/**
 * What the join-to-rollup branch reads of a compiled served copy, to answer a
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
   rerooted: readonly Binding[];
   /** The same, each under the connection the copy reads it through (for the virtual map). */
   aliased: readonly Binding[];
   /** The definitions the copy left out because a join they read crosses connections. */
   leftOut: readonly LeftOutDefinition[];
   /** The copy's checks on a query a variant over it answers; throws a {@link LegDecline}. */
   check(args: {
      /** The query source's author `sourceID`. */
      sourceID: string;
      runnable: QueryMaterializer;
      queryString: string;
      /** The re-rooted tables the variant's SQL reads. */
      tablesRead: readonly Binding[];
   }): Promise<void>;
}

/** How many declined queries one compiled copy remembers before starting over. */
const DECLINED_LIMIT = 1000;

/**
 * Served copies whose compile failed on a re-root's own text, by the package
 * connections they compiled with, then by model, bindings and copy text: a
 * recompile of the same copy over the same tables (a reload, a rebind) takes
 * the same step back without compiling it again or warning again. Held for
 * a while only, since a failure can come from a table's schema at that
 * moment.
 */
const knownRerootFailures = new WeakMap<
   object,
   Map<string, { error: unknown; until: number }>
>();
const REROOT_FAILURE_TTL_MS = 10 * 60_000;

/**
 * How long a compile that threw is judged as failed before a verdict read
 * compiles it again (a query does at once).
 */
const ESCAPED_FAILURE_RETRY_MS = 30_000;
const REROOT_FAILURES_PER_CONFIG = 64;

/** The key a copy's failure is remembered under. */
function rerootFailureKey(
   packagePath: string,
   modelPath: string,
   bindings: readonly Binding[],
   overlay: ReadonlyMap<string, string>,
): string {
   const hash = createHash("sha256");
   // The tables, by everything a binding says of one: the copy text names a
   // storage table through its handle, not its path.
   hash.update(JSON.stringify(bindings)).update("\0");
   for (const [url, text] of [...overlay].sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
   )) {
      hash.update(url).update("\0").update(text).update("\0");
   }
   return `${packagePath}\0${modelPath}\0${hash.digest("hex")}`;
}

/** One model's served copy. */
export class ServedCopyServe {
   private packagePath?: string;
   private packageConfig?: ModelConnectionInput;
   private materializePlans: Readonly<Record<string, MaterializePlan>> = {};
   /** A declaration's definition from the package model that holds it. */
   private declarationOf?: (sourceID: string) => unknown;
   /** The compiled copy per pair-table block, and the compiles in flight. */
   private readonly lanes = new CompiledCopyLanes<Compiled>(
      (key, generation, bindings, lane, asOfMs) =>
         this.compile(key, generation, bindings, lane, asOfMs),
   );

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

   /**
    * The `#(materialize)` tables of a pinned snapshot, which only the served
    * copy reads, keyed like every binding by the author's sourceID: every
    * one bound, or only those read at the pin. For the path report, which
    * places them on the queried source's chain.
    */
   materializeBindings(pinned: Pinned, which: "bound" | "fresh"): Binding[] {
      return (
         which === "bound" ? pinned.snapshot.bindings : pinned.fresh
      ).filter((b) => b.kind === "materialize");
   }

   /**
    * Whether any binding of `snapshot` could re-root a source (before
    * compiling): a database table, or a storage table while the storage
    * tier serves.
    */
   hasBindings(snapshot: Snapshot = this.current().snapshot): boolean {
      return snapshot.bindings.some((b) => this.usable(b));
   }

   /**
    * Whether the served copy may read a binding: a source's own table (never
    * a rollup's or a persisted view's); a storage table only while the host
    * has storage destinations to serve from (`PERSIST_STORAGE_MODE=on`); a
    * database table always, on its own connection.
    */
   private usable(b: Binding): boolean {
      if (b.kind !== "persist" && b.kind !== "materialize") return false;
      return isDatabase(b) || !!this.host.serveDestinationConfig();
   }

   /** The host's tables, pinned now, for a caller outside a query. */
   private current(): Pinned {
      return this.host.pinned?.() ?? NOTHING_PINNED;
   }

   /** Drop the compiled copy (bindings, destinations or the model changed). */
   invalidate(): void {
      this.lanes.invalidate();
   }

   /**
    * Whether the compiled copy is the one a query pinned now reads: compiled
    * over the bindings fresh at the pin. Right after a bind it is not: the
    * compile at bind runs in the background, and until it lands the copy
    * kept is the previous bindings' (a query compiles its own, so it never
    * reads that one).
    */
   isCurrent(pinned: Pinned = this.current()): boolean {
      return this.lanes.keepsFor(0, this.freshBindings(pinned));
   }

   /**
    * The bindings the compiled copy re-rooted that are still read at `atMs`
    * (all of them when it is absent). Past a table's freshness edge, and
    * before the commit at that edge replaces the copy, the copy kept still
    * re-roots it; a query compiles its own copy over what it pins, so only
    * what this copy reports must leave the lapsed table out. A recorded
    * fact checked against the caller's clock, no I/O.
    */
   private rerootedAt(compiled: Compiled, atMs: number | undefined): Binding[] {
      return atMs === undefined
         ? compiled.rerooted
         : compiled.rerooted.filter((b) => !readsLapsedAt([b], atMs));
   }

   /**
    * The author `sourceID`s the compiled copy re-rooted on a stored table
    * (still read at `atMs`, when given), which are the sources it answers
    * from storage; undefined before the first compile.
    */
   rerootedSourceIDs(atMs?: number): ReadonlySet<string> | undefined {
      const compiled = this.lanes.kept(0);
      if (!compiled) return undefined;
      return new Set(
         this.rerootedAt(compiled, atMs).flatMap((b) => b.sourceIDs ?? []),
      );
   }

   /**
    * Lane 0's stored and re-rooted sources, for the scope facts a bind
    * records (`scope_facts.ts`): the sources whose tables the copy reads,
    * and those its answers report re-rooted. Undefined before a compile.
    */
   scopeLane(): ScopeLane | undefined {
      const compiled = this.lanes.kept(0);
      if (!compiled?.materializer) return undefined;
      return {
         stored: new Set(compiled.rerooted.flatMap((b) => b.sourceIDs ?? [])),
         rerooted: compiled.rerootedSourceIDs ?? new Set(),
      };
   }

   /**
    * The author `sourceID`s the compiled copy re-rooted on a database table
    * and on no storage table (still read at `atMs`, when given); undefined
    * before the first compile.
    */
   rerootedOnDatabaseOnly(atMs?: number): ReadonlySet<string> | undefined {
      const compiled = this.lanes.kept(0);
      if (!compiled) return undefined;
      const rerooted = this.rerootedAt(compiled, atMs);
      const onStorage = new Set(
         rerooted
            .filter((b) => !isDatabase(b))
            .flatMap((b) => b.sourceIDs ?? []),
      );
      return new Set(
         rerooted
            .filter(isDatabase)
            .flatMap((b) => b.sourceIDs ?? [])
            .filter((id) => !onStorage.has(id)),
      );
   }

   /**
    * The author `sourceID`s the compiled copy leaves as written because they
    * extend a source it re-rooted, and so answer from that source's table by
    * name (an extension with no table of its own); empty before the first
    * compile. Not one that opts out of persist, or whose served surface the
    * load-time checks flag: those are declined at query time.
    */
   servedThroughBaseSourceIDs(atMs?: number): ReadonlySet<string> {
      const compiled = this.lanes.kept(0);
      if (!compiled?.throughBase) return new Set();
      const rerooted = this.rerootedAt(compiled, atMs);
      const ids = new Set(rerooted.flatMap((b) => b.sourceIDs ?? []));
      const names = new Set(rerooted.map((b) => b.sourceName));
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
      const compiled = this.lanes.kept(0);
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
      return this.lanes.kept(0)?.problems ?? [];
   }

   /** The served copy's compiled model, compiling it first; for reading only. */
   async compiledModelDef(pinned: Pinned = this.current()): Promise<unknown> {
      return (await this.ensureCompiled(pinned)).modelDef;
   }

   /** Compile now (at bind), so warnings exist before the first query. */
   async prepare(pinned: Pinned = this.current()): Promise<void> {
      try {
         await this.ensureCompiled(pinned);
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
      /** The query's pinned tables; a caller outside a query omits it. */
      pinned: Pinned = this.current(),
   ): Promise<{
      runnable: QueryMaterializer;
      virtualMap: VirtualMap;
      bindings: Binding[];
      tablesRead: Binding[];
      /** What the answer is judged to read for its gates (tablesReadForGates). */
      gateTablesRead: Binding[];
      /** The author sources the copy re-rooted on their tables, by `sourceID`. */
      rerootedSourceIDs: ReadonlySet<string>;
      /**
       * The served copy's compiled model, where a row-level gate is grafted
       * when this answer runs: the gate's text is the author's, kept in
       * the copy, so the graft and its proofs read the same compile that runs.
       */
      graftScope: GraftScope;
   }> {
      return this.answer(
         await this.ensureCompiled(pinned),
         queryString,
         givens,
         pinned,
      );
   }

   /**
    * The attempt every check reads: the caller's text loaded on the copy,
    * its virtual map, and the memoized decline. Declines at once when the
    * copy re-roots nothing, or already declined this text.
    */
   private async beginAttempt(
      compiled: Compiled,
      queryString: string,
      givens: Record<string, unknown> | undefined,
   ): Promise<AnswerAttempt & { materializer: ModelMaterializer }> {
      if (!compiled.materializer || compiled.rerooted.length === 0) {
         const crossConnection = compiled.problems.some(
            (p) => p.reason === "cross_connection_unbuilt",
         );
         const asWritten = crossConnection
            ? undefined
            : leftAsWritten(compiled);
         throw new LegDecline(
            crossConnection
               ? "served_copy_cross_connection"
               : "served_copy_not_served",
            asWritten?.detail,
            {
               message:
                  compiled.problems[0]?.detail ??
                  "no stored table is re-rooted in this model's served copy",
               ...(asWritten ? { tableIds: asWritten.tableIds } : {}),
            },
         );
      }
      const declined = compiled.declined?.get(queryString);
      if (declined !== undefined) throw declined;
      const decline = (
         reason: string,
         code: ServedCopyDeclineCode,
         callerDetail?: string,
         tableIds?: readonly string[],
      ): never => {
         const memo = (compiled.declined ??= new Map());
         if (memo.size >= DECLINED_LIMIT) memo.clear();
         const error = new LegDecline(code, callerDetail, {
            message: reason,
            ...(tableIds ? { tableIds } : {}),
         });
         memo.set(queryString, error);
         throw error;
      };
      // A query through a join left out across connections
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
            const zoned = new Set(
               [...(compiled.zonedLeftOut?.values() ?? [])].flatMap((n) => [
                  ...n,
               ]),
            );
            if (zoned.size > 0 && readsMissingName(zoned, error, queryString)) {
               decline(ZONED_TRUNCATION_REASON, "served_copy_time_zone");
            }
            // A stored column the re-root leaves out of its table's type (no
            // cast reads what the table holds as the source declares it: the
            // table altered outside a build, or the source retyped since).
            const storedLeftOut = new Set(
               [...(compiled.leftOutColumns?.values() ?? [])].flatMap((n) => [
                  ...n,
               ]),
            );
            if (
               storedLeftOut.size > 0 &&
               readsMissingName(storedLeftOut, error, queryString)
            ) {
               // The names the compile could not find, where it says.
               const missing = new Set(
                  [
                     ...String(
                        error instanceof Error ? error.message : error,
                     ).matchAll(/'([^']+)' is not defined/g),
                  ].map((m) => m[1]),
               );
               const read = [...storedLeftOut].filter((n) => missing.has(n));
               const named = read.length > 0 ? read : [...storedLeftOut];
               // The tables whose re-root leaves one of those columns out.
               const sourceIDs = new Set(
                  [...(compiled.leftOutColumns ?? [])]
                     .filter(([, names]) =>
                        names.some((n) => named.includes(n)),
                     )
                     .map(([sourceID]) => sourceID),
               );
               const tableIds = compiled.aliased
                  .filter((b) => b.sourceIDs?.some((id) => sourceIDs.has(id)))
                  .map((b) => b.virtualHandle);
               // Each column named with why a table leaves it out (its
               // declared and stored types, or that the table lacks it):
               // each distinct reason of the tables that leave it out.
               const why = [
                  ...new Set(
                     named.flatMap((c) => {
                        const reasons = [...sourceIDs]
                           .map((id) => compiled.leftOutWhy?.get(id)?.get(c))
                           .filter((w): w is string => w !== undefined);
                        // Every re-root records why for each column it
                        // leaves out; one that did not is never named, as
                        // it may be hidden.
                        return reasons.length > 0
                           ? reasons
                           : [noCastPhrase("a stored column")];
                     }),
                  ),
               ];
               decline(
                  "the query reads a stored column the served copy leaves out of its table's type",
                  "served_copy_column_left_out",
                  `the stored table cannot answer with live's columns: ${why.join("; ")}`,
                  tableIds,
               );
            }
            throw error;
         }
      };
      const materializer = compiled.materializer;
      const virtualMap = buildVirtualMap(compiled.aliased);
      await this.host.ensureSharedQueryModel?.(materializer);
      const runnable = materializer.loadRestrictedQuery(queryString);
      // The prepared query, read once, when a check needs it. No check
      // mutates it.
      let preparedRead: ReturnType<typeof readPreparedQuery> | undefined;
      const prepared = () => (preparedRead ??= readPreparedQuery(runnable));
      return {
         compiled,
         queryString,
         givens,
         runnable,
         virtualMap,
         decline,
         compiling,
         prepared,
         materializer,
      };
   }

   /**
    * {@link loadQuery} in one compiled copy: lane 0's first, then, for a query
    * that reads another block of a multi-path pair table, that block's copy.
    *
    * The checks run in a fixed order, and the first that declines names the
    * decline code: a gate the copy cannot graft, an opted-out source, a query
    * that reads no re-rooted table, the source's zone, the result's shape, a
    * pair table's block, an aggregate over a record join, a pair table, the
    * query's time zone, a zoned column.
    */
   private async answer(
      compiled: Compiled,
      queryString: string,
      givens: Record<string, unknown> | undefined,
      pinned: Pinned,
   ): Promise<Awaited<ReturnType<ServedCopyServe["loadQuery"]>>> {
      const attempt = await this.beginAttempt(compiled, queryString, givens);
      const { runnable, virtualMap, prepared, decline } = attempt;
      await this.checkSourceDeclines(attempt, "chain");
      const { sql, tablesRead } = await readsRerootedTables(attempt);
      await checkGateColumnsKept(attempt, tablesRead);
      await this.checkSourceDeclines(attempt, "zone");
      await this.checkResultShape(attempt, tablesRead);
      const pairsRead = await pairsReadPerTable(
         attempt,
         sql,
         (compiled.pairs ?? []).filter((p) =>
            tablesRead.some((b) => b.tablePath === p.tablePath),
         ),
      );
      const lane = await pairBlockLane(attempt, pairsRead);
      if (lane !== undefined) {
         return this.answer(
            await this.ensureCompiled(pinned, lane),
            queryString,
            givens,
            pinned,
         );
      }
      await checkRecordJoins({
         ...recordJoinsRead(compiled, tablesRead, pairsRead),
         prepared,
         modelDef: compiled.modelDef,
         queryString,
         decline,
      });
      await checkPairTables(attempt, pairsRead);
      await this.checkTimeZone(attempt, tablesRead);
      await checkZonedColumns(attempt, tablesRead);
      return {
         runnable,
         virtualMap,
         bindings: compiled.rerooted,
         tablesRead,
         gateTablesRead: tablesReadForGates(sql, compiled.rerooted, tablesRead),
         rerootedSourceIDs: compiled.rerootedSourceIDs ?? new Set(),
         graftScope: {
            modelDef: compiled.modelDef as ModelDef,
            materializer: attempt.materializer,
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
    * The declines that depend only on the queried source and this compile,
    * in the order the walks run them: `chain`, a gate the copy cannot graft
    * then an opted-out or unsafe source on its chain; `zone`, the zone it
    * computes in. They are read from what the copy's compile recorded for a
    * named source; a source written inline walks, and is counted.
    */
   private async checkSourceDeclines(
      attempt: AnswerAttempt,
      which: "chain" | "zone",
   ): Promise<void> {
      const walk = () =>
         which === "chain"
            ? (async () => {
                 await this.checkGateGraft(attempt);
                 await checkOptOut(attempt);
              })()
            : checkSourceZone(attempt);
      const recorded = await this.recordedDeclines(attempt, which);
      if (recorded === "unrecorded") {
         reportQueryWalk("served_copy_declines");
         return walk();
      }
      if (recorded) attempt.decline(recorded.reason, recorded.code);
   }

   /**
    * The decline the copy's compile recorded for the queried source, at
    * `which`; undefined for none, `unrecorded` for a source written inline
    * or one the compile did not record.
    */
   private async recordedDeclines(
      attempt: AnswerAttempt,
      which: "chain" | "zone",
   ): Promise<SourceDecline | undefined | "unrecorded"> {
      const { compiled, compiling, prepared } = attempt;
      const declines = compiled.sourceDeclines;
      if (declines === undefined) return "unrecorded";
      let structRef: unknown;
      if (which === "chain") {
         // Read the query's compile only where the walks would.
         if (!this.mayHaveRowLevelGates(compiled) && !readsOptOuts(compiled)) {
            return undefined;
         }
         structRef = (await compiling(prepared)).structRef;
      } else {
         if (compiled.authorModelDef === undefined) return undefined;
         try {
            structRef = (await prepared()).structRef;
         } catch {
            return SOURCE_ZONE_DECLINE;
         }
      }
      if (typeof structRef !== "string") return "unrecorded";
      const d = declines.get(structRef);
      if (d === undefined) return "unrecorded";
      return which === "chain" ? (d.graft ?? d.optOut) : d.zone;
   }

   /**
    * A row-level gate is grafted on this compile when the answer runs, and
    * the graft must find the source that declared it. A re-root that
    * replaced that source (a stored source over a same-file gated base)
    * leaves no declarer here, and the graft would deny every caller: the
    * query is declined instead, and the next candidate (live) answers it
    * under the author's gate.
    */
   private async checkGateGraft(attempt: AnswerAttempt): Promise<void> {
      const { compiled, decline, compiling, prepared } = attempt;
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
   }

   /**
    * A table whose re-root leaves a stored column out answers a query
    * naming that column live (it does not compile here), but a wildcard
    * (`select: *`, in the query or a view it calls) expands without it:
    * the answer would lack a column live returns. Such a query is
    * answered here only when its result has the shape the author's
    * compile gives it.
    */
   private async checkResultShape(
      attempt: AnswerAttempt,
      tablesRead: readonly Binding[],
   ): Promise<void> {
      const { compiled, queryString, givens, runnable, virtualMap } = attempt;
      const { decline, compiling } = attempt;
      if (tablesRead.some((b) => compiled.leftOutTables?.has(b.tablePath))) {
         const live = await this.host
            .authorResultShape?.(queryString, givens)
            .catch(() => undefined);
         // The author's compile failed: with these givens (one required and
         // missing, one of the wrong type), not with every caller's. The
         // decline is this caller's alone, so it is not remembered for the
         // query's text, which the memo is keyed by; every other decline
         // here compares result shapes, which no given's value changes.
         if (live === undefined) {
            throw new LegDecline("served_copy_not_served", undefined, {
               message:
                  "the author's compile of the query, which the served copy's result shape is checked against, failed",
            });
         }
         const served = resultShapeOf(
            await compiling(() =>
               runnable.getPreparedResult({
                  virtualMap,
                  givens: givens as never,
               }),
            ),
         );
         if (live !== served) {
            // Only a zoned truncation left out of each table read: that is
            // the column the result lacks.
            const zonedOnly = tablesRead.every((b) => {
               const id = b.sourceIDs?.[0] ?? "";
               const zoned = compiled.zonedLeftOut?.get(id);
               return (compiled.leftOutColumns?.get(id) ?? []).every(
                  (n) => zoned?.has(n) === true,
               );
            });
            if (zonedOnly) {
               decline(ZONED_TRUNCATION_REASON, "served_copy_time_zone");
            }
            // The tables read that leave a column out, and why for each
            // column live's result has and the copy's lacks: a column of the
            // caller's own result, named as a query naming it would be.
            const leaving = tablesRead.filter((b) =>
               compiled.leftOutTables?.has(b.tablePath),
            );
            const lacking = namesLacking(live, served);
            const why = [
               ...new Set(
                  leaving.flatMap((b) =>
                     lacking.flatMap((n) => {
                        const phrase = compiled.leftOutWhy
                           ?.get(b.sourceIDs?.[0] ?? "")
                           ?.get(n);
                        return phrase === undefined ? [] : [phrase];
                     }),
                  ),
               ),
            ];
            decline(
               "the query's result includes a column the served copy leaves out of a stored table's type",
               "served_copy_not_served",
               why.length > 0
                  ? `the stored table cannot answer with live's columns: ${why.join("; ")}`
                  : "the query's result includes a column the stored table leaves out",
               leaving.map((b) => b.virtualHandle),
            );
         }
      }
   }

   /**
    * A query that reads a table in another zone than it was built in
    * ({@link readsInAnotherZone}) is answered live, whatever it reads, when
    * the table's rows depend on the zone: a `where:` the build evaluated in
    * its source's zone, a joined source's fields stored as a record (an
    * inlined join, a pair table's children), each as the build computed it
    * in its source's zone; or a rolling window, whose cut-off moves: Malloy
    * takes its offset in the zone's wall time, so across a daylight-saving
    * change a read at the build's own clock needs up to an hour older than
    * the build stored. A table's zone-dependent columns alone are decided
    * per column (`checkZonedColumns`).
    */
   private async checkTimeZone(
      attempt: AnswerAttempt,
      tablesRead: readonly Binding[],
   ): Promise<void> {
      const { compiled, decline, prepared } = attempt;
      const zoned = tablesRead.filter(
         (b) =>
            compiled.computedTables?.has(b.tablePath) ||
            this.readsRollingWindow(b),
      );
      if (zoned.length === 0) return;
      const computed = zoned.some((b) =>
         compiled.computedTables?.has(b.tablePath),
      );
      const zones = readZonesOf(compiled, await prepared());
      if (zoned.some((b) => readsInAnotherZone(compiled, zones, b))) {
         decline(
            computed
               ? "the query reads a table in another time zone than it was built in (its rows, or a joined source's stored fields, depend on it)"
               : "the query reads a table in another time zone than it was built in, which moves a rolling window's cut-off past what its table stores",
            "served_copy_time_zone",
         );
      }
   }

   /**
    * What the join-to-rollup branch builds on to answer a query in a
    * storage destination: the copy's re-rooted files
    * and connections, over which it compiles the query's source with one
    * join re-pointed at a joined source's rollup in the same destination,
    * and the checks this copy runs on a query it answers, for the tables
    * that variant reads. Undefined when the copy re-roots nothing.
    */
   async joinRollupBasis(
      pinned: Pinned,
   ): Promise<ServedCopyJoinBasis | undefined> {
      const compiled = await this.ensureCompiled(pinned);
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
    * no check here: the join branch declines every query that sets a
    * `timezone:` (`TIME_ZONE`) before it compiles one. Throws a
    * {@link LegDecline}.
    */
   private async checkJoinVariant(
      compiled: Compiled,
      args: {
         sourceID: string;
         runnable: QueryMaterializer;
         queryString: string;
         tablesRead: readonly Binding[];
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
   private readsRollingWindow(binding: Binding): boolean {
      return (binding.sourceIDs ?? []).some(
         (id) =>
            rollingWindowsOf(this.materializePlans[id]?.readTimeTerms).length >
            0,
      );
   }

   /**
    * `decline` read again against the pin: a `served_copy_not_served` that
    * the pin caused, because a table on the queried source's chain is past
    * its freshness window and was left out of the copy, is
    * `served_copy_stale`. Anything else is returned as it is.
    */
   async staleDeclineFor(
      decline: unknown,
      authorRunnable: QueryMaterializer,
      pinned: Pinned,
   ): Promise<unknown> {
      if (
         !(decline instanceof LegDecline) ||
         decline.code !== "served_copy_not_served"
      ) {
         return decline;
      }
      const fresh = new Set(this.freshBindings(pinned));
      const stale = pinned.snapshot.bindings.filter(
         (b) => this.usable(b) && !fresh.has(b),
      );
      if (stale.length === 0) return decline;
      let chain: string[];
      try {
         const prepared = await readPreparedQuery(authorRunnable);
         const modelDef = prepared.modelDef ?? this.host.modelDef();
         chain = SourceMap.of(modelDef).chainOf(
            resolveStructRefSource(prepared.structRef, modelDef as never),
         );
      } catch {
         return decline;
      }
      return staleOnChain(stale, chain)
         ? new LegDecline("served_copy_stale", undefined, {
              message: "a table on the queried source's chain is stale",
           })
         : decline;
   }

   /** The bindings the copy may read at the pin. */
   private freshBindings(pinned: Pinned): Binding[] {
      return pinned.fresh.filter((b) => this.usable(b));
   }

   /**
    * The compiled copy for `lane` ({@link CompiledCopyLanes.ensure}): every
    * pair table that stores several inlined paths is read for its block of
    * that number (its last, past its own count), the rest of the copy as at
    * lane 0.
    */
   private ensureCompiled(pinned: Pinned, lane = 0): Promise<Compiled> {
      return this.lanes.ensure(
         this.freshBindings(pinned),
         lane,
         pinned.asOf.getTime(),
      );
   }

   /**
    * The re-root for one binding, when it stores a source this model
    * declares (directly or through an import), else undefined.
    */
   private rerootFor(
      binding: Binding,
      map: SourceMap,
      problems: ServedCopyProblem[],
      reservedNames: readonly string[],
      lane = 0,
      /**
       * Keep a query-shaped source's zoned truncations, restated, for the
       * read checks a query runs (`checkZonedColumns`); a chained build runs
       * none, so it leaves them out.
       */
      restatesStageZoned = false,
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
      const leftOutWhy = new Map<string, string>();
      const subject: RerootSubject = {
         binding,
         node,
         sourceName,
         def,
         fieldsByName,
         timeframeOf,
         problems,
         storesOutputNames: !plan,
         columnRef: columnRefOf(plan, fieldsByName),
         leftOutWhy,
      };
      if (plan) {
         const stored = materializedColumnsOf(subject, plan);
         if (!stored) return undefined;
         ({ columns, columnsLeftOut, inlinedJoins } = stored);
      } else {
         const stored = v1ColumnsOf(subject);
         if (!stored) return undefined;
         ({ columns, columnsLeftOut } = stored);
      }
      const declarable = this.declarableColumns(subject, {
         columns,
         columnsLeftOut,
      });
      if (!declarable) return undefined;
      ({ columns, columnsLeftOut } = declarable);
      const zoned = zonedTruncations(
         columns,
         def,
         fieldsByName,
         chainZoneOf(map, def) ?? inlineBaseZoneOf(def),
         restatesStageZoned,
      );
      if (zoned.leftOut.length > 0) {
         // Left out like any column no read gives exactly, with its own
         // reason when a query reads it (`readsZonedTruncation`).
         columns = zoned.columns;
         columnsLeftOut = [...columnsLeftOut, ...zoned.leftOut];
         if (columns.length === 0) {
            problems.push({
               sourceName,
               sourceID: node.sourceID,
               reason: "column_types",
               ...leftOutDetail(subject, columnsLeftOut),
            });
            return undefined;
         }
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
            // The leaf's key check, as the run that wrote the table recorded
            // it beside the table (`pairLeafKeyChecks`), read only while it
            // checked the key the leaf declares now: the plan a reload keeps
            // names the key at load, and nothing re-addresses the table when
            // the leaf's primary key moves.
            (sourceID) =>
               checkedKeyIs(
                  binding.primaryKeyColumns,
                  sourceID,
                  primaryKeyOf(map.node(sourceID)?.def),
               ) && requireUnique(verdictOf(binding.primaryKeys?.[sourceID])),
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
         ...(typeof (node.def as { queryTimezone?: unknown }).queryTimezone ===
         "string"
            ? { zone: (node.def as { queryTimezone: string }).queryTimezone }
            : {}),
         columns,
         reservedNames,
         ...(plan ? planDimensionsOf(plan, columnsLeftOut) : {}),
         ...(zoned.restated.length > 0
            ? { restatedTruncations: zoned.restated }
            : {}),
         ...(zoned.leftOut.length > 0 ? { zonedLeftOut: zoned.leftOut } : {}),
         ...(zoned.stageZoned
            ? { stageZonedTruncations: zoned.stageZoned }
            : {}),
         ...(plan?.readTimeTerms?.length
            ? { readTimeTerms: plan.readTimeTerms }
            : {}),
         ...(inlinedJoins.length > 0 ? { inlinedJoins } : {}),
         ...(columnsLeftOut.length > 0
            ? {
                 columnsLeftOut,
                 leftOutWhy: leftOutWhyOf(subject, columnsLeftOut),
              }
            : {}),
         // An `except:` naming a column the table does not store has nothing
         // to exclude on any re-root, v1 or `#(materialize)`.
         dropsUnstoredExcepts: true as const,
         ...(!plan
            ? {
                 keepsIncludes: true as const,
                 outputNames: queryOutputNames(def.fields),
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
    * The re-root's columns as its `type:` can declare them: a big integer
    * read through its native type, and a column no declaration reads back as
    * live does left out (`columnsLeftOut`). Undefined, with a `column_types`
    * problem, when no column is left or one cannot be declared.
    */
   private declarableColumns(
      subject: RerootSubject,
      kept: { columns: ServedColumn[]; columnsLeftOut: string[] },
   ): { columns: ServedColumn[]; columnsLeftOut: string[] } | undefined {
      const { binding, node, sourceName, def, fieldsByName, problems } =
         subject;
      let { columns, columnsLeftOut } = kept;
      // A column live answers above 2^53 (a BIGINT) is
      // read through its native type, which keeps every digit; one no cast
      // reads exactly is left out, so only a query reading it answers live.
      const ownReads = bigIntegerReadsOf(
         columns,
         def.fields,
         def,
         binding,
         subject.storesOutputNames,
         this.host.readsStoredIntegers,
      );
      columns = columns.flatMap((c) => {
         if (!ownReads.has(c.name)) return [c];
         const nativeCast = ownReads.get(c.name);
         if (nativeCast) return [{ ...c, nativeCast }];
         columnsLeftOut.push(c.name);
         subject.leftOutWhy.set(c.name, leftOutPhraseOf(subject, c));
         return [];
      });
      // A stored scalar whose field live types as a warehouse-native type
      // (`sql native`: a UUID, a Postgres `jsonb` or composite stored as
      // text) would be declared a `string`: another type than live's and,
      // for some, other text. Left out, so only a query reading it answers
      // live. Decided from live's field type, never from a value.
      columns = columns.flatMap((c) => {
         if (
            !liveTypesAsNative(fieldsByName.get(c.name)) ||
            storedMalloyType(c.type) === undefined
         ) {
            return [c];
         }
         columnsLeftOut.push(c.name);
         subject.leftOutWhy.set(c.name, leftOutPhraseOf(subject, c));
         return [];
      });
      if (columns.length === 0) {
         problems.push({
            sourceName,
            sourceID: node.sourceID,
            reason: "column_types",
            ...leftOutDetail(subject, columnsLeftOut),
         });
         return undefined;
      }
      // A column the `type:` cannot declare faithfully would fail the whole
      // model's copy, not just this source. It is left out (a nest a
      // destination stores as a type Malloy cannot read back, a list or
      // struct whose field live types as something else, or a Postgres
      // `bytea` the build kept as a BLOB, say), so only a query that reads
      // it answers live; a source with no column left goes to its fallback.
      // A stored list or struct is declared in live's shape, or left out
      // when no declaration reads it back as live does
      // (`storedMalloyTypeFor`).
      const keepsBigIntegers = liveKeepsBigIntegers(
         (def as { dialect?: unknown }).dialect,
      );
      const nestedDeclaration = (c: ServedColumn) =>
         nestedDeclarationOf(
            c.type,
            fieldsByName.get(c.name),
            keepsBigIntegers,
         );
      columns = columns.map((c) => {
         const declared = nestedDeclaration(c);
         return declared && declared !== storedMalloyType(c.type)
            ? { ...c, declaredType: declared }
            : c;
      });
      const undeclarableColumn = (c: ServedColumn) =>
         storedMalloyType(c.type) === undefined ||
         nestedDeclaration(c) === undefined;
      const undeclarable = columns
         .filter(undeclarableColumn)
         .map((c) => c.name);
      for (const c of columns.filter(undeclarableColumn)) {
         subject.leftOutWhy.set(
            c.name,
            leftOutPhrase(
               subject.columnRef(c.name),
               fieldsByName.get(c.name),
               c.type,
            ),
         );
      }
      if (undeclarable.length > 0 && undeclarable.length < columns.length) {
         columns = columns.filter((c) => !undeclarable.includes(c.name));
         columnsLeftOut = [...columnsLeftOut, ...undeclarable];
      }
      if (columns.some(undeclarableColumn)) {
         problems.push({
            sourceName,
            sourceID: node.sourceID,
            reason: "column_types",
            detail: undeclarable
               .map((n) => subject.leftOutWhy.get(n))
               .join("; "),
         });
         return undefined;
      }
      return { columns, columnsLeftOut };
   }

   /**
    * A source this model reaches only as a join: its declaration, as
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
    * The trailing `extend` runs of a `#(materialize)` source's ancestors, in
    * its own file or the files it imports, root first
    * ({@link keptAncestorChainOrDeclared}, confirmed against each file's
    * text), which its re-root keeps ahead of its own blocks. Kept
    * for an extension table, whose table holds every one of their rows
    * and stored columns, so it loses no measure, join or view any of them
    * declares; and for one whose bases declare a measure, view or join it
    * inherits, which the re-root would otherwise lose. Undefined when
    * the source is neither, or no such ancestor is on the walk: a source with
    * none keeps only its own blocks (decision 2). A base's given-reading
    * `where:` or dimension is never left out of a build (it refuses the
    * source at plan), so no block is kept to re-apply one.
    */
   private keptBlocksFor(
      reroot: Reroot & { sourceID: string },
      map: SourceMap,
      fileText: (url: string) => string | undefined,
   ): string | undefined {
      const plan = this.materializePlans[reroot.sourceID];
      if (!plan) return undefined;
      // Across files: an extension in a file that imports its base's file
      // keeps that base's blocks too, so it is re-rooted on its own table
      // rather than left reading its base's.
      const confirmed = confirmedAncestors(
         fileText,
         reroot.url,
         reroot.range,
         keptAncestorChainOrDeclared(
            map,
            reroot.sourceID,
            reroot.url,
            fileText,
         ),
      );
      const needed =
         confirmed.some(
            (a) => this.materializePlans[a.sourceID] !== undefined,
         ) ||
         // A base the re-root replaces declares a measure, view or join
         // the source inherits; only its block, written again over the
         // table, keeps it (else the source is not carried, call 2).
         confirmed.some((a) => declaresBlocks(map.node(a.sourceID)?.def));
      if (!needed) return undefined;
      return confirmed
         .flatMap((a) => (a.runText ? [a.runText] : []))
         .reverse()
         .join(" ");
   }

   /**
    * The files a chained `storage=` build of `downstreamSourceID` compiles
    * on its destination alone: each bound upstream re-rooted in the file
    * that declares it, as this served copy re-roots it ({@link
    * rerootCandidates}), and every declaration off the downstream's path
    * blanked (`chainBuildFileSet`), so nothing compiled reads the
    * warehouse. Declines, with why, when an upstream does not re-root, the
    * downstream reaches past its re-rooted upstreams, or a file it reaches
    * cannot be read. Compiles nothing: the build compiles the files on its
    * own session.
    */
   chainBuildFiles(
      downstreamSourceID: string,
      bindings: readonly Binding[],
      /**
       * A persisted view's or named query's build: the author's declaration
       * it runs, kept by name, and its generated build file as the entry,
       * declaring `view.downstreamName`.
       */
      view?: NonNullable<ChainBuildRequest["view"]> & {
         downstreamName: string;
      },
   ): ChainBuildText {
      const modelDef = this.host.modelDef();
      const map = SourceMap.of(modelDef);
      const lift = authorModelLiftContext(
         modelDef,
         compiledTextReader(modelDef, this.packagePath),
      );
      const problems: ServedCopyProblem[] = [];
      const { candidates } = this.rerootCandidates(
         bindings.filter((b) => b.kind !== "rollup"),
         map,
         lift,
         problems,
         0,
      );
      const why = (detail: string): ChainBuildText => ({ declined: detail });
      if (candidates.length === 0) {
         return why(
            problems.length > 0
               ? `no stored upstream could be re-rooted (${problems
                    .map((p) => `'${p.sourceName}': ${p.reason}`)
                    .join(", ")})`
               : "no stored upstream could be re-rooted",
         );
      }
      const elsewhere = zonedElsewhere(map, downstreamSourceID, candidates);
      if (elsewhere.length > 0) {
         return why(
            `the downstream reads ${elsewhere
               .map((n) => `'${n}'`)
               .join(
                  ", ",
               )} in another time zone than its table was built in, and a value stored in one zone (a restated timestamp truncation, a computed dimension) is not the one it computes in another`,
         );
      }
      const set = chainBuildFileSet({
         lift,
         downstreamSourceID,
         ...(view
            ? {
                 downstreamName: view.authorName,
                 ...(view.keepAlso ? { keepAlso: view.keepAlso } : {}),
              }
            : {}),
         bindings,
         reroots: candidates.map((c) => c.reroot),
         importsOf: compiledImports(modelDef),
      });
      if ("declined" in set) return set;
      const blanked = set.files;
      const copy = buildServedCopy(
         candidates.map((c) => c.reroot),
         (u) => blanked.get(u),
      );
      if (copy.refused.length > 0) {
         return why(
            `a stored upstream did not re-root (${copy.refused
               .map((r) => `'${r.sourceName}': ${r.reason}`)
               .join(", ")})`,
         );
      }
      const files = new Map(blanked);
      for (const [url, text] of copy.files) files.set(url, text);
      const rerootedSpans = [...copy.spans].flatMap(([url, list]) =>
         list.map((span) => ({
            url,
            start: span.declaration.start,
            end: span.declaration.end,
         })),
      );
      withPersistenceFlag(files, set.entry, rerootedSpans);
      if (view) files.set(view.companion.url, view.companion.text);
      const aliased = candidates.map((c) => ({
         ...c.binding,
         destinationName: servedConnectionOf(c.binding),
      }));
      return {
         files,
         entry: view ? view.companion.url : set.entry,
         downstreamName: view ? view.downstreamName : set.downstreamName,
         connection: aliased[0].destinationName,
         virtualMap: buildVirtualMap(aliased),
         rerooted: candidates.map((c) => c.reroot.sourceName),
         rerootedSpans,
      };
   }

   /**
    * The text a `storage=` rollup's build over its base's stored table
    * compiles from on the destination alone: the base re-rooted on that
    * table as this served copy re-roots it for a query, nothing else of the
    * author's files, and the rollup's generated build file as the entry, so
    * the rollup groups exactly the rows a query of the base reads there.
    * Declines when the base does not re-root on one of `bindings`.
    */
   rollupBuildFiles(
      rollup: NonNullable<ChainBuildRequest["rollup"]>,
      downstreamName: string,
      bindings: readonly Binding[],
   ): ChainBuildText {
      const modelDef = this.host.modelDef();
      const map = SourceMap.of(modelDef);
      const lift = authorModelLiftContext(
         modelDef,
         compiledTextReader(modelDef, this.packagePath),
      );
      const problems: ServedCopyProblem[] = [];
      const { candidates } = this.rerootCandidates(
         bindings.filter((b) => b.kind !== "rollup"),
         map,
         lift,
         problems,
         0,
      );
      const base = candidates.find(
         (c) => c.reroot.sourceID === rollup.baseSourceID,
      );
      if (!base) {
         return {
            declined:
               problems.length > 0
                  ? `the base did not re-root on its stored table (${problems
                       .map((p) => `'${p.sourceName}': ${p.reason}`)
                       .join(", ")})`
                  : "the base did not re-root on its stored table",
         };
      }
      const set = rollupBuildFileSet({
         lift,
         baseSourceID: rollup.baseSourceID,
         reroot: base.reroot,
         importsOf: compiledImports(modelDef),
      });
      if ("declined" in set) return set;
      const blanked = set.files;
      const copy = buildServedCopy([base.reroot], (u) => blanked.get(u));
      if (copy.refused.length > 0) {
         return {
            declined: `the base did not re-root (${copy.refused
               .map((r) => `'${r.sourceName}': ${r.reason}`)
               .join(", ")})`,
         };
      }
      const files = new Map(blanked);
      for (const [url, text] of copy.files) files.set(url, text);
      files.set(rollup.companion.url, rollup.companion.text);
      const rerootedSpans = [...copy.spans].flatMap(([url, list]) =>
         list.map((span) => ({
            url,
            start: span.declaration.start,
            end: span.declaration.end,
         })),
      );
      const aliased = {
         ...base.binding,
         destinationName: servedConnectionOf(base.binding),
      };
      return {
         files,
         entry: rollup.companion.url,
         downstreamName,
         connection: aliased.destinationName,
         virtualMap: buildVirtualMap([aliased]),
         rerooted: [base.reroot.sourceName],
         rerootedSpans,
      };
   }

   /**
    * Each binding this model can re-root, with its re-root
    * ({@link rerootFor}), less the sources that read a re-rooted source by
    * name and so stay as written. `throughBase` names those; `withoutKept`
    * maps an extension table's re-root to its fallback without its
    * ancestors' kept blocks. Shared by the served copy's compile and the
    * chained build's text, so both re-root one way.
    */
   private rerootCandidates(
      bindings: readonly Binding[],
      map: SourceMap,
      lift: ReturnType<typeof authorModelLiftContext>,
      problems: ServedCopyProblem[],
      lane: number,
      /**
       * Storage bindings of sources bound in both tiers to pass over for
       * their database twin: a compile already dropped them.
       */
      passedOver: ReadonlySet<Binding> = new Set(),
      /** Filled with each source bound in both tiers: its storage binding's database twin. */
      twins?: Map<Binding, Binding>,
      /** {@link rerootFor}'s `restatesStageZoned`: true for a query's compile. */
      restatesStageZoned = false,
   ): {
      candidates: { binding: Binding; reroot: AuthorReroot }[];
      throughBase: NonNullable<Compiled["throughBase"]>[number][];
      withoutKept: Map<
         { binding: Binding; reroot: AuthorReroot },
         { binding: Binding; reroot: AuthorReroot } | undefined
      >;
   } {
      let candidates: {
         binding: Binding;
         reroot: AuthorReroot;
      }[] = [];
      const reservedNames = modelFieldNames(map);
      for (const binding of bindings) {
         const reroot = this.rerootFor(
            binding,
            map,
            problems,
            reservedNames,
            lane,
            restatesStageZoned,
         );
         if (reroot) {
            candidates.push({
               binding,
               reroot: reroot as AuthorReroot,
            });
         }
      }
      // A source bound in both tiers is re-rooted on one table: two re-roots
      // of one declaration splice it twice, and both are refused. The storage
      // table, unless a compile already dropped it (`passedOver`): then its
      // database twin.
      const keyOf = (c: (typeof candidates)[number]) =>
         rerootKey(c.reroot.url, c.reroot.sourceName);
      const byKey = new Map<string, (typeof candidates)[number][]>();
      for (const c of candidates) {
         byKey.set(keyOf(c), [...(byKey.get(keyOf(c)) ?? []), c]);
      }
      for (const same of byKey.values()) {
         const storage = same.find((c) => c.binding.tier === "storage");
         const database = same.find((c) => c.binding.tier === "database");
         if (storage && database) twins?.set(storage.binding, database.binding);
      }
      candidates = candidates.filter((c) => {
         if (passedOver.has(c.binding)) return false;
         const tiers: Tier[] = (byKey.get(keyOf(c)) ?? [])
            .filter((o) => !passedOver.has(o.binding))
            .map((o) => o.binding.tier);
         return servedCopyReadsTier(c.binding.tier, tiers);
      });
      // An extension of a re-rooted source reads it by name already, and
      // re-rooting the extension too would replace the base it inherits its
      // parent's fields from. Leave it as written, unless it is an extension
      // table: a `#(materialize)` source with its own table, whose
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
      // holds, so it is re-rooted on that table even when its base is too.
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
      // back to the extension as written when it reads a
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
      return { candidates, throughBase, withoutKept };
   }

   /**
    * The copy over `bindings`: compiled once, and again for each source
    * bound in both tiers whose storage table the passes dropped (a field it
    * does not carry, a refusal, the ladder), re-rooted then on its database
    * table, as if the storage one were not bound.
    */
   private async compile(
      key: string,
      generation: number,
      bindings: Binding[],
      lane = 0,
      /** The pin's time, which a remembered failure is held against. */
      asOf = 0,
   ): Promise<Compiled> {
      try {
         return await this.compileWithFallbacks(
            key,
            generation,
            bindings,
            lane,
            asOf,
         );
      } catch (error) {
         // A failure the passes did not catch is the copy failing to compile,
         // recorded so a verdict judges it (every source answers live) rather
         // than reading it pending forever. It may be transient, so the next
         // request compiles again, and a verdict read does once the backoff
         // has passed.
         const failed: Compiled = {
            key,
            generation,
            over: bindings,
            lane,
            rerooted: [],
            aliased: [],
            escaped: true,
            problems: [
               {
                  reason: "compile_failed",
                  detail:
                     error instanceof Error ? error.message : String(error),
               },
            ],
         };
         setTimeout(() => {
            failed.retryDue = true;
         }, ESCAPED_FAILURE_RETRY_MS).unref?.();
         return failed;
      }
   }

   private async compileWithFallbacks(
      key: string,
      generation: number,
      bindings: Binding[],
      lane: number,
      asOf: number,
   ): Promise<Compiled> {
      const passedOver = new Set<Binding>();
      // Why each storage table passed over was dropped, from the compile
      // that dropped it: still warned, as served from the database instead.
      const fellBack: ServedCopyProblem[] = [];
      for (;;) {
         const twins = new Map<Binding, Binding>();
         const compiled = await this.compileOver(
            key,
            generation,
            bindings,
            lane,
            asOf,
            passedOver,
            twins,
         );
         const rerooted = new Set(compiled.rerooted);
         const dropped = [...twins.keys()].filter(
            (b) => !passedOver.has(b) && !rerooted.has(b),
         );
         if (dropped.length === 0) {
            if (fellBack.length === 0) return compiled;
            // Only for a source the database table now serves.
            const onDatabase = new Set(
               compiled.rerooted.flatMap((b) => b.sourceIDs ?? []),
            );
            return {
               ...compiled,
               problems: [
                  ...compiled.problems,
                  ...fellBack.filter(
                     (p) =>
                        p.sourceID !== undefined && onDatabase.has(p.sourceID),
                  ),
               ],
            };
         }
         const droppedIDs = new Set(dropped.flatMap((b) => b.sourceIDs ?? []));
         for (const p of compiled.problems) {
            if (p.sourceID !== undefined && droppedIDs.has(p.sourceID)) {
               fellBack.push({ ...p, servedFromDatabase: true });
            }
         }
         for (const b of dropped) passedOver.add(b);
      }
   }

   private async compileOver(
      key: string,
      generation: number,
      bindings: Binding[],
      lane: number,
      asOf: number,
      passedOver: ReadonlySet<Binding>,
      twins: Map<Binding, Binding>,
   ): Promise<Compiled> {
      const over = bindings;
      const problems: ServedCopyProblem[] = [];
      // Storage bindings reach here only with destinations (`usable`); a
      // copy of database tables alone needs none.
      const destinations =
         this.host.serveDestinationConfig() ?? NO_DESTINATIONS;
      // No table to re-root on: the copy is the author's model, so there is
      // nothing to read or compile.
      if (!this.packagePath || !this.packageConfig || bindings.length === 0) {
         return {
            key,
            generation,
            over,
            lane,
            rerooted: [],
            aliased: [],
            problems,
         };
      }
      const modelDef = this.host.modelDef();
      const map = SourceMap.of(modelDef);
      const lift = authorModelLiftContext(
         modelDef,
         compiledTextReader(modelDef, this.packagePath),
      );
      const prepared = this.rerootCandidates(
         bindings,
         map,
         lift,
         problems,
         lane,
         passedOver,
         twins,
         true,
      );
      const { throughBase, withoutKept } = prepared;
      const state: CopyPassState = {
         asOf,
         candidates: prepared.candidates,
         withoutKept,
         inlinedKept: [],
         pairsDropped: [],
         problems,
      };
      // At most three passes per candidate: each pass either compiles with
      // every remaining source whole, or takes one step down a source's
      // ladder (its inlined joins kept as written, its kept blocks dropped,
      // the source dropped: see `contain`), or drops the sources it lost
      // fields on; and one more each that keeps every inlined join as written
      // and drops the pair tables.
      const maxPasses = 3 * state.candidates.length + 3;
      // A pass compiled inline is a full compile on the main thread: hand
      // the loop a turn between them, so a long ladder does not hold it
      // throughout.
      const yielder = createEventLoopYielder();
      for (let pass = 0; pass <= maxPasses; pass++) {
         if (pass > 0) await yielder.maybeYield();
         const copy = buildServedCopy(
            state.candidates.map((c) => c.reroot),
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
               // Keyed by the refused re-root's sourceID, as every other
               // problem of one source is (the dry run reads it by that).
               const sourceID = state.candidates.find(
                  (c) =>
                     c.reroot.url === r.url &&
                     c.reroot.sourceName === r.sourceName,
               )?.reroot.sourceID;
               problems.push({
                  sourceName: r.sourceName,
                  ...(sourceID ? { sourceID } : {}),
                  reason: r.reason,
                  ...(r.hiddenUnstored
                     ? { hiddenUnstored: r.hiddenUnstored }
                     : {}),
               });
            }
         }
         state.candidates = state.candidates.filter(
            (c) => !refused.has(`${c.reroot.url}\u0000${c.reroot.sourceName}`),
         );
         if (state.candidates.length === 0) {
            // Nothing left to compile: a pair table dropped was the only one.
            problems.push(...state.pairsDropped);
            return {
               key,
               generation,
               over,
               lane,
               rerooted: [],
               aliased: [],
               problems,
            };
         }
         const rerooted = state.candidates.map((c) => c.binding);
         const aliased = rerooted.map((b) => ({
            ...b,
            destinationName: servedConnectionOf(b),
         }));
         const overlay = new Map(copy.files);
         const packagePath = this.packagePath;
         const config = servedCopyConfig(
            this.packageConfig,
            destinations(),
            new Set(
               rerooted
                  .filter((b) => !isDatabase(b))
                  .map((b) => b.destinationName),
            ),
         );
         const load = this.passLoader(packagePath, config);
         const loaded = await this.loadCopy(state, copy, overlay, load, lift);
         if (loaded === "retry") continue;
         if (loaded === "failed") {
            return {
               key,
               generation,
               over,
               lane,
               rerooted: [],
               aliased: [],
               problems,
            };
         }
         const { materializer, servedDef, servedRuntime, leftOut, files } =
            loaded;
         // Only a join of a source still re-rooted was kept for it.
         problems.push(
            ...state.inlinedKept.filter((p) =>
               state.candidates.some(
                  (c) => c.reroot.sourceName === p.sourceName,
               ),
            ),
         );
         state.inlinedKept = [];
         problems.push(...state.pairsDropped);
         state.pairsDropped = [];
         const servedMap = SourceMap.of(servedDef);
         if (rerootsLosing(state, copy, map, servedMap, leftOut)) continue;
         return this.compiledCopy(state, {
            key,
            generation,
            over,
            lane,
            modelDef,
            servedMap,
            servedDef,
            servedRuntime,
            materializer,
            leftOut,
            files,
            config,
            rerooted,
            aliased,
            throughBase,
         });
      }
      return {
         key,
         generation,
         over,
         lane,
         rerooted: [],
         aliased: [],
         problems,
      };
   }

   /**
    * How a pass's files are compiled into a model: on the host's other
    * thread when it takes them ({@link ServedCopyHost.compileElsewhere}),
    * else here, as `packagePath`'s model over them with `config`'s
    * connections.
    */
   private passLoader(packagePath: string, config: MalloyConfig): CopyLoad {
      const modelPath = this.host.modelPath();
      const compiler = packageFiles(packagePath, modelPath, config);
      return async (files) => {
         const elsewhere = await this.host.compileElsewhere?.({
            packagePath,
            modelPath,
            config,
            files: keyedOverlay(files),
         });
         if (elsewhere) return elsewhere;
         const { runtime, materializer } = await compiler.loadModel(files);
         const model = await materializer.getModel();
         return {
            materializer,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            def: modelDefOf(model as any),
            runtime: runtime as unknown as GraftRuntime,
         };
      };
   }

   /**
    * Forget the failures remembered for this package's connections: the
    * destinations or the warehouse changed, so a copy that failed may not.
    */
   forgetFailures(): void {
      const config = this.packageConfig;
      if (typeof config === "object" && config) {
         knownRerootFailures.delete(config);
      }
   }

   /** The failure remembered for a copy's key, while it is held. */
   private knownFailure(
      key: string | undefined,
      asOf: number,
   ): { error: unknown } | undefined {
      const config = this.packageConfig;
      // A copy pinned at no time (a preview or dry run) neither reads nor
      // writes the memo: it has no clock to expire one against.
      if (
         key === undefined ||
         asOf <= 0 ||
         typeof config !== "object" ||
         !config
      ) {
         return undefined;
      }
      const byKey = knownRerootFailures.get(config);
      const known = byKey?.get(key);
      if (!known) return undefined;
      if (known.until <= asOf) {
         byKey!.delete(key);
         return undefined;
      }
      return known;
   }

   /** Remember that a copy's compile failed on a re-root's own text. */
   private rememberFailure(
      key: string | undefined,
      error: unknown,
      asOf: number,
   ): void {
      const config = this.packageConfig;
      if (
         key === undefined ||
         asOf <= 0 ||
         typeof config !== "object" ||
         !config
      ) {
         return;
      }
      let byKey = knownRerootFailures.get(config);
      if (!byKey) {
         byKey = new Map();
         knownRerootFailures.set(config, byKey);
      }
      // Oldest first out, so a package with many failing copies stays bounded.
      if (byKey.size >= REROOT_FAILURES_PER_CONFIG) {
         byKey.delete(byKey.keys().next().value!);
      }
      byKey.set(key, { error, until: asOf + REROOT_FAILURE_TTL_MS });
   }

   /**
    * Load one pass's copy. On a failure, take the step down the ladder it
    * calls for (a left-out column's source, containment, cross-connection
    * joins left out, inlined joins kept, pair tables or extension tables
    * dropped) and ask for another pass (`retry`), or give the copy up
    * (`failed`, with the problem reported).
    */
   private async loadCopy(
      state: CopyPassState,
      copy: ServedCopyText,
      overlay: ReadonlyMap<string, string>,
      load: CopyLoad,
      lift: ReturnType<typeof authorModelLiftContext>,
   ): Promise<LoadedCopy | "retry" | "failed"> {
      const { withoutKept, problems } = state;
      let materializer: ModelMaterializer;
      let servedDef: unknown;
      let servedRuntime: GraftRuntime;
      let leftOut: LeftOutDefinition[] = [];
      let files: ReadonlyMap<string, string> = overlay;
      const failureKey = this.packagePath
         ? rerootFailureKey(
              this.packagePath,
              this.host.modelPath(),
              state.candidates.map((c) => c.binding),
              overlay,
           )
         : undefined;
      const known = this.knownFailure(failureKey, state.asOf);
      try {
         if (known) throw known.error;
         ({
            materializer,
            def: servedDef,
            runtime: servedRuntime,
         } = await load(overlay));
      } catch (err) {
         // A destination catalog the worker lost (connection.ts reopens it)
         // is not the copy's text: neither contained to a source nor
         // remembered, or a failure recorded while the destination reopens
         // would outlive the invalidation that reopen triggers. A compile
         // reports it as a `failed-to-fetch-table-schema` problem quoting the
         // typed error, which the check reads too.
         if (isDuckLakeCatalogUnavailable(err)) {
            problems.push({
               reason: "compile_failed",
               detail: err instanceof Error ? err.message : String(err),
            });
            return "failed";
         }
         if (withoutLeftOutColumn(state, copy, err)) return "retry";
         if (this.contain(state, copy, err, copy.files, "precise", !!known)) {
            if (!known) this.rememberFailure(failureKey, err, state.asOf);
            return "retry";
         }
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
         if (!kept) {
            if (keepInlinedJoins(state, err)) return "retry";
            const detail = err instanceof Error ? err.message : String(err);
            if (withoutPairs(state, detail)) return "retry";
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
               state.candidates = state.candidates.flatMap((c) => {
                  if (!withoutKept.has(c)) return [c];
                  const fallback = withoutKept.get(c);
                  return fallback ? [fallback] : [];
               });
               withoutKept.clear();
               return "retry";
            }
            if (this.contain(state, copy, err, copy.files, "last_resort"))
               return "retry";
            problems.push({
               reason: classifyServedCopyFailure(err),
               detail,
            });
            return "failed";
         }
         try {
            files = keyedOverlay(kept.overlay);
            ({
               materializer,
               def: servedDef,
               runtime: servedRuntime,
            } = await load(files));
         } catch (again) {
            if (this.contain(state, copy, again, kept.overlay, "precise"))
               return "retry";
            if (keepInlinedJoins(state, again)) return "retry";
            const detail =
               again instanceof Error ? again.message : String(again);
            if (withoutPairs(state, detail)) return "retry";
            if (this.contain(state, copy, again, kept.overlay, "last_resort"))
               return "retry";
            problems.push({ reason: "compile_failed", detail });
            return "failed";
         }
         leftOut = kept.leftOut;
      }
      return { materializer, servedDef, servedRuntime, leftOut, files };
   }

   /**
    * Containment (served_copy_containment.ts): a failure traced to one
    * re-root's own text costs that source alone. Its ladder: a source
    * that inlines join_ones keeps them as written first (the record is
    * the one text the copy writes over a join's place, as
    * keepInlinedJoins below, but for this source only); an extension
    * table falls back to its own blocks (or to as written); any other
    * source is dropped and answers live, with a coded warning. Each
    * step is one pass; the rest of the model compiles again.
    */
   private contain(
      state: CopyPassState,
      copy: ServedCopyText,
      error: unknown,
      files: ReadonlyMap<string, string>,
      mode: TraceMode,
      /** The failure was remembered, not compiled now: warned of already. */
      remembered = false,
   ): boolean {
      const { withoutKept, problems } = state;
      const failing = new Map(
         failingReroots(error, files, copy.spans, mode).map((f) => [
            rerootKey(f.url, f.sourceName),
            f,
         ]),
      );
      if (failing.size === 0) return false;
      const detail = error instanceof Error ? error.message : String(error);
      let changed = false;
      state.candidates = state.candidates.flatMap((c) => {
         const f = failing.get(rerootKey(c.reroot.url, c.reroot.sourceName));
         if (!f) return [c];
         changed = true;
         logger[this.host.quiet || remembered ? "debug" : "warn"](
            "A re-root does not compile in the served copy; that source alone steps back",
            {
               modelPath: this.host.modelPath(),
               sourceName: c.reroot.sourceName,
            },
         );
         if (c.reroot.inlinedJoins?.length) {
            state.inlinedKept.push(
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
            reason: c.reroot.pair ? "pair_not_compiled" : "reroot_not_compiled",
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
   }

   /**
    * The compiled copy, once every source still re-rooted compiles and keeps
    * what the author's has: its re-rooted tables, what each answer checks of
    * them, and the sources it declines (opted out, or flagged by the
    * classification checks).
    */
   private compiledCopy(state: CopyPassState, built: BuiltCopy): Compiled {
      const { candidates, problems } = state;
      const { key, generation, over, lane, modelDef, servedMap } = built;
      const { servedDef, servedRuntime, materializer, leftOut } = built;
      const { files, config, rerooted, aliased, throughBase } = built;
      // Every declaration the model reaches, imported ones included.
      const optOutIDs = new Set<string>();
      for (const node of servedMap.nodes()) {
         if (optsOutOfPersist(node.def as DerivedSourceDef)) {
            optOutIDs.add(node.sourceID);
         }
      }
      // The sources stored at all: a binding the copy was compiled over.
      const storedNames = new Set(over.map((b) => b.sourceName));
      for (const l of leftOut) {
         if (!l.crossConnection) continue;
         const sourceName = l.declaration ?? l.name;
         problems.push({
            sourceName,
            reason: "cross_connection_join",
            join: l.join ?? l.name,
            ...(l.join !== undefined ? { leftOutWhole: true as const } : {}),
            ...(storedNames.has(sourceName)
               ? {}
               : { declarerNotStored: true as const }),
         });
      }
      const unsafeIDs = classificationCeiling(
         this.host.modelPath(),
         modelDef,
         servedDef,
         servedMap,
         problems,
      );
      const compiled: Compiled = {
         key,
         generation,
         over,
         lane,
         // The zone each table was built in, read from the author's model:
         // a re-root replaces a source's base with its stored table, which
         // drops a zone it inherits from that base.
         authorModelDef: modelDef,
         builtZones: new Map(
            candidates.map((c) => [
               c.reroot.sourceID,
               chainZoneOf(SourceMap.of(modelDef), c.reroot.authorDef),
            ]),
         ),
         // An inlined join's columns are the joined source's fields as the
         // build computed them, in its time zone, and a pair table's child
         // columns too: read only as records, so any read in another zone is
         // declined, as one of a table whose rows depend on the zone is.
         computedTables: new Set(
            candidates
               .filter(
                  (c) =>
                     c.reroot.zonedRows === true ||
                     (c.reroot.inlinedJoins ?? []).length > 0 ||
                     (c.reroot.pair?.childStoredDimensions ?? []).length > 0,
               )
               .map((c) => c.binding.tablePath),
         ),
         // A stored computed dimension of the source's own, unless the probe
         // compiler found it the same in every zone, holds the value
         // the build computed in the source's zone; a restated timestamp
         // truncation is truncated again at read. Each is declined only
         // where a query reads it in another zone (`checkZonedColumns`).
         zonedColumns: new Map(
            candidates
               .map(
                  (c) =>
                     [
                        c.reroot.sourceID,
                        new Set([
                           ...(c.reroot.zonedDimensions ?? []),
                           ...(c.reroot.restatedTruncations ?? []),
                        ]),
                     ] as const,
               )
               .filter(([, names]) => names.size > 0),
         ),
         zonedLeftOut: new Map(
            candidates
               .filter((c) => c.reroot.zonedLeftOut?.length)
               .map((c) => [c.reroot.sourceID, new Set(c.reroot.zonedLeftOut)]),
         ),
         stageZonedTruncations: new Map(
            candidates.flatMap((c) => {
               const s = c.reroot.stageZonedTruncations;
               return s
                  ? [
                       [
                          c.reroot.sourceID,
                          { zone: s.zone, names: new Set(s.names) },
                       ] as const,
                    ]
                  : [];
            }),
         ),
         leftOutTables: new Set(
            candidates
               .filter((c) => (c.reroot.columnsLeftOut ?? []).length > 0)
               .map((c) => c.binding.tablePath),
         ),
         leftOutColumns: new Map(
            candidates
               .filter((c) => (c.reroot.columnsLeftOut ?? []).length > 0)
               .map((c) => [c.reroot.sourceID, c.reroot.columnsLeftOut!]),
         ),
         leftOutWhy: new Map(
            candidates
               .filter((c) => c.reroot.leftOutWhy !== undefined)
               .map((c) => [c.reroot.sourceID, c.reroot.leftOutWhy!]),
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
         rerootedSourceIDs: rerootedSourceIDsOf(candidates, throughBase),
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
      compiled.sourceDeclines = this.sourceDeclinesOf(compiled);
      return compiled;
   }

   /**
    * The declines that depend only on the source a query runs on and this
    * compile, for each source the copy names: a gate it cannot graft, an
    * opted-out or unsafe source on its chain, a zone it computes in that
    * differs from the author's. Recorded at the copy's compile, so a query
    * on a named source looks its declines up; only one on a source written
    * inline (`orders extend { … }`) walks.
    */
   private sourceDeclinesOf(
      compiled: Compiled,
   ): ReadonlyMap<string, SourceDeclines> {
      const out = new Map<string, SourceDeclines>();
      const contents = (compiled.modelDef as ModelDef | undefined)?.contents;
      for (const name of Object.keys(contents ?? {})) {
         const struct = resolveStructRefSource<{ as?: string; name?: string }>(
            name,
            compiled.modelDef as never,
         );
         if (struct === undefined) continue;
         out.set(name, this.declinesFor(compiled, name));
      }
      return out;
   }

   /** {@link sourceDeclinesOf} for the source `structRef` names: the walk. */
   private declinesFor(compiled: Compiled, structRef: unknown): SourceDeclines {
      const struct = resolveStructRefSource<{ as?: string; name?: string }>(
         structRef,
         compiled.modelDef as never,
      );
      const name = struct?.as ?? struct?.name;
      const graft =
         this.mayHaveRowLevelGates(compiled) &&
         name !== undefined &&
         this.gateCannotGraft(compiled, name)
            ? GATE_GRAFT_DECLINE
            : undefined;
      const optOut = readsOptOuts(compiled)
         ? optOutDeclineOf(compiled, structRef)
         : undefined;
      const zone =
         compiled.authorModelDef !== undefined &&
         sourceZoneDiffers(compiled, structRef)
            ? SOURCE_ZONE_DECLINE
            : undefined;
      return {
         ...(graft ? { graft } : {}),
         ...(optOut ? { optOut } : {}),
         ...(zone ? { zone } : {}),
      };
   }
}

/** The decline for a row-level gate the copy cannot graft (checkGateGraft). */
const GATE_GRAFT_DECLINE: SourceDecline = {
   reason:
      "a row-level gate on the queried source was declared on a source the served copy re-roots, so it cannot be grafted here",
   code: "served_copy_not_served",
};

/** A served copy's text, as {@link buildServedCopy} writes it. */
type ServedCopyText = ReturnType<typeof buildServedCopy>;

/** A pass's copy, read into a runtime. */
type CopyLoad = (files: ReadonlyMap<string, string>) => Promise<{
   materializer: ModelMaterializer;
   def: unknown;
   runtime: GraftRuntime;
}>;

/** A pass's copy, loaded: what the bind guard and the compile read. */
interface LoadedCopy {
   materializer: ModelMaterializer;
   servedDef: unknown;
   servedRuntime: GraftRuntime;
   leftOut: LeftOutDefinition[];
   files: ReadonlyMap<string, string>;
}

/**
 * The author sources a compiled copy reads from a stored table: each one
 * still re-rooted past the passes, and each extension left as written whose
 * base is one of those (by its chain, or the base name its text declares).
 * An extension whose base a pass dropped reads that base as written.
 */
export function rerootedSourceIDsOf(
   candidates: readonly { reroot: { sourceID: string; sourceName: string } }[],
   throughBase: NonNullable<Compiled["throughBase"]>,
): ReadonlySet<string> {
   const ids = new Set(candidates.map((c) => c.reroot.sourceID));
   const names = new Set(candidates.map((c) => c.reroot.sourceName));
   const kept = throughBase.filter(
      (t) =>
         t.chain.some((id) => ids.has(id)) ||
         (t.baseName !== undefined && names.has(t.baseName)),
   );
   return new Set([...ids, ...kept.map((t) => t.sourceID)]);
}

/** What a compiled copy is built from, past its last pass. */
interface BuiltCopy extends LoadedCopy {
   key: string;
   generation: number;
   over: readonly Binding[];
   lane: number;
   modelDef: unknown;
   servedMap: SourceMap;
   config: ReturnType<typeof servedCopyConfig>;
   rerooted: Binding[];
   aliased: Binding[];
   throughBase: NonNullable<Compiled["throughBase"]>[number][];
}

/**
 * The served copy compile's passes: the sources still re-rooted, and what
 * each step down their ladders has to report once the copy compiles.
 */
interface CopyPassState {
   /** The pin's time, in ms: what a remembered failure is held against. */
   asOf: number;
   candidates: { binding: Binding; reroot: AuthorReroot }[];
   /** An extension table's re-root, to its fallback without kept blocks. */
   withoutKept: Map<
      { binding: Binding; reroot: AuthorReroot },
      { binding: Binding; reroot: AuthorReroot } | undefined
   >;
   /**
    * Inlined joins kept as written because the copy did not compile with
    * their records (see keepInlinedJoins, below), and pair tables dropped
    * because it did not compile with them: each reported only once it
    * compiles without them (when it still does not, they were not the
    * cause).
    */
   inlinedKept: ServedCopyProblem[];
   pairsDropped: ServedCopyProblem[];
   problems: ServedCopyProblem[];
}

/**
 * The record that replaces an inlined join is the one text the
 * served copy writes over a join's place; when the copy does not
 * compile with it (the source's own text reads the join in a way
 * the record cannot answer), compile again with every inlined join
 * kept as written, which reads the joined source through the join.
 * Each join kept is reported once the copy compiles without the
 * records: when it still does not, they were not the cause.
 */
function keepInlinedJoins(state: CopyPassState, error: unknown): boolean {
   if (!state.candidates.some((c) => c.reroot.inlinedJoins)) return false;
   const detail = error instanceof Error ? error.message : String(error);
   // Added to, not replaced: a join containment already kept as
   // written for its own source (see `contain`) stays reported.
   const reported = new Set(
      state.inlinedKept.map((p) => `${p.sourceName}\u0000${p.join}`),
   );
   state.inlinedKept = [
      ...state.inlinedKept,
      ...state.candidates.flatMap((c) =>
         (c.reroot.inlinedJoins ?? [])
            .filter(
               (j) => !reported.has(`${c.reroot.sourceName}\u0000${j.join}`),
            )
            .map((j) => ({
               sourceName: c.reroot.sourceName,
               reason: "inline_join_kept" as const,
               join: j.join,
               detail,
            })),
      ),
   ];
   state.candidates = state.candidates.map((c) => {
      if (!c.reroot.inlinedJoins) return c;
      const { inlinedJoins: _kept, ...reroot } = c.reroot;
      return { ...c, reroot };
   });
   return true;
}

/**
 * A column left out of its source's `type:` (a v1 column no cast
 * makes faithful; a big-integer column no native cast reads
 * exactly). When the model's own text names it
 * (a dimension or measure over it, an extension's dimension), the
 * copy cannot compile with it gone: that source is left as written
 * and answers live, and every other source keeps its table.
 */
function withoutLeftOutColumn(
   state: CopyPassState,
   copy: ServedCopyText,
   error: unknown,
): boolean {
   const { withoutKept, problems } = state;
   const message = error instanceof Error ? error.message : String(error);
   // A path through a record left out (`ecom.qty`) names the record too.
   const named = new Set(
      [
         ...message.matchAll(
            /'([^']+)' is not defined|Reference to undefined (?:value|object) `?([^`\s]+)/g,
         ),
      ].flatMap((m) => {
         const name = m[1] ?? m[2];
         return [name, name.split(".")[0]];
      }),
   );
   if (named.size === 0) return false;
   const naming = state.candidates.filter((c) =>
      (c.reroot.columnsLeftOut ?? []).some((n) => named.has(n)),
   );
   if (naming.length === 0) return false;
   // The re-root the failure is placed in, when it is one of them:
   // another source that leaves out a column of the same name keeps
   // its table. A failure placed elsewhere (an extension left as
   // written) costs every source that leaves the name out.
   const traced = new Set(
      failingReroots(error, copy.files, copy.spans, "precise").map((f) =>
         rerootKey(f.url, f.sourceName),
      ),
   );
   const placed = naming.filter((c) =>
      traced.has(rerootKey(c.reroot.url, c.reroot.sourceName)),
   );
   const failing = placed.length > 0 ? placed : naming;
   for (const c of failing) {
      // The left-out columns the model's text reads, each with why.
      const read = (c.reroot.columnsLeftOut ?? []).filter((n) => named.has(n));
      problems.push({
         sourceName: c.reroot.sourceName,
         sourceID: c.reroot.sourceID,
         reason: "column_types",
         ...(read.length > 0
            ? {
                 detail: read
                    .map((n) => c.reroot.leftOutWhy?.get(n))
                    .filter((w): w is string => w !== undefined)
                    .join("; "),
              }
            : {}),
      });
      withoutKept.delete(c);
   }
   state.candidates = state.candidates.filter((c) => !failing.includes(c));
   return true;
}

/**
 * A pair table's served source drops what its record cannot carry
 * (the joined source's measures and views): author text that reads
 * one through the join does not compile there. When the copy does
 * not compile, try again without the pair tables first, so the
 * model's other tables still serve.
 */
function withoutPairs(state: CopyPassState, detail: string): boolean {
   const pairs = state.candidates.filter((c) => c.reroot.pair);
   if (pairs.length === 0) return false;
   state.pairsDropped = pairs.map((c) => ({
      sourceName: c.reroot.sourceName,
      reason: "pair_not_compiled" as const,
      detail,
   }));
   state.candidates = state.candidates.filter((c) => !c.reroot.pair);
   return true;
}

/**
 * The bind guard (`proveReroot`): a re-rooted source keeps every
 * field the author's has, each exactly as hidden, and applies
 * every filter the author's applies at read. One that does not is
 * left as written, and the copy compiled again. A field it inherited
 * from a base the re-root replaced (a measure, a view, a join
 * declared on the base) is not in its own text, so it is lost; what
 * a cross-connection join took with it is reported on its own, when
 * it was left out of this source or one it is built from. A given-
 * reading `where:` term the re-root dropped would answer every
 * caller with every caller's rows. A source the served compile holds
 * only as a join is read from that join; one that applies a
 * `where:` is left as written too, since the governor proves a
 * joined source's filters against its declaring file's live compile,
 * whose text the re-root changed.
 *
 * Returns whether a source was left as written, so the copy compiles again.
 */
function rerootsLosing(
   state: CopyPassState,
   copy: ServedCopyText,
   map: SourceMap,
   servedMap: SourceMap,
   leftOut: readonly LeftOutDefinition[],
): boolean {
   const { withoutKept, problems } = state;
   const keptAsWritten = (c: (typeof state.candidates)[number]) =>
      (copy.spans.get(c.reroot.url) ?? []).find(
         (s) => s.sourceName === c.reroot.sourceName,
      )?.termsKeptAsWritten ?? [];
   const lossOf = (c: (typeof state.candidates)[number]) => {
      const node = map.node(c.reroot.sourceID);
      const chain = new Set(
         (node ? map.chainOf(node.def) : [c.reroot.sourceID])
            .map((id) => map.node(id)?.name)
            .filter((n): n is string => n !== undefined),
      );
      chain.add(c.reroot.sourceName);
      return proveReroot({
         author: c.reroot.authorDef,
         served: servedDefOf(servedMap, c.reroot.sourceID),
         servedHasOwnNode: hasOwnNode(servedMap, c.reroot.sourceID),
         excused: new Set([
            ...leftOut
               .filter((l) => chain.has(l.declaration ?? l.name))
               .map((l) => l.name),
            ...(c.reroot.columnsLeftOut ?? []),
         ]),
         termsKeptAsWritten: keptAsWritten(c),
         givenTerms: givenReadingFilterTerms,
      });
   };
   // A source this model does not name (only a join reaches it, an import
   // of an import) that the served compile holds nowhere: the joins that
   // reach it were left out (a join across connections, answered live and
   // warned on its own), so nothing in this model reads it and its re-root
   // is moot. Left as written, with no warning of its own: it loses nothing
   // a caller can read here.
   const unreached = state.candidates.filter(
      (c) =>
         map.node(c.reroot.sourceID)?.name === undefined &&
         servedMap.node(c.reroot.sourceID) === undefined &&
         servedDefOf(servedMap, c.reroot.sourceID) === undefined,
   );
   if (unreached.length > 0) {
      for (const c of unreached) withoutKept.delete(c);
      state.candidates = state.candidates.filter((c) => !unreached.includes(c));
      return true;
   }
   const losses = new Map(state.candidates.map((c) => [c, lossOf(c)]));
   const lost = state.candidates.filter((c) => {
      const loss = losses.get(c);
      return loss === "fields" || loss === "access";
   });
   const unscoped = state.candidates.filter((c) => {
      const loss = losses.get(c);
      return (
         loss === "given_terms" ||
         loss === "terms_as_written" ||
         loss === "filtered_join"
      );
   });
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
      state.candidates = state.candidates.filter(
         (c) => !lost.includes(c) && !unscoped.includes(c),
      );
      return true;
   }
   return false;
}

/**
 * One binding's author declaration, as each phase of its re-root
 * ({@link ServedCopyServe}'s `rerootFor`) reads it.
 */
interface RerootSubject {
   binding: Binding;
   node: { sourceID: string; def: unknown };
   sourceName: string;
   def: {
      fields?: unknown[];
      location?: { url?: string; range?: ServedCopyRange };
   };
   /** The author's field of each name. */
   fieldsByName: ReadonlyMap<
      string,
      { name?: unknown; as?: unknown; timeframe?: unknown }
   >;
   /** The timeframe a truncation carries, to restore on the field. */
   timeframeOf(name: string): { timeframe?: string };
   problems: ServedCopyProblem[];
   /**
    * The table stores each column under its query's output name (a v1
    * table, no `#(materialize)` plan), not under the field's own name
    * after its renames (`BigIntegerReadOptions.storesOutputNames`).
    */
   storesOutputNames: boolean;
   /** How a caller's detail names a column: `column \`x\``, or `a hidden column`. */
   columnRef(name: string): string;
   /** Why each column left out so far is, by name ({@link leftOutPhrase}). */
   leftOutWhy: Map<string, string>;
}

/**
 * A live field's Malloy type as the author would write it (`string[]`,
 * `{ a::string, n::number }[]`), for a caller's detail; undefined when it is
 * not a basic type, or a list or record of them.
 */
function liveTypeText(field: unknown, depth = 0): string | undefined {
   const f = field as
      | { type?: unknown; elementTypeDef?: unknown; fields?: unknown }
      | undefined;
   if (typeof f?.type !== "string" || depth > 16) return undefined;
   if (f.type === "record") return recordTypeText(f.fields, depth);
   if (f.type !== "array") return f.type;
   const element = f.elementTypeDef as { type?: unknown } | undefined;
   const inner =
      element?.type === "record_element"
         ? recordTypeText(f.fields, depth)
         : liveTypeText(element, depth + 1);
   return inner === undefined ? undefined : `${inner}[]`;
}

/** {@link liveTypeText} of a record's fields, `{ a::string, n::number }`. */
function recordTypeText(fields: unknown, depth: number): string | undefined {
   if (!Array.isArray(fields) || fields.length === 0) return undefined;
   const parts: string[] = [];
   for (const raw of fields) {
      const field = raw as { name?: unknown; as?: unknown };
      const name = field.as ?? field.name;
      const type = liveTypeText(raw, depth + 1);
      if (typeof name !== "string" || type === undefined) return undefined;
      parts.push(`${name}::${type}`);
   }
   return `{ ${parts.join(", ")} }`;
}

/** Whether live answers a field as a list (of scalars or records) or a record. */
function isListOrRecord(field: unknown): boolean {
   const type = (field as { type?: unknown } | undefined)?.type;
   return type === "array" || type === "record";
}

/** How a caller's detail names a column the author's source hides. */
const HIDDEN_COLUMN = "a hidden column";

/**
 * Why a stored column is left out, or cannot be carried, in a caller's
 * words: the column (as {@link RerootSubject.columnRef} names it) with its
 * declared type and its stored type, or that the table does not hold it.
 */
function leftOutPhrase(
   ref: string,
   field: unknown,
   storedType: string | undefined,
): string {
   const missing = storedType === undefined;
   // A hidden column's types would tell a caller what it holds: none given.
   if (ref === HIDDEN_COLUMN) {
      return missing
         ? `${ref} is not in the stored table`
         : `${ref} has no declaration that reads it back as live answers it`;
   }
   const live = liveTypeText(field);
   const declared = live === undefined ? "" : `declared \`${live}\``;
   if (missing) {
      return `${ref}${declared ? ` (${declared})` : ""} is not in the stored table`;
   }
   return (
      `${ref} (${declared ? `${declared}, ` : ""}stored as ${storedType}) ` +
      "has no declaration that reads it back as live answers it"
   );
}

/** {@link leftOutPhrase} for one of the re-root's stored columns. */
function leftOutPhraseOf(subject: RerootSubject, c: ServedColumn): string {
   return leftOutPhrase(
      subject.columnRef(c.name),
      subject.fieldsByName.get(c.name),
      c.type,
   );
}

/** The phrase for a column no cast reads as declared, as a decline has said it. */
function noCastPhrase(ref: string): string {
   return `no cast reads ${ref} as declared`;
}

/**
 * A `#(materialize)` re-root's stored computed dimensions, those zoned, and
 * whether its rows are, less the columns it leaves out.
 */
function planDimensionsOf(
   plan: MaterializePlan,
   columnsLeftOut: readonly string[],
): Pick<AuthorReroot, "storedDimensions" | "zonedRows" | "zonedDimensions"> {
   const stored = plan.columns.filter(
      (c) =>
         c.computed &&
         c.join === undefined &&
         !columnsLeftOut.includes(c.dimension),
   );
   const zonedDimensions = stored
      .filter((c) => !c.zoneFree)
      .map((c) => c.dimension);
   return {
      storedDimensions: stored.map((c) => c.dimension),
      ...(!plan.zoneFreeRows ? { zonedRows: true } : {}),
      ...(zonedDimensions.length > 0 ? { zonedDimensions } : {}),
   };
}

/**
 * How a caller's detail names a re-root's column: `column \`x\``, or `a
 * hidden column` when the author's source hides it (a plan column with an
 * access modifier, or a field with one).
 */
function columnRefOf(
   plan: MaterializePlan | undefined,
   fieldsByName: ReadonlyMap<string, unknown>,
): (name: string) => string {
   const hidden = new Set([
      ...(plan?.columns ?? [])
         .filter((c) => c.access !== undefined)
         .flatMap((c) => [c.column, c.dimension]),
      ...[...fieldsByName]
         .filter(
            ([, f]) =>
               (f as { accessModifier?: unknown }).accessModifier !== undefined,
         )
         .map(([name]) => name),
   ]);
   return (name) => (hidden.has(name) ? HIDDEN_COLUMN : `column \`${name}\``);
}

/** Why each of `columns` is left out, by name, for a decline's detail. */
function leftOutWhyOf(
   subject: RerootSubject,
   columns: readonly string[],
): Map<string, string> {
   return new Map(
      columns.map((c) => [
         c,
         subject.leftOutWhy.get(c) ?? noCastPhrase(subject.columnRef(c)),
      ]),
   );
}

/** A `column_types` problem's `detail` naming `columns`, when there are any. */
function leftOutDetail(
   subject: RerootSubject,
   columns: readonly string[],
): { detail?: string } {
   if (columns.length === 0) return {};
   return {
      detail: columns
         .map(
            (c) =>
               subject.leftOutWhy.get(c) ?? noCastPhrase(subject.columnRef(c)),
         )
         .join("; "),
   };
}

/**
 * A `#(materialize)` re-root's columns: exactly what the build stored, the
 * plan's columns, and its inlined joins' records. Undefined, with a
 * `column_types` problem, when the table does not hold what the plan says
 * or a joined column has no faithful read.
 */
function materializedColumnsOf(
   subject: RerootSubject,
   plan: MaterializePlan,
):
   | {
        columns: ServedColumn[];
        columnsLeftOut: string[];
        inlinedJoins: InlinedJoin[];
     }
   | undefined {
   const { binding, node, sourceName, def, problems } = subject;
   let inlinedJoins: InlinedJoin[] = [];
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
   // A stored column whose type is not live's (the upstream column's type
   // changed under the same content address, or the table was altered): read
   // through a faithful cast, else left out so only a query reading it
   // answers live, as a v1 table's is (v1ColumnsOf). Never declared as the
   // stored type, which would answer a number as text.
   const typePlan = materializedReadPlan(
      plan.columns.flatMap((c) => {
         const type = stored.get(c.column);
         return c.join === undefined &&
            c.column === c.dimension &&
            type !== undefined
            ? [{ name: c.column, type }]
            : [];
      }),
      def,
      !isDatabase(binding),
   );
   const castTo = new Map(
      typePlan.casts.map((c) => [
         c.column,
         c.castTo === "BOOLEAN" ? ("boolean" as const) : ("date" as const),
      ]),
   );
   const own = materializedOwnColumns(subject, plan, {
      stored,
      lineage,
      leftOut: new Set(typePlan.leftOut.map((m) => m.column)),
      castTo,
   });
   if (!own) return undefined;
   const { columns, columnsLeftOut } = own;
   const joined = inlinedJoinsOf(plan, def.fields, stored, lineage);
   if (
      joined === undefined ||
      joined.some((j) =>
         j.columns.some((c) => storedMalloyType(c.type) === undefined),
      )
   ) {
      const undeclared = undeclaredJoinedColumns(def.fields, joined ?? []);
      problems.push({
         sourceName,
         sourceID: node.sourceID,
         reason: "column_types",
         ...(undeclared.length > 0 ? { detail: undeclared.join("; ") } : {}),
      });
      return undefined;
   }
   // A joined big-integer field is read through its native type in the
   // record; one no cast reads exactly leaves the source to its
   // fallback.
   const readOptions = bigIntegerOptionsOf(def, binding);
   const joinedReads = joined.map((j) => {
      const fields = joinedFieldsAt(def.fields, [j.join]);
      return storedColumnReadPlan(
         j.columns.map((c) => ({ name: c.column, type: c.type })),
         j.columns.map((c) => ({
            name: c.column,
            type: "",
            field: fields.find((f) => fieldNameOf(f) === c.name),
         })),
         { ...readOptions, ducklake: !isDatabase(binding) },
      );
   });
   // A joined field live types as a warehouse-native type has no
   // stored declaration that reads back as live's either.
   const joinedNative = joined.some((j) => {
      const fields = joinedFieldsAt(def.fields, [j.join]);
      return j.columns.some((c) =>
         liveTypesAsNative(fields.find((f) => fieldNameOf(f) === c.name)),
      );
   });
   if (
      joinedNative ||
      joinedReads.some(
         (plan) => plan.refusals.length > 0 || plan.leftOut.length > 0,
      )
   ) {
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
         const nativeCast = joinedReads[i].casts.find(
            (r) => r.column === c.column,
         )?.castTo;
         return nativeCast ? { ...c, nativeCast } : c;
      }),
   }));
   return { columns, columnsLeftOut, inlinedJoins };
}

/**
 * {@link materializedColumnsOf}'s own (not joined) columns: each plan column
 * the table stores as live's type or through a faithful cast, the ones no
 * read gives exactly left out with why. Undefined, with a `column_types`
 * problem, when the table lacks a column that is not a list, or stored one
 * under another name.
 */
function materializedOwnColumns(
   subject: RerootSubject,
   plan: MaterializePlan,
   read: {
      stored: ReadonlyMap<string, string>;
      lineage: ReturnType<typeof readSourceClassifications>;
      leftOut: ReadonlySet<string>;
      castTo: ReadonlyMap<string, "boolean" | "date">;
   },
): { columns: ServedColumn[]; columnsLeftOut: string[] } | undefined {
   const { binding, node, sourceName, fieldsByName, timeframeOf, problems } =
      subject;
   const { stored, lineage, leftOut, castTo } = read;
   const columns: ServedColumn[] = [];
   const columnsLeftOut: string[] = [];
   for (const c of plan.columns) {
      // An inlined join's columns are read under the join's name
      // (inlinedJoinsOf, below), not as fields of the source.
      if (c.join !== undefined) continue;
      if (leftOut.has(c.column)) {
         columnsLeftOut.push(c.column);
         subject.leftOutWhy.set(
            c.column,
            leftOutPhrase(
               subject.columnRef(c.column),
               fieldsByName.get(c.dimension),
               stored.get(c.column),
            ),
         );
         continue;
      }
      const type = stored.get(c.column);
      // A column live types as warehouse-native (a UUID) has no stored
      // spelling, so a database table's captured columns leave it out: it is
      // left out here too, so only a query reading it answers live, as a
      // `storage=` table's is (declarableColumns).
      if (
         type === undefined &&
         isDatabase(binding) &&
         liveTypesAsNative(fieldsByName.get(c.dimension))
      ) {
         columnsLeftOut.push(c.column);
         continue;
      }
      // A list or record the table does not hold (a `storage=` table built
      // before its build stored arrays and records, until its next full
      // build) is left out as one no declaration reads back is: only a
      // query reading it (the copy's compile of it fails, directly or
      // through a field, filter or nest over it) answers live, and every
      // other query reads the table.
      if (
         type === undefined &&
         c.column === c.dimension &&
         isListOrRecord(fieldsByName.get(c.dimension))
      ) {
         columnsLeftOut.push(c.column);
         subject.leftOutWhy.set(
            c.column,
            leftOutPhrase(
               subject.columnRef(c.column),
               fieldsByName.get(c.dimension),
               undefined,
            ),
         );
         continue;
      }
      // A build that stored a dimension under another name cannot be
      // read back as that field.
      if (type === undefined || c.column !== c.dimension) {
         problems.push({
            sourceName,
            sourceID: node.sourceID,
            reason: "column_types",
            detail:
               type === undefined
                  ? leftOutPhrase(
                       subject.columnRef(c.dimension),
                       fieldsByName.get(c.dimension),
                       undefined,
                    )
                  : `${subject.columnRef(c.dimension)} is stored under another name`,
         });
         return undefined;
      }
      const notes = storedColumnNotes(
         fieldsByName.get(c.dimension),
         lineage.get(c.dimension),
      );
      const cast = castTo.get(c.column);
      columns.push({
         name: c.column,
         type,
         ...(notes.length > 0 ? { notes } : {}),
         ...timeframeOf(c.dimension),
         ...(c.access ? { access: c.access } : {}),
         ...(cast ? { castTo: cast } : {}),
         // A zoned timestamp whose field live types `timestamptz` is
         // declared as one, as a v1 table's is.
         ...((fieldsByName.get(c.dimension) as { type?: unknown })?.type ===
            "timestamptz" &&
         /^(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ)$/i.test(type.trim())
            ? { timestamptz: true as const }
            : {}),
      });
   }
   return { columns, columnsLeftOut };
}

/**
 * Each inlined-join column no `type:` declares, as a caller's detail names
 * it: through its join, unless the join or the field is hidden.
 */
function undeclaredJoinedColumns(
   fields: unknown[] | undefined,
   joined: readonly {
      join: string;
      access?: unknown;
      columns: readonly { name: string; type: string }[];
   }[],
): string[] {
   return joined.flatMap((j) => {
      const joinFields = joinedFieldsAt(fields, [j.join]);
      return j.columns
         .filter((c) => storedMalloyType(c.type) === undefined)
         .map((c) => {
            const field = joinFields.find((f) => fieldNameOf(f) === c.name);
            const hidden =
               j.access !== undefined ||
               (field as { accessModifier?: unknown } | undefined)
                  ?.accessModifier !== undefined;
            return leftOutPhrase(
               hidden ? HIDDEN_COLUMN : `column \`${j.join}.${c.name}\``,
               field,
               c.type,
            );
         });
   });
}

/**
 * How a `#(materialize)` table's own stored columns read as the types live
 * gives their fields ({@link storedColumnReadPlan}): each stored column named
 * for a scalar field of the author's source, against that field's type, as
 * the served copy's re-root ({@link materializedColumnsOf}) reads them. The
 * `storage-column-type-mismatch` warning reports the same cast or live
 * handling (`boundTableTypeFindings`).
 */
export function materializedReadPlan(
   schema: readonly { name: string; type: string }[],
   def: { fields?: unknown[] } | undefined,
   /** A `storage=` (DuckLake) table: a 128-bit integer column is left out. */
   ducklake = false,
): ReturnType<typeof storedColumnReadPlan> {
   const live = new Map<string, string>();
   for (const f of (def?.fields ?? []) as {
      name?: unknown;
      as?: unknown;
      type?: unknown;
      join?: unknown;
      expressionType?: unknown;
   }[]) {
      const name = f.as ?? f.name;
      if (
         typeof name !== "string" ||
         typeof f.type !== "string" ||
         f.join !== undefined ||
         (f.expressionType !== undefined && f.expressionType !== "scalar")
      ) {
         continue;
      }
      live.set(name, f.type);
   }
   const own = schema.filter((c) => live.has(c.name));
   return storedColumnReadPlan(
      own,
      own.map((c) => ({ name: c.name, type: live.get(c.name) as string })),
      { queryTimezone: queryTimezoneOf(def), ducklake },
   );
}

/** Whether a stored column of `storedType` does not serve as `field`'s live type. */
function driftsFromLive(storedType: string, field: unknown): boolean {
   const type = (field as { type?: unknown } | undefined)?.type;
   return (
      typeof type === "string" && storedTypeServesAs(storedType, type) === false
   );
}

/**
 * A v1 table's columns: the stored columns its source's fields read, each
 * cast on read or left out (`columnsLeftOut`) where its stored type is not
 * the field's. Undefined, with a `column_types` problem, when none is left.
 */
function v1ColumnsOf(
   subject: RerootSubject,
): { columns: ServedColumn[]; columnsLeftOut: string[] } | undefined {
   const { binding, node, sourceName, def, fieldsByName, timeframeOf } =
      subject;
   const { problems } = subject;
   let columnsLeftOut: string[] = [];
   // A v1 table, database or `storage=`, holds the columns its build query
   // emits, hidden ones too: its columns are every column it holds, with no
   // access modifier, and the author's own text after the query (kept
   // whole, `keepsIncludes`) renames, drops and re-hides them, so a measure
   // over a hidden column compiles and the column stays exactly as hidden as
   // live. A `storage=` table leaves out a column `except:` drops; a kept
   // `include` that hides one the table lacks is refused (`hiddenUnstored`)
   // and the source answers live.
   // A stored column no field of the source names is one its own `extend`
   // block drops (`except:`, `accept:`) or an `include` re-hides under
   // another name: it stays on the stored table so the author's text
   // compiles and removes it again. Should the text not remove it, the access
   // check below finds a public field the author's source lacks and leaves
   // the source as written.
   const narrowed = v1StoredColumns(binding.schema, def.fields);
   // A column the served copy cannot read as its declared type: cast on read
   // when a cast is faithful, else left out so only a query reading it
   // answers live.
   const typePlan = storedColumnReadPlan(
      narrowed,
      declaredColumnsOf(def.fields),
      { queryTimezone: queryTimezoneOf(def), ducklake: !isDatabase(binding) },
   );
   const castTo = new Map(
      typePlan.casts.map((c) => [
         c.column,
         c.castTo === "BOOLEAN" ? ("boolean" as const) : ("date" as const),
      ]),
   );
   const leftOut = new Set(typePlan.leftOut.map((m) => m.column));
   columnsLeftOut = [...leftOut];
   const storedTypeOf = new Map(narrowed.map((c) => [c.name, c.type]));
   for (const name of leftOut) {
      subject.leftOutWhy.set(
         name,
         leftOutPhrase(
            subject.columnRef(name),
            fieldsByName.get(name),
            storedTypeOf.get(name),
         ),
      );
   }
   // A nest of the persisted query a destination did not store (a lake
   // that cannot hold a repeated record): left out like a column no
   // type reads, so only a query through it answers live, as the serve
   // shape kept the traversal local.
   const stored = new Set(binding.schema.map((c) => c.name.toLowerCase()));
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
         subject.leftOutWhy.set(
            name,
            leftOutPhrase(subject.columnRef(name), f, undefined),
         );
      }
   }
   // A column the table holds whose field live types as `json` or `sql
   // native` (a Postgres `jsonb`, DuckDB's MAP, BLOB, INTERVAL, UUID or
   // JSON): no stored type spells it, so the table's captured columns, and
   // those derived from its source, leave it out. Left out like a column no
   // type reads, so only a query reading it answers live, with the type the
   // warehouse holds it as for the reason.
   const nativeTypeOf = nativeTypeLookup(binding.nativeSchema);
   for (const f of (def.fields ?? []) as {
      name?: unknown;
      as?: unknown;
      join?: unknown;
      type?: unknown;
      e?: unknown;
   }[]) {
      if (
         typeof f.name !== "string" ||
         f.join !== undefined ||
         f.e !== undefined ||
         !liveTypesAsNative(f)
      ) {
         continue;
      }
      const name = typeof f.as === "string" ? f.as : f.name;
      if (stored.has(f.name.toLowerCase()) || stored.has(name.toLowerCase())) {
         continue;
      }
      columnsLeftOut.push(name);
      subject.leftOutWhy.set(
         name,
         leftOutPhrase(subject.columnRef(name), f, nativeTypeOf(f.name)),
      );
   }
   // Each with its field's annotations (`#(doc)`, `# label`, `#
   // percent` on the query's output field), as a `#(materialize)` column
   // carries them, so the stored field reads back as the author's.
   const columns: ServedColumn[] = narrowed
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
            /^(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ)$/i.test(c.type.trim())
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
   return { columns, columnsLeftOut };
}

/**
 * A table of several inlined paths is read one block at a time: the
 * block of the one path the query reads, from that block's copy. A
 * query that reads two paths is answered live: live aggregates each of
 * the two joins by its own key over their cross product, which no
 * block holds.
 * Returns the block the query reads when it is not this copy's lane: the
 * query is answered from that block's copy.
 */
async function pairBlockLane(
   attempt: AnswerAttempt,
   pairsRead: readonly NonNullable<Compiled["pairs"]>[number][],
): Promise<number | undefined> {
   const { compiled, decline, prepared } = attempt;
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
         return read[0];
      }
   }
   return undefined;
}

/** The queried source's struct, and the sourceIDs on its chain. */
async function queriedChain(attempt: AnswerAttempt): Promise<{
   struct: { fields?: unknown; filterList?: unknown } | undefined;
   chain: readonly string[];
}> {
   const { compiled, prepared } = attempt;
   const { structRef } = await prepared();
   const struct = resolveStructRefSource<{
      fields?: unknown;
      filterList?: unknown;
   }>(structRef, compiled.modelDef as never);
   const chain = struct
      ? SourceMap.of(compiled.modelDef).chainOf(struct as never)
      : [];
   return { struct, chain };
}

/**
 * The pair tables a query reads, one entry per table. Sources whose build SQL
 * is identical (differing only in gates or `name=`) share one database table
 * at their one content address, so each is a pair at that one path. Read
 * once, that table is read as the queried source's own when the queried
 * source is one of them, as on `storage=`, where each source has a table of
 * its own. Read more than once, or by a source that is none of them, every
 * sharer is kept, and {@link checkPairTables} declines it as a read through
 * a join.
 */
async function pairsReadPerTable(
   attempt: AnswerAttempt,
   sql: string,
   pairsRead: NonNullable<Compiled["pairs"]>,
): Promise<NonNullable<Compiled["pairs"]>> {
   const byPath = new Map<string, NonNullable<Compiled["pairs"]>>();
   for (const p of pairsRead) {
      byPath.set(p.tablePath, [...(byPath.get(p.tablePath) ?? []), p]);
   }
   if (byPath.size === pairsRead.length) return pairsRead;
   const { chain } = await queriedChain(attempt);
   return [...byPath].flatMap(([tablePath, sharers]) => {
      const own = sharers.find((p) => chain.includes(p.sourceID));
      return sharers.length > 1 && own && sqlTableReads(sql, tablePath) === 1
         ? [own]
         : sharers;
   });
}

/**
 * A pair table (an inlined join_many) is read flat: exact only for the
 * aggregates pair_flat_query.ts proves, and only as the queried
 * source's own table, never through a join to it (its rows would fan
 * out the joining source).
 */
async function checkPairTables(
   attempt: AnswerAttempt,
   pairsRead: readonly NonNullable<Compiled["pairs"]>[number][],
): Promise<void> {
   const { compiled, decline, prepared } = attempt;
   if (pairsRead.length > 0) {
      const { query } = await prepared();
      const { struct, chain } = await queriedChain(attempt);
      const own = pairsRead.find((p) => chain.includes(p.sourceID));
      const verdict =
         pairsRead.length > 1 || !own
            ? {
                 kind: "refused" as const,
                 reason:
                    "the query reads a pair table other than as the source it stores (through a join to it, whose rows would fan out the joining source, or from a source built on it)",
              }
            : await (async () => {
                 const gates = await pairGatesRead(compiled, struct, own.pair);
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
         ? { oneParentLevels: pair.oneParentLevels }
         : {}),
      // A stored computed child field need not be null on a childless
      // parent's null-extended row, so a sum of one reads that row: at the
      // leaf as above it.
      ...(pair.childStoredDimensions.length > 0
         ? {
              computedFields: pair.childStoredDimensions.map(
                 (d) => `${pair.join}.${d}`,
              ),
           }
         : {}),
      ...(pair.levelPresent?.some((p) => p !== undefined)
         ? { levelPresentFields: pair.levelPresent.map((p) => p?.field) }
         : {}),
      ...(pair.leafKeyChecked ? { leafKeyChecked: true } : {}),
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
 * How the served copy reads one pair table, from its plan, the stored
 * schema, the author's compiled source (`sourceFields`) and the joined
 * sources': each stored child column renamed internally under a name no
 * field of the model has (`taken`), and each measure over one level above the
 * leaf re-declared over that level's rows. `lane` picks the block a table of
 * several inlined paths is read for (its first, otherwise). Undefined when
 * the table does not store what the plan says (a column missing).
 * `leafKeyChecked` says whether the run that wrote the table recorded the
 * leaf's key unique and never null, for the leaf source's sourceID.
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
   leafKeyChecked: (sourceID: string) => boolean = () => false,
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
   const records = pairRecordsOf(
      plan,
      block,
      sourceFields,
      stored,
      map,
      lineage,
      bigRead,
      fresh,
   );
   if (!records) return undefined;
   const { own, others, childStoredDimensions, annotatedFields } = records;
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
   // Each level of the path has a row where its primary key, read through
   // the record, is not null: the test Malloy makes of a joined source's own
   // rows (Malloy 0.0.438 on). Declared only where the build checked
   // that key unique and never null, so the test fails on a childless row's
   // null-extended row alone: a level above the leaf on every build (its
   // check fails the build), the leaf only where its recorded check passed.
   const path = {
      join: block.join,
      hops: (block.hops ?? []).map((h) => h.join),
   };
   const leaf = 1 + path.hops.length;
   const levelKeys = [
      { key: block.childKey, sourceID: block.childSourceID },
      ...(block.hops ?? []).map((h) => ({ key: h.key, sourceID: h.sourceID })),
   ];
   const leafChecked = leafKeyChecked(levelKeys[leaf - 1].sourceID);
   const levelPresent: ({ field: string; keyPath: string[] } | undefined)[] =
      levelKeys.map(({ key }, i) => {
         const level = i + 1;
         const columns =
            level === 1 ? own.childColumns : (own.hops ?? [])[i - 1]?.columns;
         if (!columns?.some((c) => c.name === key)) return undefined;
         if (level === leaf && !leafChecked) return undefined;
         const names = [path.join, ...path.hops.slice(0, i)];
         return {
            field: fresh(`__${names.join("__")}__present`),
            keyPath: [...names, key],
         };
      });
   const parentMeasures = pairParentMeasures(
      sourceFields,
      path,
      block,
      childStoredDimensions,
      levelPresent,
      leaf,
      fresh,
   );
   // Only a level a re-declared measure reads its presence field at
   // declares one: no query can name it.
   const presentUsed = levelPresent.map((p, i) =>
      parentMeasures.some((m) => m.present && m.level === i + 1)
         ? p
         : undefined,
   );
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
      ...(presentUsed.some((p) => p !== undefined)
         ? { levelPresent: presentUsed }
         : {}),
      ...(leafChecked ? { leafKeyChecked: true as const } : {}),
      ...(blockReroot ? { block: blockReroot } : {}),
      ...(own.access ? { access: own.access } : {}),
   };
}

/**
 * The records of one pair table's blocks: `block`'s own, and the other
 * blocks' of a table of several inlined paths, each with its stored child
 * columns renamed internally (`fresh`), with the block's child fields that
 * carry annotations and its stored computed dimensions. Undefined when the
 * table does not store a column the plan names, or a child column has no
 * faithful read.
 */
function pairRecordsOf(
   plan: MaterializePlan,
   block: MaterializePair,
   sourceFields: readonly unknown[],
   stored: ReadonlyMap<string, string>,
   map: SourceMap,
   lineage: ReadonlyMap<string, FieldClassification>,
   bigRead: (
      field: unknown,
      column: { name: string; type: string },
   ) => BigIntegerRead | undefined,
   fresh: (base: string) => string,
) {
   const blocks = plan.pairBlocks;
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
         // record; one no cast reads exactly leaves
         // the table to its fallback.
         const big = bigRead(field, { name: c.column, type });
         // So does one live types as a warehouse-native type, which no
         // stored declaration reads back as.
         // So does one stored as another type than live's.
         if (
            (big && !big.castTo) ||
            liveTypesAsNative(field) ||
            driftsFromLive(type, field)
         ) {
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
   return { own, others, childStoredDimensions, annotatedFields };
}

/**
 * A measure over one level is re-declared over that level's rows: above
 * the leaf under its flag, and at any level of the path under its
 * presence field where it needs one (it counts, or sums a value not null
 * on a childless row's null-extended row). One at the leaf that needs
 * none reads every row as it stands. One reading several levels, one that
 * needs a presence field it cannot have (none declared, or a `min` or
 * `max` beside the count), or anything else, is left as it is, so a query
 * that reads it is answered live (pair_flat_query.ts).
 */
function pairParentMeasures(
   sourceFields: readonly unknown[],
   path: { join: string; hops: string[] },
   block: MaterializePair,
   childStoredDimensions: readonly string[],
   levelPresent: readonly ({ field: string; keyPath: string[] } | undefined)[],
   leaf: number,
   fresh: (base: string) => string,
): PairMeasure[] {
   const levels = pairMeasureLevels(sourceFields, path);
   const presence = pairMeasurePresence(sourceFields, {
      ...path,
      computedFields: childStoredDimensions.map((d) => `${block.join}.${d}`),
   });
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
      const present =
         only > 0 &&
         presence.get(name) === "needed" &&
         levelPresent[only - 1] !== undefined;
      if (only === leaf && !present) continue;
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
         ...(present ? { present: true as const } : {}),
      });
   }
   return parentMeasures;
}

/**
 * The load-time classification checks, run on the served copy's own compile:
 * the compile that answers a query. A re-root that changed what a source
 * exposes or how a field is classified (a stored column that lost its
 * field's class, say) would show here as a `#(carries)` leak or an
 * unclassified field the compile of record does not have; such a source is
 * never served from the copy (the bind guard's `classificationNotKept`), and
 * a `classification_not_kept` problem names each finding. A fail-closed
 * backstop: Publisher's re-roots keep every class, so nothing reaches it
 * unless one stops doing so. Returns those sources' `sourceID`s.
 */
export function classificationCeiling(
   modelPath: string,
   recordDef: unknown,
   servedDef: unknown,
   servedMap: SourceMap,
   problems: ServedCopyProblem[],
): Set<string> {
   const unsafe = new Set<string>();
   const added = classificationNotKept(
      classificationFindingsBySource(modelPath, recordDef as ModelDef),
      classificationFindingsBySource(modelPath, servedDef as ModelDef),
   );
   for (const [source, { leaks, coverage }] of added) {
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
 * A declaration's definition in the served copy's compile: its node, or, for
 * a source this model reaches only as a join (an index that imports the file
 * joining it, not the file declaring it), the joined definition a join
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
 * The big-integer columns among `columns`, by name, to
 * the native type each is read through, or to undefined when no cast reads it
 * exactly ({@link bigIntegerReads}). Decided by the author's source's dialect
 * (the one live answers on) and where the binding's table is.
 */
function bigIntegerReadsOf(
   columns: readonly { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
   def: unknown,
   binding: Binding,
   /** The table stores its query's output names (a v1 table). */
   storesOutputNames: boolean,
   /** A build reads the stored column in SQL, where no digit is lost. */
   asStored?: boolean,
): Map<string, string | undefined> {
   if (asStored) return new Map();
   return new Map(
      bigIntegerReads(columns, fields, {
         ...bigIntegerOptionsOf(def, binding),
         storesOutputNames,
      }).map((r) => [r.column, r.castTo] as const),
   );
}

/**
 * What decides a big-integer column's read for a binding: the author's
 * source's dialect, where the table is, and, for a database table, the
 * warehouse types its build captured (`nativeSchema`).
 */
function bigIntegerOptionsOf(
   def: unknown,
   binding: Binding,
): BigIntegerReadOptions {
   return {
      dialect: (def as { dialect?: unknown }).dialect,
      database: isDatabase(binding),
      ...(isDatabase(binding)
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
         // A record field has no cast to read it as live's type, so a
         // drifted one leaves the source to its fallback.
         if (driftsFromLive(type, field)) return undefined;
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
 * A v1 table's columns as its source's fields: every stored
 * column matched to the author's field of its name (exactly, else one field
 * whose name folds to it, a warehouse folding the case DESCRIBE reports), with the
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
 * A v1 table's stored columns as its source reads them: each one a field
 * names ({@link storedColumnsWithAccess}), then each one none names. The one
 * list the served copy re-roots on and the type warning checks.
 */
export function v1StoredColumns(
   schema: readonly { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
): { name: string; type: string }[] {
   return [
      ...storedColumnsWithAccess(schema, fields).map(({ name, type }) => ({
         name,
         type,
      })),
      ...unnamedStoredColumns(schema, fields),
   ];
}

/**
 * Whether a compiled source declares (or inherits) a measure, a view or a
 * join: what a re-root that replaces it as a base would lose unless it keeps
 * its blocks.
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
function unnamedStoredColumns(
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
/**
 * The top-level field names of one result shape ({@link resultShapeOf}) the
 * other lacks: `live`'s names not in `served`. Empty when either does not
 * parse.
 */
function namesLacking(live: string, served: string): string[] {
   const names = (shape: string): string[] | undefined => {
      try {
         const parsed: unknown = JSON.parse(shape);
         if (!Array.isArray(parsed)) return undefined;
         return parsed.flatMap((f) =>
            typeof f === "string"
               ? [f]
               : f !== null && typeof f === "object"
                 ? Object.keys(f)
                 : [],
         );
      } catch {
         return undefined;
      }
   };
   const had = names(served);
   if (had === undefined) return [];
   return (names(live) ?? []).filter((n) => !had.includes(n));
}

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

/**
 * Whether live types a field as a warehouse-native type Malloy has no type
 * of its own for (`sql native`, or `json`): no `type:` declaration of a
 * stored scalar reads back as that.
 */
function liveTypesAsNative(field: unknown): boolean {
   const type = (field as { type?: unknown } | undefined)?.type;
   return type === "sql native" || type === "json";
}

/**
 * A stored list or struct column's declaration in the shape live answers its
 * field in (`storedMalloyTypeFor`), or undefined when none reads it back as
 * live does: a field live types as `sql native` (a Postgres composite type,
 * stored by DuckDB as a STRUCT), or one holding such a leaf or a `bigint`
 * number live keeps every digit of. A list or record live answers, stored as
 * a scalar (its text, say), is fitted the same way, so it is left out rather
 * than declared as that scalar. Any other column, or one whose field is not
 * known here, keeps the stored type's own declaration.
 */
function nestedDeclarationOf(
   storedType: string,
   field: unknown,
   keepsBigIntegers: boolean,
): string | undefined {
   const declared = storedMalloyType(storedType);
   if (declared === undefined) return undefined;
   const liveType = (field as { type?: unknown } | undefined)?.type;
   const nested =
      declared.startsWith("{") ||
      declared.endsWith("[]") ||
      liveType === "array" ||
      liveType === "record";
   if (!nested || field === undefined) return declared;
   return storedMalloyTypeFor(storedType, field, keepsBigIntegers);
}

/**
 * The re-rooted upstreams a chained build's downstream reads in another time
 * zone than their tables were built in, when each stores a value that
 * depends on the zone: a timestamp truncation restated over its stored value
 * (`d is d__stored.day`), which the downstream would truncate again in its
 * own zone; a computed dimension, or a record of joined fields, the build
 * evaluated in its own. The served copy declines such a read per query
 * (`checkZonedColumns`, `checkTimeZone`); a chained build runs no read
 * checks, so the chain is declined and the downstream recomputes from its
 * source, as live computes it. The zones are compared as
 * `readsInAnotherZone` compares them: the zone the downstream reads in, and
 * every zone its definition sets. A downstream that cannot be resolved
 * declines every such upstream.
 */
function zonedElsewhere(
   map: SourceMap,
   downstreamSourceID: string,
   candidates: readonly { reroot: AuthorReroot }[],
): string[] {
   const zoneDependent = candidates.filter(
      ({ reroot: r }) =>
         (r.restatedTruncations ?? []).length > 0 ||
         (r.zonedDimensions ?? []).length > 0 ||
         r.zonedRows === true ||
         (r.inlinedJoins ?? []).length > 0 ||
         (r.pair?.childStoredDimensions ?? []).length > 0,
   );
   if (zoneDependent.length === 0) return [];
   const def = map.node(downstreamSourceID)?.def;
   if (def === undefined) return zoneDependent.map((c) => c.reroot.sourceName);
   const read = chainZoneOf(map, def) ?? inlineBaseZoneOf(def);
   const set = queryTimezones(def);
   return zoneDependent
      .filter(({ reroot }) => {
         const built = chainZoneOf(map, reroot.authorDef);
         return !sameZone(read, built) || set.some((z) => !sameZone(z, built));
      })
      .map((c) => c.reroot.sourceName);
}

/**
 * The plan's reader of a stacked build's compared strings, beside
 * {@link planChainBuildText}: the package-load worker's one door into
 * serve/, which must not load the barrel.
 */
export { chainBuildStringReads } from "./chain_string_reads";

/**
 * {@link chainBuildText} as the plan is handed it, for the package at
 * `packagePath` read through `config` (plan/storage_over_stored.ts).
 */
export function planChainBuildText(
   packagePath: string,
   config: ModelConnectionInput,
): PlanChainBuildText {
   return (request, model) =>
      chainBuildText(request, {
         modelPath: model.modelPath,
         modelDef: model.modelDef,
         context: [packagePath, config, model.materializePlans ?? {}],
      });
}

/** What the chained build's text is written from, for one model. */
interface ChainBuildModel {
   modelPath: string;
   modelDef: unknown;
   context: Parameters<ServedCopyServe["setContext"]>;
}

/**
 * The served copy's answer to a chained `storage=` build, for the host
 * port's `chainBuildText` (through `Model.chainBuildText` and the facade):
 * the text a downstream builds from on its destination alone, with every
 * stored upstream re-rooted as the served copy re-roots it. It binds exactly
 * the upstream entries the request carries, in a served copy of its own that
 * it discards, so the package's bound tables are neither read nor changed. A
 * binding whose name means another source in this model is not bound, as on
 * the serve path. Undefined when this model does not declare the downstream.
 */
export function chainBuildText(
   request: ChainBuildRequest,
   model: ChainBuildModel,
): ChainBuildText | undefined {
   const lift = authorModelLiftContext(
      model.modelDef,
      compiledTextReader(model.modelDef, model.context[0]),
   );
   if (
      request.view
         ? lift.contents[request.view.authorName] === undefined
         : !lift.sourceNameById.has(
              request.rollup?.baseSourceID ?? request.downstream.sourceID,
           )
   ) {
      return undefined;
   }
   const entries: Record<string, ManifestEntry> = {};
   const aliases: Record<string, string[]> = {};
   const sourceIDs = new Map<string, readonly string[] | undefined>();
   for (const upstream of request.upstreams) {
      // An upstream table holding a 128-bit integer (a list's or struct's
      // leaf included) holds its digits rounded
      // (DuckLake stores it as a DOUBLE): never stacked on, so the
      // downstream recomputes from its raw source rather than storing those
      // rounded values as exact. Its text then reads the upstream as written.
      if (
         (upstream.entry.schema ?? []).some((c) =>
            holdsDucklakeDouble(c.type ?? ""),
         )
      ) {
         continue;
      }
      const address = upstream.entry.sourceEntityId;
      entries[address] = upstream.entry;
      const names = Object.keys(upstream.names);
      for (const name of names) {
         aliases[name] = names;
         sourceIDs.set(`${name}\u0000${address}`, upstream.names[name]);
      }
   }
   // Without a plan's sourceIDs a binding names its source by name alone.
   const known = [...sourceIDs.values()].some((ids) => ids !== undefined);
   const bindings = deriveServeBindings(
      entries,
      aliases,
      () => request.destination.schema,
      known
         ? (name, address) => [
              ...(sourceIDs.get(`${name}\u0000${address}`) ?? []),
           ]
         : undefined,
   ).filter(
      (b) =>
         b.destinationName === request.destination.name &&
         b.kind !== "rollup" &&
         bindingServesModelSource(b, lift.contents),
   );
   const copy = new ServedCopyServe({
      modelPath: () => model.modelPath,
      modelDef: () => model.modelDef,
      // Nothing is compiled here: the build compiles the text on its session.
      serveDestinationConfig: () => undefined,
      quiet: true,
      readsStoredIntegers: true,
   });
   copy.setContext(...model.context);
   return request.rollup
      ? copy.rollupBuildFiles(request.rollup, request.downstream.name, bindings)
      : copy.chainBuildFiles(
           request.downstream.sourceID,
           bindings,
           request.view
              ? { ...request.view, downstreamName: request.downstream.name }
              : undefined,
        );
}

/**
 * A query-shaped source's output names: each field its query outputs, by the
 * name it has before its `extend` renames it (`name`, beside `as`). A
 * dimension the `extend` computes (`e`) is none.
 */
function queryOutputNames(fields: readonly unknown[] | undefined): string[] {
   return (fields ?? []).flatMap((f) => {
      const { name, e, join, type } = (f ?? {}) as {
         name?: unknown;
         e?: unknown;
         join?: unknown;
         type?: unknown;
      };
      return typeof name === "string" &&
         e === undefined &&
         join === undefined &&
         type !== "turtle"
         ? [name]
         : [];
   });
}

/** Whether a stale binding stores a source on `chain` (the queried source's). */
export function staleOnChain(
   stale: readonly Binding[],
   chain: readonly string[],
): boolean {
   const ids = new Set(chain);
   return stale.some((b) => (b.sourceIDs ?? []).some((id) => ids.has(id)));
}

/**
 * The re-root's timestamp truncations, which it restates over the stored
 * value (`d is d__stored.day`) and so truncates again in the zone the reading
 * query runs in: the identity only in the zone the build truncated it in. A
 * query-shaped source with a zone (set in its query, on itself, or inherited
 * from the base its query runs on, `ev_ny -> { … }`) reads in another zone
 * than it was built in (its query's stage zone ends with the
 * query, and its own applies only at read, not to the query that built the
 * table). With `restatesStageZoned`, when its query's stages computed in one
 * zone ({@link queryStageZone}), each such column stays, restated
 * (`stageZoned`), and a query reads one only where it truncates it again in
 * that zone (`checkZonedColumns`): `DATE_TRUNC` of a value already truncated
 * in a zone, in that zone, is that value. Otherwise each is left out
 * (`leftOut`) and a query reading it answers live. A source with no zone
 * keeps its columns (`restated`), built in the source's own zone, and a
 * query that reads one in another zone is declined (`checkZonedColumns`).
 */
function zonedTruncations(
   columns: readonly ServedColumn[],
   def: unknown,
   fieldsByName: ReadonlyMap<string, unknown>,
   /** The zone the source's chain sets, which its query's stage inherits. */
   chainZone?: string,
   restatesStageZoned = false,
): {
   columns: ServedColumn[];
   leftOut: string[];
   restated: string[];
   stageZoned?: { zone: string | undefined; names: string[] };
} {
   const truncation = (c: ServedColumn) => {
      const type = (fieldsByName.get(c.name) as { type?: unknown } | undefined)
         ?.type;
      return (
         c.timeframe !== undefined &&
         TIMEFRAMES.has(c.timeframe) &&
         !c.castTo &&
         (type === "timestamp" || type === "timestamptz")
      );
   };
   const truncated = columns.filter(truncation).map((c) => c.name);
   const queryShaped = (def as { query?: unknown } | undefined)?.query;
   if (queryShaped === undefined || !(queryTimezoneOf(def) ?? chainZone)) {
      return { columns: [...columns], leftOut: [], restated: truncated };
   }
   const stage = restatesStageZoned ? queryStageZone(def) : undefined;
   if (stage !== undefined) {
      return {
         columns: [...columns],
         leftOut: [],
         restated: [],
         ...(truncated.length > 0
            ? { stageZoned: { zone: stage.zone, names: truncated } }
            : {}),
      };
   }
   return {
      columns: columns.filter((c) => !truncation(c)),
      leftOut: truncated,
      restated: [],
   };
}
