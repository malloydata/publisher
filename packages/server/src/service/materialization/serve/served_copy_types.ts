// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's shared shapes: one compile of a model's served copy
 * ({@link Compiled}), one query's answer in progress in it
 * ({@link AnswerAttempt}), and why a copy or one of its sources is not
 * served ({@link ServedCopyProblem}). Read by the served copy
 * (served_copy_serve.ts) and its answer checks (served_copy_checks.ts).
 */

import type { ModelMaterializer, QueryMaterializer } from "@malloydata/malloy";
import type { GraftRuntime } from "../../gate_classification";
import type { ModelConnectionInput } from "../../model_runtime";
import type { FlatPair } from "../../governance/ir/pair_flat_query";
import type { readPreparedQuery } from "../../governance/ir/prepared_query";
import type { Binding } from "../domain/binding";
import type { LegDecline, ServedCopyDeclineCode } from "../domain/leg_decline";
import type { ServedCopyProblemReason } from "../domain/source_decline";
import type { LeftOutDefinition } from "../text/definition_cut";

export type VirtualMap = Map<string, Map<string, string>>;

/** Why a model's served copy, or one source in it, is not served. */
export interface ServedCopyProblem {
   /**
    * The left-out source's identity, when the problem is one source's: what a
    * caller keys on (the keying rule), never its name.
    */
   sourceID?: string;
   /** The source, when the problem is one source's. */
   sourceName?: string;
   reason: ServedCopyProblemReason;
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
   /**
    * The compiler's message, for logs and the package warning only. For
    * `column_types`: the columns the table cannot carry, each named (unless
    * hidden) with its declared and stored types, caller-safe, so the package
    * warning and the path report's `detail` say which.
    */
   detail?: string;
   /**
    * For `definition_not_parsed`: the columns the source's kept
    * `include { … }` hides that its stored table does not hold.
    */
   hiddenUnstored?: string[];
   /**
    * The problem is a storage table's, of a source bound in both tiers: the
    * served copy dropped that table and serves the source from its database
    * table instead.
    */
   servedFromDatabase?: true;
}

export interface Compiled {
   key: string;
   /** The invalidation generation it was compiled under. */
   generation: number;
   /**
    * The bindings it was compiled over: those it may read at the pin, by
    * identity. A pin whose bindings differ only in what the compile does not
    * read ({@link compileInputOf}) reuses it, rebound to those bindings.
    */
   over: readonly Binding[];
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
   /** The runtime it compiled in: a row-level graft reloads through it. */
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
   rerooted: Binding[];
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
    * probe compiler did not clear, or an inlined join's or pair table's
    * child column; or whose rows the probe did not clear (a `where:` with a
    * timestamp literal). The build evaluated it in the default timezone, so a
    * query that sets its own `timezone:` would read values live computes
    * differently.
    */
   computedTables?: ReadonlySet<string>;
   /**
    * Each re-root's zone-dependent stored columns, by its `sourceID` (two
    * sources can share one content-addressed table): a computed dimension
    * the probe compiler did not clear of the zone, and a restated timestamp
    * truncation, each built in the re-root's zone and read in the query's
    * (`checkZonedColumns`).
    */
   zonedColumns?: ReadonlyMap<string, ReadonlySet<string>>;
   /**
    * The zone each re-root's table was built in, by its `sourceID`: its own
    * or one it inherits, read from the author's model ({@link chainZoneOf}).
    */
   builtZones?: ReadonlyMap<string, string | undefined>;
   /** The author's model, whose sources keep the zones they inherit. */
   authorModelDef?: unknown;
   /**
    * Each re-root's timestamp truncations left out because they were stored
    * in another zone than any read's (`zonedTruncations`), by its
    * `sourceID`, so a query that reads one is declined with that reason.
    */
   zonedLeftOut?: ReadonlyMap<string, ReadonlySet<string>>;
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
   /** The columns each re-root leaves out, by its `sourceID`. */
   leftOutColumns?: ReadonlyMap<string, readonly string[]>;
   /**
    * Why each re-root leaves each column out, by its `sourceID`, then
    * column: a caller-safe phrase naming the column (unless it is hidden)
    * with its declared and stored types, for a decline's caller detail.
    */
   leftOutWhy?: ReadonlyMap<string, ReadonlyMap<string, string>>;
   aliased: Binding[];
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
    * The pair tables (inlined join_many) the copy reads flat: a query that
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
    * served copy: a query that reads one skips the served copy
    * with reason `cross_connection`.
    */
   leftOut?: readonly LeftOutDefinition[];
   /**
    * The overlay the copy compiled from (the re-rooted files, by overlay
    * key) and the connections it compiled with: the join-to-rollup branch
    * compiles a variant over the same files (see {@link ServedCopyServe.joinRollupBasis}).
    */
   files?: ReadonlyMap<string, string>;
   config?: ModelConnectionInput;
}

/**
 * One query's answer in progress in one compiled copy, as each of
 * {@link ServedCopyServe}'s answer checks reads it.
 */
export interface AnswerAttempt {
   compiled: Compiled;
   queryString: string;
   givens: Record<string, unknown> | undefined;
   runnable: QueryMaterializer;
   virtualMap: VirtualMap;
   /** Decline the query, remembered on the compile; never returns. */
   decline(
      reason: string,
      code: ServedCopyDeclineCode,
      callerDetail?: string,
   ): never;
   /** Run `work`, declining a query that reads a join left out across connections. */
   compiling<T>(work: () => Promise<T>): Promise<T>;
   /** The prepared query, read once. */
   prepared(): ReturnType<typeof readPreparedQuery>;
}
