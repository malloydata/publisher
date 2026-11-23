// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a chained `storage=` build asks for, and gets back, through the host
 * port's `chainBuildText`: the Malloy a downstream builds from in its
 * destination, every stored upstream it reads re-rooted on that upstream's
 * table there, as the served copy re-roots it.
 *
 * Its sibling, the plan's `chainStringReads` port, reads which strings such
 * a build compares, which its upstreams' recorded contracts must compare as
 * live does (`string_reads.ts`): the destination compares them byte-exact.
 *
 * Made by the run (run/storage_build_strategy.ts) and answered by the package
 * facade, which asks the served copy (serve/served_copy_serve.ts). Every input
 * is explicit: the upstreams' entries are the ones this run holds, schemas
 * included, never what the package has bound, since a build must not re-root
 * on an older table. Plain data both ways.
 *
 * Types only.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import type { ColumnContract } from "./column_contract";
import type { StringReads } from "./string_reads";
import type { MaterializePlan } from "./table_plans";
import type { ManifestEntry } from "./types";

/** The downstream, its destination, and the stored upstreams it may read. */
export interface ChainBuildRequest {
   /** The downstream persist source, by its plan's sourceID and name. */
   downstream: { sourceID: string; name: string };
   /** The destination the downstream and every upstream are stored in. */
   destination: { name: string; schema: string };
   /**
    * Each stored upstream in that destination: the entry this run holds for
    * its table (with its schema), and each name that reads that table with
    * the plan's sourceIDs for that name there (none known: the name alone).
    */
   upstreams: readonly {
      entry: ManifestEntry;
      names: Readonly<Record<string, readonly string[] | undefined>>;
   }[];
   /**
    * Set for a rollup, whose build source no author file declares: its
    * base, which the one upstream stores, and the rollup's own generated
    * build file (`downstream.name` declares it), served at `url` beside the
    * base's model and importing the base from it.
    */
   rollup?: {
      baseSourceID: string;
      companion: { url: string; text: string };
   };
   /**
    * Set for a persisted view or named query, whose build source no author
    * file declares: the author's declaration it runs (`authorName`: the
    * view's source, or the query), kept with its path to the stored
    * upstreams, and its generated build file (`downstream.name` declares
    * it), served at `url` beside the author's model and importing from it.
    */
   view?: {
      authorName: string;
      /** More of the author's declarations to keep as written (a query). */
      keepAlso?: readonly string[];
      companion: { url: string; text: string };
   };
}

/**
 * The files the downstream compiles from on the destination alone, or why
 * there are none. `files` holds every file the entry reaches, by URL: the
 * author's, with each upstream re-rooted in the file that declares it and
 * every declaration off the downstream's path blanked.
 */
export type ChainBuildText =
   | {
        files: ReadonlyMap<string, string>;
        /** The downstream's declaring file, which the build loads. */
        entry: string;
        /** The downstream's name in that file. */
        downstreamName: string;
        /** The connection the re-rooted upstreams read the destination by. */
        connection: string;
        /** connection → virtual handle → quoted table path. */
        virtualMap: ReadonlyMap<string, ReadonlyMap<string, string>>;
        /** The upstreams re-rooted, by name. */
        rerooted: readonly string[];
        /**
         * Where each re-rooted upstream's declaration lies in `files`, by
         * URL and character range: a definition inside one that does not
         * compile on the destination (a join to a source the build cannot
         * read, and what reads it) may be left out, as the served copy leaves
         * out a join across connections. Nothing outside them is.
         */
        rerootedSpans: readonly { url: string; start: number; end: number }[];
     }
   | {
        declined: string;
        /**
         * `package_stale`: the package serves a model older than its files,
         * so no text can be written from them that the plan's addresses
         * describe. Absent: the text could not be written for `declined`.
         */
        code?: "package_stale";
     };

/**
 * The build whose compared strings the plan's `chainStringReads` port reads,
 * by the author's names: a query-shaped `downstream`'s own query over its
 * base, a `rollup`'s grouping of its base, or a view's or named query's
 * `run:` text. `stored` names, by sourceID, the upstreams whose tables it
 * reads.
 */
export type ChainStringReadsRequest = { stored: readonly string[] } & (
   | { downstream: string; rollup?: undefined }
   | {
        rollup: {
           baseName: string;
           grain: readonly string[];
           measures: readonly string[];
        };
     }
   | { run: string }
);

/**
 * Reads the strings a build compares ({@link ChainStringReadsRequest}) in
 * the model `load` compiles queries in, at plan: handed to the plan, which
 * may not read the served copy.
 */
export type PlanChainStringReads = (
   load: (query: string) => QueryMaterializer,
   request: ChainStringReadsRequest,
) => Promise<StringReads>;

/**
 * The served copy's `chainBuildText`, as the plan is handed it (plan may not
 * read the served copy): the text a downstream of `model` builds from in its
 * destination, or undefined when `model` does not declare it.
 */
export type PlanChainBuildText = (
   request: ChainBuildRequest,
   model: {
      modelPath: string;
      modelDef: unknown;
      materializePlans?: Readonly<Record<string, MaterializePlan>>;
   },
) => ChainBuildText | undefined;

/**
 * The stored columns the package has bound for the upstream `sourceID` in
 * `destination` (its recorded contract's native types, else its schema), or
 * undefined when none is bound there: what a plan stands in for that
 * upstream's table (plan/storage_over_stored.ts).
 */
export type BoundStorageColumns = (
   sourceID: string,
   destination: string,
) => { name: string; type: string }[] | undefined;

/**
 * The column contract the package has bound for the upstream `sourceID` in
 * `destination`, or undefined when none is bound there (or it records
 * none): what a plan judges a build's compared strings against
 * (`string_reads.ts`) before any run.
 */
export type BoundStorageContract = (
   sourceID: string,
   destination: string,
) => ColumnContract | undefined;

/**
 * The column contract the package has bound for the table of `sourceID`: in
 * storage destination `storage` when one is given, else its database table;
 * undefined when none is bound there (or it records none). What a plan
 * judges an extension table's build over a pair-table parent by (the
 * parent's leaf key checks, `ColumnContract.keys`) before any run.
 */
export type BoundTableContract = (
   sourceID: string,
   storage: string | undefined,
) => ColumnContract | undefined;
