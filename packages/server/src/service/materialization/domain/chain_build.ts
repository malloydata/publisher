// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a chained `storage=` build asks for, and gets back, through the host
 * port's `chainBuildText`: the Malloy a downstream builds from in its
 * destination, every stored upstream it reads re-rooted on that upstream's
 * table there, as the served copy re-roots it.
 *
 * Made by the run (run/storage_build_strategy.ts) and answered by the package
 * facade, which asks the served copy (serve/served_copy_serve.ts). Every input
 * is explicit: the upstreams' entries are the ones this run holds, schemas
 * included, never what the package has bound, since a build must not re-root
 * on an older table. Plain data both ways.
 *
 * Types only.
 */

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
   | { declined: string };
