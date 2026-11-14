// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   Connection as MalloyConnection,
   Manifest,
   PersistSource,
} from "@malloydata/malloy";
import type { BuildInstruction, ManifestEntry } from "../types";
import type { QueryMetadata } from "../../query_metadata";
import type { sourceIncrementalRefresh } from "./incremental_build";
import type { SwapDependents } from "./name_view";

/** A source's incremental refresh the run can act on: its context and lineage. */
type IncrementalRefreshTarget = NonNullable<
   ReturnType<typeof sourceIncrementalRefresh>
>;

/**
 * How one tier materializes one source. A run renders the source's SQL and
 * classifies its stored upstreams once (source_build.ts), then hands the
 * source to the strategies of its tier in order: the first to return an entry
 * built it, and `undefined` passes the source to the next.
 *
 * - colocated: {@link IncrementalDeltaStrategy} advances an incremental
 *   table by a bounded delta, else {@link ColocatedCtasStrategy} rebuilds it
 *   by a staging CTAS and a swap onto the served name (one transaction where
 *   DDL is transactional), in the source's own warehouse;
 * - storage: {@link StorageBuildStrategy} builds into a `storage=` destination
 *   (its delta, when there is one, applies inside the destination session);
 * - instructed: {@link InstructedBuild} drives a run's instructions through
 *   the tiers in dependency order and records each outcome.
 */
export interface BuildStrategy<Request> {
   build(request: Request): Promise<ManifestEntry | undefined>;
}

/** What a colocated build of one source starts from. */
export interface ColocatedBuildRequest {
   persistSource: PersistSource;
   instruction: BuildInstruction;
   connection: MalloyConnection;
   /** The run's build Manifest, updated with this source's table once built. */
   manifest: Manifest;
   /** The build SQL, rendered over the run's manifest. */
   buildSQL: string;
   /** Every statement of the build carries these (see buildRunSQLOptions). */
   runOptions: { queryMetadata?: QueryMetadata };
   /** How the source's stored upstreams were read, for its entry. */
   upstreamReuse: Pick<
      ManifestEntry,
      "upstreamReuse" | "upstreamRecomputeReason"
   >;
   /** The source's incremental refresh, when it declares one the run can act on. */
   incrementalRefresh?: IncrementalRefreshTarget;
   /**
    * A `name=` view that reads the very table this build replaces, on a
    * dialect whose views bind to their table: the swap drops and remakes it
    * in its transaction (see {@link SwapDependents}).
    */
   swapDependents?: SwapDependents;
}
