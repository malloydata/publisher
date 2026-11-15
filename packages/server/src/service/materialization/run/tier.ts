// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The tier port a build runs through ({@link TierAdapter}), and what a
 * colocated build of one source starts from. Read by the source build and
 * the two adapters.
 *
 * Types only.
 */

import type {
   Connection as MalloyConnection,
   Manifest,
   PersistSource,
} from "@malloydata/malloy";
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import type { BuildTier } from "../../../materialization_metrics";
import type { QueryMetadata } from "../../query_metadata";
import type { sourceIncrementalRefresh } from "./incremental_build";

/** A source's incremental refresh the run can act on: its context and lineage. */
type IncrementalRefreshTarget = NonNullable<
   ReturnType<typeof sourceIncrementalRefresh>
>;

/**
 * The tier port: how a table is built where it lives. A run renders a
 * source's SQL and classifies its stored upstreams once (source_build.ts),
 * then hands the source to its tier's adapter, which builds it by whichever
 * method the run can take (a delta, else in full) and returns its manifest
 * entry:
 *
 * - {@link ColocatedTier} (colocated_build_strategy.ts), in the source's own
 *   warehouse: a bounded delta for an incremental table
 *   (incremental_delta_strategy.ts), else a staging CTAS and a swap onto the
 *   served name (one transaction where DDL is transactional);
 * - {@link StorageBuildStrategy} (storage_build_strategy.ts), into a `storage=`
 *   destination, its delta, when there is one, applied inside the
 *   destination session.
 *
 * A table's kind changes nothing here: it is in the text the plan wrote.
 */
export interface TierAdapter<Request> {
   readonly tier: BuildTier;
   build(request: Request): Promise<ManifestEntry>;
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
    * Statements that drop this package's own leftover `name=` view at the
    * name, run first in the swap's transaction (leftover_name_views.ts).
    */
   replacesView?: string[];
}
