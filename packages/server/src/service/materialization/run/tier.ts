// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The tier port a build runs through ({@link TierAdapter}): building a table,
 * dropping one, and running a key check over one, where the table lives.
 * Also what a database build of one source starts from, and where each
 * tier's tables are reached. Read by the source build, the instructed
 * build's key checks, the reclaimer and the two adapters.
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
import type { ApiConnection } from "../../model";
import type { QueryMetadata } from "../../query_metadata";
import type { RollupKeyRecord } from "./inline_uniqueness";
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
 * - {@link DatabaseTier} (database_build_strategy.ts), in the source's own
 *   warehouse: a bounded delta for an incremental table
 *   (incremental_delta_strategy.ts), else a staging CTAS and a swap onto the
 *   served name (one transaction where DDL is transactional);
 * - {@link StorageBuildStrategy} (storage_build_strategy.ts), into a `storage=`
 *   destination, its delta, when there is one, applied inside the
 *   destination session.
 *
 * A table's kind changes nothing here: it is in the text the plan wrote.
 *
 * The same adapter drops a table it holds (the reclaimer's drops) and runs a
 * key check over one (the instructed build's checks after a build), at the
 * tier's {@link DatabaseSite} or {@link StorageSite}.
 */
export interface TierAdapter<Request, Site> {
   readonly tier: BuildTier;
   build(request: Request): Promise<ManifestEntry>;
   /**
    * Drop one table at `site`, and the names a crash can leave beside it
    * when the table names its source ({@link DroppedTable}). Exactly the
    * named tables, never a pattern or scan; whoever calls decides what goes.
    */
   drop(site: Site, table: DroppedTable): Promise<void>;
   /**
    * Run a key check over a table at `site`: what it found, or undefined
    * when it did not run. A throw is read as the same: the key is unknown and
    * the build goes on (instructed_build.ts, checkAndRecordKey).
    */
   checkKey(
      site: Site,
      check: KeyCheck,
      table: string,
   ): Promise<KeyCheckRecord | undefined>;
}

/** A table a tier drops. */
export interface DroppedTable {
   physicalTableName: string;
   /**
    * The source the table was built for: the staging and backup names a
    * crash can leave derive from it, and are dropped after the table.
    * Absent, only the table is dropped.
    */
   leftoversOf?: string;
   /**
    * A failed drop of the table: rethrow, or return when the table is gone
    * anyway (an earlier release's view holds the name). Absent, it rethrows.
    */
   onDropFailed?: (err: unknown) => Promise<void>;
}

/** A key check: one row when a key value repeats or is NULL. */
export interface KeyCheck {
   keyColumn: string;
   sql: string;
}

/** What a key check found over one table. */
export type KeyCheckRecord = Omit<RollupKeyRecord, "checkedInRun">;

/** Where a database table is reached: its warehouse connection. */
export interface DatabaseSite {
   connection: MalloyConnection;
   /** Every statement run there carries these (a drop's attribution). */
   runOptions?: { queryMetadata?: QueryMetadata };
}

/** Where a `storage=` table is reached: its destination. */
export interface StorageSite {
   destinationName: string;
   destinationConnection: ApiConnection;
   environmentPath: string;
}

/** What a database build of one source starts from. */
export interface DatabaseBuildRequest {
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
