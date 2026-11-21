// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Each passthrough source type's part in a `storage=` build, keyed by the
 * source connection's configured type ({@link FederatedSourceType}), never by
 * Malloy's dialect: how a read is prepared and, where it can be, labelled;
 * what a session needs before the read and after the build; what a read
 * cost; how a cancelled build's warehouse jobs are cancelled; and whether
 * the late-row check reads a stored source's window. The engines' own
 * modules (passthrough/) implement them; the build session and incremental
 * storage read them here, and never branch on a source type themselves.
 *
 * Looked up with `Object.hasOwn`, so a type string that names an
 * `Object.prototype` member is no entry; a caller keeps what it did for a
 * type with no entry.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../../api";
import type { FederatedSourceType } from "../../dialects";
import type { QueryMetadata } from "../../query_metadata";
import type { BuildReadCost } from "./build_read_cost";
import {
   prepareBigQueryRead,
   splitBigQueryRead,
   trackBigQueryBuildJobs,
   type BigQueryJobListerFactory,
} from "./passthrough/bigquery";

type ApiConnection = components["schemas"]["Connection"];

/**
 * What the CTAS reads from, plus the warehouse's id for the read when its shape
 * yields one. `jobId` lets a build's cost be recovered by exact key rather than
 * by matching query text.
 */
export interface PassthroughRead {
   /** The SELECT the CTAS wraps. */
   selectSQL: string;
   /** The warehouse's id for the read, null when this shape does not produce one. */
   jobId: string | null;
   /**
    * What the read cost, when the shape that issued it also reported it. Read
    * from the same job record that located the result table: a script's child
    * job is reachable only through its parent, so no later lookup can find it.
    */
   cost: BuildReadCost | null;
}

/** Test seams a passthrough build reads; production passes none. */
export interface PassthroughDeps {
   /** The BigQuery jobs API a cancelled build cancels its reads through. */
   bigQueryJobs?: BigQueryJobListerFactory;
}

/** One passthrough source type's part in a build. */
interface PassthroughBuild {
   /**
    * `sql` as this engine's read sends it, and the label a labelled read
    * applies; a label is returned only when the engine can issue the read
    * labelled ({@link splitRead}).
    */
   prepareRead(
      session: DuckDBConnection,
      handle: string,
      sql: string,
      queryMetadata?: QueryMetadata,
   ): Promise<{ sql: string; label?: string }>;
   /** The labelled read of a prepared `sql`, issued in its own shape. */
   splitRead?(
      session: DuckDBConnection,
      handle: string,
      sql: string,
      label: string,
   ): Promise<PassthroughRead>;
   /**
    * Track the warehouse jobs the session's reads start, and cancel them when
    * `signal` aborts; the returned function stops tracking and waits for a
    * cancel in progress.
    */
   trackJobs?(
      session: DuckDBConnection,
      signal: AbortSignal | undefined,
      handle: string,
      connection: ApiConnection,
      deps: PassthroughDeps,
   ): () => Promise<void>;
   /**
    * Whether the late-row check reads a stored source's window through this
    * engine's passthrough (DeltaTarget.windowRows).
    */
   readonly windowRows: boolean;
}

/** A read sent as the build wrote it. */
async function readAsWritten(
   _session: DuckDBConnection,
   _handle: string,
   sql: string,
): Promise<{ sql: string; label?: string }> {
   return { sql };
}

/** Every passthrough source type's part in a build. */
const PASSTHROUGH_BUILDS: Readonly<
   Record<FederatedSourceType, PassthroughBuild>
> = Object.freeze({
   // A billed job the session's interrupt does not stop: marked, labelled
   // through the split, and cancelled in BigQuery. Its window is read by a
   // billed job outside the late-row check's abort, so it is not read.
   bigquery: {
      prepareRead: prepareBigQueryRead,
      splitRead: splitBigQueryRead,
      trackJobs: (session, signal, handle, connection, deps) =>
         trackBigQueryBuildJobs(
            session,
            signal,
            handle,
            connection,
            deps.bigQueryJobs,
         ),
      windowRows: false,
   },
   snowflake: {
      prepareRead: readAsWritten,
      windowRows: true,
   },
   postgres: {
      prepareRead: readAsWritten,
      windowRows: true,
   },
});

/** The build part of passthrough source type `type`, or undefined for any other value. */
export function passthroughBuildOf(
   type: unknown,
): PassthroughBuild | undefined {
   return typeof type === "string" && Object.hasOwn(PASSTHROUGH_BUILDS, type)
      ? PASSTHROUGH_BUILDS[type as FederatedSourceType]
      : undefined;
}
