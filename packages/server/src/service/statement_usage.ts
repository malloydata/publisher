// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { getDuckDBThreads } from "../config";
import { logger } from "../logger";
import { lazyCounter } from "../telemetry";
import { dbDuckDBInternalsMatch } from "./duckdb_instance_sharing";

/**
 * Where a metered statement ran: a model query (`serve`), raw SQL on a managed
 * connection (`sql`), a materialization build, or the admin API's load or copy.
 */
export type UsageSurface = "serve" | "sql" | "build" | "load" | "copy";

/**
 * One statement on a metered connection: a storage destination, or a connection
 * an orchestrator marked `managed`. A stable, generic record with no sink key: an
 * orchestrator's log collector maps `event: statement_usage` to its own meter.
 * Publisher never learns what is billed.
 */
export interface StatementUsage {
   environment: string;
   connection: string;
   surface: UsageSurface;
   package?: string;
   /**
    * The DuckLake catalog the statement ran on (see {@link usageCatalog}), which
    * names the same catalog whatever a connection is called. Absent when the
    * connection is not a DuckLake one or names no catalog.
    */
   catalog?: string;
   durationMs: number;
   rows?: number;
   status: "ok" | "error";
}

const usageCounter = lazyCounter(
   "publisher_statement_usage_total",
   "Statements on a metered connection (a storage destination or a managed connection), by surface and status.",
);

/** The part of a connection {@link usageCatalog} reads. */
export interface UsageCatalogSource {
   ducklakeConnection?: {
      catalog?: { metadataSchema?: string | null } | null;
   } | null;
}

/**
 * The DuckLake catalog a connection or storage destination attaches: its
 * `catalog.metadataSchema`, the schema of the catalog database that holds the
 * catalog, which is also the catalog's name on the admin API. Undefined for any
 * other connection, and for a DuckLake one that names no metadata schema.
 */
export function usageCatalog(
   connection: UsageCatalogSource | null | undefined,
): string | undefined {
   return connection?.ducklakeConnection?.catalog?.metadataSchema || undefined;
}

/**
 * The thread count a DuckDB instance of this process opens with (the
 * `duckdb.threads` setting). Undefined under `off`, where DuckDB sizes the pool
 * itself, and when the cap could not be installed (duckdb_instance_sharing.ts
 * skips it if `@malloydata/db-duckdb`'s internals changed shape): an instance
 * then opens one thread per core whatever the setting says, and a record must
 * not claim a size the statement did not run at. A connection that sets its
 * own `threads` is not reflected.
 */
function configuredThreads(): number | undefined {
   if (!dbDuckDBInternalsMatch()) return undefined;
   const threads = Number(getDuckDBThreads());
   return Number.isInteger(threads) && threads > 0 ? threads : undefined;
}

/** The process that ran the statement: the host name, a pod's name in Kubernetes. */
function workerName(): string | undefined {
   return hostname() || undefined;
}

/** A field metering reads for itself: absent, never a failed record, when it throws. */
function optional<T>(read: () => T | undefined): T | undefined {
   try {
      return read();
   } catch {
      return undefined;
   }
}

/**
 * Emits one usage record. Never throws: metering must not fail the statement
 * it describes. Each record carries a `statement_id` of its own, so a collector
 * that delivers a record twice can count it once.
 */
export function emitStatementUsage(usage: StatementUsage): void {
   try {
      const threads = optional(configuredThreads);
      const worker = optional(workerName);
      logger.info("statement_usage", {
         event: "statement_usage",
         statement_id: randomUUID(),
         environment: usage.environment,
         connection: usage.connection,
         ...(usage.catalog ? { catalog: usage.catalog } : {}),
         surface: usage.surface,
         ...(usage.package ? { package: usage.package } : {}),
         duration_ms: Math.max(0, Math.round(usage.durationMs)),
         ...(usage.rows !== undefined ? { rows: usage.rows } : {}),
         status: usage.status,
         ...(threads !== undefined ? { threads } : {}),
         ...(worker ? { worker } : {}),
      });
      usageCounter().add(1, { surface: usage.surface, status: usage.status });
   } catch {
      // Best-effort by design.
   }
}

/**
 * Times `body` and emits one record for it, success or failure, then returns or
 * rethrows its outcome unchanged.
 */
export async function withStatementUsage<T>(
   base: Omit<StatementUsage, "durationMs" | "status" | "rows">,
   body: () => Promise<T>,
   rowsOf?: (result: T) => number | undefined,
): Promise<T> {
   const start = performance.now();
   let result: T;
   try {
      result = await body();
   } catch (error) {
      emitStatementUsage({
         ...base,
         durationMs: performance.now() - start,
         status: "error",
      });
      throw error;
   }
   // Outside the body's try: reading the row count is metering's own work, so
   // a failure there is a record without rows, never a failed statement.
   let rows: number | undefined;
   try {
      rows = rowsOf?.(result);
   } catch {
      rows = undefined;
   }
   emitStatementUsage({
      ...base,
      durationMs: performance.now() - start,
      rows,
      status: "ok",
   });
   return result;
}
