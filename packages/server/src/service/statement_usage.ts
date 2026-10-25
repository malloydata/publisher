// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../logger";
import { lazyCounter } from "../telemetry";

/** Where a metered statement ran. */
export type UsageSurface = "serve" | "sql" | "build" | "load";

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
   durationMs: number;
   rows?: number;
   status: "ok" | "error";
}

const usageCounter = lazyCounter(
   "publisher_statement_usage_total",
   "Statements on a metered connection (a storage destination or a managed connection), by surface and status.",
);

/**
 * Emits one usage record. Never throws: metering must not fail the statement
 * it describes.
 */
export function emitStatementUsage(usage: StatementUsage): void {
   try {
      logger.info("statement_usage", {
         event: "statement_usage",
         environment: usage.environment,
         connection: usage.connection,
         surface: usage.surface,
         ...(usage.package ? { package: usage.package } : {}),
         duration_ms: Math.max(0, Math.round(usage.durationMs)),
         ...(usage.rows !== undefined ? { rows: usage.rows } : {}),
         status: usage.status,
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
   try {
      const result = await body();
      emitStatementUsage({
         ...base,
         durationMs: performance.now() - start,
         rows: rowsOf?.(result),
         status: "ok",
      });
      return result;
   } catch (error) {
      emitStatementUsage({
         ...base,
         durationMs: performance.now() - start,
         status: "error",
      });
      throw error;
   }
}
