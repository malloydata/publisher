// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../logger";
import { lazyCounter } from "../telemetry";
import { ResourceRepository } from "./DatabaseInterface";
import { DuckDBConnection } from "./duckdb/DuckDBConnection";
import { createDuckDBRepository } from "./duckdb/DuckDBRepository";
import { MaterializationRepository } from "./duckdb/MaterializationRepository";
import { initializeSchema } from "./duckdb/schema";

export interface StorageConfig {
   type: "duckdb";
   duckdb?: {
      path?: string;
   };
}

/** The error an interrupted run is settled with. */
export const INTERRUPTED_RUN_ERROR =
   "Interrupted: the server restarted during this run";

/**
 * Settle the runs a previous process left unsettled (see
 * MaterializationRepository.failInterrupted), before anything reads the store:
 * otherwise each would hold its package's (or version's) build slot forever,
 * refusing every later build of it with a conflict. A settled run is FAILED,
 * as a run that threw is, so what reclaims a failed run's tables (a version's
 * archive or delete) reclaims its tables too, and the tables it claimed are
 * free to the next build.
 */
export async function failInterruptedMaterializations(
   connection: DuckDBConnection,
): Promise<void> {
   const settled = await new MaterializationRepository(
      connection,
   ).failInterrupted(INTERRUPTED_RUN_ERROR);
   for (const m of settled) {
      logger.warn(
         "Marked a materialization FAILED: the server restarted during its run",
         {
            materializationId: m.id,
            environmentId: m.environmentId,
            packageName: m.packageName,
            ...(m.version !== null ? { version: m.version } : {}),
            ...(Array.isArray(m.metadata?.writesTables)
               ? { writesTables: m.metadata.writesTables }
               : {}),
         },
      );
   }
}

/**
 * Startup sweeps of interrupted materialization runs that threw. Counted
 * because the boot carries on without reporting it anywhere else.
 */
const interruptedRunSweepFailureCounter = lazyCounter(
   "publisher_interrupted_run_sweep_failures_total",
   "Startup sweeps of the materialization runs a restart interrupted that failed. The server still starts; the unsettled runs refuse new builds of their package until a later restart's sweep settles them.",
);

/**
 * Owns the storage backend (DuckDB) and exposes the resource repository. Only
 * the publisher's own control records live here; build manifests are returned
 * inline on the materialization resource and persisted by the control plane.
 */
export class StorageManager {
   private connection: DuckDBConnection | null = null;
   private repository: ResourceRepository | null = null;

   constructor(private config: StorageConfig) {}

   async initialize(reinit: boolean = false): Promise<void> {
      if (reinit) {
         logger.info(
            "Reinitialization mode: Database will be dropped and recreated",
         );
      } else {
         logger.info("Normal mode: Loading from existing database");
      }

      if (this.config.type !== "duckdb") {
         throw new Error(`Unsupported storage type: ${this.config.type}`);
      }
      const connection = new DuckDBConnection(this.config.duckdb?.path);
      try {
         await connection.initialize();
         await initializeSchema(connection, reinit);
      } catch (error) {
         // Never assigned, so close() would not reach it: release it here.
         await connection.close().catch((closeError: unknown) =>
            logger.warn(
               "Could not close the storage database after a failed initialization",
               {
                  error:
                     closeError instanceof Error
                        ? closeError.message
                        : String(closeError),
               },
            ),
         );
         throw error;
      }

      this.connection = connection;
      this.repository = createDuckDBRepository(connection);

      // Not fatal: the store works without it. A failed sweep leaves the
      // interrupted runs holding their build slots, which refuses builds of
      // those packages until a later restart's sweep settles them; failing
      // the boot would take every package down instead.
      try {
         await failInterruptedMaterializations(connection);
      } catch (error) {
         interruptedRunSweepFailureCounter().add(1);
         logger.error(
            "Could not settle the materialization runs a restart interrupted; starting anyway",
            {
               error: error instanceof Error ? error.message : String(error),
               ...(error instanceof Error && error.stack
                  ? { stack: error.stack }
                  : {}),
            },
         );
      }
   }

   /**
    * Raw SQL access for callers that don't fit `ResourceRepository` (e.g.
    * ThemeStore). Prefer `getRepository()` where it fits.
    */
   getDuckDbConnection(): DuckDBConnection {
      if (!this.connection) {
         throw new Error("Storage not initialized");
      }
      return this.connection;
   }

   getRepository(): ResourceRepository {
      if (!this.repository) {
         throw new Error("Storage not initialized. Call initialize() first.");
      }
      return this.repository;
   }

   async close(): Promise<void> {
      if (this.connection) {
         await this.connection.close();
         this.connection = null;
         this.repository = null;
      }
   }

   isInitialized(): boolean {
      return this.connection !== null && this.repository !== null;
   }
}
