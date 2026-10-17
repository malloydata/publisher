// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../logger";
import { ResourceRepository } from "./DatabaseInterface";
import { DuckDBConnection } from "./duckdb/DuckDBConnection";
import { createDuckDBRepository } from "./duckdb/DuckDBRepository";
import { initializeSchema } from "./duckdb/schema";

export interface StorageConfig {
   type: "duckdb";
   duckdb?: {
      path?: string;
   };
}

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
      await connection.initialize();
      await initializeSchema(connection, reinit);

      this.connection = connection;
      this.repository = createDuckDBRepository(connection);
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
