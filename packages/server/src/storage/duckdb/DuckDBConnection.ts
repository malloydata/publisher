// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import {
   DuckDBConnection as NeoConnection,
   DuckDBInstance,
   type DuckDBValue,
} from "@duckdb/node-api";
import * as path from "path";
import { logger } from "../../logger";
import { getDuckDBMemoryLimit, getDuckDBTempDirectory } from "../../config";

/**
 * The `memory_limit` / `temp_directory` instance options, when configured.
 * Empty when neither is set, so a deployment that has not opted in gets
 * DuckDB's defaults.
 */
function duckDBInstanceResourceOptions(): Record<string, string> {
   const options: Record<string, string> = {};
   const memoryLimit = getDuckDBMemoryLimit();
   if (memoryLimit !== undefined) {
      options["memory_limit"] = memoryLimit;
   }
   const tempDirectory = getDuckDBTempDirectory();
   if (tempDirectory !== undefined) {
      options["temp_directory"] = tempDirectory;
   }
   return options;
}

/**
 * Embedded persistence layer for the publisher's own metadata (environments,
 * packages, connections, materializations, build manifests) in `publisher.db`.
 *
 * This is a plain DAO over a durable, exclusively-owned DuckDB handle -- it is
 * deliberately NOT Malloy's `@malloydata/db-duckdb` connection, which is an
 * analytical query connection (no prepared-statement parameter binding, a
 * `:memory:` primary with ATTACH/DETACH/idle lifecycle, pooled/shared
 * instances, and a poison-pill close). Those semantics are wrong for a
 * source-of-truth store that must hold one handle open for the server's
 * lifetime and run parameterized CRUD.
 *
 * It wraps `@duckdb/node-api` (the same DuckDB engine Malloy pulls in), so the
 * repo carries a single DuckDB engine rather than a second, redundant driver.
 */
export class DuckDBConnection {
   private instance: DuckDBInstance | null = null;
   private connection: NeoConnection | null = null;
   private dbPath: string;
   private mutex: Mutex = new Mutex();

   constructor(dbPath?: string) {
      this.dbPath = dbPath || path.join(process.cwd(), "publisher.db");
   }

   async initialize(): Promise<void> {
      try {
         // This long-lived instance sizes its memory from the container like
         // every other DuckDB, and the embedding index runs real compute on it,
         // so it shares the same resource budget. Malloy's session funnel never
         // sees this class, so instance config is the only hook.
         this.instance = await DuckDBInstance.create(
            this.dbPath,
            duckDBInstanceResourceOptions(),
         );
         this.connection = await this.instance.connect();
         await this.connection.run("SELECT 42 as answer");
      } catch (err) {
         const message = err instanceof Error ? err.message : String(err);
         logger.error("Failed to create DuckDB database:", err);
         throw new Error(`Failed to initialize DuckDB: ${message}`);
      }
   }

   async close(): Promise<void> {
      try {
         if (this.connection) {
            this.connection.closeSync();
            this.connection = null;
         }
         if (this.instance) {
            this.instance.closeSync();
            this.instance = null;
         }
         // Debug, not stdout: the schema reconcile opens and closes a scratch
         // in-memory database on every boot, and an unconditional "closed" line
         // during startup reads as the real store going away.
         logger.debug("DuckDB connection closed");
      } catch (err) {
         const message = err instanceof Error ? err.message : String(err);
         throw new Error(`Failed to close DuckDB connection: ${message}`);
      }
   }

   async isInitialized(): Promise<boolean> {
      if (!this.connection) return false;

      return this.mutex.runExclusive(async () => {
         try {
            const reader = await this.connection!.runAndReadAll(
               "SELECT name FROM sqlite_master WHERE type='table' AND name='environments'",
            );
            return reader.getRowObjectsJS().length > 0;
         } catch {
            return false;
         }
      });
   }

   async run(query: string, params?: unknown[]): Promise<void> {
      if (!this.connection) {
         throw new Error("Database not initialized");
      }

      return this.mutex.runExclusive(async () => {
         try {
            await this.connection!.run(query, params as DuckDBValue[]);
         } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            throw new Error(
               `Query execution failed: ${message}\nQuery: ${query}`,
            );
         }
      });
   }

   async all<T>(query: string, params?: unknown[]): Promise<T[]> {
      if (!this.connection) {
         throw new Error("Database not initialized");
      }

      return this.mutex.runExclusive(async () => {
         try {
            const reader = await this.connection!.runAndReadAll(
               query,
               params as DuckDBValue[],
            );
            return reader.getRowObjectsJS() as T[];
         } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            throw new Error(
               `Query execution failed: ${message}\nQuery: ${query}`,
            );
         }
      });
   }

   async get<T>(query: string, params?: unknown[]): Promise<T | null> {
      const rows = await this.all<T>(query, params);
      return rows.length > 0 ? rows[0] : null;
   }
}
