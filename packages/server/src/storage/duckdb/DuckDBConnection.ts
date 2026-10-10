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
import { DatabaseConnection } from "../DatabaseInterface";
import { getDuckDBMemoryLimit, getDuckDBTempDirectory } from "../../config";

/**
 * The `memory_limit` / `temp_directory` instance options, when configured.
 * Empty when neither is set, so `DuckDBInstance.create` sees exactly what it saw
 * before and nothing changes for a deployment that has not opted in.
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
 * Whether `err` is a UNIQUE / PRIMARY KEY violation. DuckDB reports one as a
 * plain Error whose message names the duplicate key ("Duplicate key ...
 * violates primary key constraint"), so this matches that text.
 */
export function isUniqueViolation(err: unknown): boolean {
   return (
      err instanceof Error &&
      /duplicate key|unique constraint|primary key constraint/i.test(
         err.message,
      )
   );
}

/**
 * The statements available inside {@link DuckDBConnection.transaction}. They
 * run on the transaction's connection without taking its mutex, which the
 * transaction already holds.
 */
export interface TransactionQueries {
   run(query: string, params?: unknown[]): Promise<void>;
   all<T>(query: string, params?: unknown[]): Promise<T[]>;
   get<T>(query: string, params?: unknown[]): Promise<T | null>;
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
export class DuckDBConnection implements DatabaseConnection {
   private instance: DuckDBInstance | null = null;
   private connection: NeoConnection | null = null;
   private dbPath: string;
   private mutex: Mutex = new Mutex();

   constructor(dbPath?: string) {
      // Default to storing in the server root directory
      this.dbPath = dbPath || path.join(process.cwd(), "publisher.db");
   }

   async initialize(): Promise<void> {
      try {
         // The metadata store is a real DuckDB instance held open for the whole
         // server lifetime, and it sizes its memory from the container exactly
         // like every other one — so it belongs in the same budget. It is a
         // different class from Malloy's connection, so the session funnel that
         // bounds those never sees it; passed as instance config here instead,
         // which is the only hook this one has. Not idle, either: the embedding
         // index runs real compute against it.
         this.instance = await DuckDBInstance.create(
            this.dbPath,
            duckDBInstanceResourceOptions(),
         );
         this.connection = await this.instance.connect();
         // Verify the connection works
         await this.connection.run("SELECT 42 as answer");
      } catch (err) {
         const message = err instanceof Error ? err.message : String(err);
         console.error("Failed to create DuckDB database:", err);
         throw new Error(`Failed to initialize DuckDB: ${message}`);
      }
   }

   /**
    * Close the handle once the statement in flight, if any, has finished:
    * closing under a running statement frees what it is still using, which
    * hangs or crashes the process. A statement queued behind the close is
    * refused as not initialized rather than run on a closed handle.
    */
   async close(): Promise<void> {
      return this.mutex.runExclusive(() => {
         try {
            if (this.connection) {
               this.connection.closeSync();
               this.connection = null;
            }
            if (this.instance) {
               this.instance.closeSync();
               this.instance = null;
            }
            // Debug, not stdout: the schema reconcile opens and closes a
            // scratch in-memory database on every boot, and an unconditional
            // "closed" line during startup reads as the real store going away.
            logger.debug("DuckDB connection closed");
         } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            throw new Error(`Failed to close DuckDB connection: ${message}`);
         }
      });
   }

   async isInitialized(): Promise<boolean> {
      if (!this.connection) return false;

      return this.mutex.runExclusive(async () => {
         if (!this.connection) return false;
         try {
            const reader = await this.connection.runAndReadAll(
               "SELECT name FROM sqlite_master WHERE type='table' AND name='environments'",
            );
            return reader.getRowObjectsJS().length > 0;
         } catch {
            return false;
         }
      });
   }

   async run(query: string, params?: unknown[]): Promise<void> {
      this.requireConnection();
      return this.mutex.runExclusive(() =>
         this.runUnlocked(this.requireConnection(), query, params),
      );
   }

   async all<T>(query: string, params?: unknown[]): Promise<T[]> {
      this.requireConnection();
      return this.mutex.runExclusive(() =>
         this.allUnlocked<T>(this.requireConnection(), query, params),
      );
   }

   async get<T>(query: string, params?: unknown[]): Promise<T | null> {
      const rows = await this.all<T>(query, params);
      return rows.length > 0 ? rows[0] : null;
   }

   /**
    * Run `fn` as one transaction: every statement it issues through `tx`
    * commits together, or none does. The connection's mutex is held from
    * BEGIN to COMMIT/ROLLBACK, so no other statement on this connection can
    * interleave: a read inside `fn` and the write that depends on it see the
    * same state, which a sequence of `run`/`all` calls (each taking the mutex
    * on its own) cannot promise.
    *
    * `fn` must issue its statements through `tx`. Calling this connection's own
    * `run`/`all`/`get` from inside it waits for the mutex `fn` holds, forever.
    * A throw from `fn` rolls back and is rethrown unchanged.
    */
   async transaction<T>(
      fn: (tx: TransactionQueries) => Promise<T>,
   ): Promise<T> {
      this.requireConnection();
      return this.mutex.runExclusive(async () => {
         // Again under the mutex: a close may have run while this waited.
         const connection = this.requireConnection();
         const tx: TransactionQueries = {
            run: (query, params) => this.runUnlocked(connection, query, params),
            all: <R>(query: string, params?: unknown[]) =>
               this.allUnlocked<R>(connection, query, params),
            get: async <R>(query: string, params?: unknown[]) => {
               const rows = await this.allUnlocked<R>(
                  connection,
                  query,
                  params,
               );
               return rows.length > 0 ? rows[0] : null;
            },
         };
         await this.runUnlocked(connection, "BEGIN TRANSACTION");
         try {
            const result = await fn(tx);
            await this.runUnlocked(connection, "COMMIT");
            return result;
         } catch (err) {
            // Also after a failed COMMIT, so the connection is never left
            // inside an open transaction. DuckDB has usually rolled that one
            // back already, and the ROLLBACK then fails harmlessly.
            try {
               await this.runUnlocked(connection, "ROLLBACK");
            } catch (rollbackErr) {
               logger.debug("DuckDB ROLLBACK after a failed transaction", {
                  error: rollbackErr,
               });
            }
            throw err;
         }
      });
   }

   private requireConnection(): NeoConnection {
      if (!this.connection) {
         throw new Error("Database not initialized");
      }
      return this.connection;
   }

   private async runUnlocked(
      connection: NeoConnection,
      query: string,
      params?: unknown[],
   ): Promise<void> {
      try {
         await connection.run(query, params as DuckDBValue[]);
      } catch (err) {
         const message = err instanceof Error ? err.message : String(err);
         throw new Error(`Query execution failed: ${message}\nQuery: ${query}`);
      }
   }

   private async allUnlocked<T>(
      connection: NeoConnection,
      query: string,
      params?: unknown[],
   ): Promise<T[]> {
      try {
         const reader = await connection.runAndReadAll(
            query,
            params as DuckDBValue[],
         );
         return reader.getRowObjectsJS() as T[];
      } catch (err) {
         const message = err instanceof Error ? err.message : String(err);
         throw new Error(`Query execution failed: ${message}\nQuery: ${query}`);
      }
   }
}
