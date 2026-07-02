import { Mutex } from "async-mutex";
import {
   DuckDBConnection as NeoConnection,
   DuckDBInstance,
   type DuckDBValue,
} from "@duckdb/node-api";
import * as path from "path";
import { DatabaseConnection } from "../DatabaseInterface";

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
         this.instance = await DuckDBInstance.create(this.dbPath);
         this.connection = await this.instance.connect();
         // Verify the connection works
         await this.connection.run("SELECT 42 as answer");
      } catch (err) {
         const message = err instanceof Error ? err.message : String(err);
         console.error("Failed to create DuckDB database:", err);
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
         console.log("DuckDB connection closed");
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
