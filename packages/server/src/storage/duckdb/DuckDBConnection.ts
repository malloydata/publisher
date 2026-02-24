import { Mutex } from "async-mutex";
import * as duckdb from "duckdb";
import * as path from "path";
import { DatabaseConnection } from "../DatabaseInterface";

export class DuckDBConnection implements DatabaseConnection {
   private db: duckdb.Database | null = null;
   private connection: duckdb.Connection | null = null;
   private dbPath: string;
   private mutex: Mutex = new Mutex();

   constructor(dbPath?: string) {
      // Default to storing in the server root directory
      this.dbPath = dbPath || path.join(process.cwd(), "publisher.db");
   }

   async initialize(): Promise<void> {
      return new Promise((resolve, reject) => {
         this.db = new duckdb.Database(this.dbPath, {}, (err) => {
            if (err) {
               console.error("Failed to create DuckDB database:", err);
               reject(new Error(`Failed to initialize DuckDB: ${err.message}`));
               return;
            }

            // Connect synchronously
            this.connection = (
               this.db as duckdb.Database & { connect(): duckdb.Connection }
            ).connect();

            if (!this.connection) {
               reject(new Error("Failed to create connection object"));
               return;
            }

            // Verify connection works
            this.connection.all("SELECT 42 as answer", (testErr, _rows) => {
               if (testErr) {
                  console.error("Connection test failed:", testErr);
                  reject(
                     new Error(
                        `Failed to verify DuckDB connection: ${testErr.message}`,
                     ),
                  );
                  return;
               }

               resolve();
            });
         });
      });
   }

   async close(): Promise<void> {
      return new Promise((resolve, reject) => {
         if (this.connection) {
            this.connection.close((err) => {
               if (err) {
                  reject(
                     new Error(
                        `Failed to close DuckDB connection: ${err.message}`,
                     ),
                  );
                  return;
               }

               if (this.db) {
                  this.db.close((dbErr) => {
                     if (dbErr) {
                        reject(
                           new Error(
                              `Failed to close DuckDB: ${dbErr.message}`,
                           ),
                        );
                        return;
                     }
                     console.log("DuckDB connection closed");
                     resolve();
                  });
               } else {
                  resolve();
               }
            });
         } else {
            resolve();
         }
      });
   }

   async isInitialized(): Promise<boolean> {
      if (!this.connection) return false;

      return this.mutex.runExclusive(async () => {
         return new Promise<boolean>((resolve) => {
            this.connection!.all(
               "SELECT name FROM sqlite_master WHERE type='table' AND name='projects'",
               (err, rows) => {
                  if (err) {
                     resolve(false);
                     return;
                  }
                  resolve(rows && rows.length > 0);
               },
            );
         });
      });
   }

   async run(query: string, params?: unknown[]): Promise<void> {
      if (!this.connection) {
         throw new Error("Database not initialized");
      }

      return this.mutex.runExclusive(async () => {
         return new Promise<void>((resolve, reject) => {
            const callback = (err: Error | null) => {
               if (err) {
                  reject(
                     new Error(
                        `Query execution failed: ${err.message}\nQuery: ${query}`,
                     ),
                  );
                  return;
               }
               resolve();
            };

            // Pass params directly without the params argument if empty
            if (params && params.length > 0) {
               this.connection!.run(query, ...params, callback);
            } else {
               this.connection!.run(query, callback);
            }
         });
      });
   }

   async all<T>(query: string, params?: unknown[]): Promise<T[]> {
      if (!this.connection) {
         throw new Error("Database not initialized");
      }

      return this.mutex.runExclusive(async () => {
         return new Promise<T[]>((resolve, reject) => {
            const callback = (err: Error | null, rows: unknown[]) => {
               if (err) {
                  reject(
                     new Error(
                        `Query execution failed: ${err.message}\nQuery: ${query}`,
                     ),
                  );
                  return;
               }
               resolve((rows || []) as T[]);
            };

            if (params && params.length > 0) {
               this.connection!.all(query, ...params, callback);
            } else {
               this.connection!.all(query, callback);
            }
         });
      });
   }

   async get<T>(query: string, params?: unknown[]): Promise<T | null> {
      const rows = await this.all<T>(query, params);
      return rows.length > 0 ? rows[0] : null;
   }

   getConnection(): duckdb.Connection {
      if (!this.connection) {
         throw new Error("Database not initialized");
      }
      return this.connection;
   }
}
