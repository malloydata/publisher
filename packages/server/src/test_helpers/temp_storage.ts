// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import fs from "fs";
import os from "os";
import path from "path";
import type { ResourceRepository } from "../storage/DatabaseInterface";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../storage/duckdb/schema";

export interface TempStorage {
   /** A fresh temp directory, real-pathed so path comparisons hold on macOS. */
   root: string;
   /** `<root>/env`, created, and the path the environment's row records. */
   envPath: string;
   /** An in-memory storage database with the schema and the environment's row. */
   db: DuckDBConnection;
   repo: ResourceRepository;
}

/**
 * A temp directory and an in-memory storage database holding one environment
 * at `<root>/env`, as a server's storage would. Release it with
 * {@link disposeTempStorage}.
 */
export async function openTempStorage(
   prefix: string,
   environment: { id: string; name: string },
): Promise<TempStorage> {
   const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   const db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [environment.id, environment.name, envPath],
   );
   return { root, envPath, db, repo: createDuckDBRepository(db) };
}

/** Close the storage database and delete the temp directory. */
export async function disposeTempStorage(
   storage: Pick<TempStorage, "db" | "root">,
): Promise<void> {
   await storage.db.close();
   fs.rmSync(storage.root, { recursive: true, force: true });
}

export interface TempDuckDB {
   /** A fresh temp directory the connection's files live in. */
   dir: string;
   connection: MalloyDuckDBConnection;
   /** Close the connection (once) and delete the directory. */
   dispose(): Promise<void>;
}

/**
 * A Malloy DuckDB connection over a temp directory: on `<dir>/<file>` when
 * `file` is given, else in memory, with `dir` as its working directory when
 * `workingDirectory` is set (where DuckLake and `COPY` write relative files).
 * File-backed where a spec attaches or creates tables, because db-duckdb shares
 * one in-memory instance between connections of a process.
 */
export function openTempDuckDB(
   prefix: string,
   name: string,
   options: { file?: string; workingDirectory?: boolean } = {},
): TempDuckDB {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
   const connection = new MalloyDuckDBConnection(
      name,
      options.file ? path.join(dir, options.file) : ":memory:",
      ...(options.workingDirectory ? [dir] : []),
   );
   let disposed = false;
   return {
      dir,
      connection,
      dispose: async () => {
         if (disposed) return;
         disposed = true;
         await connection.close().catch(() => undefined);
         fs.rmSync(dir, { recursive: true, force: true });
      },
   };
}
