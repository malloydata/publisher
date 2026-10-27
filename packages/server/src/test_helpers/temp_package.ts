// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package on disk, loaded through the real production path
 * (`Package.create`, which compiles in the package-load worker pool), over an
 * in-memory DuckDB. For specs that need what a served package actually does,
 * not a hand-built model.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   MalloyConfig,
   type Connection,
} from "@malloydata/malloy";
import { afterAll, beforeAll } from "bun:test";
import { randomUUID } from "crypto";
import fs from "fs";
import os from "os";
import path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Model } from "../service/model";
import { Package } from "../service/package";

/**
 * An in-memory DuckDB named `duckdb`, with `seedSql` (`;`-separated
 * statements) already run on it. Only this process sees the seeded tables: a
 * package-load worker opens its own DuckDB, so a package loaded through the
 * pool reads inline `duckdb.sql(...)` rows instead.
 *
 * Each call gets its own database. db-duckdb pools `:memory:` connections
 * with the same config into one database for as long as any of them is open,
 * so two specs seeding a table of the same name would read each other's.
 * The temp directory is part of the pool's key, so a fresh one per call keeps
 * this connection out of the pool; nothing here spills, so it is never
 * created.
 */
export async function seededDuckdb(seedSql = ""): Promise<DuckDBConnection> {
   const duckdb = new DuckDBConnection({
      name: "duckdb",
      databasePath: ":memory:",
      workingDirectory: ".",
      tempDirectory: path.join(os.tmpdir(), `seeded-duckdb-${randomUUID()}`),
   });
   for (const statement of seedSql.split(";")) {
      if (statement.trim()) await duckdb.runSQL(statement.trim() + ";");
   }
   return duckdb;
}

/** An in-memory DuckDB as the `duckdb` connection of a MalloyConfig. */
export function duckdbMalloyConfig(duckdb?: DuckDBConnection): {
   malloyConfig: MalloyConfig;
   duckdb: DuckDBConnection;
} {
   const connection = duckdb ?? new DuckDBConnection("duckdb", ":memory:");
   const connections = new FixedConnectionMap(
      new Map([["duckdb", connection]]),
      "duckdb",
   );
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => connections);
   return { malloyConfig, duckdb: connection };
}

export interface TempPackageOptions {
   /** The package name; also written to publisher.json unless `manifest` is given. */
   name?: string;
   /** publisher.json contents. Defaults to `{ name }`. */
   manifest?: Record<string, unknown>;
   /** Directory-name prefix under the OS temp dir. */
   prefix?: string;
}

/**
 * Write `files` (relative path → contents) plus a publisher.json into a fresh
 * temp directory. `cleanup` removes it.
 */
export function writeTempPackage(
   files: Record<string, string>,
   {
      name = "pkg",
      manifest,
      prefix = "publisher-pkg-",
   }: TempPackageOptions = {},
): { dir: string; cleanup: () => void } {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify(manifest ?? { name }),
   );
   for (const [relative, contents] of Object.entries(files)) {
      const target = path.join(dir, relative);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, contents);
   }
   return {
      dir,
      cleanup: () => fs.rmSync(dir, { recursive: true, force: true }),
   };
}

export interface LoadedTempPackage {
   pkg: Package;
   dir: string;
   malloyConfig: MalloyConfig;
   duckdb: DuckDBConnection;
   /**
    * Releases the package's connections, closes the DuckDB connection and
    * removes the directory.
    */
   cleanup: () => Promise<void>;
}

/** {@link writeTempPackage}, then load it with `Package.create`. */
export async function loadTempPackage(
   files: Record<string, string>,
   options: TempPackageOptions & {
      environmentName?: string;
      duckdb?: DuckDBConnection;
   } = {},
): Promise<LoadedTempPackage> {
   const { dir, cleanup: removeDir } = writeTempPackage(files, options);
   const { malloyConfig, duckdb } = duckdbMalloyConfig(options.duckdb);
   const cleanup = async () => {
      await duckdb.close();
      removeDir();
   };
   try {
      const pkg = await Package.create(
         options.environmentName ?? "env",
         options.name ?? "pkg",
         dir,
         malloyConfig,
      );
      return {
         pkg,
         dir,
         malloyConfig,
         duckdb,
         cleanup: async () => {
            await pkg.getMalloyConfig().releaseConnections();
            await cleanup();
         },
      };
   } catch (error) {
      await cleanup();
      throw error;
   }
}

export interface TempModel {
   model: Model;
   dir: string;
   duckdb: DuckDBConnection;
   /** Closes the DuckDB connection and removes the directory. */
   cleanup: () => Promise<void>;
}

/**
 * {@link writeTempPackage}, then compile one model of it in process with
 * `Model.create` over `duckdb` (default: a fresh in-memory one). For specs
 * that drive a single `Model` rather than a whole served package.
 */
export async function createTempModel(
   files: Record<string, string>,
   entry: string,
   options: TempPackageOptions & { duckdb?: DuckDBConnection } = {},
): Promise<TempModel> {
   const { dir, cleanup: removeDir } = writeTempPackage(files, options);
   const duckdb = options.duckdb ?? (await seededDuckdb());
   const cleanup = async () => {
      await duckdb.close();
      removeDir();
   };
   try {
      const model = await Model.create(
         options.name ?? "pkg",
         dir,
         entry,
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, dir, duckdb, cleanup };
   } catch (error) {
      await cleanup();
      throw error;
   }
}

/**
 * Route this file's `Package.create` calls through a one-worker
 * {@link PackageLoadPool}, the production load path. Registers
 * `beforeAll`/`afterAll` in the calling scope. One pool per file: a fresh
 * worker per test crashes Bun, because DuckDB's native bindings do not
 * tolerate being loaded into several worker isolates of one process at once.
 */
export function usePackageLoadPool(): {
   /** The live pool (a new one after {@link restore}). */
   readonly pool: PackageLoadPool;
   /** Install a fresh live pool, e.g. after a test swapped in a dead one. */
   restore: () => Promise<void>;
} {
   const original = process.env.PACKAGE_LOAD_WORKERS;
   let pool: PackageLoadPool;
   const restore = async () => {
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   };
   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      await restore();
   });
   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (original === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = original;
   });
   return {
      get pool() {
         return pool;
      },
      restore,
   };
}
