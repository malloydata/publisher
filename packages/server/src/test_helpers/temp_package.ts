// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package on disk, loaded through the real production path
 * (`Package.create`, which compiles in the package-load worker pool), over an
 * in-memory DuckDB. For specs that need what a served package actually does,
 * not a hand-built model.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import fs from "fs";
import os from "os";
import path from "path";
import { Package } from "../service/package";

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
   /** Closes the DuckDB connection and removes the directory. */
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
      return { pkg, dir, malloyConfig, duckdb, cleanup };
   } catch (error) {
      await cleanup();
      throw error;
   }
}
