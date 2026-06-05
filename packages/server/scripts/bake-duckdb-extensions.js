#!/usr/bin/env node

/**
 * Pre-download ("bake") the DuckDB extensions the server loads at runtime so
 * they are on disk before any query runs.
 *
 * DuckDB fetches extensions on first use from extensions.duckdb.org and caches
 * them under ~/.duckdb/extensions/v<version>/<platform>/. Some hosts cannot
 * reach that CDN (notably the macOS GitHub Actions fleet), so an on-demand
 * fetch at query time fails and takes the query (or a test) down. Baking
 * populates the cache via the same @duckdb/node-api engine the runtime uses,
 * so later INSTALL/LOAD calls are served from disk.
 *
 * Runs as the last step of this package's build (`build` / `build:server-only`),
 * so local dev, CI, and the Docker builder all bake the same set. The Docker
 * final stage copies the baked ~/.duckdb/extensions from the builder rather
 * than re-baking, keeping a single bake mechanism.
 *
 * Each extension is baked independently: a failure (offline build, transient
 * CDN error) is logged and skipped, never aborting the others or the build.
 */

import { DuckDBInstance } from "@duckdb/node-api";

// Every extension the connection layer (packages/server/src/service/connection.ts)
// and storage manager INSTALL/LOAD at runtime, for cloud attach, the per-package
// sandbox, federated-database attach, and the materialization catalog. Keep this
// in sync with the install sites in those files.
//
// `community: true` mirrors the runtime's `FORCE INSTALL '<name>' FROM community`
// (bigquery, snowflake); the rest are core extensions installed by name.
// `registered` is the name the extension reports in duckdb_extensions() when it
// differs from the INSTALL name (only postgres -> postgres_scanner).
const EXTENSIONS = [
   { name: "httpfs", community: false }, // cloud storage (gcs/s3/azure) + per-package sandbox
   { name: "aws", community: false }, // s3 credential chain
   { name: "azure", community: false }, // azure blob storage
   { name: "postgres", community: false, registered: "postgres_scanner" }, // postgres attach + ducklake postgres catalog
   { name: "ducklake", community: false }, // materialization catalog
   { name: "bigquery", community: true },
   { name: "snowflake", community: true },
];

async function main() {
   const instance = await DuckDBInstance.create(":memory:");
   const connection = await instance.connect();

   const results = [];
   for (const { name, community, registered } of EXTENSIONS) {
      try {
         const install = community
            ? `FORCE INSTALL '${name}' FROM community;`
            : `INSTALL ${name};`;
         await connection.run(`${install} LOAD ${name};`);

         // Verify the extension actually reports as loaded, rather than trusting
         // that INSTALL/LOAD returned without error.
         const reader = await connection.runAndReadAll(
            `SELECT loaded, installed FROM duckdb_extensions() WHERE extension_name = '${registered ?? name}';`,
         );
         const row = reader.getRowObjectsJS()[0];
         const loaded = row?.loaded === true;
         const installed = row?.installed === true;

         results.push({ name, installed, loaded });
         if (loaded) {
            console.log(`baked DuckDB extension: ${name} (loaded)`);
         } else {
            console.warn(
               `DuckDB extension "${name}" installed=${installed} but not loaded`,
            );
         }
      } catch (err) {
         const message = err instanceof Error ? err.message : String(err);
         results.push({
            name,
            installed: false,
            loaded: false,
            error: message,
         });
         console.warn(`skipped DuckDB extension "${name}": ${message}`);
      }
   }

   const ok = results.filter((r) => r.loaded).map((r) => r.name);
   const missing = results.filter((r) => !r.loaded).map((r) => r.name);
   console.log(
      `DuckDB extensions baked: ${ok.length}/${results.length} loaded` +
         (ok.length ? ` [${ok.join(", ")}]` : "") +
         (missing.length ? `; not loaded [${missing.join(", ")}]` : ""),
   );

   connection.closeSync();
   instance.closeSync();
}

main().catch((err) => {
   // Never fail the build on a bake error -- the extensions are an optimization,
   // not a correctness requirement (the runtime can still fetch on demand where
   // the network allows).
   console.warn(
      `DuckDB extension bake skipped: ${err instanceof Error ? err.message : String(err)}`,
   );
});
