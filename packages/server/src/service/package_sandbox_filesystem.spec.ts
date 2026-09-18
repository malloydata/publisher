// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Package } from "./package";

/**
 * Pins the filesystem boundary of the per-package DuckDB sandbox.
 *
 * The sandbox is reachable with caller-supplied raw SQL: the connection named
 * `duckdb` is resolvable per package, and the raw-SQL query path runs the
 * statement verbatim against it. DuckDB's own file functions (`read_text`,
 * `read_csv_auto`, `COPY ... TO`, and the `httpfs` URL readers) therefore run
 * with whatever filesystem reach the session was opened with. On a worker that
 * hosts more than one tenant, "whatever the process can reach" is the host
 * filesystem, so the sandbox has to carry an explicit boundary rather than
 * inherit DuckDB's permissive default.
 *
 * Asserted here as BEHAVIOUR against a real DuckDB session rather than against
 * the config object, because the settings that produce this boundary interact
 * in a way that reading the config cannot reveal. `allowedDirectories` is a
 * carve-out FROM a denial -- DuckDB documents it as the paths allowed "even
 * when enable_external_access is false" -- so an allowlist carried on its own,
 * with no policy turning that denial on, restricts nothing. A config-shape
 * assertion would pass on exactly that no-op, which is why each case below
 * runs real SQL through the connector and reads the engine's answer.
 *
 * The package root stays readable because that is the sandbox's job: a package
 * ships its own `.csv`/`.parquet`/`.duckdb` files and the model compiles
 * against them.
 */
describe("per-package DuckDB sandbox filesystem boundary", () => {
   const tempDirs: string[] = [];
   const openConnections: DuckDBConnection[] = [];

   const tempDir = async (label: string): Promise<string> => {
      const dir = await fs.mkdtemp(
         path.join(os.tmpdir(), `pkg-sandbox-${label}-`),
      );
      // The package root is compared against DuckDB's canonicalized view of
      // it, and on macOS os.tmpdir() is a /var -> /private/var symlink.
      tempDirs.push(dir);
      return await fs.realpath(dir);
   };

   const sandboxFor = async (
      packagePath: string,
   ): Promise<DuckDBConnection> => {
      const config = Package.buildPackageMalloyConfig(packagePath, () => {
         throw new Error(
            "the sandbox must resolve without consulting the environment",
         );
      });
      const connection = await config.connections.lookupConnection("duckdb");
      openConnections.push(connection as DuckDBConnection);
      return connection as DuckDBConnection;
   };

   afterEach(async () => {
      for (const connection of openConnections) {
         await connection.close().catch(() => undefined);
      }
      openConnections.length = 0;
      for (const dir of tempDirs) {
         // Best-effort: a still-locked file must not fail the test.
         await fs
            .rm(dir, { recursive: true, force: true })
            .catch(() => undefined);
      }
      tempDirs.length = 0;
   });

   it("reads a data file the package ships", async () => {
      const packagePath = await tempDir("reads-own");
      await fs.writeFile(
         path.join(packagePath, "rows.csv"),
         "id,label\n1,alpha\n2,beta\n",
      );

      const connection = await sandboxFor(packagePath);
      const result = await connection.runSQL(
         `SELECT count(*) AS n FROM read_csv_auto('${path.join(packagePath, "rows.csv")}')`,
      );
      const rows = Array.isArray(result) ? result : result.rows;

      expect(Number(rows[0]["n"])).toBe(2);
   });

   it("refuses to read a file outside the package root", async () => {
      const packagePath = await tempDir("read-escape");
      const outside = await tempDir("read-secret");
      const secretPath = path.join(outside, "secret.txt");
      await fs.writeFile(secretPath, "tenant-secret");

      const connection = await sandboxFor(packagePath);

      await expect(
         connection.runSQL(`SELECT content FROM read_text('${secretPath}')`),
      ).rejects.toThrow(/Permission Error/i);
   });

   it("refuses to write a file outside the package root", async () => {
      const packagePath = await tempDir("write-escape");
      const outside = await tempDir("write-target");
      const writtenPath = path.join(outside, "exfiltrated.csv");

      const connection = await sandboxFor(packagePath);

      await expect(
         connection.runSQL(`COPY (SELECT 1 AS x) TO '${writtenPath}'`),
      ).rejects.toThrow(/Permission Error/i);

      // The refusal has to mean the file was never produced, not merely that
      // the statement reported an error after writing it.
      await expect(fs.access(writtenPath)).rejects.toThrow();
   });

   it("refuses to reach a network URL", async () => {
      const packagePath = await tempDir("ssrf");
      const connection = await sandboxFor(packagePath);

      // Reaching the network is the other half of what this boundary denies:
      // the same `enable_external_access` switch gates URL readers, so an
      // unsandboxed session can be pointed at an internal address.
      //
      // The assertion is that the refusal comes from CONFIGURATION rather than
      // from the request failing. `.invalid` is reserved by RFC 2606 and never
      // resolves, so an unrestricted session fails here too -- but with a DNS
      // or IO error, which `Permission Error` does not match. Matching the
      // message, not merely "it threw", is what keeps this from passing for the
      // wrong reason; a literal internal address would instead make the suite
      // issue the very request under test and hang on its timeout.
      await expect(
         connection.runSQL(
            "SELECT * FROM read_csv_auto('http://metadata.invalid/creds.csv')",
         ),
      ).rejects.toThrow(/Permission Error/i);
   });
});
