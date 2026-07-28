import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";

/**
 * Pins the DuckDB instance-isolation contract that same-named connections depend
 * on for multi-tenant safety.
 *
 * `@malloydata/db-duckdb` pools DuckDB instances in a process-global cache keyed
 * by a share key that deliberately EXCLUDES the connection name, and it caches a
 * `:memory:` primary like any other. Meanwhile the DuckLake attach aliases by
 * connection name (`ATTACH OR REPLACE … AS <name>`). Put those together and two
 * same-named connections that land on ONE pooled instance clobber each other's
 * attach: one ends up reading the other's database.
 *
 * `createIsolatedBuildSession` prevents that by giving every build session a
 * unique `workingDirectory`, relying on it participating in the share key. That
 * is an upstream implementation detail, so nothing here asserted it — and the
 * failure mode is silent: no error, just the wrong data. This spec asserts the
 * behaviour instead of the mechanism, so it holds however upstream achieves it.
 *
 * Deliberately NOT asserted: the converse (same name AND same working directory
 * DO share an instance). That is the pooling artifact, not a property we want; if
 * upstream ever made pooling connection-aware, asserting it would fail on an
 * improvement.
 *
 * Each connection creates and populates its OWN attached store rather than
 * reopening a pre-seeded file. Handing a file from one connection to another
 * requires the first to have released its OS lock, and `close()` is refcount-only
 * — which fails outright under Windows' mandatory locking. Attaching a store the
 * connection itself owns avoids the handoff, and matches how a DuckLake
 * destination is actually used.
 */
describe("DuckDB instance isolation", () => {
   const tempDirs: string[] = [];
   const openConnections: DuckDBConnection[] = [];

   const tempDir = async (label: string): Promise<string> => {
      const dir = await fs.mkdtemp(
         path.join(os.tmpdir(), `duckdb-iso-${label}-`),
      );
      tempDirs.push(dir);
      return dir;
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

   /**
    * A connection with the given name and its own working directory, which
    * attaches its own private store under `alias` and writes one distinguishing
    * value into it.
    */
   const connectionWithOwnStore = async (
      name: string,
      label: string,
      alias: string,
      value: number,
   ): Promise<DuckDBConnection> => {
      const sessionDir = await tempDir(`session-${label}`);
      const connection = new DuckDBConnection(name, ":memory:", sessionDir);
      openConnections.push(connection);
      const storeDir = await tempDir(`store-${label}`);
      const storeFile = path.join(storeDir, `${label}.duckdb`);
      // OR REPLACE mirrors the DuckLake attach: on a shared instance the second
      // caller silently replaces the first caller's alias.
      await connection.runSQL(`ATTACH OR REPLACE '${storeFile}' AS ${alias};`);
      await connection.runSQL(
         `CREATE OR REPLACE TABLE ${alias}.t AS SELECT ${value} AS x;`,
      );
      return connection;
   };

   const readValue = async (
      connection: DuckDBConnection,
      alias: string,
   ): Promise<number> => {
      const result = await connection.runSQL(`SELECT x FROM ${alias}.t;`);
      return Number(Object.values(result.rows[0])[0]);
   };

   it("keeps same-named connections apart when their working directories differ", async () => {
      // Identical connection NAME and identical `:memory:` primary — only the
      // working directory differs. That is exactly the shape a build session
      // uses, and the only thing standing between it and a shared instance.
      //
      // ORDERING IS LOAD-BEARING FOR THIS TEST. The instance cache is consulted
      // inside an async `init()`, so constructing both connections before either
      // is used lets both race past the cache miss and each create a private
      // instance — isolation for the wrong reason, and a test that could never
      // fail. Each helper call fully initializes its connection (the ATTACH
      // forces it) before the next is built, so a shared share key really would
      // hand back the cached instance. Verified: with identical working
      // directories this ordering yields 2 for both connections — the clobber.
      const a = await connectionWithOwnStore("store", "a", "lake", 1);
      const b = await connectionWithOwnStore("store", "b", "lake", 2);

      expect(await readValue(a, "lake")).toBe(1);
      expect(await readValue(b, "lake")).toBe(2);
   });

   it("keeps a build-session-shaped connection apart from a long-lived one", async () => {
      // The pairing the build-session comment calls out specifically: a transient
      // build session overlapping a long-lived serve connection. Same name, same
      // alias, both `:memory:` — distinct only by working directory.
      const serve = await connectionWithOwnStore(
         "credible",
         "serve",
         "lake",
         10,
      );
      const build = await connectionWithOwnStore(
         "credible",
         "build",
         "lake",
         20,
      );

      // The serve connection must still see its own store after the build
      // session attaches a different one under the same alias.
      expect(await readValue(serve, "lake")).toBe(10);
      expect(await readValue(build, "lake")).toBe(20);
   });
});
