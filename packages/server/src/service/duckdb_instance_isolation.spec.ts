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
 * by a share key that deliberately EXCLUDES the connection name, and it never
 * gives a `:memory:` primary a private instance on config alone. Meanwhile the
 * DuckLake attach aliases by connection name (`ATTACH OR REPLACE … AS <name>`).
 * Put those together and two same-named connections that land on ONE pooled
 * instance clobber each other's attach: one ends up reading the other's database.
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
 */
describe("DuckDB instance isolation", () => {
   const tempDirs: string[] = [];
   const openConnections: DuckDBConnection[] = [];

   const tempDir = async (label: string): Promise<string> => {
      const dir = await fs.mkdtemp(path.join(os.tmpdir(), `duckdb-iso-${label}-`));
      tempDirs.push(dir);
      return dir;
   };

   afterEach(async () => {
      for (const connection of openConnections) {
         await connection.close().catch(() => undefined);
      }
      openConnections.length = 0;
      for (const dir of tempDirs) {
         await fs.rm(dir, { recursive: true, force: true }).catch(
            () => undefined,
         );
      }
      tempDirs.length = 0;
   });

   /** A standalone DuckDB file holding `t` with a single distinguishing value. */
   const seedDatabase = async (label: string, value: number): Promise<string> => {
      const dir = await tempDir(`seed-${label}`);
      const file = path.join(dir, `${label}.duckdb`);
      const connection = new DuckDBConnection(`seed_${label}`, file, dir);
      try {
         await connection.runSQL(`CREATE OR REPLACE TABLE t AS SELECT ${value} AS x`);
      } finally {
         await connection.close().catch(() => undefined);
      }
      return file;
   };

   const firstNumber = (rows: Record<string, unknown>[]): number =>
      Number(Object.values(rows[0])[0]);

   it("keeps same-named connections apart when their working directories differ", async () => {
      const fileA = await seedDatabase("a", 1);
      const fileB = await seedDatabase("b", 2);

      // Identical connection NAME and identical `:memory:` primary — only the
      // working directory differs. That is exactly the shape a build session
      // uses, and the only thing standing between it and a shared instance.
      //
      // ORDERING IS LOAD-BEARING FOR THIS TEST. The instance cache is consulted
      // inside an async `init()`, so constructing both connections before either
      // is used lets both race past the cache miss and each create a private
      // instance — isolation for the wrong reason, and a test that could never
      // fail. Fully initialize the first (the ATTACH forces it) before building
      // the second, so a shared share key really would hand back the cached
      // instance. Verified: with identical working directories this ordering
      // yields a=2, b=2 — the clobber.
      const dirA = await tempDir("session-a");
      const a = new DuckDBConnection("store", ":memory:", dirA);
      openConnections.push(a);
      // Same alias in both, with OR REPLACE, mirroring the DuckLake attach. On a
      // shared instance the second call would silently replace the first.
      await a.runSQL(`ATTACH OR REPLACE '${fileA}' AS lake (READ_ONLY);`);

      const dirB = await tempDir("session-b");
      const b = new DuckDBConnection("store", ":memory:", dirB);
      openConnections.push(b);
      await b.runSQL(`ATTACH OR REPLACE '${fileB}' AS lake (READ_ONLY);`);

      expect(firstNumber((await a.runSQL(`SELECT x FROM lake.t;`)).rows)).toBe(1);
      expect(firstNumber((await b.runSQL(`SELECT x FROM lake.t;`)).rows)).toBe(2);
   });

   it("keeps a build-session-shaped connection apart from a long-lived one", async () => {
      // The pairing the build-session comment calls out specifically: a transient
      // build session overlapping a long-lived serve connection. Same name, same
      // alias, both `:memory:` — distinct only by working directory.
      const serveFile = await seedDatabase("serve", 10);
      const buildFile = await seedDatabase("build", 20);

      const serveDir = await tempDir("serve-session");
      const serve = new DuckDBConnection("credible", ":memory:", serveDir);
      openConnections.push(serve);
      await serve.runSQL(`ATTACH OR REPLACE '${serveFile}' AS lake (READ_ONLY);`);

      const buildDir = await tempDir("build-session");
      const build = new DuckDBConnection("credible", ":memory:", buildDir);
      openConnections.push(build);
      await build.runSQL(`ATTACH OR REPLACE '${buildFile}' AS lake (READ_ONLY);`);

      // The serve connection must still see its own database after the build
      // session attaches a different one under the same alias.
      expect(
         firstNumber((await serve.runSQL(`SELECT x FROM lake.t;`)).rows),
      ).toBe(10);
      expect(
         firstNumber((await build.runSQL(`SELECT x FROM lake.t;`)).rows),
      ).toBe(20);
   });
});
