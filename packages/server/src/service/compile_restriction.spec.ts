// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Construct containment on the `/compile` path.
 *
 * Malloy resolves a source's schema at COMPILE time -- `duckdb.sql("...")`
 * issues a `DESCRIBE` against the connection before any query runs -- so a
 * compile-only endpoint still reaches whatever the connection can reach. With
 * DuckDB's external access that makes an unrestricted compile an oracle: the
 * error text distinguishes a file that exists from one that does not, resolves
 * the columns of whatever it reads, and an `https://` path issues the request.
 *
 * These tests are written from that threat model. Each hostile case is a way
 * caller-submitted compile text could probe the filesystem or the network, and
 * each is paired with the assertion that the fragment scope refuses it. The
 * legitimate cases pin the other half of the contract: the authoring scopes
 * where the source IS the file must keep accepting `import` and
 * `connection.table(...)`, or an ordinary package becomes un-authorable.
 *
 * The oracle is the real Malloy compiler against a real DuckDB, never a
 * re-implementation of the restriction rules -- only Malloy decides which
 * spellings reach a connection.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { CompileRefusedError } from "../errors";
import { Environment } from "./environment";

// The curated model. It publishes `base_source` and nothing else; every
// hostile fragment below tries to reach past it.
const BASE_MODEL = `source: base_source is duckdb.sql("select 1 as id, 5 as n") extend {
  measure: c is count()
}`;

const TRACKS_MODEL = `import "base.malloy"
source: tracks is base_source extend {
  measure: total is n.sum()
}`;

describe("compile construct containment", () => {
   let rootDir: string;
   let env: Environment;
   // A file outside the package that the model never references. Reading it,
   // or merely learning whether it exists, is the leak under test.
   let secretPath: string;
   let absentPath: string;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-restrict-"));
      secretPath = path.join(rootDir, "secret.csv");
      absentPath = path.join(rootDir, "no-such-file.csv");
      await fs.writeFile(secretPath, "ssn,holder\n111-11-1111,alice\n");
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            '{"name":"pkg"}',
         );
         await fs.writeFile(path.join(stagingPath, "base.malloy"), BASE_MODEL);
         await fs.writeFile(
            path.join(stagingPath, "tracks.malloy"),
            TRACKS_MODEL,
         );
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const compile = (
      source: string | undefined,
      scope: "append" | "file" | "package",
      modelPath = "base.malloy",
   ) => env.compileSource("pkg", modelPath, source, false, undefined, scope);

   const errorsFor = async (
      source: string,
      scope: "append" | "file" | "package",
      modelPath = "base.malloy",
   ): Promise<string[]> => {
      const { problems } = await compile(source, scope, modelPath);
      return problems
         .filter((p) => p.severity === "error")
         .map((p) => p.message);
   };

   /** The error a compile threw, or a failure if it unexpectedly succeeded. */
   const refusalFor = async (
      source: string,
      scope: "append" | "file" | "package",
      modelPath = "base.malloy",
   ): Promise<Error> => {
      try {
         await compile(source, scope, modelPath);
      } catch (error) {
         return error as Error;
      }
      throw new Error(
         `Expected the compile to be refused, but it succeeded: ${source}`,
      );
   };

   // -- the fragment scope refuses text that reaches outside the model -------

   describe('scope "append" refuses constructs that reach a connection', () => {
      it("refuses raw SQL, so the compile-time DESCRIBE never runs", async () => {
         await expect(
            compile(
               `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: ssn }`,
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      // `name!type(...)` and the `sql_*` family are classified inside
      // `getExpression(fs)`, which needs a resolved FieldSpace -- unlike the
      // five constructs refused on sight. So they are the two the gate can
      // only see when the base model loaded, and a base model that does not
      // load has to fail the request rather than pass the fragment through.
      it("refuses a raw-SQL function call", async () => {
         await expect(
            compile(
               `run: base_source -> { group_by: v is read_csv!string('${secretPath}') }`,
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses the fragment when the base model cannot be loaded", async () => {
         // Without this the gate compiled against no base namespace, which
         // cannot resolve `base_source`, so the construct above was never
         // classified and the real compile ran it for real.
         await expect(
            compile(
               `run: base_source -> { group_by: v is read_csv!string('${secretPath}') }`,
               "append",
               "no_such_model.malloy",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses direct table access", async () => {
         await expect(
            compile(
               "run: duckdb.table('secrets') -> { aggregate: n is count() }",
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses an import that would borrow another model's surface", async () => {
         await expect(
            compile(
               'import "tracks.malloy"\nrun: tracks -> { aggregate: total }',
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses an import that escapes the package directory", async () => {
         // The import path is caller-controlled and the URL reader resolves it
         // against the filesystem with no containment of its own, so a
         // traversal reads and compiles a model the package never published --
         // including whatever connection that model declares.
         await fs.writeFile(
            path.join(rootDir, "outside.malloy"),
            `source: outside_src is duckdb.sql("select 42 as leaked") extend {\n  measure: k is count()\n}`,
         );
         await expect(
            compile(
               'import "../../outside.malloy"\nrun: outside_src -> { aggregate: k }',
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("refuses a raw table smuggled in through a join", async () => {
         await expect(
            compile(
               "source: x is base_source extend {\n" +
                  "  join_cross: s is duckdb.table('secrets')\n" +
                  "}\n" +
                  "run: x -> { group_by: s.ssn }",
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });

      it("names the offending construct and the scope that accepts it", async () => {
         // Pin the message, not merely that something threw: an author who hits
         // this needs to know which construct was refused and where it belongs.
         const error = await refusalFor(
            'run: duckdb.sql("SELECT 1 as x") -> { group_by: x }',
            "append",
         );
         expect(error.message).toContain("raw SQL is not permitted");
         expect(error.message).toContain('scope "file"');
      });
   });

   // -- the filesystem / network oracle is closed ----------------------------

   describe("the compile-time schema probe no longer reaches the filesystem", () => {
      it("cannot distinguish an existing file from a missing one", async () => {
         // The oracle: unrestricted, the present file compiles clean and the
         // absent one reports DuckDB's "No files found that match the pattern".
         // Refusing both identically is what removes the signal.
         const present = await refusalFor(
            `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: ssn }`,
            "append",
         );
         const missing = await refusalFor(
            `run: duckdb.sql("SELECT * FROM read_csv('${absentPath}')") -> { group_by: ssn }`,
            "append",
         );

         expect(present).toBeInstanceOf(CompileRefusedError);
         expect(missing).toBeInstanceOf(CompileRefusedError);
         // Same refusal either way: nothing in the response separates them.
         expect(present.message).toBe(missing.message);
         for (const message of [present.message, missing.message]) {
            expect(message).not.toContain("No files found");
            expect(message).not.toContain(secretPath);
            expect(message).not.toContain(absentPath);
         }
      });

      it("cannot enumerate the columns of a file it names", async () => {
         // Unrestricted, `ssn` resolves against the CSV's real header while a
         // bogus name reports "not defined" -- a column oracle over arbitrary
         // file content. Both must now be refused before the file is read.
         const real = await refusalFor(
            `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: ssn }`,
            "append",
         );
         const bogus = await refusalFor(
            `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: not_a_column }`,
            "append",
         );

         expect(real).toBeInstanceOf(CompileRefusedError);
         expect(bogus).toBeInstanceOf(CompileRefusedError);
         expect(bogus.message).not.toContain("not_a_column");
      });

      it("refuses an http source rather than issuing the request", async () => {
         // SSRF via the same compile-time probe. Refused on the construct, so
         // no request is made and the test needs no network.
         await expect(
            compile(
               `run: duckdb.sql("SELECT * FROM read_csv('https://example.invalid/x.csv')") -> { group_by: a }`,
               "append",
            ),
         ).rejects.toThrow(CompileRefusedError);
      });
   });

   // -- ordinary authoring keeps working -------------------------------------

   describe('scope "append" still validates ordinary fragments', () => {
      it("accepts a query over the model's published source", async () => {
         expect(
            await errorsFor("run: base_source -> { aggregate: c }", "append"),
         ).toEqual([]);
      });

      it("accepts the documented top-level query: fragment form", async () => {
         expect(
            await errorsFor(
               "query: check is base_source -> { aggregate: c }",
               "append",
            ),
         ).toEqual([]);
      });

      it("accepts the documented throwaway extend form", async () => {
         expect(
            await errorsFor(
               "source: check is base_source extend { measure: m2 is c }",
               "append",
            ),
         ).toEqual([]);
      });

      it("still reports an ordinary compile error as a diagnostic, not a refusal", async () => {
         // The gate must not swallow or restyle ordinary problems: a bad field
         // is still a diagnostic with its own message, not a 400 refusal.
         const errors = await errorsFor(
            "run: base_source -> { group_by: no_such_field }",
            "append",
         );
         expect(errors.join(" ")).toContain("no_such_field");
      });
   });

   // -- the authoring scopes are deliberately NOT restricted -----------------

   describe('scopes "file" and "package" still accept authoring constructs', () => {
      it("file: a model may define its own source from raw SQL", async () => {
         // The source IS the file here, so this is an author writing a model,
         // not a caller probing one. Restricting it would make the package
         // un-authorable.
         expect(
            await errorsFor(
               `source: fresh is duckdb.sql("select 2 as id") extend {\n  measure: c is count()\n}`,
               "file",
            ),
         ).toEqual([]);
      });

      it("file: a model may import another model in the package", async () => {
         expect(
            await errorsFor(
               'import "base.malloy"\nsource: derived is base_source extend { measure: c2 is c }',
               "file",
               "derived.malloy",
            ),
         ).toEqual([]);
      });

      it("package: a what-if replacement may use raw SQL and imports", async () => {
         const { problems } = await compile(
            'import "base.malloy"\nsource: tracks is base_source extend { measure: total is n.sum() }',
            "package",
            "tracks.malloy",
         );
         expect(problems.filter((p) => p.severity === "error")).toEqual([]);
      });
   });
});
